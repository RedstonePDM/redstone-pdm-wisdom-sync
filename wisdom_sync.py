"""
Wisdom Sync Service - Redstone PDM
====================================
Read-only extraction of job data from Wisdom (JD Wetherspoon contractor platform).
Authenticates using session cookies, queries OData API, stores in PostgreSQL.

HARD CONSTRAINTS - THIS SERVICE WILL NEVER:
- POST, PUT, PATCH or DELETE anything in Wisdom
- Upload costs to any job
- Close down any job
- Raise queries to Wetherspoon
- Modify any data on the Wisdom platform

This service is READ-ONLY without exception.
"""

import os
import re
import time
import asyncio
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from urllib.parse import urljoin

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
WISDOM_BASE     = "https://wisdom.jdwetherspoon.co.uk"
WISDOM_LOGIN    = f"{WISDOM_BASE}/wisdom(bD1lbiZjPTEwMA==)/index.htm"
WISDOM_DATA     = f"{WISDOM_BASE}/WISDOM_DATA"
WISDOM_EMAIL    = os.environ["WISDOM_EMAIL"]
WISDOM_PASSWORD = os.environ["WISDOM_PASSWORD"]
DATABASE_URL    = os.environ["DATABASE_URL"]

# Tabs and sub-tabs to extract
# Format: (Tab value, Item value, friendly name, fixed_description or None)
EXTRACTION_TARGETS = [
    ("CALLOUT",       "AWAITINGATTENDANCE", "Callout - Awaiting Attendance",         None),
    ("QUOTEREQUEST",  "AWAITINGSUBMISSION", "Quote Request - Awaiting Submission",   None),
    ("QUOTEREQUEST",  "AWAITINGAPPROVAL",   "Quote Request - Awaiting Approval",     None),
    ("QUOTE",         "AWAITINGATTENDANCE", "Quote - Awaiting Attendance",           None),
    ("MIV",           "AWAITINGATTENDANCE", "MIV - Awaiting Attendance",             "MIV Tasks"),
    ("PPM",           "AWAITINGAPPROVAL",   "PPM - Awaiting Approval",               None),
]

# Deep-scrape targets — navigate into each job for outcome reason text
OUTCOME_TARGETS = [
    ("QUOTEREQUEST", "REJECTED",      "Quote Request - Rejected"),
    ("QUOTE",        "CANCELLATIONS", "Quote - Cancellations"),
]

PAGE_SIZE = 200  # Fetch up to 200 jobs per tab in one call


# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    """Return a psycopg2 connection with RealDictCursor as default."""
    conn = psycopg2.connect(DATABASE_URL)
    return conn


def get_dict_cursor(conn):
    """Return a RealDictCursor for dict-style row access."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    """Create tables if they don't exist."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id              TEXT PRIMARY KEY,
            display_id          TEXT,
            tab                 TEXT,
            sub_tab             TEXT,
            tab_label           TEXT,
            job_type            TEXT,
            pub_name            TEXT,
            location_code       TEXT,
            postcode            TEXT,
            area                TEXT,
            trade_type          TEXT,
            sub_trade_type      TEXT,
            description         TEXT,
            additional_text     TEXT,
            due_date            TEXT,
            due_time            TEXT,
            date_released       TEXT,
            contractor_name     TEXT,
            contractor_email    TEXT,
            contractor_phone    TEXT,
            status              TEXT,
            first_seen          TIMESTAMPTZ DEFAULT NOW(),
            last_seen           TIMESTAMPTZ DEFAULT NOW(),
            last_updated        TIMESTAMPTZ DEFAULT NOW(),
            raw_json            JSONB
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id              SERIAL PRIMARY KEY,
            synced_at       TIMESTAMPTZ DEFAULT NOW(),
            tab_label       TEXT,
            jobs_found      INTEGER,
            jobs_new        INTEGER,
            jobs_updated    INTEGER,
            status          TEXT,
            error           TEXT
        );

        CREATE TABLE IF NOT EXISTS job_wetherspoons_costs (
            job_id          TEXT PRIMARY KEY,
            display_id      TEXT,
            job_type        TEXT,
            total_agreed    NUMERIC(10,2),
            visit_count     INTEGER,
            scraped_at      TIMESTAMPTZ DEFAULT NOW(),
            raw_totals_json JSONB
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info("Database initialised.")


# ── Wisdom Client (Async Playwright) ──────────────────────────────────────────

class WisdomClient:
    """
    Read-only HTTP client for the Wisdom OData API.
    Uses async Playwright to avoid asyncio loop conflicts.
    Never issues write requests.
    """

    def __init__(self):
        self.authenticated = False
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    async def authenticate(self):
        """
        Log in to Wisdom using async Playwright.
        Keeps the browser alive for all subsequent API calls.
        READ-ONLY: we only navigate and fetch data, never submit or modify anything.
        """
        import json as _json
        log.info("Authenticating with Wisdom via browser (async)...")

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        self._context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
        )
        self._page = await self._context.new_page()

        # Navigate to login page
        log.info(f"Navigating to: {WISDOM_LOGIN}")
        await self._page.goto(WISDOM_LOGIN, wait_until="domcontentloaded", timeout=60000)
        log.info("Login page loaded.")

        # Fill credentials
        await self._page.fill("input[name='sap-alias']", WISDOM_EMAIL)
        await self._page.fill("input[name='sap-password']", WISDOM_PASSWORD)
        await self._page.evaluate("callSubmitLogin('onLogin')")

        # Wait for post-login page to settle
        await self._page.wait_for_load_state("domcontentloaded", timeout=60000)
        await self._page.wait_for_timeout(2000)
        await self._page.wait_for_load_state("domcontentloaded", timeout=30000)
        log.info(f"Post-login URL: {self._page.url}")

        # Validate session
        log.info("Validating session via browser fetch...")
        result = await self._page.evaluate("""
            async () => {
                const resp = await fetch("https://wisdom.jdwetherspoon.co.uk/WISDOM_DATA/JobSet('10002107640')", {
                    headers: {
                        "Accept": "application/json",
                        "X-Requested-With": "XMLHttpRequest"
                    }
                });
                return { status: resp.status, text: await resp.text() };
            }
        """)
        log.info(f"Validation fetch: status={result['status']}, length={len(result['text'])}")
        log.info(f"Validation preview: {result['text'][:300]}")

        if result["status"] != 200:
            raise RuntimeError(f"Browser fetch validation failed: HTTP {result['status']}")

        try:
            data = _json.loads(result["text"])
            if not data.get("d", {}).get("JobId"):
                raise RuntimeError("Validation response has no JobId")
            log.info("Browser session validated successfully.")
        except Exception as e:
            raise RuntimeError(f"Validation parse error: {e}, body: {result['text'][:300]}")

        self.authenticated = True
        log.info("Authentication successful. Browser stays open for all API calls.")

    async def _browser_fetch(self, url):
        """Make a GET request via the browser context using the live SAP session."""
        import json as _json
        result = await self._page.evaluate(
            """(url) => fetch(url, {
                headers: {
                    "Accept": "application/json",
                    "X-Requested-With": "XMLHttpRequest"
                }
            }).then(r => r.text().then(t => ({ status: r.status, text: t })))""",
            url
        )
        log.debug(f"Browser fetch {url}: status={result['status']}")
        if result["status"] != 200:
            raise RuntimeError(f"Browser fetch failed: HTTP {result['status']} for {url}")
        return _json.loads(result["text"])

    async def get_job_list(self, tab, item, skip=0, top=PAGE_SIZE):
        """
        Fetch list of jobs for a given tab/sub-tab combination.
        Returns list of job objects from the OData response.
        READ-ONLY: GET request only.
        """
        url = (
            f"{WISDOM_DATA}/DashboardItemSet"
            f"(Tab='{tab}',Item='{item}')"
            f"/BusinessObject"
            f"?$skip={skip}&$top={top}&$inlinecount=allpages"
        )
        log.info(f"Fetching: {url}")
        data = await self._browser_fetch(url)
        results = data.get("d", {}).get("results", [])
        total   = int(data.get("d", {}).get("__count", len(results)))
        log.info(f"Got {len(results)} of {total} jobs")
        return results, total

    async def get_job_detail(self, job_id):
        """
        Fetch full detail for a single job by ID.
        READ-ONLY: GET request only.
        """
        url = f"{WISDOM_DATA}/JobSet('{job_id}')"
        data = await self._browser_fetch(url)
        return data.get("d", {})

    async def get_pub_postcode(self, pub_id):
        """
        Fetch pub postcode from PubSet API.
        READ-ONLY: GET request only.
        """
        uk_postcode_re = re.compile(
            r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s+\d[A-Z]{2})\b', re.I
        )
        try:
            url = f"{WISDOM_DATA}/PubSet('{pub_id}')"
            data = await self._browser_fetch(url)
            data = data.get("d", {})
            log.info(f"PubSet({pub_id}) string fields: { {k: v for k, v in data.items() if isinstance(v, str) and v} }")

            postcode = (
                data.get("PostCode", "") or data.get("Postcode", "") or data.get("PostalCode", "") or ""
            ).strip()

            if not postcode:
                for key, val in data.items():
                    if isinstance(val, str):
                        match = uk_postcode_re.search(val)
                        if match:
                            postcode = match.group(1).strip()
                            log.info(f"PubSet({pub_id}): postcode '{postcode}' extracted from field '{key}': {val}")
                            break

            if postcode:
                log.info(f"PubSet({pub_id}): final postcode = {postcode}")
            else:
                log.warning(f"PubSet({pub_id}): no postcode found. Full response: {data}")

            return postcode
        except Exception as e:
            log.warning(f"Pub postcode lookup failed for pub_id={pub_id}: {e}")
        return ""

    async def close(self):
        """Close browser and stop Playwright."""
        try:
            await self._browser.close()
            await self._playwright.stop()
            log.info("Browser closed.")
        except Exception:
            pass


# ── Database Operations ───────────────────────────────────────────────────────

def upsert_job(cur, job_data: dict, tab: str, sub_tab: str,
               tab_label: str, fixed_description: str | None):
    """Insert or update a job record in the database."""

    job_id = job_data.get("JobId") or job_data.get("DisplayId")
    if not job_id:
        return False, False

    description = fixed_description or job_data.get("Description", "").strip()
    postcode = (job_data.get("PostCode") or job_data.get("_postcode") or "").strip()
    now = datetime.now(timezone.utc)

    cur.execute("SELECT job_id, status FROM jobs WHERE job_id = %s", (job_id,))
    existing = cur.fetchone()

    row = {
        "job_id":           job_id,
        "display_id":       job_data.get("DisplayId", job_id),
        "tab":              tab,
        "sub_tab":          sub_tab,
        "tab_label":        tab_label,
        "job_type":         job_data.get("JobTypeText", ""),
        "pub_name":         job_data.get("PubName", "") or job_data.get("LocationText", ""),
        "location_code":    job_data.get("Location", ""),
        "postcode":         postcode,
        "area":             job_data.get("AreaText", ""),
        "trade_type":       job_data.get("TradetypeText", ""),
        "sub_trade_type":   job_data.get("SubtradetypeText", ""),
        "description":      description,
        "additional_text":  job_data.get("AdditionalText", ""),
        "due_date":         job_data.get("DueDate", ""),
        "due_time":         job_data.get("DueTime", ""),
        "date_released":    job_data.get("DateReleased", ""),
        "contractor_name":  job_data.get("ContractorName", ""),
        "contractor_email": job_data.get("ContractorEmail", ""),
        "contractor_phone": job_data.get("ContractorPhone", ""),
        "status":           sub_tab,
        "last_seen":        now,
        "last_updated":     now,
        "raw_json":         psycopg2.extras.Json(job_data),
    }

    if not existing:
        cur.execute("""
            INSERT INTO jobs (
                job_id, display_id, tab, sub_tab, tab_label,
                job_type, pub_name, location_code, postcode, area,
                trade_type, sub_trade_type, description, additional_text,
                due_date, due_time, date_released,
                contractor_name, contractor_email, contractor_phone,
                status, first_seen, last_seen, last_updated, raw_json
            ) VALUES (
                %(job_id)s, %(display_id)s, %(tab)s, %(sub_tab)s, %(tab_label)s,
                %(job_type)s, %(pub_name)s, %(location_code)s, %(postcode)s, %(area)s,
                %(trade_type)s, %(sub_trade_type)s, %(description)s, %(additional_text)s,
                %(due_date)s, %(due_time)s, %(date_released)s,
                %(contractor_name)s, %(contractor_email)s, %(contractor_phone)s,
                %(status)s, NOW(), %(last_seen)s, %(last_updated)s, %(raw_json)s
            )
        """, row)
        return True, False
    else:
        cur.execute("""
            UPDATE jobs SET
                tab=%(tab)s, sub_tab=%(sub_tab)s, tab_label=%(tab_label)s,
                job_type=%(job_type)s, pub_name=%(pub_name)s,
                location_code=%(location_code)s,
                postcode=CASE
                    WHEN %(postcode)s != '' THEN %(postcode)s
                    ELSE postcode
                END,
                area=%(area)s, trade_type=%(trade_type)s,
                sub_trade_type=%(sub_trade_type)s, description=%(description)s,
                additional_text=%(additional_text)s,
                due_date=%(due_date)s, due_time=%(due_time)s,
                date_released=%(date_released)s,
                contractor_name=%(contractor_name)s,
                contractor_email=%(contractor_email)s,
                contractor_phone=%(contractor_phone)s,
                status=%(status)s, last_seen=%(last_seen)s,
                last_updated=%(last_updated)s, raw_json=%(raw_json)s
            WHERE job_id=%(job_id)s
        """, row)
        return False, True


# ── Async Sync Logic ──────────────────────────────────────────────────────────

async def sync_target_async(client: WisdomClient, target: tuple, conn, cur):
    """Sync all jobs for one tab/sub-tab target."""
    tab, item, label, fixed_desc = target
    log.info(f"Syncing: {label}")

    jobs_found = jobs_new = jobs_updated = 0
    skip = 0
    pub_postcode_cache = {}

    try:
        while True:
            results, total = await client.get_job_list(tab, item, skip=skip)

            if not results:
                break

            jobs_found = total

            for job_summary in results:
                job_id = job_summary.get("JobId") or job_summary.get("DisplayId")
                if not job_id:
                    continue

                try:
                    if fixed_desc:
                        job_detail = job_summary
                    else:
                        job_detail = await client.get_job_detail(job_id)
                        await asyncio.sleep(0.2)

                    if not job_detail.get("PostCode") and not job_detail.get("_postcode"):
                        pub_id = (
                            job_detail.get("PubId")
                            or job_summary.get("PubId")
                            or ""
                        )
                        if not pub_id:
                            location = (
                                job_detail.get("Location", "")
                                or job_detail.get("LocationCode", "")
                                or job_summary.get("Location", "")
                            )
                            parts = location.split("-") if location else []
                            if len(parts) >= 2:
                                pub_id = parts[1]

                        if pub_id:
                            if pub_id not in pub_postcode_cache:
                                postcode = await client.get_pub_postcode(pub_id)
                                pub_postcode_cache[pub_id] = postcode
                                await asyncio.sleep(0.1)
                            else:
                                postcode = pub_postcode_cache[pub_id]
                            if postcode:
                                job_detail["_postcode"] = postcode
                                log.info(f"Job {job_id}: postcode set to {postcode} from pub {pub_id}")
                        else:
                            log.debug(f"Job {job_id}: no pub ID found for postcode lookup")

                except Exception as e:
                    log.warning(f"Could not fetch detail for job {job_id}: {e}")
                    job_detail = job_summary

                is_new, is_updated = upsert_job(cur, job_detail, tab, item, label, fixed_desc)
                if is_new:
                    jobs_new += 1
                elif is_updated:
                    jobs_updated += 1

            skip += len(results)
            if skip >= total:
                break

        conn.commit()

        cur.execute("""
            INSERT INTO sync_log (tab_label, jobs_found, jobs_new, jobs_updated, status)
            VALUES (%s, %s, %s, %s, 'success')
        """, (label, jobs_found, jobs_new, jobs_updated))
        conn.commit()

        log.info(f"  ✓ {label}: {jobs_found} found, {jobs_new} new, {jobs_updated} updated")

    except Exception as e:
        conn.rollback()
        cur.execute("""
            INSERT INTO sync_log (tab_label, jobs_found, jobs_new, jobs_updated, status, error)
            VALUES (%s, %s, %s, %s, 'error', %s)
        """, (label, jobs_found, jobs_new, jobs_updated, str(e)))
        conn.commit()
        log.error(f"  ✗ {label}: {e}")


async def backfill_postcodes_async(client, conn, cur):
    """Backfill postcodes for all jobs that are missing them."""
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    dict_cur.execute(
        "SELECT COUNT(*) as c FROM jobs WHERE postcode IS NULL OR postcode = ''"
    )
    row = dict_cur.fetchone()
    count = row["c"] if row else 0

    if count == 0:
        log.info("Postcode backfill: all jobs already have postcodes.")
        dict_cur.close()
        return

    log.info(f"Postcode backfill: {count} jobs missing postcodes. Fetching...")

    dict_cur.execute("""
        SELECT DISTINCT
            CASE
                WHEN location_code IS NOT NULL AND location_code != ''
                    THEN split_part(location_code, '-', 2)
                ELSE raw_json->>'PubId'
            END as pub_id,
            job_id
        FROM jobs
        WHERE (postcode IS NULL OR postcode = '')
        AND (
            (location_code IS NOT NULL AND location_code != '')
            OR (raw_json->>'PubId' IS NOT NULL AND raw_json->>'PubId' != '')
        )
    """)
    rows = dict_cur.fetchall()
    dict_cur.close()

    pub_cache = {}
    updated = 0

    for row in rows:
        pub_id = row["pub_id"]
        job_id = row["job_id"]
        if not pub_id:
            continue

        if pub_id not in pub_cache:
            postcode = await client.get_pub_postcode(pub_id)
            pub_cache[pub_id] = postcode
            await asyncio.sleep(0.15)
        else:
            postcode = pub_cache[pub_id]

        if postcode:
            cur.execute("""
                UPDATE jobs SET postcode = %s
                WHERE job_id = %s
                AND (postcode IS NULL OR postcode = '')
            """, (postcode, job_id))
            updated += cur.rowcount

    conn.commit()
    log.info(
        f"Postcode backfill complete: updated {updated} jobs "
        f"across {len(pub_cache)} unique pubs."
    )


async def remove_stale_jobs(conn, cur, sync_started_at):
    """
    After a full sync cycle, hard-delete any job that was NOT seen this cycle
    (last_seen < sync_started_at). Wisdom is the single source of truth —
    if a job is gone from Wisdom it should be gone from the planner.
    Also removes any allocations for deleted jobs to keep the grid clean.
    """
    try:
        # First remove allocations for jobs no longer on Wisdom
        cur.execute("""
            DELETE FROM allocations
            WHERE job_id IN (
                SELECT job_id FROM jobs
                WHERE last_seen < %s
            )
        """, (sync_started_at,))
        alloc_removed = cur.rowcount

        # Then delete the stale jobs themselves
        cur.execute("""
            DELETE FROM jobs
            WHERE last_seen < %s
        """, (sync_started_at,))
        jobs_removed = cur.rowcount

        conn.commit()
        if jobs_removed:
            log.info(f"Removed {jobs_removed} job(s) and {alloc_removed} allocation(s) no longer present in Wisdom.")
        else:
            log.info("No stale jobs to remove — planner matches Wisdom.")
    except Exception as e:
        conn.rollback()
        log.error(f"Remove stale jobs failed: {e}")


async def run_sync_async():
    """Run a full sync cycle across all targets — async version."""
    log.info("=" * 60)
    log.info("Wisdom Sync starting...")
    log.info("=" * 60)

    sync_started_at = datetime.now(timezone.utc)

    client = WisdomClient()
    await client.authenticate()

    conn = get_db()
    cur = conn.cursor()

    try:
        await backfill_postcodes_async(client, conn, cur)
    except Exception as e:
        log.error(f"Postcode backfill failed: {e}", exc_info=True)

    for target in EXTRACTION_TARGETS:
        await sync_target_async(client, target, conn, cur)

    # Remove any jobs no longer present in Wisdom
    await remove_stale_jobs(conn, cur, sync_started_at)

    # Scrape rejection and cancellation reasons from Wisdom
    try:
        await scrape_outcomes_async(client, conn, cur)
    except Exception as e:
        log.error(f"scrape_outcomes_async failed: {e}", exc_info=True)

    # Auto-detect wins from QUOTE > AWAITINGATTENDANCE
    try:
        await detect_wins_async(conn, cur)
    except Exception as e:
        log.error(f"detect_wins_async failed: {e}", exc_info=True)

    # Scrape agreed Wetherspoons totals for reactive/PPM jobs (billing/margin)
    try:
        await scrape_job_costs_async(client, conn, cur)
    except Exception as e:
        log.error(f"scrape_job_costs_async failed: {e}", exc_info=True)

    cur.close()
    conn.close()

    await client.close()
    log.info("Sync complete.")


def run_sync():
    """Entry point — runs the async sync in a fresh event loop."""
    asyncio.run(run_sync_async())


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()

    run_once = os.environ.get("RUN_ONCE", "false").lower() == "true"

    if run_once:
        run_sync()
    else:
        sync_interval = int(os.environ.get("SYNC_INTERVAL_MINUTES", "120"))
        log.info(f"Running in scheduled mode. Sync every {sync_interval} minutes.")

        while True:
            try:
                run_sync()
            except Exception as e:
                log.error(f"Sync cycle failed: {e}", exc_info=True)

            log.info(f"Next sync in {sync_interval} minutes.")
            time.sleep(sync_interval * 60)


# ── Outcome Scraping Functions ────────────────────────────────────────────────

async def scrape_outcome_reason(client, job_id, display_id):
    """Navigate into a rejected/cancelled Wisdom job, click the Quote tab,
    and extract the outcome reason, heading, and date."""
    try:
        job_url = (
            f"{WISDOM_BASE}/wisdom(bD1lbiZjPTEwMA==)/ContractorPortal#/jobDetail/{job_id}"
        )
        log.info(f"Scraping outcome for {display_id or job_id}")
        await client._page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
        await client._page.wait_for_timeout(2500)

        # Click the Quote tab — try multiple selectors
        clicked = False
        for selector in ["text=Quote", "a:has-text('Quote')", "[ng-click*='quote']"]:
            try:
                el = await client._page.wait_for_selector(selector, timeout=5000)
                await el.click()
                await client._page.wait_for_timeout(1500)
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            log.warning(f"Could not click Quote tab for {job_id}")
            return {}

        # Extract all visible text and parse heading + reason
        page_text = await client._page.inner_text("body")
        lines = [l.strip() for l in page_text.split("\n") if l.strip()]

        heading = ""
        reason = ""
        reason_date = ""

        known_headings = [
            "Reason for Withdrawing this Quote",
            "Declined Quote",
            "Cancellation Reason",
            "Reason for Cancellation",
            "Reason for Declining",
        ]

        for i, line in enumerate(lines):
            # Detect heading
            for h in known_headings:
                if h.lower() in line.lower():
                    heading = h
                    break

            # After heading or after "Reason" label, capture next non-nav line
            if (line == "Reason" or (heading and line in [heading])) and i + 1 < len(lines):
                nav_words = {"General", "Quote", "Notes", "KPIs", "Site Survey",
                             "Material and Labour Costs", "Request Details"}
                for j in range(i + 1, min(i + 5, len(lines))):
                    candidate = lines[j]
                    if candidate and candidate not in nav_words and len(candidate) > 2:
                        reason = candidate
                        break

            # Detect date line
            if line == "Date" and i + 1 < len(lines):
                reason_date = lines[i + 1]

        log.info(f"  heading='{heading}' reason='{reason}' date='{reason_date}'")
        return {"heading": heading, "reason": reason, "date": reason_date}

    except Exception as e:
        log.warning(f"scrape_outcome_reason failed for {job_id}: {e}")
        return {}


async def scrape_job_costs(client, job_id, display_id):
    """Navigate into a reactive (1000/3000) or PPM (2000) job and read the
    'Material and Labour Costs' tab. Sums the 'Total Cost £ X' figure shown
    on each day panel — this already includes callout/revisit fee, labour,
    and materials combined, so it's the real Wetherspoons-agreed total for
    that job without us having to reimplement the callout/revisit fee logic
    ourselves. Returns None if the tab can't be read (never guesses)."""
    try:
        job_url = (
            f"{WISDOM_BASE}/wisdom(bD1lbiZjPTEwMA==)/ContractorPortal#/jobDetail/{job_id}"
        )
        await client._page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
        await client._page.wait_for_timeout(2500)

        clicked = False
        for selector in ["text=Material and Labour Costs",
                          "a:has-text('Material and Labour Costs')",
                          "[ng-click*='Cost']"]:
            try:
                el = await client._page.wait_for_selector(selector, timeout=5000)
                await el.click()
                await client._page.wait_for_timeout(1500)
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            log.warning(f"Could not click Costs tab for {job_id}")
            return None

        page_text = await client._page.inner_text("body")
        totals = re.findall(r"Total Cost\s*£\s*([\d,]+\.\d{2})", page_text)
        if not totals:
            log.warning(f"No 'Total Cost' figures found on Costs tab for {job_id}")
            return None

        values = [float(t.replace(",", "")) for t in totals]
        total_agreed = round(sum(values), 2)
        log.info(f"  {display_id or job_id}: {len(values)} day(s), total £{total_agreed}")
        return {"total_agreed": total_agreed, "visit_count": len(values), "raw_totals": values}

    except Exception as e:
        log.warning(f"scrape_job_costs failed for {job_id}: {e}")
        return None


async def scrape_job_costs_async(client, conn, cur, max_per_cycle=20, restale_after_days=3):
    """Scrape the agreed Wetherspoons total for reactive/PPM jobs that have at
    least one submitted job card. Only (re)scrapes jobs we haven't checked
    recently, so this stays light on Wisdom — capped per sync cycle."""
    log.info("Scraping job costs (reactive/PPM billing totals)")
    try:
        # job_cards lives in the jobcard app's schema on this same database.
        cur.execute("""
            SELECT DISTINCT jc.job_id
            FROM job_cards jc
            WHERE (jc.job_id LIKE '1%%' OR jc.job_id LIKE '2%%' OR jc.job_id LIKE '3%%')
            AND jc.card_date >= NOW() - INTERVAL '90 days'
        """)
        candidate_ids = [r["job_id"] for r in cur.fetchall()]
    except Exception as e:
        conn.rollback()
        log.warning(f"Could not read job_cards for costs scrape: {e}")
        return

    scraped = 0
    for job_id in candidate_ids:
        if scraped >= max_per_cycle:
            break

        cur.execute("SELECT scraped_at FROM job_wetherspoons_costs WHERE job_id=%s", (job_id,))
        existing = cur.fetchone()
        if existing:
            age_days = (datetime.now(timezone.utc) - existing["scraped_at"]).days
            if age_days < restale_after_days:
                continue

        cur.execute("SELECT display_id, job_type FROM jobs WHERE job_id=%s", (job_id,))
        job_row = cur.fetchone()
        display_id = job_row["display_id"] if job_row else job_id
        job_type = "ppm" if job_id.startswith("2") else "reactive"

        await asyncio.sleep(0.8)
        result = await scrape_job_costs(client, job_id, display_id)
        if result is None:
            continue

        cur.execute("""
            INSERT INTO job_wetherspoons_costs
                (job_id, display_id, job_type, total_agreed, visit_count, scraped_at, raw_totals_json)
            VALUES (%s, %s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (job_id) DO UPDATE SET
                total_agreed=EXCLUDED.total_agreed,
                visit_count=EXCLUDED.visit_count,
                scraped_at=NOW(),
                raw_totals_json=EXCLUDED.raw_totals_json
        """, (job_id, display_id, job_type, result["total_agreed"],
              result["visit_count"], psycopg2.extras.Json(result["raw_totals"])))
        conn.commit()
        scraped += 1

    log.info(f"Job costs scrape complete: {scraped} job(s) updated this cycle")


async def scrape_outcomes_async(client, conn, cur):
    """Scrape rejected and cancelled jobs for outcome reasons, then record them."""
    for tab, item, label in OUTCOME_TARGETS:
        log.info(f"Scraping outcomes: {label}")
        try:
            results, total = await client.get_job_list(tab, item, skip=0, top=100)
            log.info(f"  {total} jobs found in {label}")

            for job_summary in results:
                job_id     = job_summary.get("JobId", "")
                display_id = job_summary.get("DisplayId", "") or job_id
                wisdom_status = job_summary.get("StatusText", item)
                pub_name   = (job_summary.get("PubName") or
                              job_summary.get("LocationText", ""))
                trade_type = job_summary.get("TradetypeText", "")

                if not job_id:
                    continue

                # Skip if already recorded
                cur.execute(
                    "SELECT id FROM quote_outcomes WHERE job_id=%s OR display_id=%s",
                    (job_id, display_id)
                )
                if cur.fetchone():
                    continue

                await asyncio.sleep(0.8)
                outcome_data = await scrape_outcome_reason(client, job_id, display_id)

                outcome_type = "cancelled" if item == "CANCELLATIONS" else "lost"

                # Match to survey_form
                cur.execute(
                    """SELECT id, submitted_at FROM survey_forms
                       WHERE job_id=%s OR job_id=%s
                       ORDER BY submitted_at DESC LIMIT 1""",
                    (job_id, display_id)
                )
                sf = cur.fetchone()

                cur.execute(
                    """INSERT INTO quote_outcomes
                       (job_id, display_id, survey_form_id, outcome, wisdom_status,
                        wisdom_reason, reason_heading, reason_date,
                        pub_name, trade_type, t3_decision, detected_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                       ON CONFLICT (display_id) DO UPDATE SET
                           wisdom_status=EXCLUDED.wisdom_status,
                           wisdom_reason=EXCLUDED.wisdom_reason,
                           reason_heading=EXCLUDED.reason_heading,
                           reason_date=EXCLUDED.reason_date,
                           detected_at=NOW()""",
                    (job_id, display_id, sf["id"] if sf else None,
                     outcome_type, wisdom_status,
                     outcome_data.get("reason", ""),
                     outcome_data.get("heading", ""),
                     outcome_data.get("date", ""),
                     pub_name, trade_type)
                )

                if sf:
                    cur.execute(
                        """UPDATE survey_forms SET status=%s, outcome=%s,
                           outcome_reason=%s, updated_at=NOW() WHERE id=%s""",
                        (outcome_type, outcome_type,
                         outcome_data.get("reason", ""), sf["id"])
                    )

                conn.commit()
                log.info(f"  Recorded {outcome_type}: {display_id} — "
                         f"{outcome_data.get('reason','(no reason)')}")

        except Exception as e:
            conn.rollback()
            log.error(f"scrape_outcomes_async failed for {label}: {e}", exc_info=True)


async def detect_wins_async(conn, cur):
    """Auto-detect wins: survey job appearing in QUOTE tab means JDW approved it."""
    try:
        cur.execute(
            """SELECT sf.id, sf.job_id, j.display_id, j.pub_name, j.trade_type,
                      j.date_released
               FROM survey_forms sf
               JOIN jobs j ON (j.job_id=sf.job_id OR j.display_id=sf.job_id)
               WHERE j.tab='QUOTE'
               AND sf.status NOT IN ('won','cancelled')"""
        )
        wins = cur.fetchall()
        for w in wins:
            cur.execute(
                """UPDATE survey_forms SET status='won', outcome='won',
                   updated_at=NOW() WHERE id=%s""",
                (w["id"],)
            )
            cur.execute(
                """INSERT INTO quote_outcomes
                   (job_id, display_id, survey_form_id, outcome, wisdom_status,
                    pub_name, trade_type, t3_decision, detected_at)
                   VALUES (%s,%s,%s,'won','Approved',%s,%s,NOW(),NOW())
                   ON CONFLICT (display_id) DO NOTHING""",
                (w["job_id"], w["display_id"] or w["job_id"], w["id"],
                 w["pub_name"], w["trade_type"])
            )
            log.info(f"  WIN detected: {w['display_id'] or w['job_id']}")
        conn.commit()
        if wins:
            log.info(f"Detected {len(wins)} win(s)")
        else:
            log.info("No new wins detected")
    except Exception as e:
        conn.rollback()
        log.error(f"detect_wins_async failed: {e}", exc_info=True)
