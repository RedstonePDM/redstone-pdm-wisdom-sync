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
            status          TEXT,
            payment_date    DATE,
            scraped_at      TIMESTAMPTZ DEFAULT NOW(),
            raw_totals_json JSONB
        );
    """)
    conn.commit()

    # These two columns were added after the table already existed in
    # production — CREATE TABLE IF NOT EXISTS above won't retrofit them onto
    # an existing table, so add them explicitly and tolerate them already
    # being there.
    for col_sql in [
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS status TEXT",
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS payment_date DATE",
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS pub_name TEXT",
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS trade_type TEXT",
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS pub_id TEXT",
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS wisdom_status_change_date DATE",
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS due_date DATE",
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ DEFAULT NOW()",
        # 'Date Released' — the true 'job raised' date, visible on Wisdom's
        # own job detail screen but NOT present in the PAYMENT/PAID billing
        # feed we scrape for the pipeline. Needs a separate detail lookup
        # per job (see backfill_raised_dates_async). raised_date_checked
        # tracks whether we've already attempted this job, so a job with
        # genuinely no releasable date isn't retried forever on every sync.
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS raised_date DATE",
        "ALTER TABLE job_wetherspoons_costs ADD COLUMN IF NOT EXISTS raised_date_checked BOOLEAN DEFAULT FALSE",
        """CREATE TABLE IF NOT EXISTS job_status_history (
            id SERIAL PRIMARY KEY,
            job_id TEXT NOT NULL,
            status TEXT NOT NULL,
            wisdom_status_change_date DATE,
            detected_at TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_job_status_history_job_id ON job_status_history(job_id)",
        # Geocoded pub locations — foundation for the recruitment coverage
        # map. Keyed by pub_name (matches how pubs are already grouped
        # everywhere else in the platform). One row per pub, refreshed only
        # when its postcode changes, so this stays a cheap lookup rather
        # than a per-job geocode call.
        """CREATE TABLE IF NOT EXISTS pub_locations (
            pub_name     TEXT PRIMARY KEY,
            postcode     TEXT,
            latitude     NUMERIC(9,6),
            longitude    NUMERIC(9,6),
            geocoded_at  TIMESTAMPTZ,
            geocode_failed BOOLEAN DEFAULT FALSE
        )""",
    ]:
        try:
            cur.execute(col_sql)
            conn.commit()
        except Exception:
            conn.rollback()

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
        "date_released":    job_data.get("ReleasedDate", ""),
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


async def backfill_wisdom_pub_postcodes_async(client, conn, cur):
    """Backfill postcodes into pub_locations, sourced from
    job_wetherspoons_costs (pub_id + pub_name) rather than the 'jobs' table.

    This matters: 'jobs' only holds currently-active jobs — once a job is
    Paid, its row is deleted from 'jobs' to keep the live board clean. So a
    pub whose most recent job has already been paid off has NO row left in
    'jobs' to pull a postcode from, even though it's sat there permanently
    in job_wetherspoons_costs. That was the root cause of most pubs showing
    no postcode on the Paid Jobs page. Sourcing from job_wetherspoons_costs
    instead — which never gets rows deleted — fixes this for good, and
    covers every pub that's ever appeared anywhere in the billing pipeline,
    not just ones with something live right now."""
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    dict_cur.execute("""
        SELECT DISTINCT pub_id, pub_name FROM job_wetherspoons_costs
        WHERE pub_id IS NOT NULL AND pub_id != ''
          AND pub_name IS NOT NULL AND pub_name != ''
    """)
    wisdom_pubs = dict_cur.fetchall()

    dict_cur.execute("SELECT pub_name FROM pub_locations WHERE postcode IS NOT NULL AND postcode != ''")
    already_have = {r["pub_name"] for r in dict_cur.fetchall()}
    dict_cur.close()

    to_fetch = [(r["pub_id"], r["pub_name"]) for r in wisdom_pubs if r["pub_name"] not in already_have]

    if not to_fetch:
        log.info("Wisdom pub postcode backfill: all known pubs already have a postcode.")
        return

    log.info(f"Wisdom pub postcode backfill: {len(to_fetch)} pub(s) missing a postcode. Fetching...")

    fetched = 0
    for pub_id, pub_name in to_fetch:
        postcode = await client.get_pub_postcode(pub_id)
        await asyncio.sleep(0.15)
        if not postcode:
            continue
        cur.execute("""
            INSERT INTO pub_locations (pub_name, postcode)
            VALUES (%s, %s)
            ON CONFLICT (pub_name) DO UPDATE SET
                postcode = EXCLUDED.postcode,
                latitude = CASE WHEN pub_locations.postcode = EXCLUDED.postcode THEN pub_locations.latitude ELSE NULL END,
                longitude = CASE WHEN pub_locations.postcode = EXCLUDED.postcode THEN pub_locations.longitude ELSE NULL END,
                geocode_failed = CASE WHEN pub_locations.postcode = EXCLUDED.postcode THEN pub_locations.geocode_failed ELSE FALSE END
        """, (pub_name, postcode))
        fetched += 1

    conn.commit()
    log.info(f"Wisdom pub postcode backfill complete: {fetched} of {len(to_fetch)} pub(s) got a postcode.")


def geocode_pub_locations(conn, cur):
    """Turn pub postcodes into lat/lng coordinates via postcodes.io (free,
    no API key, no rate limit for reasonable use) — foundation for the
    recruitment coverage map. Sources postcodes directly from pub_locations
    itself (populated by backfill_wisdom_pub_postcodes_async), so this only
    ever geocodes a pub once — not re-fetched on every sync — unless its
    postcode has genuinely changed (which resets latitude to NULL, picked
    up here automatically).

    This is a plain internet call via `requests`, NOT through the Wisdom
    Playwright browser session — postcodes.io is a public, unrelated
    service, so it doesn't need Wisdom's session cookies at all."""
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    dict_cur.execute("""
        SELECT pub_name, postcode FROM pub_locations
        WHERE postcode IS NOT NULL AND postcode != ''
          AND latitude IS NULL AND geocode_failed = FALSE
    """)
    to_geocode = {r["pub_name"]: r["postcode"] for r in dict_cur.fetchall()}
    dict_cur.close()

    if not to_geocode:
        log.info("Geocoding: all pub locations already up to date.")
        return

    log.info(f"Geocoding: {len(to_geocode)} pub(s) need a fresh lookup.")

    pub_names = list(to_geocode.keys())
    geocoded = 0
    failed = 0

    # postcodes.io's bulk endpoint accepts up to 100 postcodes per call.
    BATCH = 100
    for i in range(0, len(pub_names), BATCH):
        batch_pubs = pub_names[i:i + BATCH]
        batch_postcodes = [to_geocode[p] for p in batch_pubs]
        try:
            resp = requests.post(
                "https://api.postcodes.io/postcodes",
                json={"postcodes": batch_postcodes},
                timeout=15,
            )
            resp.raise_for_status()
            results = resp.json().get("result", [])
        except Exception as e:
            log.warning(f"Geocoding batch failed (pubs {i}-{i+len(batch_pubs)}): {e}")
            continue

        for pub, result in zip(batch_pubs, results):
            postcode = to_geocode[pub]
            match = result.get("result") if result else None
            if match and match.get("latitude") is not None:
                cur.execute("""
                    INSERT INTO pub_locations (pub_name, postcode, latitude, longitude, geocoded_at, geocode_failed)
                    VALUES (%s, %s, %s, %s, NOW(), FALSE)
                    ON CONFLICT (pub_name) DO UPDATE SET
                        postcode=EXCLUDED.postcode, latitude=EXCLUDED.latitude,
                        longitude=EXCLUDED.longitude, geocoded_at=NOW(), geocode_failed=FALSE
                """, (pub, postcode, match["latitude"], match["longitude"]))
                geocoded += 1
            else:
                # Postcode didn't resolve (typo, outdated postcode, etc.) —
                # record the attempt so we don't keep retrying it every
                # sync, but flag it so it's easy to find and fix manually.
                cur.execute("""
                    INSERT INTO pub_locations (pub_name, postcode, latitude, longitude, geocoded_at, geocode_failed)
                    VALUES (%s, %s, NULL, NULL, NOW(), TRUE)
                    ON CONFLICT (pub_name) DO UPDATE SET
                        postcode=EXCLUDED.postcode, geocoded_at=NOW(), geocode_failed=TRUE
                """, (pub, postcode))
                failed += 1

    conn.commit()
    log.info(f"Geocoding complete: {geocoded} pub(s) geocoded, {failed} failed (bad/unrecognised postcode).")


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

    try:
        await backfill_wisdom_pub_postcodes_async(client, conn, cur)
    except Exception as e:
        log.error(f"Wisdom pub postcode backfill failed: {e}", exc_info=True)

    try:
        geocode_pub_locations(conn, cur)
    except Exception as e:
        log.error(f"Geocoding failed: {e}", exc_info=True)

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
        await scrape_all_pipeline_stages_async(client, conn, cur)
    except Exception as e:
        log.error(f"scrape_all_pipeline_stages_async failed: {e}", exc_info=True)

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


# Each entry: (Tab, Item, default status if a row's own StatusText doesn't
# tell us otherwise). Ready For Payment blends two real statuses in one feed
# (Wisdom's own StatusText distinguishes them), everything else is one status.
#
# NOTE: HOVQUERY's Item value is a guess, following the same naming pattern
# as the other three confirmed URLs (tab name + sub-tab name, no spaces, all
# caps). This has NOT been verified against Wisdom yet — same situation as
# the INVOICED tab was originally. If the "HOV Query" count on the Reports &
# Margin page stays stuck at 0 after a sync, this is the first thing to check.
PIPELINE_TARGETS = [
    ("ADMIN",   "AWAITINGCOSTS",   "awaiting_costs"),
    ("ADMIN",   "HOVQUERY",        "hov_query"),      # unverified guess — see note above
    ("ADMIN",   "READYFORPAYMENT", None),  # status read per-row from StatusText
    ("PAYMENT", "INVOICED",        "invoiced"),
    ("PAYMENT", "PAID",            "paid"),
]


def _parse_wisdom_date(value):
    """Wisdom date fields come back as 'YYYY-MM-DD' strings or blank."""
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


async def scrape_pipeline_stage_async(client, conn, cur, tab, item, default_status):
    """Pull one Wisdom pipeline tab/item page by page and upsert each page
    into job_wetherspoons_costs as soon as it's fetched, logging a
    status-history entry whenever a job's status has changed since we last
    saw it. This is the single source of truth for where every job actually
    sits in Redstone's billing pipeline with Wetherspoons — Awaiting Costs,
    Ready for Payment, Approved to Invoice, Invoiced, or Paid.

    Rows are saved page-by-page (not all-at-the-end) so that if a page fetch
    fails partway through a large feed (e.g. a network blip), everything
    fetched so far is already safely committed to the database — nothing
    fetched is ever thrown away, and a re-run only has to pick up from
    wherever it left off."""
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Load every existing status for jobs we already know about in ONE query
    # up front, rather than a SELECT-then-INSERT round trip per row — for a
    # few thousand rows that difference is thousands of extra network calls.
    try:
        dict_cur.execute("SELECT job_id, status FROM job_wetherspoons_costs")
        known_statuses = {r["job_id"]: r["status"] for r in dict_cur.fetchall()}
    except Exception as e:
        log.error(f"Could not preload known statuses for {tab}/{item}: {e}")
        conn.rollback()
        known_statuses = {}

    updated = 0
    fetched_count = 0
    skip = 0
    total = None

    while True:
        try:
            results, total = await client.get_job_list(tab, item, skip=skip, top=PAGE_SIZE)
        except Exception as e:
            log.error(
                f"Failed to fetch {tab}/{item} at skip={skip} "
                f"(after successfully saving {updated} of {fetched_count} fetched so far): {e}"
            )
            break

        if not results:
            break

        fetched_count += len(results)

        for r in results:
            _upsert_pipeline_row(dict_cur, conn, known_statuses, tab, item, r, default_status)
            updated += 1

        # Commit after every page rather than one giant transaction for the
        # whole feed — keeps each transaction short, and means progress is
        # actually saved (not just logged) if this gets interrupted partway.
        conn.commit()
        log.info(f"  {tab}/{item}: {fetched_count} of {total} fetched, {updated} saved...")

        skip += len(results)
        if skip >= total:
            break

    dict_cur.close()
    return updated


def _upsert_pipeline_row(dict_cur, conn, known_statuses, tab, item, r, default_status):
    """Upsert a single Wisdom pipeline row. Isolated so one bad row (odd
    date format, weird characters, whatever) never silently hangs or kills
    the rest of the page — log it, roll back just this row, and keep going."""
    try:
            wisdom_internal_id = r.get("WISDOMId")
            if not wisdom_internal_id:
                return
            # Quoted (5000-series) jobs have a dual-ID quirk in Wisdom: the
            # internal WISDOMId can start with 8 while the customer-facing
            # DisplayId starts with 5 for the same job. Every other table in
            # this platform keys off DisplayId, so we do the same here.
            job_id = r.get("DisplayId") or wisdom_internal_id
            display_id = job_id

            if default_status:
                status = default_status
            else:
                status_text = (r.get("StatusText") or "").strip().lower()
                status = "approved_to_invoice" if "approved" in status_text else "ready_for_payment"

            # Job category comes from the job number prefix, not Wisdom's
            # JobTypeText — this matches Redstone's actual business rules
            # (1000/3000 = reactive hourly, 2000 = PPM day rate, 5000/8000 =
            # quoted) rather than relying on Wisdom's own text labelling,
            # which doesn't reliably distinguish MIV from ordinary reactive
            # work. Quoted jobs can show up as either a 5xxx DisplayId or an
            # 8xxx WISDOMId for the same job — job_id above already prefers
            # DisplayId, so checking display_id here is the safe one.
            id_prefix = display_id[:1] if display_id else ""
            if id_prefix == "2":
                job_type = "ppm"
            elif id_prefix in ("5", "8"):
                job_type = "quoted"
            elif id_prefix == "3":
                job_type = "miv"
            else:
                job_type = "reactive"  # 1000-series, and anything unrecognised

            try:
                total_cost = float(r.get("TotalCost") or 0)
            except (TypeError, ValueError):
                total_cost = 0.0

            payment_date = _parse_wisdom_date(r.get("PaymentDate"))
            due_date = _parse_wisdom_date(r.get("DueDate"))
            wisdom_status_change_date = _parse_wisdom_date(r.get("StatusChangeDate"))
            pub_name = r.get("PubName") or None
            pub_id = r.get("PubId") or None
            trade_type = r.get("SubtradetypeText") or None

            # Status-history entry only when this is new information —
            # either we've never seen this job, or its status has changed.
            if known_statuses.get(job_id) != status:
                dict_cur.execute("""
                    INSERT INTO job_status_history (job_id, status, wisdom_status_change_date)
                    VALUES (%s, %s, %s)
                """, (job_id, status, wisdom_status_change_date))
                known_statuses[job_id] = status

            dict_cur.execute("""
                INSERT INTO job_wetherspoons_costs
                    (job_id, display_id, job_type, total_agreed, status, payment_date,
                     pub_name, pub_id, trade_type, wisdom_status_change_date, due_date,
                     first_seen_at, scraped_at, raw_totals_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
                ON CONFLICT (job_id) DO UPDATE SET
                    display_id=EXCLUDED.display_id,
                    job_type=EXCLUDED.job_type,
                    total_agreed=EXCLUDED.total_agreed,
                    status=EXCLUDED.status,
                    payment_date=EXCLUDED.payment_date,
                    pub_name=EXCLUDED.pub_name,
                    pub_id=EXCLUDED.pub_id,
                    trade_type=EXCLUDED.trade_type,
                    wisdom_status_change_date=EXCLUDED.wisdom_status_change_date,
                    due_date=EXCLUDED.due_date,
                    scraped_at=NOW(),
                    raw_totals_json=EXCLUDED.raw_totals_json
            """, (job_id, display_id, job_type, total_cost, status, payment_date,
                  pub_name, pub_id, trade_type, wisdom_status_change_date, due_date,
                  psycopg2.extras.Json(r)))

    except Exception as row_err:
        # One bad row (odd date format, weird characters, whatever) should
        # never silently hang or kill the rest of the page — log it, roll
        # back just this row, and let the caller move on to the next one.
        log.warning(f"Skipped one row in {tab}/{item}: {row_err}")
        conn.rollback()


async def backfill_raised_dates_async(client, conn, cur, limit=None):
    """Backfill 'Date Released' (the true job-raised date) for jobs in the
    permanent job_wetherspoons_costs table, via Wisdom's job detail lookup
    (JobSet) — the billing feed itself doesn't carry this field at all.

    PPM (2000-series) is deliberately excluded — Dave confirmed this
    tracking is only wanted for reactive/MIV/quoted work.

    `limit`: caps how many jobs to attempt this call, for safe small-batch
    testing before committing to the full historic backlog (~3550 jobs).
    Pass limit=None (default) for a full, unlimited run.

    Uses the WISDOMId stored in each row's raw_totals_json as the lookup
    ID — this is what Wisdom's own internal system uses to key a job
    detail record, distinct from the customer-facing DisplayId."""
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    query = """
        SELECT job_id, raw_totals_json->>'WISDOMId' as wisdom_id
        FROM job_wetherspoons_costs
        WHERE raised_date_checked = FALSE
          AND job_type IN ('reactive', 'miv', 'quoted')
          AND raw_totals_json->>'WISDOMId' IS NOT NULL
        ORDER BY payment_date DESC NULLS LAST
    """
    if limit:
        query += f" LIMIT {int(limit)}"
    dict_cur.execute(query)
    to_fetch = dict_cur.fetchall()
    dict_cur.close()

    if not to_fetch:
        log.info("Raised-date backfill: nothing left to check.")
        return

    log.info(f"Raised-date backfill: attempting {len(to_fetch)} job(s)...")

    found = 0
    blank = 0
    errored = 0
    for i, row in enumerate(to_fetch, 1):
        job_id = row["job_id"]
        wisdom_id = row["wisdom_id"]
        try:
            detail = await client.get_job_detail(wisdom_id)
            await asyncio.sleep(0.2)
            raised = _parse_wisdom_date(detail.get("ReleasedDate"))
            if raised:
                cur.execute("""
                    UPDATE job_wetherspoons_costs
                    SET raised_date = %s, raised_date_checked = TRUE
                    WHERE job_id = %s
                """, (raised, job_id))
                found += 1
            else:
                cur.execute("""
                    UPDATE job_wetherspoons_costs SET raised_date_checked = TRUE
                    WHERE job_id = %s
                """, (job_id,))
                blank += 1
        except Exception as e:
            log.warning(f"Raised-date lookup failed for job {job_id} (WISDOMId {wisdom_id}): {e}")
            errored += 1
            continue

        if i % 50 == 0:
            conn.commit()
            log.info(f"  Raised-date backfill: {i} of {len(to_fetch)} checked so far...")

    conn.commit()
    log.info(
        f"Raised-date backfill complete: {found} found, {blank} blank "
        f"(genuinely no ReleasedDate on Wisdom's side), {errored} errored."
    )



    """Run the pipeline scrape across all four Wisdom billing stages."""
    log.info("Scraping Wisdom billing pipeline (Awaiting Costs / Ready for Payment / Invoiced / Paid)")
    for tab, item, default_status in PIPELINE_TARGETS:
        try:
            updated = await scrape_pipeline_stage_async(client, conn, cur, tab, item, default_status)
            log.info(f"  {tab}/{item}: {updated} job(s) updated")
        except Exception as e:
            log.error(f"Pipeline stage {tab}/{item} failed: {e}", exc_info=True)
    log.info("Pipeline scrape complete")


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
