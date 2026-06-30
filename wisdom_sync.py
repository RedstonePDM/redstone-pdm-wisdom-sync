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
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright
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
    ("CALLOUT",       "AWAITINGATTENDANCE", "Callout - Awaiting Attendance",        None),
    ("QUOTEREQUEST",  "AWAITINGSUBMISSION", "Quote Request - Awaiting Submission",   None),
    ("QUOTEREQUEST",  "QUERIED",            "Quote Request - Queried",               None),
    ("QUOTEREQUEST",  "AWAITINGAPPROVAL",   "Quote Request - Awaiting Approval",     None),
    ("QUOTEREQUEST",  "REJECTED",           "Quote Request - Rejected",              None),
    ("QUOTE",         "AWAITINGATTENDANCE", "Quote - Awaiting Attendance",           None),
    ("MIV",           "AWAITINGATTENDANCE", "MIV - Awaiting Attendance",             "MIV Tasks"),
    ("PPM",           "AWAITINGAPPROVAL",   "PPM - Awaiting Approval",               None),
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
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info("Database initialised.")


# ── Wisdom Authentication ─────────────────────────────────────────────────────

class WisdomClient:
    """
    Read-only HTTP client for the Wisdom OData API.
    Authenticates via session cookie. Never issues write requests.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept":           "application/json",
            "Accept-Encoding":  "gzip, deflate, br",
            "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
            "Referer":          WISDOM_LOGIN,
            "Sec-Fetch-Dest":   "empty",
            "Sec-Fetch-Mode":   "cors",
            "Sec-Fetch-Site":   "same-origin",
        })
        self.authenticated = False

    def authenticate(self):
        """
        Log in to Wisdom using a real browser (Playwright).
        The login page uses AngularJS AJAX - a standard HTTP POST does not
        complete the SAP session handshake. Playwright runs a headless browser
        that executes the JavaScript login flow properly, then we extract all
        session cookies and use them for direct API calls.
        READ-ONLY: we only navigate and extract cookies, never submit data.
        """
        log.info("Authenticating with Wisdom via browser...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            # Navigate to login page
            log.info(f"Navigating to: {WISDOM_LOGIN}")
            page.goto(WISDOM_LOGIN, wait_until="networkidle", timeout=60000)
            log.info("Login page loaded.")

            # Fill in credentials using confirmed Wisdom field names from page HTML
            page.fill("input[name='sap-alias']", WISDOM_EMAIL)
            page.fill("input[name='sap-password']", WISDOM_PASSWORD)

            # Submit using the SAP login function via JavaScript
            page.evaluate("callSubmitLogin('onLogin')")

            # Wait for navigation to complete
            page.wait_for_load_state("networkidle", timeout=60000)
            log.info(f"Post-login URL: {page.url}")

            # Extract all cookies
            cookies = context.cookies()
            log.info(f"Extracted {len(cookies)} cookies after login")
            for c in cookies:
                log.info(f"  Cookie: {c['name']} path={c['path']}")

            # Get CSRF token from page context if available
            csrf = page.evaluate("() => { try { return angular.element(document.body).injector().get('$http').defaults.headers.common['X-Csrf-Token']; } catch(e) { return null; } }")
            if csrf:
                log.info(f"CSRF from Angular context: {csrf[:20]}...")

            browser.close()

        # Load cookies into requests session with path=/
        for c in cookies:
            self.session.cookies.set(
                c["name"], c["value"],
                domain="wisdom.jdwetherspoon.co.uk",
                path="/"
            )
        log.info(f"Loaded {len(cookies)} cookies into requests session.")

        # Fetch CSRF token via requests
        csrf_resp = self.session.get(
            f"{WISDOM_DATA}/JobSet('10002107640')",
            headers={"X-Csrf-Token": "Fetch"},
            timeout=30
        )
        fetched_csrf = csrf_resp.headers.get("X-Csrf-Token")
        if fetched_csrf:
            self.session.headers["X-Csrf-Token"] = fetched_csrf
            log.info("CSRF token acquired.")

        # Validate
        test = self.session.get(
            f"{WISDOM_DATA}/JobSet('10002107640')",
            timeout=30
        )
        log.info(f"Validation: {test.status_code}, length: {len(test.text)}")
        if test.status_code == 200:
            try:
                data = test.json()
                if data.get("d", {}).get("JobId"):
                    self.authenticated = True
                    log.info("Authentication successful.")
                else:
                    log.error(f"Response: {test.text[:300]}")
                    raise RuntimeError("Got 200 but no job data.")
            except Exception as e:
                log.error(f"Parse error: {e}, body: {test.text[:300]}")
                raise
        else:
            raise RuntimeError(f"Authentication failed. Status: {test.status_code}")

    def _extract_csrf(self, response):
        """Try to extract CSRF token from response headers or HTML."""
        token = response.headers.get("X-Csrf-Token")
        if token:
            return token
        match = re.search(r'csrf[_-]?token["\s:=]+([A-Za-z0-9+/=]+)', response.text, re.I)
        if match:
            return match.group(1)
        return None

    def get_job_list(self, tab, item, skip=0, top=PAGE_SIZE):
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
        resp = self.session.get(url, timeout=30)
        log.info(f"Status: {resp.status_code}, Length: {len(resp.text)}")
        if resp.text:
            log.info(f"Preview: {resp.text[:300]}")
        resp.raise_for_status()
        data = resp.json()
        results = data.get("d", {}).get("results", [])
        total   = int(data.get("d", {}).get("__count", len(results)))
        return results, total

    def get_job_detail(self, job_id):
        """
        Fetch full detail for a single job by ID.
        READ-ONLY: GET request only.
        """
        url = f"{WISDOM_DATA}/JobSet('{job_id}')"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json().get("d", {})

    def get_pub_postcode(self, pub_id):
        """
        Fetch pub postcode from PubSet API.
        Wisdom returns the address as a single string e.g. '25 High Street OX14 5AA Abingdon'
        so we extract the UK postcode via regex from any string field.
        READ-ONLY: GET request only.
        """
        uk_postcode_re = re.compile(
            r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s+\d[A-Z]{2})\b', re.I
        )
        try:
            url = f"{WISDOM_DATA}/PubSet('{pub_id}')"
            resp = self.session.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json().get("d", {})
                log.info(f"PubSet({pub_id}) string fields: { {k: v for k, v in data.items() if isinstance(v, str) and v} }")

                # First try dedicated postcode fields
                postcode = (
                    data.get("PostCode", "") or data.get("Postcode", "") or data.get("PostalCode", "") or ""
                ).strip()

                # If no dedicated field, scan every string field for a UK postcode pattern
                # Covers Address = '25 High Street OX14 5AA Abingdon'
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
            else:
                log.warning(f"PubSet({pub_id}): HTTP {resp.status_code} - {resp.text[:200]}")
        except Exception as e:
            log.warning(f"Pub postcode lookup failed for pub_id={pub_id}: {e}")
        return ""
def upsert_job(cur, job_data: dict, tab: str, sub_tab: str,
               tab_label: str, fixed_description: str | None):
    """Insert or update a job record in the database."""

    job_id = job_data.get("JobId") or job_data.get("DisplayId")
    if not job_id:
        return False, False

    description = fixed_description or job_data.get("Description", "").strip()

    # Extract postcode — injected as _postcode from pub lookup during sync
    postcode = (job_data.get("PostCode") or job_data.get("_postcode") or "").strip()

    now = datetime.now(timezone.utc)

    # Check if job already exists
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
        return True, False  # is_new=True, is_updated=False
    else:
        # Only update postcode if we now have one and didn't before
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
        return False, True  # is_new=False, is_updated=True


def sync_target(client: WisdomClient, target: tuple, conn, cur):
    """Sync all jobs for one tab/sub-tab target."""
    tab, item, label, fixed_desc = target
    log.info(f"Syncing: {label}")

    jobs_found = jobs_new = jobs_updated = 0
    skip = 0
    pub_postcode_cache = {}

    try:
        while True:
            results, total = client.get_job_list(tab, item, skip=skip)

            if not results:
                break

            jobs_found = total

            for job_summary in results:
                job_id = job_summary.get("JobId") or job_summary.get("DisplayId")
                if not job_id:
                    continue

                # Fetch full detail for each job (gets description etc.)
                try:
                    if fixed_desc:
                        job_detail = job_summary
                    else:
                        job_detail = client.get_job_detail(job_id)
                        time.sleep(0.2)

                    # Extract postcode from pub location if not already on the job record
                    if not job_detail.get("PostCode"):
                        location = job_detail.get("Location", "")
                        parts = location.split("-") if location else []
                        if len(parts) >= 2:
                            pub_id = parts[1]
                            if pub_id not in pub_postcode_cache:
                                postcode = client.get_pub_postcode(pub_id)
                                pub_postcode_cache[pub_id] = postcode
                                time.sleep(0.1)
                            else:
                                postcode = pub_postcode_cache[pub_id]
                            if postcode:
                                job_detail["_postcode"] = postcode
                                log.info(f"Job {job_id}: postcode set to {postcode} from pub {pub_id}")

                except Exception as e:
                    log.warning(f"Could not fetch detail for job {job_id}: {e}")
                    job_detail = job_summary

                is_new, is_updated = upsert_job(
                    cur, job_detail, tab, item, label, fixed_desc
                )
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


def backfill_postcodes(client, conn, cur):
    """
    Backfill postcodes for all jobs that are missing them.
    Runs at the start of every sync cycle.
    Uses a RealDictCursor explicitly to avoid tuple/dict access errors.
    """
    # Use a dedicated dict cursor for this function
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
        SELECT DISTINCT location_code FROM jobs
        WHERE (postcode IS NULL OR postcode = '')
        AND location_code IS NOT NULL AND location_code != ''
    """)
    locations = [r["location_code"] for r in dict_cur.fetchall()]
    dict_cur.close()

    pub_cache = {}
    updated = 0

    for location in locations:
        parts = location.split("-") if location else []
        if len(parts) < 2:
            continue
        pub_id = parts[1]

        if pub_id not in pub_cache:
            postcode = client.get_pub_postcode(pub_id)
            pub_cache[pub_id] = postcode
            time.sleep(0.15)
        else:
            postcode = pub_cache[pub_id]

        if postcode:
            cur.execute("""
                UPDATE jobs SET postcode = %s
                WHERE location_code = %s
                AND (postcode IS NULL OR postcode = '')
            """, (postcode, location))
            updated += cur.rowcount

    conn.commit()
    log.info(
        f"Postcode backfill complete: updated {updated} jobs "
        f"across {len(pub_cache)} unique pubs."
    )


def run_sync():
    """Run a full sync cycle across all targets."""
    log.info("=" * 60)
    log.info("Wisdom Sync starting...")
    log.info("=" * 60)

    client = WisdomClient()
    client.authenticate()

    conn = get_db()
    cur = conn.cursor()

    # Backfill postcodes for any jobs missing them
    try:
        backfill_postcodes(client, conn, cur)
    except Exception as e:
        log.error(f"Postcode backfill failed: {e}", exc_info=True)

    for target in EXTRACTION_TARGETS:
        sync_target(client, target, conn, cur)

    cur.close()
    conn.close()
    log.info("Sync complete.")


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()

    # Determine run mode
    run_once = os.environ.get("RUN_ONCE", "false").lower() == "true"

    if run_once:
        run_sync()
    else:
        # Scheduled mode: sync every 2 hours
        sync_interval = int(os.environ.get("SYNC_INTERVAL_MINUTES", "120"))
        log.info(f"Running in scheduled mode. Sync every {sync_interval} minutes.")

        while True:
            try:
                run_sync()
            except Exception as e:
                log.error(f"Sync cycle failed: {e}", exc_info=True)

            log.info(f"Next sync in {sync_interval} minutes.")
            time.sleep(sync_interval * 60)
