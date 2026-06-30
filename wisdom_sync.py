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
    return psycopg2.connect(DATABASE_URL)


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
        """Log in to Wisdom and capture session cookies."""
        log.info("Authenticating with Wisdom...")

        # Step 1: Load the login page to get initial cookies
        resp = self.session.get(WISDOM_LOGIN, timeout=30)
        resp.raise_for_status()
        log.info(f"Login page cookies: {list(self.session.cookies.keys())}")

        # Step 2: Submit login credentials
        login_payload = {
            "sap-user":   WISDOM_EMAIL,
            "sap-passwd": WISDOM_PASSWORD,
        }
        login_resp = self.session.post(
            WISDOM_LOGIN,
            data=login_payload,
            timeout=30,
            allow_redirects=True
        )
        log.info(f"Post-login cookies: {list(self.session.cookies.keys())}")

        # Step 3: Force ALL cookies to path=/ so they are sent to /WISDOM_DATA
        # SAP sets cookies with restrictive paths (/WIS) which blocks them from
        # reaching the OData API at /WISDOM_DATA
        all_cookies = {}
        for cookie in self.session.cookies:
            all_cookies[cookie.name] = cookie.value
            log.info(f"Cookie: {cookie.name} path={cookie.path}")

        self.session.cookies.clear()
        for name, value in all_cookies.items():
            self.session.cookies.set(
                name, value,
                domain="wisdom.jdwetherspoon.co.uk",
                path="/"
            )
        log.info(f"Normalised {len(all_cookies)} cookies to path=/")

        # Step 4: Fetch CSRF token from a known job endpoint
        csrf_resp = self.session.get(
            f"{WISDOM_DATA}/JobSet('10002107640')",
            headers={"X-Csrf-Token": "Fetch"},
            timeout=30
        )
        fetched_csrf = csrf_resp.headers.get("X-Csrf-Token")
        if fetched_csrf:
            self.session.headers["X-Csrf-Token"] = fetched_csrf
            log.info(f"CSRF token acquired.")

        # Step 5: Validate by fetching a known job
        test = self.session.get(
            f"{WISDOM_DATA}/JobSet('10002107640')",
            timeout=30
        )
        log.info(f"Validation status: {test.status_code}, length: {len(test.text)}")
        if test.status_code == 200:
            try:
                data = test.json()
                if data.get("d", {}).get("JobId"):
                    self.authenticated = True
                    log.info("Authentication successful.")
                else:
                    log.error(f"Auth response body: {test.text[:300]}")
                    raise RuntimeError("Got 200 but no job data - session not authenticated.")
            except Exception as e:
                log.error(f"Auth parse error: {e}, body: {test.text[:300]}")
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


# ── Sync Logic ────────────────────────────────────────────────────────────────

def upsert_job(cur, job_data: dict, tab: str, sub_tab: str,
               tab_label: str, fixed_description: str | None):
    """Insert or update a job record in the database."""

    job_id = job_data.get("JobId") or job_data.get("DisplayId")
    if not job_id:
        return False, False

    description = fixed_description or job_data.get("Description", "").strip()

    # Extract postcode from location data if available
    postcode = job_data.get("PostCode", "") or ""

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
        cur.execute("""
            UPDATE jobs SET
                tab=%(tab)s, sub_tab=%(sub_tab)s, tab_label=%(tab_label)s,
                job_type=%(job_type)s, pub_name=%(pub_name)s,
                location_code=%(location_code)s, postcode=%(postcode)s,
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
                        # MIV - use summary data, fixed description
                        job_detail = job_summary
                    else:
                        job_detail = client.get_job_detail(job_id)
                        time.sleep(0.2)  # Be polite to the server
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


def run_sync():
    """Run a full sync cycle across all targets."""
    log.info("=" * 60)
    log.info("Wisdom Sync starting...")
    log.info("=" * 60)

    client = WisdomClient()
    client.authenticate()

    conn = get_db()
    cur = conn.cursor()

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
                log.error(f"Sync cycle failed: {e}")

            log.info(f"Next sync in {sync_interval} minutes.")
            time.sleep(sync_interval * 60)
