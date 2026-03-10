#!/usr/bin/env python3
"""
Shared utilities for all enrichment scripts.
Provides Supabase, Apollo, and Apify clients plus common helpers.
"""

import json
import os
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ─── Load .env if present (before reading env vars) ───
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

# ─── Credentials (from env vars — set in .env or shell) ───
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN", "")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

def make_logger(log_file):
    """Return a log function that writes to stdout + file."""
    path = os.path.join(SCRIPT_DIR, log_file)
    def _log(msg):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line, flush=True)
        with open(path, "a") as f:
            f.write(line + "\n")
    return _log


# ═══════════════════════════════════════════════════════════
# PROGRESS TRACKING
# ═══════════════════════════════════════════════════════════

def load_progress(filename, defaults=None):
    path = os.path.join(SCRIPT_DIR, filename)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return defaults or {}

def save_progress(data, filename):
    path = os.path.join(SCRIPT_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ═══════════════════════════════════════════════════════════
# SUPABASE CLIENT
# ═══════════════════════════════════════════════════════════

class SupabaseClient:
    def __init__(self, url=None, key=None):
        self.url = (url or SUPABASE_URL).rstrip("/")
        self.key = key or SUPABASE_KEY
        self.session = requests.Session()
        self.session.headers.update({
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        })

    def fetch(self, table, filters="", select="*", order="created_at.asc",
              limit=100, offset=0):
        """GET rows from a table with PostgREST filters."""
        url = f"{self.url}/rest/v1/{table}?select={select}&order={order}&limit={limit}&offset={offset}"
        if filters:
            url += f"&{filters}"
        r = self.session.get(url, timeout=30)
        if r.status_code == 200:
            return r.json()
        return []

    def fetch_count(self, table, filters=""):
        """Get exact count of matching rows."""
        url = f"{self.url}/rest/v1/{table}?select=count"
        if filters:
            url += f"&{filters}"
        headers = {**self.session.headers, "Prefer": "count=exact"}
        r = self.session.get(url, headers=headers, timeout=30)
        # Count is in content-range header
        cr = r.headers.get("content-range", "")
        if "/" in cr:
            try:
                return int(cr.split("/")[1])
            except (ValueError, IndexError):
                pass
        if r.status_code == 200:
            data = r.json()
            if data and isinstance(data, list) and "count" in data[0]:
                return data[0]["count"]
        return 0

    def update(self, table, pk_col, pk_val, updates, retries=2):
        """PATCH a single row by primary key."""
        url = f"{self.url}/rest/v1/{table}?{pk_col}=eq.{pk_val}"
        headers = {"Prefer": "return=minimal"}
        for attempt in range(retries + 1):
            try:
                r = self.session.patch(url, json=updates, headers=headers, timeout=15)
                if r.status_code in (200, 204):
                    return True
                if r.status_code == 429 and attempt < retries:
                    time.sleep(30 * (attempt + 1))
                    continue
                return False
            except Exception:
                if attempt < retries:
                    time.sleep(5)
                    continue
                return False
        return False

    def upsert(self, table, records, on_conflict="", retries=2):
        """POST (upsert) a batch of records."""
        url = f"{self.url}/rest/v1/{table}"
        headers = {"Prefer": "return=minimal"}
        if on_conflict:
            headers["Prefer"] = f"resolution=merge-duplicates,return=minimal"
            url += f"?on_conflict={on_conflict}"
        for attempt in range(retries + 1):
            try:
                r = self.session.post(url, json=records, headers=headers, timeout=30)
                if r.status_code in (200, 201, 204):
                    return True
                if r.status_code == 429 and attempt < retries:
                    time.sleep(30 * (attempt + 1))
                    continue
                return False
            except Exception:
                if attempt < retries:
                    time.sleep(5)
                    continue
                return False
        return False

    def insert(self, table, records, retries=2):
        """POST new records (no upsert)."""
        url = f"{self.url}/rest/v1/{table}"
        headers = {"Prefer": "return=representation"}
        for attempt in range(retries + 1):
            try:
                r = self.session.post(url, json=records, headers=headers, timeout=30)
                if r.status_code in (200, 201):
                    return r.json()
                if r.status_code == 409:
                    return []  # duplicate, skip
                if r.status_code == 429 and attempt < retries:
                    time.sleep(30 * (attempt + 1))
                    continue
                return []
            except Exception:
                if attempt < retries:
                    time.sleep(5)
                    continue
                return []
        return []

    def rpc(self, fn_name, params=None):
        """Call a Supabase RPC function."""
        url = f"{self.url}/rest/v1/rpc/{fn_name}"
        r = self.session.post(url, json=params or {}, timeout=30)
        if r.status_code == 200:
            return r.json()
        return None


# ═══════════════════════════════════════════════════════════
# APOLLO CLIENT
# ═══════════════════════════════════════════════════════════

class ApolloClient:
    BASE = "https://api.apollo.io"

    def __init__(self, api_key=None):
        self.api_key = api_key or APOLLO_API_KEY
        if not self.api_key:
            raise ValueError("APOLLO_API_KEY not set. Export it or pass to ApolloClient().")
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "X-Api-Key": self.api_key,
        })
        self.last_call = 0
        self.min_delay = 1.0  # Apollo rate limit: ~50 req/min

    def _throttle(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self.last_call = time.time()

    def _post(self, endpoint, payload, retries=2):
        self._throttle()
        url = f"{self.BASE}{endpoint}"
        for attempt in range(retries + 1):
            try:
                r = self.session.post(url, json=payload, timeout=30)
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    wait = 60 * (attempt + 1)
                    time.sleep(wait)
                    continue
                if r.status_code == 422:
                    return {"error": "unprocessable", "detail": r.text[:200]}
                return {"error": f"status_{r.status_code}", "detail": r.text[:200]}
            except Exception as e:
                if attempt < retries:
                    time.sleep(10)
                    continue
                return {"error": str(e)}
        return {"error": "max_retries"}

    def _get(self, endpoint, params=None, retries=2):
        self._throttle()
        url = f"{self.BASE}{endpoint}"
        for attempt in range(retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    time.sleep(60 * (attempt + 1))
                    continue
                return {"error": f"status_{r.status_code}"}
            except Exception as e:
                if attempt < retries:
                    time.sleep(10)
                    continue
                return {"error": str(e)}
        return {"error": "max_retries"}

    def people_search(self, domain=None, name=None, titles=None,
                      seniorities=None, limit=10, page=1):
        """Search for people by org domain and/or filters."""
        payload = {"page": page, "per_page": min(limit, 100)}
        if domain:
            payload["q_organization_domains_list"] = [domain]
        if name:
            payload["q_keywords"] = name
        if titles:
            payload["person_titles"] = titles if isinstance(titles, list) else [titles]
        if seniorities:
            payload["person_seniorities"] = seniorities if isinstance(seniorities, list) else [seniorities]
        return self._post("/v1/mixed_people/search", payload)

    def people_enrich(self, first_name=None, last_name=None, email=None,
                      domain=None, org_name=None, linkedin_url=None):
        """Enrich a single person."""
        payload = {}
        if first_name: payload["first_name"] = first_name
        if last_name: payload["last_name"] = last_name
        if email: payload["email"] = email
        if domain: payload["domain"] = domain
        if org_name: payload["organization_name"] = org_name
        if linkedin_url: payload["linkedin_url"] = linkedin_url
        return self._post("/v1/people/match", payload)

    def people_bulk_enrich(self, details):
        """Bulk enrich up to 10 people at once.
        details: list of dicts with first_name, last_name, domain, organization_name, etc.
        """
        return self._post("/v1/people/bulk_match", {"details": details[:10]})

    def org_enrich(self, domain):
        """Enrich an organization by domain."""
        return self._post("/v1/organizations/enrich", {"domain": domain})

    def org_search(self, name=None, domains=None, locations=None,
                   num_employees=None, page=1, per_page=25):
        """Search organizations."""
        payload = {"page": page, "per_page": per_page}
        if name:
            payload["q_organization_name"] = name
        if domains:
            payload["q_organization_domains_list"] = domains
        if locations:
            payload["organization_locations"] = locations
        if num_employees:
            payload["organization_num_employees_ranges"] = num_employees
        return self._post("/v1/mixed_companies/search", payload)

    def get_profile(self):
        """Get Apollo account profile and credit usage."""
        return self._get("/v1/users/me")


# ═══════════════════════════════════════════════════════════
# APIFY CLIENT
# ═══════════════════════════════════════════════════════════

class ApifyClient:
    BASE = "https://api.apify.com/v2"

    def __init__(self, token=None):
        self.token = token or APIFY_API_TOKEN
        self.session = requests.Session()

    def run_actor(self, actor_id, input_data, timeout_secs=300, memory_mb=1024):
        """Run an Apify actor synchronously and return dataset items."""
        url = f"{self.BASE}/acts/{actor_id}/run-sync-get-dataset-items"
        params = {"token": self.token, "timeout": timeout_secs, "memory": memory_mb}
        r = self.session.post(url, json=input_data, params=params, timeout=timeout_secs + 30)
        if r.status_code == 200 or r.status_code == 201:
            return r.json()
        return []

    def run_actor_async(self, actor_id, input_data, memory_mb=1024):
        """Start an actor run and return the run info (poll for results later)."""
        url = f"{self.BASE}/acts/{actor_id}/runs"
        params = {"token": self.token, "memory": memory_mb}
        r = self.session.post(url, json=input_data, params=params, timeout=30)
        if r.status_code in (200, 201):
            return r.json().get("data", {})
        return {}

    def get_run_status(self, run_id):
        """Check status of a run."""
        url = f"{self.BASE}/actor-runs/{run_id}"
        params = {"token": self.token}
        r = self.session.get(url, params=params, timeout=15)
        if r.status_code == 200:
            return r.json().get("data", {})
        return {}

    def get_dataset_items(self, dataset_id, limit=1000, offset=0):
        """Get items from a dataset."""
        url = f"{self.BASE}/datasets/{dataset_id}/items"
        params = {"token": self.token, "limit": limit, "offset": offset, "format": "json"}
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        return []

    def wait_for_run(self, run_id, poll_interval=10, max_wait=600):
        """Poll until a run finishes. Returns dataset items."""
        start = time.time()
        while time.time() - start < max_wait:
            status = self.get_run_status(run_id)
            state = status.get("status")
            if state == "SUCCEEDED":
                dataset_id = status.get("defaultDatasetId")
                if dataset_id:
                    return self.get_dataset_items(dataset_id)
                return []
            if state in ("FAILED", "ABORTED", "TIMED-OUT"):
                return []
            time.sleep(poll_interval)
        return []


# ═══════════════════════════════════════════════════════════
# NAME & DOMAIN UTILITIES
# ═══════════════════════════════════════════════════════════

# Common legal suffixes to strip
LEGAL_SUFFIXES = re.compile(
    r'\b(llc|l\.l\.c|inc|incorporated|corp|corporation|co|company|ltd|limited|'
    r'lp|l\.p|llp|l\.l\.p|pllc|p\.l\.l\.c|dba|d/b/a|series)\b\.?',
    re.IGNORECASE,
)

LOCATION_SUFFIXES = re.compile(
    r'[-–—]\s*(#?\d+|main\s*st|hwy|blvd|ave|rd|dr|st|ln|ct|way|ste|suite|'
    r'north|south|east|west|downtown|midtown|uptown)\b.*$',
    re.IGNORECASE,
)

def clean_business_name(name):
    """Strip legal suffixes and location qualifiers for matching."""
    if not name:
        return ""
    cleaned = LOCATION_SUFFIXES.sub("", name).strip()
    cleaned = LEGAL_SUFFIXES.sub("", cleaned).strip()
    cleaned = re.sub(r'[,.\-–—]+$', '', cleaned).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned

def extract_domain(url):
    """Extract bare domain from a URL."""
    if not url:
        return ""
    url = url.lower().strip()
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    url = url.split('/')[0].split('?')[0].split('#')[0]
    return url

def validate_domain(domain, timeout=5):
    """Check if a domain resolves via DNS."""
    try:
        socket.setdefaulttimeout(timeout)
        socket.getaddrinfo(domain, 80)
        return True
    except (socket.gaierror, socket.timeout, OSError):
        return False

def check_website(url, timeout=5):
    """HTTP HEAD check - returns status code or None on failure."""
    if not url.startswith("http"):
        url = f"https://{url}"
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0"})
        return r.status_code
    except Exception:
        return None

def extract_emails_from_text(text):
    """Find email addresses in text."""
    if not text:
        return []
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(pattern, text)
    # Filter out common junk
    junk = {'example.com', 'email.com', 'domain.com', 'test.com', 'sentry.io',
            'wixpress.com', 'sentry-next.wixpress.com'}
    return [e for e in emails if e.split('@')[1].lower() not in junk]

def utcnow_iso():
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════
# SCORING UTILITIES (car wash specific, reusable for other verticals)
# ═══════════════════════════════════════════════════════════

REVENUE_BENCHMARKS = {
    'car_wash': {
        'tunnel':       {'low': 800_000, 'high': 2_500_000, 'avg_ticket': 15},
        'full_service': {'low': 500_000, 'high': 1_500_000, 'avg_ticket': 25},
        'self_service': {'low': 100_000, 'high': 400_000,   'avg_ticket': 7},
        'flex_serve':   {'low': 600_000, 'high': 2_000_000, 'avg_ticket': 18},
        'unknown':      {'low': 300_000, 'high': 1_200_000, 'avg_ticket': 12},
    },
    'restaurant': {
        'default': {'low': 250_000, 'high': 2_000_000, 'avg_ticket': 30},
    },
    'salon': {
        'default': {'low': 150_000, 'high': 800_000, 'avg_ticket': 50},
    },
    'default': {
        'default': {'low': 200_000, 'high': 1_500_000, 'avg_ticket': 20},
    },
}

VALUATION_MULTIPLES = {'low': 2.5, 'high': 5.0}
REVIEW_TO_ANNUAL_CUSTOMERS = 150

def estimate_revenue(business_type, sub_type, review_count, rating):
    """Estimate revenue range based on type and review count proxy."""
    benchmarks = REVENUE_BENCHMARKS.get(business_type, REVENUE_BENCHMARKS['default'])
    bench = benchmarks.get(sub_type, benchmarks.get('default', benchmarks.get('unknown')))
    if not bench:
        bench = REVENUE_BENCHMARKS['default']['default']

    if review_count and review_count > 0:
        annual_customers = review_count * REVIEW_TO_ANNUAL_CUSTOMERS
        base_revenue = annual_customers * bench['avg_ticket']
        rating_mult = 1.0
        if rating and rating >= 4.5:
            rating_mult = 1.15
        elif rating and rating < 3.0:
            rating_mult = 0.7
        rev_low = int(base_revenue * 0.8 * rating_mult)
        rev_high = int(base_revenue * 1.2 * rating_mult)
        rev_low = max(bench['low'] // 2, min(rev_low, bench['high'] * 2))
        rev_high = max(rev_low, min(rev_high, bench['high'] * 3))
    else:
        rev_low = bench['low']
        rev_high = bench['high']

    return rev_low, rev_high

def compute_acquisition_score(review_count, rating, business_type, sub_type,
                               is_chain=False, employee_count=None):
    """Compute acquisition score 0-100 and tier A/B/C/D."""
    score = 50

    # Review count signal
    rc = review_count or 0
    if rc > 500: score += 15
    elif rc > 200: score += 10
    elif rc > 50: score += 5
    elif rc < 10: score -= 5

    # Rating signal
    r = rating or 0
    if r >= 4.5: score += 10
    elif r >= 4.0: score += 5
    elif r < 3.0: score -= 15
    elif r < 3.5: score -= 5

    # Type bonuses (car wash specific, extensible)
    if business_type == 'car_wash':
        if sub_type == 'tunnel': score += 10
        elif sub_type == 'flex_serve': score += 8
        elif sub_type == 'full_service': score += 3
        elif sub_type == 'self_service': score -= 5

    # Chain penalty
    if is_chain:
        score -= 20

    # Employee count signal
    if employee_count:
        if employee_count >= 20: score += 5
        elif employee_count <= 3: score -= 3

    score = max(0, min(100, score))

    if score >= 75: tier = 'A'
    elif score >= 60: tier = 'B'
    elif score >= 45: tier = 'C'
    else: tier = 'D'

    return score, tier
