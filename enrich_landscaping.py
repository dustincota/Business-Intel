#!/usr/bin/env python3
"""
enrich_landscaping.py — Multi-pass enrichment for landscaping businesses.

Targets Overture Maps records (85K+ with websites, phones, emails).
Runs 3 passes at $0 cost, optional Apollo pass for top prospects.

Pass 1: Domain extraction + HTTP validation (all with websites)
Pass 2: Website scraping — emails, owner names, services, hours
Pass 3: Revenue estimation + acquisition scoring + tier assignment
Pass 4: Apollo enrichment (optional, requires APOLLO_API_KEY, uses credits)

Usage:
    python3 enrich_landscaping.py                  # all passes (skip Apollo if no key)
    python3 enrich_landscaping.py --pass 1         # domain validation only
    python3 enrich_landscaping.py --pass 2         # website scraping only
    python3 enrich_landscaping.py --pass 3         # scoring only
    python3 enrich_landscaping.py --pass 4 --limit 100  # Apollo top-100
    python3 enrich_landscaping.py --resume         # resume from last position

Author: Dustin Cota — Hedgestone M&A Intelligence Platform
"""

import argparse
import concurrent.futures
import json
import logging
import os
import re
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

# Optional imports
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# ─── Load .env ───
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")

SCRIPT_DIR = Path(__file__).resolve().parent
PROGRESS_FILE = SCRIPT_DIR / "enrich_landscaping_progress.json"
LOG_FILE = SCRIPT_DIR / "enrich_landscaping_deep.log"

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# Revenue benchmarks for landscaping
REVENUE_BENCHMARKS = {
    'landscaping':       {'low': 200_000, 'high': 2_000_000, 'avg_ticket': 5_000},
    'tree_services':     {'low': 150_000, 'high': 1_500_000, 'avg_ticket': 1_200},
    'lawn_service':      {'low': 80_000,  'high': 500_000,   'avg_ticket': 200},
    'irrigation':        {'low': 200_000, 'high': 1_200_000, 'avg_ticket': 3_500},
    'landscape_architect': {'low': 300_000, 'high': 2_500_000, 'avg_ticket': 15_000},
    'gardener':          {'low': 50_000,  'high': 300_000,   'avg_ticket': 150},
    'default':           {'low': 150_000, 'high': 1_000_000, 'avg_ticket': 2_000},
}

# ═══════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════

def setup_logging():
    logger = logging.getLogger("enrich_landscaping")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(str(LOG_FILE), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

log = setup_logging()


# ═══════════════════════════════════════════════════════════
# Progress Tracking
# ═══════════════════════════════════════════════════════════

class Progress:
    def __init__(self):
        self.data = self._load()

    def _load(self):
        if PROGRESS_FILE.exists():
            try:
                return json.loads(PROGRESS_FILE.read_text())
            except (json.JSONDecodeError, IOError):
                pass
        return {
            "pass1_done": [], "pass2_done": [], "pass3_done": [],
            "pass4_done": [],
            "stats": {
                "domains_validated": 0, "domains_dead": 0,
                "websites_scraped": 0, "emails_found": 0,
                "owners_found": 0, "scored": 0,
                "apollo_enriched": 0,
            },
        }

    def save(self):
        tmp = str(PROGRESS_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=2, default=str)
        os.replace(tmp, str(PROGRESS_FILE))

    def is_done(self, pass_name, business_id):
        return business_id in self.data.get(f"{pass_name}_done", [])

    def mark_done(self, pass_name, business_id):
        key = f"{pass_name}_done"
        if key not in self.data:
            self.data[key] = []
        self.data[key].append(business_id)

    def inc(self, stat, n=1):
        self.data["stats"][stat] = self.data["stats"].get(stat, 0) + n


# ═══════════════════════════════════════════════════════════
# Supabase Client
# ═══════════════════════════════════════════════════════════

class DB:
    def __init__(self):
        self.url = SUPABASE_URL.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
        })

    def fetch(self, table, filters="", select="*", order="created_at.asc",
              limit=1000, offset=0):
        url = f"{self.url}/rest/v1/{table}?select={select}&order={order}&limit={limit}&offset={offset}"
        if filters:
            url += f"&{filters}"
        r = self.session.get(url, timeout=30)
        return r.json() if r.status_code == 200 else []

    def update(self, table, pk_col, pk_val, updates, retries=2):
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

    def insert(self, table, records, retries=2):
        url = f"{self.url}/rest/v1/{table}"
        headers = {"Prefer": "return=representation"}
        for attempt in range(retries + 1):
            try:
                r = self.session.post(url, json=records, headers=headers, timeout=30)
                if r.status_code in (200, 201):
                    return r.json()
                if r.status_code == 409:
                    return []
                if r.status_code == 429 and attempt < retries:
                    time.sleep(30 * (attempt + 1))
                    continue
                return []
            except Exception:
                if attempt < retries:
                    time.sleep(5)
                continue
        return []

    def upsert(self, table, records, on_conflict=""):
        url = f"{self.url}/rest/v1/{table}"
        headers = {"Prefer": "resolution=merge-duplicates,return=minimal"}
        if on_conflict:
            url += f"?on_conflict={on_conflict}"
        r = self.session.post(url, json=records, headers=headers, timeout=30)
        return r.status_code in (200, 201, 204)


# ═══════════════════════════════════════════════════════════
# Domain Utilities
# ═══════════════════════════════════════════════════════════

def extract_domain(url):
    """Extract bare domain from a URL."""
    if not url:
        return ""
    url = url.lower().strip()
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    return url.split('/')[0].split('?')[0].split('#')[0]


def validate_domain_fast(domain, timeout=3):
    """DNS resolution check — fast."""
    try:
        socket.setdefaulttimeout(timeout)
        socket.getaddrinfo(domain, 80)
        return True
    except (socket.gaierror, socket.timeout, OSError):
        return False


def http_check(url, timeout=5):
    """HTTP HEAD check — returns (status_code, final_url) or (None, None)."""
    if not url.startswith("http"):
        url = f"https://{url}"
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True,
                          headers={"User-Agent": USER_AGENT})
        return r.status_code, r.url
    except Exception:
        try:
            # Try HTTP if HTTPS fails
            url_http = url.replace("https://", "http://")
            r = requests.head(url_http, timeout=timeout, allow_redirects=True,
                              headers={"User-Agent": USER_AGENT})
            return r.status_code, r.url
        except Exception:
            return None, None


# ═══════════════════════════════════════════════════════════
# Website Scraping
# ═══════════════════════════════════════════════════════════

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}')
JUNK_DOMAINS = {
    'example.com', 'email.com', 'domain.com', 'test.com', 'sentry.io',
    'wixpress.com', 'squarespace.com', 'wordpress.com', 'godaddy.com',
    'googleapis.com', 'gstatic.com', 'w3.org', 'schema.org',
    'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
    'youtube.com', 'google.com', 'yelp.com', 'bbb.org', 'angi.com',
}

OWNER_PATTERNS = [
    re.compile(r'(?:owner|founder|president|ceo|principal|proprietor)\s*[:\-–—]?\s*([A-Z][a-z]+ [A-Z][a-z]+)', re.I),
    re.compile(r'(?:owned by|founded by|managed by|operated by)\s+([A-Z][a-z]+ [A-Z][a-z]+)', re.I),
    re.compile(r'([A-Z][a-z]+ [A-Z][a-z]+)\s*,?\s*(?:Owner|Founder|President|CEO|Principal)', re.I),
]

SERVICE_KEYWORDS = [
    'lawn mowing', 'lawn care', 'landscaping', 'landscape design', 'hardscaping',
    'irrigation', 'sprinkler', 'tree trimming', 'tree removal', 'stump grinding',
    'mulching', 'sod installation', 'retaining wall', 'patio', 'walkway',
    'drainage', 'grading', 'excavation', 'snow removal', 'leaf removal',
    'bush trimming', 'hedge trimming', 'fertilization', 'weed control',
    'aeration', 'seeding', 'overseeding', 'power washing', 'gutter cleaning',
    'fence', 'deck', 'pergola', 'fire pit', 'outdoor lighting',
    'commercial landscaping', 'residential landscaping', 'maintenance',
]


def scrape_website(url, timeout=10):
    """Scrape a business website for contact info, owner, services.
    Returns dict with extracted data or empty dict on failure."""
    if not HAS_BS4:
        return {}

    if not url.startswith("http"):
        url = f"https://{url}"

    try:
        r = requests.get(url, timeout=timeout, headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html",
        }, allow_redirects=True)
        if r.status_code != 200:
            return {}
    except Exception:
        return {}

    result = {}
    try:
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(r.text, "html.parser")
        except Exception:
            return {}

    text = soup.get_text(separator=" ", strip=True)

    # Extract emails
    emails = set(EMAIL_RE.findall(text))
    # Also check mailto links
    for a in soup.find_all("a", href=True):
        if "mailto:" in a["href"]:
            email = a["href"].replace("mailto:", "").split("?")[0].strip()
            if "@" in email:
                emails.add(email)

    # Filter junk
    emails = [e for e in emails if e.split("@")[1].lower() not in JUNK_DOMAINS]
    if emails:
        result["emails"] = emails[:5]

    # Extract phones from page (supplement Overture data)
    phones = PHONE_RE.findall(text)
    if phones:
        result["phones"] = list(set(phones))[:3]

    # Owner detection
    GARBAGE_NAMES = {
        'tell us', 'contact us', 'call us', 'click here', 'read more',
        'learn more', 'get started', 'sign up', 'log in', 'about us',
        'our team', 'the team', 'and businesses', 'the company',
        'the owner', 'your name', 'first last', 'full name',
    }
    for pattern in OWNER_PATTERNS:
        m = pattern.search(text)
        if m:
            name = m.group(1).strip()
            # Validation: 2+ words, not all caps, not too long, not garbage
            if (4 < len(name) < 40
                    and not name.isupper()
                    and name.lower() not in GARBAGE_NAMES
                    and len(name.split()) >= 2
                    and all(w[0].isupper() for w in name.split() if w)):
                result["owner_name"] = name
                break

    # Service detection
    text_lower = text.lower()
    found_services = []
    for svc in SERVICE_KEYWORDS:
        if svc in text_lower:
            found_services.append(svc)
    if found_services:
        result["services"] = found_services

    # Description from meta tags
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        result["description"] = meta_desc["content"][:500]

    # Title
    title = soup.find("title")
    if title and title.string:
        result["page_title"] = title.string.strip()[:200]

    # Try to find "About" page link for more owner info
    about_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        text_link = (a.get_text() or "").lower()
        if any(w in href or w in text_link for w in ["about", "our-team", "staff", "meet"]):
            full_url = a["href"]
            if full_url.startswith("/"):
                parsed = urlparse(url)
                full_url = f"{parsed.scheme}://{parsed.netloc}{full_url}"
            elif not full_url.startswith("http"):
                continue
            about_links.append(full_url)

    # Quick scrape of about page for owner names (if no owner found yet)
    if not result.get("owner_name") and about_links:
        try:
            about_url = about_links[0]
            r2 = requests.get(about_url, timeout=8, headers={
                "User-Agent": USER_AGENT, "Accept": "text/html",
            }, allow_redirects=True)
            if r2.status_code == 200:
                soup2 = BeautifulSoup(r2.text, "lxml" if "lxml" in str(type(soup.builder)) else "html.parser")
                about_text = soup2.get_text(separator=" ", strip=True)
                for pattern in OWNER_PATTERNS:
                    m = pattern.search(about_text)
                    if m:
                        name = m.group(1).strip()
                        if (4 < len(name) < 40
                                and not name.isupper()
                                and name.lower() not in GARBAGE_NAMES
                                and len(name.split()) >= 2
                                and all(w[0].isupper() for w in name.split() if w)):
                            result["owner_name"] = name
                            break
                # Also check for more emails on about page
                about_emails = set(EMAIL_RE.findall(about_text))
                for a in soup2.find_all("a", href=True):
                    if "mailto:" in a["href"]:
                        email = a["href"].replace("mailto:", "").split("?")[0].strip()
                        if "@" in email:
                            about_emails.add(email)
                about_emails = [e for e in about_emails if e.split("@")[1].lower() not in JUNK_DOMAINS]
                if about_emails:
                    existing = set(result.get("emails", []))
                    result["emails"] = list(existing | set(about_emails))[:5]
        except Exception:
            pass

    return result


# ═══════════════════════════════════════════════════════════
# Revenue Estimation & Scoring
# ═══════════════════════════════════════════════════════════

def estimate_revenue(sub_type, employee_count=None, has_website=True, services=None):
    """Estimate revenue range for a landscaping business."""
    bench = REVENUE_BENCHMARKS.get(sub_type, REVENUE_BENCHMARKS['default'])

    # Base range
    rev_low = bench['low']
    rev_high = bench['high']

    # Adjust by employee count if available
    if employee_count:
        if employee_count >= 50:
            rev_low = int(rev_low * 3)
            rev_high = int(rev_high * 3)
        elif employee_count >= 20:
            rev_low = int(rev_low * 2)
            rev_high = int(rev_high * 2)
        elif employee_count >= 10:
            rev_low = int(rev_low * 1.3)
            rev_high = int(rev_high * 1.3)
        elif employee_count <= 3:
            rev_low = int(rev_low * 0.5)
            rev_high = int(rev_high * 0.5)

    # Service breadth bonus
    if services and len(services) >= 8:
        rev_low = int(rev_low * 1.3)
        rev_high = int(rev_high * 1.3)
    elif services and len(services) >= 4:
        rev_low = int(rev_low * 1.1)
        rev_high = int(rev_high * 1.1)

    # No website = probably smaller
    if not has_website:
        rev_low = int(rev_low * 0.5)
        rev_high = int(rev_high * 0.5)

    return rev_low, rev_high


def compute_score(biz, scraped=None):
    """Acquisition score 0-100 for landscaping businesses."""
    score = 50
    scraped = scraped or {}

    # Website = legit business
    if biz.get("website"):
        score += 5

    # Phone = reachable
    if biz.get("phone"):
        score += 3

    # Email found
    if biz.get("email") or scraped.get("emails"):
        score += 5

    # Owner name identified
    if biz.get("owner_name") or scraped.get("owner_name"):
        score += 8

    # Service breadth
    services = scraped.get("services", [])
    if len(services) >= 8:
        score += 10
    elif len(services) >= 5:
        score += 5
    elif len(services) >= 3:
        score += 3

    # Sub-type bonuses (higher value service = better target)
    sub = biz.get("sub_type", "")
    if sub == "landscape_architect":
        score += 8
    elif sub in ("landscaping", "irrigation"):
        score += 5
    elif sub == "tree_services":
        score += 3
    elif sub in ("lawn_service", "gardener"):
        score -= 2  # Lower barrier to entry, harder to differentiate

    # Employee count (if known)
    ec = biz.get("employee_count")
    if ec:
        if ec >= 20:
            score += 10
        elif ec >= 10:
            score += 5
        elif ec <= 2:
            score -= 5

    # Domain validation status
    enrichment = biz.get("enrichment_status", "raw")
    if enrichment == "validated":
        score += 2
    elif enrichment == "dead_domain":
        score -= 15

    score = max(0, min(100, score))
    if score >= 75:
        tier = 'A'
    elif score >= 60:
        tier = 'B'
    elif score >= 45:
        tier = 'C'
    else:
        tier = 'D'

    return score, tier


# ═══════════════════════════════════════════════════════════
# PASS 1: Domain Extraction + Validation
# ═══════════════════════════════════════════════════════════

def _validate_one(biz):
    """Validate a single business domain — used by thread pool."""
    bid = biz["business_id"]
    website = biz.get("website", "")
    domain = extract_domain(website)

    if not domain or len(domain) < 4:
        return bid, "skip", {}

    valid = validate_domain_fast(domain)
    if valid:
        status, final_url = http_check(website)
        if status and status < 400:
            return bid, "valid", {"enrichment_status": "validated"}
        else:
            note = f"HTTP {status}" if status else "HTTP timeout"
            return bid, "dead", {"enrichment_status": "dead_domain", "notes": note}
    else:
        return bid, "dead", {"enrichment_status": "dead_domain", "notes": "DNS resolution failed"}


def run_pass1(db, progress, limit=None, workers=15):
    """Validate domains for all landscaping businesses with websites.
    Uses thread pool for parallel DNS/HTTP checks."""
    log.info("=" * 60)
    log.info(f"PASS 1: Domain Extraction + HTTP Validation ({workers} threads)")
    log.info("=" * 60)

    offset = 0
    page_size = 1000
    total_processed = 0
    total_valid = 0
    total_dead = 0
    total_skip = 0

    while True:
        businesses = db.fetch(
            "businesses",
            filters="business_type=eq.landscaping&website=not.is.null&enrichment_status=eq.raw&data_source=eq.overture",
            select="business_id,name,website",
            limit=page_size,
            offset=offset,
        )
        if not businesses:
            break

        # Filter out already-done
        todo = [b for b in businesses if not progress.is_done("pass1", b["business_id"])]
        if limit:
            remaining = limit - total_processed
            todo = todo[:remaining]

        if not todo:
            offset += page_size
            continue

        log.info(f"  Fetched {len(businesses)}, processing {len(todo)} (offset {offset})")

        # Parallel validation
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_validate_one, b): b for b in todo}
            for future in concurrent.futures.as_completed(futures):
                try:
                    bid, result_type, updates = future.result()
                except Exception as e:
                    bid = futures[future]["business_id"]
                    result_type = "dead"
                    updates = {"enrichment_status": "dead_domain", "notes": f"Error: {str(e)[:100]}"}

                if result_type == "skip":
                    total_skip += 1
                elif result_type == "valid":
                    total_valid += 1
                    progress.inc("domains_validated")
                else:
                    total_dead += 1
                    progress.inc("domains_dead")

                if updates:
                    db.update("businesses", "business_id", bid, updates)

                progress.mark_done("pass1", bid)
                total_processed += 1

        if total_processed % 100 == 0 or total_processed == len(todo):
            log.info(f"  Progress: {total_processed} done ({total_valid} valid, {total_dead} dead, {total_skip} skip)")
            progress.save()

        offset += page_size
        if limit and total_processed >= limit:
            break

    progress.save()
    log.info(f"  PASS 1 COMPLETE: {total_processed} processed, {total_valid} valid, {total_dead} dead, {total_skip} skip")
    return total_processed


# ═══════════════════════════════════════════════════════════
# PASS 2: Website Scraping
# ═══════════════════════════════════════════════════════════

def run_pass2(db, progress, limit=None):
    """Scrape validated websites for emails, owner names, services."""
    log.info("=" * 60)
    log.info("PASS 2: Website Scraping")
    log.info("=" * 60)

    if not HAS_BS4:
        log.error("BeautifulSoup4 not installed. Run: pip3 install beautifulsoup4 lxml")
        return 0

    offset = 0
    page_size = 500
    total = 0
    emails_found = 0
    owners_found = 0

    while True:
        businesses = db.fetch(
            "businesses",
            filters="business_type=eq.landscaping&enrichment_status=eq.validated&data_source=eq.overture",
            select="business_id,name,website,email,phone,owner_name",
            limit=page_size,
            offset=offset,
        )
        if not businesses:
            break

        log.info(f"  Fetched {len(businesses)} validated (offset {offset})")

        for biz in businesses:
            bid = biz["business_id"]
            if progress.is_done("pass2", bid):
                continue

            website = biz.get("website", "")
            if not website:
                progress.mark_done("pass2", bid)
                continue

            scraped = scrape_website(website)

            updates = {"enrichment_status": "scraped", "enrichment_date": datetime.now(timezone.utc).isoformat()}

            # Email — prefer scraped over existing if existing is null
            if scraped.get("emails") and not biz.get("email"):
                updates["email"] = scraped["emails"][0]
                emails_found += 1

            # Owner name
            if scraped.get("owner_name") and not biz.get("owner_name"):
                updates["owner_name"] = scraped["owner_name"]
                owners_found += 1

            # Store scraped details in notes (JSONB would be better, but notes works)
            details = {}
            if scraped.get("services"):
                details["services"] = scraped["services"]
            if scraped.get("description"):
                details["description"] = scraped["description"][:300]
            if scraped.get("emails"):
                details["all_emails"] = scraped["emails"]
            if scraped.get("phones"):
                details["scraped_phones"] = scraped["phones"]
            if scraped.get("page_title"):
                details["page_title"] = scraped["page_title"][:100]

            if details:
                existing_notes = biz.get("notes") or ""
                scraped_json = json.dumps(details, separators=(",", ":"))
                updates["notes"] = (existing_notes + "\n" if existing_notes else "") + f"[scraped]{scraped_json}"

            db.update("businesses", "business_id", bid, updates)
            progress.mark_done("pass2", bid)
            progress.inc("websites_scraped")
            if scraped.get("emails"):
                progress.inc("emails_found")
            if scraped.get("owner_name"):
                progress.inc("owners_found")
            total += 1

            if total % 50 == 0:
                log.info(f"  Progress: {total} scraped ({emails_found} emails, {owners_found} owners)")
                progress.save()

            # Polite delay between scrapes
            time.sleep(0.3)

            if limit and total >= limit:
                break

        offset += page_size
        if limit and total >= limit:
            break

    progress.save()
    log.info(f"  PASS 2 COMPLETE: {total} scraped, {emails_found} emails, {owners_found} owners found")
    return total


# ═══════════════════════════════════════════════════════════
# PASS 3: Scoring + Revenue Estimation
# ═══════════════════════════════════════════════════════════

def run_pass3(db, progress, limit=None):
    """Compute revenue estimates and acquisition scores."""
    log.info("=" * 60)
    log.info("PASS 3: Revenue Estimation + Acquisition Scoring")
    log.info("=" * 60)

    offset = 0
    page_size = 1000
    total = 0
    tier_counts = {'A': 0, 'B': 0, 'C': 0, 'D': 0}

    while True:
        # Score all landscaping from overture (validated, scraped, or raw with phone)
        businesses = db.fetch(
            "businesses",
            filters="business_type=eq.landscaping&data_source=eq.overture&acquisition_score=is.null",
            select="business_id,name,website,phone,email,owner_name,sub_type,employee_count,enrichment_status,notes",
            limit=page_size,
            offset=offset,
        )
        if not businesses:
            break

        log.info(f"  Fetched {len(businesses)} unscored (offset {offset})")

        for biz in businesses:
            bid = biz["business_id"]
            if progress.is_done("pass3", bid):
                continue

            # Parse scraped data from notes
            scraped = {}
            notes = biz.get("notes") or ""
            if "[scraped]" in notes:
                try:
                    scraped_str = notes.split("[scraped]")[1].split("\n")[0]
                    scraped = json.loads(scraped_str)
                except (json.JSONDecodeError, IndexError):
                    pass

            score, tier = compute_score(biz, scraped)
            rev_low, rev_high = estimate_revenue(
                biz.get("sub_type", "default"),
                employee_count=biz.get("employee_count"),
                has_website=bool(biz.get("website")),
                services=scraped.get("services"),
            )

            rev_mid = (rev_low + rev_high) // 2
            updates = {
                "acquisition_score": score,
                "acquisition_tier": tier,
                "estimated_revenue": rev_mid,
            }

            # Set enrichment_status if still raw
            if biz.get("enrichment_status") == "raw":
                updates["enrichment_status"] = "scored"

            db.update("businesses", "business_id", bid, updates)
            progress.mark_done("pass3", bid)
            progress.inc("scored")
            tier_counts[tier] += 1
            total += 1

            if total % 500 == 0:
                log.info(f"  Progress: {total} scored — A:{tier_counts['A']} B:{tier_counts['B']} C:{tier_counts['C']} D:{tier_counts['D']}")
                progress.save()

            if limit and total >= limit:
                break

        offset += page_size
        if limit and total >= limit:
            break

    progress.save()
    log.info(f"  PASS 3 COMPLETE: {total} scored — A:{tier_counts['A']} B:{tier_counts['B']} C:{tier_counts['C']} D:{tier_counts['D']}")
    return total


# ═══════════════════════════════════════════════════════════
# PASS 4: Apollo Enrichment (optional, uses credits)
# ═══════════════════════════════════════════════════════════

def run_pass4(db, progress, limit=100):
    """Apollo org + people enrichment on top-scored landscaping businesses."""
    if not APOLLO_API_KEY:
        log.warning("APOLLO_API_KEY not set — skipping Pass 4")
        log.info("  Set APOLLO_API_KEY in .env to enable Apollo enrichment")
        return 0

    log.info("=" * 60)
    log.info(f"PASS 4: Apollo Enrichment (limit: {limit} businesses)")
    log.info("=" * 60)

    from enrich_utils import ApolloClient
    apollo = ApolloClient()

    # Fetch top-scored businesses that haven't been Apollo-enriched
    businesses = db.fetch(
        "businesses",
        filters="business_type=eq.landscaping&data_source=eq.overture&website=not.is.null&enrichment_status=in.(validated,scraped)&acquisition_tier=in.(A,B)",
        select="business_id,name,website,phone,email,owner_name,sub_type",
        order="acquisition_score.desc",
        limit=limit,
    )

    log.info(f"  Found {len(businesses)} top-tier businesses for Apollo enrichment")
    total = 0
    people_found = 0

    for biz in businesses:
        bid = biz["business_id"]
        if progress.is_done("pass4", bid):
            continue

        domain = extract_domain(biz.get("website", ""))
        if not domain:
            progress.mark_done("pass4", bid)
            continue

        name = biz.get("name", "")
        log.info(f"  [{total+1}/{len(businesses)}] Apollo: {name} ({domain})")

        # 1. Org enrichment
        org_data = apollo.org_enrich(domain)
        updates = {}

        if org_data and not org_data.get("error"):
            org = org_data.get("organization", {})
            if org:
                if org.get("estimated_num_employees"):
                    updates["employee_count"] = org["estimated_num_employees"]
                if org.get("phone"):
                    updates["phone"] = org["phone"]
                if org.get("linkedin_url"):
                    # Store in notes since no linkedin column on businesses
                    notes = biz.get("notes") or ""
                    updates["notes"] = notes + f"\n[apollo]linkedin={org['linkedin_url']}"
                if org.get("industry"):
                    updates["industry"] = org["industry"]

        # 2. People search — find owner/manager
        people_data = apollo.people_search(
            domain=domain,
            titles=["Owner", "Founder", "President", "CEO", "General Manager", "Principal"],
            seniorities=["owner", "founder", "c_suite", "vp", "director"],
            limit=3,
        )

        if people_data and not people_data.get("error"):
            people = people_data.get("people", [])
            for person in people[:2]:
                pname = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
                if pname and not updates.get("owner_name"):
                    updates["owner_name"] = pname

                # Create/link person record
                person_record = {
                    "full_name": pname,
                    "first_name": person.get("first_name", ""),
                    "last_name": person.get("last_name", ""),
                    "title": person.get("title", ""),
                    "email": person.get("email", ""),
                    "phone": person.get("phone_numbers", [{}])[0].get("sanitized_number", "") if person.get("phone_numbers") else "",
                    "linkedin_url": person.get("linkedin_url", ""),
                    "source": "apollo",
                    "company_name": name,
                    "company_domain": domain,
                }
                # Insert into people table
                created = db.insert("people", [person_record])
                if created:
                    people_found += 1

        updates["enrichment_status"] = "enriched"
        updates["enrichment_date"] = datetime.now(timezone.utc).isoformat()
        db.update("businesses", "business_id", bid, updates)

        progress.mark_done("pass4", bid)
        progress.inc("apollo_enriched")
        total += 1

        if total % 10 == 0:
            log.info(f"  Apollo progress: {total} enriched, {people_found} people found")
            progress.save()

    progress.save()
    log.info(f"  PASS 4 COMPLETE: {total} Apollo-enriched, {people_found} people found")
    return total


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Multi-pass landscaping enrichment")
    parser.add_argument("--pass", type=int, dest="run_pass", help="Run specific pass (1-4)")
    parser.add_argument("--limit", type=int, help="Limit records per pass")
    parser.add_argument("--resume", action="store_true", help="Resume from progress file")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LANDSCAPING ENRICHMENT ENGINE — Hedgestone M&A Platform")
    log.info(f"Started: {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    db = DB()
    progress = Progress()

    if not args.resume:
        log.info(f"Previous progress: {json.dumps(progress.data.get('stats', {}), indent=2)}")

    if args.run_pass:
        passes = [args.run_pass]
    else:
        passes = [1, 2, 3]  # Default: all free passes (skip Apollo)

    for p in passes:
        if p == 1:
            run_pass1(db, progress, limit=args.limit)
        elif p == 2:
            run_pass2(db, progress, limit=args.limit)
        elif p == 3:
            run_pass3(db, progress, limit=args.limit)
        elif p == 4:
            run_pass4(db, progress, limit=args.limit or 100)

    log.info("")
    log.info("=" * 60)
    log.info("ALL PASSES COMPLETE")
    log.info(f"Final stats: {json.dumps(progress.data.get('stats', {}), indent=2)}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
