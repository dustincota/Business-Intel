#!/usr/bin/env python3
"""
enrichment_engine.py — Self-hosted enrichment engine replacing Clay + Apollo.

Goes directly to underlying data sources: SMTP verification, DNS/WHOIS,
website scraping, Google Maps (Apify), LinkedIn (Apify), Yelp/BBB (Apify),
SEC EDGAR, and job postings. Writes results to Supabase.

Requirements (pip install):
    requests beautifulsoup4 python-whois dnspython lxml

Usage:
    python3 enrichment_engine.py --type car_wash --limit 100
    python3 enrichment_engine.py --type car_wash --limit 50 --skip-apify
    python3 enrichment_engine.py --domain "example.com" --name "Example Inc"
    python3 enrichment_engine.py --check-credits

Author: Dustin Cota — M&A Intelligence Platform
"""

import argparse
import concurrent.futures
import hashlib
import json
import logging
import os
import re
import smtplib
import socket
import ssl
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import requests

# Optional imports — graceful degradation
try:
    import dns.resolver
    HAS_DNS = True
except ImportError:
    HAS_DNS = False

try:
    import whois
    HAS_WHOIS = True
except ImportError:
    HAS_WHOIS = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── Load .env ───
from pathlib import Path as _Path
_env = _Path(__file__).resolve().parent / ".env"
if _env.exists():
    for _l in _env.read_text().splitlines():
        _l = _l.strip()
        if _l and not _l.startswith("#") and "=" in _l:
            _k, _v = _l.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN", "")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_file: str = "enrichment_engine.log") -> logging.Logger:
    """Configure dual output: console + file."""
    logger = logging.getLogger("enrichment_engine")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File
    fh = logging.FileHandler(os.path.join(SCRIPT_DIR, log_file), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ---------------------------------------------------------------------------
# Progress / State Persistence
# ---------------------------------------------------------------------------

class ProgressTracker:
    """JSON-file-backed progress tracker for resume capability."""

    def __init__(self, filename: str):
        self.path = os.path.join(SCRIPT_DIR, filename)
        self.data = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {}

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=2, default=str)
        os.replace(tmp, self.path)

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.data[key] = value

    def inc(self, key, amount=1):
        self.data[key] = self.data.get(key, 0) + amount

    def add_to_set(self, key, value):
        """Track processed IDs to support resume."""
        s = set(self.data.get(key, []))
        s.add(value)
        self.data[key] = list(s)

    def in_set(self, key, value) -> bool:
        return value in self.data.get(key, [])


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Token-bucket rate limiter."""

    def __init__(self, calls_per_second: float = 1.0):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0

    def wait(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()


# ---------------------------------------------------------------------------
# Supabase Client (reuses pattern from enrich_utils.py)
# ---------------------------------------------------------------------------

class SupabaseClient:
    """Lightweight PostgREST client for Supabase."""

    def __init__(self, url: str = None, key: str = None):
        self.url = (url or SUPABASE_URL).rstrip("/")
        self.key = key or SUPABASE_KEY
        self.session = requests.Session()
        self.session.headers.update({
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        })

    def fetch(self, table: str, filters: str = "", select: str = "*",
              order: str = "created_at.asc", limit: int = 100,
              offset: int = 0) -> list:
        url = (
            f"{self.url}/rest/v1/{table}"
            f"?select={select}&order={order}&limit={limit}&offset={offset}"
        )
        if filters:
            url += f"&{filters}"
        try:
            r = self.session.get(url, timeout=30)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.warning(f"Supabase fetch error: {e}")
        return []

    def fetch_count(self, table: str, filters: str = "") -> int:
        url = f"{self.url}/rest/v1/{table}?select=count"
        if filters:
            url += f"&{filters}"
        try:
            r = self.session.get(
                url,
                headers={"Prefer": "count=exact"},
                timeout=30,
            )
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
        except Exception:
            pass
        return 0

    def update(self, table: str, pk_col: str, pk_val: str,
               updates: dict, retries: int = 2) -> bool:
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

    def upsert(self, table: str, records: list, on_conflict: str = "",
               retries: int = 2) -> bool:
        url = f"{self.url}/rest/v1/{table}"
        headers = {"Prefer": "return=minimal"}
        if on_conflict:
            headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
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

    def insert(self, table: str, records: list, retries: int = 2) -> list:
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


# ===========================================================================
#  1. EMAIL FINDER
# ===========================================================================

class EmailFinder:
    """
    Find email addresses for a person at a company.

    Methods (in order of reliability):
        1. Pattern generation + MX verification
        2. SMTP RCPT TO verification
        3. Website scraping for email patterns
        4. Catch-all domain detection
    """

    # Common junk domains to skip
    JUNK_DOMAINS = frozenset({
        "example.com", "test.com", "domain.com", "email.com",
        "sentry.io", "wixpress.com", "squarespace.com",
        "googleapis.com", "google.com", "facebook.com",
    })

    def __init__(self):
        self._mx_cache: Dict[str, List[str]] = {}
        self._catchall_cache: Dict[str, Optional[bool]] = {}
        self.rate_limiter = RateLimiter(calls_per_second=2.0)

    def find_email(self, first_name: str, last_name: str,
                   domain: str) -> Optional[Dict[str, Any]]:
        """
        Try to find and verify an email for this person.

        Returns: {"email": "...", "confidence": "high|medium|low", "method": "..."}
                 or None if nothing found.
        """
        if not first_name or not last_name or not domain:
            return None

        first = first_name.strip().lower()
        last = last_name.strip().lower()
        domain = domain.strip().lower()

        if domain in self.JUNK_DOMAINS:
            return None

        # Step 1: Check MX records (does domain accept email at all?)
        mx_hosts = self._get_mx(domain)
        if not mx_hosts:
            log.debug(f"EmailFinder: No MX for {domain}")
            return None

        # Step 2: Generate candidate patterns
        patterns = self._generate_patterns(first, last, domain)

        # Step 3: Check if this is a catch-all domain
        is_catchall = self._check_catchall(domain, mx_hosts[0])

        # Step 4: SMTP verify each pattern
        for pattern in patterns:
            self.rate_limiter.wait()
            verified = self._smtp_verify(pattern, mx_hosts[0])

            if verified is True:
                confidence = "medium" if is_catchall else "high"
                return {
                    "email": pattern,
                    "confidence": confidence,
                    "method": "smtp_verified",
                    "mx_host": mx_hosts[0],
                    "is_catchall": is_catchall,
                }
            elif verified is None:
                # SMTP inconclusive (timeout, blocked, etc.)
                # Fall through to pattern-based guess
                continue

        # Step 5: If SMTP did not work, return best-guess pattern
        # The first pattern (first.last@domain) is correct ~60% of the time
        return {
            "email": patterns[0],
            "confidence": "low",
            "method": "pattern_guess",
            "mx_host": mx_hosts[0] if mx_hosts else None,
            "is_catchall": is_catchall,
        }

    def _generate_patterns(self, first: str, last: str,
                           domain: str) -> List[str]:
        """Generate common email patterns. Ordered by frequency in the wild."""
        # Guard against empty strings or single-char names
        f = re.sub(r"[^a-z]", "", first)
        l = re.sub(r"[^a-z]", "", last)
        if not f or not l:
            return [f"info@{domain}", f"contact@{domain}"]

        candidates = [
            f"{f}.{l}@{domain}",          # john.smith@  (most common)
            f"{f[0]}{l}@{domain}",         # jsmith@
            f"{f}@{domain}",               # john@
            f"{f}{l}@{domain}",            # johnsmith@
            f"{f}_{l}@{domain}",           # john_smith@
            f"{f[0]}.{l}@{domain}",        # j.smith@
            f"{l}.{f}@{domain}",           # smith.john@
            f"{l}{f[0]}@{domain}",         # smithj@
            f"{l}@{domain}",              # smith@
            f"{f}{l[0]}@{domain}",         # johns@
        ]
        return candidates

    def _get_mx(self, domain: str) -> List[str]:
        """Resolve MX records for domain. Cached."""
        if domain in self._mx_cache:
            return self._mx_cache[domain]

        mx_hosts = []
        if HAS_DNS:
            try:
                answers = dns.resolver.resolve(domain, "MX")
                mx_hosts = sorted(
                    [(r.preference, str(r.exchange).rstrip(".")) for r in answers],
                    key=lambda x: x[0],
                )
                mx_hosts = [h[1] for h in mx_hosts]
            except Exception:
                pass

        if not mx_hosts:
            # Fallback: try domain itself as MX
            try:
                socket.getaddrinfo(domain, 25, socket.AF_INET)
                mx_hosts = [domain]
            except Exception:
                pass

        self._mx_cache[domain] = mx_hosts
        return mx_hosts

    def _smtp_verify(self, email: str, mx_host: str) -> Optional[bool]:
        """
        SMTP RCPT TO verification.

        Returns:
            True  = mailbox exists (250 response)
            False = mailbox does not exist (550/553)
            None  = inconclusive (timeout, blocked, greylisting)
        """
        try:
            smtp = smtplib.SMTP(timeout=10)
            smtp.connect(mx_host, 25)
            smtp.helo("verify.local")
            smtp.mail("check@verify.local")
            code, _msg = smtp.rcpt(email)
            smtp.quit()

            if code == 250:
                return True
            elif code in (550, 551, 552, 553, 554):
                return False
            else:
                return None
        except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError,
                smtplib.SMTPResponseException, socket.timeout,
                ConnectionRefusedError, OSError):
            return None

    def _check_catchall(self, domain: str, mx_host: str) -> Optional[bool]:
        """
        Check if domain is a catch-all (accepts all addresses).
        If catch-all, SMTP verification is unreliable.
        """
        if domain in self._catchall_cache:
            return self._catchall_cache[domain]

        # Send a random impossible address
        random_addr = f"zxqj7k9m2_{int(time.time())}@{domain}"
        result = self._smtp_verify(random_addr, mx_host)
        is_catchall = result is True
        self._catchall_cache[domain] = is_catchall
        return is_catchall

    def detect_email_provider(self, domain: str) -> Optional[str]:
        """Detect the email provider from MX records."""
        mx_hosts = self._get_mx(domain)
        if not mx_hosts:
            return None

        mx_str = " ".join(mx_hosts).lower()
        providers = {
            "Google Workspace": ["google", "gmail", "googlemail"],
            "Microsoft 365": ["outlook", "microsoft", "hotmail"],
            "Zoho": ["zoho"],
            "ProtonMail": ["protonmail"],
            "Fastmail": ["fastmail"],
            "Rackspace": ["emailsrvr.com"],
            "GoDaddy": ["secureserver.net"],
            "Namecheap": ["privateemail.com"],
            "iCloud": ["icloud.com"],
        }
        for provider, keywords in providers.items():
            if any(kw in mx_str for kw in keywords):
                return provider
        return "Other"


# ===========================================================================
#  2. COMPANY WEBSITE SCRAPER
# ===========================================================================

class CompanyScraper:
    """
    Scrape company websites for business intelligence.
    Replaces Clay's website enrichment ($0.02/record).

    Extracts: emails, phones, social links, team members, tech stack,
              pricing signals, career pages, and more.
    """

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    # Subpages to crawl in priority order
    PAGES_TO_CRAWL = [
        ("/", "homepage"),
        ("/about", "about"),
        ("/about-us", "about"),
        ("/team", "team"),
        ("/our-team", "team"),
        ("/leadership", "team"),
        ("/management", "team"),
        ("/staff", "team"),
        ("/contact", "contact"),
        ("/contact-us", "contact"),
        ("/careers", "careers"),
        ("/jobs", "careers"),
        ("/join-us", "careers"),
        ("/pricing", "pricing"),
        ("/plans", "pricing"),
        ("/membership", "pricing"),
        ("/services", "services"),
        ("/products", "services"),
    ]

    # Tech stack detection signatures
    TECH_SIGNATURES = {
        "WordPress": ["wp-content", "wp-includes", "wp-json"],
        "Shopify": ["cdn.shopify.com", "shopify.com/s/"],
        "Squarespace": ["squarespace.com", "sqsp.net", "static1.squarespace"],
        "Wix": ["wix.com", "wixstatic.com", "parastorage.com"],
        "Webflow": ["webflow.com", "assets.website-files.com"],
        "GoDaddy Builder": ["godaddysites.com", "secureservercdn.net"],
        "React": ["__NEXT_DATA__", "_next/static", "react-root"],
        "Next.js": ["_next/", "__NEXT_DATA__"],
        "Vue.js": ["__vue__", "vue.min.js", "vue.runtime"],
        "Angular": ["ng-version", "angular.min.js"],
        "Google Analytics": ["google-analytics.com", "gtag(", "UA-", "G-", "ga("],
        "Google Tag Manager": ["googletagmanager.com", "GTM-"],
        "Facebook Pixel": ["connect.facebook.net", "fbq(", "fbevents.js"],
        "HubSpot": ["hubspot.com", "hs-scripts.com", "hbspt.forms"],
        "Salesforce": ["salesforce.com", "pardot.com", "force.com"],
        "Intercom": ["intercom.io", "intercomcdn.com", "widget.intercom.io"],
        "Drift": ["drift.com", "driftt.com", "js.driftt.com"],
        "Zendesk": ["zendesk.com", "zdassets.com"],
        "Freshdesk": ["freshdesk.com", "freshworks.com"],
        "Mailchimp": ["mailchimp.com", "chimpstatic.com", "list-manage.com"],
        "Klaviyo": ["klaviyo.com", "static.klaviyo.com"],
        "Stripe": ["stripe.com", "js.stripe.com"],
        "Square": ["squareup.com", "square.site", "squarecdn.com"],
        "PayPal": ["paypal.com", "paypalobjects.com"],
        "Cloudflare": ["cdnjs.cloudflare.com", "cf-connecting-ip"],
        "AWS": ["amazonaws.com", "aws.amazon.com"],
        "Hotjar": ["hotjar.com", "static.hotjar.com"],
        "Mixpanel": ["mixpanel.com", "mxpnl.com"],
        "Segment": ["segment.com", "cdn.segment.com"],
        "Sentry": ["sentry.io", "browser.sentry-cdn.com"],
        "jQuery": ["jquery.min.js", "jquery.com"],
        "Bootstrap": ["bootstrap.min.css", "bootstrap.min.js"],
        "Tailwind CSS": ["tailwindcss", "tailwind.min.css"],
        "Font Awesome": ["fontawesome", "font-awesome"],
        "reCAPTCHA": ["recaptcha", "google.com/recaptcha"],
        "Calendly": ["calendly.com", "assets.calendly.com"],
        "Typeform": ["typeform.com"],
        "Crisp": ["crisp.chat", "client.crisp.chat"],
        "Tawk.to": ["tawk.to", "embed.tawk.to"],
        "LiveChat": ["livechat.com", "livechatinc.com"],
        "Yoast SEO": ["yoast-schema-graph", "yoast"],
        "WooCommerce": ["woocommerce", "wc-cart"],
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.USER_AGENT})
        self.rate_limiter = RateLimiter(calls_per_second=2.0)

    def scrape_company(self, domain: str) -> Dict[str, Any]:
        """
        Full company intelligence from their website.
        Returns a dict with emails, phones, social_links, tech_stack, people, etc.
        """
        if not domain:
            return {}

        base_url = f"https://{domain}"
        result = {
            "domain": domain,
            "emails": [],
            "phones": [],
            "social_links": {},
            "tech_stack": [],
            "people": [],
            "has_careers_page": False,
            "job_count": 0,
            "has_pricing": False,
            "pricing_signals": [],
            "meta_description": None,
            "meta_title": None,
            "pages_crawled": 0,
            "crawl_errors": 0,
        }

        all_emails = set()
        all_phones = set()

        for path, page_type in self.PAGES_TO_CRAWL:
            self.rate_limiter.wait()
            url = base_url + path
            html = self._fetch_page(url)

            if not html:
                if page_type == "homepage":
                    # Try www subdomain
                    html = self._fetch_page(f"https://www.{domain}" + path)
                    if not html:
                        # Try HTTP fallback
                        html = self._fetch_page(f"http://{domain}" + path)
                    if not html:
                        result["crawl_errors"] += 1
                        continue
                else:
                    continue

            result["pages_crawled"] += 1

            # Extract emails from every page
            page_emails = self._extract_emails(html)
            all_emails.update(page_emails)

            # Extract phones from every page
            page_phones = self._extract_phones(html)
            all_phones.update(page_phones)

            # Page-type-specific extraction
            if page_type == "homepage":
                result["social_links"] = self._extract_social_links(html)
                result["tech_stack"] = self._detect_tech(html)
                result["meta_description"] = self._extract_meta(html, "description")
                result["meta_title"] = self._extract_meta(html, "title")

            elif page_type == "team" and HAS_BS4:
                people = self._extract_people(html)
                result["people"].extend(people)

            elif page_type == "careers":
                result["has_careers_page"] = True
                if HAS_BS4:
                    result["job_count"] = max(
                        result["job_count"], self._count_jobs(html)
                    )

            elif page_type == "pricing":
                result["has_pricing"] = True
                prices = self._extract_pricing(html)
                if prices:
                    result["pricing_signals"].extend(prices)

            elif page_type == "contact":
                # Contact pages often have the best email/phone data
                pass  # Already extracted above

        # Deduplicate and filter
        result["emails"] = sorted(self._filter_emails(all_emails, domain))
        result["phones"] = sorted(all_phones)
        result["pricing_signals"] = result["pricing_signals"][:15]

        # Deduplicate people by name
        seen_names = set()
        unique_people = []
        for person in result["people"]:
            name = person.get("name", "").strip().lower()
            if name and name not in seen_names:
                seen_names.add(name)
                unique_people.append(person)
        result["people"] = unique_people[:20]

        return result

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with timeout, redirect following, and error handling."""
        try:
            r = self.session.get(
                url,
                timeout=12,
                allow_redirects=True,
                stream=True,
            )
            if r.status_code != 200:
                return None
            # Cap at 500KB to avoid huge pages
            content = r.text[:500_000]
            return content
        except (requests.RequestException, ConnectionError, TimeoutError):
            return None

    def _extract_emails(self, html: str) -> set:
        if not html:
            return set()
        pattern = r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
        raw = re.findall(pattern, html)
        return {e.lower() for e in raw}

    def _filter_emails(self, emails: set, domain: str) -> List[str]:
        """Filter out junk emails, image filenames, etc."""
        junk_domains = {
            "example.com", "email.com", "domain.com", "test.com",
            "sentry.io", "wixpress.com", "w3.org", "schema.org",
            "googleapis.com", "googleusercontent.com", "gstatic.com",
            "jquery.com", "jsdelivr.net", "cloudflare.com",
        }
        junk_extensions = {".png", ".jpg", ".gif", ".svg", ".webp", ".css", ".js"}

        filtered = []
        for email in emails:
            email_domain = email.split("@")[1]
            # Skip junk domains
            if email_domain in junk_domains:
                continue
            # Skip image/asset filenames parsed as emails
            if any(email.endswith(ext) for ext in junk_extensions):
                continue
            # Skip very long addresses (likely parsing artifacts)
            if len(email) > 80:
                continue
            # Prioritize same-domain emails
            filtered.append(email)

        # Sort: same-domain first, then others
        same = [e for e in filtered if domain in e.split("@")[1]]
        other = [e for e in filtered if domain not in e.split("@")[1]]
        return same + other

    def _extract_phones(self, html: str) -> set:
        if not html:
            return set()
        # Strip HTML tags first for cleaner matching
        text = re.sub(r"<[^>]+>", " ", html)
        # US phone patterns
        patterns = re.findall(
            r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]\d{4}", text
        )
        # Clean and deduplicate
        cleaned = set()
        for p in patterns:
            digits = re.sub(r"\D", "", p)
            if len(digits) == 10:
                cleaned.add(f"({digits[:3]}) {digits[3:6]}-{digits[6:]}")
            elif len(digits) == 11 and digits[0] == "1":
                digits = digits[1:]
                cleaned.add(f"({digits[:3]}) {digits[3:6]}-{digits[6:]}")
        return cleaned

    def _extract_social_links(self, html: str) -> Dict[str, str]:
        if not html:
            return {}
        links = {}
        patterns = {
            "linkedin": r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[a-zA-Z0-9\-_]+/?',
            "facebook": r'https?://(?:www\.)?facebook\.com/[a-zA-Z0-9.\-]+/?',
            "twitter": r'https?://(?:www\.)?(?:twitter|x)\.com/[a-zA-Z0-9_]+/?',
            "instagram": r'https?://(?:www\.)?instagram\.com/[a-zA-Z0-9._]+/?',
            "youtube": r'https?://(?:www\.)?youtube\.com/(?:c/|channel/|@)[a-zA-Z0-9\-_]+/?',
            "tiktok": r'https?://(?:www\.)?tiktok\.com/@[a-zA-Z0-9._]+/?',
            "yelp": r'https?://(?:www\.)?yelp\.com/biz/[a-zA-Z0-9\-]+/?',
            "bbb": r'https?://(?:www\.)?bbb\.org/[a-zA-Z0-9/\-]+/?',
        }
        for platform, pattern in patterns.items():
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                links[platform] = matches[0]
        return links

    def _extract_people(self, html: str) -> List[Dict[str, str]]:
        """Extract people names and titles from team/leadership pages."""
        if not html or not HAS_BS4:
            return []
        soup = BeautifulSoup(html, "html.parser")
        people = []

        # Strategy 1: Look for team member cards by class name
        team_classes = [
            "team", "member", "staff", "leader", "executive", "bio",
            "person", "employee", "director", "management",
        ]
        for card in soup.find_all(
            ["div", "li", "article", "section"],
            class_=lambda c: c and any(
                x in str(c).lower() for x in team_classes
            ),
        ):
            name_el = card.find(["h2", "h3", "h4", "h5", "strong", "b"])
            title_el = card.find(
                ["p", "span", "div", "small"],
                class_=lambda c: c and any(
                    x in str(c).lower()
                    for x in ["title", "role", "position", "job", "designation"]
                ),
            )
            if name_el:
                name_text = name_el.get_text(strip=True)
                # Basic validation: should look like a name (2-5 words, no URLs)
                if (
                    2 <= len(name_text.split()) <= 5
                    and "@" not in name_text
                    and "http" not in name_text
                    and len(name_text) < 60
                ):
                    person = {"name": name_text}
                    if title_el:
                        title_text = title_el.get_text(strip=True)
                        if len(title_text) < 100:
                            person["title"] = title_text
                    people.append(person)

        # Strategy 2: Look for structured data (JSON-LD)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    data = data[0] if data else {}
                if data.get("@type") == "Organization":
                    members = data.get("employee", data.get("member", []))
                    if isinstance(members, dict):
                        members = [members]
                    for m in members:
                        if isinstance(m, dict) and m.get("name"):
                            person = {"name": m["name"]}
                            if m.get("jobTitle"):
                                person["title"] = m["jobTitle"]
                            people.append(person)
            except (json.JSONDecodeError, TypeError):
                pass

        return people

    def _detect_tech(self, html: str) -> List[str]:
        """Detect technology stack from HTML source."""
        if not html:
            return []
        html_lower = html.lower()
        detected = []
        for tech_name, signatures in self.TECH_SIGNATURES.items():
            if any(sig.lower() in html_lower for sig in signatures):
                detected.append(tech_name)
        return sorted(detected)

    def _count_jobs(self, html: str) -> int:
        if not html or not HAS_BS4:
            return 0
        soup = BeautifulSoup(html, "html.parser")
        job_keywords = ["job", "position", "opening", "role", "career", "vacancy"]
        jobs = soup.find_all(
            ["li", "div", "tr", "article", "a"],
            class_=lambda c: c and any(
                x in str(c).lower() for x in job_keywords
            ),
        )
        return len(jobs) if jobs else 0

    def _extract_pricing(self, html: str) -> List[str]:
        if not html:
            return []
        text = re.sub(r"<[^>]+>", " ", html)
        prices = re.findall(
            r"\$[\d,]+(?:\.\d{2})?(?:\s*/?\s*(?:mo|month|year|yr|annually|weekly|day|wash|visit))?",
            text, re.IGNORECASE,
        )
        return list(dict.fromkeys(prices))[:15]  # Deduplicate, keep order

    def _extract_meta(self, html: str, name: str) -> Optional[str]:
        """Extract meta tag content."""
        if not html:
            return None
        if name == "title":
            match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
            return match.group(1).strip() if match else None
        match = re.search(
            rf'<meta\s+(?:name|property)=["\'](?:og:)?{name}["\']\s+content=["\']([^"\']+)',
            html,
            re.IGNORECASE,
        )
        return match.group(1).strip() if match else None


# ===========================================================================
#  3. GOOGLE MAPS SCRAPER (via Apify)
# ===========================================================================

class GoogleMapsScraper:
    """
    Google Maps business data via Apify actors.
    Cost: ~$2.50 per 1,000 results (Apify compute).
    """

    ACTOR_ID = "nwua9Gu5YrADL7ZDj"  # compass/crawler-google-places

    def __init__(self, apify_token: str):
        self.token = apify_token
        self.base_url = "https://api.apify.com/v2"
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(calls_per_second=0.5)

    def search_businesses(self, query: str, location: str,
                          max_results: int = 20) -> List[Dict]:
        """Search Google Maps. Returns list of business dicts."""
        self.rate_limiter.wait()
        input_data = {
            "searchStringsArray": [query],
            "locationQuery": location,
            "maxCrawledPlacesPerSearch": max_results,
            "language": "en",
            "maxReviews": 0,
            "maxImages": 0,
        }
        return self._run_actor(self.ACTOR_ID, input_data)

    def search_nearby(self, lat: float, lng: float, query: str = "car wash",
                      radius_m: int = 5000, max_results: int = 20) -> List[Dict]:
        """Search near a coordinate."""
        self.rate_limiter.wait()
        input_data = {
            "searchStringsArray": [query],
            "lat": str(lat),
            "lng": str(lng),
            "maxCrawledPlacesPerSearch": max_results,
            "zoom": 14,
            "language": "en",
            "maxReviews": 0,
            "maxImages": 0,
        }
        return self._run_actor(self.ACTOR_ID, input_data)

    def _run_actor(self, actor_id: str, input_data: dict,
                   timeout: int = 120) -> List[Dict]:
        """Run an Apify actor synchronously and return results."""
        url = f"{self.base_url}/acts/{actor_id}/run-sync-get-dataset-items"
        params = {"token": self.token, "timeout": timeout}
        try:
            r = self.session.post(
                url, json=input_data, params=params,
                timeout=timeout + 30,
            )
            if r.status_code in (200, 201):
                return r.json() if isinstance(r.json(), list) else []
        except Exception as e:
            log.warning(f"Apify actor error: {e}")
        return []


# ===========================================================================
#  4. LINKEDIN SCRAPER (via Apify)
# ===========================================================================

class LinkedInScraper:
    """
    LinkedIn data via Apify actors.
    Cost: Apify free tier covers most use (compute only).
    """

    COMPANY_ACTOR = "anchor/linkedin-company-scraper"
    EMPLOYEES_ACTOR = "george.the.developer/linkedin-company-employees-scraper"
    PROFILE_ACTOR = "anchor/linkedin-people-profile-scraper"

    def __init__(self, apify_token: str):
        self.token = apify_token
        self.base_url = "https://api.apify.com/v2"
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(calls_per_second=0.3)

    def get_company_profile(self, linkedin_url: str) -> Optional[Dict]:
        """Get company info: employee count, description, industry."""
        self.rate_limiter.wait()
        input_data = {
            "urls": [linkedin_url],
            "deepScrape": False,
        }
        results = self._run_actor(self.COMPANY_ACTOR, input_data, timeout=90)
        return results[0] if results else None

    def get_company_employees(self, linkedin_url: str,
                              seniority_filter: list = None,
                              max_results: int = 25) -> List[Dict]:
        """Get employees from a company's LinkedIn page."""
        self.rate_limiter.wait()
        input_data = {
            "companies": [linkedin_url],
            "maxEmployees": max_results,
            "profileDepth": "short",
        }
        if seniority_filter:
            input_data["seniorityFilter"] = seniority_filter
        return self._run_actor(self.EMPLOYEES_ACTOR, input_data, timeout=180)

    def get_person_profile(self, linkedin_url: str) -> Optional[Dict]:
        """Get person profile details."""
        self.rate_limiter.wait()
        input_data = {"profileUrls": [linkedin_url]}
        results = self._run_actor(self.PROFILE_ACTOR, input_data, timeout=60)
        return results[0] if results else None

    def _run_actor(self, actor_id: str, input_data: dict,
                   timeout: int = 120) -> List[Dict]:
        url = f"{self.base_url}/acts/{actor_id}/run-sync-get-dataset-items"
        params = {"token": self.token, "timeout": timeout}
        try:
            r = self.session.post(
                url, json=input_data, params=params,
                timeout=timeout + 30,
            )
            if r.status_code in (200, 201):
                data = r.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            log.warning(f"LinkedIn scraper error: {e}")
        return []


# ===========================================================================
#  5. REVIEW SCRAPERS (Yelp + BBB via Apify)
# ===========================================================================

class ReviewScraper:
    """
    Scrape Yelp and BBB for ratings, reviews, complaints.
    Cost: Apify free tier (compute only).
    """

    YELP_ACTOR = "lanky_quantifier/yelp-business-scraper"
    BBB_ACTOR = "curious_coder/bbb-scraper"

    def __init__(self, apify_token: str):
        self.token = apify_token
        self.base_url = "https://api.apify.com/v2"
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(calls_per_second=0.3)

    def get_yelp_data(self, business_name: str, city: str,
                      state: str) -> Optional[Dict]:
        """Get Yelp rating, reviews, categories, price level."""
        self.rate_limiter.wait()
        search_term = f"{business_name} {city} {state}".strip()
        input_data = {
            "searchTerms": [search_term],
            "maxResults": 3,
            "reviewsPerBusiness": 0,
        }
        try:
            results = self._run_actor(self.YELP_ACTOR, input_data, timeout=60)
            if results:
                # Return best match
                name_lower = business_name.lower()
                for r in results:
                    if name_lower in (r.get("name", "")).lower():
                        return r
                return results[0]
        except Exception as e:
            log.debug(f"Yelp scraper error: {e}")
        return None

    def get_bbb_data(self, business_name: str,
                     state: str) -> Optional[Dict]:
        """Get BBB rating, complaints, accreditation status."""
        self.rate_limiter.wait()
        input_data = {
            "searchTerms": [business_name],
            "location": state,
            "maxResults": 3,
        }
        try:
            results = self._run_actor(self.BBB_ACTOR, input_data, timeout=60)
            if results:
                return results[0]
        except Exception as e:
            log.debug(f"BBB scraper error: {e}")
        return None

    def _run_actor(self, actor_id: str, input_data: dict,
                   timeout: int = 60) -> List[Dict]:
        url = f"{self.base_url}/acts/{actor_id}/run-sync-get-dataset-items"
        params = {"token": self.token, "timeout": timeout}
        try:
            r = self.session.post(
                url, json=input_data, params=params,
                timeout=timeout + 30,
            )
            if r.status_code in (200, 201):
                data = r.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            log.warning(f"Review scraper error: {e}")
        return []


# ===========================================================================
#  6. DOMAIN / WHOIS INTELLIGENCE
# ===========================================================================

class DomainIntel:
    """
    Free domain intelligence: WHOIS, DNS, SSL analysis.
    No API key required. Zero cost.
    """

    def __init__(self):
        self._cache: Dict[str, Dict] = {}

    def lookup(self, domain: str) -> Dict[str, Any]:
        """Full domain intel: WHOIS + DNS + SSL."""
        if domain in self._cache:
            return self._cache[domain]

        result = {"domain": domain}
        result["whois"] = self._whois_lookup(domain)
        result["dns"] = self._dns_lookup(domain)
        result["ssl"] = self._ssl_info(domain)
        result["age_years"] = self._domain_age(result["whois"])
        result["hosting_provider"] = self._detect_hosting(result["dns"])

        self._cache[domain] = result
        return result

    def _whois_lookup(self, domain: str) -> Dict:
        if not HAS_WHOIS:
            return {}
        try:
            w = whois.whois(domain)
            creation = w.creation_date
            expiration = w.expiration_date
            # Handle lists (some registrars return multiple dates)
            if isinstance(creation, list):
                creation = creation[0]
            if isinstance(expiration, list):
                expiration = expiration[0]

            return {
                "registrar": w.registrar,
                "creation_date": str(creation) if creation else None,
                "expiration_date": str(expiration) if expiration else None,
                "name": w.name if hasattr(w, "name") else None,
                "org": w.org if hasattr(w, "org") else None,
                "state": w.state if hasattr(w, "state") else None,
                "country": w.country if hasattr(w, "country") else None,
                "name_servers": list(w.name_servers) if w.name_servers else [],
            }
        except Exception:
            return {}

    def _dns_lookup(self, domain: str) -> Dict:
        if not HAS_DNS:
            return {}
        result = {}
        # MX records
        try:
            mx = dns.resolver.resolve(domain, "MX")
            result["mx_records"] = sorted(
                [str(r.exchange).rstrip(".") for r in mx]
            )
            mx_str = " ".join(result["mx_records"]).lower()
            if "google" in mx_str or "gmail" in mx_str:
                result["email_provider"] = "Google Workspace"
            elif "outlook" in mx_str or "microsoft" in mx_str:
                result["email_provider"] = "Microsoft 365"
            elif "zoho" in mx_str:
                result["email_provider"] = "Zoho"
            elif "protonmail" in mx_str:
                result["email_provider"] = "ProtonMail"
            else:
                result["email_provider"] = "Other"
        except Exception:
            pass

        # A records (IP)
        try:
            a = dns.resolver.resolve(domain, "A")
            result["ip_addresses"] = [str(r) for r in a]
        except Exception:
            pass

        # TXT records (SPF, DMARC, verification)
        try:
            txt = dns.resolver.resolve(domain, "TXT")
            txt_records = [str(r).strip('"') for r in txt]
            result["has_spf"] = any("v=spf1" in t for t in txt_records)
            result["has_dmarc"] = False
            result["verification_providers"] = []

            verification_sigs = {
                "google-site-verification": "Google",
                "facebook-domain-verification": "Facebook",
                "ms=": "Microsoft",
                "hubspot": "HubSpot",
                "pardot": "Pardot",
                "mailchimp": "Mailchimp",
                "atlassian-domain-verification": "Atlassian",
                "adobe-idp-site-verification": "Adobe",
            }
            for t in txt_records:
                t_lower = t.lower()
                for sig, provider in verification_sigs.items():
                    if sig in t_lower:
                        result["verification_providers"].append(provider)
        except Exception:
            pass

        # DMARC
        try:
            dmarc = dns.resolver.resolve(f"_dmarc.{domain}", "TXT")
            result["has_dmarc"] = True
        except Exception:
            pass

        return result

    def _ssl_info(self, domain: str) -> Dict:
        try:
            ctx = ssl.create_default_context()
            with ctx.wrap_socket(socket.socket(), server_hostname=domain) as s:
                s.settimeout(10)
                s.connect((domain, 443))
                cert = s.getpeercert()
                issuer_dict = {}
                for item in cert.get("issuer", ()):
                    for k, v in item:
                        issuer_dict[k] = v
                subject_dict = {}
                for item in cert.get("subject", ()):
                    for k, v in item:
                        subject_dict[k] = v
                return {
                    "issuer": issuer_dict.get("organizationName"),
                    "subject_org": subject_dict.get("organizationName"),
                    "not_after": cert.get("notAfter"),
                    "not_before": cert.get("notBefore"),
                    "san_count": len(cert.get("subjectAltName", ())),
                }
        except Exception:
            return {}

    def _domain_age(self, whois_data: dict) -> Optional[int]:
        creation = whois_data.get("creation_date")
        if not creation:
            return None
        try:
            if isinstance(creation, str):
                # Try multiple date formats
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d", "%d-%b-%Y"):
                    try:
                        creation = datetime.strptime(creation.split("+")[0].strip(), fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return None
            if isinstance(creation, datetime):
                return (datetime.now() - creation).days // 365
        except Exception:
            pass
        return None

    def _detect_hosting(self, dns_data: dict) -> Optional[str]:
        """Detect hosting provider from IP/nameservers."""
        ips = dns_data.get("ip_addresses", [])
        if not ips:
            return None
        # This is a simplified check; production would use ASN lookup
        ip = ips[0]
        # Common hosting IP ranges (simplified)
        hosting_hints = {
            "cloudflare": ["104.16.", "104.17.", "104.18.", "104.19.",
                           "104.20.", "104.21.", "172.67."],
            "aws": ["52.", "54.", "18.", "3."],
            "google_cloud": ["34.", "35."],
            "godaddy": ["148.72.", "50.63."],
        }
        for provider, prefixes in hosting_hints.items():
            if any(ip.startswith(p) for p in prefixes):
                return provider
        return None


# ===========================================================================
#  7. SEC EDGAR
# ===========================================================================

class SECEdgar:
    """
    SEC EDGAR EFTS (full-text search) + filing detail.
    Completely free, no API key needed. Requires User-Agent header.
    """

    SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
    FULL_TEXT_URL = "https://efts.sec.gov/LATEST/search-index"
    EDGAR_BASE = "https://www.sec.gov/cgi-bin/browse-edgar"
    HEADERS = {"User-Agent": "CarwashIntel/1.0 research@carwashintel.com"}

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.rate_limiter = RateLimiter(calls_per_second=5.0)  # SEC allows 10/sec

    def search_company(self, company_name: str,
                       forms: str = "D,D/A") -> Dict:
        """Search EDGAR for company filings."""
        self.rate_limiter.wait()
        try:
            r = self.session.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={
                    "q": f'"{company_name}"',
                    "forms": forms,
                    "dateRange": "custom",
                    "startdt": "2020-01-01",
                },
                timeout=15,
            )
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.debug(f"SEC EDGAR search error: {e}")
        return {"hits": {"total": {"value": 0}, "hits": []}}

    def search_full_text(self, query: str, forms: str = None,
                         start_date: str = None,
                         end_date: str = None) -> Dict:
        """Full text search across all EDGAR filings."""
        self.rate_limiter.wait()
        params = {"q": query}
        if forms:
            params["forms"] = forms
        if start_date:
            params["dateRange"] = "custom"
            params["startdt"] = start_date
        if end_date:
            params["enddt"] = end_date
        try:
            r = self.session.get(
                "https://efts.sec.gov/LATEST/search-index",
                params=params,
                timeout=15,
            )
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.debug(f"SEC EDGAR full text error: {e}")
        return {"hits": {"total": {"value": 0}, "hits": []}}

    def company_search_by_name(self, company_name: str) -> List[Dict]:
        """Search EDGAR company database by name (returns CIK, name, etc)."""
        self.rate_limiter.wait()
        try:
            r = self.session.get(
                "https://www.sec.gov/cgi-bin/browse-edgar",
                params={
                    "company": company_name,
                    "CIK": "",
                    "type": "",
                    "dateb": "",
                    "owner": "include",
                    "count": "10",
                    "search_text": "",
                    "action": "getcompany",
                    "output": "atom",
                },
                timeout=15,
            )
            if r.status_code == 200:
                # Parse Atom XML response
                entries = []
                for match in re.finditer(
                    r"<CIK>(\d+)</CIK>.*?<company-name>([^<]+)</company-name>",
                    r.text,
                    re.DOTALL,
                ):
                    entries.append({
                        "cik": match.group(1),
                        "company_name": match.group(2),
                    })
                return entries
        except Exception as e:
            log.debug(f"SEC company search error: {e}")
        return []


# ===========================================================================
#  8. JOB POSTINGS SCRAPER
# ===========================================================================

class JobPostingScraper:
    """
    Scrape job postings as a growth/hiring signal.
    Uses Apify Indeed scraper.
    """

    INDEED_ACTOR = "misceres/indeed-scraper"

    def __init__(self, apify_token: str):
        self.token = apify_token
        self.base_url = "https://api.apify.com/v2"
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(calls_per_second=0.3)

    def get_job_postings(self, company_name: str,
                         location: str = None,
                         max_results: int = 10) -> List[Dict]:
        """Find active job postings for a company."""
        self.rate_limiter.wait()
        search_query = f'company:"{company_name}"'
        input_data = {
            "position": search_query,
            "location": location or "United States",
            "maxItems": max_results,
        }
        try:
            url = f"{self.base_url}/acts/{self.INDEED_ACTOR}/run-sync-get-dataset-items"
            params = {"token": self.token, "timeout": 60}
            r = self.session.post(
                url, json=input_data, params=params, timeout=90,
            )
            if r.status_code in (200, 201):
                data = r.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            log.debug(f"Indeed scraper error: {e}")
        return []

    def count_openings(self, company_name: str) -> int:
        """Quick count of active job openings."""
        jobs = self.get_job_postings(company_name, max_results=50)
        return len(jobs)


# ===========================================================================
#  9. MASTER ENRICHMENT ENGINE
# ===========================================================================

class EnrichmentEngine:
    """
    Master orchestrator that replaces Clay + Apollo.
    Layers data from every free/cheap source to build comprehensive profiles.

    Layer costs:
        1. Website scraping     - FREE
        2. Domain/WHOIS/DNS     - FREE
        3. Email finding/SMTP   - FREE
        4. SEC EDGAR            - FREE
        5. Google Maps (Apify)  - ~$2.50/1K results
        6. LinkedIn (Apify)     - free compute tier
        7. Yelp/BBB (Apify)     - free compute tier
        8. Job postings (Apify) - free compute tier
    """

    def __init__(self, apify_token: str = None,
                 supabase_url: str = None, supabase_key: str = None,
                 skip_apify: bool = False):
        # Free tools (always available)
        self.email_finder = EmailFinder()
        self.website_scraper = CompanyScraper()
        self.domain_intel = DomainIntel()
        self.sec = SECEdgar()

        # Apify-powered tools (optional)
        self.has_apify = bool(apify_token) and not skip_apify
        if self.has_apify:
            self.gmaps = GoogleMapsScraper(apify_token)
            self.linkedin = LinkedInScraper(apify_token)
            self.reviews = ReviewScraper(apify_token)
            self.jobs = JobPostingScraper(apify_token)
        else:
            self.gmaps = None
            self.linkedin = None
            self.reviews = None
            self.jobs = None

        # Database
        self.db = None
        if supabase_url or supabase_key or SUPABASE_URL:
            self.db = SupabaseClient(supabase_url, supabase_key)

    def enrich_company(self, domain: str = None, name: str = None,
                       address: str = None, city: str = None,
                       state: str = None,
                       layers: List[str] = None) -> Dict[str, Any]:
        """
        Full company enrichment from ALL sources.

        Args:
            domain: Company domain (e.g., "acmecarwash.com")
            name: Company name
            address: Full address string
            city: City name
            state: State abbreviation
            layers: Optional list of layer names to run. Default = all.
                    Options: website, domain_intel, email, sec, gmaps,
                             linkedin, reviews, jobs

        Returns: Dict with all collected data + 'sources' list.
        """
        all_layers = {"website", "domain_intel", "email", "sec",
                      "gmaps", "linkedin", "reviews", "jobs"}
        active_layers = set(layers) if layers else all_layers

        result = {
            "domain": domain,
            "name": name,
            "sources": [],
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }

        # ------------------------------------------------------------------
        # Layer 1: Website Scrape (FREE)
        # ------------------------------------------------------------------
        if "website" in active_layers and domain:
            try:
                log.info(f"  Layer 1: Website scrape -> {domain}")
                ws = self.website_scraper.scrape_company(domain)
                result["website_emails"] = ws.get("emails", [])
                result["website_phones"] = ws.get("phones", [])
                result["social_links"] = ws.get("social_links", {})
                result["tech_stack"] = ws.get("tech_stack", [])
                result["team_members"] = ws.get("people", [])
                result["has_careers_page"] = ws.get("has_careers_page", False)
                result["website_job_count"] = ws.get("job_count", 0)
                result["has_pricing"] = ws.get("has_pricing", False)
                result["pricing_signals"] = ws.get("pricing_signals", [])
                result["meta_description"] = ws.get("meta_description")
                result["meta_title"] = ws.get("meta_title")
                result["pages_crawled"] = ws.get("pages_crawled", 0)
                result["sources"].append("website")
            except Exception as e:
                log.warning(f"  Website scrape failed: {e}")

        # ------------------------------------------------------------------
        # Layer 2: Domain Intelligence (FREE)
        # ------------------------------------------------------------------
        if "domain_intel" in active_layers and domain:
            try:
                log.info(f"  Layer 2: Domain intel -> {domain}")
                di = self.domain_intel.lookup(domain)
                result["domain_age_years"] = di.get("age_years")
                result["email_provider"] = di.get("dns", {}).get("email_provider")
                result["hosting_provider"] = di.get("hosting_provider")
                result["ssl_issuer"] = di.get("ssl", {}).get("issuer")
                result["ssl_org"] = di.get("ssl", {}).get("subject_org")
                result["registrant_org"] = di.get("whois", {}).get("org")
                result["registrant_name"] = di.get("whois", {}).get("name")
                result["registrant_state"] = di.get("whois", {}).get("state")
                result["registrant_country"] = di.get("whois", {}).get("country")
                result["has_spf"] = di.get("dns", {}).get("has_spf")
                result["has_dmarc"] = di.get("dns", {}).get("has_dmarc")
                result["dns_verification_providers"] = di.get("dns", {}).get(
                    "verification_providers", []
                )
                result["sources"].append("domain_intel")
            except Exception as e:
                log.warning(f"  Domain intel failed: {e}")

        # ------------------------------------------------------------------
        # Layer 3: SEC EDGAR (FREE)
        # ------------------------------------------------------------------
        if "sec" in active_layers and name:
            try:
                log.info(f"  Layer 3: SEC EDGAR -> {name}")
                edgar = self.sec.search_company(name)
                total = edgar.get("hits", {}).get("total", {})
                total_val = total.get("value", 0) if isinstance(total, dict) else 0
                if total_val > 0:
                    hits = edgar.get("hits", {}).get("hits", [])[:5]
                    result["sec_filing_count"] = total_val
                    result["sec_filings"] = [
                        {
                            "form_type": h.get("_source", {}).get("form_type"),
                            "filed_at": h.get("_source", {}).get("file_date"),
                            "display_names": h.get("_source", {}).get(
                                "display_names", []
                            ),
                        }
                        for h in hits
                    ]
                    result["sources"].append("sec_edgar")
            except Exception as e:
                log.warning(f"  SEC EDGAR failed: {e}")

        # ------------------------------------------------------------------
        # Layer 4: Google Maps (Apify, ~$2.50/1K)
        # ------------------------------------------------------------------
        if "gmaps" in active_layers and self.gmaps and name:
            try:
                location_str = f"{city}, {state}" if city and state else (
                    address or "United States"
                )
                log.info(f"  Layer 4: Google Maps -> {name} in {location_str}")
                gmaps_data = self.gmaps.search_businesses(
                    name, location_str, max_results=5,
                )
                if gmaps_data:
                    best = self._best_gmaps_match(name, gmaps_data)
                    if best:
                        result["google_rating"] = best.get("totalScore")
                        result["google_reviews"] = best.get("reviewsCount")
                        result["google_phone"] = best.get("phone")
                        result["google_address"] = best.get("address")
                        result["google_url"] = best.get("url")
                        result["google_place_id"] = best.get("placeId")
                        result["google_category"] = best.get("categoryName")
                        result["google_hours"] = best.get("openingHours")
                        result["sources"].append("google_maps")
            except Exception as e:
                log.warning(f"  Google Maps failed: {e}")

        # ------------------------------------------------------------------
        # Layer 5: LinkedIn (Apify, free compute)
        # ------------------------------------------------------------------
        if "linkedin" in active_layers and self.linkedin:
            linkedin_url = result.get("social_links", {}).get("linkedin")
            if linkedin_url:
                try:
                    log.info(f"  Layer 5: LinkedIn -> {linkedin_url}")
                    li = self.linkedin.get_company_profile(linkedin_url)
                    if li:
                        result["linkedin_employees"] = li.get("employeeCount")
                        result["linkedin_industry"] = li.get("industry")
                        result["linkedin_description"] = (
                            li.get("description", "")[:500]
                        )
                        result["linkedin_founded"] = li.get("foundedYear")
                        result["linkedin_specialties"] = li.get("specialties")
                        result["linkedin_company_type"] = li.get("companyType")
                        result["sources"].append("linkedin")
                except Exception as e:
                    log.warning(f"  LinkedIn failed: {e}")

        # ------------------------------------------------------------------
        # Layer 6: Yelp (Apify, free)
        # ------------------------------------------------------------------
        if "reviews" in active_layers and self.reviews and name:
            try:
                city_str = city or (
                    address.split(",")[0].strip() if address else ""
                )
                state_str = state or ""
                if city_str:
                    log.info(f"  Layer 6: Yelp -> {name} in {city_str}")
                    yelp = self.reviews.get_yelp_data(name, city_str, state_str)
                    if yelp:
                        result["yelp_rating"] = yelp.get("rating")
                        result["yelp_reviews"] = yelp.get("reviewCount")
                        result["yelp_price"] = yelp.get("priceRange")
                        result["yelp_categories"] = yelp.get("categories")
                        result["sources"].append("yelp")
            except Exception as e:
                log.warning(f"  Yelp failed: {e}")

        # ------------------------------------------------------------------
        # Layer 7: Job Postings (Apify, free)
        # ------------------------------------------------------------------
        if "jobs" in active_layers and self.jobs and name:
            try:
                log.info(f"  Layer 7: Job postings -> {name}")
                job_list = self.jobs.get_job_postings(name, max_results=5)
                if job_list:
                    result["active_job_postings"] = len(job_list)
                    result["job_titles"] = [
                        j.get("positionName", j.get("title", ""))
                        for j in job_list[:5]
                    ]
                    result["sources"].append("job_postings")
            except Exception as e:
                log.warning(f"  Job postings failed: {e}")

        return result

    def enrich_person(self, first_name: str, last_name: str,
                      domain: str = None,
                      company_name: str = None,
                      linkedin_url: str = None) -> Dict[str, Any]:
        """
        Full person enrichment.

        Returns:
            Dict with email, confidence, linkedin data, etc.
        """
        result = {
            "first_name": first_name,
            "last_name": last_name,
            "domain": domain,
            "company_name": company_name,
            "sources": [],
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }

        # Email finding
        if domain:
            log.info(
                f"  Email find: {first_name} {last_name} @ {domain}"
            )
            email_result = self.email_finder.find_email(
                first_name, last_name, domain,
            )
            if email_result:
                result["email"] = email_result["email"]
                result["email_confidence"] = email_result["confidence"]
                result["email_method"] = email_result["method"]
                result["email_is_catchall"] = email_result.get("is_catchall")
                result["sources"].append("email_finder")

        # LinkedIn profile enrichment
        if linkedin_url and self.linkedin:
            try:
                log.info(f"  LinkedIn profile: {linkedin_url}")
                li = self.linkedin.get_person_profile(linkedin_url)
                if li:
                    result["linkedin_headline"] = li.get("headline")
                    result["linkedin_summary"] = (
                        li.get("summary", "")[:500]
                    )
                    result["linkedin_location"] = li.get("location")
                    result["linkedin_connections"] = li.get("connectionsCount")
                    result["sources"].append("linkedin")
            except Exception as e:
                log.warning(f"  LinkedIn profile failed: {e}")

        return result

    def _best_gmaps_match(self, name: str,
                          results: List[Dict]) -> Optional[Dict]:
        """Find best Google Maps match by name similarity."""
        name_lower = re.sub(
            r"[^a-z0-9\s]", "", name.lower(),
        ).strip()
        name_words = set(name_lower.split())

        best = None
        best_score = 0

        for item in results:
            item_name = re.sub(
                r"[^a-z0-9\s]", "", (item.get("title", "") or item.get("name", "")).lower(),
            ).strip()
            item_words = set(item_name.split())

            # Jaccard similarity
            if not name_words or not item_words:
                continue
            intersection = name_words & item_words
            union = name_words | item_words
            score = len(intersection) / len(union)

            # Substring match bonus
            if name_lower in item_name or item_name in name_lower:
                score += 0.5

            if score > best_score:
                best_score = score
                best = item

        # Require minimum threshold
        if best_score >= 0.3:
            return best
        # Fallback to first result if only one
        if len(results) == 1:
            return results[0]
        return None

    # ------------------------------------------------------------------
    # Supabase Pipeline Methods
    # ------------------------------------------------------------------

    def write_company_enrichment(self, business_id: str,
                                 enrichment: Dict) -> bool:
        """Write enrichment results back to Supabase businesses table."""
        if not self.db:
            log.warning("No Supabase connection for write")
            return False

        updates = {"updated_at": datetime.now(timezone.utc).isoformat()}

        # Map enrichment fields -> businesses table columns
        field_map = {
            "google_rating": "google_rating",
            "google_reviews": "google_review_count",
            "yelp_rating": "yelp_rating",
            "yelp_reviews": "yelp_review_count",
            "linkedin_employees": "employee_count",
        }
        for src_key, db_key in field_map.items():
            val = enrichment.get(src_key)
            if val is not None:
                updates[db_key] = val

        # Emails: pick best one for the main email field (filter junk)
        JUNK_PATTERNS = [
            "sentry", "wixpress", "your@", "test@", "example@",
            "noreply@", "no-reply@", "donotreply@", "do-not-reply@",
            "admin@", "webmaster@", "postmaster@", "root@",
            "abuse@", "mailer-daemon", "mail.com", "@sentry.",
            "placeholder", "changeme", "temp@", "null@",
        ]
        emails = enrichment.get("website_emails", [])
        emails = [e for e in emails if not any(j in e.lower() for j in JUNK_PATTERNS)]
        if emails:
            updates.setdefault("email", emails[0])

        # Phones: pick first
        phones = enrichment.get("website_phones", [])
        if phones:
            updates.setdefault("phone", phones[0])

        # Website
        if enrichment.get("domain"):
            updates.setdefault("website", f"https://{enrichment['domain']}")

        # Year established
        domain_age = enrichment.get("domain_age_years")
        if domain_age and domain_age > 0:
            updates["year_established"] = datetime.now().year - domain_age

        # Mark enrichment status
        sources = enrichment.get("sources", [])
        updates["enrichment_status"] = "enriched" if sources else "attempted"
        updates["enrichment_date"] = datetime.now(timezone.utc).isoformat()
        updates["data_source"] = (
            updates.get("data_source", "") + "," + ",".join(sources)
        ).strip(",")

        success = self.db.update("businesses", "business_id", business_id, updates)

        # Write rich data to industry_details
        if sources:
            detail_data = {}
            for key in [
                "tech_stack", "social_links", "team_members", "pricing_signals",
                "has_careers_page", "website_job_count", "has_pricing",
                "email_provider", "hosting_provider", "ssl_org",
                "registrant_org", "domain_age_years", "sec_filing_count",
                "sec_filings", "google_category", "google_hours",
                "linkedin_industry", "linkedin_description",
                "linkedin_founded", "linkedin_specialties",
                "yelp_categories", "yelp_price",
                "active_job_postings", "job_titles",
                "dns_verification_providers", "meta_description",
            ]:
                val = enrichment.get(key)
                if val is not None and val != [] and val != {}:
                    detail_data[key] = val

            if detail_data:
                # Check if industry_details row exists
                existing = self.db.fetch(
                    "industry_details",
                    filters=f"business_id=eq.{business_id}",
                    select="id,details",
                    limit=1,
                )
                if existing:
                    old_details = existing[0].get("details", {})
                    old_details.update(detail_data)
                    self.db.update(
                        "industry_details", "id", existing[0]["id"],
                        {
                            "details": old_details,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                else:
                    biz_rows = self.db.fetch(
                        "businesses",
                        filters=f"business_id=eq.{business_id}",
                        select="business_type",
                        limit=1,
                    )
                    industry = (
                        biz_rows[0].get("business_type", "unknown")
                        if biz_rows else "unknown"
                    )
                    self.db.insert("industry_details", [{
                        "business_id": business_id,
                        "industry": industry,
                        "details": detail_data,
                    }])

        return success

    def write_person_enrichment(self, enrichment: Dict,
                                business_id: str = None) -> Optional[str]:
        """Write person enrichment to people table and link to business."""
        if not self.db:
            return None

        person_data = {
            "full_name": (
                f"{enrichment.get('first_name', '')} "
                f"{enrichment.get('last_name', '')}"
            ).strip(),
            "first_name": enrichment.get("first_name"),
            "last_name": enrichment.get("last_name"),
            "email": enrichment.get("email"),
            "email_status": enrichment.get("email_confidence"),
            "company_name": enrichment.get("company_name"),
            "company_domain": enrichment.get("domain"),
            "source": "enrichment_engine",
            "source_type": ",".join(enrichment.get("sources", [])),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        # LinkedIn data
        if enrichment.get("linkedin_headline"):
            person_data["title"] = enrichment["linkedin_headline"]
        if enrichment.get("linkedin_location"):
            parts = enrichment["linkedin_location"].split(",")
            if len(parts) >= 2:
                person_data["city"] = parts[0].strip()
                person_data["state"] = parts[-1].strip()

        # Remove None values
        person_data = {k: v for k, v in person_data.items() if v is not None}

        inserted = self.db.insert("people", [person_data])
        if inserted and len(inserted) > 0:
            person_id = inserted[0].get("person_id")
            if person_id and business_id:
                self.db.insert("business_people", [{
                    "business_id": business_id,
                    "person_id": person_id,
                    "role": "owner",
                    "is_primary": True,
                }])
            return person_id
        return None


# ===========================================================================
#  CLI INTERFACE
# ===========================================================================

def extract_domain_from_url(url: str) -> str:
    """Extract bare domain from URL."""
    if not url:
        return ""
    url = url.lower().strip()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    return url.split("/")[0].split("?")[0].split("#")[0]


def single_company_enrichment(args):
    """Enrich a single company from CLI."""
    engine = EnrichmentEngine(
        apify_token=APIFY_API_TOKEN or None,
        skip_apify=args.skip_apify,
    )

    domain = args.domain
    if not domain and args.website:
        domain = extract_domain_from_url(args.website)

    result = engine.enrich_company(
        domain=domain,
        name=args.name,
        address=args.address,
        city=args.city,
        state=args.state,
    )

    print(json.dumps(result, indent=2, default=str))
    log.info(f"Sources used: {result.get('sources', [])}")


def single_person_enrichment(args):
    """Enrich a single person from CLI."""
    engine = EnrichmentEngine(
        apify_token=APIFY_API_TOKEN or None,
        skip_apify=args.skip_apify,
    )

    result = engine.enrich_person(
        first_name=args.first_name,
        last_name=args.last_name,
        domain=args.domain,
        company_name=args.company,
    )

    print(json.dumps(result, indent=2, default=str))


def batch_enrichment(args):
    """Batch enrich businesses from Supabase."""
    engine = EnrichmentEngine(
        apify_token=APIFY_API_TOKEN or None,
        skip_apify=args.skip_apify,
    )

    if not engine.db:
        log.error("Supabase connection required for batch mode")
        sys.exit(1)

    progress = ProgressTracker(f"enrichment_engine_{args.type}_progress.json")
    if not progress.get("started_at"):
        progress.set("started_at", datetime.now(timezone.utc).isoformat())
        progress.set("total_enriched", 0)
        progress.set("total_skipped", 0)
        progress.set("total_errors", 0)
        progress.set("processed_ids", [])
        progress.save()

    # Build filters
    filters = []
    if args.type:
        filters.append(f"business_type=eq.{args.type}")
    if args.status:
        filters.append(f"enrichment_status=eq.{args.status}")
    else:
        filters.append("enrichment_status=eq.raw")
    if args.state_filter:
        filters.append(f"state=eq.{args.state_filter}")
    filter_str = "&".join(filters)

    # Count total
    total = engine.db.fetch_count("businesses", filter_str)
    log.info(f"Found {total} businesses matching filters")
    log.info(f"Limit: {args.limit}, already processed: {progress.get('total_enriched', 0)}")

    batch_size = min(args.batch_size, 50)
    offset = 0
    enriched = 0
    limit = args.limit

    while enriched < limit:
        batch = engine.db.fetch(
            "businesses",
            filters=filter_str,
            select=(
                "business_id,name,website,phone,email,city,state,zip,"
                "address,owner_name,google_rating,google_review_count,"
                "business_type,sub_type,enrichment_status"
            ),
            order="google_review_count.desc.nullslast",
            limit=batch_size,
            offset=offset,
        )

        if not batch:
            log.info("No more businesses to enrich")
            break

        offset += batch_size

        for biz in batch:
            if enriched >= limit:
                break

            biz_id = biz["business_id"]

            # Skip already processed (resume support)
            if progress.in_set("processed_ids", biz_id):
                progress.inc("total_skipped")
                continue

            name = biz.get("name", "unknown")
            website = biz.get("website", "")
            domain = extract_domain_from_url(website) if website else None

            log.info(
                f"[{enriched + 1}/{limit}] Enriching: {name}"
                f" ({domain or 'no domain'})"
            )

            try:
                result = engine.enrich_company(
                    domain=domain,
                    name=name,
                    address=biz.get("address"),
                    city=biz.get("city"),
                    state=biz.get("state"),
                )

                # Write back to DB
                engine.write_company_enrichment(biz_id, result)

                # Find owner emails if team members found and no owner yet
                if (
                    not biz.get("owner_name")
                    and result.get("team_members")
                    and domain
                ):
                    for member in result["team_members"][:2]:
                        member_name = member.get("name", "")
                        parts = member_name.split()
                        if len(parts) >= 2:
                            person_result = engine.enrich_person(
                                first_name=parts[0],
                                last_name=" ".join(parts[1:]),
                                domain=domain,
                                company_name=name,
                            )
                            if person_result.get("email"):
                                engine.write_person_enrichment(
                                    person_result, business_id=biz_id,
                                )
                                # Update owner on business
                                engine.db.update(
                                    "businesses", "business_id", biz_id,
                                    {
                                        "owner_name": member_name,
                                        "email": person_result["email"],
                                    },
                                )
                                break

                enriched += 1
                progress.inc("total_enriched")
                progress.add_to_set("processed_ids", biz_id)

                sources = result.get("sources", [])
                log.info(
                    f"  -> {len(sources)} sources: {', '.join(sources)}"
                )

            except Exception as e:
                log.error(f"  ERROR enriching {name}: {e}")
                progress.inc("total_errors")
                progress.add_to_set("processed_ids", biz_id)
                # Mark as attempted so we do not retry forever
                engine.db.update(
                    "businesses", "business_id", biz_id,
                    {
                        "enrichment_status": "error",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                )

            # Save progress every 10 records
            if enriched % 10 == 0:
                progress.save()

            # Brief pause between records
            time.sleep(0.5)

    progress.set("finished_at", datetime.now(timezone.utc).isoformat())
    progress.save()

    log.info("=" * 60)
    log.info("ENRICHMENT COMPLETE")
    log.info(f"  Total enriched:  {progress.get('total_enriched', 0)}")
    log.info(f"  Total skipped:   {progress.get('total_skipped', 0)}")
    log.info(f"  Total errors:    {progress.get('total_errors', 0)}")
    log.info("=" * 60)


def check_credits(args):
    """Show what tools are available and their estimated costs."""
    log.info("=" * 60)
    log.info("ENRICHMENT ENGINE — Capability Check")
    log.info("=" * 60)

    # Python dependencies
    deps = {
        "dns.resolver (dnspython)": HAS_DNS,
        "whois (python-whois)": HAS_WHOIS,
        "BeautifulSoup (bs4)": HAS_BS4,
    }
    log.info("\nPython Dependencies:")
    for dep, available in deps.items():
        status = "OK" if available else "MISSING (pip install)"
        log.info(f"  {dep}: {status}")

    # API tokens
    log.info("\nAPI Tokens:")
    log.info(f"  APIFY_API_TOKEN: {'SET' if APIFY_API_TOKEN else 'NOT SET'}")

    # Supabase
    log.info("\nSupabase:")
    db = SupabaseClient()
    for table in ["businesses", "people", "firms", "industry_details"]:
        count = db.fetch_count(table)
        log.info(f"  {table}: {count:,} rows")

    # Business types breakdown
    log.info("\nBusiness types needing enrichment (status=raw):")
    for btype in ["car_wash", "auto_repair", "restaurant", "salon", "gym"]:
        count = db.fetch_count(
            "businesses",
            f"business_type=eq.{btype}&enrichment_status=eq.raw",
        )
        log.info(f"  {btype}: {count:,} raw")

    # Cost estimates
    log.info("\nCost Estimates (per 1,000 businesses):")
    log.info("  Website scraping:   $0.00 (free)")
    log.info("  Domain/WHOIS/DNS:   $0.00 (free)")
    log.info("  Email SMTP verify:  $0.00 (free)")
    log.info("  SEC EDGAR:          $0.00 (free)")
    log.info("  Google Maps (Apify):~$2.50 / 1K results")
    log.info("  LinkedIn (Apify):   ~$0.00 (free compute tier)")
    log.info("  Yelp (Apify):       ~$0.00 (free compute tier)")
    log.info("  Indeed jobs (Apify):~$0.00 (free compute tier)")
    log.info("  ------------------------------------------------")
    log.info("  Total per 1K:       ~$2.50 (vs Clay ~$50-200)")
    log.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Self-hosted enrichment engine — replaces Clay + Apollo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enrich a single company
  python3 enrichment_engine.py --domain acmecarwash.com --name "Acme Car Wash"

  # Enrich a person
  python3 enrichment_engine.py --person --first-name John --last-name Smith --domain acme.com

  # Batch enrich car washes from Supabase
  python3 enrichment_engine.py --type car_wash --limit 100

  # Batch enrich with no Apify (free layers only)
  python3 enrichment_engine.py --type car_wash --limit 500 --skip-apify

  # Check available tools and DB status
  python3 enrichment_engine.py --check-credits
        """,
    )

    # Mode selection
    parser.add_argument("--check-credits", action="store_true",
                        help="Show available tools, DB status, and cost estimates")
    parser.add_argument("--person", action="store_true",
                        help="Person enrichment mode (default is company)")

    # Single-company args
    parser.add_argument("--domain", type=str, help="Company domain to enrich")
    parser.add_argument("--website", type=str, help="Company website URL")
    parser.add_argument("--name", type=str, help="Company name")
    parser.add_argument("--address", type=str, help="Company address")
    parser.add_argument("--city", type=str, help="City")
    parser.add_argument("--state", type=str, help="State abbreviation")

    # Single-person args
    parser.add_argument("--first-name", type=str, dest="first_name",
                        help="Person first name")
    parser.add_argument("--last-name", type=str, dest="last_name",
                        help="Person last name")
    parser.add_argument("--company", type=str, help="Company name for person")

    # Batch args
    parser.add_argument("--type", type=str,
                        help="Business type to enrich (e.g., car_wash)")
    parser.add_argument("--limit", type=int, default=100,
                        help="Max businesses to enrich (default: 100)")
    parser.add_argument("--batch-size", type=int, default=25,
                        help="DB fetch batch size (default: 25)")
    parser.add_argument("--status", type=str, default=None,
                        help="Filter by enrichment_status (default: raw)")
    parser.add_argument("--state-filter", type=str, default=None,
                        help="Filter by state abbreviation")
    parser.add_argument("--skip-apify", action="store_true",
                        help="Skip Apify-powered scrapers (free layers only)")

    args = parser.parse_args()

    if args.check_credits:
        check_credits(args)
    elif args.person and args.first_name and args.last_name:
        single_person_enrichment(args)
    elif args.domain or args.name:
        if not args.type:
            single_company_enrichment(args)
        else:
            batch_enrichment(args)
    elif args.type:
        batch_enrichment(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
