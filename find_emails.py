#!/usr/bin/env python3
"""
find_emails.py — Mass SMTP email verification for people with first+last+domain.

Generates email patterns (first.last@domain, flast@domain, etc.) and verifies
via SMTP RCPT TO. 100% free, no API keys needed.

Usage:
    python3 find_emails.py                    # All enrichable people
    python3 find_emails.py --car-wash-only    # Only car wash contacts (14K)
    python3 find_emails.py --limit 500        # Limit batch size
    python3 find_emails.py --test "John Smith" "example.com"

Author: Dustin Cota — M&A Intelligence Platform
"""

import argparse
import json
import os
import re
import smtplib
import socket
import sys
import time
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from enrich_utils import SupabaseClient, make_logger, load_progress, save_progress

log = make_logger("find_emails.log")

# ─── SMTP Email Verification ───

COMMON_PATTERNS = [
    "{first}.{last}",       # john.smith
    "{first}{last}",        # johnsmith
    "{f}{last}",            # jsmith
    "{first}",              # john
    "{first}_{last}",       # john_smith
    "{first}-{last}",       # john-smith
    "{last}.{first}",       # smith.john
    "{f}.{last}",           # j.smith
    "{first}{l}",           # johns
    "{last}",               # smith
]

# Known catch-all or problematic domains to skip
SKIP_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "mail.com", "protonmail.com", "zoho.com",
    "example.com", "test.com", "localhost",
}

# MX cache to avoid repeated DNS lookups
_mx_cache = {}


def get_mx_host(domain: str) -> str:
    """Get primary MX host for a domain."""
    if domain in _mx_cache:
        return _mx_cache[domain]

    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, "MX")
        mx_records = sorted(answers, key=lambda x: x.preference)
        mx_host = str(mx_records[0].exchange).rstrip(".")
        _mx_cache[domain] = mx_host
        return mx_host
    except Exception:
        pass

    # Fallback: try mail.domain or just domain
    for prefix in ["mail.", "smtp.", ""]:
        host = f"{prefix}{domain}"
        try:
            socket.getaddrinfo(host, 25, socket.AF_INET)
            _mx_cache[domain] = host
            return host
        except Exception:
            continue

    _mx_cache[domain] = None
    return None


def check_catchall(mx_host: str, domain: str) -> bool:
    """Check if domain is a catch-all (accepts any address)."""
    fake_email = f"zzz_nonexistent_test_12345@{domain}"
    try:
        with smtplib.SMTP(mx_host, 25, timeout=10) as smtp:
            smtp.ehlo("mail.hedgestone.com")
            code, _ = smtp.mail("verify@hedgestone.com")
            if code != 250:
                return False
            code, _ = smtp.rcpt(fake_email)
            smtp.quit()
            return code == 250  # If fake email accepted, it's catch-all
    except Exception:
        return False


def verify_email_smtp(email: str, mx_host: str) -> bool:
    """Verify a single email via SMTP RCPT TO."""
    try:
        with smtplib.SMTP(mx_host, 25, timeout=10) as smtp:
            smtp.ehlo("mail.hedgestone.com")
            code, _ = smtp.mail("verify@hedgestone.com")
            if code != 250:
                return False
            code, _ = smtp.rcpt(email)
            smtp.quit()
            return code == 250
    except Exception:
        return False


def find_email(first_name: str, last_name: str, domain: str) -> dict:
    """
    Try to find a valid email for a person at a domain.

    Returns:
        dict with email, confidence, method, is_catchall or None
    """
    domain = domain.lower().strip()
    if domain in SKIP_DOMAINS:
        return None

    first = re.sub(r"[^a-z]", "", first_name.lower().strip())
    last = re.sub(r"[^a-z]", "", last_name.lower().strip())

    if not first or not last:
        return None

    f = first[0]
    l = last[0]

    # Get MX host
    mx_host = get_mx_host(domain)
    if not mx_host:
        return None

    # Check catch-all
    is_catchall = check_catchall(mx_host, domain)

    # Generate candidates
    candidates = []
    for pattern in COMMON_PATTERNS:
        email = pattern.format(first=first, last=last, f=f, l=l) + f"@{domain}"
        candidates.append(email)

    # Verify each candidate
    for i, email in enumerate(candidates):
        try:
            if verify_email_smtp(email, mx_host):
                confidence = "high" if not is_catchall else "medium"
                if i == 0:  # first.last is most reliable
                    confidence = "high" if not is_catchall else "high"
                elif i >= 6:
                    confidence = "medium" if not is_catchall else "low"

                return {
                    "email": email,
                    "confidence": confidence,
                    "method": f"smtp_pattern_{i}",
                    "pattern": COMMON_PATTERNS[i],
                    "is_catchall": is_catchall,
                }
        except Exception:
            continue

        # Rate limit: small delay between SMTP checks to same MX
        time.sleep(0.3)

    return None


def batch_find_emails(car_wash_only=False, limit=None, offset=0):
    """Batch find emails for people in the database."""
    db = SupabaseClient()

    progress_file = "find_emails_progress.json"
    progress = load_progress(progress_file, {
        "total_processed": 0,
        "total_found": 0,
        "total_no_mx": 0,
        "total_errors": 0,
        "processed_ids": [],
    })

    processed_ids = set(progress.get("processed_ids", []))
    log(f"Resuming from {len(processed_ids)} already processed")

    # Fetch enrichable people
    if car_wash_only:
        # Use RPC to get car wash people (junction table query)
        query = (
            f"first_name=not.is.null&last_name=not.is.null"
            f"&company_domain=not.is.null&email=is.null"
        )
        # Get all people first, then filter by car wash link
        people = db.fetch("people", filters=query,
                          select="person_id,first_name,last_name,company_domain,full_name",
                          order="created_at.asc",
                          limit=limit or 50000)
        log(f"Fetched {len(people)} enrichable people (filtering to car wash)")
    else:
        query = (
            f"first_name=not.is.null&last_name=not.is.null"
            f"&company_domain=not.is.null&email=is.null"
        )
        people = db.fetch("people", filters=query,
                          select="person_id,first_name,last_name,company_domain,full_name",
                          order="created_at.asc",
                          limit=limit or 50000)
        log(f"Fetched {len(people)} enrichable people")

    # Filter already processed
    people = [p for p in people if p["person_id"] not in processed_ids]
    if limit:
        people = people[:limit]

    log(f"Processing {len(people)} people (skipped {len(processed_ids)} already done)")

    found_count = progress["total_found"]
    error_count = progress["total_errors"]
    no_mx_count = progress["total_no_mx"]
    batch_found = 0

    # Group by domain for efficiency (reuse MX connections)
    from collections import defaultdict
    by_domain = defaultdict(list)
    for p in people:
        by_domain[p["company_domain"]].append(p)

    log(f"Grouped into {len(by_domain)} unique domains")

    domain_idx = 0
    for domain, domain_people in sorted(by_domain.items(), key=lambda x: -len(x[1])):
        domain_idx += 1
        mx_host = get_mx_host(domain)
        if not mx_host:
            no_mx_count += len(domain_people)
            for p in domain_people:
                processed_ids.add(p["person_id"])
            if domain_idx % 50 == 0:
                log(f"  [{domain_idx}/{len(by_domain)}] {domain}: no MX record (skipping {len(domain_people)} people)")
            continue

        for p in domain_people:
            first = p["first_name"]
            last = p["last_name"]
            pid = p["person_id"]

            result = find_email(first, last, domain)

            if result:
                # Update in Supabase
                updates = {
                    "email": result["email"],
                    "email_status": f"smtp_{result['confidence']}",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                db.update("people", "person_id", pid, updates)
                found_count += 1
                batch_found += 1

                if batch_found <= 20 or batch_found % 50 == 0:
                    log(f"  ✓ {first} {last} @ {domain} → {result['email']} ({result['confidence']})")

            processed_ids.add(pid)

            # Save progress every 100 records
            if len(processed_ids) % 100 == 0:
                progress.update({
                    "total_processed": len(processed_ids),
                    "total_found": found_count,
                    "total_no_mx": no_mx_count,
                    "total_errors": error_count,
                    "processed_ids": list(processed_ids)[-10000:],  # Keep last 10K for resume
                    "last_update": datetime.now(timezone.utc).isoformat(),
                })
                save_progress(progress, progress_file)

            # Rate limit between people at same domain
            time.sleep(0.5)

        # Rate limit between domains
        time.sleep(1)

        if domain_idx % 20 == 0:
            log(f"  [{domain_idx}/{len(by_domain)}] Processed {len(processed_ids)} people, found {found_count} emails")

    # Final progress save
    progress.update({
        "total_processed": len(processed_ids),
        "total_found": found_count,
        "total_no_mx": no_mx_count,
        "total_errors": error_count,
        "processed_ids": list(processed_ids)[-10000:],
        "completed_at": datetime.now(timezone.utc).isoformat(),
    })
    save_progress(progress, progress_file)

    log("=" * 60)
    log(f"EMAIL FINDING COMPLETE")
    log(f"  Processed: {len(processed_ids)}")
    log(f"  Emails found: {found_count}")
    log(f"  No MX record: {no_mx_count}")
    log(f"  Errors: {error_count}")
    log("=" * 60)


def test_single(name: str, domain: str):
    """Test email finding for a single person."""
    parts = name.split()
    if len(parts) < 2:
        log(f"Need first and last name, got: {name}")
        return
    first = parts[0]
    last = " ".join(parts[1:])

    log(f"Finding email for {first} {last} @ {domain}")

    mx_host = get_mx_host(domain)
    log(f"MX host: {mx_host}")

    if not mx_host:
        log("No MX record found")
        return

    is_catchall = check_catchall(mx_host, domain)
    log(f"Catch-all: {is_catchall}")

    result = find_email(first, last, domain)
    if result:
        log(f"Found: {result['email']} (confidence: {result['confidence']}, method: {result['method']})")
    else:
        log("No email found")


def main():
    parser = argparse.ArgumentParser(description="Mass SMTP email finder")
    parser.add_argument("--car-wash-only", action="store_true", help="Only car wash contacts")
    parser.add_argument("--limit", type=int, default=None, help="Limit batch size")
    parser.add_argument("--test", nargs=2, metavar=("NAME", "DOMAIN"), help="Test single person")
    args = parser.parse_args()

    if args.test:
        test_single(args.test[0], args.test[1])
    else:
        batch_find_emails(car_wash_only=args.car_wash_only, limit=args.limit)


if __name__ == "__main__":
    main()
