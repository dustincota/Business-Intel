#!/usr/bin/env python3
"""
backfill_domains.py — Backfill company_domain for EDGAR people from firms table.
Loads all 275K firms, builds name→domain lookup, then scans 1M+ people.

Usage:
    python3 backfill_domains.py
    nohup python3 backfill_domains.py >> backfill_domains.log 2>&1 &
"""
import re
import sys
import time
from datetime import datetime

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
from enrich_utils import SupabaseClient, make_logger

log = make_logger("backfill_domains.log")


def extract_domain(url):
    if not url:
        return None
    url = re.sub(r'^https?://(www\.)?', '', url)
    url = re.sub(r'/.*$', '', url)
    return url.strip().lower()


def main():
    db = SupabaseClient()

    # Step 1: Load ALL firms with websites (paginate at 1000)
    log("Loading firms with websites...")
    firms_lookup = {}
    offset = 0
    while True:
        batch = db.fetch("firms",
                         select="name,website",
                         filters="website=not.is.null",
                         order="name.asc",
                         limit=1000, offset=offset)
        if not batch:
            break
        for f in batch:
            name = (f.get("name") or "").strip().lower()
            domain = extract_domain(f.get("website"))
            if name and domain:
                firms_lookup[name] = domain
        offset += len(batch)
        if offset % 10000 == 0:
            log(f"  Loaded {offset} firms, {len(firms_lookup)} with domains...")
        if len(batch) < 1000:
            break

    log(f"Built lookup: {len(firms_lookup)} firms with domains from {offset} rows")

    # Step 2: Scan EDGAR people without domain (cursor-based pagination)
    # NOTE: We use person_id cursor, NOT offset, because updating records
    # removes them from the company_domain=is.null filter and causes offset
    # pagination to skip records.
    log("Matching EDGAR people to firm domains...")
    total_updated = 0
    total_scanned = 0
    last_id = ""  # cursor: person_id > last_id

    while True:
        filt = "source=eq.edgar&company_domain=is.null&company_name=not.is.null"
        if last_id:
            filt += f"&person_id=gt.{last_id}"
        batch = db.fetch("people",
                         select="person_id,company_name",
                         filters=filt,
                         order="person_id.asc",
                         limit=1000, offset=0)
        if not batch:
            break

        last_id = batch[-1]["person_id"]  # advance cursor

        matched = 0
        for p in batch:
            name = (p.get("company_name") or "").strip().lower()
            domain = firms_lookup.get(name)
            if domain:
                db.update("people", "person_id", p["person_id"],
                          {"company_domain": domain,
                           "updated_at": datetime.utcnow().isoformat() + "Z"})
                matched += 1

        total_updated += matched
        total_scanned += len(batch)

        if total_scanned % 10000 == 0 or matched > 0:
            log(f"  Scanned {total_scanned}: {matched} matched this page (total: {total_updated})")

        if len(batch) < 1000:
            break

        time.sleep(0.05)

    log(f"Done. Scanned {total_scanned} people, backfilled {total_updated} domains")


if __name__ == "__main__":
    main()
