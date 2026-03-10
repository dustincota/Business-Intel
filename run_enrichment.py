#!/usr/bin/env python3
"""
run_enrichment.py — Master orchestrator for the enrichment pipeline.

Usage:
    python3 run_enrichment.py --status              # Check progress
    python3 run_enrichment.py --free-only            # Run free layers only
    python3 run_enrichment.py --type car_wash        # Enrich car washes
    python3 run_enrichment.py --type car_wash --limit 500
    python3 run_enrichment.py --ingest census        # Ingest Census CBP data
    python3 run_enrichment.py --ingest sos-free      # Scrape free SOS data
    python3 run_enrichment.py --ingest classify      # Classify SOS records
    python3 run_enrichment.py --buyers               # Enrich 108 buyers
    python3 run_enrichment.py                        # Full pipeline

Author: Dustin Cota — M&A Intelligence Platform
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from enrich_utils import SupabaseClient, make_logger

log = make_logger("run_enrichment.log")

ENRICHMENT_PRIORITY = [
    {"type": "car_wash",     "limit": 75000,  "label": "Car Washes (core focus)"},
    {"type": "auto_repair",  "limit": 50000,  "label": "Auto Repair Shops"},
    {"type": "restaurant",   "limit": 50000,  "label": "Restaurants"},
    {"type": "medical",      "limit": 50000,  "label": "Medical Practices"},
    {"type": "salon",        "limit": 30000,  "label": "Salons & Spas"},
    {"type": "gym",          "limit": 20000,  "label": "Gyms & Fitness"},
    {"type": "gas_station",  "limit": 20000,  "label": "Gas Stations"},
    {"type": "auto_dealer",  "limit": 30000,  "label": "Auto Dealers"},
    {"type": "real_estate",  "limit": 30000,  "label": "Real Estate"},
    {"type": "construction", "limit": 50000,  "label": "Construction"},
    {"type": "laundromat",   "limit": 15000,  "label": "Laundromats"},
    {"type": "llc",          "limit": 100000, "label": "LLCs (with website)"},
    {"type": "corporation",  "limit": 100000, "label": "Corporations (with website)"},
]


def show_status(db):
    """Show comprehensive enrichment status."""
    log("=" * 70)
    log("  ENRICHMENT PIPELINE STATUS")
    log(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)

    log("\n── Database Overview ──")
    for table in ["businesses", "people", "firms", "car_wash_buyers", "business_people", "industry_details"]:
        count = db.fetch_count(table)
        log(f"  {table}: {count:,} rows")

    log("\n── Enrichment Status ──")
    for status in ["raw", "enriched", "apollo_people_searched", "google_enriched",
                    "legacy_merged", "classified", "error", "attempted"]:
        count = db.fetch_count("businesses", f"enrichment_status=eq.{status}")
        if count > 0:
            log(f"  {status}: {count:,}")

    log("\n── Business Types (classified) ──")
    for btype in ENRICHMENT_PRIORITY:
        t = btype["type"]
        total = db.fetch_count("businesses", f"business_type=eq.{t}")
        if total == 0:
            continue
        raw = db.fetch_count("businesses", f"business_type=eq.{t}&enrichment_status=eq.raw")
        has_web = db.fetch_count("businesses",
                                 f"business_type=eq.{t}&website=not.is.null&enrichment_status=eq.raw")
        enriched = total - raw
        pct = (enriched / total * 100) if total > 0 else 0
        log(f"  {t}: {total:,} total | {enriched:,} enriched ({pct:.1f}%) | {has_web:,} raw+website")

    log("\n── People ──")
    tp = db.fetch_count("people")
    we = db.fetch_count("people", "email=not.is.null")
    wp = db.fetch_count("people", "phone=not.is.null")
    log(f"  Total: {tp:,} | Email: {we:,} ({we/max(tp,1)*100:.1f}%) | Phone: {wp:,}")

    log("\n── Buyers ──")
    tb = db.fetch_count("car_wash_buyers")
    bc = db.fetch_count("car_wash_buyers", "contact_name=not.is.null")
    be = db.fetch_count("car_wash_buyers", "contact_email=not.is.null")
    log(f"  Total: {tb} | Contact: {bc} | Email: {be}")

    log("\n── Progress Files ──")
    for f in sorted(os.listdir(SCRIPT_DIR)):
        if f.endswith("_progress.json"):
            try:
                with open(os.path.join(SCRIPT_DIR, f)) as fh:
                    data = json.load(fh)
                log(f"  {f}: {data.get('total_enriched',0)} done, {data.get('total_errors',0)} err")
            except Exception:
                log(f"  {f}: unreadable")
    log("=" * 70)


def run_batch(business_type, limit, skip_apify=False, batch_size=25, state_filter=None):
    """Run enrichment engine for a business type."""
    cmd = [sys.executable, os.path.join(SCRIPT_DIR, "enrichment_engine.py"),
           "--type", business_type, "--limit", str(limit),
           "--batch-size", str(batch_size)]
    if skip_apify:
        cmd.append("--skip-apify")
    if state_filter:
        cmd.extend(["--state-filter", state_filter])

    log(f"Starting: {business_type} (limit={limit}, apify={'off' if skip_apify else 'on'})")
    try:
        proc = subprocess.Popen(cmd, cwd=SCRIPT_DIR, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                log(f"  {line}")
        proc.wait(timeout=3600*12)
        return proc.returncode == 0
    except Exception as e:
        log(f"  Failed: {e}")
        return False


def run_full_pipeline(skip_apify=False, limit_override=None, types=None):
    """Run enrichment across all business types."""
    log("=" * 70)
    log(f"  FULL PIPELINE — {'free only' if skip_apify else 'all layers'}")
    log("=" * 70)

    db = SupabaseClient()
    for p in ENRICHMENT_PRIORITY:
        btype = p["type"]
        if types and btype not in types:
            continue
        raw_ct = db.fetch_count("businesses",
                                f"business_type=eq.{btype}&enrichment_status=eq.raw&website=not.is.null")
        if raw_ct == 0:
            log(f"\nSkipping {p['label']}: no raw records with website")
            continue
        actual = min(limit_override or p["limit"], raw_ct)
        log(f"\n{'─'*50}")
        log(f"  {p['label']}: {raw_ct:,} available, enriching {actual:,}")
        run_batch(btype, actual, skip_apify=skip_apify)


def run_ingest(source):
    """Run a data ingest pipeline."""
    scripts = {
        "census": ("ingest_census_businesses.py", []),
        "sam": ("ingest_sam_gov.py", []),
        "sos": ("scrape_state_sos.py", []),
        "sos-free": ("scrape_state_sos.py", ["--free-only"]),
        "classify": ("classify_businesses.py", []),
        "firms": ("enrich_firms_websites.py", []),
    }
    entry = scripts.get(source)
    if not entry:
        log(f"Unknown: {source}. Available: {', '.join(scripts.keys())}")
        return
    script = os.path.join(SCRIPT_DIR, entry[0])
    if not os.path.exists(script):
        log(f"Not found: {script}")
        return
    cmd = [sys.executable, script] + entry[1]
    log(f"Running: {' '.join(cmd)}")
    try:
        proc = subprocess.Popen(cmd, cwd=SCRIPT_DIR, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                log(f"  {line}")
        proc.wait(timeout=3600*24)
        log(f"  Exit code: {proc.returncode}")
    except Exception as e:
        log(f"  Failed: {e}")


def enrich_buyers():
    """Enrich 108 car wash buyers using free layers."""
    from enrichment_engine import EnrichmentEngine, extract_domain_from_url
    log("=" * 70)
    log("  BUYER ENRICHMENT — 108 Car Wash Buyers")
    log("=" * 70)

    db = SupabaseClient()
    buyers = db.fetch("car_wash_buyers",
                      select="buyer_id,fund_name,website,domain,hq_city,hq_state,"
                             "contact_name,contact_email,contact_phone,linkedin_url",
                      order="buyer_id.asc", limit=200)
    if not buyers:
        log("No buyers found")
        return

    engine = EnrichmentEngine(skip_apify=True)
    enriched = 0

    for buyer in buyers:
        name = buyer.get("fund_name", "")
        website = buyer.get("website", "")
        domain = extract_domain_from_url(website) if website else (buyer.get("domain") or None)
        if not domain and not name:
            continue

        log(f"  [{enriched+1}/{len(buyers)}] {name} ({domain or 'no domain'})")
        try:
            result = engine.enrich_company(domain=domain, name=name,
                                           city=buyer.get("hq_city"),
                                           state=buyer.get("hq_state"))
            updates = {}
            emails = result.get("website_emails", [])
            if emails and not buyer.get("contact_email"):
                updates["contact_email"] = emails[0]
            phones = result.get("website_phones", [])
            if phones and not buyer.get("contact_phone"):
                updates["contact_phone"] = phones[0]
            li_url = result.get("social_links", {}).get("linkedin")
            if li_url and not buyer.get("linkedin_url"):
                updates["linkedin_url"] = li_url
            if updates:
                updates["updated_at"] = datetime.now(timezone.utc).isoformat()
                db.update("car_wash_buyers", "buyer_id", buyer["buyer_id"], updates)
                log(f"    Updated: {list(updates.keys())}")
                enriched += 1
        except Exception as e:
            log(f"    Error: {e}")
        time.sleep(1)

    log(f"\nBuyer enrichment: {enriched}/{len(buyers)} updated")


def main():
    parser = argparse.ArgumentParser(description="Master enrichment pipeline orchestrator")
    parser.add_argument("--status", action="store_true", help="Show progress")
    parser.add_argument("--type", type=str, help="Business type to enrich")
    parser.add_argument("--limit", type=int, default=None, help="Record limit")
    parser.add_argument("--free-only", action="store_true", help="No Apify (zero cost)")
    parser.add_argument("--ingest", type=str,
                        choices=["census", "sam", "sos", "sos-free", "classify", "firms"],
                        help="Run data ingest")
    parser.add_argument("--buyers", action="store_true", help="Enrich 108 buyers")
    args = parser.parse_args()

    db = SupabaseClient()
    if args.status:
        show_status(db)
    elif args.ingest:
        run_ingest(args.ingest)
    elif args.buyers:
        enrich_buyers()
    elif args.type:
        run_batch(args.type, args.limit or 100, skip_apify=args.free_only)
    else:
        run_full_pipeline(skip_apify=args.free_only, limit_override=args.limit)


if __name__ == "__main__":
    main()
