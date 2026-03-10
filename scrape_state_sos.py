#!/usr/bin/env python3
"""
Secretary of State Business Entity Scraper — All 50 US States
=============================================================
Finds car wash businesses from state SOS/corporation databases.

Strategy per state:
  TIER 1 — FREE bulk data via Socrata/Open Data APIs (TX, CO, OH)
  TIER 2 — FREE bulk CSV/TXT downloads (OH reports, NC subscriptions)
  TIER 3 — OpenCorporates API (free tier: 500 req/mo, no key needed)
  TIER 4 — Apify actors (paid per result)
  TIER 5 — States already loaded (NY, FL) — skip

Current DB: ny_sos (2.7M), fl_sunbiz (100K). This adds the other 48.

Usage:
  # Run all tiers for all states:
  python3 scrape_state_sos.py

  # Run a specific state:
  python3 scrape_state_sos.py --state TX

  # Run only free tiers (1-3):
  python3 scrape_state_sos.py --free-only

  # Resume from where you left off:
  python3 scrape_state_sos.py --resume

  # Dry-run (no DB inserts):
  python3 scrape_state_sos.py --dry-run

Run: nohup python3 scrape_state_sos.py >> scrape_sos_stdout.log 2>&1 &
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone

import requests

from enrich_utils import (
    SupabaseClient,
    ApifyClient,
    make_logger,
    load_progress,
    save_progress,
    clean_business_name,
    utcnow_iso,
)

log = make_logger("scrape_state_sos.log")
PROGRESS_FILE = "scrape_state_sos_progress.json"
BATCH_SIZE = 200  # Supabase upsert batch size

# ═══════════════════════════════════════════════════════════════════════
# CAR WASH SEARCH KEYWORDS
# ═══════════════════════════════════════════════════════════════════════

CAR_WASH_KEYWORDS = [
    "car wash", "carwash", "auto spa", "express wash",
    "auto clean", "tunnel wash", "auto wash", "autowash",
    "hand wash car", "speed wash", "quick wash", "splash wash",
    "squeaky clean car", "suds car", "bubble bath car",
    "wash express", "super wash",
]

# Regex for classification after retrieval
CAR_WASH_RE = re.compile(
    r'car\s*wash|auto\s*wash|auto\s*spa|express\s*wash|tunnel\s*wash|'
    r'auto\s*clean|hand\s*wash|speed\s*wash|quick\s*wash|splash\s*wash|'
    r'squeaky\s*clean|bubble\s*bath\s*car|suds|wash\s*express|super\s*wash',
    re.IGNORECASE,
)

# ═══════════════════════════════════════════════════════════════════════
# STATE CONFIGURATION — Approach per state
# ═══════════════════════════════════════════════════════════════════════
#
# Each state has:
#   tier:    1=free API, 2=free download, 3=opencorporates, 4=apify, 5=skip
#   method:  function name to call
#   notes:   human-readable explanation
#   cost:    estimated cost for car wash search
#
# States are sorted by estimated car wash count (highest first).
# ═══════════════════════════════════════════════════════════════════════

STATE_CONFIG = {
    # ── Already loaded ──
    "NY": {"tier": 5, "method": "skip", "notes": "Already in DB as ny_sos (2.7M records)", "cost": "$0"},
    "FL": {"tier": 5, "method": "skip", "notes": "Already in DB as fl_sunbiz (100K records)", "cost": "$0"},

    # ── TIER 1: Free Socrata/Open Data APIs ──
    "TX": {
        "tier": 1, "method": "scrape_texas_opendata",
        "notes": "Texas Open Data Portal — Active Franchise Tax Permit Holders. "
                 "Free Socrata API at data.texas.gov (dataset 9cir-efmm). "
                 "Millions of records, filter by taxpayer_name LIKE '%CAR WASH%'. "
                 "Fields: taxpayer_number, taxpayer_name, address, city, state, zip, "
                 "county, org_type, sos_file_number, sos_charter_date, naics_code.",
        "cost": "$0",
        "api_base": "https://data.texas.gov/resource/9cir-efmm.json",
    },
    "CO": {
        "tier": 1, "method": "scrape_colorado_opendata",
        "notes": "Colorado Information Marketplace — Business Entities in Colorado. "
                 "Free Socrata API at data.colorado.gov (dataset 4ykn-tg5h). "
                 "1M+ records since 1864, filter by entityname LIKE '%CAR WASH%'. "
                 "Fields: entityname, principaladdress1, principalcity, principalstate, "
                 "principalzipcode, entitystatus, entitytypeverbose, entityformdate.",
        "cost": "$0",
        "api_base": "https://data.colorado.gov/resource/4ykn-tg5h.json",
    },

    # ── TIER 2: Free bulk downloads (requires manual fetch or scripted HTTP) ──
    "OH": {
        "tier": 2, "method": "scrape_ohio_reports",
        "notes": "Ohio SOS — Free downloadable business reports (TXT format). "
                 "Reports cover new corporations, LLCs, LPs by month/year. "
                 "URL: ohiosos.gov/businesses/business-reports/download-business-report/ "
                 "Fields: charter_number, corp_name, agent, incorporator, address, date. "
                 "Must download each monthly file and grep for car wash names.",
        "cost": "$0",
    },

    # ── TIER 3: OpenCorporates API (free: 500 req/mo, no key) ──
    # For most states, we use OpenCorporates as the primary free approach.
    # Each keyword search = 1 API call, returns up to 100 results per page.
    # With 17 keywords x ~3 pages avg = ~51 calls per state.
    # 500 free calls/mo = ~9 states/month at this rate.
    "CA": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "California — bizfile Online has bulk data for $100 (Master Unload). "
                 "Alternatively, free OpenCorporates API. CA SOS also has parseforge/sos-scraper "
                 "Apify actor ($15/mo rental). OpenCorporates free tier is simplest.",
        "cost": "$0 (OpenCorporates) or $100 (CA bulk) or $15/mo (Apify rental)",
        "jurisdiction": "us_ca",
    },
    "GA": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "Georgia — Bulk corporations database costs $500 (one-time) or $1000 (FTP). "
                 "Use OpenCorporates free tier instead.",
        "cost": "$0 (OpenCorporates) or $500+ (GA bulk)",
        "jurisdiction": "us_ga",
    },
    "NC": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "North Carolina — Data Subscriptions available (paid, contact sosnc.gov). "
                 "Weekly CSV updates with full entity data including officers. "
                 "Use OpenCorporates free tier for car wash search.",
        "cost": "$0 (OpenCorporates) or paid subscription (NC SOS)",
        "jurisdiction": "us_nc",
    },
    "PA": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "Pennsylvania — Bureau of Corporations sells master data subscriptions. "
                 "3M+ entities. Use OpenCorporates free tier.",
        "cost": "$0 (OpenCorporates)",
        "jurisdiction": "us_pa",
    },
    "IN": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "Indiana — INBiz Bulk Data Services exists but charges commercial users. "
                 "Free public search at inbiz.in.gov. Use OpenCorporates.",
        "cost": "$0 (OpenCorporates)",
        "jurisdiction": "us_in",
    },
    "MI": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "Michigan — MiBusiness Registry Portal (LARA). No bulk download found. "
                 "Use OpenCorporates free tier.",
        "cost": "$0 (OpenCorporates)",
        "jurisdiction": "us_mi",
    },
    "AZ": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "Arizona — Corporation Commission has DATABASE EXTRACTION form (fee-based). "
                 "Can request specific entity types (LLC, Corp). Use OpenCorporates.",
        "cost": "$0 (OpenCorporates) or fee (AZ ACC extraction)",
        "jurisdiction": "us_az",
    },
    "IL": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "Illinois — SOS prohibits bulk download from public search. "
                 "Dept of Business Services sells data (call 217-782-6961). "
                 "Use OpenCorporates free tier.",
        "cost": "$0 (OpenCorporates)",
        "jurisdiction": "us_il",
    },
    "VA": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "Virginia — SCC has 'Download Database' option on sccefile.scc.virginia.gov "
                 "but details unclear. Use OpenCorporates.",
        "cost": "$0 (OpenCorporates)",
        "jurisdiction": "us_va",
    },
    "WA": {
        "tier": 3, "method": "scrape_opencorporates",
        "notes": "Washington — Corporations Data Extract feature discontinued. "
                 "Advanced search can export to Excel but not bulk. OpenCorporates.",
        "cost": "$0 (OpenCorporates)",
        "jurisdiction": "us_wa",
    },
    "TN": {"tier": 3, "method": "scrape_opencorporates", "notes": "Tennessee — OpenCorporates", "cost": "$0", "jurisdiction": "us_tn"},
    "MO": {"tier": 3, "method": "scrape_opencorporates", "notes": "Missouri — OpenCorporates", "cost": "$0", "jurisdiction": "us_mo"},
    "NJ": {"tier": 3, "method": "scrape_opencorporates", "notes": "New Jersey — OpenCorporates", "cost": "$0", "jurisdiction": "us_nj"},
    "SC": {"tier": 3, "method": "scrape_opencorporates", "notes": "South Carolina — OpenCorporates", "cost": "$0", "jurisdiction": "us_sc"},
    "AL": {"tier": 3, "method": "scrape_opencorporates", "notes": "Alabama — OpenCorporates", "cost": "$0", "jurisdiction": "us_al"},
    "LA": {"tier": 3, "method": "scrape_opencorporates", "notes": "Louisiana — OpenCorporates", "cost": "$0", "jurisdiction": "us_la"},
    "KY": {"tier": 3, "method": "scrape_opencorporates", "notes": "Kentucky — OpenCorporates", "cost": "$0", "jurisdiction": "us_ky"},
    "OR": {"tier": 3, "method": "scrape_opencorporates", "notes": "Oregon — OpenCorporates", "cost": "$0", "jurisdiction": "us_or"},
    "OK": {"tier": 3, "method": "scrape_opencorporates", "notes": "Oklahoma — OpenCorporates", "cost": "$0", "jurisdiction": "us_ok"},
    "CT": {"tier": 3, "method": "scrape_opencorporates", "notes": "Connecticut — OpenCorporates", "cost": "$0", "jurisdiction": "us_ct"},
    "IA": {"tier": 3, "method": "scrape_opencorporates", "notes": "Iowa — OpenCorporates", "cost": "$0", "jurisdiction": "us_ia"},
    "MS": {"tier": 3, "method": "scrape_opencorporates", "notes": "Mississippi — OpenCorporates", "cost": "$0", "jurisdiction": "us_ms"},
    "AR": {"tier": 3, "method": "scrape_opencorporates", "notes": "Arkansas — Has bulk data download (ark.org/corp-search)", "cost": "$0", "jurisdiction": "us_ar"},
    "KS": {"tier": 3, "method": "scrape_opencorporates", "notes": "Kansas — OpenCorporates", "cost": "$0", "jurisdiction": "us_ks"},
    "UT": {"tier": 3, "method": "scrape_opencorporates", "notes": "Utah — OpenCorporates", "cost": "$0", "jurisdiction": "us_ut"},
    "NV": {"tier": 3, "method": "scrape_opencorporates", "notes": "Nevada — OpenCorporates", "cost": "$0", "jurisdiction": "us_nv"},
    "NM": {"tier": 3, "method": "scrape_opencorporates", "notes": "New Mexico — OpenCorporates", "cost": "$0", "jurisdiction": "us_nm"},
    "NE": {"tier": 3, "method": "scrape_opencorporates", "notes": "Nebraska — OpenCorporates", "cost": "$0", "jurisdiction": "us_ne"},
    "WV": {"tier": 3, "method": "scrape_opencorporates", "notes": "West Virginia — OpenCorporates", "cost": "$0", "jurisdiction": "us_wv"},
    "ID": {"tier": 3, "method": "scrape_opencorporates", "notes": "Idaho — OpenCorporates", "cost": "$0", "jurisdiction": "us_id"},
    "HI": {"tier": 3, "method": "scrape_opencorporates", "notes": "Hawaii — OpenCorporates", "cost": "$0", "jurisdiction": "us_hi"},
    "NH": {"tier": 3, "method": "scrape_opencorporates", "notes": "New Hampshire — OpenCorporates", "cost": "$0", "jurisdiction": "us_nh"},
    "ME": {"tier": 3, "method": "scrape_opencorporates", "notes": "Maine — OpenCorporates", "cost": "$0", "jurisdiction": "us_me"},
    "MT": {"tier": 3, "method": "scrape_opencorporates", "notes": "Montana — OpenCorporates", "cost": "$0", "jurisdiction": "us_mt"},
    "RI": {"tier": 3, "method": "scrape_opencorporates", "notes": "Rhode Island — OpenCorporates", "cost": "$0", "jurisdiction": "us_ri"},
    "DE": {"tier": 3, "method": "scrape_opencorporates", "notes": "Delaware — OpenCorporates", "cost": "$0", "jurisdiction": "us_de"},
    "SD": {"tier": 3, "method": "scrape_opencorporates", "notes": "South Dakota — OpenCorporates", "cost": "$0", "jurisdiction": "us_sd"},
    "ND": {"tier": 3, "method": "scrape_opencorporates", "notes": "North Dakota — OpenCorporates", "cost": "$0", "jurisdiction": "us_nd"},
    "AK": {"tier": 3, "method": "scrape_opencorporates", "notes": "Alaska — OpenCorporates", "cost": "$0", "jurisdiction": "us_ak"},
    "VT": {"tier": 3, "method": "scrape_opencorporates", "notes": "Vermont — OpenCorporates", "cost": "$0", "jurisdiction": "us_vt"},
    "WY": {"tier": 3, "method": "scrape_opencorporates", "notes": "Wyoming — OpenCorporates", "cost": "$0", "jurisdiction": "us_wy"},
    "DC": {"tier": 3, "method": "scrape_opencorporates", "notes": "DC — OpenCorporates", "cost": "$0", "jurisdiction": "us_dc"},

    # ── TIER 4: Apify actors (paid fallback) ──
    # Use for states where OpenCorporates has poor coverage.
    # openactor/business-entity-search: $0.002/result (cheapest, multi-country)
    # nobel10technologies/llc-scraper: $0.005/result (uses OpenCorporates backend)
    # explorer_holdings/texas-biz-scraper: $0.025/result (TX only, Socrata API)
    # great_pistachio/us-business-search: $0.004/result (NY+FL only)
    #
    # Best fallback: openactor/business-entity-search at $0.002/result
    # Estimated ~200 car wash entities per state x $0.002 = ~$0.40 per state
    #
    # MD and MA have notoriously difficult SOS sites — good candidates for Apify.
    "MD": {
        "tier": 4, "method": "scrape_apify_openactor",
        "notes": "Maryland — SOS bulk data is fee-based. OpenCorporates coverage patchy. "
                 "Use openactor/business-entity-search ($0.002/result).",
        "cost": "~$0.40 (est. 200 results x $0.002)",
        "jurisdiction": "us_md",
    },
    "MA": {
        "tier": 4, "method": "scrape_apify_openactor",
        "notes": "Massachusetts — SOS search is limited. Use Apify fallback.",
        "cost": "~$0.40 (est. 200 results x $0.002)",
        "jurisdiction": "us_ma",
    },
    "MN": {
        "tier": 4, "method": "scrape_apify_openactor",
        "notes": "Minnesota — Use Apify fallback.",
        "cost": "~$0.40",
        "jurisdiction": "us_mn",
    },
    "WI": {
        "tier": 4, "method": "scrape_apify_openactor",
        "notes": "Wisconsin — Use Apify fallback.",
        "cost": "~$0.40",
        "jurisdiction": "us_wi",
    },
}

# ═══════════════════════════════════════════════════════════════════════
# HELPER: Build a business record for Supabase insert
# ═══════════════════════════════════════════════════════════════════════

def make_business_record(
    name, state_code, data_source, source_id=None,
    address=None, city=None, state=None, zip_code=None,
    county=None, entity_type=None, formation_date=None,
    naics_code=None, owner_name=None, agent_name=None,
    status_str=None,
):
    """Create a dict suitable for Supabase businesses table upsert."""
    # Determine business_type from name
    biz_type = None
    if CAR_WASH_RE.search(name or ""):
        biz_type = "car_wash"

    rec = {
        "business_id": str(uuid.uuid4()),
        "name": (name or "").strip()[:500],
        "legal_name": (name or "").strip()[:500],
        "data_source": data_source,
        "state": state or state_code,
        "country": "US",
        "created_at": utcnow_iso(),
        "updated_at": utcnow_iso(),
    }
    if source_id:
        rec["source_id"] = str(source_id).strip()[:100]
    if address:
        rec["address"] = address.strip()[:500]
    if city:
        rec["city"] = city.strip()[:200]
    if zip_code:
        rec["zip"] = str(zip_code).strip()[:20]
    if county:
        rec["county"] = county.strip()[:200]
    if entity_type:
        rec["sub_type"] = entity_type.strip()[:100]
    if formation_date:
        try:
            yr = int(str(formation_date)[:4])
            if 1800 <= yr <= 2030:
                rec["year_established"] = yr
        except (ValueError, TypeError):
            pass
    if naics_code:
        rec["naics_code"] = str(naics_code).strip()[:20]
    if owner_name:
        rec["owner_name"] = owner_name.strip()[:300]
    if biz_type:
        rec["business_type"] = biz_type
    if status_str:
        rec["status"] = status_str.strip()[:50]

    return rec


# ═══════════════════════════════════════════════════════════════════════
# TIER 1: Texas Open Data Portal (Socrata API)
# ═══════════════════════════════════════════════════════════════════════

def scrape_texas_opendata(db, progress, dry_run=False):
    """
    Texas — Free Socrata API on data.texas.gov
    Dataset: Active Franchise Tax Permit Holders (9cir-efmm)
    API: https://data.texas.gov/resource/9cir-efmm.json
    SoQL: $where=upper(taxpayer_name) LIKE '%CAR WASH%'
    """
    state = "TX"
    data_source = f"sos_{state.lower()}"
    api_base = STATE_CONFIG[state]["api_base"]

    if progress.get(f"{state}_done"):
        log(f"[{state}] Already completed, skipping.")
        return 0

    log(f"[{state}] Starting Texas Open Data Portal scrape (FREE Socrata API)...")
    total_inserted = 0
    session = requests.Session()

    for keyword in CAR_WASH_KEYWORDS:
        kw_upper = keyword.upper().replace("'", "''")
        offset = 0
        page_size = 1000

        while True:
            # SoQL query — case-insensitive search via upper()
            params = {
                "$where": f"upper(taxpayer_name) like '%{kw_upper}%'",
                "$limit": page_size,
                "$offset": offset,
                "$order": "taxpayer_number",
            }
            try:
                r = session.get(api_base, params=params, timeout=60)
                if r.status_code != 200:
                    log(f"[{state}] API error {r.status_code} for '{keyword}' offset={offset}")
                    break
                rows = r.json()
            except Exception as e:
                log(f"[{state}] Request error for '{keyword}': {e}")
                break

            if not rows:
                break

            batch = []
            for row in rows:
                rec = make_business_record(
                    name=row.get("taxpayer_name", ""),
                    state_code=state,
                    data_source=data_source,
                    source_id=row.get("taxpayer_number"),
                    address=row.get("taxpayer_address"),
                    city=row.get("taxpayer_city"),
                    state=row.get("taxpayer_state", state),
                    zip_code=row.get("taxpayer_zip"),
                    county=row.get("taxpayer_county"),
                    entity_type=row.get("taxpayer_organizational_type"),
                    formation_date=row.get("responsibility_beginning_date"),
                    naics_code=row.get("naics_code"),
                    status_str="active" if row.get("right_to_transact_business") == "Y" else "inactive",
                )
                batch.append(rec)

            if batch and not dry_run:
                # Upsert in chunks
                for i in range(0, len(batch), BATCH_SIZE):
                    chunk = batch[i:i + BATCH_SIZE]
                    ok = db.upsert("businesses", chunk, on_conflict="data_source,source_id")
                    if ok:
                        total_inserted += len(chunk)

            log(f"[{state}] '{keyword}' offset={offset}: {len(rows)} rows fetched, {len(batch)} queued")

            if len(rows) < page_size:
                break
            offset += page_size
            time.sleep(0.5)  # Be polite to the API

    progress[f"{state}_done"] = True
    progress[f"{state}_count"] = total_inserted
    log(f"[{state}] DONE — {total_inserted} records inserted from Texas Open Data Portal")
    return total_inserted


# ═══════════════════════════════════════════════════════════════════════
# TIER 1: Colorado Open Data (Socrata API)
# ═══════════════════════════════════════════════════════════════════════

def scrape_colorado_opendata(db, progress, dry_run=False):
    """
    Colorado — Free Socrata API on data.colorado.gov
    Dataset: Business Entities in Colorado (4ykn-tg5h)
    API: https://data.colorado.gov/resource/4ykn-tg5h.json
    SoQL: $where=upper(entityname) LIKE '%CAR WASH%'
    """
    state = "CO"
    data_source = f"sos_{state.lower()}"
    api_base = STATE_CONFIG[state]["api_base"]

    if progress.get(f"{state}_done"):
        log(f"[{state}] Already completed, skipping.")
        return 0

    log(f"[{state}] Starting Colorado Information Marketplace scrape (FREE Socrata API)...")
    total_inserted = 0
    session = requests.Session()

    for keyword in CAR_WASH_KEYWORDS:
        kw_upper = keyword.upper().replace("'", "''")
        offset = 0
        page_size = 1000

        while True:
            params = {
                "$where": f"upper(entityname) like '%{kw_upper}%'",
                "$limit": page_size,
                "$offset": offset,
                "$order": "entityid",
            }
            try:
                r = session.get(api_base, params=params, timeout=60)
                if r.status_code != 200:
                    log(f"[{state}] API error {r.status_code} for '{keyword}' offset={offset}")
                    break
                rows = r.json()
            except Exception as e:
                log(f"[{state}] Request error for '{keyword}': {e}")
                break

            if not rows:
                break

            batch = []
            for row in rows:
                rec = make_business_record(
                    name=row.get("entityname", ""),
                    state_code=state,
                    data_source=data_source,
                    source_id=row.get("entityid"),
                    address=row.get("principaladdress1"),
                    city=row.get("principalcity"),
                    state=row.get("principalstate", state),
                    zip_code=row.get("principalzipcode"),
                    entity_type=row.get("entitytypeverbose"),
                    formation_date=row.get("entityformdate"),
                    owner_name=row.get("agentfirstname", "") + " " + row.get("agentlastname", ""),
                    status_str=row.get("entitystatus"),
                )
                batch.append(rec)

            if batch and not dry_run:
                for i in range(0, len(batch), BATCH_SIZE):
                    chunk = batch[i:i + BATCH_SIZE]
                    ok = db.upsert("businesses", chunk, on_conflict="data_source,source_id")
                    if ok:
                        total_inserted += len(chunk)

            log(f"[{state}] '{keyword}' offset={offset}: {len(rows)} rows, {len(batch)} queued")

            if len(rows) < page_size:
                break
            offset += page_size
            time.sleep(0.5)

    progress[f"{state}_done"] = True
    progress[f"{state}_count"] = total_inserted
    log(f"[{state}] DONE — {total_inserted} records inserted from Colorado Open Data")
    return total_inserted


# ═══════════════════════════════════════════════════════════════════════
# TIER 2: Ohio SOS — Free downloadable reports (TXT)
# ═══════════════════════════════════════════════════════════════════════

def scrape_ohio_reports(db, progress, dry_run=False):
    """
    Ohio — Free business reports at ohiosos.gov
    Downloads monthly new-corporation TXT files, parses for car wash names.

    Note: Ohio's download page provides new filings by date range.
    For a full scan we'd need to iterate monthly files. As a simpler
    alternative, we fall back to OpenCorporates for OH as well.
    """
    state = "OH"
    if progress.get(f"{state}_done"):
        log(f"[{state}] Already completed, skipping.")
        return 0

    log(f"[{state}] Ohio — reports are TXT format with limited structure.")
    log(f"[{state}] Falling back to OpenCorporates API for better coverage.")

    # Ohio's downloadable reports are monthly new-filing TXT files without
    # consistent delimiters. OpenCorporates has better structured data.
    return scrape_opencorporates_state(db, progress, state, "us_oh", dry_run)


# ═══════════════════════════════════════════════════════════════════════
# TIER 3: OpenCorporates API (free tier — 500 req/month, no key needed)
# ═══════════════════════════════════════════════════════════════════════

OPENCORP_BASE = "https://api.opencorporates.com/v0.4"
OPENCORP_CALLS_THIS_SESSION = 0
OPENCORP_MAX_FREE = 480  # Leave buffer under 500

def scrape_opencorporates(db, progress, dry_run=False, state_code=None):
    """
    Wrapper that calls scrape_opencorporates_state with the right jurisdiction.
    Called from the main dispatch loop.
    """
    global OPENCORP_CALLS_THIS_SESSION

    if state_code is None:
        log("[OC] ERROR: state_code is required")
        return 0

    cfg = STATE_CONFIG.get(state_code, {})
    jurisdiction = cfg.get("jurisdiction", f"us_{state_code.lower()}")

    return scrape_opencorporates_state(db, progress, state_code, jurisdiction, dry_run)


def scrape_opencorporates_state(db, progress, state_code, jurisdiction, dry_run=False):
    """
    Search OpenCorporates for car wash entities in a specific US state.
    Free tier: 500 requests/month, no API key needed.
    Returns up to 100 results per page.
    """
    global OPENCORP_CALLS_THIS_SESSION

    data_source = f"sos_{state_code.lower()}"

    if progress.get(f"{state_code}_done"):
        log(f"[{state_code}] Already completed, skipping.")
        return 0

    log(f"[{state_code}] Starting OpenCorporates search (jurisdiction={jurisdiction})...")
    total_inserted = 0
    seen_ids = set()
    session = requests.Session()
    session.headers["User-Agent"] = "CarWashIntelBot/1.0 (M&A research)"

    for keyword in CAR_WASH_KEYWORDS:
        if OPENCORP_CALLS_THIS_SESSION >= OPENCORP_MAX_FREE:
            log(f"[{state_code}] OpenCorporates free limit approaching ({OPENCORP_CALLS_THIS_SESSION} calls). Stopping.")
            progress[f"{state_code}_partial"] = True
            break

        page = 1
        while page <= 5:  # Max 5 pages per keyword (500 results)
            url = f"{OPENCORP_BASE}/companies/search"
            params = {
                "q": keyword,
                "jurisdiction_code": jurisdiction,
                "page": page,
                "per_page": 100,
                "order": "score",
            }
            try:
                r = session.get(url, params=params, timeout=30)
                OPENCORP_CALLS_THIS_SESSION += 1

                if r.status_code == 403:
                    log(f"[{state_code}] OpenCorporates rate limited. Pausing 60s...")
                    time.sleep(60)
                    continue
                if r.status_code == 429:
                    log(f"[{state_code}] OpenCorporates 429 — monthly limit reached.")
                    progress[f"{state_code}_partial"] = True
                    break
                if r.status_code != 200:
                    log(f"[{state_code}] OC API error {r.status_code} for '{keyword}' p{page}")
                    break

                data = r.json()
                companies = data.get("results", {}).get("companies", [])

            except Exception as e:
                log(f"[{state_code}] OC request error: {e}")
                break

            if not companies:
                break

            batch = []
            for entry in companies:
                co = entry.get("company", {})
                oc_num = co.get("company_number", "")
                oc_name = co.get("name", "")

                # Deduplicate within this run
                dedup_key = f"{jurisdiction}:{oc_num}"
                if dedup_key in seen_ids:
                    continue
                seen_ids.add(dedup_key)

                # Parse address
                addr = co.get("registered_address_in_full", "") or ""
                addr_parts = [p.strip() for p in addr.split(",")]
                city_parsed = addr_parts[1] if len(addr_parts) > 1 else ""
                zip_parsed = ""
                # Try to extract zip from last part
                if addr_parts:
                    zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', addr_parts[-1])
                    if zip_match:
                        zip_parsed = zip_match.group(1)

                rec = make_business_record(
                    name=oc_name,
                    state_code=state_code,
                    data_source=data_source,
                    source_id=oc_num,
                    address=addr_parts[0] if addr_parts else None,
                    city=city_parsed,
                    state=state_code,
                    zip_code=zip_parsed,
                    entity_type=co.get("company_type"),
                    formation_date=co.get("incorporation_date"),
                    status_str=co.get("current_status"),
                )
                batch.append(rec)

            if batch and not dry_run:
                for i in range(0, len(batch), BATCH_SIZE):
                    chunk = batch[i:i + BATCH_SIZE]
                    ok = db.upsert("businesses", chunk, on_conflict="data_source,source_id")
                    if ok:
                        total_inserted += len(chunk)

            log(f"[{state_code}] OC '{keyword}' p{page}: {len(companies)} results, {len(batch)} new")

            total_pages = data.get("results", {}).get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1
            time.sleep(2)  # Be polite — free tier

        time.sleep(1)  # Between keywords

    progress[f"{state_code}_done"] = True
    progress[f"{state_code}_count"] = total_inserted
    log(f"[{state_code}] DONE — {total_inserted} records via OpenCorporates "
        f"(session calls so far: {OPENCORP_CALLS_THIS_SESSION})")
    return total_inserted


# ═══════════════════════════════════════════════════════════════════════
# TIER 4: Apify — openactor/business-entity-search ($0.002/result)
# ═══════════════════════════════════════════════════════════════════════

def scrape_apify_openactor(db, progress, dry_run=False, state_code=None):
    """
    Use Apify's openactor/business-entity-search for states where
    OpenCorporates coverage is poor.

    Cost: $0.002 per result + $0.0005 per actor start.
    Estimated: ~200 car wash entities per state = ~$0.40 per state.
    """
    if state_code is None:
        log("[APIFY] ERROR: state_code required")
        return 0

    data_source = f"sos_{state_code.lower()}"

    if progress.get(f"{state_code}_done"):
        log(f"[{state_code}] Already completed, skipping.")
        return 0

    apify = ApifyClient()
    if not apify.token:
        log(f"[{state_code}] APIFY_API_TOKEN not set. Skipping Apify tier. "
            f"Set env var or fall back to OpenCorporates.")
        # Fall back to OpenCorporates
        cfg = STATE_CONFIG.get(state_code, {})
        jurisdiction = cfg.get("jurisdiction", f"us_{state_code.lower()}")
        return scrape_opencorporates_state(db, progress, state_code, jurisdiction, dry_run)

    log(f"[{state_code}] Starting Apify openactor/business-entity-search...")
    total_inserted = 0
    seen_ids = set()

    for keyword in CAR_WASH_KEYWORDS:
        log(f"[{state_code}] Apify search: '{keyword}'")
        input_data = {
            "entityName": keyword,
            "countryCodes": ["US"],
        }
        try:
            results = apify.run_actor(
                "openactor~business-entity-search",
                input_data,
                timeout_secs=120,
                memory_mb=512,
            )
        except Exception as e:
            log(f"[{state_code}] Apify error for '{keyword}': {e}")
            continue

        if not results:
            log(f"[{state_code}] No results for '{keyword}'")
            continue

        batch = []
        for item in results:
            # openactor returns nested structures; adapt
            companies = []
            if isinstance(item, dict):
                if "companies" in item:
                    companies = item["companies"]
                elif "name" in item:
                    companies = [item]

            for co in companies:
                co_name = co.get("name", "")
                jurisdiction_code = co.get("jurisdiction_code", "")

                # Filter to target state only
                if jurisdiction_code and not jurisdiction_code.endswith(f"_{state_code.lower()}"):
                    continue

                co_id = co.get("company_id") or co.get("registration_number", "")
                dedup_key = f"{state_code}:{co_id}"
                if dedup_key in seen_ids:
                    continue
                seen_ids.add(dedup_key)

                # Parse address components
                addr_components = co.get("addressComponents", {})
                addr_raw = co.get("address", "")

                rec = make_business_record(
                    name=co_name,
                    state_code=state_code,
                    data_source=data_source,
                    source_id=co_id,
                    address=addr_components.get("street") or addr_raw,
                    city=addr_components.get("city"),
                    state=addr_components.get("state", state_code),
                    zip_code=addr_components.get("zip"),
                    entity_type=co.get("company_type"),
                    formation_date=co.get("incorporation_date"),
                    status_str=co.get("status") or co.get("current_status"),
                )
                batch.append(rec)

        if batch and not dry_run:
            for i in range(0, len(batch), BATCH_SIZE):
                chunk = batch[i:i + BATCH_SIZE]
                ok = db.upsert("businesses", chunk, on_conflict="data_source,source_id")
                if ok:
                    total_inserted += len(chunk)

        log(f"[{state_code}] Apify '{keyword}': {len(batch)} records for {state_code}")
        time.sleep(2)  # Between Apify runs

    progress[f"{state_code}_done"] = True
    progress[f"{state_code}_count"] = total_inserted
    log(f"[{state_code}] DONE — {total_inserted} records via Apify")
    return total_inserted


# ═══════════════════════════════════════════════════════════════════════
# MAIN DISPATCH
# ═══════════════════════════════════════════════════════════════════════

def dispatch_state(state_code, db, progress, dry_run=False):
    """Run the appropriate scraper for a state."""
    cfg = STATE_CONFIG.get(state_code)
    if not cfg:
        log(f"[{state_code}] No configuration found. Skipping.")
        return 0

    method_name = cfg["method"]

    if method_name == "skip":
        log(f"[{state_code}] SKIP — {cfg['notes']}")
        return 0

    log(f"[{state_code}] Tier {cfg['tier']} — {cfg['method']} — Est. cost: {cfg['cost']}")

    if method_name == "scrape_texas_opendata":
        return scrape_texas_opendata(db, progress, dry_run)
    elif method_name == "scrape_colorado_opendata":
        return scrape_colorado_opendata(db, progress, dry_run)
    elif method_name == "scrape_ohio_reports":
        return scrape_ohio_reports(db, progress, dry_run)
    elif method_name == "scrape_opencorporates":
        return scrape_opencorporates(db, progress, dry_run, state_code=state_code)
    elif method_name == "scrape_apify_openactor":
        return scrape_apify_openactor(db, progress, dry_run, state_code=state_code)
    else:
        log(f"[{state_code}] Unknown method: {method_name}")
        return 0


def print_state_report():
    """Print a summary of all states, their tiers, and estimated costs."""
    print("\n" + "=" * 90)
    print("  US SECRETARY OF STATE — CAR WASH ENTITY SCRAPING PLAN")
    print("=" * 90)

    tiers = {1: [], 2: [], 3: [], 4: [], 5: []}
    for st, cfg in sorted(STATE_CONFIG.items()):
        tiers[cfg["tier"]].append((st, cfg))

    tier_labels = {
        1: "TIER 1 — FREE Open Data APIs (Socrata)",
        2: "TIER 2 — FREE Bulk Downloads (CSV/TXT)",
        3: "TIER 3 — OpenCorporates API (free: 500 req/month)",
        4: "TIER 4 — Apify Paid Actors ($0.002/result)",
        5: "TIER 5 — Already in Database (skip)",
    }

    for tier_num in [1, 2, 3, 4, 5]:
        states = tiers[tier_num]
        if not states:
            continue
        print(f"\n  {tier_labels[tier_num]}")
        print("  " + "-" * 70)
        for st, cfg in states:
            notes_short = cfg["notes"][:65]
            print(f"    {st}  cost={cfg['cost']:12s}  {notes_short}")

    print(f"\n  TOTAL STATES: {len(STATE_CONFIG)}")
    free_count = sum(1 for c in STATE_CONFIG.values() if c["tier"] <= 3)
    paid_count = sum(1 for c in STATE_CONFIG.values() if c["tier"] == 4)
    skip_count = sum(1 for c in STATE_CONFIG.values() if c["tier"] == 5)
    print(f"  FREE (Tiers 1-3): {free_count} states")
    print(f"  PAID (Tier 4):    {paid_count} states — est. ${paid_count * 0.40:.2f} total")
    print(f"  SKIP (Tier 5):    {skip_count} states (already loaded)")
    print(f"\n  ESTIMATED TOTAL COST: ${paid_count * 0.40:.2f}")
    print("=" * 90 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape US Secretary of State databases for car wash business entities."
    )
    parser.add_argument("--state", type=str, help="Run only a specific state (e.g., TX, CA)")
    parser.add_argument("--free-only", action="store_true", help="Only run free tiers (1-3)")
    parser.add_argument("--resume", action="store_true", help="Resume from saved progress")
    parser.add_argument("--dry-run", action="store_true", help="No DB writes, just log")
    parser.add_argument("--report", action="store_true", help="Print state plan and exit")
    parser.add_argument("--tier", type=int, help="Only run states in this tier (1-4)")
    args = parser.parse_args()

    if args.report:
        print_state_report()
        return

    # Load or reset progress
    if args.resume:
        progress = load_progress(PROGRESS_FILE, {})
        log("Resuming from saved progress.")
    else:
        progress = {}

    db = SupabaseClient()
    grand_total = 0

    # Determine which states to process
    if args.state:
        states = [args.state.upper()]
    else:
        # Process in tier order (cheapest first), then by state priority
        tier_order = [1, 2, 3, 4] if not args.free_only else [1, 2, 3]
        if args.tier:
            tier_order = [args.tier]
        states = []
        for t in tier_order:
            tier_states = [
                st for st, cfg in STATE_CONFIG.items()
                if cfg["tier"] == t
            ]
            states.extend(sorted(tier_states))

    log(f"Processing {len(states)} states: {', '.join(states)}")
    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'} | "
        f"Free only: {args.free_only} | Resume: {args.resume}")

    for state_code in states:
        try:
            count = dispatch_state(state_code, db, progress, args.dry_run)
            grand_total += count
            save_progress(progress, PROGRESS_FILE)
            log(f"--- {state_code} complete: {count} records. Running total: {grand_total} ---")
        except KeyboardInterrupt:
            log("Interrupted! Saving progress...")
            save_progress(progress, PROGRESS_FILE)
            sys.exit(1)
        except Exception as e:
            log(f"[{state_code}] ERROR: {e}")
            save_progress(progress, PROGRESS_FILE)
            continue

    save_progress(progress, PROGRESS_FILE)
    log(f"\n{'='*60}")
    log(f"ALL DONE — Grand total: {grand_total} car wash entities inserted")
    log(f"{'='*60}")

    # Print summary
    print("\nPer-state results:")
    for st in sorted(STATE_CONFIG.keys()):
        cnt = progress.get(f"{st}_count", 0)
        done = progress.get(f"{st}_done", False)
        partial = progress.get(f"{st}_partial", False)
        status = "DONE" if done else ("PARTIAL" if partial else "PENDING")
        if cnt or done:
            print(f"  {st}: {cnt:6d} records  [{status}]")


if __name__ == "__main__":
    main()
