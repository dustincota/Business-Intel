#!/usr/bin/env python3
"""
Ingest US Census Bureau County Business Patterns (CBP) and ZIP Code Business
Patterns (ZBP) data into the businesses table.

Data source: https://www.census.gov/programs-surveys/cbp.html
Bulk CSV files: https://www2.census.gov/programs-surveys/cbp/datasets/
API reference: https://www.census.gov/data/developers/data-sets/cbp-zbp.html

This script:
1. Downloads the ZBP detail file (establishment counts by 6-digit NAICS per ZIP)
2. Downloads the ZBP totals file (aggregate employment/payroll per ZIP)
3. Maps NAICS codes to our business_type taxonomy
4. Creates one business record per (ZIP, NAICS-6) combination
5. Optionally queries the Census API for more granular data
6. Inserts into `businesses` with data_source = 'census_cbp'

The ZBP detail file for 2023 contains ~1.4M rows covering every ZIP/NAICS
combination in the US. This yields establishment COUNTS, not individual
businesses, so each record represents "N establishments of type X in ZIP Y".
We store these as market-intelligence records that we later match against
individual business records from other sources.

Record counts (approximate):
  - zbp23detail.zip: ~1.4M rows (ZIP x 6-digit NAICS)
  - cbp23co.zip:     ~1.7M rows (County x NAICS x size class)
  - Total unique establishments tracked by CBP: ~8.1M

Run: nohup python3 ingest_census_businesses.py >> ingest_census_stdout.log 2>&1 &
"""

import csv
import io
import os
import sys
import time
import zipfile
from datetime import datetime, timezone

import requests

from enrich_utils import (
    SupabaseClient,
    make_logger,
    load_progress,
    save_progress,
    utcnow_iso,
)

log = make_logger("ingest_census_businesses.log")
PROGRESS_FILE = "ingest_census_progress.json"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "census_data")

# ═══════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════

# Census data year — 2023 is the latest as of March 2026
CENSUS_YEAR = 2023
YR = str(CENSUS_YEAR)[2:]  # "23"

# Bulk download URLs (from https://www2.census.gov/programs-surveys/cbp/datasets/)
BASE_URL = f"https://www2.census.gov/programs-surveys/cbp/datasets/{CENSUS_YEAR}"
ZBP_DETAIL_URL = f"{BASE_URL}/zbp{YR}detail.zip"
ZBP_TOTALS_URL = f"{BASE_URL}/zbp{YR}totals.zip"
CBP_COUNTY_URL = f"{BASE_URL}/cbp{YR}co.zip"

# Census API (no key required for small queries, key recommended for bulk)
CENSUS_API_BASE = "https://api.census.gov/data"
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")  # Optional, get free at api.census.gov

# Batch sizes
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB chunks for download
UPSERT_BATCH_SIZE = 200  # Records per Supabase upsert call

# ═══════════════════════════════════════════════════════════
# NAICS CODE → BUSINESS TYPE MAPPING
# ═══════════════════════════════════════════════════════════

# Complete mapping of 6-digit NAICS codes to our business_type taxonomy.
# We focus on sectors most relevant to lower-middle-market M&A.
NAICS_TO_BUSINESS_TYPE = {
    # ── Car Wash ──
    "811192": "car_wash",

    # ── Auto Services ──
    "811111": "auto_repair",       # General automotive repair
    "811112": "auto_repair",       # Automotive exhaust system repair
    "811113": "auto_repair",       # Automotive transmission repair
    "811118": "auto_repair",       # Other automotive repair
    "811121": "auto_repair",       # Automotive body/paint/interior
    "811122": "auto_repair",       # Automotive glass replacement
    "811191": "auto_repair",       # Automotive oil change/lubrication
    "811198": "auto_repair",       # All other auto repair & maintenance

    # ── Auto Dealers ──
    "441110": "auto_dealer",       # New car dealers
    "441120": "auto_dealer",       # Used car dealers
    "441210": "auto_dealer",       # Recreational vehicle dealers
    "441222": "auto_dealer",       # Boat dealers
    "441228": "auto_dealer",       # Motorcycle/ATV dealers

    # ── Gas Stations ──
    "447110": "gas_station",       # Gas stations with convenience stores
    "447190": "gas_station",       # Other gasoline stations

    # ── Restaurants / Food Service ──
    "722511": "restaurant",        # Full-service restaurants
    "722513": "restaurant",        # Limited-service restaurants
    "722514": "restaurant",        # Cafeterias/buffets
    "722515": "restaurant",        # Snack and nonalcoholic beverage bars
    "722320": "restaurant",        # Caterers
    "722330": "restaurant",        # Mobile food services
    "722410": "restaurant",        # Drinking places (alcoholic)

    # ── Laundromat / Dry Cleaning ──
    "812310": "laundromat",        # Coin-operated laundries
    "812320": "laundromat",        # Drycleaning & laundry services
    "812331": "laundromat",        # Linen supply
    "812332": "laundromat",        # Industrial launderers

    # ── Salon / Beauty ──
    "812111": "salon",             # Barber shops
    "812112": "salon",             # Beauty salons
    "812113": "salon",             # Nail salons
    "812191": "salon",             # Diet/weight reducing centers
    "812199": "salon",             # Other personal care services

    # ── Gym / Fitness ──
    "713940": "gym",               # Fitness and recreational centers

    # ── Medical / Healthcare ──
    "621111": "medical",           # Offices of physicians
    "621112": "medical",           # Offices of physicians, mental health
    "621210": "medical",           # Offices of dentists
    "621310": "medical",           # Offices of chiropractors
    "621320": "medical",           # Offices of optometrists
    "621330": "medical",           # Offices of mental health practitioners
    "621340": "medical",           # Offices of therapists (PT/OT/speech)
    "621391": "medical",           # Offices of podiatrists
    "621399": "medical",           # All other misc health practitioners
    "621410": "medical",           # Family planning centers
    "621420": "medical",           # Outpatient mental health centers
    "621491": "medical",           # HMO medical centers
    "621492": "medical",           # Kidney dialysis centers
    "621493": "medical",           # Freestanding emergency centers
    "621498": "medical",           # All other outpatient care centers
    "621511": "medical",           # Medical laboratories
    "621512": "medical",           # Diagnostic imaging centers
    "621610": "medical",           # Home health care services
    "621910": "medical",           # Ambulance services
    "621991": "medical",           # Blood/organ banks
    "621999": "medical",           # All other misc ambulatory

    # ── Veterinary ──
    "541940": "veterinary",        # Veterinary services

    # ── Dental (subset of medical for M&A targeting) ──
    # Already mapped above: 621210

    # ── Pharmacy ──
    "446110": "pharmacy",          # Pharmacies and drug stores
    "446120": "pharmacy",          # Cosmetics/beauty/perfume stores
    "446130": "pharmacy",          # Optical goods stores
    "446191": "pharmacy",          # Food/health supplement stores
    "446199": "pharmacy",          # Other health/personal care

    # ── Lodging ──
    "721110": "lodging",           # Hotels and motels
    "721120": "lodging",           # Casino hotels
    "721191": "lodging",           # Bed-and-breakfast inns
    "721199": "lodging",           # All other traveler accommodation
    "721211": "lodging",           # RV parks and campgrounds
    "721214": "lodging",           # Recreational/vacation camps

    # ── Convenience Store ──
    "445120": "convenience_store", # Convenience stores

    # ── Retail (general) ──
    "441310": "retail",            # Auto parts stores
    "442110": "retail",            # Furniture stores
    "443141": "retail",            # Electronics stores
    "443142": "retail",            # Electronics/appliance stores
    "444110": "retail",            # Home centers
    "444120": "retail",            # Paint and wallpaper stores
    "444130": "retail",            # Hardware stores
    "444190": "retail",            # Building material dealers
    "445110": "retail",            # Supermarkets/grocery
    "445210": "retail",            # Meat markets
    "445220": "retail",            # Fish/seafood markets
    "445230": "retail",            # Fruit/vegetable markets
    "445291": "retail",            # Baked goods stores
    "445292": "retail",            # Confectionery/nut stores
    "448110": "retail",            # Men's clothing stores
    "448120": "retail",            # Women's clothing stores
    "448130": "retail",            # Children's clothing stores
    "448140": "retail",            # Family clothing stores
    "448150": "retail",            # Clothing accessories stores
    "448190": "retail",            # Other clothing stores
    "448210": "retail",            # Shoe stores
    "448310": "retail",            # Jewelry stores
    "448320": "retail",            # Luggage/leather goods stores
    "451110": "retail",            # Sporting goods stores
    "451120": "retail",            # Hobby/toy/game stores
    "451130": "retail",            # Sewing/needlework stores
    "451140": "retail",            # Musical instrument stores
    "451211": "retail",            # Book stores
    "451212": "retail",            # News dealers
    "452210": "retail",            # Department stores
    "452311": "retail",            # Warehouse clubs
    "452319": "retail",            # General merchandise stores
    "453110": "retail",            # Florists
    "453210": "retail",            # Office supplies
    "453220": "retail",            # Gift/novelty/souvenir
    "453310": "retail",            # Used merchandise stores
    "453910": "retail",            # Pet/pet supplies stores
    "453920": "retail",            # Art dealers
    "453930": "retail",            # Manufactured homes dealers
    "453991": "retail",            # Tobacco stores
    "453998": "retail",            # Other misc store retailers

    # ── Real Estate ──
    "531110": "real_estate",       # Lessors of residential buildings
    "531120": "real_estate",       # Lessors of nonresidential buildings
    "531130": "real_estate",       # Lessors of miniwarehouses
    "531190": "real_estate",       # Lessors of other real estate
    "531210": "real_estate",       # Real estate agents/brokers
    "531311": "real_estate",       # Residential property managers
    "531312": "real_estate",       # Nonresidential property managers
    "531320": "real_estate",       # Real estate appraisers

    # ── Construction / Trades ──
    "236115": "trades",            # New single-family housing construction
    "236116": "trades",            # New multifamily housing construction
    "236117": "trades",            # New housing operative builders
    "236118": "trades",            # Residential remodelers
    "236210": "trades",            # Industrial building construction
    "236220": "trades",            # Commercial building construction
    "238110": "trades",            # Poured concrete foundation
    "238120": "trades",            # Structural steel/precast
    "238130": "trades",            # Framing contractors
    "238140": "trades",            # Masonry contractors
    "238150": "trades",            # Glass/glazing contractors
    "238160": "trades",            # Roofing contractors
    "238170": "trades",            # Siding contractors
    "238190": "trades",            # Other foundation/structure
    "238210": "trades",            # Electrical contractors
    "238220": "trades",            # Plumbing/HVAC contractors
    "238290": "trades",            # Other building equipment
    "238310": "trades",            # Drywall/insulation
    "238320": "trades",            # Painting/wall covering
    "238330": "trades",            # Flooring contractors
    "238340": "trades",            # Tile/terrazzo
    "238350": "trades",            # Finish carpentry
    "238390": "trades",            # Other building finishing
    "238910": "trades",            # Site preparation
    "238990": "trades",            # All other specialty trade

    # ── Landscaping ──
    "561730": "landscaping",       # Landscaping services

    # ── Pest Control ──
    "561710": "pest_control",      # Exterminating/pest control

    # ── Janitorial / Cleaning ──
    "561720": "janitorial",        # Janitorial services
    "561740": "janitorial",        # Carpet/upholstery cleaning

    # ── Insurance ──
    "524210": "insurance",         # Insurance agencies/brokerages
    "524291": "insurance",         # Claims adjusting
    "524292": "insurance",         # TPA for insurance

    # ── Financial Services ──
    "523110": "financial_services",  # Investment banking
    "523120": "financial_services",  # Securities brokerage
    "523130": "financial_services",  # Commodity contracts dealing
    "523910": "financial_services",  # Misc intermediation
    "523920": "financial_services",  # Portfolio management
    "523930": "financial_services",  # Investment advice
    "523991": "financial_services",  # Trust/fiduciary
    "523999": "financial_services",  # Misc financial investment
    "541211": "financial_services",  # Offices of CPAs
    "541213": "financial_services",  # Tax preparation
    "541214": "financial_services",  # Payroll services
    "541219": "financial_services",  # Other accounting services

    # ── Legal ──
    "541110": "legal",             # Offices of lawyers
    "541120": "legal",             # Offices of notaries
    "541191": "legal",             # Title abstract/settlement
    "541199": "legal",             # All other legal services

    # ── Technology / IT ──
    "511210": "technology",        # Software publishers
    "518210": "technology",        # Data processing/hosting
    "541511": "technology",        # Custom computer programming
    "541512": "technology",        # Computer systems design
    "541513": "technology",        # Computer facilities management
    "541519": "technology",        # Other computer services

    # ── Education ──
    "611110": "education",         # Elementary/secondary schools
    "611210": "education",         # Junior colleges
    "611310": "education",         # Colleges/universities
    "611410": "education",         # Business/secretarial schools
    "611420": "education",         # Computer training
    "611430": "education",         # Professional management training
    "611511": "education",         # Cosmetology/barber schools
    "611512": "education",         # Flight training
    "611513": "education",         # Apprenticeship training
    "611519": "education",         # Other technical trade schools
    "611610": "education",         # Fine arts schools
    "611620": "education",         # Sports/recreation instruction
    "611630": "education",         # Language schools
    "611691": "education",         # Exam preparation/tutoring
    "611692": "education",         # Automobile driving schools
    "611699": "education",         # Other misc schools
    "611710": "education",         # Educational support services

    # ── Religious ──
    "813110": "religious",         # Religious organizations

    # ── Childcare ──
    "624410": "childcare",         # Child day care services

    # ── Storage ──
    "493110": "storage",           # General warehousing/storage
    "493120": "storage",           # Refrigerated warehousing
    "493130": "storage",           # Farm product warehousing
    "493190": "storage",           # Other warehousing/storage

    # ── Parking ──
    "812930": "parking",           # Parking lots and garages

    # ── Funeral / Cemetery ──
    "812210": "funeral",           # Funeral homes
    "812220": "funeral",           # Cemeteries/crematories

    # ── Manufacturing (selected sectors) ──
    "311811": "manufacturing",     # Retail bakeries
    "312111": "manufacturing",     # Soft drink manufacturing
    "312112": "manufacturing",     # Bottled water manufacturing
    "312120": "manufacturing",     # Breweries
    "312130": "manufacturing",     # Wineries
    "312140": "manufacturing",     # Distilleries
    "323111": "manufacturing",     # Commercial printing
    "332710": "manufacturing",     # Machine shops
    "333249": "manufacturing",     # Other industrial machinery

    # ── Trucking / Logistics ──
    "484110": "trucking",          # General freight trucking, local
    "484121": "trucking",          # General freight trucking, long-distance TL
    "484122": "trucking",          # General freight trucking, long-distance LTL
    "484210": "trucking",          # Used household goods moving
    "484220": "trucking",          # Specialized freight local
    "484230": "trucking",          # Specialized freight long-distance

    # ── Waste Management ──
    "562111": "waste_management",  # Solid waste collection
    "562112": "waste_management",  # Hazardous waste collection
    "562119": "waste_management",  # Other waste collection
    "562211": "waste_management",  # Hazardous waste treatment
    "562212": "waste_management",  # Solid waste landfill
    "562213": "waste_management",  # Solid waste combustors
    "562219": "waste_management",  # Other nonhazardous waste treatment
    "562910": "waste_management",  # Remediation services
    "562920": "waste_management",  # Materials recovery facilities
    "562991": "waste_management",  # Septic tank services
    "562998": "waste_management",  # Other misc waste management

    # ── Staffing ──
    "561311": "staffing",          # Employment placement agencies
    "561312": "staffing",          # Executive search services
    "561320": "staffing",          # Temporary help services
    "561330": "staffing",          # PEOs

    # ── Security ──
    "561611": "security",          # Investigation services
    "561612": "security",          # Security guards/patrol
    "561613": "security",          # Armored car services
    "561621": "security",          # Security systems (except locksmiths)
    "561622": "security",          # Locksmiths
}

# Reverse lookup: build a set of all mapped NAICS prefixes for quick filtering
MAPPED_NAICS_SET = set(NAICS_TO_BUSINESS_TYPE.keys())

# NAICS 2-digit sector descriptions (for records we can't map to 6-digit)
NAICS_SECTOR_NAMES = {
    "11": "agriculture",
    "21": "mining",
    "22": "utilities",
    "23": "construction",
    "31": "manufacturing", "32": "manufacturing", "33": "manufacturing",
    "42": "wholesale",
    "44": "retail", "45": "retail",
    "48": "transportation", "49": "transportation",
    "51": "information",
    "52": "finance",
    "53": "real_estate",
    "54": "professional_services",
    "55": "management",
    "56": "admin_support",
    "61": "education",
    "62": "healthcare",
    "71": "entertainment",
    "72": "hospitality",
    "81": "other_services",
    "92": "public_admin",
}

# Employee size class midpoints for estimation
# ZBP detail file has columns: N1_4, N5_9, N10_19, N20_49, N50_99,
# N100_249, N250_499, N500_999, N1000
SIZE_CLASS_MIDPOINTS = {
    "N1_4": 2,
    "N5_9": 7,
    "N10_19": 14,
    "N20_49": 34,
    "N50_99": 74,
    "N100_249": 174,
    "N250_499": 374,
    "N500_999": 749,
    "N1000": 1500,
}

# Revenue-per-employee estimates by sector (rough, for M&A sizing)
REVENUE_PER_EMPLOYEE = {
    "car_wash": 80_000,
    "auto_repair": 120_000,
    "restaurant": 65_000,
    "laundromat": 150_000,
    "salon": 60_000,
    "gym": 50_000,
    "medical": 200_000,
    "lodging": 55_000,
    "retail": 180_000,
    "trades": 150_000,
    "technology": 250_000,
    "financial_services": 300_000,
    "legal": 200_000,
    "trucking": 200_000,
    "waste_management": 180_000,
    "staffing": 100_000,
    "default": 130_000,
}


# ═══════════════════════════════════════════════════════════
# DOWNLOAD HELPERS
# ═══════════════════════════════════════════════════════════

def ensure_data_dir():
    """Create the census_data directory if it doesn't exist."""
    os.makedirs(DATA_DIR, exist_ok=True)


def download_file(url, dest_path):
    """Download a file with progress logging. Skips if already exists."""
    if os.path.exists(dest_path):
        size_mb = os.path.getsize(dest_path) / (1024 * 1024)
        log(f"  Already downloaded: {os.path.basename(dest_path)} ({size_mb:.1f} MB)")
        return True

    log(f"  Downloading: {url}")
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "M&A-Intelligence-Platform/1.0"})
        r = session.get(url, stream=True, timeout=120)
        r.raise_for_status()

        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0 and downloaded % (5 * DOWNLOAD_CHUNK_SIZE) == 0:
                    pct = (downloaded / total) * 100
                    log(f"    {pct:.0f}% ({downloaded // (1024*1024)} MB)")

        size_mb = os.path.getsize(dest_path) / (1024 * 1024)
        log(f"  Downloaded: {os.path.basename(dest_path)} ({size_mb:.1f} MB)")
        return True
    except Exception as e:
        log(f"  ERROR downloading {url}: {e}")
        if os.path.exists(dest_path):
            os.remove(dest_path)
        return False


def extract_csv_from_zip(zip_path):
    """Extract the first CSV/TXT file from a ZIP archive. Returns file path."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if name.lower().endswith((".csv", ".txt", ".dat")):
                dest = os.path.join(DATA_DIR, name)
                if not os.path.exists(dest):
                    zf.extract(name, DATA_DIR)
                    log(f"  Extracted: {name}")
                else:
                    log(f"  Already extracted: {name}")
                return dest
    return None


# ═══════════════════════════════════════════════════════════
# CENSUS API QUERIES
# ═══════════════════════════════════════════════════════════

def query_census_api(year, dataset, get_vars, filters, geography):
    """
    Query the Census Bureau API.

    Args:
        year: Data year (e.g. 2021)
        dataset: Dataset name (e.g. "zbp", "cbp")
        get_vars: Comma-separated variables (e.g. "ESTAB,EMPSZES")
        filters: Dict of filter params (e.g. {"NAICS2017": "811192"})
        geography: Geography spec (e.g. "for=zipcode:*")

    Returns: List of dicts
    """
    url = f"{CENSUS_API_BASE}/{year}/{dataset}"
    params = {"get": get_vars}
    params.update(filters)

    # Parse geography
    for part in geography.split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            params[k] = v

    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    try:
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            data = r.json()
            if len(data) > 1:
                headers = data[0]
                return [dict(zip(headers, row)) for row in data[1:]]
        elif r.status_code == 204:
            return []  # No data
        else:
            log(f"  Census API error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log(f"  Census API exception: {e}")
    return []


def query_zbp_by_naics(naics_code, year=None):
    """
    Query ZBP data for a specific NAICS code across all ZIP codes.
    Uses the Census API to get establishment counts.

    Note: The API may not support wildcard ZIP queries for all years.
    For bulk data, use the CSV download instead.
    """
    year = year or CENSUS_YEAR
    # For years >= 2019, ZBP is part of CBP API
    if year >= 2019:
        dataset = "cbp"
        naics_key = "NAICS2017"
    else:
        dataset = "zbp"
        naics_key = "NAICS2017"

    return query_census_api(
        year=year,
        dataset=dataset,
        get_vars="ESTAB,EMP,PAYANN",
        filters={naics_key: naics_code},
        geography="for=zipcode:*",
    )


def query_cbp_state_summary(naics_code, year=None):
    """
    Get state-level summary for a NAICS code.
    Useful for quick national overview.
    """
    year = year or CENSUS_YEAR
    return query_census_api(
        year=year,
        dataset="cbp",
        get_vars="ESTAB,EMP,PAYANN",
        filters={"NAICS2017": naics_code},
        geography="for=state:*",
    )


# ═══════════════════════════════════════════════════════════
# ZBP DETAIL FILE PARSER
# ═══════════════════════════════════════════════════════════

def _build_zbp_note(census_year, est_count, zipcode, naics, row):
    """Build a notes string for a ZBP detail record (avoids f-string backslash issues)."""
    size_parts = []
    for k in SIZE_CLASS_MIDPOINTS:
        val = row.get(k, "0").strip().strip('"')
        if val != "0":
            size_parts.append(f"{k}={val}")
    size_dist = ", ".join(size_parts)
    return (
        f"Census ZBP {census_year}: {est_count} establishments in ZIP {zipcode}. "
        f"NAICS {naics}. Size distribution: {size_dist}"
    )


def parse_zbp_detail(csv_path, progress):
    """
    Parse the ZBP detail file (ZIP x 6-digit NAICS x establishment size counts).

    Record layout (zbp{YR}detail.txt):
      ZIP, NAICS, EST, N1_4, N5_9, N10_19, N20_49, N50_99,
      N100_249, N250_499, N500_999, N1000

    Yields dicts ready for Supabase insertion.
    """
    start_row = progress.get("zbp_detail_row", 0)
    row_num = 0
    skipped = 0
    yielded = 0

    log(f"Parsing ZBP detail file: {csv_path}")
    log(f"  Resuming from row {start_row}")

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_num += 1
            if row_num <= start_row:
                continue

            naics = row.get("NAICS", "").strip().strip('"')
            zipcode = row.get("ZIP", "").strip().strip('"')
            est_str = row.get("EST", "0").strip().strip('"')

            # Skip if no valid data
            if not naics or not zipcode or naics == "------":
                skipped += 1
                continue

            # Parse establishment count
            try:
                est_count = int(est_str)
            except (ValueError, TypeError):
                est_count = 0

            if est_count <= 0:
                skipped += 1
                continue

            # Map NAICS to business type
            business_type = NAICS_TO_BUSINESS_TYPE.get(naics)
            if not business_type:
                # Try 2-digit sector fallback
                sector = naics[:2] if len(naics) >= 2 else None
                business_type = NAICS_SECTOR_NAMES.get(sector)

            if not business_type:
                skipped += 1
                continue

            # Estimate weighted average employee count per establishment
            total_weighted_emp = 0
            total_est_in_sizes = 0
            for size_col, midpoint in SIZE_CLASS_MIDPOINTS.items():
                count_str = row.get(size_col, "0").strip().strip('"')
                try:
                    count = int(count_str)
                except (ValueError, TypeError):
                    count = 0
                total_weighted_emp += count * midpoint
                total_est_in_sizes += count

            avg_employees = None
            if total_est_in_sizes > 0:
                avg_employees = round(total_weighted_emp / total_est_in_sizes)

            # Estimate revenue per establishment
            rev_per_emp = REVENUE_PER_EMPLOYEE.get(
                business_type, REVENUE_PER_EMPLOYEE["default"]
            )
            estimated_revenue = None
            if avg_employees:
                estimated_revenue = avg_employees * rev_per_emp

            # Build NAICS description for the industry field
            industry_desc = f"NAICS {naics}"

            # Create a unique source_id for dedup
            source_id = f"census_zbp_{CENSUS_YEAR}_{zipcode}_{naics}"

            record = {
                "name": f"{business_type.replace('_', ' ').title()} establishments ({est_count})",
                "business_type": business_type,
                "zip": zipcode,
                "country": "US",
                "naics_code": naics,
                "industry": industry_desc,
                "employee_count": avg_employees,
                "estimated_revenue": estimated_revenue,
                "data_source": "census_cbp",
                "source_id": source_id,
                "enrichment_status": "census_aggregate",
                "notes": _build_zbp_note(CENSUS_YEAR, est_count, zipcode, naics, row),
                "status": "active",
                "created_at": utcnow_iso(),
                "updated_at": utcnow_iso(),
            }

            yielded += 1
            yield row_num, record

    log(f"  Parsed {row_num} total rows, yielded {yielded}, skipped {skipped}")


# ═══════════════════════════════════════════════════════════
# ZBP TOTALS FILE PARSER (enriches ZIP-level employment data)
# ═══════════════════════════════════════════════════════════

def load_zbp_totals(csv_path):
    """
    Load ZIP-level aggregate stats from ZBP totals file.

    Record layout (zbp{YR}totals.txt):
      ZIP, NAME, EMPFLAG, EMP_NF, EMP, QP1_NF, QP1, AP_NF, AP, EST,
      CITY, STABBR, CTY_NAME

    Returns dict: {zipcode: {city, state, county, total_est, total_emp, annual_payroll}}
    """
    zip_data = {}
    log(f"Loading ZBP totals: {csv_path}")

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            zipcode = row.get("ZIP", "").strip().strip('"')
            if not zipcode:
                continue

            try:
                total_est = int(row.get("EST", "0").strip().strip('"'))
            except (ValueError, TypeError):
                total_est = 0

            try:
                total_emp = int(row.get("EMP", "0").strip().strip('"'))
            except (ValueError, TypeError):
                total_emp = 0

            try:
                annual_payroll = int(row.get("AP", "0").strip().strip('"'))
            except (ValueError, TypeError):
                annual_payroll = 0

            zip_data[zipcode] = {
                "city": row.get("CITY", "").strip().strip('"'),
                "state": row.get("STABBR", "").strip().strip('"'),
                "county": row.get("CTY_NAME", "").strip().strip('"'),
                "total_est": total_est,
                "total_emp": total_emp,
                "annual_payroll_k": annual_payroll,  # in thousands
            }

    log(f"  Loaded {len(zip_data)} ZIP codes from totals file")
    return zip_data


# ═══════════════════════════════════════════════════════════
# MAIN INGESTION
# ═══════════════════════════════════════════════════════════

def ingest_zbp_detail(db, zip_data, progress):
    """Ingest ZBP detail records into the businesses table."""
    detail_zip = os.path.join(DATA_DIR, f"zbp{YR}detail.zip")
    if not download_file(ZBP_DETAIL_URL, detail_zip):
        log("FATAL: Could not download ZBP detail file")
        return

    csv_path = extract_csv_from_zip(detail_zip)
    if not csv_path:
        log("FATAL: Could not extract CSV from ZBP detail ZIP")
        return

    batch = []
    total_inserted = progress.get("zbp_detail_inserted", 0)
    total_errors = progress.get("zbp_detail_errors", 0)
    last_row = progress.get("zbp_detail_row", 0)

    for row_num, record in parse_zbp_detail(csv_path, progress):
        zipcode = record.get("zip", "")

        # Enrich with ZIP-level city/state/county from totals
        if zipcode in zip_data:
            zd = zip_data[zipcode]
            record["city"] = zd.get("city") or None
            record["state"] = zd.get("state") or None
            record["county"] = zd.get("county") or None

        batch.append(record)

        if len(batch) >= UPSERT_BATCH_SIZE:
            ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
            if ok:
                total_inserted += len(batch)
            else:
                total_errors += len(batch)
                log(f"  ERROR upserting batch at row {row_num}")

            batch = []
            last_row = row_num

            # Progress checkpoint every 5000 records
            if total_inserted % 5000 < UPSERT_BATCH_SIZE:
                progress["zbp_detail_row"] = last_row
                progress["zbp_detail_inserted"] = total_inserted
                progress["zbp_detail_errors"] = total_errors
                save_progress(progress, PROGRESS_FILE)
                log(f"  Progress: row={last_row}, inserted={total_inserted}, errors={total_errors}")

    # Final batch
    if batch:
        ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
        if ok:
            total_inserted += len(batch)
        else:
            total_errors += len(batch)

    progress["zbp_detail_row"] = last_row
    progress["zbp_detail_inserted"] = total_inserted
    progress["zbp_detail_errors"] = total_errors
    progress["zbp_detail_done"] = True
    save_progress(progress, PROGRESS_FILE)

    log(f"ZBP Detail ingestion complete: inserted={total_inserted}, errors={total_errors}")


def ingest_cbp_county(db, progress):
    """
    Ingest County Business Patterns data (county x NAICS x size class).
    This provides more granular geographic data than ZBP.

    Record layout (cbp{YR}co.txt):
      FIPSTATE, FIPSCTY, NAICS, EMPFLAG, EMP_NF, EMP, QP1_NF, QP1,
      AP_NF, AP, EST, N1_4, N5_9, N10_19, N20_49, N50_99,
      N100_249, N250_499, N500_999, N1000,
      N1000_1, N1000_2, N1000_3, N1000_4,
      CENESSION
    """
    county_zip = os.path.join(DATA_DIR, f"cbp{YR}co.zip")
    if not download_file(CBP_COUNTY_URL, county_zip):
        log("FATAL: Could not download CBP county file")
        return

    csv_path = extract_csv_from_zip(county_zip)
    if not csv_path:
        log("FATAL: Could not extract CSV from CBP county ZIP")
        return

    start_row = progress.get("cbp_county_row", 0)
    total_inserted = progress.get("cbp_county_inserted", 0)
    total_errors = progress.get("cbp_county_errors", 0)

    log(f"Parsing CBP county file: {csv_path}")
    log(f"  Resuming from row {start_row}")

    batch = []
    row_num = 0

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_num += 1
            if row_num <= start_row:
                continue

            naics = row.get("NAICS", "").strip().strip('"')
            fips_state = row.get("FIPSTATE", "").strip().strip('"')
            fips_county = row.get("FIPSCTY", "").strip().strip('"')
            est_str = row.get("EST", "0").strip().strip('"')

            # Skip non-6-digit NAICS (summary rows use shorter codes)
            if not naics or len(naics) != 6 or naics == "------":
                continue

            try:
                est_count = int(est_str)
            except (ValueError, TypeError):
                est_count = 0
            if est_count <= 0:
                continue

            # Map NAICS to business type
            business_type = NAICS_TO_BUSINESS_TYPE.get(naics)
            if not business_type:
                continue  # Only ingest mapped NAICS for county data

            try:
                emp = int(row.get("EMP", "0").strip().strip('"'))
            except (ValueError, TypeError):
                emp = None

            try:
                annual_payroll = int(row.get("AP", "0").strip().strip('"'))
            except (ValueError, TypeError):
                annual_payroll = None

            fips = f"{fips_state}{fips_county}"
            source_id = f"census_cbp_county_{CENSUS_YEAR}_{fips}_{naics}"

            avg_emp_per_est = round(emp / est_count) if emp and est_count > 0 else None
            rev_per_emp = REVENUE_PER_EMPLOYEE.get(
                business_type, REVENUE_PER_EMPLOYEE["default"]
            )
            est_revenue = avg_emp_per_est * rev_per_emp if avg_emp_per_est else None

            record = {
                "name": f"{business_type.replace('_', ' ').title()} ({est_count} in FIPS {fips})",
                "business_type": business_type,
                "county": f"FIPS {fips}",
                "country": "US",
                "naics_code": naics,
                "industry": f"NAICS {naics}",
                "employee_count": avg_emp_per_est,
                "estimated_revenue": est_revenue,
                "data_source": "census_cbp",
                "source_id": source_id,
                "enrichment_status": "census_aggregate",
                "notes": (
                    f"Census CBP {CENSUS_YEAR}: {est_count} est, "
                    f"{emp or 'N/A'} emp, ${annual_payroll or 0}K payroll. "
                    f"FIPS {fips}, NAICS {naics}."
                ),
                "status": "active",
                "created_at": utcnow_iso(),
                "updated_at": utcnow_iso(),
            }

            batch.append(record)

            if len(batch) >= UPSERT_BATCH_SIZE:
                ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
                if ok:
                    total_inserted += len(batch)
                else:
                    total_errors += len(batch)

                batch = []

                if total_inserted % 5000 < UPSERT_BATCH_SIZE:
                    progress["cbp_county_row"] = row_num
                    progress["cbp_county_inserted"] = total_inserted
                    progress["cbp_county_errors"] = total_errors
                    save_progress(progress, PROGRESS_FILE)
                    log(f"  CBP County progress: row={row_num}, inserted={total_inserted}")

    # Final batch
    if batch:
        ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
        if ok:
            total_inserted += len(batch)
        else:
            total_errors += len(batch)

    progress["cbp_county_row"] = row_num
    progress["cbp_county_inserted"] = total_inserted
    progress["cbp_county_errors"] = total_errors
    progress["cbp_county_done"] = True
    save_progress(progress, PROGRESS_FILE)

    log(f"CBP County ingestion complete: inserted={total_inserted}, errors={total_errors}")


# ═══════════════════════════════════════════════════════════
# CENSUS API MODE: Individual NAICS queries
# ═══════════════════════════════════════════════════════════

def api_ingest_naics(db, naics_code, progress):
    """
    Use the Census API to pull establishment data for a specific NAICS code.
    This is an alternative/supplement to bulk CSV ingestion.

    Useful for:
    - Getting the freshest data (API may have newer data than CSV)
    - Targeting specific NAICS codes of interest
    - Filling in employment/payroll data not in ZBP detail
    """
    api_key = f"api_{naics_code}"
    if progress.get(api_key):
        log(f"  API query for NAICS {naics_code} already done, skipping")
        return

    business_type = NAICS_TO_BUSINESS_TYPE.get(naics_code, "unknown")
    log(f"  Querying Census API for NAICS {naics_code} ({business_type})...")

    # Query state-level summary first (most reliable)
    results = query_cbp_state_summary(naics_code)
    if not results:
        log(f"    No API results for NAICS {naics_code}")
        progress[api_key] = "no_data"
        return

    total_est = 0
    for r in results:
        try:
            est = int(r.get("ESTAB", "0"))
            total_est += est
        except (ValueError, TypeError):
            pass

    log(f"    NAICS {naics_code}: {total_est} total establishments across {len(results)} states")
    progress[api_key] = f"{total_est}_establishments"


# ═══════════════════════════════════════════════════════════
# TARGETED: Car Wash specific Census pull
# ═══════════════════════════════════════════════════════════

def pull_car_wash_census(db, progress):
    """
    Special-purpose pull for car wash NAICS 811192.
    Gets ZIP-level data for cross-referencing with our existing car wash records.
    """
    if progress.get("car_wash_api_done"):
        log("Car wash Census pull already done")
        return

    log("Pulling car wash (NAICS 811192) data from Census API...")

    # Try ZIP-level query (may hit API limits)
    results = query_zbp_by_naics("811192")
    if results:
        log(f"  Got {len(results)} ZIP-level results for car washes")
        batch = []
        for r in results:
            zipcode = r.get("zipcode", "")
            try:
                est = int(r.get("ESTAB", "0"))
            except (ValueError, TypeError):
                est = 0
            if est <= 0:
                continue

            try:
                emp = int(r.get("EMP", "0"))
            except (ValueError, TypeError):
                emp = None

            source_id = f"census_api_zbp_{CENSUS_YEAR}_{zipcode}_811192"
            record = {
                "name": f"Car Wash establishments ({est}) - API",
                "business_type": "car_wash",
                "zip": zipcode,
                "country": "US",
                "naics_code": "811192",
                "industry": "NAICS 811192 - Car Washes",
                "employee_count": round(emp / est) if emp and est > 0 else None,
                "data_source": "census_cbp",
                "source_id": source_id,
                "enrichment_status": "census_aggregate",
                "notes": f"Census API {CENSUS_YEAR}: {est} car wash est in ZIP {zipcode}, {emp or 'N/A'} emp",
                "status": "active",
                "created_at": utcnow_iso(),
                "updated_at": utcnow_iso(),
            }
            batch.append(record)

            if len(batch) >= UPSERT_BATCH_SIZE:
                db.upsert("businesses", batch, on_conflict="data_source,source_id")
                batch = []

        if batch:
            db.upsert("businesses", batch, on_conflict="data_source,source_id")

        log(f"  Inserted car wash Census data for ZIP codes")
    else:
        log("  ZIP-level API query returned no results (try bulk CSV instead)")

    progress["car_wash_api_done"] = True
    save_progress(progress, PROGRESS_FILE)


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    log("=" * 70)
    log("CENSUS BUSINESS PATTERNS INGESTION")
    log(f"Data year: {CENSUS_YEAR}")
    log(f"NAICS codes mapped: {len(NAICS_TO_BUSINESS_TYPE)}")
    log(f"Business types: {len(set(NAICS_TO_BUSINESS_TYPE.values()))}")
    log("=" * 70)

    ensure_data_dir()
    db = SupabaseClient()

    progress = load_progress(PROGRESS_FILE, {
        "started_at": utcnow_iso(),
        "census_year": CENSUS_YEAR,
        "zbp_detail_row": 0,
        "zbp_detail_inserted": 0,
        "zbp_detail_errors": 0,
        "cbp_county_row": 0,
        "cbp_county_inserted": 0,
        "cbp_county_errors": 0,
    })

    # ── Phase 1: Download bulk files ──
    log("\n--- Phase 1: Downloading Census data files ---")
    totals_zip = os.path.join(DATA_DIR, f"zbp{YR}totals.zip")
    download_file(ZBP_TOTALS_URL, totals_zip)

    # ── Phase 2: Load ZIP totals for city/state enrichment ──
    log("\n--- Phase 2: Loading ZIP totals for geography enrichment ---")
    totals_csv = extract_csv_from_zip(totals_zip)
    zip_data = {}
    if totals_csv:
        zip_data = load_zbp_totals(totals_csv)
    else:
        log("WARNING: Could not load ZIP totals — city/state will be empty")

    # ── Phase 3: Ingest ZBP detail (ZIP x NAICS) ──
    if not progress.get("zbp_detail_done"):
        log("\n--- Phase 3: Ingesting ZBP detail (ZIP x NAICS) ---")
        ingest_zbp_detail(db, zip_data, progress)
    else:
        log("\n--- Phase 3: ZBP detail already ingested (skipping) ---")

    # ── Phase 4: Ingest CBP county data ──
    if not progress.get("cbp_county_done"):
        log("\n--- Phase 4: Ingesting CBP county data ---")
        ingest_cbp_county(db, progress)
    else:
        log("\n--- Phase 4: CBP county already ingested (skipping) ---")

    # ── Phase 5: Car wash targeted pull via API ──
    log("\n--- Phase 5: Car wash targeted Census API pull ---")
    pull_car_wash_census(db, progress)

    # ── Phase 6: API queries for high-priority NAICS codes ──
    log("\n--- Phase 6: Census API state-level summaries ---")
    priority_naics = [
        "811192",  # Car washes
        "722511",  # Full-service restaurants
        "722513",  # Limited-service restaurants
        "811111",  # Auto repair
        "447110",  # Gas stations
        "812310",  # Laundromats
        "713940",  # Gyms
        "621210",  # Dentists
        "561730",  # Landscaping
        "238220",  # Plumbing/HVAC
    ]
    for naics in priority_naics:
        api_ingest_naics(db, naics, progress)

    save_progress(progress, PROGRESS_FILE)

    # ── Summary ──
    log("\n" + "=" * 70)
    log("CENSUS INGESTION COMPLETE")
    log(f"ZBP Detail: {progress.get('zbp_detail_inserted', 0)} records inserted")
    log(f"CBP County: {progress.get('cbp_county_inserted', 0)} records inserted")
    log(f"Total errors: {progress.get('zbp_detail_errors', 0) + progress.get('cbp_county_errors', 0)}")
    log("=" * 70)


if __name__ == "__main__":
    main()
