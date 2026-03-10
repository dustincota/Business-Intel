#!/usr/bin/env python3
"""
Ingest SAM.gov (System for Award Management) entity data into the businesses table.

Data source: https://sam.gov/data-services
API docs:    https://open.gsa.gov/api/entity-api/
Extract API: https://open.gsa.gov/api/sam-entity-extracts-api/

SAM.gov contains ~700K+ active registered entities (government contractors and
grant recipients). Each entity has:
  - UEI (Unique Entity Identifier), CAGE code
  - Legal business name, DBA name
  - Physical address (full)
  - NAICS codes with SBA small business indicator
  - Entity structure (LLC, Corp, Sole Prop, etc.)
  - Registration status and dates

For M&A intelligence, SAM.gov provides:
  - Verified business addresses and legal names
  - NAICS classifications (often more accurate than SOS filings)
  - Government contract activity (signals revenue & capability)
  - SBA size standard indicators (helps identify lower-middle-market)

TWO INGESTION MODES:

Mode 1: API (default) — uses Entity Management API v3
  - Free API key from https://sam.gov/profile/details
  - 10 records per page, max 10K per query
  - Rate limits: 1000/day (personal key) or 10000/day (system account)
  - We query by NAICS prefix to stay under limits

Mode 2: Extract — downloads monthly bulk extract
  - Same API key required
  - Gets a ZIP of pipe-delimited files
  - Much faster for full ingestion (~700K entities)
  - Monthly files: SAM_PUBLIC_MONTHLY_V2_YYYYMMDD.ZIP

Required env: SAM_GOV_API_KEY (get free at sam.gov)
Optional env: SAM_GOV_MODE=api|extract (default: api)

Run: nohup python3 ingest_sam_gov.py >> ingest_sam_stdout.log 2>&1 &
"""

import csv
import io
import json
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

log = make_logger("ingest_sam_gov.log")
PROGRESS_FILE = "ingest_sam_gov_progress.json"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "sam_data")

# ═══════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════

# SAM.gov API key — register free at https://sam.gov/profile/details
SAM_API_KEY = os.getenv("SAM_GOV_API_KEY", "")

# Ingestion mode: 'api' or 'extract'
SAM_MODE = os.getenv("SAM_GOV_MODE", "api")

# API endpoints
SAM_API_V3 = "https://api.sam.gov/entity-information/v3/entities"
SAM_EXTRACT_API = "https://api.sam.gov/data-services/v1/extracts"

# Rate limiting
API_DELAY_SECONDS = 1.0  # Delay between API calls
API_PAGE_SIZE = 10       # Max 10 per page (SAM.gov limitation)
API_MAX_PAGES = 1000     # Max pages per NAICS query (= 10K records)

# Batch sizes
UPSERT_BATCH_SIZE = 200

# ═══════════════════════════════════════════════════════════
# NAICS CODES TO QUERY (high-value M&A sectors)
# ═══════════════════════════════════════════════════════════

# We query by 2-digit NAICS prefix to capture all sub-codes.
# SAM.gov supports filtering by primaryNaics (6-digit) or naicsCode.
# For broader coverage we query 2-digit sectors and then map.
#
# Focus on sectors most relevant to $5M-$200M M&A targets.

PRIORITY_NAICS_QUERIES = [
    # Tier 1: Core M&A sectors (query these first)
    ("811192", "Car Washes"),
    ("811111", "General Auto Repair"),
    ("811112", "Auto Exhaust Repair"),
    ("811118", "Other Auto Repair"),
    ("811191", "Auto Oil Change/Lube"),
    ("722511", "Full-Service Restaurants"),
    ("722513", "Limited-Service Restaurants"),
    ("447110", "Gas Stations w/ Conv"),
    ("447190", "Other Gas Stations"),
    ("812310", "Coin-Op Laundries"),
    ("812320", "Dry Cleaning/Laundry"),
    ("713940", "Fitness Centers"),
    ("621210", "Dental Offices"),
    ("621111", "Physician Offices"),
    ("561730", "Landscaping"),
    ("238220", "Plumbing/HVAC"),
    ("238210", "Electrical Contractors"),
    ("238160", "Roofing"),
    ("484110", "Local Freight Trucking"),
    ("484121", "Long-Distance TL"),
    ("562111", "Solid Waste Collection"),
    ("541511", "Custom Programming"),
    ("541512", "Computer Systems Design"),
    ("721110", "Hotels/Motels"),
    ("624410", "Child Day Care"),
    ("812111", "Barber Shops"),
    ("812112", "Beauty Salons"),

    # Tier 2: Broader sectors (run after Tier 1)
    ("236220", "Commercial Construction"),
    ("236118", "Residential Remodelers"),
    ("238990", "Other Specialty Trade"),
    ("332710", "Machine Shops"),
    ("493110", "General Warehousing"),
    ("524210", "Insurance Agencies"),
    ("531210", "RE Agents/Brokers"),
    ("541110", "Law Offices"),
    ("541211", "CPA Offices"),
    ("561320", "Temporary Staffing"),
    ("446110", "Pharmacies"),
    ("441110", "New Car Dealers"),
    ("441120", "Used Car Dealers"),
    ("323111", "Commercial Printing"),
    ("811198", "All Other Auto Repair"),
]

# NAICS to business_type mapping (reuse from census script)
from ingest_census_businesses import NAICS_TO_BUSINESS_TYPE, NAICS_SECTOR_NAMES


# Entity structure codes to readable types
ENTITY_STRUCTURE_MAP = {
    "2L": "LLC",
    "2A": "Corporation",
    "2K": "Partnership",
    "8H": "Sole Proprietorship",
    "CY": "Municipality",
    "X6": "Federal Government",
    "ZZ": "Other",
}


# ═══════════════════════════════════════════════════════════
# SAM.gov API CLIENT
# ═══════════════════════════════════════════════════════════

class SamGovClient:
    """Client for SAM.gov Entity Management API v3."""

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "M&A-Intelligence-Platform/1.0",
        })
        self.last_call = 0
        self.calls_today = 0
        self.daily_limit = 1000  # Personal key limit

    def _throttle(self):
        """Rate limit API calls."""
        elapsed = time.time() - self.last_call
        if elapsed < API_DELAY_SECONDS:
            time.sleep(API_DELAY_SECONDS - elapsed)
        self.last_call = time.time()
        self.calls_today += 1

        if self.calls_today >= self.daily_limit:
            log(f"WARNING: Approaching daily API limit ({self.calls_today} calls)")

    def search_entities(self, naics_code=None, state=None, registration_status="A",
                        page=0, size=10, include_sections=None):
        """
        Search SAM.gov entities.

        Args:
            naics_code: 6-digit NAICS code to filter by primaryNaics
            state: 2-letter state code to filter by physical address state
            registration_status: 'A' for Active, 'E' for Expired
            page: Page number (0-indexed)
            size: Records per page (max 10)
            include_sections: Comma-separated sections to include

        Returns: dict with entityData list, or None on error
        """
        self._throttle()

        params = {
            "api_key": self.api_key,
            "registrationStatus": registration_status,
            "samRegistered": "Yes",
            "page": page,
            "size": min(size, 10),
        }

        if naics_code:
            params["primaryNaics"] = naics_code

        if state:
            params["physicalAddressProvinceOrStateCode"] = state

        if include_sections:
            params["includeSections"] = include_sections
        else:
            params["includeSections"] = "entityRegistration,coreData"

        try:
            r = self.session.get(SAM_API_V3, params=params, timeout=30)

            if r.status_code == 200:
                data = r.json()
                return data

            if r.status_code == 429:
                log(f"  Rate limited — waiting 60s")
                time.sleep(60)
                return None

            if r.status_code == 403:
                log(f"  API key unauthorized or expired (403)")
                return None

            log(f"  SAM API error {r.status_code}: {r.text[:200]}")
            return None

        except Exception as e:
            log(f"  SAM API exception: {e}")
            return None

    def get_extract_download_url(self, file_type="ENTITY", sensitivity="PUBLIC",
                                  date=None):
        """
        Get the download URL for a bulk extract file.

        Args:
            file_type: ENTITY or EXCLUSION
            sensitivity: PUBLIC, FOUO, or SENSITIVE
            date: Optional date string (YYYYMMDD)

        Returns: Download URL string or None
        """
        self._throttle()

        params = {
            "api_key": self.api_key,
            "fileType": file_type,
            "sensitivity": sensitivity,
        }
        if date:
            params["date"] = date

        try:
            r = self.session.get(SAM_EXTRACT_API, params=params, timeout=30)
            if r.status_code == 200:
                # Response may be a redirect or contain a download link
                content_type = r.headers.get("Content-Type", "")
                if "json" in content_type:
                    data = r.json()
                    return data
                else:
                    # Direct file download
                    return r.url
            log(f"  Extract API error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log(f"  Extract API exception: {e}")
        return None


# ═══════════════════════════════════════════════════════════
# ENTITY PARSER
# ═══════════════════════════════════════════════════════════

def parse_entity_to_record(entity):
    """
    Parse a SAM.gov API entity response into a businesses table record.

    The API response structure:
    {
      "entityRegistration": {
        "ueiSAM": "...",
        "cageCode": "...",
        "legalBusinessName": "...",
        "dbaName": "...",
        "registrationStatus": "Active",
        ...
      },
      "coreData": {
        "physicalAddress": {
          "addressLine1": "...",
          "city": "...",
          "stateOrProvinceCode": "...",
          "zipCode": "...",
          "countryCode": "USA"
        },
        "entityInformation": {
          "entityStructureCode": "2L",
          "entityStructureDesc": "LLC",
          ...
        },
        "generalInformation": {
          "entityStructureCode": "...",
          ...
        }
      }
    }
    """
    if not entity:
        return None

    reg = entity.get("entityRegistration", {})
    core = entity.get("coreData", {})

    uei = reg.get("ueiSAM", "")
    if not uei:
        return None

    legal_name = reg.get("legalBusinessName", "")
    dba_name = reg.get("dbaName")
    cage_code = reg.get("cageCode")
    registration_status = reg.get("registrationStatus", "")

    # Physical address
    addr = core.get("physicalAddress", {})
    address_line = addr.get("addressLine1", "")
    if addr.get("addressLine2"):
        address_line += f", {addr['addressLine2']}"
    city = addr.get("city", "")
    state = addr.get("stateOrProvinceCode", "")
    zipcode = addr.get("zipCode", "")
    country_code = addr.get("countryCode", "USA")

    # Only include US entities
    if country_code not in ("USA", "US", ""):
        return None

    # NAICS codes
    general_info = core.get("generalInformation", {})
    entity_info = core.get("entityInformation", {})

    # primaryNaics might be in different locations depending on API version
    primary_naics = None
    naics_list = []
    sba_small = None

    # Try to get NAICS from the naicsList in coreData
    naics_data = core.get("naicsList", [])
    if isinstance(naics_data, list):
        for n in naics_data:
            if isinstance(n, dict):
                code = n.get("naicsCode", "")
                if code:
                    naics_list.append(code)
                    if n.get("primaryIndicator") == "Y" or not primary_naics:
                        primary_naics = code
                    if n.get("sbaSmallBusiness") == "Y":
                        sba_small = True
                    elif n.get("sbaSmallBusiness") == "N":
                        sba_small = False

    # Entity structure
    entity_structure = entity_info.get("entityStructureCode", "")
    entity_structure_desc = entity_info.get("entityStructureDesc", "")

    # Map primary NAICS to our business_type
    business_type = None
    if primary_naics:
        business_type = NAICS_TO_BUSINESS_TYPE.get(primary_naics)
        if not business_type and len(primary_naics) >= 2:
            business_type = NAICS_SECTOR_NAMES.get(primary_naics[:2])

    # Build the display name
    display_name = legal_name
    if not display_name:
        display_name = dba_name or f"Entity {uei}"

    # Source ID for dedup
    source_id = f"sam_gov_{uei}"

    record = {
        "name": display_name,
        "legal_name": legal_name or None,
        "dba_name": dba_name or None,
        "business_type": business_type,
        "address": address_line or None,
        "city": city or None,
        "state": state or None,
        "zip": zipcode or None,
        "country": "US",
        "naics_code": primary_naics or None,
        "industry": f"NAICS {primary_naics}" if primary_naics else None,
        "owner_type": ENTITY_STRUCTURE_MAP.get(entity_structure, entity_structure_desc) or None,
        "data_source": "sam_gov",
        "source_id": source_id,
        "enrichment_status": "sam_raw",
        "status": "active" if registration_status == "Active" else "inactive",
        "notes": json.dumps({
            "uei": uei,
            "cage_code": cage_code,
            "naics_list": naics_list,
            "sba_small_business": sba_small,
            "entity_structure": entity_structure_desc,
            "registration_status": registration_status,
        }),
        "created_at": utcnow_iso(),
        "updated_at": utcnow_iso(),
    }

    return record


# ═══════════════════════════════════════════════════════════
# API INGESTION MODE
# ═══════════════════════════════════════════════════════════

def ingest_via_api(db, progress):
    """
    Ingest SAM.gov entities using the Entity Management API.
    Queries by NAICS code, paginating through results.
    """
    if not SAM_API_KEY:
        log("FATAL: SAM_GOV_API_KEY not set. Register free at https://sam.gov/profile/details")
        log("Then export SAM_GOV_API_KEY=your_key_here")
        sys.exit(1)

    client = SamGovClient(SAM_API_KEY)
    total_inserted = progress.get("api_total_inserted", 0)
    total_errors = progress.get("api_total_errors", 0)
    total_skipped = progress.get("api_total_skipped", 0)

    completed_naics = set(progress.get("api_completed_naics", []))

    for naics_code, naics_desc in PRIORITY_NAICS_QUERIES:
        if naics_code in completed_naics:
            log(f"  NAICS {naics_code} ({naics_desc}) — already done, skipping")
            continue

        log(f"\n  Querying NAICS {naics_code} ({naics_desc})...")

        page = progress.get(f"api_page_{naics_code}", 0)
        naics_inserted = 0
        naics_errors = 0
        batch = []
        empty_pages = 0

        while page < API_MAX_PAGES:
            result = client.search_entities(
                naics_code=naics_code,
                registration_status="A",
                page=page,
                size=API_PAGE_SIZE,
            )

            if result is None:
                # Retry once after a short delay
                time.sleep(5)
                result = client.search_entities(
                    naics_code=naics_code,
                    registration_status="A",
                    page=page,
                    size=API_PAGE_SIZE,
                )

            if result is None:
                naics_errors += 1
                log(f"    Page {page}: API error after retry")
                break

            entities = result.get("entityData", [])
            total_records = result.get("totalRecords", 0)

            if not entities:
                empty_pages += 1
                if empty_pages >= 3:
                    break  # No more data
                page += 1
                continue

            empty_pages = 0

            for entity in entities:
                record = parse_entity_to_record(entity)
                if record:
                    batch.append(record)
                else:
                    total_skipped += 1

            if len(batch) >= UPSERT_BATCH_SIZE:
                ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
                if ok:
                    total_inserted += len(batch)
                    naics_inserted += len(batch)
                else:
                    total_errors += len(batch)
                    naics_errors += len(batch)
                batch = []

            page += 1

            # Progress checkpoint every 10 pages
            if page % 10 == 0:
                progress[f"api_page_{naics_code}"] = page
                progress["api_total_inserted"] = total_inserted
                progress["api_total_errors"] = total_errors
                progress["api_total_skipped"] = total_skipped
                save_progress(progress, PROGRESS_FILE)

                if total_records > 0:
                    pct = min(100, (page * API_PAGE_SIZE / total_records) * 100)
                    log(f"    Page {page}/{total_records // API_PAGE_SIZE}: "
                        f"{naics_inserted} inserted ({pct:.0f}%)")

            # Check daily limit
            if client.calls_today >= client.daily_limit - 10:
                log(f"  Approaching daily API limit ({client.calls_today} calls)")
                log("  Saving progress and stopping. Re-run tomorrow to continue.")
                # Flush remaining batch
                if batch:
                    ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
                    if ok:
                        total_inserted += len(batch)
                    batch = []
                progress[f"api_page_{naics_code}"] = page
                progress["api_total_inserted"] = total_inserted
                progress["api_total_errors"] = total_errors
                progress["api_total_skipped"] = total_skipped
                save_progress(progress, PROGRESS_FILE)
                return

        # Flush remaining batch for this NAICS
        if batch:
            ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
            if ok:
                total_inserted += len(batch)
                naics_inserted += len(batch)
            else:
                total_errors += len(batch)
            batch = []

        completed_naics.add(naics_code)
        progress["api_completed_naics"] = list(completed_naics)
        progress[f"api_page_{naics_code}"] = 0  # Reset for next run
        save_progress(progress, PROGRESS_FILE)

        log(f"    NAICS {naics_code}: {naics_inserted} inserted, {naics_errors} errors")

    progress["api_total_inserted"] = total_inserted
    progress["api_total_errors"] = total_errors
    progress["api_total_skipped"] = total_skipped
    progress["api_done"] = True
    save_progress(progress, PROGRESS_FILE)

    log(f"\nAPI ingestion complete: inserted={total_inserted}, errors={total_errors}, skipped={total_skipped}")


# ═══════════════════════════════════════════════════════════
# EXTRACT (BULK DOWNLOAD) MODE
# ═══════════════════════════════════════════════════════════

def ingest_via_extract(db, progress):
    """
    Ingest SAM.gov entities from the monthly bulk extract.
    Downloads the PUBLIC extract ZIP and parses the pipe-delimited file.

    The V2 public extract contains ~700K active entities.
    Fields are pipe-delimited ("|").
    """
    if not SAM_API_KEY:
        log("FATAL: SAM_GOV_API_KEY not set")
        sys.exit(1)

    os.makedirs(DATA_DIR, exist_ok=True)

    client = SamGovClient(SAM_API_KEY)

    # Step 1: Get the extract download URL
    log("Requesting bulk extract download URL...")
    result = client.get_extract_download_url(
        file_type="ENTITY",
        sensitivity="PUBLIC",
    )

    if not result:
        log("FATAL: Could not get extract download URL")
        log("This may require a System Account with 'Read Public' permission.")
        log("Falling back to API mode...")
        ingest_via_api(db, progress)
        return

    # Step 2: Download the extract file
    download_url = result if isinstance(result, str) else None
    if isinstance(result, dict):
        # Response may contain a download link
        download_url = result.get("downloadUrl") or result.get("url")

    if not download_url:
        log("Could not determine download URL from extract API response")
        log(f"Response: {str(result)[:500]}")
        log("Falling back to API mode...")
        ingest_via_api(db, progress)
        return

    extract_path = os.path.join(DATA_DIR, "sam_public_extract.zip")
    log(f"Downloading extract: {download_url}")

    try:
        r = requests.get(download_url, stream=True, timeout=600,
                        headers={"User-Agent": "M&A-Intelligence-Platform/1.0"})
        r.raise_for_status()

        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(extract_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0 and downloaded % (10 * 1024 * 1024) == 0:
                    log(f"  Downloaded {downloaded // (1024*1024)} MB / {total // (1024*1024)} MB")

        log(f"  Downloaded: {os.path.getsize(extract_path) // (1024*1024)} MB")
    except Exception as e:
        log(f"ERROR downloading extract: {e}")
        log("Falling back to API mode...")
        ingest_via_api(db, progress)
        return

    # Step 3: Parse the extract
    log("Parsing extract file...")
    parse_extract_file(db, extract_path, progress)


def parse_extract_file(db, zip_path, progress):
    """
    Parse the SAM.gov public extract ZIP file.
    The ZIP contains one or more pipe-delimited data files.

    V2 Public Extract key columns (pipe-delimited):
    Col 0:  UEI
    Col 1:  UEI Status
    Col 2:  CAGE Code
    Col 3:  Legal Business Name
    Col 4:  DBA Name
    Col 5:  Physical Address Line 1
    Col 6:  Physical Address Line 2
    Col 7:  Physical Address City
    Col 8:  Physical Address State
    Col 9:  Physical Address ZIP
    Col 10: Physical Address ZIP+4
    Col 11: Physical Address Country Code
    Col 12: Registration Status
    Col 24: Entity Structure Code
    Col 25: Entity Structure Description
    Col 32: State of Incorporation
    Col 41: Primary NAICS
    Col 42-N: Additional NAICS codes (repeating groups)

    NOTE: Column positions may vary between extract versions.
    The script handles this by looking for header rows and field names.
    """
    start_row = progress.get("extract_row", 0)
    total_inserted = progress.get("extract_inserted", 0)
    total_errors = progress.get("extract_errors", 0)
    total_skipped = progress.get("extract_skipped", 0)

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not name.lower().endswith((".dat", ".csv", ".txt")):
                continue

            log(f"  Processing: {name}")

            with zf.open(name) as f:
                text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                row_num = 0
                header = None
                batch = []

                for line in text_stream:
                    row_num += 1
                    if row_num <= start_row:
                        continue

                    fields = line.strip().split("|")

                    # Detect header row
                    if row_num == 1 or (header is None and "UEI" in line.upper()):
                        header = [f.strip().upper() for f in fields]
                        log(f"    Header ({len(header)} cols): {header[:10]}...")
                        continue

                    if not header or len(fields) < 10:
                        total_skipped += 1
                        continue

                    # Build a dict from header + fields
                    row_dict = {}
                    for i, col_name in enumerate(header):
                        if i < len(fields):
                            row_dict[col_name] = fields[i].strip()

                    # Extract key fields
                    uei = (row_dict.get("UEI") or row_dict.get("UEISAMM")
                           or row_dict.get("UEI SAM") or "")
                    if not uei:
                        total_skipped += 1
                        continue

                    legal_name = (row_dict.get("LEGAL BUSINESS NAME")
                                  or row_dict.get("LEGAL_BUSINESS_NAME") or "")
                    dba_name = (row_dict.get("DBA NAME") or row_dict.get("DBA_NAME") or "")

                    # Address
                    address = (row_dict.get("PHYSICAL ADDRESS LINE 1")
                               or row_dict.get("PHYSICAL_ADDRESS_LINE_1") or "")
                    addr2 = (row_dict.get("PHYSICAL ADDRESS LINE 2")
                             or row_dict.get("PHYSICAL_ADDRESS_LINE_2") or "")
                    if addr2:
                        address = f"{address}, {addr2}"

                    city = (row_dict.get("PHYSICAL ADDRESS CITY")
                            or row_dict.get("PHYSICAL_ADDRESS_CITY") or "")
                    state = (row_dict.get("PHYSICAL ADDRESS PROVINCE OR STATE")
                             or row_dict.get("PHYSICAL_ADDRESS_STATE") or "")
                    zipcode = (row_dict.get("PHYSICAL ADDRESS ZIP CODE")
                               or row_dict.get("PHYSICAL_ADDRESS_ZIPPOSTAL_CODE") or "")
                    country = (row_dict.get("PHYSICAL ADDRESS COUNTRY CODE")
                               or row_dict.get("PHYSICAL_ADDRESS_COUNTRY_CODE") or "")

                    # Only US entities
                    if country and country not in ("USA", "US"):
                        total_skipped += 1
                        continue

                    # NAICS
                    primary_naics = (row_dict.get("PRIMARY NAICS")
                                     or row_dict.get("PRIMARY_NAICS") or "")
                    naics_code = primary_naics[:6] if primary_naics else None

                    # Registration status
                    reg_status = (row_dict.get("REGISTRATION STATUS")
                                  or row_dict.get("REGISTRATION_STATUS") or "")

                    # Entity structure
                    entity_struct = (row_dict.get("ENTITY STRUCTURE")
                                     or row_dict.get("ENTITY_STRUCTURE") or "")

                    # SBA small business indicator
                    sba_small = (row_dict.get("SBA BUSINESS TYPES STRING")
                                 or row_dict.get("SBA_SMALL_BUSINESS") or "")

                    # CAGE code
                    cage_code = (row_dict.get("CAGE CODE")
                                 or row_dict.get("CAGE_CODE") or "")

                    # Map NAICS to business_type
                    business_type = None
                    if naics_code:
                        business_type = NAICS_TO_BUSINESS_TYPE.get(naics_code)
                        if not business_type and len(naics_code) >= 2:
                            business_type = NAICS_SECTOR_NAMES.get(naics_code[:2])

                    display_name = legal_name or dba_name or f"Entity {uei}"
                    source_id = f"sam_gov_{uei}"

                    record = {
                        "name": display_name,
                        "legal_name": legal_name or None,
                        "dba_name": dba_name or None,
                        "business_type": business_type,
                        "address": address or None,
                        "city": city or None,
                        "state": state or None,
                        "zip": zipcode or None,
                        "country": "US",
                        "naics_code": naics_code,
                        "industry": f"NAICS {naics_code}" if naics_code else None,
                        "owner_type": entity_struct or None,
                        "data_source": "sam_gov",
                        "source_id": source_id,
                        "enrichment_status": "sam_raw",
                        "status": "active" if "active" in reg_status.lower() else "inactive",
                        "notes": json.dumps({
                            "uei": uei,
                            "cage_code": cage_code,
                            "sba_small_business": sba_small,
                            "entity_structure": entity_struct,
                        }),
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
                            progress["extract_row"] = row_num
                            progress["extract_inserted"] = total_inserted
                            progress["extract_errors"] = total_errors
                            progress["extract_skipped"] = total_skipped
                            save_progress(progress, PROGRESS_FILE)
                            log(f"    Row {row_num}: inserted={total_inserted}, "
                                f"errors={total_errors}, skipped={total_skipped}")

                # Final batch
                if batch:
                    ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
                    if ok:
                        total_inserted += len(batch)
                    else:
                        total_errors += len(batch)

    progress["extract_row"] = row_num
    progress["extract_inserted"] = total_inserted
    progress["extract_errors"] = total_errors
    progress["extract_skipped"] = total_skipped
    progress["extract_done"] = True
    save_progress(progress, PROGRESS_FILE)

    log(f"\nExtract ingestion complete: "
        f"rows={row_num}, inserted={total_inserted}, "
        f"errors={total_errors}, skipped={total_skipped}")


# ═══════════════════════════════════════════════════════════
# IRS EXEMPT ORGANIZATIONS (bonus: nonprofits with revenue)
# ═══════════════════════════════════════════════════════════

# The IRS EO BMF is available as CSV from:
# https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
#
# It contains ~1.8M tax-exempt organizations with:
#   - EIN, Name, City, State, ZIP
#   - NTEE code (nonprofit taxonomy)
#   - Revenue amount, Asset amount
#   - Subsection code (501c3, etc.)
#
# URLs for state-level files:
#   https://www.irs.gov/pub/irs-soi/eo1.csv (Region 1)
#   through eo4.csv (Region 4)
#
# This is useful for:
#   - Large nonprofits (hospitals, universities) that may be M&A targets
#   - Healthcare systems transitioning from nonprofit to for-profit
#   - Service organizations that could consolidate

IRS_BMF_URLS = {
    "region1": "https://www.irs.gov/pub/irs-soi/eo1.csv",
    "region2": "https://www.irs.gov/pub/irs-soi/eo2.csv",
    "region3": "https://www.irs.gov/pub/irs-soi/eo3.csv",
    "region4": "https://www.irs.gov/pub/irs-soi/eo4.csv",
}

def ingest_irs_exempt_orgs(db, progress):
    """
    Ingest IRS Exempt Organizations Business Master File.
    Focus on organizations with revenue > $1M (potential M&A targets).
    """
    if progress.get("irs_done"):
        log("IRS EO BMF already ingested, skipping")
        return

    log("\n--- IRS Exempt Organizations ---")
    os.makedirs(DATA_DIR, exist_ok=True)

    total_inserted = progress.get("irs_inserted", 0)
    total_skipped = progress.get("irs_skipped", 0)

    for region_name, url in IRS_BMF_URLS.items():
        if progress.get(f"irs_{region_name}_done"):
            log(f"  {region_name} already done, skipping")
            continue

        dest = os.path.join(DATA_DIR, f"irs_eo_{region_name}.csv")
        log(f"  Downloading {region_name}: {url}")

        try:
            r = requests.get(url, timeout=120,
                            headers={"User-Agent": "M&A-Intelligence-Platform/1.0"})
            r.raise_for_status()
            with open(dest, "wb") as f:
                f.write(r.content)
            log(f"    Downloaded: {len(r.content) // 1024} KB")
        except Exception as e:
            log(f"    ERROR downloading {region_name}: {e}")
            continue

        # Parse the CSV
        batch = []
        region_inserted = 0

        try:
            with open(dest, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ein = row.get("EIN", "").strip()
                    name = row.get("NAME", "").strip()
                    state = row.get("STATE", "").strip()
                    city = row.get("CITY", "").strip()
                    zipcode = row.get("ZIP", "").strip()[:5]

                    # Revenue filter: only orgs with substantial revenue
                    revenue_str = row.get("REVENUE_AMT", "0").strip()
                    try:
                        revenue = int(revenue_str)
                    except (ValueError, TypeError):
                        revenue = 0

                    # Skip tiny orgs — focus on $1M+ revenue
                    if revenue < 1_000_000:
                        total_skipped += 1
                        continue

                    # Asset amount
                    asset_str = row.get("ASSET_AMT", "0").strip()
                    try:
                        assets = int(asset_str)
                    except (ValueError, TypeError):
                        assets = 0

                    # NTEE code (nonprofit category)
                    ntee = row.get("NTEE_CD", "").strip()
                    subsection = row.get("SUBSECTION", "").strip()

                    source_id = f"irs_eo_{ein}"

                    record = {
                        "name": name,
                        "business_type": "nonprofit",
                        "city": city or None,
                        "state": state or None,
                        "zip": zipcode or None,
                        "country": "US",
                        "industry": f"NTEE {ntee}" if ntee else "Nonprofit",
                        "estimated_revenue": revenue,
                        "data_source": "irs_eo",
                        "source_id": source_id,
                        "enrichment_status": "irs_raw",
                        "status": "active",
                        "notes": json.dumps({
                            "ein": ein,
                            "ntee_code": ntee,
                            "subsection": subsection,
                            "revenue": revenue,
                            "assets": assets,
                        }),
                        "created_at": utcnow_iso(),
                        "updated_at": utcnow_iso(),
                    }

                    batch.append(record)

                    if len(batch) >= UPSERT_BATCH_SIZE:
                        ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
                        if ok:
                            total_inserted += len(batch)
                            region_inserted += len(batch)
                        batch = []

                        if region_inserted % 5000 < UPSERT_BATCH_SIZE:
                            log(f"    {region_name}: {region_inserted} inserted so far")

        except Exception as e:
            log(f"    ERROR parsing {region_name}: {e}")
            continue

        # Final batch
        if batch:
            ok = db.upsert("businesses", batch, on_conflict="data_source,source_id")
            if ok:
                total_inserted += len(batch)
                region_inserted += len(batch)

        progress[f"irs_{region_name}_done"] = True
        progress["irs_inserted"] = total_inserted
        progress["irs_skipped"] = total_skipped
        save_progress(progress, PROGRESS_FILE)
        log(f"    {region_name}: {region_inserted} orgs with $1M+ revenue inserted")

    progress["irs_done"] = True
    save_progress(progress, PROGRESS_FILE)
    log(f"  IRS EO total: {total_inserted} inserted, {total_skipped} skipped (< $1M revenue)")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    log("=" * 70)
    log("SAM.GOV + IRS ENTITY INGESTION")
    log(f"Mode: {SAM_MODE}")
    log(f"API Key: {'SET' if SAM_API_KEY else 'NOT SET'}")
    log(f"NAICS queries queued: {len(PRIORITY_NAICS_QUERIES)}")
    log("=" * 70)

    db = SupabaseClient()

    progress = load_progress(PROGRESS_FILE, {
        "started_at": utcnow_iso(),
        "mode": SAM_MODE,
        "api_total_inserted": 0,
        "api_total_errors": 0,
        "api_total_skipped": 0,
        "api_completed_naics": [],
        "extract_row": 0,
        "extract_inserted": 0,
        "irs_inserted": 0,
        "irs_skipped": 0,
    })

    # ── Phase 1: SAM.gov entities ──
    if SAM_API_KEY:
        if SAM_MODE == "extract":
            if not progress.get("extract_done"):
                log("\n--- Phase 1: SAM.gov Bulk Extract ---")
                ingest_via_extract(db, progress)
            else:
                log("\n--- Phase 1: SAM.gov extract already done ---")
        else:
            if not progress.get("api_done"):
                log("\n--- Phase 1: SAM.gov API Ingestion ---")
                ingest_via_api(db, progress)
            else:
                log("\n--- Phase 1: SAM.gov API ingestion already done ---")
    else:
        log("\nWARNING: SAM_GOV_API_KEY not set — skipping SAM.gov ingestion")
        log("Register free at https://sam.gov/profile/details")

    # ── Phase 2: IRS Exempt Organizations (no API key needed) ──
    log("\n--- Phase 2: IRS Exempt Organizations ---")
    ingest_irs_exempt_orgs(db, progress)

    # ── Summary ──
    log("\n" + "=" * 70)
    log("INGESTION COMPLETE")
    sam_inserted = (progress.get("api_total_inserted", 0)
                    + progress.get("extract_inserted", 0))
    irs_inserted = progress.get("irs_inserted", 0)
    log(f"SAM.gov entities: {sam_inserted}")
    log(f"IRS EO orgs ($1M+): {irs_inserted}")
    log(f"Total new records: {sam_inserted + irs_inserted}")
    log("=" * 70)


if __name__ == "__main__":
    main()
