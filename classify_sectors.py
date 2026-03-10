#!/usr/bin/env python3
"""
classify_sectors.py — Comprehensive M&A sector classification.

Reclassifies businesses and EDGAR filings into granular M&A-relevant sectors.
Runs in 3 phases:
  1. Break out broad categories (medical→dental, trades→roofing, etc.)
  2. Classify 2.78M generic SOS entities (llc, corporation, etc.)
  3. Tag EDGAR filings with sector labels

Usage:
    python3 classify_sectors.py              # full run
    python3 classify_sectors.py --phase 1    # only phase 1
    python3 classify_sectors.py --phase 2    # only phase 2
    python3 classify_sectors.py --phase 3    # only phase 3 (EDGAR)
    python3 classify_sectors.py --dry-run    # count matches without updating
"""

import argparse
import sys
import time

import requests

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
from enrich_utils import make_logger, SUPABASE_URL, SUPABASE_KEY

log = make_logger("classify_sectors.log")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ═══════════════════════════════════════════════════════════
# M&A SECTOR TAXONOMY
# ═══════════════════════════════════════════════════════════
# Ordered by SPECIFICITY: most specific patterns first.
# Each entry: (new_type, from_types, [LIKE patterns])
#
# from_types:
#   BROAD   = reclassify from broad existing categories
#   GENERIC = classify from generic SOS entity types
#   ALL     = both broad + generic

BROAD = ["medical", "healthcare", "trades", "construction", "office",
         "retail", "other", "recreation", "sports_facility"]
GENERIC = ["llc", "corporation", "partnership", "business", "nonprofit"]
ALL = BROAD + GENERIC

# ─── PHASE 1: Break out broad categories into specific M&A sectors ───
PHASE_1_SECTORS = [
    # Dental (from medical/healthcare)
    ("dental", ["medical", "healthcare"], [
        "%dental%", "%dentist%", "%orthodon%", "%periodon%", "%endodon%",
        "%oral surg%", "%prosthodon%",
    ]),
    # Dermatology / Med Spa (from medical/healthcare)
    ("dermatology", ["medical", "healthcare"], [
        "%dermatol%", "%medspa%", "%med spa%", "%medical spa%",
        "%skin care clinic%", "%skin center%", "%aestheti%",
    ]),
    # Urgent Care (from medical/healthcare)
    ("urgent_care", ["medical", "healthcare"], [
        "%urgent care%", "%walk-in clinic%", "%walk in clinic%",
        "%minute clinic%", "%express care%", "%immedicare%",
    ]),
    # Optometry / Eye Care (from medical/healthcare)
    ("optometry", ["medical", "healthcare", "retail"], [
        "%optometr%", "%ophthalmol%", "%eye care%", "%eye center%",
        "%eye clinic%", "%vision center%", "%lasik%", "%eye doctor%",
    ]),
    # Physical Therapy (from medical/healthcare)
    ("physical_therapy", ["medical", "healthcare"], [
        "%physical therap%", "%physiotherap%", "%pt clinic%",
        "%rehab center%", "%rehabilitation center%", "%occupational therap%",
    ]),
    # Behavioral Health (from medical/healthcare)
    ("behavioral_health", ["medical", "healthcare"], [
        "%behavioral health%", "%mental health%", "%substance abuse%",
        "%addiction%", "%counseling center%", "%psychiatric%",
        "%detox%", "%sober living%", "%recovery center%",
    ]),
    # Chiropractic (from medical/healthcare)
    ("chiropractic", ["medical", "healthcare"], [
        "%chiropract%", "%spinal%", "%spine center%",
    ]),
    # Veterinary (from medical/healthcare if any slipped through)
    ("veterinary", ["medical", "healthcare"], [
        "%veterinar%", "%animal hospital%", "%animal clinic%",
        "%pet hospital%", "%pet clinic%", "%vet clinic%",
    ]),
    # Roofing (from trades/construction)
    ("roofing", ["trades", "construction"], [
        "%roofing%", "%roof repair%", "%roof %contractor%",
        "%roofer%", "%shingle%",
    ]),
    # HVAC (from trades/construction)
    ("hvac", ["trades", "construction"], [
        "%hvac%", "%heating and cooling%", "%heating & cooling%",
        "%air condition%", "%furnace%", "%heat pump%",
    ]),
    # Plumbing (from trades/construction)
    ("plumbing", ["trades", "construction"], [
        "%plumbing%", "%plumber%", "%drain %", "%sewer %",
        "%pipe %repair%", "%water heater%",
    ]),
    # Electrical (from trades/construction)
    ("electrical", ["trades", "construction"], [
        "%electric%service%", "%electric%contract%", "%electrician%",
        "%electrical%", "%wiring%",
    ]),
    # Painting (from trades/construction)
    ("painting", ["trades", "construction"], [
        "%painting%", "%painter%", "%paint contract%",
    ]),
    # Landscaping (from trades/construction)
    ("landscaping", ["trades", "construction", "other"], [
        "%landscap%", "%lawn care%", "%lawn service%", "%tree service%",
        "%tree trimm%", "%arborist%", "%grounds%keep%", "%mowing%",
        "%irrigation%",
    ]),
]

# ─── PHASE 2: Classify generic SOS entities ───
# These patterns run against llc, corporation, partnership, business, nonprofit
PHASE_2_SECTORS = [
    # === HOT M&A SECTORS (from EDGAR analysis) ===

    # Car Wash
    ("car_wash", GENERIC, [
        "%car wash%", "%carwash%", "%car-wash%", "%auto wash%",
        "%auto spa%", "%express wash%", "%quick wash%",
    ]),

    # Self Storage
    ("self_storage", GENERIC, [
        "%self stor%", "%self-stor%", "%mini stor%", "%storage facilit%",
        "%storage unit%", "%public storage%", "%storage center%",
        "%stor-it%", "%store-it%", "%u-stor%", "%u stor%",
    ]),

    # Roofing
    ("roofing", GENERIC, [
        "%roofing%", "%roof repair%", "%roofer%", "%shingle%",
        "%roof restor%", "%roof %contractor%",
    ]),

    # Dental
    ("dental", GENERIC, [
        "%dental%", "%dentist%", "%orthodon%", "%periodon%",
        "%endodon%", "%oral surg%", "%prosthodon%", "%family dent%",
    ]),

    # Senior Living
    ("senior_living", GENERIC, [
        "%assisted living%", "%senior living%", "%memory care%",
        "%nursing home%", "%elder care%", "%senior care%",
        "%retirement home%", "%retirement communit%",
        "%adult care%", "%convalescent%",
    ]),

    # RV / Marina
    ("rv_park", GENERIC, [
        "%rv park%", "%rv resort%", "%campground%", "%camping%",
        "%trailer park%", "%mobile home park%", "%marina%",
        "%boat storage%", "%boat yard%",
    ]),

    # Veterinary
    ("veterinary", GENERIC, [
        "%veterinar%", "%animal hospital%", "%animal clinic%",
        "%pet hospital%", "%pet clinic%", "%vet clinic%",
        "%animal care%", "%pet care%", "%animal emerg%",
    ]),

    # Landscaping
    ("landscaping", GENERIC, [
        "%landscap%", "%lawn care%", "%lawn service%", "%tree service%",
        "%tree trimm%", "%arborist%", "%irrigation%", "%mowing%",
        "%grounds maint%", "%turf %", "%hardscape%",
    ]),

    # Parking
    ("parking", GENERIC, [
        "%parking%", "%garage%park%", "%valet%", "%park %lot%",
    ]),

    # Waste Management
    ("waste_management", GENERIC, [
        "%waste manage%", "%waste service%", "%waste disposal%",
        "%hauling%", "%trash %", "%garbage%", "%dumpster%",
        "%recycl%", "%sanitation%", "%junk remov%",
    ]),

    # Pest Control
    ("pest_control", GENERIC, [
        "%pest control%", "%exterminat%", "%termite%", "%pest manage%",
        "%bug %", "%insect control%", "%fumigat%",
    ]),

    # Dermatology / Med Spa
    ("dermatology", GENERIC, [
        "%dermatol%", "%medspa%", "%med spa%", "%medical spa%",
        "%aestheti%", "%skin care clinic%", "%botox%",
        "%cosmetic surg%", "%plastic surg%",
    ]),

    # Behavioral Health
    ("behavioral_health", GENERIC, [
        "%behavioral health%", "%mental health%", "%substance abuse%",
        "%addiction%", "%counseling center%", "%psychiatric%",
        "%detox%", "%sober living%", "%recovery center%",
        "%treatment center%", "%rehab center%",
    ]),

    # Urgent Care
    ("urgent_care", GENERIC, [
        "%urgent care%", "%walk-in clinic%", "%walk in clinic%",
        "%express care%", "%immedicare%",
    ]),

    # Physical Therapy
    ("physical_therapy", GENERIC, [
        "%physical therap%", "%physiotherap%", "%occupational therap%",
        "%sports medicine%", "%rehab%therap%",
    ]),

    # Home Services
    ("home_services", GENERIC, [
        "%home service%", "%home repair%", "%handyman%",
        "%home improvement%", "%remodel%", "%renovation%",
        "%home restor%", "%water damage%", "%fire restor%",
        "%mold remedi%", "%carpet clean%", "%window clean%",
    ]),

    # Staffing
    ("staffing", GENERIC, [
        "%staffing%", "%temp agency%", "%employment agency%",
        "%recruiting%", "%talent acqui%", "%workforce%",
    ]),

    # Childcare
    ("childcare", GENERIC, [
        "%childcare%", "%child care%", "%daycare%", "%day care%",
        "%preschool%", "%early learn%", "%montessori%",
        "%kindercare%", "%nursery school%",
    ]),

    # Franchise (specific)
    ("franchise", GENERIC, [
        "%franchise%",
    ]),

    # === EXISTING SECTORS — expand keyword coverage ===

    # HVAC
    ("hvac", GENERIC, [
        "%hvac%", "%heating and cooling%", "%heating & cooling%",
        "%air condition%", "%furnace%", "%heat pump%",
        "%climate control%",
    ]),

    # Plumbing
    ("plumbing", GENERIC, [
        "%plumbing%", "%plumber%", "%drain service%",
        "%sewer service%", "%water heater%", "%pipe%fitting%",
    ]),

    # Electrical
    ("electrical", GENERIC, [
        "%electric%service%", "%electric%contract%", "%electrician%",
        "%electrical work%", "%wiring%",
    ]),

    # Auto Repair
    ("auto_repair", GENERIC, [
        "%auto repair%", "%mechanic%", "%auto body%", "%collision%repair%",
        "%brake %", "%muffler%", "%transmission%", "%tire %shop%",
        "%oil change%", "%lube %", "%auto glass%",
        "%auto service%", "%car repair%",
    ]),

    # Laundromat
    ("laundromat", GENERIC, [
        "%laundromat%", "%laundry%", "%coin wash%", "%wash%fold%",
        "%dry clean%", "%cleaners%dry%",
    ]),

    # Gym / Fitness
    ("gym", GENERIC, [
        "%fitness%", "%gym %", "%crossfit%", "%yoga%", "%pilates%",
        "%martial art%", "%boxing%", "%weight%lift%",
        "%personal train%", "%health club%",
    ]),

    # Salon / Beauty
    ("salon", GENERIC, [
        "%salon%", "%barber%", "%beauty%", "%hair %", "%nail %",
        "%spa %", "%massage%", "%waxing%", "%lash %",
    ]),

    # Pharmacy
    ("pharmacy", GENERIC, [
        "%pharmacy%", "%pharmacist%", "%drug store%", "%apothecary%",
        "%compounding%",
    ]),

    # Restaurant (catch more)
    ("restaurant", GENERIC, [
        "%restaurant%", "%grill%", "%pizz%", "%burger%", "%taco%",
        "%sushi%", "%diner%", "%bistro%", "%steakhouse%", "%bbq%",
        "%barbecue%", "%wing%", "%ramen%", "%noodle%", "%pho %",
        "%buffet%", "%catering%", "%food truck%",
    ]),

    # Optometry
    ("optometry", GENERIC, [
        "%optometr%", "%ophthalmol%", "%eye care%", "%eye center%",
        "%eye clinic%", "%vision center%", "%lasik%",
    ]),

    # Chiropractic
    ("chiropractic", GENERIC, [
        "%chiropract%", "%spine%center%",
    ]),

    # Insurance
    ("insurance", GENERIC, [
        "%insurance%agenc%", "%insurance%broker%", "%insurance%service%",
    ]),

    # Real Estate
    ("real_estate", GENERIC, [
        "%real estate%", "%realty%", "%property manage%",
        "%real property%",
    ]),

    # Law Firm
    ("law_firm", GENERIC, [
        "%law firm%", "%law office%", "%attorney%", "%legal service%",
        "%lawyer%",
    ]),

    # Accounting
    ("accounting", GENERIC, [
        "%accounting%", "%accountant%", "%cpa %", "%bookkeep%",
        "%tax prep%", "%tax service%",
    ]),

    # Education
    ("education", GENERIC, [
        "%school%", "%academy%", "%university%", "%college%",
        "%tutoring%", "%education%", "%learning center%",
    ]),

    # Lodging / Hotel
    ("lodging", GENERIC, [
        "%hotel%", "%motel%", "%inn %", "% inn", "%lodge%",
        "%resort%", "%bed and breakfast%", "%b&b%",
    ]),

    # Gas Station
    ("gas_station", GENERIC, [
        "%gas station%", "%fuel%", "%petroleum%", "%filling station%",
        "%petro %",
    ]),

    # Convenience Store
    ("convenience_store", GENERIC, [
        "%convenience%", "%bodega%", "%corner store%", "%mini mart%",
    ]),

    # Auto Dealer
    ("auto_dealer", GENERIC, [
        "%auto dealer%", "%car dealer%", "%motor%sale%", "%auto sale%",
        "%used car%", "%pre-owned%",
    ]),

    # Funeral Home
    ("funeral_home", GENERIC, [
        "%funeral%", "%mortuar%", "%cremation%", "%cemeter%",
        "%memorial park%",
    ]),
]

# ─── PHASE 3: EDGAR filing sector tags ───
EDGAR_SECTORS = [
    ("car_wash", ["%car wash%", "%carwash%", "%car-wash%"]),
    ("self_storage", ["%self stor%", "%storage facilit%", "%mini stor%"]),
    ("roofing", ["%roofing%", "%roof %"]),
    ("dental", ["%dental%", "%orthodon%", "%DSO %"]),
    ("senior_living", ["%assisted living%", "%senior living%", "%memory care%", "%nursing home%", "%elder care%"]),
    ("rv_park", ["%rv park%", "%rv resort%", "%campground%", "%marina%"]),
    ("parking", ["%parking%"]),
    ("veterinary", ["%veterinar%", "%animal hospital%", "%pet care%"]),
    ("landscaping", ["%landscap%", "%lawn care%", "%tree service%"]),
    ("auto_services", ["%tire %", "%auto glass%", "%oil change%", "%lube%", "%valvoline%", "%jiffy%"]),
    ("dermatology", ["%dermatol%", "%med spa%", "%medspa%", "%aestheti%"]),
    ("home_services", ["%home service%", "%home repair%", "%restoration%"]),
    ("waste_management", ["%waste%", "%hauling%", "%recycl%", "%sanitation%"]),
    ("pharmacy", ["%pharmacy%"]),
    ("behavioral_health", ["%behavioral health%", "%mental health%", "%addiction%", "%substance%"]),
    ("urgent_care", ["%urgent care%"]),
    ("physical_therapy", ["%physical therap%"]),
    ("staffing", ["%staffing%", "%recruiting%"]),
    ("pest_control", ["%pest control%", "%exterminat%", "%termite%"]),
    ("childcare", ["%child care%", "%childcare%", "%daycare%", "%preschool%"]),
    ("hvac", ["%hvac%", "%heating and cooling%", "%air condition%"]),
    ("plumbing", ["%plumbing%", "%plumber%"]),
    ("electrical", ["%electrical%", "%electrician%"]),
    ("laundry", ["%laundry%", "%laundromat%", "%coin wash%"]),
    ("gym_fitness", ["%fitness%", "%gym %", "%crossfit%"]),
    ("restaurant", ["%restaurant%", "%food%", "%dining%", "%grill%"]),
    ("franchise", ["%franchise%"]),
    ("collision", ["%collision%", "%body shop%", "%auto body%"]),
    ("insurance", ["%insurance%"]),
    ("fleet_wash", ["%truck wash%", "%fleet wash%"]),
]


def reclassify(from_types, pattern, new_type, dry_run=False):
    """Call server-side reclassify_by_pattern() function."""
    if dry_run:
        # Count matches without updating
        type_filter = ",".join(from_types)
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/businesses"
            f"?business_type=in.({type_filter})"
            f"&name=ilike.{pattern}"
            f"&select=business_id",
            headers={**HEADERS, "Prefer": "count=exact", "Range": "0-0"},
            timeout=30,
        )
        cr = resp.headers.get("content-range", "*/0")
        return int(cr.split("/")[-1]) if "/" in cr else 0

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/reclassify_by_pattern",
        headers=HEADERS,
        json={
            "p_from_types": from_types,
            "p_pattern": pattern,
            "p_new_type": new_type,
            "p_batch_size": 10000,
        },
        timeout=600,
    )
    if resp.status_code == 200:
        return resp.json()
    else:
        return f"ERROR {resp.status_code}: {resp.text[:200]}"


def tag_edgar_sector(sector, patterns):
    """Tag EDGAR filings with sector label using SQL."""
    conditions = " OR ".join(f"LOWER(entity_name) LIKE '{p}'" for p in patterns)
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/execute_sql_rw",
        headers=HEADERS,
        json={"query": f"""
            UPDATE edgar_filings SET sector = '{sector}'
            WHERE ({conditions}) AND sector IS NULL
        """},
        timeout=300,
    )
    # Fallback: use direct SQL via PostgREST if RPC not available
    if resp.status_code != 200:
        return f"RPC unavailable ({resp.status_code})"
    return resp.json()


def run_phase(sectors, phase_name, dry_run=False):
    """Run a classification phase."""
    log(f"\n{'='*60}")
    log(f"  {phase_name}")
    log(f"{'='*60}")

    grand_total = 0
    for new_type, from_types, patterns in sectors:
        type_total = 0
        for pattern in patterns:
            result = reclassify(from_types, pattern, new_type, dry_run)
            if isinstance(result, int) and result > 0:
                type_total += result
                log(f"  {new_type:<25} {pattern:<35} -> {result:>6} rows")
            elif isinstance(result, str) and result.startswith("ERROR"):
                log(f"  {new_type:<25} {pattern:<35} -> {result}")

        if type_total > 0:
            log(f"  >>> {new_type}: {type_total} total")
            grand_total += type_total

    log(f"\n  Phase total: {grand_total:,} records reclassified")
    return grand_total


def run_edgar_phase(dry_run=False):
    """Tag EDGAR filings by sector."""
    log(f"\n{'='*60}")
    log(f"  PHASE 3: Tag EDGAR filings by sector")
    log(f"{'='*60}")

    # Build one big UPDATE with CASE WHEN for efficiency
    cases = []
    for sector, patterns in EDGAR_SECTORS:
        conditions = " OR ".join(f"LOWER(entity_name) LIKE '{p}'" for p in patterns)
        cases.append(f"WHEN ({conditions}) THEN '{sector}'")

    case_sql = "\n        ".join(cases)
    sql = f"""
        UPDATE edgar_filings
        SET sector = CASE
            {case_sql}
        END
        WHERE sector IS NULL;
    """

    if dry_run:
        log("  (dry run - showing SQL)")
        log(f"  {sql[:500]}...")
        return 0

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/exec_sql",
        headers=HEADERS,
        json={"sql_text": sql},
        timeout=600,
    )
    if resp.status_code == 200:
        result = resp.json()
        log(f"  Tagged EDGAR filings: {result}")
        return result
    else:
        log(f"  Direct RPC failed ({resp.status_code}), trying alternative...")
        # Fall back to one-at-a-time approach
        total = 0
        for sector, patterns in EDGAR_SECTORS:
            for pattern in patterns:
                conditions = f"LOWER(entity_name) LIKE '{pattern}'"
                # Use PostgREST PATCH
                resp2 = requests.patch(
                    f"{SUPABASE_URL}/rest/v1/edgar_filings"
                    f"?sector=is.null&entity_name=ilike.{pattern}",
                    headers={**HEADERS, "Prefer": "return=representation,count=exact"},
                    json={"sector": sector},
                    timeout=120,
                )
                if resp2.status_code in (200, 204):
                    cr = resp2.headers.get("content-range", "*/0")
                    count = int(cr.split("/")[-1]) if "/" in cr and cr.split("/")[-1] != "*" else 0
                    if count > 0:
                        total += count
                        log(f"  {sector:<25} {pattern:<35} -> {count:>6}")
                else:
                    log(f"  {sector:<25} {pattern:<35} -> ERROR {resp2.status_code}")
        log(f"\n  Total EDGAR filings tagged: {total}")
        return total


def print_summary():
    """Print current classification summary."""
    log(f"\n{'='*60}")
    log(f"  CLASSIFICATION SUMMARY")
    log(f"{'='*60}")

    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/rpc/exec_sql",
        headers=HEADERS,
        timeout=30,
    )
    # Use simpler approach
    import json
    from enrich_utils import SupabaseClient
    db = SupabaseClient()

    # Just log key M&A sectors
    ma_sectors = [
        "car_wash", "self_storage", "roofing", "dental", "senior_living",
        "rv_park", "veterinary", "landscaping", "parking", "waste_management",
        "dermatology", "pest_control", "behavioral_health", "hvac",
        "plumbing", "electrical", "home_services", "staffing",
        "childcare", "urgent_care", "physical_therapy", "auto_repair",
        "laundromat", "gym", "salon", "pharmacy", "funeral_home",
    ]

    log(f"\n{'Sector':<25} {'Count':>10}")
    log(f"{'-'*36}")
    for sector in ma_sectors:
        count = db.fetch_count("businesses", filters=f"business_type=eq.{sector}")
        if count and count > 0:
            log(f"  {sector:<23} {count:>10,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", type=int, choices=[1, 2, 3], help="Run specific phase")
    parser.add_argument("--dry-run", action="store_true", help="Count matches without updating")
    parser.add_argument("--summary", action="store_true", help="Just print summary")
    args = parser.parse_args()

    if args.summary:
        print_summary()
        return

    start = time.time()
    grand_total = 0

    if args.phase is None or args.phase == 1:
        grand_total += run_phase(PHASE_1_SECTORS, "PHASE 1: Break out broad categories", args.dry_run)

    if args.phase is None or args.phase == 2:
        grand_total += run_phase(PHASE_2_SECTORS, "PHASE 2: Classify generic SOS entities", args.dry_run)

    if args.phase is None or args.phase == 3:
        run_edgar_phase(args.dry_run)

    elapsed = time.time() - start
    log(f"\nDone. Total reclassified: {grand_total:,} in {elapsed:.0f}s")

    # Print summary
    print_summary()


if __name__ == "__main__":
    main()
