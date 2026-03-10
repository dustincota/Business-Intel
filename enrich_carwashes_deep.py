#!/usr/bin/env python3
"""
Deep car wash enrichment — multi-pass enrichment using every available source.

Pass 1: Apollo Org Enrichment (for washes with website/domain)
Pass 2: Apollo People Search (find owners/managers)
Pass 3: Website scraping (membership prices, services, emails)
Pass 4: Apify Google Maps scrape (for washes missing phone/rating)
Pass 5: Scoring & revenue estimates (local computation)

Targets: businesses table WHERE business_type = 'car_wash'

Run: nohup python3 enrich_carwashes_deep.py >> enrich_carwashes_deep_stdout.log 2>&1 &
"""

import re
import time
import json
from enrich_utils import (
    SupabaseClient, ApolloClient, ApifyClient,
    make_logger, load_progress, save_progress,
    extract_domain, validate_domain, extract_emails_from_text,
    clean_business_name, utcnow_iso,
    estimate_revenue, compute_acquisition_score,
    REVENUE_BENCHMARKS,
)
import requests

log = make_logger("enrich_carwashes_deep.log")
PROGRESS_FILE = "enrich_carwashes_deep_progress.json"
BATCH_SIZE = 50

# Apify actors
GMAPS_ACTOR = "fatihtahta/google-maps-scraper-enterprise"
YELP_ACTOR = "lanky_quantifier/yelp-business-scraper"

# Known chain brands (enrich them too, but flag them)
CHAIN_BRANDS = {
    'mister car wash', 'zips car wash', "tommy's express", 'tidal wave',
    'take 5 car wash', 'whistle express', 'quick quack', 'delta sonic',
    'super star car wash', 'autobell', 'mr. clean car wash', 'crew carwash',
    'rocket wash', 'splash car wash', 'club car wash', 'breeze thru',
    'flying ace express', "mike's carwash", 'wash factory', 'soapy joe',
    'clean machine', 'goo goo car wash', 'cobblestone auto spa',
    'jacksons car wash', 'otto car wash', 'imo car wash',
}

# Car wash sub-type classification keywords
WASH_TYPE_KEYWORDS = {
    'tunnel': ['tunnel', 'express', 'conveyor', 'automatic'],
    'full_service': ['full service', 'full-service', 'hand wash', 'detail'],
    'self_service': ['self service', 'self-service', 'coin', 'diy', 'bay'],
    'flex_serve': ['flex', 'hybrid', 'flex-serve'],
}


def classify_wash_type(name, description=""):
    """Classify car wash sub-type from name and description."""
    text = f"{name} {description}".lower()
    for wtype, keywords in WASH_TYPE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return wtype
    return 'unknown'


def detect_brand(name):
    """Detect if this is a known chain brand."""
    name_lower = (name or "").lower()
    for brand in CHAIN_BRANDS:
        if brand in name_lower:
            return brand.title(), True
    return None, False


def parse_website_for_intel(url, session=None):
    """Scrape a car wash website for membership prices, services, emails."""
    if not url:
        return {}
    if not url.startswith("http"):
        url = f"https://{url}"

    session = session or requests.Session()
    intel = {}

    try:
        r = session.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        if r.status_code != 200:
            return {}
        html = r.text[:100_000]  # Cap at 100KB
        text = re.sub(r'<[^>]+>', ' ', html)  # Strip HTML tags
        text_lower = text.lower()

        # Extract emails
        emails = extract_emails_from_text(text)
        if emails:
            intel["emails"] = emails[:5]

        # Detect membership/unlimited plans
        if any(kw in text_lower for kw in ['membership', 'unlimited', 'monthly plan', 'wash club', 'wash pass']):
            intel["has_membership"] = True
            # Try to find prices
            price_patterns = re.findall(r'\$(\d{1,3}(?:\.\d{2})?)\s*(?:/\s*(?:mo|month|monthly))?', text)
            if price_patterns:
                prices = sorted(set(float(p) for p in price_patterns if 10 <= float(p) <= 100))
                if prices:
                    intel["membership_price_low"] = prices[0]
                    intel["membership_price_high"] = prices[-1]

        # Detect services
        services = []
        service_keywords = {
            'exterior wash': 'exterior_wash',
            'interior clean': 'interior_clean',
            'detail': 'detailing',
            'wax': 'wax',
            'ceramic': 'ceramic_coating',
            'tire shine': 'tire_shine',
            'vacuum': 'vacuum',
            'mat clean': 'mat_cleaning',
            'rain-x': 'rain_x',
            'undercarriage': 'undercarriage',
            'wheel': 'wheel_clean',
            'air freshener': 'air_freshener',
            'oil change': 'oil_change',
            'lube': 'lube',
        }
        for keyword, service in service_keywords.items():
            if keyword in text_lower:
                services.append(service)
        if services:
            intel["services"] = services

        # Detect equipment vendors
        equipment_vendors = {
            'pdq': 'PDQ', 'washworld': 'WashWorld', 'belanger': 'Belanger',
            'sonny': "Sonny's", 'macneil': 'MacNeil', 'ryko': 'Ryko',
            'tommy car wash': 'Tommy Car Wash Systems', 'motor city': 'Motor City Wash Works',
            'petit auto wash': 'Petit Auto Wash', 'coleman hanna': 'Coleman Hanna',
            'national pride': 'National Pride', 'istobal': 'Istobal',
            'washtec': 'WashTec', 'christ': 'Christ Wash Systems',
        }
        found_vendors = []
        for keyword, vendor in equipment_vendors.items():
            if keyword in text_lower:
                found_vendors.append(vendor)
        if found_vendors:
            intel["equipment_vendors"] = found_vendors

    except Exception:
        pass

    return intel


def enrich_pass_apollo_org(db, apollo, batch):
    """Pass 1: Apollo org enrichment for businesses with website."""
    enriched = 0
    for biz in batch:
        biz_id = biz["business_id"]
        website = biz.get("website") or ""
        domain = extract_domain(website)

        if not domain or not validate_domain(domain):
            continue

        log(f"  Apollo org: {domain}")
        result = apollo.org_enrich(domain)
        org = result.get("organization", {})
        if not org:
            continue

        updates = {}
        if not biz.get("phone") and org.get("phone"):
            updates["phone"] = org["phone"]
        if not biz.get("email") and org.get("primary_email"):
            updates["email"] = org["primary_email"]
        if org.get("estimated_num_employees"):
            updates["employee_count"] = org["estimated_num_employees"]
        if org.get("linkedin_url"):
            # Store in industry_details
            pass
        if org.get("short_description"):
            # Can use for sub_type classification
            sub_type = classify_wash_type(biz.get("name", ""), org.get("short_description", ""))
            if sub_type != 'unknown' and not biz.get("sub_type"):
                updates["sub_type"] = sub_type

        if updates:
            updates["updated_at"] = utcnow_iso()
            db.update("businesses", "business_id", biz_id, updates)
            enriched += 1

            # Also update industry_details with Apollo data
            detail_updates = {}
            if org.get("linkedin_url"):
                detail_updates["linkedin_url"] = org["linkedin_url"]
            if org.get("short_description"):
                detail_updates["description"] = org["short_description"][:500]
            if org.get("estimated_num_employees"):
                detail_updates["employee_count"] = org["estimated_num_employees"]
            if org.get("founded_year"):
                detail_updates["founded_year"] = org["founded_year"]
            if org.get("keywords"):
                detail_updates["keywords"] = org["keywords"][:10]

            if detail_updates:
                existing = db.fetch(
                    "industry_details",
                    filters=f"business_id=eq.{biz_id}",
                    select="id,details",
                    limit=1,
                )
                if existing:
                    details = existing[0].get("details", {})
                    details.update(detail_updates)
                    db.update("industry_details", "id", existing[0]["id"], {"details": details, "updated_at": utcnow_iso()})
                else:
                    db.insert("industry_details", [{
                        "business_id": biz_id,
                        "industry": "car_wash",
                        "details": detail_updates,
                    }])

    return enriched


def enrich_pass_apollo_people(db, apollo, batch):
    """Pass 2: Find owners/managers via Apollo People Search."""
    contacts_found = 0
    for biz in batch:
        biz_id = biz["business_id"]
        website = biz.get("website") or ""
        domain = extract_domain(website)
        name = biz.get("name", "")

        if not domain:
            continue

        log(f"  Apollo people: {domain}")
        result = apollo.people_search(
            domain=domain,
            titles=["owner", "president", "general manager", "manager", "ceo", "founder"],
            limit=5,
        )
        people = result.get("people", [])

        for person in people[:3]:
            full_name = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
            if not full_name:
                continue

            person_data = {
                "full_name": full_name,
                "first_name": person.get("first_name"),
                "last_name": person.get("last_name"),
                "title": person.get("title"),
                "email": person.get("email"),
                "phone": (person.get("phone_numbers", [{}]) or [{}])[0].get("sanitized_number") if person.get("phone_numbers") else None,
                "linkedin_url": person.get("linkedin_url"),
                "city": person.get("city"),
                "state": person.get("state"),
                "company_name": name,
                "company_domain": domain,
                "source": "apollo_search",
                "source_type": "car_wash_owner",
                "created_at": utcnow_iso(),
                "updated_at": utcnow_iso(),
            }
            person_data = {k: v for k, v in person_data.items() if v is not None}

            inserted = db.insert("people", [person_data])
            if inserted and len(inserted) > 0:
                person_id = inserted[0].get("person_id")
                if person_id:
                    db.insert("business_people", [{
                        "business_id": biz_id,
                        "person_id": person_id,
                        "role": "owner",
                        "is_primary": (person == people[0]),
                    }])
                    contacts_found += 1

                    # Also update business owner_name if missing
                    if not biz.get("owner_name"):
                        db.update("businesses", "business_id", biz_id, {
                            "owner_name": full_name,
                            "owner_type": "individual",
                            "updated_at": utcnow_iso(),
                        })

    return contacts_found


def enrich_pass_website_scrape(db, batch):
    """Pass 3: Scrape websites for membership info, services, emails."""
    enriched = 0
    session = requests.Session()
    for biz in batch:
        biz_id = biz["business_id"]
        website = biz.get("website") or ""
        if not website:
            continue

        log(f"  Website scrape: {website}")
        intel = parse_website_for_intel(website, session)
        if not intel:
            continue

        updates = {}
        if intel.get("emails") and not biz.get("email"):
            updates["email"] = intel["emails"][0]

        # Update industry_details with website intel
        existing = db.fetch(
            "industry_details",
            filters=f"business_id=eq.{biz_id}",
            select="id,details",
            limit=1,
        )
        detail_data = {}
        if intel.get("has_membership"):
            detail_data["has_membership"] = True
        if intel.get("membership_price_low"):
            detail_data["membership_price_low"] = intel["membership_price_low"]
        if intel.get("membership_price_high"):
            detail_data["membership_price_high"] = intel["membership_price_high"]
        if intel.get("services"):
            detail_data["services"] = intel["services"]
        if intel.get("equipment_vendors"):
            detail_data["equipment_vendors"] = intel["equipment_vendors"]
        if intel.get("emails"):
            detail_data["contact_emails"] = intel["emails"]

        if detail_data:
            if existing:
                details = existing[0].get("details", {})
                details.update(detail_data)
                db.update("industry_details", "id", existing[0]["id"],
                         {"details": details, "updated_at": utcnow_iso()})
            else:
                db.insert("industry_details", [{
                    "business_id": biz_id,
                    "industry": "car_wash",
                    "details": detail_data,
                }])

        if updates:
            updates["updated_at"] = utcnow_iso()
            db.update("businesses", "business_id", biz_id, updates)
            enriched += 1

        time.sleep(0.5)  # Polite scraping

    return enriched


def enrich_pass_gmaps(db, apify, batch):
    """Pass 4: Apify Google Maps scrape for businesses missing key data."""
    if not apify.token:
        return 0

    # Only scrape businesses missing phone AND rating
    needs_gmaps = [b for b in batch
                   if not b.get("phone") and not b.get("google_rating")
                   and b.get("city") and b.get("state")]

    if not needs_gmaps:
        return 0

    enriched = 0
    # Process in small batches of 10 for Apify (each run = 1 location search)
    for biz in needs_gmaps[:20]:  # Cap per batch cycle
        name = biz.get("name", "")
        city = biz.get("city", "")
        state = biz.get("state", "")

        log(f"  Google Maps scrape: {name} in {city}, {state}")
        try:
            items = apify.run_actor(GMAPS_ACTOR, {
                "queries": name,
                "location": f"{city}, {state}",
                "radiusKm": 5,
                "maxResults": 3,
                "language": "en",
                "region": "US",
            }, timeout_secs=60)

            if not items:
                continue

            # Find best match by name similarity
            clean_name = clean_business_name(name).lower()
            best_match = None
            for item in items:
                item_name = clean_business_name(item.get("name", "")).lower()
                if clean_name in item_name or item_name in clean_name:
                    best_match = item
                    break
            if not best_match and items:
                best_match = items[0]  # Fallback to first result

            if best_match:
                updates = {}
                if best_match.get("phone") and not biz.get("phone"):
                    updates["phone"] = best_match["phone"]
                if best_match.get("website") and not biz.get("website"):
                    updates["website"] = best_match["website"]
                if best_match.get("rating"):
                    updates["google_rating"] = best_match["rating"]
                if best_match.get("user_ratings_total"):
                    updates["google_review_count"] = best_match["user_ratings_total"]
                if best_match.get("place_id"):
                    updates["source_id"] = best_match["place_id"]
                    updates["data_source"] = (biz.get("data_source") or "") + ",google_maps_apify"

                if updates:
                    updates["updated_at"] = utcnow_iso()
                    db.update("businesses", "business_id", biz["business_id"], updates)
                    enriched += 1

                    # Store rich data in industry_details
                    detail_data = {}
                    if best_match.get("opening_hours"):
                        detail_data["hours"] = best_match["opening_hours"]
                    if best_match.get("service_attributes"):
                        detail_data["attributes"] = best_match["service_attributes"]
                    if best_match.get("photos"):
                        detail_data["photos"] = best_match["photos"][:5]
                    if best_match.get("description"):
                        detail_data["gmaps_description"] = best_match["description"]

                    if detail_data:
                        existing = db.fetch("industry_details",
                                          filters=f"business_id=eq.{biz['business_id']}",
                                          select="id,details", limit=1)
                        if existing:
                            details = existing[0].get("details", {})
                            details.update(detail_data)
                            db.update("industry_details", "id", existing[0]["id"],
                                     {"details": details, "updated_at": utcnow_iso()})
                        else:
                            db.insert("industry_details", [{
                                "business_id": biz["business_id"],
                                "industry": "car_wash",
                                "details": detail_data,
                            }])

        except Exception as e:
            log(f"  GMaps scrape error: {e}")

        time.sleep(1)

    return enriched


def enrich_pass_scoring(db, batch):
    """Pass 5: Compute revenue estimates and acquisition scores."""
    scored = 0
    for biz in batch:
        biz_id = biz["business_id"]

        # Skip if already scored
        if biz.get("acquisition_score"):
            continue

        name = biz.get("name", "")
        sub_type = biz.get("sub_type") or classify_wash_type(name)
        brand, is_chain = detect_brand(name)
        review_count = biz.get("google_review_count") or 0
        rating = biz.get("google_rating")
        employee_count = biz.get("employee_count")

        # Revenue estimates
        rev_low, rev_high = estimate_revenue("car_wash", sub_type, review_count, rating)

        # Valuation
        val_low = int(rev_low * 2.5)
        val_high = int(rev_high * 5.0)

        # Acquisition score
        score, tier = compute_acquisition_score(
            review_count, rating, "car_wash", sub_type,
            is_chain=is_chain, employee_count=employee_count,
        )

        updates = {
            "estimated_revenue": rev_low,  # Use low estimate as base
            "acquisition_score": score,
            "acquisition_tier": tier,
            "enrichment_status": "enriched",
            "enrichment_date": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }

        if sub_type != 'unknown' and not biz.get("sub_type"):
            updates["sub_type"] = sub_type
        if brand and not biz.get("parent_company"):
            updates["parent_company"] = brand
            updates["owner_type"] = "chain"

        # Store detailed scoring in industry_details
        detail_data = {
            "revenue_estimate_low": rev_low,
            "revenue_estimate_high": rev_high,
            "valuation_estimate_low": val_low,
            "valuation_estimate_high": val_high,
            "acquisition_score": score,
            "acquisition_tier": tier,
            "is_chain": is_chain,
            "brand": brand,
            "wash_type": sub_type,
        }

        existing = db.fetch("industry_details",
                          filters=f"business_id=eq.{biz_id}",
                          select="id,details", limit=1)
        if existing:
            details = existing[0].get("details", {})
            details.update(detail_data)
            db.update("industry_details", "id", existing[0]["id"],
                     {"details": details, "updated_at": utcnow_iso()})
        else:
            db.insert("industry_details", [{
                "business_id": biz_id,
                "industry": "car_wash",
                "details": detail_data,
            }])

        db.update("businesses", "business_id", biz_id, updates)
        scored += 1

    return scored


def main():
    log("=" * 60)
    log("DEEP CAR WASH ENRICHMENT — Starting")
    log("=" * 60)

    db = SupabaseClient()

    # Apollo (required)
    try:
        apollo = ApolloClient()
    except ValueError as e:
        log(f"Apollo not available: {e}")
        log("Set APOLLO_API_KEY to enable Apollo enrichment")
        apollo = None

    # Apify (optional)
    try:
        apify = ApifyClient()
        if not apify.token:
            raise ValueError("no token")
    except Exception:
        apify = None
        log("Apify not available — Google Maps and LinkedIn scraping disabled")

    progress = load_progress(PROGRESS_FILE, {
        "pass1_apollo_org": 0,
        "pass2_apollo_people": 0,
        "pass3_website_scrape": 0,
        "pass4_gmaps": 0,
        "pass5_scoring": 0,
        "total_batches": 0,
    })

    batch_num = progress["total_batches"]

    while True:
        # Fetch car washes that still need enrichment
        batch = db.fetch(
            "businesses",
            filters="business_type=eq.car_wash&enrichment_status=eq.raw",
            select="business_id,name,website,phone,email,city,state,zip,owner_name,owner_type,"
                   "google_rating,google_review_count,employee_count,sub_type,parent_company,"
                   "data_source,source_id,enrichment_status,acquisition_score",
            order="google_review_count.desc.nullslast",
            limit=BATCH_SIZE,
        )

        if not batch:
            log("No more car washes to enrich. Checking for unscored...")
            # Also pick up any that have data but no score yet
            batch = db.fetch(
                "businesses",
                filters="business_type=eq.car_wash&acquisition_score=is.null",
                select="business_id,name,website,phone,email,city,state,zip,owner_name,owner_type,"
                       "google_rating,google_review_count,employee_count,sub_type,parent_company,"
                       "data_source,source_id,enrichment_status,acquisition_score",
                order="google_review_count.desc.nullslast",
                limit=BATCH_SIZE,
            )
            if not batch:
                log("All car washes enriched and scored!")
                break

        batch_num += 1
        log(f"\n{'─'*40}")
        log(f"Batch {batch_num}: {len(batch)} car washes")
        log(f"{'─'*40}")

        # Pass 1: Apollo Org Enrichment
        if apollo:
            with_website = [b for b in batch if b.get("website")]
            if with_website:
                log(f"Pass 1 — Apollo Org ({len(with_website)} with website)")
                p1 = enrich_pass_apollo_org(db, apollo, with_website)
                progress["pass1_apollo_org"] += p1
                log(f"  → {p1} enriched via Apollo Org")

        # Pass 2: Apollo People Search
        if apollo:
            with_website = [b for b in batch if b.get("website") and not b.get("owner_name")]
            if with_website:
                log(f"Pass 2 — Apollo People ({len(with_website)} missing owner)")
                p2 = enrich_pass_apollo_people(db, apollo, with_website)
                progress["pass2_apollo_people"] += p2
                log(f"  → {p2} contacts found")

        # Pass 3: Website Scraping
        with_website = [b for b in batch if b.get("website")]
        if with_website:
            log(f"Pass 3 — Website Scrape ({len(with_website)} sites)")
            p3 = enrich_pass_website_scrape(db, with_website)
            progress["pass3_website_scrape"] += p3
            log(f"  → {p3} enriched via website scrape")

        # Pass 4: Google Maps Apify Scrape
        if apify:
            log(f"Pass 4 — Google Maps Scrape")
            p4 = enrich_pass_gmaps(db, apify, batch)
            progress["pass4_gmaps"] += p4
            log(f"  → {p4} enriched via Google Maps")

        # Pass 5: Scoring
        log(f"Pass 5 — Scoring & Revenue Estimates")
        p5 = enrich_pass_scoring(db, batch)
        progress["pass5_scoring"] += p5
        log(f"  → {p5} scored")

        progress["total_batches"] = batch_num
        save_progress(progress, PROGRESS_FILE)

        log(f"Batch {batch_num} complete. Running totals: "
            f"Apollo Org={progress['pass1_apollo_org']}, "
            f"People={progress['pass2_apollo_people']}, "
            f"WebScrape={progress['pass3_website_scrape']}, "
            f"GMaps={progress['pass4_gmaps']}, "
            f"Scored={progress['pass5_scoring']}")

    save_progress(progress, PROGRESS_FILE)
    log("\nFINAL TOTALS:")
    log(f"  Apollo Org enriched:    {progress['pass1_apollo_org']}")
    log(f"  People contacts found:  {progress['pass2_apollo_people']}")
    log(f"  Website scrape enriched:{progress['pass3_website_scrape']}")
    log(f"  GMaps scrape enriched:  {progress['pass4_gmaps']}")
    log(f"  Scored:                 {progress['pass5_scoring']}")
    log("DONE")


if __name__ == "__main__":
    main()
