#!/usr/bin/env python3
"""
General business enrichment via Apollo Org Enrichment.

For businesses with a website, uses Apollo to fill in:
- Phone, email, employee count, year founded
- LinkedIn URL, industry tags, description
- Owner discovery via Apollo People Search
- Revenue estimates and acquisition scoring

Processes all business_types (not just car washes).
Pass --type <business_type> to focus on a specific type.

Run: nohup python3 enrich_businesses_apollo.py >> enrich_biz_apollo_stdout.log 2>&1 &
     nohup python3 enrich_businesses_apollo.py --type restaurant >> enrich_biz_apollo_stdout.log 2>&1 &
"""

import sys
import time
from enrich_utils import (
    SupabaseClient, ApolloClient,
    make_logger, load_progress, save_progress,
    extract_domain, validate_domain, utcnow_iso,
    estimate_revenue, compute_acquisition_score,
)

log = make_logger("enrich_businesses_apollo.log")
PROGRESS_FILE = "enrich_businesses_apollo_progress.json"
BATCH_SIZE = 50


def enrich_business(biz, apollo, db):
    """Enrich a single business via Apollo."""
    biz_id = biz["business_id"]
    website = biz.get("website") or ""
    domain = extract_domain(website)
    updates = {}

    if not domain:
        return updates

    # Apollo Org Enrichment
    result = apollo.org_enrich(domain)
    org = result.get("organization", {})
    if not org:
        return updates

    # Fill missing fields
    if not biz.get("phone") and org.get("phone"):
        updates["phone"] = org["phone"]
    if not biz.get("email") and org.get("primary_email"):
        updates["email"] = org["primary_email"]
    if org.get("estimated_num_employees") and not biz.get("employee_count"):
        updates["employee_count"] = org["estimated_num_employees"]
    if org.get("founded_year") and not biz.get("year_established"):
        updates["year_established"] = org["founded_year"]
    if org.get("industry"):
        updates["industry"] = org["industry"]

    # Store extra data in industry_details
    detail_data = {}
    if org.get("linkedin_url"):
        detail_data["linkedin_url"] = org["linkedin_url"]
    if org.get("short_description"):
        detail_data["description"] = org["short_description"][:500]
    if org.get("keywords"):
        detail_data["keywords"] = org["keywords"][:10]
    if org.get("technologies"):
        detail_data["technologies"] = [t.get("name") for t in org["technologies"][:10]] if isinstance(org["technologies"], list) else []
    if org.get("annual_revenue"):
        detail_data["apollo_annual_revenue"] = org["annual_revenue"]

    if detail_data:
        industry = biz.get("business_type") or "general"
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
                "industry": industry,
                "details": detail_data,
            }])

    # Owner discovery
    if not biz.get("owner_name"):
        people_result = apollo.people_search(
            domain=domain,
            titles=["owner", "president", "ceo", "founder", "general manager"],
            limit=3,
        )
        people = people_result.get("people", [])
        if people:
            top = people[0]
            full_name = f"{top.get('first_name', '')} {top.get('last_name', '')}".strip()
            if full_name:
                updates["owner_name"] = full_name
                updates["owner_type"] = "individual"

                # Create person record
                person_data = {
                    "full_name": full_name,
                    "first_name": top.get("first_name"),
                    "last_name": top.get("last_name"),
                    "title": top.get("title"),
                    "email": top.get("email"),
                    "phone": (top.get("phone_numbers") or [{}])[0].get("sanitized_number") if top.get("phone_numbers") else None,
                    "linkedin_url": top.get("linkedin_url"),
                    "city": top.get("city"),
                    "state": top.get("state"),
                    "company_name": biz.get("name"),
                    "company_domain": domain,
                    "source": "apollo_search",
                    "source_type": f"{biz.get('business_type', 'business')}_owner",
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
                            "is_primary": True,
                        }])
                        updates["owner_id"] = person_id

    # Revenue & scoring
    btype = biz.get("business_type") or "default"
    sub = biz.get("sub_type") or "default"
    rc = biz.get("google_review_count") or 0
    rating = biz.get("google_rating")

    rev_low, rev_high = estimate_revenue(btype, sub, rc, rating)
    if not biz.get("estimated_revenue"):
        updates["estimated_revenue"] = rev_low

    score, tier = compute_acquisition_score(
        rc, rating, btype, sub,
        is_chain=biz.get("owner_type") == "chain",
        employee_count=biz.get("employee_count") or (org.get("estimated_num_employees")),
    )
    updates["acquisition_score"] = score
    updates["acquisition_tier"] = tier
    updates["enrichment_status"] = "enriched"
    updates["enrichment_date"] = utcnow_iso()
    updates["updated_at"] = utcnow_iso()

    return updates


def main():
    # Parse optional --type arg
    biz_type = None
    if "--type" in sys.argv:
        idx = sys.argv.index("--type")
        if idx + 1 < len(sys.argv):
            biz_type = sys.argv[idx + 1]

    log("=" * 60)
    log(f"BUSINESS ENRICHMENT (Apollo) — Type: {biz_type or 'ALL'}")
    log("=" * 60)

    db = SupabaseClient()
    apollo = ApolloClient()

    progress = load_progress(PROGRESS_FILE, {
        "total_processed": 0,
        "total_enriched": 0,
        "total_owners_found": 0,
        "api_calls": 0,
    })

    batch_num = 0
    while True:
        # Build filter
        filters = "enrichment_status=eq.raw&website=not.is.null"
        if biz_type:
            filters += f"&business_type=eq.{biz_type}"
        else:
            # Skip car washes (handled by enrich_carwashes_deep.py)
            filters += "&business_type=neq.car_wash"

        batch = db.fetch(
            "businesses",
            filters=filters,
            select="business_id,name,website,phone,email,city,state,zip,"
                   "owner_name,owner_type,owner_id,google_rating,google_review_count,"
                   "employee_count,year_established,business_type,sub_type,"
                   "estimated_revenue,acquisition_score,enrichment_status",
            order="google_review_count.desc.nullslast",
            limit=BATCH_SIZE,
        )

        if not batch:
            log("No more businesses to enrich")
            break

        batch_num += 1
        log(f"\nBatch {batch_num}: {len(batch)} businesses")

        for i, biz in enumerate(batch):
            name = biz.get("name", "")[:40]
            log(f"  [{i+1}/{len(batch)}] {name}")

            try:
                updates = enrich_business(biz, apollo, db)
                progress["api_calls"] += 2  # org enrich + people search

                if updates:
                    had_owner = "owner_name" in updates
                    db.update("businesses", "business_id", biz["business_id"], updates)
                    progress["total_enriched"] += 1
                    if had_owner:
                        progress["total_owners_found"] += 1

            except Exception as e:
                log(f"    Error: {e}")

            progress["total_processed"] += 1

        save_progress(progress, PROGRESS_FILE)
        log(f"Batch {batch_num} done. "
            f"Processed={progress['total_processed']}, "
            f"Enriched={progress['total_enriched']}, "
            f"Owners={progress['total_owners_found']}")

    save_progress(progress, PROGRESS_FILE)
    log(f"\nDONE — Processed: {progress['total_processed']}, "
        f"Enriched: {progress['total_enriched']}, "
        f"Owners found: {progress['total_owners_found']}, "
        f"API calls: {progress['api_calls']}")


if __name__ == "__main__":
    main()
