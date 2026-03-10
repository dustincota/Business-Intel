#!/usr/bin/env python3
"""
Enrich car_wash_buyers with deep contact and company intel.

For each of the ~108 buyers:
1. Validate domain (DNS + HTTP)
2. Apollo Org Enrichment → company size, linkedin, phone, description
3. Apollo People Search → find key decision makers (C-suite, VP, owners)
4. Create/update contacts in `people` + link via `business_people`
5. Backfill hq_city, hq_state, contact info on buyer record

Run: nohup python3 enrich_buyers.py >> enrich_buyers_stdout.log 2>&1 &
"""

import time
from enrich_utils import (
    SupabaseClient, ApolloClient, ApifyClient,
    make_logger, load_progress, save_progress,
    extract_domain, validate_domain, clean_business_name, utcnow_iso,
)

log = make_logger("enrich_buyers.log")
PROGRESS_FILE = "enrich_buyers_progress.json"

# LinkedIn Company Employees actor (free, compute-only)
LINKEDIN_ACTOR = "george.the.developer/linkedin-company-employees-scraper"


def enrich_buyer(buyer, apollo, apify, db):
    """Enrich a single buyer with all available data."""
    buyer_id = buyer["buyer_id"]
    fund_name = buyer.get("fund_name") or ""
    domain = buyer.get("domain") or ""
    website = buyer.get("website") or ""
    firm_id = buyer.get("firm_id")

    updates = {}
    contacts_found = []

    # ── Step 1: Domain validation ──
    if not domain and website:
        domain = extract_domain(website)
        if domain:
            updates["domain"] = domain

    domain_valid = False
    if domain:
        domain_valid = validate_domain(domain)
        if not domain_valid:
            log(f"  Domain {domain} does not resolve")

    # ── Step 2: Apollo Org Enrichment ──
    org_data = {}
    if domain and domain_valid:
        log(f"  Apollo org enrich: {domain}")
        result = apollo.org_enrich(domain)
        org = result.get("organization", {})
        if org:
            org_data = org
            if not buyer.get("hq_city") and org.get("city"):
                updates["hq_city"] = org["city"]
            if not buyer.get("hq_state") and org.get("state"):
                updates["hq_state"] = org["state"]
            if not buyer.get("linkedin_url") and org.get("linkedin_url"):
                updates["linkedin_url"] = org["linkedin_url"]
            if not buyer.get("website") and org.get("website_url"):
                updates["website"] = org["website_url"]
            if org.get("estimated_num_employees"):
                updates.setdefault("enrichment_source",
                                   buyer.get("enrichment_source", ""))
                # Append to notes if needed

    # ── Step 3: Apollo People Search ──
    if domain and domain_valid:
        log(f"  Apollo people search: {domain}")
        # Search for C-suite, VP, owners, directors
        for seniority_batch in [["owner", "c_suite"], ["vp", "director"]]:
            result = apollo.people_search(
                domain=domain,
                seniorities=seniority_batch,
                limit=5,
            )
            people = result.get("people", [])
            for person in people[:3]:  # top 3 per seniority batch
                contact = {
                    "first_name": person.get("first_name"),
                    "last_name": person.get("last_name"),
                    "full_name": f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                    "title": person.get("title"),
                    "email": person.get("email"),
                    "phone": person.get("phone_numbers", [{}])[0].get("sanitized_number") if person.get("phone_numbers") else None,
                    "linkedin_url": person.get("linkedin_url"),
                    "city": person.get("city"),
                    "state": person.get("state"),
                    "company_name": person.get("organization", {}).get("name") or fund_name,
                    "company_domain": domain,
                    "source": "apollo_search",
                    "source_type": "buyer_contact",
                }
                contacts_found.append(contact)

            if people:
                break  # Got results from first seniority batch, skip second

    # ── Step 4: LinkedIn Employee Search (if domain, via Apify) ──
    if domain and domain_valid and not contacts_found and apify.token:
        log(f"  LinkedIn employee search: {domain}")
        try:
            items = apify.run_actor(LINKEDIN_ACTOR, {
                "companies": [f"https://www.linkedin.com/company/{domain.split('.')[0]}/"],
                "seniorityFilter": ["owner", "cxo", "vp", "director"],
                "profileDepth": "short",
                "maxEmployees": 10,
            }, timeout_secs=120)
            for item in (items or []):
                contact = {
                    "first_name": item.get("firstName"),
                    "last_name": item.get("lastName"),
                    "full_name": item.get("fullName") or f"{item.get('firstName', '')} {item.get('lastName', '')}".strip(),
                    "title": item.get("headline"),
                    "linkedin_url": item.get("profileUrl"),
                    "company_name": fund_name,
                    "company_domain": domain,
                    "source": "linkedin_apify",
                    "source_type": "buyer_contact",
                }
                contacts_found.append(contact)
        except Exception as e:
            log(f"  LinkedIn search error: {e}")

    # ── Step 5: Update buyer record with best contact ──
    if contacts_found:
        best = contacts_found[0]
        if not buyer.get("contact_name") and best.get("full_name"):
            updates["contact_name"] = best["full_name"]
        if not buyer.get("contact_email") and best.get("email"):
            updates["contact_email"] = best["email"]
        if not buyer.get("contact_phone") and best.get("phone"):
            updates["contact_phone"] = best["phone"]
        if not buyer.get("contact_title") and best.get("title"):
            updates["contact_title"] = best["title"]
        if not buyer.get("linkedin_url") and best.get("linkedin_url"):
            updates["linkedin_url"] = best["linkedin_url"]

    # ── Step 6: Save contacts to people + business_people ──
    for contact in contacts_found:
        if not contact.get("full_name"):
            continue
        # Insert into people table
        person_data = {
            "full_name": contact["full_name"],
            "first_name": contact.get("first_name"),
            "last_name": contact.get("last_name"),
            "title": contact.get("title"),
            "email": contact.get("email"),
            "phone": contact.get("phone"),
            "linkedin_url": contact.get("linkedin_url"),
            "city": contact.get("city"),
            "state": contact.get("state"),
            "company_name": contact.get("company_name"),
            "company_domain": contact.get("company_domain"),
            "source": contact.get("source"),
            "source_type": contact.get("source_type"),
            "created_at": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }
        # Remove None values
        person_data = {k: v for k, v in person_data.items() if v is not None}

        inserted = db.insert("people", [person_data])
        if inserted and len(inserted) > 0:
            person_id = inserted[0].get("person_id")
            # Find the associated business (buyer → firm → businesses)
            if firm_id and person_id:
                # Link person to firm's businesses
                businesses = db.fetch(
                    "businesses",
                    filters=f"parent_firm_id=eq.{firm_id}",
                    select="business_id",
                    limit=5,
                )
                for biz in businesses:
                    db.insert("business_people", [{
                        "business_id": biz["business_id"],
                        "person_id": person_id,
                        "role": "executive",
                        "is_primary": contact == contacts_found[0],
                    }])

    # ── Step 7: Mark enrichment ──
    if updates:
        updates["enrichment_source"] = (
            (buyer.get("enrichment_source") or "") + ",buyer_deep_enrich"
        ).lstrip(",")
        updates["updated_at"] = utcnow_iso()

    return updates, len(contacts_found)


def main():
    log("=" * 60)
    log("BUYER ENRICHMENT — Starting")
    log("=" * 60)

    db = SupabaseClient()
    apollo = ApolloClient()

    # Apify is optional
    try:
        apify = ApifyClient()
    except Exception:
        apify = type('obj', (object,), {'token': '', 'run_actor': lambda *a, **k: []})()
        log("Apify token not set — skipping LinkedIn enrichment")

    progress = load_progress(PROGRESS_FILE, {
        "total_processed": 0,
        "total_contacts_found": 0,
        "total_updated": 0,
    })

    # Fetch all buyers
    buyers = db.fetch(
        "car_wash_buyers",
        select="*",
        order="created_at.asc",
        limit=200,
    )
    log(f"Found {len(buyers)} buyers to process")

    for i, buyer in enumerate(buyers):
        buyer_id = buyer["buyer_id"]
        name = buyer.get("fund_name") or buyer.get("buyer_id", "")[:8]

        # Skip already enriched with deep enrichment
        if buyer.get("enrichment_source") and "buyer_deep_enrich" in (buyer.get("enrichment_source") or ""):
            continue

        log(f"[{i+1}/{len(buyers)}] Enriching buyer: {name}")

        try:
            updates, num_contacts = enrich_buyer(buyer, apollo, apify, db)

            if updates:
                success = db.update("car_wash_buyers", "buyer_id", buyer_id, updates)
                if success:
                    log(f"  Updated buyer with {len(updates)} fields, {num_contacts} contacts found")
                    progress["total_updated"] += 1
                else:
                    log(f"  Failed to update buyer {buyer_id}")
            else:
                log(f"  No new data found")

            progress["total_processed"] += 1
            progress["total_contacts_found"] += num_contacts

            if progress["total_processed"] % 10 == 0:
                save_progress(progress, PROGRESS_FILE)

        except Exception as e:
            log(f"  Error enriching buyer {name}: {e}")

        time.sleep(0.5)

    save_progress(progress, PROGRESS_FILE)
    log(f"DONE — Processed: {progress['total_processed']}, "
        f"Updated: {progress['total_updated']}, "
        f"Contacts found: {progress['total_contacts_found']}")


if __name__ == "__main__":
    main()
