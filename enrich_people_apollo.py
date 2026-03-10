#!/usr/bin/env python3
"""
Mass people enrichment via Apollo Bulk People Enrichment.

Tiered approach:
  Tier A: People linked to car wash businesses (highest priority)
  Tier B: PE fund/holding company executives
  Tier C: All remaining EDGAR people with real names

Uses Apollo Bulk Enrichment (10 people per call) for efficiency.

Run: nohup python3 enrich_people_apollo.py >> enrich_people_apollo_stdout.log 2>&1 &
Pass --tier A|B|C to run a specific tier (default: A)
"""

import sys
import time
from enrich_utils import (
    SupabaseClient, ApolloClient,
    make_logger, load_progress, save_progress, utcnow_iso,
)

log = make_logger("enrich_people_apollo.log")
PROGRESS_FILE = "enrich_people_apollo_progress.json"
BULK_SIZE = 10  # Apollo bulk max per call


def get_tier_filter(tier):
    """Return Supabase filter string for each tier."""
    base = "email=is.null&first_name=not.is.null&last_name=not.is.null"
    if tier == "A":
        # Car wash business owners — people linked via business_people
        return base + "&source_type=eq.car_wash_owner"
    elif tier == "B":
        # PE fund contacts with executive titles
        return base + "&source=eq.edgar&source_type=eq.firm_contact"
    elif tier == "C":
        # All remaining people
        return base
    return base


def build_apollo_details(people):
    """Build Apollo bulk enrichment payload from people records."""
    details = []
    for p in people:
        detail = {}
        if p.get("first_name"):
            detail["first_name"] = p["first_name"]
        if p.get("last_name"):
            detail["last_name"] = p["last_name"]
        if p.get("company_domain"):
            detail["domain"] = p["company_domain"]
        elif p.get("company_name"):
            detail["organization_name"] = p["company_name"]
        if p.get("title"):
            detail["title"] = p["title"]
        if p.get("linkedin_url"):
            detail["linkedin_url"] = p["linkedin_url"]
        # Need at least name + org to have a chance
        if ("first_name" in detail and "last_name" in detail and
                ("domain" in detail or "organization_name" in detail)):
            detail["id"] = p["person_id"]  # Track which person
            details.append(detail)
    return details


def process_apollo_results(db, results, people_by_id):
    """Map Apollo enrichment results back to people records and update."""
    matches = results.get("matches", [])
    updated = 0

    for match in matches:
        person_id = match.get("id")
        if not person_id or person_id not in people_by_id:
            continue

        apollo_person = match.get("matched_person") or match
        if not apollo_person:
            continue

        updates = {}

        # Email
        email = apollo_person.get("email")
        if email:
            updates["email"] = email
            updates["email_status"] = "apollo_enriched"

        # Phone
        phones = apollo_person.get("phone_numbers") or []
        if phones:
            sanitized = phones[0].get("sanitized_number") or phones[0].get("raw_number")
            if sanitized:
                updates["phone"] = sanitized

        # LinkedIn
        linkedin = apollo_person.get("linkedin_url")
        if linkedin:
            updates["linkedin_url"] = linkedin

        # Location
        if apollo_person.get("city"):
            updates["city"] = apollo_person["city"]
        if apollo_person.get("state"):
            updates["state"] = apollo_person["state"]

        # Company domain
        org = apollo_person.get("organization") or {}
        if org.get("primary_domain"):
            updates["company_domain"] = org["primary_domain"]

        if updates:
            updates["source"] = (people_by_id[person_id].get("source") or "") + ",apollo_enriched"
            updates["verified"] = bool(email)
            updates["updated_at"] = utcnow_iso()
            if db.update("people", "person_id", person_id, updates):
                updated += 1

    return updated


def main():
    # Parse tier from args
    tier = "A"
    if len(sys.argv) > 1:
        arg = sys.argv[1].upper().replace("--TIER", "").replace("--", "").strip()
        if arg in ("A", "B", "C"):
            tier = arg

    log("=" * 60)
    log(f"PEOPLE ENRICHMENT — Tier {tier}")
    log("=" * 60)

    db = SupabaseClient()
    apollo = ApolloClient()

    progress = load_progress(PROGRESS_FILE, {
        "tier_a_processed": 0, "tier_a_enriched": 0,
        "tier_b_processed": 0, "tier_b_enriched": 0,
        "tier_c_processed": 0, "tier_c_enriched": 0,
        "api_calls": 0,
    })

    tier_filter = get_tier_filter(tier)
    proc_key = f"tier_{tier.lower()}_processed"
    enr_key = f"tier_{tier.lower()}_enriched"

    # For tier B, limit to executive titles
    extra_filter = ""
    if tier == "B":
        extra_filter = "&title=ilike.*officer*,title=ilike.*director*,title=ilike.*president*,title=ilike.*partner*"
        # PostgREST OR: use `or` operator
        # Actually, let's just fetch and filter in Python
        extra_filter = ""

    batch_num = 0
    while True:
        # Fetch batch of people needing enrichment
        people = db.fetch(
            "people",
            filters=tier_filter,
            select="person_id,first_name,last_name,full_name,title,email,phone,"
                   "linkedin_url,city,state,company_name,company_domain,source,source_type",
            order="created_at.asc",
            limit=BULK_SIZE * 5,  # Fetch 50, process in chunks of 10
        )

        if not people:
            log(f"No more Tier {tier} people to enrich")
            break

        # For tier B, filter to executive titles in Python
        if tier == "B":
            exec_keywords = ['officer', 'director', 'president', 'partner',
                             'managing', 'principal', 'founder', 'ceo', 'cfo', 'coo']
            people = [p for p in people
                      if p.get("title") and
                      any(kw in (p["title"] or "").lower() for kw in exec_keywords)]
            if not people:
                # Skip this batch offset
                break

        # Process in chunks of BULK_SIZE
        for chunk_start in range(0, len(people), BULK_SIZE):
            chunk = people[chunk_start:chunk_start + BULK_SIZE]
            batch_num += 1

            # Build Apollo request
            details = build_apollo_details(chunk)
            if not details:
                progress[proc_key] += len(chunk)
                continue

            people_by_id = {p["person_id"]: p for p in chunk}

            log(f"Batch {batch_num}: Enriching {len(details)} people (Tier {tier})")
            result = apollo.people_bulk_enrich(details)
            progress["api_calls"] += 1

            if "error" in result:
                log(f"  Apollo error: {result['error']}")
                time.sleep(5)
                continue

            updated = process_apollo_results(db, result, people_by_id)
            progress[proc_key] += len(chunk)
            progress[enr_key] += updated
            log(f"  → {updated}/{len(chunk)} enriched")

            if batch_num % 10 == 0:
                save_progress(progress, PROGRESS_FILE)
                log(f"  Progress: processed={progress[proc_key]}, "
                    f"enriched={progress[enr_key]}, "
                    f"api_calls={progress['api_calls']}")

    save_progress(progress, PROGRESS_FILE)
    log(f"\nDONE — Tier {tier}: processed={progress[proc_key]}, enriched={progress[enr_key]}")
    log(f"Total API calls: {progress['api_calls']}")


if __name__ == "__main__":
    main()
