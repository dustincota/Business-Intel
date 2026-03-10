#!/usr/bin/env python3
"""
Firm enrichment via Apollo Organization Enrichment.

For firms with validated websites, enriches:
- LinkedIn company URL
- Employee count, industry, description
- Founded year, technologies

Prioritizes: pe_fund > holding_company > family_office > other

Run: nohup python3 enrich_firms_apollo.py >> enrich_firms_apollo_stdout.log 2>&1 &
"""

import time
from enrich_utils import (
    SupabaseClient, ApolloClient,
    make_logger, load_progress, save_progress,
    extract_domain, utcnow_iso,
)

log = make_logger("enrich_firms_apollo.log")
PROGRESS_FILE = "enrich_firms_apollo_progress.json"
BATCH_SIZE = 50

# Priority order for firm types
FIRM_TYPE_PRIORITY = ['pe_fund', 'holding_company', 'family_office',
                      'independent_sponsor', 'search_fund', 'strategic', 'other']


def enrich_firm(firm, apollo, db):
    """Enrich a single firm via Apollo Org Enrichment."""
    firm_id = firm["firm_id"]
    website = firm.get("website") or ""
    domain = extract_domain(website)

    if not domain:
        return {}

    result = apollo.org_enrich(domain)
    org = result.get("organization", {})
    if not org:
        return {}

    updates = {}

    if not firm.get("linkedin_url") and org.get("linkedin_url"):
        updates["linkedin_url"] = org["linkedin_url"]

    # Store extra info in tags JSONB
    tags = firm.get("tags") or []
    if isinstance(tags, str):
        import json
        tags = json.loads(tags) if tags else []

    new_info = {}
    if org.get("estimated_num_employees"):
        new_info["employee_count"] = org["estimated_num_employees"]
    if org.get("industry"):
        new_info["industry"] = org["industry"]
    if org.get("short_description"):
        new_info["description"] = org["short_description"][:300]
    if org.get("founded_year"):
        new_info["founded_year"] = org["founded_year"]
    if org.get("annual_revenue"):
        new_info["annual_revenue"] = org["annual_revenue"]
    if org.get("total_funding"):
        new_info["total_funding"] = org["total_funding"]

    if new_info:
        # Add enrichment data to tags
        if not isinstance(tags, list):
            tags = []
        # Remove old enrichment tag if exists
        tags = [t for t in tags if not (isinstance(t, dict) and t.get("_type") == "apollo_enrichment")]
        tags.append({"_type": "apollo_enrichment", **new_info})
        updates["tags"] = tags

    if updates:
        updates["updated_at"] = utcnow_iso()

    return updates


def main():
    log("=" * 60)
    log("FIRM ENRICHMENT (Apollo) — Starting")
    log("=" * 60)

    db = SupabaseClient()
    apollo = ApolloClient()

    progress = load_progress(PROGRESS_FILE, {
        "total_processed": 0,
        "total_enriched": 0,
        "total_linkedin_found": 0,
        "api_calls": 0,
        "current_type_index": 0,
    })

    for type_idx in range(progress.get("current_type_index", 0), len(FIRM_TYPE_PRIORITY)):
        firm_type = FIRM_TYPE_PRIORITY[type_idx]
        progress["current_type_index"] = type_idx

        log(f"\n--- Processing firm type: {firm_type} ---")

        batch_num = 0
        while True:
            batch = db.fetch(
                "firms",
                filters=f"type=eq.{firm_type}&website=not.is.null&linkedin_url=is.null",
                select="firm_id,name,type,website,linkedin_url,hq_city,hq_state,tags",
                order="created_at.asc",
                limit=BATCH_SIZE,
            )

            if not batch:
                log(f"No more {firm_type} firms to enrich")
                break

            batch_num += 1
            log(f"Batch {batch_num}: {len(batch)} {firm_type} firms")

            for i, firm in enumerate(batch):
                name = firm.get("name", "")[:40]
                log(f"  [{i+1}/{len(batch)}] {name}")

                try:
                    updates = enrich_firm(firm, apollo, db)
                    progress["api_calls"] += 1

                    if updates:
                        if db.update("firms", "firm_id", firm["firm_id"], updates):
                            progress["total_enriched"] += 1
                            if "linkedin_url" in updates:
                                progress["total_linkedin_found"] += 1
                except Exception as e:
                    log(f"    Error: {e}")

                progress["total_processed"] += 1

            save_progress(progress, PROGRESS_FILE)
            log(f"Batch done. Processed={progress['total_processed']}, "
                f"Enriched={progress['total_enriched']}, "
                f"LinkedIn={progress['total_linkedin_found']}")

    save_progress(progress, PROGRESS_FILE)
    log(f"\nDONE — Processed: {progress['total_processed']}, "
        f"Enriched: {progress['total_enriched']}, "
        f"LinkedIn found: {progress['total_linkedin_found']}")


if __name__ == "__main__":
    main()
