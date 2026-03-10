#!/usr/bin/env python3
"""
Validate firm websites via DNS resolution + HTTP HEAD checks.
Zero API cost — just DNS lookups and HTTP HEAD requests.

Clears invalid websites, normalizes domains, marks validated ones.

Run: nohup python3 enrich_firms_websites.py >> enrich_firms_web_stdout.log 2>&1 &
"""

import time
import concurrent.futures
from enrich_utils import (
    SupabaseClient,
    make_logger, load_progress, save_progress,
    extract_domain, validate_domain, check_website, utcnow_iso,
)

log = make_logger("enrich_firms_websites.log")
PROGRESS_FILE = "enrich_firms_websites_progress.json"
BATCH_SIZE = 100
MAX_WORKERS = 5  # Concurrent DNS/HTTP checks


def validate_firm_website(firm):
    """Check if a firm's website is valid. Returns (firm_id, updates)."""
    firm_id = firm["firm_id"]
    website = firm.get("website") or ""
    domain = extract_domain(website)

    if not domain:
        return firm_id, {"website": None, "updated_at": utcnow_iso()}

    # DNS check
    if not validate_domain(domain):
        return firm_id, {"website": None, "updated_at": utcnow_iso()}

    # HTTP HEAD check
    status = check_website(domain)
    if status and status < 400:
        # Valid — normalize the website URL
        normalized = f"https://{domain}"
        updates = {"updated_at": utcnow_iso()}
        if website != normalized:
            updates["website"] = normalized
        return firm_id, updates
    elif status and status >= 400:
        return firm_id, {"website": None, "updated_at": utcnow_iso()}
    else:
        # Timeout/error — keep it but don't validate
        return firm_id, {"updated_at": utcnow_iso()}


def main():
    log("=" * 60)
    log("FIRM WEBSITE VALIDATION — Starting")
    log("=" * 60)

    db = SupabaseClient()

    progress = load_progress(PROGRESS_FILE, {
        "total_checked": 0,
        "total_valid": 0,
        "total_invalid": 0,
        "total_cleared": 0,
    })

    batch_num = 0
    # Use updated_at cursor to avoid re-checking
    # Firms that haven't been touched recently
    while True:
        batch = db.fetch(
            "firms",
            filters="website=not.is.null",
            select="firm_id,name,website",
            order="created_at.asc",
            limit=BATCH_SIZE,
            offset=progress["total_checked"],
        )

        if not batch:
            log("No more firms with websites to check")
            break

        batch_num += 1
        log(f"\nBatch {batch_num}: {len(batch)} firms")

        # Parallel DNS/HTTP checks
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(validate_firm_website, firm): firm
                      for firm in batch}

            for future in concurrent.futures.as_completed(futures):
                try:
                    firm_id, updates = future.result()
                    if updates:
                        db.update("firms", "firm_id", firm_id, updates)
                        if updates.get("website") is None and "website" in updates:
                            progress["total_cleared"] += 1
                            progress["total_invalid"] += 1
                        else:
                            progress["total_valid"] += 1
                    progress["total_checked"] += 1
                except Exception as e:
                    progress["total_checked"] += 1
                    log(f"  Error: {e}")

        save_progress(progress, PROGRESS_FILE)
        log(f"Batch {batch_num} done. "
            f"Checked={progress['total_checked']}, "
            f"Valid={progress['total_valid']}, "
            f"Invalid={progress['total_invalid']}, "
            f"Cleared={progress['total_cleared']}")

    save_progress(progress, PROGRESS_FILE)
    log(f"\nDONE — Checked: {progress['total_checked']}, "
        f"Valid: {progress['total_valid']}, Invalid: {progress['total_invalid']}")


if __name__ == "__main__":
    main()
