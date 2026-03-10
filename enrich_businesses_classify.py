#!/usr/bin/env python3
"""
Classify Secretary of State business records by type using name analysis.
Zero API cost — pure text classification.

For 2.8M SOS records (ny_sos, fl_sunbiz) that have just a business name,
this script:
1. Parses business names for industry keywords
2. Assigns/refines business_type
3. Attempts fuzzy dedup against existing enriched businesses
4. Marks records as 'classified'

Run: nohup python3 enrich_businesses_classify.py >> enrich_classify_stdout.log 2>&1 &
"""

import re
import time
from enrich_utils import (
    SupabaseClient,
    make_logger, load_progress, save_progress,
    clean_business_name, utcnow_iso,
)

log = make_logger("enrich_businesses_classify.log")
PROGRESS_FILE = "enrich_businesses_classify_progress.json"
BATCH_SIZE = 500  # Large batches since no API calls

# Business type classification rules (keyword → type)
# Order matters — first match wins
CLASSIFICATION_RULES = [
    # Car wash (highest priority for this project)
    (['car wash', 'carwash', 'auto wash', 'autowash', 'auto spa',
      'express wash', 'tunnel wash', 'hand wash', 'wash express',
      'quick wash', 'speed wash', 'splash wash', 'squeaky clean',
      'bubble bath car', 'suds', 'mr clean car', 'super wash'],
     'car_wash'),

    # Auto services
    (['auto repair', 'auto body', 'collision', 'mechanic', 'brake',
      'transmission', 'muffler', 'tire shop', 'tire center',
      'oil change', 'lube', 'jiffy', 'meineke', 'midas',
      'pep boys', 'firestone', 'goodyear', 'maaco'],
     'auto_repair'),

    # Laundromat / dry cleaning
    (['laundromat', 'laundry', 'dry clean', 'cleaners', 'wash & fold',
      'coin laundry', 'self-service laundry'],
     'laundromat'),

    # Restaurant / food
    (['restaurant', 'pizza', 'burger', 'grill', 'diner', 'cafe',
      'bistro', 'sushi', 'taco', 'bbq', 'steakhouse', 'seafood',
      'kitchen', 'eatery', 'food truck', 'catering'],
     'restaurant'),

    # Retail
    (['retail', 'store', 'shop', 'mart', 'boutique', 'outlet',
      'wholesale', 'discount'],
     'retail'),

    # Gas station / convenience
    (['gas station', 'fuel', 'petroleum', 'shell', 'exxon', 'chevron',
      'bp ', 'mobil', 'sunoco', 'citgo', 'marathon', '7-eleven',
      'wawa', 'sheetz', 'quicktrip', 'racetrac', 'casey'],
     'gas_station'),

    # Convenience store
    (['convenience', 'c-store', 'mini mart', 'minimart'],
     'convenience_store'),

    # Salon / beauty
    (['salon', 'barber', 'beauty', 'hair', 'nail', 'spa ', 'massage',
      'waxing', 'lash', 'brow', 'cosmetology'],
     'salon'),

    # Gym / fitness
    (['gym', 'fitness', 'crossfit', 'yoga', 'pilates', 'boxing',
      'martial art', 'workout', 'planet fitness', 'anytime fitness',
      'orangetheory', 'gold gym'],
     'gym'),

    # Medical / dental
    (['medical', 'clinic', 'dental', 'dentist', 'doctor', 'physician',
      'hospital', 'urgent care', 'pharmacy', 'chiropractic',
      'optom', 'dermat', 'pediatr', 'orthoped'],
     'medical'),

    # Real estate
    (['real estate', 'realty', 'property', 'properties', 'landlord',
      'housing', 'apartment', 'rental', 'mortgage'],
     'real_estate'),

    # Construction / trades
    (['construction', 'plumbing', 'plumber', 'electrical', 'electrician',
      'hvac', 'heating', 'cooling', 'roofing', 'roofer', 'painting',
      'painter', 'landscap', 'concrete', 'mason', 'framing',
      'drywall', 'flooring', 'carpent', 'handyman'],
     'trades'),

    # Lodging
    (['hotel', 'motel', 'inn ', 'lodge', 'resort', 'hostel',
      'bed and breakfast', 'b&b', 'vacation rental', 'airbnb'],
     'lodging'),

    # Insurance
    (['insurance', 'state farm', 'allstate', 'geico', 'progressive',
      'liberty mutual', 'nationwide'],
     'insurance'),

    # Accounting / finance
    (['accounting', 'accountant', 'cpa', 'bookkeeping', 'tax prep',
      'financial advis', 'investment', 'wealth management'],
     'financial_services'),

    # Legal
    (['law firm', 'attorney', 'lawyer', 'legal', 'law office',
      'counsel', 'esquire'],
     'legal'),

    # Technology
    (['software', 'tech', 'digital', 'app ', 'saas', 'cloud',
      'cyber', 'data ', 'ai ', 'machine learning', 'web develop'],
     'technology'),

    # Education
    (['school', 'academy', 'university', 'college', 'tutor',
      'education', 'learning center', 'training'],
     'education'),

    # Religious
    (['church', 'temple', 'mosque', 'synagogue', 'chapel',
      'ministry', 'congregation', 'parish', 'baptist', 'methodist',
      'catholic', 'lutheran', 'presbyterian', 'episcopal'],
     'religious'),

    # Parking
    (['parking', 'garage ', 'valet', 'park & ride'],
     'parking'),
]


def classify_business(name):
    """Classify a business by its name. Returns business_type or None."""
    if not name:
        return None
    name_lower = name.lower()

    for keywords, biz_type in CLASSIFICATION_RULES:
        for kw in keywords:
            if kw in name_lower:
                return biz_type

    return None  # Can't classify


def main():
    log("=" * 60)
    log("BUSINESS NAME CLASSIFICATION — Starting")
    log("=" * 60)

    db = SupabaseClient()

    progress = load_progress(PROGRESS_FILE, {
        "total_processed": 0,
        "total_classified": 0,
        "type_counts": {},
    })

    batch_num = 0
    while True:
        # Fetch SOS records that haven't been classified
        batch = db.fetch(
            "businesses",
            filters="data_source=in.(ny_sos,fl_sunbiz)&enrichment_status=eq.raw",
            select="business_id,name,business_type",
            order="created_at.asc",
            limit=BATCH_SIZE,
        )

        if not batch:
            log("No more SOS records to classify")
            break

        batch_num += 1
        if batch_num % 10 == 1:
            log(f"Batch {batch_num}: {len(batch)} records")

        classified_in_batch = 0
        for biz in batch:
            biz_id = biz["business_id"]
            name = biz.get("name", "")
            current_type = biz.get("business_type")

            # Skip if already well-typed (not generic llc/corporation/business)
            if current_type and current_type not in ('llc', 'corporation', 'business', 'partnership', 'nonprofit'):
                # Already classified to a specific type
                db.update("businesses", "business_id", biz_id, {
                    "enrichment_status": "classified",
                    "updated_at": utcnow_iso(),
                })
                progress["total_processed"] += 1
                continue

            new_type = classify_business(name)
            updates = {
                "enrichment_status": "classified",
                "updated_at": utcnow_iso(),
            }
            if new_type:
                updates["business_type"] = new_type
                classified_in_batch += 1
                progress["type_counts"][new_type] = progress["type_counts"].get(new_type, 0) + 1

            db.update("businesses", "business_id", biz_id, updates)
            progress["total_processed"] += 1

        progress["total_classified"] += classified_in_batch

        if batch_num % 20 == 0:
            save_progress(progress, PROGRESS_FILE)
            log(f"Progress: processed={progress['total_processed']}, "
                f"classified={progress['total_classified']}")
            # Show top types
            top = sorted(progress["type_counts"].items(), key=lambda x: -x[1])[:10]
            log(f"Top types: {dict(top)}")

    save_progress(progress, PROGRESS_FILE)
    log(f"\nDONE — Processed: {progress['total_processed']}, "
        f"Classified: {progress['total_classified']}")
    log(f"Type breakdown:")
    for btype, count in sorted(progress["type_counts"].items(), key=lambda x: -x[1]):
        log(f"  {btype}: {count}")


if __name__ == "__main__":
    main()
