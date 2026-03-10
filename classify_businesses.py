#!/usr/bin/env python3
"""
Classify ~2.8M SOS business records by keyword matching on business name.
Uses the server-side classify_businesses_by_name() plpgsql function
which batches updates internally (5000 rows at a time) to avoid timeouts.

Calls via Supabase PostgREST RPC endpoint.
"""

import os
import requests
import time
from pathlib import Path

# ─── Load .env ───
_env = Path(__file__).resolve().parent / ".env"
if _env.exists():
    for _l in _env.read_text().splitlines():
        _l = _l.strip()
        if _l and not _l.startswith("#") and "=" in _l:
            _k, _v = _l.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())


SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# Classifications: (new_type, [keyword_patterns])
CLASSIFICATIONS = [
    ("restaurant", [
        "%restaurant%", "%grill%", "%pizz%", "%burger%", "%taco%", "%sushi%",
        "%diner%", "%bistro%", "%cafe %", "%steakhouse%", "%bbq%", "%kitchen%food%",
    ]),
    ("auto_repair", [
        "%auto repair%", "%mechanic%", "%auto body%", "%collision%",
        "%brake%", "%muffler%", "%transmission%", "%tire %",
    ]),
    ("gas_station", [
        "%gas station%", "%fuel%", "%petroleum%", "%petro %", "%filling station%",
    ]),
    ("auto_dealer", [
        "%auto dealer%", "%car dealer%", "%motor%sale%", "%auto sale%", "%used car%",
    ]),
    ("real_estate", [
        "%real estate%", "%realty%", "%property%manage%", "%properties%",
    ]),
    ("construction", [
        "%construction%", "%contractor%", "%building%", "%roofing%",
        "%plumbing%", "%electric%service%", "%hvac%", "%paving%",
    ]),
    ("medical", [
        "%medical%", "%dental%", "%clinic%", "%physician%", "%doctor%",
        "%health%care%", "%chiropractic%", "%pediatric%", "%dermatolog%",
        "%urgent care%", "%optometr%", "%orthodont%",
    ]),
    ("gym", [
        "%fitness%", "%gym %", "%crossfit%", "%yoga%", "%martial art%", "%boxing%",
    ]),
]


def classify_pattern(new_type, pattern, batch_size=5000):
    """Call the server-side classify_businesses_by_name() function for one pattern."""
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/classify_businesses_by_name",
        headers=HEADERS,
        json={
            "p_new_type": new_type,
            "p_pattern": pattern,
            "p_batch_size": batch_size,
        },
        timeout=600,  # 10 minute timeout per pattern
    )
    if resp.status_code == 200:
        return resp.json()  # returns the integer count
    else:
        return f"ERROR {resp.status_code}: {resp.text[:200]}"


def count_classified(new_type):
    """Count how many records have been classified to this type from SOS sources."""
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/businesses?business_type=eq.{new_type}&data_source=in.(ny_sos,fl_sunbiz)&select=business_id",
        headers={**HEADERS, "Prefer": "count=exact", "Range": "0-0"},
        timeout=30,
    )
    count = resp.headers.get("content-range", "*/0").split("/")[-1]
    return int(count) if count != "*" else 0


def main():
    grand_total = 0

    for new_type, patterns in CLASSIFICATIONS:
        type_total = 0
        print(f"\n{'='*60}")
        print(f"Classifying: {new_type}")
        print(f"{'='*60}")

        for pattern in patterns:
            print(f"  {pattern}...", end=" ", flush=True)
            start = time.time()

            result = classify_pattern(new_type, pattern)
            elapsed = time.time() - start

            if isinstance(result, int):
                print(f"{result} rows ({elapsed:.1f}s)")
                type_total += result
            else:
                print(f"{result} ({elapsed:.1f}s)")

        final_count = count_classified(new_type)
        print(f"  >> Total {new_type}: {final_count} (added {type_total} this run)")
        grand_total += type_total

    print(f"\n{'='*60}")
    print(f"GRAND TOTAL CLASSIFIED THIS RUN: {grand_total}")
    print(f"{'='*60}")

    # Full summary including earlier migrations
    print("\nFull classification summary (SOS records):")
    all_types = [t for t, _ in CLASSIFICATIONS] + ["car_wash", "laundromat"]
    total_all = 0
    for t in all_types:
        c = count_classified(t)
        print(f"  {t}: {c:,}")
        total_all += c
    print(f"  TOTAL CLASSIFIED: {total_all:,}")


if __name__ == "__main__":
    main()
