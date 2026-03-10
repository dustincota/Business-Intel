#!/usr/bin/env python3
"""
Google Places -> electrical_companies table.

- Defaults to Florida and can scan any US state by passing a state code.
- Uses Google Places Nearby Search on a dense state grid.
- Pulls Place Details inline for each new result.
- Upserts directly to Supabase using google_place_id.

Usage:
    python3 gp_electrical_companies.py
    python3 gp_electrical_companies.py FL
    python3 gp_electrical_companies.py TX
"""

import json
import math
import os
from pathlib import Path
import sys
import time
from datetime import datetime

import requests


# ─── Load .env ───
_env = Path(__file__).resolve().parent / ".env"
if _env.exists():
    for _l in _env.read_text().splitlines():
        _l = _l.strip()
        if _l and not _l.startswith("#") and "=" in _l:
            _k, _v = _l.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

TABLE_NAME = "electrical_companies"
TARGET_STATE = (sys.argv[1].upper() if len(sys.argv) > 1 else "FL")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, f"electrical_{TARGET_STATE.lower()}.log")
PROGRESS_FILE = os.path.join(
    SCRIPT_DIR, f"electrical_{TARGET_STATE.lower()}_progress.json"
)
SEEN_FILE = os.path.join(SCRIPT_DIR, f"electrical_{TARGET_STATE.lower()}_seen.json")

SEARCH_RADIUS = 20000
GRID_STEP_KM = 16
DELAY_BETWEEN_GRID = 0.2
DELAY_BETWEEN_API = 0.5
DELAY_BETWEEN_UPSERTS = 1.0
BATCH_SIZE = 50

DETAIL_FIELDS = (
    "formatted_phone_number,website,formatted_address,address_components,"
    "opening_hours,business_status,rating,user_ratings_total,types"
)

STATE_BOXES = {
    "AL": (30.2, 35.0, -88.5, -84.9),
    "AK": (51.2, 71.4, -179.1, -130.0),
    "AZ": (31.3, 37.0, -114.8, -109.0),
    "AR": (33.0, 36.5, -94.6, -89.6),
    "CA": (32.5, 42.0, -124.4, -114.1),
    "CO": (37.0, 41.0, -109.1, -102.0),
    "CT": (41.0, 42.1, -73.7, -71.8),
    "DE": (38.5, 39.8, -75.8, -75.0),
    "FL": (24.4, 31.1, -87.7, -80.0),
    "GA": (30.4, 35.0, -85.6, -80.8),
    "HI": (18.9, 22.2, -160.2, -154.8),
    "ID": (42.0, 49.0, -117.2, -111.0),
    "IL": (37.0, 42.5, -91.5, -87.5),
    "IN": (37.8, 41.8, -88.1, -84.8),
    "IA": (40.4, 43.5, -96.6, -90.1),
    "KS": (37.0, 40.0, -102.1, -94.6),
    "KY": (36.5, 39.1, -89.6, -81.9),
    "LA": (29.0, 33.0, -94.0, -89.0),
    "ME": (43.1, 47.5, -71.1, -67.0),
    "MD": (38.0, 39.7, -79.5, -75.0),
    "MA": (41.2, 42.9, -73.5, -69.9),
    "MI": (41.7, 48.3, -90.4, -82.4),
    "MN": (43.5, 49.4, -97.2, -89.5),
    "MS": (30.2, 35.0, -91.7, -88.1),
    "MO": (36.0, 40.6, -95.8, -89.1),
    "MT": (44.4, 49.0, -116.0, -104.0),
    "NE": (40.0, 43.0, -104.1, -95.3),
    "NV": (35.0, 42.0, -120.0, -114.0),
    "NH": (42.7, 45.3, -72.6, -70.7),
    "NJ": (39.0, 41.4, -75.6, -73.9),
    "NM": (31.3, 37.0, -109.0, -103.0),
    "NY": (40.5, 45.0, -79.8, -71.9),
    "NC": (33.8, 36.6, -84.3, -75.5),
    "ND": (45.9, 49.0, -104.0, -96.6),
    "OH": (38.4, 42.0, -84.8, -80.5),
    "OK": (33.6, 37.0, -103.0, -94.4),
    "OR": (42.0, 46.3, -124.6, -116.5),
    "PA": (39.7, 42.3, -80.5, -74.7),
    "RI": (41.1, 42.0, -71.9, -71.1),
    "SC": (32.0, 35.2, -83.4, -78.5),
    "SD": (42.5, 46.0, -104.1, -96.4),
    "TN": (35.0, 36.7, -90.3, -81.6),
    "TX": (25.8, 36.5, -106.6, -93.5),
    "UT": (37.0, 42.0, -114.1, -109.0),
    "VT": (42.7, 45.0, -73.4, -71.5),
    "VA": (36.5, 39.5, -83.7, -75.2),
    "WA": (45.5, 49.0, -124.8, -116.9),
    "WV": (37.2, 40.6, -82.6, -77.7),
    "WI": (42.5, 47.1, -92.9, -86.8),
    "WY": (41.0, 45.0, -111.1, -104.1),
    "DC": (38.8, 39.0, -77.1, -77.0),
}

KNOWN_BRANDS = {
    "mister sparky": "Mister Sparky",
    "mr. electric": "Mr. Electric",
    "mrelectric": "Mr. Electric",
}


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"state": TARGET_STATE, "grid_index": 0, "total_found": 0, "total_inserted": 0, "api_calls": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen(seen):
    with open(SEEN_FILE, "w") as f:
        json.dump(sorted(seen), f)


def lat_to_state(lat, lon):
    best = None
    best_dist = float("inf")
    for state, (south, north, west, east) in STATE_BOXES.items():
        if south <= lat <= north and west <= lon <= east:
            center_lat = (south + north) / 2
            center_lon = (west + east) / 2
            dist = (lat - center_lat) ** 2 + (lon - center_lon) ** 2
            if dist < best_dist:
                best = state
                best_dist = dist
    return best


def is_in_us(lat, lon):
    if lat is None or lon is None:
        return False
    if 24.5 <= lat <= 49.5 and -125 <= lon <= -66:
        return True
    if 18.5 <= lat <= 22.5 and -161 <= lon <= -154:
        return True
    if 51 <= lat <= 72 and -180 <= lon <= -130:
        return True
    return False


def generate_state_grid(state):
    if state not in STATE_BOXES:
        raise ValueError(f"Unsupported state code: {state}")

    south, north, west, east = STATE_BOXES[state]
    lat_step = GRID_STEP_KM / 111.0
    points = []
    lat = south

    while lat <= north:
        km_per_lon = max(111.0 * math.cos(math.radians(lat)), 55.0)
        lon_step = GRID_STEP_KM / km_per_lon
        lon = west
        while lon <= east:
            points.append((round(lat, 6), round(lon, 6)))
            lon += lon_step
        lat += lat_step

    return points


def nearby_search(lat, lon, page_token=None, session=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {"key": GOOGLE_API_KEY, "type": "electrician", "radius": SEARCH_RADIUS}

    if page_token:
        time.sleep(2.0)
        params["pagetoken"] = page_token
    else:
        params["location"] = f"{lat},{lon}"

    for _attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=30)
            data = r.json()
            status = data.get("status")
            if status == "OK":
                return data.get("results", []), data.get("next_page_token")
            if status == "ZERO_RESULTS":
                return [], None
            if status == "OVER_QUERY_LIMIT":
                log("    Rate limited by Google Places, waiting 60s...")
                time.sleep(60)
                continue
            if status == "INVALID_REQUEST" and page_token:
                time.sleep(2.0)
                continue
            return [], None
        except Exception:
            time.sleep(2)
    return [], None


def get_place_details(place_id, session):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {"key": GOOGLE_API_KEY, "place_id": place_id, "fields": DETAIL_FIELDS}
    time.sleep(DELAY_BETWEEN_API)

    for _attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=20)
            data = r.json()
            if data.get("status") == "OK":
                return data.get("result", {})
            if data.get("status") == "OVER_QUERY_LIMIT":
                log("    Details rate limited, waiting 60s...")
                time.sleep(60)
                continue
        except Exception:
            time.sleep(2)
    return {}


def extract_address_parts(details):
    components = details.get("address_components", [])
    parts = {"street": None, "city": None, "state": None, "zip": None, "county": None}

    for component in components:
        types = component.get("types", [])
        if "street_number" in types:
            parts["street"] = component["long_name"]
        elif "route" in types:
            route = component["long_name"]
            parts["street"] = f"{parts['street']} {route}" if parts["street"] else route
        elif "locality" in types:
            parts["city"] = component["long_name"]
        elif "administrative_area_level_1" in types:
            parts["state"] = component["short_name"]
        elif "postal_code" in types:
            parts["zip"] = component["long_name"]
        elif "administrative_area_level_2" in types:
            parts["county"] = component["long_name"]

    return parts


def detect_brand(name):
    lowered = name.lower()
    for keyword, brand in KNOWN_BRANDS.items():
        if keyword in lowered:
            return brand
    return None


def classify_company(name):
    lowered = name.lower()
    if any(keyword in lowered for keyword in ("generator", "generac")):
        return "generator"
    if any(keyword in lowered for keyword in ("solar", "photovoltaic", "pv")):
        return "solar"
    if any(
        keyword in lowered
        for keyword in (
            "low voltage",
            "structured cabling",
            "fire alarm",
            "security",
            "telecom",
            "data cabling",
        )
    ):
        return "low_voltage"
    if "industrial" in lowered:
        return "industrial"
    if "commercial" in lowered:
        return "commercial"
    if any(keyword in lowered for keyword in ("residential", "home services", "home service")):
        return "residential"
    if "marine" in lowered:
        return "marine"
    if "sign" in lowered:
        return "signage"
    if any(keyword in lowered for keyword in ("emergency", "24/7", "24 hour")):
        return "emergency"
    return "general"


def place_to_record(place, details):
    place_id = place.get("place_id", "")
    name = place.get("name", "").strip()
    lat = place.get("geometry", {}).get("location", {}).get("lat")
    lon = place.get("geometry", {}).get("location", {}).get("lng")

    if not place_id or not name or not is_in_us(lat, lon):
        return None

    address_parts = extract_address_parts(details)
    state = address_parts["state"] or lat_to_state(lat, lon)
    if state != TARGET_STATE:
        return None

    city = address_parts["city"]
    if not city:
        vicinity = place.get("vicinity", "")
        pieces = vicinity.split(", ")
        city = pieces[-1] if len(pieces) > 1 else None

    google_types = details.get("types") or place.get("types") or []
    business_status = details.get("business_status") or place.get("business_status")

    return {
        "name": name,
        "brand": detect_brand(name),
        "electrician_type": classify_company(name),
        "address": address_parts["street"] or place.get("vicinity", "").split(",")[0],
        "city": city,
        "state": state,
        "zipcode": address_parts["zip"],
        "county": address_parts["county"],
        "latitude": lat,
        "longitude": lon,
        "phone": details.get("formatted_phone_number"),
        "website": details.get("website"),
        "google_place_id": place_id,
        "google_rating": details.get("rating") or place.get("rating"),
        "google_review_count": details.get("user_ratings_total") or place.get("user_ratings_total"),
        "business_status": business_status,
        "primary_place_type": google_types[0] if google_types else None,
        "place_types": google_types,
        "source_state": TARGET_STATE,
        "data_source": "google_places",
    }


def upsert_batch(records, session):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?on_conflict=google_place_id"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    for _attempt in range(3):
        try:
            r = session.post(url, headers=headers, json=records, timeout=60)
            if r.status_code in (200, 201):
                return len(records)
            if r.status_code >= 500:
                time.sleep(5)
                continue

            log(f"    Upsert error: {r.status_code} {r.text[:300]}")
            ok = 0
            for record in records:
                try:
                    r2 = session.post(url, headers=headers, json=[record], timeout=15)
                    if r2.status_code in (200, 201):
                        ok += 1
                except Exception:
                    pass
                time.sleep(0.2)
            return ok
        except Exception:
            time.sleep(3)
    return 0


def main():
    if TARGET_STATE not in STATE_BOXES:
        sys.exit(f"Unsupported state code: {TARGET_STATE}")
    if not GOOGLE_API_KEY or not SUPABASE_URL or not SUPABASE_KEY:
        sys.exit("Missing Google or Supabase credentials.")

    log("=" * 60)
    log(f"GOOGLE PLACES -> {TABLE_NAME} ({TARGET_STATE})")
    log("=" * 60)

    progress = load_progress()
    seen = load_seen()
    session = requests.Session()
    grid = generate_state_grid(TARGET_STATE)

    start_idx = progress.get("grid_index", 0)
    total_found = progress.get("total_found", 0)
    total_inserted = progress.get("total_inserted", 0)
    api_calls = progress.get("api_calls", 0)

    log(f"  Grid: {len(grid)} points | Resuming from {start_idx}")
    log(f"  Found so far: {total_found} | Inserted: {total_inserted} | API calls: {api_calls}")
    log(f"  Seen place_ids: {len(seen)}")

    batch = []

    for idx in range(start_idx, len(grid)):
        lat, lon = grid[idx]
        log(f"[{idx + 1}/{len(grid)}] Scanning {lat}, {lon}")

        results, next_page_token = nearby_search(lat, lon, session=session)
        api_calls += 1
        pages = 1

        while True:
            for place in results:
                place_id = place.get("place_id")
                if not place_id or place_id in seen:
                    continue

                seen.add(place_id)
                details = get_place_details(place_id, session)
                api_calls += 1
                record = place_to_record(place, details)
                if not record:
                    continue

                batch.append(record)
                total_found += 1

                if len(batch) >= BATCH_SIZE:
                    inserted = upsert_batch(batch, session)
                    total_inserted += inserted
                    log(f"    Upserted {inserted}/{len(batch)} | Total inserted: {total_inserted}")
                    batch = []
                    save_seen(seen)
                    time.sleep(DELAY_BETWEEN_UPSERTS)

            if not next_page_token or pages >= 3:
                break

            results, next_page_token = nearby_search(
                lat, lon, page_token=next_page_token, session=session
            )
            api_calls += 1
            pages += 1

        progress.update(
            {
                "state": TARGET_STATE,
                "grid_index": idx + 1,
                "total_found": total_found,
                "total_inserted": total_inserted,
                "api_calls": api_calls,
            }
        )
        save_progress(progress)

        if idx % 25 == 0:
            save_seen(seen)
            log(
                f"  Progress: {idx + 1}/{len(grid)} grid points | "
                f"Found: {total_found} | Inserted: {total_inserted} | API calls: {api_calls}"
            )

        time.sleep(DELAY_BETWEEN_GRID)

    if batch:
        inserted = upsert_batch(batch, session)
        total_inserted += inserted
        log(f"Final upsert: {inserted}/{len(batch)} | Total inserted: {total_inserted}")

    save_seen(seen)
    progress.update(
        {
            "state": TARGET_STATE,
            "grid_index": len(grid),
            "total_found": total_found,
            "total_inserted": total_inserted,
            "api_calls": api_calls,
        }
    )
    save_progress(progress)

    log("Done.")


if __name__ == "__main__":
    main()
