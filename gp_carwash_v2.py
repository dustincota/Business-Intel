#!/usr/bin/env python3
"""
Google Places → car_washes table (direct).
- Nearby Search to find car washes on a US grid
- Place Details inline for each new result (phone, website, zip, hours, rating)
- Writes directly to car_washes table via google_place_id upsert
- Throttled: 0.5s between API calls, batches of 50 with 2s pauses
"""

import requests, json, time, sys, os, math
from datetime import datetime
from pathlib import Path

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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, "gp_v2.log")
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "gp_v2_progress.json")
SEEN_FILE = os.path.join(SCRIPT_DIR, "google_places_seen.json")

SEARCH_RADIUS = 50000
GRID_SPACING_LAT = 0.63
GRID_SPACING_LON_BASE = 0.80

# Throttle settings
DELAY_BETWEEN_GRID = 0.3       # seconds between grid points
DELAY_BETWEEN_API = 0.5        # seconds between Google API calls
DELAY_BETWEEN_UPSERTS = 1.0    # seconds between Supabase writes
BATCH_SIZE = 50                 # records per Supabase upsert

DETAIL_FIELDS = "formatted_phone_number,website,formatted_address,address_components,opening_hours,price_level,business_status,rating,user_ratings_total"

STATE_BOXES = {
    'AL': (30.2, 35.0, -88.5, -84.9), 'AK': (51.2, 71.4, -179.1, -130.0),
    'AZ': (31.3, 37.0, -114.8, -109.0), 'AR': (33.0, 36.5, -94.6, -89.6),
    'CA': (32.5, 42.0, -124.4, -114.1), 'CO': (37.0, 41.0, -109.1, -102.0),
    'CT': (41.0, 42.1, -73.7, -71.8), 'DE': (38.5, 39.8, -75.8, -75.0),
    'FL': (24.5, 31.0, -87.6, -80.0), 'GA': (30.4, 35.0, -85.6, -80.8),
    'HI': (18.9, 22.2, -160.2, -154.8), 'ID': (42.0, 49.0, -117.2, -111.0),
    'IL': (37.0, 42.5, -91.5, -87.5), 'IN': (37.8, 41.8, -88.1, -84.8),
    'IA': (40.4, 43.5, -96.6, -90.1), 'KS': (37.0, 40.0, -102.1, -94.6),
    'KY': (36.5, 39.1, -89.6, -81.9), 'LA': (29.0, 33.0, -94.0, -89.0),
    'ME': (43.1, 47.5, -71.1, -67.0), 'MD': (38.0, 39.7, -79.5, -75.0),
    'MA': (41.2, 42.9, -73.5, -69.9), 'MI': (41.7, 48.3, -90.4, -82.4),
    'MN': (43.5, 49.4, -97.2, -89.5), 'MS': (30.2, 35.0, -91.7, -88.1),
    'MO': (36.0, 40.6, -95.8, -89.1), 'MT': (44.4, 49.0, -116.0, -104.0),
    'NE': (40.0, 43.0, -104.1, -95.3), 'NV': (35.0, 42.0, -120.0, -114.0),
    'NH': (42.7, 45.3, -72.6, -70.7), 'NJ': (39.0, 41.4, -75.6, -73.9),
    'NM': (31.3, 37.0, -109.0, -103.0), 'NY': (40.5, 45.0, -79.8, -71.9),
    'NC': (33.8, 36.6, -84.3, -75.5), 'ND': (45.9, 49.0, -104.0, -96.6),
    'OH': (38.4, 42.0, -84.8, -80.5), 'OK': (33.6, 37.0, -103.0, -94.4),
    'OR': (42.0, 46.3, -124.6, -116.5), 'PA': (39.7, 42.3, -80.5, -74.7),
    'RI': (41.1, 42.0, -71.9, -71.1), 'SC': (32.0, 35.2, -83.4, -78.5),
    'SD': (42.5, 46.0, -104.1, -96.4), 'TN': (35.0, 36.7, -90.3, -81.6),
    'TX': (25.8, 36.5, -106.6, -93.5), 'UT': (37.0, 42.0, -114.1, -109.0),
    'VT': (42.7, 45.0, -73.4, -71.5), 'VA': (36.5, 39.5, -83.7, -75.2),
    'WA': (45.5, 49.0, -124.8, -116.9), 'WV': (37.2, 40.6, -82.6, -77.7),
    'WI': (42.5, 47.1, -92.9, -86.8), 'WY': (41.0, 45.0, -111.1, -104.1),
    'DC': (38.8, 39.0, -77.1, -77.0),
}

CHAINS = {
    'mister car wash': 'Mister Car Wash',
    'take 5': 'Take 5 Car Wash', 'take five': 'Take 5 Car Wash',
    'tommy': "Tommy's Express", "tommy's": "Tommy's Express",
    'tidal wave': 'Tidal Wave Auto Spa',
    'zips car wash': 'Zips Car Wash',
    'whistle express': 'Whistle Express',
    'quick quack': 'Quick Quack Car Wash',
    'club car wash': 'Club Car Wash',
    'splash car wash': 'Splash Car Wash',
    'delta sonic': 'Delta Sonic',
    'super star': 'Super Star Car Wash',
    'go car wash': 'Go Car Wash',
    'crew car wash': 'Crew Car Wash',
    'autobell': 'Autobell Car Wash',
    'spin car wash': 'Spin Car Wash',
    'flying ace': 'Flying Ace Express',
    'car wash usa': 'Car Wash USA Express',
    'greenlight': 'Greenlight Car Wash',
    'spotless': 'Spotless Car Wash',
    'soapy joe': "Soapy Joe's",
    'bluebird': 'Bluebird Express Car Wash',
    'rapids car wash': 'Rapids Car Wash',
    'goo-goo': 'Goo Goo Car Wash',
    'palms car wash': 'Palms Car Wash',
    'mr. clean': 'Mr. Clean Car Wash',
    'jacksons': "Jackson's Car Wash",
    'rainstorm': 'Rainstorm Car Wash',
    'mammoth': 'Mammoth Car Wash',
    'clean machine': 'Clean Machine Car Wash',
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
    return {"grid_index": 0, "total_found": 0, "total_inserted": 0, "api_calls": 0}


def save_progress(p):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)


def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen(seen):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)


def lat_to_state(lat, lon):
    best, best_dist = None, float('inf')
    for state, (s, n, w, e) in STATE_BOXES.items():
        if s <= lat <= n and w <= lon <= e:
            clat, clon = (s + n) / 2, (w + e) / 2
            dist = (lat - clat)**2 + (lon - clon)**2
            if dist < best_dist:
                best_dist, best = dist, state
    return best


def is_in_us(lat, lon):
    """Filter out Mexico/Canada results."""
    if lat is None or lon is None:
        return False
    # Continental US + Hawaii + Alaska
    if 24.5 <= lat <= 49.5 and -125 <= lon <= -66:
        return True
    if 18.5 <= lat <= 22.5 and -161 <= lon <= -154:  # Hawaii
        return True
    if 51 <= lat <= 72 and -180 <= lon <= -130:  # Alaska
        return True
    return False


def generate_grid():
    points = []
    lat = 24.5
    while lat <= 49.5:
        lon_spacing = GRID_SPACING_LON_BASE / max(math.cos(math.radians(lat)), 0.5)
        lon = -125.0
        while lon <= -66.0:
            points.append((lat, lon))
            lon += lon_spacing
        lat += GRID_SPACING_LAT
    # Alaska
    lat = 55.0
    while lat <= 65.0:
        lon = -165.0
        while lon <= -140.0:
            points.append((lat, lon))
            lon += 1.5
        lat += 1.0
    # Hawaii
    for p in [(21.31, -157.86), (20.89, -156.47), (19.72, -155.08), (19.64, -155.99), (21.97, -159.37)]:
        points.append(p)
    return points


def classify_wash(name):
    n = name.lower()
    if any(k in n for k in ['flex', 'hybrid']): return 'flex_serve'
    if any(k in n for k in ['full service', 'full-service', 'detail', 'hand wash', 'hand-wash', 'valet']): return 'full_service'
    if any(k in n for k in ['self serve', 'self-serve', 'self service', 'coin', 'diy', 'u-wash', 'u wash']): return 'self_service'
    if any(k in n for k in ['express', 'tunnel', 'drive-thru', 'drive thru', 'quick', 'speedy', 'zip', 'fast', 'splash', 'wave', 'mister', 'take 5', 'tommy', 'zips', 'tidal wave', 'whistle']): return 'tunnel'
    return 'unknown'


def detect_chain(name):
    n = name.lower()
    for kw, chain in CHAINS.items():
        if kw in n:
            return chain
    return None


def nearby_search(lat, lon, page_token=None, session=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {'key': GOOGLE_API_KEY, 'type': 'car_wash', 'radius': SEARCH_RADIUS}
    if page_token:
        params['pagetoken'] = page_token
    else:
        params['location'] = f"{lat},{lon}"

    for attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=30)
            data = r.json()
            status = data.get('status')
            if status == 'OK':
                return data.get('results', []), data.get('next_page_token')
            elif status == 'ZERO_RESULTS':
                return [], None
            elif status == 'OVER_QUERY_LIMIT':
                log(f"    Rate limited, waiting 60s...")
                time.sleep(60)
            else:
                return [], None
        except Exception as e:
            time.sleep(2)
    return [], None


def get_place_details(place_id, session):
    """Get enriched details for a place."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {'key': GOOGLE_API_KEY, 'place_id': place_id, 'fields': DETAIL_FIELDS}

    time.sleep(DELAY_BETWEEN_API)
    try:
        r = session.get(url, params=params, timeout=20)
        data = r.json()
        if data.get('status') == 'OK':
            return data.get('result', {})
    except:
        pass
    return {}


def extract_address_parts(details):
    """Parse address_components from Place Details."""
    components = details.get('address_components', [])
    parts = {'street': None, 'city': None, 'state': None, 'zip': None, 'county': None}

    for c in components:
        types = c.get('types', [])
        if 'street_number' in types:
            parts['street'] = c['long_name']
        elif 'route' in types:
            route = c['long_name']
            if parts['street']:
                parts['street'] = f"{parts['street']} {route}"
            else:
                parts['street'] = route
        elif 'locality' in types:
            parts['city'] = c['long_name']
        elif 'administrative_area_level_1' in types:
            parts['state'] = c['short_name']
        elif 'postal_code' in types:
            parts['zip'] = c['long_name']
        elif 'administrative_area_level_2' in types:
            parts['county'] = c['long_name']

    return parts


def format_hours(details):
    """Extract opening hours as JSON string."""
    oh = details.get('opening_hours', {})
    if not oh:
        return None
    weekday = oh.get('weekday_text', [])
    if weekday:
        return json.dumps(weekday)
    return None


def place_to_carwash_record(place, details):
    """Convert Nearby Search result + Details into a car_washes record."""
    place_id = place.get('place_id', '')
    name = place.get('name', '').strip()

    lat = place.get('geometry', {}).get('location', {}).get('lat')
    lon = place.get('geometry', {}).get('location', {}).get('lng')

    if not is_in_us(lat, lon):
        return None

    # Use details for richer data
    addr = extract_address_parts(details)

    # Fallback to nearby search data
    state = addr['state'] or lat_to_state(lat, lon)
    city = addr['city']
    if not city:
        vicinity = place.get('vicinity', '')
        parts = vicinity.split(', ')
        city = parts[-1] if len(parts) > 1 else None

    # Ratings - prefer details
    rating = details.get('rating') or place.get('rating')
    review_count = details.get('user_ratings_total') or place.get('user_ratings_total')

    wash_type = classify_wash(name)
    chain = detect_chain(name)
    biz_status = details.get('business_status') or place.get('business_status', '')

    return {
        'name': name,
        'brand': chain,
        'wash_type': wash_type,
        'address': addr['street'] or place.get('vicinity', '').split(',')[0],
        'city': city,
        'state': state,
        'zipcode': addr['zip'],
        'county': addr['county'],
        'latitude': lat,
        'longitude': lon,
        'phone': details.get('formatted_phone_number'),
        'website': details.get('website'),
        'google_place_id': place_id,
        'google_rating': rating,
        'google_review_count': review_count,
        'data_source': 'google_places',
    }


def upsert_batch(records, session):
    """Upsert to car_washes table."""
    url = f"{SUPABASE_URL}/rest/v1/car_washes?on_conflict=google_place_id"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    for attempt in range(3):
        try:
            r = session.post(url, headers=headers, json=records, timeout=60)
            if r.status_code in (200, 201):
                return len(records)
            elif r.status_code >= 500:
                time.sleep(5)
                continue
            else:
                log(f"    Upsert error: {r.status_code} {r.text[:300]}")
                # Try one by one
                ok = 0
                for rec in records:
                    try:
                        r2 = session.post(url, headers=headers, json=[rec], timeout=15)
                        if r2.status_code in (200, 201):
                            ok += 1
                    except:
                        pass
                    time.sleep(0.2)
                return ok
        except Exception as e:
            time.sleep(3)
    return 0


def main():
    log("=" * 60)
    log("GOOGLE PLACES → car_washes (v2, throttled)")
    log("=" * 60)

    progress = load_progress()
    seen = load_seen()
    session = requests.Session()
    grid = generate_grid()

    start_idx = progress["grid_index"]
    total_found = progress["total_found"]
    total_inserted = progress["total_inserted"]
    api_calls = progress["api_calls"]

    log(f"  Grid: {len(grid)} points | Resuming from {start_idx}")
    log(f"  Found so far: {total_found} | Inserted: {total_inserted} | API calls: {api_calls}")
    log(f"  Seen place_ids: {len(seen)}")

    batch = []
    start_time = time.time()

    for idx in range(start_idx, len(grid)):
        lat, lon = grid[idx]

        # Nearby Search
        results, next_token = nearby_search(lat, lon, session=session)
        api_calls += 1
        time.sleep(DELAY_BETWEEN_API)

        page_new = 0
        all_results = list(results)

        # Paginate (up to 2 more pages)
        pages = 1
        while next_token and pages < 3:
            time.sleep(2)  # Google requires delay for next_page_token
            more, next_token = nearby_search(lat, lon, page_token=next_token, session=session)
            api_calls += 1
            all_results.extend(more)
            pages += 1

        # Process each result
        for place in all_results:
            pid = place.get('place_id')
            if not pid or pid in seen:
                continue

            plat = place.get('geometry', {}).get('location', {}).get('lat')
            plon = place.get('geometry', {}).get('location', {}).get('lng')
            if not is_in_us(plat, plon):
                seen.add(pid)
                continue

            # Get details (phone, website, zip, hours)
            details = get_place_details(pid, session)
            api_calls += 1

            seen.add(pid)
            record = place_to_carwash_record(place, details)
            if record:
                batch.append(record)
                total_found += 1
                page_new += 1

        # Upsert batch
        if len(batch) >= BATCH_SIZE:
            inserted = upsert_batch(batch, session)
            total_inserted += inserted
            batch = []
            time.sleep(DELAY_BETWEEN_UPSERTS)

        # Log progress
        if page_new > 0 or (idx - start_idx + 1) % 50 == 0:
            elapsed = time.time() - start_time
            rate = (idx - start_idx + 1) / elapsed * 60 if elapsed > 0 else 0
            log(f"  Grid {idx+1}/{len(grid)} ({lat:.1f},{lon:.1f}): "
                f"+{page_new} new | Total: {total_found:,} found, {total_inserted:,} inserted | "
                f"{api_calls:,} API calls | {rate:.0f} pts/min")

        # Save progress every 25 grid points
        if (idx - start_idx + 1) % 25 == 0:
            progress.update(grid_index=idx+1, total_found=total_found,
                          total_inserted=total_inserted, api_calls=api_calls)
            save_progress(progress)
            save_seen(seen)

        time.sleep(DELAY_BETWEEN_GRID)

    # Final flush
    if batch:
        inserted = upsert_batch(batch, session)
        total_inserted += inserted

    progress.update(grid_index=len(grid), total_found=total_found,
                   total_inserted=total_inserted, api_calls=api_calls)
    save_progress(progress)
    save_seen(seen)

    log(f"\nDONE — {total_found:,} found, {total_inserted:,} inserted, {api_calls:,} API calls")


if __name__ == "__main__":
    main()
