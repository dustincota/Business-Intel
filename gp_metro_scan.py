#!/usr/bin/env python3
"""
Google Places → car_washes — DENSE METRO SCAN.
Runs in parallel with the coarse grid scan.
- Targets top 100 US metro areas with tight 10km radius grid
- Uses separate progress/seen files to avoid conflicts
- Same Supabase upsert (google_place_id dedup handles overlap)
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
LOG_FILE = os.path.join(SCRIPT_DIR, "gp_metro.log")
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "gp_metro_progress.json")
SEEN_FILE = os.path.join(SCRIPT_DIR, "metro_places_seen.json")  # separate to avoid file conflicts

SEARCH_RADIUS = 10000  # 10km — much tighter than coarse scan
GRID_STEP_KM = 12      # step size in km (slight overlap with 10km radius)

DELAY_BETWEEN_API = 0.5
DELAY_BETWEEN_GRID = 0.2
DELAY_BETWEEN_UPSERTS = 1.0
BATCH_SIZE = 50

DETAIL_FIELDS = "formatted_phone_number,website,formatted_address,address_components,opening_hours,price_level,business_status,rating,user_ratings_total"

# Top 100 US metros: (name, center_lat, center_lon, radius_km)
METROS = [
    ("New York", 40.7128, -74.0060, 50),
    ("Los Angeles", 34.0522, -118.2437, 60),
    ("Chicago", 41.8781, -87.6298, 45),
    ("Houston", 29.7604, -95.3698, 50),
    ("Phoenix", 33.4484, -112.0740, 45),
    ("Philadelphia", 39.9526, -75.1652, 40),
    ("San Antonio", 29.4241, -98.4936, 35),
    ("San Diego", 32.7157, -117.1611, 35),
    ("Dallas", 32.7767, -96.7970, 50),
    ("Austin", 30.2672, -97.7431, 35),
    ("Jacksonville", 30.3322, -81.6557, 35),
    ("Fort Worth", 32.7555, -97.3308, 30),
    ("San Jose", 37.3382, -121.8863, 30),
    ("Columbus", 39.9612, -82.9988, 30),
    ("Charlotte", 35.2271, -80.8431, 35),
    ("Indianapolis", 39.7684, -86.1581, 35),
    ("San Francisco", 37.7749, -122.4194, 25),
    ("Seattle", 47.6062, -122.3321, 35),
    ("Denver", 39.7392, -104.9903, 40),
    ("Nashville", 36.1627, -86.7816, 35),
    ("Washington DC", 38.9072, -77.0369, 40),
    ("Oklahoma City", 35.4676, -97.5164, 35),
    ("El Paso", 31.7619, -106.4850, 25),
    ("Boston", 42.3601, -71.0589, 35),
    ("Portland", 45.5152, -122.6784, 30),
    ("Las Vegas", 36.1699, -115.1398, 35),
    ("Memphis", 35.1495, -90.0490, 30),
    ("Louisville", 38.2527, -85.7585, 30),
    ("Baltimore", 39.2904, -76.6122, 30),
    ("Milwaukee", 43.0389, -87.9065, 25),
    ("Albuquerque", 35.0844, -106.6504, 25),
    ("Tucson", 32.2226, -110.9747, 25),
    ("Fresno", 36.7378, -119.7871, 20),
    ("Sacramento", 38.5816, -121.4944, 30),
    ("Mesa", 33.4152, -111.8315, 20),
    ("Kansas City", 39.0997, -94.5786, 35),
    ("Atlanta", 33.7490, -84.3880, 45),
    ("Omaha", 41.2565, -95.9345, 25),
    ("Colorado Springs", 38.8339, -104.8214, 25),
    ("Raleigh", 35.7796, -78.6382, 30),
    ("Long Beach", 33.7701, -118.1937, 20),
    ("Virginia Beach", 36.8529, -75.9780, 25),
    ("Miami", 25.7617, -80.1918, 40),
    ("Oakland", 37.8044, -122.2712, 20),
    ("Minneapolis", 44.9778, -93.2650, 35),
    ("Tampa", 27.9506, -82.4572, 35),
    ("Tulsa", 36.1540, -95.9928, 30),
    ("Arlington TX", 32.7357, -97.1081, 20),
    ("New Orleans", 29.9511, -90.0715, 30),
    ("Wichita", 37.6872, -97.3301, 25),
    ("Cleveland", 41.4993, -81.6944, 30),
    ("Bakersfield", 35.3733, -119.0187, 20),
    ("Aurora CO", 39.7294, -104.8319, 20),
    ("Anaheim", 33.8366, -117.9143, 20),
    ("Honolulu", 21.3069, -157.8583, 20),
    ("Santa Ana", 33.7455, -117.8677, 15),
    ("Riverside", 33.9533, -117.3962, 30),
    ("Corpus Christi", 27.8006, -97.3964, 20),
    ("Pittsburgh", 40.4406, -79.9959, 30),
    ("Lexington", 38.0406, -84.5037, 20),
    ("Stockton", 37.9577, -121.2908, 15),
    ("St Louis", 38.6270, -90.1994, 35),
    ("Cincinnati", 39.1031, -84.5120, 30),
    ("St Paul", 44.9537, -93.0900, 20),
    ("Greensboro", 36.0726, -79.7920, 20),
    ("Newark", 40.7357, -74.1724, 15),
    ("Plano", 33.0198, -96.6989, 15),
    ("Henderson", 36.0395, -114.9817, 15),
    ("Lincoln", 40.8136, -96.7026, 20),
    ("Buffalo", 42.8864, -78.8784, 25),
    ("Fort Wayne", 41.0793, -85.1394, 20),
    ("Jersey City", 40.7178, -74.0431, 10),
    ("Chula Vista", 32.6401, -117.0842, 15),
    ("Norfolk", 36.8508, -76.2859, 20),
    ("St Petersburg", 27.7676, -82.6403, 20),
    ("Chandler", 33.3062, -111.8413, 15),
    ("Laredo", 27.5036, -99.5076, 15),
    ("Madison", 43.0731, -89.4012, 20),
    ("Durham", 35.9940, -78.8986, 20),
    ("Lubbock", 33.5779, -101.8552, 20),
    ("Winston-Salem", 36.0999, -80.2442, 20),
    ("Garland", 32.9126, -96.6389, 15),
    ("Glendale AZ", 33.5387, -112.1860, 15),
    ("Hialeah", 25.8576, -80.2781, 15),
    ("Reno", 39.5296, -119.8138, 20),
    ("Baton Rouge", 30.4515, -91.1871, 25),
    ("Irvine", 33.6846, -117.8265, 15),
    ("Chesapeake", 36.7682, -76.2875, 20),
    ("Irving", 32.8140, -96.9489, 15),
    ("Scottsdale", 33.4942, -111.9261, 20),
    ("North Las Vegas", 36.1989, -115.1175, 15),
    ("Fremont", 37.5485, -121.9886, 15),
    ("Gilbert", 33.3528, -111.7890, 15),
    ("San Bernardino", 34.1083, -117.2898, 25),
    ("Boise", 43.6150, -116.2023, 25),
    ("Birmingham", 33.5207, -86.8025, 30),
    ("Rochester", 43.1566, -77.6088, 20),
    ("Richmond", 37.5407, -77.4360, 25),
    ("Spokane", 47.6588, -117.4260, 20),
    ("Detroit", 42.3314, -83.0458, 40),
    ("Salt Lake City", 40.7608, -111.8910, 30),
    ("Des Moines", 41.5868, -93.6250, 25),
    ("Orlando", 28.5383, -81.3792, 35),
]

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
    return {"metro_index": 0, "grid_index": 0, "total_found": 0, "total_inserted": 0, "api_calls": 0}


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


def generate_metro_grid(center_lat, center_lon, radius_km):
    """Generate a tight grid of points covering a metro area."""
    points = []
    km_per_lat = 111.0
    km_per_lon = 111.0 * math.cos(math.radians(center_lat))

    lat_step = GRID_STEP_KM / km_per_lat
    lon_step = GRID_STEP_KM / km_per_lon

    lat = center_lat - (radius_km / km_per_lat)
    max_lat = center_lat + (radius_km / km_per_lat)

    while lat <= max_lat:
        lon = center_lon - (radius_km / km_per_lon)
        max_lon = center_lon + (radius_km / km_per_lon)
        while lon <= max_lon:
            # Only include points within the circular radius
            dlat = (lat - center_lat) * km_per_lat
            dlon = (lon - center_lon) * km_per_lon
            if math.sqrt(dlat**2 + dlon**2) <= radius_km:
                points.append((lat, lon))
            lon += lon_step
        lat += lat_step
    return points


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
    components = details.get('address_components', [])
    parts = {'street': None, 'city': None, 'state': None, 'zip': None, 'county': None}
    for c in components:
        types = c.get('types', [])
        if 'street_number' in types:
            parts['street'] = c['long_name']
        elif 'route' in types:
            route = c['long_name']
            parts['street'] = f"{parts['street']} {route}" if parts['street'] else route
        elif 'locality' in types:
            parts['city'] = c['long_name']
        elif 'administrative_area_level_1' in types:
            parts['state'] = c['short_name']
        elif 'postal_code' in types:
            parts['zip'] = c['long_name']
        elif 'administrative_area_level_2' in types:
            parts['county'] = c['long_name']
    return parts


def place_to_record(place, details):
    place_id = place.get('place_id', '')
    name = place.get('name', '').strip()
    lat = place.get('geometry', {}).get('location', {}).get('lat')
    lon = place.get('geometry', {}).get('location', {}).get('lng')

    if not is_in_us(lat, lon):
        return None

    addr = extract_address_parts(details)
    state = addr['state']
    city = addr['city']
    if not city:
        vicinity = place.get('vicinity', '')
        parts = vicinity.split(', ')
        city = parts[-1] if len(parts) > 1 else None

    rating = details.get('rating') or place.get('rating')
    review_count = details.get('user_ratings_total') or place.get('user_ratings_total')
    biz_status = details.get('business_status') or place.get('business_status', '')

    return {
        'name': name,
        'brand': detect_chain(name),
        'wash_type': classify_wash(name),
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
    log("GOOGLE PLACES → car_washes (DENSE METRO SCAN)")
    log("=" * 60)

    progress = load_progress()
    seen = load_seen()
    session = requests.Session()

    start_metro = progress["metro_index"]
    start_grid = progress["grid_index"]
    total_found = progress["total_found"]
    total_inserted = progress["total_inserted"]
    api_calls = progress["api_calls"]

    log(f"  Metros: {len(METROS)} | Resuming from metro {start_metro}, grid {start_grid}")
    log(f"  Found: {total_found} | Inserted: {total_inserted} | API calls: {api_calls}")
    log(f"  Seen place_ids: {len(seen)}")

    batch = []
    start_time = time.time()

    for mi in range(start_metro, len(METROS)):
        metro_name, clat, clon, radius_km = METROS[mi]
        grid = generate_metro_grid(clat, clon, radius_km)

        gi_start = start_grid if mi == start_metro else 0
        metro_new = 0

        log(f"\n  [{mi+1}/{len(METROS)}] {metro_name} — {len(grid)} grid points, {radius_km}km radius")

        for gi in range(gi_start, len(grid)):
            lat, lon = grid[gi]

            results, next_token = nearby_search(lat, lon, session=session)
            api_calls += 1
            time.sleep(DELAY_BETWEEN_API)

            all_results = list(results)
            pages = 1
            while next_token and pages < 3:
                time.sleep(2)
                more, next_token = nearby_search(lat, lon, page_token=next_token, session=session)
                api_calls += 1
                all_results.extend(more)
                pages += 1

            for place in all_results:
                pid = place.get('place_id')
                if not pid or pid in seen:
                    continue

                plat = place.get('geometry', {}).get('location', {}).get('lat')
                plon = place.get('geometry', {}).get('location', {}).get('lng')
                if not is_in_us(plat, plon):
                    seen.add(pid)
                    continue

                details = get_place_details(pid, session)
                api_calls += 1
                seen.add(pid)

                record = place_to_record(place, details)
                if record:
                    batch.append(record)
                    total_found += 1
                    metro_new += 1

            if len(batch) >= BATCH_SIZE:
                inserted = upsert_batch(batch, session)
                total_inserted += inserted
                batch = []
                time.sleep(DELAY_BETWEEN_UPSERTS)

            # Save progress every 25 grid points
            if (gi + 1) % 25 == 0:
                progress.update(metro_index=mi, grid_index=gi+1, total_found=total_found,
                               total_inserted=total_inserted, api_calls=api_calls)
                save_progress(progress)
                save_seen(seen)

            time.sleep(DELAY_BETWEEN_GRID)

        log(f"  [{mi+1}/{len(METROS)}] {metro_name} done — +{metro_new} new | "
            f"Total: {total_found:,} found, {total_inserted:,} inserted | {api_calls:,} API calls")

        # Save after each metro
        progress.update(metro_index=mi+1, grid_index=0, total_found=total_found,
                       total_inserted=total_inserted, api_calls=api_calls)
        save_progress(progress)
        save_seen(seen)

    # Final flush
    if batch:
        inserted = upsert_batch(batch, session)
        total_inserted += inserted

    progress.update(metro_index=len(METROS), grid_index=0, total_found=total_found,
                   total_inserted=total_inserted, api_calls=api_calls)
    save_progress(progress)
    save_seen(seen)

    log(f"\nDONE — {total_found:,} found, {total_inserted:,} inserted, {api_calls:,} API calls")


if __name__ == "__main__":
    main()
