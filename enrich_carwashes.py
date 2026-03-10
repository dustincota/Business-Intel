#!/usr/bin/env python3
"""
Enrich existing car_washes records with:
- Google Place Details (reviews text, hours, photos, open status)
- Traffic/volume estimates (based on review count + location + type)
- Revenue & valuation estimates (industry benchmarks)
- Acquisition scoring

Processes records that haven't been enriched yet (last_enriched_at IS NULL).
"""

import requests, json, time, os, math
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
LOG_FILE = os.path.join(SCRIPT_DIR, "enrich.log")

DETAIL_FIELDS = "formatted_phone_number,website,formatted_address,address_components,reviews,opening_hours,business_status,rating,user_ratings_total,price_level,photos"
DELAY_BETWEEN_API = 0.5
BATCH_SIZE = 100  # records to fetch from Supabase at a time

# ─── Revenue benchmarks (annual, USD) ───
# Source: IBISWorld, industry reports
REVENUE_PER_TYPE = {
    'tunnel':       {'low': 800_000, 'high': 2_500_000, 'avg_ticket': 15, 'cars_per_day_low': 150, 'cars_per_day_high': 500},
    'full_service': {'low': 500_000, 'high': 1_500_000, 'avg_ticket': 25, 'cars_per_day_low': 60, 'cars_per_day_high': 200},
    'self_service': {'low': 100_000, 'high': 400_000,   'avg_ticket': 7,  'cars_per_day_low': 40, 'cars_per_day_high': 150},
    'flex_serve':   {'low': 600_000, 'high': 2_000_000, 'avg_ticket': 18, 'cars_per_day_low': 100, 'cars_per_day_high': 350},
    'unknown':      {'low': 300_000, 'high': 1_200_000, 'avg_ticket': 12, 'cars_per_day_low': 80, 'cars_per_day_high': 250},
}

# Valuation multiples (EV/Revenue)
VALUATION_MULTIPLE = {'low': 2.5, 'high': 5.0}

# Review count → traffic multiplier (more reviews = busier location)
# Rough heuristic: ~1-2% of customers leave reviews
REVIEW_TO_ANNUAL_CUSTOMERS = 150  # each review ≈ 150 annual customers


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def fetch_unenriched(session, offset=0):
    """Get car washes with google_place_id that haven't been enriched."""
    url = (f"{SUPABASE_URL}/rest/v1/car_washes"
           f"?google_place_id=not.is.null"
           f"&last_enriched_at=is.null"
           f"&select=wash_id,google_place_id,name,wash_type,google_rating,google_review_count,state,city,zipcode,county,phone,website,address,brand"
           f"&order=created_at.asc"
           f"&limit={BATCH_SIZE}&offset={offset}")
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    r = session.get(url, headers=headers, timeout=30)
    if r.status_code == 200:
        return r.json()
    log(f"  Fetch error: {r.status_code} {r.text[:200]}")
    return []


def get_place_details(place_id, session):
    """Get reviews, hours, photos from Google Places."""
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


def extract_reviews(details):
    """Pull review text, author, rating, time."""
    reviews = details.get('reviews', [])
    if not reviews:
        return None
    extracted = []
    for r in reviews[:5]:  # max 5 reviews from API
        extracted.append({
            'author': r.get('author_name', ''),
            'rating': r.get('rating'),
            'text': r.get('text', ''),
            'time': r.get('relative_time_description', ''),
        })
    return json.dumps(extracted)


def extract_hours(details):
    """Extract structured hours."""
    oh = details.get('opening_hours', {})
    if not oh:
        return None, None
    weekday = oh.get('weekday_text', [])
    is_open = oh.get('open_now')
    return json.dumps(weekday) if weekday else None, is_open


def extract_photos(details):
    """Get photo references (can be turned into URLs)."""
    photos = details.get('photos', [])
    if not photos:
        return None
    urls = []
    for p in photos[:5]:
        ref = p.get('photo_reference')
        if ref:
            urls.append(
                f"https://maps.googleapis.com/maps/api/place/photo"
                f"?maxwidth=800&photo_reference={ref}&key={GOOGLE_API_KEY}"
            )
    return urls if urls else None


def estimate_traffic(wash_type, review_count, rating):
    """Estimate monthly car count based on review volume and type."""
    benchmarks = REVENUE_PER_TYPE.get(wash_type, REVENUE_PER_TYPE['unknown'])

    if not review_count or review_count == 0:
        # No reviews — use low-end estimate
        monthly = benchmarks['cars_per_day_low'] * 30
        score = 0.3
    else:
        # More reviews = busier
        estimated_annual = review_count * REVIEW_TO_ANNUAL_CUSTOMERS
        monthly = int(estimated_annual / 12)

        # Clamp to reasonable range
        min_monthly = benchmarks['cars_per_day_low'] * 30
        max_monthly = benchmarks['cars_per_day_high'] * 30
        monthly = max(min_monthly, min(monthly, max_monthly))

        # Score: 0-1 based on where it falls in range
        score = (monthly - min_monthly) / max(max_monthly - min_monthly, 1)

    # Rating bonus/penalty
    if rating and rating >= 4.5:
        score = min(score * 1.15, 1.0)
    elif rating and rating < 3.0:
        score *= 0.7

    return monthly, round(score, 3)


def estimate_revenue(wash_type, monthly_cars):
    """Estimate annual revenue range."""
    benchmarks = REVENUE_PER_TYPE.get(wash_type, REVENUE_PER_TYPE['unknown'])
    avg_ticket = benchmarks['avg_ticket']

    annual_cars = monthly_cars * 12
    # Low estimate: 80% of avg ticket (discounts, memberships)
    rev_low = int(annual_cars * avg_ticket * 0.8)
    # High estimate: 120% of avg ticket (upsells, memberships premium)
    rev_high = int(annual_cars * avg_ticket * 1.2)

    # Clamp to industry benchmarks
    rev_low = max(rev_low, benchmarks['low'])
    rev_high = min(rev_high, benchmarks['high'])
    if rev_low > rev_high:
        rev_low, rev_high = rev_high, rev_low

    return rev_low, rev_high


def compute_acquisition_score(wash_type, rating, review_count, monthly_cars, is_chain):
    """
    Score 0-100 for acquisition attractiveness.
    Higher = more attractive target.
    """
    score = 50  # baseline

    # Revenue potential (review count as proxy)
    if review_count:
        if review_count > 500: score += 15
        elif review_count > 200: score += 10
        elif review_count > 50: score += 5
        elif review_count < 10: score -= 5

    # Rating
    if rating:
        if rating >= 4.5: score += 10
        elif rating >= 4.0: score += 5
        elif rating < 3.0: score -= 15
        elif rating < 3.5: score -= 5

    # Wash type preference (tunnels most valuable for M&A)
    type_bonus = {'tunnel': 10, 'flex_serve': 8, 'full_service': 3, 'self_service': -5, 'unknown': 0}
    score += type_bonus.get(wash_type, 0)

    # Independent > chain (chains are harder to acquire)
    if is_chain:
        score -= 20

    # Traffic volume
    if monthly_cars > 10000: score += 10
    elif monthly_cars > 5000: score += 5
    elif monthly_cars < 2000: score -= 5

    return max(0, min(100, score))


def acquisition_tier(score):
    if score >= 75: return 'A'
    if score >= 60: return 'B'
    if score >= 45: return 'C'
    return 'D'


def update_record(wash_id, updates, session):
    """Update a single car_wash record."""
    url = f"{SUPABASE_URL}/rest/v1/car_washes?wash_id=eq.{wash_id}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    try:
        r = session.patch(url, headers=headers, json=updates, timeout=15)
        return r.status_code in (200, 204)
    except:
        return False


def main():
    log("=" * 60)
    log("CAR WASH ENRICHMENT PIPELINE")
    log("=" * 60)

    session = requests.Session()
    total_enriched = 0
    api_calls = 0
    offset = 0

    while True:
        records = fetch_unenriched(session, offset=0)  # always offset 0 since we mark enriched
        if not records:
            log(f"  No more records to enrich. Total: {total_enriched}")
            break

        log(f"  Batch of {len(records)} records to enrich...")

        for rec in records:
            wash_id = rec['wash_id']
            place_id = rec.get('google_place_id')
            name = rec.get('name', '')
            wash_type = rec.get('wash_type', 'unknown')
            existing_rating = rec.get('google_rating')
            existing_reviews = rec.get('google_review_count', 0) or 0

            updates = {'last_enriched_at': datetime.utcnow().isoformat()}

            # If we have a google_place_id, pull fresh details
            if place_id:
                details = get_place_details(place_id, session)
                api_calls += 1

                if details:
                    # Fill missing address fields
                    addr = extract_address_parts(details)
                    if addr['zip'] and not rec.get('zipcode'):
                        updates['zipcode'] = addr['zip']
                    if addr['county'] and not rec.get('county'):
                        updates['county'] = addr['county']
                    if addr['city'] and not rec.get('city'):
                        updates['city'] = addr['city']
                    if addr['state'] and not rec.get('state'):
                        updates['state'] = addr['state']
                    if addr['street'] and not rec.get('address'):
                        updates['address'] = addr['street']

                    # Fill missing phone/website
                    phone = details.get('formatted_phone_number')
                    if phone and not rec.get('phone'):
                        updates['phone'] = phone
                    website = details.get('website')
                    if website and not rec.get('website'):
                        updates['website'] = website

                    # Reviews
                    reviews_json = extract_reviews(details)
                    if reviews_json:
                        updates['google_reviews_json'] = reviews_json

                    # Review sentiment summary
                    reviews = details.get('reviews', [])
                    if reviews:
                        avg_r = sum(r.get('rating', 0) for r in reviews) / len(reviews)
                        if avg_r >= 4.5:
                            updates['review_sentiment'] = 'very_positive'
                        elif avg_r >= 3.5:
                            updates['review_sentiment'] = 'positive'
                        elif avg_r >= 2.5:
                            updates['review_sentiment'] = 'mixed'
                        else:
                            updates['review_sentiment'] = 'negative'

                    # Hours
                    hours_json, is_open = extract_hours(details)
                    if hours_json:
                        updates['hours_json'] = hours_json
                    if is_open is not None:
                        updates['is_open'] = is_open

                    # Photos
                    photo_urls = extract_photos(details)
                    if photo_urls:
                        updates['google_photos_urls'] = photo_urls

                    # Update rating/review count if we got fresher data
                    rating = details.get('rating') or existing_rating
                    review_count = details.get('user_ratings_total') or existing_reviews
                    if rating:
                        updates['google_rating'] = rating
                    if review_count:
                        updates['google_review_count'] = review_count

                    # Business status
                    biz = details.get('business_status', '')
                    if biz == 'CLOSED_PERMANENTLY':
                        updates['is_open'] = False
                else:
                    rating = existing_rating
                    review_count = existing_reviews
            else:
                rating = existing_rating
                review_count = existing_reviews

            # Traffic estimates
            monthly_cars, traffic_score = estimate_traffic(wash_type, review_count, rating)
            updates['estimated_monthly_cars'] = monthly_cars
            updates['traffic_score'] = traffic_score

            # Revenue estimates
            rev_low, rev_high = estimate_revenue(wash_type, monthly_cars)
            updates['revenue_estimate_low'] = rev_low
            updates['revenue_estimate_high'] = rev_high

            # Valuation estimates
            updates['valuation_estimate_low'] = int(rev_low * VALUATION_MULTIPLE['low'])
            updates['valuation_estimate_high'] = int(rev_high * VALUATION_MULTIPLE['high'])

            # Acquisition scoring
            is_chain = rec.get('brand') is not None
            acq_score = compute_acquisition_score(wash_type, rating, review_count, monthly_cars, is_chain)
            updates['acquisition_score'] = acq_score
            updates['acquisition_tier'] = acquisition_tier(acq_score)

            # Write
            if update_record(wash_id, updates, session):
                total_enriched += 1
            else:
                log(f"    Failed to update {wash_id}")

            time.sleep(0.2)

        log(f"  Enriched: {total_enriched} | API calls: {api_calls}")

    log(f"\nDONE — {total_enriched:,} enriched, {api_calls:,} API calls")


if __name__ == "__main__":
    main()
