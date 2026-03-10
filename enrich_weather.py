#!/usr/bin/env python3
"""
Weather enrichment for car_washes.
Uses Open-Meteo API (free, no key) to pull climate normals by lat/lon.

Adds:
- Annual rainfall & snowfall
- Rain/snow days per year
- Summer/winter avg temps
- Salt belt flag (>20 snow days + freezing winters)
- Weather demand score (rain + salt = good for car wash biz)
"""

import requests, json, time, os
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


SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, "weather.log")

BATCH_SIZE = 50
DELAY_BETWEEN_API = 2.5  # Open-Meteo free tier — 2.5s to avoid 429s

# Cache weather by rounded lat/lon (0.25° grid ≈ 25km) to avoid duplicate API calls
weather_cache = {}


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def cache_key(lat, lon):
    """Round to 0.25° grid for caching — car washes within ~25km share weather."""
    return (round(lat * 4) / 4, round(lon * 4) / 4)


def fetch_weather(lat, lon, session):
    """Get climate normals from Open-Meteo climate API."""
    ck = cache_key(lat, lon)
    if ck in weather_cache:
        return weather_cache[ck]

    # Use Open-Meteo historical weather API with 2019-2024 range for averages
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': ck[0],
        'longitude': ck[1],
        'start_date': '2019-01-01',
        'end_date': '2024-12-31',
        'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum',
        'temperature_unit': 'fahrenheit',
        'precipitation_unit': 'inch',
        'timezone': 'America/Chicago',
    }

    time.sleep(DELAY_BETWEEN_API)
    for attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = 30 * (attempt + 1)
                log(f"    Weather rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log(f"    Weather API error: {r.status_code}")
                return None
            data = r.json()
            break
        except Exception as e:
            log(f"    Weather API exception: {e}")
            if attempt < 2:
                time.sleep(10)
                continue
            return None
    else:
        return None

    daily = data.get('daily', {})
    temps_max = daily.get('temperature_2m_max', [])
    temps_min = daily.get('temperature_2m_min', [])
    precip = daily.get('precipitation_sum', [])
    snow = daily.get('snowfall_sum', [])

    if not temps_max:
        return None

    # Compute averages over the date range
    years = 6  # 2019-2024

    # Annual rainfall (total precip minus snow equivalent)
    total_precip = sum(p for p in precip if p is not None)
    avg_annual_precip = total_precip / years

    # Annual snowfall
    total_snow = sum(s for s in snow if s is not None)
    avg_annual_snow = total_snow / years

    # Rain days (>0.01 inch precip)
    rain_days = sum(1 for p in precip if p is not None and p > 0.01)
    avg_rain_days = int(rain_days / years)

    # Snow days (>0.1 inch snow)
    snow_days = sum(1 for s in snow if s is not None and s > 0.1)
    avg_snow_days = int(snow_days / years)

    # Summer temps (Jun-Aug) and Winter temps (Dec-Feb)
    # Rough approach: divide by months
    summer_temps = []
    winter_temps = []
    dates = daily.get('time', [])
    for i, d in enumerate(dates):
        if i < len(temps_max) and temps_max[i] is not None and i < len(temps_min) and temps_min[i] is not None:
            month = int(d.split('-')[1])
            avg_day = (temps_max[i] + temps_min[i]) / 2
            if month in (6, 7, 8):
                summer_temps.append(avg_day)
            elif month in (12, 1, 2):
                winter_temps.append(avg_day)

    avg_summer = round(sum(summer_temps) / len(summer_temps), 1) if summer_temps else None
    avg_winter = round(sum(winter_temps) / len(winter_temps), 1) if winter_temps else None

    # Salt belt: significant snow + cold winters
    is_salt_belt = avg_snow_days > 15 and (avg_winter is not None and avg_winter < 35)

    # Weather demand score (0-100)
    # High rain = good (dirty cars), high snow = good (salt), moderate = best
    score = 50
    # Rain bonus
    if avg_rain_days > 120:
        score += 15
    elif avg_rain_days > 80:
        score += 10
    elif avg_rain_days > 50:
        score += 5
    elif avg_rain_days < 30:
        score -= 10  # very dry = less demand

    # Snow/salt bonus
    if is_salt_belt:
        score += 15
    elif avg_snow_days > 10:
        score += 8

    # Extreme cold penalty (people don't go out)
    if avg_winter is not None and avg_winter < 15:
        score -= 10
    # Hot summer bonus (pollen, dust)
    if avg_summer is not None and avg_summer > 85:
        score += 5

    score = max(0, min(100, score))

    result = {
        'avg_annual_rainfall_in': round(avg_annual_precip, 1),
        'avg_annual_snowfall_in': round(avg_annual_snow, 1),
        'avg_rain_days_per_year': avg_rain_days,
        'avg_snow_days_per_year': avg_snow_days,
        'avg_temp_summer_f': avg_summer,
        'avg_temp_winter_f': avg_winter,
        'is_salt_belt': is_salt_belt,
        'weather_demand_score': score,
    }

    weather_cache[ck] = result
    return result


def fetch_unenriched(session, offset=0):
    """Get car washes without weather data."""
    url = (f"{SUPABASE_URL}/rest/v1/car_washes"
           f"?weather_demand_score=is.null"
           f"&latitude=not.is.null"
           f"&longitude=not.is.null"
           f"&select=wash_id,latitude,longitude,state,city,name"
           f"&order=created_at.asc"
           f"&limit={BATCH_SIZE}&offset={offset}")
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    r = session.get(url, headers=headers, timeout=30)
    if r.status_code == 200:
        return r.json()
    log(f"  Fetch error: {r.status_code}")
    return []


def update_record(wash_id, updates, session):
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
    log("WEATHER ENRICHMENT")
    log("=" * 60)

    session = requests.Session()
    total = 0
    api_calls = 0
    cache_hits = 0

    while True:
        records = fetch_unenriched(session, offset=0)
        if not records:
            log(f"  Done. Total: {total}, API calls: {api_calls}, cache hits: {cache_hits}")
            break

        log(f"  Batch of {len(records)}...")

        for rec in records:
            lat = rec.get('latitude')
            lon = rec.get('longitude')

            if not lat or not lon:
                continue

            ck = cache_key(lat, lon)
            was_cached = ck in weather_cache

            weather = fetch_weather(lat, lon, session)
            if not was_cached:
                api_calls += 1
            else:
                cache_hits += 1

            if weather:
                if update_record(rec['wash_id'], weather, session):
                    total += 1
                time.sleep(0.1)
            else:
                # Mark as processed even if no data (set score to -1)
                update_record(rec['wash_id'], {'weather_demand_score': -1}, session)

        log(f"  Progress: {total} enriched | {api_calls} API calls | {cache_hits} cache hits | {len(weather_cache)} cached locations")

    log(f"\nDONE — {total:,} enriched, {api_calls:,} API calls, {cache_hits:,} cache hits")


if __name__ == "__main__":
    main()
