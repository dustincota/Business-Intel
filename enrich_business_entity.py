#!/usr/bin/env python3
"""
Business entity enrichment for car_washes.
Uses OpenCorporates API (free, no key for basic search) to pull:
- Owner / registered agent
- Incorporation date
- Entity type (LLC, Corp, etc.)
- Business status (active, dissolved)
- Officers/directors when available

Rate limited: ~1 req/sec for free tier.
"""

import requests, json, time, os, re
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
LOG_FILE = os.path.join(SCRIPT_DIR, "business_entity.log")
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "business_entity_progress.json")

BATCH_SIZE = 50
DELAY_BETWEEN_API = 1.2  # be polite to OpenCorporates free tier

# State code to OpenCorporates jurisdiction code
STATE_TO_JURISDICTION = {
    'AL': 'us_al', 'AK': 'us_ak', 'AZ': 'us_az', 'AR': 'us_ar', 'CA': 'us_ca',
    'CO': 'us_co', 'CT': 'us_ct', 'DE': 'us_de', 'FL': 'us_fl', 'GA': 'us_ga',
    'HI': 'us_hi', 'ID': 'us_id', 'IL': 'us_il', 'IN': 'us_in', 'IA': 'us_ia',
    'KS': 'us_ks', 'KY': 'us_ky', 'LA': 'us_la', 'ME': 'us_me', 'MD': 'us_md',
    'MA': 'us_ma', 'MI': 'us_mi', 'MN': 'us_mn', 'MS': 'us_ms', 'MO': 'us_mo',
    'MT': 'us_mt', 'NE': 'us_ne', 'NV': 'us_nv', 'NH': 'us_nh', 'NJ': 'us_nj',
    'NM': 'us_nm', 'NY': 'us_ny', 'NC': 'us_nc', 'ND': 'us_nd', 'OH': 'us_oh',
    'OK': 'us_ok', 'OR': 'us_or', 'PA': 'us_pa', 'RI': 'us_ri', 'SC': 'us_sc',
    'SD': 'us_sd', 'TN': 'us_tn', 'TX': 'us_tx', 'UT': 'us_ut', 'VT': 'us_vt',
    'VA': 'us_va', 'WA': 'us_wa', 'WV': 'us_wv', 'WI': 'us_wi', 'WY': 'us_wy',
    'DC': 'us_dc',
}

# Known chains to skip (they're corporate, not independent targets)
SKIP_CHAINS = {
    'Mister Car Wash', 'Take 5 Car Wash', "Tommy's Express", 'Tidal Wave Auto Spa',
    'Zips Car Wash', 'Whistle Express', 'Quick Quack Car Wash', 'Delta Sonic',
    'Super Star Car Wash', 'Autobell Car Wash',
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
    return {"total_enriched": 0, "total_skipped": 0, "total_not_found": 0, "api_calls": 0}


def save_progress(p):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)


def clean_name_for_search(name):
    """Clean car wash name for business registry search."""
    # Remove common suffixes that aren't in business names
    name = re.sub(r'\s*[-–]\s*(Metairie|David Dr|Main St|Hwy|Ave|Blvd|Rd|Drive|Street|Road|Boulevard|Avenue).*$', '', name, flags=re.IGNORECASE)
    # Remove location qualifiers
    name = re.sub(r'\s*[-–#]\s*\d+\s*$', '', name)
    # Remove "Car Wash" if it appears with another name (helps matching)
    # But keep it if the business IS just "Car Wash"
    cleaned = name.strip()
    return cleaned if cleaned else name


def search_opencorporates(name, state, session):
    """Search OpenCorporates for a business entity."""
    jurisdiction = STATE_TO_JURISDICTION.get(state)
    search_name = clean_name_for_search(name)

    url = "https://api.opencorporates.com/v0.4/companies/search"
    params = {
        'q': search_name,
        'order': 'score',
    }
    if jurisdiction:
        params['jurisdiction_code'] = jurisdiction

    time.sleep(DELAY_BETWEEN_API)
    try:
        r = session.get(url, params=params, timeout=20)
        if r.status_code == 429:
            log("    Rate limited by OpenCorporates, waiting 60s...")
            time.sleep(60)
            r = session.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return None
        data = r.json()
        companies = data.get('results', {}).get('companies', [])
        if not companies:
            return None

        # Find best match
        best = None
        best_score = 0
        for c in companies[:5]:
            company = c.get('company', {})
            co_name = (company.get('name') or '').lower()
            search_lower = search_name.lower()

            # Score the match
            score = 0
            if search_lower in co_name or co_name in search_lower:
                score = 80
            elif any(w in co_name for w in search_lower.split() if len(w) > 3):
                score = 50

            # Bonus for active companies
            if company.get('current_status', '').lower() in ('active', 'good standing', 'in existence'):
                score += 10

            # Bonus for car wash related
            if 'car wash' in co_name or 'carwash' in co_name or 'auto' in co_name:
                score += 15

            if score > best_score:
                best_score = score
                best = company

        if best and best_score >= 50:
            return best
        return None

    except Exception as e:
        log(f"    OpenCorporates error: {e}")
        return None


def get_company_details(url, session):
    """Get detailed company info including officers."""
    time.sleep(DELAY_BETWEEN_API)
    try:
        api_url = url.replace('https://opencorporates.com', 'https://api.opencorporates.com/v0.4')
        r = session.get(api_url, timeout=20)
        if r.status_code == 200:
            return r.json().get('results', {}).get('company', {})
    except:
        pass
    return None


def extract_entity_data(company, detail=None):
    """Extract useful fields from OpenCorporates data."""
    result = {}

    # Entity type
    co_type = company.get('company_type', '')
    if co_type:
        result['business_entity_type'] = co_type

    # Incorporation date
    inc_date = company.get('incorporation_date')
    if inc_date:
        result['incorporation_date'] = inc_date
        # Calculate years in business
        try:
            inc = datetime.strptime(inc_date, '%Y-%m-%d')
            years = (datetime.now() - inc).days // 365
            result['years_in_business'] = years
        except:
            pass

    # Incorporation state
    jurisdiction = company.get('jurisdiction_code', '')
    if jurisdiction and jurisdiction.startswith('us_'):
        result['incorporation_state'] = jurisdiction.replace('us_', '').upper()

    # Registered agent
    agent = company.get('registered_address_in_full') or company.get('registered_agent', {})
    if isinstance(agent, dict):
        agent_name = agent.get('name', '')
        if agent_name:
            result['registered_agent'] = agent_name
    elif isinstance(agent, str) and agent:
        result['registered_agent'] = agent[:200]

    # Status
    status = company.get('current_status', '')
    if status:
        result['enrichment_notes'] = json.dumps({
            'opencorporates_status': status,
            'opencorporates_url': company.get('opencorporates_url', ''),
            'company_number': company.get('company_number', ''),
        })

    # Officers (from detailed lookup)
    if detail:
        officers = detail.get('officers', [])
        if officers:
            names = []
            for o in officers[:5]:
                officer = o.get('officer', {})
                oname = officer.get('name', '')
                position = officer.get('position', '')
                if oname:
                    names.append(f"{oname} ({position})" if position else oname)
            if names:
                result['owner_names'] = names
                # Use first officer as primary owner
                result['owner_name'] = names[0].split(' (')[0]

    return result


def fetch_unenriched(session):
    """Get car washes without entity data — prioritize independents."""
    url = (f"{SUPABASE_URL}/rest/v1/car_washes"
           f"?incorporation_date=is.null"
           f"&business_entity_type=is.null"
           f"&state=not.is.null"
           f"&name=not.is.null"
           f"&select=wash_id,name,state,city,brand"
           f"&order=google_review_count.desc.nullslast"
           f"&limit={BATCH_SIZE}")
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    r = session.get(url, headers=headers, timeout=30)
    if r.status_code == 200:
        return r.json()
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
    log("BUSINESS ENTITY ENRICHMENT (OpenCorporates)")
    log("=" * 60)

    session = requests.Session()
    progress = load_progress()
    total_enriched = progress['total_enriched']
    total_skipped = progress['total_skipped']
    total_not_found = progress['total_not_found']
    api_calls = progress['api_calls']

    while True:
        records = fetch_unenriched(session)
        if not records:
            log("  No more records to enrich.")
            break

        log(f"  Batch of {len(records)}...")

        for rec in records:
            wash_id = rec['wash_id']
            name = rec.get('name', '')
            state = rec.get('state', '')
            brand = rec.get('brand')

            # Skip known chains — they're corporate, not independent targets
            if brand in SKIP_CHAINS:
                update_record(wash_id, {'business_entity_type': 'chain_skip'}, session)
                total_skipped += 1
                continue

            # Skip generic names that won't match well
            if not name or len(name) < 3:
                update_record(wash_id, {'business_entity_type': 'no_name'}, session)
                total_skipped += 1
                continue

            # Search OpenCorporates
            company = search_opencorporates(name, state, session)
            api_calls += 1

            if company:
                # Get detailed info (officers)
                oc_url = company.get('opencorporates_url', '')
                detail = None
                if oc_url:
                    detail = get_company_details(oc_url, session)
                    api_calls += 1

                entity_data = extract_entity_data(company, detail)
                if entity_data:
                    update_record(wash_id, entity_data, session)
                    total_enriched += 1
                    if total_enriched % 10 == 0:
                        log(f"    Enriched: {name} ({state}) → {entity_data.get('business_entity_type', '?')} "
                            f"est. {entity_data.get('incorporation_date', '?')}")
                else:
                    update_record(wash_id, {'business_entity_type': 'found_no_data'}, session)
                    total_not_found += 1
            else:
                update_record(wash_id, {'business_entity_type': 'not_found'}, session)
                total_not_found += 1

            # Save progress periodically
            if (total_enriched + total_not_found + total_skipped) % 25 == 0:
                progress.update(total_enriched=total_enriched, total_skipped=total_skipped,
                               total_not_found=total_not_found, api_calls=api_calls)
                save_progress(progress)
                log(f"  Progress: {total_enriched} enriched | {total_not_found} not found | "
                    f"{total_skipped} skipped | {api_calls} API calls")

    progress.update(total_enriched=total_enriched, total_skipped=total_skipped,
                   total_not_found=total_not_found, api_calls=api_calls)
    save_progress(progress)
    log(f"\nDONE — {total_enriched:,} enriched, {total_not_found:,} not found, "
        f"{total_skipped:,} skipped, {api_calls:,} API calls")


if __name__ == "__main__":
    main()
