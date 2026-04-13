"""
Maryland Property Lookup via Socrata Open Data API
Searches MD Real Property Assessments for property ownership records.

Uses the LICENSED dataset 9xq5-z8s2 which includes actual property owner
names. Requires Socrata account credentials (HTTP Basic Auth) for access.

Environment variables needed:
  MD_OPENDATA_APP_TOKEN  — Socrata app token (optional, improves rate limits)
  MD_OPENDATA_USERNAME   — Socrata account email (required for licensed data)
  MD_OPENDATA_PASSWORD   — Socrata account password (required for licensed data)
"""

import os
import logging
import math
from datetime import datetime
import requests

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Configuration — Licensed dataset (active)
# ─────────────────────────────────────────────

DATASET_ID = "9xq5-z8s2"
OWNER_NAME_FIELD = "record_key_owner_s_name_mdp_field_ownname1_sdat_field_7"

SOCRATA_BASE = "https://opendata.maryland.gov/resource"
APP_TOKEN = os.environ.get("MD_OPENDATA_APP_TOKEN", "")
SOCRATA_USERNAME = os.environ.get("MD_OPENDATA_USERNAME", "")
SOCRATA_PASSWORD = os.environ.get("MD_OPENDATA_PASSWORD", "")

# Fields to retrieve from the API (verified against actual dataset schema)
SELECT_FIELDS = [
    OWNER_NAME_FIELD,
    "account_id_mdp_field_acctid",
    "record_key_county_code_sdat_field_1",
    "county_name_mdp_field_cntyname",
    "mdp_street_address_mdp_field_address",
    "premise_address_name_mdp_field_premsnam_sdat_field_23",
    "premise_address_city_mdp_field_premcity_sdat_field_25",
    "premise_address_zip_code_mdp_field_premzip_sdat_field_26",
    "land_use_code_mdp_field_lu_desclu_sdat_field_50",
    "current_cycle_data_land_value_mdp_field_names_nfmlndvl_curlndvl_and_sallndvl_sdat_field_164",
    "current_cycle_data_improvements_value_mdp_field_names_nfmimpvl_curimpvl_and_salimpvl_sdat_field_165",
    "current_cycle_data_preferential_land_value_sdat_field_166",
    "c_a_m_a_system_data_year_built_yyyy_mdp_field_yearblt_sdat_field_235",
    "c_a_m_a_system_data_structure_area_sq_ft_mdp_field_sqftstrc_sdat_field_241",
    "legal_description_line_1_mdp_field_legal1_sdat_field_17",
    "sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89",
    "sales_segment_1_consideration_mdp_field_considr1_sdat_field_90",
]

# County code to name mapping
COUNTY_NAMES = {
    "01": "Allegany", "02": "Anne Arundel", "03": "Baltimore City",
    "04": "Baltimore County", "05": "Calvert", "06": "Caroline",
    "07": "Carroll", "08": "Cecil", "09": "Charles", "10": "Dorchester",
    "11": "Frederick", "12": "Garrett", "13": "Harford", "14": "Howard",
    "15": "Kent", "16": "Montgomery", "17": "Prince George's",
    "18": "Queen Anne's", "19": "St. Mary's", "20": "Somerset",
    "21": "Talbot", "22": "Washington", "23": "Wicomico", "24": "Worcester",
}

# City-to-county code for targeted filtering
CITY_TO_COUNTY = {
    "baltimore": ["03", "04"], "towson": ["04"], "dundalk": ["04"],
    "essex": ["04"], "catonsville": ["04"], "pikesville": ["04"],
    "owings mills": ["04"], "perry hall": ["04"], "parkville": ["04"],
    "annapolis": ["02"], "glen burnie": ["02"], "severna park": ["02"],
    "pasadena": ["02"], "odenton": ["02"], "crofton": ["02"],
    "rockville": ["16"], "silver spring": ["16"], "bethesda": ["16"],
    "gaithersburg": ["16"], "germantown": ["16"], "potomac": ["16"],
    "college park": ["17"], "bowie": ["17"], "upper marlboro": ["17"],
    "hyattsville": ["17"], "greenbelt": ["17"], "largo": ["17"],
    "fort washington": ["17"], "clinton": ["17"],
    "columbia": ["14"], "ellicott city": ["14"], "elkridge": ["14"],
    "bel air": ["13"], "aberdeen": ["13"], "edgewood": ["13"],
    "havre de grace": ["13"],
    "frederick": ["11"], "brunswick": ["11"], "thurmont": ["11"],
    "westminster": ["07"], "eldersburg": ["07"], "sykesville": ["07"],
    "waldorf": ["09"], "la plata": ["09"], "indian head": ["09"],
    "prince frederick": ["05"], "lusby": ["05"],
    "leonardtown": ["19"], "lexington park": ["19"],
    "hagerstown": ["22"], "boonsboro": ["22"],
    "cumberland": ["01"], "frostburg": ["01"],
    "oakland": ["12"], "mchenry": ["12"],
    "elkton": ["08"], "north east": ["08"],
    "chestertown": ["15"], "easton": ["21"],
    "denton": ["06"], "cambridge": ["10"],
    "salisbury": ["23"], "ocean city": ["24"], "berlin": ["24"],
    "princess anne": ["20"], "crisfield": ["20"],
    "laurel": ["02", "14", "17"], "mount airy": ["11", "07"],
    "charlotte hall": ["09", "19"],
}


def search_property_by_name(last_name, first_name="", city=""):
    """
    Search MD property records by owner/grantor name via Socrata API.

    Args:
        last_name:  Owner's last name (required)
        first_name: Owner's first name (optional, improves accuracy)
        city:       City from obituary (optional, speeds up search)

    Returns:
        List of property record dicts matching the app's expected format
    """
    if not last_name or len(last_name) < 2:
        return []

    last_name = last_name.strip().upper()
    first_name = first_name.strip().upper() if first_name else ""

    # Build the SoQL WHERE clause
    where_parts = []

    # Name matching — search for LAST NAME in the owner/grantor field
    if first_name:
        # Try "LAST FIRST" pattern (most common in SDAT records)
        name_pattern = f"{last_name} {first_name}"
        where_parts.append(
            f"upper({OWNER_NAME_FIELD}) like '%{_escape(name_pattern)}%'"
        )
    else:
        where_parts.append(
            f"upper({OWNER_NAME_FIELD}) like '%{_escape(last_name)}%'"
        )

    where_clause = " AND ".join(where_parts)

    # Build the API URL — keep params minimal to avoid Cloudflare blocks
    # County filtering happens in post-processing via city param
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"

    params = {
        "$where": where_clause,
        "$limit": 25,
    }

    if APP_TOKEN:
        params["$$app_token"] = APP_TOKEN

    headers = {
        "Accept": "application/json",
        "User-Agent": "MD-Property-Leads/1.0",
    }

    # Basic Auth required for licensed/restricted datasets
    auth = None
    if SOCRATA_USERNAME and SOCRATA_PASSWORD:
        auth = (SOCRATA_USERNAME, SOCRATA_PASSWORD)

    try:
        logger.info(f"Socrata API search: {last_name} {first_name} (city={city})")
        resp = requests.get(url, params=params, headers=headers, auth=auth, timeout=30)

        if resp.status_code == 200:
            records = resp.json()
            if not records:
                return []

            properties = [_format_record(r) for r in records]
            properties = [p for p in properties if p is not None]

            # Post-filter: verify name match quality
            if first_name:
                properties = _filter_by_name(properties, last_name, first_name)

            # Post-filter: prioritize properties in matching county
            county_codes = _get_county_codes(city)
            if county_codes:
                local = [p for p in properties if p.get("county_code") in county_codes]
                if local:
                    properties = local

            logger.info(f"Found {len(properties)} properties for {first_name} {last_name}")
            return properties

        elif resp.status_code in (401, 403):
            logger.error(
                "Socrata API %s — check MD_OPENDATA_USERNAME/PASSWORD env vars "
                "(Basic Auth required for licensed dataset)", resp.status_code
            )
            return []
        else:
            logger.warning(f"Socrata API {resp.status_code}: {resp.text[:200]}")
            return []

    except requests.RequestException as e:
        logger.error(f"Socrata API request failed: {e}")
        return []
    except Exception as e:
        logger.error(f"Property lookup error: {e}")
        return []


def _search_statewide(last_name, first_name=""):
    """Fallback: search all counties without city filter."""
    where_parts = []

    if first_name:
        name_pattern = f"{last_name} {first_name}"
        where_parts.append(
            f"upper({OWNER_NAME_FIELD}) like '%{_escape(name_pattern)}%'"
        )
    else:
        where_parts.append(
            f"upper({OWNER_NAME_FIELD}) like '%{_escape(last_name)}%'"
        )

    where_clause = " AND ".join(where_parts)
    select = ", ".join(SELECT_FIELDS)
    url = f"{SOCRATA_BASE}/{DATASET_ID}.json"

    params = {
        "$where": where_clause,
        "$limit": 25,
    }

    if APP_TOKEN:
        params["$$app_token"] = APP_TOKEN

    headers = {
        "Accept": "application/json",
        "User-Agent": "MD-Property-Leads/1.0",
    }

    auth = None
    if SOCRATA_USERNAME and SOCRATA_PASSWORD:
        auth = (SOCRATA_USERNAME, SOCRATA_PASSWORD)

    try:
        resp = requests.get(url, params=params, headers=headers, auth=auth, timeout=30)
        if resp.status_code == 200:
            records = resp.json()
            properties = [_format_record(r) for r in records]
            properties = [p for p in properties if p is not None]
            if first_name:
                properties = _filter_by_name(properties, last_name, first_name)
            return properties
        return []
    except Exception as e:
        logger.error(f"Statewide search failed: {e}")
        return []


def _format_record(record):
    """Convert a Socrata API record to the app's expected property dict format."""
    try:
        county_code = record.get("record_key_county_code_sdat_field_1", "")
        county_name = record.get("county_name_mdp_field_cntyname", "")
        if not county_name:
            county_name = COUNTY_NAMES.get(county_code, "Unknown")

        # Build address
        address = record.get("mdp_street_address_mdp_field_address", "")
        if not address:
            address = record.get(
                "premise_address_name_mdp_field_premsnam_sdat_field_23", ""
            )

        land_value = record.get(
            "current_cycle_data_land_value_mdp_field_names_nfmlndvl_curlndvl_and_sallndvl_sdat_field_164", ""
        )
        improve_value = record.get(
            "current_cycle_data_improvements_value_mdp_field_names_nfmimpvl_curimpvl_and_salimpvl_sdat_field_165", ""
        )
        # Calculate total from land + improvements
        total_value = ""
        try:
            lv = float(land_value) if land_value else 0
            iv = float(improve_value) if improve_value else 0
            if lv or iv:
                total_value = str(int(lv + iv))
        except (ValueError, TypeError):
            pass

        prop = {
            "owner_name": record.get(OWNER_NAME_FIELD, ""),
            "property_address": address.strip() if address else "",
            "city": record.get(
                "premise_address_city_mdp_field_premcity_sdat_field_25", ""
            ),
            "county": county_name,
            "county_code": county_code,
            "state": "MD",
            "zip_code": record.get(
                "premise_address_zip_code_mdp_field_premzip_sdat_field_26", ""
            ),
            "property_type": record.get(
                "land_use_code_mdp_field_lu_desclu_sdat_field_50", ""
            ),
            "assessed_value": total_value,
            "land_value": str(land_value) if land_value else "",
            "improvement_value": str(improve_value) if improve_value else "",
            "lot_size": "",
            "year_built": record.get(
                "c_a_m_a_system_data_year_built_yyyy_mdp_field_yearblt_sdat_field_235", ""
            ),
            "account_number": record.get("account_id_mdp_field_acctid", ""),
            "legal_description": record.get(
                "legal_description_line_1_mdp_field_legal1_sdat_field_17", ""
            ),
            "square_footage": record.get(
                "c_a_m_a_system_data_structure_area_sq_ft_mdp_field_sqftstrc_sdat_field_241", ""
            ),
            "transfer_date": record.get(
                "sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89", ""
            ),
            "sale_price": record.get(
                "sales_segment_1_consideration_mdp_field_considr1_sdat_field_90", ""
            ),
        }

        # Calculate estimated equity
        equity = estimate_equity(prop)
        prop.update(equity)

        return prop

    except Exception as e:
        logger.debug(f"Error formatting record: {e}")
        return None


def _filter_by_name(properties, last_name, first_name):
    """
    Post-filter properties to ensure name match quality.
    The API LIKE search is broad — this tightens the match.
    """
    filtered = []
    last_upper = last_name.upper()
    first_upper = first_name.upper()

    for prop in properties:
        owner = prop.get("owner_name", "").upper()
        if not owner:
            continue

        # Must contain last name
        if last_upper not in owner:
            continue

        # Check first name (allow partial — "ROBERT" matches "ROBT")
        if first_upper:
            if first_upper in owner:
                filtered.append(prop)
            elif len(first_upper) >= 3 and first_upper[:3] in owner:
                filtered.append(prop)
        else:
            filtered.append(prop)

    return filtered


def _get_county_codes(city):
    """Get county codes for a city name. Returns None if unknown."""
    if not city:
        return None

    city_lower = city.lower().strip()

    if city_lower in CITY_TO_COUNTY:
        return CITY_TO_COUNTY[city_lower]

    for city_name, codes in CITY_TO_COUNTY.items():
        if city_name in city_lower or city_lower in city_name:
            return codes

    return None


def _escape(value):
    """Escape single quotes for SoQL string literals."""
    return value.replace("'", "''")


# ─────────────────────────────────────────────
#  Equity Estimation
# ─────────────────────────────────────────────

# Historical average 30-year fixed mortgage rates by era
_RATE_BY_ERA = [
    (2021, 0.055),   # 2021+: ~5.5%
    (2011, 0.040),   # 2011-2020: ~4.0%
    (2006, 0.055),   # 2006-2010: ~5.5%
    (2000, 0.060),   # 2000-2005: ~6.0%
    (1990, 0.075),   # 1990-1999: ~7.5%
    (0,    0.085),   # Pre-1990: ~8.5%
]


def _get_mortgage_rate(year):
    """Return approximate average mortgage rate for the origination year."""
    for cutoff, rate in _RATE_BY_ERA:
        if year >= cutoff:
            return rate
    return 0.085


def _remaining_mortgage_balance(original_amount, annual_rate, term_years, months_elapsed):
    """
    Calculate remaining mortgage balance using standard amortization.
    Returns 0 if the loan term has been exceeded.
    """
    if original_amount <= 0 or annual_rate <= 0 or term_years <= 0:
        return 0

    total_months = term_years * 12
    if months_elapsed >= total_months:
        return 0

    monthly_rate = annual_rate / 12
    # Monthly payment formula
    payment = original_amount * (monthly_rate * (1 + monthly_rate) ** total_months) / \
              ((1 + monthly_rate) ** total_months - 1)
    # Remaining balance formula
    remaining = original_amount * ((1 + monthly_rate) ** total_months -
                                    (1 + monthly_rate) ** months_elapsed) / \
                ((1 + monthly_rate) ** total_months - 1)
    return max(0, remaining)


def estimate_equity(prop):
    """
    Estimate equity for a property based on available SDAT data.

    Uses assessed value as market value proxy (MD targets 100% assessment).
    Estimates mortgage from last sale price + amortization assumptions.

    Returns dict with:
        estimated_market_value, estimated_mortgage_balance, known_liens,
        estimated_equity, equity_percent, equity_confidence

    IMPORTANT: These are estimates only. Actual mortgage data is not available
    from SDAT. Confidence reflects data quality, not prediction accuracy.
    """
    result = {
        "estimated_market_value": None,
        "estimated_mortgage_balance": None,
        "known_liens": None,
        "estimated_equity": None,
        "equity_percent": None,
        "equity_confidence": "unknown",
    }

    # ── Step 1: Estimate market value from assessed value ──
    assessed = 0
    try:
        assessed = float(prop.get("assessed_value") or 0)
    except (ValueError, TypeError):
        pass

    if assessed <= 0:
        # No assessed value = can't estimate anything
        return result

    # MD SDAT assesses at ~100% market value; small bump for assessment lag
    estimated_market = round(assessed * 1.05)
    result["estimated_market_value"] = estimated_market

    # ── Step 2: Estimate mortgage from last sale ──
    sale_price = 0
    try:
        sale_price = float(prop.get("sale_price") or 0)
    except (ValueError, TypeError):
        pass

    transfer_date_str = prop.get("transfer_date", "")
    transfer_year = None
    months_since_transfer = None

    if transfer_date_str:
        try:
            # SDAT format: YYYY-MM-DD or sometimes just YYYY
            if len(transfer_date_str) >= 10:
                td = datetime.strptime(transfer_date_str[:10], "%Y-%m-%d")
            elif len(transfer_date_str) >= 4:
                td = datetime(int(transfer_date_str[:4]), 6, 1)
            else:
                td = None

            if td:
                transfer_year = td.year
                now = datetime.now()
                months_since_transfer = (now.year - td.year) * 12 + (now.month - td.month)
        except (ValueError, TypeError):
            pass

    # Determine if sale_price is usable (filter out family transfers: $0, $1, $100)
    sale_is_usable = sale_price > 1000

    estimated_mortgage = 0
    confidence = "unknown"

    if sale_is_usable and months_since_transfer is not None and transfer_year:
        # Assume 80% LTV at purchase, 30-year fixed
        original_mortgage = sale_price * 0.80
        rate = _get_mortgage_rate(transfer_year)
        estimated_mortgage = round(_remaining_mortgage_balance(
            original_mortgage, rate, 30, months_since_transfer
        ))
        result["estimated_mortgage_balance"] = estimated_mortgage

        # Confidence based on data recency
        if months_since_transfer <= 120:  # Within 10 years
            confidence = "medium"
        elif months_since_transfer <= 240:  # 10-20 years
            confidence = "medium"
        else:  # 20+ years — likely paid off or refinanced
            confidence = "low"

        # If sale was recent and price close to assessed, higher confidence
        if months_since_transfer <= 60 and sale_price > assessed * 0.5:
            confidence = "high"

    elif not sale_is_usable and months_since_transfer is not None:
        # Family transfer ($0/$1/$100) — likely no mortgage, but uncertain
        estimated_mortgage = 0
        result["estimated_mortgage_balance"] = 0
        confidence = "low"
    else:
        # No sale data at all — can't estimate mortgage
        # Assume property could be free & clear (common in probate)
        estimated_mortgage = 0
        result["estimated_mortgage_balance"] = None
        confidence = "low"

    # ── Step 3: Calculate equity ──
    known_liens = 0  # No lien data from SDAT
    result["known_liens"] = 0

    if result["estimated_mortgage_balance"] is not None:
        equity = estimated_market - estimated_mortgage - known_liens
        result["estimated_equity"] = round(equity)
        if estimated_market > 0:
            result["equity_percent"] = round((equity / estimated_market) * 100, 1)
    else:
        # Can still provide a ceiling estimate (full assessed value as max equity)
        result["estimated_equity"] = estimated_market
        result["equity_percent"] = 100.0
        confidence = "low"

    result["equity_confidence"] = confidence
    return result
