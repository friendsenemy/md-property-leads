"""
Maryland Property Lookup via Socrata Open Data API
Searches MD Real Property Assessments for property ownership records.

TWO MODES:
  1. STOPGAP (current): Uses public dataset ed4q-f8tm, searches grantor
     (seller) name field. Catches properties the deceased transferred.
  2. FULL (after license): Uses dataset 9xq5-z8s2 with owner names.
     Just change DATASET_ID and OWNER_NAME_FIELD below.

To switch to full mode once licensed access is granted:
  DATASET_ID = "9xq5-z8s2"
  OWNER_NAME_FIELD = "owner_name"  # confirm exact field name after access
"""

import os
import logging
import requests

logger = logging.getLogger(__name__)

# âââââââââââââââââââââââââââââââââââââââââââââ
#  Configuration â CHANGE THESE WHEN LICENSED
# âââââââââââââââââââââââââââââââââââââââââââââ

# Public dataset (grantor names only, no current owner)
DATASET_ID = "ed4q-f8tm"
OWNER_NAME_FIELD = "sales_segment_1_grantor_name_mdp_field_grntnam1_sdat_field_80"

# Licensed dataset (uncomment when access granted):
# DATASET_ID = "9xq5-z8s2"
# OWNER_NAME_FIELD = "owner_name"  # verify exact field name after access

SOCRATA_BASE = "https://opendata.maryland.gov/resource"
APP_TOKEN = os.environ.get("MD_OPENDATA_APP_TOKEN", "")

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

    # Name matching â search for LAST NAME in the owner/grantor field
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

    # Build the API URL â keep params minimal to avoid Cloudflare blocks
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

    try:
        logger.info(f"Socrata API search: {last_name} {first_name} (city={city})")
        resp = requests.get(url, params=params, headers=headers, timeout=30)

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

        elif resp.status_code == 403:
            logger.error("Socrata API 403 â check APP_TOKEN or dataset access")
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

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
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

        return {
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

    except Exception as e:
        logger.debug(f"Error formatting record: {e}")
        return None


def _filter_by_name(properties, last_name, first_name):
    """
    Post-filter properties to ensure name match quality.
    The API LIKE search is broad â this tightens the match.
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

        # Check first name (allow partial â "ROBERT" matches "ROBT")
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
