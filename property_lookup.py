"""
Maryland SDAT Property Lookup
Searches the Maryland State Department of Assessments and Taxation
Real Property database for property ownership records.
"""

import re
import time
import random
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SDAT_BASE = "https://sdat.dat.maryland.gov/RealProperty"
SDAT_SEARCH_URL = f"{SDAT_BASE}/Pages/default.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Maryland county codes for SDAT
MD_COUNTIES = {
    "01": "Allegany",
    "02": "Anne Arundel",
    "03": "Baltimore City",
    "04": "Baltimore County",
    "05": "Calvert",
    "06": "Caroline",
    "07": "Carroll",
    "08": "Cecil",
    "09": "Charles",
    "10": "Dorchester",
    "11": "Frederick",
    "12": "Garrett",
    "13": "Harford",
    "14": "Howard",
    "15": "Kent",
    "16": "Montgomery",
    "17": "Prince George's",
    "18": "Queen Anne's",
    "19": "St. Mary's",
    "20": "Somerset",
    "21": "Talbot",
    "22": "Washington",
    "23": "Wicomico",
    "24": "Worcester",
}


def search_property_by_name(last_name, first_name="", counties=None):
    """
    Search MD SDAT for property records by owner name.
    Returns list of property dicts.

    Args:
        last_name: Owner's last name (required)
        first_name: Owner's first name (optional, improves accuracy)
        counties: List of county codes to search. Defaults to all.

    Returns:
        List of property record dicts
    """
    if not last_name or len(last_name) < 2:
        return []

    properties = []
    search_counties = counties or list(MD_COUNTIES.keys())

    session = requests.Session()
    session.headers.update(HEADERS)

    for county_code in search_counties:
        try:
            results = _search_county(session, county_code, last_name, first_name)
            properties.extend(results)
            time.sleep(random.uniform(0.8, 1.5))
        except Exception as e:
            logger.error(f"Error searching county {county_code}: {e}")
            continue

    logger.info(
        f"Found {len(properties)} properties for {first_name} {last_name}"
    )
    return properties


def _search_county(session, county_code, last_name, first_name):
    """Search a single county for property records."""
    properties = []

    try:
        # Step 1: Get the search page to obtain form tokens (ASP.NET ViewState)
        resp = session.get(SDAT_SEARCH_URL, timeout=15)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract ASP.NET form fields
        viewstate = _get_field(soup, "__VIEWSTATE")
        viewstate_gen = _get_field(soup, "__VIEWSTATEGENERATOR")
        event_validation = _get_field(soup, "__EVENTVALIDATION")

        if not viewstate:
            logger.warning("Could not extract SDAT form tokens")
            return []

        # Step 2: Submit the owner name search
        form_data = {
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstate_gen,
            "__EVENTVALIDATION": event_validation,
            "MainContent$MainContent$cphMainContentArea$ucSearchType$"
            "wzrdRealPropertySearch$ucSearchType$ddlCounty": county_code,
            "MainContent$MainContent$cphMainContentArea$ucSearchType$"
            "wzrdRealPropertySearch$ucSearchType$ddlSearchType": "02",  # Owner Name
            "MainContent$MainContent$cphMainContentArea$ucSearchType$"
            "wzrdRealPropertySearch$ucSearchType$txtStNameOwnerName": last_name,
            "MainContent$MainContent$cphMainContentArea$ucSearchType$"
            "wzrdRealPropertySearch$ucSearchType$btnSearch": "Search",
        }

        resp = session.post(SDAT_SEARCH_URL, data=form_data, timeout=15)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Step 3: Parse results
        results_table = soup.select("table tr")
        for row in results_table[1:]:  # Skip header
            cells = row.select("td")
            if len(cells) >= 4:
                prop = _parse_property_row(cells, county_code, first_name, last_name)
                if prop:
                    properties.append(prop)

        # Also try parsing the detail page if we got a direct result
        detail = _parse_detail_page(soup, county_code)
        if detail:
            owner = detail.get("owner_name", "").lower()
            if last_name.lower() in owner:
                if not first_name or first_name.lower() in owner:
                    properties.append(detail)

    except requests.RequestException as e:
        logger.error(f"Request error for county {county_code}: {e}")
    except Exception as e:
        logger.error(f"Parse error for county {county_code}: {e}")

    return properties


def _parse_property_row(cells, county_code, first_name, last_name):
    """Parse a property result table row."""
    try:
        owner_name = cells[0].get_text(strip=True) if len(cells) > 0 else ""

        # Filter: owner name must match
        if last_name.lower() not in owner_name.lower():
            return None
        if first_name and first_name.lower() not in owner_name.lower():
            return None

        prop = {
            "owner_name": owner_name,
            "property_address": cells[1].get_text(strip=True) if len(cells) > 1 else "",
            "city": cells[2].get_text(strip=True) if len(cells) > 2 else "",
            "county": MD_COUNTIES.get(county_code, "Unknown"),
            "county_code": county_code,
            "state": "MD",
            "zip_code": cells[3].get_text(strip=True) if len(cells) > 3 else "",
            "property_type": cells[4].get_text(strip=True) if len(cells) > 4 else "",
            "assessed_value": "",
            "land_value": "",
            "improvement_value": "",
            "lot_size": "",
            "year_built": "",
            "account_number": "",
            "legal_description": "",
        }

        # Try to get detail link
        link = cells[0].select_one("a")
        if link and link.get("href"):
            prop["detail_url"] = link["href"]

        return prop

    except Exception as e:
        logger.debug(f"Error parsing property row: {e}")
        return None


def _parse_detail_page(soup, county_code):
    """Parse a property detail page for comprehensive info."""
    try:
        # Look for common SDAT detail page elements
        owner_el = soup.select_one("[id*='lblOwnerName'], [id*='Owner']")
        addr_el = soup.select_one("[id*='lblPremisesAddress'], [id*='Address']")
        value_el = soup.select_one("[id*='lblTotalAssessment'], [id*='TotalValue']")
        land_el = soup.select_one("[id*='lblLandValue'], [id*='LandValue']")
        improve_el = soup.select_one("[id*='lblImproveValue'], [id*='ImproveValue']")
        type_el = soup.select_one("[id*='lblUse'], [id*='PropertyUse']")
        year_el = soup.select_one("[id*='lblYearBuilt'], [id*='YearBuilt']")
        acct_el = soup.select_one("[id*='lblAccountNumber'], [id*='AcctNum']")
        lot_el = soup.select_one("[id*='lblLotSize'], [id*='LotSize']")
        legal_el = soup.select_one("[id*='lblLegalDescription']")

        if not owner_el:
            return None

        prop = {
            "owner_name": owner_el.get_text(strip=True) if owner_el else "",
            "property_address": addr_el.get_text(strip=True) if addr_el else "",
            "county": MD_COUNTIES.get(county_code, "Unknown"),
            "county_code": county_code,
            "city": "",
            "state": "MD",
            "zip_code": "",
            "assessed_value": _clean_currency(value_el.get_text(strip=True)) if value_el else "",
            "land_value": _clean_currency(land_el.get_text(strip=True)) if land_el else "",
            "improvement_value": _clean_currency(improve_el.get_text(strip=True)) if improve_el else "",
            "property_type": type_el.get_text(strip=True) if type_el else "",
            "year_built": year_el.get_text(strip=True) if year_el else "",
            "lot_size": lot_el.get_text(strip=True) if lot_el else "",
            "account_number": acct_el.get_text(strip=True) if acct_el else "",
            "legal_description": legal_el.get_text(strip=True) if legal_el else "",
        }

        # Extract city/zip from address
        addr = prop["property_address"]
        zip_match = re.search(r"(\d{5})(?:-\d{4})?", addr)
        if zip_match:
            prop["zip_code"] = zip_match.group(1)

        return prop if prop["owner_name"] else None

    except Exception as e:
        logger.debug(f"Error parsing detail page: {e}")
        return None


def _get_field(soup, field_name):
    """Extract a hidden form field value."""
    el = soup.find("input", {"name": field_name})
    return el.get("value", "") if el else ""


def _clean_currency(value):
    """Clean currency string to plain number string."""
    if not value:
        return ""
    cleaned = re.sub(r"[^\d.]", "", value)
    return cleaned
"""
Maryland SDAT Property Lookup
Searches the Maryland State Department of Assessments and Taxation
Real Property database for property ownership records.
"""

import re
import time
import random
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SDAT_BASE = "https://sdat.dat.maryland.gov/RealProperty"
SDAT_SEARCH_URL = f"{SDAT_BASE}/Pages/default.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Maryland county codes for SDAT
MD_COUNTIES = {
    "01": "Allegany",
    "02": "Anne Arundel",
    "03": "Baltimore City",
    "04": "Baltimore County",
    "05": "Calvert",
    "06": "Caroline",
    "07": "Carroll",
    "08": "Cecil",
    "09": "Charles",
    "10": "Dorchester",
    "11": "Frederick",
    "12": "Garrett",
    "13": "Harford",
    "14": "Howard",
    "15": "Kent",
    "16": "Montgomery",
    "17": "Prince George's",
    "18": "Queen Anne's",
    "19": "St. Mary's",
    "20": "Somerset",
    "21": "Talbot",
    "22": "Washington",
    "23": "Wicomico",
    "24": "Worcester",
}


def search_property_by_name(last_name, first_name="", counties=None):
    """
    Search MD SDAT for property records by owner name.
    Returns list of property dicts.

    Args:
        last_name: Owner's last name (required)
        first_name: Owner's first name (optional, improves accuracy)
        counties: List of county codes to search. Defaults to all.

    Returns:
        List of property record dicts
    """
    if not last_name or len(last_name) < 2:
        return []

    properties = []
    search_counties = counties or list(MD_COUNTIES.keys())

    session = requests.Session()
    session.headers.update(HEADERS)

    for county_code in search_counties:
        try:
            results = _search_county(session, county_code, last_name, first_name)
            properties.extend(results)
            time.sleep(random.uniform(0.8, 1.5))
        except Exception as e:
            logger.error(f"Error searching county {county_code}: {e}")
            continue

    logger.info(
        f"Found {len(properties)} properties for {first_name} {last_name}"
    )
    return properties


def _search_county(session, county_code, last_name, first_name):
    """Search a single county for property records."""
    properties = []

    try:
        # Step 1: Get the search page to obtain form tokens (ASP.NET ViewState)
        resp = session.get(SDAT_SEARCH_URL, timeout=15)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "lxml")

        # Extract ASP.NET form fields
        viewstate = _get_field(soup, "__VIEWSTATE")
        viewstate_gen = _get_field(soup, "__VIEWSTATEGENERATOR")
        event_validation = _get_field(soup, "__EVENTVALIDATION")

        if not viewstate:
            logger.warning("Could not extract SDAT form tokens")
            return []

        # Step 2: Submit the owner name search
        form_data = {
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstate_gen,
            "__EVENTVALIDATION": event_validation,
            "MainContent$MainContent$cphMainContentArea$ucSearchType$"
            "wzrdRealPropertySearch$ucSearchType$ddlCounty": county_code,
            "MainContent$MainContent$cphMainContentArea$ucSearchType$"
            "wzrdRealPropertySearch$ucSearchType$ddlSearchType": "02",  # Owner Name
            "MainContent$MainContent$cphMainContentArea$ucSearchType$"
            "wzrdRealPropertySearch$ucSearchType$txtStNameOwnerName": last_name,
            "MainContent$MainContent$cphMainContentArea$ucSearchType$"
            "wzrdRealPropertySearch$ucSearchType$btnSearch": "Search",
        }

        resp = session.post(SDAT_SEARCH_URL, data=form_data, timeout=15)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "lxml")

        # Step 3: Parse results
        results_table = soup.select("table tr")
        for row in results_table[1:]:  # Skip header
            cells = row.select("td")
            if len(cells) >= 4:
                prop = _parse_property_row(cells, county_code, first_name, last_name)
                if prop:
                    properties.append(prop)

        # Also try parsing the detail page if we got a direct result
        detail = _parse_detail_page(soup, county_code)
        if detail:
            owner = detail.get("owner_name", "").lower()
            if last_name.lower() in owner:
                if not first_name or first_name.lower() in owner:
                    properties.append(detail)

    except requests.RequestException as e:
        logger.error(f"Request error for county {county_code}: {e}")
    except Exception as e:
        logger.error(f"Parse error for county {county_code}: {e}")

    return properties


def _parse_property_row(cells, county_code, first_name, last_name):
    """Parse a property result table row."""
    try:
        owner_name = cells[0].get_text(strip=True) if len(cells) > 0 else ""

        # Filter: owner name must match
        if last_name.lower() not in owner_name.lower():
            return None
        if first_name and first_name.lower() not in owner_name.lower():
            return None

        prop = {
            "owner_name": owner_name,
            "property_address": cells[1].get_text(strip=True) if len(cells) > 1 else "",
            "city": cells[2].get_text(strip=True) if len(cells) > 2 else "",
            "county": MD_COUNTIES.get(county_code, "Unknown"),
            "county_code": county_code,
            "state": "MD",
            "zip_code": cells[3].get_text(strip=True) if len(cells) > 3 else "",
            "property_type": cells[4].get_text(strip=True) if len(cells) > 4 else "",
            "assessed_value": "",
            "land_value": "",
            "improvement_value": "",
            "lot_size": "",
            "year_built": "",
            "account_number": "",
            "legal_description": "",
        }

        # Try to get detail link
        link = cells[0].select_one("a")
        if link and link.get("href"):
            prop["detail_url"] = link["href"]

        return prop

    except Exception as e:
        logger.debug(f"Error parsing property row: {e}")
        return None


def _parse_detail_page(soup, county_code):
    """Parse a property detail page for comprehensive info."""
    try:
        # Look for common SDAT detail page elements
        owner_el = soup.select_one("[id*='lblOwnerName'], [id*='Owner']")
        addr_el = soup.select_one("[id*='lblPremisesAddress'], [id*='Address']")
        value_el = soup.select_one("[id*='lblTotalAssessment'], [id*='TotalValue']")
        land_el = soup.select_one("[id*='lblLandValue'], [id*='LandValue']")
        improve_el = soup.select_one("[id*='lblImproveValue'], [id*='ImproveValue']")
        type_el = soup.select_one("[id*='lblUse'], [id*='PropertyUse']")
        year_el = soup.select_one("[id*='lblYearBuilt'], [id*='YearBuilt']")
        acct_el = soup.select_one("[id*='lblAccountNumber'], [id*='AcctNum']")
        lot_el = soup.select_one("[id*='lblLotSize'], [id*='LotSize']")
        legal_el = soup.select_one("[id*='lblLegalDescription']")

        if not owner_el:
            return None

        prop = {
            "owner_name": owner_el.get_text(strip=True) if owner_el else "",
            "property_address": addr_el.get_text(strip=True) if addr_el else "",
            "county": MD_COUNTIES.get(county_code, "Unknown"),
            "county_code": county_code,
            "city": "",
            "state": "MD",
            "zip_code": "",
            "assessed_value": _clean_currency(value_el.get_text(strip=True)) if value_el else "",
            "land_value": _clean_currency(land_el.get_text(strip=True)) if land_el else "",
            "improvement_value": _clean_currency(improve_el.get_text(strip=True)) if improve_el else "",
            "property_type": type_el.get_text(strip=True) if type_el else "",
            "year_built": year_el.get_text(strip=True) if year_el else "",
            "lot_size": lot_el.get_text(strip=True) if lot_el else "",
            "account_number": acct_el.get_text(strip=True) if acct_el else "",
            "legal_description": legal_el.get_text(strip=True) if legal_el else "",
        }

        # Extract city/zip from address
        addr = prop["property_address"]
        zip_match = re.search(r"(\d{5})(?:-\d{4})?", addr)
        if zip_match:
            prop["zip_code"] = zip_match.group(1)

        return prop if prop["owner_name"] else None

    except Exception as e:
        logger.debug(f"Error parsing detail page: {e}")
        return None


def _get_field(soup, field_name):
    """Extract a hidden form field value."""
    el = soup.find("input", {"name": field_name})
    return el.get("value", "") if el else ""


def _clean_currency(value):
    """Clean currency string to plain number string."""
    if not value:
        return ""
    cleaned = re.sub(r"[^\d.]", "", value)
    return cleaned
