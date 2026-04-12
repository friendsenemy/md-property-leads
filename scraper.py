"""
Obituary Scraper for Maryland
Scrapes Legacy.com for recent Maryland obituaries.
"""

import re
import time
import random
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Maryland cities/regions to search on Legacy.com
MD_REGIONS = [
    "baltimore-md", "annapolis-md", "rockville-md", "frederick-md",
    "silver-spring-md", "bethesda-md", "columbia-md", "germantown-md",
    "waldorf-md", "bowie-md", "hagerstown-md", "salisbury-md",
    "college-park-md", "laurel-md", "towson-md", "dundalk-md",
    "ellicott-city-md", "glen-burnie-md", "pasadena-md", "essex-md",
    "cumberland-md", "gaithersburg-md", "bel-air-md", "catonsville-md",
    "owings-mills-md", "severn-md", "odenton-md", "perry-hall-md",
]


def scrape_legacy_obituaries(max_pages=3):
    """
    Scrape Legacy.com for recent Maryland obituaries.
    Returns a list of dicts with obituary data.
    """
    obituaries = []
    session = requests.Session()
    session.headers.update(HEADERS)

    for region in MD_REGIONS:
        for page in range(1, max_pages + 1):
            try:
                url = f"https://www.legacy.com/us/obituaries/{region}/browse"
                params = {"page": page}

                logger.info(f"Scraping Legacy.com: {region} page {page}")
                resp = session.get(url, params=params, timeout=15)

                if resp.status_code != 200:
                    logger.warning(f"Got status {resp.status_code} for {region} page {page}")
                    break

                soup = BeautifulSoup(resp.text, "lxml")

                # Legacy.com uses various listing structures
                listings = soup.select("a[href*='/obituary/']")

                if not listings:
                    logger.info(f"No more listings for {region} at page {page}")
                    break

                for listing in listings:
                    obit = _parse_listing(listing, region)
                    if obit and obit["full_name"]:
                        obituaries.append(obit)

                # Be respectful with rate limiting
                time.sleep(random.uniform(1.5, 3.0))

            except requests.RequestException as e:
                logger.error(f"Request error for {region} page {page}: {e}")
                continue
            except Exception as e:
                logger.error(f"Parse error for {region} page {page}: {e}")
                continue

    # Deduplicate by name + date
    seen = set()
    unique = []
    for obit in obituaries:
        key = (obit["full_name"].lower(), obit.get("date_of_death", ""))
        if key not in seen:
            seen.add(key)
            unique.append(obit)

    logger.info(f"Scraped {len(unique)} unique obituaries from Legacy.com")
    return unique


def _parse_listing(element, region):
    """Parse a single obituary listing element."""
    try:
        obit = {
            "full_name": "",
            "first_name": "",
            "last_name": "",
            "middle_name": "",
            "date_of_death": "",
            "date_of_birth": "",
            "age": None,
            "city": region.rsplit("-", 1)[0].replace("-", " ").title() if region else "",
            "state": "MD",
            "obituary_url": "",
            "obituary_text": "",
            "survived_by": "",
            "source": "Legacy.com",
            "scraped_at": datetime.now().isoformat(),
        }

        # Extract name
        name_el = element.select_one("h2, h3, .obit-name, [class*='Name']")
        if name_el:
            obit["full_name"] = name_el.get_text(strip=True)
        else:
            text = element.get_text(strip=True)
            if text:
                obit["full_name"] = text.split("\n")[0].strip()

        if not obit["full_name"]:
            return None

        # Parse name components
        _parse_name(obit)

        # Extract URL
        href = element.get("href", "")
        if href:
            if href.startswith("/"):
                obit["obituary_url"] = f"https://www.legacy.com{href}"
            elif href.startswith("http"):
                obit["obituary_url"] = href

        # Extract dates from listing text
        listing_text = element.get_text(" ", strip=True)
        _extract_dates(obit, listing_text)

        return obit

    except Exception as e:
        logger.debug(f"Error parsing listing: {e}")
        return None


def fetch_obituary_details(url):
    """
    Fetch the full obituary page to extract additional details
    like survived-by info and more precise dates.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {}

        soup = BeautifulSoup(resp.text, "lxml")
        details = {}

        # Get full obituary text
        obit_body = soup.select_one(
            ".obituary-text, .obit-body, [class*='ObituaryText'], article"
        )
        if obit_body:
            text = obit_body.get_text(" ", strip=True)
            details["obituary_text"] = text

            # Extract survived-by information
            survived_match = re.search(
                r"(?:survived by|leaves behind|is survived by|"
                r"survivors include)(.*?)(?:\.|;|$)",
                text, re.IGNORECASE
            )
            if survived_match:
                details["survived_by"] = survived_match.group(1).strip()

            # Extract dates
            _extract_dates(details, text)

            # Extract age
            age_match = re.search(r"(?:age|aged)\s+(\d{1,3})", text, re.IGNORECASE)
            if age_match:
                details["age"] = int(age_match.group(1))

        time.sleep(random.uniform(1.0, 2.0))
        return details

    except Exception as e:
        logger.error(f"Error fetching obituary details from {url}: {e}")
        return {}


def _parse_name(obit):
    """Split full name into first, middle, last."""
    name = obit["full_name"]
    # Remove common suffixes
    name = re.sub(r"\s+(Jr\.?|Sr\.?|III|IV|II)\s*$", "", name, flags=re.IGNORECASE)
    # Remove quotes/nicknames
    name = re.sub(r'["\u201c\u201d].*?["\u201c\u201d]', "", name)
    name = re.sub(r"\(.*?\)", "", name)

    parts = name.strip().split()
    if len(parts) >= 3:
        obit["first_name"] = parts[0]
        obit["last_name"] = parts[-1]
        obit["middle_name"] = " ".join(parts[1:-1])
    elif len(parts) == 2:
        obit["first_name"] = parts[0]
        obit["last_name"] = parts[1]
    elif len(parts) == 1:
        obit["last_name"] = parts[0]


def _extract_dates(data, text):
    """Extract DOD and DOB from obituary text."""
    # Common date patterns
    date_patterns = [
        r"(\w+ \d{1,2},?\s*\d{4})",
        r"(\d{1,2}/\d{1,2}/\d{4})",
        r"(\d{1,2}-\d{1,2}-\d{4})",
    ]

    # Look for death date
    death_patterns = [
        r"(?:died|passed away|passed|departed).*?on\s+",
        r"(?:died|passed away|passed|departed)\s+",
        r"(\d{1,2}/\d{1,2}/\d{4})\s*[-\u2013]\s*(\d{1,2}/\d{1,2}/\d{4})",
    ]

    for pattern in death_patterns:
        match = re.search(pattern + r"(\w+ \d{1,2},?\s*\d{4})", text, re.IGNORECASE)
        if match:
            data["date_of_death"] = match.group(1) if match.lastindex else ""
            break

    # Look for date range pattern (birth - death)
    range_match = re.search(
        r"(\w+ \d{1,2},?\s*\d{4})\s*[-\u2013]\s*(\w+ \d{1,2},?\s*\d{4})",
        text
    )
    if range_match:
        if not data.get("date_of_birth"):
            data["date_of_birth"] = range_match.group(1)
        if not data.get("date_of_death"):
            data["date_of_death"] = range_match.group(2)
