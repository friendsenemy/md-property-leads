"""
Obituary Scraper for Maryland
Scrapes Legacy.com for recent Maryland obituaries.
Extracts embedded JSON data from newspaper browse pages.
"""

import re
import json
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

# Maryland newspaper slugs on Legacy.com
MD_NEWSPAPERS = [
    "baltimoresun",
    "capitalgazette",
    "fredericknewspost",
    "washingtonpost",
    "stardem",
    "carrollcountytimes",
    "avenuenews",
    "baltimoretimes",
    "dundalkeagle",
    "newszapmd",
    "timesnews",
]


def scrape_legacy_obituaries(max_pages=2):
    """
    Scrape Legacy.com for recent Maryland obituaries.
    Fetches newspaper browse pages and extracts embedded JSON data.
    Returns a list of dicts with obituary data.
    """
    obituaries = []
    session = requests.Session()
    session.headers.update(HEADERS)

    for paper in MD_NEWSPAPERS:
        for page in range(1, max_pages + 1):
            try:
                url = f"https://www.legacy.com/us/obituaries/{paper}/browse"
                params = {"page": page}

                logger.info(f"Scraping Legacy.com: {paper} page {page}")
                resp = session.get(url, params=params, timeout=20)

                if resp.status_code != 200:
                    logger.warning(f"Got status {resp.status_code} for {paper} page {page}")
                    break

                # Extract embedded JSON obituary data from HTML
                page_obits = _extract_obituaries_json(resp.text, paper)

                if not page_obits:
                    logger.info(f"No obituaries found for {paper} at page {page}")
                    break

                # Filter to Maryland only (some papers like washingtonpost cover multiple states)
                for obit in page_obits:
                    state = obit.get("state", "")
                    if state in ("MD", "Maryland", ""):
                        obituaries.append(obit)

                logger.info(f"Found {len(page_obits)} obituaries for {paper} page {page}")

                # Be respectful with rate limiting
                time.sleep(random.uniform(1.5, 3.0))

            except requests.RequestException as e:
                logger.error(f"Request error for {paper} page {page}: {e}")
                continue
            except Exception as e:
                logger.error(f"Parse error for {paper} page {page}: {e}")
                continue

    # Deduplicate by personId or name + date
    seen = set()
    unique = []
    for obit in obituaries:
        pid = obit.get("person_id", "")
        key = pid if pid else (obit["full_name"].lower(), obit.get("date_of_death", ""))
        if key not in seen:
            seen.add(key)
            unique.append(obit)

    logger.info(f"Scraped {len(unique)} unique MD obituaries from Legacy.com")
    return unique


def _extract_obituaries_json(html, paper):
    """
    Extract obituary data from embedded JSON in Legacy.com HTML.
    Legacy embeds obituary listings as a JSON array in the page source.
    """
    obituaries = []

    try:
        # Find the obituaries JSON array that contains personId entries
        search_start = 0
        target_start = -1

        while True:
            idx = html.find('"obituaries":[', search_start)
            if idx == -1:
                break
            # Check if this array contains personId (the real obit data)
            nearby = html[idx:idx + 300]
            if "personId" in nearby:
                target_start = idx + len('"obituaries":')
                break
            search_start = idx + 1

        if target_start == -1:
            logger.debug(f"No obituaries JSON found for {paper}")
            return []

        # Find the matching closing bracket for the array
        depth = 0
        end_idx = target_start
        for i in range(target_start, min(len(html), target_start + 500000)):
            if html[i] == '[':
                depth += 1
            elif html[i] == ']':
                depth -= 1
                if depth == 0:
                    end_idx = i + 1
                    break

        json_str = html[target_start:end_idx]
        raw_obits = json.loads(json_str)

        for raw in raw_obits:
            obit = _parse_json_obituary(raw, paper)
            if obit and obit["full_name"]:
                obituaries.append(obit)

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error for {paper}: {e}")
    except Exception as e:
        logger.error(f"Error extracting obituaries for {paper}: {e}")

    return obituaries


def _parse_json_obituary(raw, paper):
    """Parse a single obituary from the embedded JSON data."""
    try:
        name_data = raw.get("name", {})
        location_data = raw.get("location", {})
        city_data = location_data.get("city", {})
        state_data = location_data.get("state", {})
        links_data = raw.get("links", {})
        obit_url_data = links_data.get("obituaryUrl", {})

        full_name = name_data.get("fullName", "")
        first_name = name_data.get("firstName", "")
        last_name = name_data.get("lastName", "")
        middle_name = name_data.get("middleName", "") or ""

        # Parse dates from fromToYears field (format: "MM/DD/YYYY - MM/DD/YYYY")
        date_of_birth = ""
        date_of_death = ""
        from_to = raw.get("fromToYears", "")
        if from_to and " - " in from_to:
            parts = from_to.split(" - ")
            if len(parts) == 2:
                date_of_birth = parts[0].strip()
                date_of_death = parts[1].strip()

        # Get obituary URL
        obituary_url = ""
        if isinstance(obit_url_data, dict):
            obituary_url = obit_url_data.get("href", "")
        elif isinstance(obit_url_data, str):
            obituary_url = obit_url_data

        obit = {
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "middle_name": middle_name,
            "date_of_death": date_of_death,
            "date_of_birth": date_of_birth,
            "age": raw.get("age"),
            "city": city_data.get("fullName", "") if isinstance(city_data, dict) else str(city_data),
            "state": state_data.get("code", "MD") if isinstance(state_data, dict) else str(state_data),
            "obituary_url": obituary_url,
            "obituary_text": raw.get("obitSnippet", "") or "",
            "survived_by": "",
            "source": f"Legacy.com/{paper}",
            "scraped_at": datetime.now().isoformat(),
            "person_id": str(raw.get("personId", "")),
        }

        return obit

    except Exception as e:
        logger.debug(f"Error parsing obituary JSON: {e}")
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

        soup = BeautifulSoup(resp.text, "html.parser")
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
                r"survivors include)(.*?)(?:\\.|;|$)",
                text, re.IGNORECASE
            )
            if survived_match:
                details["survived_by"] = survived_match.group(1).strip()

            # Extract age
            age_match = re.search(r"(?:age|aged)\\s+(\\d{1,3})", text, re.IGNORECASE)
            if age_match:
                details["age"] = int(age_match.group(1))

        time.sleep(random.uniform(1.0, 2.0))
        return details

    except Exception as e:
        logger.error(f"Error fetching obituary details from {url}: {e}")
        return {}
