"""
Obituary Scraper for Maryland - COMPLETE COVERAGE
Scrapes Legacy.com using TWO approaches for maximum coverage:
  1. Newspaper browse pages (20 verified MD newspaper partners)
  2. County-level local pages (all 23 counties + Baltimore City)
This dual approach ensures we catch EVERY Maryland obituary on Legacy.com,
including funeral-home-direct posts that never appear in any newspaper.
"""

import re
import json
import time
import random
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# Legacy.com fingerprints the TLS handshake and returns 403 to plain
# python-requests (even with browser headers). curl_cffi impersonates a
# real Chrome TLS stack and gets through. Fall back to requests if missing.
try:
    from curl_cffi import requests as _cffi_requests
    _HAS_CFFI = True
except ImportError:  # pragma: no cover
    _cffi_requests = None
    _HAS_CFFI = False


def make_session():
    """Return an HTTP session that Legacy.com will actually serve."""
    if _HAS_CFFI:
        sess = _cffi_requests.Session(impersonate="chrome")
        sess.headers.update({"Accept-Language": HEADERS["Accept-Language"]})
        return sess
    sess = requests.Session()
    sess.headers.update(HEADERS)
    return sess


logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ============================================================
# SOURCE 1: Maryland newspaper slugs on Legacy.com
# Official list from Legacy.com newspaper directory + validated
# ============================================================
MD_NEWSPAPERS = [
    # --- Baltimore Metro ---
    "baltimoresun",           # Baltimore City, Baltimore County, Harford, Howard
    "avenuenews",             # Baltimore area community paper
    "baltimoretimes",         # Baltimore City community paper
    "dundalkeagle",           # Baltimore County (Dundalk/Essex area)
    # --- Central Maryland ---
    "capitalgazette",         # Anne Arundel County (Annapolis/Severna Park)
    "carrollcountytimes",     # Carroll County (Westminster)
    "fredericknewspost",      # Frederick County
    "thedamascuslocal",       # Montgomery County (Damascus area)
    # --- DC Metro / Western Suburbs ---
    "washingtonpost",         # Montgomery, Prince George's, broader MD/DC/VA
    # --- Southern Maryland ---
    "somdnews-independent",   # Charles County (Maryland Independent)
    "somdnews-recorder",      # Calvert County (The Calvert Recorder)
    "somdnews-enterprise",    # St. Mary's County (The Enterprise)
    # --- Eastern Shore ---
    "stardem",                # Talbot & Caroline Counties (Star Democrat)
    "myeasternshoremd-kent",  # Kent County (Kent County News)
    "myeasternshoremd-qa",    # Queen Anne's County (Bay Times & Record Observer)
    "myeasternshoremd-dorchester",  # Dorchester County (Dorchester Star)
    "myeasternshoremd-timesrecord", # Eastern Shore (Times-Record)
    # --- Cecil County ---
    "cecildaily",             # Cecil County (Cecil Whig)
    # --- Border (picks up MD obits near state line) ---
    "newarkpostonline",       # Newark Post (DE) - Cecil County border
    # --- Statewide ---
    "newszapmd",              # Various MD areas
]

# ============================================================
# SOURCE 2: County-level local pages on Legacy.com
# These catch funeral-home-direct posts NOT in any newspaper
# All 23 Maryland counties + Baltimore City
# ============================================================
MD_COUNTIES = [
    "allegany-county",
    "anne-arundel-county",
    "baltimore",              # Baltimore City
    "baltimore-county",
    "calvert-county",
    "caroline-county",
    "carroll-county",
    "cecil-county",
    "charles-county",
    "dorchester-county",
    "frederick-county",
    "garrett-county",
    "harford-county",
    "howard-county",
    "kent-county",
    "montgomery-county",
    "prince-georges-county",
    "queen-annes-county",
    "saint-marys-county",
    "somerset-county",
    "talbot-county",
    "washington-county",
    "wicomico-county",
    "worcester-county",
]


def scrape_legacy_obituaries(max_pages=2):
    """
    Scrape Legacy.com for recent Maryland obituaries using both
    newspaper browse pages and county-level local pages.
    Returns a list of dicts with obituary data.
    """
    obituaries = []
    session = make_session()

    # --- Pass 1: Scrape newspaper browse pages ---
    for paper in MD_NEWSPAPERS:
        for page in range(1, max_pages + 1):
            try:
                url = f"https://www.legacy.com/us/obituaries/{paper}/browse"
                params = {"page": page}

                logger.info(f"Scraping newspaper: {paper} page {page}")
                resp = session.get(url, params=params, timeout=20)

                if resp.status_code != 200:
                    logger.warning(f"Got status {resp.status_code} for {paper} page"
                    f" {page}")
                    break

                page_obits = _extract_obituaries_json(resp.text, f"newspaper/{paper}")
                if not page_obits:
                    page_obits = _extract_obituaries_html(resp.text, f"newspaper/{paper}")

                if not page_obits:
                    logger.info(f"No obituaries found for {paper} at page {page}")
                    break

                # Filter to Maryland only (washingtonpost covers DC/VA too)
                for obit in page_obits:
                    state = obit.get("state", "")
                    if state in ("MD", "Maryland", ""):
                        obituaries.append(obit)

                logger.info(f"Found {len(page_obits)} obituaries for {paper} page {page}")
                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                logger.error(f"Error for {paper} page {page}: {e}")
                continue

    # --- Pass 2: Scrape county-level local pages ---
    # These catch funeral-home-direct posts not in any newspaper
    for county in MD_COUNTIES:
        for page in range(1, max_pages + 1):
            try:
                url = f"https://www.legacy.com/us/obituaries/local/maryland/{county}"
                params = {"page": page}

                logger.info(f"Scraping county: {county} page {page}")
                resp = session.get(url, params=params, timeout=20)

                if resp.status_code != 200:
                    logger.warning(f"Got status {resp.status_code} for county {county} page {page}")
                    break

                page_obits = _extract_obituaries_json(resp.text, f"county/{county}")
                if not page_obits:
                    page_obits = _extract_obituaries_html(resp.text, f"county/{county}")

                if not page_obits:
                    logger.info(f"No obituaries found for county {county} at page {page}")
                    break

                # Filter to Maryland only (county pages can show people born-in-MD but died elsewhere)
                for obit in page_obits:
                    state = obit.get("state", "")
                    if state in ("MD", "Maryland", ""):
                        obituaries.append(obit)

                logger.info(f"Found {len(page_obits)} county obituaries for {county} page {page}")
                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                logger.error(f"Error for county {county} page {page}: {e}")
                continue

    # Deduplicate by name, preferring richer records (old-schema with age/personId)
    by_name = {}
    for obit in obituaries:
        key = obit["full_name"].strip().lower()
        existing = by_name.get(key)
        if existing is None:
            by_name[key] = obit
        else:
            new_score = (1 if obit.get("age") else 0) + \
                        (1 if obit.get("person_id") else 0) + \
                        (1 if obit.get("obituary_text") else 0)
            old_score = (1 if existing.get("age") else 0) + \
                        (1 if existing.get("person_id") else 0) + \
                        (1 if existing.get("obituary_text") else 0)
            if new_score > old_score:
                by_name[key] = obit
    unique = list(by_name.values())
    logger.info(f"Total unique obituaries after dedup: {len(unique)} (from {len(obituaries)} raw)")
    return unique


def _extract_obituaries_json(html, source_label):
    """
    Extract obituary data from embedded JSON in Legacy.com HTML.
    Updated 2026-04: Legacy.com pages now contain TWO "obituaries" arrays:
      1. A small array (~10 items) with new schema: {title, link, imgLink, ...}
      2. A large array (~50 items) with old schema: {personId, name, location, ...}
    We extract from ALL matching arrays to get maximum coverage.
    """
    obituaries = []
    try:
        search_start = 0
        all_raw_obits = []

        while True:
            idx = html.find('"obituaries":[', search_start)
            if idx == -1:
                break

            nearby = html[idx:idx + 500]
            before = html[max(0, idx - 400):idx]
            # Skip the "Notable Obituaries" celebrity widget that sits on
            # every listing page (title/link schema, ~10 national names).
            if "celebrity-deaths" in before or "Notable Obituaries" in before:
                search_start = idx + 1
                continue
            if '"personId"' in nearby:
                arr_start = idx + len('"obituaries":')
                depth = 0
                end_idx = arr_start
                for i in range(arr_start, min(len(html), arr_start + 500000)):
                    if html[i] == '[':
                        depth += 1
                    elif html[i] == ']':
                        depth -= 1
                        if depth == 0:
                            end_idx = i + 1
                            break

                json_str = html[arr_start:end_idx]
                try:
                    raw_obits = json.loads(json_str)
                    all_raw_obits.extend(raw_obits)
                    logger.debug(f"Found array with {len(raw_obits)} items for {source_label}")
                except json.JSONDecodeError as e:
                    logger.error(f"JSON parse error in array for {source_label}: {e}")

            search_start = idx + 1

        if not all_raw_obits:
            logger.debug(f"No obituaries JSON found for {source_label}")
            return []

        for raw in all_raw_obits:
            obit = _parse_json_obituary(raw, source_label)
            if obit and obit["full_name"]:
                obituaries.append(obit)

    except Exception as e:
        logger.error(f"Error extracting obituaries for {source_label}: {e}")

    return obituaries



_STATE_NAMES = {
    "maryland": "MD", "md": "MD", "virginia": "VA", "va": "VA",
    "pennsylvania": "PA", "pa": "PA", "delaware": "DE", "de": "DE",
    "west virginia": "WV", "wv": "WV", "district of columbia": "DC",
    "washington, d.c.": "DC", "washington d.c.": "DC", "dc": "DC",
    "north carolina": "NC", "florida": "FL", "new jersey": "NJ", "new york": "NY",
}


def _extract_obituaries_html(html, source_label):
    """
    Parse Legacy.com's server-rendered listing cards (2026 layout).
    Each card has: <a href=".../person/slug-ID"> name, "YYYY - YYYY",
    a snippet <p title="..."> and a link to the funeral-home obituary page.
    """
    obituaries = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        seen = set()
        for a in soup.select('a[href*="legacy.com/person/"]'):
            href = a.get("href", "")
            m = re.search(r"/person/[^/?#]*?-(\d+)$", href)
            if not m:
                continue
            person_id = m.group(1)
            if person_id in seen:
                continue
            name_el = a.select_one(".font-serif")
            if not name_el:
                continue  # the image-only link duplicate
            seen.add(person_id)

            full_name = name_el.get_text(" ", strip=True)
            years_el = a.select_one("span.font-semibold")
            years = years_el.get_text(strip=True) if years_el else ""
            dob = dod = ""
            ym = re.match(r"(\d{4})\s*-\s*(\d{4})", years)
            if ym:
                dob, dod = ym.group(1), ym.group(2)
            elif re.fullmatch(r"\d{4}", years):
                dod = years

            snippet_el = a.select_one("p[title]")
            snippet = (snippet_el.get("title") or snippet_el.get_text(" ", strip=True)) if snippet_el else ""

            # "Jane Doe, 84, of Waldorf, Maryland, passed away ..."
            age = None
            city = ""
            state = ""
            sm = re.search(r",?\s*(?:age\s+)?(\d{1,3}),?\s+of\s+([A-Z][A-Za-z .'\-]+?),\s*([A-Z][A-Za-z .]+?)[,.]", snippet)
            if sm:
                age = int(sm.group(1))
                city = sm.group(2).strip()
                state = _STATE_NAMES.get(sm.group(3).strip().lower(), sm.group(3).strip()[:2].upper())
            else:
                sm2 = re.search(r"\bof\s+([A-Z][A-Za-z .'\-]+?),\s*(Maryland|MD)\b", snippet)
                if sm2:
                    city, state = sm2.group(1).strip(), "MD"

            # Outbound obituary link sits in the sibling "Obituary links" region
            obit_url = ""
            card = a.find_parent("div")
            for _ in range(5):
                if card is None:
                    break
                ids = {re.search(r"-(\d+)$", x.get("href", "")).group(1)
                       for x in card.select('a[href*="legacy.com/person/"]')
                       if re.search(r"-(\d+)$", x.get("href", ""))}
                if len(ids) > 1:
                    break  # walked past this person's card
                link = card.select_one('a[href*="/us/obituaries/name/"]')
                if link:
                    obit_url = link.get("href", "")
                    break
                card = card.find_parent("div")

            # Clean name: drop trailing nicknames in parens for matching
            clean = re.sub(r"\s*\([^)]*\)", "", full_name).strip()
            parts = [p for p in clean.replace(",", " ").split() if p]
            suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv"}
            while parts and parts[-1].lower() in suffixes:
                parts.pop()
            first_name = parts[0] if parts else ""
            last_name = parts[-1] if len(parts) > 1 else ""
            middle_name = " ".join(parts[1:-1]) if len(parts) > 2 else ""

            obituaries.append({
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "middle_name": middle_name,
                "date_of_death": dod,
                "date_of_birth": dob,
                "age": age,
                "city": city,
                "state": state or "",
                "obituary_url": obit_url or href,
                "obituary_text": snippet,
                "survived_by": "",
                "source": f"Legacy.com/{source_label}",
                "scraped_at": datetime.now().isoformat(),
                "person_id": person_id,
            })
    except Exception as e:
        logger.error(f"HTML parse error for {source_label}: {e}")
    return obituaries


def _parse_json_obituary(raw, source_label):
    """
    Parse a single obituary JSON object from Legacy.com.
    Legacy.com pages serve TWO schemas:
      1. Old/rich schema (preferred): personId, name{}, location{}, age,
         fromToYears, obitSnippet, links{} - ~50 items per page
      2. New/minimal schema (fallback): title, link, imgLink - ~10 items
    We try old schema FIRST because it has much richer data.
    """
    try:
        # --- Try old schema first (personId/name/location) - RICHER DATA ---
        name_data = raw.get("name", {})
        if isinstance(name_data, dict) and name_data.get("fullName"):
            location_data = raw.get("location", {})
            city_data = location_data.get("city", {})
            state_data = location_data.get("state", {})
            links_data = raw.get("links", {})
            obit_url_data = links_data.get("obituaryUrl", {})

            full_name = name_data.get("fullName", "")
            first_name = name_data.get("firstName", "")
            last_name = name_data.get("lastName", "")
            middle_name = name_data.get("middleName", "") or ""

            date_of_birth = ""
            date_of_death = ""
            from_to = raw.get("fromToYears", "")
            if from_to and " - " in str(from_to):
                parts = str(from_to).split(" - ")
                if len(parts) == 2:
                    date_of_birth = parts[0].strip()
                    date_of_death = parts[1].strip()

            obituary_url = ""
            if isinstance(obit_url_data, dict):
                obituary_url = obit_url_data.get("href", "")
            elif isinstance(obit_url_data, str):
                obituary_url = obit_url_data

            return {
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "middle_name": middle_name,
                "date_of_death": date_of_death,
                "date_of_birth": date_of_birth,
                "age": raw.get("age"),
                "city": (city_data.get("fullName") or "") if isinstance(city_data, dict) else (str(city_data) if city_data else ""),
                "state": (state_data.get("code") or "MD") if isinstance(state_data, dict) else (str(state_data) if state_data else "MD"),
                "obituary_url": obituary_url,
                "obituary_text": raw.get("obitSnippet", "") or "",
                "survived_by": "",
                "source": f"Legacy.com/{source_label}",
                "scraped_at": datetime.now().isoformat(),
                "person_id": str(raw.get("personId", "")),
            }

        # --- Fallback: new schema (title + link) ---
        title = raw.get("title", "")
        link = raw.get("link", "")
        if title and link:
            full_name = title
            date_of_birth = ""
            date_of_death = ""

            year_match = re.match(r"^(.*?)\s*\((\d{4})\s*-\s*(\d{4})\)\s*$", title)
            if year_match:
                full_name = year_match.group(1).strip()
                date_of_birth = year_match.group(2)
                date_of_death = year_match.group(3)
            else:
                year_match2 = re.match(r"^(.*?)\s*\((\d{4})\)\s*$", title)
                if year_match2:
                    full_name = year_match2.group(1).strip()
                    date_of_death = year_match2.group(2)

            name_parts = full_name.split()
            first_name = name_parts[0] if name_parts else ""
            last_name = name_parts[-1] if len(name_parts) > 1 else ""
            middle_name = " ".join(name_parts[1:-1]) if len(name_parts) > 2 else ""

            return {
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "middle_name": middle_name,
                "date_of_death": date_of_death,
                "date_of_birth": date_of_birth,
                "age": None,
                "city": "",
                "state": "MD",
                "obituary_url": link,
                "obituary_text": "",
                "survived_by": "",
                "source": f"Legacy.com/{source_label}",
                "scraped_at": datetime.now().isoformat(),
                "person_id": "",
            }

        return None

    except Exception as e:
        logger.debug(f"Error parsing obituary JSON: {e}")
        return None


def _fetch_obituary_details(url, session):
    """
    Fetch the full obituary page to extract additional details
    like survived_by, full obituary text, date of birth, etc.
    """
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return {}

        soup = BeautifulSoup(resp.text, "html.parser")
        details = {}

        # Get full obituary text
        obit_div = soup.select_one(
            "[class*='ObituaryText'], [class*='obit-text'], "
            "[class*='obituary-text'], [data-component='ObituaryText']"
        )
        if obit_div:
            details["obituary_text"] = obit_div.get_text(separator=" ", strip=True)
        else:
            # legacy.com/person/ memorial pages (2026) embed the obituary
            # inside an escaped Next.js payload: \"obituaryText\":\"...\"
            m = re.search(r'\\"obituaryText\\":\\"((?:[^"\\]|\\.)*?)\\"', resp.text)
            if not m:
                m = re.search(r'"obituaryText":"((?:[^"\\]|\\.)*?)"', resp.text)
            if m:
                try:
                    txt = json.loads('"' + m.group(1).replace('\\"', '"').replace('\\\\', '\\') + '"')
                except Exception:
                    txt = m.group(1)
                txt = BeautifulSoup(txt, "html.parser").get_text(" ", strip=True)
                if len(txt) > 40:
                    details["obituary_text"] = txt
            for key in ("dateOfDeath", "deathDate"):
                dm = re.search(r'\\?"%s\\?":\\?"(\d{4}-\d{2}-\d{2})' % key, resp.text)
                if dm:
                    details["date_of_death"] = dm.group(1)
                    break

        # Extract survived by
        text = details.get("obituary_text", "")
        if text:
            survived_match = re.search(
                r"(?:survived by|is survived by|leaves behind|"
                r"left to cherish)(.*?)(?:\.|;|$)",
                text, re.IGNORECASE
            )
            if survived_match:
                details["survived_by"] = survived_match.group(1).strip()[:500]

            # Extract date of birth if not already known
            dob_match = re.search(
                r"(?:born on|born|date of birth)[:\s]*?"
                r"[^,.\d]{0,40}?"
                r"(\w+ \d{1,2},?\s*\d{4}|\d{1,2}/\d{1,2}/\d{2,4})",
                text, re.IGNORECASE
            )
            if dob_match:
                details["date_of_birth"] = dob_match.group(1).strip()

            # Extract date of death from obituary text
            dod_match = re.search(
                r"(?:passed away|died|passed on|departed this life|entered into eternal rest|went home to be with the lord|passed peacefully|departed|passed)"
                r"[^,.\d]{0,80}?"
                r"(?:on\s+)?(\w+ \d{1,2},?\s*\d{4}|\d{1,2}/\d{1,2}/\d{2,4})",
                text, re.IGNORECASE
            )
            if dod_match:
                details["date_of_death"] = dod_match.group(1).strip()

        # Also check for structured date elements in the page (Legacy uses these)
        date_els = soup.select("[class*='date'], [class*='Date'], time")
        for el in date_els:
            date_text = el.get_text(strip=True)
            if not date_text:
                continue
            # Check datetime attribute (more reliable)
            dt_attr = el.get("datetime", "")
            if dt_attr and not details.get("date_of_death"):
                details.setdefault("date_of_death", dt_attr)

        time.sleep(random.uniform(1.0, 2.0))
        return details

    except Exception as e:
        logger.error(f"Error fetching obituary details from {url}: {e}")
        return {}


def fetch_obituary_details(url):
    """
    Public wrapper for _fetch_obituary_details.
    Creates its own session so callers don't need to manage one.
    """
    session = make_session()
    return _fetch_obituary_details(url, session)
