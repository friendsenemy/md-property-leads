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
    session = requests.Session()
    session.headers.update(HEADERS)

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
                    logger.info(f"No obituaries found for {paper} at page {page}")
                    break

                # Filter to Maryland only (washingtonpost covers DC/VA too)
                for obit in page_obits:
                    state = obit.get("state", "")
                    if state in ("MD", "Maryland", ""):
                        obituaries.append(obit)

                logger.info(f"Found {len(page_obits)} obituaries for {paper} page {page}")
                time.sleep(random.uniform(1.5, 3.0))

            except requests.RequestException as e:
                logger.error(f"Request error for {paper} page {page}: {e}")
                continue
            except Exception as e:
                logger.error(f"Parse error for {paper} page {page}: {e}")
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
                    logger.info(f"No obituaries found for county {county} at page {page}")
                    break

                # Filter to Maryland only (county pages can show people born-in-MD but died elsewhere)
                for obit in page_obits:
                    state = obit.get("state", "")
                    if state in ("MD", "Maryland", ""):
                        obituaries.append(obit)

                logger.info(f"Found {len(page_obits)} county obituaries for {county} page {page}")
                time.sleep(random.uniform(1.5, 3.0))

            except requests.RequestException as e:
                logger.error(f"Request error for county {county} page {page}: {e}")
                continue
            except Exception as e:
                logger.error(f"Parse error for county {county} page {page}: {e}")
                continue

    # Deduplicate by URL, personId, or name + date (newspaper and county pages overlap heavily)
    seen = set()
    unique = []
    for obit in obituaries:
        url = obit.get("ObituaryURL", "")
        pid = obit.get("PersonID", "")
        key = url if url else (pid if pid else (obit["FullName"].lower(), obit.get("DateOfDeath", "")))
        if key not in seen:$����������������䁹�Ё���͕���6VV��FB��W���V�VR�V�B��&�B�����vvW"��f�b%67&VB��V�V�VR��V�VR�B�&�GV&�W2g&���Vv7��6�� �b"�'��V��&�GV&�W2��F�F�&Vf�&RFVGW�"��&WGW&�V�VP���FVb�W�G&7E��&�GV&�W5��6�ↇF���6�W&6U��&V��"" �W�G&7B�&�GV'�FFg&��V�&VFFVB�4�����Vv7��6���D����Vv7�V�&VG2�&�GV'�Ɨ7F��w22�4��'&���F�RvR6�W&6R�&�F��Ww7W"�B6�V�G�vW2W6RF��26�R7G'V7GW&RࠢWFFVB##b�C��Vv7��6��vW2��r6��F��Et�&�&�GV&�W2"'&�3���6���'&����FV�2�v�F��Wr66�V���F�F�R�Ɩ����tƖ�����Т"��&vR'&���S�FV�2�v�F���B66�V���W'6��B���R���6F�������ТvRW�G&7Bg&�����F6���r'&�2F�vWB����V�6�fW&vR�"" ��&�GV&�W2��Р�G'���2f��B���&�GV&�W2�4��'&�2F�B6��F��7GV��&�BV�G&�W2�2vW2�fR�V�F��R&�&�GV&�W2"'&�3�vR6���V7Bg&��WfW'���P�2F�B6��F��2&V�FF��FV�F�f�VB'�&Ɩ�"�"'W'6��B"�W�2��6V&6��7F'B� ����&u��&�G2��Р�v���RG'VS���G���F���f��B�r&�&�GV&�W2#��r�6V&6��7F'B���b�G������'&V���26�V6��bF��2'&�6��F��2&V��&�GV'�FF��V&'���F�Ŷ�G���G��SТ�br"&Ɩ�""���V&'��"r"'W'6��B""r���V&'���'%�7F'B��G���V�r"&�&�GV&�W2#��r���2f��BF�R�F6���r6��6��r'&6�WBf�"F�R'"'%�7F'B��G���V�r"&�&�GV&�W2#��r���2f��BF�R�F6���r6��6��r'&6�WBf�"F�R'&��'&6U�6�V�B��f�"���&�vR�'%�7F'B��VↇF�����b�F�Ŷ����u�s��'&6U�6�V�B���VƖb�F�Ŷ����u�s��'&6U�6�V�B����b'&6U�6�V�B����'%�V�B����'&V���G'���&u��6����F�Ŷ'%�7F'C�'%�V�EТ7W'&V�E��&�G2��6�����G2�&u��6�␢���&u��&�G2�W�FV�B�7W'&V�E��&�G2��W�6WBf�VTW'&�#��70��6V&6��7F'B��G����2'6RV6�V�G'�g&�����&�GV'�'&�2�6��6�ƖFF��rFFg&��&�F�7FGW&W0�f�"�&W2�����&u��&�G3��2�6�V�66Rf�V�B��W2g&��&�F�66�V�2F�6�������W0�2�Wr66�V��F�F�R�Ɩ����tƖ��FW67&�F���2��B66�V��W'6��B���R���6F����FFUv�GF��FFT�V�v�B�fu7F"���26�V6�v��6�66�V�F��2V�G'�W6W2&6VB��v��6��W�2W��7@��5��Wu�66�V��'F�F�R"���&W2�"&Ɩ�"���&W0��5���E�66�V��'W'6��B"���&W2�""&��R"���&W0���b�5��Wu�66�V���2�Wr66�V�V�G'��Ɩ���&W2�vWB�&Ɩ�"�""��F�F�R��&W2�vWB�'F�F�R"�""��'6VB��'6U�f�F�2�F�F�R���b'6VC��'6VE�&�&�GV'��W&�%��Ɩ氢�&�GV&�W2�V�B�'6VB��VƖb�5���E�66�V���2��B66�V�V�G'� ���R��&W2�vWB�&��R"�""��'6VB��'6U�f�F�2���R���b'6VC��'6VE�'W'6����B%���&W2�vWB�'W'6��B"�""���&�GV&�W2�V�B�'6VB���&WGW&��&�GV&�W0��W�6WBW�6WF���2S����vvW"�FV'Vr�b$W'&�"'6��r�&�GV'��4���W�"��&WGW&��Р��FVb�'6U�f�F�2�F�F�R���"" �'6Rf�F���f�g&���&�GV'�F�F�R�"&��R"f�V�B���Vv7��&�GV&�W2ࠢ6�����GFW&�3���$f�'7B֖FF�R�7B�����Օ����"�&�'F��BFVF��V'0��$f�'7B֖FF�R�7B�&�'F��V"�҄FVF������"�v�fV�vP��$f�'7B֖FF�R�7B������"���ǒF�RFVF��V" �"" �gV�����R�" �FFU��e�&�'F��" �FFU��e�FVF��" ���b��BF�F�R�&���F�w&�FR6�FR��F�R72F�B&WGW&�2��&W6V6�ц6��2Ɨ7B�"F�7B6��F���s���6���V�G2�&r��&w5�T'r ��W'6��B�FFT�dFVF��Ɩ�g&����B66�V����"Ɩ�g&���Wr66�V���2'6RF�F�R7G&��r�6�����f�&�G2���W7G&FVB&�fP�26�����GFW&�$��R�%�V"�E�V"�"�"$��R��V"�"�"$��R�%�T"�E�T"� ��b��BF�F�R7G&�����&WGW&����P���b"�"��B��F�F�S��&WGW&����P��2W�G&7BF�R'B��&V�F�W6W0��V%�GFW&��"r���G�GҒ���G�GҒ��r ��b�F6���&R�6V&6���V%�GFW&��F�F�R����b�F6��w&�W�"���2v�B&�vRƖ�R��#��FFU��e�&�'F���F6��w&�W���FFU��e�FVF���F6��w&�W�"��V�6S��2G'�f�&�Bv�F��WB&�'F��V#�$��R������ ��V%��F6�"�&R��F6��"%����2���G�Gҕ�2�B"�F�F�R���b�V%��F6�#��gV�����R��V%��F6�"�w&�W���7G&����FFU��e�FVF���V%��F6�"�w&�W�"���27ƗB��R��F�'G0���U�'G2�gV�����R�7ƗB���f�'7E���R���U�'G5���b��U�'G2V�6R" ��7E���R���U�'G5����b�V���U�'G2��V�6R" �֖FF�U���R�""������U�'G5���Ғ�b�V���U�'G2��"V�6R" ��&WGW&���&gV�����R#�gV�����R��&f�'7E���R#�f�'7E���R��&�7E���R#��7E���R��&֖FF�U���R#�֖FF�U���R��&FFU��e�FVF�#�FFU��e�FVF���&FFU��e�&�'F�#�FFU��e�&�'F���&vR#����R��&6�G�#�""��'7FFR#�$�B"��&�&�GV'��W&�#�Ɩ���&�&�GV'��FW�B#�""��'7W'f�fVE�'�#�""��'6�W&6R#�b$�Vv7��6����6�W&6U��&V��"��'67&VE�B#�FFWF��R���r���6�f�&�B����'W'6����B#�""��Р�&WGW&����P��W�6WBW�6WF���2S����vvW"�FV'Vr�b$W'&�"'6��r�&�GV'��4���W�"��&WGW&����P���FVbfWF6���&�GV'��FWF��2�W&��"" �fWF6�F�RgV���&�GV'�vRF�W�G&7BFF�F����FWF��0�Ɩ�R7W'f�fVB�'���f��B��&R&V6�6RFFW2�"" �G'���&W7�&WVW7G2�vWB�W&���VFW'3ԄTDU%2�F��V�WC�R���b&W7�7FGW5�6�FR�#��&WGW&��Р�6�W�&VWF�gV�6�W�&W7�FW�B�&�F���'6W""��FWF��2��Р�2vWBgV���&�GV'�FW�@��&�E�&�G��6�W�6V�V7E���R��"��&�GV'��FW�B���&�B�&�G���6�72��t�&�GV'�FW�Bu��'F�6�R ����b�&�E�&�G���FW�B��&�E�&�G��vWE�FW�B�""�7G&��G'VR��FWF��5�&�&�GV'��FW�B%��FW�@��2W�G&7B7W'f�fVB�'���f�&�F���7W'f�fVE��F6��&R�6V&6���""��7W'f�fVB'���VfW2&V���GƗ27W'f�fVB'�� �"'7W'f�f�'2��6�VFR�������÷�B�"��FW�B�&R�t��$T44P����b7W'f�fVE��F6���FWF��5�'7W'f�fVE�'�%��7W'f�fVE��F6��w&�W���7G&�����2W�G&7BvP�vU��F6��&R�6V&6��""���vW�vVB��2���G��7Ғ"�FW�B�&R�t��$T44R���bvU��F6���FWF��5�&vR%����B�vU��F6��w&�W�����2W�G&7BFFR�bFVF�g&���&�GV'�FW�@�26�����GFW&�3�'76VBv�����V'��##R"��2&F�VB�&6�R�##R"�&��&��2�##R �F�E��F6��&R�6V&6���""��76VBv��76VG�F�VG�FW'FVG�V�FW&VB��F�&W7G� �"'vV�B���RF�&Rv�F��G&�6�F���VB� �"%������E׳�C�� �""��r��G��'����2��G�G���G��'���G��'���G�"�GҒ"��FW�B�&R�t��$T44P����bF�E��F6���FWF��5�&FFU��e�FVF�%��F�E��F6��w&�W���7G&�����2W�G&7BFFR�b&�'F�g&���&�GV'�FW�@�26�����GFW&�3�&&�&�����V'���C"�&&�&���V'���C �F�%��F6��&R�6V&6���""���<�onacrocting)"
                r"[^,,.\d]{0,40}?"
                r"(\w+ \d{1,2},?\s*\d{4}|\d{1,2}/\d{1,2}/\d{2,4})",
                text, re.IGNORECASE
            )
            if dob_match:
                details["date_of_birth"] = dob_match.group(1).strip()

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
