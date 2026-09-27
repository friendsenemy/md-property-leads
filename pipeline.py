"""
MD Property Leads — GitHub Actions pipeline runner.

Scrapes Maryland obituaries from Legacy.com, cross-references each deceased
person against SDAT property ownership (licensed Socrata dataset), and
writes the results to data/leads.json for the static dashboard.

No server, no database. State lives in two JSON files committed to the repo:
  data/leads.json  — every lead ever found (append-only, deduped)
  data/seen.json   — obituaries already checked, so we don't hit SDAT twice

Run locally:
  MD_OPENDATA_USERNAME=... MD_OPENDATA_PASSWORD=... python pipeline.py
"""

import os
import re
import sys
import json
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from scraper import scrape_legacy_obituaries, fetch_obituary_details
from property_lookup import search_property_by_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")
logging.getLogger("urllib3").setLevel(logging.WARNING)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
LEADS_PATH = os.path.join(DATA_DIR, "leads.json")
SEEN_PATH = os.path.join(DATA_DIR, "seen.json")

MAX_PAGES = int(os.environ.get("SCRAPE_MAX_PAGES", "2"))
MAX_NEW_PER_RUN = int(os.environ.get("MAX_NEW_PER_RUN", "500"))
WORKERS = int(os.environ.get("PIPELINE_WORKERS", "4"))
SEEN_RETENTION_DAYS = 180

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], 1)}


# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _save(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def normalize_date(text):
    """'September 3, 2026' / '9/3/2026' / '2026-09-03' / '2026' -> ISO-ish."""
    if not text:
        return ""
    text = str(text).strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return m.group(0)
    m = re.match(r"^([A-Za-z]+)\.?\s+(\d{1,2}),?\s*(\d{4})$", text)
    if m and m.group(1).lower() in MONTHS:
        return f"{m.group(3)}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", text)
    if m:
        y = int(m.group(3))
        y = y + 2000 if y < 100 else y
        return f"{y}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    if re.fullmatch(r"\d{4}", text):
        return text
    return text


def obit_key(obit):
    """Stable identity for an obituary across runs."""
    pid = (obit.get("person_id") or "").strip()
    if pid:
        return f"pid:{pid}"
    name = re.sub(r"[^a-z]", "", (obit.get("full_name") or "").lower())
    year = (normalize_date(obit.get("date_of_death")) or "")[:4]
    return f"name:{name}:{year}"


def lead_id(key):
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def is_maryland(obit):
    st = (obit.get("state") or "").strip().upper()
    return st in ("", "MD", "MARYLAND")


# ────────────────────────────────────────────────────────────
# Per-obituary work (runs in a thread)
# ────────────────────────────────────────────────────────────

def process_obituary(obit):
    """Fetch details, look up SDAT, return (key, obit, properties)."""
    key = obit_key(obit)
    url = obit.get("obituary_url", "")
    if url:
        try:
            details = fetch_obituary_details(url)
            for k in ("date_of_death", "date_of_birth", "survived_by",
                      "obituary_text", "age"):
                if details.get(k) and not obit.get(k):
                    obit[k] = details[k]
            # A real date beats a bare year from the listing card
            if details.get("date_of_death") and len(str(obit.get("date_of_death", ""))) <= 4:
                obit["date_of_death"] = details["date_of_death"]
        except Exception as e:  # pragma: no cover
            logger.debug("details failed for %s: %s", obit.get("full_name"), e)

    # Re-derive state/city from full text if the listing didn't have it
    if not obit.get("city") or not obit.get("state"):
        txt = obit.get("obituary_text") or ""
        m = re.search(r"\b(?:of|from)\s+([A-Z][A-Za-z .'\-]+?),\s*(Maryland|MD)\b", txt)
        if m:
            obit["city"] = obit.get("city") or m.group(1).strip()
            obit["state"] = "MD"

    if not is_maryland(obit):
        return key, obit, []

    last = obit.get("last_name", "")
    first = obit.get("first_name", "")
    if not last or len(last) < 2:
        return key, obit, []

    props = search_property_by_name(last, first, city=obit.get("city", ""))
    return key, obit, props or []


# ────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────

def main():
    started = _now()
    if not (os.environ.get("MD_OPENDATA_USERNAME") and os.environ.get("MD_OPENDATA_PASSWORD")):
        logger.error("MD_OPENDATA_USERNAME / MD_OPENDATA_PASSWORD not set — "
                     "the licensed SDAT dataset needs them. Aborting.")
        sys.exit(2)

    store = _load(LEADS_PATH, {"leads": [], "runs": []})
    seen = _load(SEEN_PATH, {})
    existing_ids = {l["id"] for l in store["leads"]}

    # Prune old 'seen' entries so the file doesn't grow forever
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SEEN_RETENTION_DAYS)).isoformat()
    seen = {k: v for k, v in seen.items() if v >= cutoff}

    logger.info("Scraping Legacy.com (max_pages=%s)...", MAX_PAGES)
    obits = scrape_legacy_obituaries(max_pages=MAX_PAGES)
    logger.info("Scraped %d unique obituaries", len(obits))

    unseen = [o for o in obits if obit_key(o) not in seen]
    already_seen = len(obits) - len(unseen)
    skipped_md = sum(1 for o in unseen if not is_maryland(o))
    fresh = [o for o in unseen if is_maryland(o)][:MAX_NEW_PER_RUN]
    logger.info("%d new to check (%d already seen, %d skipped as out-of-state)",
                len(fresh), already_seen, skipped_md)

    new_leads = []
    props_total = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process_obituary, o): o for o in fresh}
        done = 0
        for fut in as_completed(futs):
            done += 1
            o = futs[fut]
            try:
                key, obit, props = fut.result()
            except Exception as e:
                logger.error("failed %s: %s", o.get("full_name"), e)
                continue
            seen[key] = _now()
            if done % 25 == 0:
                logger.info("  ...%d/%d checked, %d leads so far", done, len(fresh), len(new_leads))
            if not props:
                continue
            lid = lead_id(key)
            if lid in existing_ids:
                continue
            props_total += len(props)
            lead = {
                "id": lid,
                "full_name": obit.get("full_name", ""),
                "first_name": obit.get("first_name", ""),
                "last_name": obit.get("last_name", ""),
                "middle_name": obit.get("middle_name", ""),
                "date_of_death": normalize_date(obit.get("date_of_death")),
                "date_of_birth": normalize_date(obit.get("date_of_birth")),
                "age": obit.get("age"),
                "city": obit.get("city", ""),
                "state": obit.get("state") or "MD",
                "obituary_url": obit.get("obituary_url", ""),
                "obituary_text": (obit.get("obituary_text") or "")[:2000],
                "survived_by": obit.get("survived_by", ""),
                "source": obit.get("source", ""),
                "found_at": _now(),
                "properties": props,
            }
            new_leads.append(lead)
            existing_ids.add(lid)
            logger.info("LEAD: %s — %d propert%s (%s)", lead["full_name"], len(props),
                        "y" if len(props) == 1 else "ies", props[0].get("county", "?"))

    store["leads"] = new_leads + store["leads"]
    store["leads"].sort(key=lambda l: l.get("found_at", ""), reverse=True)
    store["generated_at"] = _now()
    run = {
        "started_at": started,
        "completed_at": _now(),
        "obituaries_scraped": len(obits),
        "obituaries_checked": len(fresh),
        "properties_matched": props_total,
        "leads_created": len(new_leads),
    }
    store["runs"] = ([run] + store.get("runs", []))[:60]
    store["last_run"] = run

    _save(LEADS_PATH, store)
    _save(SEEN_PATH, seen)
    logger.info("Done: %d new leads, %d total. Wrote %s", len(new_leads), len(store["leads"]), LEADS_PATH)


if __name__ == "__main__":
    main()
