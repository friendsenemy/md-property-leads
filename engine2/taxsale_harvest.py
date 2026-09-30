"""
Tax-sale harvester: current year live, prior years from the Internet Archive.

  python -m engine2.taxsale_harvest                 # this year, live
  python -m engine2.taxsale_harvest --years 2021-2025   # backfill from Wayback

Covers the counties that publish results on RealAuction portals
(<county>.marylandtaxsale.com). Those pages serve publicly with a browser
User-Agent and paginate by POST. The Wayback Machine holds captures of the
results page for most of them going back to 2021 -- the URL is identical every
year, so a capture made in September 2023 is the 2023 sale. The archive can't
replay POST pagination, so a backfilled year is page 1 (50 rows) unless the
county's sale was small enough to fit. Rows are tagged partial=True so the
dashboard says so.

Counties that publish only PDFs (Anne Arundel, Prince George's, Montgomery,
Baltimore County, Howard, ...) are not touched here; whatever is already in
data/distress/ for them is preserved on merge.

Output: data/distress/taxsale-<year>.json, same schema the surplus watcher
already reads. Existing records for a year are merged, never dropped.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timezone

import requests

from engine2 import county_files

log = logging.getLogger("harvest")
OUT_DIR = os.path.join("data", "distress")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

# subdomain -> (county name as SDAT/ taxsale files spell it, SDAT county code)
REALAUCTION = {
    "allegany": ("Allegany", "01"), "carroll": ("Carroll", "07"), "cecil": ("Cecil", "08"),
    "charles": ("Charles", "09"), "dorchester": ("Dorchester", "10"), "frederick": ("Frederick", "11"),
    "garrett": ("Garrett", "12"), "harford": ("Harford", "13"), "kent": ("Kent", "15"),
    "queenannes": ("Queen Anne's", "18"), "stmarys": ("St. Mary's", "19"), "talbot": ("Talbot", "21"),
    "worcester": ("Worcester", "24"),
    # baltimorecity uses block/lot, not district/account -- rows are kept but keyed raw
    "baltimorecity": ("Baltimore City", "03"),
}
RESULTS_PATH = "/index.cfm?folder=auctionResults&mode=preview"

ROW = re.compile(
    r'<tr class="(?:highlightRow|evenRow|oddRow)"[^>]*>(.*?)</tr>', re.S)
SDAT = re.compile(r'County=(\d\d)&SearchType=ACCT&District=(\d+)&AccountNumber=([0-9A-Z]+)', re.I)
ADDR = re.compile(r"maps\?q=([^']+?)\s*'", re.I)
FACE = re.compile(r'class="faceValue"[^>]*>\s*\$?([\d,]+\.\d\d)', re.S)
BID = re.compile(r'class="winningBid"[^>]*>\s*\$?([\d,]+\.\d\d)', re.S)
SOLD_TO = re.compile(r'class="previewStatus"[^>]*>\s*(.*?)\s*</td>', re.S)
PAGES = re.compile(r"id='gotoPageNum'.*?</select>", re.S)
OPT = re.compile(r"<option value='(\d+)'")


def _money(s):
    try:
        return float(s.replace(",", ""))
    except (AttributeError, ValueError):
        return None


def parse_page(html, county, code, year, partial=False, source=""):
    out = {}
    for m in ROW.finditer(html):
        row = m.group(1)
        sd = SDAT.search(row)
        if not sd:
            continue
        cc, dist, acct = sd.group(1), sd.group(2), sd.group(3)
        if county == "Baltimore City":
            key = f"{cc}*{dist}-{acct}"            # block/lot, not yet mapped to SDAT
        else:
            key = f"{cc}{dist.zfill(2)}{acct}"
        addr = ADDR.search(row)
        face, bid = FACE.search(row), BID.search(row)
        st = SOLD_TO.search(row)
        sold_to = re.sub(r"<[^>]+>", "", st.group(1)).strip() if st else ""
        if re.fullmatch(r"\d+", sold_to):
            status, bidder = "SOLD", f"#{sold_to}"
        elif sold_to and re.search(r"struck|county|city|commissioner", sold_to, re.I):
            status, bidder = "STRUCK", sold_to
        elif bid and face and _money(bid.group(1)) == _money(face.group(1)):
            status, bidder = "STRUCK", sold_to or None    # bid at face = no competition
        elif bid:
            status, bidder = "SOLD", sold_to or None
        else:
            status, bidder = "LISTED", None
        rec = {"county": county, "status": status, "year": year, "source": source}
        if addr:
            rec["address"] = re.sub(r"\s+", " ", addr.group(1)).replace(" MD", "").strip()
        if face:
            rec["face"] = _money(face.group(1))
        if bid:
            rec["bid"] = _money(bid.group(1))
        if bidder:
            rec["bidder"] = bidder
        if partial:
            rec["partial"] = True
        out[key] = rec
    return out


# ---------------------------------------------------------------- live -----

def fetch_live(sub, county, code, year, session):
    base = f"https://{sub}.marylandtaxsale.com"
    url = base + RESULTS_PATH
    r = session.get(url, timeout=60)
    if r.status_code != 200 or "auctionResults" not in r.text:
        log.warning("%s: live page unavailable (%s)", sub, r.status_code)
        return {}
    html = r.text
    recs = parse_page(html, county, code, year, source=f"{sub}.marylandtaxsale.com live {date.today().isoformat()}")
    pages = 1
    pm = PAGES.search(html)
    if pm:
        nums = [int(x) for x in OPT.findall(pm.group(0))]
        pages = max(nums) if nums else 1
    # The portal honours pageNum as a plain query parameter, which is simpler and
    # more robust than replaying the search form's hidden fields.
    for p in range(2, pages + 1):
        time.sleep(0.8)
        rp = session.get(f"{url}&doSearch=true&orderBy=AdvNum&orderDir=asc&pageNum={p}", timeout=60)
        if rp.status_code != 200:
            log.warning("%s: page %d -> %s", sub, p, rp.status_code)
            break
        got = parse_page(rp.text, county, code, year,
                         source=f"{sub}.marylandtaxsale.com live {date.today().isoformat()}")
        if not got:
            log.warning("%s: page %d parsed empty — stopping", sub, p)
            break
        recs.update(got)
    log.info("%s %s live: %d rows over %d page(s)", county, year, len(recs), pages)
    return recs


# ------------------------------------------------------------- wayback -----

def wayback_captures(sub, session):
    """All 200-status captures of the results page, as (timestamp, length)."""
    target = f"{sub}.marylandtaxsale.com{RESULTS_PATH}"
    cdx = ("https://web.archive.org/cdx/search/cdx?url=" + requests.utils.quote(target, safe="")
           + "&output=json&fl=timestamp,length&filter=statuscode:200&limit=500")
    r = session.get(cdx, timeout=90)
    if r.status_code != 200:
        return []
    try:
        rows = r.json()[1:]
    except ValueError:
        return []
    return [(ts, int(ln or 0)) for ts, ln in rows]


def fetch_wayback(sub, county, code, year, session):
    caps = [c for c in wayback_captures(sub, session) if c[0].startswith(str(year))]
    if not caps:
        return {}
    # Sales run May-June; a capture later in the year is the fullest. Prefer the
    # largest capture, then the latest.
    caps.sort(key=lambda c: (c[1], c[0]), reverse=True)
    for ts, ln in caps[:3]:
        raw = f"https://web.archive.org/web/{ts}id_/https://{sub}.marylandtaxsale.com{RESULTS_PATH}"
        try:
            r = session.get(raw, timeout=120)
        except requests.RequestException as e:
            log.warning("%s %s: wayback fetch failed (%s)", sub, ts, e)
            continue
        if r.status_code != 200 or "faceValue" not in r.text:
            continue
        pages = 1
        pm = PAGES.search(r.text)
        if pm:
            nums = [int(x) for x in OPT.findall(pm.group(0))]
            pages = max(nums) if nums else 1
        recs = parse_page(r.text, county, code, year, partial=pages > 1,
                          source=f"web.archive.org capture {ts[:8]} of {sub}.marylandtaxsale.com"
                                 + (f" (page 1 of {pages})" if pages > 1 else ""))
        log.info("%s %s wayback %s: %d rows%s", county, year, ts[:8], len(recs),
                 f" (page 1 of {pages} — partial)" if pages > 1 else "")
        time.sleep(1.5)
        return recs
    return {}


# --------------------------------------------------------- county files -----

def _fetch_file(url, session, year, via_wayback=False):
    """Return bytes for a live URL, or the Wayback capture nearest to `year`."""
    target = f"https://web.archive.org/web/{year}id_/{url}" if via_wayback else url
    r = session.get(target, timeout=120, allow_redirects=True)
    if r.status_code != 200 or len(r.content) < 2000:
        return None
    if via_wayback:
        # Wayback redirects to the nearest capture; make sure it is from the year asked for
        m = re.search(r"/web/(\d{4})\d+id_/", r.url)
        if not m or m.group(1) != str(year):
            return None
    ctype = (r.headers.get("content-type") or "").lower()
    if "html" in ctype and not r.content[:5].startswith(b"%PDF"):
        return None
    return r.content


def fetch_county_file(county, year, session):
    parser, urls = county_files.urls_for(county, year)
    for via_wayback in (False, True):
        for url in urls:
            try:
                data = _fetch_file(url, session, year, via_wayback)
            except requests.RequestException as e:
                log.debug("%s %s: %s", county, url, e)
                data = None
            if not data:
                continue
            src = ("web.archive.org capture of " if via_wayback else "") + url
            try:
                recs = parser(data, year, src)
            except Exception as e:      # a malformed PDF should not kill the run
                log.warning("%s %s: parse failed on %s (%s)", county, year, url, e)
                continue
            if recs:
                log.info("%s %s %s: %d rows", county, year, "wayback" if via_wayback else "live", len(recs))
                time.sleep(1.0 if via_wayback else 0.3)
                return recs
    return {}


# --------------------------------------------------------------- merge -----

def load_year(year):
    p = os.path.join(OUT_DIR, f"taxsale-{year}.json")
    if not os.path.exists(p):
        return {"generated": None, "records": {}}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def save_year(year, blob, new_by_county):
    blob["generated"] = date.today().isoformat()
    blob.setdefault("note", "Maryland tax-sale records keyed by SDAT account. status: SOLD=lien sold to investor; "
                            "STRUCK=unsold, county holds lien; LISTED=advertised, outcome unknown. "
                            "partial=true means the row came from an Internet Archive capture of page 1 only.")
    blob["harvest"] = {**blob.get("harvest", {}), **new_by_county}
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, f"taxsale-{year}.json"), "w", encoding="utf-8") as f:
        json.dump(blob, f, ensure_ascii=False, indent=1, sort_keys=False)


def merge(existing, new, county):
    """Live rows replace wayback partials for the same county; other counties untouched."""
    recs = existing.setdefault("records", {})
    incoming_full = any(not r.get("partial") for r in new.values())
    if incoming_full:
        for k in [k for k, r in recs.items() if r.get("county") == county and r.get("partial")]:
            del recs[k]
    for k, r in new.items():
        if k in recs and not recs[k].get("partial") and r.get("partial"):
            continue                                  # never let a partial overwrite a full row
        recs[k] = r
    return existing


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default=str(date.today().year),
                    help="e.g. 2026 or 2021-2025. Current year is fetched live; others from the Wayback Machine.")
    ap.add_argument("--counties", default="", help="comma-separated subdomains to limit to")
    a = ap.parse_args()
    if "-" in a.years:
        lo, hi = a.years.split("-")
        years = list(range(int(lo), int(hi) + 1))
    else:
        years = [int(y) for y in a.years.split(",")]
    subs = [s for s in a.counties.split(",") if s] or list(REALAUCTION)
    session = requests.Session()
    session.headers["User-Agent"] = UA
    this_year = date.today().year
    for year in years:
        blob = load_year(year)
        done = {}
        for sub in subs:
            county, code = REALAUCTION[sub]
            try:
                recs = fetch_live(sub, county, code, year, session) if year == this_year \
                    else fetch_wayback(sub, county, code, year, session)
            except requests.RequestException as e:
                log.warning("%s %s: %s", sub, year, e)
                recs = {}
            if recs:
                blob = merge(blob, recs, county)
                done[county] = {"rows": len(recs), "partial": any(r.get("partial") for r in recs.values()),
                                "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        # counties that publish a results file on their own site (full lists, any year they keep up)
        if not a.counties:
            for county in county_files.COUNTY_FILES:
                recs = fetch_county_file(county, year, session)
                if recs:
                    blob = merge(blob, recs, county)
                    done[county] = {"rows": len(recs), "partial": False,
                                    "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        if done:
            save_year(year, blob, done)
            log.info("== %s: %d records on file (%d counties refreshed this run)", year, len(blob["records"]), len(done))
        else:
            log.info("== %s: nothing found", year)


if __name__ == "__main__":
    main()
