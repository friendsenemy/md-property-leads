"""
Repeat Buyers — who is buying Maryland property far below assessed value, again and again.

Pulls every parcel statewide whose last transfer is on or after SINCE from the licensed
SDAT dataset (owner name + mailing address), keeps the deals that look like a play:

  * cheap buys  — price under CHEAP_RATIO of today's assessment
  * flips       — the seller had bought it within FLIP_YEARS for much less (segment 2)

and writes a slim, gzipped file the analysis (and the dashboard later) reads:
  data/players/sales.json.gz   — the kept rows
  data/players/summary.json    — run stats

  python -m engine2.repeat_buyers                 # licensed if secrets set, else public
  python -m engine2.repeat_buyers --public
"""

import argparse
import gzip
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone

import requests

from engine2 import config
from engine2.build_index import DYNAMIC_PATTERNS, _auth, _get

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("repeat_buyers")

PUBLIC = "ed4q-f8tm"
SINCE = "2022.01.01"
CHEAP_RATIO = 0.6
FLIP_YEARS = 4
OUT_DIR = "data/players"

# alias -> regex on Socrata field names (same SDAT field ids in both datasets)
PATTERNS = {
    "acct": r"field_acctid$", "county": r"cntyname", "ooi": r"field_ooi", "addr": r"mdp_street_address_mdp_field_address$",
    "city": r"mdp_street_address_city", "legal": r"field_legal1", "lu": r"lu_desclu", "rtype": r"field_resityp",
    "g1": r"grntnam1", "conv1": r"convey1", "d1": r"field_tradate", "p1": r"considr1", "m1": r"mortgag1",
    "g2": r"sdat_field_100$", "conv2": r"sdat_field_107$", "d2": r"sdat_field_109$", "p2": r"sdat_field_110$",
    "g3": r"sdat_field_120$", "dl": r"field_dr1liber", "df": r"field_dr1folio",
    "imp": r"sdat_field_165$", "tot": r"sdat_field_172$", "yb": r"field_yearblt", "area": r"field_landarea",
    "uom": r"field_luom", "sqft": r"field_sqftstrc",
}


def columns(dataset, session):
    keys = []
    try:
        r = session.get(f"https://opendata.maryland.gov/api/views/{dataset}.json", auth=_auth(), timeout=60)
        if r.status_code == 200:
            keys = [c["fieldName"] for c in r.json().get("columns", [])]
    except requests.RequestException:
        pass
    if not keys:
        sample = _get(f"https://opendata.maryland.gov/resource/{dataset}.json", {"$limit": 200}, session)
        keys = sorted({k for rec in sample for k in rec})
    m = {}
    for a, rx in PATTERNS.items():
        m[a] = next((k for k in keys if re.search(rx, k)), None)
    for a, rx in DYNAMIC_PATTERNS.items():
        m[a] = next((k for k in keys if rx.search(k)), None)
    log.info("columns: %s", {a: bool(c) for a, c in m.items()})
    return {a: c for a, c in m.items() if c}


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def year(d):
    m = re.match(r"(\d{4})", d or "")
    return int(m.group(1)) if m else 0


def keep(r):
    p1, tot = num(r.get("p1")), num(r.get("tot"))
    if tot >= 10000 and 0 < p1 < CHEAP_RATIO * tot:
        return "cheap"
    p2 = num(r.get("p2"))
    y1, y2 = year(r.get("d1")), year(r.get("d2"))
    if p2 > 0 and p1 >= 1.5 * p2 and y2 and 0 <= y1 - y2 <= FLIP_YEARS:
        return "flip"
    return None


def run(public=False):
    session = requests.Session()
    dataset = config.DATASET_ID if (_auth() and not public) else PUBLIC
    cols = columns(dataset, session)
    url = f"https://opendata.maryland.gov/resource/{dataset}.json"
    rev = {v: k for k, v in cols.items()}
    where = f"{cols['d1']} >= '{SINCE}'"
    kept, seen, off = [], 0, 0
    while True:
        rows = _get(url, {"$select": ",".join(cols.values()), "$where": where, "$order": cols["acct"],
                          "$limit": config.PAGE_SIZE, "$offset": off}, session)
        for raw in rows:
            r = {rev[k]: v for k, v in raw.items() if k in rev}
            why = keep(r)
            if why:
                r["why"] = why
                kept.append(r)
        seen += len(rows)
        log.info("  %d scanned, %d kept", seen, len(kept))
        if len(rows) < config.PAGE_SIZE:
            break
        off += len(rows)
    os.makedirs(OUT_DIR, exist_ok=True)
    with gzip.open(f"{OUT_DIR}/sales.json.gz", "wt", encoding="utf-8") as f:
        json.dump(kept, f, separators=(",", ":"))
    summary = {"generated_at": datetime.now(timezone.utc).isoformat(), "dataset": "public" if dataset == PUBLIC else "licensed",
               "since": SINCE, "scanned": seen, "kept": len(kept), "owner_fields": [a for a in ("owner1", "mail_addr") if a in cols]}
    json.dump(summary, open(f"{OUT_DIR}/summary.json", "w"), indent=1)
    log.info("done: %s", summary)
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--public", action="store_true")
    a = ap.parse_args()
    try:
        run(a.public)
    except Exception as e:  # noqa: BLE001
        log.exception("repeat buyers failed: %s", e)
        sys.exit(1)
