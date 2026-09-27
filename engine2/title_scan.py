"""
Scan the SDAT index for unresolved-title candidates and write dashboard shards.

  python -m engine2.title_scan            # uses .cache/sdat.sqlite
  python -m engine2.title_scan --index other.sqlite --limit 200000

Outputs (committed):
  data/title/summary.json          counts, code distributions, run info
  data/title/top.json              statewide top rows by research priority
  data/title/<county_slug>.json    top rows per county
  data/title/people.json           person_key -> accounts (one person, many parcels)
Also backfills lat/lon into data/leads.json properties by account number so the
obituary dashboard can show the same aerial panel.
"""

import argparse
import heapq
import json
import logging
import os
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone

from engine2 import config
from engine2.build_index import ALL_ALIASES
from engine2.score import score, title_class
from engine2.signals import analyze

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("title_scan")

LEADS_PATH = "data/leads.json"


def slug(county):
    return re.sub(r"[^a-z0-9]+", "-", (county or "unknown").lower()).strip("-")


def _now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _dump(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def code_distributions(db):
    out = {}
    for col in ("occupancy", "homestead", "condition"):
        rows = db.execute(f"SELECT {col}, COUNT(*) FROM parcels GROUP BY {col} ORDER BY 2 DESC LIMIT 25").fetchall()
        out[col] = [[k if k is not None else "", n] for k, n in rows]
    return out


def _previous_found_at():
    """account -> found_at from the last run's shards, so timestamps mean 'first seen'."""
    seen = {}
    if not os.path.isdir(config.OUTPUT_DIR):
        return seen
    for name in os.listdir(config.OUTPUT_DIR):
        if not name.endswith(".json") or name in ("summary.json", "people.json"):
            continue
        try:
            with open(os.path.join(config.OUTPUT_DIR, name), encoding="utf-8") as f:
                for rec in json.load(f).get("rows", []):
                    if rec.get("id") and rec.get("found_at"):
                        seen.setdefault(rec["id"], rec["found_at"])
        except (OSError, ValueError):
            continue
    return seen


def scan(index_path=config.INDEX_PATH, limit=None, backfill=True):
    previous = _previous_found_at()
    run_at = _now()
    db = sqlite3.connect(index_path)
    db.row_factory = sqlite3.Row
    meta = dict(db.execute("SELECT k, v FROM meta").fetchall())
    total_rows = int(meta.get("rows", 0) or 0)

    per_county = defaultdict(list)     # county_slug -> heap of (priority, seq, record)
    statewide = []
    people = defaultdict(list)
    flag_counts, class_counts, county_counts = Counter(), Counter(), Counter()
    scanned = kept = 0
    seq = 0

    sql = f"SELECT {', '.join(ALL_ALIASES)} FROM parcels"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur = db.execute(sql)
    while True:
        chunk = cur.fetchmany(20000)
        if not chunk:
            break
        for r in chunk:
            scanned += 1
            res = analyze(dict(r))
            if not res:
                continue
            prop, flags, facts = res
            s = score(prop, flags, facts)
            if s["priority"] < config.MIN_PRIORITY_TO_KEEP:
                continue
            code, label = title_class(flags, facts)
            rec = {
                "id": prop["account_number"],
                "lead_source": "DECEASED_PROPERTY_OWNER",
                "title_class": code,
                "title_class_label": label,
                "flags": sorted(flags),
                "scores": s,
                "property": prop,
                "found_at": previous.get(prop["account_number"], run_at),
            }
            kept += 1
            seq += 1
            for f in flags:
                flag_counts[f] += 1
            class_counts[code] += 1
            cs = slug(prop["county"])
            county_counts[prop["county"]] += 1
            for o in prop["owners"]:
                people[o["person_key"]].append(prop["account_number"])
            item = (s["priority"], seq, rec)
            h = per_county[cs]
            if len(h) < config.PER_COUNTY_LIMIT:
                heapq.heappush(h, item)
            else:
                heapq.heappushpop(h, item)
            if len(statewide) < config.STATEWIDE_TOP_LIMIT:
                heapq.heappush(statewide, item)
            else:
                heapq.heappushpop(statewide, item)
        if scanned % 200000 < 20000:
            log.info("scanned %d, kept %d", scanned, kept)

    # Cross-link: same person on multiple parcels
    multi = {k: v for k, v in people.items() if len(set(v)) > 1}

    def finalize(heap):
        rows = [rec for _, _, rec in sorted(heap, key=lambda x: -x[0])]
        for rec in rows:
            accts = multi.get(next((o["person_key"] for o in rec["property"]["owners"]), None), [])
            rec["other_parcels_same_owner"] = sorted(set(a for a in accts if a != rec["id"]))[:10]
        return rows

    counties = {}
    for cs, heap in per_county.items():
        rows = finalize(heap)
        counties[cs] = {"county": rows[0]["property"]["county"] if rows else cs, "count": len(rows),
                        "total_candidates": county_counts[rows[0]["property"]["county"]] if rows else 0}
        _dump(f"{config.OUTPUT_DIR}/{cs}.json", {"generated_at": _now(), "county": counties[cs]["county"], "rows": rows})
    _dump(f"{config.OUTPUT_DIR}/top.json", {"generated_at": _now(), "rows": finalize(statewide)})
    _dump(f"{config.OUTPUT_DIR}/people.json", {"generated_at": _now(), "multi_parcel_owners": multi})

    summary = {
        "generated_at": _now(),
        "index_built_at": meta.get("built_at"),
        "index_rows": total_rows,
        "scanned": scanned,
        "candidates": kept,
        "counties": counties,
        "flags": dict(flag_counts.most_common()),
        "classes": dict(class_counts.most_common()),
        "code_distributions": code_distributions(db),
        "presets": config.PRESETS,
        "config": {
            "min_priority": config.MIN_PRIORITY_TO_KEEP,
            "per_county_limit": config.PER_COUNTY_LIMIT,
            "stale_years": config.CANDIDATE_MIN_YEARS_SINCE_TRANSFER,
            "weights": config.PRIORITY_WEIGHTS,
        },
    }
    _dump(f"{config.OUTPUT_DIR}/summary.json", summary)
    log.info("done: scanned %d, candidates %d, counties %d", scanned, kept, len(counties))
    if backfill:
        backfill_leads_coords(db)
    db.close()
    return summary


def backfill_leads_coords(db):
    """Add lat/lon (and deed ref) to obituary-engine properties that lack them."""
    if not os.path.exists(LEADS_PATH):
        return
    with open(LEADS_PATH, encoding="utf-8") as f:
        store = json.load(f)
    changed = 0
    for lead in store.get("leads", []):
        for p in lead.get("properties", []):
            if p.get("lat") or not p.get("account_number"):
                continue
            row = db.execute("SELECT lat, lon, deed_liber, deed_folio, occupancy FROM parcels WHERE acct = ?",
                             (p["account_number"],)).fetchone()
            if row and row["lat"]:
                p["lat"], p["lon"] = row["lat"], row["lon"]
                p["deed_liber"], p["deed_folio"] = row["deed_liber"], row["deed_folio"]
                p["occupancy_code"] = row["occupancy"]
                changed += 1
    if changed:
        tmp = LEADS_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=1)
        os.replace(tmp, LEADS_PATH)
        log.info("backfilled coordinates on %d obituary-lead properties", changed)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", default=config.INDEX_PATH)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-backfill", action="store_true", help="skip writing lat/lon into data/leads.json")
    ap.add_argument("--backfill-only", action="store_true", help="only update data/leads.json coordinates")
    a = ap.parse_args()
    if a.backfill_only:
        _db = sqlite3.connect(a.index)
        _db.row_factory = sqlite3.Row
        backfill_leads_coords(_db)
        _db.close()
    else:
        scan(a.index, a.limit, backfill=not a.no_backfill)
