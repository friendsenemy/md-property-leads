"""
Dirt Deals — flag raw land with development potential (Cody Bjugan's "LDP" play).

The play: find off-market acreage a builder would want once it is approved for
houses, tie it up with a long due-diligence contract, then assign it (or partner
on entitlement) to a homebuilder. This engine does the first screen statewide,
for free, from public data:

  1. SDAT parcels of MIN_ACRES+ (residential / agricultural use only)
  2. Maryland's Septic Growth Tier map (MDP)  — Tier I sewer now, Tier II sewer
     planned (the big one: growth is coming and the price doesn't show it yet),
     Tier III large-lot septic, Tier IV preservation (dropped)
  3. MDP Generalized Zoning 2025               — how dense the county lets it go
  4. New homes built nearby in the last few years  (builders are active here)
  5. Builder-owned parcels bought nearby        (licensed SDAT only: owner names)
  6. Seller-side signals Ray already tracks: long-held, out-of-state owner,
     dead owner / estate from the Title Scan output

  python -m engine2.land_watch                  # licensed SDAT if secrets are set, else public
  python -m engine2.land_watch --public         # force the public SDAT dataset (no owner names)
  python -m engine2.land_watch --county 08      # one county (SDAT county code), for a quick test

Writes data/land/candidates.json for the "Dirt Deals" dashboard tab.
"""

import argparse
import glob
import json
import logging
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

import requests

from engine2 import config
from engine2.build_index import DYNAMIC_PATTERNS, _auth, _get

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("land_watch")

L = config.LAND
PUBLIC_DATASET = "ed4q-f8tm"
MDP = "https://mdpgis.mdp.state.md.us/arcgis/rest/services/PlanningCadastre"
TIER_URL = f"{MDP}/Septic_Growth_Tiers/MapServer/0/query"
ZONING_URL = f"{MDP}/Generalized_Zoning_2025/MapServer/0/query"
OUT = "data/land/candidates.json"

COLS = {
    "acct":        "account_id_mdp_field_acctid",
    "county_code": "record_key_county_code_sdat_field_1",
    "county":      "county_name_mdp_field_cntyname",
    "address":     "mdp_street_address_mdp_field_address",
    "city":        "premise_address_city_mdp_field_premcity_sdat_field_25",
    "zip":         "premise_address_zip_code_mdp_field_premzip_sdat_field_26",
    "lat":         "mdp_latitude_mdp_field_digycord_converted_to_wgs84",
    "lon":         "mdp_longitude_mdp_field_digxcord_converted_to_wgs84",
    "land_use":    "land_use_code_mdp_field_lu_desclu_sdat_field_50",
    "zoning":      "zoning_code_mdp_field_zoning_sdat_field_45",
    "water":       "property_factors_utilities_water_mdp_field_pfuw_sdat_field_63",
    "sewer":       "property_factors_utilities_sewer_mdp_field_pfus_sdat_field_64",
    "waterfront":  "property_factors_location_waterfront_mdp_field_pflw_sdat_field_65",
    "critical":    "special_dates_and_data_critical_area_code_mdp_field_crtarcod_sdat_field_223",
    "ag_pres":     "special_dates_and_data_agr_land_preservation_fdn_area_mdp_field_agfndarea_sdat_field_213",
    "non_perc":    "special_dates_and_data_non_perc_area_mdp_field_nprcarea_sdat_field_219",
    "rezoned":     "special_dates_and_data_zoning_change_date_yyyy_mm_dd_mdp_field_znchgdat_sdat_field_224",
    "land_area":   "c_a_m_a_system_data_land_area_mdp_field_landarea_sdat_field_242",
    "land_unit":   "c_a_m_a_system_data_land_unit_of_measure_mdp_field_luom_sdat_field_243",
    "land_value":  "current_cycle_data_land_value_mdp_field_names_nfmlndvl_curlndvl_and_sallndvl_sdat_field_164",
    "impr_value":  "current_cycle_data_improvements_value_mdp_field_names_nfmimpvl_curimpvl_and_salimpvl_sdat_field_165",
    "year_built":  "c_a_m_a_system_data_year_built_yyyy_mdp_field_yearblt_sdat_field_235",
    "transfer_date": "sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89",
    "sale_price":  "sales_segment_1_consideration_mdp_field_considr1_sdat_field_90",
    "tax_map":     "map_mdp_field_map_sdat_field_42",
    "grid":        "grid_mdp_field_grid_sdat_field_43",
    "parcel":      "parcel_mdp_field_parcel_sdat_field_44",
    "subdivision": "subdivision_code_mdp_field_subdivsn_sdat_field_37",
    "lot":         "lot_mdp_field_lot_sdat_field_41",
    "block":       "block_mdp_field_block_sdat_field_40",
    "legal1":      "legal_description_line_1_mdp_field_legal1_sdat_field_17",
}

KEEP_LAND_USE = ("(R)", "(A)", "(RC)", "(CR)")     # residential, agricultural, mixed res/comm


def _num(v):
    try:
        return float(v) if v not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── SDAT ────────────────────────────────────────────────────────────────────

def owner_columns(dataset, session):
    """Owner / mailing column names exist only in the licensed dataset and their
    exact names are discovered at runtime (same patterns build_index uses)."""
    keys = []
    try:
        r = session.get(f"https://opendata.maryland.gov/api/views/{dataset}.json", auth=_auth(), timeout=60)
        if r.status_code == 200:
            keys = [c["fieldName"] for c in r.json().get("columns", [])]
    except requests.RequestException:
        pass
    found = {a: next((k for k in keys if rx.search(k)), None) for a, rx in DYNAMIC_PATTERNS.items()}
    log.info("owner columns: %s", {a: bool(c) for a, c in found.items()})
    return {a: c for a, c in found.items() if c}


def _page_all(url, select, where, session, order):
    out, offset = [], 0
    while True:
        rows = _get(url, {"$select": select, "$where": where, "$order": order,
                          "$limit": config.PAGE_SIZE, "$offset": offset}, session)
        out.extend(rows)
        log.info("  +%d rows (%d)", len(rows), len(out))
        if len(rows) < config.PAGE_SIZE:
            return out
        offset += len(rows)


def fetch_parcels(dataset, cols, session, county=None):
    area, unit = COLS["land_area"], COLS["land_unit"]
    where = (f"(({unit}='A' AND {area} >= {L['MIN_ACRES']}) OR "
             f"({unit}='S' AND {area} >= {L['MIN_ACRES'] * 43560}))")
    if county:
        where += f" AND {COLS['county_code']}='{county}'"
    select = ",".join(cols.values())
    log.info("parcels of %s+ acres%s", L["MIN_ACRES"], f" in county {county}" if county else " statewide")
    rows = _page_all(f"{config.SOCRATA_BASE}/{dataset}.json", select, where, session, COLS["acct"])
    inv = {v: k for k, v in cols.items()}
    return [{inv[k]: v for k, v in r.items() if k in inv} for r in rows]


def fetch_new_homes(dataset, session, county=None):
    """Lat/lon of every dwelling built in the last NEW_HOME_YEARS years: where
    builders are putting up houses right now."""
    y0 = date.today().year - L["NEW_HOME_YEARS"]
    where = f"{COLS['year_built']} >= '{y0}' AND {COLS['year_built']} <= '{date.today().year + 1}' AND {COLS['lat']} > 37"
    if county:
        where += f" AND {COLS['county_code']}='{county}'"
    log.info("new homes built %s+", y0)
    rows = _page_all(f"{config.SOCRATA_BASE}/{dataset}.json", f"{COLS['lat']},{COLS['lon']}", where, session, COLS["acct"])
    return [(_num(r.get(COLS["lat"])), _num(r.get(COLS["lon"]))) for r in rows if r.get(COLS["lat"])]


def fetch_builder_buys(dataset, owner_col, session, county=None):
    """Parcels a known homebuilder bought in the last BUILDER_YEARS years."""
    y0 = date.today().year - L["BUILDER_YEARS"]
    likes = " OR ".join(f"upper({owner_col}) like '%{n}%'" for n in L["BUILDERS"])
    where = f"({likes}) AND {COLS['transfer_date']} >= '{y0}.01.01' AND {COLS['lat']} > 37"
    if county:
        where += f" AND {COLS['county_code']}='{county}'"
    log.info("builder purchases since %s", y0)
    rows = _page_all(f"{config.SOCRATA_BASE}/{dataset}.json",
                     f"{COLS['lat']},{COLS['lon']},{owner_col},{COLS['transfer_date']}", where, session, COLS["acct"])
    out = []
    for r in rows:
        name = (r.get(owner_col) or "").upper()
        who = next((n for n in L["BUILDERS"] if n in name), name[:30])
        out.append((_num(r.get(COLS["lat"])), _num(r.get(COLS["lon"])), L["BUILDER_LABELS"].get(who, who.title())))
    return out


# ── Planning layers (MDP ArcGIS, free) ─────────────────────────────────────

def fetch_layer(url, fields, session):
    """Every polygon of an MDP layer in WGS84, lightly generalized, as
    (attributes, shapely geometry)."""
    from shapely.geometry import Polygon, MultiPolygon, Point
    from shapely.validation import make_valid
    feats, offset = [], 0
    while True:
        for attempt in range(1, config.MAX_RETRIES + 1):
            try:
                r = session.get(url, params={"where": "1=1", "outFields": fields, "outSR": 4326,
                                             "maxAllowableOffset": 0.0002, "orderByFields": "OBJECTID",
                                             "resultOffset": offset, "resultRecordCount": 2000, "f": "json"}, timeout=180)
                j = r.json()
                if "features" in j:
                    break
                raise ValueError(str(j)[:200])
            except (requests.RequestException, ValueError) as e:
                log.warning("layer page %d failed (attempt %d): %s", offset, attempt, e)
                time.sleep(5 * attempt)
        else:
            raise RuntimeError(f"gave up on {url}")
        for f in j["features"]:
            rings = (f.get("geometry") or {}).get("rings") or []
            shells, holes = [], []
            for ring in rings:
                if len(ring) < 4:
                    continue
                p = Polygon(ring)
                (holes if p.exterior.is_ccw else shells).append(p)   # Esri: shells clockwise, holes counter-clockwise
            if not shells:
                continue
            polys = []
            for s in shells:
                hs = [h.exterior.coords for h in holes if s.contains(Point(h.exterior.coords[0]))]
                polys.append(Polygon(s.exterior.coords, hs))
            g = polys[0] if len(polys) == 1 else MultiPolygon(polys)
            if not g.is_valid:
                g = make_valid(g)
            feats.append((f["attributes"], g))
        log.info("  %s: %d polygons", url.split("/")[-4], len(feats))
        if not j.get("exceededTransferLimit"):
            return feats
        offset += len(j["features"])


def overlay(points, feats, key):
    """For each (lat, lon), the attribute `key` of the polygon it falls in (or '')."""
    import shapely
    from shapely.strtree import STRtree
    geoms = [g for _, g in feats]
    tree = STRtree(geoms)
    pts = shapely.points([(lon, lat) for lat, lon in points])
    out = [""] * len(points)
    pi, gi = tree.query(pts, predicate="intersects")
    for p, g in zip(pi, gi):
        if not out[p]:
            out[p] = (feats[g][0].get(key) or "").strip()
    return out


# ── Nearby activity (grid buckets, no extra deps) ───────────────────────────

class Grid:
    CELL = 0.01   # degrees (~0.7 mi)

    def __init__(self, pts):
        self.cells = defaultdict(list)
        for p in pts:
            self.cells[(int(p[0] / self.CELL), int(p[1] / self.CELL))].append(p)

    def near(self, lat, lon, miles):
        r = int(miles / 0.69) + 1
        cy, cx = int(lat / self.CELL), int(lon / self.CELL)
        coslat = math.cos(math.radians(lat))
        out = []
        for dy in range(-r, r + 1):
            for dx in range(-r, r + 1):
                for p in self.cells.get((cy + dy, cx + dx), ()):
                    d = 69.0 * math.hypot(p[0] - lat, (p[1] - lon) * coslat)
                    if d <= miles:
                        out.append(p)
        return out


# ── Title Scan cross-reference (dead owner / estate / tax sale) ─────────────

def load_title_flags():
    out = {}
    for f in glob.glob("data/title/*.json"):
        if os.path.basename(f) in ("summary.json", "people.json"):
            continue
        try:
            d = json.load(open(f, encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for r in d.get("rows", []) if isinstance(d, dict) else []:
            acct = (r.get("property") or {}).get("account_number") or r.get("id")
            if acct and acct not in out:
                out[acct] = {"flags": r.get("flags") or [], "class": r.get("title_class_label") or ""}
    log.info("title-scan cross-reference: %d parcels", len(out))
    return out


# ── Distress stack (filters on the Dirt Deals tab) ──────────────────────────
# Each kept parcel gets r["distress"] (codes) and r["distress_detail"] (one readable
# line per code) from the same files the other tabs use: Title Scan (dead owner /
# estate), the county tax-sale lists, and distress_watch's signals (code cases,
# foreclosure registry, vacant notices, zero water use). Not scored — shown and
# filterable, so the land score stays a development score.

DISTRESS_LABEL = {
    "DEATH": "Owner died / estate", "TAX_SALE": "Tax sale", "STRUCK": "Unsold tax lien (county holds it)",
    "CODE": "Code / nuisance case", "FORECLOSURE": "Foreclosure registry", "VACANT": "Vacant / no water use",
}
_DEATH_WORDS = ("DECEASED", "ESTATE", "HEIRS", "PROBATE")
_SIGNAL_MAP = {
    "CODE_VIOLATIONS": "CODE", "REPEAT_CODE_CASES": "CODE", "NUISANCE_CASE": "CODE", "RECEIVERSHIP": "CODE",
    "DEMO_PERMIT": "CODE", "FORECLOSURE_REGISTRY": "FORECLOSURE",
    "VACANT_NOTICE": "VACANT", "ZERO_WATER_USAGE": "VACANT",
}
TAX_SALE_SINCE = 2024   # older certificates are almost always redeemed or foreclosed by now


def _k(a):
    return str(a or "").strip().lstrip("0")


def load_distress_sources():
    tax = defaultdict(list)
    for f in sorted(glob.glob("data/distress/taxsale-20*.json")):
        try:
            recs = json.load(open(f, encoding="utf-8")).get("records") or {}
        except (OSError, ValueError):
            continue
        for a, r in recs.items():
            if (r.get("year") or 0) >= TAX_SALE_SINCE:
                tax[_k(a)].append(r)
    sig = {}
    try:
        for a, v in json.load(open("data/distress/signals.json", encoding="utf-8")).get("by_account", {}).items():
            sig[_k(a)] = [s for s in v if s.get("precision") != "block"]   # block-level fires are never a confirmed match
    except (OSError, ValueError):
        pass
    title = {}
    for f in glob.glob("data/title/*.json"):
        if os.path.basename(f) in ("summary.json", "people.json"):
            continue
        try:
            d = json.load(open(f, encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for r in d.get("rows", []) if isinstance(d, dict) else []:
            a = (r.get("property") or {}).get("account_number") or r.get("id")
            if a:
                title.setdefault(_k(a), r)
    log.info("distress sources: %d tax-sale parcels, %d signal parcels, %d title rows", len(tax), len(sig), len(title))
    return tax, sig, title


def attach_distress(rows, sources=None):
    tax, sig, title = sources or load_distress_sources()
    for r in rows:
        a, codes, detail = _k(r.get("account")), [], {}
        t = title.get(a)
        flags = set(r.get("title_flags") or []) | set((t or {}).get("flags") or [])
        death = sorted(f for f in flags if any(w in f for w in _DEATH_WORDS) or f == "LIFE_ESTATE")
        cls = (t or {}).get("title_class") or ""
        if death or cls[:1] in ("D", "E", "H"):
            codes.append("DEATH")
            lbl = (t or {}).get("title_class_label") or ", ".join(death)
            detail["DEATH"] = f"{cls + ' · ' if cls else ''}{lbl}" + (" — verify (common name)" if cls.endswith("?") else "")
        trs = sorted(tax.get(a, []), key=lambda x: x.get("year") or 0)
        if trs:
            def line(x):
                s = f"{x.get('year')} {str(x.get('status') or '').replace('_', ' ').lower()}"
                if x.get("face"):
                    s += f" · taxes ${float(x['face']):,.0f}"
                if x.get("bid") and x.get("status") == "SOLD":
                    s += f" · bid ${float(x['bid']):,.0f}" + (f" ({x['bidder']})" if x.get("bidder") else "")
                return s
            unsold = [x for x in trs if x.get("status") in ("STRUCK", "LISTED_NOT_SOLD")]
            other = [x for x in trs if x not in unsold]
            if other:
                codes.append("TAX_SALE"); detail["TAX_SALE"] = "; ".join(line(x) for x in other)
            if unsold:
                codes.append("STRUCK"); detail["STRUCK"] = "; ".join(line(x) for x in unsold)
        kinds = defaultdict(set)
        for s in sig.get(a, []):
            c = _SIGNAL_MAP.get(s.get("kind"))
            if c:
                kinds[c].add(f"{s.get('kind', '').replace('_', ' ').lower()}{' ' + s['date'] if s.get('date') else ''} ({s.get('source', '')})")
        for c in ("CODE", "FORECLOSURE", "VACANT"):
            if kinds.get(c):
                codes.append(c); detail[c] = "; ".join(sorted(kinds[c])[:4])
        r["distress"] = codes
        r["distress_detail"] = [f"{DISTRESS_LABEL[c]}: {detail[c]}" for c in codes]
    return rows


def refresh_distress():
    """Re-attach the distress stack to the existing candidates.json (daily, after distress_watch)."""
    d = json.load(open(OUT, encoding="utf-8"))
    attach_distress(d.get("rows", []))
    d["distress_at"] = _now()
    d["by_distress"] = dict(Counter(c for r in d.get("rows", []) for c in r.get("distress", [])))
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(d, f, separators=(",", ":"))
    log.info("distress refreshed: %s", d["by_distress"])
    return d


# ── Scoring ─────────────────────────────────────────────────────────────────

def tier_group(t):
    t = (t or "").upper()
    if t.startswith("TIER 1") or t == "MUNICIPAL AREA":
        return "1"
    if t.startswith("TIER 2"):
        return "2"
    if t.startswith("TIER 3"):
        return "3"
    if t.startswith("TIER 4"):
        return "4"
    return ""


def years_since(d):
    m = re.match(r"(\d{4})", d or "")
    if not m or int(m.group(1)) < 1800:      # SDAT writes 0000.00.00 when it has no date
        return None
    return date.today().year - int(m.group(1))


def evaluate(p, title):
    """Score one parcel. Returns (score, reasons, tags) or None when it is out."""
    reasons, tags = [], []
    acres = _num(p.get("land_area")) / (43560.0 if p.get("land_unit") == "S" else 1.0)
    p["acres"] = round(acres, 2)
    tg = tier_group(p.get("tier"))
    sewer = "Public" in (p.get("sewer") or "")
    water = "Public" in (p.get("water") or "")
    gz = (p.get("gen_zone") or "").upper()
    owner = (p.get("owner1") or "").upper()

    # hard stops
    if not any(c in (p.get("land_use") or "") for c in KEEP_LAND_USE):
        return None, "land use"
    if tg == "4":
        return None, "Tier IV (preservation)"
    if _num(p.get("ag_pres")) > 0:
        return None, "ag preservation easement"
    if owner and any(m in " " + owner for m in L["SKIP_OWNERS"]):
        return None, "builder / government / nonprofit owner"
    if any(m in gz for m in ("INDUSTRIAL", "COMMERCIAL")) and not tg:
        return None, "commercial / industrial zoning"
    legal = f"{p.get('lot') or ''} {p.get('legal1') or ''}".upper()
    if (p.get("lot") or "").strip().upper() in ("OS", "O/S") or any(w in legal for w in L["SKIP_LEGAL"]):
        return None, "open space / stormwater / common area"

    s = 0
    # where the county says growth goes
    pts = L["TIER_POINTS"].get(tg, 0)
    if not tg:
        pts = L["TIER_POINTS"]["sewer_no_tier"] if sewer else L["TIER_POINTS"]["none"]
    label = {"1": "Tier I — public sewer area", "2": "Tier II — sewer PLANNED (growth coming)",
             "3": "Tier III — large-lot septic only"}.get(tg, "No tier map here" + (" — SDAT shows public sewer" if sewer else ""))
    if tg in ("1", "2") and any(k in gz for k in ("RURAL LOW", "VERY LOW", "RURAL MEDIUM")):
        # sewer on the map but zoning keeps it rural: only pays after a rezoning
        pts = L["TIER_RURAL_ZONED_POINTS"]
        label += ", but zoned rural — needs a rezoning to pay off"
        tags.append("NEEDS_REZONING")
    s += pts
    reasons.append(f"{label} (+{pts})")
    if sewer and tg in ("2", "3"):
        s += 5; reasons.append("SDAT already shows public sewer (+5)")
    if water:
        s += 3; reasons.append("Public water (+3)")

    # zoning density
    zp = next((v for k, v in L["ZONE_POINTS"] if k in gz), 0)
    s += zp
    if gz:
        reasons.append(f"Zoned {gz.title()} (+{zp})")
    zc = (p.get("zoning") or "").strip().upper()
    if zp >= 15 and L["RURAL_ZONE_CODES"].match(zc):
        s -= 10
        reasons.append(f"County zoning code on SDAT ({zc}) looks rural but the state map says {gz.title()} — confirm on the county map (-10)")
        tags.append("ZONING_MISMATCH")
    if years_since(p.get("rezoned")) is not None and years_since(p.get("rezoned")) <= L["REZONED_YEARS"]:
        s += 5; reasons.append(f"Rezoned {p.get('rezoned')} (+5)"); tags.append("RECENT_REZONING")

    # size
    sp = next((v for lo, v in L["SIZE_POINTS"] if acres >= lo), 0)
    s += sp; reasons.append(f"{acres:,.1f} acres (+{sp})")

    # raw / underbuilt
    lv, iv = _num(p.get("land_value")), _num(p.get("impr_value"))
    if iv == 0:
        s += 10; reasons.append("No buildings on it (+10)"); tags.append("RAW_LAND")
    elif iv < 0.25 * (lv + iv):
        s += 6; reasons.append("Buildings are a small part of the value — the dirt is the asset (+6)")

    # builders active nearby
    n = p.get("new_homes_nearby", 0)
    np_ = next((v for lo, v in L["NEW_HOME_POINTS"] if n >= lo), 0)
    if np_:
        s += np_; reasons.append(f"{n} new homes built within {L['RADIUS_MILES']} mi since {date.today().year - L['NEW_HOME_YEARS']} (+{np_})")
        tags.append("NEW_HOMES_NEARBY")
    if p.get("builders_nearby"):
        s += L["BUILDER_POINTS"]
        reasons.append(f"Builders bought land within {L['BUILDER_RADIUS_MILES']} mi: {', '.join(p['builders_nearby'][:4])} (+{L['BUILDER_POINTS']})")
        tags.append("BUILDER_BUYING_NEARBY")

    dev = s   # development-side score before seller signals

    # seller side
    yrs = years_since(p.get("transfer_date"))
    if yrs is not None and yrs >= L["LONG_HELD_YEARS"]:
        s += 5; reasons.append(f"Same owner {yrs} yrs (+5)"); tags.append("LONG_HELD")
    if p.get("mail_state") and p["mail_state"] != "MD":
        s += 5; reasons.append(f"Owner mails out of state ({p['mail_state']}) (+5)"); tags.append("OUT_OF_STATE")
    t = title.get(p.get("acct"))
    if t:
        msg = [f for f in t["flags"] if any(k in f for k in ("DECEASED", "ESTATE", "HEIRS", "PROBATE", "TAX_SALE"))]
        if msg:
            s += 10; reasons.append(f"Title Scan: {t['class'] or ', '.join(msg[:3])} (+10)"); tags.append("MESSY_TITLE")
            p["title_flags"] = msg

    # cautions (shown, not scored)
    cautions = []
    if "Waterfront" in (p.get("waterfront") or "") or "View" in (p.get("waterfront") or ""):
        tags.append("WATERFRONT")
        cautions.append("Waterfront — check the Chesapeake Bay Critical Area map: inside 1,000 ft of tidal water, "
                        "Resource Conservation Area land is capped at about 1 home per 20 acres")
    if (p.get("critical") or "").strip() in ("C", "L", "I"):
        cautions.append(f"SDAT marks this parcel in the Critical Area (code {p['critical']})")
    if _num(p.get("non_perc")) > 0:
        cautions.append("Part of the parcel failed a perc test — septic lots limited")
        if tg == "3":
            s -= 10; reasons.append("Non-perc area on a septic-only parcel (-10)")
    if yrs is not None and yrs <= L["RECENT_SALE_YEARS"] and _num(p.get("sale_price")) > 0:
        s -= 15; reasons.append(f"Sold {p.get('transfer_date')} for ${_num(p.get('sale_price')):,.0f} — someone may already be developing it (-15)")
        tags.append("RECENT_SALE")

    # rough yield, before engineering (roads, stormwater, setbacks eat land)
    dens = next((v for k, v in L["DENSITY"] if k in gz), 0.2 if tg == "3" else 0.5)
    if tg == "3":
        dens = min(dens, L["SEPTIC_MAX_DENSITY"])
    p["rough_lots"] = int(acres * dens * L["NET_DEVELOPABLE"])

    p["dev_score"] = min(100, dev)
    return min(100, max(0, s)), (reasons, tags, cautions)


# ── Main ────────────────────────────────────────────────────────────────────

def run(public=False, county=None):
    session = requests.Session()
    dataset = config.DATASET_ID if (_auth() and not public) else PUBLIC_DATASET
    licensed = dataset != PUBLIC_DATASET
    cols = dict(COLS)
    owner_col = None
    if licensed:
        oc = owner_columns(dataset, session)
        cols.update(oc)
        owner_col = oc.get("owner1")
    log.info("SDAT dataset %s (%s)", dataset, "licensed, owner names" if licensed else "public, no owner names")

    parcels = [p for p in fetch_parcels(dataset, cols, session, county) if _num(p.get("lat")) and _num(p.get("lon"))]
    log.info("%d parcels with coordinates", len(parcels))

    from engine2.signals import mailing_state
    for p in parcels:
        p["mail_state"] = mailing_state(p.get("mail_city"), p.get("mail_zip")) if licensed else ""

    pts = [(_num(p["lat"]), _num(p["lon"])) for p in parcels]
    log.info("septic growth tiers")
    tiers = overlay(pts, fetch_layer(TIER_URL, "TIER,County", session), "TIER")
    log.info("generalized zoning")
    zones = overlay(pts, fetch_layer(ZONING_URL, "GENZONE", session), "GENZONE")
    for p, t, z in zip(parcels, tiers, zones):
        p["tier"], p["gen_zone"] = t, z

    homes = Grid(fetch_new_homes(dataset, session, county))
    builders = Grid(fetch_builder_buys(dataset, owner_col, session, county)) if owner_col else None
    title = load_title_flags()

    kept, dropped = [], Counter()
    for p in parcels:
        la, lo = _num(p["lat"]), _num(p["lon"])
        p["new_homes_nearby"] = len(homes.near(la, lo, L["RADIUS_MILES"]))
        if builders:
            p["builders_nearby"] = sorted({b[2] for b in builders.near(la, lo, L["BUILDER_RADIUS_MILES"])})
        score, info = evaluate(p, title)
        if score is None:
            dropped[info] += 1
            continue
        if score < L["MIN_SCORE"]:
            dropped["below min score"] += 1
            continue
        reasons, tags, cautions = info
        kept.append({
            "id": p["acct"],   # same id as Title Leads, so notes on one parcel follow it across tabs
            "account": p["acct"], "county": re.sub(r"'S\b", "'s", (p.get("county") or "").title()), "county_code": p.get("county_code"),
            "address": (p.get("address") or "").strip(), "city": (p.get("city") or "").strip(), "zip": p.get("zip") or "",
            "lat": round(_num(p["lat"]), 6), "lon": round(_num(p["lon"]), 6),
            "tax_map": (p.get("tax_map") or "").strip(), "grid": (p.get("grid") or "").strip(), "parcel": (p.get("parcel") or "").strip(),
            "subdivision": (p.get("subdivision") or "").strip(), "lot": (p.get("lot") or "").strip(), "block": (p.get("block") or "").strip(),
            "legal": (p.get("legal1") or "").strip(),
            "owner": (p.get("owner1") or "").strip(), "owner2": (p.get("owner2") or "").strip(),
            "mail": " ".join(x.strip() for x in (p.get("mail_addr"), p.get("mail_city"), p.get("mail_zip")) if x),
            "mail_state": p.get("mail_state", ""),
            "acres": p["acres"], "land_use": p.get("land_use") or "", "zoning": (p.get("zoning") or "").strip(),
            "gen_zone": (p.get("gen_zone") or "").title(), "tier": p.get("tier") or "", "tier_group": tier_group(p.get("tier")),
            "sewer": p.get("sewer") or "", "water": p.get("water") or "",
            "land_value": _num(p.get("land_value")), "impr_value": _num(p.get("impr_value")),
            "year_built": p.get("year_built") or "", "transfer_date": p.get("transfer_date") or "", "sale_price": _num(p.get("sale_price")),
            "new_homes_nearby": p["new_homes_nearby"], "builders_nearby": p.get("builders_nearby") or [],
            "rough_lots": p["rough_lots"], "title_flags": p.get("title_flags") or [],
            "score": score, "dev_score": p["dev_score"], "reasons": reasons, "tags": tags, "cautions": cautions,
        })

    # keep the best per county so one big county can't crowd out the rest
    by_c = defaultdict(list)
    for r in kept:
        by_c[r["county"]].append(r)
    rows = []
    for c, rs in by_c.items():
        rs.sort(key=lambda r: (-r["score"], -r["acres"]))
        rows.extend(rs[:L["PER_COUNTY_LIMIT"]])
    rows.sort(key=lambda r: (-r["score"], -r["acres"]))
    attach_distress(rows)

    out = {
        "generated_at": _now(), "dataset": "licensed" if licensed else "public",
        "min_acres": L["MIN_ACRES"], "scanned": len(parcels), "qualified": len(kept), "rows_written": len(rows),
        "dropped": dict(dropped), "by_county": dict(Counter(r["county"] for r in rows)),
        "by_tier": dict(Counter(r["tier_group"] or "none" for r in rows)),
        "distress_at": _now(), "by_distress": dict(Counter(c for r in rows for c in r.get("distress", []))),
        "rows": rows,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    log.info("wrote %d rows (%d qualified of %d scanned) -> %s (%.1f MB)", len(rows), len(kept), len(parcels), OUT, os.path.getsize(OUT) / 1e6)
    log.info("dropped: %s", dict(dropped))
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--public", action="store_true", help="use the public SDAT dataset (no owner names)")
    ap.add_argument("--county", help="SDAT county code, e.g. 08 for Charles")
    ap.add_argument("--distress-only", action="store_true", help="only re-attach the distress stack to the existing candidates.json")
    a = ap.parse_args()
    try:
        if a.distress_only:
            refresh_distress()
        else:
            run(a.public, a.county)
    except Exception as e:   # noqa: BLE001 — surface the reason in the workflow log and keep yesterday's file
        log.exception("land watch failed: %s", e)
        sys.exit(1)
