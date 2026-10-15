"""Distress signals from fire, vacancy and code-enforcement records -- the
"something happened to this house" feed that stacks onto the parcels the rest
of the engine already tracks.

Every source here was checked by hand for (a) a usable address, (b) a date,
(c) an update cadence, and (d) robots/terms that allow automated pulls. What
each one is, and what it is not:

  OSFM fire releases   firemarshal.maryland.gov/news. Statewide. Every release
                       the State Fire Marshal investigates: address in prose,
                       county in the dateline, est. loss, occupied/vacant. The
                       big, fatal and suspicious fires -- not every kitchen fire.
  Baltimore City DHCD  egisdata.baltimorecity.gov (Open Baltimore). Open Vacant
                       Building Notices (the city's official vacant list),
                       open receivership cases, open city work orders
                       (board-ups / cleanings). Address only, no zip -- matched
                       within the city.
  Montgomery DHCA      data.montgomerycountymd.gov. Housing code violations
                       (daily, full address, condition/item/action) and
                       demolition permits (residential use codes).
  Prince George's      data.princegeorgescountymd.gov. Foreclosed Property
                       Registry (owners of foreclosed homes must register --
                       carries the tax account) and DPIE permits (demolition /
                       fire-repair types).

Not available (checked): Baltimore County publishes code violations only as
tract counts; Anne Arundel has no open code-enforcement feed; Howard's fire
feed stopped in 2019; Montgomery's fire dispatch feed carries the station
address, not the incident's; Baltimore City's foreclosure-filings layer ends in
2020. PG housing violations end in 2021.

Output: data/distress/signals.json
  by_account: {acct: [signal, ...]}   signal = {kind, source, date, detail,
               url, precision, address, county, loss, residential}
  unmatched:  sample of incidents we could not place on a parcel
title_scan.py reads by_account and turns kinds into flags/points. Fires
younger than COOLING_DAYS are kept but marked cooling (not "ready").

Address precision: exact (house number) / block ("7400 block of") / none.
A block-level incident lists the candidate parcels and is never promoted
to a confirmed match.
"""
from __future__ import annotations

import argparse
import html as htmlmod
import json
import logging
import os
import re
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone

import requests

from . import config
from .auction_watch import addr_tokens
from . import accela_code, charles_code, water_usage

log = logging.getLogger("distress_watch")

OUT_DIR = "data/distress"
OUT_PATH = os.path.join(OUT_DIR, "signals.json")
UA = {"User-Agent": "Mozilla/5.0 (compatible; md-property-leads/1.0)"}
COOLING_DAYS = 30
MAX_SIGNALS_PER_ACCOUNT = 12
MAX_TIES = 3                   # an exact address shared by more parcels than this (condo tower) is ambiguous, not a match   # multi-unit buildings can carry hundreds of code cases; keep the newest                  # a fresh fire is shown, not marked ready
FIRE_LOOKBACK_DAYS = 730
CODE_LOOKBACK_DAYS = 365
PERMIT_LOOKBACK_DAYS = 365

RESIDENTIAL_WORDS = re.compile(r"\b(house|home|townhouse|town home|dwelling|residence|residential|apartment|condo|duplex|rowhome|row home|mobile home|trailer|bungalow|rancher|cape cod|split level|colonial)\b", re.I)
NON_RESIDENTIAL_WORDS = re.compile(r"\b(barn|warehouse|commercial|business|restaurant|church|school|vehicle|car fire|truck|brush fire|woods fire|grass fire|dumpster|shed fire|garage fire|boat|store|office|hotel|motel|nursery|industrial|factory)\b", re.I)
STREET_TYPES = r"(?:Road|Rd|Street|St|Avenue|Ave|Boulevard|Blvd|Drive|Dr|Court|Ct|Lane|Ln|Way|Place|Pl|Terrace|Ter|Circle|Cir|Highway|Hwy|Pike|Trail|Trl|Run|Loop|Parkway|Pkwy|Square|Sq|Path|Turnpike|Alley|Aly)"
ADDR_RE = re.compile(r"\b(\d{1,6})\s+((?:[A-Z][A-Za-z0-9'.-]*\s+){1,5}?" + STREET_TYPES + r")\b(?!\s+(?:Fire|Volunteer))", re.I)
BLOCK_RE = re.compile(r"\b(\d{2,6})\s+block\s+of\s+((?:[A-Z][A-Za-z0-9'.-]*\s+){1,5}?" + STREET_TYPES + r")\b", re.I)
LOSS_RE = re.compile(r"\$\s?([\d,]{4,})")
COUNTY_RE = re.compile(r"^\s*([A-Z][A-Z'.\s]+?)(?: COUNTY)?, MD\s*\(", re.M)

COUNTY_NAMES = {
    "ALLEGANY": "Allegany", "ANNE ARUNDEL": "Anne Arundel", "BALTIMORE": "Baltimore", "BALTIMORE CITY": "Baltimore City",
    "CALVERT": "Calvert", "CAROLINE": "Caroline", "CARROLL": "Carroll", "CECIL": "Cecil", "CHARLES": "Charles",
    "DORCHESTER": "Dorchester", "FREDERICK": "Frederick", "GARRETT": "Garrett", "HARFORD": "Harford", "HOWARD": "Howard",
    "KENT": "Kent", "MONTGOMERY": "Montgomery", "PRINCE GEORGE'S": "Prince George's", "PRINCE GEORGES": "Prince George's",
    "QUEEN ANNE'S": "Queen Anne's", "QUEEN ANNES": "Queen Anne's", "ST. MARY'S": "St. Mary's", "ST MARY'S": "St. Mary's",
    "ST. MARYS": "St. Mary's", "SOMERSET": "Somerset", "TALBOT": "Talbot", "WASHINGTON": "Washington",
    "WICOMICO": "Wicomico", "WORCESTER": "Worcester",
}


def _get(url, **kw):
    r = requests.get(url, headers=UA, timeout=90, **kw)
    r.raise_for_status()
    return r


def _text(html):
    t = re.sub(r"<script.*?</script>|<style.*?</style>", "", html, flags=re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    return htmlmod.unescape(re.sub(r"\s+", " ", t)).strip()


# ── Source 1: State Fire Marshal releases ────────────────────────────────────
def osfm_fires(max_pages=6):
    base = "https://firemarshal.maryland.gov"
    links, out = [], []
    for p in range(0, max_pages):
        try:
            html = _get(f"{base}/news" + (f"?page={p}" if p else "")).text
        except Exception as e:  # noqa: BLE001
            log.warning("osfm list page %d: %s", p, e); break
        found = re.findall(r'href="(/news/[a-z0-9\-]+)"', html)
        found = [f for f in found if f not in links]
        if not found:
            break
        links += found
    cutoff = date.today() - timedelta(days=FIRE_LOOKBACK_DAYS)
    for path in links:
        try:
            html = _get(base + path).text
        except Exception as e:  # noqa: BLE001
            log.warning("osfm %s: %s", path, e); continue
        m = re.search(r'<time datetime="(\d{4}-\d\d-\d\d)', html)
        pub = date.fromisoformat(m.group(1)) if m else None
        if pub and pub < cutoff:
            continue
        body_m = re.search(r"<article.*?</article>|<main.*?</main>", html, re.S)
        text = _text(body_m.group(0) if body_m else html)
        title_m = re.search(r"<title>([^|<]+)", html)
        title = htmlmod.unescape(title_m.group(1)).strip() if title_m else path
        cm = COUNTY_RE.search(text)
        county = COUNTY_NAMES.get((cm.group(1).strip().upper() if cm else ""), None)
        if not county:
            # fall back to the headline
            for k, v in COUNTY_NAMES.items():
                if re.search(r"\b" + re.escape(k) + r"\b", (title + " " + text[:300]).upper()):
                    county = v; break
        # incident date: "On October 3, 2026," style, else publication date
        dm = re.search(r"On\s+([A-Z][a-z]+ \d{1,2}, \d{4})", text)
        try:
            inc = datetime.strptime(dm.group(1), "%B %d, %Y").date() if dm else pub
        except ValueError:
            inc = pub
        bm = BLOCK_RE.search(text)
        am = ADDR_RE.search(text)
        if bm and (not am or bm.start() <= am.start() + 6):
            address, precision = f"{bm.group(1)} {bm.group(2)}".strip(), "block"
        elif am:
            address, precision = f"{am.group(1)} {am.group(2)}".strip(), "exact"
        else:
            address, precision = None, "none"
        loss_m = LOSS_RE.search(text)
        loss = float(loss_m.group(1).replace(",", "")) if loss_m else None
        head = text[:600]
        residential = bool(RESIDENTIAL_WORDS.search(title + " " + head)) and not (
            NON_RESIDENTIAL_WORDS.search(title) and not RESIDENTIAL_WORDS.search(title))
        fatal = bool(re.search(r"\b(fatal|claims (?:the )?life|succumb|deceased|dies|dead)\b", title + " " + head, re.I))
        vacant = bool(re.search(r"\b(vacant|unoccupied|abandoned)\b", head, re.I))
        out.append({
            "kind": "FIRE", "source": "MD State Fire Marshal", "url": base + path,
            "date": inc.isoformat() if inc else None, "published": pub.isoformat() if pub else None,
            "county": county, "address": address, "precision": precision, "city": None, "zip": None,
            "detail": title, "loss": loss, "residential": residential, "fatal": fatal, "vacant_at_fire": vacant,
            "cause": (re.search(r"(?:cause[^.]{0,40}?(?:determined|ruled|was)\s+(?:to be\s+)?)([^.]{3,60})", text, re.I) or [None, None])[1],
        })
    log.info("osfm: %d releases, %d with an address", len(out), sum(1 for o in out if o["address"]))
    return out


# ── Source 2: Baltimore City DHCD (ArcGIS) ───────────────────────────────────
BALT = "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/DHCD_Open_Baltimore_Datasets/FeatureServer"


def _arcgis_all(layer_url, where, fields, order="OBJECTID"):
    out, offset = [], 0
    while True:
        r = _get(f"{layer_url}/query", params={
            "where": where, "outFields": ",".join(fields), "returnGeometry": "false", "f": "json",
            "resultOffset": offset, "resultRecordCount": 1000, "orderByFields": order}).json()
        feats = [f["attributes"] for f in r.get("features", [])]
        out += feats
        if len(feats) < 1000 or not r.get("exceededTransferLimit", True) and len(feats) < 1000:
            break
        offset += 1000
        if offset > 60000:
            break
    return out


def _ms(v):
    try:
        return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).date().isoformat() if v else None
    except (TypeError, ValueError, OSError):
        return None


def baltimore_city():
    out = []
    try:
        for a in _arcgis_all(f"{BALT}/1", "DateCancel IS NULL AND DateAbate IS NULL", ["NoticeNum", "DateNotice", "NT", "BLOCKLOT", "Address", "Neighborhood"]):
            out.append({"kind": "VACANT_NOTICE", "source": "Baltimore City DHCD — Vacant Building Notice (open)",
                        "url": "https://data.baltimorecity.gov/", "date": _ms(a.get("DateNotice")), "county": "Baltimore City",
                        "address": (a.get("Address") or "").strip(), "precision": "exact", "city": "BALTIMORE", "zip": None,
                        "detail": f"VBN {a.get('NoticeNum')} · {a.get('NT')} · {a.get('Neighborhood')}", "ref": a.get("NoticeNum"),
                        "blocklot": a.get("BLOCKLOT"), "residential": True})
    except Exception as e:  # noqa: BLE001
        log.warning("baltimore VBN: %s", e)
    try:
        for a in _arcgis_all(f"{BALT}/4", "SoldAtAuction IS NULL", ["ID_RS_Web", "BLOCKLOT", "Address", "DateFiled", "ReceiverAppointed", "DateAuction", "Neighborhood"]):
            out.append({"kind": "RECEIVERSHIP", "source": "Baltimore City DHCD — Receivership filed",
                        "url": "https://data.baltimorecity.gov/", "date": _ms(a.get("DateFiled")), "county": "Baltimore City",
                        "address": (a.get("Address") or "").strip(), "precision": "exact", "city": "BALTIMORE", "zip": None,
                        "detail": f"Receivership #{a.get('ID_RS_Web')} filed{' · receiver appointed' if a.get('ReceiverAppointed') else ''}{' · auction ' + _ms(a.get('DateAuction')) if a.get('DateAuction') else ''}",
                        "ref": str(a.get("ID_RS_Web")), "blocklot": a.get("BLOCKLOT"), "residential": True})
    except Exception as e:  # noqa: BLE001
        log.warning("baltimore receivership: %s", e)
    try:
        for a in _arcgis_all(f"{BALT}/9", "1=1", ["Status", "DateCreate", "WorkOrderType", "CleanType", "BLOCKLOT", "FULLADDR"]):
            if not a.get("FULLADDR"):
                continue
            out.append({"kind": "CITY_WORK_ORDER", "source": "Baltimore City DHCD — open work order",
                        "url": "https://data.baltimorecity.gov/", "date": _ms(a.get("DateCreate")), "county": "Baltimore City",
                        "address": a["FULLADDR"].strip(), "precision": "exact", "city": "BALTIMORE", "zip": None,
                        "detail": f"{a.get('WorkOrderType') or ''} {a.get('CleanType') or ''} · {a.get('Status') or ''}".strip(),
                        "blocklot": a.get("BLOCKLOT"), "residential": True})
    except Exception as e:  # noqa: BLE001
        log.warning("baltimore work orders: %s", e)
    log.info("baltimore city: %d signals", len(out))
    return out


# ── Source 3: Montgomery County (Socrata) ────────────────────────────────────
def _socrata(host, ds, where, limit=50000, order=":id"):
    out, offset = [], 0
    while True:
        r = _get(f"https://{host}/resource/{ds}.json", params={"$where": where, "$limit": 5000, "$offset": offset, "$order": order}).json()
        out += r
        if len(r) < 5000 or len(out) >= limit:
            break
        offset += 5000
    return out


def montgomery():
    out = []
    since = (date.today() - timedelta(days=CODE_LOOKBACK_DAYS)).isoformat()
    try:
        rows = _socrata("data.montgomerycountymd.gov", "k9nj-z35d",
                        f"date_filed >= '{since}' AND disposition != 'Violation Unfounded' AND item != 'No Violations Observed'")
        # one signal per case, not per line item
        by_case = {}
        for r in rows:
            k = r.get("case_number")
            c = by_case.setdefault(k, {"items": [], "r": r})
            c["items"].append(f"{r.get('condition') or ''}: {r.get('item') or ''}".strip(": "))
        for k, c in by_case.items():
            r = c["r"]
            corrected = all(x.get("corrected") for x in rows if x.get("case_number") == k)
            if r.get("date_closed") and corrected:
                continue
            out.append({"kind": "CODE_VIOLATIONS", "source": "Montgomery County DHCA — housing code case",
                        "url": "https://data.montgomerycountymd.gov/d/k9nj-z35d", "date": (r.get("date_filed") or "")[:10],
                        "county": "Montgomery", "address": r.get("street_address"), "precision": "exact",
                        "city": r.get("city"), "zip": (r.get("zip_code") or "")[:5],
                        "detail": f"Case {k} · {len(c['items'])} item(s): " + "; ".join(sorted(set(c["items"]))[:4]),
                        "ref": k, "residential": True, "open": not r.get("date_closed")})
    except Exception as e:  # noqa: BLE001
        log.warning("montgomery code: %s", e)
    try:
        psince = (date.today() - timedelta(days=PERMIT_LOOKBACK_DAYS)).isoformat()
        rows = _socrata("data.montgomerycountymd.gov", "b6ht-fw3x", f"addeddate >= '{psince}'")
        for r in rows:
            use = (r.get("usecode") or "").upper()
            if not re.search(r"DWELLING|RESIDEN|TOWNHOUSE|CONDO|DUPLEX", use):
                continue
            addr = " ".join(x for x in (r.get("stno"), r.get("predir"), r.get("stname"), r.get("suffix"), r.get("postdir")) if x)
            out.append({"kind": "DEMO_PERMIT", "source": "Montgomery County DPS — demolition permit",
                        "url": "https://data.montgomerycountymd.gov/d/b6ht-fw3x", "date": (r.get("addeddate") or "")[:10],
                        "county": "Montgomery", "address": addr, "precision": "exact", "city": r.get("city"), "zip": (r.get("zip") or "")[:5],
                        "detail": f"Permit {r.get('permitno')} · {r.get('status')} · {r.get('worktype')} · {use.title()}", "ref": r.get("permitno"),
                        "residential": True})
    except Exception as e:  # noqa: BLE001
        log.warning("montgomery demo: %s", e)
    log.info("montgomery: %d signals", len(out))
    return out


# ── Source 4: Prince George's (Socrata) ──────────────────────────────────────
def prince_georges():
    out = []
    try:
        since = (date.today() - timedelta(days=730)).isoformat()
        rows = _socrata("data.princegeorgescountymd.gov", "cni6-nr5g", f"submitteddate >= '{since}'")
        for r in rows:
            out.append({"kind": "FORECLOSURE_REGISTRY", "source": "Prince George's County — Foreclosed Property Registry",
                        "url": "https://data.princegeorgescountymd.gov/d/cni6-nr5g", "date": (r.get("submitteddate") or "")[:10],
                        "county": "Prince George's", "address": r.get("street_address"), "precision": "exact",
                        "city": r.get("city"), "zip": (r.get("zip_code") or "")[:5],
                        "detail": f"Registered {(r.get('submitteddate') or '')[:10]} · occupied: {r.get('addressoccupied')} · {r.get('propertydescription')}",
                        "ref": r.get("taxaccountnumber"), "residential": (r.get("propertydescription") or "").lower().startswith("resid")})
    except Exception as e:  # noqa: BLE001
        log.warning("pg registry: %s", e)
    try:
        psince = (date.today() - timedelta(days=PERMIT_LOOKBACK_DAYS)).isoformat()
        rows = _socrata("data.princegeorgescountymd.gov", "weik-ttee",
                        f"permit_issuance_date >= '{psince}' AND (upper(permit_type) like '%DEMO%' OR upper(permit_type) like '%RAZE%' "
                        f"OR permit_type in ('RZ','FIR') OR upper(case_name) like '%FIRE%' OR upper(case_name) like '%DEMO%' OR upper(case_name) like '%RAZE%')")
        for r in rows:
            kind = "FIRE_REPAIR_PERMIT" if "FIRE" in (r.get("case_name") or "").upper() else "DEMO_PERMIT"
            out.append({"kind": kind, "source": "Prince George's County DPIE — permit",
                        "url": "https://data.princegeorgescountymd.gov/d/weik-ttee", "date": (r.get("permit_issuance_date") or "")[:10],
                        "county": "Prince George's", "address": r.get("street_address"), "precision": "exact",
                        "city": r.get("city"), "zip": (r.get("zip_code") or "")[:5],
                        "detail": f"{r.get('permit_type')} · {r.get('case_name') or ''} · est. cost ${float(r.get('expected_construction_cost') or 0):,.0f}",
                        "ref": r.get("permit_case_id"), "residential": "RESID" in (r.get("permit_category") or "").upper() or True})
    except Exception as e:  # noqa: BLE001
        log.warning("pg permits: %s", e)
    log.info("prince george's: %d signals", len(out))
    return out


# ── Matching ─────────────────────────────────────────────────────────────────
COLS = "acct, county, address, city, zip, owner1, land_value, impr_value, land_use"


def ensure_addr_index(db):
    """One pass over the parcels table -> addr_idx(acct, num, street, zip, county),
    indexed. Turns each signal lookup from a LIKE scan over a whole county into an
    index hit (Baltimore City alone sends ~13k signals against ~240k parcels)."""
    have = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='addr_idx'").fetchone()
    if have and db.execute("SELECT COUNT(*) FROM addr_idx").fetchone()[0] > 0:
        return
    log.info("building address index …")
    db.execute("DROP TABLE IF EXISTS addr_idx")
    db.execute("CREATE TABLE addr_idx (acct TEXT, num INTEGER, street TEXT, zip TEXT, county TEXT)")
    cur = db.execute("SELECT acct, address, zip, county FROM parcels")
    batch = []
    while True:
        rows = cur.fetchmany(50000)
        if not rows:
            break
        for acct, address, zp, county in rows:
            t = addr_tokens(address)
            if len(t) > 1 and t[0].isdigit():
                batch.append((acct, int(t[0]), t[1], (zp or "")[:5], county))
        if batch:
            db.executemany("INSERT INTO addr_idx VALUES (?,?,?,?,?)", batch); batch = []
    db.execute("CREATE INDEX ix_addr_zip ON addr_idx(zip, street, num)")
    db.execute("CREATE INDEX ix_addr_county ON addr_idx(county, street, num)")
    db.commit()
    log.info("address index: %d rows", db.execute("SELECT COUNT(*) FROM addr_idx").fetchone()[0])


def _parcels(db, accts):
    if not accts:
        return []
    names = [c.strip() for c in COLS.split(",")]
    out = []
    for i in range(0, len(accts), 500):
        chunk = accts[i:i + 500]
        q = f"SELECT {COLS} FROM parcels WHERE acct IN ({','.join('?' * len(chunk))})"
        out += [dict(zip(names, r)) for r in db.execute(q, chunk).fetchall()]
    return out


def match(db, sig):
    """Exact: house number + first street word, scoped by zip when we have one,
    else by county. Block: every parcel on that street with a number in the
    block, scoped the same way. Returns (candidates, precision)."""
    toks = addr_tokens(sig.get("address"))
    if not toks or not toks[0].isdigit():
        return [], "none"
    num, street = toks[0], (toks[1] if len(toks) > 1 else "")
    if not street:
        return [], "none"
    cur = db.cursor()
    if sig.get("zip"):
        scope, args = "zip = ?", [sig["zip"]]
    elif sig.get("county"):
        scope, args = "county = ?", [sig["county"]]
    else:
        return [], "none"
    if sig.get("precision") == "block":
        lo, hi = int(num), int(num) + 99
        accts = [r[0] for r in cur.execute(f"SELECT acct FROM addr_idx WHERE {scope} AND street = ? AND num BETWEEN ? AND ?",
                                           (*args, street, lo, hi)).fetchall()]
        cands = _parcels(db, accts)
        return cands[:40], ("block" if cands else "none")
    accts = [r[0] for r in cur.execute(f"SELECT acct FROM addr_idx WHERE {scope} AND street = ? AND num = ?",
                                       (*args, street, int(num))).fetchall()]
    cands = _parcels(db, accts)
    if not cands:
        return [], "none"
    want = set(toks)
    cands.sort(key=lambda c: -len(want & set(addr_tokens(c["address"]))))
    best = cands[0]
    ties = [c for c in cands if len(want & set(addr_tokens(c["address"]))) == len(want & set(addr_tokens(best["address"])))]
    if len(ties) > MAX_TIES:
        # "1005 Chillum Rd" against a 74-unit condo with no unit given: don't smear one record over every unit
        return [], "ambiguous"
    return (ties if len(ties) > 1 else [best]), ("exact" if len(ties) == 1 else "block")


def match_by_account(db, sig):
    """Prince George's registry rows carry the 7-digit county tax account; SDAT's
    account is '17' + 2-digit district + that number."""
    ref = re.sub(r"\D", "", str(sig.get("ref") or ""))
    if not ref:
        return []
    names = [c.strip() for c in COLS.split(",")]
    if sig.get("county") == "Prince George's":
        rows = db.execute(f"SELECT {COLS} FROM parcels WHERE substr(acct, 5) = ? AND acct LIKE '17%'", (ref.zfill(7),)).fetchall()
    elif sig.get("county") == "Charles":
        # Civic Access "MainParcel" is the SDAT account itself (09 + 8 digits)
        rows = db.execute(f"SELECT {COLS} FROM parcels WHERE acct = ?", (ref,)).fetchall()
    else:
        return []
    return [dict(zip(names, r)) for r in rows]


def build(index_path, today=None):
    today = today or date.today()
    db = sqlite3.connect(index_path)
    db.execute("CREATE INDEX IF NOT EXISTS ix_zip ON parcels(zip)")
    db.execute("CREATE INDEX IF NOT EXISTS ix_county ON parcels(county)")
    ensure_addr_index(db)
    db.execute("CREATE INDEX IF NOT EXISTS ix_acct_tail ON parcels(substr(acct, 5))")
    sigs = []
    counts = {}
    for name, fn in (("osfm", osfm_fires), ("baltimore_city", baltimore_city), ("montgomery", montgomery), ("prince_georges", prince_georges),
                     ("charles_code", charles_code.signals), ("accela_code", accela_code.signals),
                     ("water_usage", water_usage.signals)):
        try:
            got = fn()
        except Exception as e:  # noqa: BLE001
            log.warning("%s failed: %s", name, e); got = []
        counts[name] = {"fetched": len(got)}
        for s in got:
            s["_src"] = name
        sigs += got

    by_acct, unmatched = {}, []
    for s in sigs:
        if s["kind"] == "FIRE" and not s.get("residential"):
            counts[s["_src"]]["non_residential"] = counts[s["_src"]].get("non_residential", 0) + 1
            continue
        cands = match_by_account(db, s)
        prec = "exact" if cands else None
        if not cands:
            cands, prec = match(db, s)
        src = s.pop("_src")
        counts[src]["matched"] = counts[src].get("matched", 0) + (1 if cands else 0)
        if not cands:
            if len(unmatched) < 400:
                unmatched.append({**{k: s.get(k) for k in ("kind", "source", "date", "county", "address", "detail", "url")}, "why": prec})
            continue
        if s.get("needs_improvements"):
            # zero water on a bare lot is just a meter stub, not a vacant house
            cands = [c for c in cands if (c.get("impr_value") or 0) > 0]
            if not cands:
                counts[src]["bare_lot"] = counts[src].get("bare_lot", 0) + 1
                continue
        for c in cands:
            rec = {**{k: v for k, v in s.items() if k not in ("city", "zip", "needs_improvements")}, "precision": prec, "matched_address": c["address"],
                   "candidates": len(cands)}
            if s["kind"] == "FIRE" and s.get("date"):
                age = (today - date.fromisoformat(s["date"])).days
                rec["days_since"] = age
                rec["cooling"] = age < COOLING_DAYS
            by_acct.setdefault(c["acct"], []).append(rec)
    for acct, lst in by_acct.items():
        lst.sort(key=lambda x: x.get("date") or "", reverse=True)
        if len(lst) > MAX_SIGNALS_PER_ACCOUNT:
            kept = lst[:MAX_SIGNALS_PER_ACCOUNT]
            kept[-1] = {**kept[-1], "detail": f"{kept[-1].get('detail') or ''} · +{len(lst) - MAX_SIGNALS_PER_ACCOUNT} more records at this parcel (multi-unit)"}
            by_acct[acct] = kept

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "note": "Fire / vacancy / code-enforcement / foreclosure-registry signals matched to SDAT parcels. "
                           "A block-level fire lists every candidate parcel and is never a confirmed match.",
                   "sources": counts, "accounts": len(by_acct),
                   "kinds": {k: sum(1 for v in by_acct.values() for x in v if x["kind"] == k)
                             for k in sorted({x["kind"] for v in by_acct.values() for x in v})},
                   "by_account": by_acct, "unmatched": unmatched}, f, ensure_ascii=False, separators=(",", ":"))
    log.info("signals: %s | accounts with signals: %d | unmatched sample: %d", counts, len(by_acct), len(unmatched))
    return by_acct


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", default=config.INDEX_PATH)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        build(a.index)
    except Exception:  # noqa: BLE001
        import traceback
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(os.path.join(OUT_DIR, "last-error.txt"), "w", encoding="utf-8") as f:
            f.write(datetime.now(timezone.utc).isoformat(timespec="seconds") + "\n" + traceback.format_exc())
        raise


if __name__ == "__main__":
    sys.exit(main())
