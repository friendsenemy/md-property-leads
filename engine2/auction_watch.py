"""Mortgage-foreclosure surplus watch — the fast feed.

A Maryland mortgage foreclosure ends at a public auction run by a substitute
trustee. The auctioneer posts the hammer price the same day. That is the
earliest public moment a surplus exists: weeks before the report of sale is
ratified, months before the auditor states the account, and months before the
trustee's deed reaches SDAT.

    surplus  =  hammer price  -  (what the lender was owed + costs)
              -  junior liens (2nd mortgage, HOA, judgments)   -> former owner

We do not know the payoff. We estimate it from the owner's own purchase
(price + year on their deed, assumed 95% LTV, 30-year amortization, plus
arrears) and we SAY it is an estimate. Only strong cases reach the board.

Feeds (robots.txt permits each; Harvey West does not, and is skipped):
  * Alex Cooper  realestate.alexcooper.com/sold-lots   (results, newest first)
                 realestate.alexcooper.com/foreclosures (scheduled sales)
    The pages embed every lot as JSON ("auction-lot-summary"); no login.

Outputs data/surplus/auctions.json:
  rows[]  one per matched sale, kind SOLD (hammer price known) or SCHEDULED
          (sale date known, owner still holds title -- a buy lead)
  Each row carries the SDAT owner (still the foreclosed owner for weeks after
  the sale), their mailing address, occupancy, assessed value, their own
  purchase price/date, and the estimate.

Legal frame, so nobody forgets: buying or taking assignment of a surplus
claim makes you a "foreclosure surplus purchaser" (RP 7-314/315: mandatory
written contract, 14-pt warning, 10-day rescission after the audit); helping
for a fee makes you a "foreclosure consultant" (RP 7-301..7-307). Maryland
lawyer first.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
from datetime import date, datetime, timezone

import requests

from . import config

log = logging.getLogger("auction_watch")

OUT_DIR = "data/surplus"
OUT_PATH = os.path.join(OUT_DIR, "auctions.json")
SEEN_PATH = os.path.join(OUT_DIR, "auctions-seen.json")

UA = {"User-Agent": "Mozilla/5.0 (compatible; md-property-leads/1.0; +https://pagesofpurposellc.com/md-property-leads/)"}

SOURCES = {
    "alexcooper_sold": "https://realestate.alexcooper.com/sold-lots",
    "alexcooper_upcoming": "https://realestate.alexcooper.com/foreclosures",
}

# --- the estimate --------------------------------------------------------
ASSUMED_LTV = 0.95            # typical purchase-money loan
ASSUMED_RATE = 0.055          # blended; a 2021 loan is lower, a 2007 loan higher
ARREARS_FACTOR = 0.12         # ~18 months of missed payments, fees, trustee costs
REFI_HAIRCUT_YEARS = 10       # a loan older than this was probably refinanced at least once --
                              # we widen the uncertainty rather than pretend it amortised
STRONG_MIN_SURPLUS = 25000    # dollars -- below this it is not worth two people's time at 40%
POSSIBLE_MIN_SURPLUS = 10000
STRONG_MIN_RATIO = 1.30       # hammer must beat the estimated payoff by this much to be "strong"
MIN_ASSESSED = 60000          # residential floor; paper lots and sheds never qualify


def _f(v):
    try:
        return float(str(v).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return 0.0


def remaining_balance(principal, rate, years_elapsed, term=30):
    """Standard amortization: what is still owed after years_elapsed."""
    if principal <= 0:
        return 0.0
    r = rate / 12.0
    n = term * 12
    k = min(max(years_elapsed, 0) * 12, n)
    if r == 0:
        return principal * (1 - k / n)
    return principal * ((1 + r) ** n - (1 + r) ** k) / ((1 + r) ** n - 1)


def estimate_payoff(purchase_price, purchase_year, sale_year):
    """(low, mid, high) payoff estimate from the owner's own purchase.
    Returns None when the purchase is unusable (no price, inherited, $0 deed)."""
    if not purchase_price or purchase_price < 20000 or not purchase_year:
        return None
    yrs = max(sale_year - purchase_year, 0)
    base = remaining_balance(purchase_price * ASSUMED_LTV, ASSUMED_RATE, yrs)
    mid = base * (1 + ARREARS_FACTOR)
    if yrs > REFI_HAIRCUT_YEARS:
        # old loan: could have been paid down -- or cashed out. Widen both ways.
        return (mid * 0.6, mid, purchase_price * ASSUMED_LTV * (1 + ARREARS_FACTOR))
    return (mid * 0.85, mid, mid * 1.15)


# --- fetch + parse --------------------------------------------------------
def fetch(url):
    r = requests.get(url, headers=UA, timeout=60)
    r.raise_for_status()
    return r.text


def embedded_lots(html):
    """Every {"type":"auction-lot-summary", ...} object in the page, parsed."""
    out = []
    for m in re.finditer(r'\{"type":"auction-lot-summary"', html):
        s = m.start(); depth = 0; k = s
        while k < len(html):
            c = html[k]
            if c == '"':
                k += 1
                while html[k] != '"':
                    if html[k] == "\\":
                        k += 1
                    k += 1
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        try:
            out.append(json.loads(html[s:k + 1]))
        except ValueError:
            pass
    seen = set(); uniq = []
    for l in out:
        if l.get("row_id") in seen:
            continue
        seen.add(l.get("row_id")); uniq.append(l)
    return uniq


_DEP = re.compile(r"Dep\.?\s*\$\s*([\d,]+)", re.I)
_TIME = re.compile(r"^\s*(\d{1,2}:\d\d\s*[ap]m)\s+", re.I)


def normalize_lot(l, kind):
    a = l.get("address") or {}
    auction = l.get("auction") if isinstance(l.get("auction"), dict) else {}
    title = l.get("title") or ""
    if not (a.get("address_line_one") and a.get("postal_code")):
        return None
    if "Substitute Trustee" not in (auction.get("title") or "") + title and not _TIME.match(title) \
            and (auction.get("auction_type") or "").lower() not in ("foreclosure",):
        # Alex Cooper mixes estate/commercial sales in; the foreclosure lots carry the
        # "9:20 am ADDRESS Dep. $X" title form or a Substitute Trustee heading.
        if not _DEP.search(title):
            return None
    dep = _DEP.search(title)
    start = auction.get("time_start") or auction.get("time_start_live_auction") or ""
    try:
        sale_dt = datetime.fromisoformat(start.replace("Z", "+00:00")).date() if start else None
    except ValueError:
        sale_dt = None
    return {
        "lot_id": l.get("row_id"),
        "kind": kind,
        "status": l.get("status"),
        "address": a.get("address_line_one"), "unit": a.get("address_line_two"),
        "city": a.get("city"), "zip": (a.get("postal_code") or "")[:5],
        "county_hint": auction.get("county"),
        "lat": (a.get("gps_coordinates") or {}).get("latitude"),
        "lon": (a.get("gps_coordinates") or {}).get("longitude"),
        "hammer": _f(l.get("sold_price")) or None,
        "deposit": _f(dep.group(1)) if dep else None,
        "sale_date": sale_dt.isoformat() if sale_dt else None,
        "auction_title": (auction.get("title") or "")[:160],
        "lawyer_code": l.get("lawyer_code"),
        "source": "realestate.alexcooper.com",
        "url": "https://realestate.alexcooper.com" + (l.get("_detail_url") or "/sold-lots"),
        "last_updated": l.get("last_updated"),
    }


# --- SDAT match -------------------------------------------------------------
_SUFFIX = {"STREET": "ST", "ROAD": "RD", "AVENUE": "AVE", "DRIVE": "DR", "COURT": "CT", "LANE": "LN",
           "PLACE": "PL", "TERRACE": "TER", "CIRCLE": "CIR", "BOULEVARD": "BLVD", "WAY": "WAY",
           "HIGHWAY": "HWY", "PARKWAY": "PKWY", "TRAIL": "TRL", "SQUARE": "SQ", "LOOP": "LOOP"}


def addr_tokens(s):
    s = re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())
    toks = [(_SUFFIX.get(t, t)) for t in s.split()]
    toks = [t for t in toks if t not in ("UNIT", "APT", "#")]
    return toks


def match_sdat(db, lot):
    """House number + first street word + zip, then prefer same suffix/unit."""
    toks = addr_tokens(lot["address"])
    if not toks or not toks[0].isdigit():
        return None
    num, street = toks[0], toks[1] if len(toks) > 1 else ""
    cur = db.cursor()
    cols = ("acct, county, address, city, zip, lat, lon, owner1, owner2, mail_addr, mail_city, mail_zip, "
            "occupancy, land_use, land_value, impr_value, year_built, transfer_date, sale_price, grantor1, "
            "deed_liber, deed_folio, dwelling_type")
    rows = list(cur.execute(f"SELECT {cols} FROM parcels WHERE zip = ? AND address LIKE ?",
                            (lot["zip"], f"{num} {street}%")))
    if not rows and street:
        rows = list(cur.execute(f"SELECT {cols} FROM parcels WHERE zip = ? AND address LIKE ?",
                                (lot["zip"], f"{num} %{street}%")))
    if not rows:
        return None
    names = [c.strip() for c in cols.split(",")]
    cands = [dict(zip(names, r)) for r in rows]
    want = set(toks)
    unit = (lot.get("unit") or "").upper().replace("UNIT", "").replace("#", "").strip()

    def score(c):
        have = set(addr_tokens(c["address"]))
        s = len(want & have)
        if unit and unit in (c["address"] or "").upper():
            s += 3
        return s
    cands.sort(key=score, reverse=True)
    best = cands[0]
    if len(cands) > 1 and score(cands[1]) == score(best) and not unit:
        best["_ambiguous"] = len(cands)
    return best


def classify(lot, s, today):
    av = _f(s["land_value"]) + _f(s["impr_value"])
    owner = (s.get("owner1") or s.get("owner2") or "").strip()
    ent = re.search(r"\b(LLC|L L C|INC|CORP|LTD|LP|LLP|TRUST|TRUSTEE|HOLDINGS?|PROPERTIES|ASSOC|BANK|CHURCH|PARTNERS)\b", owner.upper())
    purch_year = None
    td = (s.get("transfer_date") or "").strip()
    if re.match(r"^(19|20)\d\d", td):
        purch_year = int(td[:4])
    sale_year = int(lot["sale_date"][:4]) if lot.get("sale_date") else today.year
    est = estimate_payoff(_f(s.get("sale_price")), purch_year, sale_year)
    reasons = []
    tier = None
    hammer = lot.get("hammer")
    surplus = None
    if lot["kind"] == "SOLD" and hammer:
        if est:
            lo, mid, hi = est
            surplus = (hammer - hi, hammer - mid, hammer - lo)
            if hammer - mid >= STRONG_MIN_SURPLUS and hammer >= mid * STRONG_MIN_RATIO:
                tier = "STRONG"; reasons.append(f"hammer {hammer:,.0f} vs est. payoff {mid:,.0f}")
            elif hammer - mid >= POSSIBLE_MIN_SURPLUS:
                tier = "POSSIBLE"; reasons.append("hammer modestly above estimated payoff")
            else:
                reasons.append("hammer at or below estimated payoff — likely no surplus")
        else:
            # no usable purchase: fall back to value. A hammer well above assessment
            # on a long-held home usually means equity.
            if av >= MIN_ASSESSED and hammer >= 0.9 * av and (purch_year or 0) and sale_year - purch_year >= 12:
                tier = "POSSIBLE"; reasons.append("no purchase price on deed; long-held and sold near value")
            else:
                reasons.append("no purchase price on deed to estimate the payoff")
    elif lot["kind"] == "SCHEDULED":
        # pre-auction: the owner still owns it. Equity on paper = buy lead.
        if est:
            lo, mid, hi = est
            surplus = (av - hi, av - mid, av - lo)
            if av - mid >= STRONG_MIN_SURPLUS and av >= mid * STRONG_MIN_RATIO:
                tier = "STRONG"; reasons.append(f"assessed {av:,.0f} vs est. payoff {mid:,.0f} — equity to protect")
            elif av - mid >= POSSIBLE_MIN_SURPLUS:
                tier = "POSSIBLE"
        elif lot.get("deposit") and av >= MIN_ASSESSED and lot["deposit"] * 10 < 0.7 * av:
            tier = "POSSIBLE"; reasons.append("deposit suggests a debt well under value")
    if ent:
        reasons.append("owner is an entity — the person behind it is the contact")
        if tier == "STRONG":
            tier = "POSSIBLE"
    if av and av < MIN_ASSESSED:
        tier = None; reasons.append("below the residential value floor")
    return tier, reasons, est, surplus, av, owner, purch_year


def build(index_path, today=None):
    today = today or date.today()
    db = sqlite3.connect(index_path)
    db.execute("CREATE INDEX IF NOT EXISTS ix_zip ON parcels(zip)")
    lots = []
    for key, url in SOURCES.items():
        try:
            html = fetch(url)
        except Exception as e:  # noqa: BLE001
            log.warning("%s: %s", key, e); continue
        kind = "SOLD" if key.endswith("sold") else "SCHEDULED"
        raw = embedded_lots(html)
        got = [x for x in (normalize_lot(l, kind) for l in raw) if x]
        log.info("%s: %d lots embedded, %d foreclosure-shaped", key, len(raw), len(got))
        lots += got

    seen = {}
    if os.path.exists(SEEN_PATH):
        with open(SEEN_PATH, encoding="utf-8") as f:
            seen = json.load(f).get("lots", {})

    rows, unmatched, dropped = [], 0, 0
    for lot in lots:
        if lot["kind"] == "SOLD" and not lot.get("hammer"):
            continue
        s = match_sdat(db, lot)
        if not s:
            unmatched += 1; continue
        tier, reasons, est, surplus, av, owner, purch_year = classify(lot, s, today)
        first_seen = seen.get(lot["lot_id"], {}).get("first_seen") or datetime.now(timezone.utc).isoformat(timespec="seconds")
        seen[lot["lot_id"]] = {"first_seen": first_seen, "kind": lot["kind"], "tier": tier}
        if not tier:
            dropped += 1; continue
        mail = " ".join(x for x in (s.get("mail_addr"), s.get("mail_city"), s.get("mail_zip")) if x)
        rows.append({
            **lot,
            "id": f"auction:{lot['lot_id']}",
            "stage": "AUCTION_SOLD" if lot["kind"] == "SOLD" else "AUCTION_SCHEDULED",
            "tier": tier, "why": "; ".join(reasons),
            "account": s["acct"], "county": s["county"],
            "sdat_address": s["address"], "sdat_city": s["city"],
            "owner_of_record": owner, "owner2": s.get("owner2"),
            "mail": mail, "absentee": bool(mail) and not mail.upper().startswith((s["address"] or "~").upper()[:8]),
            "occupancy": s.get("occupancy"), "land_use": s.get("land_use"), "dwelling_type": s.get("dwelling_type"),
            "assessed_value": av or None, "year_built": s.get("year_built"),
            "purchase_price": _f(s.get("sale_price")) or None, "purchase_year": purch_year,
            "purchase_deed": f"{s.get('deed_liber') or ''}/{s.get('deed_folio') or ''}".strip("/"),
            "grantor": s.get("grantor1"),
            "payoff_est": [round(x) for x in est] if est else None,
            "surplus_est": [round(x) for x in surplus] if surplus else None,
            "ambiguous_match": s.get("_ambiguous"),
            "first_seen": first_seen,
            "is_new": first_seen[:10] == today.isoformat(),
        })

    rows.sort(key=lambda r: ((r["tier"] != "STRONG"), -(r["surplus_est"][1] if r["surplus_est"] else 0)))
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "note": "Mortgage-foreclosure auction results matched to SDAT, with an ESTIMATED payoff from the "
                           "owner's own purchase. Only STRONG/POSSIBLE rows are kept. Estimates, not balances.",
                   "sources": SOURCES, "counts": {"fetched": len(lots), "unmatched": unmatched, "below_bar": dropped,
                                                  "kept": len(rows)},
                   "rows": rows}, f, ensure_ascii=False, separators=(",", ":"))
    with open(SEEN_PATH, "w", encoding="utf-8") as f:
        json.dump({"lots": seen}, f, separators=(",", ":"), sort_keys=True)
    log.info("kept %d of %d (unmatched %d, below bar %d); strong %d, new today %d",
             len(rows), len(lots), unmatched, dropped,
             sum(r["tier"] == "STRONG" for r in rows), sum(r["is_new"] for r in rows))
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", default=config.INDEX_PATH)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    build(a.index)


if __name__ == "__main__":
    sys.exit(main())
