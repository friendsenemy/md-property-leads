"""
Tax-sale outcome watcher: turns the tax-sale lists into two lead streams.

  PRE-DEED  the lien sold, the deed has NOT conveyed, the owner still holds
            title and is about to lose it  -> a BUY lead (Ray's core business)
  CONVEYED  the deed HAS conveyed, which means the purchaser paid the residue
            of the bid, which means a surplus balance is owed to the former
            owner under Tax-Property Art. 14-818(a)(4)  -> a SURPLUS lead

Why this works without Maryland Judiciary Case Search (whose terms prohibit
automated access): the docket entries are only evidence of the event. The
event itself -- delivery of the deed -- is recorded in SDAT as a change of
owner of record. We already rebuild the statewide SDAT index every week, so
we can observe the conveyance directly and skip the court system entirely.

The catch is that a diff needs a baseline. Owner names we do not snapshot
this week are not recoverable later, so this runs every week from now on and
the snapshot is committed to the repo.

  python -m engine2.surplus --index .cache/sdat.sqlite
"""

import argparse
import json
import logging
import os
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta, timezone

from engine2 import config

OUT_DIR = os.path.join("data", "surplus")
WATCH_PATH = os.path.join(OUT_DIR, "watch.json")
EVENTS_PATH = os.path.join(OUT_DIR, "events.json")
LEADS_PATH = os.path.join(OUT_DIR, "leads.json")
VESTED_PATH = os.path.join(OUT_DIR, "vested.json")

log = logging.getLogger("surplus")

# Statutory clock, Tax-Property Art. 14-833 / 14-847.
REDEMPTION_WAIT_DAYS = 183          # 6 months before a foreclosure may be filed
CERT_VOID_DAYS = 730                # 2 years from certificate: complaint or void
NOTICE_DEADLINE_DAYS = 90           # 14-818(a)(6) notice to the prior owner

# A purchaser only pays the residue of the bid if taking the deed is worth it.
# Bid at or below this share of assessed value => deeding is rational, so the
# case is likely to complete and produce a surplus.
DEED_RATIONAL_MAX_BID_TO_AV = 0.85

# Only these stages are written out. TOO_EARLY is a watch state, not a lead, and
# a bid far above value never produces a deed or a surplus -- neither belongs on
# the dashboard.
OPPORTUNITY_STAGES = {"CONVEYED", "FORECLOSURE_WINDOW", "CERT_STALE", "LIEN_STRANDED"}

# A bid far above what the property is worth strands the lien holder. To get the
# deed they must pay the residue (14-818(a)(2)), which here would cost more than
# the property, so they never will -- they wait for a redemption that pays
# interest, and if it never comes they let the certificate go void at 2 years.
# 14-833(d)(1) then forfeits their money to the taxes on this same parcel.
#
# No deed means no surplus, which is why these are useless as surplus leads. But
# it also means the OWNER KEEPS THE PROPERTY, and is sitting on a delinquency
# with the one buyer who could have taken it now unable to. That is a buy lead
# with almost no competition, because every investor list reads "sold at tax
# sale" and moves on.
LIEN_STRANDED_MIN_BID_TO_AV = 1.25      # bid exceeds value by enough that deeding is irrational
LIEN_STRANDED_MIN_BID_TO_FACE = 500     # fallback when we have no assessed value to compare

# TP 14-847: "The judgment of the court shall direct the collector to execute a
# deed to the holder of the certificate of sale." So every completed Maryland tax
# foreclosure leaves the COLLECTOR as grantor in the land records, whatever year
# it happened. That makes past conveyances findable from SDAT alone -- we do not
# need to have held that year's tax-sale list.
# Calibrated against the real grantor strings the first statewide run produced
# (data/surplus/grantor-sample.json). Bare "COLLECTOR" matched a man named
# Philip Collector; bare "TAX SALE" matched investor LLCs. Both are out.
# "%COLLECTOR OF%" rather than "OF TAXES": SDAT truncates the field, so
# "PHILLIP G THOMPSON COLLECTOR OF TA" is real. The surname case has no "OF".
COLLECTOR_GRANTOR = ("%COLLECTOR OF%", "%TAX COLLECTOR%", "%DIRECTOR OF FINANCE%", "%TREASURER%")
NOT_A_COLLECTOR = ("LLC", "L.L.C", "INC", "CORP", "LTD", "TRUST", "INVESTORS", "HOLDINGS", "PARTNERS")

# An entity like "TAX SALE HOLDINGS, LLC" as grantor is a tax-sale purchaser who
# took the deed and has since sold the property on. The collector's deed is
# then the transfer BEFORE this one (grantor2). The surplus was owed to whoever
# owned it before the collector's deed, whenever that was.
INVESTOR_GRANTOR = ("%TAX SALE%", "%TAX LIEN%", "%TAXSALE%")

# TP 14-847: at 105 days the court may vest title "in the governing body of the
# county or municipal corporation in fee simple". A collector deed TO a county is
# that outcome -- the foreclosure collapsed and the county took it.
COUNTY_OWNER = ("%COUNTY COMMISSIONER%", "%BOARD OF COUNTY%", "%MAYOR AND CITY COUNCIL%",
                "%COUNTY OF %", "%COUNTY, MARYLAND%", "%COUNTY MARYLAND%",
                "%CITY OF %", "%TOWN OF %", "%COUNTY EXECUTIVE%")


def _f(v):
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# Actual sale dates by county and year (docs/tax-sale-sources.md). The 6-month
# and 2-year clocks in 14-833 run from these, so they are not guessed.
SALE_DATES = {
    2026: {"Allegany": (5, 28), "Anne Arundel": (6, 3), "Baltimore City": (5, 18), "Baltimore County": (8, 27),
           "Calvert": (5, 22), "Caroline": (8, 21), "Carroll": (6, 29), "Cecil": (6, 1), "Charles": (5, 12),
           "Dorchester": (5, 19), "Frederick": (5, 11), "Garrett": (5, 18), "Harford": (6, 3), "Howard": (6, 10),
           "Kent": (5, 21), "Montgomery": (6, 8), "Prince George's": (5, 11), "Queen Anne's": (5, 19),
           "St. Mary's": (3, 6), "Somerset": (6, 11), "Talbot": (5, 20), "Washington": (6, 2),
           "Wicomico": (6, 9), "Worcester": (6, 9)},
}


def _parse_sale_date(rec):
    year = int(rec.get("year") or date.today().year)
    md = SALE_DATES.get(year, {}).get(rec.get("county"))
    if not md:
        # Counties hold their sale in the same week every year; for a backfilled
        # year use that county's most recent known date. Any such sale is already
        # past the 2-year mark, so a few days either way changes no stage.
        for y in sorted(SALE_DATES, reverse=True):
            md = SALE_DATES[y].get(rec.get("county"))
            if md:
                break
    if md:
        return date(year, *md)
    return date(year, 6, 1)


def load_taxsale_records():
    """Every tax-sale record we hold, across all years, keyed by SDAT account."""
    out = {}
    if not os.path.isdir(config.TAXSALE_DIR):
        return out
    for name in sorted(os.listdir(config.TAXSALE_DIR)):
        if not name.startswith("taxsale-") or not name.endswith(".json"):
            continue
        with open(os.path.join(config.TAXSALE_DIR, name), encoding="utf-8") as f:
            blob = json.load(f)
        for acct, rec in (blob.get("records") or {}).items():
            prev = out.get(acct)
            # keep the most recent year for an account that sold more than once
            if prev is None or int(rec.get("year") or 0) >= int(prev.get("year") or 0):
                rec = dict(rec)
                rec["_repeat"] = bool(prev) and prev.get("year") != rec.get("year")
                out[acct] = rec
    return out


def load_watch():
    if not os.path.exists(WATCH_PATH):
        return {}
    try:
        with open(WATCH_PATH, encoding="utf-8") as f:
            return (json.load(f) or {}).get("accounts", {})
    except (OSError, ValueError):
        return {}


def sdat_rows(db, accounts):
    """Fetch the title-relevant SDAT columns for the accounts we watch."""
    cur = db.cursor()
    cols = ("acct, county, address, city, zip, lat, lon, owner1, owner2, transfer_date, sale_price, "
            "deed_liber, deed_folio, grantor1, land_value, impr_value, year_built, occupancy, "
            "mail_addr, mail_city, mail_zip")
    got = {}
    accts = list(accounts)
    for i in range(0, len(accts), 800):
        chunk = accts[i:i + 800]
        q = f"SELECT {cols} FROM parcels WHERE acct IN ({','.join('?' * len(chunk))})"
        for r in cur.execute(q, chunk):
            got[r[0]] = dict(zip([c.strip() for c in cols.split(",")], r))
    return got


def _norm_owner(v):
    return " ".join((v or "").upper().split())


def _notice_due(transfer_date, sale_dt):
    """14-818(a)(6): the collector must notify the prior owner within 90 days of the deed.

    Only meaningful if the recorded transfer post-dates the tax sale. SDAT keeps the
    prior deed date until the new one is indexed, so a pre-sale date here means the
    conveyance has not been recorded yet and there is no deadline to compute.
    """
    t = (transfer_date or "").strip().replace(".", "-")[:10]
    try:
        d = date.fromisoformat(t)
    except ValueError:
        return None
    if d < sale_dt:
        return None
    return (d + timedelta(days=NOTICE_DEADLINE_DAYS)).isoformat()


def stage(rec, sale_dt, owner_changed, today, bid_to_av=None, bid_to_face=None):
    age = (today - sale_dt).days
    if owner_changed:
        return "CONVEYED", "Deed conveyed — purchaser paid the residue of the bid"
    stranded = ((bid_to_av is not None and bid_to_av >= LIEN_STRANDED_MIN_BID_TO_AV)
                or (bid_to_av is None and bid_to_face is not None and bid_to_face >= LIEN_STRANDED_MIN_BID_TO_FACE))
    if stranded:
        return "LIEN_STRANDED", ("Bid far above the property's value — taking the deed would cost the purchaser more "
                                 "than the property is worth, so they almost certainly never will. The owner keeps it "
                                 "and the certificate goes void at 2 years. Still delinquent, and nobody is competing "
                                 "for it.")
    if age < REDEMPTION_WAIT_DAYS:
        return "TOO_EARLY", "Inside the statutory wait — no foreclosure may be filed yet"
    if age <= CERT_VOID_DAYS:
        return "FORECLOSURE_WINDOW", "Foreclosure may be filed and title has not moved — owner can still sell"
    return "CERT_STALE", ("Over 2 years since the sale and the SDAT owner never changed. Could be a void "
                          "certificate (TP 14-833 — your opening), a redemption, a case still pending, or a "
                          "recorded deed SDAT has not indexed. Check the county tax account before calling.")


def _like(col, pats):
    return "(" + " OR ".join(f"{col} LIKE ?" for _ in pats) + ")"


def scan_collector_deeds(db, today):
    """Every parcel whose last deed came from a tax collector, any year.

    This is the answer to "how far back does it go": it does not depend on our
    tax-sale lists at all. A collector deed to a private party means the
    purchaser paid the residue of the bid, so a balance was owed to whoever owned
    it before -- that is a surplus lead however old it is. A collector deed to a
    county is the 14-847 failed-foreclosure outcome instead.
    """
    cur = db.cursor()
    cols = ("acct, county, address, city, zip, lat, lon, owner1, owner2, transfer_date, sale_price, "
            "deed_liber, deed_folio, grantor1, grantor2, transfer_date2, land_value, impr_value, year_built, "
            "occupancy, legal1, land_use")
    pats = COLLECTOR_GRANTOR + INVESTOR_GRANTOR
    q = (f"SELECT {cols} FROM parcels WHERE {_like('UPPER(grantor1)', pats)} "
         f"OR {_like('UPPER(grantor2)', COLLECTOR_GRANTOR)}")
    names = [c.strip() for c in cols.split(",")]
    out, grantor_counts = [], Counter()

    def is_collector(g):
        g = (g or "").upper()
        return (any(_glob(g, p) for p in COLLECTOR_GRANTOR)
                and not any(tok in g for tok in NOT_A_COLLECTOR))

    def is_investor(g):
        g = (g or "").upper()
        return any(_glob(g, p) for p in INVESTOR_GRANTOR) and any(tok in g for tok in NOT_A_COLLECTOR)

    for r in cur.execute(q, pats + COLLECTOR_GRANTOR):
        row = dict(zip(names, r))
        g1, g2 = row.get("grantor1"), row.get("grantor2")
        if is_collector(g1):
            deed_grantor, conveyed_on, resold = g1, row["transfer_date"], False
            grantor_counts[(g1 or "").strip().upper()[:60]] += 1
        elif is_collector(g2):
            # collector deed one transfer back; the purchaser has since sold it on
            deed_grantor, conveyed_on, resold = g2, row["transfer_date2"], True
            grantor_counts[(g2 or "").strip().upper()[:60]] += 1
        elif is_investor(g1):
            # investor flipped it; the collector deed before it was not captured by
            # SDAT's two-transfer window, so its date is unknown
            deed_grantor, conveyed_on, resold = g1, None, True
            grantor_counts["(investor) " + (g1 or "").strip().upper()[:48]] += 1
        else:
            continue
        owner = (row.get("owner1") or "").upper()
        is_county = any(_glob(owner, p) for p in COUNTY_OWNER) and not resold
        year = (conveyed_on or "")[:4]
        out.append({
            "account": row["acct"], "county": row["county"],
            "address": row["address"], "city": row["city"], "zip": row["zip"],
            "lat": row["lat"], "lon": row["lon"], "year_built": row["year_built"],
            "owner_now": row["owner1"], "owner2": row["owner2"],
            "grantor": deed_grantor, "prior_grantor": row["grantor2"], "resold": resold,
            "deed": f"{row.get('deed_liber') or ''}/{row.get('deed_folio') or ''}".strip("/"),
            "conveyed_on": conveyed_on,
            "conveyed_year": int(year) if year.isdigit() else None,
            # Consideration recited on the collector's deed. On a Maryland tax deed
            # this is normally the full purchase price -- the bid -- but it is NOT
            # the surplus: that is this figure less taxes, interest, penalties and
            # costs of sale, which only the collector's record shows.
            "consideration": _f(row["sale_price"]) or None,
            "assessed_value": (_f(row["land_value"]) + _f(row["impr_value"])) or None,
            "improvements": _f(row["impr_value"]) or 0,
            "vacant_lot": _f(row["impr_value"]) == 0 and not str(row.get("year_built") or "").strip("0 "),
            "legal": (row.get("legal1") or "").strip(),
            "land_use": row.get("land_use"),
            "occupancy": row["occupancy"],
            "kind": "COUNTY_VESTED" if is_county else "TAX_DEED",
        })
    return out, grantor_counts


def _glob(value, like_pattern):
    """Match a SQL LIKE pattern in Python (only % wildcards are used here)."""
    import re as _re
    rx = "^" + ".*".join(_re.escape(part) for part in like_pattern.split("%")) + "$"
    return bool(_re.match(rx, value or ""))


def build(index_path, today=None):
    today = today or date.today()
    taxsale = load_taxsale_records()
    if not taxsale:
        log.warning("no tax-sale data in %s", config.TAXSALE_DIR)
        return
    prior = load_watch()
    db = sqlite3.connect(index_path)
    rows = sdat_rows(db, taxsale.keys())
    historical, grantor_counts = scan_collector_deeds(db, today)
    db.close()
    log.info("collector-deed scan: %d parcels conveyed by a tax collector (any year)", len(historical))
    log.info("watching %d tax-sale accounts; %d matched in SDAT", len(taxsale), len(rows))

    watch, events, leads = {}, [], []
    skipped_unconfirmed = 0
    for acct, rec in taxsale.items():
        s = rows.get(acct)
        if not s:
            continue
        # LISTED means advertised, outcome unknown -- often redeemed before the
        # sale. Nothing here can be said to have "sold", so it has no place on a
        # tab about what happened after a sale. (It is still a distress signal
        # on the Title Leads tab.)
        if rec.get("status") not in ("SOLD", "STRUCK"):
            skipped_unconfirmed += 1
            continue
        owner_now = _norm_owner(s["owner1"])
        prev = prior.get(acct) or {}
        owner_before = _norm_owner(prev.get("owner1"))
        transfer_now = (s.get("transfer_date") or "").strip()
        sale_dt = _parse_sale_date(rec)

        av = _f(s["land_value"]) + _f(s["impr_value"])
        bid, face = _f(rec.get("bid")), _f(rec.get("face"))
        if not bid and rec.get("bid_factor") and av > 0:
            bid = round(float(rec["bid_factor"]) * av, 2)     # Montgomery publishes bid as a share of assessment
        est_surplus = round(bid - face, 2) if bid and face else None
        bid_to_av = round(bid / av, 3) if bid and av > 10000 else None
        bid_to_face = round(bid / face, 1) if bid and face else None

        # A change is only meaningful against a baseline we actually recorded.
        owner_changed = bool(owner_before) and owner_before != owner_now
        st, why = stage(rec, sale_dt, owner_changed, today, bid_to_av, bid_to_face)

        watch[acct] = {
            "owner1": s["owner1"], "transfer_date": transfer_now,
            "deed": f"{s.get('deed_liber') or ''}/{s.get('deed_folio') or ''}".strip("/"),
            "first_seen": prev.get("first_seen") or _now(),
            "last_seen": _now(),
        }

        if owner_changed:
            events.append({
                "account": acct, "county": rec.get("county"), "address": s.get("address") or rec.get("address"),
                "detected": _now(), "owner_before": prev.get("owner1"), "owner_after": s["owner1"],
                "transfer_date": transfer_now, "deed": watch[acct]["deed"],
                "sale_year": rec.get("year"), "bid": bid or None, "taxes_owed": face or None,
                "est_surplus": est_surplus,
                "deed_recorded": _notice_due(transfer_now, sale_dt) is not None,
                "notice_due_by": _notice_due(transfer_now, sale_dt),
            })

        leads.append({
            "account": acct, "county": rec.get("county"), "stage": st, "stage_why": why,
            "address": s.get("address") or rec.get("address"), "city": s.get("city"), "zip": s.get("zip"),
            "lat": s.get("lat"), "lon": s.get("lon"), "year_built": s.get("year_built"),
            "deed": f"{s.get('deed_liber') or ''}/{s.get('deed_folio') or ''}".strip("/"),
            "grantor": s.get("grantor1"), "occupancy": s.get("occupancy"),
            "owner_of_record": s["owner1"], "owner2": s.get("owner2"),
            "mail": " ".join(x for x in (s.get("mail_addr"), s.get("mail_city"), s.get("mail_zip")) if x),
            "owner_at_sale": rec.get("owner"), "tax_sale_year": rec.get("year"),
            "tax_sale_status": rec.get("status"), "sold_to": rec.get("bidder"),
            "taxes_owed": face or None, "winning_bid": bid or None,
            "est_surplus": est_surplus, "assessed_value": av or None, "bid_to_assessed": bid_to_av,
            "deed_rational": (bid_to_av is not None and bid_to_av <= DEED_RATIONAL_MAX_BID_TO_AV),
            "bid_to_face": bid_to_face,
            "days_since_sale": (today - sale_dt).days,
            "repeat_sale": rec.get("_repeat", False), "partial": rec.get("partial", False),
            "transfer_date": transfer_now,
            "source": rec.get("source"),
        })

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(WATCH_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": _now(), "note":
                   "Baseline of SDAT owner-of-record for every tax-sale account. A change here means the "
                   "foreclosure completed and the deed conveyed. Committed so week-over-week diffs work.",
                   "accounts": watch}, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    old_events = []
    if os.path.exists(EVENTS_PATH):
        try:
            old_events = (json.load(open(EVENTS_PATH, encoding="utf-8")) or {}).get("events", [])
        except (OSError, ValueError):
            pass
    seen = {(e["account"], e.get("transfer_date")) for e in old_events}
    merged = old_events + [e for e in events if (e["account"], e.get("transfer_date")) not in seen]
    with open(EVENTS_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": _now(), "events": merged}, f, ensure_ascii=False, indent=1)

    # Fold the historical collector deeds in. These need no baseline and no
    # tax-sale list, so they reach back as far as SDAT records the deed.
    known = {r["account"] for r in leads}

    def is_coll(g):
        g = (g or "").upper()
        return any(_glob(g, p) for p in COLLECTOR_GRANTOR) and not any(t in g for t in NOT_A_COLLECTOR)

    floor_dropped = 0
    for h in historical:
        if h["kind"] != "TAX_DEED" or h["account"] in known:
            continue
        # A $900 deed on a paper lot carries no surplus worth a phone call. Keep
        # real houses and real money; drop the sweeps of unbuildable lots.
        if (h["assessed_value"] or 0) < config.MIN_ASSESSED_VALUE and (h["consideration"] or 0) < 10000:
            floor_dropped += 1
            continue
        ts = taxsale.get(h["account"]) or {}
        bid, face = _f(ts.get("bid")), _f(ts.get("face"))
        bid = bid or h["consideration"] or 0
        cy = h["conveyed_year"]
        leads.append({
            "account": h["account"], "county": h["county"], "stage": "CONVEYED",
            "stage_why": ("Tax-sale purchaser took the deed and has since sold the property on" if h["resold"]
                          else "Deed executed by the tax collector — the foreclosure completed and the residue was paid"),
            "address": h["address"] or (h["legal"][:60] if h["legal"] else None), "city": h["city"], "zip": h["zip"],
            "no_situs": not h["address"], "legal": h["legal"], "vacant_lot": h["vacant_lot"],
            "lat": h["lat"], "lon": h["lon"], "year_built": h["year_built"] if not h["vacant_lot"] else None,
            "deed": h["deed"], "grantor": h["grantor"], "occupancy": h["occupancy"],
            "owner_of_record": h["owner_now"], "owner2": h["owner2"], "mail": "",
            "owner_at_sale": ts.get("owner"), "tax_sale_year": ts.get("year") or cy,
            "tax_sale_status": ts.get("status") or "",
            "sold_to": ts.get("bidder") or (h["grantor"] if h["resold"] and not is_coll(h["grantor"]) else h["owner_now"]),
            "resold": h["resold"],
            "taxes_owed": face or None,
            "winning_bid": _f(ts.get("bid")) or None,            # only a real bid from a sale list
            "consideration": h["consideration"],                  # what the collector's deed recites
            "est_surplus": round(bid - face, 2) if bid and face else None,
            "assessed_value": h["assessed_value"],
            "consideration_to_assessed": (round(h["consideration"] / h["assessed_value"], 3)
                                          if h["consideration"] and (h["assessed_value"] or 0) > 10000 else None),
            "bid_to_assessed": None, "deed_rational": True, "days_since_sale": None, "repeat_sale": False,
            "years_since_conveyance": (today.year - cy) if cy else None,
            "transfer_date": h["conveyed_on"], "historical": True,
            "source": "SDAT collector deed" + (f" · {h['conveyed_year']}" if h["conveyed_year"] else ""),
        })

    log.info("skipped %d LISTED/unconfirmed rows (no sale outcome to track); %d low-value collector deeds below the floor",
             skipped_unconfirmed, floor_dropped)
    before = len(leads)
    leads = [r for r in leads if r["stage"] in OPPORTUNITY_STAGES]
    log.info("kept %d opportunities, dropped %d not-yet-actionable rows", len(leads), before - len(leads))
    log.info("  by stage: %s", dict(Counter(r["stage"] for r in leads)))

    rank = {"CONVEYED": 0, "FORECLOSURE_WINDOW": 1, "LIEN_STRANDED": 2, "CERT_STALE": 3}
    leads.sort(key=lambda r: (rank.get(r["stage"], 9),
                              -(r.get("est_surplus") or 0) if r["stage"] == "CONVEYED"
                              else -(r.get("assessed_value") or 0)))
    with open(LEADS_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": _now(), "rows": leads}, f, ensure_ascii=False, separators=(",", ":"))

    vested = [h for h in historical if h["kind"] == "COUNTY_VESTED"]
    with open(VESTED_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": _now(), "note":
                   "TP 14-847: where a certificate holder does not comply with the final judgment within 105 days, "
                   "the court may vest title in the county in fee simple. A collector deed to a county is that "
                   "outcome. The 90-day window before vesting lives only in the court docket and is not detectable here.",
                   "rows": sorted(vested, key=lambda r: -(r.get("conveyed_year") or 0))}, f,
                  ensure_ascii=False, separators=(",", ":"))

    # So we can see what the real grantor strings look like and tighten the patterns.
    with open(os.path.join(OUT_DIR, "grantor-sample.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": _now(), "top_matching_grantors": grantor_counts.most_common(60)},
                  f, ensure_ascii=False, indent=1)
    log.info("county-vested (14-847): %d", len(vested))

    c = Counter(r["stage"] for r in leads)
    log.info("stages: %s", dict(c))
    log.info("new conveyance events this run: %d (total %d)", len(merged) - len(old_events), len(merged))
    if not prior:
        log.info("FIRST RUN — baseline only. Conveyances become detectable from the next run.")
    return leads


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", default=config.INDEX_PATH)
    a = ap.parse_args()
    build(a.index)
