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
from datetime import date, datetime, timedelta, timezone

from engine2 import config

OUT_DIR = os.path.join("data", "surplus")
WATCH_PATH = os.path.join(OUT_DIR, "watch.json")
EVENTS_PATH = os.path.join(OUT_DIR, "events.json")
LEADS_PATH = os.path.join(OUT_DIR, "leads.json")

log = logging.getLogger("surplus")

# Statutory clock, Tax-Property Art. 14-833 / 14-847.
REDEMPTION_WAIT_DAYS = 183          # 6 months before a foreclosure may be filed
CERT_VOID_DAYS = 730                # 2 years from certificate: complaint or void
NOTICE_DEADLINE_DAYS = 90           # 14-818(a)(6) notice to the prior owner

# A purchaser only pays the residue of the bid if taking the deed is worth it.
# Bid at or below this share of assessed value => deeding is rational, so the
# case is likely to complete and produce a surplus.
DEED_RATIONAL_MAX_BID_TO_AV = 0.85


def _f(v):
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_sale_date(rec):
    """Approximate the sale date from the source label, else use the tax year."""
    src = (rec.get("source") or "")
    for token, mmdd in (("May", (5, 15)), ("Jun", (6, 15)), ("Apr", (4, 15)),
                        ("Mar", (3, 15)), ("Jul", (7, 15))):
        if token in src:
            return date(int(rec.get("year") or date.today().year), *mmdd)
    return date(int(rec.get("year") or date.today().year), 6, 1)


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
    cols = ("acct, county, address, city, owner1, owner2, transfer_date, sale_price, "
            "deed_liber, deed_folio, grantor1, land_value, impr_value, occupancy, mail_addr, mail_city, mail_zip")
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


def stage(rec, sale_dt, owner_changed, today):
    age = (today - sale_dt).days
    if owner_changed:
        return "CONVEYED", "Deed conveyed — purchaser paid the residue of the bid"
    if age < REDEMPTION_WAIT_DAYS:
        return "TOO_EARLY", "Inside the statutory wait — no foreclosure may be filed yet"
    if age <= CERT_VOID_DAYS:
        return "FORECLOSURE_WINDOW", "Foreclosure may be filed and title has not moved — owner can still sell"
    return "CERT_STALE", "Past the 2-year certificate deadline and title never moved"


def build(index_path, today=None):
    today = today or date.today()
    taxsale = load_taxsale_records()
    if not taxsale:
        log.warning("no tax-sale data in %s", config.TAXSALE_DIR)
        return
    prior = load_watch()
    db = sqlite3.connect(index_path)
    rows = sdat_rows(db, taxsale.keys())
    db.close()
    log.info("watching %d tax-sale accounts; %d matched in SDAT", len(taxsale), len(rows))

    watch, events, leads = {}, [], []
    for acct, rec in taxsale.items():
        s = rows.get(acct)
        if not s:
            continue
        owner_now = _norm_owner(s["owner1"])
        prev = prior.get(acct) or {}
        owner_before = _norm_owner(prev.get("owner1"))
        transfer_now = (s.get("transfer_date") or "").strip()
        sale_dt = _parse_sale_date(rec)

        # A change is only meaningful against a baseline we actually recorded.
        owner_changed = bool(owner_before) and owner_before != owner_now
        st, why = stage(rec, sale_dt, owner_changed, today)

        av = _f(s["land_value"]) + _f(s["impr_value"])
        bid, face = _f(rec.get("bid")), _f(rec.get("face"))
        est_surplus = round(bid - face, 2) if bid and face else None
        bid_to_av = round(bid / av, 3) if bid and av > 10000 else None

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
            "address": s.get("address") or rec.get("address"), "city": s.get("city"),
            "owner_of_record": s["owner1"], "owner2": s.get("owner2"),
            "mail": " ".join(x for x in (s.get("mail_addr"), s.get("mail_city"), s.get("mail_zip")) if x),
            "owner_at_sale": rec.get("owner"), "tax_sale_year": rec.get("year"),
            "tax_sale_status": rec.get("status"), "sold_to": rec.get("bidder"),
            "taxes_owed": face or None, "winning_bid": bid or None,
            "est_surplus": est_surplus, "assessed_value": av or None, "bid_to_assessed": bid_to_av,
            "deed_rational": (bid_to_av is not None and bid_to_av <= DEED_RATIONAL_MAX_BID_TO_AV),
            "days_since_sale": (today - sale_dt).days,
            "repeat_sale": rec.get("_repeat", False),
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

    leads.sort(key=lambda r: (r["stage"] != "CONVEYED", -(r.get("est_surplus") or 0)))
    with open(LEADS_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": _now(), "rows": leads}, f, ensure_ascii=False, separators=(",", ":"))

    from collections import Counter
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
