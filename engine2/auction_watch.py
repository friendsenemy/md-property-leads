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
DAILY_SOLD_PAGES = 3          # 100 results each, newest first: roughly the last 4-5 months
KEEP_SOLD_DAYS = 180          # a sold lead stays on the live board this long (court-registry phase)
ARCHIVE_AFTER_DAYS = 180      # older than this is "archive"

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


OLD_LOAN_YEARS = 15           # past this, the purchase loan tells us little: the debt being
                              # foreclosed is a refinance, HELOC or reverse mortgage we cannot see
DEPOSIT_MULTIPLE = 10         # trustees set the deposit near 10% of the debt / opening bid
DEPOSIT_MIN_SIGNAL = 15000    # a flat $10k deposit is a floor convention, not a signal


def estimate_payoff(purchase_price, purchase_year, sale_year, deposit=None, assessed=0):
    """(low, mid, high, basis) payoff estimate. Two independent clues:
       * the owner's own purchase price + year, amortised (fresh loans only)
       * the trustee's required deposit x10
    A foreclosure PROVES a debt exists, so an old purchase never yields $0: without
    a deposit signal the estimate widens up toward value. Returns None when there
    is nothing to go on."""
    model = None
    if purchase_price and purchase_price >= 20000 and purchase_year:
        yrs = max(sale_year - purchase_year, 0)
        base = remaining_balance(purchase_price * ASSUMED_LTV, ASSUMED_RATE, yrs) * (1 + ARREARS_FACTOR)
        model = (base, yrs)
    dep = deposit * DEPOSIT_MULTIPLE if deposit and deposit >= DEPOSIT_MIN_SIGNAL else None

    if model and model[1] <= OLD_LOAN_YEARS:
        m = model[0]
        if dep:
            mid = max(m, dep)
            return (min(m, dep) * 0.9, mid, mid * 1.15, "purchase loan + deposit")
        return (m * 0.85, m, m * 1.2, "purchase loan")
    if dep:
        return (dep * 0.8, dep, dep * 1.3, "deposit x10" + (" (purchase too old to model)" if model else ""))
    if model:
        # old loan, no deposit clue: anywhere from the amortised remainder up to most of the value
        lo = model[0]
        return (lo, max(lo, 0.5 * assessed), max(lo, 0.85 * assessed), "old purchase — debt is a later loan, size unknown")
    return None


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


# --- Tidewater Auctions (scheduled sales only; they publish no results) --------
TIDEWATER_URL = "https://www.tidewaterauctions.com/upcoming-real-estate-auctions/"
TIDEWATER_WEEKS = 3        # this week + the next two (the page offers a week dropdown)


def tidewater_lots():
    """Every scheduled sale Tidewater lists for the next few weeks, as SCHEDULED
    lots. Cancelled rows (hdnCancelled=1) are dropped. The week dropdown is an
    ASP.NET postback, so weeks 2+ are fetched by re-posting the form."""
    out = []
    sess = requests.Session()
    html = sess.get(TIDEWATER_URL, headers=UA, timeout=60).text
    out += _tidewater_parse(html)
    for wk in range(1, TIDEWATER_WEEKS):
        form = dict(re.findall(r'<input type="hidden" name="(__[A-Z]+)"[^>]*value="([^"]*)"', html))
        if "__VIEWSTATE" not in form:
            break
        form.update({"ctl00$ContentPlaceHolder1$ddlWeeks": str(wk), "ctl00$ContentPlaceHolder1$ddlCounties": "all",
                     "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlWeeks", "__EVENTARGUMENT": ""})
        try:
            html = sess.post(TIDEWATER_URL, data=form, headers={**UA, "Referer": TIDEWATER_URL}, timeout=60).text
        except requests.RequestException:
            break
        out += _tidewater_parse(html)
    seen = set(); uniq = []
    for l in out:
        if l["lot_id"] in seen:
            continue
        seen.add(l["lot_id"]); uniq.append(l)
    return uniq


def _tidewater_parse(html):
    lots = []
    for b in html.split('<div class="us-block">')[1:]:
        d = re.search(r'lblDate_\d+">(\d\d)/(\d\d)/(\d\d)<', b)
        county = re.search(r'lblName_\d+">([^<]+)<', b)
        if not d:
            continue
        sale_date = f"20{d.group(3)}-{d.group(1)}-{d.group(2)}"
        for it in re.findall(r'<div class="us-sale-item">(.*?)<div class="us-sale-ad">', b, re.S):
            if re.search(r'hdnCancelled_\d+" value="1"', it):
                continue
            addr = (re.search(r'lblAddressText_\d+">([^<]+)<', it)
                    or re.search(r'maps\.google\.com/maps\?daddr=([^"]+)"', it))
            dep = re.search(r'lblDeposit_\d+">\$?([\d,\.]+)<', it)
            tm = re.search(r'lblTime_\d+">([^<]+)<', it)
            client = re.search(r'lblClient_\d+">([^<]+)<', it)
            if not addr:
                continue
            a = re.sub(r"^(HUD SALE:\s*)", "", addr.group(1).strip())
            a = a.split(" - ALL DEPOSITS")[0].strip()
            m = re.match(r"^(.*?),\s*([^,]+),\s*(MD|DC)\s*(\d{5})", a)
            if not m or m.group(3) != "MD":
                continue
            street, city, _, zip5 = m.groups()
            unit = None
            um = re.search(r",?\s*(Unit|Apt|#)\s*([\w-]+)$", street, re.I)
            if um:
                unit = um.group(2); street = street[:um.start()].rstrip(", ")
            cty = county.group(1).strip() if county else ""
            lots.append({
                "lot_id": f"tw-{sale_date}-{re.sub(r'[^A-Z0-9]', '', (street + zip5).upper())}",
                "kind": "SCHEDULED", "status": "active",
                "address": street, "unit": unit, "city": city.strip(), "zip": zip5,
                "county_hint": cty, "lat": None, "lon": None,
                "hammer": None, "deposit": _f(dep.group(1)) if dep else None,
                "sale_date": sale_date,
                "auction_title": f"{(tm.group(1).strip() if tm else '')} · {cty} · client {client.group(1).strip() if client else ''}".strip(" ·"),
                "lawyer_code": client.group(1).strip() if client else None,
                "source": "tidewaterauctions.com", "url": TIDEWATER_URL, "last_updated": None,
            })
    return lots


# --- SDAT trustee deeds: every completed foreclosure, whoever auctioned it -----
TRUSTEE_DEED_LOOKBACK_DAYS = 270     # deed records 1-3 months after the sale
TRUSTEE_GRANTOR = ("%SUB%TRUSTEE%", "%SUBSTITUTE TR%", "%TRUSTEES%", "%SUB TRS%", "%SUBST TR%")
NOT_A_FORECLOSURE = re.compile(r"\b(FAMILY|LIVING|REV(OCABLE)?|IRREV(OCABLE)?|L/T|LIV TR|FAM TR|ESTATE OF|EST OF|CHURCH|FOUNDATION|BANKRUPTCY|CHAPTER 7|CH 7)\b")
REO_OWNER = re.compile(r"\b(BANK|SAVINGS|MORTGAGE|MTG|LENDING|LOAN|FEDERAL NATIONAL|FANNIE|FREDDIE|FEDERAL HOME|HUD|SECRETARY OF HOUSING|"
                       r"VETERANS AFFAIRS|CREDIT UNION|TRUSTEE|TRUST\b|WILMINGTON|DEUTSCHE|WELLS FARGO|NATIONSTAR|SHELLPOINT|BAYVIEW|"
                       r"US BANK|U S BANK|NEWREZ|CARRINGTON|PENNYMAC|LAKEVIEW|FREEDOM MORTGAGE|ROCKET|MR COOPER|SELECT PORTFOLIO|REO\b|"
                       r"ASSET|FUNDING|CAPITAL|FINANCIAL|SERVICING|HOLDINGS LLC|ACQUISITIONS?)\b")
DEED_SALE_LAG_DAYS = 75              # typical gap from auction day to the recorded trustee's deed


def trustee_deeds(db, today):
    """Every parcel whose latest deed came from substitute trustees in the
    lookback window: a completed mortgage foreclosure, whoever ran the auction.
    The consideration on the deed is the hammer price. The new owner tells us
    whether a third party outbid the lender (surplus possible) or the lender
    took it back (REO -- almost never a surplus)."""
    cutoff = (today.toordinal() - TRUSTEE_DEED_LOOKBACK_DAYS)
    cutoff_s = date.fromordinal(cutoff).strftime("%Y.%m.%d")
    cur = db.cursor()
    cols = ("acct, county, address, city, zip, lat, lon, owner1, owner2, mail_addr, mail_city, mail_zip, "
            "occupancy, land_use, land_value, impr_value, year_built, transfer_date, sale_price, grantor1, "
            "deed_liber, deed_folio, dwelling_type, transfer_date2, grantor2")
    names = [c.strip() for c in cols.split(",")]
    where = " OR ".join("UPPER(grantor1) LIKE ?" for _ in TRUSTEE_GRANTOR)
    q = f"SELECT {cols} FROM parcels WHERE transfer_date >= ? AND ({where})"
    out = []
    for r in cur.execute(q, (cutoff_s, *TRUSTEE_GRANTOR)):
        d = dict(zip(names, r))
        g = _norm(d.get("grantor1"))
        if NOT_A_FORECLOSURE.search(g):
            continue
        td = (d.get("transfer_date") or "").replace(".", "-")
        if not re.match(r"^(19|20)\d\d-\d\d-\d\d$", td):
            continue
        out.append(d)
    return out


def deed_lot(d):
    """Shape a trustee-deed parcel like an auction lot so the same pipeline runs."""
    td = (d.get("transfer_date") or "").replace(".", "-")
    deed_dt = date.fromisoformat(td)
    sale_dt = date.fromordinal(deed_dt.toordinal() - DEED_SALE_LAG_DAYS)
    return {
        "lot_id": f"deed-{d['acct']}-{td}", "kind": "SOLD", "status": "sold",
        "address": d.get("address"), "unit": None, "city": d.get("city"), "zip": (d.get("zip") or "")[:5],
        "county_hint": d.get("county"), "lat": d.get("lat"), "lon": d.get("lon"),
        "hammer": _f(d.get("sale_price")) or None, "deposit": None,
        "sale_date": sale_dt.isoformat(), "deed_date": td,
        "auction_title": f"Trustee's deed recorded {td} · grantor {d.get('grantor1')}",
        "lawyer_code": None, "source": "SDAT trustee deed", "url": "https://sdat.dat.maryland.gov/RealProperty/Pages/default.aspx",
        "last_updated": None, "from_deed": True, "_sdat": d,
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


RESIDENTIAL_USE = ("Residential", "Town House", "Residential Condominium", "Apartments")
HAMMER_MAX_TO_AV = 3.0        # a house does not sell at auction for 3x its assessment -- that is a data error
HAMMER_MIN_TO_AV = 0.15


def sanity(lot, av):
    """Older Alex Cooper rows store the price in cents, and a few are typos.
    Rescale when /100 lands in a plausible band; otherwise flag the row."""
    h = lot.get("hammer")
    if not h or not av:
        return lot, None
    if h > HAMMER_MAX_TO_AV * av and HAMMER_MIN_TO_AV * av <= h / 100 <= HAMMER_MAX_TO_AV * av:
        lot = {**lot, "hammer": round(h / 100, 2), "hammer_rescaled": True}
        return lot, None
    if h > HAMMER_MAX_TO_AV * av or h < HAMMER_MIN_TO_AV * av:
        return lot, f"hammer {h:,.0f} vs assessed {av:,.0f} — implausible, data error"
    return lot, None


def classify(lot, s, today):
    av = _f(s["land_value"]) + _f(s["impr_value"])
    lu = (s.get("land_use") or "")
    if lu and not (lu.startswith(RESIDENTIAL_USE) or lu.strip() in ("R", "TH", "RC", "M")):
        return None, [f"not residential ({lu.strip()})"], None, None, av, (s.get("owner1") or ""), None, None
    lot, bad = sanity(lot, av)
    if bad:
        return None, [bad], None, None, av, (s.get("owner1") or ""), None, None
    owner = (s.get("owner1") or s.get("owner2") or "").strip()
    ent = re.search(r"\b(LLC|L L C|INC|CORP|LTD|LP|LLP|TRUST|TRUSTEE|HOLDINGS?|PROPERTIES|ASSOC|BANK|CHURCH|PARTNERS)\b", owner.upper())
    purch_year = None
    td = (s.get("transfer_date") or "").strip()
    if re.match(r"^(19|20)\d\d", td):
        purch_year = int(td[:4])
    sale_year = int(lot["sale_date"][:4]) if lot.get("sale_date") else today.year
    est4 = estimate_payoff(_f(s.get("sale_price")), purch_year, sale_year, lot.get("deposit"), av)
    est, basis = (est4[:3], est4[3]) if est4 else (None, None)
    reasons = []
    tier = None
    hammer = lot.get("hammer")
    surplus = None
    # STRONG is judged against the HIGH end of the payoff range (conservative);
    # POSSIBLE against the middle.
    value = hammer if (lot["kind"] == "SOLD" and hammer) else (av if lot["kind"] == "SCHEDULED" else None)
    if value and est:
        lo, mid, hi = est
        surplus = (value - hi, value - mid, value - lo)
        word = "hammer" if lot["kind"] == "SOLD" else "assessed"
        if value - hi >= STRONG_MIN_SURPLUS and value >= hi * STRONG_MIN_RATIO:
            tier = "STRONG"; reasons.append(f"{word} {value:,.0f} vs est. payoff {mid:,.0f}–{hi:,.0f} ({basis})")
        elif value - mid >= POSSIBLE_MIN_SURPLUS:
            tier = "POSSIBLE"; reasons.append(f"{word} {value:,.0f} vs est. payoff {mid:,.0f} ({basis}) — thin or uncertain margin")
        else:
            reasons.append(f"{word} at or below estimated payoff ({basis}) — likely no surplus")
    elif value and not est:
        reasons.append("no purchase price and no deposit signal — cannot estimate the payoff")
    if ent:
        reasons.append("owner is an entity — the person behind it is the contact")
        if tier == "STRONG":
            tier = "POSSIBLE"
    if av and av < MIN_ASSESSED:
        tier = None; reasons.append("below the residential value floor")
    return tier, reasons, est, surplus, av, owner, purch_year, basis


_TRUSTEE = re.compile(r"\b(SUB(STITUTE)?\s+TRUSTEES?|TRUSTEES?|TR|TRS)\b")


def title_state(lot, s):
    """Has the trustee's deed already reached SDAT? If the parcel's last transfer
    is AFTER the sale date, the owner on record is the auction BUYER, not the
    person owed the surplus -- the former owner's name is then on the trustee's
    deed (grantor = substitute trustees), not in SDAT."""
    td = (s.get("transfer_date") or "").strip().replace(".", "-")
    sd = lot.get("sale_date") or ""
    if not sd or not re.match(r"^(19|20)\d\d-\d\d-\d\d", td):
        return "UNKNOWN"
    if td > sd:
        return "BUYER_ON_TITLE" if _TRUSTEE.search(_norm(s.get("grantor1"))) or (date.fromisoformat(td) - date.fromisoformat(sd)).days < 400 else "RESOLD"
    return "FORMER_OWNER_ON_TITLE"


def _norm(v):
    return " ".join((v or "").upper().split())


def collection_window(sale_date, today):
    """Where the money is right now, by age of the sale. Maryland mortgage
    foreclosure: report of sale -> ~30-45 days -> ratification -> auditor's
    account (another 1-4 months) -> surplus paid out of the court registry on
    motion. Registry funds unclaimed ~3 years go to the Comptroller (CL 17-3xx),
    where the owner can still claim them for free, forever."""
    if not sale_date:
        return None, None
    days = (today - date.fromisoformat(sale_date)).days
    if days < 120:
        return "TOO_FRESH", "Sold under 4 months ago: not ratified or audited yet — nobody can have been paid. You are first."
    if days < 1095:
        return "COURT_REGISTRY", ("Audit likely done; surplus sits in the Circuit Court registry until someone files a motion. "
                                  "Case Search docket shows an 'Order ... surplus' or 'disbursement' entry if it was paid.")
    return "COMPTROLLER", ("Over 3 years: unclaimed registry funds have usually been turned over to the Comptroller. "
                           "Search the former owner's name at claimitmd.gov — if it lists the Circuit Court as holder, "
                           "it is definitely unclaimed. Finder fees there are capped by CL §17-325; the owner claims free.")


ARCHIVE_PATH = os.path.join(OUT_DIR, "auction-archive.json")


def backfill_pages(pages, delay=1.2):
    """Walk Alex Cooper's sold-lots pages (100 per page, newest first; the server
    renders ?page=N&limit=100 without JavaScript). Returns SOLD lots."""
    import time
    out = []
    for n in range(1, pages + 1):
        try:
            html = fetch(f"{SOURCES['alexcooper_sold']}?page={n}&limit=100")
        except Exception as e:  # noqa: BLE001
            log.warning("backfill page %d: %s", n, e); break
        got = [x for x in (normalize_lot(l, "SOLD") for l in embedded_lots(html)) if x and x.get("hammer")]
        log.info("backfill page %d: %d sold foreclosure lots", n, len(got))
        if not got:
            break
        out += got
        time.sleep(delay)
    return out


def build(index_path, today=None, backfill=0):
    today = today or date.today()
    db = sqlite3.connect(index_path)
    db.execute("CREATE INDEX IF NOT EXISTS ix_zip ON parcels(zip)")
    lots = []
    if backfill:
        arch = backfill_pages(backfill)
        for l in arch:
            l["archive"] = True
        lots += arch
    for key, url in SOURCES.items():
        kind = "SOLD" if key.endswith("sold") else "SCHEDULED"
        urls = [f"{url}?page={n}&limit=100" for n in range(1, DAILY_SOLD_PAGES + 1)] if kind == "SOLD" else [url]
        for u in urls:
            try:
                html = fetch(u)
            except Exception as e:  # noqa: BLE001
                log.warning("%s: %s", key, e); continue
            raw = embedded_lots(html)
            got = [x for x in (normalize_lot(l, kind) for l in raw) if x]
            log.info("%s: %d lots embedded, %d foreclosure-shaped", u, len(raw), len(got))
            lots += got
    try:
        tw = tidewater_lots()
        log.info("tidewater: %d scheduled sales (MD, not cancelled)", len(tw))
        lots += tw
    except Exception as e:  # noqa: BLE001
        log.warning("tidewater: %s", e)

    try:
        deeds = trustee_deeds(db, today)
        log.info("sdat trustee deeds in the last %d days: %d", TRUSTEE_DEED_LOOKBACK_DAYS, len(deeds))
        lots += [deed_lot(d) for d in deeds]
    except Exception as e:  # noqa: BLE001
        log.warning("trustee deeds: %s", e)

    seen = {}
    if os.path.exists(SEEN_PATH):
        with open(SEEN_PATH, encoding="utf-8") as f:
            seen = json.load(f).get("lots", {})

    rows, unmatched, dropped = [], 0, 0
    for lot in lots:
        if lot["kind"] == "SOLD" and not lot.get("hammer"):
            continue
        if lot["kind"] == "SCHEDULED" and (lot.get("status") or "") not in ("active", "pre_sold", "upcoming", ""):
            continue                                       # cancelled / postponed
        s = lot.pop("_sdat", None) or match_sdat(db, lot)
        if not s:
            unmatched += 1; continue
        lot, _bad = sanity(lot, _f(s["land_value"]) + _f(s["impr_value"]))
        tier, reasons, est, surplus, av, owner, purch_year, basis = classify(lot, s, today)
        tstate = title_state(lot, s)
        if lot.get("from_deed") and not _bad and (s.get("land_use") or "Residential").startswith(RESIDENTIAL_USE):
            tstate = "BUYER_ON_TITLE"
            buyer = _norm(s.get("owner1"))
            h = lot.get("hammer") or 0
            if REO_OWNER.search(buyer) or not h:
                tier, reasons = None, ["lender took the property back (REO) — a credit bid, no surplus" if h else "no consideration on the deed"]
            else:
                # a third party outbid the lender. Without the loan amount we judge by how
                # far the price ran: near assessment on a long-held home is where surplus lives.
                ratio = h / av if av else 0
                yrs = None
                td2 = (s.get("transfer_date2") or "").strip()
                if re.match(r"^(19|20)\d\d", td2):
                    yrs = int(td2[:4])
                owned_since = yrs
                if ratio >= 0.85 and av >= MIN_ASSESSED and h >= 150000:
                    tier = "POSSIBLE"
                    reasons = [f"third party paid {h:,.0f} ({ratio:.0%} of assessed) — outbid the lender" + (f"; prior owner bought {owned_since}" if owned_since else "")]
                    if owned_since and today.year - owned_since >= 12 and ratio >= 0.95:
                        tier = "STRONG"; reasons.append("long-held with a price at full value — equity very likely")
                elif ratio >= 0.6 and av >= MIN_ASSESSED:
                    tier = "POSSIBLE"; reasons = [f"third party paid {h:,.0f} ({ratio:.0%} of assessed) — surplus depends on the loan balance"]
                else:
                    tier, reasons = None, [f"price {h:,.0f} is only {ratio:.0%} of assessed — unlikely to clear the loan"]
            est, basis, surplus = None, "no loan data on a deed — see the trustee's deed and the court case", None
            owner, purch_year = None, None
        elif tstate == "BUYER_ON_TITLE" and lot["kind"] == "SOLD" and not _bad and (s.get("land_use") or "Residential").startswith(RESIDENTIAL_USE):
            # SDAT already shows the auction buyer. Their purchase year/price is
            # the AUCTION, not a loan -- rebuild the estimate from the deposit only.
            est4 = estimate_payoff(0, None, int(lot["sale_date"][:4]), lot.get("deposit"), av)
            est, basis = (est4[:3], est4[3]) if est4 else (None, None)
            if est and lot.get("hammer"):
                lo, mid, hi = est; h = lot["hammer"]
                surplus = (h - hi, h - mid, h - lo)
                tier = "STRONG" if (h - hi >= STRONG_MIN_SURPLUS and h >= hi * STRONG_MIN_RATIO) else ("POSSIBLE" if h - mid >= POSSIBLE_MIN_SURPLUS else None)
                reasons = [f"hammer {h:,.0f} vs est. payoff {mid:,.0f}–{hi:,.0f} ({basis})", "title already moved to the auction buyer — former owner is named on the trustee's deed"]
            else:
                tier, reasons = None, ["title moved to the buyer and no deposit signal to estimate the payoff"]
            owner, purch_year = None, None
        win, win_note = collection_window(lot.get("sale_date"), today) if lot["kind"] == "SOLD" else (None, None)
        first_seen = seen.get(lot["lot_id"], {}).get("first_seen") or datetime.now(timezone.utc).isoformat(timespec="seconds")
        seen[lot["lot_id"]] = {"first_seen": first_seen, "kind": lot["kind"], "tier": tier}
        if not tier:
            dropped += 1; continue
        mail = " ".join(x for x in (s.get("mail_addr"), s.get("mail_city"), s.get("mail_zip")) if x)
        rows.append({
            **lot,
            "id": f"auction:{lot['lot_id']}",
            "stage": "AUCTION_SOLD" if lot["kind"] == "SOLD" else "AUCTION_SCHEDULED",
            "archive": bool(lot.get("archive")),
            "tier": tier, "why": "; ".join(reasons),
            "account": s["acct"], "county": s["county"],
            "sdat_address": s["address"], "sdat_city": s["city"],
            "owner_of_record": owner, "owner2": s.get("owner2") if owner else None,
            "title_state": tstate, "buyer_on_record": s.get("owner1") if tstate in ("BUYER_ON_TITLE", "RESOLD") else None,
            "trustee_deed": (f"{s.get('deed_liber') or ''}/{s.get('deed_folio') or ''}".strip("/") if tstate == "BUYER_ON_TITLE" else None),
            "from_deed": bool(lot.get("from_deed")), "deed_date": lot.get("deed_date"), "sale_date_estimated": bool(lot.get("from_deed")),
            "prior_owner_bought": (s.get("transfer_date2") or "")[:4] if lot.get("from_deed") else None,
            "collection_window": win, "collection_note": win_note,
            "mail": mail if owner else "", "absentee": bool(mail) and not mail.upper().startswith((s["address"] or "~").upper()[:8]),
            "occupancy": s.get("occupancy"), "land_use": s.get("land_use"), "dwelling_type": s.get("dwelling_type"),
            "assessed_value": av or None, "year_built": s.get("year_built"),
            "purchase_price": _f(s.get("sale_price")) or None, "purchase_year": purch_year,
            "purchase_deed": f"{s.get('deed_liber') or ''}/{s.get('deed_folio') or ''}".strip("/"),
            "grantor": s.get("grantor1"),
            "payoff_est": [round(x) for x in est] if est else None, "payoff_basis": basis,
            "surplus_est": [round(x) for x in surplus] if surplus else None,
            "ambiguous_match": s.get("_ambiguous"),
            "first_seen": first_seen,
            "is_new": first_seen[:10] == today.isoformat(),
        })

    rows.sort(key=lambda r: ((r["tier"] != "STRONG"), -(r["surplus_est"][1] if r["surplus_est"] else 0)))
    os.makedirs(OUT_DIR, exist_ok=True)
    for r in rows:
        age = (today - date.fromisoformat(r["sale_date"])).days if r.get("sale_date") else 0
        r["archive"] = bool(r["stage"] == "AUCTION_SOLD" and age > ARCHIVE_AFTER_DAYS)
    # carry forward: a sold lead found on an earlier day stays until it ages out,
    # even after it has scrolled off the auctioneer's first pages
    try:
        with open(OUT_PATH, encoding="utf-8") as f:
            prev = json.load(f).get("rows", [])
    except (OSError, ValueError):
        prev = []
    have = {r["lot_id"] for r in rows}
    for r in prev:
        if r.get("stage") == "AUCTION_SOLD" and r.get("lot_id") not in have and r.get("sale_date"):
            if (today - date.fromisoformat(r["sale_date"])).days <= KEEP_SOLD_DAYS:
                r["is_new"] = False
                rows.append(r); have.add(r["lot_id"])
    # dedupe by lot (same lot can appear on two pages across a day boundary)
    seen_ids = set(); dedup = []
    for r in rows:
        if r["lot_id"] in seen_ids:
            continue
        seen_ids.add(r["lot_id"]); dedup.append(r)
    rows = dedup
    rows.sort(key=lambda r: ((r["tier"] != "STRONG"), -(r["surplus_est"][1] if r["surplus_est"] else 0)))
    live_ids = {r["lot_id"] for r in rows if not r["archive"]}
    archive = [r for r in rows if r["archive"] and r["lot_id"] not in live_ids]
    if backfill:
        with open(ARCHIVE_PATH, "w", encoding="utf-8") as f:
            json.dump({"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                       "note": "One-time backfill of past foreclosure auction results (Alex Cooper, newest first). "
                               "Surplus is ESTIMATED from the deposit; the former owner is named on the trustee's deed.",
                       "pages": backfill, "rows": archive}, f, ensure_ascii=False, separators=(",", ":"))
        log.info("archive: %d rows written", len(archive))
    rows = [r for r in rows if not r["archive"]]
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "note": "Mortgage-foreclosure auction results matched to SDAT, with an ESTIMATED payoff from the "
                           "owner's own purchase. Only STRONG/POSSIBLE rows are kept. Estimates, not balances.",
                   "sources": {**SOURCES, "tidewater_upcoming": TIDEWATER_URL}, "counts": {"fetched": len(lots), "unmatched": unmatched, "below_bar": dropped,
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
    ap.add_argument("--backfill", type=int, default=0, help="also walk N pages (100 sales each) of past results into the archive")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    build(a.index, backfill=a.backfill)


if __name__ == "__main__":
    sys.exit(main())
