"""Judgment Watch: every judgment and lien recorded in a county, matched to the property owners it hits.

Tim Bender's lien-leverage play: liens and judgments make an owner think a house can't be sold,
so they'll sign over their interest cheaply. The money is in the ones that are old / about to
expire, negotiable, only on one co-owner, or stacked on a dead or absent owner's property.

Three phases, all run by .github/workflows/judgment-watch.yml (GitHub's free minutes); the fetch
runs as parallel county x 2-year-shard jobs:

  fetch   Pull every judgment / lien from the Maryland Judiciary Judgment and Liens Search
          (jportal.mdcourts.gov/judgment) by county + filing-date window — no name needed.
          The site shows at most 500 per search, so windows are split until they fit.
          A 12-year backfill runs newest -> oldest and resumes from a cursor across runs;
          every run also re-pulls the last 60 days so new filings and payoffs land.
          Raw rows: data/judgments/raw/<county>.json.gz (open AND satisfied, so payoffs drop out).
          Ray directed this automated use of the site (Nov 9, 2026) and takes responsibility for it.

  match   Match each OPEN judgment's debtor(s) to individual owners in the SDAT index for the
          same county (a Maryland judgment lien attaches to land in the county where it is
          indexed), flag and score, and write data/judgments/leads.json for the Lien Leverage tab.

  python -m engine2.judgment_watch plan [--refresh]                      # GitHub job matrix
  python -m engine2.judgment_watch fetch --county charles --shard 0 [--budget-min 320]
  python -m engine2.judgment_watch match --index .cache/sdat.sqlite
"""
import argparse
import gzip
import html
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests

BASE = "https://jportal.mdcourts.gov/judgment/"
UA = {"User-Agent": "Mozilla/5.0 (md-property-leads judgment-watch)"}
RAW_DIR = "data/judgments/raw"
OUT = "data/judgments/leads.json"
PAUSE = 0.6                # seconds between requests
CAP = 500                  # the site's per-search limit
BACKFILL_YEARS = 12        # Maryland judgments live 12 years unless renewed
RECENT_DAYS = 60

SHARD_YEARS = 2            # history is split into 2-year shards so GitHub can pull them in parallel
NSHARDS = BACKFILL_YEARS // SHARD_YEARS

# slug -> (jportal county label, SDAT county code, display name)
COUNTIES = {
    "allegany":         ("ALLEGANY COUNTY", "01", "Allegany"),
    "anne-arundel":     ("ANNE ARUNDEL COUNTY", "02", "Anne Arundel"),
    "baltimore-city":   ("BALTIMORE CITY", "03", "Baltimore City"),
    "baltimore-county": ("BALTIMORE COUNTY", "04", "Baltimore County"),
    "calvert":          ("CALVERT COUNTY", "05", "Calvert"),
    "caroline":         ("CAROLINE COUNTY", "06", "Caroline"),
    "carroll":          ("CARROLL COUNTY", "07", "Carroll"),
    "cecil":            ("CECIL COUNTY", "08", "Cecil"),
    "charles":          ("CHARLES COUNTY", "09", "Charles"),
    "dorchester":       ("DORCHESTER COUNTY", "10", "Dorchester"),
    "frederick":        ("FREDERICK COUNTY", "11", "Frederick"),
    "garrett":          ("GARRETT COUNTY", "12", "Garrett"),
    "harford":          ("HARFORD COUNTY", "13", "Harford"),
    "howard":           ("HOWARD COUNTY", "14", "Howard"),
    "kent":             ("KENT COUNTY", "15", "Kent"),
    "montgomery":       ("MONTGOMERY COUNTY", "16", "Montgomery"),
    "prince-george-s":  ("PRINCE GEORGE'S COUNTY", "17", "Prince George's"),
    "queen-anne-s":     ("QUEEN ANNE'S COUNTY", "18", "Queen Anne's"),
    "st-mary-s":        ("SAINT MARY'S COUNTY", "19", "St. Mary's"),
    "somerset":         ("SOMERSET COUNTY", "20", "Somerset"),
    "talbot":           ("TALBOT COUNTY", "21", "Talbot"),
    "washington":       ("WASHINGTON COUNTY", "22", "Washington"),
    "wicomico":         ("WICOMICO COUNTY", "23", "Wicomico"),
    "worcester":        ("WORCESTER COUNTY", "24", "Worcester"),
}

ROW_RE = re.compile(
    r'<tr class="(?:odd|even)"><td><a href="/judgment/details\.jsf\?selectedCaseId=([^"]+)">.*?</a></td>'
    r'<td><table>(.*?)</table>\s*</td><td><table>(.*?)</table>\s*</td>'
    r'<td>([^<]*)</td><td>([^<]*)</td><td>([^<]*)</td><td>([^<]*)</td><td>([^<]*)</td></tr>', re.S)
NAME_RE = re.compile(r'<span class="Value">(.*?)</span>', re.S)
COUNT_RE = re.compile(r"(\d+) items found")
VS_RE = re.compile(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"')


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def _now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ───────────────────────────────────────────────────────────────── fetch
class Portal:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update(UA)

    def _req(self, method, url, **kw):
        for i in range(4):
            try:
                time.sleep(PAUSE)
                r = self.s.request(method, url, timeout=90, **kw)
                r.raise_for_status()
                return r
            except Exception as e:  # noqa: BLE001
                log("  retry", url[-40:], e)
                time.sleep(5 * (i + 1))
                if i == 1:
                    self.s = requests.Session(); self.s.headers.update(UA)
        raise RuntimeError(f"portal unreachable: {url}")

    def search(self, county_label, d0, d1):
        """Return (reported_count, rows) for one county + filing-date window."""
        r = self._req("GET", BASE)
        vs = VS_RE.search(r.text).group(1)
        form = {
            "searchForm": "searchForm", "searchForm:companyIndicatorRadio": "N",
            "searchForm:lastName": "", "searchForm:firstName": "", "searchForm:middleName": "",
            "searchForm:county": county_label,
            "searchForm:filingStartDate": d0.strftime("%m/%d/%Y"), "searchForm:filingEndDate": d1.strftime("%m/%d/%Y"),
            "searchForm:exactFilingDate": "", "searchForm:caseSearchGet": "Search", "javax.faces.ViewState": vs,
        }
        r = self._req("POST", BASE + "judgementSearch.jsf", data=form)
        m = COUNT_RE.search(r.text)
        count = int(m.group(1)) if m else 0
        if count >= CAP:
            return count, None
        rows = parse_rows(r.text)
        pages = 1
        while len(rows) < count and pages < 30:
            vs = VS_RE.search(r.text)
            if not vs:
                break
            r = self._req("POST", BASE + "judgementResults.jsf", data={
                "j_idt8": "j_idt8", "j_idt8:_idcl": "j_idt8:scrollnext", "j_idt8:scroll": "next",
                "javax.faces.ViewState": vs.group(1)})
            got = parse_rows(r.text)
            if not got:
                break
            rows += got
            pages += 1
        if len(rows) < count:
            log(f"  short page set {d0}..{d1}: {len(rows)}/{count}")
        return count, rows


def _clean(s):
    return re.sub(r"\s+", " ", html.unescape(s or "")).strip()


def parse_rows(text):
    out = []
    for case, f, a, court, status, amt, bp, entry in ROW_RE.findall(text):
        try:
            amount = float(amt.replace("$", "").replace(",", "").strip() or 0)
        except ValueError:
            amount = 0.0
        out.append({
            "case": _clean(case),
            "for": [_clean(x) for x in NAME_RE.findall(f)],
            "against": [_clean(x) for x in NAME_RE.findall(a)],
            "status": _clean(status), "amount": amount, "book_page": _clean(bp),
            "entry": _clean(entry),
        })
    return out


def pull_window(portal, label, d0, d1, sink):
    """Fetch [d0, d1], splitting the window until each piece is under the cap."""
    count, rows = portal.search(label, d0, d1)
    if rows is None:
        if d0 == d1:
            log(f"  {d0}: over {CAP} on a single day — kept nothing for that day")
            return 0
        mid = d0 + (d1 - d0) // 2
        return pull_window(portal, label, d0, mid, sink) + pull_window(portal, label, mid + timedelta(days=1), d1, sink)
    for r in rows:
        r["filed_window"] = d0.isoformat()
        sink[row_key(r)] = r
    return len(rows)


def row_key(r):
    return "|".join([r["case"], ";".join(r["for"]), ";".join(r["against"]), f"{r['amount']:.2f}"])


def shard_path(slug, k):
    return os.path.join(RAW_DIR, f"{slug}.s{k}.json.gz")


def _read(p):
    with gzip.open(p, "rt") as fh:
        return json.load(fh)


def load_shard(slug, k):
    p = shard_path(slug, k)
    if not os.path.exists(p):
        return {"county": slug, "shard": k, "rows": {}, "backfill_cursor": None, "backfill_done": False}
    return _read(p)


def save_shard(slug, k, raw):
    os.makedirs(RAW_DIR, exist_ok=True)
    raw["updated_at"] = _now()
    # paid / vacated / withdrawn liens are dropped once seen — keeps the files small
    raw["rows"] = {key: r for key, r in raw["rows"].items() if r["status"].strip().upper() not in CLOSED}
    with gzip.open(shard_path(slug, k), "wt") as fh:
        json.dump(raw, fh, separators=(",", ":"))


def load_raw(slug):
    """Every stored row for a county: the old single file (first run) plus all shards, oldest first
    so the most recently fetched copy of a row wins. Returns the same shape the matcher expects."""
    rows, done, cursors = {}, True, []
    legacy = os.path.join(RAW_DIR, f"{slug}.json.gz")
    if os.path.exists(legacy):
        rows.update(_read(legacy)["rows"])
    for k in range(NSHARDS - 1, -1, -1):
        p = shard_path(slug, k)
        if not os.path.exists(p):
            done = False
            continue
        r = _read(p)
        rows.update(r["rows"])
        if not r.get("backfill_done"):
            done = False
            cursors.append(r.get("backfill_cursor") or "")
    return {"county": slug, "rows": rows, "backfill_done": done, "backfill_cursor": min([c for c in cursors if c], default=None)}


def shard_range(k, today):
    end = today if k == 0 else today.replace(year=today.year - SHARD_YEARS * k)
    start = today.replace(year=today.year - SHARD_YEARS * (k + 1)) + timedelta(days=1)
    return start, end


def fetch_shard(slug, k, budget_min, refresh=False):
    deadline = time.time() + budget_min * 60
    portal = Portal()
    today = date.today()
    label = COUNTIES[slug][0]
    raw = load_shard(slug, k)
    if refresh and raw.get("backfill_done"):
        raw["backfill_cursor"], raw["backfill_done"] = None, False
    rows = raw["rows"]
    start, end = shard_range(k, today)
    if k == 0:   # new filings and fresh payoffs
        d, n = today - timedelta(days=RECENT_DAYS), 0
        while d <= today:
            e = min(d + timedelta(days=6), today)
            n += pull_window(portal, label, d, e, rows)
            d = e + timedelta(days=1)
        log(f"{slug}: recent {RECENT_DAYS} days -> {n} rows")
        end = today - timedelta(days=RECENT_DAYS + 1)
    if raw.get("backfill_done"):
        save_shard(slug, k, raw)
        log(f"{slug} s{k}: history already loaded")
        return
    cur = date.fromisoformat(raw["backfill_cursor"]) if raw.get("backfill_cursor") else end
    weeks = 0
    while cur >= start and time.time() < deadline:
        s0 = max(cur - timedelta(days=6), start)
        pull_window(portal, label, s0, cur, rows)
        cur = s0 - timedelta(days=1)
        raw["backfill_cursor"] = cur.isoformat()
        weeks += 1
        if weeks % 25 == 0:
            log(f"{slug} s{k}: back to {cur} ({len(rows)} rows)")
            save_shard(slug, k, raw)
    raw["backfill_done"] = cur < start
    save_shard(slug, k, raw)
    log(f"{slug} s{k} [{start}..{end}]: {len(rows)} rows · {'done' if raw['backfill_done'] else 'resumes at ' + raw['backfill_cursor']}")


def plan(refresh=False, only=None):
    """GitHub matrix: shard 0 of every county each run, older shards until their history is loaded
    (all shards on a refresh run, so payoffs of old liens get picked up)."""
    jobs = []
    for slug in COUNTIES:
        if only and slug not in only:
            continue
        for k in range(NSHARDS):
            if k == 0 or refresh or not load_shard(slug, k).get("backfill_done"):
                jobs.append({"county": slug, "shard": k})
    print(json.dumps({"include": jobs}, separators=(",", ":")))


# ───────────────────────────────────────────────────────────────── match
GOV_RE = re.compile(r"COMPTROLLER|STATE OF MARYLAND|MARYLAND STATE|\bMD STATE\b|STATE OF MD|DEPT\.?|DEPARTMENT|LABOR|UNEMPLOY|"
                    r"UNITED STATES|INTERNAL REVENUE|\bIRS\b|COUNTY COMMISSIONERS|COMMISSIONERS OF|HEALTH|MEDICAID|"
                    r"CHILD SUPPORT|SOCIAL SERVICES|TREASUR|REVENUE|JUSTICE", re.I)
FED_RE = re.compile(r"UNITED STATES|\bU\.?S\.?A?\b|INTERNAL REVENUE|\bIRS\b|DEPARTMENT OF (THE )?TREASURY|DEPT\.? OF (THE )?TREASURY|"
                    r"JUSTICE|D[E]?PA?RTMENT OF JUSTICE|SMALL BUSINESS ADMIN", re.I)
TAX_RE = re.compile(r"COMPTROLLER|TREASUR|REVENUE|\bTAX\b", re.I)
COMPANY_RE = re.compile(r"\b(LLC|L\.L\.C|INC|CORP|CORPORATION|COMPANY|\bCO\b|LTD|LP|LLP|BANK|ASSOC|TRUST|CHURCH|"
                        r"SERVICES|HOLDINGS|PROPERTIES|GROUP|ENTERPRISES)\b", re.I)
SUFFIX = {"JR", "SR", "II", "III", "IV", "V"}
# Consumer-debt creditors: buying their defaulted consumer judgments and collecting them needs a
# Maryland collection agency license (MCALA; judgments enforced without one have been held void).
CONSUMER_RE = re.compile(r"LVNV|MIDLAND|PORTFOLIO RECOVERY|CAVALRY|JEFFERSON CAPITAL|VELOCITY|UNIFUND|ABSOLUTE RESOLUTION|"
                         r"CROWN ASSET|PALISADES|ASSET ACCEPTANCE|ENCORE|SECOND ROUND|DNF ASSOC|BANK|CREDIT UNION|\bFCU\b|"
                         r"FINANCIAL|FINANCE|LENDING|LENDMARK|ONEMAIN|CAPITAL ONE|DISCOVER|SYNCHRONY|CITIBANK|AMERICAN EXPRESS|"
                         r"MARINER|WORLD ACCEPTANCE|AUTO|MOTOR|CREDIT|LOAN|FUNDING|RECEIVABLE|COLLECTION|HOSPITAL|MEDICAL|"
                         r"HEALTH|PHYSICIAN|RADIOLOGY|DENTAL|APARTMENT|PROPERTY MANAGEMENT|LEASING|RENTAL|VERIZON|COMCAST|"
                         r"T-MOBILE|AT&T|INSURANCE|GEICO|PROGRESSIVE|ALLSTATE|STATE FARM", re.I)
STORE_RE = re.compile(r"HOME DEPOT|WALMART|WAL-MART|TARGET|LOWE'?S|SAFEWAY|GIANT|SHERIFF|POLICE", re.I)


def creditor_type(creditor):
    if GOV_RE.search(creditor):
        return "GOV"
    if CONSUMER_RE.search(creditor):
        return "CONSUMER"
    if STORE_RE.search(creditor):
        return "STORE"
    if "," in creditor and not COMPANY_RE.search(creditor):
        return "PERSON"
    return "BUSINESS"


def case_type(case):
    c = case.upper()
    if "-CR-" in c or re.search(r"-K-\d", c):
        return "Criminal restitution"
    if "-FM-" in c or "-FAM" in c:
        return "Family court"
    if "-JG-" in c:
        return "Recorded judgment / lien"
    if "-CV-" in c or re.match(r"^\d\d-C-", c):
        return "Circuit civil"
    return ""
DEAD_FLAGS = {"ALL_OWNERS_DECEASED", "DECEASED_SOLE_OWNER_HIGH", "DECEASED_SOLE_OWNER_MEDIUM", "ESTATE_IN_NAME", "HEIRS_IN_NAME",
              "PERSONAL_REP", "PROBATE_CLOSED_STILL_TITLED", "PROBATE_NONE_FOUND", "PROBATE_OPEN_STALE",
              "MULTI_GENERATION_ESTATES", "FOREIGN_PROBATE"}
CLOSED = {"SATISFIED", "WITHDRAWN", "VACATED", "RELEASED", "CANCELLED", "CANCELED", "STRICKEN", "DISMISSED", "EXPIRED", "PAID"}


def parse_debtor(name):
    """'ROSALES AGUILAR, ERICK ABELARDO' -> {'last','first','mi'}; None for companies / unparseable."""
    n = re.sub(r"^\(AKA\)", "", name.strip(), flags=re.I).upper()
    if COMPANY_RE.search(n) or "," not in n:
        return None
    last, given = n.split(",", 1)
    last = re.sub(r"[^A-Z' \-]", "", last).strip()
    toks = [t for t in re.findall(r"[A-Z][A-Z'\-]*", given) if t not in SUFFIX]
    if not last or not toks:
        return None
    first = toks[0]
    if len(first) == 1 and len(toks) > 1:
        first, toks = toks[1], [toks[1], toks[0]] + toks[2:]
    mi = toks[1][0] if len(toks) > 1 else ""
    return {"last": last.replace(" ", ""), "first": first, "mi": mi, "raw": name.strip()}


def years_between(d0, d1):
    return (d1 - d0).days / 365.25


def _date(s):
    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except (TypeError, ValueError):
        return None


def load_title_by_account():
    """Dead-owner / estate flags from the Title Scan shards (same SDAT account ids)."""
    out = {}
    for slug in COUNTIES:
        p = f"data/title/{slug}.json"
        if not os.path.exists(p):
            continue
        for r in json.load(open(p)).get("rows", []):
            out[r["id"]] = {"flags": r.get("flags", []), "class": r.get("title_class_label", ""), "lead_source": r.get("lead_source", "")}
    return out


def load_signals():
    p = "data/distress/signals.json"
    if not os.path.exists(p):
        return {}
    return json.load(open(p)).get("by_account", {})


def match(index_path):
    from engine2.owner_parse import split_individuals
    from engine2.signals import mailing_state

    db = sqlite3.connect(index_path)
    db.row_factory = sqlite3.Row
    cols = {r[1] for r in db.execute("PRAGMA table_info(parcels)")}
    want = [c for c in ("acct", "county_code", "county", "address", "city", "zip", "lat", "lon", "occupancy", "homestead",
                        "land_use", "land_value", "impr_value", "transfer_date", "owner1", "owner2", "mail_addr",
                        "mail_city", "mail_zip", "legal1", "deed_liber", "deed_folio") if c in cols]
    if "owner1" not in want:
        sys.exit("SDAT index has no owner names (public dataset) — cannot match.")
    title = load_title_by_account()
    signals = load_signals()
    today = date.today()
    leads, stats = [], {}

    for slug, (label, code, disp) in COUNTIES.items():
        raw = load_raw(slug)
        # owner index: (LAST, FIRST) -> list of (person, parcel)
        owners, by_name = {}, {}
        for p in db.execute(f"SELECT {','.join(want)} FROM parcels WHERE county_code=?", (code,)):
            p = dict(p)
            people = split_individuals(p.get("owner1") or "", p.get("owner2") or "")
            if not people:
                continue
            p["_people"] = people
            owners[p["acct"]] = p
            for person in people:
                k = (re.sub(r"[^A-Z]", "", person["last_name"]), person["first_name"])
                by_name.setdefault(k, []).append((person, p["acct"]))
        # open judgments grouped by matched parcel
        hits = {}
        open_rows = [r for r in raw["rows"].values() if r["status"].strip().upper() not in CLOSED]
        matched_rows = 0
        for r in open_rows:
            seen = set()
            for nm in r["against"]:
                d = parse_debtor(nm)
                if not d:
                    continue
                cands = by_name.get((re.sub(r"[^A-Z]", "", d["last"]), d["first"]), [])
                distinct_people = {c[0]["person_key"] for c in cands}
                for person, acct in cands:
                    pmi = person.get("middle_initial") or ""
                    if d["mi"] and pmi and d["mi"] != pmi:
                        continue
                    if d["mi"] and pmi:
                        conf = "HIGH"
                    elif len(distinct_people) == 1:
                        conf = "MEDIUM"
                    else:
                        conf = "LOW"
                    if conf == "LOW":
                        continue
                    key = (acct, r["case"], r["amount"])
                    if key in seen:
                        continue
                    seen.add(key)
                    hits.setdefault(acct, []).append({**r, "debtor": d["raw"], "owner_match": person["normalized_name"], "confidence": conf})
            if seen:
                matched_rows += 1
        stats[slug] = {"raw_rows": len(raw["rows"]), "open": len(open_rows), "open_matched": matched_rows,
                       "parcels_hit": len(hits), "backfill_done": raw.get("backfill_done", False),
                       "backfill_cursor": raw.get("backfill_cursor")}

        for acct, js in hits.items():
            p = owners[acct]
            leads.append(build_lead(p, js, disp, title.get(acct), signals.get(acct, []), mailing_state, today))

    # owner-occupied homes are skipped in this play (the Writ Watch tab covers sheriff's sales), and
    # keeping them would push the file past what a browser — and GitHub — handle well
    occupied_hits = sum(1 for x in leads if x["occupied"])
    leads = [x for x in leads if not x["occupied"]]
    for x in leads:
        for k in ("lat", "lon", "land_use"):
            x.pop(k, None)
    stats["_owner_occupied_dropped"] = occupied_hits
    leads.sort(key=lambda x: -x["score"])
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump({"generated_at": _now(), "counties": [v[2] for v in COUNTIES.values()], "stats": stats, "rows": leads},
              open(OUT, "w"), separators=(",", ":"))
    log(json.dumps(stats, indent=1))
    log(f"wrote {len(leads)} parcels to {OUT}")


def build_lead(p, js, county, title, sigs, mailing_state, today):
    assessed = (float(p.get("land_value") or 0) + float(p.get("impr_value") or 0))
    total = sum(j["amount"] for j in js)
    people = p["_people"]
    debtor_keys = {j["owner_match"] for j in js}
    ms = mailing_state(p.get("mail_city"), p.get("mail_zip"))
    occupied = (p.get("occupancy") or "").upper() in ("H", "D")   # SDAT: H = principal residence, D = principal residence plus extra land
    tflags = (title or {}).get("flags", [])
    dead = bool(DEAD_FLAGS.intersection(tflags))
    co_dead = not dead and "CO_OWNER_DECEASED" in tflags

    tags, reasons, score = [], [], 0
    judgments = []
    for j in sorted(js, key=lambda x: _date(x["entry"]) or today):
        ed = _date(j["entry"])
        age = years_between(ed, today) if ed else 0
        creditor = "; ".join(j["for"])
        kind = "STATE / GOV LIEN" if GOV_RE.search(creditor) else "JUDGMENT"
        if kind != "JUDGMENT" and TAX_RE.search(creditor):
            kind = "TAX LIEN"
        if FED_RE.search(creditor) and "CREDIT UNION" not in creditor.upper():
            kind = "FEDERAL LIEN (usually IRS)"
            # IRS collection generally ends 10 years after assessment unless extended or refiled
            expires = ed.replace(year=ed.year + 10) if ed else None
        expires = ed.replace(year=ed.year + 12) if ed else None
        ctype = creditor_type(creditor)
        judgments.append({"case": j["case"], "creditor": creditor, "creditor_type": ctype, "case_type": case_type(j["case"]),
                          "buyable": ctype in ("PERSON", "BUSINESS") and j["amount"] >= 2500,
                          "debtor": j["debtor"], "amount": j["amount"],
                          "entry": j["entry"], "age_years": round(age, 1), "kind": kind,
                          "expires": expires.isoformat() if expires else "", "confidence": j["confidence"],
                          "book_page": j.get("book_page", "")})

    n = len(judgments)
    fed = [j for j in judgments if j["kind"].startswith("FEDERAL")]
    if fed:
        tags.append("FED_LIEN")
        old = [j for j in fed if j["age_years"] >= 8]
        if old:
            score += 12; tags.append("FED_OLD")
            reasons.append(f"Federal (usually IRS) lien filed {max(j['age_years'] for j in old):.0f}+ years ago — IRS liens generally run 10 years unless refiled; may be dead or nearly dead (+12)")
    # ── the lien side
    if n >= 3:
        score += 20; tags.append("STACKED"); reasons.append(f"{n} open judgments/liens on the owner (+20)")
    elif n == 2:
        score += 12; tags.append("STACKED"); reasons.append("2 open judgments/liens on the owner (+12)")
    else:
        score += 5; reasons.append("1 open judgment/lien (+5)")
    if any(j["kind"] != "JUDGMENT" for j in judgments):
        score += 12; tags.append("GOV_LIEN"); reasons.append("State/government or tax lien — often settles for what it can actually collect (+12)")
    soon = [j for j in judgments if j["expires"] and 0 <= (date.fromisoformat(j["expires"]) - today).days <= 730]
    if soon:
        score += 15; tags.append("EXPIRING"); reasons.append(f"{len(soon)} lien(s) expire within 2 years unless renewed — creditor's leverage is fading (+15)")
    elif any(j["age_years"] >= 8 for j in judgments):
        score += 8; tags.append("OLD_LIEN"); reasons.append("Lien 8+ years old (+8)")
    if total >= 25000:
        score += 10; reasons.append(f"${total:,.0f} total owed — big enough to scare the owner (+10)")
    elif total >= 5000:
        score += 5; reasons.append(f"${total:,.0f} total owed (+5)")
    # only some owners are debtors -> likely tenancy by the entirety or a single share
    if len(people) > 1 and len(debtor_keys) < len(people):
        # tenancy by the entirety needs a married couple: exactly two people joined by "&" on the
        # first owner line with the same surname. Anything else is treated as a separate share.
        from engine2.owner_parse import split_individuals
        line1 = split_individuals(p.get("owner1") or "")
        # SDAT puts the second spouse either after "&" on line 1 or alone on line 2
        same_last = len(people) == 2 and len({x["last_name"] for x in people}) == 1
        if same_last:
            tags.append("ONE_SPOUSE_ONLY"); score += 10
            reasons.append("Judgment against only one of the spouses — if they own as tenants by the entirety the lien may not touch the house (+10)")
        else:
            tags.append("ONE_OWNER_ONLY"); score += 8
            reasons.append("Judgment against only one co-owner — it hits that share only; buy around it (+8)")
    # ── the seller side
    if dead:
        score += 25; tags.append("DEAD_OWNER"); reasons.append("Title Scan: owner died / estate unresolved (+25) — confirm with obituaries")
    elif co_dead:
        score += 12; tags.append("CO_OWNER_DEAD"); reasons.append("Title Scan: a co-owner died (+12) — confirm with obituaries")
    if not occupied:
        score += 8; tags.append("NOT_OWNER_OCCUPIED"); reasons.append("Not owner-occupied (+8)")
    if ms and ms != "MD":
        score += 10; tags.append("OUT_OF_STATE"); reasons.append(f"Tax bill mails out of state ({ms}) (+10)")
    kinds = sorted({s.get("kind") for s in sigs if s.get("kind")})
    if kinds:
        score += min(20, 8 * len(kinds)); tags.append("DISTRESS"); reasons.append(f"Distress signals: {', '.join(kinds)} (+{min(20, 8 * len(kinds))})")
    td = p.get("transfer_date") or ""
    try:
        held = years_between(date.fromisoformat(td[:10]), today)
    except ValueError:
        held = 0
    if held >= 15:
        score += 6; tags.append("LONG_HELD"); reasons.append(f"Owned {int(held)} years (+6)")
    # ── Logan Fullmer's lien-buy play: buy an existing recorded judgment from a creditor who can
    # sell it without a collection license problem, record the assignment, demand payoff with
    # interest, and enforce by writ of execution + sheriff's sale if unpaid. Needs a property that
    # isn't the debtor's home and has equity behind the lien.
    buyable = [j for j in judgments if j["buyable"]]
    # a judgment against only one spouse usually can't reach a tenancy-by-the-entirety house — not a lien to buy
    if buyable and not occupied and assessed >= 75000 and "ONE_SPOUSE_ONLY" not in tags:
        b = max(buyable, key=lambda x: x["amount"])
        score += 10; tags.append("LIEN_BUY")
        reasons.append(f"Lien-buy candidate: ${b['amount']:,.0f} judgment owed to {b['creditor']} ({b['creditor_type'].lower()} creditor) "
                       f"on a non-homestead property assessed ${assessed:,.0f} (+10)")
    # ── money
    equity = assessed - total
    if assessed < 40000:
        score -= 15; reasons.append("Assessed under $40K (−15)")
    elif equity >= 150000:
        score += 10; reasons.append(f"Assessed ${assessed:,.0f} vs ${total:,.0f} in judgments (+10; mortgages not counted)")
    if all(j["confidence"] == "MEDIUM" for j in judgments):
        score -= 5; reasons.append("Name match without a middle initial — confirm it's the same person (−5)")
    if occupied:
        tags.append("OWNER_OCCUPIED")
    cautions = []
    if (dead or co_dead) and any(j["age_years"] < 3 for j in judgments):
        cautions.append("Lien filed in the last 3 years against an owner the Title Scan thinks died — often a same-name son/father (Jr./Sr.). Confirm who the debtor is before relying on it.")
    if any(j["confidence"] == "MEDIUM" for j in judgments):
        cautions.append("At least one match has no middle initial on one side — confirm the debtor and the owner are the same person (Case Search party address, Land Records).")
    if occupied:
        cautions.append("Owner-occupied (homestead) — skip unless it's the sheriff's-sale play.")
    if any(j["creditor_type"] == "CONSUMER" for j in judgments):
        cautions.append("Consumer-debt judgment(s) (debt buyer / bank / medical / landlord): buying and collecting these in Maryland needs a collection agency license — use them as owner pressure, not as a lien to buy.")
    cautions.append("The circuit judgment index can still say 'open' after the District Court case was paid — open the case in Case Search and check its Judgment Status before counting on any lien.")
    if "LIEN_BUY" in tags:
        cautions.append("Lien-buy: confirm in Land Records the property is free & clear (or the lien still has equity behind it) and the judgment wasn't renewed, released or appealed before you offer the creditor anything.")

    return {
        "id": p["acct"], "account": p["acct"], "county": county,
        "address": (p.get("address") or "").title(), "city": (p.get("city") or "").title(), "zip": p.get("zip") or "",
        "owner": p.get("owner1") or "", "owner2": p.get("owner2") or "",
        "mail": " ".join(x for x in (p.get("mail_addr"), p.get("mail_city"), p.get("mail_zip")) if x), "mail_state": ms,
        "occupied": occupied, "assessed": round(assessed), "total_owed": round(total, 2), "equity_est": round(equity),
        "transfer_date": td[:10], "legal": p.get("legal1") or "",
        "deed": f"{p.get('deed_liber') or ''}/{p.get('deed_folio') or ''}".strip("/"),
        "lat": p.get("lat"), "lon": p.get("lon"), "land_use": p.get("land_use") or "",
        "title_class": (title or {}).get("class", ""), "title_flags": tflags, "signals": kinds,
        "judgments": judgments, "tags": tags, "reasons": reasons, "cautions": cautions, "score": max(0, min(100, score)),
    }


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--county", choices=list(COUNTIES), required=True)
    f.add_argument("--shard", type=int, required=True)
    f.add_argument("--budget-min", type=float, default=320)
    f.add_argument("--refresh", action="store_true")
    pl = sub.add_parser("plan")
    pl.add_argument("--refresh", action="store_true")
    pl.add_argument("--county", action="append")
    m = sub.add_parser("match")
    m.add_argument("--index", default=".cache/sdat.sqlite")
    a = ap.parse_args()
    if a.cmd == "fetch":
        fetch_shard(a.county, a.shard, a.budget_min, a.refresh)
    elif a.cmd == "plan":
        plan(a.refresh, a.county)
    else:
        match(a.index)


if __name__ == "__main__":
    main()
