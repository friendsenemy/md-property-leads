"""
Probate PR audience — keeps the "MD Probate PRs" Facebook custom audience current.

Every run rebuilds the list from scratch (no state is stored anywhere):
  1. Reads the Maryland-Probate-Daily-Leads sheet (estates + personal reps + phones).
  2. Keeps estates opened in the last --months months (default 12).
  3. Checks each decedent against the licensed SDAT owner dataset:
       keep    - decedent still owns Maryland property
       drop    - property sold/transferred since the date of death,
                 decedent owns nothing, or the name is too common to be sure
  4. Replaces the Meta customer list with the kept reps (phone/email/name/ZIP),
     so reps whose house sold, or whose estate aged out, leave the audience.

The reps' phones and emails never touch the repo: they live only in memory
during the run and go straight to Meta, SHA-256 hashed.

  python pr_audience.py                 # full run (needs the env vars below)
  python pr_audience.py --dry-run       # counts only, nothing sent to Meta
  python pr_audience.py --csv OUT.csv   # also write the Meta-format file locally

Env:
  MD_OPENDATA_USERNAME / MD_OPENDATA_PASSWORD  opendata.maryland.gov login or API key
  MD_OPENDATA_APP_TOKEN                        optional
  META_ACCESS_TOKEN                            system-user token with ads_management
  META_PR_AUDIENCE_ID                          the "MD Probate PRs" audience id
  PR_SKIP_SUPPLEMENT                           optional JSON {"estate#": {"phones": [], "emails": []}}
                                               for reps traced outside the sheet (SmartSkip)
"""

import os
import re
import io
import csv
import sys
import json
import time
import hashlib
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import requests

SHEET_ID = "1FcbKsxZZ_ISGm3llCxdlxhAIsz4IFwafMmyQOQydzSw"
LEADS_GID = "765796531"
SOCRATA = "https://opendata.maryland.gov/resource/9xq5-z8s2.json"
GRAPH = "https://graph.facebook.com/v21.0"

O = "record_key_owner_s_name_mdp_field_ownname1_sdat_field_7"
G = "sales_segment_1_grantor_name_mdp_field_grntnam1_sdat_field_80"
T = "sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89"
A = "account_id_mdp_field_acctid"
C = "county_name_mdp_field_cntyname"

SUFFIX = {"JR", "SR", "II", "III", "IV", "V", "ESQ"}
OWNER_TAIL = {"ET", "AL", "ETAL", "TR", "TRUSTEE", "LE", "PR"}
MAX_OWNED = 4          # more matching parcels than this = common name, can't be sure
BATCH = 12             # decedents per SDAT query


# ---------------------------------------------------------------- sheet

def mdy(s):
    m = re.match(r"\s*(\d{1,2})/(\d{1,2})/(\d{4})", s or "")
    return datetime(int(m[3]), int(m[1]), int(m[2])) if m else None


def read_sheet():
    # plain export keeps every cell as text (gviz drops "W123" estate numbers)
    url = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export"
           f"?format=csv&gid={LEADS_GID}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.text)))
    head = [h.strip() for h in rows[0]]
    col = {h: i for i, h in reversed(list(enumerate(head)))}  # first occurrence wins
    trace_at = col.get("Trace Status", 15)
    need = ["Estate Number", "County", "Filing Date", "Date of Death", "Date Opened",
            "Decedent Name", "Rep First Name", "Rep Last Name", "Rep City",
            "Rep State", "Rep Zip"]
    missing = [n for n in need if n not in col]
    if missing:
        sys.exit(f"Sheet layout changed, missing columns: {missing}")
    out = {}
    for r in rows[1:]:
        r = r + [""] * (len(head) - len(r))
        est = r[col["Estate Number"]].strip().split(".")[0]
        if not est or est in out:
            continue
        phones = []
        for cell in r[trace_at + 1:]:
            d = re.sub(r"\D", "", cell)
            if len(d) == 11 and d[0] == "1":
                d = d[1:]
            if len(d) == 10 and d not in phones:
                phones.append(d)
        out[est] = {
            "estate": est,
            "county": r[col["County"]].strip(),
            "filed": mdy(r[col["Filing Date"]]),
            "opened": mdy(r[col["Date Opened"]]),
            "dod": mdy(r[col["Date of Death"]]),
            "decedent": r[col["Decedent Name"]].strip(),
            "fn": r[col["Rep First Name"]].strip(),
            "ln": r[col["Rep Last Name"]].strip(),
            "ct": r[col["Rep City"]].strip(),
            "st": r[col["Rep State"]].strip(),
            "zip": r[col["Rep Zip"]].strip(),
            "phones": phones,
            "emails": [],
        }
    return list(out.values())


def split_name(full):
    toks = [t for t in re.split(r"[\s.,]+", full.upper()) if t]
    while toks and toks[-1] in SUFFIX:
        toks.pop()
    if len(toks) < 2:
        return None
    clean = lambda s: re.sub(r"[^A-Z'\-]", "", s)
    return clean(toks[0]), clean(toks[-1]), (toks[1] if len(toks) > 2 else "")


# ---------------------------------------------------------------- SDAT

def socrata_get(where):
    auth = None
    if os.environ.get("MD_OPENDATA_USERNAME") and os.environ.get("MD_OPENDATA_PASSWORD"):
        auth = (os.environ["MD_OPENDATA_USERNAME"], os.environ["MD_OPENDATA_PASSWORD"])
    params = {"$limit": 2000, "$select": ",".join([O, G, T, A]), "$where": where}
    tok = os.environ.get("MD_OPENDATA_APP_TOKEN")
    if tok:
        params["$$app_token"] = tok
    for attempt in range(4):
        r = requests.get(SOCRATA, params=params, auth=auth, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (401, 403):
            sys.exit("opendata.maryland.gov refused the login (MD_OPENDATA_USERNAME/PASSWORD)")
        time.sleep(2 + attempt * 3)
    raise RuntimeError(f"SDAT query failed: {r.status_code} {r.text[:200]}")


def name_re(first, last):
    esc = lambda s: re.escape(s).replace("'", ".").replace("\\-", ".")
    return re.compile(r"^" + esc(last) + r"\b.*\b" + esc(first) + r"\b")


def middle_ok(owner, first, middle):
    """Reject an owner record whose middle initial or suffix shows a different person."""
    owner = owner.replace(",", " ")
    seg = next((s for s in re.split(r"[-&]", owner) if re.search(r"\b" + re.escape(first) + r"\b", s)), owner)
    toks = seg.split()
    nxt = toks[toks.index(first) + 1] if first in toks and toks.index(first) + 1 < len(toks) else ""
    if nxt in SUFFIX:
        return False
    if not middle or not nxt or nxt in OWNER_TAIL:
        return True
    return nxt[0] == middle[0]


def classify(est, recs):
    """-> ("owns", [acct...]) | ("sold", []) | ("none", []) | ("namesake", [])"""
    first, last, middle = est["name"]
    rx = name_re(first, last)
    dod = est["dod"].strftime("%Y%m%d") if est["dod"] else ""
    owned, sold = [], False
    for p in recs:
        owner = (p.get(O) or "").upper().replace(",", " ")
        grantor = (p.get(G) or "").upper().replace(",", " ")
        is_owner, is_grantor = bool(rx.search(owner)), bool(rx.search(grantor))
        if is_owner and middle_ok(owner, first, middle):
            owned.append(p.get(A, "").replace(" ", ""))
        elif is_grantor and not is_owner and dod and (p.get(T) or "").replace(".", "") >= dod:
            sold = True
    if len(owned) > MAX_OWNED:
        return "namesake", []
    if owned:
        return "owns", owned
    return ("sold", []) if sold else ("none", [])


def check_property(estates):
    by_county = {}
    for e in estates:
        by_county.setdefault(e["county"], []).append(e)
    jobs = [(c, v[i:i + BATCH]) for c, v in by_county.items() for i in range(0, len(v), BATCH)]

    def run(job):
        county, batch = job
        q = lambda s: s.replace("'", "''")
        ors = " OR ".join(
            f"upper({O}) like '{q(e['name'][1])}%{q(e['name'][0])}%' OR "
            f"upper({G}) like '{q(e['name'][1])}%{q(e['name'][0])}%'" for e in batch)
        recs = socrata_get(f"{C}='{q(county)}' AND ({ors})")
        for e in batch:
            e["status"], e["accts"] = classify(e, recs)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(run, jobs))


# ---------------------------------------------------------------- Meta

def h(v):
    return hashlib.sha256(v.encode("utf-8")).hexdigest() if v else ""


def meta_rows(e):
    """One row per phone/email so every identifier gets its own match chance."""
    fn = re.sub(r"[^a-z]", "", e["fn"].lower())
    ln = re.sub(r"[^a-z]", "", e["ln"].lower())
    z = re.sub(r"\D", "", e["zip"]).zfill(5)[:5] if re.sub(r"\D", "", e["zip"]) else ""
    ct = re.sub(r"[^a-z]", "", e["ct"].lower())
    st = re.sub(r"[^a-z]", "", e["st"].lower())[:2]
    base = [h(fn), h(ln), h(z), h(ct), h(st), h("us")]
    ids = [("", "1" + p) for p in e["phones"][:3]] + [(m.strip().lower(), "") for m in e["emails"][:3]]
    if not ids:
        ids = [("", "")]
    return [[h(em), h(ph)] + base for em, ph in ids]


def push_to_meta(rows):
    token, aud = os.environ.get("META_ACCESS_TOKEN"), os.environ.get("META_PR_AUDIENCE_ID")
    if not token or not aud:
        print("META_ACCESS_TOKEN / META_PR_AUDIENCE_ID not set - skipping Meta upload")
        return
    schema = ["EMAIL", "PHONE", "FN", "LN", "ZIP", "CT", "ST", "COUNTRY"]
    session_id = int(time.time())
    size = 5000
    chunks = [rows[i:i + size] for i in range(0, len(rows), size)] or [[]]
    for n, chunk in enumerate(chunks, 1):
        r = requests.post(f"{GRAPH}/{aud}/usersreplace", data={
            "access_token": token,
            "session": json.dumps({"session_id": session_id, "batch_seq": n,
                                   "last_batch_flag": n == len(chunks),
                                   "estimated_num_total": len(rows)}),
            "payload": json.dumps({"schema": schema, "data": chunk}),
        }, timeout=120)
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        if r.status_code != 200 or "error" in body:
            sys.exit(f"Meta upload failed (batch {n}): {json.dumps(body.get('error', body))[:400]}")
        print(f"Meta batch {n}/{len(chunks)}: received {body.get('num_received')}, "
              f"invalid {body.get('num_invalid_entries')}")


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--csv", help="also write the Meta-format customer file here (has phones - keep private)")
    a = ap.parse_args()

    cutoff = datetime.now() - timedelta(days=round(a.months * 30.44))
    sheet = read_sheet()
    estates = []
    for e in sheet:
        start = e["opened"] or e["filed"]
        nm = split_name(e["decedent"])
        if start and start >= cutoff and nm and e["county"]:
            e["name"] = nm
            estates.append(e)
    print(f"sheet: {len(sheet)} estates, {len(estates)} opened in the last {a.months} months")

    sup = json.loads(os.environ.get("PR_SKIP_SUPPLEMENT") or "{}")
    for e in estates:
        s = sup.get(e["estate"])
        if s:
            e["phones"] = e["phones"] or s.get("phones", [])
            e["emails"] = s.get("emails", [])

    check_property(estates)
    counts = {}
    for e in estates:
        counts[e["status"]] = counts.get(e["status"], 0) + 1
    keep = [e for e in estates if e["status"] == "owns"]
    print("property check: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    print(f"audience: {len(keep)} reps "
          f"({sum(1 for e in keep if e['phones'])} with phone, "
          f"{sum(1 for e in keep if e['emails'])} with email)")

    if a.csv:
        with open(a.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["email"] * 3 + ["phone"] * 3 + ["fn", "ln", "zip", "ct", "st", "country"])
            for e in keep:
                em = (e["emails"] + ["", "", ""])[:3]
                ph = (["1" + p for p in e["phones"]] + ["", "", ""])[:3]
                w.writerow(em + ph + [e["fn"].title(), e["ln"].title(), re.sub(r"\D", "", e["zip"])[:5],
                                      e["ct"].title(), e["st"].upper(), "US"])

    rows = [row for e in keep for row in meta_rows(e)]
    if a.dry_run:
        print(f"dry run: {len(rows)} rows would be sent to Meta")
        return
    if len(keep) < 100:
        sys.exit(f"Only {len(keep)} reps matched - refusing to shrink the audience that far. Check the sheet.")
    push_to_meta(rows)


if __name__ == "__main__":
    main()
