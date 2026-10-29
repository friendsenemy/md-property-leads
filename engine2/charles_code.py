"""Charles County code-enforcement cases (Tyler EnerGov "Civic Access" portal).

Source: https://land.charlescountymd.gov/energov_prod/selfservice#/search
(Search Public Records -> Code Case). The portal is a single-page app; its
public search is served by the JSON endpoint the page itself calls
(api/energov/search/search). We call that same endpoint, slowly, and keep only
what the public results list shows: case number, type, status, address,
parcel (= SDAT account), opened / closed dates.

We deliberately do NOT read the Violations, Attachments or Contacts tabs: the
portal marks those "You must be a contact on this record" and we don't go
around that. The violation text has to come from a Public Information Act
request (see docs/pia-requests.md).

Output: data/distress/charles-code.json
  {generated_at, counts, cases: {case_number: {...}}}
Each case keeps first_seen / last_seen and a status history so a case that
goes In Progress -> Escalated is visible.

engine2.distress_watch reads this file and turns open cases into parcel
signals (CODE_VIOLATIONS / NUISANCE_CASE / ZONING_CASE / REPEAT_CODE_CASES).

Sweep: the request body is the one the portal's own Advanced Search sends
(engine2/charles_search_template.json, captured from the page); the search
only honours the filters when sent in exactly that shape. We walk opened-date
windows one month at a time, 100 per page. A daily run re-reads the last 60
days (new cases + status changes on recent ones); a weekly run walks every
month back to 2020 so old cases that close or escalate get picked up. One
request per second, ~170 requests for a full walk.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests

log = logging.getLogger("charles_code")

BASE = "https://land.charlescountymd.gov/energov_prod/selfservice/api"
PORTAL = "https://land.charlescountymd.gov/energov_prod/selfservice#/search"
CASE_URL = "https://land.charlescountymd.gov/energov_prod/selfservice#/codeCase/{id}"
OUT_DIR = "data/distress"
OUT_PATH = os.path.join(OUT_DIR, "charles-code.json")
TEMPLATE = os.path.join(os.path.dirname(__file__), "charles_search_template.json")
HISTORY_START = date(2020, 1, 1)
RECENT_DAYS = 60
PAGE_SIZE = 100
SLEEP = 1.0
HEADERS = {
    "User-Agent": "Mozilla/5.0 (md-property-leads; public code-case search)",
    "Accept": "application/json", "Content-Type": "application/json",
    "Tyler-Tenant-Culture": "en-US", "tenantId": "1",
    "tenantName": "CharlesCountyProd", "Tyler-TenantUrl": "CharlesCountyProd",
}

OPEN_STATUSES = {"in progress", "escalated", "hold", "on hold", "open", "pending", "new", "re-opened", "reopened"}


def _session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _criteria(s):
    return json.load(open(TEMPLATE, encoding="utf-8"))


def _page(s, base, page, frm=None, to=None):
    m = json.loads(json.dumps(base))
    m.update({"PageNumber": page, "PageSize": PAGE_SIZE})
    m["CodeCaseCriteria"].update({"PageNumber": page, "PageSize": PAGE_SIZE, "CodeCaseStatusId": "none",
                                  "OpenedDateFrom": f"{frm.isoformat()}T05:00:00.000Z" if frm else None,
                                  "OpenedDateTo": f"{to.isoformat()}T05:00:00.000Z" if to else None})
    for attempt in range(3):
        try:
            r = s.post(BASE + "/energov/search/search", data=json.dumps(m), timeout=90)
            r.raise_for_status()
            j = r.json()
            if not j.get("Success"):
                raise RuntimeError(j.get("ErrorMessage") or "search failed")
            return j["Result"]
        except Exception as e:  # noqa: BLE001
            log.warning("page %d attempt %d: %s", page, attempt + 1, e)
            time.sleep(5 * (attempt + 1))
    return None


def _slim(e):
    a = e.get("Address") or {}
    return {
        "case_id": e.get("CaseId"),
        "case_number": e.get("CaseNumber"),
        "type": e.get("CaseType"),
        "status": e.get("CaseStatus"),
        "opened": (e.get("ApplyDate") or "")[:10] or None,
        "closed": (e.get("CompleteDate") or e.get("FinalDate") or "")[:10] or None,
        "address": (e.get("AddressDisplay") or a.get("FullAddress") or "").strip(),
        "street": " ".join(x for x in (a.get("AddressLine1"), a.get("PreDirection"), a.get("AddressLine2"),
                                       a.get("StreetTypeName"), a.get("PostDirection")) if x).strip(),
        "unit": a.get("UnitOrSuite") or "",
        "city": a.get("City") or "",
        "zip": (a.get("PostalCode") or "")[:5],
        "parcel": (e.get("MainParcel") or "").strip(),
        "project": e.get("ProjectName") or "",
    }


def is_open(status):
    s = (status or "").lower()
    return not s.startswith("closed") and (s in OPEN_STATUSES or "progress" in s or "escalat" in s or "hold" in s)


def load():
    try:
        return json.load(open(OUT_PATH, encoding="utf-8"))
    except (OSError, ValueError):
        return {"cases": {}}


def _windows(full):
    """Month windows [from, to) to walk, oldest first."""
    today = date.today()
    start = HISTORY_START if full else today - timedelta(days=RECENT_DAYS)
    out = []
    cur = date(start.year, start.month, 1) if full else start
    while cur <= today:
        nxt = (date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)) if full else today + timedelta(days=1)
        out.append((cur, min(nxt, today + timedelta(days=1))))
        cur = nxt
    return out


def sweep(full=False):
    """full=False: re-read the last RECENT_DAYS. full=True: every month since 2020."""
    s = _session()
    base = _criteria(s)
    data = load()
    cases = data.setdefault("cases", {})
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    seen = new = changed = requests_made = 0
    portal_total = 0
    for frm, to in _windows(full):
        page = 1
        while True:
            res = _page(s, base, page, frm, to)
            requests_made += 1
            if not res:
                log.warning("window %s..%s page %d: no response, moving on", frm, to, page)
                break
            if page == 1:
                portal_total += res.get("TotalFound") or 0
                log.info("%s..%s: %d cases, %d pages", frm, to, res.get("TotalFound") or 0, res.get("TotalPages") or 0)
            for e in res.get("EntityResults") or []:
                rec = _slim(e)
                if not rec["case_number"]:
                    continue
                seen += 1
                old = cases.get(rec["case_number"])
                if old is None:
                    rec["first_seen"] = now
                    rec["status_history"] = [{"status": rec["status"], "seen": now}]
                    cases[rec["case_number"]] = rec
                    new += 1
                else:
                    if old.get("status") != rec["status"]:
                        old.setdefault("status_history", []).append({"status": rec["status"], "seen": now})
                        changed += 1
                    for k, v in rec.items():
                        if v not in (None, ""):
                            old[k] = v
                cases[rec["case_number"]]["last_seen"] = now
            if page >= (res.get("TotalPages") or 0):
                break
            page += 1
            time.sleep(SLEEP)
        time.sleep(SLEEP)
    data["generated_at"] = now
    data["source"] = PORTAL
    data["portal_total_in_windows"] = portal_total
    data["last_full_sweep"] = now if full else data.get("last_full_sweep")
    data["counts"] = {
        "cases_kept": len(cases), "seen_this_run": seen, "new": new, "status_changed": changed,
        "requests": requests_made, "open": sum(1 for c in cases.values() if is_open(c["status"])),
        "by_type": _count(cases, "type"), "by_status": _count(cases, "status"),
        "by_year": _count({k: {"y": (v.get("opened") or "")[:4]} for k, v in cases.items()}, "y"),
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    log.info("saved %d cases (%d new, %d status changes, %d open, %d requests)", len(cases), new, changed, data["counts"]["open"], requests_made)
    return data


def _count(cases, key):
    out = {}
    for c in cases.values():
        out[c.get(key) or "?"] = out.get(c.get(key) or "?", 0) + 1
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


PIA_DIR = os.path.join(OUT_DIR, "pia")


def pia_descriptions():
    """case_number -> description text released by the County under a PIA request
    (data/distress/pia/charles-code-descriptions-<date>.csv). Newest file wins."""
    import csv
    import glob
    out = {}
    for path in sorted(glob.glob(os.path.join(PIA_DIR, "charles-code-descriptions-*.csv"))):
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                if r.get("case_number") and r.get("description"):
                    out[r["case_number"].strip()] = {"text": r["description"].strip(), "source": r.get("source") or os.path.basename(path)}
    return out


def signals(data=None):
    """Turn the stored cases into distress_watch signal dicts (one per open
    case, plus a REPEAT_CODE_CASES signal for parcels with 3+ cases since 2020)."""
    data = data or load()
    cases = data.get("cases") or {}
    out = []
    per_parcel = {}
    for c in cases.values():
        key = c.get("parcel") or c.get("address")
        if key:
            per_parcel.setdefault(key, []).append(c)
    kind_for = {"Building Code Violation": "CODE_VIOLATIONS", "Nuisance": "NUISANCE_CASE",
                "Zoning": "ZONING_CASE", "Infrastructure Violation": "CODE_VIOLATIONS"}
    pia = pia_descriptions()
    for c in cases.values():
        if not is_open(c.get("status")):
            continue
        kind = kind_for.get(c.get("type") or "", "CODE_VIOLATIONS")
        esc = "escalat" in (c.get("status") or "").lower()
        out.append({
            "kind": kind, "source": "Charles County Code Enforcement (Civic Access)",
            "url": CASE_URL.format(id=c["case_id"]) if c.get("case_id") else PORTAL,
            "date": c.get("opened"), "county": "Charles",
            "address": c.get("street") or c.get("address"), "precision": "exact",
            "city": c.get("city"), "zip": c.get("zip"), "ref": c.get("parcel"),
            "detail": f"{c.get('case_number')} · {c.get('type')} · {c.get('status')}"
                      + (" · ESCALATED" if esc else "")
                      + (f" · CITED: {pia[c['case_number']]['text'][:300]} [{pia[c['case_number']]['source']}]" if c.get("case_number") in pia else "")
                      + (f" · {len(per_parcel.get(c.get('parcel') or c.get('address'), []))} cases at this parcel since 2020"
                         if len(per_parcel.get(c.get("parcel") or c.get("address"), [])) > 1 else ""),
            "residential": True, "escalated": esc,
        })
    for key, lst in per_parcel.items():
        if len(lst) >= 3:
            newest = max(lst, key=lambda c: c.get("opened") or "")
            out.append({
                "kind": "REPEAT_CODE_CASES", "source": "Charles County Code Enforcement (Civic Access)",
                "url": PORTAL, "date": newest.get("opened"), "county": "Charles",
                "address": newest.get("street") or newest.get("address"), "precision": "exact",
                "city": newest.get("city"), "zip": newest.get("zip"), "ref": newest.get("parcel"),
                "detail": f"{len(lst)} code cases at this parcel: " + ", ".join(
                    f"{c['case_number']} ({c.get('type')}, {c.get('status')})" for c in sorted(lst, key=lambda c: c.get("opened") or "", reverse=True)[:6]),
                "residential": True,
            })
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="walk every month since 2020 (default: last 60 days)")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    sweep(a.full)


if __name__ == "__main__":
    sys.exit(main())
