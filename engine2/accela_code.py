"""Code-enforcement cases from Accela Citizen Access portals (Baltimore County,
Anne Arundel "Land Use Navigator"). Washington County's portal exposes no
enforcement module publicly; Howard County's requires a login, so neither is here.

Each portal has a public "Search Enforcement Records" form that accepts a date
range and returns a paged list (10 per page) of case number, type, the
complaint / violation description, address and status — the same list a person
sees on the page. We submit that form the way the browser does (ASP.NET
postback with the page's own viewstate), walk the pager, one request per
second, and keep what the public list shows. No login, no gated tabs.

Output: data/distress/accela-code.json
  {generated_at, sites: {key: {counts...}}, cases: {"<site>:<case_number>": {...}}}

distress_watch turns open cases into signals (CODE_VIOLATIONS / NUISANCE_CASE /
REPEAT_CODE_CASES), matched to SDAT by address inside the county.
"""
from __future__ import annotations

import argparse
import html as htmlmod
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests

log = logging.getLogger("accela_code")

OUT_DIR = "data/distress"
OUT_PATH = os.path.join(OUT_DIR, "accela-code.json")
SLEEP = 1.0
RECENT_DAYS = int(os.environ.get("ACCELA_RECENT_DAYS", "45"))
HISTORY_START = date(2023, 1, 1)
MAX_PAGES_PER_WINDOW = 400       # 4,000 records; windows are a month, so this is a safety stop

SITES = {
    "baltimore_county": {
        "county": "Baltimore County",
        "base": "https://citizenaccess.baltimorecountymd.gov/CitizenAccess",
        "home": "https://citizenaccess.baltimorecountymd.gov/CitizenAccess/Cap/CapHome.aspx?module=Enforcement&TabName=Enforcement",
        "detail": "https://citizenaccess.baltimorecountymd.gov/CitizenAccess/Cap/CapHome.aspx?module=Enforcement&TabName=Enforcement",
        "source": "Baltimore County — Code Enforcement (CitizenAccess)",
    },
    "anne_arundel": {
        "county": "Anne Arundel",
        "base": "https://aca-prod.accela.com/AACO",
        "home": "https://aca-prod.accela.com/AACO/Cap/CapHome.aspx?module=Enforcement&TabName=Enforcement",
        "detail": "https://aca-prod.accela.com/AACO/Cap/CapHome.aspx?module=Enforcement&TabName=Enforcement",
        "source": "Anne Arundel County — Complaints / Violations (Land Use Navigator)",
    },
}

CLOSED_RE = re.compile(r"closed|complete|resolved|abated|void|cancel|dismiss|no violation|unfounded|duplicate|withdrawn|compliance achieved|in compliance", re.I)


def is_open(status):
    return bool(status) and not CLOSED_RE.search(status)


def _session(site):
    s = requests.Session()
    origin = re.match(r"https://[^/]+", site["base"]).group(0)
    s.headers.update({"User-Agent": "Mozilla/5.0 (md-property-leads; public enforcement search)",
                      "Referer": site["home"], "Origin": origin})
    return s


def _form(h):
    form = {}
    for m in re.finditer(r"<input([^>]*)>", h):
        a = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
        if a.get("name") and a.get("type") in ("hidden", "text", "tel"):
            form[a["name"]] = htmlmod.unescape(a.get("value", ""))
    for m in re.finditer(r'<select name="([^"]+)"[^>]*>(.*?)</select>', h, re.S):
        sel = re.search(r'<option[^>]*selected[^>]*value="([^"]*)"', m.group(2)) or re.search(r'<option[^>]*value="([^"]*)"', m.group(2))
        form[m.group(1)] = sel.group(1) if sel else ""
    return form


def _clean(c):
    return htmlmod.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c))).strip()


def _rows(t):
    hdr = re.findall(r'<tr class="ACA_TabRow_Header[^"]*".*?</tr>', t, re.S)
    cols = [_clean(c) for c in re.findall(r"<th[^>]*>(.*?)</th>", hdr[0], re.S)] if hdr else []
    out = []
    for row in re.findall(r'<tr class="ACA_TabRow_(?:Odd|Even)[^"]*".*?</tr>', t, re.S):
        cells = [_clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)]
        rec = dict(zip(cols, cells))
        m = re.search(r'href="([^"]*CapDetail\.aspx[^"]*)"', row)
        if m:
            rec["_detail"] = htmlmod.unescape(m.group(1))
        out.append(rec)
    return out


def _next_target(t):
    m = re.search(r"__doPostBack\(&#39;([^&]*)&#39;,&#39;&#39;\)\">Next &gt;", t)
    return m.group(1) if m else None


def _post(s, url, form, target):
    form = dict(form)
    form.update({"__EVENTTARGET": target, "__EVENTARGUMENT": ""})
    for attempt in range(3):
        try:
            r = s.post(url, data=form, timeout=120)
            r.raise_for_status()
            if "cross-site request forgery" in r.text:
                raise RuntimeError("CSRF check")
            return r.text
        except Exception as e:  # noqa: BLE001
            log.warning("post attempt %d: %s", attempt + 1, e)
            time.sleep(5 * (attempt + 1))
    return None


def search(site, frm, to):
    """All enforcement records opened in [frm, to] on one portal."""
    s = _session(site)
    h = s.get(site["home"], timeout=60).text
    form = _form(h)
    form["ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate"] = frm.strftime("%m/%d/%Y")
    form["ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate"] = to.strftime("%m/%d/%Y")
    t = _post(s, site["home"], form, "ctl00$PlaceHolderMain$btnNewSearch")
    if t is None:
        return None
    out, page = [], 1
    while True:
        out += _rows(t)
        nxt = _next_target(t)
        if not nxt or page >= MAX_PAGES_PER_WINDOW:
            break
        page += 1
        time.sleep(SLEEP)
        t = _post(s, site["home"], _form(t), nxt)
        if t is None:
            break
    return out


def _slim(site_key, site, r):
    addr = r.get("Address") or ""
    # Baltimore County repeats the house number: "350 350 HOMBERG AVE, ESSEX MD 21221, 350"
    addr = re.sub(r"^(\d+)\s+\1\s+", r"\1 ", addr)
    addr = re.sub(r",\s*\d+\s*$", "", addr).strip()
    zm = re.search(r"\b(\d{5})(?:-\d{4})?\s*$", addr)
    city = ""
    cm = re.search(r",\s*([A-Z .'-]+?)\s+MD\b", addr)
    if cm:
        city = cm.group(1).strip()
    street = re.split(r",", addr)[0].strip()
    num = r.get("Case Number") or r.get("Record Number") or ""
    return {
        "site": site_key, "county": site["county"],
        "case_number": num, "type": r.get("Case Type") or r.get("Record Type") or "",
        "status": r.get("Status") or "", "opened": _mdy(r.get("Date")),
        "description": (r.get("Description") or "")[:600],
        "address": addr, "street": street, "city": city, "zip": zm.group(1) if zm else "",
        "url": (re.match(r"https://[^/]+", site["base"]).group(0) + r["_detail"]) if r.get("_detail", "").startswith("/") else (r.get("_detail") or site["detail"]),
    }


def _mdy(s):
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s or "")
    return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}" if m else None


def load():
    try:
        return json.load(open(OUT_PATH, encoding="utf-8"))
    except (OSError, ValueError):
        return {"cases": {}, "sites": {}}


def _windows(full):
    today = date.today()
    if not full:
        return [(today - timedelta(days=RECENT_DAYS), today)]
    out, cur = [], HISTORY_START
    while cur <= today:
        nxt = date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
        out.append((cur, min(nxt - timedelta(days=1), today)))
        cur = nxt
    return out


def sweep(full=False, only=None):
    data = load()
    cases = data.setdefault("cases", {})
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for key, site in SITES.items():
        if only and key not in only:
            continue
        seen = new = changed = 0
        failed = False
        for frm, to in _windows(full):
            try:
                rows = search(site, frm, to)
            except Exception as e:  # noqa: BLE001
                log.warning("%s %s..%s failed: %s", key, frm, to, e)
                rows = None
            if rows is None:
                failed = True
                continue
            log.info("%s %s..%s: %d records", key, frm, to, len(rows))
            for r in rows:
                rec = _slim(key, site, r)
                if not rec["case_number"]:
                    continue
                seen += 1
                k = f"{key}:{rec['case_number']}"
                old = cases.get(k)
                if old is None:
                    rec["first_seen"] = now
                    rec["status_history"] = [{"status": rec["status"], "seen": now}]
                    cases[k] = rec
                    new += 1
                else:
                    if old.get("status") != rec["status"]:
                        old.setdefault("status_history", []).append({"status": rec["status"], "seen": now})
                        changed += 1
                    for f, v in rec.items():
                        if v not in (None, ""):
                            old[f] = v
                cases[k]["last_seen"] = now
            time.sleep(SLEEP)
        mine = [c for c in cases.values() if c["site"] == key]
        data["sites"][key] = {"county": site["county"], "last_run": now, "failed": failed, "seen": seen, "new": new,
                              "status_changed": changed, "kept": len(mine), "open": sum(1 for c in mine if is_open(c["status"])),
                              "by_type": _count(mine, "type"), "by_status": _count(mine, "status")}
        log.info("%s: kept %d (new %d, changed %d, open %d)", key, len(mine), new, changed, data["sites"][key]["open"])
        data["generated_at"] = now
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    data["generated_at"] = now
    if full:
        data["last_full_sweep"] = now
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    return data


def _count(cases, key):
    out = {}
    for c in cases:
        out[c.get(key) or "?"] = out.get(c.get(key) or "?", 0) + 1
    return dict(sorted(out.items(), key=lambda kv: -kv[1])[:25])


NUISANCE_RE = re.compile(r"grass|weed|trash|debris|junk|rubbish|litter|vehicle|rat|rodent|nuisance|overgrown|dumping", re.I)
STRUCTURE_RE = re.compile(r"vacant|abandon|unsafe|condemn|collaps|structur|roof|fire|boarded|unsecured|open to|dilapidat|demoli|uninhabit|no (?:water|power|heat)|squat", re.I)


def signals(data=None):
    data = data or load()
    cases = data.get("cases") or {}
    out, per = [], {}
    for c in cases.values():
        key = (c["site"], (c.get("street") or "").upper(), c.get("zip"))
        per.setdefault(key, []).append(c)
    for c in cases.values():
        if not is_open(c.get("status")):
            continue
        text = f"{c.get('type')} {c.get('description')}"
        ctype = c.get("type") or ""
        if re.search(r"vacant", ctype, re.I):
            kind = "VACANT_NOTICE"                      # Baltimore County "Vacant Property Inventory"
        elif re.search(r"zoning", ctype, re.I):
            kind = "ZONING_CASE"
        elif STRUCTURE_RE.search(text) or re.search(r"building|plumbing|electrical|property maintenance|housing|unsafe|rental", ctype, re.I):
            kind = "CODE_VIOLATIONS"
        else:
            kind = "NUISANCE_CASE"
        cited = bool(re.search(r"citation|correction notice|in violation|hearing", c.get("status") or "", re.I))
        n = len(per[(c["site"], (c.get("street") or "").upper(), c.get("zip"))])
        out.append({
            "kind": kind, "source": SITES[c["site"]]["source"], "url": c.get("url"),
            "date": c.get("opened"), "county": c["county"], "address": c.get("street"), "precision": "exact",
            "city": c.get("city"), "zip": c.get("zip"),
            "detail": f"{c['case_number']} · {c.get('type')} · {c.get('status')}"
                      + (" · INSPECTOR CONFIRMED" if cited else (" · complaint, not yet inspected" if re.search(r"complaint", ctype, re.I) else ""))
                      + (f" · {c['description'][:220]}" if c.get("description") else "")
                      + (f" · {n} cases at this address" if n > 1 else ""),
            "residential": True, "escalated": cited,
        })
    for key, lst in per.items():
        if len(lst) >= 3:
            newest = max(lst, key=lambda c: c.get("opened") or "")
            out.append({
                "kind": "REPEAT_CODE_CASES", "source": SITES[newest["site"]]["source"], "url": newest.get("url"),
                "date": newest.get("opened"), "county": newest["county"], "address": newest.get("street"), "precision": "exact",
                "city": newest.get("city"), "zip": newest.get("zip"),
                "detail": f"{len(lst)} code cases at this address: " + ", ".join(
                    f"{c['case_number']} ({c.get('type')}, {c.get('status')})" for c in sorted(lst, key=lambda c: c.get("opened") or "", reverse=True)[:6]),
                "residential": True,
            })
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="walk every month since 2023 (default: last 45 days)")
    ap.add_argument("--only", help="comma-separated site keys")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    sweep(a.full, set(a.only.split(",")) if a.only else None)


if __name__ == "__main__":
    sys.exit(main())
