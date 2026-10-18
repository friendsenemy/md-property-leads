"""Writ Watch: sheriff's sales of a judgment debtor's real property (Md. Rules 2/3-641..644).

Writes data/writ/watch.json for the Writ Watch tab.

Automatic sources (public pages only):
  * Montgomery County Sheriff "Sheriff Sales" page — the only Maryland sheriff that posts every
    real-property sale online (each listing is a PDF of the published notice; listings are deleted
    the business day after the sale). We read the table, then each notice: case, creditor, debtor,
    property, sale date/time and terms.
  * Cases Ray pasted into the tab (shared notes sheet rows "writ:<case>#writ") — we refresh their
    parcel facts from the state iMap parcel layer so the equity estimate stays current.

Not automated on purpose: Maryland Judiciary Case Search (its terms prohibit automated access) and
SDAT Real Property search. Ray pastes Case Search pages by hand; this script never touches either.
"""
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests

OUT = "data/writ/watch.json"
MC_PAGE = "https://www.montgomerycountymd.gov/sheriffs-office/sheriff-sales"
IMAP = "https://mdgeodata.md.gov/imap/rest/services/PlanningCadastre/MD_PropertyData/MapServer/0/query"
NOTES_URL = os.environ.get("SHARED_NOTES_URL", "")
UA = {"User-Agent": "Mozilla/5.0 (md-property-leads writ-watch; public data)"}
HOA_RE = re.compile(r"ASSOC|HOMEOWN|HMOWNR|CONDOMINIUM|COUNCIL OF (?:UNIT|CO-)|UNIT OWNERS|COMMUNITY|\bHOA\b|TOWNHOUSE|VILLAGE|RECREATION|\bPOA\b|OWNERS", re.I)
SUF = re.compile(r"^(DRIVE|DR|ROAD|RD|LANE|LN|COURT|CT|STREET|ST|AVENUE|AVE|WAY|PLACE|PL|TERRACE|TER|TERR|CIRCLE|CIR|BOULEVARD|BLVD|PARKWAY|PKWY|HIGHWAY|HWY|TRAIL|TRL|LOOP|ROW|RUN|PIKE|SQUARE|SQ)$")
FIELDS = "ACCTID,ADDRESS,CITY,ZIPCODE,OOI,NFMTTLVL,TRADATE,CONSIDR1,DR1LIBER,DR1FOLIO,JURSCODE,OWNADD1,OWNCITY,DESCLU,YEARBLT,SDATWEBADR"


def get(url, **kw):
    for i in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=60, **kw)
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            print("  retry", url[:90], e, file=sys.stderr)
            time.sleep(4 * (i + 1))
    return None


def parcel(address, jurs):
    """Look a street address up in the state parcel layer (public, no login)."""
    a = re.sub(r"\s+", " ", re.sub(r"[.,#]", " ", (address or "").upper())).strip()
    m = re.match(r"^(\d+[A-Z]?)\s+(.+)$", a)
    if not m:
        return None
    name = []
    for w in m.group(2).split(" "):
        if w in ("UNIT", "APT", "STE", "SUITE"):
            break
        if SUF.match(w) and name:
            break
        name.append(w)
        if len(name) >= 4:
            break
    where = "ADDRESS LIKE '%s%%'" % (m.group(1) + " " + " ".join(name)).replace("'", "''")
    if jurs:
        where += " AND JURSCODE='%s'" % jurs
    r = get(IMAP, params={"where": where, "outFields": FIELDS, "returnGeometry": "false", "f": "json"})
    feats = (r.json().get("features") if r else None) or []
    if not feats:
        return None
    p = feats[0]["attributes"]
    return {"acct": p["ACCTID"], "address": p["ADDRESS"], "city": p["CITY"], "zip": p["ZIPCODE"], "ooi": p["OOI"],
            "value": p["NFMTTLVL"], "tradate": p["TRADATE"], "price": p["CONSIDR1"],
            "deed": f'{p["DR1LIBER"]}/{p["DR1FOLIO"]}' if p["DR1LIBER"] else "", "jurs": p["JURSCODE"],
            "mail": ", ".join(x for x in (p["OWNADD1"], p["OWNCITY"]) if x), "use": p["DESCLU"], "built": p["YEARBLT"],
            "sdat": p["SDATWEBADR"], "matches": len(feats)}


def pdf_text(url):
    r = get(url)
    if not r:
        return ""
    try:
        from pypdf import PdfReader
        return "\n".join((pg.extract_text() or "") for pg in PdfReader(io.BytesIO(r.content)).pages)
    except Exception as e:  # noqa: BLE001
        print("  pdf read failed", url, e, file=sys.stderr)
        return ""


def parse_notice(text, url, addr_cell, date_cell):
    """Pull the useful facts out of a published sheriff's-sale notice."""
    t = re.sub(r"[ \t]+", " ", text)
    one = re.sub(r"\s+", " ", t)
    case = re.search(r"\b([A-Z]-\d{2,3}-[A-Z]{2}-\d{2}-\d{6}|\d{2}-\d{2}-\d{7}-\d{4}|\d{6}-?[A-Z]?)\b", one)
    cap = re.search(r"([A-Z0-9][A-Za-z0-9 ,.'&\-]{3,120}?)\s+(?:v\.|vs\.?|versus)\s+([A-Z][A-Za-z0-9 ,.'&\-]{2,120}?)(?:,|\s{2}|\s+Case|\s+Civil|\s+No\.)", one)
    when = re.search(r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})(?:[^.]{0,40}?(\d{1,2}:\d{2}\s*[AaPp]\.?\s*[Mm]\.?))?", one)
    dep = re.search(r"\$\s?([\d,]+(?:\.\d{2})?)\s*(?:deposit|DEPOSIT)|deposit of\s*\$\s?([\d,]+)", one)
    plaintiff = (cap.group(1).strip() if cap else "")
    plaintiff = re.sub(r"^.*?(?:REAL PROPERTY|REAL ESTATE|SHERIFF'?S SALE|NOTICE OF SALE)\s*[:\-]?\s*", "", plaintiff, flags=re.I).strip()
    row = {
        "case": (case.group(1) if case else "MC-" + re.sub(r"\W+", "", addr_cell)[:24]).upper(),
        "source": "montgomery-sheriff", "from": "Montgomery Sheriff sale notice", "notice_url": url,
        "county": "MONT", "court": "Montgomery County", "address": addr_cell, "city": "",
        "plaintiff": plaintiff, "defendant": cap.group(2).strip() if cap else "",
        "caption": f"{plaintiff} vs. {cap.group(2).strip()}" if cap else "",
        "hoa": bool(HOA_RE.search(plaintiff)),
        "sale_date": date_cell or (datetime.strptime(when.group(1), "%B %d, %Y").strftime("%Y-%m-%d") if when else ""),
        "sale_time": (when.group(2) or "") if when else "",
        "deposit": (dep.group(1) or dep.group(2)) if dep else "",
        "subject_to": bool(re.search(r"subject to", one, re.I)),
        "notice_excerpt": one[:900],
    }
    return row


def montgomery():
    r = get(MC_PAGE)
    if not r:
        return [], {"checked": now(), "count": 0, "error": "page unreachable"}
    html = r.text
    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        link = re.search(r'href="([^"]+)"[^>]*>(.*?)</a>', tr, re.S | re.I)
        if not link:
            continue
        href = link.group(1)
        if href.startswith("/"):
            href = "https://www.montgomerycountymd.gov" + href
        addr = re.sub(r"<[^>]+>|\s+", " ", link.group(2)).strip()
        cells = [re.sub(r"<[^>]+>|\s+", " ", c).strip() for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S | re.I)]
        dcell = next((c for c in cells if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}|\b20\d{2}\b", c)), "")
        dm = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", dcell)
        iso = ""
        if dm:
            y = int(dm.group(3)) + (2000 if len(dm.group(3)) == 2 else 0)
            iso = f"{y:04d}-{int(dm.group(1)):02d}-{int(dm.group(2)):02d}"
        text = pdf_text(href) if href.lower().endswith(".pdf") else ""
        row = parse_notice(text, href, addr, iso)
        row["prop"] = parcel(addr, "MONT")
        rows.append(row)
        time.sleep(1)
    return rows, {"checked": now(), "count": len(rows)}


def pasted():
    """Cases Ray pasted in the tab, from the shared notes sheet (newest row per case)."""
    if not NOTES_URL:
        return []
    r = get(NOTES_URL, params={"t": int(time.time())})
    if not r:
        return []
    try:
        rows = r.json().get("rows") or []
    except Exception:  # noqa: BLE001
        return []
    rows.sort(key=lambda x: str(x.get("ts", "")), reverse=True)
    seen, out = set(), []
    for row in rows:
        rid = str(row.get("id", ""))
        if not rid.endswith("#writ") or rid in seen:
            continue
        seen.add(rid)
        try:
            c = json.loads(row.get("notes") or "{}")
        except Exception:  # noqa: BLE001
            continue
        if not c.get("case"):
            continue
        if c.get("address") and not c.get("satisfied"):
            p = parcel(c["address"], c.get("county") or "")
            if p:
                c["prop"] = p
            time.sleep(0.5)
        c["source"] = "pasted"
        out.append(c)
    return out


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    prev = {}
    try:
        prev = {r["case"]: r for r in json.load(open(OUT)).get("rows", [])}
    except Exception:  # noqa: BLE001
        pass
    mc_rows, mc_meta = montgomery()
    print(f"Montgomery sheriff page: {mc_meta.get('count')} listing(s)")
    ps = pasted()
    print(f"Pasted cases refreshed: {len(ps)}")
    by = {}
    # keep Montgomery listings seen earlier for 60 days after their sale date (the county deletes them the next day)
    for c, r in prev.items():
        if r.get("source") == "montgomery-sheriff" and r.get("sale_date"):
            try:
                if (datetime.now() - datetime.strptime(r["sale_date"], "%Y-%m-%d")).days <= 60:
                    by[c] = r
            except ValueError:
                pass
    for r in mc_rows + ps:
        r.setdefault("first_seen", (prev.get(r["case"]) or {}).get("first_seen") or now())
        by[r["case"]] = r
    out = {"generated_at": now(), "sources": {"montgomery": mc_meta, "pasted": {"count": len(ps)}},
           "rows": sorted(by.values(), key=lambda r: (r.get("sale_date") or "9999", r["case"]))}
    json.dump(out, open(OUT, "w"), indent=1)
    new = [r for r in mc_rows if r["case"] not in prev]
    print(f"Wrote {OUT}: {len(out['rows'])} rows, {len(new)} new Montgomery listing(s)")


if __name__ == "__main__":
    main()
