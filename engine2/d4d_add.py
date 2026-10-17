"""
Driving for Dollars: add or update a property Ray spotted on the road.

  python -m engine2.d4d_add 0502011816 research.json

The first argument is the SDAT account (county code + district + account, the
ACCTID in Maryland's parcel layer). research.json holds what the deep dive found:
  {"spotted": "2026-10-17", "verdict": "warm", "headline": "...",
   "owners": ["..."], "deed": "KPS 4308/321 ...", "findings": ["...", "..."],
   "next_steps": ["..."], "sources": ["..."]}
Parcel facts (address, owner on SDAT, values, structure, coordinates) come from the
state's public parcel layer, so the card always matches SDAT's own record.

Writes data/d4d/leads.json, one row per account (re-running updates the row). The
row id is the account number, the same id the shared notes sheet uses.
"""
import datetime as dt
import json
import os
import sys

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", "d4d", "leads.json")
LAYER = "https://mdgeodata.md.gov/imap/rest/services/PlanningCadastre/MD_PropertyData/MapServer/0/query"
COUNTY = {"01": "Allegany", "02": "Anne Arundel", "03": "Baltimore City", "04": "Baltimore County", "05": "Calvert",
          "06": "Caroline", "07": "Carroll", "08": "Cecil", "09": "Charles", "10": "Dorchester", "11": "Frederick",
          "12": "Garrett", "13": "Harford", "14": "Howard", "15": "Kent", "16": "Montgomery", "17": "Prince George's",
          "18": "Queen Anne's", "19": "St. Mary's", "20": "Somerset", "21": "Talbot", "22": "Washington",
          "23": "Wicomico", "24": "Worcester"}


def parcel(acct):
    r = requests.get(LAYER, params={"where": f"ACCTID='{acct}'", "outFields": "*", "returnGeometry": "true",
                                    "outSR": 4326, "f": "json"}, timeout=90)
    r.raise_for_status()
    feats = r.json().get("features") or []
    if not feats:
        raise SystemExit(f"{acct}: not in the state parcel layer")
    a, g = feats[0]["attributes"], feats[0].get("geometry") or {}
    s = lambda k: (str(a.get(k)).strip() if a.get(k) not in (None, "") else "")
    num = lambda k: a.get(k) or 0
    liber, folio = s("DR1LIBER").lstrip("0"), s("DR1FOLIO").lstrip("0")
    trad = s("TRADATE")
    return {
        "account": acct, "county": COUNTY.get(acct[:2], acct[:2]),
        "property_address": s("ADDRESS"), "city": s("CITY"), "zip_code": s("ZIPCODE"),
        "owner_mailing": ", ".join(x for x in (s("OWNADD1"), s("OWNADD2"), f'{s("OWNCITY")} {s("OWNSTATE")} {s("OWNERZIP")}'.strip()) if x),
        "map": s("MAP"), "grid": s("GRID"), "parcel": s("PARCEL"), "lot": s("LOT"), "subdivision": s("SUBDIVSN"),
        "legal": " ".join(x for x in (s("LEGAL1"), s("LEGAL2"), s("LEGAL3")) if x),
        "zoning": s("ZONING"), "land_use": s("DESCLU"), "acres": a.get("ACRES"),
        "year_built": s("YEARBLT"), "sqft": a.get("SQFTSTRC"), "style": s("DESCSTYL").replace("STRY ", ""),
        "land_value": num("NFMLNDVL"), "impr_value": num("NFMIMPVL"), "assessed_value": num("NFMTTLVL"),
        "deed_ref": f"{liber}/{folio}" if liber else "",
        "last_transfer": f"{trad[:4]}-{trad[4:6]}-{trad[6:]}" if len(trad) == 8 else "",
        "last_price": a.get("CONSIDR1") or 0,
        "owner_occupied_code": s("OOI"), "homestead_qualified": s("HOMQLDAT") if s("HOMQLCOD") else "",
        "lat": g.get("y"), "lon": g.get("x"), "sdat_url": s("SDATWEBADR"),
    }


def run(acct, research_path):
    research = json.load(open(research_path))
    row = parcel(acct)
    row.update(research)
    row["id"] = acct
    row["updated"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    data = json.load(open(OUT)) if os.path.exists(OUT) else {"rows": []}
    data["rows"] = [r for r in data["rows"] if r.get("id") != acct] + [row]
    data["rows"].sort(key=lambda r: (r.get("spotted") or "", r.get("id")), reverse=True)
    data["generated_at"] = row["updated"]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(data, open(OUT, "w"), indent=1)
    print(f"{acct} {row['property_address']}, {row['city']} -> {OUT} ({len(data['rows'])} rows)")


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2])
