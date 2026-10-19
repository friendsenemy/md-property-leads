"""
Servicer-Owned Houses — every Maryland parcel whose current owner is a loan servicer, note
investor, loan trust or lender (statewide, licensed SDAT feed with owner names).

Why: when a second-mortgage company (Real Time Resolutions, a Wilmington Savings Fund Society
trust, etc.) forecloses its junior lien, it ends up owning a house that still carries the first
mortgage. It is not a homeowner, so nobody mails it; it usually just wants out. Baltimore XV bought
3908 Dunes Way, Burtonsville from Real Time Resolutions for $9,000 subject to a $327K first and
resold it a month later for $529,000.

  python -m engine2.servicer_owned          # writes data/players/servicer_owned.json
"""

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone

import requests

from engine2 import config
from engine2.build_index import _auth, _get
from engine2.repeat_buyers import columns, PUBLIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("servicer_owned")

# junior-lien servicers / note investors first (least competition), then trusts, then big REO sellers
GROUPS = {
    "junior_servicer": ["REAL TIME RESOLUTION", "SPECIALIZED LOAN", "FCI LENDER", "BSI FINANCIAL", "SN SERVICING", "BOSCO",
                        "STATEBRIDGE", "PLANET HOME", "FAY SERVICING", "ROUNDPOINT", "SELENE", "RUSHMORE", "MCLP ASSET",
                        "VRMTG", "LSF9", "LSF10", "LSF11", "TOWD POINT", "CSMC", "MFRA", "PRETIUM", "BAYVIEW", "LAKEVIEW",
                        "CARRINGTON MORTGAGE", "SHELLPOINT", "NEWREZ", "SERVBANK", "GREGORY FUNDING", "KONDAUR", "FRANKLIN CREDIT",
                        "BCMB", "VENTURES TRUST", "ATLANTICA", "ALLIED FIRST", "CIG FINANCIAL", "HOME POINT", "MIDFIRST",
                        "PENNYMAC", "NATIONSTAR", "MR COOPER", "OCWEN", "PHH MORTGAGE", "DITECH", "CENLAR", "LOANCARE"],
    "loan_trust": ["WILMINGTON SAVINGS FUND", "WILMINGTON TRUST", "U S BANK TRUST", "US BANK TRUST", "U.S. BANK TRUST",
                   "DEUTSCHE BANK NATIONAL TRUST", "BANK OF NEW YORK MELLON", "CHRISTIANA TRUST", "HSBC BANK USA",
                   "CITIBANK N A AS TRUSTEE", "WELLS FARGO BANK N A AS TRUSTEE", "MORTGAGE TRUST", "LOAN TRUST", "ASSET TRUST",
                   "TITLING TRUST", "ACQUISITION TRUST", "REO TRUST"],
    "agency_reo": ["FEDERAL NATIONAL MORTGAGE", "FEDERAL HOME LOAN MORTGAGE", "SECRETARY OF HOUSING", "SECRETARY OF VETERANS"],
}


def run(public=False):
    s = requests.Session()
    dataset = config.DATASET_ID if (_auth() and not public) else PUBLIC
    cols = columns(dataset, s)
    if "owner1" not in cols:
        log.error("owner names need the licensed dataset (secrets missing)"); sys.exit(2)
    url = f"https://opendata.maryland.gov/resource/{dataset}.json"
    o = cols["owner1"]
    rev = {v: k for k, v in cols.items()}
    out, seen = [], set()
    for group, names in GROUPS.items():
        for i in range(0, len(names), 6):
            chunk = names[i:i + 6]
            where = " OR ".join(f"upper({o}) like '%{n.replace(chr(39), chr(39) * 2)}%'" for n in chunk)
            off = 0
            while True:
                rows = _get(url, {"$select": ",".join(cols.values()), "$where": where, "$limit": config.PAGE_SIZE, "$offset": off}, s)
                for raw in rows:
                    r = {rev[k]: v for k, v in raw.items() if k in rev}
                    if r.get("acct") in seen:
                        continue
                    seen.add(r.get("acct"))
                    own = (r.get("owner1") or "").upper()
                    r["group"] = group
                    r["matched"] = next((n for n in chunk if n in own), "")
                    out.append(r)
                log.info("%s %s: +%d", group, chunk[0], len(rows))
                if len(rows) < config.PAGE_SIZE:
                    break
                off += len(rows)
    os.makedirs("data/players", exist_ok=True)
    json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "count": len(out), "rows": out},
              open("data/players/servicer_owned.json", "w"), separators=(",", ":"))
    log.info("wrote %d servicer/trust/agency-owned parcels", len(out))


if __name__ == "__main__":
    try:
        run("--public" in sys.argv)
    except Exception as e:  # noqa: BLE001
        log.exception("servicer scan failed: %s", e)
        sys.exit(1)
