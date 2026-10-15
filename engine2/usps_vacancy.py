"""
USPS vacancy check for the leads Ray is about to mail.

USPS's free Addresses 3.0 API returns, for each delivery address, a
`vacant` flag (Y/N) that carriers set when a house has not collected mail
for about 90 days. That flag is the post office's own vacancy signal.

Ground rules (USPS API terms + Ray's credit budget):
  * Only leads the dashboard already rates "Likely vacant" (score 60+),
    i.e. the short list headed for a mail drop. No county-wide sweeps.
  * At most MAX_PER_RUN lookups per run, under the default 60/hour quota.
  * A checked address is not re-checked for RECHECK_DAYS.

Needs repo secrets USPS_CLIENT_ID and USPS_CLIENT_SECRET (the Consumer Key
and Consumer Secret from the USPS developer portal). Without them it exits
quietly so the workflow never fails.

  python -m engine2.usps_vacancy
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

TITLE_DIR = os.path.join("data", "title")
OUT_PATH = os.path.join("data", "usps", "vacancy.json")
API = os.environ.get("USPS_API_BASE", "https://apis.usps.com")
COUNTIES = os.environ.get(
    "USPS_COUNTIES", "charles,calvert,st-mary-s,prince-george-s,anne-arundel"
).split(",")
MAX_PER_RUN = int(os.environ.get("USPS_MAX_PER_RUN", "50"))
RECHECK_DAYS = int(os.environ.get("USPS_RECHECK_DAYS", "60"))
MIN_SCORE = int(os.environ.get("USPS_MIN_SCORE", "60"))  # dashboard's "Likely vacant" tier

log = logging.getLogger("usps_vacancy")


def vacancy_score(r):
    """Python copy of TitleApp.vacancy() in static/js/title.js (public-record part only)."""
    p = r.get("property") or {}
    f = set(r.get("flags") or [])
    if "VACANT_LAND" in f or "Residential" not in (p.get("property_type") or ""):
        return None
    s = 0
    pts = {"VACANT_NOTICE": 60, "CONDEMNED": 50, "RETURNED_MAIL": 30, "FIRE": 25,
           "CITY_WORK_ORDER": 15, "NUISANCE_CASE": 12}
    s += sum(v for k, v in pts.items() if k in f)
    s += 10 if "REPEAT_CODE_CASES" in f else 6 if "CODE_VIOLATIONS" in f else 0
    if f & {"ALL_OWNERS_DECEASED", "DECEASED_SOLE_OWNER_HIGH", "DECEASED_SOLE_OWNER_MEDIUM"}:
        s += 25
        ysd = max([m.get("years_since_death") or 0 for m in p.get("death_matches") or []
                   if m.get("identity_confidence") != "LOW"] or [0])
        if ysd >= 3:
            s += 10
    if f & {"PROBATE_CLOSED_STILL_TITLED", "PROBATE_NONE_FOUND"}:
        s += 5
    home = (p.get("occupancy_code") or "").upper() == "H"
    hs = bool((p.get("homestead_code") or "").strip())
    s += 15 if "NO_HOMESTEAD" in f else 0
    s += 0 if home else 10
    s += 10 if "MAIL_MISMATCH" in f else 5 if "CARE_OF" in f else 0
    s += 10 if f & {"TAX_SALE_SOLD", "TAX_SALE_STRUCK"} else 0
    s += 5 if "TAX_SALE_REPEAT" in f else 0
    s += 8 if "POOR_CONDITION" in f else 4 if "BELOW_AVG_CONDITION" in f else 0
    s += -35 if (home and hs) else -10 if home else 0
    return max(0, min(100, s))


def candidates(done):
    now = datetime.now(timezone.utc)
    out = []
    for slug in COUNTIES:
        path = os.path.join(TITLE_DIR, f"{slug.strip()}.json")
        if not os.path.exists(path):
            log.warning("no %s", path)
            continue
        for r in json.load(open(path)).get("rows", []):
            p = r.get("property") or {}
            addr = (p.get("property_address") or "").strip()
            zip5 = (p.get("zip_code") or "")[:5]
            if not addr or not zip5.isdigit() or zip5 == "00000":
                continue
            sc = vacancy_score(r)
            if sc is None or sc < MIN_SCORE:
                continue
            prev = done.get(r["id"])
            if prev and now - datetime.fromisoformat(prev["checked"]) < timedelta(days=RECHECK_DAYS):
                continue
            out.append((sc, r["scores"].get("priority", 0), r["id"], addr, p.get("city") or "", zip5))
    out.sort(reverse=True)
    return out


def token(cid, secret):
    r = requests.post(f"{API}/oauth2/v3/token", timeout=30, json={
        "client_id": cid, "client_secret": secret, "grant_type": "client_credentials"})
    r.raise_for_status()
    return r.json()["access_token"]


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cid, secret = os.environ.get("USPS_CLIENT_ID"), os.environ.get("USPS_CLIENT_SECRET")
    if not cid or not secret:
        log.info("USPS_CLIENT_ID / USPS_CLIENT_SECRET not set; skipping.")
        return 0
    done = {}
    if os.path.exists(OUT_PATH):
        done = json.load(open(OUT_PATH)).get("results", {})
    todo = candidates(done)
    log.info("%d leads due for a USPS check; doing up to %d", len(todo), MAX_PER_RUN)
    if not todo:
        return 0
    hdr = {"Authorization": f"Bearer {token(cid, secret)}", "Accept": "application/json"}
    n = 0
    for sc, _, lid, addr, city, zip5 in todo[:MAX_PER_RUN]:
        q = {"streetAddress": addr, "state": "MD", "ZIPCode": zip5}
        if city:
            q["city"] = city
        r = requests.get(f"{API}/addresses/v3/address", params=q, headers=hdr, timeout=30)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if r.status_code == 429:
            log.warning("USPS hourly quota reached; stopping, the rest go next run.")
            break
        if r.status_code in (400, 404):
            done[lid] = {"checked": now, "found": False, "address": addr}
        elif r.ok:
            info = r.json().get("additionalInfo") or {}
            a = r.json().get("address") or {}
            done[lid] = {
                "checked": now, "found": True, "address": addr,
                "vacant": info.get("vacant"), "dpv": info.get("DPVConfirmation"),
                "business": info.get("business"),
                "usps_address": f'{a.get("streetAddress", "")}, {a.get("city", "")} {a.get("ZIPCode", "")}-{a.get("ZIPPlus4") or ""}'.strip("-"),
            }
        else:
            log.warning("USPS %s for %s: %s", r.status_code, addr, r.text[:200])
            continue
        n += 1
        time.sleep(1.2)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    vac = sum(1 for v in done.values() if v.get("vacant") == "Y")
    json.dump({"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "checked": len(done), "vacant": vac, "results": done},
              open(OUT_PATH, "w"), indent=1, sort_keys=True)
    log.info("checked %d this run; %d of %d on file flagged vacant by USPS", n, vac, len(done))
    return 0


if __name__ == "__main__":
    sys.exit(main())
