"""Water-usage vacancy signals from utility exports Ray obtains by records request.

Files live in data/distress/water/<county-slug>-<utility>-<date>.csv with columns
  service_location, account, <YYYY/M> ... (monthly usage, gallons), Total
The county is taken from the file name prefix:
  stmarys-  -> St. Mary's   (MetCom)
  charles-  -> Charles      (Charles County Utilities)
  calvert-  -> Calvert
  (add more prefixes in COUNTY_PREFIX as exports arrive)

Signal: ZERO_WATER_USAGE when the newest MIN_ZERO_MONTHS billing months are all
zero. "Zero all year" is called out in the detail. A zero-usage service
location on a parcel with no improvements is just an empty lot with a meter
stub, so distress_watch drops those at match time (impr_value == 0).
"""
from __future__ import annotations

import csv
import glob
import logging
import os
import re

log = logging.getLogger("water_usage")

WATER_DIR = os.path.join("data", "distress", "water")
MIN_ZERO_MONTHS = 3
COUNTY_PREFIX = {"stmarys": "St. Mary's", "charles": "Charles", "calvert": "Calvert",
                 "annearundel": "Anne Arundel", "princegeorges": "Prince George's", "baltimorecounty": "Baltimore County"}
UTILITY_NAME = {"metcom": "St. Mary's County Metropolitan Commission (MetCom)"}


def _month_key(h):
    m = re.match(r"(\d{4})/(\d{1,2})$", h.strip())
    return (int(m.group(1)), int(m.group(2))) if m else None


def signals():
    out = []
    for path in sorted(glob.glob(os.path.join(WATER_DIR, "*.csv"))):
        name = os.path.basename(path)
        parts = name.split("-")
        county = COUNTY_PREFIX.get(parts[0].lower())
        if not county:
            log.warning("%s: unknown county prefix, skipped", name)
            continue
        utility = UTILITY_NAME.get(parts[1].lower(), parts[1]) if len(parts) > 2 else "county utility"
        asof = re.search(r"(\d{4}-\d{2}-\d{2})", name)
        asof = asof.group(1) if asof else None
        n = 0
        with open(path, encoding="utf-8", newline="") as f:
            rd = csv.DictReader(f)
            months = sorted((h for h in rd.fieldnames if _month_key(h)), key=_month_key)
            if not months:
                log.warning("%s: no month columns", name)
                continue
            for r in rd:
                usage = [int(float(r.get(m) or 0)) for m in months]
                recent = usage[-MIN_ZERO_MONTHS:]
                if any(recent):
                    continue
                zero_run = 0
                for u in reversed(usage):
                    if u:
                        break
                    zero_run += 1
                all_zero = zero_run == len(usage)
                addr = (r.get("service_location") or "").strip()
                if not addr or not addr[0].isdigit():
                    continue
                last = months[-1]
                out.append({
                    "kind": "ZERO_WATER_USAGE",
                    "source": f"{utility} — water usage export",
                    "url": None, "date": f"{_month_key(last)[0]}-{_month_key(last)[1]:02d}-01",
                    "county": county, "address": addr, "precision": "exact", "city": None, "zip": None,
                    "detail": (f"Zero water use for {'all ' if all_zero else ''}{zero_run} straight billing months "
                               f"({months[-zero_run].replace('/', '-')} → {last.replace('/', '-')})"
                               + ("" if all_zero else f" after using {sum(usage[:-zero_run]):,} gal earlier")
                               + f" · utility acct {r.get('account', '').strip()} · export {asof or 'n/a'}"),
                    "ref": None, "residential": True,
                    "all_zero": all_zero, "zero_months": zero_run, "needs_improvements": True,
                })
                n += 1
        log.info("%s: %d zero-usage service locations", name, n)
    return out
