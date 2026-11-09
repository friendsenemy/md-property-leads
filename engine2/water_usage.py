"""Water-usage vacancy signals from utility exports Ray obtains by records request.

Files live in data/distress/water/<county-slug>-<utility>-<date>.csv with columns
  service_location, account, <YYYY/M> ... (monthly usage, gallons), Total
The county is taken from the file name prefix:
  stmarys-  -> St. Mary's   (MetCom)
  charles-  -> Charles      (Charles County Utilities)
  calvert-  -> Calvert
  (add more prefixes in COUNTY_PREFIX as exports arrive)

Shut-off lists (data/distress/water/<county>-<utility>-shutoffs-<date>.csv, columns
  tran_date, service_address, zip, source) are water service disconnect /
shut-off transactions released under a PIA request. Signal: WATER_SHUTOFF, one
per address. Charles bills quarterly, so repeat shut-offs one cycle apart mean
the account was reconnected each time (occupied, chronically late); a single
shut-off with nothing after it may mean the water is still off (possible
vacancy). 3+ shut-offs is flagged WATER_SHUTOFF_CHRONIC.

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


def shutoff_signals():
    out = []
    for path in sorted(glob.glob(os.path.join(WATER_DIR, "*-shutoffs-*.csv"))):
        name = os.path.basename(path)
        county = COUNTY_PREFIX.get(name.split("-")[0].lower())
        if not county:
            log.warning("%s: unknown county prefix, skipped", name)
            continue
        per = {}
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                addr = (r.get("service_address") or "").strip().upper()
                if not addr or not addr[0].isdigit():
                    continue
                key = (addr, (r.get("zip") or "").strip()[:5])
                per.setdefault(key, {"dates": [], "source": r.get("source") or "county utility shut-off list"})
                per[key]["dates"].append((r.get("tran_date") or "").strip())
        for (addr, zip5), v in per.items():
            dates = sorted(d for d in v["dates"] if d)
            n = len(dates)
            out.append({
                "kind": "WATER_SHUTOFF",
                "source": v["source"],
                "url": None, "date": dates[-1] if dates else None,
                "county": county, "address": addr, "precision": "exact", "city": None, "zip": zip5 or None,
                # Charles bills quarterly, so one shut-off per cycle = reconnected and shut off again
                # each quarter (occupied, chronically late); a single shut-off may still be off (vacant?).
                "detail": ((f"Water shut off in {n} quarterly billing cycles ({dates[0]} → {dates[-1]}) — reconnected "
                            f"each time, so OCCUPIED but chronically behind on the bill"
                            if n >= 3 else
                            f"Water shut off twice ({dates[0]}, {dates[-1]}) — reconnected once, late payer"
                            if n == 2 else
                            f"Water service shut off {dates[0]} — no later transaction, so either paid up or STILL OFF (possible vacancy)")
                           + " · county utility shut-off list (PIA)"),
                "ref": None, "residential": True, "shutoffs": n, "needs_improvements": True,
            })
        log.info("%s: %d shut-off addresses", name, len(per))
    return out


def signals():
    out = shutoff_signals()
    for path in sorted(glob.glob(os.path.join(WATER_DIR, "*.csv"))):
        name = os.path.basename(path)
        if "-shutoffs-" in name:
            continue
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
