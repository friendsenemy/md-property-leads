"""
Heir audience keeper — the master list of skip-traced, obituary-confirmed heirs
and the weekly Meta customer-list file built from it.

The master list holds phones and emails, so it never goes in this (public) repo.
Pass its path on the command line; it lives in Ray's private storage.

  python heir_audience.py MASTER.json OUT_DIR [--days 180]

Each run:
  1. Drops every heir whose family member died more than --days ago
     (default 180 = 6 months). Dropped heirs are deleted from the master.
  2. Writes OUT_DIR/meta_heir_audience.csv in Meta's customer-list template
     (email x3, phone x3 with +1, fn, ln, zip, ct, st, country, doby, age).
     Upload it with "Replace customer list" on the existing audience so the
     dropped heirs leave the Facebook audience too.
  3. Writes OUT_DIR/heir_review.csv: one heir per line with the property,
     county and SDAT account it ties to.

Records in the master: {"heir_key": {...heir..., "date_of_death": "YYYY-MM-DD",
"found_at": ISO, "lead_id", "property_address", "county", "sdat_account",
"relation", "phones": [...], "emails": [...]}}
"""

import os
import re
import csv
import sys
import json
import argparse
from datetime import datetime, timedelta, timezone

META_COLS = ["email", "email", "email", "phone", "phone", "phone",
             "fn", "ln", "zip", "ct", "st", "country", "doby", "age"]


def death_date(rec):
    """Exact date of death when known, otherwise when the lead was found."""
    d = rec.get("date_of_death") or ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
        return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat((rec.get("found_at") or "").replace("Z", "+00:00"))
    except ValueError:
        return None


def phone_e164(p):
    digits = re.sub(r"\D", "", p or "")
    if len(digits) == 10:
        return "1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return digits
    return ""


def prune(master, days, now=None):
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    keep, dropped = {}, []
    for k, rec in master.items():
        dd = death_date(rec)
        if dd is None or dd < cutoff:
            dropped.append(rec)
        else:
            keep[k] = rec
    return keep, dropped


def meta_row(rec):
    emails = [e.strip().lower() for e in rec.get("emails", []) if e and "@" in e][:3]
    phones = [p for p in (phone_e164(x) for x in rec.get("phones", [])) if p][:3]
    emails += [""] * (3 - len(emails))
    phones += [""] * (3 - len(phones))
    zip5 = re.sub(r"\D", "", rec.get("zip") or "")[:5]
    return emails + phones + [
        (rec.get("fn") or "").strip(), (rec.get("ln") or "").strip(), zip5,
        (rec.get("ct") or "").strip(), (rec.get("st") or "").strip().upper(), "US",
        str(rec.get("doby") or ""), str(rec.get("age") or ""),
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("master")
    ap.add_argument("out_dir")
    ap.add_argument("--days", type=int, default=180)
    a = ap.parse_args()

    master = json.load(open(a.master)) if os.path.exists(a.master) else {}
    master, dropped = prune(master, a.days)
    json.dump(master, open(a.master, "w"), indent=1)

    os.makedirs(a.out_dir, exist_ok=True)
    rows = [meta_row(r) for r in master.values() if r.get("phones") or r.get("emails")]
    with open(os.path.join(a.out_dir, "meta_heir_audience.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(META_COLS)
        w.writerows(rows)

    with open(os.path.join(a.out_dir, "heir_review.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["heir", "relation", "phones", "emails", "heir_city", "heir_state",
                    "decedent", "date_of_death", "drops_off_after",
                    "property_address", "county", "sdat_account"])
        for r in sorted(master.values(), key=lambda r: (r.get("county", ""), r.get("decedent", ""))):
            dd = death_date(r)
            w.writerow([f"{r.get('fn','')} {r.get('ln','')}", r.get("relation", ""),
                        "\n".join(r.get("phones", [])), "\n".join(r.get("emails", [])),
                        r.get("ct", ""), r.get("st", ""), r.get("decedent", ""),
                        r.get("date_of_death", ""),
                        (dd + timedelta(days=a.days)).date().isoformat() if dd else "",
                        r.get("property_address", ""), r.get("county", ""),
                        r.get("sdat_account", "")])

    print(f"kept {len(master)} heirs ({len(rows)} with a phone or email), "
          f"dropped {len(dropped)} past {a.days} days")


if __name__ == "__main__":
    main()
