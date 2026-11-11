"""
Dead-owner side of the judgment feed.

Takes every parcel in data/judgments/leads.json (all open-judgment parcels
statewide) and asks three death sources whether the OWNER is dead:

  1. MSA SE-151 death index 1973-2014      (engine2.death_index, identity-scored)
  2. Register of Wills estate files          (data/probate/<account>.json, from
                                              probate_import / probate_lookup)
  3. Obituary leads                          (data/leads.json: Legacy obituaries
                                              already matched to SDAT parcels)

Owners found in any source become data/judgments/deceased.json plus a
markdown brief, sorted by equity.  No network calls; everything is local.

  python -m engine2.lien_deceased                 # full run
  python -m engine2.lien_deceased --min-equity 100000
"""

import argparse
import glob
import json
import os
import re
from datetime import date

from engine2 import config
from engine2.death_index import DeathIndex
from engine2.owner_parse import split_individuals

LEADS = os.path.join("data", "judgments", "leads.json")
OBITS = os.path.join("data", "leads.json")
OUT_JSON = os.path.join("data", "judgments", "deceased.json")
OUT_MD = os.path.join("data", "judgments", "deceased.md")

COUNTY_CODE = {
    "Allegany": "01", "Anne Arundel": "02", "Baltimore City": "03", "Baltimore County": "04", "Baltimore": "04",
    "Calvert": "05", "Caroline": "06", "Carroll": "07", "Cecil": "08", "Charles": "09", "Dorchester": "10",
    "Frederick": "11", "Garrett": "12", "Harford": "13", "Howard": "14", "Kent": "15", "Montgomery": "16",
    "Prince George's": "17", "Queen Anne's": "18", "St. Mary's": "19", "Somerset": "20", "Talbot": "21",
    "Washington": "22", "Wicomico": "23", "Worcester": "24",
}


def load_rows():
    d = json.load(open(LEADS))
    return d if isinstance(d, list) else d.get("rows") or d.get("leads")


def transfer_year(row):
    m = re.match(r"(\d{4})", row.get("transfer_date") or "")
    return int(m[1]) if m else None


def load_probate():
    """account -> newest estate dict (only estates whose decedent is a person)."""
    out = {}
    for p in glob.glob(os.path.join(config.PROBATE_DIR, "*.json")):
        if os.path.basename(p).startswith("_"):
            continue
        try:
            d = json.load(open(p))
        except Exception:
            continue
        ests = d.get("estates") or []
        if ests:
            out[d.get("account") or os.path.basename(p)[:-5]] = ests[0]
    return out


def load_obits():
    """account -> obituary lead (data/leads.json has properties[] with SDAT account)."""
    out = {}
    try:
        d = json.load(open(OBITS))
    except Exception:
        return out
    rows = d if isinstance(d, list) else d.get("leads") or d.get("rows") or []
    for lead in rows:
        for prop in lead.get("properties") or []:
            acct = re.sub(r"\s+", "", str(prop.get("account_number") or prop.get("account") or prop.get("account_id") or ""))
            if acct:
                out[acct] = lead
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-equity", type=int, default=0)
    ap.add_argument("--min-identity", default="MEDIUM", choices=["LOW", "MEDIUM", "HIGH"])
    a = ap.parse_args()
    rank = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}

    rows = load_rows()
    probate = load_probate()
    obits = load_obits()
    deaths = DeathIndex()
    deaths.load()

    hits = []
    for r in rows:
        if (r.get("equity_est") or 0) < a.min_equity:
            continue
        acct = re.sub(r"\s+", "", r.get("account") or r.get("id") or "")
        found = []
        est = probate.get(acct)
        if est:
            found.append({"source": "Register of Wills", "decedent": est.get("decedent"), "dod": est.get("date_of_death"),
                          "estate": est.get("estate_number"), "status": est.get("status"),
                          "prs": est.get("personal_reps"), "attorney": est.get("attorney"), "confidence": "HIGH"})
        ob = obits.get(acct)
        if ob:
            found.append({"source": "Obituary", "decedent": ob.get("full_name"), "dod": ob.get("date_of_death"),
                          "url": ob.get("obituary_url"), "survived_by": ob.get("survived_by"), "confidence": "HIGH"})
        cc = COUNTY_CODE.get(r.get("county") or "", "")
        ty = transfer_year(r)
        for person in split_individuals(r.get("owner") or "", r.get("owner2") or ""):
            m = deaths.match(person, cc, transfer_year=ty)
            if m and rank[m["identity_confidence"]] >= rank[a.min_identity]:
                found.append({"source": "MSA death index", "decedent": m["owner"], "dod": m["death_date"],
                              "age": m["age_at_death"], "confidence": m["identity_confidence"],
                              "points": m["identity_points"], "same_name_deaths": m["same_name_deaths_statewide"]})
        if not found:
            continue
        # Consistency check: a judgment entered AGAINST THIS NAME after the death
        # date means a living person with the same name was sued (namesake heir,
        # or a wrong death-index match). Demote those — the owner is probably alive.
        dod_years = []
        for fnd in found:
            m = re.match(r"(\d{4})", fnd.get("dod") or "")
            m2 = re.search(r"/(\d{4})$", fnd.get("dod") or "")
            if m:
                dod_years.append(int(m[1]))
            elif m2:
                dod_years.append(int(m2[1]))
        conflicts = []
        for j in r.get("judgments") or []:
            ey = re.search(r"(\d{4})", j.get("entry") or "")
            if ey and dod_years and int(ey[1]) > min(dod_years) and j.get("creditor_type") != "GOV":
                conflicts.append(f"{j.get('creditor') or 'judgment'} entered {j.get('entry')} after death")
        if conflicts:
            for fnd in found:
                fnd["confidence"] = "LOW"
            if rank["LOW"] < rank[a.min_identity]:
                continue
        buyable = [j for j in r.get("judgments") or [] if j.get("buyable")]
        hits.append({
            "account": acct, "address": r.get("address"), "city": r.get("city"), "county": r.get("county"),
            "owner": r.get("owner"), "owner2": r.get("owner2"), "mail_state": r.get("mail_state"),
            "assessed": r.get("assessed"), "equity_est": r.get("equity_est"), "total_owed": r.get("total_owed"),
            "transfer_date": r.get("transfer_date"), "title_flags": r.get("title_flags"),
            "usps_vacant": r.get("usps_vacant"),
            "deaths": found, "conflicts": conflicts,
            "best_confidence": max((f["confidence"] for f in found), key=lambda c: rank[c]),
            "buyable_judgments": [{k: j.get(k) for k in ("case", "creditor", "amount", "entry", "expires")} for j in buyable],
            "all_judgments": len(r.get("judgments") or []),
            "score": r.get("score"),
        })

    hits.sort(key=lambda h: (-rank[h["best_confidence"]], -(h["equity_est"] or 0)))
    json.dump({"generated": date.today().isoformat(), "parcels_checked": len(rows), "hits": hits},
              open(OUT_JSON, "w"), indent=1)

    with open(OUT_MD, "w") as f:
        f.write(f"# Judgment parcels with a dead owner — {date.today()}\n\n")
        f.write(f"{len(rows):,} open-judgment parcels checked against the Register of Wills files, obituary leads and the "
                f"1973–2014 state death index. {len(hits)} owners dead.\n\n")
        for conf in ("HIGH", "MEDIUM", "LOW"):
            sub = [h for h in hits if h["best_confidence"] == conf]
            if not sub:
                continue
            f.write(f"## {conf} identity match ({len(sub)})\n\n")
            f.write("| Property | SDAT acct | Owner | Equity | Death | Buyable judgment(s) |\n|---|---|---|---|---|---|\n")
            for h in sub:
                d = h["deaths"][0]
                death = f"{d['source']}: {d.get('decedent')} d. {d.get('dod')}"
                if d["source"] == "Register of Wills":
                    pr = (d.get("prs") or [{}])[0]
                    death += f" — estate {d.get('estate')} {d.get('status')}; PR {pr.get('name', '')} {pr.get('address', '')}"
                bj = "; ".join(f"{j['creditor']} ${j['amount']:,.0f} ({j['case']})" for j in h["buyable_judgments"]) or "— (gov/non-buyable only)"
                f.write(f"| {h['address']}, {h['city']} ({h['county']}) | {h['account']} | {h['owner']} | ${(h['equity_est'] or 0):,.0f} | {death} | {bj} |\n")
            f.write("\n")
    print(f"{len(rows)} parcels, {len(hits)} with a dead owner "
          f"({sum(1 for h in hits if h['best_confidence']=='HIGH')} HIGH, "
          f"{sum(1 for h in hits if h['best_confidence']=='MEDIUM')} MEDIUM); "
          f"{sum(1 for h in hits if h['buyable_judgments'])} have a buyable judgment")


if __name__ == "__main__":
    main()
