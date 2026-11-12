"""
Creditor abandonment (Courtney Fricke / Tim Bender): the lienholder who is not doing
its job is the third profile next to the abandoned property and the absent owner.

Detector 1 — STALE TAX CERTIFICATE
  A tax-sale certificate holder must file to foreclose the right of redemption
  within 2 years of the sale or the certificate is void (TP 14-833).  A parcel that
  was SOLD at tax sale two or more years ago, is STILL on a later sale list (so the
  taxes were never redeemed), and has no Right of Redemption suit on Case Search is
  a lien nobody is enforcing: the owner has been sitting on it for years and the
  cert holder only ever wanted interest.  Those owners are the tired, chronic
  delinquents, and the certificate is a lever that can be bought from a holder who
  has already lost the right to use it.

Inputs
  data/distress/taxsale-<year>.json          (by account: county, status, year, owner, face, bid, bidder)
  data/distress/ror_cases.json               (Case Search: Right of Redemption suits by tax buyers,
                                              {caseNumber: {t: title, d: filed, s: status, c: county, p: plaintiff}})
Outputs
  data/distress/stale_certs.json             (the candidates with the evidence)
  merges kind STALE_TAX_CERT into data/distress/signals.json accounts[]

  python -m engine2.creditor_watch
"""
import glob
import json
import os
import re
from datetime import date

TAX_GLOB = os.path.join("data", "distress", "taxsale-*.json")
ROR = os.path.join("data", "distress", "ror_cases.json")
OUT = os.path.join("data", "distress", "stale_certs.json")
SIGNALS = os.path.join("data", "distress", "signals.json")

COMPANY = re.compile(r"\b(LLC|L L C|INC|CORP|TRUST|TR\b|LP\b|LTD|COMPANY|CO\b|PARTNERS|ASSOC|CHURCH|BANK|HOLDINGS|PROPERTIES|INVESTMENTS|ESTATES OF|ESTATE OF|ET AL)", re.I)
SUFFIX = {"JR", "SR", "II", "III", "IV", "ET", "AL", "ETAL", "TR", "TRS"}


def load_year(path):
    d = json.load(open(path))
    r = d.get("records") if isinstance(d, dict) else d
    return r if isinstance(r, dict) else {x["account"]: x for x in r}


def owner_tokens(owner):
    """SDAT 'CRANDELL JOHN S' -> ('CRANDELL', 'JOHN')"""
    t = [x for x in re.findall(r"[A-Z]+", (owner or "").upper()) if x not in SUFFIX]
    return (t[0], t[1]) if len(t) >= 2 else ((t[0], "") if t else ("", ""))


def title_defendant(title):
    """'Columbia Liens LLC vs. John S Crandell, et al.' -> ('CRANDELL', 'JOHN')"""
    m = re.search(r"\bvs?\.?\s+(.+?)(?:,\s*et\s+al\.?)?\s*$", title or "", re.I)
    if not m:
        return ("", "")
    name = re.sub(r"[^A-Za-z ,'-]", " ", m[1]).upper()
    if "," in name:                       # 'CRANDELL, JOHN S'
        last, first = name.split(",", 1)
        t = [x for x in re.findall(r"[A-Z]+", first) if x not in SUFFIX]
        return (re.findall(r"[A-Z]+", last)[0] if re.findall(r"[A-Z]+", last) else "", t[0] if t else "")
    t = [x for x in re.findall(r"[A-Z]+", name) if x not in SUFFIX]
    return (t[-1], t[0]) if len(t) >= 2 else ((t[0], "") if t else ("", ""))


def main():
    years = {}
    for p in sorted(glob.glob(TAX_GLOB)):
        y = int(re.search(r"(\d{4})", os.path.basename(p))[1])
        years[y] = load_year(p)
    this_year = date.today().year
    ror = json.load(open(ROR)) if os.path.exists(ROR) else {}
    ror_by_county = {}
    for cn, c in ror.items():
        ror_by_county.setdefault((c.get("c") or "").upper().replace(" COUNTY", ""), []).append((cn, c))

    out = []
    for y, recs in sorted(years.items()):
        if this_year - y < 2:
            continue
        for acct, r in recs.items():
            if r.get("status") != "SOLD" or (r.get("bidder") or "").lower() == "county":
                continue
            later = [ly for ly in years if ly > y and acct in years[ly]]
            if not later:
                continue                                      # redeemed or foreclosed — the cert did its job
            latest = years[max(later)][acct]
            owner = r.get("owner") or latest.get("owner") or ""
            if not owner or COMPANY.search(owner):
                continue                                      # individuals only (Ray skips investor-owners)
            county = (r.get("county") or latest.get("county") or "").upper()
            last, first = owner_tokens(owner)
            suits = []
            for cn, c in ror_by_county.get(county.replace(" COUNTY", ""), []):
                dl, df = title_defendant(c.get("t"))
                if last and dl == last and (not first or not df or df == first):
                    suits.append({"case": cn, "title": c.get("t"), "filed": c.get("d"), "status": c.get("s"), "plaintiff": c.get("p")})
            out.append({
                "account": acct, "county": r.get("county") or latest.get("county"), "owner": owner,
                "address": r.get("address") or latest.get("address"),
                "sold_year": y, "cert_bidder": r.get("bidder"), "cert_face": r.get("face"), "cert_bid": r.get("bid"),
                "assessed": r.get("assessed") or latest.get("assessed"),
                "still_listed": sorted(later), "latest_face": latest.get("face"), "latest_status": latest.get("status"),
                "years_since_sale": this_year - y,
                "ror_suits": suits,
                "stale": not suits and this_year - y >= 2,
            })
    stale = [o for o in out if o["stale"]]
    stale.sort(key=lambda o: (-(o["years_since_sale"]), -(o.get("assessed") or 0)))
    json.dump({"generated": date.today().isoformat(), "checked": len(out), "stale": len(stale),
               "ror_cases_loaded": len(ror), "rows": stale,
               "with_suit": [o for o in out if o["ror_suits"]]}, open(OUT, "w"), indent=1)

    # Detector 2 — SLEEPING LENDER (zombie deed of trust).  Needs Land Records, which the
    # engine cannot drive, so the browser pass writes data/distress/zombie_dot.json:
    #   [{account, county, owner, dot: "book/page", dot_date, lender, assessed, last_activity}]
    # for parcels that are tax-delinquent, carry an unreleased deed of trust, and show no
    # substitute-trustee appointment or foreclosure in Land Records.  Merged here as ZOMBIE_DOT.
    zombies = []
    zp = os.path.join("data", "distress", "zombie_dot.json")
    if os.path.exists(zp):
        zombies = json.load(open(zp))
        zombies = zombies.get("rows", zombies) if isinstance(zombies, dict) else zombies

    # merge into the distress signals the Title Leads / Lien Leverage tabs already read
    if os.path.exists(SIGNALS):
        sig = json.load(open(SIGNALS))
        accts = sig.setdefault("by_account", {})
        for a in list(accts):
            accts[a] = [s for s in accts[a] if s.get("kind") not in ("STALE_TAX_CERT", "ZOMBIE_DOT")]
            if not accts[a]:
                del accts[a]
        for o in stale:
            accts.setdefault(o["account"], []).append({
                "kind": "STALE_TAX_CERT", "source": "Tax-sale results + Case Search (engine2.creditor_watch)",
                "date": f"{o['sold_year']}-06-01", "county": o["county"], "address": o.get("address"),
                "detail": (f"Sold at the {o['sold_year']} tax sale to {o['cert_bidder']} (${(o['cert_face'] or 0):,.0f} owed), "
                           f"still on the {', '.join(map(str, o['still_listed']))} list(s), and no Right of Redemption suit filed in "
                           f"{o['years_since_sale']} years — the certificate is void (TP 14-833) and nobody is enforcing the lien"),
            })
        for z in zombies:
            accts.setdefault(z["account"], []).append({
                "kind": "ZOMBIE_DOT", "source": "Land Records + tax-sale list (engine2.creditor_watch browser pass)",
                "date": z.get("dot_date"), "county": z.get("county"), "address": z.get("address"),
                "detail": (f"Deed of trust {z.get('dot')} ({z.get('dot_date')}, {z.get('lender')}) never released, taxes delinquent, "
                           f"and no substitute trustee or foreclosure of record{(' — last activity ' + z['last_activity']) if z.get('last_activity') else ''}: "
                           f"the lender is not protecting its own lien"),
            })
        sig.setdefault("kinds", {})["STALE_TAX_CERT"] = len(stale)
        sig["kinds"]["ZOMBIE_DOT"] = len(zombies)
        sig["accounts"] = len(accts)
        json.dump(sig, open(SIGNALS, "w"), indent=1)
    print(f"{len(out)} old certificates still delinquent; {len(stale)} with no redemption suit (stale); "
          f"{sum(1 for o in out if o['ror_suits'])} already in suit")


if __name__ == "__main__":
    main()
