"""
Re-cut the dead-owner lien list using three checks the name match can't do:

  1. Resident check  (data/judgments/verify_residents.json, from cyberbackgroundchecks
                      address pages: who lives there now, who is marked Deceased, and
                      whether the "dead" owner shows up alive at another address)
  2. Debtor check    (data/judgments/verify_cases.json, Case Search detail: the judgment
                      defendant's address vs the property / tax-bill address)
  3. Spouse rule     (co-owner with the same last name + judgment against one name =
                      tenancy by the entirety; the lien does not reach the house)

Input queue: data/judgments/verify_queue.json.  Output: data/judgments/verified.json + .md
  python -m engine2.verify_dead
"""
import json
import os
import re
from datetime import date

Q = os.path.join("data", "judgments", "verify_queue.json")
RES = os.path.join("data", "judgments", "verify_residents.tsv")
CS = os.path.join("data", "judgments", "verify_cases.tsv")
OUT = os.path.join("data", "judgments", "verified.json")
OUT_MD = os.path.join("data", "judgments", "verified.md")

SUFFIX = {"JR", "SR", "II", "III", "IV", "V"}


def toks(name):
    return [t for t in re.findall(r"[A-Z]+", (name or "").upper()) if t not in SUFFIX]


def owner_fl(owner):
    """SDAT 'LAST FIRST M' -> (first, last)."""
    t = toks(owner)
    return (t[1], t[0]) if len(t) >= 2 else ("", t[0] if t else "")


def person_fl(name):
    """'Warren Alexander Statesman JR' -> (first, last)."""
    t = toks(name)
    return (t[0], t[-1]) if len(t) >= 2 else ("", t[0] if t else "")


def same_person(first, last, pfirst, plast):
    return bool(first and last and pfirst == first and plast == last)


def norm_addr(a):
    a = (a or "").upper()
    a = re.sub(r"\b(APT|UNIT|#|STE)\b.*$", "", a)
    a = re.sub(r"[^A-Z0-9 ]", " ", a)
    a = re.sub(r"\b(ROAD|RD|STREET|ST|AVENUE|AVE|DRIVE|DR|LANE|LN|COURT|CT|CIRCLE|CIR|PLACE|PL|TERRACE|TER|WAY|BOULEVARD|BLVD|HIGHWAY|HWY|PIKE|TRAIL|TRL|RUN|PARKWAY|PKWY|NORTH|N|SOUTH|S|EAST|E|WEST|W|NE|NW|SE|SW)\b", " ", a)
    return " ".join(a.split())


def addr_key(a):
    n = norm_addr(a)
    m = re.match(r"(\d+)\s+(\w+)", n)
    return (m[1], m[2]) if m else None


def load_res():
    """acct \t name \t age \t lives-at  ->  {acct: {"p": [...]}}; a bare 'NONE' line = page had no people."""
    out = {}
    if not os.path.exists(RES):
        return out
    for line in open(RES):
        parts = line.rstrip("\n").split("\t")
        if not parts[0].strip():
            continue
        acct = re.sub(r"\s+", "", parts[0])
        rec = out.setdefault(acct, {"p": []})
        if len(parts) >= 4:
            rec["p"].append({"n": parts[1], "a": parts[2], "la": parts[3]})
        elif len(parts) >= 2 and parts[1] == "BLOCKED":
            rec["err"] = "blocked"
    return out


def load_cs():
    """case \t defendant \t address -> {case: {"defs": [{"n","a"}]}}"""
    out = {}
    if not os.path.exists(CS):
        return out
    for line in open(CS):
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 2 or not parts[0].strip():
            continue
        out.setdefault(parts[0].strip(), {"defs": []})["defs"].append({"n": parts[1], "a": parts[2] if len(parts) > 2 else ""})
    return out


EST = os.path.join("data", "judgments", "row_estates.tsv")


def load_estates():
    """acct|estate# \t type \t status \t dod \t will \t PRs \t attorney \t docket -> {acct: [estate dicts]}
    (Register of Wills dockets pulled in the browser; only EXACT name+date matches were pulled)."""
    out = {}
    if not os.path.exists(EST):
        return out
    for line in open(EST):
        p = line.rstrip("\n").split("\t")
        if len(p) < 4 or "|" not in p[0] or p[1] == "PENDING":
            continue
        acct, est = p[0].split("|", 1)
        prs = []
        for m in re.finditer(r"([A-Z][A-Z .,'-]+?)\s*\[([^\]]+)\]", p[5] if len(p) > 5 else ""):
            prs.append({"name": m[1].strip(" ;"), "address": m[2].strip()})
        out.setdefault(acct, []).append({"estate": est, "type": p[1], "status": p[2], "dod": p[3],
                                          "will": p[4] if len(p) > 4 else "", "prs": prs,
                                          "attorney": p[6] if len(p) > 6 else "", "docket": p[7] if len(p) > 7 else ""})
    return out


def main():
    q = json.load(open(Q))
    res = load_res()
    cs = load_cs()
    estates = load_estates()
    out = []
    for p in q:
        flags, notes = [], []
        conf = "MEDIUM"
        dead_names = [(d["name"], d.get("rel", "")) for d in p.get("dead") or []]
        if not dead_names:  # title-scan only: the owner names themselves are the claim
            dead_names = [(" ".join(reversed(owner_fl(p["owner"]))), "owner")]
        prop_key = addr_key(p.get("address") or "")
        mail_key = addr_key(p.get("mail") or "")
        owner_last = owner_fl(p["owner"])[1]
        o2_last = owner_fl(p["owner2"])[1] if p.get("owner2") else ""

        # 1. residents
        r = res.get(p["acct"])
        confirmed, alive_hit, occupants, family_occ = [], [], [], []
        if r and not r.get("err"):
            for per in r.get("p", []):
                pf, pl = person_fl(per["n"])
                lives_here = (per.get("la") == "HERE") or (prop_key and addr_key(per.get("la") or "") == prop_key)
                age = per.get("a") or ""
                for dn, rel in dead_names:
                    df, dl = person_fl(dn)
                    if same_person(df, dl, pf, pl):
                        if age == "Deceased":
                            confirmed.append(dn)
                        elif age.isdigit():
                            alive_hit.append(f"{per['n']} age {age}" + (f" at {per['la'][:40]}" if per.get("la") and per["la"] != "HERE" else ""))
                if lives_here:
                    occupants.append(f"{per['n']} ({age})")
                    if pl and (pl == owner_last or pl == o2_last):
                        family_occ.append(per["n"])
            if alive_hit:
                # the people-search site lags deaths by a year or two; an "alive" listing AT the
                # house with no other address is weak, one at a different address is strong
                elsewhere = [a for a in alive_hit if " at " in a]
                if elsewhere:
                    # same first+last name alive somewhere else: either the owner is alive or it is a
                    # namesake (son, cousin). Without a birth year we cannot tell, so LOW, not KILL.
                    flags.append("ALIVE_ELSEWHERE"); conf = "LOW"
                    notes.append("Same name listed alive at another address (owner alive, or a namesake — confirm at the Register of Wills): " + "; ".join(elsewhere[:2]))
                else:
                    flags.append("ALIVE_LISTED_HERE"); conf = "LOW"
                    notes.append("People-search still lists the owner alive at the house (site lags deaths): " + "; ".join(alive_hit[:2]))
            if confirmed:
                flags.append("DEATH_CONFIRMED")
                notes.append("Resident data marks deceased: " + "; ".join(sorted(set(confirmed))[:2]))
            if family_occ:
                flags.append("OCCUPIED_FAMILY")
                notes.append("Family at the house: " + "; ".join(family_occ[:3]))
            elif occupants:
                flags.append("OCCUPIED")
                notes.append("Occupant(s): " + "; ".join(occupants[:3]))
            else:
                flags.append("NO_RESIDENT_LISTED")
        else:
            flags.append("RESIDENTS_UNCHECKED")

        # 2. debtor address (and the clerk's own "DEC'D" tag on a defendant)
        match, mismatch = 0, 0
        for j in p.get("judg") or []:
            c = cs.get(j.get("case") or "")
            if c and any("DEC'D" in (d.get("n") or "").upper() for d in c.get("defs", [])):
                if "DEATH_CONFIRMED" not in flags:
                    flags.append("DEATH_CONFIRMED")
                notes.append("Circuit Court indexed the defendant as DEC'D")
        for j in p.get("judg") or []:
            c = cs.get(j.get("case") or "")
            if not c or c.get("err"):
                continue
            keys = set()
            for d in c.get("defs", []):
                for a in (d.get("a") or "").split(" | "):
                    k = addr_key(a)
                    if k:
                        keys.add(k)
            if not keys:
                continue
            if (prop_key and prop_key in keys) or (mail_key and mail_key in keys):
                match += 1
            else:
                mismatch += 1
                notes.append(f"{j.get('cred')} debtor address {sorted(keys)[0][0]} {sorted(keys)[0][1].title()} ≠ property/mail")
        if mismatch and not match:
            flags.append("DEBTOR_ADDR_MISMATCH")
            if conf != "KILL":
                conf = "LOW"
        elif match:
            flags.append("DEBTOR_ADDR_MATCH")

        # 3. spouse rule
        if o2_last and o2_last == owner_last:
            names_sued = set()
            for j in p.get("judg") or []:
                c = cs.get(j.get("case") or "")
                for d in (c or {}).get("defs", []):
                    names_sued.add(person_fl(d.get("n", "").replace(",", " "))[0] if "," not in d.get("n", "") else toks(d["n"].split(",")[1])[0] if toks(d["n"].split(",")[1]) else "")
            o1f, o2f = owner_fl(p["owner"])[0], owner_fl(p["owner2"])[0]
            if names_sued and not ({o1f, o2f} <= names_sued):
                flags.append("TBE_ONE_SPOUSE")
                notes.append("Co-owners share a last name and only one was sued — tenancy by the entirety likely; lien may not reach the house")

        # 4. Register of Wills (exact name + date-of-death match pulled in the browser)
        ests = estates.get(p["acct"]) or []
        pr_contacts = []
        if ests:
            e = ests[0]
            if "DEATH_CONFIRMED" not in flags:
                flags.append("DEATH_CONFIRMED")
            flags.append("ESTATE_OPEN" if e["status"] == "OPEN" else "ESTATE_CLOSED")
            if re.search(r"SHOW CAUSE|REMOV|DELINQUEN", e.get("docket") or "", re.I):
                flags.append("ESTATE_STALLED")
            for pr in e["prs"]:
                pr_contacts.append(f"{pr['name']} (PR) — {pr['address']}")
                if prop_key and addr_key(pr["address"]) == prop_key:
                    if "OCCUPIED_FAMILY" not in flags:
                        flags.append("OCCUPIED_FAMILY")
                    notes.append(f"PR {pr['name']} gives the property as their address — family in the house")
            notes.append(f"Register of Wills: estate {e['estate']} ({e['type']}, {e['status']}, will {e['will'].lower()}), d. {e['dod']}"
                         + (f"; attorney {e['attorney']}" if e.get("attorney") else "")
                         + ("; docket shows show-cause/removal — stalled estate" if "ESTATE_STALLED" in flags else ""))
        elif p.get("dead") and any(d.get("src") in ("Legacy", "Obituary", "Find a Grave") for d in p["dead"]):
            # death documented by an obituary/grave but no estate anywhere in Maryland = probate gap
            flags.append("NO_ESTATE_PROBATE_GAP")
            notes.append("No estate opened anywhere in Maryland (statewide Register of Wills search) — probate gap; an heir or a creditor must open one before the house can be sold")

        # deed after death (already in grave scoring, re-check here when data allows)
        if conf not in ("KILL", "LOW") and "DEATH_CONFIRMED" in flags and "DEBTOR_ADDR_MISMATCH" not in flags:
            conf = "HIGH"
        if "OCCUPIED_FAMILY" in flags and conf == "HIGH":
            conf = "HIGH-OCCUPIED"
        out.append({**p, "flags": flags, "confidence": conf, "verify_notes": notes, "pr_contacts": pr_contacts})

    order = {"HIGH": 0, "HIGH-OCCUPIED": 1, "MEDIUM": 2, "LOW": 3, "KILL": 4}
    out.sort(key=lambda x: (order[x["confidence"]], 0 if "DEBTOR_ADDR_MATCH" in x["flags"] else 1, -(x.get("equity") or 0)))
    json.dump({"generated": date.today().isoformat(), "rows": out}, open(OUT, "w"), indent=1)
    counts = {k: sum(1 for x in out if x["confidence"] == k) for k in order}
    with open(OUT_MD, "w") as f:
        f.write(f"# Dead-owner lien parcels — verified cut, {date.today()}\n\n{counts}\n\n"
                "Checks: (1) current-resident lookup on the property address — done for 55 of 284 before the people-search site's bot "
                "protection stopped it (the rest carry RESIDENTS_UNCHECKED and cannot rate HIGH); (2) Case Search defendant address vs "
                "property / tax-bill address on all 519 judgments; (3) co-owner-with-same-surname rule (tenancy by the entirety). "
                "HIGH = death confirmed by an independent record and nothing contradicts it. LOW = a contradiction that needs the "
                "Register of Wills before anyone is called.\n\n")
        for conf in order:
            rows = [x for x in out if x["confidence"] == conf]
            if not rows:
                continue
            f.write(f"## {conf} ({len(rows)})\n\n| Property / SDAT | Owner | Death claim | Equity | Liens | Verification |\n|---|---|---|---|---|---|\n")
            for x in rows:
                dead = "; ".join(f"{d['name'].title()} d. {d['dod']} ({d['src']})" for d in x.get("dead") or []) or "title scan"
                liens = "; ".join(f"{j['cred']} ${(j['amt'] or 0):,.0f}" for j in (x.get("judg") or [])[:3])
                f.write(f"| {x['address']}, {x['city']} ({x['county']}) {x['acct']} | {x['owner']}{(' / ' + x['owner2']) if x.get('owner2') else ''} | {dead} | ${(x.get('equity') or 0):,.0f} | {liens} | **{conf}** — {'; '.join(x['verify_notes']) or ', '.join(x['flags'])} |\n")
            f.write("\n")
    print(counts)


if __name__ == "__main__":
    main()
