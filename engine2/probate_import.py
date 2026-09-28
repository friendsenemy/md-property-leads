"""
Import a Register of Wills estate record that a person looked up by hand.

  python -m engine2.probate_import --accounts 0903007987,0903006522 --file page.txt
  python -m engine2.probate_import --accounts 0903007987 < page.txt

Paste the whole Estate Record page (Ctrl+A, Ctrl+C on registers.maryland.gov).
Writes data/probate/<account>.json for each account; the weekly title scan
joins these and reclassifies the property (D2 open, D2-S stale open, D3 closed
but still titled to the decedent).

This is deliberately manual: the Register of Wills terms prohibit automated
commercial use without written permission (request pending, docs/).
"""

import argparse
import json
import os
import re
import sys
from datetime import date, datetime

OUT_DIR = os.path.join("data", "probate")

FIELD = {
    "estate_number": r"Estate Number:\s*([A-Z0-9\-]+)",
    "status": r"Status:\s*([A-Z ]+?)\s*(?:\n|Date|Type)",
    "type": r"Type:\s*([A-Z]{2})\b",
    "date_opened": r"Date Opened:\s*([\d/]+)",
    "date_closed": r"Date Closed:\s*([\d/]+)",
    "decedent": r"Decedent Name:\s*(.+?)\s*(?:\n|Date of Death)",
    "date_of_death": r"Date of Death:\s*([\d/]+)",
    "date_of_filing": r"Date of Filing:\s*([\d/]+)",
    "will": r"Will:\s*([A-Z ]+?)\s*(?:\n|Date)",
    "aliases": r"Aliases:\s*(.*?)\s*(?:\n|Personal Reps)",
    "attorney": r"Attorney:\s*(.*?)\s*(?:\n|NOTE|Docket)",
    "county": r"Estate Record \(([A-Za-z' .]+?)(?: County)?\)",
}
PR_RX = re.compile(r"Personal Reps?:\s*(.+?)(?=\n\s*Attorney:|\n\s*NOTE|\Z)", re.S)
PR_ITEM = re.compile(r"([A-Z][A-Z .,'\-]+?)\s*\[(.+?)\]")
DOCKET = re.compile(r"^\s*(\d{2}/\d{2}/\d{4})\s+(\d+)\s+(\d{4})\s+(.+?)\s+(\d+)\s*$", re.M)

RELATION = re.compile(r"\b([A-Z][A-Z'\-]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][A-Z'\-]+){1,2}),\s*(SON|DAUGHTER|SPOUSE|WIFE|HUSBAND|BROTHER|SISTER|MOTHER|FATHER|GRANDSON|GRANDDAUGHTER|NIECE|NEPHEW|FRIEND|HEIR)\b")
JOINT = re.compile(r"JNT WITH\s+([A-Z][A-Z .'\-]+?)(?:\s+#|\s*$)")
CORR = re.compile(r"CORRESPONDENCE RECEIVED FROM\s+([A-Z][A-Z .'\-]+?)(?:,\s*(ESQUIRE|ESQ\.?))?\s*$")
SUCC = re.compile(r"APPOINTING\s+([A-Z][A-Z .'\-]+?),\s*SUCCESSOR")
MAIL_TO = re.compile(r"CERTIFIED/REGISTERED MAIL/\s*([A-Z][A-Z .'\-]+?)(?:,\s*ESQUIRE)?\s*$")


def _d(s):
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except (ValueError, AttributeError):
            pass
    return None


def parse(text):
    text = text.replace("\r", "")
    rec = {}
    for k, rx in FIELD.items():
        m = re.search(rx, text, re.I | re.S)
        rec[k] = m.group(1).strip() if m else ""
    prs = []
    m = PR_RX.search(text)
    if m:
        for name, addr in PR_ITEM.findall(m.group(1)):
            prs.append({"name": name.strip(" ,"), "address": addr.strip()})
        if not prs:
            first = m.group(1).strip().splitlines()[0].strip() if m.group(1).strip() else ""
            # blank "Personal Reps:" runs straight into the next label — don't capture it
            if first and not re.match(r"^(Attorney|NOTE|Docket|Date|Aliases|Reference)\b", first, re.I):
                prs.append({"name": first, "address": ""})
    rec["personal_reps"] = prs
    docket = []
    for filed, num, code, desc, pages in DOCKET.findall(text):
        docket.append({"filed": filed, "n": int(num), "code": code, "desc": desc.strip(), "pages": int(pages)})
    rec["docket"] = docket
    rec["last_docket_activity"] = max((_d(d["filed"]) for d in docket if _d(d["filed"])), default=None)
    rec["last_docket_activity"] = rec["last_docket_activity"].isoformat() if rec["last_docket_activity"] else ""

    people = []
    seen = set()

    def add(name, role, src):
        name = re.sub(r"\s+", " ", name).strip(" .,")
        if len(name) < 5 or name in seen:
            return
        seen.add(name)
        people.append({"name": name, "role": role, "source": src})

    for p in prs:
        add(p["name"], "PERSONAL_REP", "estate header")
    for d in docket:
        for nm, rel in RELATION.findall(d["desc"]):
            add(nm, rel, f"docket {d['n']}")
        for nm in JOINT.findall(d["desc"]):
            add(nm, "JOINT_OWNER_NONPROBATE", f"docket {d['n']}")
        for nm, esq in CORR.findall(d["desc"]):
            add(nm, "ATTORNEY" if esq else "CORRESPONDENT", f"docket {d['n']}")
        for nm in SUCC.findall(d["desc"]):
            add(nm, "SUCCESSOR_PR", f"docket {d['n']}")
        for nm in MAIL_TO.findall(d["desc"]):
            add(nm, "NOTICED_PARTY", f"docket {d['n']}")
    rec["interested_persons"] = people

    # timing
    dod, opened, closed = _d(rec["date_of_death"]), _d(rec["date_opened"] or rec["date_of_filing"]), _d(rec["date_closed"])
    today = date.today()
    rec["death_to_probate_days"] = (opened - dod).days if dod and opened else None
    rec["probate_age_days"] = ((closed or today) - opened).days if opened else None
    rec["days_since_last_docket"] = (today - _d(rec["last_docket_activity"].replace("-", "/")[5:] + "/" + rec["last_docket_activity"][:4])).days if False else None
    if rec["last_docket_activity"]:
        rec["days_since_last_docket"] = (today - date.fromisoformat(rec["last_docket_activity"])).days
    rec["status"] = rec["status"].upper()
    # FP = foreign proceeding: the real probate is in another jurisdiction and only
    # an exemplified copy is filed here. The will and heir list live in that court.
    rec["foreign_proceeding"] = rec.get("type", "").upper() == "FP" or "FOREIGN" in rec.get("will", "").upper()
    rec["imported_at"] = today.isoformat()
    rec["source"] = "Register of Wills Estate Search (manual lookup)"
    return rec


def load_existing(path):
    """Existing estates for this account, so a parcel can carry more than one."""
    if not os.path.exists(path):
        return []
    try:
        data = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if isinstance(data, dict) and "estates" in data:
        return data["estates"]
    return [data] if isinstance(data, dict) and data.get("estate_number") else []


def classify(rec, still_titled_to_decedent=True):
    st = rec.get("status", "")
    if st == "OPEN":
        stale = (rec.get("probate_age_days") or 0) > 3 * 365 or (rec.get("days_since_last_docket") or 0) > 2 * 365
        return ("D2-S", "Estate open and stale") if stale else ("D2", "Estate currently open")
    if st == "CLOSED":
        if still_titled_to_decedent:
            return "D3", "Estate closed — property still titled to decedent"
        return "RESOLVED", "Estate closed and property transferred"
    return "UNKNOWN", f"Estate status '{st}'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--accounts", required=True, help="comma-separated SDAT account numbers this estate applies to")
    ap.add_argument("--file", help="text file with the pasted page (default: stdin)")
    a = ap.parse_args()
    text = open(a.file, encoding="utf-8").read() if a.file else sys.stdin.read()
    rec = parse(text)
    if not rec.get("estate_number") or not rec.get("decedent"):
        sys.exit("Could not find 'Estate Number' / 'Decedent Name' in the pasted text — paste the whole Estate Record page.")
    code, label = classify(rec)
    rec["title_class"], rec["title_class_label"] = code, label
    os.makedirs(OUT_DIR, exist_ok=True)
    for acct in [x.strip() for x in a.accounts.split(",") if x.strip()]:
        path = os.path.join(OUT_DIR, f"{acct}.json")
        estates = [e for e in load_existing(path) if e.get("estate_number") != rec["estate_number"]]
        estates.append(rec)
        estates.sort(key=lambda e: _d(e.get("date_of_death") or "") or date.min)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"account": acct, "estates": estates}, f, ensure_ascii=False, indent=1)
        print(f"wrote {path}: estate {rec['estate_number']} ({rec['decedent']}) {rec['status']} → {code} ({label}); "
              f"PR {', '.join(p['name'] for p in rec['personal_reps']) or '—'}; "
              f"{len(rec['interested_persons'])} people; {len(rec['docket'])} docket entries; "
              f"{len(estates)} estate(s) now on this parcel")


if __name__ == "__main__":
    main()
