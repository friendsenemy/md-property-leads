"""
Build data/findings.json — the "Worked file" every tab shows on a parcel.

Sources (all public record):
  data/findings_manual.json          hand-entered verdicts (GO / PARK / KILL) with title, death, contacts, numbers
  data/judgments/row_estates.tsv     Register of Wills dockets pulled for the dead-owner queue
  data/judgments/row_matches.tsv     the estate-index rows those dockets came from
  data/judgments/verify_queue.json   the dead-owner queue (address, owner, death claim)

Estate rows become findings on their own when no manual entry exists:
  OPEN estate, PR not at the property   -> "WORK" (PR is the signer)
  OPEN/CLOSED estate, PR at the property -> "PARK" (family in the house)
  CLOSED estate, PR elsewhere            -> "WORK" (needs a PR deed; PR is still the contact)
Dead-owner rows with a documented death and NO estate statewide -> "WORK" (probate gap).

  python -m engine2.findings_build
"""
import json
import os
import re
from datetime import date

MANUAL = os.path.join("data", "findings_manual.json")
EST = os.path.join("data", "judgments", "row_estates.tsv")
MATCH = os.path.join("data", "judgments", "row_matches.tsv")
QUEUE = os.path.join("data", "judgments", "verify_queue.json")
OUT = os.path.join("data", "findings.json")

STREET = r"\b(ROAD|RD|STREET|ST|AVENUE|AVE|DRIVE|DR|LANE|LN|COURT|CT|CIRCLE|CIR|PLACE|PL|TERRACE|TER|WAY|BOULEVARD|BLVD|HIGHWAY|HWY|PIKE|TRAIL|TRL|RUN|PARKWAY|PKWY|NORTH|N|SOUTH|S|EAST|E|WEST|W|NE|NW|SE|SW)\b"


def addr_key(a):
    a = re.sub(r"\b(APT|UNIT|#|STE)\b.*$", "", (a or "").upper())
    a = re.sub(r"[^A-Z0-9 ]", " ", a)
    a = " ".join(re.sub(STREET, " ", a).split())
    m = re.match(r"(\d+)\s+(\w+)", a)
    return (m[1], m[2]) if m else None


def load_estates():
    out = {}
    if not os.path.exists(EST):
        return out
    for line in open(EST):
        p = line.rstrip("\n").split("\t")
        if len(p) < 4 or "|" not in p[0] or p[1] == "PENDING":
            continue
        acct, est = p[0].split("|", 1)
        prs = [{"name": m[1].strip(" ;"), "address": m[2].strip()}
               for m in re.finditer(r"([A-Z][A-Z .,'-]+?)\s*\[([^\]]+)\]", p[5] if len(p) > 5 else "")]
        out.setdefault(acct, []).append({"estate": est, "type": p[1], "status": p[2], "dod": p[3],
                                          "will": p[4] if len(p) > 4 else "", "prs": prs,
                                          "attorney": p[6] if len(p) > 6 else "", "docket": p[7] if len(p) > 7 else ""})
    return out


def load_matched_accts():
    """accounts that had ANY exact Register of Wills match (so 'no estate' means searched and absent)."""
    out = set()
    if os.path.exists(MATCH):
        for line in open(MATCH):
            p = line.split("|")
            if len(p) > 11 and p[11].strip() == "EXACT":
                out.add(p[0].strip())
    return out


def main():
    manual = json.load(open(MANUAL)) if os.path.exists(MANUAL) else {}
    manual = {re.sub(r"[\s-]", "", k): v for k, v in manual.items() if not k.startswith("_")}
    estates = load_estates()
    matched = load_matched_accts()
    queue = {re.sub(r"[\s-]", "", r["acct"]): r for r in json.load(open(QUEUE))} if os.path.exists(QUEUE) else {}
    today = date.today().isoformat()
    out = {}

    for acct, q in queue.items():
        ests = estates.get(acct) or []
        prop_key = addr_key(q.get("address"))
        dead = "; ".join(f"{d['name'].title()} d. {d['dod']} ({d['src']})" for d in q.get("dead") or [])
        if ests:
            e = ests[0]
            at_home = [pr for pr in e["prs"] if prop_key and addr_key(pr["address"]) == prop_key]
            stalled = bool(re.search(r"SHOW CAUSE|REMOV|DELINQUEN", e.get("docket") or "", re.I))
            pro = any(re.search(r"\bESQ\b", pr["name"]) or re.search(r"N WASHINGTON ST|ROCKVILLE", pr["address"]) for pr in e["prs"])
            verdict = "PARK" if at_home else "WORK"
            reason = (f"Register of Wills estate {e['estate']} ({e['type']}, {e['status']}, will {e['will'].lower()}), d. {e['dod']}. "
                      + ("PR gives the property as their address — family in the house. " if at_home else "")
                      + ("Show-cause / removal orders on the docket — stalled estate, PR likely wants out. " if stalled else "")
                      + ("Professional PR (attorney) — appointed when no family stepped up; their job is to sell. " if pro else "")
                      + ("Estate still open: the PR can sign now." if e["status"] == "OPEN" and not at_home else
                         ("Estate closed with the house still in the decedent's name — needs a PR deed; the PR is still the contact." if not at_home else "")))
            out[acct] = {
                "verdict": verdict, "confidence": "HIGH" if e["status"] == "OPEN" or stalled else "MEDIUM", "checked": today,
                "play": "Dead owner — estate on file" + (" (stalled)" if stalled else ""),
                "reason": reason.strip(), "death": dead,
                "contacts": [f"{pr['name']} (PR) — {pr['address']}" for pr in e["prs"]] + ([f"Estate attorney — {e['attorney']}"] if e.get("attorney") else []),
                "next": "Land Records DOT check, then letter/call to the PR" if verdict == "WORK" else "Do not mail — occupied by family",
                "source": "Register of Wills docket",
            }
        elif q.get("dead") and acct not in matched:
            out[acct] = {
                "verdict": "WORK", "confidence": "MEDIUM", "checked": today, "play": "Dead owner — probate gap",
                "reason": "Death documented by obituary / Find a Grave; statewide Register of Wills search found NO estate — nobody has touched the title. An heir (or a creditor) must open an estate before the house can be sold.",
                "death": dead,
                "contacts": [], "next": "Pull the obituary for survivors; Land Records for deed/DOT; resident check.",
                "source": "Register of Wills statewide search (no match)",
            }

    for acct, f in manual.items():          # hand verdicts win
        out[acct] = {**out.get(acct, {}), **f}

    counts = {}
    for f in out.values():
        counts[f["verdict"]] = counts.get(f["verdict"], 0) + 1
    json.dump({"generated": today, "counts": counts, "by_account": out}, open(OUT, "w"), indent=1)
    print(counts, len(out))


if __name__ == "__main__":
    main()
