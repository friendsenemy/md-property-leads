"""
Pre-probate heir audience — keeps the "Pre-Probate Heirs MD" Facebook custom
audience current from the probate sheet's PreProbate tab.

The PreProbate tab is filled every morning by the Apps Script heir tracer:
one row per (recent death, property), with up to three living heirs per row
(name, relation, phone, email) and a Match Evidence line per heir saying why
that phone belongs to that person (lives at the property / related to the
decedent / town only).

Every run rebuilds the list from scratch:
  1. Reads the PreProbate tab.
  2. Keeps rows whose date of death is within --months (default 6).
  3. Keeps heirs that have a phone or email AND whose match evidence is not a
     bare name-only guess (those are as likely to be a stranger as the heir).
  4. Replaces the Meta customer list with those heirs, de-duplicated by
     phone/email, SHA-256 hashed. Heirs whose decedent has aged out of the
     window drop off automatically.

Nothing is stored in the repo; the log prints counts only.

  python heir_audience.py             # full run
  python heir_audience.py --dry-run   # counts only

Env:
  META_ACCESS_TOKEN        system-user token with ads_management
  META_HEIR_AUDIENCE_ID    the "Pre-Probate Heirs MD" audience id
"""

import os
import re
import io
import csv
import sys
import json
import time
import hashlib
import argparse
from datetime import datetime, timedelta

import requests

SHEET_ID = "1FcbKsxZZ_ISGm3llCxdlxhAIsz4IFwafMmyQOQydzSw"
PREPROBATE_GID = "372958489"
GRAPH = "https://graph.facebook.com/v21.0"
MIN_PEOPLE = 50        # refuse to replace the audience with fewer than this (sheet broke?)


def parse_date(s):
    s = (s or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return datetime(int(m[1]), int(m[2]), int(m[3]))
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return datetime(int(m[3]), int(m[1]), int(m[2]))
    return None


def read_tab():
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={PREPROBATE_GID}"
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.text)))
    head = [h.strip() for h in rows[0]]
    col = {h: i for i, h in enumerate(head)}
    for n in ["Lead ID", "Date of Death", "Trace Status", "Heir 1 Name", "Match Evidence"]:
        if n not in col:
            sys.exit(f"PreProbate tab layout changed, missing column: {n}")
    out = []
    for r in rows[1:]:
        r = r + [""] * (len(head) - len(r))
        out.append({h: r[i] for h, i in col.items()})
    return out


def evidence_for(row):
    """heir full name (lower) -> confidence label from the Match Evidence cell."""
    ev = {}
    for line in (row.get("Match Evidence") or "").splitlines():
        m = re.match(r"^(.+?):\s*(STRONG|FAIR|ONLY MATCH|NAME ONLY)", line.strip())
        if m:
            ev[m[1].strip().lower()] = m[2]
    return ev


def heirs_from(row):
    ev = evidence_for(row)
    for n in (1, 2, 3):
        name = (row.get(f"Heir {n} Name") or "").strip()
        phone = re.sub(r"\D", "", row.get(f"Heir {n} Phone") or "")
        email = (row.get(f"Heir {n} Email") or "").strip().lower()
        if not name or not (phone or email):
            continue
        if len(phone) == 11 and phone[0] == "1":
            phone = phone[1:]
        if phone and len(phone) != 10:
            phone = ""
        conf = ev.get(name.lower(), "")
        if conf == "NAME ONLY":
            continue
        parts = name.split()
        yield {"fn": parts[0], "ln": parts[-1] if len(parts) > 1 else "",
               "phone": phone, "email": email, "conf": conf or "unscored"}


def h(v):
    return hashlib.sha256(v.encode("utf-8")).hexdigest() if v else ""


def meta_rows(p):
    fn = re.sub(r"[^a-z]", "", p["fn"].lower())
    ln = re.sub(r"[^a-z]", "", p["ln"].lower())
    base = [h(fn), h(ln), h("us")]
    ids = []
    if p["phone"]:
        ids.append(("", "1" + p["phone"]))
    if p["email"]:
        ids.append((p["email"], ""))
    return [[h(em), h(ph)] + base for em, ph in ids]


def push_to_meta(rows):
    token, aud = os.environ.get("META_ACCESS_TOKEN"), os.environ.get("META_HEIR_AUDIENCE_ID")
    if not token or not aud:
        print("META_ACCESS_TOKEN / META_HEIR_AUDIENCE_ID not set - skipping Meta upload")
        return
    schema = ["EMAIL", "PHONE", "FN", "LN", "COUNTRY"]
    session_id = int(time.time())
    size = 5000
    chunks = [rows[i:i + size] for i in range(0, len(rows), size)] or [[]]
    for n, chunk in enumerate(chunks, 1):
        r = requests.post(f"{GRAPH}/{aud}/usersreplace", data={
            "access_token": token,
            "session": json.dumps({"session_id": session_id, "batch_seq": n,
                                   "last_batch_flag": n == len(chunks),
                                   "estimated_num_total": len(rows)}),
            "payload": json.dumps({"schema": schema, "data": chunk}),
        }, timeout=120)
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        if r.status_code != 200 or "error" in body:
            sys.exit(f"Meta upload failed (batch {n}): {json.dumps(body.get('error', body))[:400]}")
        print(f"Meta batch {n}/{len(chunks)}: received {body.get('num_received')}, "
              f"invalid {body.get('num_invalid_entries')}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=6)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    cutoff = datetime.now() - timedelta(days=round(a.months * 30.44))
    rows = read_tab()
    recent = [r for r in rows if (parse_date(r["Date of Death"]) or datetime.min) >= cutoff]
    traced = [r for r in recent if (r.get("Trace Status") or "").startswith("Traced")]
    print(f"PreProbate tab: {len(rows)} rows, {len(recent)} deaths in the last {a.months} months, "
          f"{len(traced)} traced")

    people, seen, skipped_name_only = {}, set(), 0
    conf_counts = {}
    for r in traced:
        for p in heirs_from(r):
            key = p["phone"] or p["email"]
            if key in seen:
                continue
            seen.add(key)
            people[key] = p
            conf_counts[p["conf"]] = conf_counts.get(p["conf"], 0) + 1
    for r in traced:
        skipped_name_only += sum(1 for v in evidence_for(r).values() if v == "NAME ONLY")

    print(f"audience: {len(people)} heirs "
          f"({sum(1 for p in people.values() if p['phone'])} with phone, "
          f"{sum(1 for p in people.values() if p['email'])} with email); "
          f"confidence: " + ", ".join(f"{k} {v}" for k, v in sorted(conf_counts.items())) +
          f"; skipped {skipped_name_only} name-only guesses")

    meta = [row for p in people.values() for row in meta_rows(p)]
    if a.dry_run:
        print(f"dry run: {len(meta)} rows would be sent to Meta")
        return
    if len(people) < MIN_PEOPLE:
        sys.exit(f"Only {len(people)} heirs - refusing to shrink the audience that far. Check the PreProbate tab.")
    push_to_meta(meta)


if __name__ == "__main__":
    main()
