"""
Automated Register of Wills estate lookups for owners the death index says died.

Allowed ONLY under the written permission the Register of Wills granted on
2026-10-15 (docs/register-of-wills-permission-2026-10-15.md). The limits in
engine2/config.py (ROW_*) are the ones promised in that request:

  * one search per decedent, only for owners already flagged from public
    property records (SDAT + death index) as possibly deceased
  * open estates re-checked no more than once every 30 days (direct record
    link, no new search)
  * a few dozen people a day, with a pause between every request, off-peak
  * a User-Agent naming us and a contact email
  * no bulk download, no redistribution

Search = last name + first name, statewide (one query catches estates opened
in another county). A hit is accepted only when the estate's date of death is
within 3 days of the death-index date AND a given name matches. When a name
has more estates than one results page shows, the person is logged UNCERTAIN
(left for a hand check) instead of being called "no estate".

Writes data/probate/<account>.json (same format as engine2.probate_import) and
data/probate/_lookups.json (who was searched, when, result). The title scan
picks the results up on its next run.

  python -m engine2.probate_lookup            # normal daily run
  ROW_MAX_PEOPLE=5 python -m engine2.probate_lookup
"""

import glob
import html
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime

import requests

from engine2 import config
from engine2.probate_import import classify, load_existing, parse

BASE = "https://registers.maryland.gov/RowNetWeb/Estates/"
SEARCH = BASE + "frmEstateSearch2.aspx"
DETAIL = BASE + "frmDocketImages.aspx?src=row&RecordId={}"
LOG_PATH = os.path.join(config.PROBATE_DIR, "_lookups.json")
TITLE_DIR = os.path.join("data", "title")

log = logging.getLogger("probate_lookup")


class Budget(Exception):
    pass


class Client:
    def __init__(self, max_requests, delay):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": config.ROW_USER_AGENT})
        self.left = max_requests
        self.delay = delay
        self.last = 0.0

    def _wait(self):
        if self.left <= 0:
            raise Budget()
        gap = time.time() - self.last
        if gap < self.delay:
            time.sleep(self.delay - gap)
        self.left -= 1
        self.last = time.time()

    def get(self, url):
        self._wait()
        r = self.s.get(url, timeout=45)
        r.raise_for_status()
        return r.text

    def post(self, url, data):
        self._wait()
        r = self.s.post(url, data=data, timeout=45)
        r.raise_for_status()
        return r.text


def hidden(page):
    return {n: html.unescape(v) for n, v in
            re.findall(r'<input type="hidden" name="([^"]+)" id="[^"]+" value="([^"]*)"', page)}


def page_text(page):
    t = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", "", page)
    t = re.sub(r"(?i)<br\s*/?>|</tr>|</p>|</div>", "\n", t)
    t = re.sub(r"(?i)</td>|</th>", "\t", t)
    t = html.unescape(re.sub(r"<[^>]+>", "", t)).replace("\xa0", " ")
    t = re.sub(r"[ ]*\t[ \t]*", "\t", t)
    t = re.sub(r"\n[ \t]*", "\n", t)
    return re.sub(r"\n+", "\n", t)


ROW_RX = re.compile(
    r'<tr[^>]*>\s*<td[^>]*>([^<]*)</td>\s*<td[^>]*><a href="frmDocketImages\.aspx\?src=row&amp;RecordId=(\d+)"[^>]*>([^<]+)</a></td>'
    r'\s*<td[^>]*>([^<]*)</td>\s*<td[^>]*>([^<]*)</td>\s*<td[^>]*>([^<]*)</td>\s*<td[^>]*>([^<]*)</td>\s*<td[^>]*>([^<]*)</td>',
    re.S)


def search(client, last, first):
    """One search: last + first name, statewide, decedent party. Returns (rows, total)."""
    form = client.get(SEARCH)
    data = hidden(form)
    data.update({"txtEstateNo": "", "txtLN": last, "txtFN": first, "txtMN": "", "cboCountyId": "",
                 "cboStatus": "", "cboType": "", "cboPartyType": "Decedent",
                 "DateOfFilingFrom": "", "DateOfFilingTo": "", "txtDOF": "", "cmdSearch": "Search"})
    res = client.post(SEARCH, data)
    rows = []
    for county, rid, est, filed, died, typ, status, name in ROW_RX.findall(res):
        rows.append({"county": html.unescape(county).strip(), "record_id": rid, "estate_number": est.strip(),
                     "filed": filed.strip(), "dod": died.strip(), "type": typ.strip(), "status": status.strip(),
                     "name": re.sub(r"\s+", " ", html.unescape(name)).strip()})
    m = re.search(r"\((\d+) RECORDS? TOTAL\)", res)
    return rows, int(m.group(1)) if m else len(rows)


def same_death(row_dod, dod_iso, slack_days=3):
    try:
        a = datetime.strptime(row_dod, "%m/%d/%Y").date()
    except ValueError:
        return False
    return abs((a - date.fromisoformat(dod_iso)).days) <= slack_days


_PAGES = {}


def fetch_estate(client, record_id):
    if record_id not in _PAGES:                 # one fetch per record per run, however many parcels share it
        _PAGES[record_id] = client.get(DETAIL.format(record_id))
    rec = parse(page_text(_PAGES[record_id]))
    rec["record_id"] = record_id
    rec["record_url"] = DETAIL.format(record_id)
    rec["source"] = f"Register of Wills Estate Search (automated, permission {config.ROW_PERMISSION_DATE})"
    code, label = classify(rec)
    rec["title_class"], rec["title_class_label"] = code, label
    return rec


def name_ok(result_name, owner):
    """'BLACKWELL , AGNES GENEVA' vs owner first/middle. Any given name in common counts."""
    given = set(re.sub(r"[^A-Z ]", " ", result_name.split(",", 1)[-1].upper()).split())
    mine = {w for w in (owner.get("first_name", ""), owner.get("middle_name", ""), owner.get("record_middle", ""))
            for w in w.upper().split() if len(w) > 1}
    return bool(given & mine)


def save_estate(acct, rec):
    path = os.path.join(config.PROBATE_DIR, f"{acct}.json")
    estates = [e for e in load_existing(path)
               if e.get("estate_number") != rec.get("estate_number")
               and not (e.get("title_class") == "D1" and e.get("decedent") == rec.get("decedent_searched"))]
    estates.append(rec)
    os.makedirs(config.PROBATE_DIR, exist_ok=True)
    json.dump({"account": acct, "estates": estates}, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def save_none(acct, owner_name, dod):
    path = os.path.join(config.PROBATE_DIR, f"{acct}.json")
    estates = load_existing(path)
    if any(e.get("decedent_searched") == owner_name or (e.get("decedent") or "").upper() == owner_name for e in estates):
        return
    estates.append({"estate_number": "", "status": "NONE FOUND", "decedent": owner_name, "decedent_searched": owner_name,
                    "date_of_death": dod, "title_class": "D1",
                    "title_class_label": "No Maryland estate found (statewide search by last name + date of death)",
                    "imported_at": date.today().isoformat(),
                    "source": f"Register of Wills Estate Search (automated, permission {config.ROW_PERMISSION_DATE})"})
    os.makedirs(config.PROBATE_DIR, exist_ok=True)
    json.dump({"account": acct, "estates": estates}, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def alive_set():
    try:
        raw = json.load(open(config.ALIVE_OVERRIDES_PATH, encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {k: set(v.get("names") or ["*"]) for k, v in raw.items() if not k.startswith("_")}


def candidates():
    """person key ("NAME|YYYY-MM-DD") -> {owner, dod, accounts, rank}. One person can own several
    parcels; they are searched once. Ray's counties first, then HIGH identity, then lead priority."""
    alive = alive_set()
    order = {c: i for i, c in enumerate(config.ROW_PRIORITY_COUNTIES)}
    people = {}
    for path in glob.glob(os.path.join(TITLE_DIR, "*.json")):
        slug = os.path.splitext(os.path.basename(path))[0]
        if slug in ("top", "people") or slug.startswith("_"):
            continue
        try:
            rows = json.load(open(path, encoding="utf-8")).get("rows", [])
        except (OSError, ValueError):
            continue
        for r in rows:
            p = r.get("property") or {}
            acct = r.get("id") or p.get("account_number")
            owners = {o.get("normalized_name"): o for o in p.get("owners") or []}
            for m in p.get("death_matches") or []:
                name, dod = m.get("owner"), m.get("death_date")
                if not name or not dod or m.get("identity_confidence") == "LOW":
                    continue
                if acct in alive and ("*" in alive[acct] or name in alive[acct]):
                    continue
                o = dict(owners.get(name) or {})
                if not o.get("last_name"):
                    continue
                o["record_middle"] = m.get("record_middle") or ""
                rank = (order.get(slug, 99), 0 if m.get("identity_confidence") == "HIGH" else 1,
                        -((r.get("scores") or {}).get("priority") or 0))
                key = f"{name}|{dod}"
                cur = people.setdefault(key, {"name": name, "owner": o, "dod": dod, "accounts": [], "rank": rank})
                if acct not in cur["accounts"]:
                    cur["accounts"].append(acct)
                cur["rank"] = min(cur["rank"], rank)
    return people


def apply_result(entry, accounts, client, name):
    """Write the person's search result onto each of their parcels (detail pages only, no new search)."""
    for acct in accounts:
        if entry["status"] == "NONE FOUND":
            save_none(acct, name, entry["dod"])
        elif entry.get("record_ids"):
            for rid in entry["record_ids"]:
                rec = fetch_estate(client, rid)
                rec["decedent_searched"] = name
                save_estate(acct, rec)


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    max_people = int(os.environ.get("ROW_MAX_PEOPLE", config.ROW_MAX_PEOPLE_PER_RUN))
    max_people = min(max_people, config.ROW_MAX_PEOPLE_PER_RUN)       # never above the promised limit
    client = Client(config.ROW_MAX_REQUESTS_PER_RUN, max(config.ROW_DELAY_SECONDS, 3.0))
    try:
        done = json.load(open(LOG_PATH, encoding="utf-8")).get("searched", {})
    except (OSError, ValueError):
        done = {}
    today = date.today()
    found = none = rechecked = attached = 0
    try:
        # 1. open estates: re-read the record page once every 30 days (no new search)
        for key, e in done.items():
            if e.get("status") == "OPEN" and e.get("record_ids") and \
                    (today - date.fromisoformat(e["checked"])).days >= config.ROW_RECHECK_OPEN_DAYS:
                for acct in e["accounts"]:
                    for rid in e["record_ids"]:
                        rec = fetch_estate(client, rid)
                        rec["decedent_searched"] = key.split("|")[0]
                        save_estate(acct, rec)
                        e["status"] = rec.get("status") or e["status"]
                e["checked"] = today.isoformat()
                rechecked += 1
        people = candidates()
        # 2. people already searched who now show up on another parcel: copy the result, no new search
        for key, c in people.items():
            e = done.get(key)
            if e:
                new = [a for a in c["accounts"] if a not in e["accounts"]]
                if new and e["status"] in ("NONE FOUND", "OPEN", "CLOSED", "PENDIN", "ARCHIV"):
                    apply_result(e, new, client, c["name"])
                    e["accounts"] += new
                    attached += len(new)
        # 3. new people: one search each
        todo = sorted((c["rank"], k) for k, c in people.items() if k not in done)
        log.info("%d deceased owners not yet searched; doing up to %d", len(todo), max_people)
        for _, key in todo[:max_people]:
            c = people[key]
            o, dod = c["owner"], c["dod"]
            rows, total = search(client, o["last_name"], o.get("first_name", ""))
            hits = [x for x in rows if same_death(x["dod"], dod) and name_ok(x["name"], o)][:3]
            e = {"checked": today.isoformat(), "dod": dod, "accounts": list(c["accounts"]), "results": total}
            if hits:
                e.update({"status": hits[0]["status"], "record_ids": [h["record_id"] for h in hits],
                          "estate_number": hits[0]["estate_number"], "county": hits[0]["county"]})
                found += 1
            elif total > len(rows):
                e["status"] = "UNCERTAIN"          # more same-name estates than one page shows; check by hand
            else:
                e["status"] = "NONE FOUND"
                none += 1
            done[key] = e
            apply_result(e, c["accounts"], client, c["name"])
            log.info("%s (d. %s) on %s: %s", c["name"], dod, ",".join(c["accounts"]), e["status"] +
                     (f" #{e['estate_number']} {e['county']}" if hits else ""))
    except Budget:
        log.info("request budget for this run used up; the rest go next run")
    except requests.RequestException as ex:
        log.warning("Register of Wills request failed (%s); stopping this run", ex)
    os.makedirs(config.PROBATE_DIR, exist_ok=True)
    json.dump({"permission": config.ROW_PERMISSION_DATE, "updated": today.isoformat(), "searched": done},
              open(LOG_PATH, "w", encoding="utf-8"), indent=1, sort_keys=True)
    log.info("run done: %d estates found, %d none found, %d uncertain total, %d open re-checked, "
             "%d parcels attached from earlier searches, %d requests left", found, none,
             sum(1 for e in done.values() if e.get("status") == "UNCERTAIN"), rechecked, attached, client.left)
    return 0


if __name__ == "__main__":
    sys.exit(main())
