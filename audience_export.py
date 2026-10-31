"""
Pre-probate inbound audience export — runs after pipeline.py every morning.

Turns the "survived by" line of each recent obituary lead into named heirs,
then writes three files to data/audience/:

  meta_custom_audience.csv  Upload to Meta Ads Manager -> Audiences ->
                            Custom Audience -> Customer list. Columns are
                            Meta's own identifiers (fn, ln, ct, st, zip,
                            country), so it maps with no editing. Free: no
                            skip trace needed, Meta matches on name + town.
  survivors_review.csv      One row per heir with the decedent, property
                            address, county and SDAT account # — so every
                            name can be looked up and checked.
  smartskip_bulk.csv        Last 7 days of new leads in SmartSkip's bulk
                            upload layout (owner name + property address),
                            only if you ever decide to pay for a skip trace.

Grandchildren and great-grandchildren are deliberately left out — they are
often minors and aren't decision makers. Pets and friends are dropped too.
"""

import os
import re
import csv
import json
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
LEADS_PATH = os.path.join(ROOT, "data", "leads.json")
OUT_DIR = os.path.join(ROOT, "data", "audience")

AUDIENCE_DAYS = int(os.environ.get("AUDIENCE_DAYS", "120"))   # pre-probate window
SMARTSKIP_DAYS = int(os.environ.get("SMARTSKIP_DAYS", "7"))

STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
    "dc": "DC", "d.c": "DC",
}
ABBRS = set(STATES.values())

# relationship word -> label; order matters (grand/great checked first)
RELATIONS = [
    (r"great[- ]?grand", "SKIP"), (r"grand(?:child|children|son|sons|daughter|daughters)", "SKIP"),
    (r"\bdogs?\b|\bcats?\b|fur bab", "SKIP"), (r"\bfriends?\b", "SKIP"),
    (r"\b(?:wife|husband|spouse|partner|fianc[eé]e?)\b", "spouse"),
    (r"\b(?:sons?|daughters?|children|child|step-?(?:son|daughter|children))\b", "child"),
    (r"\b(?:brothers?|sisters?|siblings?)\b", "sibling"),
    (r"\b(?:mother|father|parents)\b", "parent"),
    (r"\b(?:nieces?|nephews?)\b", "niece/nephew"),
    (r"\b(?:in-law|daughter-in-law|son-in-law|brother-in-law|sister-in-law)\b", "in-law"),
]
FILLER = re.compile(
    r"\b(?:his|her|their|the|a|an|loving|beloved|devoted|dear|cherished|special|former|"
    r"wife|husband|spouse|partner|fianc[eé]e?|sons?|daughters?|children|child|step-?\w+|"
    r"brothers?|sisters?|siblings?|mother|father|parents|nieces?|nephews?|in-law|"
    r"one|two|three|four|five|six|seven|eight|nine|ten|of\s+\d+\s+years|of over \d+ years|"
    r"years|including|survived|by|memory|are|is|family|members?|love|life|whom|married|"
    r"in|with|and|also|many|several|numerous|other)\b", re.I)
NOT_NAMES = {"Jr", "Sr", "II", "III", "IV", "Maryland", "Virginia", "Pennsylvania", "Oak", "Crest",
             "God", "Lord", "Christ", "Church", "MD", "VA", "PA", "DC", "US", "USA"}
SUFFIX = re.compile(r"\b(Jr|Sr|II|III|IV)\.?$")


def _clean(text):
    t = text or ""
    t = re.sub(r"[\"“”]([^\"“”]{1,20})[\"“”]", " ", t)          # nicknames "Debbi"
    t = re.sub(r"\bn[ée]e\s+[A-Z][a-z]+", " ", t)                 # née Ash
    t = re.sub(r"\b[o0]f (?:over |nearly |almost )?\d+ years", " ", t, flags=re.I)
    t = t.replace("&", " and ")
    return t


def _relation(chunk, current):
    low = chunk.lower()
    for pat, label in RELATIONS:
        if re.search(pat, low):
            return label
    return current


def _names_in(chunk, default_last, keep_parens=True):
    """'James (Connie) Powell' -> [James Powell, Connie Powell];
       'Kimberly Nyquist (Christopher)' -> [Kimberly Nyquist, Christopher Nyquist];
       'Beth' -> [Beth <decedent last>]."""
    paren = re.findall(r"\(([^)]*)\)", chunk)
    base = re.sub(r"\([^)]*\)", " ", chunk)
    base = FILLER.sub(" ", base)
    toks = [w.strip(".'") for w in re.findall(r"[A-Z][a-zA-Z'\-]+\.?", base)]
    toks = [w for w in toks if w and w not in NOT_NAMES and len(w) > 1]
    if not toks:
        return []
    first, last = toks[0], (toks[-1] if len(toks) > 1 else default_last)
    out = [(first, last)]
    for p in (paren if keep_parens else []):
        p = FILLER.sub(" ", p)
        ptoks = [w.strip(".'") for w in re.findall(r"[A-Z][a-zA-Z'\-]+", p)]
        ptoks = [w for w in ptoks if w not in NOT_NAMES]
        if not ptoks:
            continue
        if len(ptoks) == 1:
            out.append((ptoks[0], last))           # in-law spouse takes the family name
        else:
            out.append((ptoks[0], ptoks[-1]))
    return out


def parse_survivors(survived_by, decedent_last, default_city):
    text = _clean(survived_by)
    if not text.strip():
        return []
    chunks = re.split(r",|;|:|\band\b", text)
    people, group, pending, placed, relation = [], [], [], [], None
    for raw in chunks:
        ch = raw.strip(" .")
        if not ch:
            continue
        m = re.match(r"^(?:all\s+)?of\s+(.+)$", ch, re.I)
        if m:                                       # "of Ardmore" / "of Leonardtown"
            place = m.group(1).strip()
            st = STATES.get(place.lower())
            for p in (pending or group[-1:]):
                if st:
                    p["st"] = st
                else:
                    p["ct"] = place
            placed, pending = (pending or group[-1:]), []
            continue
        if ch.lower() in STATES or ch.upper() in ABBRS:   # trailing state after a city
            for p in placed:
                if p.get("ct") and not p.get("st"):
                    p["st"] = STATES.get(ch.lower(), ch.upper())
            continue
        place = None
        m = re.search(r"\s+of\s+(.+)$", ch)        # "Bob Hirt of Perry Hall"
        if m:
            place, ch = m.group(1).strip(), ch[:m.start()]
        new_rel = _relation(ch, relation)
        if new_rel != relation:
            group = []
        relation = new_rel
        if relation == "SKIP" or relation is None:
            continue
        rel, last_default, keep = relation, decedent_last, True
        if relation == "spouse" and people:         # "her husband, David" after a child
            rel, last_default = "in-law", people[-1]["ln"]
        if relation == "spouse" and not people:     # "Nancy (Harwood)" = maiden name
            keep = False
        for i, (fn, ln) in enumerate(_names_in(ch, last_default, keep)):
            if ln.lower() == fn.lower():
                continue
            p = {"fn": fn, "ln": ln, "relation": rel if i == 0 else "in-law"}
            people.append(p)
            group.append(p)
            pending.append(p)
        if place:
            st = STATES.get(place.lower())
            for p in pending:
                if st:
                    p["st"] = st
                else:
                    p["ct"] = place
            placed, pending = pending, []
    # location defaults: unknown -> decedent's town, Maryland
    for p in people:
        if not p.get("ct") and not p.get("st"):
            p["ct"], p["st"], p["loc_guess"] = default_city, "MD", True
        p.setdefault("st", "MD")
        p.setdefault("ct", "")
    # de-dupe within the obituary
    seen, uniq = set(), []
    for p in people:
        k = (p["fn"].lower(), p["ln"].lower())
        if k not in seen:
            seen.add(k)
            uniq.append(p)
    return uniq


def _when(lead):
    dod = lead.get("date_of_death") or ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", dod):
        return datetime.fromisoformat(dod).replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(lead.get("found_at", "").replace("Z", "+00:00"))
    except ValueError:
        return None


def main():
    store = json.load(open(LEADS_PATH, encoding="utf-8"))
    now = datetime.now(timezone.utc)
    cut_aud = now - timedelta(days=AUDIENCE_DAYS)
    cut_skip = now - timedelta(days=SMARTSKIP_DAYS)
    os.makedirs(OUT_DIR, exist_ok=True)

    meta_rows, review_rows, skip_rows, meta_seen = [], [], [], set()
    for lead in store.get("leads", []):
        when = _when(lead)
        props = lead.get("properties") or []
        prop = props[0] if props else {}
        city = (lead.get("city") or prop.get("city") or "").title()

        found = lead.get("found_at", "")
        try:
            found_dt = datetime.fromisoformat(found.replace("Z", "+00:00"))
        except ValueError:
            found_dt = None
        if found_dt and found_dt >= cut_skip and prop:
            skip_rows.append({
                "First Name": lead.get("first_name", ""), "Last Name": lead.get("last_name", ""),
                "Property Address": prop.get("property_address", ""),
                "Property City": (prop.get("city") or "").title(), "Property State": "MD",
                "Property Zip": prop.get("zip_code", ""),
                "Mailing Address": prop.get("property_address", ""),
                "Mailing City": (prop.get("city") or "").title(), "Mailing State": "MD",
                "Mailing Zip": prop.get("zip_code", ""),
                "SDAT Account": prop.get("account_number", ""), "County": prop.get("county", ""),
            })

        if not when or when < cut_aud:
            continue
        for p in parse_survivors(lead.get("survived_by", ""), lead.get("last_name", ""), city):
            zip_ = prop.get("zip_code", "") if p.get("loc_guess") else ""
            review_rows.append({
                "heir_name": f"{p['fn']} {p['ln']}", "relation": p["relation"],
                "heir_town": p["ct"], "heir_state": p["st"],
                "town_is_guess": "yes" if p.get("loc_guess") else "",
                "decedent": lead.get("full_name", ""), "date_of_death": lead.get("date_of_death", ""),
                "property_address": prop.get("property_address", ""),
                "property_city": (prop.get("city") or "").title(), "county": prop.get("county", ""),
                "sdat_account": prop.get("account_number", ""),
                "obituary_url": lead.get("obituary_url", ""),
            })
            key = (p["fn"].lower(), p["ln"].lower(), p["st"])
            if key in meta_seen:
                continue
            meta_seen.add(key)
            meta_rows.append({"fn": p["fn"], "ln": p["ln"], "ct": p["ct"], "st": p["st"],
                              "zip": zip_, "country": "US"})

    def write(name, rows, cols):
        with open(os.path.join(OUT_DIR, name), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)

    write("meta_custom_audience.csv", meta_rows, ["fn", "ln", "ct", "st", "zip", "country"])
    write("survivors_review.csv", review_rows, list(review_rows[0].keys()) if review_rows else
          ["heir_name"])
    write("smartskip_bulk.csv", skip_rows, list(skip_rows[0].keys()) if skip_rows else
          ["First Name"])
    print(f"audience: {len(meta_rows)} heirs for Meta, {len(review_rows)} review rows, "
          f"{len(skip_rows)} SmartSkip rows")


if __name__ == "__main__":
    main()
