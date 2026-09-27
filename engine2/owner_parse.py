"""
Owner-name classification and conservative splitting for SDAT owner strings.

SDAT stores individuals as "LAST FIRST MIDDLE" and joint owners as
"LAST FIRST M & FIRST M" or "LAST FIRST & LAST FIRST". Anything that looks
like a business, trust, estate, government or nonprofit is never split.
"""

import re

from engine2 import config

OWNER_TYPES = ("INDIVIDUAL", "MULTIPLE_INDIVIDUALS", "TRUST", "LLC", "CORPORATION",
               "ESTATE", "GOVERNMENT", "NONPROFIT", "OTHER", "UNKNOWN")

_SUFFIXES = {"SR", "JR", "II", "III", "IV", "V"}
_JOINERS = {"&", "AND", "ET", "AL", "ETAL", "WF", "HW", "H/W", "ETUX", "ETVIR"}
_NOISE = {"TR", "TRS", "TRUSTEE", "TRUSTEES", "LE", "L/E", "ETAL", "ET", "AL", "WF", "HW", "C/O"}

# Flag detectors — (flag, compiled regex on the padded upper-case string)
_FLAG_PATTERNS = [
    ("ESTATE_IN_NAME",   re.compile(r"\b(ESTATE OF|EST OF|ESTATE|EST)\b")),
    ("HEIRS_IN_NAME",    re.compile(r"\bHEIRS?\b")),
    ("DECEASED_IN_NAME", re.compile(r"\b(DECEASED|DEC'?D)\b")),
    ("PERSONAL_REP",     re.compile(r"\b(PERS(ONAL)? REP(RESENTATIVE)?|P/R|PR OF|EXECUT(OR|RIX)|ADMINISTRAT(OR|RIX))\b")),
    ("LIFE_ESTATE",      re.compile(r"\b(LIFE ESTATE|L/E|LIFE TENANT|LIFE EST|L EST)\b")),
    ("CARE_OF",          re.compile(r"\bC/O\b")),
    ("ET_AL",            re.compile(r"\b(ET ?AL|ETAL)\b")),
    ("SURVIVING",        re.compile(r"\b(SURV|SURVIVING|SURVIVOR)\b")),
    ("TRUSTEE",          re.compile(r"\b(TR|TRS|TRUSTEES?|TRUST)\b")),
    ("CONSERVATOR",      re.compile(r"\b(CONSERVATOR|GUARDIAN|GDN|POA|ATTORNEY IN FACT|AIF)\b")),
]


def clean(name):
    # SDAT glues markers on with hyphens ("NAOMI-LIFE EST", "ETAL-LIFE ESTATE"); treat as spaces.
    name = (name or "").upper().replace(",", " ").replace(".", " ").replace("-", " ").replace("(", " ").replace(")", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name


_REAL_ESTATE = re.compile(r"\bREAL\s+(ESTATE|EST)S?\b")
_LIFE_ESTATE = re.compile(r"\b(LIFE\s+(ESTATE|EST)|L\s+EST|L/E|LIFE TENANT)\b")


def _without_business_estate(text):
    """'WAL-MART REAL ESTATE' is a company, not a decedent's estate."""
    return _REAL_ESTATE.sub(" REALTY ", text)


def flags_for(owner1, owner2=""):
    """Return the set of title-relevant flags found in the owner text."""
    text = _without_business_estate(f" {clean(owner1)} {clean(owner2)} ")
    found = set()
    for flag, rx in _FLAG_PATTERNS:
        if rx.search(text):
            found.add(flag)
    # "LIFE ESTATE" contains "ESTATE": only keep ESTATE_IN_NAME when the text
    # names a decedent's estate independently ("ESTATE OF", "EST OF", trailing "ESTATE").
    if "LIFE_ESTATE" in found and "ESTATE_IN_NAME" in found:
        stripped = _LIFE_ESTATE.sub(" ", text)
        if not re.search(r"\b(ESTATE OF|EST OF|ESTATE|EST)\b", stripped):
            found.discard("ESTATE_IN_NAME")
    # "EST" alone is also a common abbreviation of a street name in legal text,
    # but in the owner field it is almost always "estate". Keep it.
    return found


def classify(owner1, owner2=""):
    """Bucket the owner record. Returns one of OWNER_TYPES."""
    text = f" {clean(owner1)} {clean(owner2)} "
    if not text.strip():
        return "UNKNOWN"
    if any(m in text for m in config.GOVERNMENT_MARKERS):
        return "GOVERNMENT"
    if _REAL_ESTATE.search(text):
        return "CORPORATION"
    estate_text = _LIFE_ESTATE.sub(" ", text)
    # Unambiguous decedent language wins over anything else
    if re.search(r"\b(ESTATE OF|EST OF|HEIRS?( OF)?|PERS(ONAL)? REP|P/R|PR OF|DECEASED|DEC'?D|EXECUT(OR|RIX)|ADMINISTRAT(OR|RIX))\b", estate_text):
        return "ESTATE"
    if " LLC" in text or " L L C" in text:
        return "LLC"
    if any(m in text for m in config.BUSINESS_MARKERS):
        return "CORPORATION"
    if any(m in text for m in config.NONPROFIT_MARKERS):
        return "NONPROFIT"
    if any(m in text for m in config.TRUST_MARKERS):
        return "TRUST"
    # Bare "ESTATE"/"EST" with a person-shaped name ("SMITH JOHN A EST")
    if any(m in estate_text for m in config.ESTATE_MARKERS):
        return "ESTATE"
    people = split_individuals(owner1, owner2)
    if len(people) >= 2:
        return "MULTIPLE_INDIVIDUALS"
    if len(people) == 1:
        return "INDIVIDUAL"
    return "OTHER"


def _tokens(s):
    return [t for t in re.findall(r"[A-Z][A-Z'\-]*|&", clean(s)) if t]


def _person(last, given):
    """Build a person dict from surname + given tokens (FIRST [MIDDLE...] [SUFFIX])."""
    given = [g for g in given if g not in _NOISE]
    suffix = ""
    if given and given[-1] in _SUFFIXES:
        suffix = given[-1]
        given = given[:-1]
    if not given:
        return None
    first = given[0]
    middle = " ".join(given[1:])
    if len(first) == 1:  # "SMITH J ROBERT" — initial first, real name second
        if len(given) > 1:
            first, middle = given[1], given[0]
        else:
            return None
    key = re.sub(r"[^A-Z]", "", f"{last}{first}{middle[:1]}")
    return {
        "last_name": last,
        "first_name": first,
        "middle_name": middle,
        "middle_initial": middle[:1],
        "suffix": suffix,
        "normalized_name": " ".join(x for x in (first, middle, last, suffix) if x),
        "person_key": key,
    }


def split_individuals(owner1, owner2=""):
    """
    Conservatively split an SDAT owner string into person dicts.
    Returns [] when the string does not look like people.
    """
    text = f" {clean(owner1)} "
    if _REAL_ESTATE.search(text):
        return []
    probe = _LIFE_ESTATE.sub(" ", text)
    if any(m in probe for m in config.BUSINESS_MARKERS + config.GOVERNMENT_MARKERS
           + config.NONPROFIT_MARKERS + config.TRUST_MARKERS + config.ESTATE_MARKERS):
        return []
    # Names after "C/O" are mail handlers, not owners; tenancy markers are noise.
    stripped = re.split(r"\bC/O\b", text)[0]
    stripped = re.sub(r"\b(LIFE ESTATE|LIFE EST|L EST|L/E|LIFE TENANT|ET ?AL|ETAL|SURV(IVING|IVOR)?|H/W|T/E|J/T|T/C|DECEASED|DEC'?D|EST|CONSERVATOR|GUARDIAN|GDN|POA|AIF)\b", " ", stripped)
    stripped = stripped.replace("/", " & ")   # "FRANK S/CONSTANCE FOARD" = two owners
    toks = _tokens(stripped)
    if not toks or len(toks) < 2 or len(toks) > 9:
        return []
    if any(ch.isdigit() for ch in owner1 or ""):
        return []

    # Split on joiners: "&", "AND", "ET UX"...
    groups, cur = [], []
    for t in toks:
        if t in _JOINERS:
            if cur:
                groups.append(cur)
            cur = []
        else:
            cur.append(t)
    if cur:
        groups.append(cur)
    groups = [g for g in groups if g]
    if not groups:
        return []

    first_group = groups[0]
    last = first_group[0]
    people = []
    p = _person(last, first_group[1:])
    if not p:
        return []
    people.append(p)

    for g in groups[1:]:
        # "& MARY L"  -> shares surname;  "& JONES MARY L" -> own surname if 3+ tokens
        # and first token is not a plausible given name position. We take
        # 3+ tokens as LAST FIRST MIDDLE, 1–2 tokens as FIRST [MIDDLE].
        if len(g) >= 3:
            q = _person(g[0], g[1:])
        else:
            q = _person(last, g)
        if q:
            people.append(q)

    # owner2 line, when present, is usually a second owner with full name
    if owner2:
        t2 = _tokens(owner2)
        if 2 <= len(t2) <= 5 and not any(x in _JOINERS for x in t2):
            q = _person(t2[0], t2[1:])
            if q and q["person_key"] not in {x["person_key"] for x in people}:
                people.append(q)

    return people[:4]
