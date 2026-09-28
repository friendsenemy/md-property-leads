"""
Turn one parcel row from the index into flags and a property record.
Pure functions; no I/O. Thresholds come from config.
"""

import re
from datetime import date

from engine2 import config
from engine2.owner_parse import classify, flags_for, split_individuals
from property_lookup import estimate_equity, COUNTY_NAMES

RESIDENTIAL_HINTS = ("RESIDENTIAL", "TOWN HOUSE", "TOWNHOUSE", "CONDO", "APARTMENT", "DWELLING")


def _num(v):
    try:
        return float(v) if v not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _year(datestr):
    s = (datestr or "").replace(".", "-").strip()
    if len(s) >= 4 and s[:4].isdigit():
        y = int(s[:4])
        return y if 1800 < y <= date.today().year else None
    return None


_ADDR_NOISE = re.compile(r"\b(APT|UNIT|STE|SUITE|LOT|BLDG|FL|#)\b.*$")


def _addr_key(a):
    """(house number, first street word) — ignores unit letters/suffixes."""
    a = re.sub(r"[^A-Z0-9 ]", " ", a.upper())
    a = _ADDR_NOISE.sub("", a)
    m = re.match(r"\s*(\d+)\s*[A-Z]?\s+(?:[A-Z]\s+)?([A-Z]+)", a)
    if not m:
        return None
    return m.group(1), m.group(2)


def _same_street_address(mail, prem):
    km, kp = _addr_key(mail), _addr_key(prem)
    if not km or not kp:
        return False           # PO BOX, out-of-state formats, etc. count as different
    return km == kp


def is_residential(land_use):
    lu = (land_use or "").upper()
    return lu.startswith("R") or any(h in lu for h in RESIDENTIAL_HINTS)


def analyze(row, today=None, force=False, deaths=None):
    """
    row: dict with the aliases from build_index.ALL_ALIASES.
    Returns (property_dict, flags:set, facts:dict) or None if ineligible.
    """
    today = today or date.today()
    land_v, impr_v = _num(row.get("land_value")), _num(row.get("impr_value"))
    assessed = land_v + impr_v
    if assessed < config.MIN_ASSESSED_VALUE:
        return None

    owner1, owner2 = row.get("owner1") or "", row.get("owner2") or ""
    owner_type = classify(owner1, owner2)
    if owner_type in ("GOVERNMENT", "LLC", "CORPORATION", "NONPROFIT", "UNKNOWN"):
        return None

    flags = flags_for(owner1, owner2)
    if owner_type == "MULTIPLE_INDIVIDUALS":
        flags.add("MULTIPLE_INDIVIDUALS")
    if owner_type == "TRUST":
        # "SMITH FAMILY ESTATE TRUST" is estate planning, not a decedent's estate
        flags.discard("ESTATE_IN_NAME")

    ty = _year(row.get("transfer_date"))
    years_since_transfer = (today.year - ty) if ty else None
    # "0000.00.00" / blank = deed predates SDAT's electronic sales records
    # (roughly pre-1980s). That is the OLDEST ownership there is, not "unknown".
    no_recorded_transfer = ty is None and (row.get("transfer_date") or "").strip() in ("", "0000.00.00", "0000-00-00")
    estate_signal = bool(flags & {"ESTATE_IN_NAME", "HEIRS_IN_NAME", "DECEASED_IN_NAME",
                                  "PERSONAL_REP", "LIFE_ESTATE", "SURVIVING", "CONSERVATOR"})
    stale = no_recorded_transfer or (years_since_transfer is not None and years_since_transfer >= config.CANDIDATE_MIN_YEARS_SINCE_TRANSFER)
    owners = split_individuals(owner1, owner2)
    death_matches, years_since_death = [], None
    if deaths is not None and owners:
        cc = (row.get("county_code") or "").zfill(2)
        for o in owners:
            m = deaths.match(o, cc, transfer_year=ty, no_recorded_transfer=no_recorded_transfer)
            if m:
                death_matches.append(m)
        strong = [m for m in death_matches if m["identity_confidence"] in ("HIGH", "MEDIUM")]
        if death_matches:
            if len(owners) == 1:
                flags.add("DECEASED_SOLE_OWNER_" + death_matches[0]["identity_confidence"])
            elif strong and len(strong) == len(owners):
                flags.add("ALL_OWNERS_DECEASED")
            elif strong:
                flags.add("CO_OWNER_DECEASED")
            elif len(owners) == 1:
                pass
            if strong:
                years_since_death = max(m["years_since_death"] for m in strong)
    death_signal = bool(flags & {"DECEASED_SOLE_OWNER_HIGH", "DECEASED_SOLE_OWNER_MEDIUM", "ALL_OWNERS_DECEASED"})
    if not estate_signal and not stale and not force and not death_signal:
        return None
    if stale:
        flags.add("STALE_OWNERSHIP")
    if no_recorded_transfer:
        flags.add("NO_RECORDED_TRANSFER")

    residential = is_residential(row.get("land_use"))
    occ = (row.get("occupancy") or "").strip().upper()
    if occ not in config.OWNER_OCCUPIED_CODES:
        flags.add("ABSENTEE")
    if residential and not (row.get("homestead") or "").strip():
        flags.add("NO_HOMESTEAD")
    if residential and impr_v <= 0:
        flags.add("VACANT_LAND")
    yb = _year(row.get("year_built"))
    if yb and yb < config.OLD_STRUCTURE_YEAR:
        flags.add("OLD_STRUCTURE")
    cond = (row.get("condition") or "").strip()
    if cond and cond in config.POOR_CONDITION_CODES:
        flags.add("POOR_CONDITION")
    mail = (row.get("mail_addr") or "").strip().upper()
    prem = (row.get("address") or "").strip().upper()
    if mail.startswith("C/O") or mail.startswith("C O "):
        flags.add("CARE_OF")
    elif mail and prem and not _same_street_address(mail, prem):
        flags.add("MAIL_MISMATCH")
    if cond and cond in config.BELOW_AVERAGE_CONDITION_CODES:
        flags.add("BELOW_AVG_CONDITION")

    prop = {
        "account_number": row.get("acct"),
        "owner_name": owner1 or owner2,
        "owner_name_2": owner2 if owner1 else "",
        "owner_type": owner_type,
        "owners": owners,
        "death_matches": death_matches,
        "property_address": row.get("address") or "",
        "city": row.get("city") or "",
        "zip_code": row.get("zip") or "",
        "county": COUNTY_NAMES.get((row.get("county_code") or "").zfill(2), row.get("county") or ""),
        "county_code": row.get("county_code") or "",
        "lat": row.get("lat"),
        "lon": row.get("lon"),
        "property_type": row.get("land_use") or "",
        "assessed_value": str(int(assessed)),
        "land_value": str(int(land_v)),
        "improvement_value": str(int(impr_v)),
        "year_built": row.get("year_built") or "",
        "square_footage": row.get("sqft") or "",
        "condition_code": cond,
        "occupancy_code": occ,
        "homestead_code": (row.get("homestead") or "").strip(),
        "legal_description": row.get("legal1") or "",
        "deed_liber": row.get("deed_liber") or "",
        "deed_folio": row.get("deed_folio") or "",
        "transfer_date": row.get("transfer_date") or "",
        "sale_price": row.get("sale_price") or "",
        "grantor": row.get("grantor1") or "",
        "prior_transfer_date": row.get("transfer_date2") or "",
        "prior_grantor": row.get("grantor2") or "",
        "mailing_address": row.get("mail_addr") or "",
        "years_since_transfer": years_since_transfer,
    }
    prop.update(estimate_equity(prop))
    facts = {"residential": residential, "years_since_transfer": years_since_transfer,
             "estate_signal": estate_signal, "no_recorded_transfer": no_recorded_transfer,
             "years_since_death": years_since_death}
    return prop, flags, facts
