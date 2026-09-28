"""
Three explained scores plus a weighted research priority. Every point comes
with a reason string so the dashboard can show why a row ranks where it does.
"""

from engine2 import config


def _cap(v):
    return int(max(0, min(100, round(v))))


def title_complexity(flags, facts):
    pts, reasons = 0.0, []
    P = config.TITLE_POINTS
    for f in ("ESTATE_IN_NAME", "HEIRS_IN_NAME", "DECEASED_IN_NAME", "PERSONAL_REP",
              "LIFE_ESTATE", "CARE_OF", "ET_AL", "SURVIVING", "CONSERVATOR", "TRUSTEE", "MULTIPLE_INDIVIDUALS"):
        if f in flags:
            pts += P[f]
            reasons.append(f"{f.replace('_', ' ').title()} (+{P[f]})")
    y = facts.get("years_since_transfer")
    if facts.get("no_recorded_transfer"):
        pts += P["STALE_CAP"]
        reasons.append(f"No recorded transfer — deed predates electronic records (+{P['STALE_CAP']})")
    elif y and y >= config.CANDIDATE_MIN_YEARS_SINCE_TRANSFER:
        s = min(P["STALE_CAP"], (y - config.CANDIDATE_MIN_YEARS_SINCE_TRANSFER) * P["STALE_PER_YEAR"] + 5)
        pts += s
        reasons.append(f"No transfer in {y} yrs (+{int(s)})")
    for f in ("DECEASED_SOLE_OWNER_HIGH", "DECEASED_SOLE_OWNER_MEDIUM", "DECEASED_SOLE_OWNER_LOW",
              "ALL_OWNERS_DECEASED", "CO_OWNER_DECEASED"):
        if f in flags:
            pts += P[f]
            reasons.append(f"{f.replace('_', ' ').title()} (+{P[f]})")
    for f in ("PROBATE_CLOSED_STILL_TITLED", "PROBATE_OPEN_STALE", "PROBATE_OPEN", "PROBATE_NONE_FOUND"):
        if f in flags:
            pts += P[f]
            reasons.append(f"{f.replace('_', ' ').title()} (+{P[f]}, Register of Wills record)")
    ysd = facts.get("years_since_death")
    if ysd and (flags & {"DECEASED_SOLE_OWNER_HIGH", "DECEASED_SOLE_OWNER_MEDIUM", "ALL_OWNERS_DECEASED"}):
        extra = min(P["DECADES_SINCE_DEATH_CAP"], ysd * P["DECADES_SINCE_DEATH_PER_YEAR"])
        pts += extra
        reasons.append(f"{ysd} years since death, no transfer (+{int(extra)})")
    long_held = facts.get("no_recorded_transfer") or (y or 0) >= 25
    if long_held and (flags & {"TAX_SALE_SOLD", "TAX_SALE_STRUCK"}):
        pts += P["STALE_AND_DELINQUENT"]
        reasons.append(f"Decades-old deed + tax lien: owner likely gone (+{P['STALE_AND_DELINQUENT']})")
    return _cap(pts), reasons


def distress(flags):
    hard, soft, reasons = 0.0, 0.0, []
    for f, v in config.DISTRESS_HARD_POINTS.items():
        if f in flags:
            hard += v
            reasons.append(f"{f.replace('_', ' ').title()} (+{v}, verified record)")
    for f, v in config.DISTRESS_SOFT_POINTS.items():
        if f in flags:
            soft += v
            reasons.append(f"{f.replace('_', ' ').title()} (+{v})")
    soft = min(config.DISTRESS_SOFT_CAP, soft)
    if soft == config.DISTRESS_SOFT_CAP:
        reasons.append(f"Soft indicators capped at {config.DISTRESS_SOFT_CAP}")
    return _cap(min(100, hard) + soft), reasons


def financial(prop):
    F = config.FINANCIAL
    pts, reasons = 0.0, []
    eq = prop.get("estimated_equity")
    if eq is not None:
        eq = float(eq)
        if eq > F["EQUITY_FLOOR"]:
            frac = min(1.0, (eq - F["EQUITY_FLOOR"]) / (F["EQUITY_FULL_AT"] - F["EQUITY_FLOOR"]))
            p = frac * F["EQUITY_POINTS"]
            pts += p
            reasons.append(f"Est. equity ${int(eq):,} (+{int(p)})")
    av = float(prop.get("assessed_value") or 0)
    if av > 0:
        p = min(1.0, av / F["VALUE_FULL_AT"]) * F["VALUE_POINTS"]
        pts += p
        reasons.append(f"Assessed ${int(av):,} (+{int(p)})")
    c = prop.get("equity_confidence", "unknown")
    pts += F["CONFIDENCE_POINTS"].get(c, 0)
    return _cap(pts), reasons


def score(prop, flags, facts):
    t, tr = title_complexity(flags, facts)
    d, dr = distress(flags)
    f, fr = financial(prop)
    W = config.PRIORITY_WEIGHTS
    priority = _cap(t * W["title"] + f * W["financial"] + d * W["distress"])
    return {
        "title_complexity": t,
        "distress": d,
        "distress_verified": any(f in flags for f in config.DISTRESS_HARD_POINTS),
        "financial": f,
        "priority": priority,
        "reasons": {"title": tr, "distress": dr, "financial": fr},
    }


def title_class(flags, facts):
    """Classification. D-classes come from probate/death records; E from SDAT owner text; S from deed age."""
    if "PROBATE_CLOSED_STILL_TITLED" in flags:
        return "D3", "Estate CLOSED but property still titled to decedent (Register of Wills)"
    if "PROBATE_OPEN_STALE" in flags:
        return "D2-S", "Estate open and stale (Register of Wills)"
    if "PROBATE_OPEN" in flags:
        return "D2", "Estate currently open (Register of Wills)"
    if "PROBATE_NONE_FOUND" in flags:
        return "D1", "Deceased owner — no Maryland estate located (Register of Wills searched)"
    if "ALL_OWNERS_DECEASED" in flags:
        return "D5", "All titled owners deceased (death index) — no transfer since"
    if "DECEASED_SOLE_OWNER_HIGH" in flags or "DECEASED_SOLE_OWNER_MEDIUM" in flags:
        return "D1", "Deceased sole owner still on title (death index) — no estate check yet"
    if "ESTATE_IN_NAME" in flags or "DECEASED_IN_NAME" in flags or "PERSONAL_REP" in flags:
        return "E1", "Estate named as owner — decedent still on title"
    if "HEIRS_IN_NAME" in flags:
        return "E2", "Heirs named as owner — unprobated inheritance"
    if "LIFE_ESTATE" in flags:
        return "E3", "Life estate — remainder interest pending"
    if "SURVIVING" in flags:
        return "E4", "Surviving co-owner noted on title"
    if "CONSERVATOR" in flags:
        return "E5", "Conservator / guardian / POA on title — owner likely incapacitated"
    if "CO_OWNER_DECEASED" in flags:
        return "D4", "One co-owner deceased (death index) — survivor may hold title"
    if "DECEASED_SOLE_OWNER_LOW" in flags:
        return "D1?", "Possible deceased owner (common name — low identity confidence)"
    y = facts.get("years_since_transfer") or 0
    if facts.get("no_recorded_transfer"):
        return "S3", "No recorded transfer — ownership predates SDAT sales records"
    if y >= 40:
        return "S3", f"No transfer in {y} years"
    if y >= 25:
        return "S2", f"No transfer in {y} years"
    return "S1", f"No transfer in {y} years"
