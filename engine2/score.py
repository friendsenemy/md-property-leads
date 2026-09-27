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
    if y and y >= config.CANDIDATE_MIN_YEARS_SINCE_TRANSFER:
        s = min(P["STALE_CAP"], (y - config.CANDIDATE_MIN_YEARS_SINCE_TRANSFER) * P["STALE_PER_YEAR"] + 5)
        pts += s
        reasons.append(f"No transfer in {y} yrs (+{int(s)})")
    return _cap(pts), reasons


def distress(flags):
    pts, reasons = 0.0, []
    for f, v in config.DISTRESS_POINTS.items():
        if f in flags:
            pts += v
            reasons.append(f"{f.replace('_', ' ').title()} (+{v})")
    return _cap(pts), reasons


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
        "financial": f,
        "priority": priority,
        "reasons": {"title": tr, "distress": dr, "financial": fr},
    }


def title_class(flags, facts):
    """Phase-1 classification (no probate data yet)."""
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
    y = facts.get("years_since_transfer") or 0
    if y >= 40:
        return "S3", f"No transfer in {y} years"
    if y >= 25:
        return "S2", f"No transfer in {y} years"
    return "S1", f"No transfer in {y} years"
