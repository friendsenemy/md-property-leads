"""
Death provider #1: Maryland State Archives death index 1973–2014 (MSA SE-151),
released under the Public Information Act to Reclaim The Records, public domain.
Files: data/death/msa-se151-<year>.csv.gz (last, first_middle, year, month, day,
sex, race, age, county_code, cert).

Two separate scores per match, never merged:
  death_confidence     — how sure we are THIS RECORD is a real death (always HIGH
                         here: it is a state vital record) — kept for future
                         providers such as obituaries.
  identity_confidence  — how sure we are the dead person IS the SDAT owner
                         (name uniqueness, middle initial, county, age/deed
                         plausibility, transfer-after-death exclusion).
"""

import csv
import glob
import gzip
import logging
import os
import re
from collections import defaultdict
from datetime import date

from engine2 import config

log = logging.getLogger("death_index")

# MSA county codes → SDAT county codes
MSA_TO_SDAT = {"1": "01", "2": "02", "3": "04", "4": "05", "5": "06", "6": "07", "7": "08", "8": "09",
               "9": "10", "10": "11", "11": "12", "12": "13", "13": "14", "14": "15", "15": "16",
               "16": "17", "17": "18", "18": "19", "19": "20", "20": "21", "21": "22", "22": "23",
               "23": "24", "30": "03"}
# Counties whose residents commonly die in a neighbouring county's hospital
ADJACENT = {
    "19": {"09", "05", "17"}, "05": {"19", "09", "02", "17"}, "09": {"19", "05", "17"},
    "02": {"03", "04", "17", "14", "05"}, "17": {"16", "02", "09", "05", "14"}, "14": {"02", "04", "16", "17", "07"},
    "04": {"03", "13", "07", "02", "14"}, "03": {"04", "02"}, "13": {"04", "08"}, "07": {"04", "11", "14"},
    "16": {"17", "11", "14"}, "11": {"16", "07", "22"}, "22": {"11", "01"}, "01": {"22", "12"}, "12": {"01"},
    "18": {"15", "21", "06", "02"}, "15": {"18", "08"}, "21": {"18", "06", "10"}, "06": {"18", "21", "10"},
    "10": {"21", "06", "23"}, "23": {"10", "20", "24"}, "20": {"23", "24"}, "24": {"23", "20"}, "08": {"13", "15"},
}
SUFFIX = {"JR", "SR", "II", "III", "IV"}


def _norm_first(s):
    toks = [t for t in re.findall(r"[A-Z]+", (s or "").upper()) if t not in SUFFIX]
    return toks


class DeathIndex:
    def __init__(self, folder=None):
        self.by_name = defaultdict(list)     # (LAST, FIRST) -> [record tuples]
        self.freq = {}
        self.loaded = False
        self.folder = folder or os.path.join("data", "death")

    def load(self):
        n = 0
        for path in sorted(glob.glob(os.path.join(self.folder, "msa-se151-*.csv.gz"))):
            with gzip.open(path, "rt", newline="") as f:
                for r in csv.DictReader(f):
                    fm = _norm_first(r["first_middle"])
                    if not fm:
                        continue
                    raw_last = r["last"].upper().strip()
                    sm = re.search(r"\s+(JR|SR|II|III|IV)$", raw_last)
                    rec_suffix = sm.group(1) if sm else ""
                    raw_first = r["first_middle"].upper()
                    if not rec_suffix:
                        sm2 = re.search(r"\b(JR|SR|II|III|IV)\b", raw_first)
                        rec_suffix = sm2.group(1) if sm2 else ""
                    last = re.sub(r"[^A-Z]", "", re.sub(r"\s+(JR|SR|II|III|IV)$", "", raw_last))
                    key = (last, fm[0])
                    self.by_name[key].append((
                        int(r["year"]), int(r["month"] or 0), int(r["day"] or 0),
                        fm[1][0] if len(fm) > 1 else "", " ".join(fm[1:]),
                        int(r["age"]) if r["age"] else None,
                        MSA_TO_SDAT.get(r["county_code"], ""), r["cert"], r["sex"], rec_suffix,
                    ))
                    n += 1
        self.freq = {k: len(v) for k, v in self.by_name.items()}
        self.loaded = True
        log.info("death index: %d records, %d distinct names", n, len(self.by_name))
        return n

    def match(self, person, county_code, transfer_year=None, no_recorded_transfer=False):
        """
        person: dict from owner_parse.split_individuals
        Returns best match dict or None.
        """
        if not self.loaded:
            return None
        last = re.sub(r"[^A-Z]", "", (person.get("last_name") or "").upper())
        first = (person.get("first_name") or "").upper()
        if len(first) < 2 or not last:
            return None
        cands = self.by_name.get((last, first))
        if not cands:
            return None
        freq = len(cands)
        mi = (person.get("middle_initial") or "").upper()
        results = []
        own_suffix = (person.get("suffix") or "").upper()
        for (y, m, d, cmi, cmid, age, cty, cert, sex, rec_suffix) in cands:
            # Deed recorded after this death → not this person (or a namesake heir)
            if transfer_year and y < transfer_year:
                continue
            # Middle initial conflict is disqualifying; agreement is strong evidence
            if mi and cmi and mi != cmi:
                continue
            # Age plausibility: must have been an adult when the deed was recorded
            if age is not None and transfer_year:
                birth = y - age
                if transfer_year - birth < 18:
                    continue
            pts = 0
            # Generational suffixes: JR on the deed and SR (or none) on the death
            # record is the classic father/son namesake trap.
            if own_suffix or rec_suffix:
                if own_suffix == rec_suffix:
                    pts += 15
                else:
                    pts -= 35
            if mi and cmi and mi == cmi:
                pts += 40
            if cty == county_code:
                pts += 30
            elif cty in ADJACENT.get(county_code, set()):
                pts += 15
            elif cty == "":
                pts += 5     # 1974 volume has no county
            # rarity of the full name statewide over 42 years — the dominant factor.
            # A middle-initial hit on a name shared by 40 decedents is not evidence.
            if freq <= 2:
                pts += 30
            elif freq <= 6:
                pts += 20
            elif freq <= 12:
                pts += 5
            elif freq <= 25:
                pts -= 15
            elif freq <= 60:
                pts -= 30
            else:
                pts -= 45
            if age is not None and age >= 60:
                pts += 5
            results.append((pts, y, m, d, age, cty, cert, cmi, cmid))
        if not results:
            return None
        results.sort(reverse=True)
        best = results[0]
        pts = best[0]
        # more than one plausible record for this owner → ambiguity penalty
        plausible = [r for r in results if r[0] >= pts - 10]
        if len(plausible) > 1:
            pts -= 20 * (len(plausible) - 1)
        if len(results) > 3:
            pts -= 5 * (len(results) - 3)
        if pts >= 85 and freq <= 6 and len(plausible) == 1:
            conf = "HIGH"
        elif pts >= 50 and freq <= 20:
            conf = "MEDIUM"
        elif pts >= 25:
            conf = "LOW"
        else:
            return None
        return {
            "owner": person.get("normalized_name"),
            "death_date": f"{best[1]:04d}-{best[2]:02d}-{best[3]:02d}",
            "death_year": best[1],
            "age_at_death": best[4],
            "death_county_code": best[5],
            "certificate": best[6],
            "record_middle": (best[8] or best[7] or ""),
            "identity_confidence": conf,
            "identity_points": int(pts),
            "death_confidence": "HIGH",
            "same_name_deaths_statewide": freq,
            "candidates_after_filters": len(results),
            "source": "MSA SE-151 death index 1973-2014",
            "years_since_death": date.today().year - best[1],
        }
