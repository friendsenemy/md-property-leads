"""
All tunable numbers for engine 2 live here. Nothing else in engine2/ should
hard-code a threshold or weight.
"""

# ── SDAT source ─────────────────────────────────────────────────────────────
DATASET_ID = "9xq5-z8s2"                 # licensed, owner names present
SOCRATA_BASE = "https://opendata.maryland.gov/resource"
PAGE_SIZE = 50000                        # Socrata max for SODA 2.1
REQUEST_TIMEOUT = 300
MAX_RETRIES = 5

# ── Index location (never committed) ────────────────────────────────────────
CACHE_DIR = ".cache"
INDEX_PATH = f"{CACHE_DIR}/sdat.sqlite"

# ── Output ──────────────────────────────────────────────────────────────────
OUTPUT_DIR = "data/title"
PER_COUNTY_LIMIT = 400        # rows kept per county shard
STATEWIDE_TOP_LIMIT = 1000    # rows in top.json
MIN_PRIORITY_TO_KEEP = 35     # below this a row is not written anywhere

# ── Eligibility ─────────────────────────────────────────────────────────────
MIN_ASSESSED_VALUE = 40000    # skip slivers, sheds, $5k lots
CANDIDATE_MIN_YEARS_SINCE_TRANSFER = 12   # stale-ownership candidates (absent an estate marker)

# ── Owner-occupancy / homestead codes as SDAT stores them ───────────────────
# 'H' = owner-occupied principal residence in SDAT's OOI field. Everything else
# (blank, 'N', other) is treated as not owner-occupied. The scan writes value
# counts to summary.json so this can be recalibrated against real data.
OWNER_OCCUPIED_CODES = {"H"}

# ── Title-complexity points (capped at 100) ─────────────────────────────────
TITLE_POINTS = {
    "ESTATE_IN_NAME": 45,      # "ESTATE OF", "EST OF", "ESTATE" in owner
    "HEIRS_IN_NAME": 45,       # "HEIRS OF", "HEIRS"
    "DECEASED_IN_NAME": 45,    # "DECEASED", "DEC'D", "DECD"
    "PERSONAL_REP": 35,        # "PERS REP", "PERSONAL REP", "P/R", "PR OF"
    "LIFE_ESTATE": 10,         # estate-planning deed: passes automatically at death; low complexity
    "CARE_OF": 10,             # "C/O" — someone else handles the mail
    "ET_AL": 15,               # "ET AL" — more owners than the record names
    "SURVIVING": 20,           # "SURV", "SURVIVING"
    "TRUSTEE": 8,              # trust-held; deceased-trustee check is a later phase
    "MULTIPLE_INDIVIDUALS": 6,
    # stale ownership: points per full year past the candidate threshold
    "STALE_PER_YEAR": 1.5,
    "STALE_CAP": 35,
}

# ── Distress points (capped at 100) ─────────────────────────────────────────
DISTRESS_POINTS = {
    "ABSENTEE": 30,            # not owner-occupied
    "NO_HOMESTEAD": 10,        # no homestead credit on a residential parcel
    "VACANT_LAND": 15,         # improvement value 0 on a residential-zoned lot
    "OLD_STRUCTURE": 10,       # built before OLD_STRUCTURE_YEAR
    "POOR_CONDITION": 25,      # CAMA dwelling condition code in POOR_CONDITION_CODES
    "BELOW_AVG_CONDITION": 12, # "Below Average (3)" — 2% of parcels statewide
    "MAIL_MISMATCH": 20,       # owner mailing address differs from premise (when columns exist)
}
OLD_STRUCTURE_YEAR = 1950
# CAMA dwelling grade/condition labels as SDAT stores them, e.g. "Low (1)",
# "Economy (2)", "Below Average (3)", "Average (4)". summary.json carries the
# live distribution; adjust here if a county uses different labels.
POOR_CONDITION_CODES = {"Low (1)", "Economy (2)", "Poor", "Very Poor", "Unsound"}
BELOW_AVERAGE_CONDITION_CODES = {"Below Average (3)"}

# ── Financial points (capped at 100) ────────────────────────────────────────
FINANCIAL = {
    "EQUITY_FULL_AT": 300000,   # equity >= this → 60 pts
    "EQUITY_FLOOR": 25000,      # equity below this → 0 pts
    "EQUITY_POINTS": 60,
    "VALUE_FULL_AT": 400000,    # assessed >= this → 25 pts
    "VALUE_POINTS": 25,
    "CONFIDENCE_POINTS": {"high": 15, "medium": 10, "low": 5, "unknown": 0},
}

# ── Research priority = weighted blend of the three ─────────────────────────
PRIORITY_WEIGHTS = {
    "title": 0.55,
    "financial": 0.25,
    "distress": 0.20,
}

# ── Owner classification vocab ──────────────────────────────────────────────
BUSINESS_MARKERS = (
    " LLC", " L L C", " INC", " CORP", " CO ", " COMPANY", " LP", " LLP", " LTD",
    " PARTNERS", " PARTNERSHIP", " HOLDINGS", " PROPERTIES", " INVESTMENTS",
    " ENTERPRISES", " GROUP", " DEVELOPMENT", " REALTY", " VENTURES", " CAPITAL",
    " HOMES", " BUILDERS", " ASSOCIATES", " BANK", " MORTGAGE", " FUND",
    " REAL ESTATE", " REAL EST ", " REALTY", " PTNSHP", " PTNRS", " PTRS", " LIMITED", " CORPORATION",
    " INDUSTRIES", " SERVICES", " STORES", " MARKET", " RESTAURANT", " MOTEL", " HOTEL", " APARTMENTS",
)
GOVERNMENT_MARKERS = (
    "STATE OF MARYLAND", "COUNTY COMMISSIONERS", "BOARD OF EDUCATION", "MAYOR AND CITY",
    "MAYOR & CITY", "CITY OF ", "COUNTY OF ", "TOWN OF ", "UNITED STATES", "HOUSING AUTHORITY",
    "DEPARTMENT OF", "DEPT OF", "COMMISSION", " AUTHORITY", "BOARD OF ", "MARYLAND NATIONAL",
    "PARK AND PLANNING", "SCHOOL", "PUBLIC WORKS",
)
NONPROFIT_MARKERS = (
    " CHURCH", " CONGREGATION", " MINISTRIES", " TEMPLE", " SYNAGOGUE", " MOSQUE", " PARISH",
    " DIOCESE", " ASSOCIATION", " ASSN", " FOUNDATION", " SOCIETY", " LODGE", " CEMETERY",
    " CLUB", " FIRE DEPT", " FIRE DEPARTMENT", " VOLUNTEER", " LEAGUE", " COUNCIL", " UNION",
    " HOA", " CONDOMINIUM", " CONDO", " COOPERATIVE", " HOMEOWNERS",
)
TRUST_MARKERS = (" TRUST", " TRUSTEE", " TRUSTEES", " TR ", " TRS ", " REV TR", " LIV TR", " FAMILY TR")
ESTATE_MARKERS = (" ESTATE", " EST OF", " EST ", " ESTATE OF", "ESTATE OF ", " HEIRS", " PERS REP",
                  " PERSONAL REP", " P/R", " PR OF", " DECEASED", " DEC'D", " DECD", " EXECUTOR",
                  " EXECUTRIX", " ADMINISTRATOR", " ADMINISTRATRIX")

# ── Presets shown in the dashboard (name → filter spec) ─────────────────────
PRESETS = [
    {"id": "estate_named",   "label": "Estate / Heirs in Owner Name", "any_flags": ["ESTATE_IN_NAME", "HEIRS_IN_NAME", "DECEASED_IN_NAME", "PERSONAL_REP"]},
    {"id": "life_estate",    "label": "Life Estate (heirs pre-named)", "any_flags": ["LIFE_ESTATE"]},
    {"id": "poor_condition", "label": "Poor / Below-Avg Condition",   "any_flags": ["POOR_CONDITION", "BELOW_AVG_CONDITION"]},
    {"id": "stale_absentee", "label": "Owned 25+ yrs, Absentee",      "min_years_since_transfer": 25, "flags": ["ABSENTEE"]},
    {"id": "stale_40",       "label": "Owned 40+ yrs",                "min_years_since_transfer": 40},
    {"id": "high_equity",    "label": "High Equity + Estate Signal",  "min_equity": 200000, "any_flags": ["ESTATE_IN_NAME", "HEIRS_IN_NAME", "DECEASED_IN_NAME", "PERSONAL_REP", "LIFE_ESTATE"]},
    {"id": "vacant_lot",     "label": "Vacant Lot + Estate Signal",   "flags": ["VACANT_LAND"], "any_flags": ["ESTATE_IN_NAME", "HEIRS_IN_NAME", "DECEASED_IN_NAME"]},
    {"id": "top",            "label": "Highest Research Priority",    "sort": "priority"},
]
