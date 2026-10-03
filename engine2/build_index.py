"""
Pull the statewide SDAT parcel table into a local SQLite index.

  MD_OPENDATA_USERNAME=... MD_OPENDATA_PASSWORD=... python -m engine2.build_index
  python -m engine2.build_index --max-pages 1          # smoke test
  python -m engine2.build_index --dataset ed4q-f8tm    # public (no owner names) plumbing test

The index is ~2.4M rows with a slim column set and lives in .cache/ (gitignored).
It is rebuilt weekly by the title-scan workflow and cached between runs.
"""

import argparse
import logging
import os
import re
import sqlite3
import sys
import time

import requests

from engine2 import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("build_index")

# column alias -> SDAT field name. Owner/mailing columns are discovered at
# runtime because the public dataset hides them and their exact names differ.
STATIC_COLUMNS = {
    "acct":        "account_id_mdp_field_acctid",
    "county_code": "record_key_county_code_sdat_field_1",
    "county":      "county_name_mdp_field_cntyname",
    "address":     "mdp_street_address_mdp_field_address",
    "city":        "premise_address_city_mdp_field_premcity_sdat_field_25",
    "zip":         "premise_address_zip_code_mdp_field_premzip_sdat_field_26",
    "lat":         "mdp_latitude_mdp_field_digycord_converted_to_wgs84",
    "lon":         "mdp_longitude_mdp_field_digxcord_converted_to_wgs84",
    "occupancy":   "record_key_owner_occupancy_code_mdp_field_ooi_sdat_field_6",
    "homestead":   "homestead_qualification_code_mdp_field_homqlcod_sdat_field_259",
    "land_use":    "land_use_code_mdp_field_lu_desclu_sdat_field_50",
    "exempt_class": "exempt_class_mdp_field_exclass_descexcl_sdat_field_49",
    "land_value":  "current_cycle_data_land_value_mdp_field_names_nfmlndvl_curlndvl_and_sallndvl_sdat_field_164",
    "impr_value":  "current_cycle_data_improvements_value_mdp_field_names_nfmimpvl_curimpvl_and_salimpvl_sdat_field_165",
    "year_built":  "c_a_m_a_system_data_year_built_yyyy_mdp_field_yearblt_sdat_field_235",
    "sqft":        "c_a_m_a_system_data_structure_area_sq_ft_mdp_field_sqftstrc_sdat_field_241",
    "condition":   "c_a_m_a_system_data_dwelling_condition_code_sdat_field_233",
    "dwelling_type": "additional_c_a_m_a_data_dwelling_type_mdp_field_strubldg_sdat_field_265",
    "legal1":      "legal_description_line_1_mdp_field_legal1_sdat_field_17",
    "deed_liber":  "deed_reference_1_liber_mdp_field_dr1liber_sdat_field_30",
    "deed_folio":  "deed_reference_1_folio_mdp_field_dr1folio_sdat_field_31",
    "transfer_date": "sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89",
    "sale_price":  "sales_segment_1_consideration_mdp_field_considr1_sdat_field_90",
    "grantor1":    "sales_segment_1_grantor_name_mdp_field_grntnam1_sdat_field_80",
    "transfer_date2": "sales_segment_2_transfer_date_yyyy_mm_dd_sdat_field_109",
    "grantor2":    "sales_segment_2_grantor_name_sdat_field_100",
    # how a deed moved + the money behind it. "(4) non-arms-length ... foreclosure"
    # is the statewide foreclosure marker; the mortgage fields are the ORIGINAL
    # loan amounts, which is what a surplus estimate actually needs.
    "how_conveyed1": "sales_segment_1_how_conveyed_ind_mdp_field_convey1_sdat_field_87",
    "mortgage1":   "sales_segment_1_mortgage_mdp_field_mortgag1_sdat_field_92",
    "how_conveyed2": "sales_segment_2_how_conveyed_ind_sdat_field_107",
    "sale_price2": "sales_segment_2_consideration_sdat_field_110",
    "mortgage2":   "sales_segment_2_mortgage_sdat_field_112",
    "grantor3":    "sales_segment_3_grantor_name_sdat_field_120",
    "how_conveyed3": "sales_segment_3_how_conveyed_ind_sdat_field_127",
    "transfer_date3": "sales_segment_3_transfer_date_yyyy_mm_dd_sdat_field_129",
    "sale_price3": "sales_segment_3_consideration_sdat_field_130",
    "mortgage3":   "sales_segment_3_mortgage_sdat_field_132",
}

# Discovered by regex against the first record's keys.
DYNAMIC_PATTERNS = {
    "owner1":    re.compile(r"ownname1"),
    "owner2":    re.compile(r"ownname2"),
    "mail_addr": re.compile(r"(owner|mailing).*(address|addr).*(line_?1|_1)|mail.*addr.*1"),
    "mail_city": re.compile(r"(owner|mailing).*city|mail.*city"),
    "mail_zip":  re.compile(r"(owner|mailing).*zip|mail.*zip"),
}

ALL_ALIASES = list(STATIC_COLUMNS) + list(DYNAMIC_PATTERNS)


def _auth():
    u, p = os.environ.get("MD_OPENDATA_USERNAME", ""), os.environ.get("MD_OPENDATA_PASSWORD", "")
    return (u, p) if u and p else None


def _params(extra):
    p = dict(extra)
    tok = os.environ.get("MD_OPENDATA_APP_TOKEN", "")
    if tok:
        p["$$app_token"] = tok
    return p


def _get(url, params, session):
    delay = 5
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            r = session.get(url, params=_params(params), auth=_auth(), timeout=config.REQUEST_TIMEOUT,
                            headers={"Accept": "application/json", "User-Agent": "MD-Property-Leads/2.0"})
            if r.status_code == 200:
                return r.json()
            if r.status_code in (401, 403):
                log.error("Socrata %s — check MD_OPENDATA_USERNAME/PASSWORD", r.status_code)
                sys.exit(2)
            log.warning("Socrata %s (attempt %d): %s", r.status_code, attempt, r.text[:200])
        except requests.RequestException as e:
            log.warning("request failed (attempt %d): %s", attempt, e)
        time.sleep(delay)
        delay = min(delay * 2, 120)
    raise RuntimeError("Socrata gave up after retries")


def discover_columns(url, session):
    """Fetch one record and map every alias to a real column (or None)."""
    dataset = url.rsplit("/", 1)[-1].replace(".json", "")
    keys = []
    try:
        r = session.get(f"https://opendata.maryland.gov/api/views/{dataset}.json", auth=_auth(),
                        params=_params({}), timeout=60)
        if r.status_code == 200:
            keys = [c["fieldName"] for c in r.json().get("columns", [])]
    except requests.RequestException:
        pass
    if not keys:  # fallback: union of keys across a sample (Socrata omits null fields)
        sample = _get(url, {"$limit": 200}, session)
        keys = sorted({k for rec in sample for k in rec})
    mapping = {}
    for alias, col in STATIC_COLUMNS.items():
        mapping[alias] = col if col in keys else None
    for alias, rx in DYNAMIC_PATTERNS.items():
        hit = next((k for k in keys if rx.search(k)), None)
        mapping[alias] = hit
    missing = [a for a, c in mapping.items() if c is None]
    log.info("columns: %d resolved, missing: %s", len(mapping) - len(missing), missing or "none")
    return mapping


def create_db(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)
    db = sqlite3.connect(path)
    cols = ", ".join(f"{a} TEXT" for a in ALL_ALIASES if a != "acct")
    db.execute(f"CREATE TABLE parcels (acct TEXT PRIMARY KEY, {cols})")
    db.execute("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT)")
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    return db


def build(dataset=config.DATASET_ID, max_pages=None, out=config.INDEX_PATH):
    url = f"{config.SOCRATA_BASE}/{dataset}.json"
    session = requests.Session()
    mapping = discover_columns(url, session)
    select_cols = [c for c in mapping.values() if c]
    order_col = mapping["acct"]
    if not order_col:
        raise SystemExit("account id column missing — cannot page deterministically")

    db = create_db(out)
    insert_sql = f"INSERT OR REPLACE INTO parcels ({', '.join(ALL_ALIASES)}) VALUES ({', '.join('?' * len(ALL_ALIASES))})"

    offset, page, total = 0, 0, 0
    started = time.time()
    while True:
        page += 1
        if max_pages and page > max_pages:
            break
        rows = _get(url, {"$select": ",".join(select_cols), "$order": order_col,
                          "$limit": config.PAGE_SIZE, "$offset": offset}, session)
        if not rows:
            break
        batch = []
        for r in rows:
            batch.append(tuple((r.get(mapping[a]) if mapping[a] else None) for a in ALL_ALIASES))
        db.executemany(insert_sql, batch)
        db.commit()
        total += len(rows)
        offset += len(rows)
        log.info("page %d: +%d rows (%d total, %.0fs)", page, len(rows), total, time.time() - started)
        if len(rows) < config.PAGE_SIZE:
            break

    db.execute("CREATE INDEX IF NOT EXISTS ix_county ON parcels(county_code)")
    db.execute("CREATE INDEX IF NOT EXISTS ix_transfer ON parcels(transfer_date)")
    db.execute("INSERT OR REPLACE INTO meta VALUES ('built_at', datetime('now'))")
    db.execute("INSERT OR REPLACE INTO meta VALUES ('rows', ?)", (str(total),))
    db.execute("INSERT OR REPLACE INTO meta VALUES ('dataset', ?)", (dataset,))
    db.execute("INSERT OR REPLACE INTO meta VALUES ('columns', ?)", (repr(mapping),))
    db.commit()
    db.close()
    log.info("index built: %d rows -> %s (%.1f MB)", total, out, os.path.getsize(out) / 1e6)
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default=config.DATASET_ID)
    ap.add_argument("--max-pages", type=int, default=None)
    ap.add_argument("--out", default=config.INDEX_PATH)
    ap.add_argument("--list-columns", action="store_true", help="print every SDAT field name and exit (needs the Socrata login)")
    a = ap.parse_args()
    if a.list_columns:
        import json as _json
        logging.basicConfig(level=logging.INFO)
        sess = requests.Session()
        url = f"{config.SOCRATA_BASE}/{a.dataset}.json"
        r = sess.get(f"https://opendata.maryland.gov/api/views/{a.dataset}.json", auth=_auth(), params=_params({}), timeout=60)
        keys = [c["fieldName"] for c in r.json().get("columns", [])] if r.status_code == 200 else sorted({k for rec in _get(url, {"$limit": 200}, sess) for k in rec})
        sample = _get(url, {"$limit": 50, "$where": "sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89 >= '2026.06.01'"}, sess)
        os.makedirs("data/surplus", exist_ok=True)
        with open("data/surplus/sdat-columns.json", "w", encoding="utf-8") as f:
            _json.dump({"columns": keys, "sample_values": {k: sorted({str(rec.get(k)) for rec in sample if rec.get(k) is not None})[:12]
                                                           for k in keys if any(w in k for w in ("conv", "transfer", "type", "grant", "sale", "deed", "mort"))}}, f, indent=1)
        print(len(keys), "columns written to data/surplus/sdat-columns.json")
        sys.exit(0)
    build(a.dataset, a.max_pages, a.out)
