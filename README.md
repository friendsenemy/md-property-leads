# MD Property Leads

**Maryland Pre-Probate Lead Generator** — finds recently deceased Marylanders who owned real estate, before the estate hits probate court.

**Live dashboard:** https://pagesofpurposellc.com/md-property-leads/

## How it works

Every morning a GitHub Action:

1. Scrapes new Maryland obituaries from Legacy.com (20 newspaper feeds + all 24 county pages)
2. Pulls each obituary page for date of death, age, city, and survived-by heirs
3. Searches the deceased's name against MD SDAT property ownership records (licensed Socrata dataset)
4. Estimates equity from assessed value and last sale price
5. Commits the results to `data/leads.json`

The dashboard is a static page on GitHub Pages that reads that file. No server, no database, nothing to keep alive. Lead status and notes are stored in your browser.

## Running a scrape on demand

Click **Run Scrape Now** on the dashboard (or go to Actions → Daily Scrape → Run workflow). Results appear in about 10 minutes — refresh the page.

## Setup (already done)

Repository secrets (Settings → Secrets and variables → Actions):

| Secret | Purpose |
|---|---|
| `MD_OPENDATA_USERNAME` | Socrata login email — required for the owner-name dataset |
| `MD_OPENDATA_PASSWORD` | Socrata password |
| `MD_OPENDATA_APP_TOKEN` | Optional, raises rate limits |

GitHub Pages: Settings → Pages → Source: *Deploy from a branch* → `main` / `/ (root)`.

## Files

| File | What it does |
|---|---|
| `pipeline.py` | The daily job: scrape → SDAT lookup → write `data/leads.json` |
| `scraper.py` | Legacy.com scraper (uses `curl_cffi` to get past their TLS fingerprinting) |
| `property_lookup.py` | SDAT Socrata queries + equity estimation |
| `index.html`, `static/` | The dashboard |
| `data/leads.json` | Every lead ever found (append-only, deduped) |
| `data/seen.json` | Obituaries already checked, so SDAT isn't queried twice |
| `engine2/` | Title Leads engine: index builder, owner parser, signals, scoring, scan |
| `data/title/` | Title-scan output shards (per county + statewide top) |
| `static/js/title.js`, `static/js/aerial.js` | Title Leads tab and the free aerial/street imagery panel |
| `app.py`, `database.py` | Legacy Flask/SQLite server — optional, for running locally |

## Run locally

```bash
pip install -r requirements.txt
MD_OPENDATA_USERNAME=you@example.com MD_OPENDATA_PASSWORD=... python pipeline.py
python -m http.server 8000   # then open http://localhost:8000
```

## Engine 2 — Title Leads (statewide, property-first)

The second tab on the dashboard. Instead of starting from a death, it starts from
every parcel in Maryland and looks for ownership that appears unresolved.

Weekly (Mondays, or **Actions → Title Scan (Engine 2) → Run workflow**):

1. `engine2/build_index.py` pulls all ~2.4M SDAT parcels (slim columns) into a
   local SQLite index. Cached for the week; never committed.
2. `engine2/title_scan.py` classifies every owner (individual, co-owners, estate,
   trust, LLC, government…), flags title signals, scores, and writes
   `data/title/<county>.json` (top 400 per county), `data/title/top.json`
   (statewide top 1000) and `data/title/summary.json`.
3. It also backfills lat/lon + deed liber/folio onto obituary leads so both tabs
   get the aerial panel.

Signals used today (all from SDAT, no death or probate lookup):

| Flag | Meaning |
|---|---|
| `ESTATE_IN_NAME` / `DECEASED_IN_NAME` / `PERSONAL_REP` | Decedent's estate is still the titled owner (class **E1**) |
| `HEIRS_IN_NAME` | "Heirs of …" on title — unprobated inheritance (**E2**) |
| `LIFE_ESTATE` | Remainder interest pending (**E3**) |
| `SURVIVING` | Surviving co-owner noted (**E4**) |
| `STALE_OWNERSHIP` | No transfer in 12+ years; **S1/S2/S3** at 12/25/40 yrs |
| `ABSENTEE`, `NO_HOMESTEAD`, `VACANT_LAND`, `OLD_STRUCTURE`, `POOR_CONDITION`, `MAIL_MISMATCH` | Distress indicators |

Scores: **Title Complexity**, **Financial**, **Distress**, blended into
**Research Priority**. Every point has a reason shown in the lead modal. Weights
and thresholds live in `engine2/config.py`.

**Death detection (automated, 1973–2014):** every individual owner is matched
against the Maryland State Archives death index (MSA SE-151, 1.65M deaths,
public domain via Reclaim The Records; `data/death/`). Each match carries a
separate *identity confidence* (name rarity, middle initial, generational
suffix, county of death, age vs. deed date; deeds recorded after the death are
excluded). Classes **D1** deceased sole owner still on title, **D4** deceased
co-owner, **D5** all owners deceased. Deaths after 2014 are not in the index —
the daily obituary scrape covers new deaths going forward.

**Not yet automated:** probate status. The Register of Wills estate search
prohibits commercial use without written permission (request pending); it is a
pluggable provider slot.

Imagery: Maryland iMAP 6-inch aerials (2020 west / 2022 Eastern Shore) and USGS
NAIP, plus Street View / Maps / Mapillary links — no API keys, no billing.

## Data sources

- Obituaries: Legacy.com (public listings)
- Property: Maryland SDAT Real Property Assessments via opendata.maryland.gov — dataset `9xq5-z8s2` (owner names, login required). The public `ed4q-f8tm` dataset has the same fields with owner names removed.

Property records are public information in Maryland. Comply with Legacy.com's terms and applicable law when using this data commercially.
