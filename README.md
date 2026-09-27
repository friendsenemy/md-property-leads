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
| `app.py`, `database.py` | Legacy Flask/SQLite server — optional, for running locally |

## Run locally

```bash
pip install -r requirements.txt
MD_OPENDATA_USERNAME=you@example.com MD_OPENDATA_PASSWORD=... python pipeline.py
python -m http.server 8000   # then open http://localhost:8000
```

## Data sources

- Obituaries: Legacy.com (public listings)
- Property: Maryland SDAT Real Property Assessments via opendata.maryland.gov — dataset `9xq5-z8s2` (owner names, login required). The public `ed4q-f8tm` dataset has the same fields with owner names removed.

Property records are public information in Maryland. Comply with Legacy.com's terms and applicable law when using this data commercially.
