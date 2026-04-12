# MD Property Leads

**Maryland Probate Real Estate Lead Generator**

Automatically scrapes daily obituaries from Legacy.com for Maryland, cross-references deceased individuals against MD SDAT (State Department of Assessments and Taxation) property records, and surfaces leads where the deceased owned property â giving you first-mover advantage on potential estate sales.

![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Flask](https://img.shields.io/badge/Flask-3.0-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## Features

- **Automated Daily Scraping** â Scheduled scraper runs at 6:00 AM daily via APScheduler
- **Smart Cross-Referencing** â Matches obituary names against MD SDAT property ownership records
- **Property-Only Leads** â Only surfaces leads where the deceased actually owned property
- **Cyberpunk Dashboard** â Dark neon UI with real-time stats, search, filtering, and sorting
- **Lead Management** â Track lead status (New â Hot â Contacted â Closed) with notes
- **Skip Tracing Export** â Download leads as properly formatted CSV for skip tracing tools
- **Manual Scrape Trigger** â Run a scrape on demand from the dashboard

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/md-property-leads.git
cd md-property-leads
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the app

```bash
python app.py
```

### 4. Open in browser

Navigate to **http://localhost:5000**

## How It Works

1. **Scrape**: Pulls recent obituaries from Legacy.com across 28+ Maryland cities/regions
2. **Parse**: Extracts name, date of death, age, survived-by info from obituary listings
3. **Lookup**: Searches MD SDAT database for property records matching the deceased's name
4. **Match**: If the deceased owned property in Maryland, a lead is created
5. **Display**: Leads appear on the dashboard with property details, assessed values, and county info

## Skip Tracing Export Format

The CSV export includes all fields needed for standard skip tracing:

| Field | Description |
|-------|-------------|
| First/Last/Middle Name | Deceased's parsed name |
| Date of Death/Birth | Key dates |
| Property Address | Full property address from SDAT |
| County | Maryland county |
| Assessed Value | Current assessed value |
| Land/Improvement Value | Value breakdown |
| Year Built | Property age |
| Survived By | Potential heirs from obituary |
| Obituary URL | Link to full obituary |

## Configuration

Edit the scrape schedule in `app.py`:

```python
scheduler.add_job(
    run_scrape_pipeline,
    trigger="cron",
    hour=6,      # Change hour (0-23)
    minute=0,    # Change minute (0-59)
)
```

## Tech Stack

- **Backend**: Python 3.9+ / Flask
- **Scraping**: BeautifulSoup4 / Requests
- **Database**: SQLite
- **Scheduler**: APScheduler
- **Frontend**: Vanilla JS with CSS custom properties

## Legal Notice

This tool accesses publicly available data from Legacy.com and the Maryland SDAT. Ensure you comply with all applicable terms of service and local laws when using scraped data for commercial purposes. Property records are public information in the state of Maryland.

## License

MIT
