# Handoff: how this tool is built, and how to build it for another state

Written so a fresh conversation can pick this up cold. Read this, then
`README.md`, then the files it points at. Everything here was built in
September 2026 for Maryland; the second half is a starting brief for
Virginia (King George County first).

## What the product is

A static dashboard (GitHub Pages) fed by scheduled GitHub Actions jobs that
read **public records** and turn them into four lists of motivated sellers:

| Tab | Source | Signal |
|---|---|---|
| Recent Death Leads | obituaries, daily | someone died this week and owned real estate |
| Title Leads | statewide property database + death index + estate records, weekly | the owner on title is dead and title never moved |
| Surplus Funds Leads | county tax-sale results, monthly + history | lien sold; owner still holds title (buy) / deed conveyed (surplus owed) / overbid (lien stranded) / repeat sales |
| Failed Foreclosures | property database grantor + owner | tax-sale foreclosure collapsed and the locality took title |

No paid data, no scraping of sites whose terms forbid it. Budget is $0.

## Architecture (all in this repo)

```
scraper.py / pipeline.py / property_lookup.py   Engine 1: obituaries -> property matches -> data/leads.json
engine2/build_index.py      weekly: 2.4M-parcel SDAT index -> .cache/sdat.sqlite (cached per ISO week, never committed)
engine2/title_scan.py       weekly: every parcel scored and classified -> data/title/*.json
engine2/death_index.py      MSA death index 1973-2014 (public domain) -> identity matching with confidence
engine2/probate_import.py   manual Register of Wills import (terms forbid automation) -> data/probate/
engine2/taxsale_harvest.py  monthly: RealAuction portals live + Internet Archive back to 2021 -> data/distress/taxsale-<yr>.json
engine2/county_files.py     parsers for counties that publish results as PDFs (AA, PG, MoCo, Washington, Calvert, Caroline)
engine2/surplus.py          weekly: tax-sale outcomes -> data/surplus/{leads,vested,watch,events}.json
engine2/score.py, signals.py, owner_parse.py, config.py   scoring, flags, name classification, every tunable
index.html + static/js/{dashboard,title,surplus,vested,aerial}.js   the four tabs; status/notes in localStorage
guide.html                  the field guide (plain-English, per-tab, with phone scripts)
.github/workflows/          daily-scrape (6 AM ET), title-scan (Mon 4 AM ET), taxsale-harvest (2nd of month), probate-import (manual)
docs/                       tax-sale-sources.md (per-county table), surplus-mpia-request.md, register-of-wills-request.md
```

Credentials (Socrata app token/login for the licensed SDAT dataset) are
GitHub Actions secrets only. Nothing sensitive reaches the frontend.

## Ideas that transfer to any state

1. **Property-first, not person-first.** Index every parcel in the
   jurisdiction, then run signals over it. Obituaries alone miss 90% of
   the dead-owner inventory because most deaths happened years ago.
2. **Classification ladder.** D-classes (a death or estate record proves
   it) beat E-classes (owner text says "ESTATE OF") beat S-classes (deed
   is just old). Keep *death confidence* separate from *identity
   confidence* (is this decedent actually the owner on title?).
3. **Hard vs soft distress.** Verified county records (tax sale, vacancy
   notice, condemnation) are capped at 100; inferential hints (absentee,
   no homestead, old structure) are capped at 30 combined. Never let soft
   signals impersonate hard ones.
4. **The dispositive event, not the paperwork.** Court dockets are evidence;
   the *deed* is the event. A change of owner on the property database
   proves a foreclosure completed without touching court systems whose
   terms forbid automation. Snapshot owners weekly and diff; a diff needs
   a baseline, so start snapshotting on day one.
5. **Owner-at-sale vs owner-now.** When a tax-sale list names the owner, a
   different name on title later (with a deed after the sale, or the
   bidder now holding title) is a conveyance. Exclude the owner's own
   trust/LLC (shared surname) and lenders (mortgage foreclosure).
6. **Bid vs value, not bid vs taxes.** A bid far above assessed value will
   never be paid, so no deed and no surplus; but it means the owner keeps
   the property with the lien holder stranded. Different lead, not a dead
   row. Repeats (same parcel sold in multiple years) are the strongest
   buy signal on the board.
7. **Nothing is really gone.** County sites keep prior years at
   year-derived URLs more often than you'd think (PG, Anne Arundel did);
   the Wayback Machine CDX index covers the rest. Fetch from GitHub
   Actions, not from a sandbox with an egress proxy.
8. **Only show opportunities.** Watch states (too early, redeemed, no
   surplus possible) stay out of the UI. Honest labels ("Needs Verifying")
   beat confident ones the data can't support, because the action they
   invite is telling someone about their house.
9. **Automation cadence that matches reality.** Obits daily; property
   index weekly; tax-sale lists monthly (sales are seasonal); manual
   imports where terms require a human. Keep scheduled runs infrequent
   to save credits.
10. **Legal guardrails in the product, not a memo.** Modal "Next Step" text
    says what to do and what not to do (don't buy surplus claims without
    a lawyer; verify the lien is still open before calling an owner).

## What is Maryland-specific and must be replaced

- **SDAT** (statewide assessment database on Socrata) -- Virginia has no
  statewide equivalent; it's locality by locality.
- **Account number scheme** (county code + district + account).
- **Tax *lien* sale model** with 6-month wait, 2-year certificate void,
  redemption until judgment, high-bid premium, residue on credit
  (Tax-Property Art. 14-8xx). Virginia does not sell liens at all.
- **Register of Wills** as the probate office and the estate-search site.
- **MSA SE-151 death index** (MD State Archives, public domain via Reclaim
  The Records).
- **RealAuction** `<county>.marylandtaxsale.com` portals.
- The 24 jurisdiction list, sale-date table, PDF parsers.

## Virginia: starting brief (verify every line before building on it)

Virginia is a **judicial tax sale / deed sale** state, not a lien state.
The locality (through its treasurer, usually via outside counsel) files a
bill in circuit court once taxes are delinquent long enough, a special
commissioner sells the property at public auction, and the buyer gets a
deed. There is **no post-sale redemption**; the owner can stop it only by
paying in full before the sale. Code of Virginia Title 58.1, Chapter 39,
Article 4 (58.1-3965 and following) is the place to start; there is also a
nonjudicial sale path for low-value parcels (around 58.1-3975).

What that changes in the product:

- **"Buy Window" = between suit filed and auction date.** That is the whole
  window. Once sold, it's gone. The lists of parcels scheduled for sale
  are the core feed.
- **Surplus is real and simpler.** Sale proceeds above taxes, costs and
  liens go to the former owner (check 58.1-3967/3969 for the mechanics
  and what happens to unclaimed money -- likely Virginia's unclaimed
  property division after a period). Confirm whether Virginia regulates
  surplus-recovery agreements before offering anything beyond pointing
  the owner at the claim process.
- **No "lien stranded" or "repeat" stages** in the Maryland sense; think
  instead about *parcels that appear on a sale list and are then pulled*
  (owner paid under pressure) -- those owners are stressed and solvent.
- **A Virginia-only signal worth building first:** a **List of Heirs**
  (Va. Code 64.2-509) is filed with the circuit court clerk when someone
  dies owning real estate, and it names the heirs. It is a public record
  indexed in the land records. That is the dead-owner-with-named-heirs
  lead the Maryland tool has to triangulate from three sources.
- **Probate** is handled by the **circuit court clerk**, not a separate
  office. Qualification of an executor/administrator is a clerk record.

Sources to evaluate for King George County (free first):

- King George County Treasurer and Commissioner of the Revenue pages:
  delinquent lists, scheduled tax sales, and the parcel/GIS viewer (the
  real-property data likely sits in a vendor system -- find which, and
  whether it has a bulk export or a documented API).
- **TACS** (Taxing Authority Consulting Services) runs judicial sales for
  many Virginia localities and posts upcoming sales with parcel lists;
  check whether King George uses them or another auctioneer.
- **Virginia circuit court case information** (online case system) --
  read its terms before any automation; expect the same manual-lookup
  pattern as Maryland's Register of Wills.
- **Land records** are the clerk's "Secure Remote Access" (subscription,
  per-clerk). Probably not free; budget $0 means manual pulls on specific
  deeds only.
- **Obituaries**: same approach as Maryland; the matching target is the
  county's own parcel data rather than a statewide database.
- **Virginia unclaimed property** (Treasury) search for escheated surplus.
- **Death records**: Virginia vital records are restricted for recent
  deaths; don't plan on a free statewide death file.

Build order that worked here and should work there: (1) get the parcel
data and owner names for the whole county into a SQLite index on a
schedule; (2) stand up the dashboard with one real tab; (3) add the
tax-sale list feed and the List of Heirs feed; (4) only then score and
classify; (5) write the field guide last, from real cases.

## King George County: what was verified on 2026-10-01 (start here)

**Parcel database (the SDAT equivalent) — free, bulk, no terms problem.**
King George publishes its full CAMA assessment table as ArcGIS open data:

    https://services2.arcgis.com/S8zMJrpz61FbvL5t/arcgis/rest/services/Parcels/FeatureServer/0/query
    (hub: https://data-king-george.opendata.arcgis.com/ — "Parcels", updated daily; also "Addresses", "TaxIndex")

15,056 parcels, 2,000 per page, 8 requests for the whole county. Useful
fields: PIN (the parcel id, e.g. "33       130E" — keep the internal
spaces, there is also PINNOSPACE), LNAM/FNAM (owner), ADD1/CITY/STATE/ZIP5
(mailing), PHYSICALAD, TOTLD/IMPRV/TOTPR (land/improvements/total value),
YRBLT, OCCUP, COND, ACRE, DESC1, DBOOK/DPAGE (deed), WBOOK/WPAGE (will
book — title passed by will), GRNTR, SELLP/YRSLD. CVYR is the assessment
year, not a conveyance year — do not use it as transfer date.

Deed recency proxy: DBOOK is mixed-format. Values under ~1000 are old
book numbers (decades old). Nine-digit values are instrument numbers
YYNNNNNNN, so 250001786 is a 2025 deed — the year is the first two digits.

First-pass counts on the live data:
- 126 parcels whose owner text says ESTATE / HEIRS / LIFE ESTATE / DECEASED
  (e.g. "ROBERT MILTON CURRY JR ESTATE", "NORRIS C W ESTATE" on deed book 30)
- 175 more with ET AL and no estate word (co-heirs)
- 97 with a will-book reference on the parcel (inherited title)
- ~3,000 with a deed book number under 300 (very old title)
- 3,242 whose owner mails outside the county (absentee)
- 797 trustee-held (living trusts — mostly NOT estates; score separately)

**Delinquency (per parcel, the piece the open data lacks).**
Treasurer e-services, ASP.NET webforms behind a click-through legal
statement that is a liability/SSN disclaimer with NO prohibition on
automated or commercial use (read it; it is quoted nowhere else):

    https://eservices.kinggeorgecountyva.gov/applications/REPublicInquiry/webform1.aspx   (inquiry — use this)
    https://eservices.kinggeorgecountyva.gov/applications/PayREtax/webform1.aspx           (payment — avoid)

Flow: GET webform1 -> POST __VIEWSTATE etc. + ctl00$MainContent$btnAccept
-> it redirects to a second disclaimer (/applications/TRdisclaimer/) ->
accept again -> the inquiry form. Keep one session. Walk the 15,056 PINs
at a polite rate (one request per 1-2 s, one pass per month, overnight) —
small county, small server. Store balance due, years delinquent, and the
owner name the Treasurer has (it can differ from the GIS owner, which is
itself a signal).

**Entities.** For LLC/INC owners, the Virginia SCC Clerk's Information
System (cis.scc.virginia.gov) shows status (active / cancelled /
terminated) and the registered agent and officers. Check its terms before
automating; a cancelled entity still on title is a stuck-title lead in its
own right, and the officer names feed the death match.

**Death match.** No free statewide death file for recent years. Use
obituaries (same scraper, point it at King George / Fredericksburg /
Dahlgren / Colonial Beach papers and funeral homes) matched against the
GIS owner names; and the will-book / ESTATE / HEIRS fields above, which
are the county telling you directly.

**Build order for King George:** (1) nightly pull of the Parcels layer into
SQLite; (2) dashboard with a Title tab from the owner-text / will-book /
old-deed-book signals — that alone is ~400 leads on day one; (3) monthly
delinquency walk via REPublicInquiry, joined by PIN; (4) obituary matching;
(5) SCC entity status; (6) tax-sale notices (judicial sale lists from the
Treasurer / the county's auction counsel) for the pre-auction buy window.
