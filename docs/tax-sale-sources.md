# Maryland 2026 tax-sale sources (free, public, no login)

Harvested 2026-09-28 into `data/distress/taxsale-2026.json` (11,433 records, keyed by SDAT account).
Statuses: `SOLD` lien sold to investor · `STRUCK` unsold, county holds lien · `LISTED` advertised, outcome unknown · `LISTED_NOT_SOLD` advertised but absent from published results (likely redeemed pre-sale).

SDAT account = county code + the county's parcel id, zero-padded: most counties `CC + DD + 6`; Anne Arundel `02 + 13`; Prince George's `17 + DD + 7`; Baltimore County `04 + DD + 10`; Montgomery `16 + DD + 8` (results give only the 8-digit account → matched by suffix); Baltimore City uses block/lot (not mapped yet).

| County | Sale 2026 | What is public | Where |
|---|---|---|---|
| Allegany | May 28 | results (portal) + unsold OTC xlsx | allegany.marylandtaxsale.com/index.cfm?folder=auctionResults&mode=preview ; gov.allconet.org/DocumentCenter/View/10955 |
| Anne Arundel | Jun 3 | results PDF, remaining liens PDF | aacounty.org/sites/default/files/2026-06/2026-tax-sale-results.pdf |
| Baltimore City | May 18 | pre-sale newspaper ad only (block/lot prose) | thedailyrecord.com/files/2026/03/TDR_20260325_C_1-64.pdf |
| Baltimore County | Aug 27 | final advertised list xlsx (results not posted yet) | bcg-prod.baltimorecountymd.gov/.../taxadvertisingfile4.xlsx |
| Calvert | May 22 | results PDF (bid = face ⇒ struck) | calvertcountymd.gov/DocumentCenter/View/57137 |
| Caroline | Aug 21 | eligible list PDF + results PDF | carolinemd.org/DocumentCenter/View/12329 and /12385 |
| Carroll | Jun 29–30 | results (portal + PDF) | carroll.marylandtaxsale.com ; carrollcountymd.gov/media/o05nvhg1/fy-26-tax-sale-results.pdf |
| Cecil | Jun 1 | advertised list PDF, results (portal), county-owned liens PDF | cecilcountymd.gov/DocumentCenter/View/5971 ; cecil.marylandtaxsale.com |
| Charles | May 12 | results (portal) | charles.marylandtaxsale.com (auctionResults preview) |
| Dorchester | May 19 | advertising list PDF, results PDF + portal | dorchestermd.gov/wp-content/uploads/2026/05/2026-TAX-SALE-RESULTS.pdf |
| Frederick | May 11 | results by property PDF + portal | frederickcountymd.gov/Archive.aspx?ADID=16906 |
| Garrett | May 18–22 | results (portal) | garrett.marylandtaxsale.com |
| Harford | Jun 3 | results PDF + portal (with addresses) | harfordcountymd.gov/DocumentCenter/View/39099 |
| Howard | Jun 10 | property list CSV (portal download) + results xlsx | taxsale.howardcountymd.gov/Public/Propertylist.aspx ; howardcountymd.gov/finance/resource/2026-tax-sale-results |
| Kent | May 21 | results (portal) + OTC PDF | kent.marylandtaxsale.com ; kentcounty.com/Documents/Finance/… OTC 6.9.26.pdf |
| Montgomery | Jun 8 | results PDF (bidder + 8-digit account only) | assets.montgomerycountymd.gov/files/2026-06/tax_lien_sale_result_2026.pdf |
| Prince George's | May 11 | results PDF + assignment list | taxsale.princegeorgescountymd.gov/2026Taxsaleresults.pdf |
| Queen Anne's | May 19 | results PDF + portal (with addresses) | qac.org/DocumentCenter/View/25650 |
| St. Mary's | Mar 6 | pre-sale list xlsx | stmaryscountymd.gov/docs/FINAL_2026_LIST.xlsx |
| Somerset | Jun 11 | assignment list PDF only (3 parcels) | cms7files1.revize.com/somersetcountymd/ASSIGNMENT LIST 2026 07 21 26.pdf |
| Talbot | May 20 | results PDF + portal | talbotcountymd.gov/uploads/File/finance/2026_Tax_Sale/2026_TAX_SALE_RESULTS.pdf |
| Washington | Jun 2 | newspaper ad PDF + results PDF | washco-md.net/wp-content/uploads/Newspaper-2026-AD.pdf and /2026-Tax-Sale-Results.pdf |
| Wicomico | Jun 9 | newspaper list PDF + OTC PDF (no sold list) | wicomicocounty.org/DocumentCenter/View/14099 and /14387 |
| Worcester | Jun 9 | results (portal, with addresses) + OTC PDF | worcester.marylandtaxsale.com ; worcestermd.gov/sites/default/files/2026-Over-Counter060226.pdf |

RealAuction portals (`<county>.marylandtaxsale.com`) return 403 to a bare client but serve `auctionResults&mode=preview` publicly with a browser User-Agent, 50 rows/page (POST `pageNum`). Rows carry SDAT deep links (county/district/account) and, for some counties, the situs address.

Redemption: none of these files show post-sale redemption. Maryland owners may redeem until the lien holder forecloses the right of redemption (earliest 6 months after sale; 9 in some cases). Per-account status needs the county's tax inquiry page.

Next adapter step: `engine2/adapters/taxsale.py` that re-pulls these each summer, keeps prior years for `TAX_SALE_REPEAT`, and adds Baltimore City via block/lot → SDAT mapping.
