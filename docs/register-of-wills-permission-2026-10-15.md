# Register of Wills: written permission (received 2026-10-15)

Reply from **Website Administration, Office of the Register of Wills, State of Maryland** (registers.maryland.gov) to our request of 2026-10-15 (`docs/register-of-wills-request.md`). Received at another of Ray's email addresses and pasted here verbatim. Keep the original email, with headers, in that inbox.

---

Dear Mr. Ray,

Thank you for contacting our office regarding your request on behalf of Pages of Purpose LLC. We have reviewed both portions of your inquiry and provide the following response.

**PART 1 – REQUEST FOR WRITTEN PERMISSION: APPROVED**

Your request for limited automated access to the Maryland Register of Wills Estate Search system is approved, subject to the conditions outlined in your original correspondence.

Specifically, Pages of Purpose LLC is granted permission to:

* Conduct automated searches for individual decedents previously identified through publicly available property assessment records.
* Retrieve and retain publicly displayed estate information, including case numbers, estate status, filing dates, dates of death, personal representative names, and docket information.
* Recheck previously identified open estates no more than once every 30 days.
* Conduct a limited number of searches daily, with reasonable delays between requests to avoid placing unnecessary demands on the system.

This authorization applies exclusively to the limited activities described in your request and does not permit unrestricted scraping, bulk database extraction, redistribution of records, or activities that interfere with normal website operations.

Permission is granted for your stated business purpose and may be modified or revoked if these conditions are not followed.

**PART 2 – PUBLIC INFORMATION ACT REQUEST: DENIED**

Regarding your request for statewide electronic lists of estates opened and closed from January 1, 2025, to the present, and for continuing monthly reports, we are unable to fulfill this request as submitted.

Our office does not maintain a precompiled statewide spreadsheet or CSV report containing the requested information in the format described.

The Maryland Public Information Act provides access to existing public records but does not require an agency to create new records or establish an ongoing reporting service in response to a request.

Accordingly, your request for the proposed statewide compilation and recurring monthly reports is denied as submitted.

Individual estate records remain available for public inspection through the Estate Search system at https://registers.maryland.gov.

You may submit a revised request identifying specific existing records maintained by our office for further consideration.

**ADDITIONAL INFORMATION**

With respect to your inquiry about legal notices, published estate notices remain available through their existing public publication channels. Our office does not currently offer a downloadable public feed for this purpose.

Thank you for your cooperation and for taking the initiative to request authorization before implementing automated access.

Sincerely,
Website Administration
Office of the Register of Wills
State of Maryland
https://registers.maryland.gov

---

## How the tool honors it

`engine2/probate_lookup.py` + `.github/workflows/probate-lookup.yml`, limits in `engine2/config.py` (`ROW_*`):

| Condition | Implementation |
|---|---|
| Only decedents identified from property records | Candidates are SDAT owners with a MEDIUM/HIGH death-index match; confirmed-alive overrides skipped |
| One search per person | Searches keyed by name + date of death; a person on several parcels is searched once |
| Open estates re-checked at most every 30 days | `ROW_RECHECK_OPEN_DAYS = 30`, re-reads the record page directly (no new search) |
| Limited daily volume, reasonable delays | 30 people / 90 requests per run, 6 s between requests, one run a day at 2:41 AM ET |
| Identifies itself | User-Agent names Pages of Purpose and mrdustinray@gmail.com |
| No bulk extraction / redistribution | No statewide sweeps; data stays in this private tool |
