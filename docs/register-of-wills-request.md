# Request for written permission — Register of Wills Estate Search / Legal Notices

Send from your business email. Two copies:

1. **Email** to your local Register at **stmarysrow@registers.maryland.gov** (St. Mary's County; 301-475-5566), subject line below. Ask them to forward it to whoever administers the statewide website if that is not them. The site's own "Contact Web Manager" link (footer of registers.maryland.gov) is the second address to CC.
2. **Mail** a signed copy to: Register of Wills, St. Mary's County, PO Box 602, Leonardtown, MD 20650.

Dated copies of both go in the repo under `docs/` once sent, so there is a record if anyone asks.

---

**Subject:** Request for written permission — limited automated use of Estate Search and Legal Notices data

Dear Register of Wills / Website Administrator,

I am the owner of Pages of Purpose LLC, a Maryland real estate company based in Charlotte Hall (St. Mary's County). We buy homes directly from owners and heirs, often properties that have sat in a deceased owner's name for years because no estate was opened or an estate stalled.

I am writing to request written permission, as required by the website's Terms of Use, to use data from the Estate Search and the Legal Notices search at registers.maryland.gov for that business purpose, in a limited and automated way. Specifically:

**What I am asking to do**

- Look up individual decedents by name, at most one query per person, only after a person has already been identified from public property-assessment records (SDAT) as a possible deceased owner.
- Record the public case fields the search already displays: estate number, county, estate type, status, filing date, date of death, personal representative name, and docket dates.
- Re-check an open estate no more than once every 30 days to see whether it has closed.

**What I will not do**

- No bulk downloading, no crawling of the full database, no repeated statewide sweeps.
- Total volume of a few dozen queries per day, at off-peak hours, with pauses between requests, well below what a single staff user generates in a busy afternoon.
- Requests will identify themselves with a contact email in the User-Agent header so your staff can reach me or block them at any time.
- No resale, republication, or redistribution of the data. It is used only to decide whether to send a letter to an estate's personal representative or heirs.

**Alternative I would gladly accept instead**

If automated access to the website is not something you grant, I would appreciate any of the following:

- A periodic extract (monthly is fine) of newly opened and newly closed estates statewide, in any format, under the Public Information Act. I am happy to pay reasonable reproduction costs and to sign any acceptable-use agreement.
- Guidance on whether the Legal Notices published under the 2024 estate-notice law are available as a feed or download, since those notices are published specifically for public dissemination.

I understand the site's disclaimers about accuracy and timeliness and will treat the data accordingly. I am glad to answer questions, provide the exact query pattern, or adjust any of the above to what your office is comfortable with.

Thank you for your time and for maintaining a public resource that makes estate information accessible.

Respectfully,

Dustin Ray
Owner, Pages of Purpose LLC
9945 Bowling Drive, Charlotte Hall, MD 20622
mrdustinray@gmail.com
pagesofpurposellc.com

---

## If they say yes

The reply is the authorization. Save it under `docs/`, then the `RegisterOfWillsProvider` (probate provider interface, Phase 4) gets enabled with the exact limits promised above hard-coded in `engine2/config.py`.

## If they say no or don't answer in 30 days

File a Public Information Act request for the extract with the same office (Maryland PIA, General Provisions Art. §4-101 et seq.). The website request above already contains the language; resend it titled "Public Information Act Request" and name the records: "electronic list of estates opened and estates closed statewide during [period], with decedent name, date of death, estate number, county, estate type, status, filing date, and personal representative name."
