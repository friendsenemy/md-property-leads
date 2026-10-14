# Public Information Act requests (free data the portals don't publish)

Maryland's Public Information Act (General Provisions Article §4-101 et seq.) lets anyone
ask a county for its records. There is no fee for the first two hours of search/prep time
(§4-206(c)), and an electronic export of an existing database usually falls inside that.
Send by email, keep a copy, and note the date — the agency has 10 working days to respond
and 30 days to produce (§4-203).

Both letters below are ready to send as-is. Fill in nothing but the date.

---

## 1. Baltimore County — code-enforcement case export

**To:** Baltimore County Department of Permits, Approvals and Inspections — Code Enforcement
**Email:** PAI@baltimorecountymd.gov (cc: the County's PIA representative, piarequest@baltimorecountymd.gov)
**Subject:** Public Information Act request — code enforcement case list (electronic)

> Dear Custodian of Records,
>
> Under the Maryland Public Information Act, General Provisions Article §4-101 et seq., I request
> an electronic copy of the Department's code-enforcement case list for cases opened from
> January 1, 2020 to the present, including for each case: case number, case type / violation
> category, property address, tax account or parcel number, date opened, current status, date
> closed (if closed), and whether the case is marked vacant, unsafe, condemned, or referred for
> demolition.
>
> I am requesting the data in its existing electronic form (CSV, Excel, or a database export),
> not paper copies. I do not need inspector notes, photographs, or complainant information, and
> I ask that any personal information of complainants be withheld rather than used as a reason
> to deny the request.
>
> This is the same information the County's CitizenAccess portal shows one case at a time; I am
> asking for it in bulk so it can be analyzed as public data. Under §4-206(c) I ask that the first
> two hours of search and preparation be provided without charge. If you estimate that fees will
> exceed $50, please contact me before incurring them.
>
> If any part of this request is denied, please cite the specific exemption relied on and release
> all reasonably severable portions, as §4-203 requires.
>
> Please send the records to mrdustinray@gmail.com.
>
> Thank you,
> Dustin Ray
> Pages of Purpose LLC
> 9945 Bowling Drive, Charlotte Hall, MD 20622

---

## 2. Charles County — code case violation descriptions

The Civic Access portal shows every code case's number, type, status, address and parcel, but
the "Violations" tab is restricted to contacts on the record. The violation text (what the
inspector actually cited) is a public record in the County's possession and can be requested.

**To:** Charles County Department of Planning & Growth Management — Codes, Permits & Inspection Services
**Email:** PGMinfo@charlescountymd.gov (cc: the County Attorney's PIA representative, PIA@charlescountymd.gov)
**Subject:** Public Information Act request — code case violation list (electronic)

> Dear Custodian of Records,
>
> Under the Maryland Public Information Act, General Provisions Article §4-101 et seq., I request
> an electronic export from the County's EnerGov system of code-enforcement cases opened from
> January 1, 2020 to the present, including for each case: case number, case type, status, date
> opened, date closed, property address, parcel number, and each violation cited on the case
> (code section and short description, e.g. "2021 IRC 105.1 – Construction without a Permit",
> "unsafe structure", "vacant / unsecured", "accumulation of trash and debris").
>
> I am requesting the data in its existing electronic form (CSV, Excel, or a database report),
> not paper copies, and not inspector narratives, photographs, or complainant information.
>
> The case list itself is already public on the County's Civic Access portal; this request is only
> for the violation field that the portal restricts to case contacts. Under §4-206(c) I ask that the
> first two hours of search and preparation be provided without charge, and that you contact me
> before incurring any fee above $50.
>
> If any part of this request is denied, please cite the specific exemption and release all
> reasonably severable portions, as §4-203 requires.
>
> Please send the records to mrdustinray@gmail.com.
>
> Thank you,
> Dustin Ray
> Pages of Purpose LLC
> 9945 Bowling Drive, Charlotte Hall, MD 20622

---

## What to do with the reply

Save the file the county sends as `data/distress/pia/<county>-<date>.csv` and tell the tool;
the importer matches it on parcel number and the violation text becomes the signal detail.
A denial letter is also useful — it tells us exactly which field they consider exempt.
