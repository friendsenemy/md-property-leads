# MPIA request: tax-sale surplus balances

## Why this record has to exist

Md. Code, Tax-Property Art. **§ 14-818(a)(4)–(6)** puts three obligations on every
county collector:

- **(a)(4)** any balance of the purchase price above taxes, interest, penalties and
  costs of sale "shall be paid by the collector to the person entitled to the
  balance," or into court if there is a dispute;
- **(a)(5)** each county "shall establish a process" for an entitled person to claim
  the balance, applied uniformly, and **may not require a court order** unless the
  payment is disputed;
- **(a)(6)** within **90 days after delivering a deed**, the collector "shall notify
  the prior property owner of record" of the amount of the balance and of the
  claim process.

A collector cannot satisfy (a)(6) without maintaining a list of deeds delivered,
the balance on each, and who was notified. That list is the record we want, and
it is the only source that answers **"has it been claimed yet"** — nothing in the
land records or SDAT shows whether the money moved.

Records of a county collector are public records under the **Maryland Public
Information Act**, Gen. Prov. Art. Title 4. The custodian must grant or deny
within **30 days** (§ 4-203), must tell you in writing within **10 working days**
if it will take longer, and **may not charge for the first 2 hours** of search and
preparation time (§ 4-206). Keep each request inside that two hours and it is free.

## The letter

Paste as-is, fill the four brackets. Send by email to the collector's office
(the address on that county's tax-sale page) and copy the county attorney if the
tax-sale page names one.

---

**Subject: Maryland Public Information Act request — tax sale surplus balances, § 14-818(a)(6)**

[Collector of Taxes / Director of Finance]
[County], Maryland

Dear Custodian of Records:

Under the Maryland Public Information Act, Md. Code, General Provisions Article,
§ 4-101 et seq., I request copies of the following records of the collector for
tax years **2021 through the present**:

1. Any list, register, log, spreadsheet or database export of properties sold at
   tax sale for which a **deed has been delivered to the purchaser**, showing for
   each: the property account number, the date the deed was delivered, the
   purchase price, and the amount of taxes, interest, penalties and costs of sale
   applied against it.

2. For each such property, the **amount of any balance** over the amount required
   for payment of taxes, interest, penalties and costs of sale, as described in
   Tax-Property Article § 14-818(a)(4).

3. Records showing, for each such balance, **whether it has been claimed or
   disbursed**, the date of any disbursement, and whether the balance was paid
   into a court of competent jurisdiction under § 14-818(a)(4)(ii).

4. A copy of the **written process** the county has established under
   § 14-818(a)(5) for a person entitled to a balance to claim it, including any
   claim form and instructions.

5. A copy of the **form of notice** the collector sends to prior property owners
   of record under § 14-818(a)(6).

I am not requesting the notices themselves or any personal contact information
contained in them. If any part of item 1, 2 or 3 is withheld, please release the
remainder and cite the specific statutory exemption for each withheld field, as
§ 4-203(c) requires.

Electronic copies in their existing format — CSV, Excel or a database export —
are preferred and satisfy this request in full. Please advise before incurring
any fee beyond the two hours of search and preparation time that § 4-206(c)
exempts from charge, and I will narrow the date range rather than incur a fee.

Thank you.

[Your name]
Pages of Purpose LLC
[address]
[email / phone]

---

## Where to send it

One request per jurisdiction. Baltimore City's collector is the Director of
Finance; the counties are the Treasurer or Director of Finance depending on the
county. Take the exact office and address from each county's own tax-sale page.

Allegany · Anne Arundel · Baltimore City · Baltimore County · Calvert · Caroline ·
Carroll · Cecil · Charles · Dorchester · Frederick · Garrett · Harford · Howard ·
Kent · Montgomery · Prince George's · Queen Anne's · St. Mary's · Somerset ·
Talbot · Washington · Wicomico · Worcester

**Send one first.** Pick a county with a small tax sale and a responsive finance
office — Calvert, Kent, Queen Anne's or Talbot — and see what comes back before
spending 24 requests. The first response tells you what these records actually
look like, which is what the parser should be built against.

The **Office of the State Tax Sale Ombudsman** (DAT, sdat.taxsale@maryland.gov)
is worth one separate request asking whether it holds any statewide compilation
of § 14-818(a)(5) county claim processes. One answer there could save 24 letters.

## What to do with the responses

`engine2/surplus.py` already tracks which parcels conveyed. When a county's list
arrives, it supplies the two fields the SDAT diff cannot: the **exact balance** and
**whether it has been claimed**. Same pattern as the Register of Wills import —
the lookup is manual, the parsing and storage are not. Send me a response and I
will write the importer for that county's format.
