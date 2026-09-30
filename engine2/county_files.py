"""
Parsers for the counties that publish tax-sale results as files on their own
sites rather than on a RealAuction portal. Each returns {sdat_account: record}
in the same shape as the RealAuction parser.

URL templates take the sale year. Where a county keeps prior years live
(Prince George's, Anne Arundel) the template just works; where it does not,
the harvester retries the same URL through the Internet Archive.
"""

import io
import re
import logging

log = logging.getLogger("countyfiles")

MONEY = r"\$?\s*([\d,]+\.\d\d)"


def _m(s):
    try:
        return float(str(s).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return None


def _pdf_lines(data):
    import pdfplumber
    out = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            for ln in (page.extract_text() or "").splitlines():
                if ln.strip():
                    out.append(re.sub(r"\s+", " ", ln.strip()))
    return out


# ---------------------------------------------------------------- Anne Arundel
# LIST# glued to a 12-digit parcel; bidder number (4 digits) glued to bidder name.
# "8100003915206 JACKSON BRYAN P 1,258,633 1015ARCH PF LLC 706,722.00 8,002.66 40,653.76 48,656.42"
AA = re.compile(r"^(\d+?)(\d{12}) (.+?) ([\d,]+) (\d{4})(\S.*?) ([\d,]+\.\d\d) ([\d,]+\.\d\d) ([\d,]+\.\d\d) ([\d,]+\.\d\d)$")


def parse_anne_arundel(data, year, source):
    out = {}
    for ln in _pdf_lines(data):
        m = AA.match(ln)
        if not m:
            continue
        _, parcel, owner, assessed, bidder_no, bidder, bid, taxes, premium, total = m.groups()
        acct = "02" + parcel.zfill(13)
        out[acct] = {"county": "Anne Arundel", "status": "SOLD", "year": year, "source": source,
                     "owner": owner.strip(), "assessed": _m(assessed), "bid": _m(bid), "face": _m(taxes),
                     "premium": _m(premium), "bidder": bidder.strip(), "bidder_no": bidder_no}
    return out


# ------------------------------------------------------------- Prince George's
# The column order changes between years, so the header line is read first:
#   2025+: ... Assessed Bidder Winning Amt Taxes Bid Prem Total Due
#   2024:  ... Assessed Bidder Winning Amt Bid Prem Taxes Total Due
# District is 1-2 digits (zero-padded some years, not others); blanks are "$ -".
PG_ROW = re.compile(r"^\d+ \d+ (\d{1,2}) (\d{7}) (.+?) \$? ?([\d,]+(?:\.\d\d)?) (\d+) (.*)$")
PG_TAIL = re.compile(r"\$ ?(-|[\d,]+\.\d\d)")


def parse_prince_georges(data, year, source):
    out, order = {}, None
    for ln in _pdf_lines(data):
        if ln.startswith("Batch") and "Winning" in ln:
            hdr = ln.upper()
            order = ["winning", "taxes", "premium", "total"] if hdr.index("TAXES") < hdr.index("PREM") \
                else ["winning", "premium", "taxes", "total"]
            continue
        m = PG_ROW.match(ln)
        if not m or order is None:
            continue
        dist, parcel, owner, assessed, bidder, tail = m.groups()
        vals = [(_m(v) if v != "-" else None) for v in PG_TAIL.findall(tail)]
        cols = dict(zip(order, vals))
        bid, face = cols.get("winning"), cols.get("taxes")
        acct = "17" + dist.zfill(2) + parcel
        struck = bidder == "1" and bid is not None and face is not None and abs(bid - face) < 0.01
        out[acct] = {"county": "Prince George's", "status": "STRUCK" if struck else "SOLD", "year": year,
                     "source": source, "owner": owner.strip(), "assessed": _m(assessed), "bid": bid, "face": face,
                     "premium": cols.get("premium"), "bidder": "County" if struck else f"#{bidder}"}
    return out


# ------------------------------------------------------------------ Montgomery
# "BUFFALO ROSE LLC 10 01294987 0.5473"  -- bid is a factor of assessed value,
# not a dollar figure. Keyed 16*XXXXXXXX like the rest of the Montgomery data;
# the surplus watcher multiplies the factor by SDAT's assessment.
MC = re.compile(r"^(.+?) (\d{2}) (\d{8}) (0?\.\d{3,4})$")


def parse_montgomery(data, year, source):
    out = {}
    for ln in _pdf_lines(data):
        m = MC.match(ln)
        if not m:
            continue
        bidder, dist, acct8, factor = m.groups()
        out["16*" + acct8] = {"county": "Montgomery", "status": "SOLD", "year": year, "source": source,
                              "bid_factor": float(factor), "bidder": bidder.strip(), "district": dist}
    return out


# ------------------------------------------------------------------ Washington
# "28 03005240 1 Washington County Commissioner 29,392.30 - - 29,392.30"
WA = re.compile(r"^\d+ (\d{8}) (\d+) (.+?) ([\d,]+\.\d\d) (-|[\d,]+\.\d\d) (-|[\d,]+\.\d\d) ([\d,]+\.\d\d)$")


def parse_washington(data, year, source):
    out = {}
    for ln in _pdf_lines(data):
        m = WA.match(ln)
        if not m:
            continue
        parcel, bidder_no, bidder, sale_amt, total_bid, premium, total = m.groups()
        struck = "commissioner" in bidder.lower()
        rec = {"county": "Washington", "status": "STRUCK" if struck else "SOLD", "year": year, "source": source,
               "face": _m(sale_amt), "bidder": "County" if struck else bidder.strip()}
        if total_bid != "-":
            rec["bid"] = _m(total_bid)
        out["22" + parcel] = rec
    return out


# --------------------------------------------------------------------- Calvert
# "01-013947 GIBSON STEWART D 442 SOLLERS WHARF RD 117,367 4,508.88 10,000.00"
CV = re.compile(r"^(\d{2})-(\d{6}) (.+?) ([\d,]+) ([\d,]+\.\d\d) ([\d,]+\.\d\d)$")


def parse_calvert(data, year, source):
    out = {}
    for ln in _pdf_lines(data):
        m = CV.match(ln)
        if not m:
            continue
        dist, parcel, owner_loc, assessed, sale_amt, bid = m.groups()
        b, f = _m(bid), _m(sale_amt)
        struck = b is not None and f is not None and abs(b - f) < 0.01
        out["05" + dist + parcel] = {"county": "Calvert", "status": "STRUCK" if struck else "SOLD", "year": year,
                                     "source": source, "owner": owner_loc.strip(), "assessed": _m(assessed),
                                     "face": f, "bid": b, "bidder": "County" if struck else None}
    return out


# -------------------------------------------------------------------- Caroline
# "201-000918 $2,283.73 $11,385.36 $13,669.09 $ 125,500.00 13RS Assets LLC"   (seq glued to id)
# "101-000152 $365.80 UNSOLD"
CA = re.compile(r"^\d*?(\d{2}-\d{6}) \$([\d,]+\.\d\d) (?:UNSOLD|(?:\$[\d,]+\.\d\d ?)*\$ ?([\d,]+\.\d\d) (\d+)(.+))$")


def parse_caroline(data, year, source):
    out = {}
    for ln in _pdf_lines(data):
        m = CA.match(ln)
        if not m:
            continue
        pid, face, bid, bidder_no, bidder = m.groups()
        acct = "06" + pid.replace("-", "")
        if bid is None:
            out[acct] = {"county": "Caroline", "status": "STRUCK", "year": year, "source": source, "face": _m(face)}
        else:
            out[acct] = {"county": "Caroline", "status": "SOLD", "year": year, "source": source,
                         "face": _m(face), "bid": _m(bid), "bidder": (bidder or "").strip()}
    return out


# ------------------------------------------------------------------- registry
# name -> (parser, [url templates with {y}]). First template that returns a
# parseable file wins; the harvester also tries each through the Wayback Machine.
COUNTY_FILES = {
    "Anne Arundel": (parse_anne_arundel, [
        "https://www.aacounty.org/sites/default/files/{y}-06/{y}-tax-sale-results.pdf",
        "https://www.aacounty.org/sites/default/files/{y}-07/{y}-tax-sale-results.pdf",
        "https://www.aacounty.org/sites/default/files/{y}-05/{y}-tax-sale-results.pdf",
    ]),
    "Prince George's": (parse_prince_georges, [
        "https://taxsale.princegeorgescountymd.gov/{y}Taxsaleresults.pdf",
    ]),
    "Montgomery": (parse_montgomery, [
        "https://assets.montgomerycountymd.gov/files/{y}-06/tax_lien_sale_result_{y}.pdf",
        "https://assets.montgomerycountymd.gov/files/{y}-07/tax_lien_sale_result_{y}.pdf",
    ]),
    "Washington": (parse_washington, [
        "https://www.washco-md.net/wp-content/uploads/{y}-Tax-Sale-Results.pdf",
    ]),
    # DocumentCenter ids are not year-derivable; only the known current file.
    "Calvert": (parse_calvert, {2026: ["https://www.calvertcountymd.gov/DocumentCenter/View/57137"]}),
    "Caroline": (parse_caroline, {2026: ["https://www.carolinemd.org/DocumentCenter/View/12385"]}),
}


def urls_for(county, year):
    parser, tpl = COUNTY_FILES[county]
    if isinstance(tpl, dict):
        return parser, list(tpl.get(year, []))
    return parser, [t.format(y=year) for t in tpl]
