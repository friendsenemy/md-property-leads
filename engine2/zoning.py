"""
Zoning layer — what can be BUILT on each lead parcel (entitlement upside).

The play: a dead-owner / distressed single-family house on a lot zoned for more
(multifamily, commercial, mixed-use, small-lot rowhouse, Montgomery's attainable-
housing zones) sells to a builder for land value, not house value.

Sources (all free public county/state ArcGIS REST layers; polygons are downloaded
once per county into .cache/zoning/ and matched locally by point-in-polygon, or —
for parcel-level layers that carry the SDAT account id — attributes are joined by
account): every county now has its own layer (LAYERS). Parcels inside incorporated
towns (town-coded or holes in the county layer) fall through to the Maryland Dept of
Planning statewide Generalized Zoning, one point query each (coarser classes, still
tells SF vs MF/C).

Outputs
  data/zoning/<county>.json   {acct: {zone, label, upside: HIGH|MEDIUM|null, why, src}}
  data/zoning/upside.json     only HIGH/MEDIUM rows statewide (small; the Liens tab reads it)
  data/zoning/summary.json    counts per county + source + fetched date

  python -m engine2.zoning                 # all counties with a lead shard
  python -m engine2.zoning baltimore-county montgomery
  python -m engine2.zoning --refresh ...   # re-download polygons
"""
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date

CACHE = os.path.join(".cache", "zoning")
OUT = os.path.join("data", "zoning")
TITLE = os.path.join("data", "title")

STATEWIDE = ("https://mdpgis.mdp.state.md.us/arcgis/rest/services/PlanningCadastre/Generalized_Zoning/MapServer/0", "GENZONE")  # MDP Generalized Zoning 2021, whole state

# slug -> (layer url, zone field or None=autodetect[, options])
#   options: label   = description field to show next to the code
#            town    = regex of codes that mean "inside an incorporated town" (county layer says nothing;
#                      the parcel falls through to the MDP statewide point query like an unmatched point)
#            overlay = regex of overlay codes to skip when a point also sits in a base-zone polygon
#            acct    = attribute field holding the SDAT account id -> parcel-level layer, matched by account
#                      (attributes only, no polygon download)
LAYERS = {
    "baltimore-county": ("https://bcgis.baltimorecountymd.gov/arcgis/rest/services/DevelopmentManagement/Zoning/MapServer/1", "ZONE_CLASS"),
    "montgomery": ("https://mcmaps.org/server11/rest/services/Overlays/MCAtlas_Zoning/MapServer/4", "ZONE_"),
    "prince-george-s": ("https://gisdata.pgplanning.org/arcgis/rest/services/Applications/ZoningCertificationLetter/MapServer/59", "CLASS"),
    "anne-arundel": ("https://gis.aacounty.org/arcgis/rest/services/OpenData/Planning_OpenData/MapServer/29", "ZONED"),
    "baltimore-city": ("https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/Zoning_/FeatureServer/0", None),
    "charles": ("https://services7.arcgis.com/3BMWkdyrt45RNCrq/arcgis/rest/services/Zoning/FeatureServer/0", None),
    "harford": ("https://services.arcgis.com/q8r0H9SbF6PzNpYE/arcgis/rest/services/2017Zoning/FeatureServer/0", None),
    "carroll": ("https://services.arcgis.com/Uf0DiYpD9NOFO5YH/arcgis/rest/services/Zoning/FeatureServer/0", None),
    "washington": ("https://services2.arcgis.com/uxxyl33jRTSmjre5/arcgis/rest/services/Washington_County_Zoning/FeatureServer/21", None),
    "frederick": ("https://fcgis.frederickcountymd.gov/server_pub/rest/services/PlanningAndPermitting/Zoning/MapServer/1", "TYPE"),
    "caroline": ("https://services6.arcgis.com/M2dyOQipFC1IggF3/arcgis/rest/services/ZONING/FeatureServer/0", None),
    # ── added 2026-11-27 (were MDP statewide fallbacks) ──
    "howard": ("https://services1.arcgis.com/67LSIn5CxTeTDnwV/arcgis/rest/services/Zoning_Oct2025/FeatureServer/0", "ZONE", {"label": "CATEGORY"}),  # HoCoGIS, Oct 2025; no municipalities
    "calvert": ("https://gis.calvertcountymd.gov/server/rest/services/Planning/Zoning/MapServer/0", "ZONING", {"label": "ZONING_DESC", "acct": "ACCTID", "town": r"^(N/A|ROW)$"}),  # 2025 code, parcel-level; Chesapeake Beach / North Beach = N/A
    "st-mary-s": ("https://gis.stmaryscountymd.gov/server/rest/services/Public/General4/MapServer/4", "CATEGORY", {}),  # Leonardtown not covered
    "cecil": ("https://cecilmaps.org/arcgis/rest/services/ConsolidatedWebService/MapServer/33", "ZONING", {"acct": "ACCTID", "town": r"^(Town|Water)$"}),  # parcel-level; 8 towns = Town
    "queen-anne-s": ("https://gis.qac.org/arcgis/rest/services/QAC_PUBLIC/QAC_Planning/MapServer/0", "Zone", {"label": "GeneralZone", "town": r"^(Barclay|Centr|ChHill|Mill|QnAnne|Qnstown|Sudl|Temp)$"}),  # ORD 23-13 comprehensive rezoning
    "kent": ("https://services6.arcgis.com/RWHujKM5TItecIqp/arcgis/rest/services/Parcel_Viewer_experience_builder/FeatureServer/17", "zoning", {"label": "Zoning_full_name"}),  # unincorporated only
    "talbot": ("https://services6.arcgis.com/ScyqVZ3lUUihZ0ky/arcgis/rest/services/Zoning_Effective_With_Annexations/FeatureServer/8", "Zoning2018", {"town": r"^MUNI$"}),  # Easton/St Michaels/Oxford/Trappe = MUNI
    "wicomico": ("https://gisapps.wicomicocounty.org/server/rest/services/WicomicoZoningMar12026/MapServer/1", "COUNTY_ZON", {"label": "Zone_Long", "town": r"^Municipality$"}),
    "worcester": ("https://wcg-gisweb.co.worcester.md.us/arcgis/rest/services/Zoning/MapServer/1", "Designation_1", {"town": r"^MUN$"}),  # Ocean City/Berlin/Snow Hill/Pocomoke = MUN
    "dorchester": ("https://services7.arcgis.com/yqhlYKSnzjiOzQig/arcgis/rest/services/Zoning_Layer2021/FeatureServer/0", "CLASS_1", {"town": r"^MUNI$"}),
    "somerset": ("https://services1.arcgis.com/X3lKekbdaBmNjCHu/arcgis/rest/services/Somerset_County_GIS/FeatureServer/3", "ELEM_TEXT", {"label": "ZoneDesc", "town": r"^ANNEXED", "overlay": r"^(AP|OC)$"}),  # ESRGC-hosted for Somerset P&Z; AP/OC are overlays
    "allegany": ("https://services6.arcgis.com/dQQCC5HsocVNGmxM/arcgis/rest/services/Zoning_Districts_V260817CC/FeatureServer/1", "zone", {"town": r"^M$"}),  # Cumberland/Frostburg etc = M
    "garrett": ("https://gis.garrettcounty.org/server/rest/services/Zoning/FeatureServer/8", "flu", {"label": "db_8hla5_1"}),  # Deep Creek watershed + 6 towns only; rest of county is unzoned
}
FALLBACK = []  # counties with no usable county layer -> MDP statewide (none as of 2026-11-27; kept for the summary / tests)
TOWN_ZONES = re.compile(r"^(CITY|TOWN|TOWN ZONING|LA PLATA|INDIAN HEAD|MUNICIPAL|INCORPORATED|MUNI)\b", re.I)

# ── upside rules: (regex on the zone code, level, why). County-specific first, then generic.
RULES = {
    "baltimore-county": [
        (r"^DR\s*(10\.5|16)", "HIGH", "{z}: townhouse / apartment density by right"),
        (r"^(RAE|RO|OR|BL|BM|BR|BLR|BMR|BRR|CT|CCC|MLR|ML|MH|SE|OT|TC)", "HIGH", "{z}: commercial / office / mixed-use / employment zone"),
        (r"^DR\s*5\.5", "MEDIUM", "DR 5.5: 5.5 units/acre — extra lots only on a half-acre-plus parcel; check acreage"),
    ],
    "montgomery": [
        (r"^(CR|CRT|CRN|GR|NR|LSC|EOF|TOMX|TMD|RT|TF|R-(10|20|30)\b|R-H|RMH|RMX|PD)", "HIGH", "{z}: multifamily / townhouse / mixed-use zone"),
        (r"^R-(40|60|90)\b", "MEDIUM", "{z}: single-family zone where ZTA 25-02 (2025) allows duplex/triplex/small multiplex on many lots — check priority-corridor map"),
        (r"^(IM|IL|IH|CBD|CSP)", "HIGH", "{z}: industrial / commercial"),
    ],
    "prince-george-s": [
        (r"^(RMF|RTO|RSF-A|LCD|TAC|NAC|CGO|CN|CS|IE|IH|LTO|C-S-C|C-O|C-M|M-X-T|M-U|MU-|M-A-C|R-T\b|R-18|R-30|R-35|R-10\b|R-20\b|I-[1-4]|TDO|L-A-C|D-D-O|R-U)", "HIGH", "{z}: multifamily / townhouse / commercial / mixed-use / small-lot zone"),
        (r"^(RSF-95|RSF-65|R-55|R-80)", "MEDIUM", "{z}: small single-family lots — an extra lot only if the parcel is 2× the minimum"),
    ],
    "anne-arundel": [
        (r"^(R10|R15|R22|C[1-4]|W[1-3]|MXD|OT|MA|MC|TC|OTC|MB)", "HIGH", "{z}: 10+ units/acre, commercial, industrial or mixed-use"),
        (r"^R5\b", "MEDIUM", "R5: 5 units/acre — an extra lot only on a half-acre-plus parcel; check acreage"),
    ],
    "baltimore-city": [
        (r"^(R-(9|10)|C-[1-5]|OR-|IMU|TOD|D-|I-|BSC|EC|H|PC|MI|OIC|OR)", "HIGH", "{z}: multifamily, commercial, mixed-use or industrial"),
        (r"^R-[1-6]\b", None, ""),
        (r"^R-[78]\b", "MEDIUM", "{z}: rowhouse zone allowing multifamily conversion on larger lots"),
    ],
    "charles": [
        (r"^(RM|RH|RO|CB|CC|CV|MX|MUD|TOD|BP|IG|IH|CN|CRD)", "HIGH", "{z}: medium/high-density residential, commercial or mixed-use"),
        (r"^(PUD|PRD|PMH)", "MEDIUM", "{z}: planned development — density set by the approved plan"),
    ],
    "harford": [
        (r"\((R[34])\)|^(RO|B[1-3]|CI|MO|LI|GI|VB|VR|MR)|Business|Industrial|Mixed", "HIGH", "{z}: 3.5+ units/acre, commercial, mixed-use or industrial"),
        (r"\(R2\)", "MEDIUM", "{z}: 2 units/acre — splittable on a 1+ acre lot"),
    ],
    "frederick": [
        (r"^(MUN|ROW)\b", None, ""),
        (r"^(R-?(5|8|12|16)|VC|GC|LI|GI|MXD|ORI|MX|MU|TC|CDD|HS)", "HIGH", "{z}: medium/high-density residential, commercial, mixed-use or industrial"),
        (r"^(R-?3|PUD)\b", "MEDIUM", "{z}: low-density / planned development — extra units only on a larger parcel"),
    ],
    "calvert": [  # 2025 zoning ordinance codes (EC/FFD/I-1/I-2/MC/RC/RCD/RD/RND/TC/WCD); older codes kept for cached rows
        (r"^(TC|EC|MC|RC|I-?[12]|MX|LI|CMC|ETC|VD|RCD-?TC|PUD)\b", "HIGH", "{z}: town center / employment / commercial / industrial"),
        (r"^RD\b", "MEDIUM", "RD: Residential District — suburban lots; an extra lot only on a larger parcel, check acreage"),
        (r"^(WCD|RCD|RND|FFD)\b", None, ""),
    ],
    "howard": [
        (r"(MXD|TNC|TOD)", "HIGH", "{z}: mixed-use / traditional-neighborhood / transit overlay — density by plan"),
        (r"^(R-SA-8|R-A-15|R-APT|R-MH|R-VH|R-SI|RSI|R-H-ED|B-[12]|BR|SC|CCT|CAC|CE-|CEF|POR|PEC|M-[12]|HC|HO|OT|PSC|PGCC)", "HIGH", "{z}: 8+ units/acre, apartments, commercial, office or industrial"),
        (r"^(R-12|R-SC)\b", "MEDIUM", "{z}: 3.5–4 units/acre single-family — extra lot on a half-acre-plus parcel"),
        (r"^(NT|R-20|R-ED|RR|RC)\b", None, ""),  # NT = Columbia New Town: use fixed by the village Final Development Plan, no by-right upside
    ],
    "st-mary-s": [  # CM CMX I LCI MXH MXL MXM OBP RCL RH RL RL-T RM RMX RNC RPD RSC TMX VMX
        (r"^(CMX|MXH|MXM|MXL|TMX|VMX|RMX|CM|I|LCI|OBP|RH|RSC)$", "HIGH", "{z}: mixed-use / commercial / industrial / high-density residential"),
        (r"^RM$", "MEDIUM", "RM: Residential Medium-density — an extra lot or two on a larger parcel (Development District)"),
        (r"^(RL|RL-T|RNC|RPD|RCL)$", None, ""),
    ],
    "cecil": [  # BG BI BL EMU LDR M1 M2 MB MEA MH NAR OS RM RMU RR SAR Split ST UR VR
        (r"^(BG|BI|BL|EMU|RMU|RM|UR|M[12]|MB|MH)$", "HIGH", "{z}: business / industrial / mixed-use / multifamily / urban residential"),
        (r"^VR$", "MEDIUM", "VR: Village Residential small lots — extra lot on a larger parcel"),
        (r"^Split$", "MEDIUM", "Split-zoned parcel — part of it may carry a higher zone; check the split-zoning layer"),
        (r"^(LDR|ST|RR|NAR|SAR|OS|MEA)$", None, ""),
    ],
    "queen-anne-s": [  # NC-x = Neighborhood Conservation at 1 unit per x acres; SR/UR = suburban/urban residential
        (r"^(UR|UC|SC|CS|SI|LIHS|SIBE|VC|GVC|WVC|SHVC|GNC|GGMC|KISC|CMPD|SMPD|TC|APKIG)$", "HIGH", "{z}: urban residential, commercial, industrial, village center or planned development"),
        (r"^SR$", "MEDIUM", "SR: Suburban Residential ~3.5 units/acre — extra lot on a half-acre-plus parcel"),
        (r"^(NC-|AG|E$|SE$|GPRN)", None, ""),
    ],
    "kent": [
        (r"^(C|CCA|EC|ECCA|IV|M|MXD|MXDCA)$", "HIGH", "{z}: commercial / employment / intense village / mixed-use / marine"),
        (r"^(CR|V)$", "MEDIUM", "{z}: community residential / village small lots — extra lot on a larger parcel"),
        (r"^(CAR|AZD|RC|RCD)$", None, ""),
    ],
    "talbot": [
        (r"^(GC|LC|LI|TC|VM|WRC|VC)$", "HIGH", "{z}: commercial / industrial / town center / village mixed-use"),
        (r"^(TR|VR)$", "MEDIUM", "{z}: town / village residential small lots — extra lot on a larger parcel"),
        (r"^(VH|RR|RC|CP|AC)$", None, ""),
    ],
    "wicomico": [  # numbers are minimum lot size in 1,000 sq ft
        (r"^(C-[123]|CID|I-[12]|LB-[12]|LBR|ABP)$", "HIGH", "{z}: commercial / light business & residential / industrial"),
        (r"^(R-8|R-15|TT)$", "MEDIUM", "{z}: small single-family lots (8–15k sq ft) or town-transitional — extra lot on a larger parcel"),
        (r"^(R-20|R-30|A-1|VC|REC)$", None, ""),
    ],
    "worcester": [
        (r"^(C[123]|CM|I[12]|R[34])$", "HIGH", "{z}: commercial / industrial / general residential (multifamily allowed)"),
        (r"^(R2|V1)$", "MEDIUM", "{z}: suburban / village residential small lots — extra lot on a larger parcel"),
        (r"^(R1|E1|A[12]|RP|CA)$", None, ""),
    ],
    "dorchester": [
        (r"^(B-[12]|I-[12])", "HIGH", "{z}: business / industrial"),
        (r"^(SR|V)(-RCA)?$", "MEDIUM", "{z}: suburban residential / village — extra lot on a larger parcel (RCA overlay limits density)"),
        (r"^(AC|RC|RR)", None, ""),
    ],
    "somerset": [
        (r"^(C-[12]|I-[12]|MRC|R-3|OC)$", "HIGH", "{z}: commercial / industrial / maritime residential-commercial / high-density residential"),
        (r"^R-2$", "MEDIUM", "R-2: medium-density residential — extra lot on a larger parcel"),
        (r"^(R-1|AR|CO|AP)$", None, ""),
    ],
    "allegany": [
        (r"^(B-[12]|GU|I)$", "HIGH", "{z}: business / general urban / industrial"),
        (r"^R-2$", "MEDIUM", "R-2: residential district allowing two-family and higher-density housing — check the ordinance"),
        (r"^(R-1|A|C)$", None, ""),
    ],
    "garrett": [  # Deep Creek watershed + town codes; everything else in the county is unzoned
        (r"^(C|CBA|GC|TC|EC|PD)$", "HIGH", "{z}: commercial / central business / town center / planned development"),
        (r"^(TR|SR)$", "MEDIUM", "{z}: town / suburban residential small lots — extra lot on a larger parcel"),
        (r"^(AR|RD|RR|CR[12]|LR[12]|R|RIP|ROW)$", None, ""),  # LR1/LR2 = platted Deep Creek lake lots, not splittable
    ],
    "_statewide": [
        (r"^(MIXED USE|COMMERCIAL|INDUSTRIAL|HIGH DENSITY RESIDENTIAL)", "HIGH", "{z} (MDP generalized class): more than a single house allowed — confirm the county code"),
        # MDP's MEDIUM DENSITY bucket is ordinary single-family subdivision zoning (R-10/R-20 type) and
        # RURAL MEDIUM is large-lot rural — neither is a builder-upside signal, so no MEDIUM flag statewide.
        (r".", None, ""),
    ],
    "carroll": [
        (r"^(C-[1-3]|I-[1-2]|B-|R-7,500)", "HIGH", "{z}: commercial / industrial / small-lot"),
        (r"^R-10,000", "MEDIUM", "R-10,000: splittable on a half-acre-plus parcel"),
    ],
    "washington": [
        (r"^(RM|RU|BL|BG|HI|IR|PI|ORI|RB|RT|ORT)\b", "HIGH", "{z}: multifamily / business / industrial"),
    ],
}
GENERIC = [
    (r"\b(MF|MULTI|APART|TOWN|TH|RM|RMF|RH|R-?H|HD|MHD|MDR|HDR)\b", "HIGH", "{z}: multifamily / townhouse zone"),
    (r"^(C|B|CB|CC|CG|CN|CL|CR|CS|GC|LC|NC|HC|VC|TC|MX|MU|MXD|OR|OT|TOD|PD|PUD|I|LI|GI|IH|IL|IM|M|ML|MH|BP|W)[\s\-\d]", "HIGH", "{z}: commercial / mixed-use / industrial zone"),
    (r"^(C|B|I|M|MX|MU|TC|VC)$", "HIGH", "{z}: commercial / mixed-use / industrial zone"),
    (r"^R-?(10|15|20|22|30)(?![,\d])", "HIGH", "{z}: higher-density residential"),
    (r"^R-?(5|6|7|8)(?![,\d])", "MEDIUM", "{z}: small-lot residential — extra lot on a larger parcel"),
    (r"^R-?(7,500|10,000)\b", "MEDIUM", "{z}: small lots — an extra lot only if the parcel is 2× the minimum"),
    (r"(Commercial|Industrial|Mixed|Multi|High Density|Medium Density|Town Center)", "HIGH", "{z}: generalized zoning class allows more than a single house"),
]
NONE_HINT = re.compile(r"^(A|AG|AC|AR|RC|RR|RL|RLD|RA|RE|RE-?[12]|RCD|FFD|RPD|RNC|C-?O-?N|OS|RDD|R-?200|R-?90|DR\s*[12]\b|R1\b|R-1\b|EC|RURAL|Resource|Conservation|Agricult)", re.I)


def http_json(url, tries=3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "md-property-leads/zoning"}), timeout=90) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if i == tries - 1:
                raise
            time.sleep(3 * (i + 1))


def statewide_point(lon, lat):
    """MDP's server will not hand out its polygons (geometry exceeds its transfer limit), so the
    statewide fallback is a point-in-polygon query per parcel, paced at ~4/sec."""
    q = urllib.parse.urlencode({"f": "json", "geometry": f"{lon},{lat}", "geometryType": "esriGeometryPoint", "inSR": 4326,
                                "spatialRel": "esriSpatialRelIntersects", "outFields": "GENZONE,JURSCODE,MUNICIPALITY_NAME", "returnGeometry": "false"})
    d = http_json(f"{STATEWIDE[0]}/query?{q}")
    f = (d.get("features") or [{}])[0].get("attributes") or {}
    return str(f.get("GENZONE") or "").strip(), str(f.get("MUNICIPALITY_NAME") or "").strip()


def detect_zone_field(fields):
    names = [f["name"] for f in fields if f.get("type") == "esriFieldTypeString"]
    for pat in (r"^(ZONE_|ZONED|ZONING|ZONE|ZONE_CLASS|CLASS|ZONECODE|ZONE_CODE|ZN_CODE|CODE|DISTRICT|ZONINGDIST|ZONE_DIST|ZONE_TYPE|ZONINGCLASS|ZONECLASS|ZONING_CODE|ZONE_DESIG|DESIG)$",
                r"ZON", r"CLASS|CODE|DIST"):
        for n in names:
            if re.search(pat, n, re.I) and not re.search(r"DESC|NAME|LABEL|URL|COLOR|NOTE|LINK", n, re.I):
                return n
    return names[0] if names else None


def download(slug, url, refresh=False, out_fields="*", geometry=True, extra=None, cache_dir=CACHE):
    """Page a whole layer into .cache/<dir>/<slug>.json. geometry=False fetches attributes only
    (parcel-level layers joined by account); extra = additional query params (e.g. maxAllowableOffset)."""
    os.makedirs(cache_dir, exist_ok=True)
    fn = os.path.join(cache_dir, f"{slug}.json")
    if os.path.exists(fn) and not refresh:
        return json.load(open(fn))
    meta = http_json(url + "?f=json")
    if "error" in meta:
        raise RuntimeError(f"{slug}: {meta['error']}")
    fields = meta.get("fields", [])
    page = min(int(meta.get("maxRecordCount") or 1000), 2000)
    feats, off = [], 0
    while True:
        params = {"f": "json", "where": "1=1", "outFields": out_fields, "returnGeometry": "true" if geometry else "false",
                  "outSR": 4326, "geometryPrecision": 6, "resultOffset": off, "resultRecordCount": page}
        params.update(extra or {})
        q = urllib.parse.urlencode(params)
        d = http_json(f"{url}/query?{q}")
        if "error" in d:
            raise RuntimeError(f"{slug}: {d['error']}")
        f = d.get("features", [])
        feats.extend(f)
        print(f"  {slug}: {len(feats)} {'polygons' if geometry else 'rows'}", flush=True)
        if len(f) < page or not d.get("exceededTransferLimit", len(f) == page):
            break
        off += len(f)
        time.sleep(0.5)
    out = {"url": url, "fetched": date.today().isoformat(), "fields": fields, "features": feats, "name": meta.get("name"), "geometry": geometry}
    json.dump(out, open(fn, "w"))
    return out


def build_tree(layer, zone_field):
    from shapely.geometry import Polygon, MultiPolygon
    from shapely.strtree import STRtree
    geoms, attrs = [], []
    for f in layer["features"]:
        g = f.get("geometry") or {}
        rings = g.get("rings")
        if not rings:
            continue
        try:
            polys = [Polygon(r) for r in rings if len(r) >= 4]
            polys = [p.buffer(0) if not p.is_valid else p for p in polys]
            if not polys:
                continue
            geom = polys[0] if len(polys) == 1 else MultiPolygon(polys).buffer(0)
        except Exception:
            continue
        geoms.append(geom)
        attrs.append(f.get("attributes") or {})
    return STRtree(geoms), geoms, attrs


def norm_code(z):
    """'R - 15' -> 'R-15' (Wicomico pads its hyphens); collapse inner whitespace."""
    return re.sub(r"\s+", " ", re.sub(r"\s*-\s*", "-", str(z or ""))).strip()


def acct_key(a):
    return re.sub(r"[\s-]", "", str(a or ""))


def hits_for(tree, geoms, attrs, pt):
    out = []
    for i in tree.query(pt):
        if geoms[i].contains(pt) or geoms[i].touches(pt):
            out.append(attrs[i])
    return out


def classify(slug, zone, label):
    z = norm_code(zone)
    if not z or TOWN_ZONES.search(z):
        return None, ""
    txt = f"{z} {label or ''}"
    for pat, lvl, why in RULES.get(slug, []):
        if re.search(pat, z, re.I):
            return (lvl, why.format(z=z)) if lvl else (None, "")
    if NONE_HINT.search(z):
        return None, ""
    for pat, lvl, why in GENERIC:
        if re.search(pat, txt, re.I):
            return lvl, why.format(z=z)
    return None, ""


def rows_for(slug):
    fn = os.path.join(TITLE, f"{slug}.json")
    if not os.path.exists(fn):
        return []
    d = json.load(open(fn))
    rows = d if isinstance(d, list) else d.get("rows") or []
    out = []
    for r in rows:
        p = r.get("property") or {}
        try:
            lat, lon = float(p.get("lat")), float(p.get("lon"))
        except (TypeError, ValueError):
            continue
        out.append((re.sub(r"[\s-]", "", str(p.get("account_number") or r.get("id") or "")), lon, lat, p))
    return out


def run(slug, refresh=False):
    from shapely.geometry import Point
    if slug not in LAYERS:
        return run_statewide(slug, refresh)
    url, field, *rest = LAYERS[slug]
    opts = rest[0] if rest else {}
    src = "county"
    rule_slug = slug
    town_re = re.compile(opts["town"], re.I) if opts.get("town") else None
    overlay_re = re.compile(opts["overlay"], re.I) if opts.get("overlay") else None
    by_acct = None
    if opts.get("acct"):
        # parcel-level layer: attributes only, joined on the SDAT account id
        want = ",".join(x for x in (opts["acct"], field, opts.get("label")) if x)
        layer = download(slug, url, refresh, out_fields=want, geometry=False)
        by_acct = {acct_key(f["attributes"].get(opts["acct"])): f["attributes"] for f in layer["features"] if f.get("attributes")}
    else:
        layer = download(slug, url, refresh)
    field = field or detect_zone_field(layer["fields"])
    if "label" in opts:
        label_field = opts["label"]
    else:
        label_field = next((f["name"] for f in layer["fields"] if re.search(r"DESC|NAME|LABEL|TYPE", f["name"], re.I) and f["name"] != field and f.get("type") == "esriFieldTypeString"), None)
    if by_acct is None:
        tree, geoms, attrs = build_tree(layer, field)
    res, n_up, unmatched, n_town = {}, 0, [], 0
    for acct, lon, lat, p in rows_for(slug):
        if by_acct is not None:
            hit = by_acct.get(acct)
        else:
            hits = hits_for(tree, geoms, attrs, Point(lon, lat))
            if overlay_re and len(hits) > 1:
                hits = [h for h in hits if not overlay_re.search(norm_code(h.get(field)))] or hits
            hit = hits[0] if hits else None
        if not hit:
            unmatched.append((acct, lon, lat, p))
            continue
        zone = norm_code(hit.get(field) or hit.get("LONGZONE") or hit.get("CODE") or "")
        if not zone or (town_re and town_re.search(zone)) or TOWN_ZONES.search(zone):
            n_town += 1  # inside an incorporated town: the county layer carries no zone -> statewide point query
            unmatched.append((acct, lon, lat, p))
            continue
        label = str(hit.get(label_field) or "").strip() if label_field else ""
        lvl, why = classify(rule_slug, zone, label)
        sf = (p.get("property_type") or "").startswith("Residential")
        if lvl and not sf:
            lvl, why = (lvl, why + " (parcel is not coded residential — may already be commercial)")
        res[acct] = {"zone": zone, "label": label, "upside": lvl, "why": why, "src": src}
        n_up += bool(lvl)
    # rows the county layer did not cover (towns, holes): statewide generalized zoning, point by point.
    # MDP answers are reused even on --refresh (the statewide layer is a 2021 snapshot; --refresh is about county polygons).
    prev = {}
    pfn = os.path.join(OUT, f"{slug}.json")
    if os.path.exists(pfn):
        prev = {k: v for k, v in json.load(open(pfn)).items() if v.get("src", "").startswith("MDP")}
    for acct, lon, lat, p in unmatched:
        if acct in prev:
            res[acct] = prev[acct]
            n_up += bool(prev[acct].get("upside"))
            continue
        try:
            zone, muni = statewide_point(lon, lat)
        except Exception as e:
            print(f"  statewide point failed: {e}")
            break
        time.sleep(0.25)
        lvl, why = classify("_statewide", zone, muni) if zone else (None, "")
        res[acct] = {"zone": zone, "label": muni, "upside": lvl, "why": why, "src": "MDP generalized (statewide)"}
        n_up += bool(lvl)
    os.makedirs(OUT, exist_ok=True)
    json.dump(res, open(os.path.join(OUT, f"{slug}.json"), "w"), separators=(",", ":"))
    n_county = sum(1 for v in res.values() if v.get("src") == "county")
    print(f"{slug}: {len(res)} zoned ({n_up} with upside; {n_county} from county layer, {n_town} in towns) · field {field} · {src}")
    return {"zoned": len(res), "upside": n_up, "county_layer": n_county, "in_town": n_town, "field": field, "src": src, "layer": url, "fetched": layer.get("fetched")}


def run_statewide(slug, refresh=False):
    """Counties with no usable county layer: MDP generalized zoning, one point query per parcel (incremental)."""
    pfn = os.path.join(OUT, f"{slug}.json")
    res = {} if refresh or not os.path.exists(pfn) else json.load(open(pfn))
    n_new = 0
    for v in res.values():  # re-apply current rules to cached MDP answers
        if v.get("zone"):
            v["upside"], v["why"] = classify("_statewide", v["zone"], v.get("label"))
    for acct, lon, lat, p in rows_for(slug):
        if acct in res:
            continue
        try:
            zone, muni = statewide_point(lon, lat)
        except Exception as e:
            print(f"  {slug}: statewide point failed after {n_new}: {e}")
            break
        n_new += 1
        time.sleep(0.25)
        if not zone:
            res[acct] = {"zone": "", "label": muni, "upside": None, "why": "", "src": "MDP generalized (statewide)"}
            continue
        lvl, why = classify("_statewide", zone, muni)
        res[acct] = {"zone": zone, "label": muni, "upside": lvl, "why": why, "src": "MDP generalized (statewide)"}
        if n_new % 200 == 0:
            os.makedirs(OUT, exist_ok=True)
            json.dump(res, open(pfn, "w"), separators=(",", ":"))
            print(f"  {slug}: {n_new} queried", flush=True)
    os.makedirs(OUT, exist_ok=True)
    json.dump(res, open(pfn, "w"), separators=(",", ":"))
    n_up = sum(1 for v in res.values() if v.get("upside"))
    zoned = sum(1 for v in res.values() if v.get("zone"))
    print(f"{slug}: {zoned} zoned ({n_up} with upside) · MDP generalized (statewide), {n_new} new point queries")
    return {"zoned": zoned, "upside": n_up, "field": "GENZONE", "src": "MDP generalized (statewide)", "layer": STATEWIDE[0], "fetched": date.today().isoformat()}


def finish():
    summ, up = {}, {}
    sfn = os.path.join(OUT, "summary.json")
    if os.path.exists(sfn):
        summ = json.load(open(sfn)).get("counties", {})
    for fn in os.listdir(OUT):
        if fn.endswith(".json") and fn not in ("summary.json", "upside.json"):
            for k, v in json.load(open(os.path.join(OUT, fn))).items():
                if v.get("upside"):
                    up[k] = {"zone": v["zone"], "upside": v["upside"], "why": v["why"], "county": fn[:-5]}
    json.dump(up, open(os.path.join(OUT, "upside.json"), "w"), separators=(",", ":"))
    return summ, up


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    refresh = "--refresh" in sys.argv
    slugs = args or [f[:-5] for f in os.listdir(TITLE) if f.endswith(".json") and f[:-5] not in ("top", "people", "summary")]
    sfn = os.path.join(OUT, "summary.json")
    summ = json.load(open(sfn)).get("counties", {}) if os.path.exists(sfn) else {}
    for s in slugs:
        try:
            summ[s] = run(s, refresh)
        except Exception as e:
            print(f"{s}: FAILED {e}")
            summ[s] = {**summ.get(s, {}), "error": str(e)[:200]}
        os.makedirs(OUT, exist_ok=True)
        json.dump({"generated": date.today().isoformat(), "counties": summ}, open(sfn, "w"), indent=1)
    _, up = finish()
    print("upside rows statewide:", len(up))


if __name__ == "__main__":
    main()
