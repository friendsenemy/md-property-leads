"""
Zoning layer — what can be BUILT on each lead parcel (entitlement upside).

The play: a dead-owner / distressed single-family house on a lot zoned for more
(multifamily, commercial, mixed-use, small-lot rowhouse, Montgomery's attainable-
housing zones) sells to a builder for land value, not house value.

Sources (all free public county/state ArcGIS REST layers; polygons are downloaded
once per county into .cache/zoning/ and matched locally by point-in-polygon):
  county zoning layers where the county publishes one, else the Maryland Dept of
  Planning statewide Generalized Zoning (coarser classes, still tells SF vs MF/C).

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

# slug -> (layer url, zone field or None=autodetect)
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
}
FALLBACK = ["howard", "calvert", "st-mary-s", "cecil", "queen-anne-s", "kent", "talbot", "wicomico", "worcester", "dorchester", "somerset", "allegany", "garrett"]  # no usable county layer found; MDP statewide
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
    "calvert": [
        (r"^(TC|MX|MC|I-?1|I-?2|LI|CMC|ETC|VD|RCD-?TC|PUD)", "HIGH", "{z}: town center / commercial / industrial"),
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
    "st-mary-s": [
        (r"^(RMX|CMX|DMX|TMX|VMX|CC|I|CRM|RSC|RH|RM|TMX)", "HIGH", "{z}: mixed-use / commercial / medium-density"),
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


def download(slug, url, refresh=False):
    os.makedirs(CACHE, exist_ok=True)
    fn = os.path.join(CACHE, f"{slug}.json")
    if os.path.exists(fn) and not refresh:
        return json.load(open(fn))
    meta = http_json(url + "?f=json")
    fields = meta.get("fields", [])
    page = min(int(meta.get("maxRecordCount") or 1000), 2000)
    feats, off = [], 0
    while True:
        q = urllib.parse.urlencode({"f": "json", "where": "1=1", "outFields": "*", "returnGeometry": "true", "outSR": 4326,
                                    "geometryPrecision": 6, "resultOffset": off, "resultRecordCount": page})
        d = http_json(f"{url}/query?{q}")
        if "error" in d:
            raise RuntimeError(f"{slug}: {d['error']}")
        f = d.get("features", [])
        feats.extend(f)
        print(f"  {slug}: {len(feats)} polygons", flush=True)
        if len(f) < page or not d.get("exceededTransferLimit", len(f) == page):
            break
        off += len(f)
        time.sleep(0.5)
    out = {"url": url, "fetched": date.today().isoformat(), "fields": fields, "features": feats, "name": meta.get("name")}
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


def classify(slug, zone, label):
    z = (zone or "").strip()
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
    url, field = LAYERS.get(slug, STATEWIDE)
    src = "county" if slug in LAYERS else "MDP generalized (statewide)"
    rule_slug = slug if slug in LAYERS else "_statewide"
    if slug not in LAYERS:
        return run_statewide(slug, refresh)
    layer = download(slug, url, refresh)
    field = field or detect_zone_field(layer["fields"])
    label_field = next((f["name"] for f in layer["fields"] if re.search(r"DESC|NAME|LABEL|TYPE", f["name"], re.I) and f["name"] != field and f.get("type") == "esriFieldTypeString"), None)
    tree, geoms, attrs = build_tree(layer, field)
    res, n_up, unmatched = {}, 0, []
    for acct, lon, lat, p in rows_for(slug):
        pt = Point(lon, lat)
        hit = None
        for i in tree.query(pt):
            if geoms[i].contains(pt) or geoms[i].touches(pt):
                hit = attrs[i]
                break
        if not hit:
            unmatched.append((acct, lon, lat, p))
            continue
        zone = str(hit.get(field) or hit.get("LONGZONE") or hit.get("CODE") or "").strip()
        label = str(hit.get(label_field) or "").strip() if label_field else ""
        lvl, why = classify(rule_slug, zone, label)
        sf = (p.get("property_type") or "").startswith("Residential")
        if lvl and not sf:
            lvl, why = (lvl, why + " (parcel is not coded residential — may already be commercial)")
        res[acct] = {"zone": zone, "label": label, "upside": lvl, "why": why, "src": src}
        n_up += bool(lvl)
    # rows the county layer did not cover (towns, holes): statewide generalized zoning, point by point
    prev = {}
    pfn = os.path.join(OUT, f"{slug}.json")
    if os.path.exists(pfn) and not refresh:
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
        if not zone:
            continue
        lvl, why = classify("_statewide", zone, muni)
        res[acct] = {"zone": zone, "label": muni, "upside": lvl, "why": why, "src": "MDP generalized (statewide)"}
        n_up += bool(lvl)
    os.makedirs(OUT, exist_ok=True)
    json.dump(res, open(os.path.join(OUT, f"{slug}.json"), "w"), separators=(",", ":"))
    print(f"{slug}: {len(res)} zoned ({n_up} with upside) · field {field} · {src}")
    return {"zoned": len(res), "upside": n_up, "field": field, "src": src, "layer": url, "fetched": layer.get("fetched")}


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
