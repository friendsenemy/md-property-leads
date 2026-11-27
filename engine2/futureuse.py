"""
Future land use layer — what the ADOPTED county plan says the parcel should become
(and whether it sits inside a pending comprehensive rezoning / sector plan area).

The play: a dead-owner house still zoned single-family whose county plan already
calls for mixed use / medium-high density / a transit or town center — or whose
block is inside a plan amendment that is about to rezone it — is land a builder
will pay for before the zoning map catches up. Zoning (engine2/zoning.py) tells
what can be built today; this layer tells what the county intends.

Sources: free public ArcGIS REST layers of each county's adopted comprehensive-plan
Future / Planned Land Use map (SOURCES below), plus pending-rezoning / sector-plan
/ growth-corridor layers where a county publishes one. Counties with no plan layer
fall back to the MDP Growth & Conservation "Place Areas" model, clearly labelled and
never counted as UPZONE on its own. Polygons are cached in .cache/futureuse/ and
matched locally (shapely STRtree); parcel-level layers are joined by SDAT account;
servers that will not hand out polygons are queried point by point (~0.25 s pacing,
answers kept in data/futureuse/<county>.json so re-runs only query new leads).

Classification
  plan_class 3 = mixed-use / commercial / industrial / transit / town-center / high-density
             2 = medium-density / growth-area / village / development-district
             1 = low-density / rural / conservation / established single-family
             0 = parks, public, water, municipal (town plan governs) — no signal
  zone_class from data/zoning: HIGH=3, MEDIUM=2, any other zone=1
  upside  UPZONE  plan_class > zone_class (plan already exceeds today's zoning)
          PENDING parcel inside a pending rezoning / sector plan / plan amendment area
          None    plan agrees with zoning (or no signal)

Outputs
  data/futureuse/<county>.json  {acct: {plan, plan_class, zone, zone_class, upside, pending, why, src}}
  data/futureuse/upside.json    UPZONE/PENDING rows statewide (the Title/Liens tabs read it)
  data/futureuse/summary.json   counts per county + source

  python -m engine2.futureuse                   # all counties with a lead shard
  python -m engine2.futureuse howard charles
  python -m engine2.futureuse --refresh ...     # re-download polygons / attribute joins
  python -m engine2.futureuse --requery ...     # also redo per-point answers
"""
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import date

from engine2.zoning import http_json, download, build_tree, rows_for, acct_key, hits_for, OUT as ZONING_OUT

CACHE = os.path.join(".cache", "futureuse")
OUT = os.path.join("data", "futureuse")
PACE = 0.25

# ── generic plan-category classifier (checked in order; first match wins) ──
RANK_PATTERNS = [
    (3, r"Transit|Town Center|Downtown|Mixed[- ]?Use|Mixed Residential|Mixed Office|Activity Center|Regional Center|Urban (Skyline|Corridor|Center|Neighborhood|Residential)|Commercial|Business|Industr|Employment|Office|Maritime|Marine|High[- ]Density|Higher Density|Medium-High|High Intensity|Multi-?Family|Apartment|Corridor|Node|Village Center|Village-Commercial|Village Mixed|Crossroads Commercial|Rural Commerce|Special Economic|Redevelopment|Core\b|Planned Development|Town Transition|Light Business|\bMXD\b|\bMX\b|\bind\b|high den|Transitional Comm"),
    (0, r"Park|Open Space|Public|Institution|\bWater\b|Wetland|Stream|Municipal|Military|Federal|School|Campus|Reservoir|Transportation|Right.of.Way|\bROW\b|Airport|Airfield|Solid Waste|Mineral|Extract|Mining|Undefined|^NA$|Non-County|Cemetery|Utility|Green Infrastructure|Special Use|Greenbelt|Historic$|\bOPE\b|Protected|Buffer|University|College|Community Facilit|City of|^Town$|^TOWN$|^Municipality$|^Municipalities$|^M$|Annapolis|Leonardtown|Columbia Open"),
    (2, r"Medium[- ]Density|Medium Intensity|Low-Medium|Residential Medium|Suburban Growth|Suburban Residential|Growth Area|Future Growth|Future Expansion|Town Growth|Municipal Growth|Development District|Connected Neighborhood|Village|Rural Crossroads|Historic Communities|Town Annexation|Residential-Medium|R-7500|R-10000|MGA|med den|Town Residential|Corridor Focused|Targeted Growth|Future Municipal Growth"),
    (1, r"Low[- ]Density|Lower Density|Low Intensity|Rural|Agricult|Farm|Forest|Conservation|Preserv|Estate|Large Lot|Resource|Established|Existing Developed|Community Conservation|Countryside|Limited Development|Critical Area|Residential Low|Single-Family|Waterfront Community|Suburban Transition|Rural Tier|Established Community|Large Lot|R-40000|low den|Groundwater|Watershed|Residential$|Residential Business|Residential Office|Lake Residential"),
]
RANK_NAMES = {3: "high-intensity", 2: "medium-density / growth", 1: "low-density / rural", 0: "no plan signal"}


def plan_rank(cat, overrides=None):
    c = str(cat or "").strip()
    if not c:
        return 0
    if overrides and c in overrides:
        return overrides[c]
    for rank, pat in RANK_PATTERNS:
        if re.search(pat, c, re.I):
            return rank
    return 1


# ── per-county sources, applied in order. kinds:
#   flu      planned land-use category (first hit wins across flu sources; later ones fill holes)
#   overlay  area that raises the plan class (transit / mixed-use / center overlays, growth areas)
#   pending  pending comprehensive rezoning / sector plan / plan amendment -> PENDING
#   note     informational area appended to "why" (recent CZMP issue, governing master plan)
# modes: poly (download polygons), acct (attributes joined by SDAT account; acct_len = trailing digits), point (per-parcel query)
SOURCES = {
    "anne-arundel": [
        {"kind": "flu", "name": "Plan2040 GDP Planned Land Use (amended Jul 2026)", "url": "https://gis.aacounty.org/arcgis/rest/services/OpenData/Planning_GDP_OpenData/MapServer/2", "field": "PLU_Description"},
        {"kind": "overlay", "name": "Plan2040 Development Policy Area overlay", "url": "https://gis.aacounty.org/arcgis/rest/services/OpenData/Planning_GDP_OpenData/MapServer/0", "field": "subarea", "rank": 3},
    ],
    "prince-george-s": [
        {"kind": "flu", "name": "M-NCPPC Future Land Use (Plan 2035 + master/sector plans; AGOL copy of Future_Land_Use_Py 2026-09)", "url": "https://services1.arcgis.com/qTQ6qYkHpxlu0G82/arcgis/rest/services/Future_Land_Use/FeatureServer/0", "field": "FLU_GENERALIZED", "label": "ACTIVE_PLAN",
         "ranks": {"Residential High": 3, "Residential Medium-High": 3, "Residential Medium": 2, "Residential Low": 1, "Neighborhood Mixed Use": 3, "Office": 3}},
        {"kind": "overlay", "name": "Plan 2035 Center", "url": "https://gisdata.pgplanning.org/arcgis/rest/services/Map_Services/C_I_Z/MapServer/0", "field": "TYPE_CODE", "label": "NAME", "rank": 3},
        {"kind": "pending", "name": "M-NCPPC active sector plan / SMA / plan amendment", "url": "https://gisdata.pgplanning.org/arcgis/rest/services/Applications/PGCPOngoingandActiveProjects/MapServer/0", "field": "PROJECT_NAME", "label": "CATEGORY",
         "where": "CATEGORY IN ('Sector Plan','Sectional Map Amendment (SMA)','Minor Plan Amendment') AND STATUS IN ('ACTIVE','PROPOSED')"},
    ],
    "baltimore-county": [
        {"kind": "flu", "name": "Master Plan 2030 Place Types", "url": "https://bcgisapps.baltimorecountymd.gov/arcgis/rest/services/MasterPlans2030Retrofit/MapServer/7", "field": "PLACETYPE", "label": "PLACETYPENAME",
         "ranks": {"Node": 3, "Village": 2, "Connected Neighborhood": 1, "Established Neighborhood": 1, "Rural Residential": 1}},  # Connected Neighborhood = existing urban neighborhood; only Nodes are density targets
        {"kind": "note", "name": "CZMP 2024 issue", "url": "https://bcgisdata.baltimorecountymd.gov/arcgis/rest/services/DevelopmentManagement/Zoning/MapServer/14", "field": "ISSUE_NUM"},
    ],
    "montgomery": [
        {"kind": "flu", "name": "Thrive Montgomery 2050 growth tier", "url": "https://montgomeryplans.org/server/rest/services/Thrive_General_Plan_Growth_Map/MapServer/8", "field": "NAMES",
         "ranks": {"Corridor Focused Growth Areas": 1, "Suburban Transition Areas": 1, "Rural Tier": 1}},  # tiers are county-wide swaths: context only; UPZONE in MoCo comes from ZTA 25-02 eligibility
        {"kind": "note", "name": "master plan", "url": "https://montgomeryplans.org/server9/rest/services/Overlays/Master_Plans_Adopted/MapServer/0", "field": "NAME"},
        {"kind": "overlay", "name": "ZTA 25-02 attainable-housing eligible parcel (duplex/triplex/quadplex by right on an SF lot)", "url": "https://montgomeryplans.org/server/rest/services/AHOM_MIL1/MapServer/4", "field": "STATUS", "mode": "acct", "acct": "ACCT", "acct_len": 8, "rank": 3, "const": "AHOM eligible"},
        {"kind": "pending", "name": "master / sector plan in progress", "url": "https://montgomeryplans.org/server/rest/services/Overlays/Master_Plan_Stack/MapServer/0", "field": "NAME", "where": "(PLANPDF = 'In Progress' OR ((PLANPDF IS NULL OR PLANPDF = '') AND APPROVALDATE IS NULL)) AND NAME IS NOT NULL"},  # Friendship Heights, Eastern Silver Spring Communities (Nov 2026)
    ],
    "baltimore-city": [
        {"kind": "flu", "name": "Our Baltimore comprehensive plan 2024 Proposed Land Use", "url": "https://services1.arcgis.com/43Lm3JYE3nM91DAF/arcgis/rest/services/ProposedLandUse_final/FeatureServer/1", "field": "ProposedLU", "label": "zoningLUgroup", "zone_equiv": "zoningLUgroup", "mode": "point", "fields": ["zoning_mismatch", "label"],  # zone_equiv: the city's own translation of today's zoning into plan categories -> UPZONE only where the plan beats it
         "ranks": {"Residential: Medium Density": 2, "Residential: Higher Density": 3, "Residential: Lower Density": 1, "Mixed Use: Predominantly Residential": 3, "Mixed Use: Hospital and Education Campus": 3, "Transit-Oriented Development": 3}},
    ],
    "howard": [
        {"kind": "flu", "name": "HoCo By Design (2023) character area", "url": "https://services1.arcgis.com/67LSIn5CxTeTDnwV/arcgis/rest/services/FLUM_ExportFeatures/FeatureServer/0", "field": "CHAR_AREA", "mode": "point",
         "ranks": {"Rural Conservation": 1, "Rural Living": 1, "Rural Crossroads": 2, "Single-Family Neighborhood": 1, "Multi-Family Neighborhood": 3, "Mixed-Use Neighborhood": 3, "Mixed-Use": 3, "Mixed-Use Activity Center": 3, "Village Activity Center": 3, "Regional Activity Center": 3, "Transit Activity Center": 3, "Downtown Columbia": 3, "Historic Communities": 2, "Suburban Commercial": 3, "Industrial": 3, "Industrial Mixed-Use": 3, "Campus": 0, "Recreation Open Space": 0, "Special Use": 0}},
    ],
    "charles": [
        {"kind": "flu", "name": "2016 Comprehensive Plan Land Use", "url": "https://services7.arcgis.com/3BMWkdyrt45RNCrq/arcgis/rest/services/LandUse3/FeatureServer/0", "field": "LANDUSE",
         "overlay_re": r"^(Development District|Transit Corridor|Town Growth Areas|Major Stream Valleys|Protected Lands)", "overlay_ranks": {"Transit Corridor": 3},
         "ranks": {"Residential": 2, "Rural Residential": 1, "Suburban Large Lot District": 1, "Town": 0}},
    ],
    "harford": [
        {"kind": "flu", "name": "HarfordNEXT (2016) Land Use Plan", "url": "https://services.arcgis.com/q8r0H9SbF6PzNpYE/arcgis/rest/services/2016LandUse/FeatureServer/0", "field": "TAG_12", "label": "DEFINITION",
         "ranks": {"HI": 3, "MO": 3, "IE": 3, "MI": 2, "VILLAGE": 2, "LI": 1, "AG": 1, "APG": 0, "HCC": 0, "PARK": 0, "TOWN": 0, "UCNM": 0, "WATER": 0}},
    ],
    "frederick": [
        {"kind": "flu", "name": "Livable Frederick / Comprehensive Plan designation (county)", "url": "https://fcgis.frederickcountymd.gov/server_pub/rest/services/PlanningAndPermitting/ComprehensivePlan/MapServer/7", "field": "TYPE", "mode": "acct", "acct": "TAX_ACCT",
         "codes": {"A": "Agricultural / Rural", "US": "Urban Skyline", "UC": "Urban Corridor/Center", "CC": "Cultural Corridor", "UN": "Urban Neighborhood", "IC": "Industrial Center", "IN": "Industrial Neighborhood", "OS": "Open Space", "NR": "Natural Resource", "PO": "Public Parkland / Open Space", "RurC": "Rural Community", "RurR": "Rural Residential", "LDR": "Low Density Residential", "MDR": "Medium Density Residential", "HDR": "High Density Residential", "VC": "Village Center", "GC": "General Commercial", "LI": "Limited Industrial", "ORI": "Office / Research / Industrial", "GI": "General Industrial", "MX": "Mixed Use", "MXD": "Mixed Use Development", "MM": "Mineral Mining", "I": "Institutional", "FMG": "Future Municipal Growth", "BF": "Buffer", "W": "Water", "ROW": "Right of Way"},
         "ranks": {"Cultural Corridor": 3, "Rural Community": 1}},
        {"kind": "flu", "name": "City of Frederick 2020 Future Land Use", "url": "https://services7.arcgis.com/kzGLdibqPsT2oxPF/arcgis/rest/services/2020_LU/FeatureServer/9", "field": "LandUse"},
    ],
    "carroll": [
        {"kind": "flu", "name": "Carroll 2014 Master Plan (2019 amendment) Future Land Use", "url": "https://services.arcgis.com/Uf0DiYpD9NOFO5YH/arcgis/rest/services/LU_FutureLandUse_County/FeatureServer/0", "field": "FutureLU"},
        {"kind": "flu", "name": "Freedom Community Comprehensive Plan", "url": "https://services.arcgis.com/Uf0DiYpD9NOFO5YH/arcgis/rest/services/DesLandUse_Freedom/FeatureServer/0", "field": "FLU"},
        {"kind": "flu", "name": "Finksburg Corridor Plan", "url": "https://services.arcgis.com/Uf0DiYpD9NOFO5YH/arcgis/rest/services/DesLandUse_Finksburg/FeatureServer/0", "field": "DESIGNATED"},
        {"kind": "flu", "name": "Hampstead Community Plan (2023)", "url": "https://services.arcgis.com/Uf0DiYpD9NOFO5YH/arcgis/rest/services/LU_DesLandUse_Hampstead/FeatureServer/0", "field": "DLU"},
        {"kind": "flu", "name": "Sykesville Community Plan", "url": "https://services.arcgis.com/Uf0DiYpD9NOFO5YH/arcgis/rest/services/DesLandUse_Sykesville/FeatureServer/0", "field": "DLU"},
        {"kind": "flu", "name": "Mount Airy Community Plan (2025)", "url": "https://services.arcgis.com/Uf0DiYpD9NOFO5YH/arcgis/rest/services/LU_DesLandUse_MtAiry/FeatureServer/0", "field": "Land_Use"},
        {"kind": "pending", "name": "Carroll comprehensive rezoning request", "url": "https://services.arcgis.com/Uf0DiYpD9NOFO5YH/arcgis/rest/services/ComprehensiveRezoning/FeatureServer/0", "field": "StaffRecZoning", "label": "Phase", "mode": "acct", "acct": "ACCTID"},
    ],
    "washington": [
        {"kind": "flu", "name": "Comprehensive Plan 2040 (adopted Sep 2025) Land Use Policy", "url": "https://services2.arcgis.com/uxxyl33jRTSmjre5/arcgis/rest/services/ZoningLULC_CP_2025/FeatureServer/0", "field": "LU_FULLNAME", "ranks": {"Rural Village": 2}},
    ],
    "st-mary-s": [
        {"kind": "flu", "name": "St. Mary's Comprehensive Plan Land Use (2010 as amended; 2050 plan approved Oct 2026 not yet published)", "url": "https://gis.stmaryscountymd.gov/server/rest/services/Public/General4/MapServer/2", "field": "CATEGORY", "label": "DEV_DIST",
         "ranks": {"Residential Medium Density": 2, "Residential Low Density": 1, "Residential Low Density Transitional": 1, "Rural Preservation": 1, "Rural Residential": 1, "Residential High Density": 3, "Residential Mixed Use": 3, "RCL": 1, "Non-Profit Institutional": 0}},
        {"kind": "overlay", "name": "growth area", "url": "https://gis.stmaryscountymd.gov/server/rest/services/Public/General4/MapServer/0", "field": "Type", "label": "Name", "where": "Type IN ('Development District','Town Center')"},  # note only (no rank): the plan category inside still governs
    ],
    "calvert": [
        {"kind": "flu", "name": "Calvert 2019 Comprehensive Plan Future Land Use (Jul 2022)", "url": "https://gis.calvertcountymd.gov/server/rest/services/Planning/Future_Land_Use__July_2022/MapServer/0", "field": "District", "label": "Town_Cente",
         "ranks": {"Residential": 2, "Waterfront Community": 1, "Future Expansion Area": 2, "Rural Residential": 1, "Farm and Forest": 1}},
    ],
    "cecil": [
        {"kind": "flu", "name": "Cecil 2010 Comprehensive Plan Land Use", "url": "https://cecilmaps.org/arcgis/rest/services/Hosted/Land_Use_Plan_Comprehensive_Plan/FeatureServer/0", "field": "flu", "label": "ga_type",
         "ranks": {"Medium High Density": 3, "Mixed Residential": 3, "Medium Density": 2, "Low Density": 1, "Employment": 3, "Municipality": 0}},
    ],
    "dorchester": [
        {"kind": "flu", "name": "Dorchester 2019 Comprehensive Plan Future Land Use", "url": "https://services7.arcgis.com/yqhlYKSnzjiOzQig/arcgis/rest/services/Future_Land_Use/FeatureServer/0", "field": "FutureLU", "ranks": {"Municipal": 0}},
    ],
    "worcester": [
        {"kind": "flu", "name": "Worcester Land Use Plan (adopted Oct 2026)", "url": "https://wcg-gisweb.co.worcester.md.us/arcgis/rest/services/Worcester_Data/Worcester_Future_Land_Use/MapServer/0", "field": "LandUse", "extra": {"maxAllowableOffset": 0.00005},
         "ranks": {"Residential": 1, "Growth Area": 2, "Village": 2, "Commercial Center": 3, "Industry": 3, "Municipality": 0}},
    ],
    "talbot": [
        {"kind": "flu", "name": "Talbot 2016 Comprehensive Plan Land Use", "url": "https://services6.arcgis.com/ScyqVZ3lUUihZ0ky/arcgis/rest/services/Septic_Tier_Land_Use_Plan_Aug2016/FeatureServer/16", "field": "Plan_Use",
         "ranks": {"Critical Area": 1, "Existing Developed": 1, "Future Growth": 2, "Town Annexation": 2, "Town": 0}},
    ],
    "wicomico": [
        {"kind": "flu", "name": "Wicomico 2017 Comprehensive Plan growth-area land use", "url": "https://services8.arcgis.com/qHmLtrNEAttmEZfU/arcgis/rest/services/Sewer_GrowthAreas/FeatureServer/0", "field": "PROPLU2017",
         "miss": "Rural / Agricultural (outside 2017 growth areas)", "miss_rank": 1, "ranks": {"Salisbury University Planning Area": 0, "Town Transition": 2, "Village Conservation": 1}},
    ],
    "queen-anne-s": [
        {"kind": "flu", "name": "Queen Anne's 2022 Comprehensive Plan growth area (plan implemented by ORD 23-13 comprehensive rezoning; no category map published)", "url": "https://gis.qac.org/arcgis/rest/services/QAC_PUBLIC/QAC_Planning/MapServer/1", "field": "COMMUNITY", "prefix": "Growth area: ", "rank": 1,
         "miss": "Outside growth areas", "miss_rank": 1},  # info only: the 2023 comprehensive rezoning already implemented the plan
    ],
}
# MDP Growth & Conservation "Place Areas" model (Feb 2026): statewide, built from PFAs, septic tiers, generalized
# zoning and MDP's growth model — NOT an adopted county plan. Used only where the county publishes no FLU layer.
MDP_PLACE = {"kind": "flu", "name": "MDP Growth & Conservation place-area model (no adopted-plan layer published by the county)", "url": "https://mdpgis.mdp.state.md.us/arcgis/rest/services/PlanningCadastre/Place_Areas_GC/MapServer/2", "field": "PlanningArea", "model": True, "out_fields": "PlanningArea,JURSCODE", "page": 50, "extra": {"maxAllowableOffset": 0.0001},
             "ranks": {"Targeted Growth and Revitalization": 2, "Future Growth Area": 2, "Established Community": 1, "Large Lot Development Areas": 1, "Rural Resource Area": 1}}
NO_PLAN_LAYER = ["kent", "caroline", "somerset", "allegany", "garrett"]
for _s in NO_PLAN_LAYER:
    SOURCES[_s] = [MDP_PLACE]


TOWN_CODES = re.compile(r"^(MUN|MUNI|Town|Municipality|M|N/A|ROW|CITY|TOWN|MUNICIPAL|INCORPORATED)\b", re.I)


def zone_class(z):
    """3 builder lot, 2 split/add, 1 any other real zone, None = unknown (no zoning row, or a town code)."""
    if not z or not z.get("zone") or TOWN_CODES.search(z["zone"]):
        return None
    return {"HIGH": 3, "MEDIUM": 2}.get(z.get("upside"), 1)


def cache_slug(slug, i, src):
    return f"{slug}-{i}-{re.sub(r'[^A-Za-z0-9]+', '_', src['url'].split('/services/')[-1])[:60]}"


def load_poly(slug, i, src, refresh):
    extra = dict(src.get("extra") or {})
    if src.get("where"):
        extra["where"] = src["where"]
    layer = download(cache_slug(slug, i, src), src["url"], refresh, out_fields=src.get("out_fields", "*"), extra=extra, cache_dir=CACHE, page=src.get("page"))
    return build_tree(layer, src["field"])


def load_acct(slug, i, src, refresh):
    want = ",".join(x for x in (src["acct"], src["field"], src.get("label")) if x)
    extra = {"where": src["where"]} if src.get("where") else None
    layer = download(cache_slug(slug, i, src), src["url"], refresh, out_fields=want, geometry=False, extra=extra, cache_dir=CACHE)
    n = src.get("acct_len")
    out = {}
    for f in layer["features"]:
        a = acct_key((f.get("attributes") or {}).get(src["acct"]))
        if a:
            out[a[-n:] if n else a] = f["attributes"]
    return out


def point_query(src, lon, lat):
    fields = [src["field"]] + [x for x in (src.get("label"),) if x] + list(src.get("fields") or [])
    q = urllib.parse.urlencode({"f": "json", "geometry": f"{lon},{lat}", "geometryType": "esriGeometryPoint", "inSR": 4326,
                                "spatialRel": "esriSpatialRelIntersects", "outFields": ",".join(fields), "returnGeometry": "false",
                                **({"where": src["where"]} if src.get("where") else {})})
    d = http_json(f"{src['url']}/query?{q}")
    if "error" in d:
        raise RuntimeError(str(d["error"])[:200])
    f = d.get("features") or []
    return f[0].get("attributes") if f else None


def category(src, attrs):
    """Plan category text for a hit (code -> label, prefix, constant)."""
    if attrs is None:
        return ""
    if src.get("const"):
        return src["const"]
    v = str(attrs.get(src["field"]) or "").strip()
    if src.get("codes"):
        v = src["codes"].get(v, v)
    return (src.get("prefix") or "") + v


def run(slug, refresh=False, requery=False):
    from shapely.geometry import Point
    sources = SOURCES.get(slug)
    if not sources:
        print(f"{slug}: no plan source configured")
        return {"error": "no plan source"}
    rows = rows_for(slug)
    zfn = os.path.join(ZONING_OUT, f"{slug}.json")
    zoning = json.load(open(zfn)) if os.path.exists(zfn) else {}
    pfn = os.path.join(OUT, f"{slug}.json")
    prev = json.load(open(pfn)) if os.path.exists(pfn) and not requery else {}
    # load every source once
    loaded = []
    for i, src in enumerate(sources):
        mode = src.get("mode", "poly")
        try:
            if mode == "poly":
                loaded.append((src, mode, load_poly(slug, i, src, refresh)))
            elif mode == "acct":
                loaded.append((src, mode, load_acct(slug, i, src, refresh)))
            else:
                loaded.append((src, mode, None))
        except Exception as e:
            print(f"  {slug}: source {i} ({src['name'][:40]}) failed: {e}")
    res, n_up, n_pend, n_plan, n_q = {}, 0, 0, 0, 0
    point_failed = False
    for acct, lon, lat, p in rows:
        pt = Point(lon, lat)
        plan, plan_rank_v, plan_src, label, notes, pending, extra = "", None, "", "", [], "", {}
        plan_raw, queried, zc_equiv = "", False, None  # queried: a per-point source answered (even with no hit) -> keep a stub so re-runs skip it
        for src, mode, data in loaded:
            # ── get the hit(s) for this parcel from this source
            hits = []
            if mode == "poly":
                tree, geoms, attrs = data
                hits = hits_for(tree, geoms, attrs, pt)
            elif mode == "acct":
                n = src.get("acct_len")
                h = data.get(acct[-n:] if n else acct)
                hits = [h] if h else []
            else:  # point query (cached in the previous output for this source)
                pv = prev.get(acct)
                if pv and pv.get("src") == src["name"] and "plan" in pv and not requery:
                    hits = [{src["field"]: pv.get("plan_raw", pv.get("plan")), src.get("label") or "_": pv.get("label"), **(pv.get("extra") or {})}] if pv.get("plan") else []
                    queried = True
                elif not point_failed:
                    try:
                        h = point_query(src, lon, lat)
                        n_q += 1
                        queried = True
                        time.sleep(PACE)
                        hits = [h] if h else []
                        if n_q % 200 == 0:
                            print(f"  {slug}: {n_q} point queries", flush=True)
                    except Exception as e:
                        print(f"  {slug}: point query failed after {n_q}: {e}")
                        point_failed = True
            kind = src["kind"]
            if kind == "flu":
                if plan:
                    continue  # an earlier flu source already answered
                base, ovl = hits, []
                if src.get("overlay_re") and hits:
                    ore = re.compile(src["overlay_re"], re.I)
                    ovl = [h for h in hits if ore.search(str(h.get(src["field"]) or ""))]
                    base = [h for h in hits if h not in ovl]
                if base:
                    plan = category(src, base[0])
                    plan_raw = str(base[0].get(src["field"]) or "")
                    plan_rank_v = src.get("rank") if "rank" in src else plan_rank(plan, src.get("ranks"))
                    label = str(base[0].get(src.get("label")) or "").strip() if src.get("label") else ""
                    if src.get("zone_equiv") and base[0].get(src["zone_equiv"]):
                        zc_equiv = plan_rank(str(base[0].get(src["zone_equiv"])), src.get("ranks"))
                    plan_src = src["name"]
                    for x in src.get("fields") or []:
                        extra[x] = base[0].get(x)
                    for o in ovl:
                        oc = str(o.get(src["field"]) or "")
                        notes.append(f"inside {oc}")
                        orank = next((r for k, r in (src.get("overlay_ranks") or {}).items() if oc.startswith(k)), None)
                        if orank is not None:
                            plan_rank_v = max(plan_rank_v, orank)
                elif src.get("miss") and not hits:
                    plan, plan_raw = src["miss"], ""
                    plan_rank_v = src["miss_rank"] if "miss_rank" in src else plan_rank(plan, src.get("ranks"))
                    plan_src = src["name"]
            elif kind == "overlay" and hits:
                c = category(src, hits[0])
                lb = str(hits[0].get(src.get("label")) or "").strip() if src.get("label") else ""
                notes.append(f"{src['name']}: {c}{' — ' + lb if lb else ''}")
                if src.get("rank") and (plan_rank_v is None or plan_rank_v >= 1):  # never promote parks / institutional / water
                    plan_rank_v = max(plan_rank_v or 0, src["rank"])
            elif kind == "pending" and hits:
                c = category(src, hits[0])
                lb = str(hits[0].get(src.get("label")) or "").strip() if src.get("label") else ""
                pending = f"{src['name']}: {c}{' (' + lb + ')' if lb else ''}"
            elif kind == "note" and hits:
                c = category(src, hits[0])
                if c:
                    notes.append(f"{src['name']} {c}")
        if not plan and not pending and not notes:
            if queried:
                res[acct] = {"plan": "", "plan_class": 0, "zone": (zoning.get(acct) or {}).get("zone") or "", "zone_class": None, "upside": None, "pending": "", "why": "", "src": next(s["name"] for s, m, _ in loaded if m == "point")}
            continue
        z = zoning.get(acct) or {}
        zc = zc_equiv if zc_equiv is not None else zone_class(z)
        zone = z.get("zone") or ""
        model = any(s.get("model") for s, _, _ in loaded)
        upside, why = None, ""
        pr = plan_rank_v or 0
        # UPZONE: the plan class beats today's zoning class. With unknown zoning (town parcel / no zoning row) only a
        # class-3 plan counts — a "medium density" plan over unknown zoning is not evidence of anything.
        if not model and ((zc is not None and pr > zc and pr >= 2) or (zc is None and pr == 3)):
            upside = "UPZONE"
            cur = f"current zoning {zone} ({'builder lot' if zc == 3 else 'split/add' if zc == 2 else 'single-family / rural'})" if zc else f"current zoning {zone or 'unknown'} (town / not classified)"
            why = f"plan calls for {plan or 'higher intensity'} ({RANK_NAMES[pr]}) vs {cur}"
            if notes:
                why += "; " + "; ".join(notes)
        elif pending:
            upside = "PENDING"
            why = f"{pending} — plan/zoning may change before the estate settles" + (f"; plan: {plan}" if plan else "")
        else:
            why = (f"plan ({plan}) agrees with current zoning {zone}" if plan and zone else f"plan: {plan}" if plan else "") + ("; " + "; ".join(notes) if notes else "")
        if upside == "UPZONE" and pending:
            why += f"; also {pending}"
        row = {"plan": plan, "plan_class": pr, "zone": zone, "zone_class": zc, "upside": upside, "pending": pending, "why": why, "src": plan_src or (loaded[0][0]["name"] if loaded else "")}
        if label:
            row["label"] = label
        if extra:
            row["extra"] = extra
        if plan and plan_raw != plan:
            row["plan_raw"] = plan_raw
        res[acct] = row
        n_up += upside == "UPZONE"
        n_pend += bool(pending)
        n_plan += bool(plan)
    if point_failed:  # keep what we had for the rows we could not query this run
        for k, v in prev.items():
            res.setdefault(k, v)
    os.makedirs(OUT, exist_ok=True)
    json.dump(res, open(pfn, "w"), separators=(",", ":"))
    src_name = loaded[0][0]["name"] if loaded else ""
    print(f"{slug}: {n_plan} with a plan category, {n_up} UPZONE, {n_pend} in pending areas · {n_q} point queries · {src_name[:60]}")
    return {"planned": n_plan, "upzone": n_up, "pending": n_pend, "rows": len(res), "src": src_name, "model_only": slug in NO_PLAN_LAYER,
            "layers": [s["url"] for s in sources], "fetched": date.today().isoformat(), "point_queries": n_q, "point_failed": point_failed}


def finish():
    up = {}
    for fn in os.listdir(OUT):
        if fn.endswith(".json") and fn not in ("summary.json", "upside.json"):
            for k, v in json.load(open(os.path.join(OUT, fn))).items():
                if v.get("upside"):
                    up[k] = {"plan": v["plan"], "upside": v["upside"], "why": v["why"], "zone": v.get("zone"), "pending": v.get("pending"), "county": fn[:-5]}
    json.dump(up, open(os.path.join(OUT, "upside.json"), "w"), separators=(",", ":"))
    return up


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    refresh, requery = "--refresh" in sys.argv, "--requery" in sys.argv
    title = os.path.join("data", "title")
    slugs = args or [f[:-5] for f in os.listdir(title) if f.endswith(".json") and f[:-5] not in ("top", "people", "summary")]
    sfn = os.path.join(OUT, "summary.json")
    summ = json.load(open(sfn)).get("counties", {}) if os.path.exists(sfn) else {}
    for s in slugs:
        try:
            summ[s] = run(s, refresh, requery)
        except Exception as e:
            print(f"{s}: FAILED {e}")
            summ[s] = {**summ.get(s, {}), "error": str(e)[:200]}
        os.makedirs(OUT, exist_ok=True)
        json.dump({"generated": date.today().isoformat(), "no_plan_layer": NO_PLAN_LAYER, "counties": summ}, open(sfn, "w"), indent=1)
    up = finish()
    print("future-use upside rows statewide:", len(up))


if __name__ == "__main__":
    main()
