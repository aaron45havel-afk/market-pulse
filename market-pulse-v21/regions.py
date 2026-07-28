"""Market definitions for the strict home-buying screen. /norcal

The California screen was built with California's assumptions baked in:
fourteen anchor cities of 300k+, Bay-calibrated dining thresholds, a
microclimate layer whose enemies are fog, wind corridors and inland
heat. None of that ports. New England is polycentric (exactly ONE city
clears 300k), its commutes are rail as much as road, and its climate
axis is maritime moderation rather than coastal fog.

This module holds what differs per market so the gate engine itself
stays one implementation:

  anchors            job-market gravity, not raw city population
  universe           which states / radius
  climate            per-market subregion layer (data/regions_<mkt>.json)
  hazards            per-market subregion layer (coastal flood, insurance)
  food               calibrated WITHIN the market by percentile, so a
                     genuinely good regional food town isn't failed for
                     not being San Francisco

Sub-region tables are researched data files, absent-tolerant: a market
with no file yet simply has no climate/hazard opinion, and the engine
labels that honestly rather than passing everything.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

_DATA = Path(__file__).resolve().parent / "data"

# ── California (the original screen, unchanged in behaviour) ─────────
CA_ANCHORS = {
    "SF": (37.7793, -122.4193), "SJ": (37.3382, -121.8863),
    "OAK": (37.8044, -122.2712), "SAC": (38.5816, -121.4944),
    "FRES": (36.7378, -119.7871), "STK": (37.9577, -121.2908),
    "BAK": (35.3733, -119.0187), "LA": (34.0522, -118.2437),
    "LB": (33.7701, -118.1937), "ANA": (33.8366, -117.9143),
    "SNA": (33.7455, -117.8677), "IRV": (33.6846, -117.8265),
    "RIV": (33.9806, -117.3755), "SD": (32.7157, -117.1611),
}

# ── New England ──────────────────────────────────────────────────────
# Anchors are METRO job markets, not "city >= 300k" — that rule admits
# only Boston here and would fail the entire region. Metro populations
# in the comment are the justification for inclusion.
NE_ANCHORS = {
    "BOS": (42.3601, -71.0589),   # Boston metro ~4.9M
    "PVD": (41.8240, -71.4128),   # Providence metro ~1.6M
    "HFD": (41.7658, -72.6734),   # Hartford metro ~1.2M
    "WOR": (42.2626, -71.8023),   # Worcester metro ~980k
    "NHV": (41.3083, -72.9279),   # New Haven metro ~860k
    "STM": (41.0534, -73.5387),   # Stamford/Lower Fairfield + NYC via Metro-North
    "BDG": (41.1792, -73.1894),   # Bridgeport metro ~940k
    "PWM": (43.6591, -70.2568),   # Portland ME metro ~550k
    "MHT": (42.9956, -71.4548),   # Manchester-Nashua NH metro ~420k
    "PSM": (43.0718, -70.7626),   # Portsmouth / NH Seacoast hub
    "NLC": (41.3557, -72.0995),   # New London / Groton (sub base, Electric Boat)
}

MARKETS = {
    "CA": {
        "label": "California",
        "states": ("CA",),
        "anchors": CA_ANCHORS,
        "universe_radius_mi": 40.0,
        "regions": ("All CA", "NorCal", "SoCal", "San Diego"),
        "default_region": "All CA",
        "climate_axis": "fog belt / wind corridor / inland heat excluded",
    },
    "NE": {
        "label": "New England",
        "states": ("MA", "RI", "CT", "NH", "ME"),
        "anchors": NE_ANCHORS,
        "universe_radius_mi": 35.0,
        "regions": ("All New England", "Boston metro", "CT Shoreline",
                    "RI + South Coast", "NH Seacoast", "Maine Coast",
                    "Inland"),
        "default_region": "All New England",
        "climate_axis": "maritime moderation — interior cold & snow-load excluded",
    },
}


# ── National expansion markets ───────────────────────────────────────
# Each carries its OWN climate axis — porting New England's maritime
# logic to Phoenix or Minneapolis would be meaningless. Anchors are
# metro job markets; tiers are scored within each region's distribution.
MTW_ANCHORS = {
    "DEN": (39.7392, -104.9903), "COS": (38.8339, -104.8214),
    "FNL": (40.5853, -105.0844), "SLC": (40.7608, -111.8910),
    "BOI": (43.6150, -116.2023), "BZN": (45.6770, -111.0429),
    "ABQ": (35.0844, -106.6504), "PHX": (33.4484, -112.0740),
    "TUS": (32.2226, -110.9747), "RNO": (39.5296, -119.8138),
}
SE_ANCHORS = {
    "RDU": (35.7796, -78.6382), "CLT": (35.2271, -80.8431),
    "AVL": (35.5951, -82.5515), "GSP": (34.8526, -82.3940),
    "CHS": (32.7765, -79.9311), "SAV": (32.0809, -81.0912),
    "BNA": (36.1627, -86.7816), "TYS": (35.9606, -83.9207),
    "CHA": (35.0456, -85.3097), "HSV": (34.7304, -86.5861),
    "RIC": (37.5407, -77.4360), "ATL": (33.7490, -84.3880),
}
MW_ANCHORS = {
    "MSP": (44.9778, -93.2650), "MSN": (43.0731, -89.4012),
    "ARB": (42.2808, -83.7430), "CHI": (41.8781, -87.6298),
    "CMH": (39.9612, -82.9988), "IND": (39.7684, -86.1581),
    "PIT": (40.4406, -79.9959), "CVG": (39.1031, -84.5120),
    "MKC": (39.0997, -94.5786), "DSM": (41.5868, -93.6250),
    "OMA": (41.2565, -95.9345), "MKE": (43.0389, -87.9065),
}
PNW_ANCHORS = {
    "SEA": (47.6062, -122.3321), "PDX": (45.5152, -122.6784),
    "GEG": (47.6588, -117.4260), "BND": (44.0582, -121.3153),
    "EUG": (44.0521, -123.0868), "OLM": (47.0379, -122.9007),
    "BLI": (48.7519, -122.4787), "YKM": (46.6021, -120.5059),
}

MARKETS.update({
    "MTW": {"label": "Mountain West", "states": ("CO", "UT", "ID", "MT", "NM", "AZ", "NV", "WY"),
            "anchors": MTW_ANCHORS, "universe_radius_mi": 35.0,
            "regions": ("All Mountain West", "Front Range", "Wasatch", "Desert Southwest",
                        "Northern Rockies", "Inland"),
            "default_region": "All Mountain West",
            "climate_axis": "aridity + altitude — desert extreme heat and severe-winter altitude excluded"},
    "SE": {"label": "Southeast", "states": ("NC", "SC", "GA", "TN", "VA", "AL"),
           "anchors": SE_ANCHORS, "universe_radius_mi": 35.0,
           "regions": ("All Southeast", "Carolina Piedmont", "Appalachian Upland",
                       "Atlantic Coast", "Tennessee Valley", "Inland"),
           "default_region": "All Southeast",
           "climate_axis": "summer heat + humidity, moderated by Appalachian elevation"},
    "MW": {"label": "Midwest", "states": ("MN", "WI", "MI", "IL", "OH", "IN", "IA", "MO", "KS", "NE", "PA"),
           "anchors": MW_ANCHORS, "universe_radius_mi": 35.0,
           "regions": ("All Midwest", "Ohio Valley", "Great Lakes", "Upper Midwest", "Inland"),
           "default_region": "All Midwest",
           "climate_axis": "continental extremes — deep-winter north and lake-effect snow belts excluded"},
    "PNW": {"label": "Pacific NW", "states": ("WA", "OR"),
            "anchors": PNW_ANCHORS, "universe_radius_mi": 35.0,
            "regions": ("All Pacific NW", "Puget Sound", "Willamette Valley",
                        "East of the Cascades", "Inland"),
            "default_region": "All Pacific NW",
            "climate_axis": "the Cascade rain shadow — east-side extremes excluded, winter gloom labelled"},
})

# Tier vocabularies differ per market (each region's "best" has its own
# name), so the pass/fail ladder is resolved per market rather than
# assuming New England's words.
MARKET_TIER_LADDER = {
    "NE": ("temperate-coastal", "moderate", "harsh-interior"),
    "MTW": ("temperate-highland", "moderate", "harsh"),
    "SE": ("temperate-upland", "moderate", "harsh-interior", "harsh"),
    "MW": ("river-valley-temperate", "moderate", "harsh-interior", "harsh"),
    "PNW": ("temperate-marine", "moderate", "harsh"),
}

MARKET_KEYS = tuple(MARKETS)


def market_of_state(state: str) -> str | None:
    st = (state or "").upper()
    for key, m in MARKETS.items():
        if st in m["states"]:
            return key
    return None


# ── Sub-region layers (researched JSON, absent-tolerant) ─────────────

@lru_cache(maxsize=8)
def _layer(market: str, kind: str) -> dict:
    """kind: 'climate' | 'hazard'. Returns {} when not yet researched."""
    p = _DATA / f"regions_{market.lower()}_{kind}.json"
    if not p.exists():
        return {}
    try:
        with open(p) as f:
            return json.load(f).get("table", {})
    except (OSError, ValueError):
        return {}


@lru_cache(maxsize=8)
def _town_index(market: str, kind: str) -> dict:
    """'City, ST' -> subregion key, built from each subregion's town list."""
    idx = {}
    for sub, rec in _layer(market, kind).items():
        for town in rec.get("towns", []):
            idx[town.strip()] = sub
    return idx


def subregion_for(market: str, city: str, state: str, kind: str = "climate") -> str | None:
    name = (city or "").replace(f", {state}", "").strip()
    return _town_index(market, kind).get(f"{name}, {state}")


CLIMATE_TIERS = ("temperate-coastal", "moderate", "harsh-interior")


def climate_of(market: str, city: str, state: str,
               winter_tolerance: str = "moderate") -> tuple[bool, str, dict | None]:
    """(passes, tier label, full record). Unclassified always fails — the
    gate never passes a town we have no climate opinion about.

    winter_tolerance is the user's own dial, because "how much winter is
    too much" is a preference, not a fact:
      temperate-coastal  only the mildest band (CT/RI shore, Cape, South Coast)
      moderate (default) + Boston metro, North Shore, Providence, Bay west
      harsh-interior     everything classified — Maine and NH included
    """
    sub = subregion_for(market, city, state, "climate")
    if not sub:
        return False, "unclassified — needs review", None
    rec = _layer(market, "climate").get(sub, {})
    tier = rec.get("tier", "unclassified")
    ladder = MARKET_TIER_LADDER.get(market, CLIMATE_TIERS)
    if tier not in ladder:
        return False, tier, {**rec, "subregion": sub}
    # Map the caller's generic tolerance onto this market's own ladder.
    if winter_tolerance in ladder:
        limit_i = ladder.index(winter_tolerance)
    elif winter_tolerance == "temperate-coastal":
        limit_i = 0
    elif winter_tolerance == "harsh-interior":
        limit_i = len(ladder) - 1
    else:
        limit_i = 1 if len(ladder) > 1 else 0
    return ladder.index(tier) <= limit_i, tier, {**rec, "subregion": sub}


def hazard_of(market: str, city: str, state: str) -> dict | None:
    sub = subregion_for(market, city, state, "hazard")
    if not sub:
        return None
    rec = _layer(market, "hazard").get(sub)
    return {**rec, "subregion": sub} if rec else None


def hazard_ok(hz: dict | None, *, strict_surge: bool = False,
              allow_crisis_insurance: bool = False) -> tuple[bool, list[str]]:
    """Coastal-hazard gate — New England's disqualifying-local-condition
    analogue to California's fog/heat exclusions.

    RESOLUTION NOTE: surge risk is encoded per SUB-REGION, but flood
    exposure is per PARCEL — most of Branford or Quincy sits well outside
    the VE/AE zones that make the subregion "high". So surge is a visible
    FLAG by default rather than a town-wide exclusion; strict_surge turns
    it into a hard gate for someone who wants nothing near the water.
    Insurance crisis IS market-wide (it prices every policy in the
    subregion), so that one hard-fails unless explicitly allowed."""
    if not hz:
        return True, []
    flags = []
    if hz.get("surge_flood_risk") == "high":
        flags.append("high surge / FEMA VE-AE exposure on the waterfront strip")
    if hz.get("insurance_climate") == "crisis":
        flags.append("insurance market in crisis")
    elif hz.get("insurance_climate") == "strained":
        flags.append("strained insurance market")
    if hz.get("seasonality") in ("heavy-tourist", "summer-surge"):
        flags.append(f"seasonality: {hz['seasonality']}")
    ok = True
    if strict_surge and hz.get("surge_flood_risk") == "high":
        ok = False
    if not allow_crisis_insurance and hz.get("insurance_climate") == "crisis":
        ok = False
    return ok, flags


def food_threshold(scores: list[float], pct: float = 75.0) -> float:
    """Dining bar calibrated WITHIN the market: the pct-th percentile of
    the market's own universe. An absolute Bay-Area number would fail
    Portland ME and Providence, which are genuinely strong food towns —
    the gate asks 'top-tier for this region', which is what it always
    meant."""
    vals = sorted(v for v in scores if v is not None)
    if not vals:
        return 0.0
    k = (len(vals) - 1) * (pct / 100.0)
    lo, hi = int(k), min(int(k) + 1, len(vals) - 1)
    return round(vals[lo] + (vals[hi] - vals[lo]) * (k - lo), 1)


# ── Region tagging within a market ───────────────────────────────────

_SOCAL_COUNTIES = {
    "Los Angeles County", "Orange County", "Ventura County",
    "San Bernardino County", "Riverside County", "Santa Barbara County",
    "San Luis Obispo County", "Kern County", "Imperial County",
}
_BOSTON_COUNTIES = {"Suffolk County", "Norfolk County", "Middlesex County",
                    "Essex County", "Plymouth County"}
_CT_SHORE_COUNTIES = {"New Haven County", "Fairfield County",
                      "New London County", "Middlesex County"}


def region_tag(market: str, county: str | None, state: str | None,
               climate_sub: str | None = None) -> str:
    c = county or ""
    st = (state or "").upper()
    if market in _SUB_PILL:
        return _pill_from_sub(market, climate_sub)
    if market == "CA":
        if c == "San Diego County":
            return "San Diego"
        return "SoCal" if c in _SOCAL_COUNTIES else "NorCal"
    # New England: sub-region pills follow the coastline, not county lines.
    sub = climate_sub or ""
    if st == "ME":
        return "Maine Coast" if "coast" in sub.lower() or "casco" in sub.lower() else "Inland"
    if st == "NH":
        return "NH Seacoast" if "seacoast" in sub.lower() else "Inland"
    if st == "RI":
        return "RI + South Coast"
    if st == "CT":
        if "shore" in sub.lower() or "gold coast" in sub.lower():
            return "CT Shoreline"
        return "CT Shoreline" if c in _CT_SHORE_COUNTIES and "interior" not in sub.lower() else "Inland"
    if st == "MA":
        if "south coast" in sub.lower() or "cape" in sub.lower():
            return "RI + South Coast"
        return "Boston metro" if c in _BOSTON_COUNTIES else "Inland"
    return "Inland"


# Region pills for the expansion markets are derived from the climate
# sub-region key, which already encodes the geography the pills describe.
_SUB_PILL = {
    "MTW": ((("front-range", "denver", "pikes", "boulder"), "Front Range"),
            (("wasatch", "salt-lake", "utah-valley", "tooele"), "Wasatch"),
            (("phoenix", "sonoran", "tucson", "desert", "vegas", "rio-grande", "albuquerque"), "Desert Southwest"),
            (("treasure", "boise", "bozeman", "gallatin", "montana", "reno", "truckee"), "Northern Rockies")),
    "SE": ((("piedmont", "charlotte", "triangle", "raleigh", "richmond"), "Carolina Piedmont"),
           (("upland", "blue_ridge", "blue-ridge", "escarpment", "appalach", "asheville", "chattanooga"), "Appalachian Upland"),
           (("coast", "charleston", "savannah", "lowcountry", "low_country"), "Atlantic Coast"),
           (("nashville", "tennessee", "middle_tn", "huntsville"), "Tennessee Valley")),
    "MW": ((("ohio_valley", "ohio-valley", "cincinnati", "river_valley", "river-valley"), "Ohio Valley"),
           (("lake", "chicago", "milwaukee", "michigan", "erie"), "Great Lakes"),
           (("minne", "twin_cities", "twin-cities", "madison", "wisconsin", "iowa", "upper"), "Upper Midwest")),
    "PNW": ((("puget", "seattle", "sound", "kitsap"), "Puget Sound"),
            (("willamette", "portland", "eugene", "salem"), "Willamette Valley"),
            (("east", "spokane", "yakima", "bend", "cascade-east", "columbia"), "East of the Cascades")),
}


def _pill_from_sub(market: str, sub: str | None) -> str:
    s = (sub or "").lower()
    for keys, pill in _SUB_PILL.get(market, ()):
        if any(k in s for k in keys):
            return pill
    return "Inland"
