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


def climate_of(market: str, city: str, state: str) -> tuple[bool, str, dict | None]:
    """(passes, tier label, full record). Unclassified fails — the gate
    never passes a town we have no climate opinion about."""
    sub = subregion_for(market, city, state, "climate")
    if not sub:
        return False, "unclassified — needs review", None
    rec = _layer(market, "climate").get(sub, {})
    tier = rec.get("tier", "unclassified")
    # Best-in-region and solidly moderate pass; harsh interior fails.
    return tier in ("temperate-coastal", "moderate"), tier, {**rec, "subregion": sub}


def hazard_of(market: str, city: str, state: str) -> dict | None:
    sub = subregion_for(market, city, state, "hazard")
    if not sub:
        return None
    rec = _layer(market, "hazard").get(sub)
    return {**rec, "subregion": sub} if rec else None


def hazard_ok(hz: dict | None, *, allow_surge: bool = False,
              allow_crisis_insurance: bool = False) -> tuple[bool, list[str]]:
    """Coastal-hazard gate. New England's version of a disqualifying
    local condition: storm surge / FEMA flood exposure and an insurance
    market that has stopped functioning. Unknown hazard data does NOT
    fail the gate (it is an add-on, unlike climate) but is reported."""
    if not hz:
        return True, []
    flags = []
    if hz.get("surge_flood_risk") == "high":
        flags.append("high storm-surge / FEMA flood exposure")
    if hz.get("insurance_climate") == "crisis":
        flags.append("insurance market in crisis")
    if hz.get("seasonality") == "heavy-tourist":
        flags.append("heavy seasonal tourism")
    ok = True
    if not allow_surge and hz.get("surge_flood_risk") == "high":
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
