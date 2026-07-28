"""Crime / safety layer for the ZIP boards. /headroom

REAL published crime statistics only — FBI UCR / NIBRS agency (city police
department) figures compiled in data/headroom/crime.json, each entry
carrying its year, source and confidence. Coverage is deliberately
partial: a ZIP whose city we have not verified reports UNKNOWN, never
"safe". A wrong "safe" label puts a family somewhere it shouldn't be, so
the filter treats absence of evidence as absence of safety.

WHAT THIS IS NOT: the zips.db `crime_index` column is a socioeconomic
heuristic (density + income + education — see scripts/build_national_zips
crime_proxy, which says so in its own docstring). It correlates −0.76 with
median income, so filtering on it filters on income, not safety. It is
deliberately NOT used here, and must never be surfaced as a crime figure.

RESOLUTION CAVEAT: figures are CITY-wide (the reporting police agency).
Big cities contain both very safe and very dangerous ZIPs, and one city
rate cannot distinguish them — the UI must say so. Treat this as a
screen-out for cities that are unambiguously high-crime, plus a
verified-safe list for small towns where the city rate IS the local rate.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

_CRIME_PATH = Path(__file__).resolve().parent / "data" / "headroom" / "crime.json"

# FBI national benchmarks (per 100k residents), confirmed during research.
US_VIOLENT = 364.0
US_PROPERTY = 1954.0

# Tiers on the violent-crime rate per 100k. Anchored to the national rate
# rather than invented: "very safe" is under ~40% of national, "safe" under
# ~70%, "average" spans the national rate, and elevated/high are multiples.
TIERS = (
    ("very_safe", 0.0, 150.0, "VERY SAFE", "under 40% of the US rate"),
    ("safe", 150.0, 250.0, "SAFE", "under 70% of the US rate"),
    ("average", 250.0, 450.0, "AVERAGE", "around the US rate (~364)"),
    ("elevated", 450.0, 800.0, "ELEVATED", "1.2-2.2x the US rate"),
    ("high", 800.0, float("inf"), "HIGH", "over 2.2x the US rate"),
)
TIER_ORDER = {"very_safe": 0, "safe": 1, "average": 2, "elevated": 3, "high": 4,
              "unknown": 9}


@lru_cache(maxsize=1)
def _crime_table() -> dict:
    if not _CRIME_PATH.exists():
        return {}
    with open(_CRIME_PATH) as f:
        return json.load(f).get("table", {})


def _norm(city: str, state: str) -> str:
    """zips.db stores name as either 'Gary, IN' or 'Gary'; normalize both to
    the 'City, ST' key convention used by the crime table."""
    c = (city or "").strip()
    if c.endswith(f", {state}"):
        return c
    return f"{c}, {state}" if c else ""


def zip_safety(city: str, state: str) -> dict:
    """Safety record for a ZIP, keyed by its city agency. Always returns a
    dict; tier 'unknown' when we have no verified figure."""
    rec = _crime_table().get(_norm(city, state))
    if not rec or rec.get("violent_per_100k") is None:
        return {"tier": "unknown", "label": "UNKNOWN", "violent": None,
                "property": None, "year": None, "confidence": None,
                "source": (rec or {}).get("source"),
                "note": (rec or {}).get("note") or "no verified FBI figure for this city",
                "vs_us": None}
    v = float(rec["violent_per_100k"])
    tier = next(t for t in TIERS if t[1] <= v < t[2])
    return {"tier": tier[0], "label": tier[3], "violent": round(v),
            "property": (round(float(rec["property_per_100k"]))
                         if rec.get("property_per_100k") is not None else None),
            "year": rec.get("year"), "confidence": rec.get("confidence"),
            "source": rec.get("source"), "note": rec.get("note"),
            "vs_us": round(v / US_VIOLENT, 2), "tier_desc": tier[4]}


def passes(safety: dict, max_tier: str, allow_unknown: bool = False) -> bool:
    """Does this ZIP clear the user's safety bar? Unknown never passes
    unless the user explicitly opts in to seeing unverified markets."""
    if safety["tier"] == "unknown":
        return allow_unknown
    return TIER_ORDER[safety["tier"]] <= TIER_ORDER.get(max_tier, 1)


def coverage() -> dict:
    """How much of the table is real data — surfaced in the UI so the user
    always knows what fraction of the country we can actually vouch for."""
    t = _crime_table()
    with_data = sum(1 for v in t.values() if v.get("violent_per_100k") is not None)
    return {"cities": len(t), "with_data": with_data,
            "meta": (json.loads(_CRIME_PATH.read_text()).get("_meta", {})
                     if _CRIME_PATH.exists() else {})}
