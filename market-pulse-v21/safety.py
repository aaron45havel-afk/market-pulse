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

# FBI national benchmarks (per 100k residents), FBI "Crime in the Nation 2024"
# released Aug/Sep 2025 and cross-checked against BJS + USAFacts during the
# research pass. Violent fell 4.5% and property 8.1% vs 2023 — property in
# particular is well below the ~1,900-2,000 figure that older sources quote,
# so comparisons must use these.
US_VIOLENT = 359.1
US_PROPERTY = 1760.1

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

# TIER_ORDER is a SORT key and includes "unknown" so unverified rows sink
# to the bottom of a board. It is NOT the set of bars a user may select.
# Validating a user-supplied tier against TIER_ORDER lets ?safetier=unknown
# through, and because unknown sorts at 9 every verified tier compares
# <= 9 — the gate silently turns off while the UI still shows the first
# option. Validate against this instead.
SELECTABLE = frozenset(t[0] for t in TIERS)
DEFAULT_TIER = "safe"


def valid_tier(tier: str | None) -> str:
    """Coerce a query param to a real, selectable tier. Anything else —
    empty, junk, or the internal 'unknown' sentinel — falls back to the
    default bar rather than disabling the gate."""
    return tier if tier in SELECTABLE else DEFAULT_TIER


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
    dict; tier 'unknown' when we have no verified figure.

    confidence == "suspect" is also treated as UNKNOWN: those are cities
    where the researcher found the published figure implausible (partial
    NIBRS submission, conflicting aggregators, or a non-reporting CDP).
    A suspiciously LOW rate is the dangerous failure mode here — it would
    label a place safe for a family — so suspect never passes the gate."""
    rec = _crime_table().get(_norm(city, state))
    if rec and rec.get("confidence") == "suspect":
        return {"tier": "unknown", "label": "UNVERIFIED", "violent": None,
                "property": None, "year": rec.get("year"), "confidence": "suspect",
                "source": rec.get("source"), "note": rec.get("note"),
                "vs_us": None, "published_violent": rec.get("violent_per_100k")}
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
    # Defence in depth: an unrecognised (or "unknown") bar must tighten to
    # the default, never widen to "everything passes".
    return TIER_ORDER[safety["tier"]] <= TIER_ORDER[valid_tier(max_tier)]


def coverage() -> dict:
    """How much of the table is real data — surfaced in the UI so the user
    always knows what fraction of the country we can actually vouch for."""
    t = _crime_table()
    with_data = sum(1 for v in t.values() if v.get("violent_per_100k") is not None)
    # A published figure we've flagged "suspect" is demoted to unknown by
    # zip_safety, so it can never clear the gate. Reporting it under
    # "verified" overstates coverage — count what is actually usable.
    usable = sum(1 for v in t.values()
                 if v.get("violent_per_100k") is not None
                 and v.get("confidence") != "suspect")
    return {"cities": len(t), "with_data": with_data, "usable": usable,
            "suspect": with_data - usable,
            "meta": (json.loads(_CRIME_PATH.read_text()).get("_meta", {})
                     if _CRIME_PATH.exists() else {})}
