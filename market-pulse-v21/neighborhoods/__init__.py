"""Neighborhood-level (ZIP) data + investor scoring across all states.

Architecture:
  • adapters/ — one per data source. Each adapter exposes a fetch(zip, ctx)
    method that returns a dict of fields for that ZIP (or {} if no data).
    Adapters are key-gated where needed so the system runs out of the box
    even before Walk Score / Yelp keys are wired.
  • cache.py — file-based JSON cache with per-source TTLs. ZIP-level data
    moves slowly (months/years), so default TTL is 60 days.
  • zip_resolver.py — county FIPS → list of ZIP codes. Backed by a bundled
    crosswalk JSON; fall through gracefully when a county isn't mapped.
  • scoring.py — sub-score functions + personas (investor / lifestyle /
    balanced). Same scoring engine as the Dallas page, generalized.

Public API: get_county_neighborhoods(state, fips) → dict shaped exactly
like /api/dallas-neighborhoods so the same map template can render any
county.
"""
from __future__ import annotations
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable

from .adapters import ALL_ADAPTERS
from .cache import cache_get, cache_set
from .scoring import compute_zip_metrics, PERSONAS, DEFAULT_PERSONA
from .zip_resolver import get_zips_for_county, get_county_name

logger = logging.getLogger(__name__)

# Aggregated TTL for the *bundle* of one ZIP's data. Individual adapter
# fetches each have their own TTL inside cache_get; this one covers the
# composite shape we return to the API.
ZIP_BUNDLE_TTL_DAYS = 60


def _fetch_one_zip(zip_code: str, county_ctx: dict) -> dict:
    """Run every adapter for a single ZIP and merge the fields.

    Each adapter is responsible for its own caching at the source-payload
    level. We merge results with later adapters never overwriting fields
    set by earlier ones — order in ALL_ADAPTERS is the precedence order
    (ACS first because it's authoritative for demographics).
    """
    merged: dict = {"zip": zip_code}
    for adapter in ALL_ADAPTERS:
        try:
            fields = adapter.fetch(zip_code, county_ctx) or {}
        except Exception as e:
            logger.warning("Adapter %s failed for ZIP %s: %s", adapter.name, zip_code, e)
            fields = {}
        for k, v in fields.items():
            if k not in merged or merged[k] in (None, "", []):
                merged[k] = v
    return merged


def _coverage_summary(rows: list[dict]) -> dict:
    """Per-dimension % of ZIPs that have a value — surfaced in the UI so
    the user can see exactly which dimensions are populated for this
    county and which still need an API key or city-specific adapter."""
    if not rows:
        return {}
    fields = [
        "median_home_value", "median_rent_monthly", "median_household_income",
        "pct_bachelors", "population", "crime_index", "walk_score",
        "restaurant_score",
    ]
    out = {}
    for f in fields:
        n = sum(1 for r in rows if r.get(f) not in (None, "", 0))
        out[f] = round(n / len(rows) * 100, 1)
    return out


def get_county_neighborhoods(state: str, fips: str) -> dict:
    """Build the full neighborhood payload for a county.

    Returns the same shape as dallas_neighborhoods.get_dallas_neighborhoods()
    so the existing map template can render it untouched.
    """
    state = state.upper()
    zips = get_zips_for_county(state, fips)
    county_name = get_county_name(state, fips) or f"FIPS {fips}"

    if not zips:
        return {
            "state": state,
            "fips": fips,
            "county_name": county_name,
            "as_of": "live",
            "personas": PERSONAS,
            "default_persona": DEFAULT_PERSONA,
            "neighborhoods": [],
            "coverage": {},
            "sources": _sources_block(),
            "caveats": [
                f"No ZIP-county mapping yet for {county_name} — add it to data/county_zips.json.",
            ],
        }

    cache_key = f"county_bundle:{state}:{fips}"
    cached = cache_get(cache_key, ttl_days=ZIP_BUNDLE_TTL_DAYS)
    if cached:
        return cached

    county_ctx = {"state": state, "fips": fips, "county_name": county_name}

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_fetch_one_zip, z, county_ctx): z for z in zips}
        for fut in as_completed(futs):
            try:
                rows.append(fut.result())
            except Exception as e:
                logger.warning("ZIP %s aggregation failed: %s", futs[fut], e)

    enriched = []
    for raw in rows:
        # If an adapter didn't supply a name, use ZIP as the label.
        raw.setdefault("name", f"ZIP {raw['zip']}")
        # Skip ZIPs with no real data at all (no adapters returned anything).
        if not any(raw.get(k) for k in ("median_home_value", "median_household_income", "population")):
            continue
        metrics = compute_zip_metrics(raw)
        enriched.append({**raw, **metrics})
    enriched.sort(key=lambda x: x["composite_score"], reverse=True)

    payload = {
        "state": state,
        "fips": fips,
        "county_name": county_name,
        "as_of": "live (Census ACS) + cached lifestyle data",
        "personas": PERSONAS,
        "default_persona": DEFAULT_PERSONA,
        "neighborhoods": enriched,
        "coverage": _coverage_summary(enriched),
        "sources": _sources_block(),
        "caveats": _caveats_for(enriched),
    }
    cache_set(cache_key, payload)
    return payload


def _sources_block() -> dict:
    return {
        "demographics": "U.S. Census ACS 2022 5-year (B19013, B15003, B01003, B25077, B25064)",
        "walkability": "Walk Score API (set WALKSCORE_API_KEY env var to activate)",
        "restaurants": "Yelp Fusion API (set YELP_API_KEY env var to activate)",
        "crime": "Per-city Socrata open-data portals (Dallas live; more cities being added)",
    }


def _caveats_for(rows: list[dict]) -> list[str]:
    notes = [
        "ZIP codes are USPS routes, not true neighborhoods — Bishop Arts and parts of Oak Cliff share ZIPs.",
        "% bachelor's+ is a school-quality proxy. Direct STAAR / accountability ratings would be more accurate.",
    ]
    has_walk = any(r.get("walk_score") for r in rows)
    has_rest = any(r.get("restaurant_score") for r in rows)
    has_crime = any(r.get("crime_index") for r in rows)
    if not has_walk:
        notes.append("Walkability not yet populated for this county — set WALKSCORE_API_KEY to activate.")
    if not has_rest:
        notes.append("Restaurant density not yet populated for this county — set YELP_API_KEY to activate.")
    if not has_crime:
        notes.append("Crime data not yet populated for this county — needs a city-specific Socrata adapter.")
    return notes
