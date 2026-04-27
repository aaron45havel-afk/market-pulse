"""ZIP-level neighborhood investor data per state.

Same shape and scoring as dallas_neighborhoods.py — this module just adds
metros for additional states and reuses the Dallas scoring/persona logic so
weights stay consistent across the dashboard.

Coverage right now: TX (Dallas County, via DALLAS_ZIPS), CA (Los Angeles
County), AZ (Maricopa County / Phoenix metro). Add more by appending entries
to STATE_METROS.

All numbers are hand-curated snapshots from public sources (Zillow Research,
U.S. Census ACS, local PD open data, Walk Score, Yelp). They're meant to be
directionally correct, not authoritative — see the per-metro caveats below.
"""
from __future__ import annotations

from dallas_neighborhoods import (
    DALLAS_ZIPS,
    DATA_AS_OF,
    PERSONAS,
    DEFAULT_PERSONA,
    compute_zip_metrics,
)


# ─────────────────────────────────────────────────────────────────────────────
# CA — Los Angeles County (LA city + close-in beach/hill submarkets)
# ─────────────────────────────────────────────────────────────────────────────
LA_ZIPS: dict[str, dict] = {
    "90210": {
        "name": "Beverly Hills",
        "lat": 34.0901, "lng": -118.4065,
        "median_home_value": 4_500_000, "median_rent_monthly": 8_500,
        "crime_index": 22, "pct_bachelors": 78,
        "median_household_income": 195_000, "population": 21_000,
        "walk_score": 55, "restaurant_score": 75,
        "tags": ["luxury", "low-crime", "top-schools"],
    },
    "90402": {
        "name": "Santa Monica N",
        "lat": 34.0339, "lng": -118.5089,
        "median_home_value": 3_500_000, "median_rent_monthly": 5_500,
        "crime_index": 30, "pct_bachelors": 78,
        "median_household_income": 180_000, "population": 13_000,
        "walk_score": 70, "restaurant_score": 80,
        "tags": ["beach", "luxury", "walkable"],
    },
    "90291": {
        "name": "Venice",
        "lat": 33.9939, "lng": -118.4596,
        "median_home_value": 2_300_000, "median_rent_monthly": 4_500,
        "crime_index": 55, "pct_bachelors": 70,
        "median_household_income": 130_000, "population": 35_000,
        "walk_score": 80, "restaurant_score": 88,
        "tags": ["beach", "walkable", "gentrified"],
    },
    "90048": {
        "name": "Beverly Grove",
        "lat": 34.0717, "lng": -118.3756,
        "median_home_value": 1_400_000, "median_rent_monthly": 3_400,
        "crime_index": 42, "pct_bachelors": 68,
        "median_household_income": 110_000, "population": 22_000,
        "walk_score": 78, "restaurant_score": 80,
        "tags": ["walkable", "central"],
    },
    "90039": {
        "name": "Silver Lake / Atwater",
        "lat": 34.0911, "lng": -118.2664,
        "median_home_value": 1_300_000, "median_rent_monthly": 3_000,
        "crime_index": 50, "pct_bachelors": 60,
        "median_household_income": 95_000, "population": 32_000,
        "walk_score": 75, "restaurant_score": 85,
        "tags": ["walkable", "hip", "gentrified"],
    },
    "90042": {
        "name": "Highland Park",
        "lat": 34.1155, "lng": -118.1872,
        "median_home_value": 950_000, "median_rent_monthly": 2_800,
        "crime_index": 55, "pct_bachelors": 38,
        "median_household_income": 75_000, "population": 60_500,
        "walk_score": 65, "restaurant_score": 72,
        "tags": ["gentrifying", "walkable", "appreciation-play"],
    },
    "90019": {
        "name": "Mid-City",
        "lat": 34.0490, "lng": -118.3470,
        "median_home_value": 1_050_000, "median_rent_monthly": 2_700,
        "crime_index": 60, "pct_bachelors": 42,
        "median_household_income": 70_000, "population": 64_000,
        "walk_score": 75, "restaurant_score": 60,
        "tags": ["central", "gentrifying"],
    },
    "90065": {
        "name": "Mt Washington / Glassell Park",
        "lat": 34.0980, "lng": -118.2183,
        "median_home_value": 950_000, "median_rent_monthly": 2_700,
        "crime_index": 55, "pct_bachelors": 45,
        "median_household_income": 78_000, "population": 41_000,
        "walk_score": 50, "restaurant_score": 50,
        "tags": ["hilly", "appreciation-play"],
    },
    "90008": {
        "name": "Baldwin Hills / Crenshaw",
        "lat": 34.0093, "lng": -118.3540,
        "median_home_value": 850_000, "median_rent_monthly": 2_400,
        "crime_index": 65, "pct_bachelors": 32,
        "median_household_income": 65_000, "population": 32_000,
        "walk_score": 60, "restaurant_score": 45,
        "tags": ["established", "improving"],
    },
    "90011": {
        "name": "South Central",
        "lat": 34.0086, "lng": -118.2587,
        "median_home_value": 580_000, "median_rent_monthly": 2_100,
        "crime_index": 78, "pct_bachelors": 8,
        "median_household_income": 42_000, "population": 110_000,
        "walk_score": 65, "restaurant_score": 35,
        "tags": ["high-cap-rate", "high-density", "high-crime"],
    },
    "90004": {
        "name": "Koreatown / Larchmont",
        "lat": 34.0758, "lng": -118.3092,
        "median_home_value": 1_100_000, "median_rent_monthly": 2_500,
        "crime_index": 55, "pct_bachelors": 52,
        "median_household_income": 72_000, "population": 64_000,
        "walk_score": 88, "restaurant_score": 92,
        "tags": ["walkable", "high-density", "restaurants"],
    },
    "90744": {
        "name": "Wilmington / Harbor",
        "lat": 33.7805, "lng": -118.2611,
        "median_home_value": 580_000, "median_rent_monthly": 2_200,
        "crime_index": 70, "pct_bachelors": 12,
        "median_household_income": 50_000, "population": 51_000,
        "walk_score": 50, "restaurant_score": 30,
        "tags": ["industrial", "high-cap-rate"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# AZ — Maricopa County (Phoenix / Scottsdale / Tempe / Chandler)
# ─────────────────────────────────────────────────────────────────────────────
PHOENIX_ZIPS: dict[str, dict] = {
    "85003": {
        "name": "Downtown Phoenix",
        "lat": 33.4500, "lng": -112.0740,
        "median_home_value": 410_000, "median_rent_monthly": 1_900,
        "crime_index": 70, "pct_bachelors": 35,
        "median_household_income": 55_000, "population": 13_500,
        "walk_score": 70, "restaurant_score": 75,
        "tags": ["urban", "walkable", "transit"],
    },
    "85016": {
        "name": "Biltmore / Camelback East",
        "lat": 33.5060, "lng": -112.0370,
        "median_home_value": 575_000, "median_rent_monthly": 2_200,
        "crime_index": 50, "pct_bachelors": 55,
        "median_household_income": 88_000, "population": 38_000,
        "walk_score": 55, "restaurant_score": 80,
        "tags": ["walkable", "restaurants", "mid-upper"],
    },
    "85018": {
        "name": "Arcadia",
        "lat": 33.4920, "lng": -111.9970,
        "median_home_value": 1_100_000, "median_rent_monthly": 3_100,
        "crime_index": 30, "pct_bachelors": 65,
        "median_household_income": 130_000, "population": 31_000,
        "walk_score": 45, "restaurant_score": 65,
        "tags": ["top-tier", "low-crime", "family"],
    },
    "85050": {
        "name": "N Phoenix / Desert Ridge",
        "lat": 33.6900, "lng": -111.9810,
        "median_home_value": 720_000, "median_rent_monthly": 2_700,
        "crime_index": 25, "pct_bachelors": 60,
        "median_household_income": 130_000, "population": 31_500,
        "walk_score": 25, "restaurant_score": 55,
        "tags": ["family", "newer", "low-crime"],
    },
    "85254": {
        "name": "Scottsdale (NE Phx)",
        "lat": 33.6240, "lng": -111.9810,
        "median_home_value": 880_000, "median_rent_monthly": 2_900,
        "crime_index": 22, "pct_bachelors": 62,
        "median_household_income": 128_000, "population": 51_000,
        "walk_score": 30, "restaurant_score": 60,
        "tags": ["upscale", "family", "low-crime"],
    },
    "85251": {
        "name": "Old Town Scottsdale",
        "lat": 33.4940, "lng": -111.9220,
        "median_home_value": 720_000, "median_rent_monthly": 2_600,
        "crime_index": 45, "pct_bachelors": 60,
        "median_household_income": 92_000, "population": 23_000,
        "walk_score": 75, "restaurant_score": 90,
        "tags": ["walkable", "nightlife", "restaurants"],
    },
    "85260": {
        "name": "Scottsdale N",
        "lat": 33.6160, "lng": -111.8820,
        "median_home_value": 980_000, "median_rent_monthly": 3_000,
        "crime_index": 18, "pct_bachelors": 70,
        "median_household_income": 145_000, "population": 30_000,
        "walk_score": 25, "restaurant_score": 55,
        "tags": ["luxury", "low-crime", "family"],
    },
    "85266": {
        "name": "N Scottsdale / Carefree",
        "lat": 33.7820, "lng": -111.9100,
        "median_home_value": 1_300_000, "median_rent_monthly": 3_500,
        "crime_index": 12, "pct_bachelors": 72,
        "median_household_income": 165_000, "population": 13_500,
        "walk_score": 15, "restaurant_score": 45,
        "tags": ["luxury", "low-crime"],
    },
    "85283": {
        "name": "Tempe S / ASU-adj",
        "lat": 33.3870, "lng": -111.9300,
        "median_home_value": 470_000, "median_rent_monthly": 2_100,
        "crime_index": 45, "pct_bachelors": 45,
        "median_household_income": 78_000, "population": 36_500,
        "walk_score": 50, "restaurant_score": 60,
        "tags": ["college", "rental-demand"],
    },
    "85226": {
        "name": "Chandler N / Tech Corridor",
        "lat": 33.3170, "lng": -111.9460,
        "median_home_value": 540_000, "median_rent_monthly": 2_300,
        "crime_index": 25, "pct_bachelors": 55,
        "median_household_income": 105_000, "population": 50_500,
        "walk_score": 35, "restaurant_score": 55,
        "tags": ["family", "tech", "newer"],
    },
    "85008": {
        "name": "East Phoenix",
        "lat": 33.4640, "lng": -111.9930,
        "median_home_value": 360_000, "median_rent_monthly": 1_700,
        "crime_index": 70, "pct_bachelors": 22,
        "median_household_income": 50_000, "population": 50_000,
        "walk_score": 50, "restaurant_score": 45,
        "tags": ["affordable", "high-cap-rate", "improving"],
    },
    "85033": {
        "name": "W Phoenix / Maryvale",
        "lat": 33.4820, "lng": -112.1730,
        "median_home_value": 320_000, "median_rent_monthly": 1_700,
        "crime_index": 78, "pct_bachelors": 10,
        "median_household_income": 48_000, "population": 73_500,
        "walk_score": 45, "restaurant_score": 35,
        "tags": ["high-cap-rate", "high-crime"],
    },
    "85308": {
        "name": "Glendale / Arrowhead",
        "lat": 33.6500, "lng": -112.1810,
        "median_home_value": 470_000, "median_rent_monthly": 2_100,
        "crime_index": 35, "pct_bachelors": 35,
        "median_household_income": 82_000, "population": 60_000,
        "walk_score": 30, "restaurant_score": 45,
        "tags": ["family", "suburb"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Per-metro registry. Each entry maps a state code to a metro deep-dive that
# the /real-estate/{state}/map route can render. TX uses the existing Dallas
# County dataset from dallas_neighborhoods.py so weights/data stay in one
# place.
# ─────────────────────────────────────────────────────────────────────────────
STATE_METROS: dict[str, dict] = {
    "TX": {
        "metro_label": "Dallas County",
        "subtitle": "ZIP-level cap rate, crime, schools, income & affordability for Dallas County — composite weighted for an SFR investor lens.",
        "map_center": {"lat": 32.78, "lng": -96.80, "zoom": 11},
        "zips": DALLAS_ZIPS,
        "extra_caveats": [
            "ZIP codes are USPS routes, not true neighborhoods — Bishop Arts and parts of Oak Cliff share ZIPs.",
        ],
    },
    "CA": {
        "metro_label": "Los Angeles County",
        "subtitle": "ZIP-level scores for greater Los Angeles — beach, hills, urban core, and high-cap-rate South LA submarkets.",
        "map_center": {"lat": 34.05, "lng": -118.30, "zoom": 10},
        "zips": LA_ZIPS,
        "extra_caveats": [
            "LA ZIPs span huge populations — 90011 alone has ~110K residents and is heterogeneous.",
            "Beach ZIPs (90291, 90402) trade at a beach premium that distorts cap-rate scoring vs. hill/inland ZIPs.",
        ],
    },
    "AZ": {
        "metro_label": "Maricopa County",
        "subtitle": "ZIP-level scores for the Phoenix metro — Phoenix proper, Scottsdale, Tempe, Chandler, Glendale.",
        "map_center": {"lat": 33.50, "lng": -112.00, "zoom": 10},
        "zips": PHOENIX_ZIPS,
        "extra_caveats": [
            "Phoenix metro spans multiple cities — Scottsdale and Chandler PD report separately from Phoenix PD; the crime index is normalized across all of them but precision varies.",
            "A few Scottsdale ZIPs (85254, 85266) bridge city limits — values reflect the population centroid.",
        ],
    },
}


def list_supported_states() -> list[str]:
    """State codes that have a metro deep-dive available."""
    return list(STATE_METROS.keys())


def get_state_neighborhoods(state: str) -> dict | None:
    """Return enriched neighborhoods for a state's primary metro, or None
    if the state has no deep-dive yet. Same shape as the legacy
    get_dallas_neighborhoods() output so the Leaflet map template can stay
    generic."""
    state = state.upper()
    metro = STATE_METROS.get(state)
    if metro is None:
        return None

    enriched = []
    for zip_code, raw in metro["zips"].items():
        metrics = compute_zip_metrics(raw)
        enriched.append({"zip": zip_code, **raw, **metrics})
    enriched.sort(key=lambda x: x["composite_score"], reverse=True)

    base_caveats = [
        "% bachelor's+ is a school-quality proxy. Direct district / accountability ratings would be more accurate.",
        "Crime index is a relative ranking inside the metro, not a per-capita rate.",
        "Walkability + restaurant scores are approximations from public Walk Score / Yelp data; live APIs are a follow-up.",
        f"Snapshot from {DATA_AS_OF}. Refresh sources annually.",
    ]
    return {
        "state": state,
        "metro_label": metro["metro_label"],
        "subtitle": metro["subtitle"],
        "map_center": metro["map_center"],
        "as_of": DATA_AS_OF,
        "personas": PERSONAS,
        "default_persona": DEFAULT_PERSONA,
        "neighborhoods": enriched,
        "sources": {
            "home_value_rent": "Zillow Research (ZHVI / ZORI public data)",
            "crime_index": "Local PD open-data portals (normalized 0-100)",
            "education_income_population": "U.S. Census ACS 2022 5-year",
            "walkability": "Walk Score (public city/neighborhood scores; live API is a follow-up)",
            "restaurants": "Yelp Fusion 4+ star count within ~0.5mi (snapshot; live API is a follow-up)",
        },
        "caveats": metro.get("extra_caveats", []) + base_caveats,
    }
