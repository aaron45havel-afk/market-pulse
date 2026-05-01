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
    _ZILLOW_OVERRIDES,
    _apply_zillow_overrides,
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
# UT — Utah County (Provo / Orem / Silicon Slopes corridor, south of SLC)
# ─────────────────────────────────────────────────────────────────────────────
PROVO_ZIPS: dict[str, dict] = {
    "84601": {
        "name": "Provo Downtown / BYU adj",
        "lat": 40.2338, "lng": -111.6585,
        "median_home_value": 430_000, "median_rent_monthly": 1_800,
        "crime_index": 38, "pct_bachelors": 65,
        "median_household_income": 55_000, "population": 38_000,
        "walk_score": 65, "restaurant_score": 60,
        "tags": ["college", "rental-demand", "walkable"],
    },
    "84604": {
        "name": "Provo N / Edgemont",
        "lat": 40.2680, "lng": -111.6500,
        "median_home_value": 580_000, "median_rent_monthly": 2_200,
        "crime_index": 25, "pct_bachelors": 70,
        "median_household_income": 80_000, "population": 32_000,
        "walk_score": 45, "restaurant_score": 50,
        "tags": ["family", "low-crime", "established"],
    },
    "84606": {
        "name": "East Provo / Provost",
        "lat": 40.2280, "lng": -111.6300,
        "median_home_value": 480_000, "median_rent_monthly": 1_900,
        "crime_index": 35, "pct_bachelors": 50,
        "median_household_income": 65_000, "population": 26_000,
        "walk_score": 50, "restaurant_score": 45,
        "tags": ["established", "mid-tier"],
    },
    "84057": {
        "name": "Orem",
        "lat": 40.2840, "lng": -111.7000,
        "median_home_value": 510_000, "median_rent_monthly": 2_000,
        "crime_index": 30, "pct_bachelors": 45,
        "median_household_income": 72_000, "population": 44_000,
        "walk_score": 45, "restaurant_score": 55,
        "tags": ["family", "mid-tier"],
    },
    "84058": {
        "name": "Orem N / UVU",
        "lat": 40.3060, "lng": -111.7050,
        "median_home_value": 540_000, "median_rent_monthly": 2_000,
        "crime_index": 28, "pct_bachelors": 50,
        "median_household_income": 75_000, "population": 36_000,
        "walk_score": 40, "restaurant_score": 50,
        "tags": ["college", "family"],
    },
    "84097": {
        "name": "N Orem / Lindon",
        "lat": 40.3260, "lng": -111.7100,
        "median_home_value": 620_000, "median_rent_monthly": 2_200,
        "crime_index": 22, "pct_bachelors": 55,
        "median_household_income": 92_000, "population": 24_000,
        "walk_score": 30, "restaurant_score": 35,
        "tags": ["family", "low-crime"],
    },
    "84003": {
        "name": "American Fork",
        "lat": 40.3770, "lng": -111.7950,
        "median_home_value": 580_000, "median_rent_monthly": 2_200,
        "crime_index": 25, "pct_bachelors": 50,
        "median_household_income": 98_000, "population": 41_000,
        "walk_score": 30, "restaurant_score": 40,
        "tags": ["family", "growth"],
    },
    "84043": {
        "name": "Lehi / Silicon Slopes",
        "lat": 40.3915, "lng": -111.8505,
        "median_home_value": 720_000, "median_rent_monthly": 2_500,
        "crime_index": 18, "pct_bachelors": 60,
        "median_household_income": 130_000, "population": 90_000,
        "walk_score": 25, "restaurant_score": 50,
        "tags": ["tech", "family", "low-crime", "growth"],
    },
    "84005": {
        "name": "Eagle Mountain / Saratoga Springs",
        "lat": 40.3140, "lng": -111.9000,
        "median_home_value": 560_000, "median_rent_monthly": 2_300,
        "crime_index": 18, "pct_bachelors": 45,
        "median_household_income": 115_000, "population": 56_000,
        "walk_score": 15, "restaurant_score": 25,
        "tags": ["family", "growth", "newer"],
    },
    "84062": {
        "name": "Pleasant Grove",
        "lat": 40.3640, "lng": -111.7370,
        "median_home_value": 560_000, "median_rent_monthly": 2_200,
        "crime_index": 22, "pct_bachelors": 48,
        "median_household_income": 98_000, "population": 41_000,
        "walk_score": 30, "restaurant_score": 40,
        "tags": ["family", "established", "low-crime"],
    },
    "84660": {
        "name": "Spanish Fork",
        "lat": 40.1150, "lng": -111.6550,
        "median_home_value": 480_000, "median_rent_monthly": 2_000,
        "crime_index": 25, "pct_bachelors": 38,
        "median_household_income": 90_000, "population": 47_000,
        "walk_score": 25, "restaurant_score": 35,
        "tags": ["family", "affordable"],
    },
    "84663": {
        "name": "Springville",
        "lat": 40.1648, "lng": -111.6107,
        "median_home_value": 510_000, "median_rent_monthly": 2_100,
        "crime_index": 25, "pct_bachelors": 40,
        "median_household_income": 88_000, "population": 36_000,
        "walk_score": 30, "restaurant_score": 35,
        "tags": ["family", "established"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# NV — Las Vegas valley (Clark County): Strip, Summerlin, Henderson, NLV
# ─────────────────────────────────────────────────────────────────────────────
LAS_VEGAS_ZIPS: dict[str, dict] = {
    "89109": {
        "name": "The Strip / Resort Corridor",
        "lat": 36.1147, "lng": -115.1728,
        "median_home_value": 350_000, "median_rent_monthly": 1_900,
        "crime_index": 80, "pct_bachelors": 30,
        "median_household_income": 48_000, "population": 5_500,
        "walk_score": 75, "restaurant_score": 95,
        "tags": ["tourist", "high-density", "high-crime"],
    },
    "89102": {
        "name": "Westgate / Chinatown",
        "lat": 36.1500, "lng": -115.1835,
        "median_home_value": 280_000, "median_rent_monthly": 1_500,
        "crime_index": 70, "pct_bachelors": 22,
        "median_household_income": 45_000, "population": 39_000,
        "walk_score": 60, "restaurant_score": 78,
        "tags": ["affordable", "high-cap-rate"],
    },
    "89117": {
        "name": "Spring Valley",
        "lat": 36.1380, "lng": -115.2820,
        "median_home_value": 430_000, "median_rent_monthly": 2_000,
        "crime_index": 45, "pct_bachelors": 35,
        "median_household_income": 75_000, "population": 51_000,
        "walk_score": 35, "restaurant_score": 50,
        "tags": ["family", "mid-tier"],
    },
    "89148": {
        "name": "Spring Valley S / Mountains Edge",
        "lat": 36.0500, "lng": -115.2950,
        "median_home_value": 480_000, "median_rent_monthly": 2_200,
        "crime_index": 35, "pct_bachelors": 38,
        "median_household_income": 92_000, "population": 64_000,
        "walk_score": 28, "restaurant_score": 40,
        "tags": ["family", "newer", "low-crime"],
    },
    "89134": {
        "name": "Summerlin / The Lakes",
        "lat": 36.1925, "lng": -115.3065,
        "median_home_value": 620_000, "median_rent_monthly": 2_500,
        "crime_index": 22, "pct_bachelors": 55,
        "median_household_income": 110_000, "population": 22_000,
        "walk_score": 30, "restaurant_score": 45,
        "tags": ["upscale", "low-crime", "family"],
    },
    "89135": {
        "name": "Summerlin S / The Vistas",
        "lat": 36.1480, "lng": -115.3290,
        "median_home_value": 720_000, "median_rent_monthly": 2_800,
        "crime_index": 20, "pct_bachelors": 60,
        "median_household_income": 130_000, "population": 30_000,
        "walk_score": 30, "restaurant_score": 55,
        "tags": ["upscale", "low-crime", "family"],
    },
    "89138": {
        "name": "Summerlin W / The Cliffs",
        "lat": 36.1745, "lng": -115.3520,
        "median_home_value": 850_000, "median_rent_monthly": 3_000,
        "crime_index": 15, "pct_bachelors": 65,
        "median_household_income": 145_000, "population": 14_000,
        "walk_score": 25, "restaurant_score": 50,
        "tags": ["luxury", "low-crime"],
    },
    "89052": {
        "name": "Henderson / Anthem",
        "lat": 36.0030, "lng": -115.0450,
        "median_home_value": 540_000, "median_rent_monthly": 2_300,
        "crime_index": 25, "pct_bachelors": 50,
        "median_household_income": 105_000, "population": 56_000,
        "walk_score": 25, "restaurant_score": 45,
        "tags": ["family", "low-crime"],
    },
    "89074": {
        "name": "Henderson / Green Valley",
        "lat": 36.0367, "lng": -115.0610,
        "median_home_value": 460_000, "median_rent_monthly": 2_100,
        "crime_index": 30, "pct_bachelors": 42,
        "median_household_income": 88_000, "population": 39_000,
        "walk_score": 35, "restaurant_score": 55,
        "tags": ["family", "established"],
    },
    "89014": {
        "name": "Henderson E / Whitney Ranch",
        "lat": 36.0540, "lng": -115.0420,
        "median_home_value": 410_000, "median_rent_monthly": 1_950,
        "crime_index": 38, "pct_bachelors": 32,
        "median_household_income": 72_000, "population": 49_000,
        "walk_score": 40, "restaurant_score": 50,
        "tags": ["mid-tier"],
    },
    "89030": {
        "name": "N Las Vegas / Cheyenne",
        "lat": 36.2090, "lng": -115.1240,
        "median_home_value": 320_000, "median_rent_monthly": 1_700,
        "crime_index": 78, "pct_bachelors": 12,
        "median_household_income": 45_000, "population": 56_000,
        "walk_score": 45, "restaurant_score": 35,
        "tags": ["high-cap-rate", "high-crime"],
    },
    "89106": {
        "name": "W Las Vegas",
        "lat": 36.1810, "lng": -115.1700,
        "median_home_value": 290_000, "median_rent_monthly": 1_500,
        "crime_index": 82, "pct_bachelors": 14,
        "median_household_income": 38_000, "population": 31_000,
        "walk_score": 55, "restaurant_score": 40,
        "tags": ["high-cap-rate", "high-crime"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# NV — Reno / Sparks (Washoe County): UNR, downtown, Caughlin, Spanish Springs
# ─────────────────────────────────────────────────────────────────────────────
RENO_ZIPS: dict[str, dict] = {
    "89501": {
        "name": "Downtown Reno",
        "lat": 39.5240, "lng": -119.8138,
        "median_home_value": 400_000, "median_rent_monthly": 1_800,
        "crime_index": 65, "pct_bachelors": 42,
        "median_household_income": 50_000, "population": 4_000,
        "walk_score": 75, "restaurant_score": 75,
        "tags": ["urban", "walkable", "gentrifying"],
    },
    "89502": {
        "name": "East Reno / UNR-adj",
        "lat": 39.5070, "lng": -119.7740,
        "median_home_value": 410_000, "median_rent_monthly": 1_800,
        "crime_index": 55, "pct_bachelors": 38,
        "median_household_income": 58_000, "population": 38_000,
        "walk_score": 50, "restaurant_score": 50,
        "tags": ["college", "mid-tier"],
    },
    "89503": {
        "name": "NW Reno",
        "lat": 39.5400, "lng": -119.8400,
        "median_home_value": 480_000, "median_rent_monthly": 1_950,
        "crime_index": 38, "pct_bachelors": 45,
        "median_household_income": 72_000, "population": 36_000,
        "walk_score": 45, "restaurant_score": 45,
        "tags": ["family", "established"],
    },
    "89509": {
        "name": "SW Reno / Plumas",
        "lat": 39.5000, "lng": -119.8400,
        "median_home_value": 620_000, "median_rent_monthly": 2_300,
        "crime_index": 28, "pct_bachelors": 60,
        "median_household_income": 105_000, "population": 28_000,
        "walk_score": 35, "restaurant_score": 45,
        "tags": ["upscale", "low-crime", "family"],
    },
    "89519": {
        "name": "SW Reno / Caughlin Ranch",
        "lat": 39.4900, "lng": -119.8650,
        "median_home_value": 850_000, "median_rent_monthly": 2_800,
        "crime_index": 18, "pct_bachelors": 70,
        "median_household_income": 135_000, "population": 13_000,
        "walk_score": 25, "restaurant_score": 40,
        "tags": ["luxury", "low-crime", "family"],
    },
    "89511": {
        "name": "South Reno / Mt Rose Hwy",
        "lat": 39.4180, "lng": -119.7930,
        "median_home_value": 780_000, "median_rent_monthly": 2_700,
        "crime_index": 18, "pct_bachelors": 65,
        "median_household_income": 125_000, "population": 25_000,
        "walk_score": 18, "restaurant_score": 35,
        "tags": ["upscale", "newer", "scenic"],
    },
    "89523": {
        "name": "NW Reno / Somersett",
        "lat": 39.5670, "lng": -119.9100,
        "median_home_value": 620_000, "median_rent_monthly": 2_400,
        "crime_index": 22, "pct_bachelors": 55,
        "median_household_income": 115_000, "population": 20_000,
        "walk_score": 25, "restaurant_score": 35,
        "tags": ["family", "newer", "low-crime"],
    },
    "89506": {
        "name": "N Reno / Stead",
        "lat": 39.6700, "lng": -119.8480,
        "median_home_value": 410_000, "median_rent_monthly": 1_800,
        "crime_index": 42, "pct_bachelors": 28,
        "median_household_income": 68_000, "population": 49_000,
        "walk_score": 25, "restaurant_score": 30,
        "tags": ["affordable", "growth"],
    },
    "89521": {
        "name": "SE Reno / Damonte Ranch",
        "lat": 39.4570, "lng": -119.7480,
        "median_home_value": 580_000, "median_rent_monthly": 2_300,
        "crime_index": 22, "pct_bachelors": 50,
        "median_household_income": 108_000, "population": 32_000,
        "walk_score": 25, "restaurant_score": 40,
        "tags": ["family", "newer", "growth"],
    },
    "89436": {
        "name": "Sparks / Spanish Springs",
        "lat": 39.6360, "lng": -119.7150,
        "median_home_value": 510_000, "median_rent_monthly": 2_100,
        "crime_index": 28, "pct_bachelors": 38,
        "median_household_income": 92_000, "population": 47_000,
        "walk_score": 22, "restaurant_score": 35,
        "tags": ["family", "newer"],
    },
    "89431": {
        "name": "Sparks Central",
        "lat": 39.5460, "lng": -119.7430,
        "median_home_value": 370_000, "median_rent_monthly": 1_700,
        "crime_index": 50, "pct_bachelors": 22,
        "median_household_income": 58_000, "population": 40_000,
        "walk_score": 45, "restaurant_score": 45,
        "tags": ["affordable", "mid-tier"],
    },
    "89434": {
        "name": "Sparks E / Industrial",
        "lat": 39.5340, "lng": -119.7160,
        "median_home_value": 440_000, "median_rent_monthly": 1_900,
        "crime_index": 38, "pct_bachelors": 28,
        "median_household_income": 70_000, "population": 35_000,
        "walk_score": 30, "restaurant_score": 35,
        "tags": ["mid-tier", "industrial"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# NV — Carson City (consolidated city / county): state capital, retiree-heavy
# ─────────────────────────────────────────────────────────────────────────────
CARSON_CITY_ZIPS: dict[str, dict] = {
    "89701": {
        "name": "Central Carson City",
        "lat": 39.1638, "lng": -119.7674,
        "median_home_value": 440_000, "median_rent_monthly": 1_800,
        "crime_index": 50, "pct_bachelors": 32,
        "median_household_income": 60_000, "population": 19_000,
        "walk_score": 55, "restaurant_score": 60,
        "tags": ["urban", "walkable", "established"],
    },
    "89703": {
        "name": "W Carson City",
        "lat": 39.1620, "lng": -119.7950,
        "median_home_value": 510_000, "median_rent_monthly": 1_950,
        "crime_index": 30, "pct_bachelors": 38,
        "median_household_income": 72_000, "population": 9_500,
        "walk_score": 30, "restaurant_score": 35,
        "tags": ["family", "established", "low-crime"],
    },
    "89705": {
        "name": "S Carson / Indian Hills",
        "lat": 39.0950, "lng": -119.7900,
        "median_home_value": 560_000, "median_rent_monthly": 2_100,
        "crime_index": 28, "pct_bachelors": 35,
        "median_household_income": 82_000, "population": 14_500,
        "walk_score": 18, "restaurant_score": 25,
        "tags": ["family", "newer", "rural-edge"],
    },
    "89706": {
        "name": "E Carson / Industrial",
        "lat": 39.2050, "lng": -119.7300,
        "median_home_value": 380_000, "median_rent_monthly": 1_700,
        "crime_index": 42, "pct_bachelors": 22,
        "median_household_income": 58_000, "population": 16_000,
        "walk_score": 25, "restaurant_score": 30,
        "tags": ["affordable", "industrial"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# FL — Miami-Dade County: Brickell, Beach, Coral Gables, Kendall, Aventura
# ─────────────────────────────────────────────────────────────────────────────
MIAMI_ZIPS: dict[str, dict] = {
    "33131": {
        "name": "Brickell",
        "lat": 25.7615, "lng": -80.1918,
        "median_home_value": 720_000, "median_rent_monthly": 4_200,
        "crime_index": 55, "pct_bachelors": 75,
        "median_household_income": 130_000, "population": 32_000,
        "walk_score": 90, "restaurant_score": 90,
        "tags": ["urban", "walkable", "luxury", "financial"],
    },
    "33133": {
        "name": "Coconut Grove",
        "lat": 25.7282, "lng": -80.2435,
        "median_home_value": 1_200_000, "median_rent_monthly": 4_800,
        "crime_index": 38, "pct_bachelors": 70,
        "median_household_income": 145_000, "population": 22_000,
        "walk_score": 70, "restaurant_score": 75,
        "tags": ["walkable", "upscale", "established"],
    },
    "33134": {
        "name": "Coral Gables",
        "lat": 25.7493, "lng": -80.2612,
        "median_home_value": 1_500_000, "median_rent_monthly": 4_500,
        "crime_index": 28, "pct_bachelors": 75,
        "median_household_income": 165_000, "population": 50_000,
        "walk_score": 60, "restaurant_score": 70,
        "tags": ["upscale", "low-crime", "top-schools"],
    },
    "33139": {
        "name": "South Beach",
        "lat": 25.7820, "lng": -80.1340,
        "median_home_value": 580_000, "median_rent_monthly": 3_800,
        "crime_index": 60, "pct_bachelors": 60,
        "median_household_income": 80_000, "population": 41_000,
        "walk_score": 90, "restaurant_score": 95,
        "tags": ["beach", "walkable", "high-density"],
    },
    "33140": {
        "name": "Mid-Beach",
        "lat": 25.8170, "lng": -80.1280,
        "median_home_value": 620_000, "median_rent_monthly": 4_000,
        "crime_index": 38, "pct_bachelors": 55,
        "median_household_income": 90_000, "population": 25_000,
        "walk_score": 75, "restaurant_score": 75,
        "tags": ["beach", "residential"],
    },
    "33156": {
        "name": "Pinecrest / Palmetto Bay",
        "lat": 25.6700, "lng": -80.3000,
        "median_home_value": 2_200_000, "median_rent_monthly": 5_500,
        "crime_index": 18, "pct_bachelors": 70,
        "median_household_income": 180_000, "population": 18_000,
        "walk_score": 25, "restaurant_score": 35,
        "tags": ["luxury", "low-crime", "top-schools", "family"],
    },
    "33186": {
        "name": "Kendall",
        "lat": 25.6510, "lng": -80.4290,
        "median_home_value": 480_000, "median_rent_monthly": 2_800,
        "crime_index": 35, "pct_bachelors": 38,
        "median_household_income": 80_000, "population": 65_000,
        "walk_score": 30, "restaurant_score": 50,
        "tags": ["family", "established", "mid-tier"],
    },
    "33180": {
        "name": "Aventura",
        "lat": 25.9560, "lng": -80.1390,
        "median_home_value": 640_000, "median_rent_monthly": 3_200,
        "crime_index": 22, "pct_bachelors": 60,
        "median_household_income": 110_000, "population": 39_000,
        "walk_score": 70, "restaurant_score": 75,
        "tags": ["luxury", "low-crime", "retiree"],
    },
    "33161": {
        "name": "North Miami",
        "lat": 25.8920, "lng": -80.1830,
        "median_home_value": 360_000, "median_rent_monthly": 2_300,
        "crime_index": 60, "pct_bachelors": 32,
        "median_household_income": 58_000, "population": 46_000,
        "walk_score": 55, "restaurant_score": 50,
        "tags": ["affordable", "high-cap-rate"],
    },
    "33126": {
        "name": "W Miami / Doral S",
        "lat": 25.7775, "lng": -80.3060,
        "median_home_value": 580_000, "median_rent_monthly": 3_200,
        "crime_index": 30, "pct_bachelors": 50,
        "median_household_income": 95_000, "population": 49_000,
        "walk_score": 40, "restaurant_score": 60,
        "tags": ["family", "newer", "growth"],
    },
    "33172": {
        "name": "Doral / Sweetwater",
        "lat": 25.7900, "lng": -80.3700,
        "median_home_value": 540_000, "median_rent_monthly": 3_000,
        "crime_index": 32, "pct_bachelors": 48,
        "median_household_income": 90_000, "population": 45_000,
        "walk_score": 35, "restaurant_score": 50,
        "tags": ["family", "newer"],
    },
    "33125": {
        "name": "Little Havana",
        "lat": 25.7740, "lng": -80.2300,
        "median_home_value": 420_000, "median_rent_monthly": 2_400,
        "crime_index": 65, "pct_bachelors": 30,
        "median_household_income": 52_000, "population": 47_000,
        "walk_score": 70, "restaurant_score": 75,
        "tags": ["urban", "high-density", "high-cap-rate"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# FL — Orange County (Orlando metro): Downtown, Winter Park, Lake Nona, Dr Phillips
# ─────────────────────────────────────────────────────────────────────────────
ORLANDO_ZIPS: dict[str, dict] = {
    "32801": {
        "name": "Downtown Orlando / Lake Eola",
        "lat": 28.5400, "lng": -81.3790,
        "median_home_value": 440_000, "median_rent_monthly": 2_200,
        "crime_index": 60, "pct_bachelors": 58,
        "median_household_income": 72_000, "population": 14_000,
        "walk_score": 80, "restaurant_score": 80,
        "tags": ["urban", "walkable", "transit"],
    },
    "32803": {
        "name": "Audubon Park / Mills 50",
        "lat": 28.5570, "lng": -81.3500,
        "median_home_value": 510_000, "median_rent_monthly": 2_200,
        "crime_index": 45, "pct_bachelors": 60,
        "median_household_income": 85_000, "population": 22_000,
        "walk_score": 65, "restaurant_score": 80,
        "tags": ["walkable", "hip", "established"],
    },
    "32804": {
        "name": "College Park",
        "lat": 28.5750, "lng": -81.3990,
        "median_home_value": 560_000, "median_rent_monthly": 2_300,
        "crime_index": 38, "pct_bachelors": 60,
        "median_household_income": 92_000, "population": 18_000,
        "walk_score": 55, "restaurant_score": 65,
        "tags": ["family", "walkable", "established"],
    },
    "32806": {
        "name": "SODO / Delaney Park",
        "lat": 28.5180, "lng": -81.3690,
        "median_home_value": 480_000, "median_rent_monthly": 2_100,
        "crime_index": 40, "pct_bachelors": 55,
        "median_household_income": 88_000, "population": 24_000,
        "walk_score": 50, "restaurant_score": 60,
        "tags": ["family", "mid-upper"],
    },
    "32814": {
        "name": "Baldwin Park",
        "lat": 28.5660, "lng": -81.3260,
        "median_home_value": 720_000, "median_rent_monthly": 2_800,
        "crime_index": 22, "pct_bachelors": 70,
        "median_household_income": 130_000, "population": 12_000,
        "walk_score": 60, "restaurant_score": 60,
        "tags": ["family", "newer", "low-crime", "walkable"],
    },
    "32827": {
        "name": "Lake Nona / Medical City",
        "lat": 28.4140, "lng": -81.3010,
        "median_home_value": 620_000, "median_rent_monthly": 2_600,
        "crime_index": 22, "pct_bachelors": 60,
        "median_household_income": 115_000, "population": 20_000,
        "walk_score": 25, "restaurant_score": 35,
        "tags": ["family", "newer", "growth", "low-crime"],
    },
    "32825": {
        "name": "East Orlando / UCF-adj",
        "lat": 28.5330, "lng": -81.2680,
        "median_home_value": 410_000, "median_rent_monthly": 2_100,
        "crime_index": 42, "pct_bachelors": 38,
        "median_household_income": 72_000, "population": 56_000,
        "walk_score": 30, "restaurant_score": 45,
        "tags": ["college", "family", "mid-tier"],
    },
    "32836": {
        "name": "Bay Hill / Dr Phillips",
        "lat": 28.4540, "lng": -81.5100,
        "median_home_value": 1_000_000, "median_rent_monthly": 4_200,
        "crime_index": 22, "pct_bachelors": 70,
        "median_household_income": 150_000, "population": 27_000,
        "walk_score": 25, "restaurant_score": 60,
        "tags": ["luxury", "low-crime", "family"],
    },
    "32837": {
        "name": "Hunters Creek",
        "lat": 28.4060, "lng": -81.4310,
        "median_home_value": 480_000, "median_rent_monthly": 2_400,
        "crime_index": 30, "pct_bachelors": 45,
        "median_household_income": 90_000, "population": 26_000,
        "walk_score": 30, "restaurant_score": 40,
        "tags": ["family", "established"],
    },
    "32839": {
        "name": "SW Orlando",
        "lat": 28.4720, "lng": -81.4220,
        "median_home_value": 340_000, "median_rent_monthly": 1_800,
        "crime_index": 65, "pct_bachelors": 22,
        "median_household_income": 52_000, "population": 50_000,
        "walk_score": 35, "restaurant_score": 35,
        "tags": ["affordable", "high-cap-rate"],
    },
    "32789": {
        "name": "Winter Park",
        "lat": 28.5970, "lng": -81.3550,
        "median_home_value": 850_000, "median_rent_monthly": 3_000,
        "crime_index": 22, "pct_bachelors": 75,
        "median_household_income": 135_000, "population": 30_000,
        "walk_score": 60, "restaurant_score": 70,
        "tags": ["upscale", "low-crime", "top-schools", "walkable"],
    },
    "32835": {
        "name": "Metro West",
        "lat": 28.5300, "lng": -81.4690,
        "median_home_value": 390_000, "median_rent_monthly": 2_000,
        "crime_index": 45, "pct_bachelors": 42,
        "median_household_income": 68_000, "population": 50_000,
        "walk_score": 35, "restaurant_score": 50,
        "tags": ["family", "mid-tier"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# UT — Wasatch Front North (Salt Lake + Davis + Weber + Cache counties)
# Covers SLC core, Bountiful, Farmington, Ogden, Logan. Logan is ~80mi north
# of SLC so the map zoom is wide; treat Cache County as a separate submarket.
# ─────────────────────────────────────────────────────────────────────────────
WASATCH_NORTH_ZIPS: dict[str, dict] = {
    # Salt Lake City (Salt Lake County)
    "84101": {
        "name": "Downtown SLC",
        "lat": 40.7589, "lng": -111.8883,
        "median_home_value": 480_000, "median_rent_monthly": 1_950,
        "crime_index": 65, "pct_bachelors": 50,
        "median_household_income": 58_000, "population": 8_500,
        "walk_score": 80, "restaurant_score": 85,
        "tags": ["urban", "walkable", "transit"],
    },
    "84102": {
        "name": "East Downtown / U of U",
        "lat": 40.7637, "lng": -111.8595,
        "median_home_value": 620_000, "median_rent_monthly": 2_100,
        "crime_index": 50, "pct_bachelors": 75,
        "median_household_income": 72_000, "population": 19_000,
        "walk_score": 75, "restaurant_score": 70,
        "tags": ["college", "walkable", "rental-demand"],
    },
    "84103": {
        "name": "Avenues / Capitol Hill",
        "lat": 40.7860, "lng": -111.8810,
        "median_home_value": 720_000, "median_rent_monthly": 2_200,
        "crime_index": 35, "pct_bachelors": 70,
        "median_household_income": 92_000, "population": 18_500,
        "walk_score": 65, "restaurant_score": 55,
        "tags": ["historic", "walkable", "established"],
    },
    "84105": {
        "name": "Sugar House",
        "lat": 40.7263, "lng": -111.8632,
        "median_home_value": 680_000, "median_rent_monthly": 2_200,
        "crime_index": 45, "pct_bachelors": 65,
        "median_household_income": 85_000, "population": 24_000,
        "walk_score": 70, "restaurant_score": 75,
        "tags": ["walkable", "hip", "established"],
    },
    "84108": {
        "name": "Federal Heights / E Bench",
        "lat": 40.7693, "lng": -111.8410,
        "median_home_value": 1_150_000, "median_rent_monthly": 2_800,
        "crime_index": 22, "pct_bachelors": 85,
        "median_household_income": 145_000, "population": 18_500,
        "walk_score": 35, "restaurant_score": 40,
        "tags": ["top-tier", "low-crime", "schools"],
    },
    "84109": {
        "name": "Holladay",
        "lat": 40.6603, "lng": -111.8210,
        "median_home_value": 850_000, "median_rent_monthly": 2_500,
        "crime_index": 25, "pct_bachelors": 65,
        "median_household_income": 115_000, "population": 26_500,
        "walk_score": 35, "restaurant_score": 50,
        "tags": ["upscale", "family", "low-crime"],
    },
    # Davis County (Bountiful, Centerville, Farmington, Kaysville)
    "84010": {
        "name": "Bountiful",
        "lat": 40.8898, "lng": -111.8807,
        "median_home_value": 620_000, "median_rent_monthly": 2_300,
        "crime_index": 28, "pct_bachelors": 50,
        "median_household_income": 98_000, "population": 43_500,
        "walk_score": 35, "restaurant_score": 45,
        "tags": ["family", "established", "low-crime"],
    },
    "84014": {
        "name": "Centerville",
        "lat": 40.9183, "lng": -111.8723,
        "median_home_value": 580_000, "median_rent_monthly": 2_200,
        "crime_index": 25, "pct_bachelors": 48,
        "median_household_income": 95_000, "population": 17_500,
        "walk_score": 30, "restaurant_score": 35,
        "tags": ["family", "established"],
    },
    "84025": {
        "name": "Farmington",
        "lat": 40.9818, "lng": -111.8870,
        "median_home_value": 720_000, "median_rent_monthly": 2_400,
        "crime_index": 22, "pct_bachelors": 55,
        "median_household_income": 115_000, "population": 25_000,
        "walk_score": 30, "restaurant_score": 50,
        "tags": ["family", "newer", "low-crime"],
    },
    "84037": {
        "name": "Kaysville",
        "lat": 41.0353, "lng": -111.9385,
        "median_home_value": 620_000, "median_rent_monthly": 2_200,
        "crime_index": 22, "pct_bachelors": 50,
        "median_household_income": 108_000, "population": 35_000,
        "walk_score": 28, "restaurant_score": 40,
        "tags": ["family", "low-crime", "established"],
    },
    # Weber County (Ogden)
    "84401": {
        "name": "Downtown Ogden / 25th St",
        "lat": 41.2230, "lng": -111.9738,
        "median_home_value": 340_000, "median_rent_monthly": 1_600,
        "crime_index": 60, "pct_bachelors": 32,
        "median_household_income": 48_000, "population": 16_000,
        "walk_score": 70, "restaurant_score": 70,
        "tags": ["walkable", "gentrifying", "affordable"],
    },
    "84403": {
        "name": "South Ogden",
        "lat": 41.1800, "lng": -111.9450,
        "median_home_value": 420_000, "median_rent_monthly": 1_800,
        "crime_index": 40, "pct_bachelors": 35,
        "median_household_income": 72_000, "population": 38_500,
        "walk_score": 30, "restaurant_score": 35,
        "tags": ["family", "mid-tier"],
    },
    "84404": {
        "name": "North Ogden / Pleasant View",
        "lat": 41.2860, "lng": -112.0130,
        "median_home_value": 470_000, "median_rent_monthly": 1_900,
        "crime_index": 35, "pct_bachelors": 32,
        "median_household_income": 78_000, "population": 41_000,
        "walk_score": 25, "restaurant_score": 30,
        "tags": ["family", "established"],
    },
    "84405": {
        "name": "Riverdale / S Ogden",
        "lat": 41.1690, "lng": -112.0010,
        "median_home_value": 380_000, "median_rent_monthly": 1_750,
        "crime_index": 50, "pct_bachelors": 25,
        "median_household_income": 62_000, "population": 31_000,
        "walk_score": 35, "restaurant_score": 45,
        "tags": ["affordable", "mid-tier"],
    },
    # Cache County (Logan)
    "84321": {
        "name": "Logan / USU",
        "lat": 41.7355, "lng": -111.8338,
        "median_home_value": 400_000, "median_rent_monthly": 1_700,
        "crime_index": 30, "pct_bachelors": 55,
        "median_household_income": 52_000, "population": 36_000,
        "walk_score": 55, "restaurant_score": 55,
        "tags": ["college", "rental-demand", "affordable"],
    },
    "84341": {
        "name": "North Logan / Hyde Park",
        "lat": 41.7700, "lng": -111.8050,
        "median_home_value": 480_000, "median_rent_monthly": 1_900,
        "crime_index": 18, "pct_bachelors": 50,
        "median_household_income": 78_000, "population": 17_500,
        "walk_score": 25, "restaurant_score": 30,
        "tags": ["family", "low-crime"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# UT — Washington County (St. George / Ivins / Hurricane — "Southern Utah")
# ─────────────────────────────────────────────────────────────────────────────
ST_GEORGE_ZIPS: dict[str, dict] = {
    "84770": {
        "name": "St. George Central / W",
        "lat": 37.0965, "lng": -113.5684,
        "median_home_value": 480_000, "median_rent_monthly": 1_900,
        "crime_index": 35, "pct_bachelors": 38,
        "median_household_income": 68_000, "population": 32_000,
        "walk_score": 50, "restaurant_score": 60,
        "tags": ["established", "family"],
    },
    "84790": {
        "name": "St. George S / SunRiver",
        "lat": 37.0590, "lng": -113.5710,
        "median_home_value": 560_000, "median_rent_monthly": 2_200,
        "crime_index": 22, "pct_bachelors": 42,
        "median_household_income": 82_000, "population": 38_000,
        "walk_score": 30, "restaurant_score": 45,
        "tags": ["family", "retiree", "newer", "low-crime"],
    },
    "84780": {
        "name": "Washington City",
        "lat": 37.1320, "lng": -113.5080,
        "median_home_value": 510_000, "median_rent_monthly": 2_000,
        "crime_index": 25, "pct_bachelors": 32,
        "median_household_income": 78_000, "population": 30_000,
        "walk_score": 30, "restaurant_score": 35,
        "tags": ["family", "growth"],
    },
    "84738": {
        "name": "Hurricane",
        "lat": 37.1750, "lng": -113.2890,
        "median_home_value": 420_000, "median_rent_monthly": 1_800,
        "crime_index": 30, "pct_bachelors": 22,
        "median_household_income": 65_000, "population": 22_000,
        "walk_score": 35, "restaurant_score": 30,
        "tags": ["affordable", "family", "growth"],
    },
    "84765": {
        "name": "Santa Clara / Ivins",
        "lat": 37.1248, "lng": -113.6572,
        "median_home_value": 620_000, "median_rent_monthly": 2_400,
        "crime_index": 18, "pct_bachelors": 45,
        "median_household_income": 92_000, "population": 14_000,
        "walk_score": 25, "restaurant_score": 30,
        "tags": ["retiree", "low-crime", "scenic"],
    },
    "84757": {
        "name": "Leeds",
        "lat": 37.2436, "lng": -113.3650,
        "median_home_value": 720_000, "median_rent_monthly": 2_800,
        "crime_index": 15, "pct_bachelors": 40,
        "median_household_income": 98_000, "population": 1_200,
        "walk_score": 10, "restaurant_score": 15,
        "tags": ["rural", "low-crime", "luxury"],
    },
    "84745": {
        "name": "La Verkin",
        "lat": 37.2017, "lng": -113.2715,
        "median_home_value": 360_000, "median_rent_monthly": 1_600,
        "crime_index": 35, "pct_bachelors": 18,
        "median_household_income": 58_000, "population": 4_500,
        "walk_score": 25, "restaurant_score": 20,
        "tags": ["affordable", "rural"],
    },
    "84783": {
        "name": "Toquerville",
        "lat": 37.2540, "lng": -113.2855,
        "median_home_value": 480_000, "median_rent_monthly": 1_800,
        "crime_index": 22, "pct_bachelors": 25,
        "median_household_income": 72_000, "population": 1_800,
        "walk_score": 15, "restaurant_score": 15,
        "tags": ["rural", "low-crime"],
    },
}


# Apply Zillow overrides (loaded once in dallas_neighborhoods) to every
# metro dict. Hand-curated values stay in the source file as the snapshot;
# fresh ZHVI / ZORI values from data/zillow_overrides.json win when
# available. DALLAS_ZIPS is patched in dallas_neighborhoods.py itself.
for _zips in (LA_ZIPS, PHOENIX_ZIPS, PROVO_ZIPS, WASATCH_NORTH_ZIPS, ST_GEORGE_ZIPS,
              LAS_VEGAS_ZIPS, RENO_ZIPS, CARSON_CITY_ZIPS,
              MIAMI_ZIPS, ORLANDO_ZIPS):
    _apply_zillow_overrides(_zips, _ZILLOW_OVERRIDES)


# ─────────────────────────────────────────────────────────────────────────────
# Per-metro registry. Keys are metro slugs — usually a state code, but states
# with more than one metro deep-dive use a `{state}-{tag}` suffix (e.g.
# UT-STG for Washington County / St. George). The /real-estate/{slug}/map
# route looks up the slug here directly. STATE_TO_METROS keeps the
# state→metro list ordering used by the dashboard link + the in-page metro
# switcher.
# ─────────────────────────────────────────────────────────────────────────────
STATE_METROS: dict[str, dict] = {
    "TX": {
        "state": "TX",
        "metro_label": "Dallas County",
        "subtitle": "ZIP-level cap rate, crime, schools, income & affordability for Dallas County — composite weighted for an SFR investor lens.",
        "map_center": {"lat": 32.78, "lng": -96.80, "zoom": 11},
        "zips": DALLAS_ZIPS,
        "extra_caveats": [
            "ZIP codes are USPS routes, not true neighborhoods — Bishop Arts and parts of Oak Cliff share ZIPs.",
        ],
    },
    "CA": {
        "state": "CA",
        "metro_label": "Los Angeles County",
        "subtitle": "ZIP-level scores for greater Los Angeles — beach, hills, urban core, and high-cap-rate South LA submarkets.",
        "map_center": {"lat": 34.05, "lng": -118.30, "zoom": 10},
        "zips": LA_ZIPS,
        "extra_caveats": [
            "LA ZIPs span huge populations — 90011 alone has ~110K residents and is heterogeneous.",
            "Beach ZIPs (90291, 90402) trade at a beach premium that distorts cap-rate scoring vs. hill/inland ZIPs.",
        ],
    },
    "UT": {
        "state": "UT",
        "metro_label": "Utah County (Provo / Silicon Slopes)",
        "subtitle": "ZIP-level scores for the Utah Valley — Provo, Orem, and the Lehi tech corridor south of Salt Lake.",
        "map_center": {"lat": 40.30, "lng": -111.75, "zoom": 10},
        "zips": PROVO_ZIPS,
        "extra_caveats": [
            "84601 / 84602 are BYU-dominated — median income reads low because the population is mostly students.",
            "84043 (Lehi) is huge by Utah standards (~90K) and absorbs most of the 'Silicon Slopes' tech wage premium.",
        ],
    },
    "UT-SLC": {
        "state": "UT",
        "metro_label": "Wasatch Front North (SLC / Ogden / Logan)",
        "subtitle": "ZIP-level scores for Salt Lake County and the corridor north — SLC core, Davis County (Bountiful / Farmington / Kaysville), Weber County (Ogden), and Cache County (Logan).",
        "map_center": {"lat": 41.05, "lng": -111.95, "zoom": 8},
        "zips": WASATCH_NORTH_ZIPS,
        "extra_caveats": [
            "Logan (84321 / 84341) is ~80 miles north of SLC — the map zoom is wide so the Cache County submarket fits. Treat Logan as a separate market driven by USU + ag, not by SLC.",
            "Weber County (84401-84405) has a different employer base than Salt Lake — Hill AFB to the south plus aerospace/manufacturing — so cap rates compress less in downturns.",
            "84108 (Federal Heights) bridges the U of U medical corridor and the wealthiest blocks of SLC; values are pulled toward the upper end of the ZIP.",
        ],
    },
    "UT-STG": {
        "state": "UT",
        "metro_label": "Washington County (St. George)",
        "subtitle": "ZIP-level scores for Southern Utah — St. George, Washington City, Hurricane, Ivins. Heavy retiree/family mix, very different cycle from the Wasatch Front.",
        "map_center": {"lat": 37.13, "lng": -113.45, "zoom": 10},
        "zips": ST_GEORGE_ZIPS,
        "extra_caveats": [
            "St. George is a retiree/snowbird market — owner occupancy is high and rental supply is thin, so cap rates here are noisier than in Wasatch Front metros.",
            "Outlying ZIPs (84757, 84745, 84783) have small populations; treat their scores as directional, not statistically robust.",
        ],
    },
    "NV-LV": {
        "state": "NV",
        "metro_label": "Las Vegas / Clark County",
        "subtitle": "ZIP-level scores for the Las Vegas valley — Strip, Spring Valley, Summerlin, Henderson, North Las Vegas.",
        "map_center": {"lat": 36.10, "lng": -115.20, "zoom": 10},
        "zips": LAS_VEGAS_ZIPS,
        "extra_caveats": [
            "89109 (the Strip) is mostly tourist accommodation — small permanent population means the rental signal is noisy and the cap rate isn't comparable to residential ZIPs.",
            "Summerlin (89134/89135/89138) is master-planned with HOA dues that can add \\$100–\\$300/mo to PITI not reflected in the score.",
            "Henderson PD reports separately from Las Vegas Metro — crime indexes are normalized but precision varies.",
        ],
    },
    "NV-CC": {
        "state": "NV",
        "metro_label": "Carson City",
        "subtitle": "ZIP-level scores for the Carson City consolidated city/county — state capital, retiree-heavy, smaller market with its own dynamics distinct from Reno.",
        "map_center": {"lat": 39.16, "lng": -119.76, "zoom": 12},
        "zips": CARSON_CITY_ZIPS,
        "extra_caveats": [
            "Carson City's economy is heavy on state-government employment — local cycles track legislative-session timing more than tech / national trends.",
            "Small market (~58K residents across 4 ZIPs) means fewer comparable sales; values are noisier month-to-month than larger metros.",
            "High owner-occupancy + thin rental supply makes the cap-rate signal less reliable than in Reno or Vegas; treat rental scores as directional.",
        ],
    },
    "NV-RNO": {
        "state": "NV",
        "metro_label": "Reno / Sparks (Washoe County)",
        "subtitle": "ZIP-level scores for the Reno-Sparks metro — UNR, downtown, Caughlin Ranch, Spanish Springs, Damonte Ranch.",
        "map_center": {"lat": 39.55, "lng": -119.78, "zoom": 10},
        "zips": RENO_ZIPS,
        "extra_caveats": [
            "Reno's market reset hard during the Tesla Gigafactory boom (2015–2020); current values are post-reset and can move quickly with each tech-employer expansion or contraction.",
            "South Reno ZIPs (89511, 89521) span established neighborhoods plus active development — within-ZIP variance is high.",
            "89434 / 89431 in Sparks include some industrial parcels; SFR data trends entry-level.",
        ],
    },
    "FL-MIA": {
        "state": "FL",
        "metro_label": "Miami-Dade County",
        "subtitle": "ZIP-level scores for greater Miami — Brickell, the Beaches, Coral Gables, Pinecrest, Aventura, Kendall, Doral.",
        "map_center": {"lat": 25.78, "lng": -80.20, "zoom": 10},
        "zips": MIAMI_ZIPS,
        "extra_caveats": [
            "Miami's housing market is heavily exposed to international capital flows + the post-Surfside (2021) condo insurance / SIRS reserve crunch — beach + high-rise condo carrying costs can be \\$500–\\$2K/mo above the score's PITI assumption.",
            "Hurricane / flood insurance for ZIPs east of I-95 (33139/33140/33180) often runs 2–3× the inland rate; not reflected in cap-rate scoring.",
            "Pinecrest (33156) lot sizes skew large — median home value reflects \\$2M+ estate-style properties more than typical SFR.",
        ],
    },
    "FL-ORL": {
        "state": "FL",
        "metro_label": "Orlando / Orange County",
        "subtitle": "ZIP-level scores for the Orlando metro — Downtown, Winter Park, Lake Nona, Dr Phillips, UCF area, plus high-cap-rate SW pockets.",
        "map_center": {"lat": 28.53, "lng": -81.38, "zoom": 10},
        "zips": ORLANDO_ZIPS,
        "extra_caveats": [
            "Orlando ZIPs near the Disney / Universal corridor (32836, 32837) compete with short-term-rental investors; cap rate signal can be distorted vs. typical SFR rentals.",
            "Lake Nona (32827) is a master-planned high-growth area — values move quickly with each new Medical City employer announcement.",
            "FL hurricane / wind insurance has roughly tripled in 5 years; not modeled in cap-rate scoring.",
        ],
    },
    "AZ": {
        "state": "AZ",
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

# Ordered list of metro slugs per state. Drives the dashboard's
# default link target (first entry) and the metro switcher on the map page.
STATE_TO_METROS: dict[str, list[str]] = {
    "TX": ["TX"],
    "CA": ["CA"],
    "UT": ["UT", "UT-SLC", "UT-STG"],
    "AZ": ["AZ"],
    "NV": ["NV-LV", "NV-RNO", "NV-CC"],
    "FL": ["FL-MIA", "FL-ORL"],
}


def list_supported_states() -> list[str]:
    """Two-letter state codes that have at least one metro deep-dive."""
    return list(STATE_TO_METROS.keys())


def default_metro_slug(state: str) -> str | None:
    """The metro slug the state-dashboard link should target by default."""
    metros = STATE_TO_METROS.get(state.upper())
    return metros[0] if metros else None


def metros_for_state(state: str) -> list[dict]:
    """Sibling list for the in-page metro switcher: ordered metros for a
    state with their slug + label. Returns [] for unsupported states."""
    state = state.upper()
    return [
        {"slug": slug, "label": STATE_METROS[slug]["metro_label"]}
        for slug in STATE_TO_METROS.get(state, [])
        if slug in STATE_METROS
    ]


def get_state_neighborhoods(slug: str) -> dict | None:
    """Return enriched neighborhoods for a metro slug (e.g. 'TX', 'UT-STG'),
    or None if the slug isn't wired up yet. Shape stays the same as
    get_dallas_neighborhoods() so the Leaflet template can be generic."""
    slug = slug.upper()
    metro = STATE_METROS.get(slug)
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
        "slug": slug,
        "state": metro["state"],
        "metro_label": metro["metro_label"],
        "subtitle": metro["subtitle"],
        "map_center": metro["map_center"],
        "siblings": metros_for_state(metro["state"]),
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
