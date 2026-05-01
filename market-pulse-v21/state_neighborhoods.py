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
# GA — Atlanta metro (Fulton + DeKalb + Cobb + Gwinnett + Forsyth)
# ─────────────────────────────────────────────────────────────────────────────
ATLANTA_ZIPS: dict[str, dict] = {
    "30303": {
        "name": "Downtown Atlanta",
        "lat": 33.7540, "lng": -84.3900,
        "median_home_value": 400_000, "median_rent_monthly": 1_800,
        "crime_index": 75, "pct_bachelors": 50,
        "median_household_income": 48_000, "population": 8_000,
        "walk_score": 80, "restaurant_score": 75,
        "tags": ["urban", "walkable", "transit", "high-crime"],
    },
    "30309": {
        "name": "Midtown",
        "lat": 33.7820, "lng": -84.3895,
        "median_home_value": 580_000, "median_rent_monthly": 2_400,
        "crime_index": 50, "pct_bachelors": 75,
        "median_household_income": 95_000, "population": 25_000,
        "walk_score": 90, "restaurant_score": 90,
        "tags": ["urban", "walkable", "restaurants"],
    },
    "30308": {
        "name": "Old Fourth Ward",
        "lat": 33.7720, "lng": -84.3700,
        "median_home_value": 620_000, "median_rent_monthly": 2_500,
        "crime_index": 50, "pct_bachelors": 70,
        "median_household_income": 90_000, "population": 18_000,
        "walk_score": 80, "restaurant_score": 85,
        "tags": ["walkable", "hip", "gentrified"],
    },
    "30312": {
        "name": "Grant Park / Reynoldstown",
        "lat": 33.7390, "lng": -84.3680,
        "median_home_value": 560_000, "median_rent_monthly": 2_400,
        "crime_index": 55, "pct_bachelors": 62,
        "median_household_income": 85_000, "population": 22_000,
        "walk_score": 75, "restaurant_score": 70,
        "tags": ["walkable", "gentrified", "hip"],
    },
    "30318": {
        "name": "Westside / Howell Mill",
        "lat": 33.7920, "lng": -84.4310,
        "median_home_value": 440_000, "median_rent_monthly": 2_300,
        "crime_index": 60, "pct_bachelors": 50,
        "median_household_income": 68_000, "population": 47_000,
        "walk_score": 60, "restaurant_score": 70,
        "tags": ["gentrifying", "high-cap-rate", "appreciation-play"],
    },
    "30327": {
        "name": "Buckhead",
        "lat": 33.8550, "lng": -84.4080,
        "median_home_value": 1_400_000, "median_rent_monthly": 4_000,
        "crime_index": 28, "pct_bachelors": 80,
        "median_household_income": 200_000, "population": 18_000,
        "walk_score": 35, "restaurant_score": 60,
        "tags": ["luxury", "low-crime", "top-schools"],
    },
    "30307": {
        "name": "Inman Park / Candler Park",
        "lat": 33.7720, "lng": -84.3500,
        "median_home_value": 720_000, "median_rent_monthly": 2_800,
        "crime_index": 40, "pct_bachelors": 75,
        "median_household_income": 115_000, "population": 13_000,
        "walk_score": 80, "restaurant_score": 80,
        "tags": ["walkable", "upscale", "established"],
    },
    "30030": {
        "name": "Decatur",
        "lat": 33.7748, "lng": -84.2963,
        "median_home_value": 720_000, "median_rent_monthly": 2_500,
        "crime_index": 30, "pct_bachelors": 75,
        "median_household_income": 110_000, "population": 25_000,
        "walk_score": 75, "restaurant_score": 75,
        "tags": ["family", "walkable", "top-schools", "low-crime"],
    },
    "30033": {
        "name": "Druid Hills / Emory",
        "lat": 33.8060, "lng": -84.3155,
        "median_home_value": 850_000, "median_rent_monthly": 2_800,
        "crime_index": 28, "pct_bachelors": 80,
        "median_household_income": 140_000, "population": 28_000,
        "walk_score": 50, "restaurant_score": 50,
        "tags": ["family", "top-schools", "low-crime"],
    },
    "30062": {
        "name": "East Cobb / Marietta",
        "lat": 34.0270, "lng": -84.4860,
        "median_home_value": 620_000, "median_rent_monthly": 2_400,
        "crime_index": 22, "pct_bachelors": 65,
        "median_household_income": 130_000, "population": 65_000,
        "walk_score": 25, "restaurant_score": 40,
        "tags": ["family", "top-schools", "low-crime"],
    },
    "30097": {
        "name": "Duluth (Gwinnett)",
        "lat": 34.0030, "lng": -84.1450,
        "median_home_value": 520_000, "median_rent_monthly": 2_300,
        "crime_index": 25, "pct_bachelors": 60,
        "median_household_income": 115_000, "population": 38_000,
        "walk_score": 30, "restaurant_score": 60,
        "tags": ["family", "newer", "low-crime"],
    },
    "30041": {
        "name": "Cumming / Forsyth",
        "lat": 34.2380, "lng": -84.1410,
        "median_home_value": 650_000, "median_rent_monthly": 2_500,
        "crime_index": 18, "pct_bachelors": 60,
        "median_household_income": 135_000, "population": 65_000,
        "walk_score": 18, "restaurant_score": 35,
        "tags": ["family", "newer", "low-crime", "growth"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# TX — Austin / Travis County: Downtown, East Austin, Tarrytown, S Congress,
# Westlake-adj, NW Hills, tech corridor, Barton Creek, S/E Austin
# ─────────────────────────────────────────────────────────────────────────────
AUSTIN_ZIPS: dict[str, dict] = {
    "78701": {
        "name": "Downtown Austin",
        "lat": 30.2700, "lng": -97.7430,
        "median_home_value": 620_000, "median_rent_monthly": 2_800,
        "crime_index": 55, "pct_bachelors": 70,
        "median_household_income": 95_000, "population": 13_000,
        "walk_score": 80, "restaurant_score": 90,
        "tags": ["urban", "walkable", "transit"],
    },
    "78702": {
        "name": "East Austin",
        "lat": 30.2640, "lng": -97.7170,
        "median_home_value": 620_000, "median_rent_monthly": 2_500,
        "crime_index": 50, "pct_bachelors": 55,
        "median_household_income": 80_000, "population": 25_000,
        "walk_score": 65, "restaurant_score": 80,
        "tags": ["walkable", "hip", "gentrified"],
    },
    "78703": {
        "name": "Tarrytown / Clarksville",
        "lat": 30.2880, "lng": -97.7660,
        "median_home_value": 1_800_000, "median_rent_monthly": 4_200,
        "crime_index": 22, "pct_bachelors": 80,
        "median_household_income": 200_000, "population": 15_000,
        "walk_score": 60, "restaurant_score": 65,
        "tags": ["luxury", "low-crime", "top-schools"],
    },
    "78704": {
        "name": "S Congress / Bouldin / Travis Heights",
        "lat": 30.2470, "lng": -97.7660,
        "median_home_value": 880_000, "median_rent_monthly": 2_800,
        "crime_index": 38, "pct_bachelors": 70,
        "median_household_income": 115_000, "population": 50_000,
        "walk_score": 70, "restaurant_score": 85,
        "tags": ["walkable", "hip", "established"],
    },
    "78705": {
        "name": "UT West Campus",
        "lat": 30.2940, "lng": -97.7390,
        "median_home_value": 580_000, "median_rent_monthly": 2_200,
        "crime_index": 52, "pct_bachelors": 65,
        "median_household_income": 55_000, "population": 28_000,
        "walk_score": 75, "restaurant_score": 75,
        "tags": ["college", "rental-demand", "walkable"],
    },
    "78731": {
        "name": "Northwest Hills",
        "lat": 30.3580, "lng": -97.7700,
        "median_home_value": 1_100_000, "median_rent_monthly": 3_200,
        "crime_index": 22, "pct_bachelors": 75,
        "median_household_income": 150_000, "population": 22_000,
        "walk_score": 30, "restaurant_score": 50,
        "tags": ["upscale", "low-crime", "family"],
    },
    "78759": {
        "name": "N Spicewood / Anderson Mill",
        "lat": 30.4040, "lng": -97.7560,
        "median_home_value": 620_000, "median_rent_monthly": 2_500,
        "crime_index": 28, "pct_bachelors": 65,
        "median_household_income": 115_000, "population": 45_000,
        "walk_score": 30, "restaurant_score": 50,
        "tags": ["family", "tech", "low-crime"],
    },
    "78717": {
        "name": "Avery Ranch / Brushy Creek",
        "lat": 30.4910, "lng": -97.7460,
        "median_home_value": 650_000, "median_rent_monthly": 2_500,
        "crime_index": 22, "pct_bachelors": 60,
        "median_household_income": 130_000, "population": 36_000,
        "walk_score": 25, "restaurant_score": 35,
        "tags": ["family", "newer", "low-crime"],
    },
    "78745": {
        "name": "South Austin",
        "lat": 30.2120, "lng": -97.8090,
        "median_home_value": 520_000, "median_rent_monthly": 2_200,
        "crime_index": 40, "pct_bachelors": 42,
        "median_household_income": 78_000, "population": 60_000,
        "walk_score": 35, "restaurant_score": 50,
        "tags": ["family", "mid-tier"],
    },
    "78735": {
        "name": "Barton Creek / SW Austin",
        "lat": 30.2600, "lng": -97.8580,
        "median_home_value": 1_400_000, "median_rent_monthly": 3_500,
        "crime_index": 18, "pct_bachelors": 75,
        "median_household_income": 180_000, "population": 14_000,
        "walk_score": 20, "restaurant_score": 35,
        "tags": ["luxury", "low-crime", "top-schools"],
    },
    "78741": {
        "name": "East Riverside",
        "lat": 30.2330, "lng": -97.7140,
        "median_home_value": 360_000, "median_rent_monthly": 1_800,
        "crime_index": 65, "pct_bachelors": 30,
        "median_household_income": 52_000, "population": 50_000,
        "walk_score": 50, "restaurant_score": 50,
        "tags": ["affordable", "high-cap-rate", "gentrifying"],
    },
    "78744": {
        "name": "SE Austin",
        "lat": 30.1820, "lng": -97.7400,
        "median_home_value": 400_000, "median_rent_monthly": 1_900,
        "crime_index": 55, "pct_bachelors": 28,
        "median_household_income": 58_000, "population": 48_000,
        "walk_score": 35, "restaurant_score": 35,
        "tags": ["affordable", "high-cap-rate"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# TX — Houston / Harris County: Inner Loop, Galleria, Energy Corridor, Pearland
# ─────────────────────────────────────────────────────────────────────────────
HOUSTON_ZIPS: dict[str, dict] = {
    "77002": {
        "name": "Downtown Houston",
        "lat": 29.7610, "lng": -95.3617,
        "median_home_value": 310_000, "median_rent_monthly": 2_000,
        "crime_index": 65, "pct_bachelors": 60,
        "median_household_income": 70_000, "population": 9_000,
        "walk_score": 75, "restaurant_score": 80,
        "tags": ["urban", "walkable", "transit"],
    },
    "77006": {
        "name": "Montrose",
        "lat": 29.7430, "lng": -95.3900,
        "median_home_value": 480_000, "median_rent_monthly": 2_200,
        "crime_index": 50, "pct_bachelors": 70,
        "median_household_income": 85_000, "population": 22_000,
        "walk_score": 75, "restaurant_score": 90,
        "tags": ["walkable", "hip", "restaurants"],
    },
    "77007": {
        "name": "The Heights",
        "lat": 29.7900, "lng": -95.4000,
        "median_home_value": 620_000, "median_rent_monthly": 2_500,
        "crime_index": 38, "pct_bachelors": 70,
        "median_household_income": 110_000, "population": 32_000,
        "walk_score": 70, "restaurant_score": 80,
        "tags": ["walkable", "gentrified", "established"],
    },
    "77005": {
        "name": "West University / Rice",
        "lat": 29.7180, "lng": -95.4200,
        "median_home_value": 1_500_000, "median_rent_monthly": 4_500,
        "crime_index": 18, "pct_bachelors": 85,
        "median_household_income": 200_000, "population": 18_000,
        "walk_score": 60, "restaurant_score": 60,
        "tags": ["luxury", "top-schools", "low-crime", "family"],
    },
    "77019": {
        "name": "River Oaks",
        "lat": 29.7500, "lng": -95.4150,
        "median_home_value": 2_500_000, "median_rent_monthly": 5_500,
        "crime_index": 22, "pct_bachelors": 85,
        "median_household_income": 250_000, "population": 14_000,
        "walk_score": 55, "restaurant_score": 65,
        "tags": ["luxury", "low-crime", "top-schools"],
    },
    "77024": {
        "name": "Memorial",
        "lat": 29.7700, "lng": -95.5300,
        "median_home_value": 900_000, "median_rent_monthly": 3_200,
        "crime_index": 22, "pct_bachelors": 75,
        "median_household_income": 165_000, "population": 36_000,
        "walk_score": 30, "restaurant_score": 50,
        "tags": ["luxury", "low-crime", "family"],
    },
    "77056": {
        "name": "Galleria / Tanglewood",
        "lat": 29.7430, "lng": -95.4640,
        "median_home_value": 360_000, "median_rent_monthly": 2_300,
        "crime_index": 45, "pct_bachelors": 70,
        "median_household_income": 120_000, "population": 22_000,
        "walk_score": 65, "restaurant_score": 80,
        "tags": ["walkable", "high-density", "shopping"],
    },
    "77098": {
        "name": "Upper Kirby / Greenway",
        "lat": 29.7370, "lng": -95.4150,
        "median_home_value": 540_000, "median_rent_monthly": 2_500,
        "crime_index": 38, "pct_bachelors": 75,
        "median_household_income": 115_000, "population": 18_000,
        "walk_score": 60, "restaurant_score": 75,
        "tags": ["walkable", "upscale", "established"],
    },
    "77079": {
        "name": "Energy Corridor",
        "lat": 29.7700, "lng": -95.6300,
        "median_home_value": 410_000, "median_rent_monthly": 2_100,
        "crime_index": 30, "pct_bachelors": 65,
        "median_household_income": 110_000, "population": 32_000,
        "walk_score": 25, "restaurant_score": 45,
        "tags": ["family", "low-crime", "tech"],
    },
    "77036": {
        "name": "Sharpstown",
        "lat": 29.6940, "lng": -95.5300,
        "median_home_value": 230_000, "median_rent_monthly": 1_500,
        "crime_index": 60, "pct_bachelors": 28,
        "median_household_income": 50_000, "population": 60_000,
        "walk_score": 50, "restaurant_score": 55,
        "tags": ["affordable", "high-cap-rate", "high-density"],
    },
    "77033": {
        "name": "Sunnyside",
        "lat": 29.6580, "lng": -95.3550,
        "median_home_value": 200_000, "median_rent_monthly": 1_500,
        "crime_index": 78, "pct_bachelors": 14,
        "median_household_income": 38_000, "population": 25_000,
        "walk_score": 40, "restaurant_score": 25,
        "tags": ["high-cap-rate", "high-crime", "speculative"],
    },
    "77584": {
        "name": "Pearland (south suburb)",
        "lat": 29.5500, "lng": -95.3000,
        "median_home_value": 400_000, "median_rent_monthly": 2_200,
        "crime_index": 25, "pct_bachelors": 55,
        "median_household_income": 110_000, "population": 75_000,
        "walk_score": 25, "restaurant_score": 40,
        "tags": ["family", "newer", "low-crime"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# TX — San Antonio / Bexar County: Downtown, Pearl, Stone Oak, military bases
# ─────────────────────────────────────────────────────────────────────────────
SAN_ANTONIO_ZIPS: dict[str, dict] = {
    "78205": {
        "name": "Downtown / King William",
        "lat": 29.4250, "lng": -98.4900,
        "median_home_value": 300_000, "median_rent_monthly": 1_800,
        "crime_index": 60, "pct_bachelors": 50,
        "median_household_income": 55_000, "population": 7_000,
        "walk_score": 75, "restaurant_score": 80,
        "tags": ["urban", "walkable", "historic"],
    },
    "78212": {
        "name": "Monte Vista / Mahncke Park",
        "lat": 29.4630, "lng": -98.5050,
        "median_home_value": 440_000, "median_rent_monthly": 2_000,
        "crime_index": 45, "pct_bachelors": 60,
        "median_household_income": 75_000, "population": 28_000,
        "walk_score": 65, "restaurant_score": 70,
        "tags": ["walkable", "historic", "established"],
    },
    "78215": {
        "name": "Tobin Hill / Pearl District",
        "lat": 29.4400, "lng": -98.4860,
        "median_home_value": 390_000, "median_rent_monthly": 1_900,
        "crime_index": 55, "pct_bachelors": 55,
        "median_household_income": 65_000, "population": 9_000,
        "walk_score": 70, "restaurant_score": 80,
        "tags": ["walkable", "gentrified", "hip"],
    },
    "78230": {
        "name": "Castle Hills",
        "lat": 29.5550, "lng": -98.5290,
        "median_home_value": 560_000, "median_rent_monthly": 2_300,
        "crime_index": 30, "pct_bachelors": 65,
        "median_household_income": 105_000, "population": 36_000,
        "walk_score": 35, "restaurant_score": 50,
        "tags": ["upscale", "low-crime", "established"],
    },
    "78248": {
        "name": "Hollywood Park / Shavano",
        "lat": 29.6020, "lng": -98.5200,
        "median_home_value": 880_000, "median_rent_monthly": 3_000,
        "crime_index": 18, "pct_bachelors": 75,
        "median_household_income": 165_000, "population": 12_000,
        "walk_score": 25, "restaurant_score": 35,
        "tags": ["luxury", "low-crime", "family"],
    },
    "78258": {
        "name": "Stone Oak",
        "lat": 29.6360, "lng": -98.4400,
        "median_home_value": 620_000, "median_rent_monthly": 2_500,
        "crime_index": 22, "pct_bachelors": 65,
        "median_household_income": 140_000, "population": 38_000,
        "walk_score": 25, "restaurant_score": 40,
        "tags": ["family", "newer", "low-crime"],
    },
    "78249": {
        "name": "Northwest / UTSA",
        "lat": 29.5460, "lng": -98.6300,
        "median_home_value": 360_000, "median_rent_monthly": 1_800,
        "crime_index": 35, "pct_bachelors": 55,
        "median_household_income": 80_000, "population": 60_000,
        "walk_score": 30, "restaurant_score": 45,
        "tags": ["college", "family", "rental-demand"],
    },
    "78232": {
        "name": "North Central",
        "lat": 29.5760, "lng": -98.4790,
        "median_home_value": 420_000, "median_rent_monthly": 2_000,
        "crime_index": 32, "pct_bachelors": 55,
        "median_household_income": 88_000, "population": 32_000,
        "walk_score": 30, "restaurant_score": 45,
        "tags": ["family", "established"],
    },
    "78207": {
        "name": "West Side",
        "lat": 29.4280, "lng": -98.5550,
        "median_home_value": 180_000, "median_rent_monthly": 1_200,
        "crime_index": 70, "pct_bachelors": 12,
        "median_household_income": 35_000, "population": 50_000,
        "walk_score": 50, "restaurant_score": 40,
        "tags": ["affordable", "high-cap-rate", "high-crime"],
    },
    "78211": {
        "name": "South Side",
        "lat": 29.3380, "lng": -98.5440,
        "median_home_value": 180_000, "median_rent_monthly": 1_200,
        "crime_index": 65, "pct_bachelors": 14,
        "median_household_income": 40_000, "population": 36_000,
        "walk_score": 30, "restaurant_score": 30,
        "tags": ["affordable", "high-cap-rate"],
    },
    "78239": {
        "name": "NE / Windcrest",
        "lat": 29.5470, "lng": -98.4040,
        "median_home_value": 260_000, "median_rent_monthly": 1_650,
        "crime_index": 42, "pct_bachelors": 32,
        "median_household_income": 62_000, "population": 38_000,
        "walk_score": 30, "restaurant_score": 35,
        "tags": ["family", "mid-tier"],
    },
    "78247": {
        "name": "Live Oak / NE family",
        "lat": 29.5710, "lng": -98.4060,
        "median_home_value": 340_000, "median_rent_monthly": 1_950,
        "crime_index": 28, "pct_bachelors": 45,
        "median_household_income": 92_000, "population": 50_000,
        "walk_score": 28, "restaurant_score": 40,
        "tags": ["family", "established", "low-crime"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# TX — Fort Worth / Tarrant County: Cultural District, TCU, NE luxury suburbs
# ─────────────────────────────────────────────────────────────────────────────
FORT_WORTH_ZIPS: dict[str, dict] = {
    "76102": {
        "name": "Downtown Fort Worth",
        "lat": 32.7530, "lng": -97.3320,
        "median_home_value": 400_000, "median_rent_monthly": 1_800,
        "crime_index": 60, "pct_bachelors": 55,
        "median_household_income": 58_000, "population": 9_000,
        "walk_score": 75, "restaurant_score": 75,
        "tags": ["urban", "walkable"],
    },
    "76107": {
        "name": "Cultural District / West 7th",
        "lat": 32.7440, "lng": -97.3680,
        "median_home_value": 480_000, "median_rent_monthly": 2_100,
        "crime_index": 45, "pct_bachelors": 65,
        "median_household_income": 85_000, "population": 30_000,
        "walk_score": 70, "restaurant_score": 75,
        "tags": ["walkable", "hip", "established"],
    },
    "76109": {
        "name": "TCU / South Hills",
        "lat": 32.7180, "lng": -97.3700,
        "median_home_value": 720_000, "median_rent_monthly": 2_500,
        "crime_index": 28, "pct_bachelors": 75,
        "median_household_income": 135_000, "population": 32_000,
        "walk_score": 50, "restaurant_score": 65,
        "tags": ["upscale", "top-schools", "college", "low-crime"],
    },
    "76104": {
        "name": "Near Southside / Magnolia",
        "lat": 32.7390, "lng": -97.3220,
        "median_home_value": 380_000, "median_rent_monthly": 1_900,
        "crime_index": 55, "pct_bachelors": 50,
        "median_household_income": 65_000, "population": 15_000,
        "walk_score": 60, "restaurant_score": 70,
        "tags": ["walkable", "gentrified", "appreciation-play"],
    },
    "76111": {
        "name": "Riverside",
        "lat": 32.7860, "lng": -97.2960,
        "median_home_value": 230_000, "median_rent_monthly": 1_500,
        "crime_index": 65, "pct_bachelors": 22,
        "median_household_income": 48_000, "population": 28_000,
        "walk_score": 35, "restaurant_score": 35,
        "tags": ["affordable", "high-cap-rate"],
    },
    "76112": {
        "name": "Eastside",
        "lat": 32.7600, "lng": -97.2530,
        "median_home_value": 210_000, "median_rent_monthly": 1_500,
        "crime_index": 70, "pct_bachelors": 18,
        "median_household_income": 44_000, "population": 38_000,
        "walk_score": 35, "restaurant_score": 35,
        "tags": ["affordable", "high-cap-rate", "high-crime"],
    },
    "76137": {
        "name": "NE Fort Worth",
        "lat": 32.8420, "lng": -97.2940,
        "median_home_value": 360_000, "median_rent_monthly": 1_950,
        "crime_index": 30, "pct_bachelors": 40,
        "median_household_income": 82_000, "population": 50_000,
        "walk_score": 25, "restaurant_score": 35,
        "tags": ["family", "established"],
    },
    "76092": {
        "name": "Southlake",
        "lat": 32.9450, "lng": -97.1250,
        "median_home_value": 1_400_000, "median_rent_monthly": 4_000,
        "crime_index": 12, "pct_bachelors": 80,
        "median_household_income": 250_000, "population": 32_000,
        "walk_score": 18, "restaurant_score": 35,
        "tags": ["luxury", "top-schools", "low-crime"],
    },
    "76051": {
        "name": "Grapevine",
        "lat": 32.9290, "lng": -97.0820,
        "median_home_value": 580_000, "median_rent_monthly": 2_400,
        "crime_index": 22, "pct_bachelors": 60,
        "median_household_income": 130_000, "population": 51_000,
        "walk_score": 45, "restaurant_score": 50,
        "tags": ["family", "low-crime", "established"],
    },
    "76244": {
        "name": "Keller",
        "lat": 32.9410, "lng": -97.2350,
        "median_home_value": 620_000, "median_rent_monthly": 2_400,
        "crime_index": 18, "pct_bachelors": 60,
        "median_household_income": 145_000, "population": 48_000,
        "walk_score": 22, "restaurant_score": 35,
        "tags": ["family", "top-schools", "low-crime"],
    },
    "76063": {
        "name": "Mansfield",
        "lat": 32.5750, "lng": -97.1280,
        "median_home_value": 440_000, "median_rent_monthly": 2_200,
        "crime_index": 25, "pct_bachelors": 50,
        "median_household_income": 115_000, "population": 75_000,
        "walk_score": 22, "restaurant_score": 35,
        "tags": ["family", "newer", "low-crime"],
    },
    "76016": {
        "name": "Arlington N / UTA",
        "lat": 32.6970, "lng": -97.1530,
        "median_home_value": 310_000, "median_rent_monthly": 1_800,
        "crime_index": 35, "pct_bachelors": 38,
        "median_household_income": 75_000, "population": 35_000,
        "walk_score": 35, "restaurant_score": 45,
        "tags": ["college", "family", "mid-tier"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# CA — SF Bay Area: SF proper + Piedmont/Oakland anchors
# Cap rates here are notoriously bad (1.5-3%); the page is more useful as a
# pure appreciation/walkability lens than as an investor-cash-flow lens.
# ─────────────────────────────────────────────────────────────────────────────
SF_BAY_ZIPS: dict[str, dict] = {
    "94102": {
        "name": "Tenderloin / Civic Center",
        "lat": 37.7833, "lng": -122.4167,
        "median_home_value": 720_000, "median_rent_monthly": 2_800,
        "crime_index": 92, "pct_bachelors": 50,
        "median_household_income": 50_000, "population": 32_000,
        "walk_score": 99, "restaurant_score": 90,
        "tags": ["urban", "walkable", "high-crime", "high-density"],
    },
    "94103": {
        "name": "SoMa",
        "lat": 37.7720, "lng": -122.4110,
        "median_home_value": 1_100_000, "median_rent_monthly": 3_800,
        "crime_index": 65, "pct_bachelors": 70,
        "median_household_income": 115_000, "population": 30_000,
        "walk_score": 95, "restaurant_score": 90,
        "tags": ["urban", "walkable", "tech", "high-density"],
    },
    "94105": {
        "name": "South Beach / Embarcadero",
        "lat": 37.7890, "lng": -122.3900,
        "median_home_value": 1_400_000, "median_rent_monthly": 4_500,
        "crime_index": 50, "pct_bachelors": 80,
        "median_household_income": 145_000, "population": 14_000,
        "walk_score": 98, "restaurant_score": 95,
        "tags": ["luxury", "walkable", "waterfront"],
    },
    "94107": {
        "name": "Potrero Hill / Mission Bay",
        "lat": 37.7695, "lng": -122.3937,
        "median_home_value": 1_500_000, "median_rent_monthly": 4_500,
        "crime_index": 55, "pct_bachelors": 75,
        "median_household_income": 145_000, "population": 22_000,
        "walk_score": 80, "restaurant_score": 85,
        "tags": ["biotech", "walkable", "newer"],
    },
    "94110": {
        "name": "Mission District",
        "lat": 37.7488, "lng": -122.4176,
        "median_home_value": 1_400_000, "median_rent_monthly": 3_800,
        "crime_index": 65, "pct_bachelors": 65,
        "median_household_income": 110_000, "population": 75_000,
        "walk_score": 95, "restaurant_score": 95,
        "tags": ["walkable", "gentrified", "restaurants", "high-density"],
    },
    "94114": {
        "name": "Castro / Noe Valley",
        "lat": 37.7600, "lng": -122.4350,
        "median_home_value": 2_000_000, "median_rent_monthly": 4_200,
        "crime_index": 35, "pct_bachelors": 80,
        "median_household_income": 155_000, "population": 31_000,
        "walk_score": 95, "restaurant_score": 85,
        "tags": ["walkable", "family", "top-schools", "low-crime"],
    },
    "94115": {
        "name": "Pacific Heights / Western Addition",
        "lat": 37.7860, "lng": -122.4360,
        "median_home_value": 2_800_000, "median_rent_monthly": 4_500,
        "crime_index": 35, "pct_bachelors": 80,
        "median_household_income": 175_000, "population": 32_000,
        "walk_score": 95, "restaurant_score": 80,
        "tags": ["luxury", "walkable", "low-crime"],
    },
    "94117": {
        "name": "Haight-Ashbury / Cole Valley",
        "lat": 37.7710, "lng": -122.4450,
        "median_home_value": 1_800_000, "median_rent_monthly": 4_000,
        "crime_index": 45, "pct_bachelors": 80,
        "median_household_income": 140_000, "population": 31_000,
        "walk_score": 95, "restaurant_score": 85,
        "tags": ["walkable", "established", "hip"],
    },
    "94118": {
        "name": "Inner Richmond",
        "lat": 37.7820, "lng": -122.4640,
        "median_home_value": 1_800_000, "median_rent_monthly": 3_800,
        "crime_index": 35, "pct_bachelors": 70,
        "median_household_income": 130_000, "population": 38_000,
        "walk_score": 90, "restaurant_score": 75,
        "tags": ["family", "walkable", "established"],
    },
    "94122": {
        "name": "Inner Sunset",
        "lat": 37.7600, "lng": -122.4830,
        "median_home_value": 1_700_000, "median_rent_monthly": 3_500,
        "crime_index": 30, "pct_bachelors": 75,
        "median_household_income": 135_000, "population": 60_000,
        "walk_score": 90, "restaurant_score": 70,
        "tags": ["family", "walkable", "established"],
    },
    "94123": {
        "name": "Marina / Cow Hollow",
        "lat": 37.8000, "lng": -122.4360,
        "median_home_value": 2_500_000, "median_rent_monthly": 4_500,
        "crime_index": 35, "pct_bachelors": 85,
        "median_household_income": 185_000, "population": 24_000,
        "walk_score": 95, "restaurant_score": 90,
        "tags": ["luxury", "walkable", "low-crime"],
    },
    "94131": {
        "name": "Glen Park / Diamond Heights",
        "lat": 37.7440, "lng": -122.4400,
        "median_home_value": 1_900_000, "median_rent_monthly": 4_000,
        "crime_index": 25, "pct_bachelors": 75,
        "median_household_income": 155_000, "population": 28_000,
        "walk_score": 75, "restaurant_score": 60,
        "tags": ["family", "established", "low-crime"],
    },
    "94612": {
        "name": "Downtown Oakland",
        "lat": 37.8060, "lng": -122.2700,
        "median_home_value": 560_000, "median_rent_monthly": 2_800,
        "crime_index": 70, "pct_bachelors": 50,
        "median_household_income": 70_000, "population": 30_000,
        "walk_score": 95, "restaurant_score": 80,
        "tags": ["urban", "walkable", "gentrifying", "high-cap-rate"],
    },
    "94611": {
        "name": "Piedmont / Montclair",
        "lat": 37.8290, "lng": -122.2300,
        "median_home_value": 2_800_000, "median_rent_monthly": 5_500,
        "crime_index": 18, "pct_bachelors": 85,
        "median_household_income": 245_000, "population": 40_000,
        "walk_score": 50, "restaurant_score": 50,
        "tags": ["luxury", "top-schools", "low-crime", "family"],
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
              MIAMI_ZIPS, ORLANDO_ZIPS, ATLANTA_ZIPS,
              AUSTIN_ZIPS, HOUSTON_ZIPS, SAN_ANTONIO_ZIPS, FORT_WORTH_ZIPS,
              SF_BAY_ZIPS):
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
    "TX-HOU": {
        "state": "TX",
        "metro_label": "Houston / Harris County",
        "subtitle": "ZIP-level scores for the Houston metro — Inner Loop (Downtown, Montrose, Heights, West U, River Oaks, Memorial), Galleria + Energy Corridor employment cores, plus high-cap-rate Sharpstown and Sunnyside.",
        "map_center": {"lat": 29.76, "lng": -95.40, "zoom": 10},
        "zips": HOUSTON_ZIPS,
        "extra_caveats": [
            "Houston has no zoning, so ZIP-level data masks significant block-by-block variance — particularly in 77036 / 77033 where redevelopment activity is patchy.",
            "Energy-sector employment (77079 Energy Corridor especially) makes Houston the most cyclical TX metro — cap rates and rent demand swing with oil prices.",
            "Hurricane / flood insurance has roughly tripled in 5 years and varies by FEMA flood zone within ZIP — the cap-rate scoring uses the state-avg insurance figure as a placeholder.",
        ],
    },
    "TX-FW": {
        "state": "TX",
        "metro_label": "Fort Worth / Tarrant County",
        "subtitle": "ZIP-level scores for the Fort Worth side of DFW — Cultural District, TCU, Near Southside, plus the wealthy NE suburbs (Southlake, Grapevine, Keller, Colleyville-adj) and Mansfield / Arlington.",
        "map_center": {"lat": 32.80, "lng": -97.30, "zoom": 10},
        "zips": FORT_WORTH_ZIPS,
        "extra_caveats": [
            "Fort Worth and Dallas (TX) are sister metros within DFW — some ZIPs near the county border (Grapevine, Coppell-adj) overlap with the Dallas County dataset.",
            "Lockheed Martin's Fort Worth plant is a major single-employer concentration (~15K+ jobs) — Northwest and Keller-adj demand tracks defense-spending cycles.",
            "Top-tier suburbs (Southlake / Carroll ISD, Keller / Keller ISD, Colleyville / GCISD) command sustained school-district premiums that ZIP-level % bachelor's+ understates.",
        ],
    },
    "TX-SA": {
        "state": "TX",
        "metro_label": "San Antonio / Bexar County",
        "subtitle": "ZIP-level scores for the San Antonio metro — Downtown / King William, the Pearl District, Castle Hills, Stone Oak, Hollywood Park, plus high-cap-rate West and South Side ZIPs.",
        "map_center": {"lat": 29.50, "lng": -98.50, "zoom": 10},
        "zips": SAN_ANTONIO_ZIPS,
        "extra_caveats": [
            "San Antonio is the largest metro in the dataset where home values stay genuinely affordable (median sub-$300K in core ZIPs). Cap rates skew high but appreciation lags Dallas/Austin/Houston.",
            "Stone Oak (78258) is master-planned territory; HOA dues add ~\\$80–\\$200/mo to PITI not modeled in the score.",
            "Military bases (Lackland, Randolph, Fort Sam Houston, JBSA) provide a meaningful rental-demand floor across multiple ZIPs — cap-rate scoring doesn't isolate this.",
        ],
    },
    "TX-AUS": {
        "state": "TX",
        "metro_label": "Austin / Travis County",
        "subtitle": "ZIP-level scores for the Austin metro — Downtown, East Austin, Tarrytown, S Congress, NW Hills, the tech corridor (Anderson Mill / Avery Ranch), Barton Creek, and the high-cap-rate SE pockets.",
        "map_center": {"lat": 30.30, "lng": -97.75, "zoom": 11},
        "zips": AUSTIN_ZIPS,
        "extra_caveats": [
            "Austin's market peaked early-2022 then reset 15–20% through 2024; current values reflect post-correction state but volatility remains higher than other TX metros.",
            "Big-tech employment exposure (Apple, Tesla, Meta, Google all have major Austin offices) means cap rates can shift quickly with hiring/layoff cycles — local rent demand tracks tech headcount.",
            "Tarrytown / Westlake-adj (78703) is in Eanes ISD, separate from AISD — top-rated schools but lot sizes + price reflect that.",
            "East Austin (78702, 78741) has some of the fastest gentrification in the country; cap rates here lag the underlying home-value appreciation trend.",
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
    "CA-SF": {
        "state": "CA",
        "metro_label": "San Francisco / Bay Area",
        "subtitle": "ZIP-level scores for SF proper + close-in East Bay anchors (Oakland + Piedmont). Cap rates here are notoriously bad (1.5-3%); use this view for walkability + appreciation, not cash flow.",
        "map_center": {"lat": 37.77, "lng": -122.42, "zoom": 12},
        "zips": SF_BAY_ZIPS,
        "extra_caveats": [
            "SF condo HOA dues run \\$800-1500/mo and aren't reflected in the cap rate — adjust mentally before comparing to LA.",
            "Rent control caps annual increases for tenured tenants (SF residential ordinance, Costa-Hawkins exemptions for newer condos) — buy-to-let math depends heavily on whether you're buying a covered unit.",
            "Earthquake insurance (CEA or private) is \\$1500-3000/yr extra and not in the standard insurance multiplier.",
            "94612 (Downtown Oakland) is the only genuine high-cap-rate option in the dataset — but Oakland's crime + ULTRA Measure W transfer-tax dynamics make it a different risk profile.",
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
    "GA-ATL": {
        "state": "GA",
        "metro_label": "Atlanta Metro",
        "subtitle": "ZIP-level scores for the Atlanta metro — intown (Midtown, Buckhead, O4W, Inman Park, Westside, Grant Park), DeKalb (Decatur, Druid Hills), Cobb (East Cobb), Gwinnett (Duluth), and Forsyth exurbs (Cumming).",
        "map_center": {"lat": 33.85, "lng": -84.35, "zoom": 10},
        "zips": ATLANTA_ZIPS,
        "extra_caveats": [
            "Atlanta crime varies block-by-block within several intown ZIPs (especially 30318 Westside and 30312 Grant Park) — ZIP-level crime indexes can mask significant micro-neighborhood variation.",
            "Forsyth + Cherokee exurbs (30041 etc.) saw a major post-pandemic price surge; Zillow auto-refresh keeps the home value current but cap-rate scoring lags behind real-time rental shifts.",
            "School quality varies widely within Cobb + Gwinnett — ZIP-level % bachelor's+ is a coarse proxy; check specific feeder patterns for school-driven decisions.",
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
    "TX": ["TX", "TX-FW", "TX-HOU", "TX-SA", "TX-AUS"],
    "CA": ["CA", "CA-SF"],
    "UT": ["UT", "UT-SLC", "UT-STG"],
    "AZ": ["AZ"],
    "NV": ["NV-LV", "NV-RNO", "NV-CC"],
    "FL": ["FL-MIA", "FL-ORL"],
    "GA": ["GA-ATL"],
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
