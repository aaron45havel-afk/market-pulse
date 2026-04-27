"""Dallas County ZIP-level neighborhood investor scoring.

This is a hardcoded snapshot drawn from public data sources, NOT a live
feed — the goal is to produce a usable neighborhood map for SFR-investor
decisions while we figure out which paid APIs are worth wiring up.

Snapshot date: ~April 2024.

Sources:
  • Home value / rent: Zillow Research public data (ZHVI / ZORI by ZIP).
    https://www.zillow.com/research/data/
  • Crime index: Dallas Police open-data portal (incidents per 1,000
    residents, normalized to a 0–100 scale where 50 ≈ Dallas city avg).
    https://www.dallasopendata.com/
  • Education / income / population: U.S. Census ACS 2022 5-year
    estimates (B15003 % bachelor's+, B19013 median HH income, B01003
    population). Used as a school-quality proxy where TEA STAAR data
    isn't directly addressable by ZIP.

Limitations to surface in the UI:
  • ZIPs ≠ neighborhoods — Bishop Arts and parts of Oak Cliff sit
    inside the same ZIP code; consider this a coarse first cut.
  • % bachelor's+ is a school-quality proxy, not a direct rating.
  • Crime index is a relative ranking, not a per-capita rate.
"""
from __future__ import annotations

DATA_AS_OF = "2024-04"

# ZIP-level snapshot for Dallas County + close-in suburbs.
# Coordinates are approximate centroids of the ZIP polygon (USPS).
DALLAS_ZIPS: dict[str, dict] = {
    # Downtown / Uptown / urban core
    "75201": {
        "name": "Downtown Dallas",
        "lat": 32.7831, "lng": -96.7969,
        "median_home_value": 450_000,
        "median_rent_monthly": 2_300,
        "crime_index": 78,
        "pct_bachelors": 68,
        "median_household_income": 95_000,
        "population": 19_500,
        "tags": ["urban", "transit", "high-density"],
    },
    "75204": {
        "name": "Uptown / State-Thomas",
        "lat": 32.8001, "lng": -96.7935,
        "median_home_value": 460_000,
        "median_rent_monthly": 2_400,
        "crime_index": 62,
        "pct_bachelors": 73,
        "median_household_income": 105_000,
        "population": 33_000,
        "tags": ["urban", "walkable", "nightlife"],
    },
    "75206": {
        "name": "Greenville / Lower Greenville",
        "lat": 32.8330, "lng": -96.7700,
        "median_home_value": 510_000,
        "median_rent_monthly": 2_300,
        "crime_index": 55,
        "pct_bachelors": 64,
        "median_household_income": 82_000,
        "population": 33_500,
        "tags": ["walkable", "restaurants", "gentrifying"],
    },
    "75219": {
        "name": "Oak Lawn",
        "lat": 32.8120, "lng": -96.8190,
        "median_home_value": 430_000,
        "median_rent_monthly": 2_200,
        "crime_index": 60,
        "pct_bachelors": 65,
        "median_household_income": 78_000,
        "population": 32_000,
        "tags": ["urban", "walkable"],
    },

    # Park Cities / Highland Park-adjacent (the prestige tier)
    "75205": {
        "name": "Highland Park / SMU",
        "lat": 32.8390, "lng": -96.7920,
        "median_home_value": 1_500_000,
        "median_rent_monthly": 3_500,
        "crime_index": 18,
        "pct_bachelors": 86,
        "median_household_income": 175_000,
        "population": 25_000,
        "tags": ["top-schools", "low-crime", "luxury"],
    },
    "75225": {
        "name": "Preston Hollow (S) / Park Cities",
        "lat": 32.8700, "lng": -96.7950,
        "median_home_value": 1_200_000,
        "median_rent_monthly": 3_100,
        "crime_index": 22,
        "pct_bachelors": 84,
        "median_household_income": 165_000,
        "population": 24_500,
        "tags": ["top-schools", "low-crime", "luxury"],
    },
    "75230": {
        "name": "Preston Hollow (N) / Devonshire",
        "lat": 32.9050, "lng": -96.7900,
        "median_home_value": 950_000,
        "median_rent_monthly": 3_000,
        "crime_index": 30,
        "pct_bachelors": 75,
        "median_household_income": 130_000,
        "population": 36_000,
        "tags": ["good-schools", "low-crime"],
    },

    # East Dallas / Lakewood
    "75214": {
        "name": "Lakewood / Lower Greenville E",
        "lat": 32.8230, "lng": -96.7510,
        "median_home_value": 650_000,
        "median_rent_monthly": 2_400,
        "crime_index": 38,
        "pct_bachelors": 70,
        "median_household_income": 110_000,
        "population": 38_000,
        "tags": ["good-schools", "walkable", "family"],
    },
    "75218": {
        "name": "Lakewood / Casa Linda",
        "lat": 32.8430, "lng": -96.7180,
        "median_home_value": 580_000,
        "median_rent_monthly": 2_400,
        "crime_index": 35,
        "pct_bachelors": 64,
        "median_household_income": 102_000,
        "population": 25_000,
        "tags": ["family", "established"],
    },
    "75223": {
        "name": "Casa View / Lochwood",
        "lat": 32.8400, "lng": -96.7000,
        "median_home_value": 320_000,
        "median_rent_monthly": 1_900,
        "crime_index": 58,
        "pct_bachelors": 38,
        "median_household_income": 60_000,
        "population": 13_500,
        "tags": ["affordable", "improving"],
    },
    "75228": {
        "name": "East Dallas / White Rock E",
        "lat": 32.8200, "lng": -96.6800,
        "median_home_value": 320_000,
        "median_rent_monthly": 2_000,
        "crime_index": 60,
        "pct_bachelors": 32,
        "median_household_income": 58_000,
        "population": 64_500,
        "tags": ["affordable", "appreciation-play"],
    },

    # North Dallas / RISD pocket
    "75240": {
        "name": "North Dallas / Galleria",
        "lat": 32.9250, "lng": -96.7700,
        "median_home_value": 420_000,
        "median_rent_monthly": 2_200,
        "crime_index": 50,
        "pct_bachelors": 56,
        "median_household_income": 78_000,
        "population": 25_500,
        "tags": ["job-center"],
    },
    "75243": {
        "name": "Lake Highlands",
        "lat": 32.9080, "lng": -96.7260,
        "median_home_value": 340_000,
        "median_rent_monthly": 2_000,
        "crime_index": 56,
        "pct_bachelors": 42,
        "median_household_income": 65_000,
        "population": 50_500,
        "tags": ["mid-tier", "RISD"],
    },
    "75248": {
        "name": "North Dallas / RISD core",
        "lat": 32.9620, "lng": -96.7800,
        "median_home_value": 680_000,
        "median_rent_monthly": 2_700,
        "crime_index": 28,
        "pct_bachelors": 70,
        "median_household_income": 125_000,
        "population": 31_000,
        "tags": ["top-schools", "low-crime", "family"],
    },
    "75252": {
        "name": "Far North Dallas / Plano-adj",
        "lat": 32.9970, "lng": -96.7770,
        "median_home_value": 520_000,
        "median_rent_monthly": 2_500,
        "crime_index": 32,
        "pct_bachelors": 64,
        "median_household_income": 110_000,
        "population": 25_000,
        "tags": ["top-schools", "family"],
    },
    "75254": {
        "name": "Far North Dallas / Prestonwood",
        "lat": 32.9540, "lng": -96.8130,
        "median_home_value": 580_000,
        "median_rent_monthly": 2_600,
        "crime_index": 30,
        "pct_bachelors": 68,
        "median_household_income": 118_000,
        "population": 17_500,
        "tags": ["family", "low-crime"],
    },
    "75231": {
        "name": "Vickery Meadow",
        "lat": 32.8720, "lng": -96.7570,
        "median_home_value": 380_000,
        "median_rent_monthly": 2_000,
        "crime_index": 75,
        "pct_bachelors": 35,
        "median_household_income": 48_000,
        "population": 47_000,
        "tags": ["high-crime", "high-density"],
    },
    "75229": {
        "name": "Bachman Lake / Walnut Hill",
        "lat": 32.8850, "lng": -96.8540,
        "median_home_value": 590_000,
        "median_rent_monthly": 2_300,
        "crime_index": 48,
        "pct_bachelors": 50,
        "median_household_income": 75_000,
        "population": 38_500,
        "tags": ["mid-tier"],
    },

    # West / Northwest Dallas
    "75220": {
        "name": "Bachman / Love Field",
        "lat": 32.8580, "lng": -96.8730,
        "median_home_value": 350_000,
        "median_rent_monthly": 2_000,
        "crime_index": 58,
        "pct_bachelors": 38,
        "median_household_income": 55_000,
        "population": 45_500,
        "tags": ["affordable"],
    },

    # Oak Cliff / South Dallas (the gentrification + cap-rate plays)
    "75208": {
        "name": "Bishop Arts / N Oak Cliff",
        "lat": 32.7400, "lng": -96.8350,
        "median_home_value": 400_000,
        "median_rent_monthly": 1_800,
        "crime_index": 52,
        "pct_bachelors": 44,
        "median_household_income": 64_000,
        "population": 28_500,
        "tags": ["walkable", "restaurants", "gentrifying", "appreciation-play"],
    },
    "75211": {
        "name": "Cockrell Hill / W Oak Cliff",
        "lat": 32.7330, "lng": -96.8800,
        "median_home_value": 280_000,
        "median_rent_monthly": 1_700,
        "crime_index": 65,
        "pct_bachelors": 18,
        "median_household_income": 52_000,
        "population": 73_000,
        "tags": ["high-cap-rate", "affordable"],
    },
    "75216": {
        "name": "S Oak Cliff",
        "lat": 32.7110, "lng": -96.7950,
        "median_home_value": 200_000,
        "median_rent_monthly": 1_400,
        "crime_index": 82,
        "pct_bachelors": 14,
        "median_household_income": 39_000,
        "population": 49_000,
        "tags": ["high-cap-rate", "high-crime"],
    },
    "75224": {
        "name": "S Oak Cliff / Wynnewood",
        "lat": 32.7240, "lng": -96.8330,
        "median_home_value": 240_000,
        "median_rent_monthly": 1_500,
        "crime_index": 70,
        "pct_bachelors": 18,
        "median_household_income": 45_000,
        "population": 35_500,
        "tags": ["high-cap-rate", "high-crime"],
    },
    "75232": {
        "name": "S Dallas / Red Bird",
        "lat": 32.6810, "lng": -96.8410,
        "median_home_value": 220_000,
        "median_rent_monthly": 1_500,
        "crime_index": 72,
        "pct_bachelors": 22,
        "median_household_income": 48_000,
        "population": 32_500,
        "tags": ["high-cap-rate"],
    },
    "75215": {
        "name": "S Dallas / Fair Park",
        "lat": 32.7610, "lng": -96.7570,
        "median_home_value": 230_000,
        "median_rent_monthly": 1_500,
        "crime_index": 88,
        "pct_bachelors": 20,
        "median_household_income": 40_000,
        "population": 18_500,
        "tags": ["high-crime", "high-cap-rate", "speculative"],
    },
}


# ───── Sub-score helpers (each returns 0-100) ─────

def _score_cap_rate(cap_rate_pct: float) -> float:
    """Investor cap rate: 3% = bad (priced for appreciation only),
    8% = excellent cash flow. Linear in between, clamped at endpoints."""
    if cap_rate_pct <= 3.0:
        return 0.0
    if cap_rate_pct >= 8.0:
        return 100.0
    return (cap_rate_pct - 3.0) / 5.0 * 100.0


def _score_crime_safety(crime_index: float) -> float:
    """Crime index 0-100 (higher = more crime). Score is the inverse."""
    return max(0.0, min(100.0, 100.0 - crime_index))


def _score_schools(pct_bachelors: float) -> float:
    """% adults with bachelor's+ as a school-quality proxy.
    20% = bottom-tier, 70% = top-tier. Linear in between."""
    if pct_bachelors <= 20:
        return 0.0
    if pct_bachelors >= 70:
        return 100.0
    return (pct_bachelors - 20) / 50.0 * 100.0


def _score_income(median_hh_income: float) -> float:
    """Tenant pool quality + appreciation tailwind.
    $40K = bottom, $130K = top. Linear in between."""
    if median_hh_income <= 40_000:
        return 0.0
    if median_hh_income >= 130_000:
        return 100.0
    return (median_hh_income - 40_000) / 90_000.0 * 100.0


def _score_affordability(median_home_value: float) -> float:
    """Lower price = lower entry-cost barrier for investors building a portfolio.
    $200K = excellent entry, $1.0M+ = locked out. Linear in between."""
    if median_home_value <= 200_000:
        return 100.0
    if median_home_value >= 1_000_000:
        return 0.0
    return (1_000_000 - median_home_value) / 800_000.0 * 100.0


# Composite weights — tuned for an SFR investor lens. Cap rate dominates
# because it's the only factor that directly drives cash flow today;
# crime + schools matter for tenant quality and exit liquidity.
WEIGHTS = {
    "cap_rate":      0.35,
    "crime_safety":  0.20,
    "schools":       0.20,
    "income":        0.15,
    "affordability": 0.10,
}


def compute_zip_metrics(z: dict) -> dict:
    """Compute derived metrics + sub-scores + composite for one ZIP."""
    home_value = z["median_home_value"]
    annual_rent = z["median_rent_monthly"] * 12
    cap_rate_pct = (annual_rent / home_value * 100) if home_value > 0 else 0.0
    rent_to_price = annual_rent / home_value if home_value > 0 else 0.0

    sub_scores = {
        "cap_rate":      _score_cap_rate(cap_rate_pct),
        "crime_safety":  _score_crime_safety(z["crime_index"]),
        "schools":       _score_schools(z["pct_bachelors"]),
        "income":        _score_income(z["median_household_income"]),
        "affordability": _score_affordability(home_value),
    }
    composite = sum(sub_scores[k] * WEIGHTS[k] for k in WEIGHTS)
    return {
        "cap_rate_pct": round(cap_rate_pct, 2),
        "rent_to_price": round(rent_to_price, 4),
        "sub_scores": {k: round(v, 1) for k, v in sub_scores.items()},
        "composite_score": round(composite, 1),
    }


def get_dallas_neighborhoods() -> dict:
    """Return the full Dallas ZIP dataset with derived metrics + scores."""
    enriched = []
    for zip_code, raw in DALLAS_ZIPS.items():
        metrics = compute_zip_metrics(raw)
        enriched.append({
            "zip": zip_code,
            **raw,
            **metrics,
        })
    enriched.sort(key=lambda x: x["composite_score"], reverse=True)
    return {
        "as_of": DATA_AS_OF,
        "weights": WEIGHTS,
        "neighborhoods": enriched,
        "sources": {
            "home_value_rent": "Zillow Research (ZHVI / ZORI public data)",
            "crime_index": "Dallas Police open-data portal (normalized 0-100)",
            "education_income_population": "U.S. Census ACS 2022 5-year",
        },
        "caveats": [
            "ZIP codes are USPS routes, not true neighborhoods — Bishop Arts and parts of Oak Cliff share ZIPs.",
            "% bachelor's+ is a school-quality proxy. Direct STAAR / accountability ratings would be more accurate.",
            "Crime index is a relative ranking inside Dallas, not a per-capita rate.",
            f"Snapshot from {DATA_AS_OF}. Refresh sources annually.",
        ],
    }
