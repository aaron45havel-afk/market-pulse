"""Landscaper pricing engine — /landscaper (alias /jardin).

Built for a small Bay Area crew: price by ZIP wealth instead of one
flat rate everywhere. The same 45-minute mow-and-blow is worth $50 in
Hayward and $110 in Atherton — the client's reference point is what
their neighbors pay, not what the work costs.

ZIP tiers: Bay ZIPs bucketed into quintiles by median home value
(fully-populated Zillow data; ACS income shown as a sanity check when
present). Each tier carries a suggested price card:

  per-visit mow & blow · monthly (4.3 visits) · $/1,000 sqft of lawn ·
  MINIMUM stop fee (every stop has ~25 min of fixed load/drive time —
  rule #1 of route pricing: no visit below the minimum) · upsell list
  (cleanup, mulch installed, hedge hr, irrigation call, gutter clear).

Rates are 2026 Bay-market ballparks — editable constants, reviewed
annually. The page's client book / costs / routes all live in the
user's browser (localStorage) — no client PII server-side.
"""
from __future__ import annotations

import sqlite3
import statistics
from pathlib import Path

_ZIPS_DB = Path(__file__).resolve().parent / "data" / "zips.db"
_RATES_JSON = Path(__file__).resolve().parent / "data" / "rates.json"


def _cpi_adjust() -> dict:
    """Inflation multiplier for the rate cards: CPIAUCSL(latest) ÷
    CPIAUCSL(base month of the July-2026 calibration), written weekly by
    refresh_rates.py. Clamped [1.0, 1.5] — rates never deflate below the
    calibration and a data glitch can't run away. Missing/malformed →
    1.0 (calibration dollars)."""
    import json
    try:
        with open(_RATES_JSON, encoding="utf-8") as fh:
            cpi = (json.load(fh) or {}).get("cpi") or {}
        base, latest = cpi.get("base"), cpi.get("latest")
        if isinstance(base, (int, float)) and isinstance(latest, (int, float)) and base > 0:
            return {"mult": max(1.0, min(1.5, latest / base)),
                    "as_of": cpi.get("latest_month"),
                    "base_month": cpi.get("base_month")}
    except (OSError, ValueError):
        pass
    return {"mult": 1.0, "as_of": None, "base_month": "2026-06"}

BAY_COUNTIES = {
    "San Francisco County", "San Mateo County", "Santa Clara County",
    "Alameda County", "Contra Costa County", "Marin County",
    "Sonoma County", "Napa County", "Solano County",
}

# Tier price cards (1 = working-class ZIPs … 5 = Atherton-tier).
# visit: weekly mow & blow per visit · ksqft: $ per 1,000 sqft of lawn
# per visit · min: minimum stop fee · monthly = visit × 4.3 (computed).
TIER_RATES = {
    1: {"visit": 45,  "ksqft": 8,  "min": 40,  "mult": 0.80},
    2: {"visit": 55,  "ksqft": 10, "min": 45,  "mult": 0.90},
    3: {"visit": 65,  "ksqft": 12, "min": 55,  "mult": 1.00},
    4: {"visit": 80,  "ksqft": 15, "min": 65,  "mult": 1.20},
    5: {"visit": 100, "ksqft": 20, "min": 80,  "mult": 1.50},
}
# Tier-3 upsell base prices; scaled by tier mult.
UPSELL_BASE = {
    "cleanup": 450,      # spring/fall cleanup
    "mulch_yd": 95,      # per cubic yard installed
    "hedge_hr": 85,      # hedge/shrub work per hour
    "irrigation": 120,   # irrigation repair call (first hour)
    "gutter": 180,       # gutter clearing
}
BIWEEKLY_FACTOR = 1.25   # biweekly visits price higher per visit

# Bay cities with gas leaf-blower bans (electric-only) — his richest
# target ZIPs are mostly ban cities, so the battery transition is a
# market-access issue, not just compliance.
GAS_BLOWER_BAN_CITIES = [
    "Berkeley", "Palo Alto", "Los Altos", "Los Altos Hills", "Mill Valley",
    "Belvedere", "Tiburon", "Sausalito", "Ross", "Fairfax", "Larkspur",
    "Menlo Park", "Atherton", "Portola Valley", "Woodside", "Los Gatos",
    "Sonoma", "Oakland",
]


def bay_pricing() -> dict:
    """{zips: [...], tiers: {...}} for the page. Tier = home-value
    quintile within the Bay universe."""
    if not _ZIPS_DB.exists():
        return {"zips": [], "tiers": TIER_RATES}
    conn = sqlite3.connect(str(_ZIPS_DB))
    conn.row_factory = sqlite3.Row
    try:
        marks = ",".join("?" * len(BAY_COUNTIES))
        rows = conn.execute(f"""
            SELECT zip, name, county, lat, lng, population,
                   median_home_value, median_household_income
            FROM zips
            WHERE state = 'CA' AND county IN ({marks})
              AND population > 1000 AND median_home_value IS NOT NULL
        """, tuple(BAY_COUNTIES)).fetchall()
    finally:
        conn.close()

    values = sorted(r["median_home_value"] for r in rows)
    if not values:
        return {"zips": [], "tiers": TIER_RATES}
    qs = statistics.quantiles(values, n=5)   # 4 cut points → 5 tiers

    def tier_of(v: float) -> int:
        for i, cut in enumerate(qs):
            if v < cut:
                return i + 1
        return 5

    cpi = _cpi_adjust()
    m = cpi["mult"]
    out = []
    for r in rows:
        t = tier_of(r["median_home_value"])
        rates = TIER_RATES[t]
        mult = rates["mult"]
        income = r["median_household_income"]
        visit = round(rates["visit"] * m)
        monthly = round(visit * 4.3)
        out.append({
            "zip": r["zip"],
            "city": (r["name"] or "").replace(", CA", ""),
            "county": (r["county"] or "").replace(" County", ""),
            "lat": r["lat"], "lng": r["lng"],
            "tier": t,
            "hv": r["median_home_value"],
            "income": income,
            "visit": visit,
            "monthly": monthly,
            "ksqft": round(rates["ksqft"] * m, 1),
            "min": round(rates["min"] * m),
            "pct_income": round(monthly * 12 / income * 100, 2) if income else None,
            "gas_ban": any(c in (r["name"] or "") for c in GAS_BLOWER_BAN_CITIES),
            "upsells": {k: round(v * mult * m) for k, v in UPSELL_BASE.items()},
        })
    out.sort(key=lambda z: z["zip"])
    return {"zips": out, "tiers": TIER_RATES,
            "biweekly_factor": BIWEEKLY_FACTOR,
            "ban_cities": GAS_BLOWER_BAN_CITIES,
            "cpi": cpi}
