"""NorCal Real Estate — the Bay Area strict screen + deal checker.

Five quality gates + one budget gate, applied to every Bay ZIP in
zips.db. Strictness with receipts: every ZIP shows which gates it
passed/failed and by how much; near-misses (failed by a hair) are
surfaced instead of hidden, because with filters this tight the
interesting information is often *why* something just missed.

Real-data calibrations (measured against zips.db, not vibes):
  • restaurant_score is density-biased — SF cores run 76-84 while
    famously food-rich small downtowns (Los Gatos 13, Mill Valley 15,
    Burlingame's ZIP diluted by Hillsborough acreage) score low. Gate
    threshold is scaled to the Bay distribution (≥45 ≈ top ~25% of
    non-SF towns) plus a short, documented override list for downtowns
    the metric is provably blind to.
  • "Excellent" crime = crime_index ≤ 33 ≈ top quintile of the Bay.
  • A literal AND of the strictest possible thresholds returns the
    empty set — these defaults are the tightest calibration that keeps
    the screen informative. All tunable via query params.

Climate is a hand-encoded microclimate map (there is no free ZIP-level
fog/wind dataset): temperate passes; fog belt, wind corridor, and
inland heat fail with the reason shown. Judgment layer — override-able.

Budget (the 2-person, $200k reality): buying power = (assets −
reserves) ÷ down% → max purchase. Entry price is the condo-tier ZHVI
from data/norcal_condo.json when the monthly refresh has run (the
buyable segment in these towns is condos/THs), else the all-homes
median with an honest label. BUYABLE = all quality gates + budget;
ASPIRATIONAL = quality passes, budget doesn't.
"""
from __future__ import annotations

import json
import math
import sqlite3
import statistics
from pathlib import Path

_ZIPS_DB = Path(__file__).resolve().parent / "data" / "zips.db"
_CONDO_OVERLAY = Path(__file__).resolve().parent / "data" / "norcal_condo.json"

# ── Defaults (all overridable via /norcal query params) ──────────────
ASSETS_DEFAULT = 200_000
RESERVES_DEFAULT = 40_000
DOWN_PCT_DEFAULT = 20.0
CRIME_MAX = 33.0          # ≈ top quintile of Bay ZIPs (lower = safer)
FOOD_MIN = 45.0           # ≈ top quartile of non-SF towns on our scale
ACCESS_MIN_MAX = 30.0     # est. off-peak minutes to nearest anchor
STEADY_CAGR_MIN = 1.0     # %/yr over the window
STEADY_DD_MAX = -22.0     # max drawdown floor (60-mo window)
STEADY_VOL_MAX = 9.0      # stdev of YoY changes

ANCHORS = {
    "SF":  (37.7793, -122.4193),
    "SJ":  (37.3382, -121.8863),
    "OAK": (37.8044, -122.2712),
}
# Universe: CA ZIPs within this haversine radius of any anchor.
UNIVERSE_RADIUS_MI = 40.0
CORRIDOR_FACTOR = 1.25    # haversine → road miles
AVG_MPH = 60.0            # off-peak freeway average
SURFACE_BUFFER_MIN = 4.0  # getting on/off the freeway

# Restaurant-metric blind spots: towns with celebrated dining strips
# whose ZIP score is diluted by low density / acreage. Short, factual,
# documented — the metric divides by area, these downtowns don't.
FOOD_OVERRIDES = {
    "94010": "Burlingame Ave strip (score diluted by Hillsborough acreage)",
    "95030": "Downtown Los Gatos (N Santa Cruz Ave)",
    "94941": "Downtown Mill Valley",
    "94025": "Santa Cruz Ave, Menlo Park",
}

# ── Microclimate map (hand-encoded judgment layer) ───────────────────
# tier: 'temperate' passes. Everything else fails with its reason.
# Matched by substring against the ZIP's city name; ZIP overrides win.
CLIMATE_CITY = {
    # Temperate band — mid-Peninsula & sheltered pockets
    "Burlingame": "temperate", "Hillsborough": "temperate",
    "San Mateo": "temperate", "Belmont": "temperate",
    "San Carlos": "temperate", "Redwood City": "temperate",
    "Atherton": "temperate", "Menlo Park": "temperate",
    "Palo Alto": "temperate", "Los Altos": "temperate",
    "Mountain View": "temperate", "Sunnyvale": "temperate",
    "Cupertino": "temperate", "Santa Clara": "temperate",
    "Campbell": "temperate", "Saratoga": "temperate",
    "Los Gatos": "temperate", "San Jose": "temperate",
    "Alameda": "temperate", "Piedmont": "temperate",
    "Oakland": "temperate",          # hills/Rockridge side; flats fail crime anyway
    "Albany": "temperate", "El Cerrito": "temperate",
    "San Rafael": "temperate", "Larkspur": "temperate",
    "Corte Madera": "temperate", "Kentfield": "temperate",
    "Fremont": "temperate", "Newark": "temperate", "Union City": "temperate",
    "Milpitas": "temperate", "Foster City": "wind — afternoon bay winds",
    # Fog belt
    "Daly City": "fog belt", "Pacifica": "fog belt",
    "South San Francisco": "fog belt", "San Bruno": "fog belt (SFO gap)",
    "Millbrae": "fog edge (SFO gap)", "San Francisco": "wind + fog (city-wide strict fail)",
    "Mill Valley": "fog edge (Tam gap)", "Sausalito": "wind + fog (Gate gap)",
    "Berkeley": "fog edge (Gate gap)", "Emeryville": "wind (Gate gap)",
    "Brisbane": "wind (Candlestick gap)",
    # Inland heat
    "Walnut Creek": "inland heat", "Concord": "inland heat",
    "Pleasant Hill": "inland heat", "Martinez": "inland heat",
    "Lafayette": "inland heat (borderline)", "Orinda": "inland heat (borderline)",
    "Moraga": "inland heat (borderline)", "Danville": "inland heat",
    "San Ramon": "inland heat", "Dublin": "inland heat",
    "Pleasanton": "inland heat", "Livermore": "inland heat + Altamont wind",
    "Antioch": "inland heat", "Pittsburg": "inland heat",
    "Brentwood": "inland heat", "Oakley": "inland heat",
    "Morgan Hill": "inland heat", "Gilroy": "inland heat",
    "Vallejo": "wind (Carquinez)", "Benicia": "wind (Carquinez)",
    "Hercules": "wind", "Rodeo": "wind", "Richmond": "wind",
    "San Leandro": "temperate", "Hayward": "temperate",
    "Castro Valley": "temperate",
}
CLIMATE_ZIP_OVERRIDES: dict[str, str] = {
    # e.g. "94131": "fog belt",   # add specific corrections here
}


def _haversine_mi(lat1, lng1, lat2, lng2) -> float:
    r = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# The Bay breaks haversine: Redwood Shores → Oakland is 16 straight-line
# miles but crosses open water. Sides: SF/Peninsula/Marin = west, East
# Bay = east, San Jose = south (reachable from both without a bridge).
_EAST_COUNTIES = {"Alameda County", "Contra Costa County"}
_ANCHOR_SIDE = {"SF": "west", "OAK": "east", "SJ": "south"}
_BRIDGE_PENALTY_MIN = 25.0


def _access(lat, lng, county: str | None) -> tuple[str, float]:
    """(nearest anchor, est. off-peak minutes) with a bridge penalty
    when ZIP and anchor sit on opposite sides of the Bay."""
    zip_side = "east" if (county or "") in _EAST_COUNTIES else "west"
    best, minutes = "", 9e9
    for name, (alat, alng) in ANCHORS.items():
        road = _haversine_mi(lat, lng, alat, alng) * CORRIDOR_FACTOR
        mins = road / AVG_MPH * 60 + SURFACE_BUFFER_MIN
        aside = _ANCHOR_SIDE[name]
        if aside != "south" and aside != zip_side:
            mins += _BRIDGE_PENALTY_MIN
        if mins < minutes:
            best, minutes = name, mins
    return best, round(minutes, 1)


def _climate(zip_code: str, city_name: str) -> tuple[bool, str]:
    """(passes, tier/reason)."""
    if zip_code in CLIMATE_ZIP_OVERRIDES:
        tier = CLIMATE_ZIP_OVERRIDES[zip_code]
        return tier == "temperate", tier
    city = (city_name or "").replace(", CA", "")
    for key, tier in CLIMATE_CITY.items():
        if key in city:
            return tier == "temperate", tier
    return False, "unclassified — needs review"


def _steadiness(history_json: str | None) -> dict | None:
    """CAGR / volatility / max drawdown from a monthly value series."""
    if not history_json:
        return None
    try:
        vals = [float(v) for v in json.loads(history_json) if v]
    except (ValueError, TypeError):
        return None
    if len(vals) < 30 or vals[0] <= 0:
        return None
    years = (len(vals) - 1) / 12
    cagr = ((vals[-1] / vals[0]) ** (1 / years) - 1) * 100
    yoy = [(vals[i] / vals[i - 12] - 1) * 100 for i in range(12, len(vals))
           if vals[i - 12] > 0]
    vol = statistics.pstdev(yoy) if len(yoy) >= 6 else None
    peak, max_dd = vals[0], 0.0
    for v in vals:
        peak = max(peak, v)
        max_dd = min(max_dd, (v / peak - 1) * 100)
    return {"cagr": round(cagr, 1),
            "vol": round(vol, 1) if vol is not None else None,
            "max_dd": round(max_dd, 1),
            "months": len(vals)}


def _load_condo_overlay() -> dict:
    try:
        with open(_CONDO_OVERLAY, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("series"), dict):
            return data
    except (OSError, ValueError):
        pass
    return {}


def buying_power(assets: float, reserves: float, down_pct: float) -> dict:
    deployable = max(0.0, assets - reserves)
    max_purchase = deployable / (down_pct / 100.0) if down_pct > 0 else 0.0
    return {"assets": assets, "reserves": reserves, "deployable": deployable,
            "down_pct": down_pct, "max_purchase": round(max_purchase)}


def _universe(conn) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT zip, name, county, lat, lng, population, restaurant_score,
               crime_index, walk_score, median_home_value, median_rent_monthly,
               history_zhvi
        FROM zips
        WHERE state = 'CA' AND lat IS NOT NULL AND population > 3000
          AND lat BETWEEN 36.9 AND 38.4 AND lng BETWEEN -123.1 AND -121.3
    """).fetchall()
    out = []
    for r in rows:
        if min(_haversine_mi(r["lat"], r["lng"], a[0], a[1])
               for a in ANCHORS.values()) <= UNIVERSE_RADIUS_MI:
            out.append(r)
    return out


def screen(assets: float = ASSETS_DEFAULT, reserves: float = RESERVES_DEFAULT,
           down_pct: float = DOWN_PCT_DEFAULT,
           crime_max: float = CRIME_MAX, food_min: float = FOOD_MIN,
           access_max: float = ACCESS_MIN_MAX) -> dict:
    """Run the six gates over the Bay universe. Returns tiers + stats."""
    if not _ZIPS_DB.exists():
        return {"buyable": [], "aspirational": [], "near": [], "rest": [],
                "power": buying_power(assets, reserves, down_pct),
                "universe_n": 0, "condo_as_of": None}
    conn = sqlite3.connect(str(_ZIPS_DB))
    try:
        universe = _universe(conn)
    finally:
        conn.close()
    condo = _load_condo_overlay()
    condo_series = condo.get("series") or {}
    power = buying_power(assets, reserves, down_pct)

    scored = []
    for r in universe:
        z = r["zip"]
        anchor, minutes = _access(r["lat"], r["lng"], r["county"])
        clim_ok, clim_tier = _climate(z, r["name"])
        food = r["restaurant_score"]
        food_ok = (food is not None and food >= food_min) or z in FOOD_OVERRIDES
        crime = r["crime_index"]
        crime_ok = crime is not None and crime <= crime_max

        c_entry = condo_series.get(z) if isinstance(condo_series.get(z), dict) else None
        if c_entry and isinstance(c_entry.get("price"), (int, float)):
            entry_price = c_entry["price"]
            entry_kind = "condo"
            steady = {"cagr": c_entry.get("cagr15"), "vol": c_entry.get("vol"),
                      "max_dd": c_entry.get("max_dd"), "months": c_entry.get("n_months")}
        else:
            entry_price = r["median_home_value"]
            entry_kind = "all-homes"
            steady = _steadiness(r["history_zhvi"])

        steady_ok = bool(steady and steady.get("cagr") is not None
                         and steady["cagr"] >= STEADY_CAGR_MIN
                         and (steady.get("max_dd") is None or steady["max_dd"] >= STEADY_DD_MAX)
                         and (steady.get("vol") is None or steady["vol"] <= STEADY_VOL_MAX))

        gates = {
            "access": minutes <= access_max,
            "food": food_ok,
            "safety": crime_ok,
            "climate": clim_ok,
            "steady": steady_ok,
        }
        quality_n = sum(gates.values())
        buyable_ok = entry_price is not None and entry_price <= power["max_purchase"]
        gates["budget"] = buyable_ok

        scored.append({
            "zip": z, "name": (r["name"] or "").replace(", CA", ""),
            "county": r["county"], "population": r["population"],
            "anchor": anchor, "minutes": minutes,
            "food": food, "food_override": FOOD_OVERRIDES.get(z),
            "crime": crime, "walk": r["walk_score"],
            "climate_tier": clim_tier,
            "entry_price": entry_price, "entry_kind": entry_kind,
            "median_home_value": r["median_home_value"],
            "median_rent": r["median_rent_monthly"],
            "steady": steady,
            "gates": gates, "quality_n": quality_n,
        })

    buyable = [s for s in scored if s["quality_n"] == 5 and s["gates"]["budget"]]
    aspirational = [s for s in scored if s["quality_n"] == 5 and not s["gates"]["budget"]]
    near = [s for s in scored if s["quality_n"] == 4]
    key = lambda s: (s["minutes"], -(s["food"] or 0))
    for lst in (buyable, aspirational, near):
        lst.sort(key=key)
    return {
        "buyable": buyable, "aspirational": aspirational, "near": near,
        "universe_n": len(scored), "power": power,
        "condo_as_of": condo.get("as_of"),
        "thresholds": {"crime_max": crime_max, "food_min": food_min,
                       "access_max": access_max},
    }


def deal_check(zip_code: str, price: float, sqft: float | None = None,
               income: float | None = None, assets: float = ASSETS_DEFAULT,
               reserves: float = RESERVES_DEFAULT, down_pct: float = DOWN_PCT_DEFAULT,
               rate_pct: float = 6.5) -> dict | None:
    """Verdict for one listing against the strict screen + financing."""
    res = screen(assets, reserves, down_pct)
    row = next((s for s in res["buyable"] + res["aspirational"] + res["near"]
                for _ in [0] if s["zip"] == zip_code), None)
    if row is None:
        # Not in the top tiers — rescreen to find it anywhere in universe.
        if not _ZIPS_DB.exists():
            return None
        conn = sqlite3.connect(str(_ZIPS_DB))
        try:
            allrows = _universe(conn)
        finally:
            conn.close()
        raw = next((r for r in allrows if r["zip"] == zip_code), None)
        if raw is None:
            return None
        anchor, minutes = _access(raw["lat"], raw["lng"], raw["county"])
        clim_ok, tier = _climate(zip_code, raw["name"])
        steady = _steadiness(raw["history_zhvi"])
        row = {"zip": zip_code, "name": (raw["name"] or "").replace(", CA", ""),
               "anchor": anchor, "minutes": minutes, "food": raw["restaurant_score"],
               "food_override": FOOD_OVERRIDES.get(zip_code),
               "crime": raw["crime_index"], "climate_tier": tier,
               "entry_price": raw["median_home_value"], "entry_kind": "all-homes",
               "median_home_value": raw["median_home_value"],
               "median_rent": raw["median_rent_monthly"], "steady": steady,
               "gates": {"access": minutes <= ACCESS_MIN_MAX,
                         "food": (raw["restaurant_score"] or 0) >= FOOD_MIN
                                 or zip_code in FOOD_OVERRIDES,
                         "safety": (raw["crime_index"] or 99) <= CRIME_MAX,
                         "climate": clim_ok,
                         "steady": bool(steady and steady.get("cagr", -9) >= STEADY_CAGR_MIN)},
               "quality_n": 0}
        row["quality_n"] = sum(row["gates"].values())

    power = res["power"]
    # Price quality vs the ZIP benchmark.
    bench = row.get("entry_price") or row.get("median_home_value")
    vs_bench = round((price / bench - 1) * 100, 1) if bench else None
    if vs_bench is None:
        price_verdict = "no benchmark"
    elif vs_bench <= -10:
        price_verdict = "DISCOUNT"
    elif vs_bench <= 8:
        price_verdict = "FAIR"
    else:
        price_verdict = "PAYING UP"

    # Financing at rate_pct.
    down = price * down_pct / 100
    loan = price - down
    r = rate_pct / 100 / 12
    n = 360
    p_and_i = loan * (r * (1 + r) ** n) / ((1 + r) ** n - 1) if r > 0 else loan / n
    tax = price * 0.0125 / 12          # CA prop 13 + typical parcel extras
    ins = 2400 / 12
    piti = p_and_i + tax + ins
    income_needed = piti * 12 / 0.28
    cash_after = power["assets"] - down - price * 0.02   # ~2% closing
    fits_budget = price <= power["max_purchase"]
    qualifies = (income is not None and income * 0.28 / 12 >= piti) if income else None

    return {
        "row": row, "price": price, "sqft": sqft,
        "ppsf": round(price / sqft) if sqft else None,
        "vs_bench": vs_bench, "price_verdict": price_verdict,
        "down": round(down), "piti": round(piti),
        "income_needed": round(income_needed),
        "income": income, "qualifies": qualifies,
        "cash_after": round(cash_after),
        "reserve_ok": cash_after >= power["reserves"],
        "fits_budget": fits_budget, "power": power, "rate_pct": rate_pct,
    }
