"""Data providers for real estate and finance data."""
import os, json, time, logging
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import pandas as pd

logger = logging.getLogger(__name__)

CACHE_DIR = Path("/tmp/market_pulse_cache")
CACHE_DIR.mkdir(exist_ok=True)

# ═══════════════════════════════════════════════════
# GEOGRAPHY: States → Counties (FIPS codes)
# ═══════════════════════════════════════════════════

STATES = {
    "CA": {"name": "California", "fips": "06"},
    "NV": {"name": "Nevada", "fips": "32"},
    "RI": {"name": "Rhode Island", "fips": "44"},
    "AZ": {"name": "Arizona", "fips": "04"},
    "WA": {"name": "Washington", "fips": "53"},
    "UT": {"name": "Utah", "fips": "49"},
    "TN": {"name": "Tennessee", "fips": "47"},
    "TX": {"name": "Texas", "fips": "48"},
    "IN": {"name": "Indiana", "fips": "18"},
}

COUNTIES = {
    "CA": {
        "6001":  "Alameda County",
        "6013":  "Contra Costa County",
        "6075":  "San Francisco County",
        "6081":  "San Mateo County",
        "6085":  "Santa Clara County",
        "6037":  "Los Angeles County",
        "6059":  "Orange County",
        "6073":  "San Diego County",
        "6067":  "Sacramento County",
        "6077":  "San Joaquin County",
        "6019":  "Fresno County",
        "6029":  "Kern County",
        "6071":  "San Bernardino County",
        "6065":  "Riverside County",
        "6097":  "Sonoma County",
        "6055":  "Napa County",
        "6041":  "Marin County",
        "6095":  "Solano County",
        "6113":  "Yolo County",
        "6061":  "Placer County",
    },
    "NV": {
        "32003": "Clark County",
        "32031": "Washoe County",
        "32510": "Carson City",
        "32007": "Elko County",
        "32019": "Lyon County",
        "32029": "Storey County",
        "32005": "Douglas County",
    },
    "RI": {
        "44007": "Providence County",
        "44003": "Kent County",
        "44009": "Washington County",
        "44005": "Newport County",
        "44001": "Bristol County",
    },
    "AZ": {
        "4013":  "Maricopa County",
        "4019":  "Pima County",
        "4021":  "Pinal County",
        "4025":  "Yavapai County",
        "4005":  "Coconino County",
        "4015":  "Mohave County",
        "4027":  "Yuma County",
        "4003":  "Cochise County",
        "4017":  "Navajo County",
    },
    "WA": {
        "53033": "King County",
        "53053": "Pierce County",
        "53061": "Snohomish County",
        "53063": "Spokane County",
        "53067": "Thurston County",
        "53035": "Kitsap County",
        "53011": "Clark County",
        "53073": "Whatcom County",
        "53077": "Yakima County",
        "53057": "Skagit County",
    },
    "UT": {
        "49035": "Salt Lake County",
        "49049": "Utah County",
        "49011": "Davis County",
        "49057": "Weber County",
        "49043": "Summit County",
        "49053": "Washington County",
        "49045": "Tooele County",
        "49051": "Wasatch County",
        "49005": "Cache County",
        "49029": "Morgan County",
    },
    "TN": {
        "47037": "Davidson County",
        "47157": "Shelby County",
        "47065": "Hamilton County",
        "47093": "Knox County",
        "47149": "Rutherford County",
        "47187": "Williamson County",
        "47189": "Wilson County",
        "47125": "Montgomery County",
        "47165": "Sumner County",
        "47147": "Robertson County",
    },
    "TX": {
        "48201": "Harris County",
        "48113": "Dallas County",
        "48453": "Travis County",
        "48029": "Bexar County",
        "48439": "Tarrant County",
        "48085": "Collin County",
        "48121": "Denton County",
        "48157": "Fort Bend County",
        "48491": "Williamson County",
        "48339": "Montgomery County",
        "48215": "Hidalgo County",
        "48141": "El Paso County",
        "48027": "Bell County",
        "48355": "Nueces County",
        "48061": "Cameron County",
    },
    "IN": {
        "18097": "Marion County",           # Indianapolis
        "18089": "Lake County",              # Gary / Hammond
        "18003": "Allen County",             # Fort Wayne
        "18057": "Hamilton County",          # Carmel / Fishers (wealthy Indy suburb)
        "18141": "St. Joseph County",        # South Bend
        "18039": "Elkhart County",
        "18157": "Tippecanoe County",        # Lafayette / Purdue
        "18163": "Vanderburgh County",       # Evansville
        "18127": "Porter County",            # Valparaiso / NW Indiana
        "18105": "Monroe County",            # Bloomington / IU
        "18081": "Johnson County",           # Indy south suburbs
        "18063": "Hendricks County",         # Indy west suburbs
    },
}

# ═══════════════════════════════════════════════════
# FRED SERIES PATTERNS (county-level, using FIPS)
# ═══════════════════════════════════════════════════
# All sourced from Realtor.com via FRED — monthly, updated through ~Feb 2026

COUNTY_SERIES = {
    # Realtor.com Housing Inventory Core Metrics
    "median_list_price":      "MEDLISPRI{fips}",        # Median listing price ($)
    "median_list_price_mom":  "MEDLISPRIMM{fips}",      # Median listing price MoM change (%)
    "active_listings":        "ACTLISCOU{fips}",        # Active listing count
    "new_listings":           "NEWLISCOU{fips}",        # New listing count
    "days_on_market":         "MEDDAYONMAR{fips}",      # Median days on market
    "pending_ratio":          "PENRAT{fips}",           # Pending listing ratio (%)
    "price_reduced_count":    "PRIREDCOU{fips}",        # Price reduced listing count
    "median_sqft":            "MEDSQUFEE{fips}",        # Median home size (sq ft)
    "price_per_sqft":         "MEDLISPRIPERSQUFEE{fips}",# Median listing price per sq ft
    "median_list_price_yoy":  "MEDLISPRYY{fips}",       # Median listing price YoY change (%)
}

# Additional county-level series from other sources
COUNTY_SERIES_EXTRA = {
    "house_price_index":      "ATNHPIUS{fips_padded}A",         # FHFA All-Transactions HPI (annual)
    "median_income":          "MHICA{fips_padded}A052NCEN",     # Census median household income (annual)
    "homeownership_rate":     "HOWNRATEACS0{fips_padded}",      # ACS homeownership rate (annual)
}

# National series
NATIONAL_SERIES = {
    "mortgage_30yr":          "MORTGAGE30US",            # 30-year fixed mortgage rate (weekly)
    "mortgage_15yr":          "MORTGAGE15US",            # 15-year fixed mortgage rate (weekly)
    "mortgage_5yr_arm":       "MORTGAGE5US",             # 5/1 ARM rate (weekly)
    "fed_funds_rate":         "FEDFUNDS",               # Federal funds effective rate (monthly)
    "cpi_shelter":            "CUSR0000SAH1",           # CPI: Shelter (monthly, for rent inflation)
    "housing_starts":         "HOUST",                  # New housing starts (monthly, national)
    "building_permits":       "PERMIT",                 # Building permits (monthly, national)
    "consumer_sentiment":     "UMCSENT",                # U of Michigan consumer sentiment
    "national_hpi":           "USSTHPI",                # National house price index (FHFA)
    "us_median_list_price":   "MEDLISPRIUS",            # National median listing price
    "us_active_listings":     "ACTLISCOUUS",            # National active listing count
    "us_days_on_market":      "MEDDAYONMARUS",          # National median DOM
    "us_new_listings":        "NEWLISCOUUS",            # National new listing count
    "us_pending_ratio":       "PENRATUS",               # National pending ratio
}

# National buyer-demand leading indicator (surfaced on the dashboard as the
# "New Home Sales" / "Buyer Demand" card + chart).
#
# Ideally this would be the MBA Weekly Mortgage Application Purchase Index —
# a weekly real-time leading indicator that suggests where closed sales are
# headed over the next several months. MBA's feed isn't on FRED's free tier,
# so we accept any FRED series ID via env var MBA_PURCHASE_SERIES.
# Recommended default: HSN1F (New One-Family Houses Sold, SAAR thousands,
# monthly — U.S. Census Bureau). Swap to an MBA series ID if you have a
# Haver/Bloomberg/MBA feed plugged into FRED. Unset → card shows "—".
MBA_PURCHASE_SERIES = os.environ.get("MBA_PURCHASE_SERIES", "").strip()
if MBA_PURCHASE_SERIES:
    NATIONAL_SERIES["mba_purchase_index"] = MBA_PURCHASE_SERIES

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


# ═══════════════════════════════════════════════════
# STATE COST-OF-OWNERSHIP TABLES
# ═══════════════════════════════════════════════════
# Effective residential property tax rates (as % of market value) — sourced
# from the Tax Foundation / ATTOM Data Solutions 2024 effective-rate tables.
# These are state averages; individual metros can vary ±40% (e.g. Austin,
# Memphis, and parts of RI are meaningfully higher than their state means).
# Used by the affordability factor to compute true PITI vs. raw P&I.
STATE_PROPERTY_TAX_RATE = {
    "CA": 0.0075,   # Prop 13 keeps state avg low; resets to purchase price on sale
    "NV": 0.0059,
    "RI": 0.0140,
    "AZ": 0.0063,
    "WA": 0.0087,
    "UT": 0.0058,
    "TN": 0.0067,
    "TX": 0.0180,   # High rates offset the no-income-tax advantage
    "IN": 0.0084,   # Statutory 1% residential cap; effective ~0.84% after homestead
}

# Rough average annual homeowners insurance premium ($/yr) on a median home.
# Big state differentiator because of hurricane / hail / wildfire exposure.
# Sources: NAIC Homeowners Insurance Report, Insurance Information Institute.
STATE_INSURANCE_ANNUAL = {
    "CA": 1500,   # Higher in wildfire zones but state avg is moderate
    "NV": 1100,
    "RI": 1700,
    "AZ": 1400,
    "WA": 1100,
    "UT": 1000,
    "TN": 1500,
    "TX": 3900,   # Hurricane + hail belt — structurally high
    "IN": 1500,   # Edge of Tornado Alley — moderate wind/hail exposure
}

# TX homestead exemption: ~$100K off taxable value for owner-occupied homes
# (passed in 2023). Other states have smaller or income-linked exemptions
# that are less material — modeled only for TX for now.
STATE_HOMESTEAD_EXEMPTION = {
    "TX": 100000,
}

# Median household income by state — U.S. Census Bureau ACS 2023 1-year
# estimates. Used as a fallback when FRED's state-level series
# (MEHOINUS{suffix}A672N) fails to fetch or is unavailable. Income moves
# ~3-5%/yr, so bump these annually when the next ACS release lands.
STATE_MEDIAN_INCOME_FALLBACK = {
    "CA": 96334,
    "NV": 76364,
    "RI": 86658,
    "AZ": 77315,
    "WA": 94605,
    "UT": 93421,
    "TN": 67097,
    "TX": 76292,
    "IN": 70051,
}


# ═══════════════════════════════════════════════════
# CACHING
# ═══════════════════════════════════════════════════

def _cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"

def _read_cache(key: str, max_age_hours: int = 6):
    p = _cache_path(key)
    if p.exists():
        age = time.time() - p.stat().st_mtime
        if age < max_age_hours * 3600:
            return json.loads(p.read_text())
    return None

def _write_cache(key: str, data):
    _cache_path(key).write_text(json.dumps(data, default=str))


# ═══════════════════════════════════════════════════
# FRED DATA FETCHING
# ═══════════════════════════════════════════════════

def _fetch_series(fred, series_id: str, start: str = "2016-01-01") -> dict | None:
    """Fetch a single FRED series, return dict with dates/values or None."""
    try:
        s = fred.get_series(series_id, observation_start=start)
        s = s.dropna()
        if len(s) == 0:
            return None
        values = [round(float(v), 2) for v in s.values]
        # Normalize pending-ratio units: FRED's county-level PENRAT{fips}
        # series often publishes as a decimal ratio (0.0-1.0) while the
        # state/national versions (PENRATUS, PENRATCA, etc.) publish as
        # percent (0-100). Rescale the decimal form to percent so UI
        # labels ("X%") and scoring thresholds are consistent across
        # levels. A max-value < 3 is a safe decimal-vs-percent signature
        # for pending ratio, which realistically always sits 10-90% when
        # expressed as a percentage.
        if series_id.startswith("PENRAT") and values and max(values) < 3:
            values = [round(v * 100, 2) for v in values]
        dates = [d.strftime("%Y-%m-%d") for d in s.index]
        result = {
            "dates": dates,
            "values": values,
            "current": values[-1],
            "series_id": series_id,
        }
        # YoY change (compare to ~12 months ago)
        if len(values) >= 13:
            old_val = values[-13]
            if old_val != 0:
                result["yoy_change"] = round((values[-1] - old_val) / abs(old_val) * 100, 1)
        # Peak and trough in last 24 months
        recent = values[-min(24, len(values)):]
        result["peak_24m"] = max(recent)
        result["trough_24m"] = min(recent)
        result["pct_from_peak"] = round((values[-1] - result["peak_24m"]) / result["peak_24m"] * 100, 1) if result["peak_24m"] != 0 else 0
        # 6-month trend (avg of last 3 vs avg of prior 3)
        if len(values) >= 6:
            recent_avg = sum(values[-3:]) / 3
            prior_avg = sum(values[-6:-3]) / 3
            if prior_avg != 0:
                result["trend_6m"] = round((recent_avg - prior_avg) / abs(prior_avg) * 100, 1)
        return result
    except Exception as e:
        logger.debug(f"Could not fetch {series_id}: {e}")
        return None


def _fetch_batch(fred, series_map: dict, start: str = "2016-01-01") -> dict:
    """Fetch multiple FRED series in parallel using threads."""
    result = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_series, fred, sid, start): name for name, sid in series_map.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                data = future.result()
                if data:
                    result[name] = data
            except Exception:
                pass
    return result


def get_national_data(api_key: str | None) -> dict:
    """Fetch all national-level macro indicators."""
    cached = _read_cache("national", max_age_hours=24)
    if cached:
        return cached
    if not api_key:
        return {"error": "FRED_API_KEY not set"}
    from fredapi import Fred
    fred = Fred(api_key=api_key)
    result = _fetch_batch(fred, NATIONAL_SERIES)
    _write_cache("national", result)
    return result


def get_county_data(api_key: str | None, state_code: str, fips: str) -> dict:
    """Fetch all available FRED series for a specific county."""
    cache_key = f"county_{fips}"
    cached = _read_cache(cache_key, max_age_hours=24)
    if cached:
        return cached
    if not api_key:
        return {"error": "FRED_API_KEY not set"}
    from fredapi import Fred
    fred = Fred(api_key=api_key)

    result = {
        "fips": fips,
        "county_name": COUNTIES.get(state_code, {}).get(fips, f"FIPS {fips}"),
        "state": state_code,
    }

    # Build all series to fetch in parallel
    all_series = {}
    for name, pattern in COUNTY_SERIES.items():
        all_series[name] = pattern.format(fips=fips)

    fips_padded = fips.zfill(5)
    extra_series = {}
    for name, pattern in COUNTY_SERIES_EXTRA.items():
        extra_series[name] = pattern.format(fips_padded=fips_padded)

    # Fetch all in parallel
    result.update(_fetch_batch(fred, all_series))
    result.update(_fetch_batch(fred, extra_series, start="2010-01-01"))

    # County signals: also pass national context (mortgage rate)
    national_ctx = get_national_data(api_key) or {}
    result["signals"] = compute_buy_signals(result, national=national_ctx)
    _write_cache(cache_key, result)
    return result


def get_all_state_data(api_key: str | None) -> dict:
    """Fetch summary data for all states + national — parallelized."""
    cached = _read_cache("all_states", max_age_hours=24)
    if cached:
        return cached
    if not api_key:
        return {"error": "FRED_API_KEY not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"}
    from fredapi import Fred
    fred = Fred(api_key=api_key)

    result = {"states": {}, "national": {}}

    # Build ALL series across national + 4 states into one big batch
    all_to_fetch = {}
    # National
    for name, sid in NATIONAL_SERIES.items():
        all_to_fetch[f"national__{name}"] = sid

    # States
    state_suffix = {
        "CA": "CA", "NV": "NV", "RI": "RI", "AZ": "AZ",
        "WA": "WA", "UT": "UT", "TN": "TN", "TX": "TX",
        "IN": "IN",
    }
    for code in STATES:
        suffix = state_suffix[code]
        state_series = {
            "median_list_price": f"MEDLISPRI{suffix}",
            "active_listings": f"ACTLISCOU{suffix}",
            "days_on_market": f"MEDDAYONMAR{suffix}",
            "new_listings": f"NEWLISCOU{suffix}",
            "pending_ratio": f"PENRAT{suffix}",
            "price_reduced_count": f"PRIREDCOU{suffix}",
            "median_sale_price": f"MEDSFHP{suffix}",
            "median_income": f"MEHOINUS{suffix}A672N",
            # Realtor.com publishes $/sqft at the state level too; not all state
            # suffixes have it on FRED, but the parallel fetcher silently drops
            # missing series so it's safe to request.
            "price_per_sqft": f"MEDLISPRIPERSQUFEE{suffix}",
        }
        for name, sid in state_series.items():
            all_to_fetch[f"{code}__{name}"] = sid

    # Fetch everything in parallel (10 threads)
    fetched = _fetch_batch(fred, all_to_fetch)

    # Sort results into national vs state buckets
    for key, data in fetched.items():
        parts = key.split("__", 1)
        if parts[0] == "national":
            result["national"][parts[1]] = data
        else:
            state_code = parts[0]
            metric = parts[1]
            if state_code not in result["states"]:
                result["states"][state_code] = {
                    "code": state_code,
                    "name": STATES[state_code]["name"],
                    "counties": list(COUNTIES.get(state_code, {}).keys()),
                }
            result["states"][state_code][metric] = data

    # Compute signals for each state (pass national so mortgage-rate factor can score)
    for code in result["states"]:
        result["states"][code]["signals"] = compute_buy_signals(
            result["states"][code], national=result.get("national", {})
        )

    _write_cache("all_states", result)
    return result


# ═══════════════════════════════════════════════════
# BUY SIGNAL SCORING
# ═══════════════════════════════════════════════════

def _lerp_score(value, breakpoints):
    """Piecewise-linear scoring: breakpoints = sorted list of (x, score) tuples.
    Values outside the endpoints clamp to the nearest endpoint score. Gives
    smooth continuous scores rather than step-function bins, so a 0.01 change
    in the input doesn't flip a bucket."""
    if value is None or not breakpoints:
        return 0.0
    bps = sorted(breakpoints, key=lambda x: x[0])
    if value <= bps[0][0]:
        return float(bps[0][1])
    if value >= bps[-1][0]:
        return float(bps[-1][1])
    for i in range(len(bps) - 1):
        x0, y0 = bps[i]
        x1, y1 = bps[i + 1]
        if x0 <= value <= x1:
            if x1 == x0:
                return float(y0)
            t = (value - x0) / (x1 - x0)
            return float(y0 + t * (y1 - y0))
    return 0.0


def _signal_label(points, max_points):
    """Map a sub-score fraction to a display badge."""
    if max_points <= 0:
        return "NEUTRAL"
    pct = points / max_points
    if pct >= 0.75:
        return "BUY"
    if pct >= 0.50:
        return "LEAN BUY"
    if pct >= 0.30:
        return "NEUTRAL"
    if pct >= 0.10:
        return "LEAN WAIT"
    return "WAIT"


def _monthly_mortgage_payment(principal, annual_rate_pct, years=30):
    """Standard amortizing-mortgage monthly payment."""
    if principal <= 0 or annual_rate_pct is None:
        return None
    r = (annual_rate_pct / 100.0) / 12.0
    n = years * 12
    if r == 0:
        return principal / n
    return principal * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def compute_buy_signals(data: dict, national: dict | None = None) -> dict:
    """
    Compute a buy/hold/wait market-timing signal from available indicators.

    Key design choices (vs. earlier versions):
      • Fixed 14-point scale with neutral filler for missing factors, so scores
        are comparable across states that differ in FRED data coverage.
      • Continuous (piecewise-linear) scoring — no step-function buckets.
      • Price direction is a single composite (peak / YoY / price-per-sqft) to
        prevent triple-counting the same underlying signal.
      • Affordability is payment-to-income (uses the actual 30-yr mortgage),
        not raw price-to-income — so the score responds to rate changes.
      • Mortgage-rate environment is its own factor (level vs 10-yr avg +
        recent trend), replacing the docstring-ghost factor in v1.
      • DOM / inventory / pending all benchmark against the longest available
        history (up to 10 years) rather than a rolling 24-month window, so
        "bad-but-normal-bad" markets can't baseline their way into WAIT.

    Factor weights (sum to 14.0):
      1. Price direction composite ........... 3.0
      2. Days on market ...................... 2.0
      3. Active inventory .................... 2.0
      4. New listings trend .................. 1.0
      5. Price reductions .................... 1.0
      6. Pending ratio ....................... 1.0
      7. Affordability (payment/income) ...... 2.0
      8. Mortgage-rate environment ........... 2.0
    """
    national = national or {}
    factors = []
    score = 0.0
    MAX = 14.0
    # neutral filler (used when a factor is missing — keeps 14pt denominator
    # so state/county scores remain comparable)
    NEUTRAL_FRACTION = 0.4  # treat missing factors as mildly "WAIT" neutral

    def add_factor(name, value_str, points, weight, detail=""):
        nonlocal score
        pts = max(0.0, min(weight, round(points, 2)))
        factors.append({
            "name": name,
            "value": value_str,
            "signal": _signal_label(pts, weight),
            "points": pts,
            "max_points": weight,
            "detail": detail,
        })
        score += pts

    def add_missing(name, weight, reason=""):
        nonlocal score
        pts = NEUTRAL_FRACTION * weight
        factors.append({
            "name": name + " (no data)",
            "value": "—",
            "signal": "NO DATA",
            "points": round(pts, 2),
            "max_points": weight,
            "detail": reason or "Series unavailable — neutral filler applied.",
        })
        score += pts

    # ─────────── Factor 1: Price Direction Composite (3 pts) ───────────
    # Blends three correlated-but-useful views of the same signal so the
    # total influence of "price direction" is capped at 3 pts (not the 4
    # it used to get across 3 separate factors).
    w1 = 3.0
    price_subs = []  # list of (label, sub_points, sub_weight)
    if "median_list_price" in data:
        pct_from_peak = data["median_list_price"].get("pct_from_peak", 0) or 0
        # -15% -> 1.5, -10% -> 1.2, -5% -> 0.6, 0% -> 0.0
        sp = _lerp_score(pct_from_peak, [(-15, 1.5), (-10, 1.2), (-5, 0.6), (-2, 0.3), (0, 0.0)])
        price_subs.append(("peak", sp, 1.5, f"{pct_from_peak:.1f}% vs 24-mo peak"))
    if "median_list_price_yoy" in data:
        yoy = data["median_list_price_yoy"].get("current", 0) or 0
        sp = _lerp_score(yoy, [(-10, 0.75), (-5, 0.6), (0, 0.35), (3, 0.1), (8, 0.0)])
        price_subs.append(("yoy", sp, 0.75, f"YoY {yoy:+.1f}%"))
    elif "median_list_price" in data:
        yoy = data["median_list_price"].get("yoy_change")
        if yoy is not None:
            sp = _lerp_score(yoy, [(-10, 0.75), (-5, 0.6), (0, 0.35), (3, 0.1), (8, 0.0)])
            price_subs.append(("yoy", sp, 0.75, f"YoY {yoy:+.1f}%"))
    if "price_per_sqft" in data:
        t = data["price_per_sqft"].get("trend_6m")
        if t is not None:
            sp = _lerp_score(t, [(-8, 0.75), (-4, 0.55), (0, 0.3), (4, 0.0)])
            price_subs.append(("psf", sp, 0.75, f"$/sqft 6-mo {t:+.1f}%"))
    if price_subs:
        pts = sum(sub[1] for sub in price_subs)
        # Normalise to the 3-pt weight regardless of how many sub-signals existed
        available_weight = sum(sub[2] for sub in price_subs)
        pts_scaled = pts * (w1 / available_weight) if available_weight > 0 else 0
        detail_str = " · ".join(sub[3] for sub in price_subs)
        add_factor("Price direction (composite)", detail_str, pts_scaled, w1,
                   "Blends price-vs-peak, YoY, and $/sqft 6-mo trend so price direction isn't triple-counted.")
    else:
        add_missing("Price direction", w1)

    # ─────────── Factor 2: Days on Market (2 pts) ───────────
    w2 = 2.0
    if "days_on_market" in data and len(data["days_on_market"]["values"]) >= 12:
        vals = data["days_on_market"]["values"]
        current = vals[-1]
        # Benchmark against longest-available history (up to 10 yrs / 120 months)
        window = min(120, len(vals))
        hist_avg = sum(vals[-window:]) / window
        ratio = current / hist_avg if hist_avg > 0 else 1.0
        # 1.0 ratio = neutral (1 pt). >1.3 = BUY (2 pts). <0.85 = seller's mkt (0).
        pts = _lerp_score(ratio, [(0.80, 0.0), (0.95, 0.5), (1.05, 1.1), (1.25, 1.8), (1.40, 2.0)])
        add_factor("Days on market vs history",
                   f"{current:.0f} days (hist avg {hist_avg:.0f}, {ratio:.2f}×)",
                   pts, w2,
                   "Higher = more buyer negotiation leverage. Benchmarked vs longest available history, not a rolling window.")
    else:
        add_missing("Days on market", w2)

    # ─────────── Factor 3: Active Inventory (2 pts) ───────────
    w3 = 2.0
    if "active_listings" in data and len(data["active_listings"]["values"]) >= 12:
        vals = data["active_listings"]["values"]
        current = vals[-1]
        window = min(120, len(vals))
        hist_avg = sum(vals[-window:]) / window
        ratio = current / hist_avg if hist_avg > 0 else 1.0
        pts = _lerp_score(ratio, [(0.70, 0.0), (0.90, 0.4), (1.00, 0.9), (1.15, 1.4), (1.30, 1.8), (1.50, 2.0)])
        add_factor("Active inventory vs history",
                   f"{current:,.0f} (hist avg {hist_avg:,.0f}, {ratio:.2f}×)",
                   pts, w3,
                   "More inventory = more buyer choice. Benchmarked against longest available history.")
    else:
        add_missing("Active inventory", w3)

    # ─────────── Factor 4: New Listings Trend (1 pt) ───────────
    w4 = 1.0
    if "new_listings" in data:
        t = data["new_listings"].get("trend_6m")
        if t is not None:
            pts = _lerp_score(t, [(-10, 0.0), (-3, 0.2), (0, 0.4), (5, 0.7), (15, 1.0)])
            add_factor("New listings 6-mo trend", f"{t:+.1f}%", pts, w4,
                       "Rising supply pipeline = better buyer conditions ahead.")
        else:
            add_missing("New listings trend", w4)
    else:
        add_missing("New listings trend", w4)

    # ─────────── Factor 5: Price Reductions (1 pt) ───────────
    w5 = 1.0
    if "price_reduced_count" in data:
        t = data["price_reduced_count"].get("trend_6m")
        if t is not None:
            pts = _lerp_score(t, [(-20, 0.0), (-5, 0.25), (5, 0.5), (20, 0.9), (40, 1.0)])
            add_factor("Price-cut activity (6-mo trend)", f"{t:+.1f}%", pts, w5,
                       "Accelerating cuts = seller capitulation, stronger buyer negotiation.")
        else:
            add_missing("Price cuts", w5)
    else:
        add_missing("Price cuts", w5)

    # ─────────── Factor 6: Pending Ratio (1 pt) ───────────
    w6 = 1.0
    if "pending_ratio" in data and len(data["pending_ratio"]["values"]) >= 6:
        vals = data["pending_ratio"]["values"]
        current = vals[-1]
        window = min(60, len(vals))
        hist_avg = sum(vals[-window:]) / window
        ratio = current / hist_avg if hist_avg > 0 else 1.0
        # Lower pending ratio = weaker demand = better for buyer
        pts = _lerp_score(ratio, [(0.70, 1.0), (0.85, 0.75), (0.95, 0.5), (1.05, 0.25), (1.20, 0.0)])
        add_factor("Pending ratio vs history",
                   f"{current:.1f}% (hist avg {hist_avg:.1f}%, {ratio:.2f}×)",
                   pts, w6,
                   "Low ratio = fewer deals under contract = less buyer competition.")
    else:
        add_missing("Pending ratio", w6)

    # ─────────── Factor 7: Affordability — True PITI/Income (2 pts) ───────────
    # Upgrade: computes full PITI (Principal + Interest + Tax + Insurance)
    # rather than raw P&I. Uses state-specific property-tax effective rates
    # and homeowners-insurance premiums, plus the TX homestead exemption.
    # Breakpoints recalibrated to the canonical 28% front-end DTI standard
    # (which is defined on PITI, not P&I).
    #
    # Why this matters: TX has no state income tax but ~1.8% property-tax
    # rates + high insurance, which offsets much of its apparent
    # affordability edge. CA has low property-tax rates (Prop 13) so its
    # affordability penalty is almost entirely from price/income/rate.
    w7 = 2.0
    mortgage = (national or {}).get("mortgage_30yr", {})
    mort_rate = mortgage.get("current") if isinstance(mortgage, dict) else None
    price = None
    price_src = None
    if "median_sale_price" in data:
        price = data["median_sale_price"].get("current")
        price_src = "sale"
    elif "median_list_price" in data:
        price = data["median_list_price"].get("current")
        price_src = "list"
    income = data.get("median_income", {}).get("current") if "median_income" in data else None
    # State code can come from either path (get_all_state_data sets "code",
    # get_county_data sets "state"). Fall back to data["state"] for county.
    state_code = data.get("code") or data.get("state")
    # Income fallback — FRED state-level series lag and occasionally fail to
    # resolve. Fall back to the hardcoded Census ACS 2023 figure so the
    # affordability factor always has an income baseline.
    income_source = "FRED state series"
    if not income or income <= 0:
        fallback = STATE_MEDIAN_INCOME_FALLBACK.get(state_code)
        if fallback:
            income = fallback
            income_source = "Census ACS 2023 (fallback)"

    if price and income and income > 0 and mort_rate:
        principal = price * 0.80  # 20% down
        monthly_pi = _monthly_mortgage_payment(principal, mort_rate, years=30)
        if monthly_pi:
            # Property tax (apply TX homestead exemption where relevant)
            tax_rate = STATE_PROPERTY_TAX_RATE.get(state_code, 0.011)  # ~1.1% fallback ≈ US avg
            exemption = STATE_HOMESTEAD_EXEMPTION.get(state_code, 0)
            taxable_value = max(0, price - exemption)
            annual_tax = taxable_value * tax_rate
            monthly_tax = annual_tax / 12.0
            # Home insurance
            annual_ins = STATE_INSURANCE_ANNUAL.get(state_code, 1800)  # ~US avg fallback
            monthly_ins = annual_ins / 12.0
            # Full PITI
            monthly_piti = monthly_pi + monthly_tax + monthly_ins
            monthly_income = income / 12.0
            piti_pct = monthly_piti / monthly_income
            # Underwriting standard: 28% front-end DTI (PITI/income) is the
            # canonical "healthy" threshold. 36% = borderline, 43% = distress.
            pts = _lerp_score(piti_pct, [
                (0.20, 2.0),   # excellent
                (0.28, 1.6),   # healthy (classic 28% rule)
                (0.33, 1.0),   # borderline
                (0.40, 0.5),   # stretched
                (0.48, 0.2),   # distress
                (0.55, 0.0),   # unaffordable
            ])
            exempt_note = f" minus ${exemption:,.0f} homestead exemption" if exemption else ""
            add_factor(
                "Affordability (PITI/income)",
                (f"{piti_pct*100:.0f}% of income · "
                 f"PITI ${monthly_piti:,.0f}/mo = "
                 f"P&I ${monthly_pi:,.0f} + tax ${monthly_tax:,.0f} + ins ${monthly_ins:,.0f}"),
                pts, w7,
                (f"Full PITI on 80% LTV {price_src} price at {mort_rate:.2f}% vs monthly "
                 f"median income (${income:,.0f}/yr · {income_source}). "
                 f"Uses {state_code or 'state'} property-tax rate "
                 f"{tax_rate*100:.2f}%{exempt_note} and ~${annual_ins:,.0f}/yr insurance. "
                 "Scored against the canonical 28% front-end DTI underwriting standard."),
            )
        else:
            add_missing("Affordability", w7, "Could not compute mortgage payment.")
    elif price and income and income > 0:
        # Fallback — rate unavailable, degrade to price/income (no PITI possible)
        ratio = price / income
        pts = _lerp_score(ratio, [(3.0, 2.0), (4.5, 1.2), (6.0, 0.6), (8.0, 0.2), (10.0, 0.0)])
        add_factor("Affordability (price/income, fallback)",
                   f"{ratio:.1f}× income (rate unavailable — PITI not computed)", pts, w7,
                   "Rate data missing — using raw price/income as a rough fallback.")
    else:
        add_missing("Affordability", w7, "Need price, income, and mortgage-rate data.")

    # ─────────── Factor 8: Mortgage-Rate Environment (2 pts) ───────────
    w8 = 2.0
    if isinstance(mortgage, dict) and mortgage.get("values"):
        vals = mortgage["values"]
        current = vals[-1]
        # ~10-yr avg if available (weekly series → 520 obs); else all-history
        window = min(520, len(vals))
        hist_avg = sum(vals[-window:]) / window
        trend_6m = mortgage.get("trend_6m", 0) or 0
        # Two sub-signals, each up to 1 pt:
        # 8a) Rate direction (falling = BUY — refi optionality + easing payment)
        sp_trend = _lerp_score(trend_6m, [(-15, 1.0), (-5, 0.8), (0, 0.4), (5, 0.2), (15, 0.0)])
        # 8b) Rate level (elevated = less buyer competition, room for future refi)
        level_pct = (current - hist_avg) / hist_avg * 100 if hist_avg > 0 else 0
        sp_level = _lerp_score(level_pct, [(-25, 0.2), (-10, 0.4), (0, 0.6), (15, 0.8), (30, 1.0)])
        pts = sp_trend + sp_level
        add_factor("Mortgage-rate environment",
                   f"{current:.2f}% ({level_pct:+.0f}% vs hist avg, 6-mo trend {trend_6m:+.1f}%)",
                   pts, w8,
                   "Falling rates improve payment + refi optionality. Elevated rates reduce buyer competition.")
    else:
        add_missing("Mortgage-rate environment", w8, "30-yr mortgage data unavailable.")

    # ─────────── Final Rating ───────────
    score = round(score, 2)
    pct = score / MAX
    if pct >= 0.70:
        rating, detail = "STRONG BUY", "Multiple indicators favor buyers — compelling entry point."
    elif pct >= 0.55:
        rating, detail = "BUY", "Most indicators favor buyers."
    elif pct >= 0.42:
        rating, detail = "LEAN BUY", "Tilting buyer-friendly with mixed signals."
    elif pct >= 0.30:
        rating, detail = "NEUTRAL", "Balanced market — no strong buyer or seller edge."
    elif pct >= 0.18:
        rating, detail = "LEAN WAIT", "Seller-tilted with some softening — consider waiting for clearer entry."
    else:
        rating, detail = "WAIT", "Seller's market — conditions favor waiting for better entry."

    return {
        "factors": factors,
        "score": score,
        "max_score": MAX,
        "score_pct": round(pct * 100, 1),
        "rating": rating,
        "rating_detail": detail,
    }


# ═══════════════════════════════════════════════════
# STOCK SCREENER — Financial Modeling Prep (FMP) API
# ═══════════════════════════════════════════════════

FMP_BASES = [
    "https://financialmodelingprep.com/stable",
    "https://financialmodelingprep.com/api/v3",
]


def _fmp_get(endpoint: str, params: dict, api_key: str, base_url: str = None) -> list | dict | None:
    """Make a GET request to FMP API. Tries stable then v3 fallback."""
    bases = [base_url] if base_url else FMP_BASES
    params_copy = dict(params)
    params_copy["apikey"] = api_key

    for base in bases:
        query = "&".join(f"{k}={v}" for k, v in params_copy.items())
        url = f"{base}/{endpoint}?{query}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"FMP OK: {base}/{endpoint} returned {len(data)} results")
                    return data
                elif isinstance(data, dict) and "Error Message" not in data:
                    logger.info(f"FMP OK: {base}/{endpoint} returned dict")
                    return data
                else:
                    logger.warning(f"FMP empty/error from {base}/{endpoint}: {str(data)[:200]}")
        except Exception as e:
            logger.warning(f"FMP error {base}/{endpoint}: {e}")
    return None


def _test_fmp_connection(api_key: str) -> dict:
    """Test FMP API connectivity and return debug info."""
    results = {}
    # Test basic quote endpoint
    for base in FMP_BASES:
        try:
            url = f"{base}/quote?symbol=AAPL&apikey={api_key}"
            req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
                results[base + "/quote"] = {"status": "OK", "count": len(data) if isinstance(data, list) else 1, "sample": str(data)[:200]}
        except Exception as e:
            results[base + "/quote"] = {"status": "ERROR", "error": str(e)[:200]}

    # Test screener endpoints
    screener_endpoints = ["company-screener", "stock-screener", "stock_screener"]
    for ep in screener_endpoints:
        for base in FMP_BASES:
            try:
                url = f"{base}/{ep}?marketCapMoreThan=100000000000&limit=3&apikey={api_key}"
                req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read())
                    results[f"{base}/{ep}"] = {"status": "OK", "count": len(data) if isinstance(data, list) else 1, "sample": str(data)[:300]}
            except Exception as e:
                results[f"{base}/{ep}"] = {"status": "ERROR", "error": str(e)[:200]}
    return results


def get_stock_screener_fmp(api_key: str | None) -> list[dict]:
    """
    Fetch stocks with net profit margin > 20% using FMP Stock Screener.
    Covers all market caps including micro-cap and small-cap.
    Tries multiple endpoint patterns for compatibility.
    """
    cached = _read_cache("fmp_screener", max_age_hours=6)
    if cached:
        return cached

    if not api_key:
        return [{"error": "FMP_API_KEY not set. Get a free key at https://financialmodelingprep.com/register"}]

    all_results = []

    # Try multiple screener endpoint names
    screener_endpoints = ["company-screener", "stock-screener", "stock_screener"]

    cap_ranges = [
        ("Mega", 200_000_000_000, None),
        ("Large", 10_000_000_000, 200_000_000_000),
        ("Mid", 2_000_000_000, 10_000_000_000),
        ("Small", 300_000_000, 2_000_000_000),
        ("Micro", 0, 300_000_000),
    ]

    # Find a working screener endpoint first
    working_endpoint = None
    working_base = None
    for ep in screener_endpoints:
        for base in FMP_BASES:
            test_params = {"marketCapMoreThan": 100000000000, "limit": 3}
            data = _fmp_get(ep, test_params, api_key, base_url=base)
            if data and isinstance(data, list) and len(data) > 0:
                working_endpoint = ep
                working_base = base
                logger.info(f"FMP: Found working screener at {base}/{ep}")
                break
        if working_endpoint:
            break

    if not working_endpoint:
        logger.error("FMP: No working screener endpoint found")
        # Fallback: use stock-list + profile approach
        return _fallback_screener(api_key)

    for cap_label, cap_min, cap_max in cap_ranges:
        # Try both parameter naming conventions
        params = {
            "marketCapMoreThan": cap_min,
            "isActivelyTrading": "true",
            "exchange": "NYSE,NASDAQ,AMEX",
            "limit": 500,
        }
        if cap_max:
            params["marketCapLessThan"] = cap_max

        # Try with netIncomeMargin filter
        params_with_margin = dict(params)
        params_with_margin["netIncomeMarginMoreThan"] = 20

        data = _fmp_get(working_endpoint, params_with_margin, api_key, base_url=working_base)

        # If margin filter didn't work, fetch without and filter client-side
        if not data or not isinstance(data, list) or len(data) == 0:
            data = _fmp_get(working_endpoint, params, api_key, base_url=working_base)

        if data and isinstance(data, list):
            for item in data:
                item["_cap_category"] = cap_label
            all_results.extend(data)
            logger.info(f"FMP screener {cap_label} cap: {len(data)} results")

    # Now enrich with quotes for price/DMA data
    # Get all tickers
    tickers = [r.get("symbol") for r in all_results if r.get("symbol")]

    # Fetch quotes in batches of 50 (FMP supports comma-separated)
    quotes_map = {}
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        symbols = ",".join(batch)
        quote_data = _fmp_get("quote", {"symbol": symbols}, api_key)
        if quote_data and isinstance(quote_data, list):
            for q in quote_data:
                quotes_map[q.get("symbol")] = q

    # Build final results
    results = []
    for item in all_results:
        ticker = item.get("symbol", "")
        if not ticker:
            continue

        quote = quotes_map.get(ticker, {})
        price = quote.get("price") or item.get("price", 0)
        market_cap = item.get("marketCap") or quote.get("marketCap", 0)

        # Calculate distances
        two_hundred_dma = quote.get("priceAvg200", 0)
        fifty_day_ma = quote.get("priceAvg50", 0)
        year_high = quote.get("yearHigh", 0)
        year_low = quote.get("yearLow", 0)
        dma_distance = ((price - two_hundred_dma) / two_hundred_dma * 100) if two_hundred_dma and price else 0
        high_distance = ((price - year_high) / year_high * 100) if year_high and price else 0

        # Net margin from screener
        net_margin = item.get("netIncomeMargin")
        if net_margin is None:
            continue
        # FMP returns as decimal (0.25) or percentage depending on endpoint
        if net_margin > 1:
            net_margin_pct = round(net_margin, 1)
        else:
            net_margin_pct = round(net_margin * 100, 1)

        if net_margin_pct < 20:
            continue

        revenue_growth = item.get("revenueGrowth")
        if revenue_growth is not None:
            if abs(revenue_growth) < 1:
                revenue_growth = round(revenue_growth * 100, 1)
            else:
                revenue_growth = round(revenue_growth, 1)

        results.append({
            "ticker": ticker,
            "name": item.get("companyName", ticker),
            "sector": item.get("sector", "N/A"),
            "industry": item.get("industry", "N/A"),
            "price": round(price, 2) if price else 0,
            "market_cap": market_cap,
            "market_cap_fmt": _fmt_market_cap(market_cap),
            "cap_category": item.get("_cap_category", "N/A"),
            "net_margin": net_margin_pct,
            "revenue_growth": revenue_growth,
            "two_hundred_dma": round(two_hundred_dma, 2) if two_hundred_dma else None,
            "dma_distance": round(dma_distance, 1),
            "fifty_day_ma": round(fifty_day_ma, 2) if fifty_day_ma else None,
            "fifty_two_high": round(year_high, 2) if year_high else None,
            "high_distance": round(high_distance, 1),
            "fifty_two_low": round(year_low, 2) if year_low else None,
            "volume": quote.get("volume", 0),
            "avg_volume": quote.get("avgVolume", 0),
            "pe_ratio": round(quote.get("pe", 0), 1) if quote.get("pe") else None,
            "eps": round(quote.get("eps", 0), 2) if quote.get("eps") else None,
            "exchange": item.get("exchange", "N/A"),
            "country": item.get("country", "N/A"),
        })

    # Deduplicate by ticker
    seen = set()
    deduped = []
    for r in results:
        if r["ticker"] not in seen:
            seen.add(r["ticker"])
            deduped.append(r)

    deduped.sort(key=lambda x: x["net_margin"], reverse=True)
    _write_cache("fmp_screener", deduped)
    return deduped


def _fallback_screener(api_key: str) -> list[dict]:
    """Fallback: fetch top stocks via stock-list and filter by getting key metrics."""
    logger.info("FMP: Using fallback screener approach")
    # Get the full stock list
    data = _fmp_get("stock-list", {}, api_key)
    if not data or not isinstance(data, list):
        return []

    # Filter to US exchanges only
    us_stocks = [s for s in data if s.get("exchangeShortName") in ("NYSE", "NASDAQ", "AMEX") and s.get("type") == "stock"]

    # Get key metrics for batches — this uses more API calls but works on free tier
    results = []
    # Limit to first 200 by market cap to save API calls
    # Sort by name just to be consistent
    tickers = [s.get("symbol") for s in us_stocks[:500] if s.get("symbol")]

    batch_size = 50
    for i in range(0, min(len(tickers), 200), batch_size):
        batch = tickers[i:i + batch_size]
        symbols = ",".join(batch)
        quote_data = _fmp_get("quote", {"symbol": symbols}, api_key)
        if quote_data and isinstance(quote_data, list):
            for q in quote_data:
                # Filter for PE > 0 and reasonable metrics
                pe = q.get("pe")
                eps = q.get("eps")
                price = q.get("price", 0)
                market_cap = q.get("marketCap", 0)
                if price and market_cap and eps and pe and pe > 0:
                    results.append(q)

    # Build output (without net margin since quotes don't have it)
    output = []
    for q in results:
        market_cap = q.get("marketCap", 0)
        cap_cat = "Micro" if market_cap < 3e8 else "Small" if market_cap < 2e9 else "Mid" if market_cap < 1e10 else "Large" if market_cap < 2e11 else "Mega"
        price = q.get("price", 0)
        dma200 = q.get("priceAvg200", 0)
        year_high = q.get("yearHigh", 0)
        output.append({
            "ticker": q.get("symbol", ""),
            "name": q.get("name", ""),
            "sector": "N/A",
            "industry": "N/A",
            "price": round(price, 2),
            "market_cap": market_cap,
            "market_cap_fmt": _fmt_market_cap(market_cap),
            "cap_category": cap_cat,
            "net_margin": 0,
            "revenue_growth": None,
            "two_hundred_dma": round(dma200, 2) if dma200 else None,
            "dma_distance": round((price - dma200) / dma200 * 100, 1) if dma200 and price else 0,
            "fifty_day_ma": round(q.get("priceAvg50", 0), 2) if q.get("priceAvg50") else None,
            "fifty_two_high": round(year_high, 2) if year_high else None,
            "high_distance": round((price - year_high) / year_high * 100, 1) if year_high and price else 0,
            "fifty_two_low": round(q.get("yearLow", 0), 2) if q.get("yearLow") else None,
            "volume": q.get("volume", 0),
            "avg_volume": q.get("avgVolume", 0),
            "pe_ratio": round(q.get("pe", 0), 1) if q.get("pe") else None,
            "eps": round(q.get("eps", 0), 2) if q.get("eps") else None,
            "exchange": q.get("exchange", "N/A"),
            "country": "US",
        })

    _write_cache("fmp_screener", output)
    return output


def _fmt_market_cap(val: int) -> str:
    if not val:
        return "N/A"
    if val >= 1e12:
        return f"${val/1e12:.1f}T"
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    if val >= 1e6:
        return f"${val/1e6:.0f}M"
    return f"${val:,.0f}"
