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

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


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

    result["signals"] = compute_buy_signals(result)
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
    state_suffix = {"CA": "CA", "NV": "NV", "RI": "RI", "AZ": "AZ"}
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

    # Compute signals for each state
    for code in result["states"]:
        result["states"][code]["signals"] = compute_buy_signals(result["states"][code])

    _write_cache("all_states", result)
    return result


# ═══════════════════════════════════════════════════
# BUY SIGNAL SCORING
# ═══════════════════════════════════════════════════

def compute_buy_signals(data: dict) -> dict:
    """
    Compute buy/hold/wait signals based on all available market indicators.
    Each factor scores 0-2 points. More factors = more robust signal.

    Factors:
    1. Price vs 24-month peak (are prices correcting?)
    2. Days on market trend (is buyer leverage increasing?)
    3. Inventory / active listings vs historical avg
    4. New listings trend (is supply expanding?)
    5. Price reduced count (are sellers cutting prices?)
    6. Pending ratio (is demand slowing?)
    7. Affordability: price-to-income ratio
    8. Price per sq ft trend
    9. Mortgage rate environment
    """
    signals = {"factors": [], "score": 0, "max_score": 0}

    # ── Factor 1: Price vs Peak ──
    if "median_list_price" in data:
        d = data["median_list_price"]
        signals["max_score"] += 2
        pct = d.get("pct_from_peak", 0)
        if pct < -10:
            signals["factors"].append({"name": "Price correction from peak", "value": f"{pct:.1f}%", "signal": "BUY", "points": 2, "detail": "Prices have dropped significantly from recent highs"})
            signals["score"] += 2
        elif pct < -5:
            signals["factors"].append({"name": "Price softening from peak", "value": f"{pct:.1f}%", "signal": "LEAN BUY", "points": 1, "detail": "Prices are cooling but haven't dropped sharply"})
            signals["score"] += 1
        elif pct < -2:
            signals["factors"].append({"name": "Price plateau near peak", "value": f"{pct:.1f}%", "signal": "NEUTRAL", "points": 0.5, "detail": "Prices are flat to slightly down"})
            signals["score"] += 0.5
        else:
            signals["factors"].append({"name": "Prices at or near peak", "value": f"{pct:.1f}%", "signal": "WAIT", "points": 0, "detail": "Market is at peak pricing — less room for upside"})

    # ── Factor 2: Days on Market ──
    if "days_on_market" in data:
        d = data["days_on_market"]
        signals["max_score"] += 2
        vals = d["values"]
        if len(vals) >= 12:
            current = vals[-1]
            avg_12m = sum(vals[-12:]) / 12
            ratio = current / avg_12m if avg_12m > 0 else 1
            if ratio > 1.25:
                signals["factors"].append({"name": "Days on market elevated", "value": f"{current:.0f} days (avg {avg_12m:.0f})", "signal": "BUY", "points": 2, "detail": "Homes sitting longer = more negotiation leverage"})
                signals["score"] += 2
            elif ratio > 1.05:
                signals["factors"].append({"name": "Days on market rising", "value": f"{current:.0f} days (avg {avg_12m:.0f})", "signal": "LEAN BUY", "points": 1, "detail": "Market slowing slightly, some buyer leverage"})
                signals["score"] += 1
            else:
                signals["factors"].append({"name": "Days on market low/normal", "value": f"{current:.0f} days (avg {avg_12m:.0f})", "signal": "SELLER'S MKT", "points": 0, "detail": "Homes selling quickly — competitive market"})

    # ── Factor 3: Active Inventory ──
    if "active_listings" in data:
        d = data["active_listings"]
        signals["max_score"] += 2
        vals = d["values"]
        if len(vals) >= 12:
            current = vals[-1]
            avg_24m = sum(vals[-min(24,len(vals)):]) / min(24, len(vals))
            ratio = current / avg_24m if avg_24m > 0 else 1
            if ratio > 1.3:
                signals["factors"].append({"name": "Inventory well above average", "value": f"{current:,.0f} (avg {avg_24m:,.0f})", "signal": "BUY", "points": 2, "detail": "Lots of homes to choose from = buyer's market"})
                signals["score"] += 2
            elif ratio > 1.1:
                signals["factors"].append({"name": "Inventory above average", "value": f"{current:,.0f} (avg {avg_24m:,.0f})", "signal": "LEAN BUY", "points": 1, "detail": "Supply is building, shifting toward buyers"})
                signals["score"] += 1
            else:
                signals["factors"].append({"name": "Inventory tight", "value": f"{current:,.0f} (avg {avg_24m:,.0f})", "signal": "LOW SUPPLY", "points": 0, "detail": "Limited inventory = competitive bidding likely"})

    # ── Factor 4: New Listings Trend ──
    if "new_listings" in data:
        d = data["new_listings"]
        signals["max_score"] += 1
        trend = d.get("trend_6m")
        if trend is not None:
            if trend > 10:
                signals["factors"].append({"name": "New listings surging", "value": f"+{trend:.1f}% (6mo trend)", "signal": "BUY", "points": 1, "detail": "Fresh supply hitting market — more options coming"})
                signals["score"] += 1
            elif trend > 0:
                signals["factors"].append({"name": "New listings increasing", "value": f"+{trend:.1f}% (6mo trend)", "signal": "LEAN BUY", "points": 0.5, "detail": "Supply gradually improving"})
                signals["score"] += 0.5
            else:
                signals["factors"].append({"name": "New listings declining", "value": f"{trend:.1f}% (6mo trend)", "signal": "TIGHT", "points": 0, "detail": "Fewer new homes coming to market"})

    # ── Factor 5: Price Reductions ──
    if "price_reduced_count" in data:
        d = data["price_reduced_count"]
        signals["max_score"] += 2
        vals = d["values"]
        if len(vals) >= 6:
            trend = d.get("trend_6m")
            if trend is not None and trend > 20:
                signals["factors"].append({"name": "Price cuts accelerating", "value": f"+{trend:.1f}% (6mo trend)", "signal": "BUY", "points": 2, "detail": "Sellers are capitulating — strong negotiation position"})
                signals["score"] += 2
            elif trend is not None and trend > 5:
                signals["factors"].append({"name": "Price cuts increasing", "value": f"+{trend:.1f}% (6mo trend)", "signal": "LEAN BUY", "points": 1, "detail": "More sellers willing to negotiate"})
                signals["score"] += 1
            else:
                val_str = f"{trend:.1f}%" if trend is not None else "stable"
                signals["factors"].append({"name": "Price cuts stable/declining", "value": val_str, "signal": "NEUTRAL", "points": 0, "detail": "Sellers holding firm on pricing"})

    # ── Factor 6: Pending Ratio (demand indicator) ──
    if "pending_ratio" in data:
        d = data["pending_ratio"]
        signals["max_score"] += 1
        vals = d["values"]
        if len(vals) >= 6:
            current = vals[-1]
            # Pending ratio: higher = more demand. Lower = less demand (good for buyers)
            avg = sum(vals[-12:]) / min(12, len(vals))
            if current < avg * 0.85:
                signals["factors"].append({"name": "Demand weakening (pending ratio)", "value": f"{current:.1f}% (avg {avg:.1f}%)", "signal": "BUY", "points": 1, "detail": "Fewer homes going under contract — less competition"})
                signals["score"] += 1
            elif current < avg:
                signals["factors"].append({"name": "Demand softening", "value": f"{current:.1f}% (avg {avg:.1f}%)", "signal": "LEAN BUY", "points": 0.5, "detail": "Slightly fewer pending sales than normal"})
                signals["score"] += 0.5
            else:
                signals["factors"].append({"name": "Demand strong (pending ratio)", "value": f"{current:.1f}% (avg {avg:.1f}%)", "signal": "HOT", "points": 0, "detail": "High pending rate = competitive market"})

    # ── Factor 7: Affordability (price-to-income) ──
    if "median_sale_price" in data and "median_income" in data:
        signals["max_score"] += 2
        price = data["median_sale_price"]["current"]
        income = data["median_income"]["current"]
        if income > 0:
            ratio = price / income
            if ratio < 4:
                signals["factors"].append({"name": "Highly affordable (price/income)", "value": f"{ratio:.1f}x", "signal": "BUY", "points": 2, "detail": "Homes priced below 4x median income — historically affordable"})
                signals["score"] += 2
            elif ratio < 5.5:
                signals["factors"].append({"name": "Moderately affordable", "value": f"{ratio:.1f}x", "signal": "LEAN BUY", "points": 1, "detail": "Near historical norms for affordability"})
                signals["score"] += 1
            elif ratio < 7:
                signals["factors"].append({"name": "Affordability stretched", "value": f"{ratio:.1f}x", "signal": "STRETCHED", "points": 0.5, "detail": "Prices outpacing incomes — watch for correction"})
                signals["score"] += 0.5
            else:
                signals["factors"].append({"name": "Severely unaffordable", "value": f"{ratio:.1f}x", "signal": "EXPENSIVE", "points": 0, "detail": "Prices far exceed income — high risk of correction"})
    elif "median_list_price" in data and "median_income" in data:
        signals["max_score"] += 2
        price = data["median_list_price"]["current"]
        income = data["median_income"]["current"]
        if income > 0:
            ratio = price / income
            if ratio < 5:
                signals["factors"].append({"name": "Affordable (list price/income)", "value": f"{ratio:.1f}x", "signal": "BUY", "points": 2})
                signals["score"] += 2
            elif ratio < 7:
                signals["factors"].append({"name": "Moderate affordability", "value": f"{ratio:.1f}x", "signal": "NEUTRAL", "points": 1})
                signals["score"] += 1
            else:
                signals["factors"].append({"name": "Unaffordable", "value": f"{ratio:.1f}x", "signal": "EXPENSIVE", "points": 0})

    # ── Factor 8: Price per Sq Ft trend ──
    if "price_per_sqft" in data:
        d = data["price_per_sqft"]
        signals["max_score"] += 1
        trend = d.get("trend_6m")
        if trend is not None:
            if trend < -5:
                signals["factors"].append({"name": "Price/sqft declining", "value": f"{trend:.1f}% (6mo)", "signal": "BUY", "points": 1, "detail": "Value improving — getting more for your money"})
                signals["score"] += 1
            elif trend < 0:
                signals["factors"].append({"name": "Price/sqft flat to down", "value": f"{trend:.1f}% (6mo)", "signal": "LEAN BUY", "points": 0.5})
                signals["score"] += 0.5
            else:
                signals["factors"].append({"name": "Price/sqft rising", "value": f"+{trend:.1f}% (6mo)", "signal": "RISING", "points": 0})

    # ── Factor 9: YoY Price Change (from FRED if available) ──
    if "median_list_price_yoy" in data:
        d = data["median_list_price_yoy"]
        signals["max_score"] += 1
        current = d["current"]
        if current < -5:
            signals["factors"].append({"name": "Prices down YoY (FRED)", "value": f"{current:.1f}%", "signal": "BUY", "points": 1})
            signals["score"] += 1
        elif current < 0:
            signals["factors"].append({"name": "Prices slightly down YoY", "value": f"{current:.1f}%", "signal": "LEAN BUY", "points": 0.5})
            signals["score"] += 0.5
        else:
            signals["factors"].append({"name": "Prices up YoY", "value": f"+{current:.1f}%", "signal": "RISING", "points": 0})

    # ── Final Rating ──
    max_s = signals["max_score"]
    if max_s > 0:
        pct = signals["score"] / max_s
        if pct >= 0.70:
            signals["rating"] = "STRONG BUY"
            signals["rating_detail"] = "Multiple indicators favor buyers — compelling entry point"
        elif pct >= 0.55:
            signals["rating"] = "BUY"
            signals["rating_detail"] = "Most indicators are favorable for buyers"
        elif pct >= 0.40:
            signals["rating"] = "LEAN BUY"
            signals["rating_detail"] = "Some positive signals but mixed overall"
        elif pct >= 0.25:
            signals["rating"] = "NEUTRAL"
            signals["rating_detail"] = "Market is balanced — neither strongly favoring buyers or sellers"
        else:
            signals["rating"] = "WAIT"
            signals["rating_detail"] = "Seller's market — consider waiting for better conditions"
        signals["score_pct"] = round(pct * 100)
    else:
        signals["rating"] = "INSUFFICIENT DATA"
        signals["rating_detail"] = "Not enough data to generate a signal"
        signals["score_pct"] = 0

    return signals


# ═══════════════════════════════════════════════════
# STOCK SCREENER — Financial Modeling Prep (FMP) API
# ═══════════════════════════════════════════════════

FMP_BASE = "https://financialmodelingprep.com/stable"


def _fmp_get(endpoint: str, params: dict, api_key: str) -> list | dict | None:
    """Make a GET request to FMP API."""
    params["apikey"] = api_key
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{FMP_BASE}/{endpoint}?{query}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.warning(f"FMP API error for {endpoint}: {e}")
        return None


def get_stock_screener_fmp(api_key: str | None) -> list[dict]:
    """
    Fetch stocks with net profit margin > 20% using FMP Stock Screener.
    Covers all market caps including micro-cap and small-cap.
    Single API call returns up to 1000 results.
    """
    cached = _read_cache("fmp_screener", max_age_hours=6)
    if cached:
        return cached

    if not api_key:
        return [{"error": "FMP_API_KEY not set. Get a free key at https://financialmodelingprep.com/register"}]

    all_results = []

    # FMP screener — fetch companies with >20% net income margin
    # Pull in batches by market cap range to get micro/small/mid/large caps
    cap_ranges = [
        ("Mega", 200_000_000_000, None),          # >$200B
        ("Large", 10_000_000_000, 200_000_000_000), # $10B-$200B
        ("Mid", 2_000_000_000, 10_000_000_000),     # $2B-$10B
        ("Small", 300_000_000, 2_000_000_000),      # $300M-$2B
        ("Micro", 0, 300_000_000),                   # <$300M
    ]

    for cap_label, cap_min, cap_max in cap_ranges:
        params = {
            "netIncomeMarginMoreThan": 20,
            "marketCapMoreThan": cap_min,
            "isActivelyTrading": "true",
            "exchange": "NYSE,NASDAQ,AMEX",
            "limit": 500,
        }
        if cap_max:
            params["marketCapLessThan"] = cap_max

        data = _fmp_get("company-screener", params, api_key)
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
