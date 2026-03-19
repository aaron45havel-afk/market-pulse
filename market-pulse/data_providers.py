"""Data providers for real estate and finance data."""
import os, json, time, logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

CACHE_DIR = Path("/tmp/market_pulse_cache")
CACHE_DIR.mkdir(exist_ok=True)

STATES = {
    "CA": {"name": "California", "fips": "06"},
    "NV": {"name": "Nevada", "fips": "32"},
    "RI": {"name": "Rhode Island", "fips": "44"},
    "AZ": {"name": "Arizona", "fips": "04"},
}

# FRED series IDs per state
# Median listing price, active listings, median days on market
FRED_SERIES = {
    "mortgage_30yr": "MORTGAGE30US",
    "CA": {
        "median_list_price": "MEDLISPRI06",
        "median_sale_price": "MEDSFHPCA",
        "active_listings": "ACTLISCOU06",
        "days_on_market": "MEDDAYONMAR06",
        "median_income": "MEHOINUSCAA672N",
        "new_listings": "NEWLISCOU06",
    },
    "NV": {
        "median_list_price": "MEDLISPRI32",
        "median_sale_price": "MEDSFHPNV",
        "active_listings": "ACTLISCOU32",
        "days_on_market": "MEDDAYONMAR32",
        "median_income": "MEHOINUSNVA672N",
        "new_listings": "NEWLISCOU32",
    },
    "RI": {
        "median_list_price": "MEDLISPRI44",
        "median_sale_price": "MEDSFHPRI",
        "active_listings": "ACTLISCOU44",
        "days_on_market": "MEDDAYONMAR44",
        "median_income": "MEHOINUSRIA672N",
        "new_listings": "NEWLISCOU44",
    },
    "AZ": {
        "median_list_price": "MEDLISPRI04",
        "median_sale_price": "MEDSFHPAZ",
        "active_listings": "ACTLISCOU04",
        "days_on_market": "MEDDAYONMAR04",
        "median_income": "MEHOINUSAZA672N",
        "new_listings": "NEWLISCOU04",
    },
}

# S&P 500 tickers for screening — we'll fetch a broad set
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


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


def get_fred_data(api_key: str | None) -> dict:
    """Fetch real estate data from FRED for all 4 states."""
    cached = _read_cache("fred_all", max_age_hours=12)
    if cached:
        return cached

    if not api_key:
        return {"error": "FRED_API_KEY not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"}

    from fredapi import Fred
    fred = Fred(api_key=api_key)

    result = {"states": {}, "mortgage_rate": {}}

    # Mortgage rate (national)
    try:
        s = fred.get_series("MORTGAGE30US", observation_start="2015-01-01")
        s = s.dropna()
        result["mortgage_rate"] = {
            "dates": [d.strftime("%Y-%m-%d") for d in s.index],
            "values": [round(float(v), 2) for v in s.values],
            "current": round(float(s.iloc[-1]), 2),
        }
    except Exception as e:
        logger.warning(f"Failed to fetch mortgage rate: {e}")

    for state_code, series_map in FRED_SERIES.items():
        if state_code == "mortgage_30yr":
            continue
        state_data = {"code": state_code, "name": STATES[state_code]["name"]}

        for metric_name, series_id in series_map.items():
            try:
                s = fred.get_series(series_id, observation_start="2015-01-01")
                s = s.dropna()
                if len(s) == 0:
                    continue
                state_data[metric_name] = {
                    "dates": [d.strftime("%Y-%m-%d") for d in s.index],
                    "values": [round(float(v), 2) for v in s.values],
                    "current": round(float(s.iloc[-1]), 2),
                }
                # Calculate YoY change
                if len(s) >= 12:
                    current_val = float(s.iloc[-1])
                    year_ago = float(s.iloc[-12]) if "month" in str(s.index.freq) else float(s.iloc[-4])
                    if year_ago != 0:
                        state_data[metric_name]["yoy_change"] = round((current_val - year_ago) / year_ago * 100, 1)
            except Exception as e:
                logger.warning(f"Failed to fetch {series_id} for {state_code}: {e}")

        # Compute buy signals
        signals = compute_buy_signals(state_data)
        state_data["signals"] = signals
        result["states"][state_code] = state_data

    _write_cache("fred_all", result)
    return result


def compute_buy_signals(state_data: dict) -> dict:
    """Compute buy/sell/hold signals based on market indicators."""
    signals = {"factors": [], "score": 0, "rating": "NEUTRAL"}
    max_score = 0

    # 1. Price trend: Are prices declining from peak?
    if "median_list_price" in state_data:
        prices = state_data["median_list_price"]["values"]
        if len(prices) >= 6:
            max_score += 2
            recent = prices[-1]
            peak = max(prices[-24:]) if len(prices) >= 24 else max(prices)
            pct_from_peak = (recent - peak) / peak * 100
            if pct_from_peak < -10:
                signals["factors"].append({"name": "Price below peak", "value": f"{pct_from_peak:.1f}%", "signal": "BUY", "points": 2})
                signals["score"] += 2
            elif pct_from_peak < -5:
                signals["factors"].append({"name": "Price cooling from peak", "value": f"{pct_from_peak:.1f}%", "signal": "LEAN BUY", "points": 1})
                signals["score"] += 1
            else:
                signals["factors"].append({"name": "Price near/at peak", "value": f"{pct_from_peak:.1f}%", "signal": "WAIT", "points": 0})

    # 2. Days on market increasing = buyer leverage
    if "days_on_market" in state_data:
        dom = state_data["days_on_market"]["values"]
        if len(dom) >= 6:
            max_score += 2
            recent_dom = dom[-1]
            avg_dom = sum(dom[-12:]) / min(len(dom), 12)
            if recent_dom > avg_dom * 1.2:
                signals["factors"].append({"name": "Days on market rising", "value": f"{recent_dom:.0f} days", "signal": "BUY", "points": 2})
                signals["score"] += 2
            elif recent_dom > avg_dom:
                signals["factors"].append({"name": "Days on market above avg", "value": f"{recent_dom:.0f} days", "signal": "LEAN BUY", "points": 1})
                signals["score"] += 1
            else:
                signals["factors"].append({"name": "Days on market low", "value": f"{recent_dom:.0f} days", "signal": "SELLER'S MARKET", "points": 0})

    # 3. Inventory levels
    if "active_listings" in state_data:
        listings = state_data["active_listings"]["values"]
        if len(listings) >= 12:
            max_score += 2
            recent_inv = listings[-1]
            avg_inv = sum(listings[-24:]) / min(len(listings), 24)
            if recent_inv > avg_inv * 1.2:
                signals["factors"].append({"name": "Inventory above average", "value": f"{recent_inv:,.0f}", "signal": "BUY", "points": 2})
                signals["score"] += 2
            elif recent_inv > avg_inv:
                signals["factors"].append({"name": "Inventory normalizing", "value": f"{recent_inv:,.0f}", "signal": "LEAN BUY", "points": 1})
                signals["score"] += 1
            else:
                signals["factors"].append({"name": "Inventory tight", "value": f"{recent_inv:,.0f}", "signal": "WAIT", "points": 0})

    # 4. Mortgage rate trend
    # (passed separately, handled in the view)

    # 5. Affordability: price to income ratio
    if "median_sale_price" in state_data and "median_income" in state_data:
        max_score += 2
        price = state_data["median_sale_price"]["current"]
        income = state_data["median_income"]["current"]
        if income > 0:
            ratio = price / income
            # Historical norm ~3-4x, stretched markets 5-7x+
            if ratio < 4.5:
                signals["factors"].append({"name": "Price-to-income ratio", "value": f"{ratio:.1f}x", "signal": "BUY", "points": 2})
                signals["score"] += 2
            elif ratio < 6:
                signals["factors"].append({"name": "Price-to-income ratio", "value": f"{ratio:.1f}x", "signal": "NEUTRAL", "points": 1})
                signals["score"] += 1
            else:
                signals["factors"].append({"name": "Price-to-income stretched", "value": f"{ratio:.1f}x", "signal": "EXPENSIVE", "points": 0})

    # Final rating
    if max_score > 0:
        pct = signals["score"] / max_score
        if pct >= 0.7:
            signals["rating"] = "BUY"
        elif pct >= 0.5:
            signals["rating"] = "LEAN BUY"
        elif pct >= 0.3:
            signals["rating"] = "NEUTRAL"
        else:
            signals["rating"] = "WAIT"
    signals["max_score"] = max_score

    return signals


def get_stock_screener_data() -> list[dict]:
    """Fetch stock data and filter for net profit margin > 20%."""
    cached = _read_cache("screener", max_age_hours=6)
    if cached:
        return cached

    # Get a broad list of tickers — S&P 500 + some extras
    try:
        tables = pd.read_html(SP500_URL)
        sp500 = tables[0]
        tickers = sp500["Symbol"].str.replace(".", "-", regex=False).tolist()
        sectors = dict(zip(sp500["Symbol"].str.replace(".", "-", regex=False), sp500["GICS Sector"]))
        sub_industries = dict(zip(sp500["Symbol"].str.replace(".", "-", regex=False), sp500["GICS Sub-Industry"]))
    except Exception:
        # Fallback to a hardcoded list of well-known tickers
        tickers = ["AAPL","MSFT","GOOGL","META","NVDA","AMZN","TSLA","V","MA","AVGO",
                    "JPM","UNH","JNJ","PG","HD","ABBV","MRK","PEP","KO","COST",
                    "ADBE","CRM","NFLX","AMD","INTC","QCOM","TXN","NOW","AMAT","LRCX"]
        sectors = {}
        sub_industries = {}

    results = []
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            data = yf.download(batch, period="1y", group_by="ticker", progress=False, threads=True)
        except Exception:
            continue

        for ticker in batch:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                net_margin = info.get("profitMargins")
                if net_margin is None or net_margin < 0.20:
                    continue

                revenue_growth = info.get("revenueGrowth")
                market_cap = info.get("marketCap", 0)
                current_price = info.get("currentPrice") or info.get("regularMarketPrice", 0)
                fifty_two_high = info.get("fiftyTwoWeekHigh", 0)
                fifty_two_low = info.get("fiftyTwoWeekLow", 0)
                two_hundred_dma = info.get("twoHundredDayAverage", 0)
                name = info.get("shortName", ticker)

                # Distance from 200 DMA
                dma_distance = 0
                if two_hundred_dma and current_price:
                    dma_distance = (current_price - two_hundred_dma) / two_hundred_dma * 100

                # Distance from 52w high
                high_distance = 0
                if fifty_two_high and current_price:
                    high_distance = (current_price - fifty_two_high) / fifty_two_high * 100

                results.append({
                    "ticker": ticker,
                    "name": name,
                    "sector": sectors.get(ticker, info.get("sector", "N/A")),
                    "industry": sub_industries.get(ticker, info.get("industry", "N/A")),
                    "price": round(current_price, 2) if current_price else 0,
                    "market_cap": market_cap,
                    "market_cap_fmt": _fmt_market_cap(market_cap),
                    "net_margin": round(net_margin * 100, 1),
                    "revenue_growth": round(revenue_growth * 100, 1) if revenue_growth else None,
                    "two_hundred_dma": round(two_hundred_dma, 2) if two_hundred_dma else None,
                    "dma_distance": round(dma_distance, 1),
                    "fifty_two_high": round(fifty_two_high, 2) if fifty_two_high else None,
                    "high_distance": round(high_distance, 1),
                    "fifty_two_low": round(fifty_two_low, 2) if fifty_two_low else None,
                })
            except Exception as e:
                logger.debug(f"Skipping {ticker}: {e}")
                continue

    results.sort(key=lambda x: x["net_margin"], reverse=True)
    _write_cache("screener", results)
    return results


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
