"""
SEC EDGAR XBRL Frames API — Net-Net & Deep Value Stock Screener.

Uses the SEC's free XBRL Frames API to get balance sheet data for ALL public companies
in ~6 API calls. No API key needed. Covers every market cap including micro-caps.

Key endpoints:
- https://www.sec.gov/files/company_tickers.json (CIK → ticker mapping)
- https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/USD/{period}.json (bulk financial data)

NCAV = (Current Assets - Total Liabilities) / Shares Outstanding
Net-Net = Stock trading below its NCAV per share
"""
import json, logging, time, urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

SEC_USER_AGENT = "MarketPulse/1.0 invoice@archfms.com"
CACHE_DIR = Path("/tmp/market_pulse_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Balance sheet items (instantaneous — use "I" suffix)
BALANCE_SHEET_CONCEPTS = {
    "current_assets": "AssetsCurrent",
    "total_liabilities": "Liabilities",
    "total_assets": "Assets",
    "stockholders_equity": "StockholdersEquity",
    "cash": "CashAndCashEquivalentsAtCarryingValue",
    "shares_outstanding": "CommonStockSharesOutstanding",
    "preferred_stock": "PreferredStockValue",
}

# Income statement items (duration — no "I" suffix)
INCOME_CONCEPTS = {
    "net_income": "NetIncomeLoss",
    "revenue": "Revenues",
    "gross_profit": "GrossProfit",
}


def _sec_get(url: str) -> dict | list | None:
    """Make a GET request to SEC EDGAR with required User-Agent."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": SEC_USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.warning(f"SEC EDGAR error for {url}: {e}")
        return None


def _cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"


def _read_cache(key: str, max_age_hours: int = 24):
    p = _cache_path(key)
    if p.exists():
        age = time.time() - p.stat().st_mtime
        if age < max_age_hours * 3600:
            return json.loads(p.read_text())
    return None


def _write_cache(key: str, data):
    _cache_path(key).write_text(json.dumps(data, default=str))


def get_ticker_cik_map() -> dict:
    """Fetch CIK → ticker mapping from SEC. Returns {cik_int: {ticker, name}}."""
    cached = _read_cache("sec_tickers", max_age_hours=168)  # cache for 1 week
    if cached:
        return cached

    data = _sec_get("https://www.sec.gov/files/company_tickers.json")
    if not data:
        return {}

    # Build cik → {ticker, name} map
    result = {}
    for entry in data.values():
        cik = entry.get("cik_str")
        ticker = entry.get("ticker", "")
        name = entry.get("title", "")
        if cik and ticker:
            result[str(cik)] = {"ticker": ticker, "name": name}

    _write_cache("sec_tickers", result)
    logger.info(f"SEC: Loaded {len(result)} ticker mappings")
    return result


def _fetch_xbrl_frame(concept: str, period: str, unit: str = "USD") -> dict:
    """
    Fetch XBRL frame data for a concept across ALL companies.
    Returns {cik_str: value} dict.
    """
    url = f"https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/{unit}/{period}.json"
    data = _sec_get(url)
    if not data or "data" not in data:
        logger.warning(f"SEC: No data for {concept}/{period}")
        return {}

    result = {}
    for entry in data["data"]:
        cik = str(entry.get("cik", ""))
        val = entry.get("val")
        if cik and val is not None:
            # Keep the most recent filing if duplicates
            if cik not in result or entry.get("end", "") > result.get(cik, {}).get("end", ""):
                result[cik] = {"val": val, "end": entry.get("end", ""), "entity": entry.get("entityName", "")}

    logger.info(f"SEC: {concept}/{period} returned {len(result)} companies")
    return result


def _determine_latest_period():
    """Determine the most recent quarterly period string for XBRL frames."""
    from datetime import datetime
    now = datetime.now()
    year = now.year
    month = now.month

    # XBRL frames lag by ~3-4 months. Use the quarter before the current one.
    # Q1 filings available ~May-June, Q2 ~Aug-Sep, Q3 ~Nov-Dec, Q4 ~Feb-Mar
    if month <= 3:
        # We're in Q1 — Q3 of previous year should be available
        return f"CY{year-1}Q3", f"CY{year-1}Q3"
    elif month <= 6:
        # We're in Q2 — Q4 of previous year should be available
        return f"CY{year-1}Q4", f"CY{year-1}Q4"
    elif month <= 9:
        # We're in Q3 — Q1 of current year should be available
        return f"CY{year}Q1", f"CY{year}Q1"
    else:
        # We're in Q4 — Q2 of current year should be available
        return f"CY{year}Q2", f"CY{year}Q2"


def fetch_all_financials() -> dict:
    """
    Fetch balance sheet + income statement data for ALL public companies.
    Returns {cik: {current_assets, total_liabilities, net_income, ...}}
    """
    cached = _read_cache("sec_financials", max_age_hours=24)
    if cached:
        return cached

    bs_period, is_period = _determine_latest_period()
    bs_period_i = bs_period + "I"  # instantaneous for balance sheet

    all_data = {}  # {cik: {metric: value}}

    # Fetch balance sheet items (instantaneous) in parallel
    def fetch_bs(name, concept):
        return name, _fetch_xbrl_frame(concept, bs_period_i)

    def fetch_is(name, concept):
        return name, _fetch_xbrl_frame(concept, bs_period)

    # Also try "shares" in a different unit
    def fetch_shares():
        # Try USD first, then "shares" unit
        result = _fetch_xbrl_frame("CommonStockSharesOutstanding", bs_period_i, "shares")
        if not result:
            result = _fetch_xbrl_frame("EntityCommonStockSharesOutstanding", bs_period_i, "shares")
        return "shares_outstanding", result

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        # Balance sheet
        for name, concept in BALANCE_SHEET_CONCEPTS.items():
            if name == "shares_outstanding":
                futures.append(executor.submit(fetch_shares))
            elif name == "preferred_stock":
                futures.append(executor.submit(fetch_bs, name, concept))
            else:
                futures.append(executor.submit(fetch_bs, name, concept))
        # Income statement
        for name, concept in INCOME_CONCEPTS.items():
            futures.append(executor.submit(fetch_is, name, concept))

        for future in as_completed(futures):
            try:
                metric_name, data = future.result()
                for cik, entry in data.items():
                    if cik not in all_data:
                        all_data[cik] = {}
                    all_data[cik][metric_name] = entry["val"]
                    all_data[cik]["_entity"] = entry.get("entity", "")
            except Exception as e:
                logger.warning(f"SEC fetch error: {e}")

    _write_cache("sec_financials", all_data)
    logger.info(f"SEC: Combined financial data for {len(all_data)} companies")
    return all_data


def build_net_net_screener() -> list[dict]:
    """
    Build the complete net-net / deep value screener.
    1. Fetch balance sheet data from SEC EDGAR (all companies)
    2. Map CIK to tickers
    3. Calculate NCAV, margins, ratios
    4. Fetch live prices via yfinance for candidates
    5. Return sorted results
    """
    cached = _read_cache("net_net_screener", max_age_hours=12)
    if cached:
        return cached

    # Step 1: Get financial data
    financials = fetch_all_financials()
    if not financials:
        return [{"error": "Failed to fetch SEC EDGAR data"}]

    # Step 2: Get ticker mapping
    ticker_map = get_ticker_cik_map()
    if not ticker_map:
        return [{"error": "Failed to fetch SEC ticker data"}]

    # Step 3: Calculate NCAV and filter
    candidates = []
    for cik, data in financials.items():
        ticker_info = ticker_map.get(cik)
        if not ticker_info:
            continue

        current_assets = data.get("current_assets")
        total_liabilities = data.get("total_liabilities")
        shares = data.get("shares_outstanding")
        total_assets = data.get("total_assets")
        equity = data.get("stockholders_equity")
        cash = data.get("cash", 0) or 0
        net_income = data.get("net_income")
        revenue = data.get("revenue")
        preferred = data.get("preferred_stock", 0) or 0

        # Must have current assets, liabilities, and shares to calculate NCAV
        if not current_assets or not total_liabilities or not shares or shares <= 0:
            continue

        ncav = current_assets - total_liabilities - preferred
        ncav_per_share = ncav / shares

        # Calculate additional metrics
        book_value = equity if equity else (total_assets - total_liabilities if total_assets else None)
        book_per_share = book_value / shares if book_value and shares else None

        net_margin = None
        if net_income is not None and revenue and revenue > 0:
            net_margin = round(net_income / revenue * 100, 1)

        current_ratio = None
        if current_assets and total_liabilities and total_liabilities > 0:
            current_ratio = round(current_assets / total_liabilities, 2)

        candidates.append({
            "cik": cik,
            "ticker": ticker_info["ticker"],
            "name": ticker_info["name"],
            "ncav": ncav,
            "ncav_per_share": round(ncav_per_share, 2),
            "current_assets": current_assets,
            "total_liabilities": total_liabilities,
            "total_assets": total_assets,
            "cash": cash,
            "shares_outstanding": shares,
            "book_value": book_value,
            "book_per_share": round(book_per_share, 2) if book_per_share else None,
            "net_income": net_income,
            "revenue": revenue,
            "net_margin": net_margin,
            "current_ratio": current_ratio,
            "preferred_stock": preferred,
        })

    logger.info(f"SEC: {len(candidates)} companies with calculable NCAV")

    # Step 4: Get live prices for ALL candidates with positive NCAV
    # (negative NCAV = liabilities exceed current assets, not interesting)
    positive_ncav = [c for c in candidates if c["ncav_per_share"] > 0]
    logger.info(f"SEC: {len(positive_ncav)} companies with positive NCAV")

    # Fetch prices via yfinance in batches
    try:
        import yfinance as yf
        tickers = [c["ticker"] for c in positive_ncav]

        # Batch download prices
        price_map = {}
        batch_size = 100
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            try:
                data = yf.download(batch, period="1d", progress=False, threads=True)
                if "Close" in data.columns:
                    for ticker in batch:
                        try:
                            if len(batch) == 1:
                                price = float(data["Close"].iloc[-1])
                            else:
                                price = float(data["Close"][ticker].iloc[-1])
                            if price > 0:
                                price_map[ticker] = price
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"yfinance batch error: {e}")

            # Also get extra data (market cap, 200 DMA, sector) from .info for top candidates
            # But only for net-nets (price < NCAV) to save API calls

        # Enrich with yfinance Ticker info for the best candidates
        enriched = []
        for c in positive_ncav:
            price = price_map.get(c["ticker"])
            if not price:
                continue

            ncav_ratio = round(price / c["ncav_per_share"], 2) if c["ncav_per_share"] > 0 else 999

            # Calculate market cap
            market_cap = price * c["shares_outstanding"]

            # P/E ratio
            pe_ratio = None
            if c["net_income"] and c["net_income"] > 0 and c["shares_outstanding"] > 0:
                eps = c["net_income"] / c["shares_outstanding"]
                if eps > 0:
                    pe_ratio = round(price / eps, 1)

            # P/B ratio
            pb_ratio = None
            if c["book_per_share"] and c["book_per_share"] > 0:
                pb_ratio = round(price / c["book_per_share"], 2)

            # Cap category
            if market_cap >= 200e9:
                cap_cat = "Mega"
            elif market_cap >= 10e9:
                cap_cat = "Large"
            elif market_cap >= 2e9:
                cap_cat = "Mid"
            elif market_cap >= 300e6:
                cap_cat = "Small"
            else:
                cap_cat = "Micro"

            enriched.append({
                "ticker": c["ticker"],
                "name": c["name"],
                "price": round(price, 2),
                "ncav_per_share": c["ncav_per_share"],
                "ncav_ratio": ncav_ratio,
                "is_net_net": ncav_ratio <= 1.0,
                "market_cap": int(market_cap),
                "market_cap_fmt": _fmt_cap(market_cap),
                "cap_category": cap_cat,
                "current_assets": c["current_assets"],
                "total_liabilities": c["total_liabilities"],
                "cash": c["cash"],
                "book_per_share": c["book_per_share"],
                "pb_ratio": pb_ratio,
                "pe_ratio": pe_ratio,
                "net_margin": c["net_margin"],
                "current_ratio": c["current_ratio"],
                "revenue": c["revenue"],
                "net_income": c["net_income"],
            })

    except ImportError:
        logger.error("yfinance not installed — cannot fetch live prices")
        enriched = []

    # Sort by NCAV ratio (lowest = cheapest relative to liquidation value)
    enriched.sort(key=lambda x: x["ncav_ratio"])

    _write_cache("net_net_screener", enriched)
    logger.info(f"Net-net screener: {len(enriched)} stocks with prices, {sum(1 for e in enriched if e['is_net_net'])} net-nets")
    return enriched


def get_net_net_enriched(ticker: str) -> dict | None:
    """Get detailed info for a single stock from yfinance."""
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.info
        return {
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "two_hundred_dma": info.get("twoHundredDayAverage"),
            "fifty_day_ma": info.get("fiftyDayAverage"),
            "fifty_two_high": info.get("fiftyTwoWeekHigh"),
            "fifty_two_low": info.get("fiftyTwoWeekLow"),
            "avg_volume": info.get("averageVolume"),
            "description": info.get("longBusinessSummary", "")[:300],
        }
    except Exception:
        return None


def _fmt_cap(val) -> str:
    if not val:
        return "N/A"
    if val >= 1e12:
        return f"${val/1e12:.1f}T"
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    if val >= 1e6:
        return f"${val/1e6:.0f}M"
    return f"${val:,.0f}"
