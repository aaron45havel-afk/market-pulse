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

    # Step 4: Pre-filter HARD before fetching prices
    # Only look up prices for companies that could plausibly be net-nets
    positive_ncav = [c for c in candidates
                     if c["ncav_per_share"] > 0.50       # NCAV/share > $0.50 (skip penny stocks)
                     and c["shares_outstanding"] > 100000  # At least 100K shares (real company)
                     and c.get("current_ratio") and c["current_ratio"] >= 0.5  # Somewhat solvent
                     ]

    # Sort by NCAV descending (biggest balance sheets first — more likely to be real companies)
    positive_ncav.sort(key=lambda x: x["ncav"], reverse=True)

    # Limit to top 300 candidates to avoid hammering Yahoo
    positive_ncav = positive_ncav[:300]
    logger.info(f"SEC: {len(positive_ncav)} candidates selected for price lookup")

    # Fetch prices via yfinance — SMALL batches, sequential, with delays
    try:
        import yfinance as yf
        tickers = [c["ticker"] for c in positive_ncav]

        price_map = {}
        batch_size = 20  # Small batches to avoid connection pool overflow
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            try:
                data = yf.download(batch, period="5d", progress=False, threads=False)
                if data is not None and not data.empty and "Close" in data.columns:
                    for ticker in batch:
                        try:
                            if len(batch) == 1:
                                close_series = data["Close"].dropna()
                            else:
                                close_series = data["Close"][ticker].dropna()
                            if len(close_series) > 0:
                                price = float(close_series.iloc[-1])
                                if price > 0:
                                    price_map[ticker] = price
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"yfinance batch {i//batch_size} error: {e}")

            # Rate limit: pause between batches
            if i + batch_size < len(tickers):
                time.sleep(0.5)

            # Log progress
            if (i // batch_size) % 5 == 0:
                logger.info(f"SEC: Price fetch progress: {len(price_map)} prices from {i + len(batch)} tickers")

        logger.info(f"SEC: Got prices for {len(price_map)} out of {len(tickers)} tickers")

        # Enrich with data
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


# ═══════════════════════════════════════════════════
# PORTFOLIO BUILDER — $50K, 25 equal-weight positions
# ═══════════════════════════════════════════════════

EXCLUDED_SECTORS = {"Financial Services", "Financials", "Real Estate", "Banks", "Insurance",
                    "Banking", "REIT", "REITs", "Savings Institutions", "Finance"}

# SIC codes for financials/REITs (used when sector info not available)
FINANCIAL_SIC_RANGES = [
    (6000, 6999),  # Finance, Insurance, Real Estate
]


def build_portfolio(capital: float = 50000, num_positions: int = 25) -> dict:
    """
    Build a net-net portfolio with position sizing.

    Filters:
    - P/NCAV < 1.0 (true net-nets), expand to 1.5 if not enough
    - Market cap > $5M
    - Current ratio > 1.0
    - Exclude financials/REITs
    - Positive NCAV only

    Returns portfolio dict with positions and summary stats.
    """
    cached = _read_cache("portfolio", max_age_hours=12)
    if cached:
        return cached

    # Get the full screener data
    all_stocks = build_net_net_screener()
    if not all_stocks or (len(all_stocks) == 1 and isinstance(all_stocks[0], dict) and "error" in all_stocks[0]):
        return {"error": "No screener data available", "positions": []}

    # Enrich top candidates with sector/volume data from yfinance
    # Only fetch for stocks that pass initial filters
    initial_candidates = [s for s in all_stocks
                          if s.get("ncav_ratio", 999) <= 2.0
                          and s.get("market_cap", 0) >= 5_000_000
                          and s.get("current_ratio") and s.get("current_ratio") >= 1.0]

    logger.info(f"Portfolio: {len(initial_candidates)} candidates pass initial filters, enriching top 100...")

    # Enrich with sector/volume data — LIMIT to top 40, with rate limiting
    try:
        import yfinance as yf
        enrich_count = min(40, len(initial_candidates))
        for idx, candidate in enumerate(initial_candidates[:enrich_count]):
            try:
                stock = yf.Ticker(candidate["ticker"])
                info = stock.info
                candidate["sector"] = info.get("sector", "N/A")
                candidate["industry"] = info.get("industry", "N/A")
                candidate["avg_volume"] = info.get("averageVolume", 0)
                candidate["avg_dollar_volume"] = (info.get("averageVolume", 0) or 0) * candidate.get("price", 0)
                candidate["two_hundred_dma"] = info.get("twoHundredDayAverage")
                candidate["fifty_two_high"] = info.get("fiftyTwoWeekHigh")
                candidate["fifty_two_low"] = info.get("fiftyTwoWeekLow")
                candidate["exchange"] = info.get("exchange", "N/A")
            except Exception:
                candidate["sector"] = "N/A"
                candidate["industry"] = "N/A"
                candidate["avg_volume"] = 0
                candidate["avg_dollar_volume"] = 0
            # Rate limit
            if idx % 5 == 4:
                time.sleep(1)
        logger.info(f"Portfolio: Enriched {enrich_count} candidates with sector/volume data")
    except ImportError:
        pass

    # Apply portfolio filters
    filtered = []
    for s in initial_candidates[:100]:
        # Skip financials/REITs
        sector = s.get("sector", "N/A")
        if sector in EXCLUDED_SECTORS:
            continue

        # Min market cap $5M
        if s.get("market_cap", 0) < 5_000_000:
            continue

        # Current ratio > 1.0
        if not s.get("current_ratio") or s["current_ratio"] < 1.0:
            continue

        # Min daily dollar volume $10K (tradeable)
        avg_dvol = s.get("avg_dollar_volume", 0)
        if avg_dvol and avg_dvol < 10_000:
            continue

        # Price must be > $0.50 (avoid sub-penny stocks)
        if s.get("price", 0) < 0.50:
            continue

        filtered.append(s)

    # Select positions — prefer true net-nets (P/NCAV < 1.0), then expand
    net_nets = [s for s in filtered if s.get("ncav_ratio", 999) <= 1.0]
    deep_value = [s for s in filtered if 1.0 < s.get("ncav_ratio", 999) <= 1.5]

    # Build portfolio: fill with net-nets first, then deep value
    portfolio_stocks = net_nets[:num_positions]
    if len(portfolio_stocks) < num_positions:
        remaining = num_positions - len(portfolio_stocks)
        portfolio_stocks.extend(deep_value[:remaining])

    # Calculate position sizes
    position_size = round(capital / num_positions, 2) if num_positions > 0 else 0
    positions = []
    for i, s in enumerate(portfolio_stocks):
        price = s.get("price", 0)
        shares_to_buy = int(position_size / price) if price > 0 else 0
        actual_cost = round(shares_to_buy * price, 2)

        # DMA distance
        dma_dist = None
        if s.get("two_hundred_dma") and price:
            dma_dist = round((price - s["two_hundred_dma"]) / s["two_hundred_dma"] * 100, 1)

        # 52w high distance
        high_dist = None
        if s.get("fifty_two_high") and price:
            high_dist = round((price - s["fifty_two_high"]) / s["fifty_two_high"] * 100, 1)

        positions.append({
            "rank": i + 1,
            "ticker": s["ticker"],
            "name": s["name"],
            "sector": s.get("sector", "N/A"),
            "industry": s.get("industry", "N/A"),
            "cap_category": s.get("cap_category", "N/A"),
            "price": price,
            "ncav_per_share": s["ncav_per_share"],
            "ncav_ratio": s.get("ncav_ratio", 0),
            "is_net_net": s.get("ncav_ratio", 999) <= 1.0,
            "market_cap": s.get("market_cap", 0),
            "market_cap_fmt": s.get("market_cap_fmt", "N/A"),
            "pe_ratio": s.get("pe_ratio"),
            "pb_ratio": s.get("pb_ratio"),
            "net_margin": s.get("net_margin"),
            "current_ratio": s.get("current_ratio"),
            "target_allocation": position_size,
            "shares_to_buy": shares_to_buy,
            "actual_cost": actual_cost,
            "weight_pct": round(actual_cost / capital * 100, 1) if capital > 0 else 0,
            "avg_volume": s.get("avg_volume", 0),
            "dma_distance": dma_dist,
            "high_distance": high_dist,
        })

    # Portfolio summary
    total_invested = sum(p["actual_cost"] for p in positions)
    cash_remaining = round(capital - total_invested, 2)
    avg_ncav_ratio = round(sum(p["ncav_ratio"] for p in positions) / len(positions), 2) if positions else 0
    num_true_net_nets = sum(1 for p in positions if p["is_net_net"])
    profitable_count = sum(1 for p in positions if p.get("net_margin") and p["net_margin"] > 0)

    result = {
        "portfolio": {
            "capital": capital,
            "num_positions": len(positions),
            "target_positions": num_positions,
            "position_size": position_size,
            "total_invested": total_invested,
            "cash_remaining": cash_remaining,
            "avg_ncav_ratio": avg_ncav_ratio,
            "num_net_nets": num_true_net_nets,
            "num_profitable": profitable_count,
            "total_candidates_screened": len(all_stocks),
            "passed_filters": len(filtered),
        },
        "positions": positions,
        "filters_applied": {
            "max_ncav_ratio": 1.5,
            "min_market_cap": "$5M",
            "min_current_ratio": 1.0,
            "min_price": "$0.50",
            "min_daily_dollar_volume": "$10K",
            "excluded_sectors": "Financials, REITs, Banks, Insurance",
            "exchanges": "NYSE, NASDAQ, AMEX",
        },
    }

    _write_cache("portfolio", result)
    return result


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
