"""
SEC EDGAR XBRL Frames API — Net-Net & Deep Value Stock Screener.

Uses the SEC's free XBRL Frames API to get balance sheet data for ALL public companies.
No API key needed. Covers every market cap including micro-caps.
User looks up live prices themselves on Yahoo Finance.

NCAV = (Current Assets - Total Liabilities - Preferred Stock) / Shares Outstanding
"""
import json, logging, time, urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

SEC_USER_AGENT = "MarketPulse/1.0 invoice@archfms.com"
CACHE_DIR = Path("/tmp/market_pulse_cache")
CACHE_DIR.mkdir(exist_ok=True)

BALANCE_SHEET_CONCEPTS = {
    "current_assets": "AssetsCurrent",
    "total_liabilities": "Liabilities",
    "total_assets": "Assets",
    "stockholders_equity": "StockholdersEquity",
    "cash": "CashAndCashEquivalentsAtCarryingValue",
    "preferred_stock": "PreferredStockValue",
}

INCOME_CONCEPTS = {
    "net_income": "NetIncomeLoss",
    "revenue": "Revenues",
    "gross_profit": "GrossProfit",
}


def _sec_get(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": SEC_USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.warning(f"SEC EDGAR error: {url}: {e}")
        return None


def _cache_path(key):
    return CACHE_DIR / f"{key}.json"

def _read_cache(key, max_age_hours=24):
    p = _cache_path(key)
    if p.exists():
        if time.time() - p.stat().st_mtime < max_age_hours * 3600:
            return json.loads(p.read_text())
    return None

def _write_cache(key, data):
    _cache_path(key).write_text(json.dumps(data, default=str))


def get_ticker_cik_map():
    cached = _read_cache("sec_tickers", max_age_hours=168)
    if cached:
        return cached
    data = _sec_get("https://www.sec.gov/files/company_tickers.json")
    if not data:
        return {}
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


def _fetch_xbrl_frame(concept, period, unit="USD"):
    url = f"https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/{unit}/{period}.json"
    data = _sec_get(url)
    if not data or "data" not in data:
        return {}
    result = {}
    for entry in data["data"]:
        cik = str(entry.get("cik", ""))
        val = entry.get("val")
        if cik and val is not None:
            if cik not in result or entry.get("end", "") > result.get(cik, {}).get("end", ""):
                result[cik] = {"val": val, "end": entry.get("end", ""), "entity": entry.get("entityName", "")}
    logger.info(f"SEC: {concept}/{period} returned {len(result)} companies")
    return result


def _determine_latest_period():
    from datetime import datetime
    now = datetime.now()
    y, m = now.year, now.month
    if m <= 3:
        return f"CY{y-1}Q3", f"CY{y-1}Q3"
    elif m <= 6:
        return f"CY{y-1}Q4", f"CY{y-1}Q4"
    elif m <= 9:
        return f"CY{y}Q1", f"CY{y}Q1"
    else:
        return f"CY{y}Q2", f"CY{y}Q2"


def fetch_all_financials():
    cached = _read_cache("sec_financials", max_age_hours=24)
    if cached:
        return cached

    bs_period, is_period = _determine_latest_period()
    bs_period_i = bs_period + "I"
    all_data = {}

    def fetch_bs(name, concept):
        return name, _fetch_xbrl_frame(concept, bs_period_i)

    def fetch_is(name, concept):
        return name, _fetch_xbrl_frame(concept, is_period)

    def fetch_shares():
        result = _fetch_xbrl_frame("CommonStockSharesOutstanding", bs_period_i, "shares")
        if not result:
            result = _fetch_xbrl_frame("EntityCommonStockSharesOutstanding", bs_period_i, "shares")
        return "shares_outstanding", result

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for name, concept in BALANCE_SHEET_CONCEPTS.items():
            futures.append(executor.submit(fetch_bs, name, concept))
        for name, concept in INCOME_CONCEPTS.items():
            futures.append(executor.submit(fetch_is, name, concept))
        futures.append(executor.submit(fetch_shares))

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


def _sanity_check_shares(current_assets, total_assets, total_liabilities, equity, shares):
    """
    Detect and fix shares outstanding unit mismatches.

    The problem: SEC EDGAR XBRL sometimes reports shares in different units
    (actual count, thousands, millions) while balance sheet items are always in USD.

    Fix: Use book value per share as a reasonableness check.
    If book/share > $10,000, shares are likely reported in thousands.
    If book/share > $10,000,000, shares are likely in millions.

    Also cross-validate: for a normal company, total assets / shares should give
    a reasonable "assets per share" number (typically $1 - $10,000).
    """
    if not shares or shares <= 0:
        return None

    # Calculate book value for sanity check
    book = equity if equity else (total_assets - total_liabilities if total_assets and total_liabilities else None)
    ref_value = total_assets or current_assets  # Use total assets as reference

    if not ref_value or ref_value <= 0:
        return shares

    # Assets per share — for normal companies this should be roughly $1-$50,000
    assets_per_share = ref_value / shares

    # If assets per share is absurdly high, shares are probably in thousands or millions
    if assets_per_share > 1_000_000:
        # Shares likely in millions — multiply by 1,000,000
        corrected = shares * 1_000_000
        logger.debug(f"Shares correction: {shares} -> {corrected} (assets/share was ${assets_per_share:,.0f})")
        return corrected
    elif assets_per_share > 10_000:
        # Shares likely in thousands — multiply by 1,000
        corrected = shares * 1_000
        logger.debug(f"Shares correction: {shares} -> {corrected} (assets/share was ${assets_per_share:,.0f})")
        return corrected
    elif assets_per_share < 0.01:
        # Shares are way too high — probably reported in units but balance sheet in thousands
        # This means balance sheet values might be in thousands
        # Skip these — too ambiguous
        return None

    return shares


def build_net_net_screener():
    """Build screener from SEC EDGAR data only. No live prices."""
    cached = _read_cache("net_net_screener", max_age_hours=12)
    if cached:
        return cached

    financials = fetch_all_financials()
    if not financials:
        return [{"error": "Failed to fetch SEC EDGAR data"}]

    ticker_map = get_ticker_cik_map()
    if not ticker_map:
        return [{"error": "Failed to fetch SEC ticker data"}]

    results = []
    skipped_sanity = 0

    for cik, data in financials.items():
        ticker_info = ticker_map.get(cik)
        if not ticker_info:
            continue

        current_assets = data.get("current_assets")
        total_liabilities = data.get("total_liabilities")
        raw_shares = data.get("shares_outstanding")
        total_assets = data.get("total_assets")
        equity = data.get("stockholders_equity")
        cash = data.get("cash", 0) or 0
        net_income = data.get("net_income")
        revenue = data.get("revenue")
        preferred = data.get("preferred_stock", 0) or 0

        if not current_assets or not total_liabilities or not raw_shares:
            continue

        # Sanity check and fix shares outstanding
        shares = _sanity_check_shares(current_assets, total_assets, total_liabilities, equity, raw_shares)
        if not shares or shares <= 0:
            skipped_sanity += 1
            continue

        ncav = current_assets - total_liabilities - preferred
        ncav_per_share = ncav / shares

        # Skip negative NCAV
        if ncav_per_share <= 0:
            continue

        # Additional sanity: NCAV per share should be reasonable
        # For any real company, NCAV/share shouldn't exceed $5,000
        if ncav_per_share > 5000:
            skipped_sanity += 1
            continue

        # Skip if shares < 100K (likely a shell or data error)
        if shares < 100_000:
            continue

        book_value = equity if equity else (total_assets - total_liabilities if total_assets else None)
        book_per_share = book_value / shares if book_value and shares else None

        # Sanity check book/share too
        if book_per_share and abs(book_per_share) > 50_000:
            skipped_sanity += 1
            continue

        net_margin = None
        if net_income is not None and revenue and revenue > 0:
            net_margin = round(net_income / revenue * 100, 1)

        current_ratio = None
        if current_assets and total_liabilities and total_liabilities > 0:
            current_ratio = round(current_assets / total_liabilities, 2)

        cash_per_share = round(cash / shares, 2) if cash and shares else 0

        # Sanity: cash per share shouldn't exceed NCAV per share by a huge amount
        if cash_per_share > 10_000:
            cash_per_share = 0  # Probably a unit mismatch

        results.append({
            "ticker": ticker_info["ticker"],
            "name": ticker_info["name"],
            "ncav_per_share": round(ncav_per_share, 2),
            "book_per_share": round(book_per_share, 2) if book_per_share else None,
            "cash_per_share": cash_per_share,
            "current_assets": current_assets,
            "total_liabilities": total_liabilities,
            "total_assets": total_assets,
            "cash": cash,
            "shares_outstanding": int(shares),
            "net_income": net_income,
            "revenue": revenue,
            "net_margin": net_margin,
            "current_ratio": current_ratio,
            "ncav": ncav,
            "ncav_fmt": _fmt_cap(abs(ncav)),
        })

    # Sort by NCAV per share descending
    results.sort(key=lambda x: x["ncav_per_share"], reverse=True)

    _write_cache("net_net_screener", results)
    logger.info(f"Net-net screener: {len(results)} companies with positive NCAV ({skipped_sanity} skipped for sanity)")
    return results


def build_portfolio(capital=50000, num_positions=25):
    """Show top candidates from SEC EDGAR — no prices, user checks themselves."""
    cached = _read_cache("portfolio", max_age_hours=12)
    if cached:
        return cached

    all_stocks = build_net_net_screener()
    if not all_stocks or (len(all_stocks) == 1 and isinstance(all_stocks[0], dict) and "error" in all_stocks[0]):
        return {"error": "No screener data available", "positions": []}

    filtered = [s for s in all_stocks
                if s.get("current_ratio") and s["current_ratio"] >= 1.0
                and s.get("ncav_per_share", 0) > 0.50
                and s.get("shares_outstanding", 0) > 500_000]

    positions = []
    position_size = round(capital / num_positions, 2)

    for i, s in enumerate(filtered[:num_positions]):
        positions.append({
            "rank": i + 1,
            "ticker": s["ticker"],
            "name": s["name"],
            "ncav_per_share": s["ncav_per_share"],
            "book_per_share": s.get("book_per_share"),
            "cash_per_share": s.get("cash_per_share", 0),
            "net_margin": s.get("net_margin"),
            "current_ratio": s.get("current_ratio"),
            "ncav_fmt": s.get("ncav_fmt", "N/A"),
            "current_assets": s["current_assets"],
            "total_liabilities": s["total_liabilities"],
            "target_allocation": position_size,
        })

    result = {
        "portfolio": {
            "capital": capital,
            "num_positions": len(positions),
            "target_positions": num_positions,
            "position_size": position_size,
            "total_candidates_screened": len(all_stocks),
            "passed_filters": len(filtered),
            "note": "Look up current prices on Yahoo Finance. Buy if price < NCAV/share.",
        },
        "positions": positions,
        "filters_applied": {
            "min_ncav_per_share": "$0.50",
            "min_current_ratio": 1.0,
            "min_shares_outstanding": "500K",
            "positive_ncav_only": True,
        },
    }

    _write_cache("portfolio", result)
    return result


def _fmt_cap(val):
    if not val:
        return "N/A"
    if val >= 1e12:
        return f"${val/1e12:.1f}T"
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    if val >= 1e6:
        return f"${val/1e6:.0f}M"
    return f"${val:,.0f}"
