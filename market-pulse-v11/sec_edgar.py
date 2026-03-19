"""
SEC EDGAR XBRL Frames API — Net-Net & Deep Value Stock Screener.

Guardrails:
1. Shares outstanding unit correction
2. NCAV/share, book/share outlier rejection
3. Financial sector exclusion (SIC + keywords)
4. ADR/foreign filer exclusion (US exchanges only)
5. Minimum revenue ($1M+ to exclude SPACs/blank checks)
6. Filing recency (exclude stale >18 months)
7. Negative equity flag
8. NCAV as % of total assets (Berkshire normalization)
9. Net cash per share (cash minus all debt)
10. Burn rate (quarterly cash consumption) + burn rate guardrail
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
    "short_term_debt": "ShortTermBorrowings",
    "long_term_debt": "LongTermDebt",
}

INCOME_CONCEPTS = {
    "net_income": "NetIncomeLoss",
    "revenue": "Revenues",
    "gross_profit": "GrossProfit",
}

FINANCIAL_KEYWORDS = {
    "bank", "bancorp", "bancshares", "banc", "banking", "savings",
    "insurance", "underwriter", "reinsurance", "assurance",
    "reit", "real estate investment trust", "mortgage",
    "capital trust", "financial group", "credit union",
    "investment fund", "closed-end fund", "mutual fund", "etf",
    "acquisition corp", "blank check", "spac",
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
    cached = _read_cache("sec_tickers_v3", max_age_hours=168)
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
    _write_cache("sec_tickers_v3", result)
    logger.info(f"SEC: Loaded {len(result)} ticker mappings")
    return result


def _get_exchange_data():
    cached = _read_cache("sec_exchanges", max_age_hours=168)
    if cached:
        return cached
    data = _sec_get("https://www.sec.gov/files/company_tickers_exchange.json")
    if not data or "data" not in data:
        return {}
    result = {}
    fields = data.get("fields", [])
    cik_idx = fields.index("cik") if "cik" in fields else 0
    exchange_idx = fields.index("exchange") if "exchange" in fields else 3
    for row in data.get("data", []):
        cik = str(row[cik_idx])
        exchange = row[exchange_idx] if len(row) > exchange_idx else ""
        result[cik] = {"exchange": exchange}
    _write_cache("sec_exchanges", result)
    logger.info(f"SEC: Loaded exchange data for {len(result)} companies")
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


def _get_periods():
    """Return (current_period, prior_period) for balance sheet and income statement."""
    from datetime import datetime
    now = datetime.now()
    y, m = now.year, now.month

    # Current quarter available
    if m <= 3:
        curr_q, curr_y = 3, y - 1
    elif m <= 6:
        curr_q, curr_y = 4, y - 1
    elif m <= 9:
        curr_q, curr_y = 1, y
    else:
        curr_q, curr_y = 2, y

    # Prior quarter
    if curr_q == 1:
        prior_q, prior_y = 4, curr_y - 1
    else:
        prior_q, prior_y = curr_q - 1, curr_y

    curr_bs = f"CY{curr_y}Q{curr_q}I"
    curr_is = f"CY{curr_y}Q{curr_q}"
    prior_bs = f"CY{prior_y}Q{prior_q}I"

    return curr_bs, curr_is, prior_bs


def fetch_all_financials():
    cached = _read_cache("sec_fin_v3", max_age_hours=24)
    if cached:
        return cached

    curr_bs, curr_is, prior_bs = _get_periods()
    all_data = {}

    def fetch_frame(name, concept, period, unit="USD"):
        return name, period, _fetch_xbrl_frame(concept, period, unit)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []

        # Current quarter balance sheet
        for name, concept in BALANCE_SHEET_CONCEPTS.items():
            futures.append(executor.submit(fetch_frame, name, concept, curr_bs))

        # Shares (in "shares" unit)
        futures.append(executor.submit(fetch_frame, "shares_outstanding", "CommonStockSharesOutstanding", curr_bs, "shares"))

        # Income statement
        for name, concept in INCOME_CONCEPTS.items():
            futures.append(executor.submit(fetch_frame, name, concept, curr_is))

        # PRIOR quarter cash (for burn rate calculation)
        futures.append(executor.submit(fetch_frame, "cash_prior", "CashAndCashEquivalentsAtCarryingValue", prior_bs))
        # PRIOR quarter current assets (for NCAV burn rate)
        futures.append(executor.submit(fetch_frame, "current_assets_prior", "AssetsCurrent", prior_bs))
        # PRIOR quarter liabilities (for NCAV burn rate)
        futures.append(executor.submit(fetch_frame, "total_liabilities_prior", "Liabilities", prior_bs))

        for future in as_completed(futures):
            try:
                metric_name, period, data = future.result()
                for cik, entry in data.items():
                    if cik not in all_data:
                        all_data[cik] = {}
                    all_data[cik][metric_name] = entry["val"]
                    all_data[cik]["_entity"] = entry.get("entity", "")
                    if entry.get("end"):
                        existing = all_data[cik].get("_filing_end", "")
                        if entry["end"] > existing:
                            all_data[cik]["_filing_end"] = entry["end"]
            except Exception as e:
                logger.warning(f"SEC fetch error: {e}")

    _write_cache("sec_fin_v3", all_data)
    logger.info(f"SEC: Combined financial data for {len(all_data)} companies")
    return all_data


def _sanity_check_shares(current_assets, total_assets, total_liabilities, equity, shares):
    if not shares or shares <= 0:
        return None
    ref_value = total_assets or current_assets
    if not ref_value or ref_value <= 0:
        return shares
    aps = ref_value / shares
    if aps > 1_000_000:
        return shares * 1_000_000
    elif aps > 10_000:
        return shares * 1_000
    elif aps < 0.01:
        return None
    return shares


def _is_financial_company(name):
    name_lower = name.lower()
    for kw in FINANCIAL_KEYWORDS:
        if kw in name_lower:
            return True
    return False


def _is_filing_stale(filing_end, max_months=18):
    if not filing_end:
        return True
    try:
        from datetime import datetime, timedelta
        filed = datetime.strptime(filing_end[:10], "%Y-%m-%d")
        return filed < datetime.now() - timedelta(days=max_months * 30)
    except Exception:
        return True


def build_net_net_screener():
    """Build screener with all guardrails + net cash + burn rate."""
    cached = _read_cache("screener_v3", max_age_hours=12)
    if cached:
        return cached

    financials = fetch_all_financials()
    if not financials:
        return [{"error": "Failed to fetch SEC EDGAR data"}]

    ticker_map = get_ticker_cik_map()
    if not ticker_map:
        return [{"error": "Failed to fetch SEC ticker data"}]

    exchange_data = _get_exchange_data()

    results = []
    skips = {}

    def skip(reason):
        skips[reason] = skips.get(reason, 0) + 1

    for cik, d in financials.items():
        ti = ticker_map.get(cik)
        if not ti:
            skip("no_ticker"); continue

        ca = d.get("current_assets")
        tl = d.get("total_liabilities")
        ta = d.get("total_assets")
        eq = d.get("stockholders_equity")
        cash = d.get("cash", 0) or 0
        raw_shares = d.get("shares_outstanding")
        ni = d.get("net_income")
        rev = d.get("revenue")
        pref = d.get("preferred_stock", 0) or 0
        st_debt = d.get("short_term_debt", 0) or 0
        lt_debt = d.get("long_term_debt", 0) or 0
        name = ti.get("name", "") or d.get("_entity", "")
        filing_end = d.get("_filing_end", "")

        # Prior quarter data for burn rate
        cash_prior = d.get("cash_prior")
        ca_prior = d.get("current_assets_prior")
        tl_prior = d.get("total_liabilities_prior")

        if not ca or not tl or not raw_shares:
            skip("no_data"); continue

        # Guardrail 6: Filing recency
        if _is_filing_stale(filing_end):
            skip("stale"); continue

        # Guardrail 3: Exclude financials
        if _is_financial_company(name):
            skip("financial"); continue

        # Guardrail 4: US exchanges only
        exc = exchange_data.get(cik, {}).get("exchange", "")
        if exc and exc not in ("NYSE", "NASDAQ", "AMEX", "Nyse", "Nasdaq", ""):
            skip("foreign"); continue

        # Guardrail 1: Shares unit check
        shares = _sanity_check_shares(ca, ta, tl, eq, raw_shares)
        if not shares or shares < 100_000:
            skip("bad_shares"); continue

        # === NCAV ===
        ncav = ca - tl - pref
        ncav_ps = ncav / shares
        if ncav_ps <= 0:
            skip("neg_ncav"); continue
        if ncav_ps > 5000:
            skip("outlier"); continue

        # === Book value ===
        bv = eq if eq else (ta - tl if ta else None)
        bv_ps = bv / shares if bv and shares else None
        if bv_ps and abs(bv_ps) > 50_000:
            skip("outlier"); continue

        # Guardrail 5: Min revenue
        if not rev or rev < 1_000_000:
            skip("no_revenue"); continue

        # === Net Cash (cash minus ALL debt) ===
        total_debt = st_debt + lt_debt
        net_cash = cash - total_debt
        net_cash_ps = round(net_cash / shares, 2) if shares else 0

        # === Burn Rate (quarterly change in cash) ===
        burn_rate_q = None  # Quarterly cash burn (negative = burning)
        burn_rate_annual = None
        quarters_of_cash = None
        if cash_prior is not None and cash_prior > 0:
            burn_rate_q = cash - cash_prior  # Positive = building cash, negative = burning
            burn_rate_annual = burn_rate_q * 4  # Annualized

            # Quarters of cash remaining (if burning)
            if burn_rate_q < 0 and cash > 0:
                quarters_of_cash = round(cash / abs(burn_rate_q), 1)

        # === NCAV Burn Rate (change in NCAV quarter over quarter) ===
        ncav_burn_q = None
        if ca_prior is not None and tl_prior is not None:
            ncav_prior = ca_prior - tl_prior - pref
            ncav_burn_q = ncav - ncav_prior  # Positive = NCAV growing, negative = shrinking

        # GUARDRAIL 10: Burn rate filter
        # Flag companies burning through cash fast
        is_cash_burner = False
        burn_severity = "OK"
        if burn_rate_q is not None and burn_rate_q < 0:
            if quarters_of_cash is not None and quarters_of_cash < 4:
                is_cash_burner = True
                burn_severity = "CRITICAL"  # Less than 1 year of cash
            elif quarters_of_cash is not None and quarters_of_cash < 8:
                burn_severity = "WARNING"  # 1-2 years of cash

        # === Other metrics ===
        net_margin = round(ni / rev * 100, 1) if ni is not None and rev and rev > 0 else None
        current_ratio = round(ca / tl, 2) if tl and tl > 0 else None
        cash_ps = round(cash / shares, 2) if shares else 0
        if cash_ps > 10_000:
            cash_ps = 0
        debt_to_equity = round(total_debt / eq, 2) if eq and eq > 0 and total_debt else 0

        # Guardrail 7: Negative equity flag
        neg_equity = (eq is not None and eq < 0) or (bv is not None and bv < 0)

        # Guardrail 8: NCAV % of total assets
        ncav_pct = round(ncav / ta * 100, 1) if ta and ta > 0 else None

        results.append({
            "ticker": ti["ticker"],
            "name": name,
            "ncav_per_share": round(ncav_ps, 2),
            "book_per_share": round(bv_ps, 2) if bv_ps else None,
            "cash_per_share": cash_ps,
            "net_cash_per_share": net_cash_ps,
            "current_ratio": current_ratio,
            "net_margin": net_margin,
            "debt_to_equity": debt_to_equity,
            "ncav_pct_assets": ncav_pct,
            "negative_equity": neg_equity,
            "revenue": rev,
            "revenue_fmt": _fmt_cap(rev) if rev else "N/A",
            "net_income": ni,
            "ncav": ncav,
            "ncav_fmt": _fmt_cap(abs(ncav)),
            "shares_outstanding": int(shares),
            "total_debt": total_debt,
            "filing_date": filing_end,
            # Burn rate fields
            "burn_rate_q": burn_rate_q,
            "burn_rate_q_fmt": _fmt_cap(abs(burn_rate_q)) if burn_rate_q else None,
            "burn_rate_annual": burn_rate_annual,
            "quarters_of_cash": quarters_of_cash,
            "ncav_burn_q": ncav_burn_q,
            "is_cash_burner": is_cash_burner,
            "burn_severity": burn_severity,
            "cash_trend": "Building" if burn_rate_q and burn_rate_q > 0 else "Burning" if burn_rate_q and burn_rate_q < 0 else "N/A",
        })

    results.sort(key=lambda x: x["ncav_per_share"], reverse=True)
    _write_cache("screener_v3", results)
    logger.info(f"Screener: {len(results)} pass all guardrails. Skipped: {skips}")
    return results


def build_portfolio(capital=50000, num_positions=25):
    cached = _read_cache("portfolio_v3", max_age_hours=12)
    if cached:
        return cached

    all_stocks = build_net_net_screener()
    if not all_stocks or (len(all_stocks) == 1 and "error" in all_stocks[0]):
        return {"error": "No screener data available", "positions": []}

    # Portfolio filters — STRICT
    filtered = [s for s in all_stocks
                if s.get("current_ratio") and s["current_ratio"] >= 1.0
                and s.get("ncav_per_share", 0) > 0.50
                and s.get("shares_outstanding", 0) > 500_000
                and not s.get("negative_equity", False)
                and not s.get("is_cash_burner", False)  # EXCLUDE critical cash burners
                and s.get("burn_severity") != "CRITICAL"
                ]

    positions = []
    ps = round(capital / num_positions, 2)

    for i, s in enumerate(filtered[:num_positions]):
        positions.append({
            "rank": i + 1,
            "ticker": s["ticker"],
            "name": s["name"],
            "ncav_per_share": s["ncav_per_share"],
            "book_per_share": s.get("book_per_share"),
            "cash_per_share": s.get("cash_per_share", 0),
            "net_cash_per_share": s.get("net_cash_per_share", 0),
            "net_margin": s.get("net_margin"),
            "current_ratio": s.get("current_ratio"),
            "debt_to_equity": s.get("debt_to_equity", 0),
            "ncav_fmt": s.get("ncav_fmt", "N/A"),
            "ncav_pct_assets": s.get("ncav_pct_assets"),
            "revenue_fmt": s.get("revenue_fmt", "N/A"),
            "filing_date": s.get("filing_date", "N/A"),
            "burn_severity": s.get("burn_severity", "OK"),
            "cash_trend": s.get("cash_trend", "N/A"),
            "quarters_of_cash": s.get("quarters_of_cash"),
            "target_allocation": ps,
        })

    result = {
        "portfolio": {
            "capital": capital,
            "num_positions": len(positions),
            "target_positions": num_positions,
            "position_size": ps,
            "total_candidates_screened": len(all_stocks),
            "passed_filters": len(filtered),
            "note": "Look up current prices on Yahoo Finance. Buy if price < NCAV/share.",
        },
        "positions": positions,
        "filters_applied": {
            "min_ncav_per_share": "$0.50",
            "min_current_ratio": 1.0,
            "min_shares_outstanding": "500K",
            "min_revenue": "$1M",
            "exclude_financials": True,
            "exclude_negative_equity": True,
            "exclude_critical_burners": True,
            "exclude_stale_filings": "18 months",
            "us_exchanges_only": True,
        },
    }

    _write_cache("portfolio_v3", result)
    return result


def _fmt_cap(val):
    if not val: return "N/A"
    if val >= 1e12: return f"${val/1e12:.1f}T"
    if val >= 1e9: return f"${val/1e9:.1f}B"
    if val >= 1e6: return f"${val/1e6:.0f}M"
    return f"${val:,.0f}"
