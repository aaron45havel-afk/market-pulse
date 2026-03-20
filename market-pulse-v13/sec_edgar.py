"""
SEC EDGAR XBRL + Twelve Data — True Net-Net Screener

Per Graham/Carlisle/Oxman methodology:
- NCAV = (Current Assets - Total Liabilities - Preferred Stock) / Shares Outstanding
- A net-net is a stock where PRICE < NCAV per share
- Graham's sweet spot: Price/NCAV ≤ 0.67 (buying at 2/3 of liquidation value)
- Exclude Price/NCAV < 0.01 (data errors / about to be delisted)

Architecture:
1. SEC EDGAR XBRL Frames → balance sheet for ALL companies (~6 API calls)
2. Pre-filter to ~50 smallest NCAV companies (most likely to be actual net-nets)
3. Twelve Data → live prices for just those ~50 tickers (4-5 API calls)
4. Calculate Price/NCAV → sort ascending → true net-nets rise to the top
"""
import os, json, logging, time, urllib.request
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
}

FINANCIAL_KEYWORDS = {
    "bank", "bancorp", "bancshares", "banc", "banking", "savings",
    "insurance", "underwriter", "reinsurance", "assurance",
    "reit", "real estate investment trust", "mortgage",
    "capital trust", "financial group", "credit union",
    "investment fund", "closed-end fund", "mutual fund", "etf",
    "acquisition corp", "blank check", "spac",
    "biotech", "biotherapeutics", "biopharma", "biopharmaceutical",
    "therapeutics", "pharmaceutical", "pharma",
}


# ═══════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════

def _sec_get(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": SEC_USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.warning(f"SEC error: {url}: {e}")
        return None

def _cache_path(key): return CACHE_DIR / f"{key}.json"

def _read_cache(key, max_age_hours=24):
    p = _cache_path(key)
    if p.exists() and time.time() - p.stat().st_mtime < max_age_hours * 3600:
        return json.loads(p.read_text())
    return None

def _write_cache(key, data):
    _cache_path(key).write_text(json.dumps(data, default=str))

def _fmt(val):
    if not val: return "N/A"
    v = abs(val)
    if v >= 1e12: return f"${v/1e12:.1f}T"
    if v >= 1e9: return f"${v/1e9:.1f}B"
    if v >= 1e6: return f"${v/1e6:.0f}M"
    return f"${v:,.0f}"

def _is_financial(name):
    nl = name.lower()
    return any(kw in nl for kw in FINANCIAL_KEYWORDS)

def _is_stale(filing_end, max_months=18):
    if not filing_end: return True
    try:
        from datetime import datetime, timedelta
        return datetime.strptime(filing_end[:10], "%Y-%m-%d") < datetime.now() - timedelta(days=max_months * 30)
    except: return True

def _fix_shares(ca, ta, tl, eq, shares):
    if not shares or shares <= 0: return None
    ref = ta or ca
    if not ref or ref <= 0: return shares
    aps = ref / shares
    if aps > 1_000_000: return shares * 1_000_000
    elif aps > 10_000: return shares * 1_000
    elif aps < 0.01: return None
    return shares


# ═══════════════════════════════════════════════════
# TWELVE DATA — Targeted price fetch for ~50 tickers
# ═══════════════════════════════════════════════════

def _fetch_prices_twelve_data(tickers):
    """Fetch prices for a small set of tickers from Twelve Data. Max ~50."""
    api_key = os.getenv("TWELVE_DATA_API_KEY", "")
    if not api_key:
        logger.warning("TWELVE_DATA_API_KEY not set")
        return {}

    prices = {}
    batch_size = 8
    calls = 0

    # Test with AAPL first
    try:
        url = f"https://api.twelvedata.com/price?symbol=AAPL&apikey={api_key}"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            test = json.loads(resp.read())
        logger.info(f"Twelve Data test: {test}")
        if "price" in test:
            prices["AAPL"] = float(test["price"])
        else:
            logger.error(f"Twelve Data test failed: {test}")
            return {}
        calls += 1
    except Exception as e:
        logger.error(f"Twelve Data test error: {e}")
        return {}

    # Filter out AAPL from our list
    tickers = [t for t in tickers if t != "AAPL"]

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        symbols = ",".join(batch)

        try:
            url = f"https://api.twelvedata.com/price?symbol={symbols}&apikey={api_key}"
            req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())

            if len(batch) == 1:
                if isinstance(data, dict) and "price" in data:
                    prices[batch[0]] = float(data["price"])
            else:
                if isinstance(data, dict):
                    for ticker, val in data.items():
                        if isinstance(val, dict) and "price" in val:
                            try: prices[ticker] = float(val["price"])
                            except: pass

            calls += 1
        except Exception as e:
            logger.warning(f"Twelve Data batch error: {e}")

        # Rate limit: 8 calls/min on free tier
        if calls >= 7:
            logger.info(f"Twelve Data: pausing, got {len(prices)} prices")
            time.sleep(62)
            calls = 0
        else:
            time.sleep(1)

    logger.info(f"Twelve Data: got {len(prices)} prices for {len(tickers)+1} tickers")
    return prices


# ═══════════════════════════════════════════════════
# SEC EDGAR DATA
# ═══════════════════════════════════════════════════

def _fetch_xbrl_frame(concept, period, unit="USD"):
    url = f"https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/{unit}/{period}.json"
    data = _sec_get(url)
    if not data or "data" not in data: return {}
    result = {}
    for e in data["data"]:
        cik = str(e.get("cik", ""))
        val = e.get("val")
        if cik and val is not None:
            if cik not in result or e.get("end", "") > result.get(cik, {}).get("end", ""):
                result[cik] = {"val": val, "end": e.get("end", ""), "entity": e.get("entityName", "")}
    logger.info(f"SEC: {concept}/{period} → {len(result)} companies")
    return result

def _get_periods():
    from datetime import datetime
    y, m = datetime.now().year, datetime.now().month
    if m <= 3: cq, cy = 3, y-1
    elif m <= 6: cq, cy = 4, y-1
    elif m <= 9: cq, cy = 1, y
    else: cq, cy = 2, y
    pq, py = (4, cy-1) if cq == 1 else (cq-1, cy)
    return f"CY{cy}Q{cq}I", f"CY{cy}Q{cq}", f"CY{py}Q{pq}I"

def get_ticker_map():
    cached = _read_cache("tickers_v4", max_age_hours=168)
    if cached: return cached
    data = _sec_get("https://www.sec.gov/files/company_tickers.json")
    if not data: return {}
    result = {str(e.get("cik_str")): {"ticker": e.get("ticker",""), "name": e.get("title","")} for e in data.values() if e.get("cik_str") and e.get("ticker")}
    _write_cache("tickers_v4", result)
    return result

def get_exchange_data():
    cached = _read_cache("exchanges_v2", max_age_hours=168)
    if cached: return cached
    data = _sec_get("https://www.sec.gov/files/company_tickers_exchange.json")
    if not data or "data" not in data: return {}
    fields = data.get("fields", [])
    ci = fields.index("cik") if "cik" in fields else 0
    ei = fields.index("exchange") if "exchange" in fields else 3
    result = {str(r[ci]): {"exchange": r[ei] if len(r) > ei else ""} for r in data.get("data", [])}
    _write_cache("exchanges_v2", result)
    return result

def fetch_financials():
    cached = _read_cache("fin_v4", max_age_hours=24)
    if cached: return cached
    curr_bs, curr_is, prior_bs = _get_periods()
    all_data = {}

    def fetch(name, concept, period, unit="USD"):
        return name, _fetch_xbrl_frame(concept, period, unit)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = []
        for n, c in BALANCE_SHEET_CONCEPTS.items():
            futs.append(ex.submit(fetch, n, c, curr_bs))
        futs.append(ex.submit(fetch, "shares_outstanding", "CommonStockSharesOutstanding", curr_bs, "shares"))
        for n, c in INCOME_CONCEPTS.items():
            futs.append(ex.submit(fetch, n, c, curr_is))
        futs.append(ex.submit(fetch, "cash_prior", "CashAndCashEquivalentsAtCarryingValue", prior_bs))

        for f in as_completed(futs):
            try:
                name, data = f.result()
                for cik, entry in data.items():
                    if cik not in all_data: all_data[cik] = {}
                    all_data[cik][name] = entry["val"]
                    all_data[cik]["_entity"] = entry.get("entity", "")
                    if entry.get("end"):
                        if entry["end"] > all_data[cik].get("_end", ""):
                            all_data[cik]["_end"] = entry["end"]
            except Exception as e:
                logger.warning(f"Fetch error: {e}")

    _write_cache("fin_v4", all_data)
    logger.info(f"SEC: {len(all_data)} companies")
    return all_data


# ═══════════════════════════════════════════════════
# NET-NET SCREENER — The correct implementation
# ═══════════════════════════════════════════════════

def build_net_net_screener():
    """
    True net-net screener:
    1. Get balance sheet data from SEC EDGAR
    2. Calculate NCAV for all companies
    3. Pre-filter to ~50 smallest NCAV companies (most likely to be net-nets)
    4. Fetch prices from Twelve Data for just those ~50
    5. Calculate Price/NCAV → sort by lowest → these are the real net-nets
    """
    cached = _read_cache("screener_v4", max_age_hours=12)
    if cached: return cached

    financials = fetch_financials()
    if not financials: return [{"error": "Failed to fetch SEC EDGAR data"}]
    tickers = get_ticker_map()
    if not tickers: return [{"error": "Failed to fetch ticker data"}]
    exchanges = get_exchange_data()

    # Step 1: Calculate NCAV for all companies, apply guardrails
    candidates = []
    skips = {}
    def skip(r): skips[r] = skips.get(r, 0) + 1

    for cik, d in financials.items():
        ti = tickers.get(cik)
        if not ti: skip("no_ticker"); continue

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
        filing_end = d.get("_end", "")
        cash_prior = d.get("cash_prior")

        if not ca or not tl or not raw_shares: skip("no_data"); continue
        if _is_stale(filing_end): skip("stale"); continue
        if _is_financial(name): skip("financial"); continue

        exc = exchanges.get(cik, {}).get("exchange", "")
        if exc and exc not in ("NYSE", "NASDAQ", "AMEX", "Nyse", "Nasdaq", ""): skip("foreign"); continue

        shares = _fix_shares(ca, ta, tl, eq, raw_shares)
        if not shares or shares < 100_000: skip("bad_shares"); continue

        ncav = ca - tl - pref
        ncav_ps = ncav / shares
        if ncav_ps <= 0: skip("neg_ncav"); continue
        if ncav_ps > 5000: skip("outlier"); continue

        # Revenue filter — but looser for net-nets (some are asset-rich but low-rev)
        # Only exclude zero revenue
        if rev is not None and rev <= 0: skip("no_rev"); continue

        bv = eq if eq else (ta - tl if ta else None)
        bv_ps = round(bv / shares, 2) if bv and shares else None
        if bv_ps and abs(bv_ps) > 50_000: skip("outlier"); continue

        net_margin = round(ni / rev * 100, 1) if ni is not None and rev and rev > 0 else None
        cr = round(ca / tl, 2) if tl and tl > 0 else None
        total_debt = st_debt + lt_debt
        net_cash = cash - total_debt
        net_cash_ps = round(net_cash / shares, 2) if shares else 0
        de = round(total_debt / eq, 2) if eq and eq > 0 and total_debt else 0

        # Burn rate
        burn_q = (cash - cash_prior) if cash_prior is not None else None
        burn_sev = "OK"
        qoc = None
        if burn_q is not None and burn_q < 0 and cash > 0:
            qoc = round(cash / abs(burn_q), 1)
            if qoc < 4: burn_sev = "CRITICAL"
            elif qoc < 8: burn_sev = "WARNING"

        neg_eq = (eq is not None and eq < 0)

        candidates.append({
            "ticker": ti["ticker"], "name": name,
            "ncav_per_share": round(ncav_ps, 2),
            "book_per_share": bv_ps,
            "cash_per_share": round(cash / shares, 2) if shares else 0,
            "net_cash_per_share": net_cash_ps,
            "current_ratio": cr, "net_margin": net_margin,
            "debt_to_equity": de,
            "ncav": ncav, "ncav_fmt": _fmt(ncav),
            "revenue": rev, "revenue_fmt": _fmt(rev) if rev else "N/A",
            "shares": int(shares),
            "negative_equity": neg_eq,
            "burn_severity": burn_sev, "quarters_of_cash": qoc,
            "cash_trend": "Building" if burn_q and burn_q > 0 else "Burning" if burn_q and burn_q < 0 else "N/A",
            "filing_date": filing_end,
        })

    logger.info(f"Screener: {len(candidates)} candidates with positive NCAV. Skipped: {skips}")

    # Step 2: Pre-filter to companies most likely to be net-nets
    # Net-nets are SMALL companies — sort by total NCAV ascending (smallest first)
    # Only fetch prices for the smallest ~60 by NCAV (these are the micro/small caps)
    candidates.sort(key=lambda x: x["ncav"])
    price_candidates = candidates[:60]

    # Step 3: Fetch prices from Twelve Data
    ticker_list = [c["ticker"] for c in price_candidates]
    price_map = _fetch_prices_twelve_data(ticker_list)

    # Step 4: Calculate Price/NCAV and build final results
    results = []
    for c in candidates:
        price = price_map.get(c["ticker"])
        if price and price > 0:
            p_ncav = round(price / c["ncav_per_share"], 2) if c["ncav_per_share"] > 0 else 999
            # Graham outlier filter: exclude Price/NCAV < 0.01
            if p_ncav < 0.01: continue
            is_net_net = p_ncav <= 1.0
            is_graham = p_ncav <= 0.67
        else:
            p_ncav = None
            is_net_net = None
            is_graham = None
            price = None

        market_cap = int(price * c["shares"]) if price else None

        results.append({
            **c,
            "price": round(price, 2) if price else None,
            "price_ncav": p_ncav,
            "is_net_net": is_net_net,
            "is_graham": is_graham,
            "market_cap": market_cap,
            "market_cap_fmt": _fmt(market_cap) if market_cap else "N/A",
            "has_price": price is not None,
        })

    # Sort: stocks WITH prices first, by Price/NCAV ascending (cheapest = best)
    # Then stocks without prices, by NCAV/share ascending (smallest = most likely net-nets)
    with_price = sorted([r for r in results if r["has_price"]], key=lambda x: x["price_ncav"])
    without_price = sorted([r for r in results if not r["has_price"]], key=lambda x: x["ncav_per_share"])

    final = with_price + without_price
    _write_cache("screener_v4", final)

    net_net_count = sum(1 for r in final if r.get("is_net_net"))
    graham_count = sum(1 for r in final if r.get("is_graham"))
    logger.info(f"Screener: {len(final)} total, {len(with_price)} with prices, {net_net_count} net-nets, {graham_count} Graham bargains")
    return final


def build_portfolio(capital=50000, num_positions=25):
    cached = _read_cache("portfolio_v4", max_age_hours=12)
    if cached: return cached

    all_stocks = build_net_net_screener()
    if not all_stocks or (len(all_stocks) == 1 and "error" in all_stocks[0]):
        return {"error": "No data", "positions": []}

    # Portfolio: only stocks with prices AND Price/NCAV < 1.5 (preferably < 1.0)
    # Also apply quality filters
    eligible = [s for s in all_stocks
                if s.get("has_price")
                and s.get("price_ncav") is not None
                and s["price_ncav"] <= 1.5  # Max 1.5x NCAV
                and s["price_ncav"] >= 0.01  # Not data errors
                and s.get("current_ratio") and s["current_ratio"] >= 1.0
                and not s.get("negative_equity")
                and s.get("burn_severity") != "CRITICAL"
                and s.get("price", 0) >= 0.50  # No penny stocks
                ]

    # If not enough net-nets, expand to 2x NCAV
    if len(eligible) < num_positions:
        eligible = [s for s in all_stocks
                    if s.get("has_price")
                    and s.get("price_ncav") is not None
                    and s["price_ncav"] <= 2.0
                    and s["price_ncav"] >= 0.01
                    and s.get("current_ratio") and s["current_ratio"] >= 1.0
                    and not s.get("negative_equity")
                    and s.get("burn_severity") != "CRITICAL"
                    and s.get("price", 0) >= 0.50
                    ]

    # Already sorted by Price/NCAV from screener
    positions = []
    ps = round(capital / num_positions, 2)

    for i, s in enumerate(eligible[:num_positions]):
        shares_to_buy = int(ps / s["price"]) if s["price"] and s["price"] > 0 else 0
        actual_cost = round(shares_to_buy * s["price"], 2) if s["price"] else 0

        positions.append({
            "rank": i + 1,
            "ticker": s["ticker"], "name": s["name"],
            "price": s["price"],
            "ncav_per_share": s["ncav_per_share"],
            "price_ncav": s["price_ncav"],
            "is_net_net": s.get("is_net_net", False),
            "is_graham": s.get("is_graham", False),
            "net_cash_per_share": s.get("net_cash_per_share", 0),
            "net_margin": s.get("net_margin"),
            "current_ratio": s.get("current_ratio"),
            "debt_to_equity": s.get("debt_to_equity", 0),
            "burn_severity": s.get("burn_severity", "OK"),
            "market_cap_fmt": s.get("market_cap_fmt", "N/A"),
            "revenue_fmt": s.get("revenue_fmt", "N/A"),
            "shares_to_buy": shares_to_buy,
            "actual_cost": actual_cost,
            "target_allocation": ps,
        })

    total_invested = sum(p["actual_cost"] for p in positions)

    result = {
        "portfolio": {
            "capital": capital,
            "num_positions": len(positions),
            "target_positions": num_positions,
            "position_size": ps,
            "total_invested": total_invested,
            "cash_remaining": round(capital - total_invested, 2),
            "total_screened": len(all_stocks),
            "with_prices": sum(1 for s in all_stocks if s.get("has_price")),
            "net_nets_found": sum(1 for s in all_stocks if s.get("is_net_net")),
            "graham_bargains": sum(1 for s in all_stocks if s.get("is_graham")),
        },
        "positions": positions,
        "methodology": "Buy stocks where Price < NCAV per share (Graham net-net strategy). Sorted by Price/NCAV ascending — cheapest relative to liquidation value first.",
    }

    _write_cache("portfolio_v4", result)
    return result
