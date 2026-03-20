"""
SEC EDGAR XBRL + Manual Price Entry — True Net-Net Screener

Filters:
- Total assets < $500M (real micro/small caps only)
- Total NCAV < $200M
- NCAV/share > $0.50
- Shares: 500K - 100M
- Revenue > $0 (no pre-revenue)
- US exchanges only
- Exclude financials, banks, REITs, insurance, biotech/pharma
- Filing < 18 months
- No negative equity
- No critical cash burners (< 4 quarters of cash)

Includes dividends per share from SEC EDGAR.
"""
import os, json, logging, time, urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

SEC_UA = "MarketPulse/1.0 invoice@archfms.com"
CACHE = Path("/tmp/market_pulse_cache")
CACHE.mkdir(exist_ok=True)

BS_CONCEPTS = {
    "current_assets": "AssetsCurrent",
    "total_liabilities": "Liabilities",
    "total_assets": "Assets",
    "stockholders_equity": "StockholdersEquity",
    "cash": "CashAndCashEquivalentsAtCarryingValue",
    "preferred_stock": "PreferredStockValue",
    "short_term_debt": "ShortTermBorrowings",
    "long_term_debt": "LongTermDebt",
}

IS_CONCEPTS = {
    "net_income": "NetIncomeLoss",
    "revenue": "Revenues",
    "dividends_paid": "PaymentsOfDividendsCommonStock",
}

EXCLUDED_KEYWORDS = {
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

def _get(url):
    try:
        r = urllib.request.Request(url, headers={"User-Agent": SEC_UA})
        with urllib.request.urlopen(r, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.warning(f"SEC: {url}: {e}")
        return None

def _cp(k): return CACHE / f"{k}.json"

def _rc(k, hrs=24):
    p = _cp(k)
    if p.exists() and time.time() - p.stat().st_mtime < hrs * 3600:
        return json.loads(p.read_text())
    return None

def _wc(k, d):
    _cp(k).write_text(json.dumps(d, default=str))

def _fmt(v):
    if not v: return "N/A"
    v = abs(v)
    if v >= 1e12: return f"${v/1e12:.1f}T"
    if v >= 1e9: return f"${v/1e9:.1f}B"
    if v >= 1e6: return f"${v/1e6:.0f}M"
    if v >= 1e3: return f"${v/1e3:.0f}K"
    return f"${v:,.0f}"

def _excluded(name):
    nl = name.lower()
    return any(kw in nl for kw in EXCLUDED_KEYWORDS)

def _stale(end, months=18):
    if not end: return True
    try:
        from datetime import datetime, timedelta
        return datetime.strptime(end[:10], "%Y-%m-%d") < datetime.now() - timedelta(days=months * 30)
    except: return True

def _fix_shares(ca, ta, tl, eq, s):
    if not s or s <= 0: return None
    ref = ta or ca
    if not ref or ref <= 0: return s
    aps = ref / s
    if aps > 1_000_000: return s * 1_000_000
    elif aps > 10_000: return s * 1_000
    elif aps < 0.01: return None
    return s


# ═══════════════════════════════════════════════════
# SEC EDGAR DATA
# ═══════════════════════════════════════════════════

def _frame(concept, period, unit="USD"):
    url = f"https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/{unit}/{period}.json"
    d = _get(url)
    if not d or "data" not in d: return {}
    r = {}
    for e in d["data"]:
        cik = str(e.get("cik", ""))
        val = e.get("val")
        if cik and val is not None:
            if cik not in r or e.get("end", "") > r.get(cik, {}).get("end", ""):
                r[cik] = {"val": val, "end": e.get("end", ""), "entity": e.get("entityName", "")}
    logger.info(f"SEC: {concept}/{period} → {len(r)}")
    return r

def _periods():
    from datetime import datetime
    y, m = datetime.now().year, datetime.now().month
    if m <= 3: cq, cy = 3, y-1
    elif m <= 6: cq, cy = 4, y-1
    elif m <= 9: cq, cy = 1, y
    else: cq, cy = 2, y
    pq, py = (4, cy-1) if cq == 1 else (cq-1, cy)
    return f"CY{cy}Q{cq}I", f"CY{cy}Q{cq}", f"CY{py}Q{pq}I"

def get_tickers():
    c = _rc("tickers_v5", 168)
    if c: return c
    d = _get("https://www.sec.gov/files/company_tickers.json")
    if not d: return {}
    r = {str(e["cik_str"]): {"ticker": e["ticker"], "name": e.get("title","")} for e in d.values() if e.get("cik_str") and e.get("ticker")}
    _wc("tickers_v5", r)
    return r

def get_exchanges():
    c = _rc("exc_v3", 168)
    if c: return c
    d = _get("https://www.sec.gov/files/company_tickers_exchange.json")
    if not d or "data" not in d: return {}
    f = d.get("fields", [])
    ci = f.index("cik") if "cik" in f else 0
    ei = f.index("exchange") if "exchange" in f else 3
    r = {str(row[ci]): {"exchange": row[ei] if len(row) > ei else ""} for row in d.get("data", [])}
    _wc("exc_v3", r)
    return r

def fetch_financials():
    c = _rc("fin_v5", 24)
    if c: return c
    cbs, cis, pbs = _periods()
    data = {}

    def f(n, concept, period, unit="USD"):
        return n, _frame(concept, period, unit)

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = []
        for n, concept in BS_CONCEPTS.items():
            futs.append(ex.submit(f, n, concept, cbs))
        futs.append(ex.submit(f, "shares", "CommonStockSharesOutstanding", cbs, "shares"))
        for n, concept in IS_CONCEPTS.items():
            futs.append(ex.submit(f, n, concept, cis))
        # Dividends per share (declared)
        futs.append(ex.submit(f, "div_per_share", "CommonStockDividendsPerShareDeclared", cis, "USD/shares"))
        # Prior quarter cash for burn rate
        futs.append(ex.submit(f, "cash_prior", "CashAndCashEquivalentsAtCarryingValue", pbs))

        for fut in as_completed(futs):
            try:
                name, result = fut.result()
                for cik, entry in result.items():
                    if cik not in data: data[cik] = {}
                    data[cik][name] = entry["val"]
                    data[cik]["_entity"] = entry.get("entity", "")
                    if entry.get("end") and entry["end"] > data[cik].get("_end", ""):
                        data[cik]["_end"] = entry["end"]
            except Exception as e:
                logger.warning(f"Fetch: {e}")

    _wc("fin_v5", data)
    logger.info(f"SEC: {len(data)} companies total")
    return data


# ═══════════════════════════════════════════════════
# NET-NET SCREENER
# ═══════════════════════════════════════════════════

def build_net_net_screener():
    c = _rc("screener_v5", 12)
    if c: return c

    fin = fetch_financials()
    if not fin: return [{"error": "SEC EDGAR fetch failed"}]
    tickers = get_tickers()
    if not tickers: return [{"error": "Ticker fetch failed"}]
    exc = get_exchanges()

    results = []
    skips = {}
    def sk(r): skips[r] = skips.get(r, 0) + 1

    for cik, d in fin.items():
        ti = tickers.get(cik)
        if not ti: sk("no_ticker"); continue

        ca = d.get("current_assets")
        tl = d.get("total_liabilities")
        ta = d.get("total_assets")
        eq = d.get("stockholders_equity")
        cash = d.get("cash", 0) or 0
        raw_sh = d.get("shares")
        ni = d.get("net_income")
        rev = d.get("revenue")
        pref = d.get("preferred_stock", 0) or 0
        st_debt = d.get("short_term_debt", 0) or 0
        lt_debt = d.get("long_term_debt", 0) or 0
        div_ps = d.get("div_per_share")
        div_paid = d.get("dividends_paid", 0) or 0
        name = ti.get("name", "") or d.get("_entity", "")
        end = d.get("_end", "")
        cash_prior = d.get("cash_prior")

        if not ca or not tl or not raw_sh: sk("no_data"); continue
        if _stale(end): sk("stale"); continue
        if _excluded(name): sk("excluded"); continue

        e = exc.get(cik, {}).get("exchange", "")
        if e and e not in ("NYSE", "NASDAQ", "AMEX", "Nyse", "Nasdaq", ""): sk("foreign"); continue

        shares = _fix_shares(ca, ta, tl, eq, raw_sh)
        if not shares: sk("bad_shares"); continue

        # ── SIZE FILTERS ──
        if shares < 500_000 or shares > 100_000_000: sk("shares_range"); continue
        if ta and ta > 500_000_000: sk("too_large"); continue

        ncav = ca - tl - pref
        ncav_ps = ncav / shares
        if ncav_ps <= 0: sk("neg_ncav"); continue
        if ncav > 200_000_000: sk("too_large"); continue
        if ncav_ps < 0.50: sk("too_small"); continue
        if ncav_ps > 5000: sk("outlier"); continue

        if rev is not None and rev <= 0: sk("no_rev"); continue

        # Negative equity check
        neg_eq = (eq is not None and eq < 0)

        bv = eq if eq else (ta - tl if ta else None)
        bv_ps = round(bv / shares, 2) if bv and shares else None
        if bv_ps and abs(bv_ps) > 50_000: sk("outlier"); continue

        margin = round(ni / rev * 100, 1) if ni is not None and rev and rev > 0 else None
        cr = round(ca / tl, 2) if tl and tl > 0 else None
        total_debt = st_debt + lt_debt
        net_cash = cash - total_debt
        net_cash_ps = round(net_cash / shares, 2)
        de = round(total_debt / eq, 2) if eq and eq > 0 and total_debt else 0

        # Dividends
        if div_ps is not None:
            div_per_share = round(div_ps, 4)
        elif div_paid and div_paid > 0 and shares:
            div_per_share = round(div_paid / shares, 4)
        else:
            div_per_share = 0

        # Burn rate
        burn_q = (cash - cash_prior) if cash_prior is not None else None
        burn_sev = "OK"
        qoc = None
        if burn_q is not None and burn_q < 0 and cash > 0:
            qoc = round(cash / abs(burn_q), 1)
            if qoc < 4: burn_sev = "CRITICAL"
            elif qoc < 8: burn_sev = "WARNING"

        # Market cap estimate (NCAV-based minimum)
        est_mktcap = ncav  # minimum — real mktcap needs price

        results.append({
            "ticker": ti["ticker"],
            "name": name,
            "ncav_per_share": round(ncav_ps, 2),
            "book_per_share": bv_ps,
            "cash_per_share": round(cash / shares, 2),
            "net_cash_per_share": net_cash_ps,
            "current_ratio": cr,
            "net_margin": margin,
            "debt_to_equity": de,
            "dividend_per_share": div_per_share,
            "pays_dividend": div_per_share > 0,
            "ncav": ncav,
            "ncav_fmt": _fmt(ncav),
            "total_assets": ta,
            "total_assets_fmt": _fmt(ta) if ta else "N/A",
            "revenue": rev,
            "revenue_fmt": _fmt(rev) if rev else "N/A",
            "shares": int(shares),
            "eps": round(ni / shares, 2) if ni and shares else None,
            "est_mktcap_fmt": _fmt(est_mktcap),
            "negative_equity": neg_eq,
            "burn_severity": burn_sev,
            "quarters_of_cash": qoc,
            "cash_trend": "Building" if burn_q and burn_q > 0 else "Burning" if burn_q and burn_q < 0 else "N/A",
            "filing_date": end,
            # These get filled in client-side when user enters price
            "price": None,
            "price_ncav": None,
            "is_net_net": None,
            "is_graham": None,
            "has_price": False,
            "market_cap": None,
            "market_cap_fmt": None,
        })

    results.sort(key=lambda x: x["ncav_per_share"], reverse=True)
    _wc("screener_v5", results)
    logger.info(f"Screener: {len(results)} candidates. Skipped: {skips}")
    return results


def build_portfolio(capital=50000, num_positions=25):
    c = _rc("portfolio_v5", 12)
    if c: return c

    stocks = build_net_net_screener()
    if not stocks or (len(stocks) == 1 and "error" in stocks[0]):
        return {"error": "No data", "positions": []}

    # Portfolio shows all candidates that pass quality filters
    # User enters prices to determine which are actual net-nets
    filtered = [s for s in stocks
                if s.get("current_ratio") and s["current_ratio"] >= 1.0
                and not s.get("negative_equity")
                and s.get("burn_severity") != "CRITICAL"]

    ps = round(capital / num_positions, 2)
    positions = [{
        "rank": i + 1,
        **{k: s[k] for k in ["ticker", "name", "ncav_per_share", "book_per_share",
            "cash_per_share", "net_cash_per_share", "net_margin", "current_ratio",
            "debt_to_equity", "dividend_per_share", "pays_dividend", "burn_severity",
            "cash_trend", "ncav_fmt", "total_assets_fmt", "revenue_fmt", "est_mktcap_fmt",
            "filing_date", "shares"]},
        "target_allocation": ps,
    } for i, s in enumerate(filtered[:num_positions * 2])]  # Show 2x positions so user has options

    result = {
        "portfolio": {
            "capital": capital,
            "num_positions": num_positions,
            "position_size": ps,
            "candidates_shown": len(positions),
            "total_screened": len(stocks),
            "passed_quality": len(filtered),
        },
        "positions": positions,
        "methodology": "Enter prices from Yahoo Finance. Buy where Price < NCAV/share. Target 25 positions at $2,000 each.",
    }
    _wc("portfolio_v5", result)
    return result
