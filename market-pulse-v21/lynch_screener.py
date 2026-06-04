"""Peter Lynch GARP screener — value-tilted growth.

Criteria (stricter than Lynch's PEG<1.0 classic — user's spec):
  • Market cap > $1B (large-cap only)
  • P/E < 10 (deep value floor)
  • 3-year EPS CAGR > 10% (real growth, not a one-quarter blip)
  • Debt/Equity < 0.5 (low leverage)
  • CapEx / Operating Cash Flow < 0.5 (asset-light / shareholder-friendly)
  • Listed on NYSE/NASDAQ/AMEX (includes ADRs — buyable on Schwab/Fidelity)
  • Positive trailing earnings (P/E must be defined + positive)
  • Excludes SPACs, warrants, and a few junk patterns

Two-stage pipeline to keep API calls manageable across ~10K tickers:
  Stage 1 — bulk XBRL frames + Stooq prices → cheap filter on cap + P/E
  Stage 2 — for survivors, pull companyfacts (full XBRL history) →
            compute growth, debt, capex/OCF
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from statistics import median

# Reuse the SEC HTTP + cache helpers from the existing screener module
# so behavior, rate-limit, and caching are consistent across screeners.
from sec_edgar import (
    SEC_UA,
    CACHE,
    EXCLUDED_KEYWORDS,
    EXCLUDED_SIC_RANGES,
    _get,
    _rc,
    _wc,
    _is_excluded_sic,
    _excluded_keyword,
    _is_warrant,
    get_tickers,
    get_exchanges,
    get_company_details_bulk,
)

log = logging.getLogger(__name__)

# Lynch screener thresholds. Centralized so we can iterate / surface
# in the UI / freeze into snapshot metadata.
LYNCH_RULES = {
    "market_cap_min": 1_000_000_000,          # $1B
    "pe_max": 10.0,
    "eps_growth_3yr_min_pct": 10.0,
    "debt_to_equity_max": 0.5,
    "capex_to_ocf_max": 0.5,
    "exchanges": ("NYSE", "Nasdaq", "NASDAQ", "AMEX", "NYSE American"),
    "min_eps_years": 3,                       # need at least 3 fiscal years of EPS
}

MAJOR_EXCHANGES = {e.upper() for e in LYNCH_RULES["exchanges"]}

# Stooq endpoint for a single ticker's daily CSV. .us suffix is
# Stooq's convention for US-listed securities. Cheap, no API key.
STOOQ_DAILY_URL = "https://stooq.com/q/d/l/?s={t}.us&i=d"


# ─── Stooq price feed ───────────────────────────────────────────────
def _fetch_stooq_last_close(ticker: str) -> float | None:
    """Return the most recent daily close from Stooq, or None on miss.

    Stooq is intentionally tolerant: any 4xx/empty/HTML body → None
    rather than blowing up. With ~3000 tickers we expect a handful of
    misses (delisted, ticker change, Stooq doesn't cover) and they
    just drop out of the screen."""
    try:
        url = STOOQ_DAILY_URL.format(t=ticker.lower())
        req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
        with urllib.request.urlopen(req, timeout=15) as r:
            text = r.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        return None
    if not text or "Date" not in text.splitlines()[0]:
        return None
    # CSV header: Date,Open,High,Low,Close,Volume — take the last row's Close.
    lines = [ln for ln in text.strip().splitlines() if ln]
    if len(lines) < 2:
        return None
    last = lines[-1].split(",")
    if len(last) < 5:
        return None
    try:
        return float(last[4])
    except ValueError:
        return None


def fetch_prices_bulk(tickers: list[str], max_workers: int = 16) -> dict[str, float]:
    """Parallel Stooq fetches with a 24h cache. Returns {ticker: close}."""
    cached = _rc("lynch_prices_v1", 24) or {}
    missing = [t for t in tickers if t not in cached]
    if not missing:
        return cached

    log.info("Stooq prices: %d cached, fetching %d new …", len(cached), len(missing))
    fresh: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_fetch_stooq_last_close, t): t for t in missing}
        done = 0
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                price = fut.result()
            except Exception:
                price = None
            if price is not None:
                fresh[t] = price
            done += 1
            if done % 200 == 0:
                log.info("  Stooq: %d/%d done (%d hits)", done, len(missing), len(fresh))

    cached.update(fresh)
    _wc("lynch_prices_v1", cached)
    log.info("Stooq prices: %d total (%d new, %d misses)",
             len(cached), len(fresh), len(missing) - len(fresh))
    return cached


# ─── SEC companyfacts (per-company XBRL history) ─────────────────────
def _fetch_companyfacts(cik: str) -> dict | None:
    """Pull full XBRL history for one company. SEC publishes this as
    a single JSON per CIK; one request gives us every concept across
    every period the company has reported."""
    padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json"
    return _get(url)


def _annual_eps_history(facts: dict) -> list[tuple[str, float]]:
    """Extract annual diluted EPS for the last 4-5 fiscal years.

    Tries EarningsPerShareDiluted first (most common), falls back to
    EarningsPerShareBasic if the diluted figure isn't reported.
    Returns [(fy_end_date, eps), …] sorted oldest → newest."""
    if not facts:
        return []
    us_gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    for concept in ("EarningsPerShareDiluted", "EarningsPerShareBasic"):
        units = (us_gaap.get(concept) or {}).get("units") or {}
        # The unit key for EPS is typically "USD/shares".
        for unit_key, entries in units.items():
            if "shares" not in unit_key.lower():
                continue
            annuals = [
                (e["end"], float(e["val"]))
                for e in entries
                if e.get("fp") == "FY" and e.get("form", "").startswith("10-K")
                and e.get("val") is not None
            ]
            if not annuals:
                continue
            # Latest entry per fiscal-year-end wins (handles amendments).
            best: dict[str, float] = {}
            for end, val in annuals:
                best[end] = val
            ordered = sorted(best.items())  # by date string
            if ordered:
                return ordered
    return []


def _latest_balance_sheet(facts: dict) -> dict:
    """Latest period values for the balance-sheet concepts we need:
    total debt = short_term + long_term, plus stockholders equity."""
    out = {"short_term_debt": 0.0, "long_term_debt": 0.0, "equity": None}
    if not facts:
        return out
    us_gaap = (facts.get("facts") or {}).get("us-gaap") or {}

    def latest_val(concept: str) -> float | None:
        units = (us_gaap.get(concept) or {}).get("units") or {}
        usd = units.get("USD") or []
        if not usd:
            return None
        # Most recent end-date wins, regardless of form.
        latest = max(usd, key=lambda e: e.get("end", ""))
        v = latest.get("val")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    st = latest_val("ShortTermBorrowings") or latest_val("DebtCurrent") or 0.0
    lt = latest_val("LongTermDebt") or latest_val("LongTermDebtNoncurrent") or 0.0
    eq = latest_val("StockholdersEquity")
    out["short_term_debt"] = st or 0.0
    out["long_term_debt"] = lt or 0.0
    out["equity"] = eq
    return out


def _ttm_capex_and_ocf(facts: dict) -> tuple[float | None, float | None]:
    """Trailing-twelve-month CapEx and Operating Cash Flow.

    Cash-flow line items report cumulative-to-date YTD in 10-Qs and
    full-year in 10-Ks, so TTM = latest full FY value (simplest, no
    quarterly stitching). Good enough for a screening filter; not
    bench-grade financial analysis."""
    if not facts:
        return None, None
    us_gaap = (facts.get("facts") or {}).get("us-gaap") or {}

    def latest_fy(concept: str) -> float | None:
        units = (us_gaap.get(concept) or {}).get("units") or {}
        usd = units.get("USD") or []
        # Annual 10-K, FY frame.
        annuals = [
            e for e in usd
            if e.get("fp") == "FY" and e.get("form", "").startswith("10-K")
            and e.get("val") is not None
        ]
        if not annuals:
            return None
        latest = max(annuals, key=lambda e: e.get("end", ""))
        try:
            return float(latest["val"])
        except (TypeError, ValueError):
            return None

    capex = latest_fy("PaymentsToAcquirePropertyPlantAndEquipment")
    ocf = latest_fy("NetCashProvidedByUsedInOperatingActivities")
    return capex, ocf


def _latest_shares_outstanding(facts: dict) -> float | None:
    """Latest CommonStockSharesOutstanding (or EntityCommonStockSharesOutstanding
    fallback). Many filers report both; we just want the most recent
    reasonable value."""
    if not facts:
        return None
    us_gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    dei = (facts.get("facts") or {}).get("dei") or {}
    candidates = [
        us_gaap.get("CommonStockSharesOutstanding"),
        dei.get("EntityCommonStockSharesOutstanding"),
        us_gaap.get("WeightedAverageNumberOfDilutedSharesOutstanding"),
    ]
    for c in candidates:
        if not c:
            continue
        units = c.get("units") or {}
        shares = units.get("shares") or []
        if not shares:
            continue
        latest = max(shares, key=lambda e: e.get("end", ""))
        v = latest.get("val")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            continue
    return None


# ─── Universe + screening ────────────────────────────────────────────
def _build_universe() -> list[dict]:
    """All major-exchange-listed US filers minus financials/biotech/
    warrants/SPACs. Returns [{cik, ticker, name, exchange, sic}, …].

    Includes ADRs (foreign companies listed on US exchanges) since
    those are buyable on Schwab/Fidelity/Chase like any other ticker."""
    tickers = get_tickers()
    exchanges = get_exchanges()
    log.info("Universe candidates: %d tickers, %d with exchange data",
             len(tickers), len(exchanges))

    # First pass — exchange + name + warrant filter (cheap, no SEC calls).
    pre: list[dict] = []
    for cik, t in tickers.items():
        ticker = (t.get("ticker") or "").upper()
        name = t.get("name") or ""
        if not ticker or not name:
            continue
        if _is_warrant(ticker):
            continue
        if _excluded_keyword(name):
            continue
        exch = (exchanges.get(cik) or {}).get("exchange", "")
        if exch and exch.upper() not in MAJOR_EXCHANGES:
            continue
        pre.append({"cik": cik, "ticker": ticker, "name": name, "exchange": exch})

    log.info("After exchange/name/warrant pre-filter: %d", len(pre))

    # Second pass — SIC filter. Bulk-fetch (cached 1 week in
    # get_company_details_bulk) so this is cheap on subsequent runs.
    ciks = [r["cik"] for r in pre]
    details = get_company_details_bulk(ciks)
    out: list[dict] = []
    for row in pre:
        d = details.get(row["cik"]) or {}
        sic = d.get("sic")
        if _is_excluded_sic(sic):
            continue
        row["sic"] = sic
        out.append(row)
    log.info("After SIC filter: %d universe", len(out))
    return out


def _cagr(start: float, end: float, years: float) -> float | None:
    """CAGR % = (end/start)^(1/years) - 1, ×100.

    Requires both endpoints positive. Lynch's screen is about *growing*
    earnings, so a swing through zero (negative → positive or vice
    versa) doesn't fit — we return None and the row is filtered out."""
    if start is None or end is None or years <= 0:
        return None
    if start <= 0 or end <= 0:
        return None
    return ((end / start) ** (1.0 / years) - 1.0) * 100.0


def _screen_one(row: dict, price: float, facts: dict) -> dict | None:
    """Apply Lynch criteria to one company. Returns a result dict if
    it passes every filter; None otherwise. Returned dict is what
    ends up in the snapshot JSON."""
    shares = _latest_shares_outstanding(facts)
    if not shares or shares <= 0:
        return None
    market_cap = price * shares
    if market_cap < LYNCH_RULES["market_cap_min"]:
        return None

    eps_history = _annual_eps_history(facts)
    if len(eps_history) < LYNCH_RULES["min_eps_years"] + 1:
        return None  # need start + end => at least 4 years

    # TTM EPS = most recent full FY (good enough for a screening filter;
    # using stitched quarterly would be more accurate but adds complexity).
    ttm_eps = eps_history[-1][1]
    if ttm_eps is None or ttm_eps <= 0:
        return None  # Lynch wants positive earnings

    pe = price / ttm_eps
    if pe <= 0 or pe > LYNCH_RULES["pe_max"]:
        return None

    # 3-year EPS CAGR — endpoints are 4 fiscal years apart.
    start_eps = eps_history[-4][1] if len(eps_history) >= 4 else eps_history[0][1]
    eps_growth = _cagr(start_eps, ttm_eps, 3.0)
    if eps_growth is None or eps_growth < LYNCH_RULES["eps_growth_3yr_min_pct"]:
        return None

    bs = _latest_balance_sheet(facts)
    equity = bs["equity"]
    if equity is None or equity <= 0:
        return None
    total_debt = (bs["short_term_debt"] or 0.0) + (bs["long_term_debt"] or 0.0)
    debt_to_equity = total_debt / equity
    if debt_to_equity > LYNCH_RULES["debt_to_equity_max"]:
        return None

    capex, ocf = _ttm_capex_and_ocf(facts)
    capex_to_ocf = None
    if capex is not None and ocf is not None and ocf > 0:
        capex_to_ocf = capex / ocf
        if capex_to_ocf > LYNCH_RULES["capex_to_ocf_max"]:
            return None
    elif capex is None or ocf is None:
        # Missing cash-flow data → can't verify low-capex criterion.
        # Skip rather than admit a row we can't validate.
        return None

    peg = pe / eps_growth if eps_growth > 0 else None

    return {
        "ticker": row["ticker"],
        "name": row["name"],
        "exchange": row.get("exchange") or "",
        "sic": row.get("sic"),
        "price": round(price, 2),
        "shares_outstanding": int(shares),
        "market_cap": int(market_cap),
        "ttm_eps": round(ttm_eps, 2),
        "pe_ratio": round(pe, 2),
        "eps_3yr_cagr_pct": round(eps_growth, 1),
        "peg": round(peg, 2) if peg is not None else None,
        "debt_to_equity": round(debt_to_equity, 2),
        "total_debt": int(total_debt),
        "equity": int(equity),
        "ttm_capex": int(capex) if capex is not None else None,
        "ttm_ocf": int(ocf) if ocf is not None else None,
        "capex_to_ocf": round(capex_to_ocf, 2) if capex_to_ocf is not None else None,
        "eps_history": [{"fy_end": d, "eps": round(v, 2)} for d, v in eps_history[-5:]],
    }


def build_lynch_screener(max_companyfacts: int | None = None) -> list[dict]:
    """Full pipeline. Returns list of passing companies sorted by PEG asc.

    ``max_companyfacts`` caps how many companies we deep-pull from
    SEC after the price/shares prescreen — useful for testing or for
    a fast dry-run; None = no cap.
    """
    universe = _build_universe()
    tickers = [u["ticker"] for u in universe]
    prices = fetch_prices_bulk(tickers)
    log.info("Prices: %d/%d tickers priced", len(prices), len(tickers))

    # Companyfacts is the expensive call (one HTTP per CIK). Trim
    # universe to companies with a price first; that drops delisteds,
    # OTC-only, etc.
    priced = [u for u in universe if u["ticker"] in prices]
    if max_companyfacts:
        priced = priced[:max_companyfacts]
    log.info("Deep-pulling companyfacts for %d companies …", len(priced))

    results: list[dict] = []
    last_log = time.time()
    for i, row in enumerate(priced, 1):
        # Rate-limit: SEC allows 10 req/sec.
        if i % 10 == 0:
            time.sleep(1.1)
        # Per-CIK cache so re-running the script during testing doesn't
        # re-hit SEC for facts we just pulled.
        cache_key = f"lynch_facts_{row['cik']}"
        facts = _rc(cache_key, 168)  # 1 week — fundamentals change slowly
        if facts is None:
            facts = _fetch_companyfacts(row["cik"]) or {}
            _wc(cache_key, facts)
        try:
            hit = _screen_one(row, prices[row["ticker"]], facts)
        except Exception as e:  # pragma: no cover — defensive
            log.warning("Lynch screen error for %s: %s", row["ticker"], e)
            hit = None
        if hit:
            results.append(hit)
        if time.time() - last_log > 30:
            log.info("  facts: %d/%d done (%d passing)", i, len(priced), len(results))
            last_log = time.time()

    # Sort by PEG ascending (best Lynch values first); fall back to
    # P/E if PEG is missing.
    results.sort(key=lambda r: (r.get("peg") if r.get("peg") is not None else r["pe_ratio"]))
    log.info("Lynch screener: %d companies passing all filters", len(results))
    return results
