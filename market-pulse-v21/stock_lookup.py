"""
Lightweight stock lookup — Yahoo Finance for live quote/history, SEC EDGAR
for fundamentals. No API keys, no extra dependencies.

Yahoo Finance: hits the public /v8/finance/chart endpoint via urllib. This
is the same endpoint yfinance uses internally; it's stable, free, and
returns both meta (current price, 52-wk range, market cap) and a daily
price series we can chart.

SEC EDGAR: reuses the existing sec_edgar.get_tickers() map to resolve
ticker -> CIK, then fetches /api/xbrl/companyfacts and pulls the most
recent annual (10-K) value for each fundamental concept.
"""
import json
import logging
import time
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

UA = "MarketPulse/1.0 (+invoice@archfms.com)"
CACHE = Path("/tmp/market_pulse_cache")
CACHE.mkdir(exist_ok=True)


def _cache_path(key: str) -> Path:
    return CACHE / f"{key}.json"


def _read_cache(key: str, max_age_sec: int):
    p = _cache_path(key)
    if p.exists() and time.time() - p.stat().st_mtime < max_age_sec:
        try:
            return json.loads(p.read_text())
        except Exception:
            return None
    return None


def _write_cache(key: str, data):
    try:
        _cache_path(key).write_text(json.dumps(data))
    except Exception as e:
        logger.warning(f"cache write {key}: {e}")


def _http_json(url: str, headers: dict | None = None, timeout: int = 15):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


# ─── Yahoo Finance: live quote + price history ───────────────────────

def get_quote(ticker: str, period: str = "1y") -> dict:
    """Live quote + daily price history.

    Returns a dict with: symbol, name, currency, exchange, price, prev_close,
    day_change, day_change_pct, fifty_two_week_high/low, market_cap,
    regular_market_volume, history (list of [ts_ms, close]).
    """
    t = (ticker or "").strip().upper()
    if not t:
        return {"error": "No ticker provided."}

    # 5-minute cache. Intraday quotes drift but we don't need tick-by-tick.
    key = f"yf_quote_{t}_{period}"
    cached = _read_cache(key, max_age_sec=300)
    if cached:
        return cached

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(t)}"
        f"?range={period}&interval=1d&includePrePost=false"
    )
    try:
        data = _http_json(url)
    except Exception as e:
        logger.warning(f"Yahoo quote {t}: {e}")
        return {"error": f"Could not fetch quote for {t}."}

    chart = (data or {}).get("chart") or {}
    err = chart.get("error")
    if err:
        return {"error": f"{t}: {(err or {}).get('description') or 'No data.'}"}

    results = chart.get("result") or []
    if not results:
        return {"error": f"No data returned for {t}."}

    r = results[0]
    meta = r.get("meta") or {}
    timestamps = r.get("timestamp") or []
    indicators = ((r.get("indicators") or {}).get("quote") or [{}])[0]
    closes = indicators.get("close") or []

    # Yahoo occasionally emits null closes (weekends, halts). Filter them out.
    history = [[ts * 1000, round(float(c), 4)] for ts, c in zip(timestamps, closes) if c is not None]

    price = meta.get("regularMarketPrice")
    prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
    day_change = (price - prev_close) if (price is not None and prev_close) else None
    day_change_pct = (day_change / prev_close * 100) if (day_change is not None and prev_close) else None

    out = {
        "symbol": meta.get("symbol") or t,
        "name": meta.get("longName") or meta.get("shortName") or t,
        "currency": meta.get("currency"),
        "exchange": meta.get("exchangeName") or meta.get("fullExchangeName"),
        "price": price,
        "prev_close": prev_close,
        "day_change": day_change,
        "day_change_pct": day_change_pct,
        "fifty_two_week_high": meta.get("fiftyTwoWeekHigh"),
        "fifty_two_week_low": meta.get("fiftyTwoWeekLow"),
        "market_cap": meta.get("marketCap"),
        "regular_market_volume": meta.get("regularMarketVolume"),
        "history": history,
    }
    _write_cache(key, out)
    return out


# ─── SEC EDGAR: latest annual fundamentals ───────────────────────────

# (concept, friendly label) — order = display order in the UI.
FUNDAMENTAL_CONCEPTS = [
    ("Revenues",                                              "Revenue"),
    ("RevenueFromContractWithCustomerExcludingAssessedTax",   "Revenue"),
    ("NetIncomeLoss",                                         "Net Income"),
    ("EarningsPerShareBasic",                                 "EPS (basic)"),
    ("EarningsPerShareDiluted",                               "EPS (diluted)"),
    ("Assets",                                                "Total Assets"),
    ("Liabilities",                                           "Total Liabilities"),
    ("StockholdersEquity",                                    "Stockholders Equity"),
    ("CashAndCashEquivalentsAtCarryingValue",                 "Cash & Equivalents"),
    ("CommonStockSharesOutstanding",                          "Shares Outstanding"),
]


def _ticker_to_cik(ticker: str) -> str | None:
    """Resolve ticker -> CIK using the SEC's company_tickers map (cached
    by sec_edgar.get_tickers)."""
    from sec_edgar import get_tickers
    try:
        tickers = get_tickers() or {}
    except Exception as e:
        logger.warning(f"get_tickers: {e}")
        return None
    t = (ticker or "").strip().upper()
    for cik, info in tickers.items():
        if (info.get("ticker") or "").upper() == t:
            return str(cik)
    return None


def _sec_get(url: str, timeout: int = 30):
    from sec_edgar import SEC_UA
    req = urllib.request.Request(url, headers={"User-Agent": SEC_UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _latest_annual(unit_entries: list) -> dict | None:
    """Pick the latest 10-K (FY) entry; fall back to most recent by end date."""
    fy = [e for e in (unit_entries or []) if e.get("fp") == "FY" and e.get("form") in ("10-K", "10-K/A")]
    if not fy:
        fy = unit_entries or []
    if not fy:
        return None
    fy.sort(key=lambda e: e.get("end") or "", reverse=True)
    return fy[0]


def get_fundamentals(ticker: str) -> dict:
    """Latest annual fundamentals from SEC EDGAR.

    Returns: { ticker, cik, name, items: [{label, concept, value, end, unit}] }
    """
    t = (ticker or "").strip().upper()
    if not t:
        return {"error": "No ticker provided."}

    key = f"sec_fundamentals_{t}"
    cached = _read_cache(key, max_age_sec=24 * 3600)  # fundamentals change slowly
    if cached:
        return cached

    cik = _ticker_to_cik(t)
    if not cik:
        return {"error": f"No SEC filer found for {t}."}

    padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json"
    try:
        facts = _sec_get(url)
    except Exception as e:
        logger.warning(f"EDGAR companyfacts {t}: {e}")
        return {"error": f"Could not fetch SEC fundamentals for {t}."}

    name = facts.get("entityName") or t
    us_gaap = ((facts.get("facts") or {}).get("us-gaap") or {})

    seen_labels: set[str] = set()
    items: list[dict] = []
    for concept, label in FUNDAMENTAL_CONCEPTS:
        if label in seen_labels:
            continue
        node = us_gaap.get(concept)
        if not node:
            continue
        units = node.get("units") or {}
        unit_key = next(iter(units.keys()), None)
        if not unit_key:
            continue
        latest = _latest_annual(units.get(unit_key) or [])
        if not latest or latest.get("val") is None:
            continue
        items.append({
            "label": label,
            "concept": concept,
            "value": latest["val"],
            "end": latest.get("end"),
            "unit": unit_key,
        })
        seen_labels.add(label)

    out = {"ticker": t, "cik": cik, "name": name, "items": items}
    _write_cache(key, out)
    return out
