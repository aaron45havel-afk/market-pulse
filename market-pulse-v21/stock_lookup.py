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


# ─── Ticker search: Yahoo, then Nasdaq, then the SEC's own list ─────
#
# Typing "vxus" should find Vanguard Total International Stock ETF. The SEC's
# company list knows operating companies but not fund names, so the first
# two sources are market sites; the SEC list is the floor that still
# answers when both block a cloud IP. Parsing is separate from fetching
# (pricefeed.py's lesson): none of these hosts answers the dev sandbox, so
# correctness is proven offline against recorded responses.

import re  # noqa: E402

SEARCH_TYPES = {"EQUITY": "Stock", "ETF": "ETF", "MUTUALFUND": "Mutual fund"}
# US listings: AAPL, BRK.B / BRK-B, FXAIX. Foreign lines (0700.HK, SAP.DE),
# indices (^GSPC), futures (ES=F) and crypto (BTC-USD) are not holdings here.
_US_SYMBOL = re.compile(r"^[A-Z]{1,5}(?:[.\-][A-Z]{1,2})?$")
_QUERY_OK = re.compile(r"^[A-Za-z0-9 .&'\-]{1,40}$")
SEARCH_LIMIT = 8
# The agent Yahoo answers from cloud IPs (pricefeed.PLAIN_UA, probed 2026-10-02).
_YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (market-pulse-refresh/1.0)", "Accept": "application/json"}
_NASDAQ_HEADERS = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
                   "Accept": "application/json", "Accept-Language": "en-US,en;q=0.9",
                   "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"}


def clean_query(q) -> str | None:
    """The search text, or None when it is empty or holds characters no
    ticker or fund name needs."""
    s = " ".join(str(q or "").split())
    return s if s and _QUERY_OK.match(s) else None


def parse_yahoo_search(payload) -> list[dict]:
    """Yahoo's /v1/finance/search → [{symbol, name, type, exchange}]: US-listed
    stocks, ETFs and mutual funds only."""
    out = []
    for r in (payload or {}).get("quotes") or []:
        if not isinstance(r, dict):
            continue
        sym = str(r.get("symbol") or "").strip().upper()
        typ = SEARCH_TYPES.get(str(r.get("quoteType") or "").upper())
        # Yahoo writes a US share class with a dash (BRK-B); a dot is always
        # another market's suffix (SAP.DE, 0700.HK, VOD.L).
        if not typ or "." in sym or not _US_SYMBOL.match(sym):
            continue
        out.append({"symbol": sym, "name": r.get("longname") or r.get("shortname") or sym, "type": typ,
                    "exchange": r.get("exchDisp") or r.get("exchange") or ""})
    return out


def _nasdaq_type(asset: str) -> str | None:
    a = (asset or "").upper()
    if "ETF" in a:
        return "ETF"
    if "MUTUAL" in a or "FUND" in a:
        return "Mutual fund"
    if a in ("STOCKS", "STOCK", "EQUITY", "COMMON STOCK"):
        return "Stock"
    return None


def parse_nasdaq_lookup(payload) -> list[dict]:
    """Nasdaq's /api/autocomplete/slookup → [{symbol, name, type, exchange}]."""
    out = []
    for r in (payload or {}).get("data") or []:
        if not isinstance(r, dict):
            continue
        sym = str(r.get("symbol") or "").strip().upper()
        typ = _nasdaq_type(r.get("asset") or r.get("subCategory") or "")
        if not typ or not _US_SYMBOL.match(sym):
            continue
        out.append({"symbol": sym, "name": r.get("name") or sym, "type": typ, "exchange": r.get("exchange") or ""})
    return out


def sec_lookup(q: str, tickers: dict) -> list[dict]:
    """The SEC's company list: a ticker that starts with the text, then a
    company whose name contains it. Operating companies only — the SEC list
    does not name funds."""
    qu, ql = q.upper(), q.lower()
    starts, named = [], []
    for info in (tickers or {}).values():
        t, n = str(info.get("ticker") or "").upper(), str(info.get("name") or "")
        if not _US_SYMBOL.match(t):
            continue
        row = {"symbol": t, "name": n.title() if n.isupper() else n, "type": "Stock", "exchange": ""}
        if t.startswith(qu):
            starts.append(row)
        elif ql in n.lower():
            named.append(row)
    starts.sort(key=lambda r: (len(r["symbol"]), r["symbol"]))
    return (starts + named)[:SEARCH_LIMIT]


def rank_results(results: list[dict], q: str) -> list[dict]:
    """The exact ticker first, then tickers that start with the text, then the
    rest as the source ordered them; one row per ticker."""
    qu = q.upper().replace(" ", "")
    seen, rows = set(), []
    for i, r in enumerate(results):
        if r["symbol"] in seen:
            continue
        seen.add(r["symbol"])
        rows.append((0 if r["symbol"] == qu else 1 if r["symbol"].startswith(qu) else 2, i, r))
    return [r for _, _, r in sorted(rows, key=lambda x: (x[0], x[1]))][:SEARCH_LIMIT]


SEARCH_SOURCES = (
    {"name": "yahoo", "headers": _YAHOO_HEADERS, "parse": parse_yahoo_search,
     "url": "https://query1.finance.yahoo.com/v1/finance/search?q={q}&quotesCount=10&newsCount=0&listsCount=0"},
    {"name": "nasdaq", "headers": _NASDAQ_HEADERS, "parse": parse_nasdaq_lookup,
     "url": "https://api.nasdaq.com/api/autocomplete/slookup/10?search={q}"},
)


def search_tickers(q, fetch=None, sec_tickers=None) -> dict:
    """{q, results: [{symbol, name, type, exchange}], source}. Each source is
    tried in turn; one that fails or finds nothing hands over to the next;
    the SEC's list is last. Answers are cached for a day."""
    qq = clean_query(q)
    if not qq:
        return {"q": str(q or "")[:40], "results": [], "source": None}
    key = "search_" + re.sub(r"[^a-z0-9]+", "_", qq.lower())
    cached = _read_cache(key, 86_400)
    if cached:
        return cached
    fetch = fetch or _http_json
    for src in SEARCH_SOURCES:
        try:
            payload = fetch(src["url"].format(q=urllib.parse.quote(qq)), headers=src["headers"], timeout=8)
        except Exception as e:  # noqa: BLE001 — the next source answers instead
            logger.info(f"ticker search {src['name']} {qq!r}: {e}")
            continue
        res = src["parse"](payload)
        if res:
            out = {"q": qq, "results": rank_results(res, qq), "source": src["name"]}
            _write_cache(key, out)
            return out
    if sec_tickers is None:
        try:
            from sec_edgar import get_tickers
            sec_tickers = get_tickers() or {}
        except Exception as e:  # noqa: BLE001
            logger.info(f"ticker search sec: {e}")
            sec_tickers = {}
    res = sec_lookup(qq, sec_tickers)
    out = {"q": qq, "results": rank_results(res, qq), "source": "sec" if res else None}
    if res:
        _write_cache(key, out)
    return out


def parse_nasdaq_quote(payload) -> dict | None:
    """Nasdaq's /api/quote/{s}/info → {name, price, day_change, day_change_pct}."""
    d = (payload or {}).get("data") or {}
    pd = d.get("primaryData") or {}

    def num(v):
        try:
            return float(str(v).replace("$", "").replace(",", "").replace("%", "").replace("+", "").strip())
        except (TypeError, ValueError):
            return None
    price = num(pd.get("lastSalePrice"))
    if price is None or price <= 0:
        return None
    return {"name": d.get("companyName") or d.get("symbol"), "price": price, "day_change": num(pd.get("netChange")),
            "day_change_pct": num(pd.get("percentageChange")), "as_of": pd.get("lastTradeTimestamp")}


def get_price(ticker: str, fetch=None, quote=None) -> dict:
    """The latest price for one US ticker: Yahoo's chart (five days, not a
    year), else Nasdaq's quote as a stock, an ETF or a mutual fund."""
    t = (ticker or "").strip().upper()
    if not _US_SYMBOL.match(t):
        return {"error": "That is not a US ticker."}
    q = (quote or get_quote)(t, "5d")
    if q.get("price") is not None:
        return {"symbol": q.get("symbol") or t, "name": q.get("name") or t, "price": q["price"],
                "day_change": q.get("day_change"), "day_change_pct": q.get("day_change_pct"), "source": "yahoo"}
    fetch = fetch or _http_json
    for cls in ("stocks", "etf", "mutualfunds"):
        try:
            got = parse_nasdaq_quote(fetch(f"https://api.nasdaq.com/api/quote/{urllib.parse.quote(t)}/info?assetclass={cls}",
                                           headers=_NASDAQ_HEADERS, timeout=8))
        except Exception as e:  # noqa: BLE001
            logger.info(f"nasdaq price {t} {cls}: {e}")
            continue
        if got:
            return {"symbol": t, **got, "source": "nasdaq"}
    return {"error": f"No price for {t} right now."}
