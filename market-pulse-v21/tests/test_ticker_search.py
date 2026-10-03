"""Ticker search and the light price lookup (stock_lookup.search_tickers,
get_price): each source's response parsed from a recorded shape, the order
the sources are tried in, the SEC list as the floor, input cleaning, and the
routes. Offline — Yahoo, Nasdaq and sec.gov do not answer the dev sandbox.

Run: python tests/test_ticker_search.py
"""
from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import stock_lookup as S  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


TMP = Path(tempfile.mkdtemp())
S.CACHE = TMP                      # a clean cache for every run

YAHOO = {"quotes": [
    {"symbol": "VXUS", "shortname": "Vanguard Total Intl Stock ETF", "longname": "Vanguard Total International Stock ETF",
     "quoteType": "ETF", "exchange": "PCX", "exchDisp": "NYSEArca"},
    {"symbol": "VXUS.MX", "longname": "Vanguard Total International Stock ETF", "quoteType": "ETF", "exchDisp": "Mexico"},
    {"symbol": "^VXUSIV", "shortname": "index", "quoteType": "INDEX"},
    {"symbol": "BTC-USD", "shortname": "Bitcoin", "quoteType": "CRYPTOCURRENCY"},
    {"symbol": "BRK-B", "longname": "Berkshire Hathaway Inc.", "quoteType": "EQUITY", "exchDisp": "NYSE"},
    {"symbol": "FXAIX", "longname": "Fidelity 500 Index Fund", "quoteType": "MUTUALFUND", "exchDisp": "Nasdaq"},
    "junk",
], "news": [{"title": "not a quote"}]}
NASDAQ = {"data": [
    {"symbol": "VXUS", "name": "Vanguard Total International Stock ETF", "exchange": "NYSEARCA", "asset": "ETF"},
    {"symbol": "VXUSX", "name": "Some fund", "asset": "MUTUALFUNDS"},
    {"symbol": "AAPL", "name": "Apple Inc.", "asset": "STOCKS"},
    {"symbol": "SPX", "name": "S&P 500 Index", "asset": "INDEX"},
]}

# ── parsing each source ────────────────────────────────────────────
y = S.parse_yahoo_search(YAHOO)
check([r["symbol"] for r in y] == ["VXUS", "BRK-B", "FXAIX"],
      "Yahoo: US-listed stocks, ETFs and mutual funds only — no foreign line, index or crypto")
check(y[0] == {"symbol": "VXUS", "name": "Vanguard Total International Stock ETF", "type": "ETF", "exchange": "NYSEArca"},
      "the long name, the type in words, the exchange as people say it")
check(y[2]["type"] == "Mutual fund" and S.parse_yahoo_search(None) == [] and S.parse_yahoo_search({"quotes": None}) == [],
      "a mutual fund is a mutual fund; an empty answer is no rows, not a crash")
n = S.parse_nasdaq_lookup(NASDAQ)
check([(r["symbol"], r["type"]) for r in n] == [("VXUS", "ETF"), ("VXUSX", "Mutual fund"), ("AAPL", "Stock")],
      "Nasdaq: its asset classes in the same three words; an index is not a holding")

SEC = {"1": {"ticker": "KR", "name": "KROGER CO"}, "2": {"ticker": "KW", "name": "Kennedy-Wilson Holdings"},
       "3": {"ticker": "KWR", "name": "Quaker Houghton"}, "4": {"ticker": "LEN", "name": "LENNAR CORP"},
       "5": {"ticker": "BAD TICKER", "name": "x"}}
check([r["symbol"] for r in S.sec_lookup("kw", SEC)] == ["KW", "KWR"] and S.sec_lookup("lennar", SEC)[0]["symbol"] == "LEN",
      "the SEC list: tickers that start with the text (shortest first), then names that contain it")
check(S.sec_lookup("lennar", SEC)[0]["name"] == "Lennar Corp", "and a shouted name is set in title case")

check([r["symbol"] for r in S.rank_results([{"symbol": "VXUSX"}, {"symbol": "AVXUS"}, {"symbol": "VXUS"}, {"symbol": "VXUS"}], "vxus")]
      == ["VXUS", "VXUSX", "AVXUS"], "the exact ticker first, then those that start with it; one row per ticker")

# ── input cleaning ─────────────────────────────────────────────────
check(S.clean_query("  vxus  ") == "vxus" and S.clean_query("Berkshire  Hathaway") == "Berkshire Hathaway"
      and S.clean_query("") is None and S.clean_query("<script>") is None and S.clean_query("x" * 41) is None,
      "the search text is trimmed; anything a name or ticker does not need is refused before any request")

# ── the order the sources are tried in ─────────────────────────────
calls = []


def fake(answers):
    def f(url, headers=None, timeout=None):
        calls.append(url)
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a
    return f


r = S.search_tickers("vxus", fetch=fake([YAHOO]), sec_tickers={})
check(r["source"] == "yahoo" and r["results"][0]["symbol"] == "VXUS" and "search?q=vxus" in calls[0],
      "Yahoo answers first")
check("market-pulse" in S.SEARCH_SOURCES[0]["headers"]["User-Agent"],
      "with the plain named agent Yahoo answers from cloud IPs (a browser string gets 429)")
calls.clear()
r2 = S.search_tickers("vxus", fetch=fake([]), sec_tickers={})
check(r2 == r and not calls, "a repeat search is answered from the day's cache, no request")
calls.clear()
r3 = S.search_tickers("apple", fetch=fake([RuntimeError("429"), NASDAQ]), sec_tickers={})
check(r3["source"] == "nasdaq" and len(calls) == 2 and "nasdaq.com" in calls[1],
      "YAHOO BLOCKED: Nasdaq answers instead, and the page still gets results")
calls.clear()
r4 = S.search_tickers("lennar", fetch=fake([{"quotes": []}, {"data": []}]), sec_tickers=SEC)
check(r4["source"] == "sec" and r4["results"][0]["symbol"] == "LEN", "neither finds it: the SEC's list is the floor")
r5 = S.search_tickers("zzzz", fetch=fake([RuntimeError("x"), RuntimeError("y")]), sec_tickers={})
check(r5 == {"q": "zzzz", "results": [], "source": None} and not (TMP / "search_zzzz.json").exists(),
      "nothing anywhere: an honest empty answer, not cached (the next try may reach a source)")
check(S.search_tickers("<x>")["results"] == [], "a refused query asks nobody")

# ── the price ──────────────────────────────────────────────────────
q_ok = lambda t, period: {"symbol": t, "name": "Vanguard Total International Stock ETF", "price": 34.12,  # noqa: E731
                          "day_change": -0.27, "day_change_pct": -0.79, "history": [[1, 2]] * 5}
p = S.get_price("vxus", quote=q_ok)
check(p == {"symbol": "VXUS", "name": "Vanguard Total International Stock ETF", "price": 34.12, "day_change": -0.27,
            "day_change_pct": -0.79, "source": "yahoo"}, "the latest price from Yahoo, without the year of history")
seen = []
S.get_price("VTI", quote=lambda t, period: seen.append(period) or {"price": 1.0})
check(seen == ["5d"], "it asks Yahoo for five days, not a year")
NQ = {"data": {"symbol": "VXUS", "companyName": "Vanguard Total International Stock ETF", "primaryData": {
    "lastSalePrice": "$34.12", "netChange": "-0.27", "percentageChange": "-0.79%", "lastTradeTimestamp": "Oct 3, 2026"}}}
calls.clear()
p2 = S.get_price("VXUS", quote=lambda t, period: {"error": "blocked"}, fetch=fake([RuntimeError("no stock"), NQ]))
check(p2["price"] == 34.12 and p2["day_change_pct"] == -0.79 and p2["source"] == "nasdaq"
      and "assetclass=stocks" in calls[0] and "assetclass=etf" in calls[1],
      "YAHOO DOWN: Nasdaq's quote, tried as a stock, then as an ETF")
check(S.parse_nasdaq_quote({"data": {"primaryData": {"lastSalePrice": "N/A"}}}) is None, "no price is no price")
check("error" in S.get_price("VXUS", quote=lambda t, period: {"error": "x"}, fetch=fake([RuntimeError()] * 3)),
      "nowhere to ask: it says so")
check("error" in S.get_price("not a ticker!") and "error" in S.get_price("0700.HK"),
      "only US tickers are looked up")

# ── the routes ─────────────────────────────────────────────────────
import main  # noqa: E402

saved = (S.search_tickers, S.get_price)
S.search_tickers = lambda q: {"q": q, "results": [{"symbol": "VXUS"}], "source": "yahoo"}
S.get_price = lambda t: {"symbol": t, "price": 1.0}
try:
    rs = asyncio.run(main.api_stock_search(q="vxus"))
    rp = asyncio.run(main.api_stock_price("VXUS"))
    check(rs.status_code == 200 and b'"VXUS"' in rs.body and rp.status_code == 200 and b'"price"' in rp.body,
          "GET /api/stock/search and /api/stock/{ticker}/price answer JSON")
finally:
    S.search_tickers, S.get_price = saved

html = open(os.path.join(ROOT, "templates", "capital.html")).read()
check("data-ticker-search" in html and "/api/stock/search?q=" in html and "/price'" in html,
      "the page's ticker boxes use the search and the price")
shutil.rmtree(TMP, ignore_errors=True)

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} ticker-search checks passed.")
