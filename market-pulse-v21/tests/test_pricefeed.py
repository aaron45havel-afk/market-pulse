"""Price-feed parsing checks.

Run:  python tests/test_pricefeed.py      (exit 0 = all pass)

Neither Yahoo nor Stooq is reachable from the dev sandbox, and one of them
demonstrably blocks CI runners. So the parsing lives in pricefeed.py where
it can be proved against fixtures, and the fetcher is only plumbing.

The property that matters most here is the one that broke production: a
source can fail while returning HTTP 200. Stooq answers an exhausted quota
with the plain text "Exceeded the daily hits limit", and an unknown symbol
with "No data" — both as 200. If the parser accepts those, they become a
company that appears never to have traded, which is not an error on the
page. It is a finding. A false one, in exactly the direction this screen
is looking.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pricefeed as P

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── Stooq: the happy path ───────────────────────────────────────────
CSV = "\n".join(
    ["Date,Open,High,Low,Close,Volume"]
    + [f"2026-0{1 + i // 28}-{1 + i % 28:02d},10.0,10.5,9.9,{10 + i * 0.01:.2f},{1000 + i}"
       for i in range(300)]
)
got = P.parse_stooq_csv(CSV)
check(got is not None, "a well-formed Stooq CSV parses")
check(len(got["volumes"]) == P.TRADING_DAYS,
      f"300 rows trim to the trailing {P.TRADING_DAYS} (got {len(got['volumes'])})")
check(got["volumes"][-1] == 1299, f"the newest volume is last (got {got['volumes'][-1]})")
check(abs(got["price"] - 12.99) < 0.001, f"price is the last close (got {got['price']})")

short = P.parse_stooq_csv("Date,Open,High,Low,Close,Volume\n2026-01-02,1,1,1,1.25,500")
check(short["volumes"] == [500.0], "a one-row file keeps its single session")
check(short["price"] == 1.25, "and its close")

# ── Stooq failures that arrive as HTTP 200 ──────────────────────────
# This is the whole reason the parser exists as a separate testable unit.
for body, label in (("Exceeded the daily hits limit", "an exhausted quota"),
                    ("No data", "an unknown symbol"),
                    ("<html><body>503</body></html>", "an HTML error page"),
                    ("", "an empty body"),
                    ("   \n  \n", "whitespace only"),
                    ("Symbol,Something\nfoo,bar", "a CSV with the wrong columns")):
    check(P.parse_stooq_csv(body) is None,
          f"{label} parses to None, not to a company that never trades")
check(P.parse_stooq_csv(None) is None, "None input is None, not a crash")

# Column ORDER is read from the header, not assumed.
reordered = P.parse_stooq_csv("Date,Volume,Close\n2026-01-02,777,3.50")
check(reordered is not None and reordered["volumes"] == [777.0],
      "volume is located by header name, not by position")
check(reordered["price"] == 3.50, "and so is close")

# A blank or N/D volume is UNKNOWN. Coercing it to 0 would manufacture a
# no-trade session — the exact signal the dark-days column measures.
gappy = P.parse_stooq_csv("Date,Close,Volume\n2026-01-02,5,100\n2026-01-03,5,\n2026-01-04,5,N/D")
check(gappy["volumes"] == [100.0, None, None],
      f"missing volume stays None, never 0 (got {gappy['volumes']})")
check(gappy["price"] == 5.0, "a gappy file still yields a price")

# Rows too short to contain the columns are skipped, not fatal.
ragged = P.parse_stooq_csv("Date,Close,Volume\n2026-01-02,5,100\nbroken\n2026-01-03,6,200")
check(ragged["volumes"] == [100.0, 200.0], "a truncated line is skipped")
check(ragged["price"] == 6.0, "and the good rows still count")

# ── Yahoo ───────────────────────────────────────────────────────────
def _y(vols, closes=None, price=42.0):
    res = {"meta": {}, "indicators": {"quote": [{"volume": vols, "close": closes or []}]}}
    if price is not None:
        res["meta"]["regularMarketPrice"] = price
    return {"chart": {"result": [res]}}


y = P.parse_yahoo_chart(_y([100, 200, 300]))
check(y is not None and y["volumes"] == [100, 200, 300], "Yahoo volumes parse")
check(y["price"] == 42.0, "Yahoo meta price is used when present")

y2 = P.parse_yahoo_chart(_y([1, 2], closes=[9.0, None], price=None))
check(y2["price"] == 9.0,
      f"with no meta price it falls back to the last REAL close (got {y2['price']})")

y3 = P.parse_yahoo_chart(_y(list(range(400))))
check(len(y3["volumes"]) == P.TRADING_DAYS,
      "Yahoo history is trimmed to the same window as Stooq, so turnover "
      "means the same thing whichever source answered")

for payload, label in (({}, "an empty object"),
                       ({"chart": {"result": []}}, "no result"),
                       ({"chart": {"error": "Not Found"}}, "an error body"),
                       (_y([]), "an empty volume array"),
                       (_y(None), "a null volume array"),
                       (None, "None")):
    check(P.parse_yahoo_chart(payload) is None,
          f"Yahoo {label} parses to None, never to an empty series")

# ── the two sources must agree on shape ─────────────────────────────
# Downstream code takes whatever answered. If the shapes diverged, a
# source switch would change the numbers rather than just their origin.
a = P.parse_stooq_csv("Date,Close,Volume\n2026-01-02,7.5,120\n2026-01-03,7.5,130")
b = P.parse_yahoo_chart(_y([120, 130], price=7.5))
check(set(a) == set(b) == {"volumes", "price"},
      f"both sources return the same keys ({set(a)} vs {set(b)})")
check(a["volumes"] == b["volumes"] and a["price"] == b["price"],
      "the same underlying data yields the same result from either source")

# ── symbol formatting ───────────────────────────────────────────────
check(P.stooq_symbol("AAPL") == "aapl.us", "Stooq wants lowercase with a market suffix")
check(P.stooq_symbol("BRK.B") == "brk-b.us",
      f"a class dot becomes a hyphen for Stooq (got {P.stooq_symbol('BRK.B')})")
check(P.stooq_symbol("  ko ") == "ko.us", "surrounding whitespace is stripped")
check(P.yahoo_symbol("aapl") == "AAPL", "Yahoo wants uppercase")

# ── the source registry ─────────────────────────────────────────────
check(len(P.SOURCES) >= 2, "there is more than one source, because one of them blocks CI")
for s in P.SOURCES:
    check(all(k in s for k in ("name", "url", "symbol", "parse", "json", "note")),
          f"source {s.get('name')!r} is fully specified")
    check("{s}" in s["url"], f"source {s['name']} templates its symbol")
    _COUNT += 1
check(P.source_by_name("stooq") is not None, "sources are addressable by name")
check(P.source_by_name("nope") is None, "an unknown source name yields None, not a crash")
check(len(P.PROBE_TICKERS) >= 3,
      "the probe uses several tickers — one delisting must not condemn a good source")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} price-feed checks passed.")
print(f"   sources: {', '.join(s['name'] for s in P.SOURCES)} · "
      f"trailing window {P.TRADING_DAYS} sessions")
print("   HTTP-200 failures ('Exceeded the daily hits limit', 'No data', HTML) "
      "all parse to None")
sys.exit(0)
