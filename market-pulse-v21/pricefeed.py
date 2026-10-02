"""Daily price and volume, from whichever free source will still answer.

WHY THIS MODULE EXISTS.

The quiet-value screen needs one year of daily share volume per candidate.
The SEC publishes the fundamentals — cash, debt, capex, shares outstanding
— free, officially and forever. It does not publish market data, because
the exchanges own that and sell it. So the volume half of the screen rests
on a free source that nobody has promised us, and the failure mode is not
theoretical: a run on 2026-08-09 fetched 100 tickers from Yahoo's chart
endpoint and got data for ZERO of them, and every weekly run after that
failed the same way.

IT WAS THE USER AGENT, NOT THE RUNNER. A probe on 2026-10-02 from GitHub's
runners: Yahoo answered 429 to a Chrome browser string and to Python's
default agent, and 200 to a plain named agent — all 400 small filers, 395
with a full year of volume, in 37 seconds on three workers. The Compounders
build had been using that agent against the same endpoint all along. So
each source now carries its own headers, and the one that works is not
"look more like a browser".

That run took an hour to establish this, because each rejection triggered
a retry ladder that slept 92 seconds before giving up, and nothing logged
a word. An hour of total failure produced no diagnosis at all.

The lesson is not "pick a better source". It is that a source you do not
control has to be TREATED as one:

  * PARSING IS SEPARATE FROM FETCHING and lives here, where it can be
    tested offline against a fixture. sec.gov, Yahoo and Stooq are all
    unreachable from the dev sandbox; if correctness depended on the
    network we could not prove any of it.

  * EVERY SOURCE IS PROVEN BEFORE THE BUDGET IS SPENT. Three known-liquid
    tickers, up front. A dead source costs ten seconds to discover, not
    sixty minutes.

  * THERE IS MORE THAN ONE. When one blocks the runner, the run should
    move to the next and SAY SO on the page, not die and not silently
    publish a thinner board.

Returned shape is identical across sources:  {"volumes": [...], "price": float}
"""
from __future__ import annotations

# A year of US trading is ~252 sessions. Sources that carry full history
# get trimmed to the trailing window so turnover means the same thing
# whichever one answered.
TRADING_DAYS = 252

# Names liquid enough that a source returning nothing for them is broken,
# not merely thin. Deliberately boring and cross-listed nowhere exotic.
PROBE_TICKERS = ("AAPL", "KO", "MSFT")


def _f(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f and f not in (float("inf"), float("-inf")) else None


def parse_yahoo_chart(payload: dict | None) -> dict | None:
    """Yahoo's v8 chart response. One call carries price and volume both.

    Returns None rather than an empty series on a miss: downstream, an
    empty volume list reads as "never traded", which is a real and very
    different finding from "no data".
    """
    results = ((payload or {}).get("chart") or {}).get("result") or []
    if not results:
        return None
    res = results[0]
    quote = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    vols = quote.get("volume")
    if not isinstance(vols, list) or not vols:
        return None

    price = _f((res.get("meta") or {}).get("regularMarketPrice"))
    if price is None:                       # fall back to the last real close
        for c in reversed(quote.get("close") or []):
            price = _f(c)
            if price is not None:
                break
    return {"volumes": vols[-TRADING_DAYS:], "price": price}


def parse_stooq_csv(text: str | None) -> dict | None:
    """Stooq's daily CSV: Date,Open,High,Low,Close,Volume, oldest first.

    Stooq signals trouble in-band rather than with a status code — an
    exhausted quota comes back as the plain text "Exceeded the daily hits
    limit", and an unknown symbol as a one-line "No data". Both arrive as
    HTTP 200, so the parser is the only thing standing between them and a
    row that looks like a company which never trades.
    """
    if not text:
        return None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None
    header = lines[0].lower()
    if "date" not in header or "volume" not in header:
        return None                          # error text, HTML, anything else

    cols = [c.strip().lower() for c in header.split(",")]
    try:
        vi, ci = cols.index("volume"), cols.index("close")
    except ValueError:
        return None

    vols: list[float | None] = []
    last_close = None
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) <= max(vi, ci):
            continue
        # A blank or "N/D" volume is UNKNOWN, and must stay None. Coercing
        # it to 0 would manufacture a no-trade session, which is exactly
        # the signal the dark-days column exists to measure.
        vols.append(_f(parts[vi]))
        close = _f(parts[ci])
        if close is not None:
            last_close = close
    if not vols:
        return None
    return {"volumes": vols[-TRADING_DAYS:], "price": last_close}


def parse_nasdaq_history(payload: dict | None) -> dict | None:
    """Nasdaq's historical-quote API: data.tradesTable.rows, NEWEST FIRST,
    every figure a string ("$12.34", "1,234,567", "N/A"). Returned oldest
    first like the others. An unparseable volume stays None — unknown, not
    a no-trade day."""
    rows = ((((payload or {}).get("data") or {}).get("tradesTable") or {}).get("rows")) or []
    if not isinstance(rows, list) or not rows:
        return None

    def num(v):
        return _f(str(v).replace("$", "").replace(",", "").strip()) if v is not None else None
    rows = list(reversed(rows))
    vols = [num(r.get("volume")) for r in rows if isinstance(r, dict)]
    if not vols:
        return None
    price = None
    for r in reversed(rows):
        price = num((r or {}).get("close"))
        if price is not None:
            break
    return {"volumes": vols[-TRADING_DAYS:], "price": price}


def stooq_symbol(ticker: str) -> str:
    """Stooq wants lowercase with a market suffix, and a hyphen where US
    tickers use a class dot: BRK.B is brk-b.us."""
    return ticker.strip().lower().replace(".", "-") + ".us"


def yahoo_symbol(ticker: str) -> str:
    return ticker.strip().upper()


def nasdaq_symbol(ticker: str) -> str:
    return ticker.strip().upper()


# A plain, named agent. Yahoo answers it from GitHub's runners; it answers
# a browser string and Python's default agent with 429 (probe, 2026-10-02).
PLAIN_UA = "Mozilla/5.0 (market-pulse-refresh/1.0)"
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# Ordered by what answered GitHub's runners on 2026-10-02: Yahoo (400 of 400
# small filers), Nasdaq (two thirds — no OTC names), Stooq (a JavaScript
# browser check). Which one actually answers is still settled at runtime by
# the probe, and the answer is recorded in the payload so the board can say
# where its numbers came from. {fd}/{td} are the window's dates.
SOURCES = (
    {"name": "yahoo",
     "url": "https://query1.finance.yahoo.com/v8/finance/chart/{s}?range=1y&interval=1d",
     "symbol": yahoo_symbol, "parse": parse_yahoo_chart, "json": True,
     "headers": {"User-Agent": PLAIN_UA, "Accept": "application/json"},
     "note": "Undocumented endpoint behind the consumer site. Answers a plain named "
             "agent; 429s browser strings and Python's default from cloud IPs."},
    {"name": "nasdaq",
     "url": ("https://api.nasdaq.com/api/quote/{s}/historical?assetclass=stocks"
             "&fromdate={fd}&limit=400&todate={td}"),
     "symbol": nasdaq_symbol, "parse": parse_nasdaq_history, "json": True,
     "headers": {"User-Agent": BROWSER_UA, "Accept": "application/json",
                 "Accept-Language": "en-US,en;q=0.9", "Origin": "https://www.nasdaq.com",
                 "Referer": "https://www.nasdaq.com/"},
     "note": "Nasdaq's quote API. No key. Exchange-listed only — no OTC names."},
    {"name": "stooq",
     "url": "https://stooq.com/q/d/l/?s={s}&i=d",
     "symbol": stooq_symbol, "parse": parse_stooq_csv, "json": False,
     "headers": {"User-Agent": BROWSER_UA, "Accept": "text/csv, */*"},
     "note": "Free daily CSV. No key. Behind a JavaScript browser check since September 2026."},
)


def source_by_name(name: str) -> dict | None:
    for s in SOURCES:
        if s["name"] == name:
            return s
    return None


# ═══════════════════════════════════════════════════════════════════
# BULK QUOTES — the whole market in one request
# ═══════════════════════════════════════════════════════════════════
#
# THE MISTAKE THAT MADE FREE PRICES LOOK IMPOSSIBLE was asking one
# endpoint per company. The compounders build issues ~2,000 Yahoo chart
# calls and gets rate-limited; the quiet-value build did the same and
# died. From that we concluded free market data was gone.
#
# It is not: the requirement is 5,673 numbers ONCE A MONTH, and rate
# limits are about requests, not rows. Every source below returns the
# entire US market in a single response, or in a few tens of batched
# ones. That is a completely different ask from 5,673 quotes.
#
# MARKET CAP IS PREFERABLE TO PRICE HERE, which is the second half of the
# realisation. Price/tangible book is market cap over tangible book, and
# computing it that way never touches a share count — a figure this build
# is missing for 962 of 5,673 companies and had catastrophically wrong for
# Universe Pharmaceuticals (563,338 shares against $55.8bn of equity).
# Market cap also gives an independent sanity check on the balance sheet:
# a company the market prices at $20m cannot own $55bn.

def _money(v) -> float | None:
    """Parse the several ways these feeds write a number.

    "$213.25", "1,234.00", "3186971000000.00", 213.25, "N/A", "", "--".
    """
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return _f(v)
    if not isinstance(v, str):
        return None
    t = v.strip().replace("$", "").replace(",", "").replace("%", "")
    if not t or t.upper() in ("N/A", "NA", "--", "-", "UNCH"):
        return None
    mult = 1.0
    if t and t[-1] in "KMBT":
        mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[t[-1]]
        t = t[:-1]
    try:
        return float(t) * mult
    except ValueError:
        return None


def _row(ticker, price, cap) -> tuple[str, dict] | None:
    t = (ticker or "").strip().upper()
    p, c = _money(price), _money(cap)
    if not t or (p is None and c is None):
        return None
    if p is not None and p <= 0:
        p = None
    if c is not None and c <= 0:
        c = None
    if p is None and c is None:
        return None
    return t, {"price": p, "market_cap": c}


def parse_nasdaq_screener(payload: dict | None) -> dict:
    """{TICKER: {price, market_cap}} from Nasdaq's own screener download.

    One request covers every company listed on Nasdaq, NYSE and AMEX —
    which is the universe this screen already works from, since it keys on
    company_tickers_exchange.json.
    """
    rows = (((payload or {}).get("data") or {}).get("table") or {}).get("rows")
    if rows is None:
        rows = ((payload or {}).get("data") or {}).get("rows") or []
    out = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        got = _row(r.get("symbol"), r.get("lastsale"), r.get("marketCap"))
        if got:
            out[got[0]] = got[1]
    return out


def parse_stockanalysis(payload: dict | None) -> dict:
    """{TICKER: {price, market_cap}} from stockanalysis.com's screener."""
    data = (payload or {}).get("data")
    rows = data.get("data") if isinstance(data, dict) else data
    out = {}
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        got = _row(r.get("s") or r.get("symbol"),
                   r.get("price"), r.get("marketCap"))
        if got:
            out[got[0]] = got[1]
    return out


def parse_yahoo_quotes(payload: dict | None) -> dict:
    """{TICKER: {price, market_cap}} from Yahoo's BATCHED quote endpoint.

    Up to ~200 symbols per call, so the whole board is ~30 requests rather
    than 5,673. The per-symbol chart endpoint is what gets these runners
    blocked; this is a different order of magnitude of politeness.
    """
    res = ((payload or {}).get("quoteResponse") or {}).get("result")
    if res is None:
        res = (payload or {}).get("result") or []
    out = {}
    for r in res or []:
        if not isinstance(r, dict):
            continue
        got = _row(r.get("symbol"), r.get("regularMarketPrice"),
                   r.get("marketCap"))
        if got:
            out[got[0]] = got[1]
    return out


# Tried in order. Each is free, needs no key, and returns the market in
# one shot (Yahoo in ~30 batches). A source that answers with too little
# of the board is treated as a failure rather than a partial success.
BULK_SOURCES = (
    {"name": "nasdaq",
     "url": ("https://api.nasdaq.com/api/screener/stocks"
             "?tableonly=true&limit=25000&download=true"),
     "parse": parse_nasdaq_screener, "batched": False},
    {"name": "stockanalysis",
     "url": ("https://stockanalysis.com/api/screener/s/f"
             "?m=marketCap&s=desc&c=s,n,price,marketCap&cn=10000&i=stocks"),
     "parse": parse_stockanalysis, "batched": False},
    {"name": "yahoo-batch",
     "url": "https://query1.finance.yahoo.com/v7/finance/quote?symbols={syms}",
     "parse": parse_yahoo_quotes, "batched": True, "batch_size": 200},
)

# Below this share of the board carrying a quote, publish nothing rather
# than a board where the discount column is mostly blank and the net-net
# census reads low because we could not look.
BULK_COVERAGE_MIN = 0.50
