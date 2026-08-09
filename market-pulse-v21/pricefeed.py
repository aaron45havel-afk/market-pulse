"""Daily price and volume, from whichever free source will still answer.

WHY THIS MODULE EXISTS.

The quiet-value screen needs one year of daily share volume per candidate.
The SEC publishes the fundamentals — cash, debt, capex, shares outstanding
— free, officially and forever. It does not publish market data, because
the exchanges own that and sell it. So the volume half of the screen rests
on a free source that nobody has promised us, and the failure mode is not
theoretical: a run on 2026-08-09 fetched 100 tickers from Yahoo's chart
endpoint and got data for ZERO of them. Yahoo rejects GitHub Actions IPs.

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


def stooq_symbol(ticker: str) -> str:
    """Stooq wants lowercase with a market suffix, and a hyphen where US
    tickers use a class dot: BRK.B is brk-b.us."""
    return ticker.strip().lower().replace(".", "-") + ".us"


def yahoo_symbol(ticker: str) -> str:
    return ticker.strip().upper()


# Ordered by expectation, not preference. Yahoo carries better micro-cap
# coverage; Stooq is a plain static CSV host and is far less inclined to
# block a datacenter IP. Which one actually answers is settled at runtime
# by the probe, never by this ordering, and the answer is recorded in the
# payload so the board can say where its numbers came from.
SOURCES = (
    {"name": "stooq",
     "url": "https://stooq.com/q/d/l/?s={s}&i=d",
     "symbol": stooq_symbol, "parse": parse_stooq_csv, "json": False,
     "note": "Free daily CSV. No key. Coverage of very small US listings is uneven."},
    {"name": "yahoo",
     "url": "https://query1.finance.yahoo.com/v8/finance/chart/{s}?range=1y&interval=1d",
     "symbol": yahoo_symbol, "parse": parse_yahoo_chart, "json": True,
     "note": "Undocumented endpoint behind the consumer site. Rejects cloud IPs with 429."},
)


def source_by_name(name: str) -> dict | None:
    for s in SOURCES:
        if s["name"] == name:
            return s
    return None
