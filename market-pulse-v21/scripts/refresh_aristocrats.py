"""Monthly refresh of dividend-aristocrat valuation data for /aristocrats.

For every ticker in aristocrats.UNIVERSE, pulls from Yahoo Finance's
chart API (no key needed):
  • 6 years of monthly closes + dividend events  → current TTM yield,
    the stock's own 5-yr median trailing yield (the value anchor), and
    the 5-yr dividend CAGR (the Chowder growth term)
  • 1 year of daily closes                       → 52-week high/low

Writes data/aristocrats.json:

    {
      "as_of": "2026-07-14",
      "fetched_at": "2026-07-14T13:30Z",
      "count": 88,
      "tickers": {
        "PEP": {"y": 4.11, "median_y5": 2.86, "dg5": 6.0, "price": 138.4,
                 "pct_off_52wk_high": 21.3, "pct_above_52wk_low": 2.1},
        ...
      }
    }

aristocrats._merged_universe() overlays these per-field at request
time; payout/debt gates keep their hand-seeded values (Yahoo's chart
API doesn't carry them, and they move slowly).

Failure model: per-ticker failures are logged and skipped — partial
data is still useful and still gets committed. The run only exits
non-zero when < 30% of the universe succeeded (Yahoo layout change or
a block), in which case the workflow doesn't commit and the app keeps
serving the previous overlay.

Cadence: 10th of each month (see refresh-aristocrats.yml) + manual
workflow_dispatch for the first population run.
"""
from __future__ import annotations

import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from aristocrats import UNIVERSE  # noqa: E402

CHART_URL = ("https://query1.finance.yahoo.com/v8/finance/chart/"
             "{t}?range={rng}&interval={iv}{events}")
HEADERS = {"User-Agent": "Mozilla/5.0 (market-pulse-refresh/1.0)",
           "Accept": "application/json"}
SLEEP_BETWEEN = 0.35          # polite pacing: ~2 req/ticker, ~90 tickers
MIN_SUCCESS_FRACTION = 0.30


def _fetch(url: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _chart_result(payload: dict) -> dict | None:
    try:
        res = payload["chart"]["result"][0]
    except (KeyError, IndexError, TypeError):
        return None
    return res


def _ttm_dividends(divs: list[tuple[int, float]], at_ts: int) -> float:
    """Sum of dividends in the 365 days ending at at_ts."""
    year = 365 * 24 * 3600
    return sum(amt for ts, amt in divs if at_ts - year < ts <= at_ts)


def fetch_ticker(ticker: str) -> dict | None:
    # Yahoo uses '-' for share classes (BF-B) — universe already stores
    # tickers in Yahoo form.
    monthly = _chart_result(_fetch(CHART_URL.format(
        t=ticker, rng="7y", iv="1mo", events="&events=div")))
    if not monthly:
        return None
    ts = monthly.get("timestamp") or []
    closes = (monthly.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    div_events = ((monthly.get("events") or {}).get("dividends") or {})
    divs = sorted((int(d["date"]), float(d["amount"]))
                  for d in div_events.values()
                  if d.get("date") and d.get("amount"))
    if not ts or not closes or not divs:
        return None

    points = [(t, c) for t, c in zip(ts, closes) if c]
    if len(points) < 24:
        return None
    now_ts, price = points[-1]

    ttm_now = _ttm_dividends(divs, now_ts)
    if ttm_now <= 0 or price <= 0:
        return None
    current_yield = ttm_now / price * 100

    # 5-yr median trailing yield: TTM dividends ÷ close at each of the
    # last 60 monthly bars (needs a year of dividend lead-in, which the
    # 6y range provides).
    yields = []
    for t, c in points[-60:]:
        ttm = _ttm_dividends(divs, t)
        if ttm > 0 and c > 0:
            yields.append(ttm / c * 100)
    median_y5 = statistics.median(yields) if len(yields) >= 24 else None

    # 5-yr dividend CAGR: TTM now vs TTM ending ~60 months ago. If the
    # event series doesn't reach back far enough to fully cover that
    # trailing window (short history, sparse Yahoo data), anchor the
    # "then" window just inside the covered span and annualize over the
    # actual distance — an uncovered window silently overstates growth.
    year_s = 365 * 24 * 3600
    anchor = max(now_ts - 5 * year_s, divs[0][0] + int(1.05 * year_s))
    ttm_then = _ttm_dividends(divs, anchor)
    years = (now_ts - anchor) / year_s
    dg5 = (((ttm_now / ttm_then) ** (1 / years) - 1) * 100
           if ttm_then > 0 and years >= 2 else None)

    time.sleep(SLEEP_BETWEEN)
    daily = _chart_result(_fetch(CHART_URL.format(
        t=ticker, rng="1y", iv="1d", events="")))
    pct_off_high = pct_above_low = None
    if daily:
        dcloses = [c for c in ((daily.get("indicators", {}).get("quote") or [{}])[0].get("close") or []) if c]
        if dcloses:
            hi, lo = max(dcloses), min(dcloses)
            if hi > 0:
                pct_off_high = (1 - price / hi) * 100
            if lo > 0:
                pct_above_low = (price / lo - 1) * 100

    out = {
        "y":     round(current_yield, 2),
        "price": round(price, 2),
    }
    if median_y5 is not None:
        out["median_y5"] = round(median_y5, 2)
    if dg5 is not None:
        out["dg5"] = round(dg5, 2)
    if pct_off_high is not None:
        out["pct_off_52wk_high"] = round(pct_off_high, 1)
    if pct_above_low is not None:
        out["pct_above_52wk_low"] = round(pct_above_low, 1)
    return out


def main() -> int:
    tickers = [a["t"] for a in UNIVERSE]
    print(f"[aristocrats] Fetching {len(tickers)} tickers from Yahoo chart API…")
    results: dict[str, dict] = {}
    failures: list[str] = []
    for i, t in enumerate(tickers, 1):
        try:
            row = fetch_ticker(t)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                OSError, ValueError, KeyError) as e:
            row = None
            print(f"[aristocrats] {t}: fetch failed — {e}")
        if row:
            results[t] = row
        else:
            failures.append(t)
        if i % 10 == 0:
            print(f"[aristocrats] {i}/{len(tickers)} done ({len(results)} ok)")
        time.sleep(SLEEP_BETWEEN)

    frac = len(results) / max(1, len(tickers))
    print(f"[aristocrats] Success: {len(results)}/{len(tickers)} "
          f"({frac:.0%}); failed: {', '.join(failures) or 'none'}")
    if frac < MIN_SUCCESS_FRACTION:
        print("[aristocrats] Too many failures — Yahoo may have changed "
              "its chart API or blocked the runner. Not writing output.")
        return 2

    now = datetime.now(timezone.utc)
    payload = {
        "as_of":      now.strftime("%Y-%m-%d"),
        "fetched_at": now.strftime("%Y-%m-%dT%H:%MZ"),
        "count":      len(results),
        "tickers":    results,
    }
    out_path = Path(__file__).resolve().parent.parent / "data" / "aristocrats.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"[aristocrats] ✓ Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
