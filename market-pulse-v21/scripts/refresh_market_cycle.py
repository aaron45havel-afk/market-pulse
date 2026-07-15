"""Monthly equity-market cycle proxy for the 11 countries OECD dropped.

OECD pruned its Composite Leading Indicator (and its whole confidence-
indicator suite) to 17 economies, dropping these 11 from our set:
  CH, NL, SE, PL, CL, HK, SG, TW, TH, MY, PH.
There is no single free, low-maintenance CLI-equivalent that covers all
of them — the "proper" answer is ~8 different national statistics APIs,
several needing keys and returning Excel. That's a maintenance trap.

Instead we build ONE uniform proxy from a single already-proven source
(Yahoo Finance index data — the same fetch refresh_aristocrats.py uses):
each country's main equity index, turned into a business-cycle reading.
Share prices are themselves a core component of the OECD CLI, and since
/global-values exists to time country *equity* entries, the market's own
cycle is a defensible — and directly relevant — signal. It is written to
data/market_cycle.json, tagged so the page badges it "market proxy",
never mistaken for true OECD CLI.

Method (amplitude-adjusted, mirrors how CLI is built):
  • ~7 years of monthly index closes.
  • cyclical deviation = log(price) − trailing 36-month mean of log(price)
    (how far the market sits above/below its own trend).
  • z = deviation ÷ its own historical stdev.
  • cli_equiv = 100 + clip(z, ±3) × 1.5  → same ~97–103 band as CLI, so
    country_data/composite_scores consume it unchanged.
  • trend from the 3-month change in cli_equiv (noisier than CLI monthly,
    so a wider ±0.15 deadband).

Resilient: tries candidate symbols per country, logs failures, and only
writes if ≥ 1/3 succeeded — a partial file still covers most and the app
falls back to snapshot for the rest.

Cadence: 12th of each month (after the OECD run on the 10th).
"""
from __future__ import annotations

import json
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=8y&interval=1mo"
HEADERS = {"User-Agent": "Mozilla/5.0 (market-pulse-refresh/1.0)",
           "Accept": "application/json"}
SLEEP_BETWEEN = 0.4
MIN_SUCCESS_FRACTION = 0.34

TREND_WINDOW_MONTHS = 36          # trailing trend the cycle deviates from
MIN_POINTS = 48                   # need trend window + room for stdev + trend
CLI_SCALE = 1.5                   # z→cli_equiv gain (±3σ → ~95.5–104.5)
TREND_DEADBAND = 0.15

# Each country → ordered candidate Yahoo symbols (first that returns
# enough history wins). '^' is URL-encoded at fetch time.
COUNTRY_INDEX: dict[str, list[tuple[str, str]]] = {
    # code:      [(symbol, human label), ...]
    "CH": [("^SSMI", "Swiss Market Index")],
    "NL": [("^AEX", "AEX (Amsterdam)")],
    "SE": [("^OMX", "OMX Stockholm 30"), ("^OMXSPI", "OMX Stockholm PI"), ("^OMXS30", "OMXS30")],
    "PL": [("WIG20.WA", "WIG20 (Warsaw)"), ("^WIG20", "WIG20"), ("WIG.WA", "WIG")],
    "CL": [("^IPSA", "IPSA (Santiago)")],
    "HK": [("^HSI", "Hang Seng")],
    "SG": [("^STI", "Straits Times")],
    "TW": [("^TWII", "TAIEX (Taiwan)")],
    "TH": [("^SET.BK", "SET (Thailand)"), ("^SETI", "SET Index")],
    "MY": [("^KLSE", "FTSE Bursa Malaysia KLCI")],
    "PH": [("PSEI.PS", "PSEi (Philippines)"), ("^PSI", "PSEi"), ("PSEI.PH", "PSEi")],
}


def _fetch(url: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _monthly_closes(symbol: str) -> list[float] | None:
    """Ordered (oldest→newest) monthly closes for a Yahoo symbol, or None."""
    url = CHART_URL.format(sym=urllib.parse.quote(symbol, safe=""))
    try:
        payload = _fetch(url)
        res = payload["chart"]["result"][0]
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
            OSError, ValueError, KeyError, IndexError, TypeError):
        return None
    closes = (res.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    closes = [c for c in closes if c]
    return closes if len(closes) >= MIN_POINTS else None


def _log(x: float) -> float:
    import math
    return math.log(x)


def cycle_from_closes(closes: list[float]) -> dict | None:
    """Turn a monthly close series into {value, prev, trend} on CLI's ~100
    anchor. Returns None if there isn't enough clean history."""
    logs = [_log(c) for c in closes if c > 0]
    if len(logs) < MIN_POINTS:
        return None
    w = TREND_WINDOW_MONTHS
    # Deviation of each month from its own trailing w-month trend.
    devs = []
    for i in range(w, len(logs)):
        trend = statistics.fmean(logs[i - w:i])
        devs.append(logs[i] - trend)
    if len(devs) < 6:
        return None
    sd = statistics.pstdev(devs) or 1e-9

    def cli_equiv(dev: float) -> float:
        z = max(-3.0, min(3.0, dev / sd))
        return round(100 + z * CLI_SCALE, 2)

    now = cli_equiv(devs[-1])
    prev = cli_equiv(devs[-2])
    ref3 = cli_equiv(devs[-4]) if len(devs) >= 4 else prev
    delta = now - ref3
    trend = "rising" if delta > TREND_DEADBAND else "falling" if delta < -TREND_DEADBAND else "flat"
    return {"value": now, "prev": prev, "trend": trend}


def fetch_country(code: str, candidates: list[tuple[str, str]]) -> dict | None:
    for symbol, label in candidates:
        closes = _monthly_closes(symbol)
        time.sleep(SLEEP_BETWEEN)
        if not closes:
            print(f"[market] {code}: {symbol} — no usable history")
            continue
        cyc = cycle_from_closes(closes)
        if not cyc:
            print(f"[market] {code}: {symbol} — not enough clean points")
            continue
        cyc["source_label"] = f"{label} cycle proxy"
        print(f"[market] {code}: {symbol} → cli_equiv {cyc['value']} ({cyc['trend']})")
        return cyc
    return None


def main() -> int:
    print(f"[market] Building equity cycle proxy for {len(COUNTRY_INDEX)} countries…")
    out: dict[str, dict] = {}
    failures: list[str] = []
    for code, candidates in COUNTRY_INDEX.items():
        entry = fetch_country(code, candidates)
        if entry:
            out[code] = entry
        else:
            failures.append(code)

    frac = len(out) / max(1, len(COUNTRY_INDEX))
    print(f"[market] Success: {len(out)}/{len(COUNTRY_INDEX)} ({frac:.0%}); "
          f"failed: {', '.join(failures) or 'none'}")
    if frac < MIN_SUCCESS_FRACTION:
        print("[market] Too many failures — Yahoo may have changed its API or "
              "blocked the runner. Not writing output.")
        return 2

    now = datetime.now(timezone.utc)
    payload = {
        "as_of":      now.strftime("%Y-%m"),
        "fetched_at": now.strftime("%Y-%m-%dT%H:%MZ"),
        "method":     "equity index: log-price deviation from 36-mo trend, "
                      "z-scored, mapped to 100 +/- 1.5*z",
        "series":     out,
    }
    out_path = Path(__file__).resolve().parent.parent / "data" / "market_cycle.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"[market] ✓ Wrote {len(out)} countries → {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
