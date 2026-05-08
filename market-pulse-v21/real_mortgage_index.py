"""
Real Mortgage Payment Price Index — Case-Shiller home prices, adjusted
for inflation and the actual mortgage rate at each point in time.

The point of this index, originally John Wake's (RealEstateDecoded.com):
home prices alone are misleading. A $400,000 house at 3% is a totally
different deal from a $400,000 house at 7%. This index combines the
sale price (Case-Shiller), the mortgage rate at that moment (Freddie
Mac 30Y), and inflation (CPI excluding shelter) into a single "what
you'd actually pay each month, in real dollars" series.

  index[t] = (nominal_pi[t] / nominal_pi[base])
           × (cpi[base] / cpi[t])
           × 100

  where nominal_pi[t] is the standard 30-year P&I on a Case-Shiller-
  indexed home at that month's mortgage rate, with `down_pct`% down,
  and base = January 1990 (so a reading of 100 = "as expensive as 1990").

CPI Less Shelter is the right deflator: shelter is the input we're
measuring, so deflating by full CPI (which includes shelter) would
mute the very effect we want to see.
"""
import json
import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE = Path("/tmp/market_pulse_cache")
CACHE.mkdir(exist_ok=True)

# Case-Shiller seasonally-adjusted FRED series IDs.
CASE_SHILLER_SERIES = {
    "US":  "CSUSHPISA",   # US National HPI
    "BOS": "BOXRSA",      # Boston
    "CHA": "CRXRSA",      # Charlotte
    "CHI": "CHXRSA",      # Chicago
    "CLE": "CEXRSA",      # Cleveland
    "DEN": "DNXRSA",      # Denver
    "LAS": "LVXRSA",      # Las Vegas
    "LA":  "LXXRSA",      # Los Angeles
    "MIA": "MIXRSA",      # Miami
    "MIN": "MNXRSA",      # Minneapolis
    "NYC": "NYXRSA",      # New York
    "PHX": "PHXRSA",      # Phoenix
    "POR": "POXRSA",      # Portland
    "SD":  "SDXRSA",      # San Diego
    "SF":  "SFXRSA",      # San Francisco
    "SEA": "SEXRSA",      # Seattle
    "TPA": "TPXRSA",      # Tampa
    "DC":  "WDXRSA",      # Washington DC
}

METRO_LABELS = {
    "US":  "USA",
    "BOS": "Boston",
    "CHA": "Charlotte",
    "CHI": "Chicago",
    "CLE": "Cleveland",
    "DEN": "Denver",
    "LAS": "Las Vegas",
    "LA":  "Los Angeles",
    "MIA": "Miami",
    "MIN": "Minneapolis",
    "NYC": "New York",
    "PHX": "Phoenix",
    "POR": "Portland",
    "SD":  "San Diego",
    "SF":  "San Francisco",
    "SEA": "Seattle",
    "TPA": "Tampa",
    "DC":  "Washington DC",
}

START_DATE = "1985-01-01"          # need a few years before 1990 to anchor
BASE_PERIOD = "1990-01"            # January 1990 = 100
MORTGAGE_RATE_SERIES = "MORTGAGE30US"
CPI_LESS_SHELTER_SERIES = "CUSR0000SA0L2"  # CPI-U All items less shelter, SA


def _cp(k):
    return CACHE / f"{k}.json"


def _rc(k, hrs=24):
    p = _cp(k)
    if p.exists() and time.time() - p.stat().st_mtime < hrs * 3600:
        try:
            return json.loads(p.read_text())
        except Exception:
            return None
    return None


def _rc_any(k):
    """Return cached value regardless of age, for stale-fallback when
    the upstream (FRED) is down. Used after a fetch fails so the user
    still gets a chart instead of an error banner."""
    p = _cp(k)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return None
    return None


def _wc(k, d):
    try:
        _cp(k).write_text(json.dumps(d))
    except Exception as e:
        logger.warning(f"rmpi cache write {k}: {e}")


def _monthly_pi(home_value, rate_pct, down_pct, years=30):
    """Monthly P&I on a `home_value` home, `down_pct`% down, fixed `rate_pct`/yr."""
    if rate_pct is None or rate_pct <= 0 or home_value is None or home_value <= 0:
        return None
    principal = home_value * (1 - down_pct / 100.0)
    r = (rate_pct / 100.0) / 12.0
    n = years * 12
    if r == 0:
        return principal / n
    return principal * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def compute_index(metro: str = "US", down_pct: float = 10.0) -> dict:
    """Compute the real-mortgage-payment index time series for a metro.

    Returns a dict with the series, key stats (now / 2006 peak / 2012
    trough), and comparison percentages. Cached for 24h per
    (metro, down_pct).
    """
    metro = (metro or "US").upper()
    if metro not in CASE_SHILLER_SERIES:
        return {"error": f"Unknown metro '{metro}'."}

    try:
        down_pct = float(down_pct)
    except (TypeError, ValueError):
        return {"error": "Invalid down_pct."}
    if down_pct < 0 or down_pct >= 100:
        return {"error": "down_pct must be between 0 and 99."}

    cache_key = f"rmpi_{metro}_d{int(down_pct)}"
    cached = _rc(cache_key, hrs=24)
    if cached:
        return cached

    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        return {"error": "FRED_API_KEY not set on the server. Add it to enable this page."}

    try:
        from fredapi import Fred
        import pandas as pd
    except Exception as e:
        return {"error": f"Server missing dependency: {e}"}

    # FRED occasionally returns 5xx during their own outages. Retry
    # twice with a short backoff before giving up — most transient
    # blips clear within a couple of seconds.
    last_err = None
    hpi = rate = cpi = None
    for attempt in range(3):
        try:
            fred = Fred(api_key=api_key)
            hpi = fred.get_series(CASE_SHILLER_SERIES[metro], observation_start=START_DATE).dropna()
            rate = fred.get_series(MORTGAGE_RATE_SERIES, observation_start=START_DATE).dropna()
            cpi = fred.get_series(CPI_LESS_SHELTER_SERIES, observation_start=START_DATE).dropna()
            last_err = None
            break
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))

    if last_err is not None:
        logger.warning(f"FRED fetch failed for rmpi {metro}: {last_err}")
        # Stale-cache fallback: if we have any prior result for this
        # (metro, down_pct) on disk, serve it with a `stale` flag so
        # the chart still renders during a FRED outage. The frontend
        # can show a small "data may be a few days old" notice.
        stale = _rc_any(cache_key)
        if stale:
            stale = dict(stale)
            stale["stale"] = True
            stale["stale_reason"] = f"FRED upstream error: {last_err}"
            return stale
        return {"error": f"FRED fetch failed: {last_err}"}

    # Mortgage rate is weekly (Thursdays); resample to monthly mean so it
    # aligns with HPI + CPI (both monthly).
    rate_monthly = rate.resample("ME").mean()
    df = pd.concat([
        hpi.rename("hpi").resample("ME").last(),
        rate_monthly.rename("rate"),
        cpi.rename("cpi").resample("ME").last(),
    ], axis=1).dropna()

    if df.empty:
        return {"error": "No overlapping data after alignment."}

    df["pi"] = [
        _monthly_pi(h, r, down_pct) for h, r in zip(df["hpi"], df["rate"])
    ]
    df = df.dropna()
    if df.empty:
        return {"error": "Could not compute payments."}

    # Anchor on Jan 1990 if available; fall back to first month otherwise.
    base_mask = df.index.strftime("%Y-%m") == BASE_PERIOD
    if base_mask.any():
        base_row = df[base_mask].iloc[0]
        base_actual = BASE_PERIOD
    else:
        base_row = df.iloc[0]
        base_actual = df.index[0].strftime("%Y-%m")
    base_pi = float(base_row["pi"])
    base_cpi = float(base_row["cpi"])

    df["index"] = (df["pi"] / base_pi) * (base_cpi / df["cpi"]) * 100

    series = [
        {"date": d.strftime("%Y-%m-%d"), "value": round(float(v), 1)}
        for d, v in df["index"].items()
    ]

    now_v = series[-1]["value"]
    now_d = series[-1]["date"]

    # 2006 peak — max within 2004-2008. Wide window so metros that peaked
    # earlier (LA, SF) or later (NY, MIA) all get captured.
    peak_window = df[(df.index >= "2004-01-01") & (df.index <= "2008-12-31")]
    peak_v = peak_d = None
    if not peak_window.empty:
        peak_v = round(float(peak_window["index"].max()), 1)
        peak_d = peak_window["index"].idxmax().strftime("%Y-%m-%d")

    # 2012 trough — min within 2011-2014. Same idea.
    trough_window = df[(df.index >= "2011-01-01") & (df.index <= "2014-12-31")]
    trough_v = trough_d = None
    if not trough_window.empty:
        trough_v = round(float(trough_window["index"].min()), 1)
        trough_d = trough_window["index"].idxmin().strftime("%Y-%m-%d")

    out = {
        "metro": metro,
        "label": METRO_LABELS[metro],
        "down_pct": down_pct,
        "base_period": base_actual,
        "as_of": now_d,
        "series": series,
        "stats": {
            "now": now_v, "now_date": now_d,
            "peak_2006": peak_v, "peak_2006_date": peak_d,
            "trough_2012": trough_v, "trough_2012_date": trough_d,
        },
        "comparison": {
            "vs_1990_pct":  round(now_v - 100, 0),
            "vs_2006_pct":  round((now_v - peak_v)   / peak_v   * 100, 0) if peak_v   else None,
            "vs_2012_pct":  round((now_v - trough_v) / trough_v * 100, 0) if trough_v else None,
        },
    }
    _wc(cache_key, out)
    return out


def list_metros() -> list[dict]:
    """List of {code, label} for the metro picker."""
    return [{"code": code, "label": METRO_LABELS[code]} for code in CASE_SHILLER_SERIES]
