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
from datetime import date, timedelta
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


# ── WHY THE CHART STOPS WHERE IT STOPS ──────────────────────────────
# The three inputs are inner-joined, so the newest month on the chart is
# the newest month ALL THREE have. They do not publish on the same
# schedule, and the slowest one therefore sets the date:
#
#   30-year mortgage rate   weekly, current to within days
#   CPI less shelter        monthly, ~2 weeks after month end
#   Case-Shiller            monthly, ~2 MONTHS after month end
#
# So the chart trailing "today" by two months is the data arriving, not
# the page failing to refresh — but a bare "as of May 2026" looks exactly
# like neglect, which is what prompted this. These functions let the page
# say which input it is waiting on and when that input next publishes,
# rather than leaving a reader to guess.
SOURCE_LABELS = {
    "hpi": "Case-Shiller home prices",
    "rate": "30-year mortgage rate",
    "cpi": "CPI less shelter",
}

# S&P publish Case-Shiller at 9am ET on the last Tuesday of each month,
# and the report carries the month TWO before it: the release on
# 2026-08-25 is the June 2026 index.
CASE_SHILLER_LAG_MONTHS = 2


def last_tuesday(year: int, month: int) -> date:
    """The last Tuesday of a month — Case-Shiller's release day."""
    nxt = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    eom = nxt - timedelta(days=1)
    # weekday(): Mon=0, Tue=1. Step back to the most recent Tuesday.
    return eom - timedelta(days=(eom.weekday() - 1) % 7)


def next_case_shiller_release(today: date) -> dict:
    """The next Case-Shiller release on or after `today`, and what it adds.

    Returns the release date and the month that release will cover, so a
    reader waiting for the chart to advance knows the actual date to look
    again rather than being told "soon".
    """
    y, m = today.year, today.month
    rel = last_tuesday(y, m)
    if rel < today:
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        rel = last_tuesday(y, m)
    cy, cm = y, m - CASE_SHILLER_LAG_MONTHS
    if cm <= 0:
        cm += 12
        cy -= 1
    return {"date": rel.isoformat(), "covers": f"{cy:04d}-{cm:02d}"}


def binding_source(last_observation: dict) -> str | None:
    """Which input is holding the chart back.

    MEASURED, never assumed. Case-Shiller is the usual answer, but if CPI
    or the rate series ever stalls this must name that instead — a page
    that blames the wrong feed sends its reader to check the wrong thing.
    Returns None when nothing is known rather than guessing.
    """
    known = {k: v for k, v in (last_observation or {}).items() if v}
    if not known:
        return None
    return min(known, key=lambda k: known[k])


def months_between(earlier: str, later: str) -> int | None:
    """Whole months from one YYYY-MM to another. None if either is junk."""
    try:
        ey, em = (int(x) for x in earlier.split("-")[:2])
        ly, lm = (int(x) for x in later.split("-")[:2])
    except (ValueError, AttributeError, TypeError):
        return None
    return (ly - ey) * 12 + (lm - em)


def freshness(last_observation: dict, as_of_month: str, today: date) -> dict:
    """Everything the page needs to explain its own end date."""
    src = binding_source(last_observation)
    return {
        "as_of_month": as_of_month,
        "last_observation": dict(last_observation or {}),
        "binding_source": src,
        "binding_label": SOURCE_LABELS.get(src),
        "months_behind": months_between(as_of_month,
                                        f"{today.year:04d}-{today.month:02d}"),
        "next_case_shiller": next_case_shiller_release(today),
    }


def _refresh_freshness(payload: dict) -> dict:
    """Re-date a cached payload's freshness block against today.

    Payloads written before this block existed have no `last_observation`
    to work from. Those keep whatever they have rather than getting an
    invented one — an unexplained date is a smaller lie than a wrong
    explanation, and the next fetch replaces it anyway.
    """
    f = (payload or {}).get("freshness")
    if not f or not f.get("last_observation"):
        return payload
    out = dict(payload)
    out["freshness"] = freshness(f["last_observation"],
                                 f.get("as_of_month") or "",
                                 date.today())
    return out


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
        # The series are a day old at most, but "next release" and "months
        # behind" are statements about NOW. Served straight from cache they
        # would eventually name a release date already in the past — the
        # page would be confidently wrong about the one thing a reader
        # came to it for. Recompute them against today's date.
        return _refresh_freshness(cached)

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

    # Recorded BEFORE the join, because the join is what hides the answer:
    # afterwards every series ends on the same month and there is no way
    # to tell which one ran out first.
    def _last_month(s):
        return s.index[-1].strftime("%Y-%m") if len(s) else None

    last_observation = {"hpi": _last_month(hpi), "rate": _last_month(rate),
                        "cpi": _last_month(cpi)}

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
        "freshness": freshness(last_observation, now_d[:7], date.today()),
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
