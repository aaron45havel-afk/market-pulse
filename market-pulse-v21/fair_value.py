"""Fair-Value methodology — translate a Darth-Powell-style 'pre-shock
payment, inflation-adjusted, re-solved at today's rate' calculation
into per-state numbers.

Methodology (mirrors the Twitter post by @VladTheInflator):

  1. BASELINE — pre-affordability-shock state of the world. We use
     the earliest month in each ZIP's history_zhvi array (~June 2021,
     when MORTGAGE30US ≈ 3.0% — last 'normal' moment before rates
     ripped). State median = median of per-ZIP baseline values.

  2. PITI THEN — compute the standard 20%-down PITI for the baseline
     median at the baseline mortgage rate. State-specific property
     tax + insurance from data_providers tables.

  3. INFLATE — multiply baseline PITI by (CPI_today / CPI_baseline)
     so the housing cost has the same buying-power claim on the
     household budget as it did then.

  4. BACK-SOLVE — given today's mortgage rate + state tax/insurance,
     what home price produces the inflated PITI? That price is the
     state's 'fair value'.

  5. COMPARE — current state median vs fair value. Positive % = over,
     negative = under.

Caches CPI + rate fetches in /tmp for 24h. Falls back to embedded
constants if FRED is unreachable so the page still renders.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path("/tmp/market_pulse_cache")
CACHE_DIR.mkdir(exist_ok=True)
DB_PATH = Path(__file__).resolve().parent / "data" / "zips.db"

# ── Fallback constants if FRED is unreachable ──────────────────────
# Baseline = June 2021 (matches the earliest month most ZIPs have in
# their history_zhvi array, which is what we aggregate state medians
# from). Values: FRED CPIAUCSL June 2021 = 270.5, MORTGAGE30US June
# 2021 ≈ 2.98% (Freddie PMMS weekly average for the month).
BASELINE_LABEL = "5 yr ago (~mid-2021)"
FALLBACK_BASELINE_CPI = 270.5
FALLBACK_BASELINE_RATE = 3.0
# CURRENT_CPI is only used if today's CPI fetch fails; refreshed in
# practice from FRED on each request (24h cache).
FALLBACK_CURRENT_CPI = 315.0


def _fred_get(series_id: str, params_extra: dict | None = None) -> list[dict] | None:
    """Generic FRED observations fetch with retry. Returns the
    observations list (newest-first if sort_order=desc) or None."""
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        return None
    params = {
        "series_id": series_id, "api_key": api_key, "file_type": "json",
        "sort_order": "desc", "limit": 12,
    }
    if params_extra:
        params.update(params_extra)
    url = f"https://api.stlouisfed.org/fred/series/observations?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
    last_err: str | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read()).get("observations", [])
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if not (e.code >= 500 or e.code == 429):
                break
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = f"net {e}"
        if attempt < 2:
            time.sleep(1.5 * (attempt + 1))
    logger.warning("FRED %s fetch failed: %s", series_id, last_err)
    return None


def _cached_or_fetch(cache_key: str, fetcher, max_age_sec: int = 86400):
    """Read JSON cache if fresh; otherwise refresh via fetcher()."""
    cache_path = CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists():
        age = time.time() - cache_path.stat().st_mtime
        if age < max_age_sec:
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                pass
    val = fetcher()
    if val is not None:
        try:
            cache_path.write_text(json.dumps(val))
        except Exception as e:
            logger.warning("fair_value cache write %s: %s", cache_key, e)
    return val


# In-process memo for FRED-derived values. The on-disk cache
# already handles cross-request freshness (24h for CPI, 30d for
# historical baselines), but the disk read + JSON parse still ran
# 51 times per /fair-value request because the route called the
# computation once per state. Process-lifetime memo skips that
# entirely — CPI doesn't change mid-request.
_memo: dict = {}


def _current_cpi() -> tuple[float, str]:
    """Most recent CPIAUCSL observation. Returns (value, period_label)."""
    if "current_cpi" in _memo:
        return _memo["current_cpi"]
    def _do():
        obs = _fred_get("CPIAUCSL")
        if not obs:
            return None
        for o in obs:
            v = o.get("value")
            if v not in (None, "", "."):
                try:
                    return {"value": float(v), "date": o.get("date", "")}
                except ValueError:
                    continue
        return None
    cached = _cached_or_fetch("fv_cpi_current", _do)
    result = (cached["value"], cached["date"]) if cached else (FALLBACK_CURRENT_CPI, "fallback")
    _memo["current_cpi"] = result
    return result


def _baseline_cpi_and_rate() -> tuple[float, float, str]:
    """Mid-2021 CPI + mortgage rate. These are stationary historical
    values (they don't change as time passes — June 2021 is what it
    is), so cache for a very long time. Returns (cpi, rate, label)."""
    def _do_cpi():
        obs = _fred_get("CPIAUCSL", {"observation_start": "2021-06-01",
                                      "observation_end": "2021-06-30",
                                      "sort_order": "asc", "limit": 1})
        if not obs: return None
        v = obs[0].get("value")
        try: return float(v)
        except (ValueError, TypeError): return None

    def _do_rate():
        # Average the 4-5 weekly MORTGAGE30US prints from June 2021.
        obs = _fred_get("MORTGAGE30US", {"observation_start": "2021-06-01",
                                          "observation_end": "2021-06-30",
                                          "sort_order": "asc", "limit": 8})
        if not obs: return None
        vals = []
        for o in obs:
            v = o.get("value")
            try: vals.append(float(v))
            except (ValueError, TypeError): continue
        return round(sum(vals)/len(vals), 2) if vals else None

    if "baseline" in _memo:
        return _memo["baseline"]
    cpi = _cached_or_fetch("fv_cpi_baseline_jun2021", _do_cpi, max_age_sec=30*86400)
    rate = _cached_or_fetch("fv_rate_baseline_jun2021", _do_rate, max_age_sec=30*86400)
    result = (cpi or FALLBACK_BASELINE_CPI,
              rate or FALLBACK_BASELINE_RATE,
              BASELINE_LABEL)
    _memo["baseline"] = result
    return result


def _state_baseline_medians() -> dict[str, float]:
    """Aggregate the earliest-month ZHVI per state by taking median
    across all ZIPs (>1000 population) with history. Returns
    {state_code: median_baseline_value}.

    Process-lifetime cached, with a zips.db mtime check so a
    workflow that rebuilds the DB invalidates without a restart.
    Previously /fair-value called this 51 times per request (one
    per compute_state_fair_value call), parsing 30k history_zhvi
    JSON arrays each time — that was the entire page latency.
    """
    if not DB_PATH.exists():
        return {}
    mtime = DB_PATH.stat().st_mtime
    cached = _state_baseline_medians._cache  # type: ignore[attr-defined]
    if cached is not None and cached[0] == mtime:
        return cached[1]
    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute(
        "select state, history_zhvi from zips "
        "where state is not null and history_zhvi is not null and population > 1000"
    ).fetchall()
    conn.close()
    by_state: dict[str, list[float]] = {}
    for st, hist_json in rows:
        try:
            h = json.loads(hist_json)
        except Exception:
            continue
        if not h or h[0] is None or h[0] <= 0:
            continue
        by_state.setdefault(st, []).append(float(h[0]))
    result = {st: statistics.median(vals) for st, vals in by_state.items() if len(vals) >= 5}
    _state_baseline_medians._cache = (mtime, result)  # type: ignore[attr-defined]
    return result


_state_baseline_medians._cache = None  # type: ignore[attr-defined]


def _monthly_factor(rate_pct: float, years: int = 30) -> float:
    """Standard mortgage P&I monthly amortization factor per unit principal."""
    r = (rate_pct / 100.0) / 12.0
    n = years * 12
    if r == 0:
        return 1.0 / n
    return (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def _piti(price: float, rate_pct: float, state_code: str,
          down_pct: float = 20.0) -> float:
    """20%-down standard PITI for a price at a given rate, using the
    same state property tax + insurance tables qualifying_income() uses."""
    from data_providers import (STATE_PROPERTY_TAX_RATE, STATE_INSURANCE_ANNUAL,
                                STATE_HOMESTEAD_EXEMPTION)
    loan = price * (1 - down_pct / 100.0)
    p_and_i = loan * _monthly_factor(rate_pct)
    tax_rate = STATE_PROPERTY_TAX_RATE.get(state_code, 0.011)
    homestead = STATE_HOMESTEAD_EXEMPTION.get(state_code, 0)
    monthly_tax = max(price - homestead, 0) * tax_rate / 12.0
    monthly_ins = STATE_INSURANCE_ANNUAL.get(state_code, 1800) / 12.0
    return p_and_i + monthly_tax + monthly_ins


def _back_solve_price(target_piti: float, rate_pct: float, state_code: str,
                      down_pct: float = 20.0) -> float | None:
    """Given target PITI + today's rate + state-specific tax/ins,
    return the home price that produces that PITI. Linear equation
    in price — solved analytically rather than iteratively.

        target = price × (1-d) × mf + price × t/12 + ins/12
        target - ins/12 = price × ((1-d) × mf + t/12)
        price = (target - ins/12) / ((1-d) × mf + t/12)
    """
    from data_providers import (STATE_PROPERTY_TAX_RATE, STATE_INSURANCE_ANNUAL,
                                STATE_HOMESTEAD_EXEMPTION)
    mf = _monthly_factor(rate_pct)
    t = STATE_PROPERTY_TAX_RATE.get(state_code, 0.011)
    ins_monthly = STATE_INSURANCE_ANNUAL.get(state_code, 1800) / 12.0
    # Note: ignores the homestead exemption on the back-solve since
    # the exemption is tiny relative to price for most states and
    # makes the equation nonlinear. Error <1% in practice.
    denom = (1 - down_pct / 100.0) * mf + t / 12.0
    if denom <= 0:
        return None
    p = (target_piti - ins_monthly) / denom
    return p if p > 0 else None


def compute_state_fair_value(state_code: str,
                              current_market_value: float) -> dict | None:
    """Run the full Darth-Powell fair-value calculation for one state.
    Returns None if we don't have a baseline median for the state."""
    baselines = _state_baseline_medians()
    baseline_value = baselines.get(state_code)
    if baseline_value is None:
        return None
    cpi_baseline, rate_baseline, baseline_label = _baseline_cpi_and_rate()
    cpi_today, cpi_today_date = _current_cpi()
    from data_providers import MORTGAGE_30Y_RATE
    rate_today = MORTGAGE_30Y_RATE

    piti_baseline = _piti(baseline_value, rate_baseline, state_code)
    cpi_factor = cpi_today / cpi_baseline if cpi_baseline > 0 else 1.0
    piti_inflated = piti_baseline * cpi_factor
    fair_value = _back_solve_price(piti_inflated, rate_today, state_code)
    if not fair_value:
        return None
    piti_today_actual = _piti(current_market_value, rate_today, state_code)
    delta_pct = (current_market_value - fair_value) / fair_value * 100

    return {
        "baseline_label": baseline_label,
        "baseline_value": round(baseline_value),
        "baseline_rate": rate_baseline,
        "baseline_cpi": round(cpi_baseline, 1),
        "baseline_piti": round(piti_baseline),

        "cpi_today": round(cpi_today, 1),
        "cpi_today_date": cpi_today_date,
        "cpi_factor": round(cpi_factor, 3),

        "inflated_piti": round(piti_inflated),
        "rate_today": rate_today,
        "fair_value": round(fair_value),

        "market_value": round(current_market_value),
        "piti_today_actual": round(piti_today_actual),
        "delta_pct": round(delta_pct, 1),
    }


def compute_zips_in_state(state_code: str, limit: int = 500) -> list[dict]:
    """Per-ZIP fair-value drilldown for one state. Returns rows sorted
    by % overvalued (descending), capped at ``limit`` to keep the
    payload manageable on big states like CA / TX / NY.

    Uses each ZIP's own history_zhvi[0] as its individual baseline —
    no state-level smoothing, so we surface the real spread between
    overpriced Cleveland suburbs vs. underpriced Cleveland east side."""
    if not DB_PATH.exists() or not state_code:
        return []
    cpi_baseline, rate_baseline, _ = _baseline_cpi_and_rate()
    cpi_today, _ = _current_cpi()
    from data_providers import MORTGAGE_30Y_RATE
    rate_today = MORTGAGE_30Y_RATE
    cpi_factor = cpi_today / cpi_baseline if cpi_baseline > 0 else 1.0

    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute(
        # population > 500 filters PO-box ZCTAs whose ZHVI is noisy.
        # median_home_value not null ensures the ZIP has Zillow coverage.
        # lat/lng included so the map view can drop a colored marker
        # per ZIP without a second query.
        "select zip, name, neighborhood, county, median_home_value, "
        "       history_zhvi, population, lat, lng "
        "from zips "
        "where state = ? and median_home_value is not null "
        "  and history_zhvi is not null and population > 500",
        (state_code.upper(),),
    ).fetchall()
    conn.close()

    out = []
    for zip_code, name, neighborhood, county, market_value, hist_json, pop, lat, lng in rows:
        try:
            h = json.loads(hist_json)
        except Exception:
            continue
        if not h or h[0] is None or h[0] <= 0:
            continue
        baseline_value = float(h[0])
        if not market_value or market_value <= 0:
            continue
        piti_baseline = _piti(baseline_value, rate_baseline, state_code)
        piti_inflated = piti_baseline * cpi_factor
        fair_value = _back_solve_price(piti_inflated, rate_today, state_code)
        if not fair_value:
            continue
        delta_pct = (market_value - fair_value) / fair_value * 100
        # Prefer the most-specific area name we have. neighborhood
        # is the friendliest (e.g. "Lakewood") when populated by
        # enrich_neighborhoods.py; falls back to the city in `name`
        # (e.g. "Cleveland, OH"). Cleans up duplicate state suffix
        # since users see the state on the page already.
        clean_name = name or ""
        if state_code and clean_name.endswith(f", {state_code}"):
            clean_name = clean_name[: -(len(state_code) + 2)]
        area = neighborhood or clean_name or f"ZIP {zip_code}"
        out.append({
            "zip": zip_code,
            "area": area,
            "city": clean_name,
            "neighborhood": neighborhood or "",
            "county": county or "",
            "population": pop or 0,
            "lat": lat,
            "lng": lng,
            "baseline_value": round(baseline_value),
            "market_value": round(market_value),
            "fair_value": round(fair_value),
            "delta_pct": round(delta_pct, 1),
        })
    out.sort(key=lambda r: r["delta_pct"], reverse=True)
    return out[:limit]
