"""Build the national ZIPs SQLite database from public, free, stable sources.

Output: ``data/zips.db`` — one row per ZCTA covered by Zillow ZHVI, with:
  - lat / lng / area / population              (Census 2020 ZCTA Gazetteer)
  - state                                       (Census 2020 ZCTA→state crosswalk)
  - median_home_value + home_value_yoy          (Zillow ZHVI per ZIP)
  - median_rent_monthly                         (Zillow ZORI per ZIP, or imputed)
  - median_household_income, pct_bachelors      (Census ACS 2022 5-year API)
  - walk_score, crime_index, restaurant_score   (proxies — see helpers)
  - cap_rate_pct, composite_{balanced,investor,lifestyle,score}
                                                (compute_zip_metrics, same
                                                 formula real metros use)

This is the data spine for serving viewport-filtered ZIPs to /map. The
hand-curated metro datasets (DALLAS_ZIPS, etc.) stay separate and remain
authoritative for their ZIPs — this DB augments coverage to the ~30K
ZIPs Zillow tracks nationally without any hand-tuning.

Sources, all free and stable:
  * Zillow Research public CSVs (ZHVI all-ZIPs, ZORI all-ZIPs)
  * Census 2020 ZCTA Gazetteer (centroid, area, population)
  * Census 2020 ZCTA→State relationship file
  * Census ACS 2022 5-year API (income + education) — single bulk call

Cadence: monthly, via .github/workflows/refresh-national-zips.yml. Runs
after the existing refresh-zillow workflow so the per-ZIP CSVs are at
their latest before this fans them out into the DB.

Usage:
    python scripts/build_national_zips.py [--dry-run] [--limit N]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from zipfile import ZipFile

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "data" / "zips.db"

# compute_zip_metrics is the canonical scoring formula — same one real
# metros (DALLAS_ZIPS etc.) flow through. Using it here means national
# ZIPs and hand-curated ZIPs sit on the same composite scale.
sys.path.insert(0, str(REPO_ROOT))
from dallas_neighborhoods import compute_zip_metrics  # noqa: E402

# ─── Sources ────────────────────────────────────────────────────────
ZHVI_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zhvi/"
    "Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
)
ZORI_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zori/"
    "Zip_zori_uc_sfrcondomfr_sm_month.csv"
)
GAZETTEER_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2020_Gazetteer/2020_Gaz_zcta_national.zip"
)
# Note: state + city come from the Zillow ZHVI CSV directly (it has
# State and City columns) so we don't need a separate Census ZCTA→state
# crosswalk. Saves one external dependency and one network call.
ACS_API = "https://api.census.gov/data/2022/acs/acs5"
ACS_VARS = (
    "B19013_001E,"   # Median household income
    "B15003_001E,"   # Pop 25+ (denominator for bachelor's %)
    "B15003_022E,"   # Bachelor's
    "B15003_023E,"   # Master's
    "B15003_024E,"   # Professional
    "B15003_025E,"   # Doctorate
    "B01003_001E,"   # Total population
    # ── Multifamily-investor signals ─────────────────────────────
    "B25003_001E,"   # Tenure — total occupied (denominator)
    "B25003_003E,"   # Tenure — renter-occupied (numerator for pct_renter)
    "B25024_001E,"   # Units in structure — total (denominator)
    "B25024_004E,"   # Units in structure — 2 units
    "B25024_005E,"   # Units in structure — 3-4 units
    "B25024_006E,"   # Units in structure — 5-9 units
    "B25024_007E,"   # Units in structure — 10-19 units
    "B25024_008E,"   # Units in structure — 20-49 units
    "B25024_009E,"   # Units in structure — 50+ units (numerator: sum of 2+ unit categories = pct_multi_unit)
    "B25070_001E,"   # Rent as % of income — total renter households
    "B25070_007E,"   # Rent burden 30-34.9%
    "B25070_008E,"   # Rent burden 35-39.9%
    "B25070_009E,"   # Rent burden 40-49.9%
    "B25070_010E,"   # Rent burden 50%+
    "B25070_011E"    # Rent burden — not computed (subtract from total before computing %)
)

# State FIPS → 2-letter code. Covers 50 + DC + the 5 territories that
# may show up in Census files. Anything else gets dropped.
STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}

# National median price-to-rent ratio used to impute rent for ZIPs
# that ZHVI tracks but ZORI doesn't. Updated occasionally. ZORI
# coverage is ~5K ZIPs vs ZHVI ~30K, so imputation matters.
NATIONAL_PRICE_TO_RENT = 17.0


# ─── Network helpers ────────────────────────────────────────────────
def _http_get(url: str, timeout: int = 180) -> bytes:
    """GET with retry on 5xx/429/network errors. 4xx still fails fast."""
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
    last_err: str | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if not (e.code >= 500 or e.code == 429):
                raise SystemExit(f"{last_err} fetching {url}")
        except urllib.error.URLError as e:
            last_err = f"Network error {e.reason}"
        if attempt < 2:
            time.sleep(3 * (attempt + 1))
    raise SystemExit(f"{last_err} fetching {url} (after 3 attempts)")


def fetch_text(url: str, label: str) -> str:
    log.info("Fetching %s …", label)
    return _http_get(url).decode("utf-8", errors="replace")


def fetch_zip_member(url: str, label: str, member_pattern: str) -> str:
    """Download a .zip and return the text of the matching member."""
    log.info("Fetching %s …", label)
    data = _http_get(url)
    z = ZipFile(io.BytesIO(data))
    for name in z.namelist():
        if re.search(member_pattern, name):
            return z.read(name).decode("utf-8", errors="replace")
    raise SystemExit(f"No member matching {member_pattern!r} in {url}")


# ─── Parsers ────────────────────────────────────────────────────────
def parse_zhvi_per_zip(csv_text: str) -> dict[str, dict]:
    """Parse Zillow ZHVI per-ZIP CSV. Returns
    {zip: {home_value, home_value_yoy?, state, city}}. Skips ZIPs with
    no recent non-empty value. State and city come from the CSV's own
    State/City columns — no external Census crosswalk needed."""
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    zip_idx = header.index("RegionName")
    state_idx = header.index("State") if "State" in header else None
    city_idx = header.index("City") if "City" in header else None
    county_idx = header.index("CountyName") if "CountyName" in header else None
    date_cols = sorted(
        [(i, h) for i, h in enumerate(header) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", h)],
        key=lambda x: x[1],
    )
    if not date_cols:
        raise SystemExit("ZHVI CSV had no date columns — schema changed?")
    out: dict[str, dict] = {}
    for row in reader:
        zcode = row[zip_idx].strip().zfill(5)
        if not (zcode.isdigit() and len(zcode) == 5):
            continue
        latest_val = None
        latest_pos = None
        for pos, (col_i, _) in enumerate(reversed(date_cols)):
            if col_i < len(row) and row[col_i]:
                try:
                    latest_val = float(row[col_i])
                    latest_pos = len(date_cols) - 1 - pos
                    break
                except ValueError:
                    continue
        if latest_val is None or latest_pos is None:
            continue
        prior_pos = latest_pos - 12
        prior_val = None
        if prior_pos >= 0:
            col_i = date_cols[prior_pos][0]
            if col_i < len(row) and row[col_i]:
                try:
                    prior_val = float(row[col_i])
                except ValueError:
                    pass
        entry: dict = {"home_value": int(round(latest_val))}
        if prior_val and prior_val > 0:
            entry["home_value_yoy"] = round((latest_val - prior_val) / prior_val * 100, 1)
        if state_idx is not None and state_idx < len(row):
            entry["state"] = row[state_idx].strip().upper()
        if city_idx is not None and city_idx < len(row):
            entry["city"] = row[city_idx].strip()
        if county_idx is not None and county_idx < len(row):
            entry["county"] = row[county_idx].strip()
        # Capture the trailing 60 monthly values for the forecast
        # helper. Drops empties / parse-errors silently — forecast just
        # works with whatever monotonic series we recover (≥12 needed).
        history: list[float] = []
        for col_i, _ in date_cols[-60:]:   # ~5 years of monthly data
            if col_i < len(row) and row[col_i]:
                try:
                    history.append(float(row[col_i]))
                except ValueError:
                    continue
        if len(history) >= 12:
            entry["history"] = history
        out[zcode] = entry
    log.info("  → %d ZIPs with ZHVI", len(out))
    return out


# ─── 12-month forecast (damped Holt-Winters, level + trend) ────────
# No statsmodels / Prophet dep — the math is 20 lines and runs in <1ms
# per ZIP. Damped trend (phi < 1) prevents the forecast from
# extrapolating wildly when the recent trend is steep; long-horizon
# growth tapers off, which matches the post-2022 cooling pattern.
#
# Parameters tuned for monthly-frequency, smoothed (ZHVI-style) data:
#   alpha = 0.4   — moderate weight on the latest observation
#   beta  = 0.1   — slow trend update; keeps forecasts stable
#   phi   = 0.92  — strong damping; 12-mo forecast settles at
#                    roughly trend × (1 - phi^12) / (1 - phi) ≈ 7×monthly
#
# Returns None when history < 12 — forecast would be unreliable.
# Tag the method in the returned dict so future versions (Prophet,
# ARIMA, ML) can A/B without breaking the API contract.
def forecast_home_value(history: list[float], alpha: float = 0.5,
                        beta: float = 0.15, phi: float = 0.98) -> dict | None:
    # Param tuning: phi=0.98 captures ~65-75% of recent trend over a
    # 12-month horizon; phi=0.92 (initial guess) was too aggressive
    # and projected only ~25%, missing real growth on a 5%-YoY series.
    # Conservative-but-not-flatlining matches the "directional, not
    # predictive" framing in the popup.
    if not history or len(history) < 12:
        return None
    # Init: level = first value, trend = avg first-12-month diff.
    level = history[0]
    trend_window = min(11, len(history) - 1)
    trend = (history[trend_window] - history[0]) / trend_window
    for i in range(1, len(history)):
        prev_level = level
        level = alpha * history[i] + (1 - alpha) * (level + phi * trend)
        trend = beta * (level - prev_level) + (1 - beta) * phi * trend
    if not history[-1]:
        return None
    # Multi-horizon: 3mo / 6mo / 12mo / 60mo (5yr) projections via the
    # geometric damping. Accumulate the damped trend at each horizon.
    horizons = (3, 6, 12, 60)
    out: dict = {"forecast_method": "damped_holt_v1"}
    forecast = level
    damp = 1.0
    for h in range(1, max(horizons) + 1):
        damp *= phi
        forecast += damp * trend
        if h in horizons:
            tag = f"forecast_{h}mo" if h < 60 else "forecast_60mo"
            if forecast <= 0:
                continue
            out[f"forecast_{h}mo_value"] = int(round(forecast))
            out[f"forecast_{h}mo_pct"] = round(
                (forecast / history[-1] - 1) * 100, 1
            )
    # Backwards-compat with P142's column names (used by /api/zips):
    if "forecast_12mo_value" in out:
        out["forecast_home_value_12mo"] = out["forecast_12mo_value"]
        out["forecast_pct_change_12mo"] = out["forecast_12mo_pct"]
    return out


def parse_zori_per_zip(csv_text: str) -> dict[str, int]:
    """Parse Zillow ZORI per-ZIP CSV. Returns {zip: median_rent_monthly}."""
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    zip_idx = header.index("RegionName")
    date_cols = sorted(
        [(i, h) for i, h in enumerate(header) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", h)],
        key=lambda x: x[1],
    )
    out: dict[str, int] = {}
    for row in reader:
        zcode = row[zip_idx].strip().zfill(5)
        if not (zcode.isdigit() and len(zcode) == 5):
            continue
        for col_i, _ in reversed(date_cols):
            if col_i < len(row) and row[col_i]:
                try:
                    out[zcode] = int(round(float(row[col_i])))
                    break
                except ValueError:
                    continue
    log.info("  → %d ZIPs with ZORI", len(out))
    return out


def parse_gazetteer(text: str) -> dict[str, dict]:
    """Parse the 2020 ZCTA Gazetteer (tab-delimited). Returns
    {zip: {lat, lng, aland_km2}}. The file has no state column; the
    ZCTA→state crosswalk fills that in."""
    out: dict[str, dict] = {}
    for i, line in enumerate(text.splitlines()):
        if i == 0:
            continue
        parts = line.split("\t")
        # Columns: GEOID, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI, INTPTLAT, INTPTLONG
        if len(parts) < 7:
            continue
        zcode = parts[0].strip().zfill(5)
        try:
            aland_km2 = float(parts[1]) / 1_000_000.0
            lat = float(parts[5])
            lng = float(parts[6])
        except (ValueError, IndexError):
            continue
        out[zcode] = {"lat": lat, "lng": lng, "aland_km2": aland_km2}
    log.info("  → %d ZCTAs in Gazetteer", len(out))
    return out


def fetch_acs_zcta() -> dict[str, dict]:
    """Census ACS 2022 5-year API call — one bulk request returns all
    ~33K ZCTAs. Keys: median_household_income, pct_bachelors, population.
    Census null markers (negative values) become None.

    Census now requires an API key for ACS bulk queries (used to be
    optional). Set CENSUS_API_KEY as a GitHub repo secret + workflow
    env var. Free signup: https://api.census.gov/data/key_signup.html

    Census occasionally returns a non-JSON body (HTML throttle page or
    a truncated empty body) with a 2xx status. Retry up to 3× on JSON
    parse failure with linear backoff before giving up."""
    api_key = os.environ.get("CENSUS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit(
            "CENSUS_API_KEY is not set. Get one free at "
            "https://api.census.gov/data/key_signup.html and add it as a "
            "GitHub repo secret + workflow env var."
        )
    url = (
        f"{ACS_API}?get={ACS_VARS}"
        f"&for=zip%20code%20tabulation%20area:*"
        f"&key={api_key}"
    )
    log.info("Fetching Census ACS 2022 5-year ZCTA data …")
    rows = None
    last_err: str | None = None
    for attempt in range(3):
        raw = _http_get(url)
        try:
            rows = json.loads(raw)
            break
        except json.JSONDecodeError as e:
            preview = raw[:120].decode("utf-8", errors="replace").replace("\n", " ")
            last_err = f"{e}; body starts with: {preview!r}"
            log.warning("Census ACS returned non-JSON on attempt %d/3: %s", attempt + 1, last_err)
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
    if rows is None:
        raise SystemExit(f"Census ACS API kept returning non-JSON after 3 attempts: {last_err}")
    headers = rows[0]
    zcta_idx = headers.index("zip code tabulation area")
    inc_idx = headers.index("B19013_001E")
    edu_total_idx = headers.index("B15003_001E")
    ba_idx = headers.index("B15003_022E")
    ma_idx = headers.index("B15003_023E")
    prof_idx = headers.index("B15003_024E")
    doc_idx = headers.index("B15003_025E")
    pop_idx = headers.index("B01003_001E")
    # Multifamily signal indices (B25003 tenure, B25024 units in
    # structure, B25070 gross rent as % of income).
    ten_tot_idx   = headers.index("B25003_001E")
    ten_rent_idx  = headers.index("B25003_003E")
    units_tot_idx = headers.index("B25024_001E")
    units_2_idx   = headers.index("B25024_004E")
    units_34_idx  = headers.index("B25024_005E")
    units_59_idx  = headers.index("B25024_006E")
    units_1019_idx= headers.index("B25024_007E")
    units_2049_idx= headers.index("B25024_008E")
    units_50p_idx = headers.index("B25024_009E")
    rb_tot_idx     = headers.index("B25070_001E")
    rb_30_idx      = headers.index("B25070_007E")
    rb_35_idx      = headers.index("B25070_008E")
    rb_40_idx      = headers.index("B25070_009E")
    rb_50_idx      = headers.index("B25070_010E")
    rb_notcomp_idx = headers.index("B25070_011E")

    def _int(v):
        try:
            n = int(v)
        except (ValueError, TypeError):
            return None
        return n if n >= 0 else None

    out: dict[str, dict] = {}
    for row in rows[1:]:
        zcode = row[zcta_idx].strip().zfill(5)
        income = _int(row[inc_idx])
        edu_total = _int(row[edu_total_idx])
        ba_count = sum((_int(row[i]) or 0) for i in (ba_idx, ma_idx, prof_idx, doc_idx))
        pct_bach = (ba_count / edu_total * 100) if (edu_total and edu_total > 0) else None

        # ── Multifamily signals ─────────────────────────────────────
        ten_tot = _int(row[ten_tot_idx])
        ten_rent = _int(row[ten_rent_idx])
        pct_renter = (ten_rent / ten_tot * 100) if (ten_tot and ten_rent is not None) else None

        units_tot = _int(row[units_tot_idx])
        multi_count = sum((_int(row[i]) or 0) for i in
                          (units_2_idx, units_34_idx, units_59_idx,
                           units_1019_idx, units_2049_idx, units_50p_idx))
        pct_multi = (multi_count / units_tot * 100) if (units_tot and units_tot > 0) else None

        # Rent burden denominator excludes "not computed" households so the
        # percentage reflects share of *renters with computable burden* that
        # are paying 30%+. Matches how HUD/Census typically report it.
        rb_tot = _int(row[rb_tot_idx]) or 0
        rb_notcomp = _int(row[rb_notcomp_idx]) or 0
        rb_denom = rb_tot - rb_notcomp
        rb_30plus = sum((_int(row[i]) or 0) for i in
                        (rb_30_idx, rb_35_idx, rb_40_idx, rb_50_idx))
        pct_rent_burdened = (rb_30plus / rb_denom * 100) if rb_denom > 0 else None

        out[zcode] = {
            "median_household_income": income,
            "pct_bachelors": round(pct_bach, 1) if pct_bach is not None else None,
            "population": _int(row[pop_idx]),
            "pct_renter_occupied": round(pct_renter, 1) if pct_renter is not None else None,
            "pct_multi_unit": round(pct_multi, 1) if pct_multi is not None else None,
            "pct_rent_burdened": round(pct_rent_burdened, 1) if pct_rent_burdened is not None else None,
        }
    log.info("  → %d ZCTAs with ACS data", len(out))
    return out


# ─── Proxy helpers ──────────────────────────────────────────────────
def walk_proxy(density: float | None) -> float:
    """Population density (people per km²) → walk-score proxy 10-90.
    Saturating curve. Real Walk Score correlates ~0.7 with log-density
    across cities, which is good enough for a first-pass national
    surface. Hand-curated metros override this with measured Walk
    Score values."""
    if density is None or density <= 0:
        return 25.0
    return min(90.0, 10.0 + 80.0 * (1 - 1 / (1 + density / 1500.0)))


def restaurant_proxy(walk: float) -> float:
    """Restaurant density tracks walkability closely enough for a
    proxy; scale walk-score with a bottom cutoff so rural ZIPs zero
    out instead of carrying an artificial 'urban-lite' restaurant
    score."""
    return max(0.0, (walk - 20) * 1.3)


def crime_proxy(density: float | None, income: int | None, pct_bach: float | None) -> float:
    """Heuristic crime index 0-100 (lower=safer) derived from socioeconomic
    inputs we already have per ZIP. Crime correlates strongly (in aggregate)
    with population density (urban property crime), low income (more
    desperate environments), and low education (compounding risk factor).

    This is NOT real crime data — it's a directionally-correct proxy that
    differentiates ZIPs based on factors that DO predict crime. Phase B
    of the crime work layers in FBI UCR county anchors so each ZIP is
    calibrated against its county's real per-100K rate; this proxy then
    provides within-county variation.

    Three sub-factors, each 0-1, weighted blend onto a 15-75 output range:
      density:  log10 scale, 0 at <30/km² (rural), 1 at 30K+/km² (NYC core)
      income:   inverse linear, 1 at $30K, 0 at $200K+
      edu:      inverse linear, 1 at <10% bachelor's+, 0 at >70%

    Output baseline ~25 (suburban-mid) lets hand-curated ZIPs (which
    range 18-65 in DALLAS_ZIPS etc.) overlap meaningfully when both
    flow through the same compute_zip_metrics scoring pipeline.
    """
    import math
    # Density factor — log scale because crime scales sub-linearly with
    # density, not linearly (LA 3K/km² isn't 6x the crime of suburb 500/km²).
    if density is None or density <= 0:
        density_f = 0.0
    else:
        density_f = max(0.0, min(1.0, (math.log10(max(density, 1)) - 1.5) / 3.0))
    # Income factor — inverse linear. National median ~$70K. Map $30K→1.0,
    # $200K→0.0. Wealthy ZIPs (Moraga $200K+) drop crime below baseline.
    inc = income if income else 70000
    income_f = max(0.0, min(1.0, (200000 - inc) / 170000.0))
    # Education factor — bachelor's rate as a compounding signal.
    # National avg ~33%, top metros ~60%, lowest <15%. Map 10%→1.0, 70%→0.
    bach = pct_bach if pct_bach is not None else 30.0
    edu_f = max(0.0, min(1.0, (70.0 - bach) / 60.0))
    # Weighted blend. Density and income carry equal weight (both strong
    # predictors); education adds a smaller corrective. Output range 15-75.
    blend = 0.40 * density_f + 0.40 * income_f + 0.20 * edu_f
    return round(15.0 + blend * 60.0, 1)


def impute_rent(home_value: int) -> int:
    """Estimate monthly rent from home_value when ZORI doesn't cover
    the ZIP. Uses a national price-to-rent of ~17 (annual). Rough but
    directionally right. Marked rent_source='imputed' in the DB so
    consumers can flag it."""
    return int(round(home_value / NATIONAL_PRICE_TO_RENT / 12))


# ─── DB write ───────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE zips (
    zip                      TEXT PRIMARY KEY,
    state                    TEXT,
    name                     TEXT,    -- 'City, ST' from Zillow ZHVI
    county                   TEXT,    -- 'Franklin County' (etc) from Zillow ZHVI CountyName
    neighborhood             TEXT,    -- e.g. 'Short North' — populated by enrich_neighborhoods.py
    lat                      REAL,
    lng                      REAL,
    aland_km2                REAL,
    population               INTEGER,
    population_density       REAL,
    median_home_value        INTEGER,
    home_value_yoy           REAL,
    median_rent_monthly      INTEGER,
    rent_source              TEXT,    -- 'zori' or 'imputed'
    median_household_income  INTEGER,
    pct_bachelors            REAL,
    -- Multifamily-investor signals (Census ACS 2022 5-yr). Populated
    -- on the next refresh-national-zips run; NULL on older DBs.
    --   pct_renter_occupied:    share of occupied units that are
    --                           rented (high = tenant pool already
    --                           exists).
    --   pct_multi_unit:         share of housing stock in 2+ unit
    --                           buildings (high = existing density).
    --   pct_rent_burdened:      share of renters paying 30%+ of
    --                           income on rent (low = stable tenants).
    pct_renter_occupied      REAL,
    pct_multi_unit           REAL,
    pct_rent_burdened        REAL,
    walk_score               REAL,
    crime_index              REAL,
    restaurant_score         REAL,
    cap_rate_pct             REAL,
    composite_balanced       REAL,
    composite_investor       REAL,
    composite_lifestyle      REAL,
    composite_score          REAL,
    -- 12-month forward forecast (Phase A of paid feature). Damped
    -- Holt-Winters on Zillow ZHVI history. Null when ZIP has too
    -- little history (<12 months). 'method' tags the model used so
    -- future versions can A/B without breaking the API contract.
    forecast_home_value_12mo INTEGER,
    forecast_pct_change_12mo REAL,
    -- Additional horizons (P143). Same model, projected forward N
    -- months. NULL when history < 12 OR forecast went non-positive.
    forecast_3mo_value       INTEGER,
    forecast_3mo_pct         REAL,
    forecast_6mo_value       INTEGER,
    forecast_6mo_pct         REAL,
    forecast_60mo_value      INTEGER,
    forecast_60mo_pct        REAL,
    -- Trailing 60 monthly ZHVI values, JSON-encoded list (oldest →
    -- newest). Powers the historical chart on /zip/{zip}. ~600
    -- bytes/ZIP × 25K = ~15MB extra in zips.db, acceptable.
    history_zhvi             TEXT,
    forecast_method          TEXT,
    as_of                    TEXT
);
-- Indexes the Phase-2 viewport endpoint will use:
--   * by-state for state-zoom lists
--   * (lat, lng) for bbox queries
--   * composite_balanced DESC for top-N within a region
CREATE INDEX idx_zips_state     ON zips(state);
CREATE INDEX idx_zips_latlng    ON zips(lat, lng);
CREATE INDEX idx_zips_composite ON zips(composite_balanced DESC);
"""


def build_db(rows: list[dict], dry_run: bool) -> None:
    if dry_run:
        log.info("--dry-run: would write %d rows to %s", len(rows), DB_PATH)
        for r in rows[:3]:
            log.info(
                "  %s  %s  $%s · $%s/mo · cap=%.1f%% · bal=%.1f",
                r["zip"], r["state"], r["median_home_value"],
                r["median_rent_monthly"], r["cap_rate_pct"],
                r["composite_balanced"],
            )
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    cols = list(rows[0].keys())
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT INTO zips ({','.join(cols)}) VALUES ({placeholders})"
    conn.executemany(sql, [[r.get(c) for c in cols] for r in rows])
    conn.commit()
    # VACUUM must run outside a transaction. Compacts + reclaims space
    # so the committed file stays as small as possible (the GitHub
    # Action commits zips.db on each monthly refresh).
    conn.execute("VACUUM")
    conn.close()
    size_mb = DB_PATH.stat().st_size / 1_000_000
    log.info("Wrote %d rows to %s (%.2f MB)", len(rows), DB_PATH, size_mb)


# ─── Main ───────────────────────────────────────────────────────────
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + parse + score, but don't write zips.db.",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Stop after N rows. 0 = all (default).",
    )
    args = parser.parse_args(argv)

    # Pull all sources up-front so we fail fast if any feed is broken.
    zhvi = parse_zhvi_per_zip(fetch_text(ZHVI_URL, "Zillow ZHVI per ZIP"))
    zori = parse_zori_per_zip(fetch_text(ZORI_URL, "Zillow ZORI per ZIP"))
    gaz = parse_gazetteer(fetch_zip_member(
        GAZETTEER_URL, "Census 2020 ZCTA Gazetteer",
        r"2020_Gaz_zcta_national\.txt$",
    ))
    acs = fetch_acs_zcta()

    # Load the neighborhood enrichment cache. enrich_neighborhoods.py
    # populates this incrementally via Photon reverse-geocoding — each
    # ZIP gets a sub-city locality name like "Short North" or "Bexley"
    # where OSM has it tagged. ZIPs without a cache hit (rural, or not
    # enriched yet) just miss the field; popup falls back to City+County.
    neighborhoods_path = REPO_ROOT / "data" / "zip_neighborhoods.json"
    neighborhoods: dict[str, str] = {}
    if neighborhoods_path.exists():
        try:
            payload = json.loads(neighborhoods_path.read_text())
            neighborhoods = payload.get("neighborhoods", {})
            log.info("Loaded %d cached neighborhood names", len(neighborhoods))
        except (json.JSONDecodeError, OSError):
            log.warning("Could not load %s — proceeding without neighborhoods", neighborhoods_path)

    log.info("Joining feeds …")
    rows: list[dict] = []
    skipped = {"no_centroid": 0, "no_income": 0}
    today = date.today().isoformat()
    forecast_count = 0
    for z, zh in zhvi.items():
        # Run the forecast in the join loop so we don't have to
        # re-iterate later. Stores result on zh under '_forecast' for
        # the row-build below to pick up. None when history < 12.
        if zh.get("history"):
            f = forecast_home_value(zh["history"])
            if f:
                zh["_forecast"] = f
                forecast_count += 1
        g = gaz.get(z)
        if not g:
            skipped["no_centroid"] += 1
            continue
        a = acs.get(z, {})
        income = a.get("median_household_income")
        if not income:
            skipped["no_income"] += 1
            continue
        pct_bach = a.get("pct_bachelors")
        if pct_bach is None:
            pct_bach = 30.0   # rough national fallback for missing edu data
        pop = a.get("population") or 0
        density = pop / g["aland_km2"] if g["aland_km2"] > 0 else 0
        walk = walk_proxy(density)
        rest = restaurant_proxy(walk)
        crime = crime_proxy(density, income, pct_bach)
        rent = zori.get(z)
        rent_source = "zori" if rent else "imputed"
        if not rent:
            rent = impute_rent(zh["home_value"])
        # Same compute_zip_metrics call real metros use → composite
        # numbers land on the same scale as DALLAS_ZIPS, HOUSTON_ZIPS, etc.
        m = compute_zip_metrics({
            "median_home_value": zh["home_value"],
            "median_rent_monthly": rent,
            "median_household_income": income,
            "crime_index": crime,
            "pct_bachelors": pct_bach,
            "walk_score": walk,
            "restaurant_score": rest,
        })
        # State + city are in the ZHVI CSV directly. State is a 2-letter
        # code; we whitelist against the 50+DC set so we don't carry
        # territories Zillow lists separately. City lets us label rows
        # as e.g. "Dallas, TX" instead of a bare "ZCTA 75201".
        state = zh.get("state", "")
        if state and state not in STATE_FIPS.values():
            state = ""
        city = zh.get("city", "")
        name = f"{city}, {state}" if (city and state) else f"ZCTA {z}"
        rows.append({
            "zip": z,
            "state": state,
            "name": name,
            "county": zh.get("county", ""),
            "neighborhood": neighborhoods.get(z, ""),
            "lat": g["lat"],
            "lng": g["lng"],
            "aland_km2": round(g["aland_km2"], 3),
            "population": pop,
            "population_density": round(density, 1),
            "median_home_value": zh["home_value"],
            "home_value_yoy": zh.get("home_value_yoy"),
            "median_rent_monthly": rent,
            "rent_source": rent_source,
            "median_household_income": income,
            "pct_bachelors": pct_bach,
            # Multifamily fields from ACS — may be None on ZIPs with
            # tiny renter populations / suppressed Census counts.
            "pct_renter_occupied": a.get("pct_renter_occupied"),
            "pct_multi_unit": a.get("pct_multi_unit"),
            "pct_rent_burdened": a.get("pct_rent_burdened"),
            "walk_score": round(walk, 1),
            "crime_index": crime,
            "restaurant_score": round(rest, 1),
            "cap_rate_pct": m["cap_rate_pct"],
            "composite_balanced": m["composite_by_persona"]["balanced"],
            "composite_investor": m["composite_by_persona"]["investor"],
            "composite_lifestyle": m["composite_by_persona"]["lifestyle"],
            "composite_score": m["composite_score"],
            # Phase-A forecast — 12-month forward home value via damped
            # Holt-Winters on the trailing ZHVI history. None when the
            # ZIP has too little history (<12 months); popup hides the
            # row when the field is null.
            "forecast_home_value_12mo": (zh.get("_forecast") or {}).get("forecast_home_value_12mo"),
            "forecast_pct_change_12mo": (zh.get("_forecast") or {}).get("forecast_pct_change_12mo"),
            "forecast_method":          (zh.get("_forecast") or {}).get("forecast_method"),
            # Multi-horizon forecasts (P143) — null when no _forecast.
            "forecast_3mo_value":  (zh.get("_forecast") or {}).get("forecast_3mo_value"),
            "forecast_3mo_pct":    (zh.get("_forecast") or {}).get("forecast_3mo_pct"),
            "forecast_6mo_value":  (zh.get("_forecast") or {}).get("forecast_6mo_value"),
            "forecast_6mo_pct":    (zh.get("_forecast") or {}).get("forecast_6mo_pct"),
            "forecast_60mo_value": (zh.get("_forecast") or {}).get("forecast_60mo_value"),
            "forecast_60mo_pct":   (zh.get("_forecast") or {}).get("forecast_60mo_pct"),
            # Persist the history so /zip/{zip} can chart it. JSON-encoded
            # list of values, oldest first. None when the ZIP doesn't
            # have a history (rare; mostly newly-added ZIPs).
            "history_zhvi": (json.dumps([round(v, 0) for v in zh["history"]]) if zh.get("history") else None),
            "as_of": today,
        })
        if args.limit and len(rows) >= args.limit:
            break

    log.info(
        "Built %d rows · skipped %d no-centroid · %d no-income · %d with 12mo forecast",
        len(rows), skipped["no_centroid"], skipped["no_income"], forecast_count,
    )
    if not rows:
        log.error("No rows produced — aborting.")
        return 1
    # Quick coverage report — useful for spotting feed regressions.
    rs_counts: dict[str, int] = {}
    state_counts: dict[str, int] = {}
    for r in rows:
        rs_counts[r["rent_source"]] = rs_counts.get(r["rent_source"], 0) + 1
        state_counts[r["state"]] = state_counts.get(r["state"], 0) + 1
    log.info("Rent source: %s", rs_counts)
    log.info("States covered: %d", len([s for s in state_counts if s]))
    build_db(rows, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
