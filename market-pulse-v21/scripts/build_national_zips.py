"""Build the national ZIPs SQLite database from public, free, stable sources.

Output: ``data/zips.db`` — one row per ZCTA covered by Zillow ZHVI, with:
  - lat / lng / area / population              (Census 2020 ZCTA Gazetteer)
  - state                                       (Census 2020 ZCTA→state crosswalk)
  - median_home_value + home_value_yoy          (Zillow ZHVI per ZIP)
  - median_rent_monthly                         (Zillow ZORI per ZIP, or imputed)
  - median_household_income, pct_bachelors      (Census ACS 2022 5-year API)
  - walk_score, crime_index, restaurant_score   (proxies — see helpers; read
                                                 by /norcal and /value-add,
                                                 never by the ZIP map or page)
  - cap_rate_pct                                (rent_ladder.cap_rate_pct; the
                                                 rent ladder re-resolves it)
  - history_zhvi                                (trailing 60 months of ZHVI)

No composite score and no forecast: both were retired with the old map
(DECISIONS, map rebuild phase 5). The ZIP map and ZIP page read
data/zip_profile.db (scripts/build_zip_profile.py).

Readers: /multifamily, /headroom, /value-add, /norcal and /fair-value
(see each module); the ~30K ZIPs Zillow tracks nationally.

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

sys.path.insert(0, str(REPO_ROOT))
import rent_ladder as RL  # noqa: E402

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
    "B25070_011E,"   # Rent burden — not computed (subtract from total before computing %)
    # ── Value-add signals (age of housing stock) ─────────────────
    "B25034_001E,"   # Year built — total structures (denominator)
    "B25034_009E,"   # Year built 1950-1959
    "B25034_010E,"   # Year built 1940-1949
    "B25034_011E,"   # Year built 1939 or earlier
    "B25035_001E"    # Median year structure built
)
# Age 25–34 (young professional) — B01001, male and female 25–29 / 30–34.
ACS_AGE_VARS = ("B01001_001E", "B01001_011E", "B01001_012E", "B01001_035E", "B01001_036E")
# One keyless bulk file per table (acs_bulk.py). The vintage is the newest
# on the Census server, not a pinned year.
ACS_TABLES = ("b01003", "b19013", "b15003", "b25003", "b25024", "b25070",
              "b25034", "b25035", "b01001")

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
        # Capture the trailing 60 monthly values (history_zhvi), read by
        # /multifamily, /headroom and /fair-value for value trajectories.
        # Drops empties / parse-errors silently (≥12 needed to keep one).
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


def _load_acs_from_prior_db() -> dict[str, dict]:
    """Fallback: read the ACS columns from the previous zips.db so a
    Census outage / missing key doesn't tank the monthly refresh. ACS
    is an annual 5-yr survey — carrying values forward a month or two
    is harmless. Returns the same shape as fetch_acs_zcta().

    Defensive about schema drift: the multifamily columns
    (pct_renter_occupied, pct_multi_unit, pct_rent_burdened) were added
    recently, so a prior db built before that change won't have them.
    Pull only columns that actually exist; default the rest to None."""
    if not DB_PATH.exists():
        return {}
    wanted = [
        "median_household_income",
        "pct_bachelors",
        "population",
        "pct_renter_occupied",
        "pct_multi_unit",
        "pct_rent_burdened",
        "pct_pre_1960",
        "median_year_built",
        "pct_age_25_34",
        "pct_2_4_units",
    ]
    out: dict[str, dict] = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        existing = {r[1] for r in conn.execute("PRAGMA table_info(zips)")}
        available = [c for c in wanted if c in existing]
        if not available:
            conn.close()
            return {}
        cols_sql = ", ".join(["zip", *available])
        for row in conn.execute(f"SELECT {cols_sql} FROM zips"):
            zcode = row[0]
            entry = {c: None for c in wanted}
            for col, val in zip(available, row[1:]):
                entry[col] = val
            out[zcode] = entry
        conn.close()
    except sqlite3.Error as e:
        log.warning("Could not read prior ACS from zips.db: %s", e)
    return out


def derive_acs(v: dict) -> dict:
    """One ZCTA's ACS estimates (API spelling, e.g. "B25003_003E") → the
    zips.db columns. Missing or suppressed estimates are None and yield
    None, never zero."""
    def g(k):
        return v.get(k)

    def share(num, den):
        return round(num / den * 100, 1) if (den and den > 0 and num is not None) else None

    edu_total = g("B15003_001E")
    ba = sum((g(k) or 0) for k in ("B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E"))
    units_tot = g("B25024_001E")
    multi = sum((g(k) or 0) for k in ("B25024_004E", "B25024_005E", "B25024_006E",
                                      "B25024_007E", "B25024_008E", "B25024_009E"))
    two_four = sum((g(k) or 0) for k in ("B25024_004E", "B25024_005E"))
    # Rent burden: share of renters WITH a computable burden paying 30%+ —
    # "not computed" households come out of the denominator.
    rb_denom = (g("B25070_001E") or 0) - (g("B25070_011E") or 0)
    rb_30 = sum((g(k) or 0) for k in ("B25070_007E", "B25070_008E", "B25070_009E", "B25070_010E"))
    pre60 = sum((g(k) or 0) for k in ("B25034_009E", "B25034_010E", "B25034_011E"))
    yb_med = g("B25035_001E")
    age = sum((g(k) or 0) for k in ACS_AGE_VARS[1:])
    ten_rent = g("B25003_003E")
    return {
        "median_household_income": g("B19013_001E") or None,
        "pct_bachelors": share(ba, edu_total),
        "population": g("B01003_001E"),
        "pct_renter_occupied": share(ten_rent, g("B25003_001E")),
        "pct_multi_unit": share(multi, units_tot),
        "pct_2_4_units": share(two_four, units_tot),
        "pct_rent_burdened": share(rb_30, rb_denom),
        "pct_pre_1960": share(pre60, g("B25034_001E")),
        # Census uses 0 / 18xx sentinels for suppressed medians.
        "median_year_built": yb_med if yb_med and yb_med >= 1900 else None,
        "pct_age_25_34": share(age, g("B01001_001E")),
    }


def fetch_acs_zcta() -> dict[str, dict]:
    """Census ACS 5-year, every ZCTA, from the Bureau's keyless bulk files
    (acs_bulk.py) — the newest vintage on the server.

    The data API now requires a key, and a missing or invalid one made
    this step "carry forward" values that were already empty, month after
    month, while the job went green. The bulk files need no key. If they
    can't be read, the prior values are still carried so the rest of the
    refresh runs — but as a visible GitHub warning, not a quiet log line."""
    import acs_bulk as AB
    wanted = set(ACS_VARS.split(",")) | set(ACS_AGE_VARS)
    try:
        year = AB.latest_year(date.today().year)
        log.info("Fetching Census ACS %d 5-year (bulk files, %d tables) …", year, len(ACS_TABLES))
        raw = AB.fetch(ACS_TABLES, year, wanted)
    except AB.AcsUnavailable as e:
        print(f"::warning::Census ACS bulk files unavailable ({e}) — carrying "
              f"forward the prior zips.db values")
        prior = _load_acs_from_prior_db()
        log.info("  → %d ZCTAs carried forward from prior zips.db", len(prior))
        return prior
    out = {z: derive_acs(v) for z, v in raw.items()}
    log.info("  → %d ZCTAs with ACS %d data", len(out), year)
    return out


# ─── Proxy helpers ──────────────────────────────────────────────────
def walk_proxy(density: float | None) -> float:
    """Population density (people per km²) → walk-score proxy 10-90.
    Saturating curve. Real Walk Score correlates ~0.7 with log-density
    across cities. A density curve, not a walkability measurement —
    the ZIP map and ZIP page do not show it."""
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

    Output baseline ~25 (suburban-mid). A socioeconomic proxy, not a
    crime rate: boards that judge safety use FBI figures (safety.py).
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


# impute_rent() USED TO LIVE HERE AND HAS BEEN DELETED.
#
# It returned `home_value / 17 / 12` for any ZIP outside Zillow's ZORI
# file and wrote that to median_rent_monthly under rent_source='imputed'
# — 17,358 of 25,774 ZIPs, 67% of the country and 73% of Ohio.
#
# It was not "rough but directionally right". It was the home value
# divided by 204, carrying no information about rent whatsoever, and
# every yield derived from it was arithmetic performed on itself: across
# all 17,358 imputed ZIPs there were FIVE distinct cap rates, each 5.88%
# by construction. A $120k ZIP and an $890k ZIP got the same yield. The
# rent_source flag let consumers mark it, but a marked number that means
# nothing is still sitting on the page being read as a rent.
#
# Rent now comes from scripts/refresh_rents.py, which fills the ladder
# in rent_ladder.py from measured sources only — ZORI, HUD SAFMR/FMR,
# Census ACS B25064. A ZIP no source covers gets NULL and the board
# renders an absence. Do not reintroduce a fallback here: if coverage
# needs to go up, add a tier to the ladder.


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
    -- Multifamily-investor signals (Census ACS 5-yr, newest vintage,
    -- from the keyless bulk files — acs_bulk.py).
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
    pct_pre_1960             REAL,
    median_year_built        INTEGER,
    --   pct_age_25_34:          residents aged 25–34 (young professional)
    --   pct_2_4_units:          housing units in 2–4 unit buildings
    pct_age_25_34            REAL,
    pct_2_4_units            REAL,
    walk_score               REAL,
    crime_index              REAL,
    restaurant_score         REAL,
    cap_rate_pct             REAL,
    -- Trailing 60 monthly ZHVI values, JSON-encoded list (oldest →
    -- newest), for the value-trajectory reads on /multifamily,
    -- /headroom and /fair-value.
    history_zhvi             TEXT,
    as_of                    TEXT
);
CREATE INDEX idx_zips_state     ON zips(state);
CREATE INDEX idx_zips_latlng    ON zips(lat, lng);
"""


def snapshot_rent_ladder(path: Path) -> dict:
    """{zip: {column: value}} for the rent-ladder columns refresh_rents.py
    adds, read from the database about to be replaced.

    This build deletes zips.db and lays the rows down again, and those
    columns aren't part of its schema — so without this, every ZIP whose
    rent came from HUD lost it on the 1st of the month until the rent
    refresh on the 2nd, and lost it for a month if HUD failed that day."""
    import refresh_rents as R
    if not path.exists():
        return {}
    conn = sqlite3.connect(path)
    try:
        have = {r[1] for r in conn.execute("PRAGMA table_info(zips)")}
        cols = [c for c, _ in R.RENT_COLUMNS if c in have]
        if not cols:
            return {}
        return {r[0]: dict(zip(cols, r[1:])) for r in
                conn.execute(f"SELECT zip, {', '.join(cols)} FROM zips")}
    finally:
        conn.close()


def restore_rent_ladder(conn, snapshot: dict, zori: dict) -> dict | None:
    """Put the stored per-source rents back and re-resolve every ZIP.

    The stored HUD and ACS figures are carried; ZORI is this build's fresh
    pull, authoritative as always. Resolution is refresh_rents.apply —
    the same ladder, precedence and cap-rate arithmetic — so a rebuilt
    database reads exactly as the rent refresh left it, with this month's
    Zillow figures."""
    import refresh_rents as R
    if not snapshot:
        return None
    R.ensure_columns(conn)
    cols = sorted({c for rec in snapshot.values() for c in rec})
    have = {r[0] for r in conn.execute("SELECT zip FROM zips")}
    conn.executemany(
        f"UPDATE zips SET {', '.join(f'{c}=?' for c in cols)} WHERE zip=?",
        [[rec.get(c) for c in cols] + [z] for z, rec in snapshot.items() if z in have])
    conn.commit()
    _z, safmr, fmr, acs = R.carry_stored(conn)
    return R.apply(conn, zori, safmr, fmr, acs, date.today().isoformat(), dry_run=False)


def build_db(rows: list[dict], dry_run: bool, zori: dict | None = None) -> None:
    if dry_run:
        log.info("--dry-run: would write %d rows to %s", len(rows), DB_PATH)
        for r in rows[:3]:
            log.info(
                "  %s  %s  $%s · $%s/mo · cap=%s%%",
                r["zip"], r["state"], r["median_home_value"],
                r["median_rent_monthly"], r["cap_rate_pct"],
            )
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    ladder = snapshot_rent_ladder(DB_PATH)
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    cols = list(rows[0].keys())
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT INTO zips ({','.join(cols)}) VALUES ({placeholders})"
    conn.executemany(sql, [[r.get(c) for c in cols] for r in rows])
    conn.commit()
    cov = restore_rent_ladder(conn, ladder, zori or {})
    if cov:
        log.info("Rent ladder carried across the rebuild: %s measured (%s)",
                 f"{cov['real_pct']}%", cov["by_tier"])
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
        help="Fetch + parse, but don't write zips.db.",
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
    for z, zh in zhvi.items():
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
        # ZORI or nothing. The other tiers (HUD SAFMR/FMR, Census ACS)
        # are filled in afterwards by scripts/refresh_rents.py, which
        # runs on the 2nd — this build only ever lays down the rows.
        # NOTHING IS IMPUTED: a ZIP ZORI does not cover leaves here with
        # no rent, and refresh_rents decides whether any other source
        # can answer for it.
        rent = zori.get(z)
        rent_source = "zori" if rent else None
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
            "pct_pre_1960": a.get("pct_pre_1960"),
            "median_year_built": a.get("median_year_built"),
            "pct_age_25_34": a.get("pct_age_25_34"),
            "pct_2_4_units": a.get("pct_2_4_units"),
            "walk_score": round(walk, 1),
            "crime_index": crime,
            "restaurant_score": round(rest, 1),
            # Net of a flat 40% — the rent ladder's arithmetic, which
            # restore_rent_ladder re-runs on every ZIP after this insert.
            "cap_rate_pct": RL.cap_rate_pct(rent, zh["home_value"]),
            # JSON-encoded list of values, oldest first. None when the ZIP
            # doesn't have a history (rare; mostly newly-added ZIPs).
            "history_zhvi": (json.dumps([round(v, 0) for v in zh["history"]]) if zh.get("history") else None),
            "as_of": today,
        })
        if args.limit and len(rows) >= args.limit:
            break

    log.info(
        "Built %d rows · skipped %d no-centroid · %d no-income",
        len(rows), skipped["no_centroid"], skipped["no_income"],
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
    build_db(rows, args.dry_run, zori)
    return 0


if __name__ == "__main__":
    sys.exit(main())
