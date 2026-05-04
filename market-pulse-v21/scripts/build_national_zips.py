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
import re
import sqlite3
import sys
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
ZCTA_STATE_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
    "tab20_zcta520_state20_natl.txt"
)
ACS_API = "https://api.census.gov/data/2022/acs/acs5"
ACS_VARS = (
    "B19013_001E,"   # Median household income
    "B15003_001E,"   # Pop 25+ (denominator for bachelor's %)
    "B15003_022E,"   # Bachelor's
    "B15003_023E,"   # Master's
    "B15003_024E,"   # Professional
    "B15003_025E,"   # Doctorate
    "B01003_001E"    # Total population
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
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        raise SystemExit(f"HTTP {e.code} fetching {url}")
    except urllib.error.URLError as e:
        raise SystemExit(f"Network error fetching {url}: {e.reason}")


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
    """Parse Zillow ZHVI per-ZIP CSV. Returns {zip: {home_value,
    home_value_yoy?}}. Skips ZIPs with no recent non-empty value."""
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    zip_idx = header.index("RegionName")
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


def parse_zcta_state(text: str) -> dict[str, str]:
    """Parse the ZCTA→state relationship file (pipe-delimited). Some
    ZCTAs straddle state lines; we keep the first record we see, which
    is the state with the most overlap (file is sorted that way)."""
    out: dict[str, str] = {}
    for i, line in enumerate(text.splitlines()):
        if i == 0:
            continue
        parts = line.split("|")
        if len(parts) < 4:
            continue
        zcode = parts[0].strip().zfill(5)
        state_fips = parts[2].strip().zfill(2)
        if zcode in out:
            continue
        code = STATE_FIPS.get(state_fips)
        if code:
            out[zcode] = code
    log.info("  → %d ZCTAs with state code", len(out))
    return out


def fetch_acs_zcta() -> dict[str, dict]:
    """Census ACS 2022 5-year API call — one bulk request returns all
    ~33K ZCTAs. Keys: median_household_income, pct_bachelors, population.
    Census null markers (negative values) become None."""
    url = f"{ACS_API}?get={ACS_VARS}&for=zip%20code%20tabulation%20area:*"
    log.info("Fetching Census ACS 2022 5-year ZCTA data …")
    raw = _http_get(url)
    rows = json.loads(raw)
    headers = rows[0]
    zcta_idx = headers.index("zip code tabulation area")
    inc_idx = headers.index("B19013_001E")
    edu_total_idx = headers.index("B15003_001E")
    ba_idx = headers.index("B15003_022E")
    ma_idx = headers.index("B15003_023E")
    prof_idx = headers.index("B15003_024E")
    doc_idx = headers.index("B15003_025E")
    pop_idx = headers.index("B01003_001E")

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
        out[zcode] = {
            "median_household_income": income,
            "pct_bachelors": round(pct_bach, 1) if pct_bach is not None else None,
            "population": _int(row[pop_idx]),
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


def crime_proxy() -> float:
    """No state-level crime feed is reliably free at ZCTA scale (FBI
    UCR is county; PD open-data isn't national). Default to the 50
    mid-baseline so crime_safety contributes a neutral signal until a
    real source lands. Hand-curated metros override per ZIP."""
    return 50.0


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
    name                     TEXT,
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
    walk_score               REAL,
    crime_index              REAL,
    restaurant_score         REAL,
    cap_rate_pct             REAL,
    composite_balanced       REAL,
    composite_investor       REAL,
    composite_lifestyle      REAL,
    composite_score          REAL,
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
    zcta_state = parse_zcta_state(fetch_text(
        ZCTA_STATE_URL, "Census 2020 ZCTA→state crosswalk",
    ))
    acs = fetch_acs_zcta()

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
        crime = crime_proxy()
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
        rows.append({
            "zip": z,
            "state": zcta_state.get(z, ""),
            "name": f"ZCTA {z}",
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
            "walk_score": round(walk, 1),
            "crime_index": crime,
            "restaurant_score": round(rest, 1),
            "cap_rate_pct": m["cap_rate_pct"],
            "composite_balanced": m["composite_by_persona"]["balanced"],
            "composite_investor": m["composite_by_persona"]["investor"],
            "composite_lifestyle": m["composite_by_persona"]["lifestyle"],
            "composite_score": m["composite_score"],
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
    build_db(rows, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
