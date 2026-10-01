#!/usr/bin/env python3
"""Build data/zip_profile.db — the one dataset behind the rebuilt map and ZIP page.

WHY A NEW FILE. data/zips.db is read by eleven other modules (multifamily,
headroom, fair_value, value_add, norcal ...) and carries columns the rebuild
retires: persona composites, a damped-Holt forecast that does not beat
"last year repeats", and walk/restaurant/crime "scores" computed from
density, income and education. Changing it in place would move every one
of those pages at once. This file is built beside it and owns nothing else.

WHAT IS DIFFERENT, AND WHY:

  * EVERY CENSUS ZCTA IN THE 50 STATES + DC, not only the ~25.7k Zillow
    values. State and county come from the Census ZCTA-to-county
    relationship file (largest land share), so a ZIP Zillow does not cover
    still has its Census housing, rent, tax and risk figures.
  * CENSUS PLACEHOLDERS ARE NOT NUMBERS. A median in an open-ended interval
    ("$250,000+", "built 1939 or earlier") is stored as NULL with the bound
    and direction in acs_flags, read from the margin-of-error annotation.
    Margins of error are kept for the medians a reader will compare.
  * MEASURED INPUTS FOR UNDERWRITING: the ZIP's effective property-tax rate
    (median taxes paid ÷ median owner value), rental and homeowner vacancy,
    rent by bedroom (Census and HUD), owner costs, renter and owner incomes,
    and the state tax/insurance defaults from re_assumptions.py.
  * ZILLOW WITH DATES. Every value carries the month it is for; trend
    figures (1, 3, 5, 10 years, peak and drawdown) come from the full series.
    The series themselves go to data/zip_series.db for charts, one
    compressed row per ZIP so a page reads only its own.
  * RISK FROM REAL SOURCES: FEMA National Risk Index expected annual loss
    and NOAA 1991-2020 normals, joined from the files already in the repo.

No composite, no score, no forecast. Run in GitHub Actions — the sandbox
cannot reach Census or Zillow.

    python scripts/build_zip_profile.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import re
import sqlite3
import statistics
import sys
import time
import urllib.error
import urllib.request
import zipfile
import zlib
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import acs_bulk as AB  # noqa: E402
import re_assumptions as RA  # noqa: E402
import zip_market as ZM  # noqa: E402

log = logging.getLogger("zip_profile")

OUT_DB = ROOT / "data" / "zip_profile.db"
SERIES_OUT = ROOT / "data" / "zip_series.db"
ZIPS_DB = ROOT / "data" / "zips.db"
HAZARDS = ROOT / "data" / "zip_hazards.json"
CLIMATE = ROOT / "data" / "zip_climate.json"

GAZETTEER_URL = ("https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
                 "2020_Gazetteer/2020_Gaz_zcta_national.zip")
ZCTA_COUNTY_URL = ("https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
                   "zcta520/tab20_zcta520_county20_natl.txt")
ZILLOW = "https://files.zillowstatic.com/research/public_csvs"
ZHVI_URL = f"{ZILLOW}/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
ZHVI_BR_URL = f"{ZILLOW}/zhvi/Zip_zhvi_bdrmcnt_{{n}}_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
ZORI_URL = f"{ZILLOW}/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"
UA = {"User-Agent": "MarketPulse/1.0 (zip profile; invoice@archfms.com)"}

# 50 states + DC. Puerto Rico and the territories have Census ZCTAs but no
# Zillow, HUD-by-ZIP or NRI parity with the states, so they are left out
# rather than shown half-measured.
STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT",
    "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL",
    "18": "IN", "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
    "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO", "30": "MT", "31": "NE",
    "32": "NV", "33": "NH", "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA", "54": "WV",
    "55": "WI", "56": "WY",
}

# ── Census ACS 5-year: the tables and the variables read from them ───
ACS_TABLES = ("b01003", "b19013", "b25119", "b15003", "b25003", "b25002", "b25004",
              "b25024", "b25035", "b25077", "b25103", "b25064", "b25031", "b25071",
              "b25088", "b25070")
# Medians: kept with their margin of error, and checked for placeholders.
ACS_MEDIANS = {
    "B19013_001E": "acs_median_income",
    "B25119_002E": "acs_owner_income",
    "B25119_003E": "acs_renter_income",
    "B25077_001E": "acs_median_value",
    "B25103_001E": "acs_median_taxes",
    "B25064_001E": "acs_gross_rent",
    "B25031_002E": "acs_rent_br0",
    "B25031_003E": "acs_rent_br1",
    "B25031_004E": "acs_rent_br2",
    "B25031_005E": "acs_rent_br3",
    "B25031_006E": "acs_rent_br4",
    "B25031_007E": "acs_rent_br5",
    "B25071_001E": "acs_rent_pct_income",
    "B25088_002E": "acs_owner_cost_mortgaged",
    "B25035_001E": "acs_year_built",
}
# Margins kept as columns for the medians a reader compares directly.
MOE_COLUMNS = ("B19013_001E", "B25077_001E", "B25103_001E", "B25064_001E")
ACS_COUNTS = (
    "B01003_001E",
    "B15003_001E", "B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E",
    "B25003_001E", "B25003_002E", "B25003_003E",
    "B25002_001E", "B25002_003E",
    "B25004_002E", "B25004_003E", "B25004_004E", "B25004_005E",
    "B25024_001E", "B25024_002E", "B25024_003E", "B25024_004E", "B25024_005E",
    "B25024_006E", "B25024_007E", "B25024_008E", "B25024_009E", "B25024_010E",
    "B25070_001E", "B25070_007E", "B25070_008E", "B25070_009E", "B25070_010E", "B25070_011E",
)
ACS_WANTED = set(ACS_MEDIANS) | set(ACS_COUNTS)

# Vacancy is a ratio of small counts in small ZIPs; below this many units
# in its denominator the rate is noise and is stored as NULL.
VACANCY_MIN_BASE = 50

SERIES_MONTHS = 120   # monthly ZHVI kept for charts: ten years


# ═══════════════════════════════════════════════════════════════════
# PURE PARSERS
# ═══════════════════════════════════════════════════════════════════

def parse_gazetteer(text: str) -> dict:
    """2020 ZCTA Gazetteer (tab-delimited) → {zip: {lat, lng, aland_km2}}."""
    out = {}
    lines = text.splitlines()
    if not lines:
        return out
    head = [h.strip() for h in lines[0].split("\t")]
    try:
        i_geo, i_land = head.index("GEOID"), head.index("ALAND")
        i_lat, i_lng = head.index("INTPTLAT"), head.index("INTPTLONG")
    except ValueError as e:
        raise ValueError(f"Gazetteer header changed: {head}") from e
    for line in lines[1:]:
        p = [x.strip() for x in line.split("\t")]
        if len(p) <= max(i_geo, i_land, i_lat, i_lng):
            continue
        try:
            out[p[i_geo].zfill(5)] = {"lat": float(p[i_lat]), "lng": float(p[i_lng]),
                                      "aland_km2": float(p[i_land]) / 1e6}
        except ValueError:
            continue
    return out


def parse_zcta_county(text: str) -> dict:
    """Census 2020 ZCTA-to-county relationship file (pipe-delimited) →
    {zip: {state, county_fips, county}}, each ZIP assigned to the county
    holding the largest share of its land."""
    lines = text.splitlines()
    head = [h.strip() for h in lines[0].lstrip("﻿").split("|")]

    def col(name):
        try:
            return head.index(name)
        except ValueError as e:
            raise ValueError(f"ZCTA-county header has no {name}: {head}") from e
    iz, ic = col("GEOID_ZCTA5_20"), col("GEOID_COUNTY_20")
    iname, iland = col("NAMELSAD_COUNTY_20"), col("AREALAND_PART")
    best: dict = {}
    for line in lines[1:]:
        p = line.split("|")
        if len(p) <= max(iz, ic, iname, iland):
            continue
        z, cf = p[iz].strip(), p[ic].strip()
        if not (len(z) == 5 and z.isdigit() and len(cf) == 5):
            continue
        try:
            land = float(p[iland] or 0)
        except ValueError:
            land = 0.0
        st = STATE_FIPS.get(cf[:2])
        if not st:
            continue
        if z not in best or land > best[z][0]:
            best[z] = (land, {"state": st, "county_fips": cf, "county": p[iname].strip()})
    return {z: v for z, (_, v) in best.items()}


def parse_zillow(text: str, keep_months: int | None = None) -> dict:
    """A Zillow ZIP CSV → {zip: {start, vals, city, state, county, metro}}.

    `vals` runs monthly from `start` ('YYYY-MM', the first non-empty month)
    to the last non-empty month, with None where a month is blank inside
    that span. `keep_months` trims to the last N months (start moves)."""
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    zi = header.index("RegionName")
    meta = {k: header.index(k) for k in ("City", "State", "CountyName", "Metro") if k in header}
    dates = [(i, h[:7]) for i, h in enumerate(header) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", h)]
    dates.sort(key=lambda x: x[1])
    if not dates:
        raise ValueError("Zillow CSV has no date columns")
    out = {}
    for row in reader:
        if zi >= len(row):
            continue
        z = row[zi].strip().zfill(5)
        if not (len(z) == 5 and z.isdigit()):
            continue
        vals = []
        for i, _m in dates:
            v = row[i].strip() if i < len(row) else ""
            try:
                vals.append(float(v) if v else None)
            except ValueError:
                vals.append(None)
        first = next((k for k, v in enumerate(vals) if v is not None), None)
        if first is None:
            continue
        last = max(k for k, v in enumerate(vals) if v is not None)
        if keep_months is not None:
            first = max(first, last - keep_months + 1)
        rec = {"start": dates[first][1], "vals": vals[first:last + 1]}
        for k, i in meta.items():
            rec[k.lower()] = row[i].strip() if i < len(row) else ""
        out[z] = rec
    return out


def month_add(ym: str, n: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    t = y * 12 + (m - 1) + n
    return f"{t // 12:04d}-{t % 12 + 1:02d}"


def _back(vals: list, months: int):
    """The value `months` before the last one, or None."""
    k = len(vals) - 1 - months
    return vals[k] if 0 <= k < len(vals) else None


def _chg(now, then):
    return round((now / then - 1) * 100, 1) if (now and then) else None


def zillow_summary(rec: dict) -> dict:
    """Latest value and month, 1/3/5/10-year change, and the all-series
    peak with the drawdown from it. Changes need the exact month back, not
    the nearest available one: a blank month gives None, not a guess."""
    vals, start = rec["vals"], rec["start"]
    now = vals[-1]
    peak_i = max(range(len(vals)), key=lambda k: vals[k] if vals[k] is not None else -1)
    out = {"latest": now, "month": month_add(start, len(vals) - 1),
           "yoy_pct": _chg(now, _back(vals, 12)), "chg_3y_pct": _chg(now, _back(vals, 36)),
           "chg_5y_pct": _chg(now, _back(vals, 60)), "chg_10y_pct": _chg(now, _back(vals, 120)),
           "peak": vals[peak_i], "peak_month": month_add(start, peak_i)}
    out["from_peak_pct"] = _chg(now, out["peak"])
    return out


def decode_acs(rec: dict, medians_of: dict) -> tuple[dict, dict]:
    """One ZCTA's ACS record (parsed with keep_moe) → (values, flags).

    A median whose margin carries the open-interval code is a placeholder:
    the value becomes None and flags[col] = {"bound": value, "side": top or
    bottom}. Side is read against that variable's national median, which is
    where every top-code sits above and every bottom-code below."""
    vals, flags = {}, {}
    for var, col in ACS_MEDIANS.items():
        v = rec.get(var)
        if v is not None and AB.is_open_interval(rec, var):
            med = medians_of.get(var)
            side = "top" if (med is None or v >= med) else "bottom"
            flags[col] = {"bound": v, "side": side}
            v = None
        vals[col] = v
        if var in MOE_COLUMNS:
            m = rec.get(var[:-1] + "M")
            vals[col + "_moe"] = m if (m is not None and m >= 0) else None
    return vals, flags


def _share(num, den):
    return round(num / den * 100, 1) if (num is not None and den) else None


def derive_counts(r: dict) -> dict:
    """Shares and vacancy from the ACS counts. Census vacancy definitions:
    rental = vacant-for-rent ÷ (renter-occupied + for-rent + rented-not-
    occupied); homeowner = for-sale-only ÷ (owner-occupied + for-sale-only +
    sold-not-occupied). Below VACANCY_MIN_BASE units the rate is NULL."""
    g = r.get
    s = lambda *ks: sum((g(k) or 0) for k in ks)  # noqa: E731
    units = g("B25024_001E")
    edu = g("B15003_001E")
    rent_base = s("B25003_003E", "B25004_002E", "B25004_003E")
    own_base = s("B25003_002E", "B25004_004E", "B25004_005E")
    rb_den = (g("B25070_001E") or 0) - (g("B25070_011E") or 0)
    return {
        "population": g("B01003_001E"),
        "households": g("B25003_001E"),
        "pct_bachelors": _share(s("B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E"), edu),
        "pct_renter": _share(g("B25003_003E"), g("B25003_001E")),
        "pct_vacant": _share(g("B25002_003E"), g("B25002_001E")),
        "rental_vacancy_pct": (_share(g("B25004_002E"), rent_base)
                               if rent_base >= VACANCY_MIN_BASE else None),
        "rental_vacancy_base": rent_base or None,
        "owner_vacancy_pct": (_share(g("B25004_004E"), own_base)
                              if own_base >= VACANCY_MIN_BASE else None),
        "pct_sfr_detached": _share(g("B25024_002E"), units),
        "pct_sfr_attached": _share(g("B25024_003E"), units),
        "pct_2_4_units": _share(s("B25024_004E", "B25024_005E"), units),
        "pct_5plus_units": _share(s("B25024_006E", "B25024_007E", "B25024_008E", "B25024_009E"), units),
        "pct_mobile": _share(g("B25024_010E"), units),
        "pct_rent_burdened": (_share(s("B25070_007E", "B25070_008E", "B25070_009E", "B25070_010E"),
                                     rb_den) if rb_den > 0 else None),
    }


def national_medians(acs: dict) -> dict:
    """{var: national median of the ZCTA values} for the median variables —
    the reference decode_acs reads top versus bottom against."""
    out = {}
    for var in ACS_MEDIANS:
        v = [r[var] for r in acs.values() if r.get(var) is not None]
        if v:
            out[var] = statistics.median(v)
    return out


# ═══════════════════════════════════════════════════════════════════
# FETCHING (Actions only)
# ═══════════════════════════════════════════════════════════════════

def _get(url: str, attempts: int = 3, timeout: int = 600) -> bytes:
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = e
            log.warning("  %s failed (%s), retry %d", url, e, i + 1)
            time.sleep(5 * (i + 1))
    raise SystemExit(f"could not fetch {url}: {last}")


def _text(url: str) -> str:
    log.info("GET %s", url)
    return _get(url).decode("utf-8", "replace")


def _zip_member(url: str) -> str:
    log.info("GET %s", url)
    with zipfile.ZipFile(io.BytesIO(_get(url))) as zf:
        name = next(n for n in zf.namelist() if n.endswith(".txt"))
        return zf.read(name).decode("utf-8", "replace")


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _zips_db_rents() -> dict:
    """HUD rent by bedroom and the Photon neighbourhood name, from zips.db
    (refresh_rents owns the HUD calls; this build does not repeat them)."""
    if not ZIPS_DB.exists():
        return {}
    c = sqlite3.connect(f"file:{ZIPS_DB}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    cols = {r[1] for r in c.execute("PRAGMA table_info(zips)")}
    want = [k for k in ("neighborhood", "rent_br0", "rent_br1", "rent_br2", "rent_br3",
                        "rent_br4", "rent_bedroom_tier", "rent_as_of") if k in cols]
    out = {r["zip"]: dict(r) for r in c.execute(f"SELECT zip, {', '.join(want)} FROM zips")}
    c.close()
    return out


# ═══════════════════════════════════════════════════════════════════
# SCHEMA AND BUILD
# ═══════════════════════════════════════════════════════════════════

COLUMNS = [
    # identity
    ("zip", "TEXT PRIMARY KEY"), ("state", "TEXT"), ("county_fips", "TEXT"), ("county", "TEXT"),
    ("city", "TEXT"), ("metro", "TEXT"), ("neighborhood", "TEXT"),
    ("lat", "REAL"), ("lng", "REAL"), ("aland_km2", "REAL"), ("density", "REAL"),
    # Zillow
    ("zhvi", "INTEGER"), ("zhvi_month", "TEXT"), ("zhvi_yoy_pct", "REAL"),
    ("zhvi_3y_pct", "REAL"), ("zhvi_5y_pct", "REAL"), ("zhvi_10y_pct", "REAL"),
    ("zhvi_peak", "INTEGER"), ("zhvi_peak_month", "TEXT"), ("zhvi_from_peak_pct", "REAL"),
    ("zhvi_2br", "INTEGER"), ("zhvi_3br", "INTEGER"), ("zhvi_4br", "INTEGER"),
    ("zori", "INTEGER"), ("zori_month", "TEXT"), ("zori_yoy_pct", "REAL"),
    # HUD (via zips.db)
    ("hud_rent_br0", "INTEGER"), ("hud_rent_br1", "INTEGER"), ("hud_rent_br2", "INTEGER"),
    ("hud_rent_br3", "INTEGER"), ("hud_rent_br4", "INTEGER"), ("hud_rent_tier", "TEXT"),
    # Census counts and shares
    ("population", "INTEGER"), ("households", "INTEGER"), ("pct_bachelors", "REAL"),
    ("pct_renter", "REAL"), ("pct_vacant", "REAL"), ("rental_vacancy_pct", "REAL"),
    ("rental_vacancy_base", "INTEGER"), ("owner_vacancy_pct", "REAL"),
    ("pct_sfr_detached", "REAL"), ("pct_sfr_attached", "REAL"), ("pct_2_4_units", "REAL"),
    ("pct_5plus_units", "REAL"), ("pct_mobile", "REAL"), ("pct_rent_burdened", "REAL"),
    # Census medians (+ margins)
    *[(c, "REAL") for c in ACS_MEDIANS.values()],
    *[(ACS_MEDIANS[v] + "_moe", "REAL") for v in MOE_COLUMNS],
    ("acs_flags", "TEXT"),
    # derived ratios
    ("price_to_income", "REAL"), ("price_to_rent", "REAL"),
    # tax and insurance defaults
    ("tax_rate_acs", "REAL"), ("tax_rate_zip", "REAL"), ("tax_rate_zip_basis", "TEXT"),
    ("tax_rate_investor", "REAL"), ("tax_basis_investor", "TEXT"),
    ("tax_rate_owner", "REAL"), ("tax_basis_owner", "TEXT"),
    ("ins_landlord_300k", "REAL"), ("ins_owner_300k", "REAL"),
    # FEMA NRI expected annual building loss, $/yr per $100k of building value
    ("haz_flood", "REAL"), ("haz_wildfire", "REAL"), ("haz_wind", "REAL"),
    ("haz_quake", "REAL"), ("haz_total", "REAL"),
    # NOAA 1991-2020 normals at the nearest qualifying station
    ("clim_winter_low", "REAL"), ("clim_summer_high", "REAL"), ("clim_days_90", "REAL"),
    ("clim_nights_32", "REAL"), ("clim_snow_in", "REAL"), ("clim_station_km", "REAL"),
    # Realtor.com listings, latest month (this ZIP, then its county)
    *[(c, "TEXT" if c.endswith("_month") else "REAL") for c in (
        "rdc_month", "rdc_active", "rdc_active_yoy_pct", "rdc_new", "rdc_pending",
        "rdc_pending_ratio", "rdc_dom", "rdc_dom_yoy_pct", "rdc_price_cut_pct",
        "rdc_price_cut_yoy_pp", "rdc_list_price", "rdc_list_price_yoy_pct", "rdc_list_ppsf",
        "rdc_list_sqft", "rdc_quality_flag", "rdc_thin",
        "cty_rdc_month", "cty_rdc_active", "cty_rdc_active_yoy_pct", "cty_rdc_pending_ratio",
        "cty_rdc_dom", "cty_rdc_price_cut_pct", "cty_rdc_list_price", "cty_rdc_list_price_yoy_pct")],
    # Redfin sales, latest 90-day window (frozen at 2026-05-31; see zip_market.py)
    *[(c, "TEXT" if c.endswith(("_end", "_begin")) else "REAL") for c in (
        "rf_period_begin", "rf_period_end", "rf_sale_price", "rf_sale_price_yoy_pct",
        "rf_homes_sold", "rf_sale_ppsf", "rf_sale_to_list_pct", "rf_sold_above_list_pct",
        "rf_months_supply", "rf_sold_dom", "rf_thin", "rf_sfr_sale_price", "rf_sfr_homes_sold",
        "rf_mf24_sale_price", "rf_mf24_homes_sold")],
]
COLUMN_NAMES = [c for c, _ in COLUMNS]


MARKET_PREFIXES = ("rdc_", "cty_rdc_", "rf_")
RDC_COUNTY_KEEP = ("month", "active", "active_yoy_pct", "pending_ratio", "dom", "price_cut_pct",
                   "list_price", "list_price_yoy_pct")
REDFIN_KEEP = ("period_begin", "period_end", "sale_price", "sale_price_yoy_pct", "homes_sold",
               "sale_ppsf", "sale_to_list_pct", "sold_above_list_pct", "months_supply", "sold_dom",
               "thin", "sfr_sale_price", "sfr_homes_sold", "mf24_sale_price", "mf24_homes_sold")


def apply_market(row: dict, rdc_zip: dict, rdc_county: dict, redfin: dict) -> None:
    """Copy one ZIP's market figures onto its row, each source under its own
    prefix so a page can never mistake a listing figure for a sale figure,
    or a county figure for the ZIP's."""
    z = rdc_zip.get(row["zip"])
    if z:
        for k, v in z.items():
            row[f"rdc_{k}"] = int(v) if k == "thin" else v
    c = rdc_county.get(row.get("county_fips") or "")
    if c:
        for k in RDC_COUNTY_KEEP:
            row[f"cty_rdc_{k}"] = c.get(k)
    f = redfin.get(row["zip"])
    if f:
        for k in REDFIN_KEEP:
            v = f.get(k)
            row[f"rf_{k}"] = int(v) if (k == "thin" and v is not None) else v


def carry_forward(rows: list[dict], prior: dict, prefixes: tuple) -> int:
    """Fill the columns under `prefixes` from the previous build, unchanged
    and still carrying their own month — a failed fetch shows last month's
    figures with last month's date, never blanks and never a fresh date."""
    n = 0
    for r in rows:
        old = prior.get(r["zip"])
        if not old:
            continue
        for k, v in old.items():
            if k.startswith(prefixes):
                r[k] = v
        n += 1
    return n


def _prior_rows() -> dict:
    if not OUT_DB.exists():
        return {}
    c = sqlite3.connect(f"file:{OUT_DB}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    try:
        return {r["zip"]: dict(r) for r in c.execute("SELECT * FROM zip_profile")}
    except sqlite3.Error:
        return {}
    finally:
        c.close()


def build_rows(geo: dict, county: dict, acs: dict, zhvi: dict, zhvi_br: dict, zori: dict,
               rents: dict, hazards: dict, climate: dict, market: dict | None = None) -> list[dict]:
    """Join everything onto the Census ZCTA list. Pure: every input is a dict.
    `market` = {"rdc_zip": ..., "rdc_county": ..., "redfin": ...}, any may be {}."""
    med = national_medians(acs)
    mk = market or {}
    rows = []
    for z, g in sorted(geo.items()):
        cty = county.get(z)
        if not cty:            # outside the 50 states + DC
            continue
        a = acs.get(z, {})
        vals, flags = decode_acs(a, med)
        counts = derive_counts(a)
        zh, zo, rt = zhvi.get(z), zori.get(z), rents.get(z, {})
        row = {k: None for k in COLUMN_NAMES}
        row.update(zip=z, state=cty["state"], county_fips=cty["county_fips"],
                   county=cty["county"], lat=round(g["lat"], 6), lng=round(g["lng"], 6),
                   aland_km2=round(g["aland_km2"], 3))
        row.update(vals)
        row.update(counts)
        row["acs_flags"] = json.dumps(flags, separators=(",", ":")) if flags else None
        if counts["population"] and g["aland_km2"] > 0:
            row["density"] = round(counts["population"] / g["aland_km2"], 1)
        if zh:
            s = zillow_summary(zh)
            row.update(zhvi=round(s["latest"]), zhvi_month=s["month"], zhvi_yoy_pct=s["yoy_pct"],
                       zhvi_3y_pct=s["chg_3y_pct"], zhvi_5y_pct=s["chg_5y_pct"],
                       zhvi_10y_pct=s["chg_10y_pct"], zhvi_peak=round(s["peak"]),
                       zhvi_peak_month=s["peak_month"], zhvi_from_peak_pct=s["from_peak_pct"],
                       city=zh.get("city") or None, metro=zh.get("metro") or None)
        for n in (2, 3, 4):
            rb = zhvi_br.get(n, {}).get(z)
            if rb:
                row[f"zhvi_{n}br"] = round(rb["vals"][-1])
        if zo:
            s = zillow_summary(zo)
            row.update(zori=round(s["latest"]), zori_month=s["month"], zori_yoy_pct=s["yoy_pct"])
        row["neighborhood"] = rt.get("neighborhood") or None
        for b in range(5):
            row[f"hud_rent_br{b}"] = rt.get(f"rent_br{b}")
        row["hud_rent_tier"] = rt.get("rent_bedroom_tier")
        # Ratios a professional reads first. Price-to-rent uses the market
        # asking rent only: a voucher or Census rent against a market value
        # is two different homes.
        inc = row["acs_median_income"]
        if row["zhvi"] and inc:
            row["price_to_income"] = round(row["zhvi"] / inc, 2)
        if row["zhvi"] and row["zori"]:
            row["price_to_rent"] = round(row["zhvi"] / (row["zori"] * 12), 2)
        row["tax_rate_acs"] = RA.zip_effective_rate(row["acs_median_taxes"], row["acs_median_value"])
        hz = hazards.get(z)
        if isinstance(hz, dict):
            parts = [hz.get(k) for k in ("fl", "wf", "wd", "eq")]
            row.update(haz_flood=parts[0], haz_wildfire=parts[1], haz_wind=parts[2], haz_quake=parts[3])
            if all(p is not None for p in parts):
                row["haz_total"] = round(sum(parts), 2)
        cl = climate.get(z)
        if isinstance(cl, dict):
            row.update(clim_winter_low=cl.get("wl"), clim_summer_high=cl.get("sh"),
                       clim_days_90=cl.get("d90"), clim_nights_32=cl.get("d32"),
                       clim_snow_in=cl.get("sn"), clim_station_km=cl.get("tk"))
        apply_market(row, mk.get("rdc_zip") or {}, mk.get("rdc_county") or {},
                     mk.get("redfin") or {})
        rows.append(row)

    apply_tax_defaults(rows)
    for r in rows:
        i = RA.insurance_defaults(r["state"])
        r.update(ins_landlord_300k=i["landlord_300k"], ins_owner_300k=i["owner_300k"])
    return rows


def zip_tax_rate(row: dict, county_med: dict, state_med: dict) -> tuple:
    """(rate %, basis) for one ZIP: the measured Census rate, or — when the
    Census top-coded its median taxes at "$10,000+" — an estimate.

    The top code is still information: median taxes of at least $10,001 on
    the ZIP's median value is a FLOOR on the rate. The estimate is the
    greater of that floor and the county's median measured rate (the
    state's when the county has none). 925 ZIPs on the 2024 vintage, 4.9%
    of residents, almost all in NY, NJ and CA; without this they fell
    through to the statewide rate, which put Manhattan on upstate's."""
    if row.get("tax_rate_acs") is not None:
        return row["tax_rate_acs"], "measured"
    flag = json.loads(row.get("acs_flags") or "{}").get("acs_median_taxes")
    value = row.get("acs_median_value")
    if flag and flag.get("side") == "top" and value:
        floor = flag["bound"] / value * 100
        ref = county_med.get(row.get("county_fips")) or state_med.get(row.get("state"))
        return round(max(floor, ref or 0), 3), "top-coded"
    return None, None


def apply_tax_defaults(rows: list[dict]) -> None:
    """Fill tax_rate_zip and the investor/owner defaults. Needs every row:
    county and state medians come from MEASURED rates only."""
    by_county: dict = {}
    for r in rows:
        if r.get("tax_rate_acs") is not None:
            by_county.setdefault(r.get("county_fips"), []).append(r["tax_rate_acs"])
    county_med = {k: statistics.median(v) for k, v in by_county.items()}
    st_med = RA.state_medians({r["zip"]: (r["state"], r.get("tax_rate_acs")) for r in rows})
    for r in rows:
        rate, basis = zip_tax_rate(r, county_med, st_med)
        r.update(tax_rate_zip=rate, tax_rate_zip_basis=basis)
        t = RA.tax_defaults(r["state"], rate, st_med.get(r["state"]), zip_basis=basis)
        r.update(tax_rate_investor=t["investor_pct"], tax_basis_investor=t["investor_basis"],
                 tax_rate_owner=t["owner_pct"], tax_basis_owner=t["owner_basis"])




def series_payload(zhvi: dict, zori: dict, meta: dict) -> dict:
    """Chart series: ZHVI in hundreds of dollars (a $100 step is below what
    a smoothed index can tell apart), ZORI in dollars; None for gaps."""
    def pack(d, div, keep):
        out = {}
        for z, rec in d.items():
            vals = rec["vals"][-keep:] if keep else rec["vals"]
            start = month_add(rec["start"], len(rec["vals"]) - len(vals))
            out[z] = [start, [None if v is None else round(v / div) for v in vals]]
        return out
    return {"_meta": {**meta, "zhvi_unit": "USD/100", "zori_unit": "USD"},
            "zhvi": pack(zhvi, 100, SERIES_MONTHS), "zori": pack(zori, 1, None)}


def write_series_db(payload: dict, path: Path) -> None:
    """Chart series → SQLite, one zlib-compressed JSON row per (zip, kind).

    A page needs one ZIP's series. A single gzip of all of them had to be
    decompressed and parsed whole (~150 MB of Python objects) to serve
    one; this is one indexed read and a few hundred bytes to inflate."""
    tmp = path.with_suffix(".tmp")
    if tmp.exists():
        tmp.unlink()
    c = sqlite3.connect(tmp)
    c.execute("CREATE TABLE series (zip TEXT, kind TEXT, start TEXT, data BLOB, "
              "PRIMARY KEY (zip, kind))")
    c.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    for kind in ("zhvi", "zori"):
        c.executemany("INSERT INTO series VALUES (?, ?, ?, ?)",
                      [(z, kind, start, zlib.compress(json.dumps(vals, separators=(",", ":")).encode(), 9))
                       for z, (start, vals) in payload.get(kind, {}).items()])
    c.executemany("INSERT INTO meta VALUES (?, ?)",
                  [(k, str(v)) for k, v in payload.get("_meta", {}).items()])
    c.commit()
    c.execute("VACUUM")
    c.close()
    tmp.replace(path)


def read_series(path: Path, zip_code: str) -> dict:
    """{kind: (start 'YYYY-MM', [values])} for one ZIP; ZHVI in USD/100."""
    if not path.exists():
        return {}
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return {k: (start, json.loads(zlib.decompress(blob)))
                for k, start, blob in c.execute(
                    "SELECT kind, start, data FROM series WHERE zip = ?", (zip_code,))}
    finally:
        c.close()


def write_db(rows: list[dict], meta: dict, path: Path) -> None:
    tmp = path.with_suffix(".tmp")
    if tmp.exists():
        tmp.unlink()
    c = sqlite3.connect(tmp)
    c.execute(f"CREATE TABLE zip_profile ({', '.join(f'{n} {t}' for n, t in COLUMNS)})")
    c.execute("CREATE INDEX idx_zp_state ON zip_profile(state)")
    c.execute("CREATE INDEX idx_zp_latlng ON zip_profile(lat, lng)")
    c.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    c.executemany(f"INSERT INTO zip_profile ({', '.join(COLUMN_NAMES)}) VALUES "
                  f"({', '.join('?' * len(COLUMN_NAMES))})",
                  [[r[k] for k in COLUMN_NAMES] for r in rows])
    c.executemany("INSERT INTO meta VALUES (?, ?)", [(k, str(v)) for k, v in meta.items()])
    c.commit()
    c.execute("VACUUM")
    c.close()
    tmp.replace(path)


def coverage(rows: list[dict]) -> dict:
    n = len(rows)
    keys = ("zhvi", "zori", "hud_rent_br2", "acs_median_value", "acs_median_taxes", "tax_rate_acs",
            "acs_gross_rent", "acs_median_income", "rental_vacancy_pct", "haz_total",
            "clim_winter_low", "rdc_month", "cty_rdc_month", "rf_period_end")
    return {"rows": n, **{k: sum(1 for r in rows if r.get(k) is not None) for k in keys},
            "acs_flagged": sum(1 for r in rows if r.get("acs_flags"))}


# Refuse to publish a board that lost most of its inputs. A partial outage
# must not overwrite a good file with a thin one.
FLOORS = {"rows": 32_000, "acs_median_value": 25_000, "zhvi": 24_000, "haz_total": 24_000}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="build and report; write nothing")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    t0 = time.time()

    geo = parse_gazetteer(_zip_member(GAZETTEER_URL))
    county = parse_zcta_county(_text(ZCTA_COUNTY_URL))
    log.info("  %d ZCTAs in the Gazetteer, %d in the 50 states + DC", len(geo), len(county))

    year = AB.latest_year(date.today().year)
    log.info("ACS %d 5-year, %d tables", year, len(ACS_TABLES))
    acs = AB.fetch(ACS_TABLES, year, ACS_WANTED, keep_moe=True)

    # What the margin-of-error annotations actually flag, per variable — the
    # first run's log is the evidence that the placeholder rule is right.
    for var in ACS_MEDIANS:
        jam = [r[var] for r in acs.values() if r.get(var) is not None and AB.is_open_interval(r, var)]
        if jam:
            log.info("  open-interval %s: %d ZCTAs, values %s", var, len(jam),
                     sorted(set(jam))[:6])

    zhvi = parse_zillow(_text(ZHVI_URL))
    zhvi_br = {n: parse_zillow(_text(ZHVI_BR_URL.format(n=n)), keep_months=1) for n in (2, 3, 4)}
    zori = parse_zillow(_text(ZORI_URL))
    log.info("  Zillow: %d ZHVI, %s by bedroom, %d ZORI", len(zhvi),
             {n: len(v) for n, v in zhvi_br.items()}, len(zori))

    # Market activity. A failed source is carried forward from the previous
    # build with its own dates; it never blocks the rest of the profile.
    market, failed, mmeta = {}, [], {}
    for key, fn, prefix in (("rdc_zip", lambda: ZM.fetch_rdc(ZM.RDC_ZIP_URL, "postal_code"), "rdc_"),
                            ("rdc_county", lambda: ZM.fetch_rdc(ZM.RDC_COUNTY_URL, "county_fips"), "cty_rdc_"),
                            ("redfin", ZM.fetch_redfin, "rf_")):
        try:
            market[key] = fn()
            log.info("  market %s: %d regions", key, len(market[key]))
        except Exception as e:  # noqa: BLE001 — any failure means carry forward
            print(f"::warning::market source {key} unavailable ({e}) — carrying forward")
            market[key], failed = {}, failed + [prefix]
    mmeta["rdc_last_modified"] = ZM.last_modified(ZM.RDC_ZIP_URL)
    mmeta["redfin_last_modified"] = ZM.last_modified(ZM.REDFIN_ZIP_URL)

    # Both files nest their ZIPs under "zips" beside "_meta".
    hazards, climate = _load_json(HAZARDS), _load_json(CLIMATE)
    rows = build_rows(geo, county, acs, zhvi, zhvi_br, zori, _zips_db_rents(),
                      hazards.get("zips", {}), climate.get("zips", {}), market)
    if failed:
        n = carry_forward(rows, _prior_rows(), tuple(failed))
        log.info("  carried %s forward for %d ZIPs", failed, n)
    cov = coverage(rows)
    log.info("coverage: %s", cov)
    short = {k: (cov[k], f) for k, f in FLOORS.items() if cov[k] < f}
    if short:
        log.error("refusing to publish — below floor: %s", short)
        return 1

    zhvi_months = sorted({r["zhvi_month"] for r in rows if r["zhvi_month"]})
    zori_months = sorted({r["zori_month"] for r in rows if r["zori_month"]})
    meta = {
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "acs_vintage": f"{year - 4}-{year} 5-year",
        "zhvi_last_month": zhvi_months[-1] if zhvi_months else "",
        "zori_last_month": zori_months[-1] if zori_months else "",
        "gazetteer": "2020 ZCTA", "zcta_county": "Census 2020 relationship file, largest land share",
        "hazards_as_of": (hazards.get("_meta") or {}).get("as_of", ""),
        "climate_as_of": (climate.get("_meta") or {}).get("as_of", ""),
        "hud_rents_from": "data/zips.db (refresh_rents.py)",
        "rdc_last_month": max((r["rdc_month"] for r in rows if r.get("rdc_month")), default=""),
        "redfin_last_period": max((r["rf_period_end"] for r in rows if r.get("rf_period_end")), default=""),
        "market_carried_forward": ",".join(failed),
        "market_attribution": "Listings: Realtor.com Economic Research. Sales: Redfin Data Center "
                              "(ZIP tracker frozen since 2026-06-02).",
        **mmeta,
        "coverage": json.dumps(cov),
        "elapsed_s": round(time.time() - t0),
    }
    if args.dry_run:
        log.info("--dry-run: %d rows, nothing written. meta %s", len(rows), meta)
        return 0
    write_db(rows, meta, OUT_DB)
    write_series_db(series_payload(zhvi, zori, {k: meta[k] for k in
                                                ("built_at", "zhvi_last_month", "zori_last_month")}),
                    SERIES_OUT)
    log.info("wrote %s (%.1f MB) and %s (%.1f MB) in %ds", OUT_DB.name,
             OUT_DB.stat().st_size / 1e6, SERIES_OUT.name, SERIES_OUT.stat().st_size / 1e6,
             time.time() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
