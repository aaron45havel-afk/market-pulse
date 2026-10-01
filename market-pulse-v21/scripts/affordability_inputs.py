"""Inputs for the affordability page's fixed baseline (scripts/build_zip_profile.py).

THE BASELINE IS A FIXED YEAR, 2019 — the last full year before the
pandemic and the 2020-21 rate collapse. It does not roll forward with the
data, so "today against 2019" means the same thing next year as this year.
Each input is that year's own figure:

  * price   — the ZIP's 2019 average Zillow ZHVI (all twelve months, or none)
  * income  — Census ACS 2015-2019 5-year median household income, in 2019
              dollars. It does not overlap the 2020-2024 estimate the page
              uses for today, which is the Census Bureau's rule for
              comparing two 5-year periods.
  * rate    — the 2019 average of Freddie Mac's weekly 30-year rate
  * CPI     — 2019 and the current ACS end year's averages, and the latest
              month, so today's Census income can be brought to today's
              dollars (assuming no real growth since).

Census and FRED are fetched in GitHub Actions; the sandbox cannot reach
either. The parsers are pure and tested on fixtures.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger(__name__)

BASELINE_YEAR = 2019
ACS19_VINTAGE = "2015-2019 5-year"

# Census null and annotation codes. -666666666 is "no estimate"; a margin of
# -333333333 marks a median in an open-ended interval ("$250,000+").
NO_ESTIMATE = -666666666
MOE_OPEN = -333333333

CENSUS_URL = "https://api.census.gov/data/{year}/acs/acs5"
FRED_API = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
UA = {"User-Agent": "MarketPulse/1.0 (affordability baseline)"}

STATE_FIPS = ("01 02 04 05 06 08 09 10 11 12 13 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 "
              "31 32 33 34 35 36 37 38 39 40 41 42 44 45 46 47 48 49 50 51 53 54 55 56").split()


class SourceUnavailable(RuntimeError):
    pass


# ── Zillow ────────────────────────────────────────────────────────────

def annual_average(rec: dict | None, year: int = BASELINE_YEAR) -> float | None:
    """The mean of one calendar year's twelve monthly values from a
    parse_zillow record, or None unless all twelve are present."""
    if not rec:
        return None
    y0, m0 = int(rec["start"][:4]), int(rec["start"][5:7])
    first = (year - y0) * 12 + (1 - m0)
    if first < 0 or first + 12 > len(rec["vals"]):
        return None
    vals = rec["vals"][first:first + 12]
    if any(v is None for v in vals):
        return None
    return sum(vals) / 12


# ── Census ACS 2015-2019 median household income ─────────────────────

def parse_census_income(rows: list[list], national_median: float | None = None) -> dict:
    """Census API rows (header first) → {zcta: {"income": v, "coded": side}}.

    "No estimate" is None. An open-interval median keeps its bound as the
    value, with coded = "top" or "bottom" (read against the national median),
    so a page can print "$250,000+" rather than a measured $250,001."""
    if not rows:
        return {}
    head = rows[0]
    try:
        iv, im = head.index("B19013_001E"), head.index("B19013_001M")
        iz = head.index("zip code tabulation area")
    except ValueError as e:
        raise ValueError(f"unexpected Census header {head}") from e
    raw = {}
    for r in rows[1:]:
        z = str(r[iz]).zfill(5)
        try:
            v = int(float(r[iv])) if r[iv] not in (None, "") else None
            m = int(float(r[im])) if r[im] not in (None, "") else None
        except ValueError:
            continue
        if v is None or v <= NO_ESTIMATE or v < 0:
            raw[z] = (None, None)
        else:
            raw[z] = (v, m)
    if national_median is None:
        vals = [v for v, _ in raw.values() if v is not None]
        national_median = statistics.median(vals) if vals else None
    out = {}
    for z, (v, m) in raw.items():
        coded = None
        if v is not None and m == MOE_OPEN:
            coded = "top" if (national_median is None or v >= national_median) else "bottom"
        out[z] = {"income": v, "coded": coded}
    return out


def _get_json(url: str, attempts: int = 3, timeout: int = 300):
    """Errors name the response, never the URL — a keyed URL must not reach
    a log. The Census API answers some refusals (a bad key, an unsupported
    geography) with HTTP 200 and an HTML or text page, so a body that is not
    JSON is reported by its first characters and not retried."""
    last = None
    for k in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
                body = r.read()
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}: {e.read()[:160]!r}"
            if e.code == 400:        # a malformed query does not improve on retry
                raise SourceUnavailable(last) from e
        except (urllib.error.URLError, TimeoutError) as e:
            last = str(e)
        else:
            try:
                return json.loads(body)
            except ValueError:
                raise SourceUnavailable(f"not JSON ({len(body)} bytes): {body[:160]!r}") from None
        time.sleep(3 * (k + 1))
    raise SourceUnavailable(last or "unknown")


def fetch_acs19_income(min_rows: int = 30_000) -> dict:
    """All ZCTAs' 2015-2019 median household income. Keyless first (one
    national call is well inside the keyless allowance), then — only if
    that fails — with CENSUS_API_KEY. Each try is one national call, or one
    call per state if the API wants ZCTAs nested in states for this vintage."""
    base = CENSUS_URL.format(year=BASELINE_YEAR)
    q = {"get": "B19013_001E,B19013_001M", "for": "zip code tabulation area:*"}
    key = os.environ.get("CENSUS_API_KEY", "").strip()
    errors = []
    for label, extra in (("keyless", {}), ("keyed", {"key": key} if key else None)):
        if extra is None:
            continue
        try:
            try:
                rows = _get_json(f"{base}?{urllib.parse.urlencode({**q, **extra})}")
            except SourceUnavailable as e:
                errors.append(f"{label} national: {e}")
                rows = None
                for st in STATE_FIPS:
                    part = _get_json(f"{base}?{urllib.parse.urlencode({**q, **extra, 'in': f'state:{st}'})}")
                    rows = part if rows is None else rows + part[1:]
            out = parse_census_income(rows or [])
        except (SourceUnavailable, ValueError) as e:
            errors.append(f"{label} by state: {e}")
            continue
        if len(out) < min_rows:
            errors.append(f"{label}: only {len(out):,} ZCTAs (floor {min_rows:,})")
            continue
        log.info("  ACS %s income: %s, %d ZCTAs", ACS19_VINTAGE, label, len(out))
        return out
    raise SourceUnavailable("; ".join(errors))


# ── FRED: CPI and the 30-year mortgage rate ──────────────────────────

def parse_fred_csv(text: str) -> dict:
    """fredgraph.csv → {'YYYY-MM-DD': float}; '.' (missing) is skipped."""
    out = {}
    reader = csv.reader(io.StringIO(text))
    next(reader, None)
    for row in reader:
        if len(row) < 2:
            continue
        try:
            out[row[0].strip()] = float(row[1])
        except ValueError:
            continue
    return out


def fetch_fred(series: str, start: str = "2018-01-01") -> dict:
    """{'YYYY-MM-DD': value}. The API when FRED_API_KEY is set, else FRED's
    keyless graph CSV."""
    key = os.environ.get("FRED_API_KEY", "").strip()
    if key:
        q = urllib.parse.urlencode({"series_id": series, "api_key": key, "file_type": "json",
                                    "observation_start": start})
        obs = _get_json(f"{FRED_API}?{q}").get("observations", [])
        out = {}
        for o in obs:
            try:
                out[o["date"]] = float(o["value"])
            except (KeyError, ValueError):
                continue
        return out
    last = None
    for k in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(FRED_CSV.format(series=series),
                                                               headers=UA), timeout=120) as r:
                return {d: v for d, v in parse_fred_csv(r.read().decode()).items() if d >= start}
        except (urllib.error.URLError, TimeoutError) as e:
            last = str(e)
            time.sleep(3 * (k + 1))
    raise SourceUnavailable(f"FRED {series}: {last}")


def year_mean(obs: dict, year: int, min_n: int) -> float | None:
    """Mean of the observations dated in `year`, or None with fewer than
    `min_n` (12 for monthly CPI, 50 for the weekly mortgage rate)."""
    vals = [v for d, v in obs.items() if d.startswith(f"{year}-")]
    return round(sum(vals) / len(vals), 3) if len(vals) >= min_n else None


def macro(cpi: dict, pmms: dict, acs_end_year: int) -> dict:
    """The page's fixed constants, each named by what it is."""
    latest = max(cpi) if cpi else None
    return {
        "baseline_year": BASELINE_YEAR,
        "pmms": {str(BASELINE_YEAR): year_mean(pmms, BASELINE_YEAR, 50)},
        "cpi": {str(BASELINE_YEAR): year_mean(cpi, BASELINE_YEAR, 12),
                str(acs_end_year): year_mean(cpi, acs_end_year, 12),
                "latest": cpi.get(latest) if latest else None,
                "latest_month": latest[:7] if latest else None},
        "source": "FRED CPIAUCSL (CPI-U, monthly) and MORTGAGE30US (Freddie Mac PMMS, weekly)",
    }


def macro_complete(m: dict | None, acs_end_year: int) -> bool:
    if not m:
        return False
    c = m.get("cpi") or {}
    return all(v is not None for v in (
        (m.get("pmms") or {}).get(str(BASELINE_YEAR)), c.get(str(BASELINE_YEAR)),
        c.get(str(acs_end_year)), c.get("latest")))
