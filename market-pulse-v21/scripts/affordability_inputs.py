"""Inputs for the affordability page's fixed baseline (scripts/build_zip_profile.py).

THE BASELINE IS A FIXED YEAR, 2019 — the last full year before the
pandemic and the 2020-21 rate collapse. It does not roll forward with the
data, so "today against 2019" means the same thing next year as this year.
Each input is that year's own figure:

  * price   — the ZIP's 2019 average Zillow ZHVI (all twelve months, or none)
  * income  — Census ACS 2015-2019 5-year median household income, in 2019
              dollars, from the keyless summary file. It does not overlap the
              2020-2024 estimate the page uses for today, which is the Census
              Bureau's rule for comparing two 5-year periods.
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
import zipfile

log = logging.getLogger(__name__)

BASELINE_YEAR = 2019
ACS19_VINTAGE = "2015-2019 5-year"

# A negative estimate is a Census annotation ("no estimate"), never a value.
# A margin of -333333333 marks a median in an open-ended interval, as do
# B19013's jam values themselves.
MOE_OPEN = -333333333
JAM_TOP, JAM_BOTTOM = 250001, 2499     # "$250,000+" and "under $2,500"

# The 2015-2019 vintage predates the table-based bulk files acs_bulk.py reads
# (they begin with 2017-2021), and the Census API refuses keyless requests
# (this repo's key never activated — DECISIONS 2026-09-28). Its keyless form
# is the sequence-based summary file: a lookup says which sequence and column
# hold a table, a geography file maps record numbers to GEOIDs, and one zip
# per sequence holds the estimates and the margins.
SF19 = "https://www2.census.gov/programs-surveys/acs/summary_file/2019"
SF19_LOOKUP = f"{SF19}/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.txt"
SF19_DIR = f"{SF19}/data/5_year_seq_by_state/UnitedStates/All_Geographies_Not_Tracts_Block_Groups"
SF19_GEO = f"{SF19_DIR}/g20195us.csv"
SF19_SEQ = SF19_DIR + "/20195us{seq}000.zip"
ZCTA_SUMLEVEL, ZCTA_GEOID = "860", "86000US"

FRED_API = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
UA = {"User-Agent": "MarketPulse/1.0 (affordability baseline)"}


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

def sequence_position(lookup_text: str, table: str = "B19013") -> tuple[str, int]:
    """(sequence '0058', 0-based column) of a one-cell table's first cell,
    from the vintage's Seq_Table_Number_Lookup. 'Start Position' counts the
    six leading fields (FILEID … LOGRECNO), 1-based."""
    for row in csv.reader(io.StringIO(lookup_text)):
        if len(row) > 4 and row[1] == table and row[4].strip():
            return row[2].strip(), int(row[4]) - 1
    raise ValueError(f"{table} not in the sequence lookup")


def parse_geo(text: str) -> dict:
    """g20195us.csv → {LOGRECNO: zcta} for the ZCTA rows (summary level 860)."""
    out = {}
    for row in csv.reader(io.StringIO(text)):
        if len(row) > 4 and row[2] == ZCTA_SUMLEVEL:
            gid = next((f for f in row if f.startswith(ZCTA_GEOID)), None)
            if gid:
                out[row[4]] = gid[len(ZCTA_GEOID):len(ZCTA_GEOID) + 5]
    return out


def _cell(row: list, col: int):
    try:
        return int(float(row[col]))
    except (IndexError, ValueError):
        return None


def parse_sequence(e_text: str, m_text: str, col: int, geo: dict,
                   national_median: float | None = None) -> dict:
    """Estimate and margin files of one sequence → {zcta: {"income", "coded"}}.

    A negative estimate is an annotation, stored None. An open-interval median
    keeps its bound as the value, coded "top" or "bottom" (against the national
    median), so a page prints "$250,000+" rather than a measured $250,001."""
    est = {r[5]: _cell(r, col) for r in csv.reader(io.StringIO(e_text)) if len(r) > 5 and r[5] in geo}
    moe = {r[5]: _cell(r, col) for r in csv.reader(io.StringIO(m_text)) if len(r) > 5 and r[5] in geo}
    vals = [v for v in est.values() if v is not None and v > 0]
    med = national_median if national_median is not None else (statistics.median(vals) if vals else None)
    out = {}
    for rec, z in geo.items():
        if rec not in est:
            continue
        v = est[rec]
        if v is None or v < 0:
            out[z] = {"income": None, "coded": None}
            continue
        coded = None
        if moe.get(rec) == MOE_OPEN or v in (JAM_TOP, JAM_BOTTOM):
            coded = "top" if (med is None or v >= med) else "bottom"
        out[z] = {"income": v, "coded": coded}
    return out


def _get(url: str, attempts: int = 3, timeout: int = 600) -> bytes:
    """Errors name the file, never the full URL (a keyed URL must not reach
    a log)."""
    last = None
    for k in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code in (400, 404):
                break
        except (urllib.error.URLError, TimeoutError) as e:
            last = str(e)
        time.sleep(3 * (k + 1))
    raise SourceUnavailable(f"{url.split('?')[0].rsplit('/', 1)[-1]}: {last}")


def fetch_acs19_income(min_rows: int = 30_000) -> dict:
    """All ZCTAs' 2015-2019 median household income, from the keyless
    sequence-based summary file."""
    seq, col = sequence_position(_get(SF19_LOOKUP).decode("latin-1"))
    geo = parse_geo(_get(SF19_GEO).decode("latin-1"))
    with zipfile.ZipFile(io.BytesIO(_get(SF19_SEQ.format(seq=seq)))) as zf:
        names = {n.lower(): n for n in zf.namelist()}
        e_name = names.get(f"e20195us{seq}000.txt")
        m_name = names.get(f"m20195us{seq}000.txt")
        if not (e_name and m_name):
            raise SourceUnavailable(f"sequence {seq} zip holds {zf.namelist()}")
        e_text = zf.read(e_name).decode("latin-1")
        m_text = zf.read(m_name).decode("latin-1")
    out = parse_sequence(e_text, m_text, col, geo)
    log.info("  ACS %s: sequence %s column %d, %d ZCTAs, %d with an income",
             ACS19_VINTAGE, seq, col + 1, len(out), sum(1 for v in out.values() if v["income"]))
    if len(out) < min_rows:
        raise SourceUnavailable(f"ACS 2019 income: only {len(out):,} ZCTAs (floor {min_rows:,})")
    return out


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
        try:
            obs = json.loads(_get(f"{FRED_API}?{q}")).get("observations", [])
        except ValueError as e:
            raise SourceUnavailable(f"FRED {series}: not JSON") from e
        out = {}
        for o in obs:
            try:
                out[o["date"]] = float(o["value"])
            except (KeyError, ValueError):
                continue
        return out
    return {d: v for d, v in parse_fred_csv(_get(FRED_CSV.format(series=series)).decode()).items()
            if d >= start}


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
