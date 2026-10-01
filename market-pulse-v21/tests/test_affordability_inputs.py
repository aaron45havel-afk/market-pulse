"""The affordability page's fixed 2019 baseline (scripts/affordability_inputs.py)
and how the ZIP profile build joins and carries it.

Run: python tests/test_affordability_inputs.py
"""
from __future__ import annotations

import io
import json
import os
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import affordability_inputs as AI  # noqa: E402
import build_zip_profile as B  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# THE 2019 AVERAGE PRICE — all twelve months or nothing
# ══════════════════════════════════════════════════════════════════
full = {"start": "2018-06", "vals": [100.0] * 7 + [float(v) for v in range(200, 212)] + [300.0] * 5}
check(AI.annual_average(full) == sum(range(200, 212)) / 12,
      "2019's twelve months, found from a series that starts mid-2018")
gap = {**full, "vals": full["vals"][:10] + [None] + full["vals"][11:]}
check(AI.annual_average(gap) is None, "A BLANK MONTH IN 2019 GIVES NO AVERAGE — not an eleven-month one")
check(AI.annual_average({"start": "2019-03", "vals": [1.0] * 40}) is None,
      "a series that begins in March 2019 has no 2019 average")
check(AI.annual_average({"start": "2016-01", "vals": [1.0] * 40}) is None,
      "nor one that ends before December 2019")
check(AI.annual_average(None) is None, "no Zillow series, no baseline price")

# ══════════════════════════════════════════════════════════════════
# CENSUS 2015-2019 MEDIAN HOUSEHOLD INCOME — the keyless summary file
# ══════════════════════════════════════════════════════════════════
LOOKUP = "\n".join([
    "File ID,Table ID,Sequence Number,Line Number,Start Position,Total Cells in Table,"
    "Total Cells in Sequence,Table Title,Subject Area",
    "ACSSF,B01001,0001,,7,49 CELLS,,SEX BY AGE,Age-Sex",
    "ACSSF,B19013,0058,,177,1 CELL,,MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS "
    "(IN 2019 INFLATION-ADJUSTED DOLLARS),Income",
    "ACSSF,B19013,0058,,,,,Universe:  Households,",
    "ACSSF,B19013,0058,1,,,,Median household income in the past 12 months,"])
check(AI.sequence_position(LOOKUP) == ("0058", 176),
      "B19013 IS SEQUENCE 0058, COLUMN 177 (1-based, counting the six leading fields) — as published")
try:
    AI.sequence_position(LOOKUP, "B99999")
    check(False, "a table missing from the lookup must fail loudly")
except ValueError:
    check(True, "")


def g(sumlevel, rec, geoid):
    return ",".join(["ACSSF", "US", sumlevel, "00", rec] + [""] * 40 + [geoid, '"name"', "", "", ""])


GEO = "\n".join([g("010", "0000001", "01000US"), g("860", "0033001", "86000US44107"),
                  g("860", "0033002", "86000US10007"), g("860", "0033003", "86000US99999"),
                  g("860", "0033004", "86000US00601"), g("860", "0033005", "86000US01001"),
                  g("050", "0000100", "05000US39035")])
geo = AI.parse_geo(GEO)
check(geo == {"0033001": "44107", "0033002": "10007", "0033003": "99999", "0033004": "00601",
              "0033005": "01001"},
      "summary level 860 rows map record numbers to ZCTAs; the nation and counties are skipped")


def seq_row(rec, val):
    return ",".join(["ACSSF", "2019e5", "us", "000", "0058", rec] + ["0"] * 170 + [str(val)])


E = "\n".join(seq_row(r, v) for r, v in (("0000001", 62843), ("0033001", 61000), ("0033002", 250001),
                                           ("0033003", 2499), ("0033004", -666666666), ("0033005", 58000)))
M = "\n".join(seq_row(r, v) for r, v in (("0000001", 150), ("0033001", 4100), ("0033002", -333333333),
                                           ("0033003", -333333333), ("0033004", -222222222), ("0033005", 9000)))
inc = AI.parse_sequence(E, M, 176, geo)
check(inc["44107"] == {"income": 61000, "coded": None}, "a measured median is kept as is")
check(inc["10007"] == {"income": 250001, "coded": "top"},
      "A TOP-CODED $250,001 IS MARKED 'top', so the page prints '$250,000+'")
check(inc["99999"]["coded"] == "bottom", "and a bottom-code is marked 'bottom'")
check(inc["00601"]["income"] is None, "Census 'no estimate' is None, never -666,666,666")
check("01001" in inc and len(inc) == 5, "every ZCTA record is read, and only those")

_real_get = AI._get
calls = []


def fake_get(url, attempts=3, timeout=600):
    calls.append(url.rsplit("/", 1)[-1])
    if url == AI.SF19_LOOKUP:
        return LOOKUP.encode()
    if url == AI.SF19_GEO:
        return GEO.encode()
    if url == AI.SF19_SEQ.format(seq="0058"):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("e20195us0058000.txt", E)
            zf.writestr("m20195us0058000.txt", M)
        return buf.getvalue()
    raise AI.SourceUnavailable(url)


AI._get = fake_get
try:
    got = AI.fetch_acs19_income(min_rows=5)
    check(got["10007"]["coded"] == "top" and calls[-1] == "20195us0058000.zip",
          "the fetch reads the lookup, the geography and sequence 0058's zip")
    try:
        AI.fetch_acs19_income(min_rows=30_000)
        check(False, "a thin answer must not publish")
    except AI.SourceUnavailable:
        check(True, "")
finally:
    AI._get = _real_get

# ══════════════════════════════════════════════════════════════════
# FRED — yearly averages and the latest CPI
# ══════════════════════════════════════════════════════════════════
CSV = "observation_date,CPIAUCSL\n2019-01-01,251.7\n2019-02-01,.\n2020-01-01,258.0\n"
fc = AI.parse_fred_csv(CSV)
check(fc == {"2019-01-01": 251.7, "2020-01-01": 258.0}, "FRED's '.' (missing) is skipped")
cpi = {f"2019-{m:02d}-01": 250.0 + m for m in range(1, 13)}
cpi.update({f"2024-{m:02d}-01": 310.0 + m for m in range(1, 13)})
cpi.update({"2026-07-01": 333.0, "2026-08-01": 334.131})
pmms = {f"2019-01-{d:02d}": 4.0 for d in range(1, 27)}
pmms.update({f"2019-07-{d:02d}": 3.88 for d in range(1, 27)})
m = AI.macro(cpi, pmms, 2024)
check(m["cpi"]["2019"] == 256.5 and m["cpi"]["2024"] == 316.5,
      "CPI yearly averages for the baseline and the ACS end year")
check(m["cpi"]["latest"] == 334.131 and m["cpi"]["latest_month"] == "2026-08", "and the latest month")
check(m["pmms"]["2019"] == 3.94 and m["baseline_year"] == 2019,
      "the 2019 mortgage rate is the mean of that year's weekly survey")
check(AI.macro_complete(m, 2024), "a complete set is complete")
thin = AI.macro({k: v for k, v in cpi.items() if not k.startswith("2019-12")}, pmms, 2024)
check(thin["cpi"]["2019"] is None and not AI.macro_complete(thin, 2024),
      "ELEVEN MONTHS OF 2019 CPI IS NOT A 2019 AVERAGE — and the set is incomplete")
check(AI.year_mean(dict(list(pmms.items())[:49]), 2019, 50) is None,
      "nor is half a year of weekly rates")


_key = os.environ.pop("FRED_API_KEY", None)
AI._get = lambda url, attempts=3, timeout=600: CSV.encode()
try:
    check(AI.fetch_fred("CPIAUCSL", start="2019-06-01") == {"2020-01-01": 258.0},
          "without a key FRED's graph CSV is read, from the start date on")
finally:
    AI._get = _real_get
os.environ["FRED_API_KEY"] = "test-key"
AI._get = lambda url, attempts=3, timeout=600: json.dumps({"observations": [
    {"date": "2019-01-01", "value": "251.7"}, {"date": "2019-02-01", "value": "."}]}).encode()
try:
    check(AI.fetch_fred("CPIAUCSL") == {"2019-01-01": 251.7}, "with a key, the API")
finally:
    AI._get = _real_get
    del os.environ["FRED_API_KEY"]
    if _key is not None:
        os.environ["FRED_API_KEY"] = _key

# ══════════════════════════════════════════════════════════════════
# THE BUILD JOINS AND CARRIES IT
# ══════════════════════════════════════════════════════════════════
geo = {"44107": {"lat": 41.48, "lng": -81.8, "aland_km2": 14.0},
       "10007": {"lat": 40.71, "lng": -74.0, "aland_km2": 0.4},
       "44999": {"lat": 41.0, "lng": -81.0, "aland_km2": 5.0}}
county = {z: {"state": s, "county_fips": f, "county": c} for z, s, f, c in (
    ("44107", "OH", "39035", "Cuyahoga County"), ("10007", "NY", "36061", "New York County"),
    ("44999", "OH", "39035", "Cuyahoga County"))}
zh = {"44107": {"start": "2018-06", "vals": full["vals"], "city": "Lakewood"}}
rws = {r["zip"]: r for r in B.build_rows(geo, county, {}, zh, {}, {}, {}, {}, {}, None, inc)}
check(rws["44107"]["zhvi_2019"] == round(sum(range(200, 212)) / 12)
      and rws["44107"]["acs19_median_income"] == 61000 and rws["44107"]["acs19_income_coded"] is None,
      "the 2019 price and income land on the row")
check(rws["10007"]["acs19_median_income"] == 250001 and rws["10007"]["acs19_income_coded"] == "top",
      "a top-coded 2019 income keeps its bound and its mark")
check(rws["44999"]["acs19_median_income"] is None and rws["44999"]["zhvi_2019"] is None,
      "A ZCTA NEW IN 2020, OR WITHOUT ZILLOW, HAS NO BASELINE — not a borrowed one")
check(all(c in B.COLUMN_NAMES for c in ("zhvi_2019", "acs19_median_income", "acs19_income_coded")),
      "the baseline columns are in the schema")
fresh = B.build_rows(geo, county, {}, zh, {}, {}, {}, {}, {}, None, {})
n = B.carry_forward(fresh, {"44107": {"zip": "44107", "acs19_median_income": 60000,
                                      "acs19_income_coded": None, "zhvi": 1}}, ("acs19_",))
f = {r["zip"]: r for r in fresh}
check(n == 1 and f["44107"]["acs19_median_income"] == 60000 and f["44107"]["zhvi"] == 300,
      "A FAILED CENSUS CALL CARRIES LAST BUILD'S 2019 INCOMES, and nothing else")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} affordability-baseline checks passed.")
print("   2019 price, income and rate: fixed year, complete or absent, carried when a source fails")
