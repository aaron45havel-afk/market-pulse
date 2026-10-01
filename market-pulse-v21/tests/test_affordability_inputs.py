"""The affordability page's fixed 2019 baseline (scripts/affordability_inputs.py)
and how the ZIP profile build joins and carries it.

Run: python tests/test_affordability_inputs.py
"""
from __future__ import annotations

import io
import json
import os
import sys

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
# CENSUS 2015-2019 MEDIAN HOUSEHOLD INCOME
# ══════════════════════════════════════════════════════════════════
HEAD = ["B19013_001E", "B19013_001M", "zip code tabulation area"]
rows = [HEAD,
        ["61000", "4100", "44107"],
        ["250001", "-333333333", "10007"],
        ["2499", "-333333333", "99999"],
        ["-666666666", "-222222222", "00601"],
        ["58000", "9000", "1001"]]
inc = AI.parse_census_income(rows)
check(inc["44107"] == {"income": 61000, "coded": None}, "a measured median is kept as is")
check(inc["10007"] == {"income": 250001, "coded": "top"},
      "A TOP-CODED $250,001 IS MARKED 'top', so the page prints '$250,000+'")
check(inc["99999"]["coded"] == "bottom", "and a bottom-code is marked 'bottom'")
check(inc["00601"]["income"] is None, "Census 'no estimate' is None, never -666,666,666")
check("01001" in inc, "ZCTAs zero-filled")
per_state = AI.parse_census_income([HEAD[:2] + ["state", HEAD[2]], ["61000", "4100", "39", "44107"]])
check(per_state["44107"]["income"] == 61000, "the state-nested response shape parses too")
try:
    AI.parse_census_income([["NAME", "B01001_001E"], ["x", "1"]])
    check(False, "a changed Census header must fail loudly")
except ValueError:
    check(True, "")

calls = []


def fake_get_json(url, attempts=3, timeout=300):
    calls.append(url)
    if "in=state" not in url:
        raise AI.SourceUnavailable("HTTP 400")
    st = url.split("state%3A")[1][:2]
    return [HEAD[:2] + ["state", HEAD[2]], ["50000", "100", st, f"{st}001"]]


_real = AI._get_json
AI._get_json = fake_get_json
try:
    got = AI.fetch_acs19_income(min_rows=51)
    check(len(got) == 51 and len(calls) == 52,
          "IF THE NATIONAL ZCTA CALL IS REFUSED, ONE CALL PER STATE + DC")
    try:
        AI.fetch_acs19_income(min_rows=30_000)
        check(False, "a thin answer must not publish")
    except AI.SourceUnavailable:
        check(True, "")
finally:
    AI._get_json = _real

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


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


_urlopen = AI.urllib.request.urlopen
_key = os.environ.pop("FRED_API_KEY", None)
AI.urllib.request.urlopen = lambda req, timeout=0: _Resp(CSV.encode())
try:
    check(AI.fetch_fred("CPIAUCSL", start="2019-06-01") == {"2020-01-01": 258.0},
          "without a key FRED's graph CSV is read, from the start date on")
finally:
    AI.urllib.request.urlopen = _urlopen
os.environ["FRED_API_KEY"] = "test-key"
AI._get_json = lambda url, attempts=3, timeout=300: {
    "observations": [{"date": "2019-01-01", "value": "251.7"}, {"date": "2019-02-01", "value": "."}]}
try:
    check(AI.fetch_fred("CPIAUCSL") == {"2019-01-01": 251.7}, "with a key, the API")
finally:
    AI._get_json = _real
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
