"""The zip_profile build's pure parts (scripts/build_zip_profile.py) and the
margin-of-error handling it relies on (acs_bulk.py keep_moe).

Run: python tests/test_zip_profile.py
"""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import acs_bulk as AB  # noqa: E402
import build_zip_profile as B  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# MARGINS OF ERROR AND PLACEHOLDERS (acs_bulk)
# ══════════════════════════════════════════════════════════════════
B19013 = [
    "GEO_ID|B19013_E001|B19013_M001",
    "860Z200US20854|250001|-333333333",     # Potomac MD: "$250,000+"
    "860Z200US25922|2499|-333333333",       # bottom-coded
    "860Z200US44107|71250|4120",
    "860Z200US99999|-666666666|-222222222",  # suppressed
]
plain = AB.parse_table(B19013)
check(plain["20854"] == {"B19013_001E": 250001},
      "DEFAULT UNCHANGED: without keep_moe the margins are dropped as before")
moe = AB.parse_table(B19013, keep_moe=True)
check(moe["44107"] == {"B19013_001E": 71250, "B19013_001M": 4120},
      "keep_moe returns each margin beside its estimate")
check(moe["20854"]["B19013_001M"] == AB.MOE_OPEN_INTERVAL and AB.is_open_interval(moe["20854"], "B19013_001E"),
      "THE OPEN-INTERVAL CODE SURVIVES: it is the only record that 250,001 is a "
      "top-code, not a measured median")
check(not AB.is_open_interval(moe["44107"], "B19013_001E"), "a real median is not flagged")
check(moe["99999"] == {"B19013_001E": None, "B19013_001M": -222222222},
      "a suppressed estimate is None; its margin keeps the code that says why")
check(AB.parse_table(B19013, {"B19013_001E"}, keep_moe=True)["44107"]["B19013_001M"] == 4120,
      "asking for an estimate brings its margin when keep_moe is on")
check(AB.api_name("B19013_M001", keep_moe=True) == "B19013_001M"
      and AB.api_name("B19013_M001") is None, "margin naming is opt-in")

# ══════════════════════════════════════════════════════════════════
# GEOGRAPHY
# ══════════════════════════════════════════════════════════════════
GAZ = ("GEOID\tALAND\tAWATER\tALAND_SQMI\tAWATER_SQMI\tINTPTLAT\tINTPTLONG\n"
       "94105\t942000\t0\t0.36\t0\t37.789649\t-122.393067\n"
       "00601\t166659789\t799296\t64.3\t0.3\t18.180555\t-66.749961\n")
g = B.parse_gazetteer(GAZ)
check(g["94105"] == {"lat": 37.789649, "lng": -122.393067, "aland_km2": 0.942},
      "Gazetteer: centroid and land area by column NAME, not position")
REL = ("OID_ZCTA5_20|GEOID_ZCTA5_20|NAMELSAD_ZCTA5_20|AREALAND_ZCTA5_20|OID_COUNTY_20|"
       "GEOID_COUNTY_20|NAMELSAD_COUNTY_20|AREALAND_COUNTY_20|AREALAND_PART\n"
       "1|10009|ZCTA5 10009|1000|9|36061|New York County|58|900\n"
       "2|10009|ZCTA5 10009|1000|9|36047|Kings County|180|100\n"
       "3|00601|ZCTA5 00601|5000|9|72001|Adjuntas Municipio|100|5000\n"
       "4|94105|ZCTA5 94105|942|9|06075|San Francisco County|121|942\n")
cty = B.parse_zcta_county(REL)
check(cty["10009"] == {"state": "NY", "county_fips": "36061", "county": "New York County"},
      "A ZIP SPLIT ACROSS COUNTIES TAKES THE ONE WITH MOST OF ITS LAND")
check("00601" not in cty, "Puerto Rico is outside the 50 states + DC")
check(cty["94105"]["state"] == "CA", "state comes from the county FIPS, not from Zillow")
try:
    B.parse_zcta_county("GEOID|X\n1|2\n")
    check(False, "a changed header must fail loudly")
except ValueError:
    check(True, "")

# ══════════════════════════════════════════════════════════════════
# ZILLOW SERIES WITH DATES
# ══════════════════════════════════════════════════════════════════
months = [f"{y}-{m:02d}-28" for y in range(2014, 2026) for m in range(1, 13)]
vals = [""] * 3 + [str(100_000 + 1_000 * k) for k in range(len(months) - 3)]
vals[50] = ""                                           # one blank month inside
ZH = ("RegionID,SizeRank,RegionName,RegionType,StateName,State,City,Metro,CountyName,"
      + ",".join(months) + "\n"
      + "1,1,2138,zip,MA,MA,Cambridge,Boston,Middlesex County," + ",".join(vals) + "\n"
      + "2,2,99999,zip,XX,XX,Nowhere,,X County," + ",".join([""] * len(months)) + "\n")
z = B.parse_zillow(ZH)
rec = z["02138"]
check(rec["start"] == "2014-04" and rec["vals"][0] == 100_000 and len(rec["vals"]) == len(months) - 3,
      "THE SERIES STARTS AT ITS FIRST NON-EMPTY MONTH, AND SAYS WHICH MONTH THAT IS "
      "(zips.db stored 60 values with no start month, and pages guessed)")
check(rec["vals"][47] is None, "a blank month inside the span stays a gap, not a shift")
check(rec["city"] == "Cambridge" and rec["metro"] == "Boston" and "99999" not in z,
      "ZIPs zero-filled, metadata kept, all-blank rows dropped")
s = B.zillow_summary(rec)
last = 100_000 + 1_000 * (len(months) - 4)
check(s["latest"] == last and s["month"] == "2025-12",
      "latest value carries the month it is for")
check(s["yoy_pct"] == round((last / (last - 12_000) - 1) * 100, 1),
      "1-year change against exactly twelve months back")
check(s["chg_10y_pct"] is None or isinstance(s["chg_10y_pct"], float),
      "10-year change present only when that month exists")
gap = {"start": "2020-01", "vals": [100.0] + [None] * 11 + [110.0, 120.0]}
check(B.zillow_summary(gap)["yoy_pct"] is None,
      "IF THE MONTH TWELVE BACK IS BLANK THE CHANGE IS NONE — not the nearest month")
dd = {"start": "2020-01", "vals": [100.0, 150.0, 120.0]}
check(B.zillow_summary(dd)["peak"] == 150 and B.zillow_summary(dd)["peak_month"] == "2020-02"
      and B.zillow_summary(dd)["from_peak_pct"] == -20.0,
      "peak, its month, and the drawdown from it")
check(B.parse_zillow(ZH, keep_months=12)["02138"]["start"] == "2025-01",
      "keep_months trims from the front and moves the start month")
check(B.month_add("2024-11", 3) == "2025-02" and B.month_add("2024-01", -1) == "2023-12",
      "month arithmetic across years")

# ══════════════════════════════════════════════════════════════════
# CENSUS VALUES
# ══════════════════════════════════════════════════════════════════
nat = {"B19013_001E": 75_000, "B25035_001E": 1975}
vals_, flags = B.decode_acs({"B19013_001E": 250001, "B19013_001M": -333333333,
                             "B25035_001E": 1939, "B25035_001M": -333333333,
                             "B25077_001E": 900_000, "B25077_001M": 41_000}, nat)
check(vals_["acs_median_income"] is None
      and flags["acs_median_income"] == {"bound": 250001, "side": "top"},
      "A TOP-CODED INCOME IS NOT A NUMBER: NULL, with '$250,001, top' in the flags")
check(vals_["acs_year_built"] is None and flags["acs_year_built"]["side"] == "bottom",
      "'built 1939 or earlier' is a bottom-code")
check(vals_["acs_median_value"] == 900_000 and vals_["acs_median_value_moe"] == 41_000,
      "a measured median keeps its value and margin")
check(vals_["acs_median_income_moe"] is None, "a placeholder's margin is not a margin")

cnt = B.derive_counts({"B25003_001E": 1000, "B25003_002E": 600, "B25003_003E": 400,
                       "B25004_002E": 30, "B25004_003E": 10, "B25004_004E": 6, "B25004_005E": 4,
                       "B25024_001E": 1100, "B25024_002E": 700, "B25024_004E": 100,
                       "B25024_005E": 100, "B25024_006E": 200, "B25070_001E": 400,
                       "B25070_011E": 20, "B25070_007E": 50, "B25070_008E": 40,
                       "B25070_009E": 40, "B25070_010E": 60})
check(cnt["rental_vacancy_pct"] == round(30 / 440 * 100, 1) and cnt["rental_vacancy_base"] == 440,
      "rental vacancy = for rent ÷ (renter-occupied + for rent + rented not occupied)")
check(cnt["owner_vacancy_pct"] == round(6 / 610 * 100, 1),
      "homeowner vacancy = for sale ÷ (owner-occupied + for sale + sold not occupied)")
check(cnt["pct_2_4_units"] == round(200 / 1100 * 100, 1) and cnt["pct_sfr_detached"] == 63.6,
      "housing mix from units in structure")
check(cnt["pct_rent_burdened"] == round(190 / 380 * 100, 1),
      "rent burden excludes 'not computed' households from the base")
check(B.derive_counts({"B25003_003E": 20, "B25004_002E": 5})["rental_vacancy_pct"] is None,
      "a vacancy rate on 25 units is noise, and is NULL")

# ══════════════════════════════════════════════════════════════════
# THE JOIN
# ══════════════════════════════════════════════════════════════════
geo = {"94105": {"lat": 37.79, "lng": -122.39, "aland_km2": 0.942},
       "95999": {"lat": 38.0, "lng": -121.0, "aland_km2": 10.0},
       "00601": {"lat": 18.2, "lng": -66.7, "aland_km2": 166.0}}
county = {"94105": {"state": "CA", "county_fips": "06075", "county": "San Francisco County"},
          "95999": {"state": "CA", "county_fips": "06067", "county": "Sacramento County"}}
acs = {"94105": {"B01003_001E": 13861, "B19013_001E": 225000, "B25077_001E": 1_200_000,
                 "B25103_001E": 9000, "B25003_001E": 8000, "B25003_003E": 5200},
       "95999": {"B01003_001E": 4000, "B25077_001E": 400_000, "B25103_001E": 3200}}
zh = {"94105": {"start": "2025-01", "vals": [1_100_000.0] * 12 + [1_126_984.0],
                "city": "San Francisco", "metro": "San Francisco-Oakland"}}
zo = {"94105": {"start": "2025-01", "vals": [5500.0] * 12 + [5958.0]}}
rows = B.build_rows(geo, county, acs, zh, {}, zo,
                    {"94105": {"rent_br2": 3697, "rent_bedroom_tier": "fmr", "neighborhood": "SoMa"}},
                    {"94105": {"fl": 50.0, "wf": 0.1, "wd": 5.0, "eq": 300.0}}, {})
by = {r["zip"]: r for r in rows}
check(set(by) == {"94105", "95999"},
      "EVERY CENSUS ZIP IN THE 50 STATES + DC, with or without Zillow; Puerto Rico out")
check(by["95999"]["zhvi"] is None and by["95999"]["acs_median_value"] == 400_000
      and by["95999"]["tax_rate_acs"] == 0.8,
      "a ZIP Zillow does not cover still has its Census value and tax rate")
check(by["94105"]["zhvi"] == 1_126_984 and by["94105"]["zhvi_month"] == "2026-01"
      and by["94105"]["city"] == "San Francisco",
      "Zillow joined with its month")
check(by["94105"]["price_to_rent"] == round(1_126_984 / (5958 * 12), 2)
      and by["95999"]["price_to_rent"] is None,
      "PRICE-TO-RENT ONLY AGAINST A MARKET ASKING RENT — never a voucher or Census rent")
check(by["94105"]["price_to_income"] == round(1_126_984 / 225_000, 2), "price-to-income")
check(by["94105"]["tax_rate_investor"] is not None and by["94105"]["tax_rate_owner"] >= 1.10,
      "California purchase rates applied to the tax defaults")
check(by["94105"]["haz_total"] == 355.1 and by["95999"]["haz_total"] is None,
      "FEMA total only when all four hazards are known")
check(by["94105"]["hud_rent_br2"] == 3697 and by["94105"]["neighborhood"] == "SoMa",
      "HUD bedroom rents and the neighbourhood name carried from zips.db")
check(by["94105"]["density"] == round(13861 / 0.942, 1) and by["94105"]["pct_renter"] == 65.0,
      "density and renter share")
check(set(B.COLUMN_NAMES) >= set(by["94105"]) and len(B.COLUMN_NAMES) == len(set(B.COLUMN_NAMES)),
      "every row key is a schema column, and no column is declared twice")
for banned in ("composite", "walk", "crime", "restaurant", "forecast", "score"):
    check(not any(banned in c for c in B.COLUMN_NAMES),
          f"NO '{banned}' COLUMN: the rebuild carries measurements only")

pay = B.series_payload(zh, zo, {"built_at": "x"})
check(pay["zhvi"]["94105"] == ["2025-01", [11000] * 12 + [11270]]
      and pay["zori"]["94105"][0] == "2025-01" and pay["_meta"]["zhvi_unit"] == "USD/100",
      "chart series carry their start month; ZHVI in hundreds of dollars")
json.dumps(pay)  # serialisable

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for f in _FAILS:
        print("  ✗", f)
    sys.exit(1)
print(f"OK — all {_COUNT} zip-profile build checks passed.")
print("   every Census ZIP; placeholders flagged not stored; Zillow with dates; no scores")
