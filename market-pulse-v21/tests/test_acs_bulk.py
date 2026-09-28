"""Keyless Census ACS, and the rent ladder surviving the monthly ZIP rebuild.

Run:  python tests/test_acs_bulk.py      (exit 0 = all pass)

Pure: no network. The fixture copies the layout of the Bureau's 2024
table-based summary files as read from GitHub's runners in September 2026.
"""
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import acs_bulk as A                                        # noqa: E402
import build_national_zips as B                             # noqa: E402
import refresh_census_acs_state as S                       # noqa: E402
import refresh_rents as R                                   # noqa: E402

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# THE BULK FILE
# ══════════════════════════════════════════════════════════════════
B25003 = [
    b"GEO_ID|B25003_E001|B25003_M001|B25003_E002|B25003_M002|B25003_E003|B25003_M003\n",
    b"0100000US|127482865|169417|83227478|195366|44255387|120470\n",       # the nation
    b"1400000US39035107101|1520|90|700|80|820|95\n",                     # a tract
    b"860Z200US44107|24390|412|13040|398|11350|402\n",
    b"860Z200US00601|5768|303|3891|305|1877|271\n",                      # leading zero
    b"860Z200US99999|-666666666|-222222222|0|0|0|0\n",                  # suppressed
]
_t = A.parse_table(B25003)
check(set(_t) == {"44107", "00601", "99999"},
      "only ZCTA rows are kept — the nation and the tract are skipped — and a "
      "leading-zero ZIP survives")
check(_t["44107"] == {"B25003_001E": 24390, "B25003_002E": 13040, "B25003_003E": 11350},
      "estimates come back under the API's names; margins of error are dropped")
check(_t["99999"]["B25003_001E"] is None,
      "A CENSUS SENTINEL IS AN ABSENCE, never -666,666,666 renters")
check(A.parse_table(B25003, {"B25003_003E"})["44107"] == {"B25003_003E": 11350},
      "`wanted` limits the variables kept")
try:
    A.parse_table([b"<html>Missing Key</html>\n"])
    check(False, "an HTML page must not parse as an empty table")
except ValueError:
    check(True, "AN HTML ERROR PAGE RAISES instead of reading as 'no ZIP has data' "
                "— the failure that kept the old ACS columns empty for months")
check(A.api_name("B01001_E036") == "B01001_036E" and A.api_name("B01001_M036") is None,
      "B01001_E036 → B01001_036E; margins → None")

B01002 = [b"GEO_ID|B01002_E001|B01002_M001\n",
          b"0400000US39|39.6|0.1\n", b"0400000US72|44.1|0.2\n",
          b"860Z200US44107|36.2|1.4\n"]
_st = A.parse_table(B01002, prefix=A.STATE_PREFIX)
check(_st == {"39": {"B01002_001E": 39.6}, "72": {"B01002_001E": 44.1}},
      "STATE ROWS BY THEIR OWN PREFIX, and a median keeps its decimal — "
      "Ohio's median age is 39.6, not 39")
_sd = S.derive_state({"39": {"B19013_001E": 69680, "B15003_001E": 8_000_000,
                             "B15003_022E": 1_400_000, "B15003_023E": 600_000,
                             "B15003_024E": 100_000, "B15003_025E": 100_000,
                             "B01002_001E": 39.6},
                      "72": {"B19013_001E": 24_000}})
check(_sd == {"OH": {"median_income": 69680, "pct_bachelors_state": 27.5, "median_age": 39.6}},
      "the state job derives income, bachelor's share and median age; Puerto "
      "Rico (72) is not one of the 50 + DC and is skipped")


# ══════════════════════════════════════════════════════════════════
# DERIVED COLUMNS — the same arithmetic the API version did, plus two
# ══════════════════════════════════════════════════════════════════
V = {"B19013_001E": 61250, "B01003_001E": 24390,
     "B15003_001E": 1000, "B15003_022E": 200, "B15003_023E": 80,
     "B15003_024E": 10, "B15003_025E": 10,
     "B25003_001E": 10_000, "B25003_003E": 4_650,
     "B25024_001E": 11_000, "B25024_004E": 900, "B25024_005E": 1_300,
     "B25024_006E": 400, "B25024_007E": 200, "B25024_008E": 100, "B25024_009E": 300,
     "B25070_001E": 4_650, "B25070_011E": 150, "B25070_007E": 450,
     "B25070_008E": 300, "B25070_009E": 400, "B25070_010E": 850,
     "B25034_001E": 11_000, "B25034_009E": 1_500, "B25034_010E": 800, "B25034_011E": 2_200,
     "B25035_001E": 1958,
     "B01001_001E": 24_390, "B01001_011E": 1_100, "B01001_012E": 1_000,
     "B01001_035E": 1_050, "B01001_036E": 950}
_d = B.derive_acs(V)
check(_d["median_household_income"] == 61250 and _d["population"] == 24390,
      "income and population pass through")
check(_d["pct_bachelors"] == 30.0, "bachelor's+ = (200+80+10+10)/1000 = 30%")
check(_d["pct_renter_occupied"] == 46.5, "renters 4,650 / 10,000 occupied = 46.5%")
check(_d["pct_multi_unit"] == 29.1, "2+ unit buildings 3,200 / 11,000 = 29.1%")
check(_d["pct_2_4_units"] == 20.0,
      "2–4 UNIT STOCK, the new column: (900 + 1,300) / 11,000 = 20% — the "
      "house-hack inventory the broader multi-unit share can't isolate")
check(_d["pct_rent_burdened"] == 44.4,
      "rent burden excludes 'not computed' from the denominator: 2,000 / 4,500")
check(_d["pct_pre_1960"] == 40.9 and _d["median_year_built"] == 1958,
      "age of stock: 4,500 / 11,000 built before 1960")
check(_d["pct_age_25_34"] == 16.8,
      "AGED 25–34, the new column: (1,100 + 1,000 + 1,050 + 950) / 24,390 = 16.8%")
_e = B.derive_acs({"B19013_001E": None, "B25003_001E": 0, "B25003_003E": 0,
                   "B25035_001E": 0, "B01001_001E": None})
check(_e["median_household_income"] is None and _e["pct_renter_occupied"] is None
      and _e["median_year_built"] is None and _e["pct_age_25_34"] is None,
      "no data or a zero denominator is None, never 0% — and a 0 median year "
      "built is Census's suppression sentinel")
check(set(_d) >= {"pct_age_25_34", "pct_2_4_units"}
      and "pct_age_25_34" in B.SCHEMA and "pct_2_4_units" in B.SCHEMA,
      "the two new columns are in the schema the build writes")


# ══════════════════════════════════════════════════════════════════
# THE RENT LADDER SURVIVES THE MONTHLY REBUILD
# ══════════════════════════════════════════════════════════════════
# The build deletes zips.db and lays the rows down again. Before this, every
# column refresh_rents.py adds went with it: HUD rents vanished on the 1st
# and came back on the 2nd — or a month later if HUD failed that day.
_dir = Path(tempfile.mkdtemp())
_old = _dir / "zips.db"
_c = sqlite3.connect(_old)
_c.executescript(B.SCHEMA)
_COLS = ("zip, state, median_home_value, median_rent_monthly, crime_index, pct_bachelors, "
         "median_household_income, walk_score, restaurant_score")
_SCORE = (40.0, 30.0, 60_000, 50.0, 40.0)   # every real row carries these
_c.executemany(f"INSERT INTO zips ({_COLS}) VALUES (?,?,?,?,?,?,?,?,?)",
               [("44107", "OH", 300_000, 1_467) + _SCORE, ("45693", "OH", 90_000, None) + _SCORE,
                ("44001", "OH", 150_000, None) + _SCORE])
_c.commit()
R.ensure_columns(_c)
R.apply(_c, {"44107": 1_467}, {"44001": {"bedrooms": {"1": 980, "2": 1_190}}},
        {"45693": {"bedrooms": {"2": 1_003}}, "44001": {"bedrooms": {"2": 1_274}}}, {},
        "2026-09-02", dry_run=False)
_c.close()
_snap = B.snapshot_rent_ladder(_old)
check(_snap["45693"]["rent_fmr"] == 1_003 and _snap["44001"]["rent_safmr"] == 1_190,
      "the snapshot reads the stored per-source rents from the database about "
      "to be replaced")

_new = sqlite3.connect(":memory:")
_new.executescript(B.SCHEMA)
_new.executemany(f"INSERT INTO zips ({_COLS}) VALUES (?,?,?,?,?,?,?,?,?)",
                 [("44107", "OH", 310_000, 1_480) + _SCORE, ("45693", "OH", 92_000, None) + _SCORE,
                  ("44001", "OH", 151_000, None) + _SCORE])
_new.commit()
B.restore_rent_ladder(_new, _snap, {"44107": 1_480})
_rows = {r[0]: r[1:] for r in _new.execute(
    "SELECT zip, median_rent_monthly, rent_tier, rent_br1 FROM zips")}
check(_rows["45693"][:2] == (1_003, "fmr") and _rows["44001"][:2] == (1_190, "safmr"),
      "HUD RENTS SURVIVE THE REBUILD: the county FMR and the SAFMR are back, "
      "under their own tiers")
check(_rows["44001"][2] == 980, "bedroom splits too")
check(_rows["44107"][:2] == (1_480, "zori"),
      "and ZORI is this build's fresh figure, not the stored one")
check(B.restore_rent_ladder(_new, {}, {}) is None,
      "a first build with no prior database has nothing to carry")


if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ACS-bulk and rebuild checks passed.")
sys.exit(0)
