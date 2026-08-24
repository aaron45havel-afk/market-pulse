"""Parsers behind the rent refresh, proved without a network.

Run:  python tests/test_rents_build.py      (exit 0 = all pass)

huduser.gov, api.census.gov and files.zillowstatic.com are all
unreachable from the sandbox this was written in, so every parser had to
be provable against a fixture or it would ship unverified. That
constraint shaped the script: the fetch functions do nothing but
assemble a URL and hand the body to one of these, and all the logic that
can be wrong lives here.

The failure that matters most in this file is the quiet one. A parser
that returns {} for a source it did not understand looks exactly like "a
source with no data for that ZIP", and the ladder falls silently to a
worse tier — or to nothing — with no error anywhere. So the checks below
care less about happy-path extraction than about what happens when the
shape is wrong.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))

import refresh_rents as R

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def raises(exc, fn, *a):
    try:
        fn(*a)
        return False
    except exc:
        return True


# ── ZORI: a wide CSV with one column per month ──
ZORI = (
    "RegionID,SizeRank,RegionName,RegionType,StateName,2026-05-31,2026-06-30,2026-07-31\n"
    "1,1,44107,zip,OH,1440,1455,1467\n"
    "2,2,44116,zip,OH,1560,1570,\n"          # stopped reporting this month
    "3,3,90210,zip,CA,,,\n"                   # never reported
    "4,4,07030,zip,NJ,3100,3150,3200\n"       # leading zero must survive
    "5,5,99999,zip,XX,41813,41813,41813\n"    # implausible
)
_z = R.parse_zori_csv(ZORI)
check(_z["44107"] == 1467, "the newest month wins")
check(_z["44116"] == 1570,
      "a ZIP that stopped reporting keeps its LAST REAL month rather than "
      "being dropped — Zillow pads trailing months with blanks, and "
      "reading the final column blindly would delete every such ZIP")
check("90210" not in _z, "a row with no values at all is absent, not zero")
check("07030" in _z, "a leading-zero ZIP survives as a 5-character string")
check("99999" not in _z,
      "an implausible value is rejected at the parser rather than carried "
      "into the ladder to be rejected later — or worse, not at all")
check(R.parse_zori_csv("") == {} and R.parse_zori_csv("RegionName\n") == {},
      "empty input yields no rows rather than raising")
check(raises(SystemExit, R.parse_zori_csv,
             "Foo,Bar\n1,2\n"),
      "a CSV with no RegionName column FAILS LOUDLY. Returning {} would "
      "look identical to 'Zillow published an empty file' and would "
      "silently wipe the ZORI tier nationwide")
check(raises(SystemExit, R.parse_zori_csv,
             "RegionID,RegionName,StateName\n1,44107,OH\n"),
      "and so does one with no dated month columns")


# ── HUD county FMR ──
HUD_COUNTY = {"data": {"year": 2026, "basicdata": {
    "Efficiency": 780, "One-Bedroom": 890, "Two-Bedroom": 1100,
    "Three-Bedroom": 1450, "Four-Bedroom": 1720}}}
_h = R.parse_hud_fmr_json(HUD_COUNTY)
check(_h["bedrooms"]["2"] == 1100 and _h["bedrooms"]["0"] == 780,
      "county FMR bedrooms are extracted")
check(_h["year"] == 2026, "along with the fiscal year the numbers are for")
check(R.parse_hud_fmr_json({"data": {"basicdata": [HUD_COUNTY["data"]["basicdata"]]}}
                           )["bedrooms"]["2"] == 1100,
      "a multi-county metro returning basicdata as a LIST is handled — HUD "
      "has shipped both shapes, and only one of them was ever documented")
check(raises(ValueError, R.parse_hud_fmr_json, {})
      and raises(ValueError, R.parse_hud_fmr_json, {"data": {}})
      and raises(ValueError, R.parse_hud_fmr_json, {"data": {"basicdata": "nope"}}),
      "an unrecognised shape RAISES rather than returning empty bedrooms — "
      "a silent {} here reads as 'this county has no FMR' and would drop a "
      "whole state without a line in the log")
check(R.parse_hud_fmr_json({"data": {"basicdata": {"Two-Bedroom": 41813}}}
                           )["bedrooms"] == {},
      "and an implausible HUD figure is dropped, leaving no bedrooms rather "
      "than one impossible one")


# ── HUD SAFMR, per ZIP ──
SAFMR = {"data": {"year": 2026, "basicdata": [
    {"zip_code": "44107", "Efficiency": 905, "One-Bedroom": 1030,
     "Two-Bedroom": 1433, "Three-Bedroom": 1800, "Four-Bedroom": 2473},
    {"zip_code": "44126", "Two-Bedroom": 1290},
    {"zip_code": "bogus", "Two-Bedroom": 1000},
]}}
_s = R.parse_hud_safmr_json(SAFMR)
check(_s["44107"]["bedrooms"]["2"] == 1433 and len(_s["44107"]["bedrooms"]) == 5,
      "a full SAFMR record yields all five bedrooms")
check(_s["44126"]["bedrooms"] == {"2": 1290},
      "and a partial one yields what it has, rather than being discarded")
check("bogus" not in _s, "a non-numeric ZIP is skipped")
check(R.parse_hud_safmr_json({"data": {"basicdata": SAFMR["data"]["basicdata"][0]}}
                             ).get("44107") is not None,
      "a single-ZIP response arriving as an object rather than a list works")
check(raises(ValueError, R.parse_hud_safmr_json, {"data": {"basicdata": 5}}),
      "and a shape nobody expected raises")


# ── HUD's published SAFMR CSV, the second way in ──
SAFMR_CSV = (
    "ZIP Code,HUD Metro Area,SAFMR 0BR,SAFMR 1BR,SAFMR 2BR,SAFMR 3BR,SAFMR 4BR\n"
    "44107,Cleveland,$905,\"$1,030\",\"$1,433\",\"$1,800\",\"$2,473\"\n"
    "44126,Cleveland,$860,$980,\"$1,290\",\"$1,650\",\"$2,100\"\n"
)
_c = R.parse_hud_safmr_csv(SAFMR_CSV)
check(_c["44107"]["bedrooms"]["2"] == 1433,
      "the published CSV parses, dollar signs and thousands commas and all")
check(_c["44126"]["bedrooms"]["4"] == 2100, "across every bedroom column")
check(R.parse_hud_safmr_csv(
    "zip_code,fy2026_safmr_0,fy2026_safmr_2\n44107,905,1433\n"
)["44107"]["bedrooms"]["2"] == 1433,
      "and under HUD's other column naming — the fiscal year is baked into "
      "these headers and changes every October, so they are matched by "
      "shape rather than by an exact string that expires")
check(R.parse_hud_safmr_csv("") == {}, "an empty CSV yields nothing")
check(raises(SystemExit, R.parse_hud_safmr_csv, "a,b\n1,2\n"),
      "a CSV with no ZIP column fails loudly")


# ── Census ACS ──
ACS = [["B25064_001E", "zip code tabulation area"],
       ["1180", "44107"],
       ["-666666666", "44126"],     # Census null sentinel
       ["980", "07030"],
       ["3", "99999"]]              # implausible
_a = R.parse_acs_zcta(ACS)
check(_a["44107"] == 1180, "an ordinary ZCTA rent is read")
check("44126" not in _a,
      "CENSUS NULL SENTINELS BECOME AN ABSENCE. -666666666 passed through "
      "would be a negative rent, and clamped to zero it would read as free "
      "housing — both worse than an empty cell")
check(_a["07030"] == 980, "leading-zero ZCTAs survive")
check("99999" not in _a, "and an implausible value is dropped")
check(R.parse_acs_zcta([]) == {} and R.parse_acs_zcta(None) == {}
      and R.parse_acs_zcta([["B25064_001E"]]) == {},
      "empty or header-only responses yield nothing rather than raising")
check(raises(SystemExit, R.parse_acs_zcta, [["NAME", "state"], ["Ohio", "39"]]),
      "a response missing B25064_001E fails loudly rather than returning {} "
      "— Census renames variables between vintages and a silent empty would "
      "read as 'no ZCTA has a rent'")


# ── the columns the script owns ──
check(len(R.RENT_COLUMNS) == 13
      and {c for c, _ in R.RENT_COLUMNS} >= {"rent_tier", "rent_basis",
                                             "rent_zori", "rent_acs"},
      "the migration adds the tier, the basis and one column per source, so "
      "the winning number can always be traced back to what produced it")
check(all(t in ("TEXT", "INTEGER") for _, t in R.RENT_COLUMNS),
      "with plain column types SQLite will not coerce")


# ── the write path, against a real (temporary) sqlite database ──
#
# THE BUG THIS SECTION EXISTS FOR blanked 16,752 rows and raised no
# error. apply() recomputes every row from whatever it is handed, so a
# --skip-hud run resolved every SAFMR-tier ZIP to nothing and wrote the
# nothing — destroying good data on a run that was never asked to touch
# HUD. It is invisible in a happy-path test because every source is
# present there.
import sqlite3
import tempfile

_db = os.path.join(tempfile.mkdtemp(), "zips.db")
_c = sqlite3.connect(_db)
_c.execute("""CREATE TABLE zips (zip TEXT PRIMARY KEY, state TEXT,
    median_home_value INTEGER, median_rent_monthly INTEGER,
    rent_source TEXT, cap_rate_pct REAL)""")
_c.executemany("INSERT INTO zips VALUES (?,?,?,?,?,?)", [
    ("44107", "OH", 300977, None, None, None),
    ("44116", "OH", 414218, None, None, None),
    ("44126", "OH", 292231, None, None, None),
])
_c.commit()

R.ensure_columns(_c)
_cols = {r[1] for r in _c.execute("PRAGMA table_info(zips)")}
check(_cols >= {c for c, _ in R.RENT_COLUMNS},
      "the migration adds every rent column to an existing table")
R.ensure_columns(_c)
check(True, "and running it twice is harmless — ALTER is guarded on what exists")

# A good run: ZORI answers one ZIP, SAFMR two, ACS all three.
R.apply(_c, {"44126": 1426},
        {"44107": {"bedrooms": {"0": 905, "1": 1030, "2": 1433}},
         "44116": {"bedrooms": {"2": 1577}}},
        {}, {"44107": 1100, "44116": 1200, "44126": 1000}, {},
        "2026-08-24", dry_run=False)


def _row(z):
    return _c.execute("SELECT median_rent_monthly, rent_tier, rent_basis, "
                      "rent_br1, cap_rate_pct FROM zips WHERE zip=?", (z,)).fetchone()


check(_row("44126")[:3] == (1426, "zori", "asking"),
      "ZORI wins where it exists, and the basis is stored beside the number")
check(_row("44107")[:3] == (1433, "safmr", "voucher-floor"),
      "SAFMR answers where ZORI does not")
check(_row("44107")[3] == 1030,
      "and its bedroom split lands in its own columns")
check(_row("44107")[4] == 3.43,
      "cap rate is recomputed from the tier that actually answered")

_before = {z: _row(z) for z in ("44107", "44116", "44126")}
_carried = R.carry_stored(_c)
check(len(_carried[0]) == 1 and len(_carried[1]) == 2 and len(_carried[2]) == 3,
      "carry_stored reads back exactly what each source contributed")

# The partial run: ZORI fetched, HUD and ACS skipped.
R.apply(_c, {"44126": 1426}, _carried[1], {}, _carried[2], {},
        "2026-09-01", dry_run=False)
check({z: _row(z) for z in ("44107", "44116", "44126")} == _before,
      "A PARTIAL RUN CHANGES NOTHING IT DID NOT FETCH. Carrying the stored "
      "columns for unreached sources is the whole difference — without it "
      "this same call blanked the rent, tier and cap rate of every "
      "SAFMR-tier row, 16,752 of them on the real database, silently")

# Without the carry, the damage is visible — this is the bug, pinned.
R.apply(_c, {"44126": 1426}, {}, {}, {}, {}, "2026-09-01", dry_run=False)
check(_row("44107") == (None, None, None, None, None),
      "and dropping the carry reproduces it exactly, so the fix cannot be "
      "removed without this failing")
check(_row("44126")[0] == 1426,
      "while the ZIP the fetched source did cover is untouched")

# A source that DID fetch is authoritative, silences included.
R.apply(_c, {}, {"44107": {"bedrooms": {"2": 1433}}}, {}, {}, {},
        "2026-09-01", dry_run=False)
check(_row("44126")[0] is None,
      "a ZIP the fetched source no longer lists loses its value rather than "
      "keeping a stale one forever — carrying is for sources that did not "
      "run, not for answers that changed")

# dry_run writes nothing at all.
_snap = _row("44107")
R.apply(_c, {"44107": 9999}, {}, {}, {}, {}, "2099-01-01", dry_run=True)
check(_row("44107") == _snap, "--dry-run writes nothing")
_c.close()


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} rent-refresh parser checks passed.")
sys.exit(0)
