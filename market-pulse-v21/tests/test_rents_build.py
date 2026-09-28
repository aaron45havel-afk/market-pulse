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
import urllib.error
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


# ── The shapes HUD's API actually returned, FY2027 (probed 2026-09-27) ──
# A Small Area metro county (Cuyahoga): a list whose FIRST record is the
# metro-wide figure labelled "MSA level", then one record per ZIP.
CUYAHOGA = {"data": {
    "county_name": "Cuyahoga County, OH", "metro_status": "1.0", "smallarea_status": "1",
    "metro_name": "Cleveland, OH", "year": "2027", "basicdata": [
        {"zip_code": "MSA level", "Efficiency": 924, "One-Bedroom": 1050,
         "Two-Bedroom": 1274, "Three-Bedroom": 1628, "Four-Bedroom": 1756},
        {"zip_code": "44001", "Efficiency": 860, "One-Bedroom": 980,
         "Two-Bedroom": 1190, "Three-Bedroom": 1520, "Four-Bedroom": 1640},
        {"zip_code": "44107", "Two-Bedroom": 1433}]}}
# A non-metro county (Adams): one object, with the year INSIDE it.
ADAMS = {"data": {"county_name": "Adams County, OH", "metro_status": "0",
                  "basicdata": {"Efficiency": 758, "One-Bedroom": 849, "Two-Bedroom": 1003,
                                "Three-Bedroom": 1251, "Four-Bedroom": 1318, "year": "2027"}}}
_cz = R.parse_hud_safmr_json(CUYAHOGA)
check(set(_cz) == {"44001", "44107"} and _cz["44001"]["bedrooms"]["2"] == 1190,
      "the real metro payload yields one SAFMR per ZIP, and the 'MSA level' "
      "record is not mistaken for a ZIP")
check(R.parse_hud_fmr_json(CUYAHOGA)["bedrooms"]["2"] == 1274,
      "and the county's FMR is the 'MSA level' record — the metro-wide figure")
_flip = {"data": dict(CUYAHOGA["data"], basicdata=CUYAHOGA["data"]["basicdata"][::-1])}
check(R.parse_hud_fmr_json(_flip)["bedrooms"]["2"] == 1274,
      "PICKED BY ITS LABEL, NOT ITS POSITION: reversed, the list starts with a "
      "ZIP, and taking the first record would file that ZIP's rent as the county's")
check(raises(ValueError, R.parse_hud_fmr_json,
             {"data": {"basicdata": CUYAHOGA["data"]["basicdata"][1:]}}),
      "a list of ZIPs with no area-level record raises rather than guessing one")
_ad = R.parse_hud_fmr_json(ADAMS)
check(_ad["bedrooms"] == {"0": 758, "1": 849, "2": 1003, "3": 1251, "4": 1318}
      and _ad["year"] == "2027",
      "the real non-metro payload yields all five bedrooms, and the year that "
      "HUD tucks inside basicdata for these")
check(R.parse_hud_safmr_json(ADAMS) == {},
      "and it has no ZIP-level rents — a non-metro county never claims SAFMR")


# ── ZIP → county by state + name ──
HUD_COUNTIES = [("OH", "Adams County", "39001"), ("OH", "Cuyahoga County", "39035"),
                ("VA", "Richmond city", "51760"), ("VA", "Richmond County", "51159"),
                ("NM", "Doña Ana County", "35013"), ("MO", "St. Louis County", "29189"),
                ("MD", "Prince George's County", "24033"),
                ("IN", "Adams County", "18001"),
                ("AL", "DeKalb County", "01049"), ("IA", "O'Brien County", "19141"),
                ("MO", "Ste. Genevieve County", "29186"), ("MO", "St. Louis city", "29510"),
                ("IN", "LaPorte County", "18091"),
                ("XX", "Twin County", "99001"), ("XX", "twin county", "99002")]
ZIP_ROWS = [("45693", "OH", "Adams County"), ("44107", "OH", "Cuyahoga County"),
            ("46711", "IN", "Adams County"),
            ("23219", "VA", "Richmond city"), ("22572", "VA", "Richmond County"),
            ("88001", "NM", "Dona Ana County"), ("63105", "MO", "St Louis County"),
            ("20706", "MD", "Prince Georges County"),
            ("99999", "XX", "Twin County"), ("44999", "OH", "Nowhere County"),
            ("35967", "AL", "De Kalb County"), ("51201", "IA", "O Brien County"),
            ("63670", "MO", "Sainte Genevieve County"), ("63101", "MO", "Saint Louis City"),
            ("63122", "MO", "Saint Louis County"), ("46350", "IN", "La Porte County")]
_f, _un = R.county_fips_by_name(ZIP_ROWS, HUD_COUNTIES)
check(_f["45693"] == "39001" and _f["46711"] == "18001",
      "SAME NAME, DIFFERENT STATE, DIFFERENT COUNTY: Adams County, Ohio and "
      "Adams County, Indiana each get their own FMR")
check(_f["23219"] == "51760" and _f["22572"] == "51159",
      "Richmond city and Richmond County, Virginia stay two places — "
      "normalising spelling never touches the words")
check(_f["88001"] == "35013" and _f["63105"] == "29189" and _f["20706"] == "24033",
      "spelling differences match: an accent, a period, an apostrophe")
check(_f["35967"] == "01049" and _f["51201"] == "19141" and _f["46350"] == "18091"
      and _f["63670"] == "29186",
      "THE SPELLINGS THE FIRST NATIONAL RUN MISSED NOW MATCH: De Kalb/DeKalb, "
      "O Brien/O'Brien, La Porte/LaPorte, Sainte/Ste. — 423 ZIPs were left "
      "without a county rent on nothing but spelling")
check(_f["63101"] == "29510" and _f["63122"] == "29189",
      "and Saint/St. matches without merging St. Louis city into St. Louis "
      "County — two places, two FMRs")
check("99999" not in _f and "44999" not in _f,
      "A NAME THAT MAPS TO TWO COUNTIES, OR TO NONE, GETS NO FMR — "
      "ambiguity is dropped, not resolved by a guess")
check(_un.get(("OH", "Nowhere County")) == 1 and _un.get(("XX", "Twin County")) == 1,
      "and both are reported, so a naming mismatch is visible in the log")


# ── Merging a HUD pull: authoritative only where it asked ──
_hud = {"safmr": {"44107": {"bedrooms": {"2": 1433}}},
        "fmr": {"39035": {"bedrooms": {"2": 1274}}, "39001": {"bedrooms": {"2": 1003}}},
        "counties": [("OH", "Adams County", "39001"), ("OH", "Cuyahoga County", "39035"),
                     ("OH", "Butler County", "39017")],
        "failed": {"39017"}, "requested": 88}
_rows = [("44107", "OH", "Cuyahoga County"), ("44001", "OH", "Cuyahoga County"),
         ("45693", "OH", "Adams County"), ("45011", "OH", "Butler County"),
         ("46711", "IN", "Adams County")]
_cs = {"44001": {"bedrooms": {"2": 1111}}, "46711": {"bedrooms": {"2": 999}}}
_cf = {"45011": {"bedrooms": {"2": 1500}}, "46711": {"bedrooms": {"2": 950}},
       "45693": {"bedrooms": {"2": 1}}}
_ms, _mf, _rep = R.merge_hud(_hud, _cs, _cf, _rows, {"OH"})
check(_mf["44001"]["bedrooms"]["2"] == 1274 and _mf["45693"]["bedrooms"]["2"] == 1003,
      "THE COUNTY TIER FILLS: a ZIP gets its county's FMR through the name "
      "join — the tier that matched nothing while zips.db held only names")
check("44001" not in _ms,
      "a ZIP HUD's pull covered but gave no SAFMR loses its stored SAFMR — "
      "a source that asked is authoritative, silences included")
check(_mf["45011"]["bedrooms"]["2"] == 1500,
      "a ZIP in a county whose request FAILED keeps its stored FMR, rather "
      "than a timeout reading as HUD saying the county has none")
check(_ms["46711"]["bedrooms"]["2"] == 999 and _mf["46711"]["bedrooms"]["2"] == 950,
      "A RUN LIMITED TO OHIO LEAVES INDIANA'S HUD RENTS ALONE — without the "
      "carry, --states OH would erase every other state's")
check(_rep["failed_counties"] == 1 and _rep["carried_fmr"] == 2,
      "and the report counts what failed and what was carried")
check(R.merge_hud(dict(_hud, failed={f"c{i}" for i in range(5)}), {}, {}, _rows, None) is None,
      "MORE THAN 5% OF COUNTIES FAILING DISCARDS THE PULL — five of 88 is a "
      "broken run, not five counties with no rents")
_ms2, _mf2, _ = R.merge_hud(dict(_hud, failed=set()), _cs, _cf, _rows, None)
check("46711" not in _ms2 and "46711" not in _mf2,
      "an unlimited run that reached everything carries nothing: Indiana's "
      "Adams County is not in this pull, so its ZIP has no HUD rent")


# ── New England: HUD sets rents by town ──
# Figures from the live API (FY2027). listCounties/CT still returns the
# pre-2022 codes, which /fmr/data answers 404; the crosswalk carries the
# 2022 planning-region codes, which work.
check(R.town_key("0900302060") == R.town_key("0911002060") == "0902060"
      and R.town_key("09003") == "",
      "a town is its state + town code: Avon's old and new CT codes are one town")
check(R.is_town({"fips_code": "0900302060", "town_name": "Avon town"})
      and not R.is_town({"fips_code": "3600199999", "county_name": "Albany County", "town_name": ""})
      and not R.is_town({"fips_code": "2500199999", "town_name": "x"}),
      "listCounties entries are told apart: a town, versus a county ('…99999', no town name)")

_xw = {"data": {"year": "2026", "quarter": "2", "crosswalk_type": "zip-countysub", "results": [
    {"zip": "06001", "geoid": "0911027600", "res_ratio": 0.0244, "tot_ratio": 0.0211},
    {"zip": "06001", "geoid": "0911068940", "res_ratio": 0.00045, "tot_ratio": 0.0004},
    {"zip": "06001", "geoid": "0911002060", "res_ratio": 0.9741, "tot_ratio": 0.9759},
    {"zip": "06902", "geoid": "0919073070", "res_ratio": 0.9993, "tot_ratio": 0.9994},
    {"zip": "06902", "geoid": "0919033620", "res_ratio": 0.0007, "tot_ratio": 0.0006},
    {"zip": "06199", "geoid": "0911037070", "res_ratio": 0, "tot_ratio": 0.7},
    {"zip": "06199", "geoid": "0911082590", "res_ratio": 0, "tot_ratio": 0.3},
    {"zip": "06355", "geoid": "0918000000", "res_ratio": 1, "tot_ratio": 1}]}}
_zt, _code = R.parse_zip_towns(_xw)
check(_zt["06001"][0] == ("0902060", 0.9741) and len(_zt["06001"]) == 3
      and _code["0902060"] == "0911002060",
      "the crosswalk parses: each ZIP's towns largest first, and each town's current code")
check(_zt["06199"][0] == ("0937070", 0.7),
      "a ZIP with no homes (a business or PO-box ZIP) is placed by all its addresses")
check("06355" not in _zt and "0900000" not in _code,
      "'county subdivision not defined' (…00000, open water) is not a town")
for _bad in ({}, {"data": {}}, {"data": {"results": "x"}}, None):
    try:
        R.parse_zip_towns(_bad)
        check(False, f"a crosswalk response without results must raise ({_bad!r})")
    except ValueError:
        check(True, "a crosswalk response without a results list raises, never reads as empty")

_listing = [{"fips_code": "0900302060", "county_name": "Hartford County", "town_name": "Avon town"},
            {"fips_code": "0900173070", "county_name": "Fairfield County", "town_name": "Stamford town"},
            {"fips_code": "0901301080", "county_name": "Tolland County", "town_name": "Andover town"},
            {"fips_code": "0900302060", "county_name": "Hartford County", "town_name": "Avon town"}]
_todo, _skip = R.town_requests(_listing, {"0902060": "0911002060", "0973070": "0919073070"})
check(_todo == [("0902060", "0911002060", "Avon town"), ("0973070", "0919073070", "Stamford town")],
      "CONNECTICUT IS ASKED FOR BY ITS 2022 CODES: listCounties' 0900302060 is requested "
      "as 0911002060 — the old codes are what made every CT request a 404")
check(_skip == 1, "a town the crosswalk puts no ZIP in is not requested; a repeat is asked once")

# Worcester County, MA: four HUD rent areas in one county.
_fmr = {k: {"bedrooms": {"2": v}} for k, v in (
    ("2513875", 2043),    # Clinton town → Worcester HMFA
    ("2530840", 2499),    # Hopedale town → Eastern Worcester County
    ("2544245", 1659),    # New Braintree → Western Worcester County
    ("2558405", 1659),    # Royalston → Western Worcester County
    ("2502130", 1900))}   # Ashburnham → Fitchburg-Leominster
_zt2 = {"01510": [("2513875", 0.93), ("2530840", 0.07)],
        "01005": [("2544245", 0.45), ("2558405", 0.40), ("2544999", 0.0)],
        "01430": [("2502130", 0.45), ("2513875", 0.45), ("2530840", 0.10)],
        "01999": [("2599999", 0.8)],
        "01002": [("2530840", 0.3), ("2502130", 0.7)]}
_trows = [(z, "MA", "Worcester County") for z in ("01510", "01005", "01430", "01999", "01002", "01003")]
_tf, _deps, _trep = R.town_fmr_by_zip(_trows, _zt2, _fmr)
check(_tf["01510"]["bedrooms"]["2"] == 2043,
      "A ZIP TAKES ITS OWN TOWN'S FMR: Clinton's $2,043 (Worcester area), not whichever "
      "Worcester County town HUD happened to list last — up to $840 a month apart")
check(_tf["01005"]["bedrooms"]["2"] == 1659,
      "a ZIP split between towns in the SAME rent area gets that area's FMR")
check("01430" not in _tf and _trep["split"] == 1,
      "A ZIP STRADDLING TWO RENT AREAS GETS NO FMR, rather than a coin flip between them")
check("01999" not in _tf and _trep["no_rent"] == 1 and _deps["01999"] == {"2599999"},
      "a ZIP whose town has no rent gets none, and remembers which town it waited on")
check(_tf["01002"]["bedrooms"]["2"] == 1900 and "01003" not in _tf and _trep["no_town"] == 1,
      "the largest town wins when it holds most homes; a ZIP the crosswalk lacks is counted")
check(_deps["01430"] == {"2502130", "2513875", "2530840"} and _deps["01510"] == {"2513875"},
      "a split ZIP depends on every town it touches; a clear one only on its own")

# merge_hud: New England ZIPs go through towns, never the county-name join.
_hudt = {"safmr": {}, "fmr": {"25027": {"bedrooms": {"2": 2499}}, "39001": {"bedrooms": {"2": 1003}}},
         "counties": [("MA", "Worcester County", "25027"), ("OH", "Adams County", "39001")],
         "town_fmr": {"2513875": {"bedrooms": {"2": 2043}}},
         "zip_towns": {"01510": [("2513875", 0.93)], "01520": [("2530840", 1.0)],
                       "06001": [("0902060", 0.97)]},
         "town_states": {"MA"}, "failed": {"2530840", "state:CT"}, "requested": 100}
_trows2 = [("01510", "MA", "Worcester County"), ("01520", "MA", "Worcester County"),
           ("06001", "CT", "Hartford County"), ("45693", "OH", "Adams County")]
_mst, _mft, _rept = R.merge_hud(_hudt, {}, {"01520": {"bedrooms": {"2": 1234}},
                                            "06001": {"bedrooms": {"2": 1500}}}, _trows2, None)
check(_mft["01510"]["bedrooms"]["2"] == 2043 and _mft["45693"]["bedrooms"]["2"] == 1003,
      "NEW ENGLAND ZIPS TAKE THEIR TOWN'S FMR, NOT THE COUNTY'S — and the rest of the "
      "country still joins by county")
check(_mft["01520"]["bedrooms"]["2"] == 1234,
      "a ZIP whose town request FAILED keeps its stored FMR")
check(_mft["06001"]["bedrooms"]["2"] == 1500,
      "a state whose crosswalk failed keeps every stored FMR")
check(_rept["towns"]["matched"] == 1 and _rept["town_states"] == ["MA"],
      "the report counts the town matches")

# fetch_hud end to end against a stub HUD: the wiring is where CT broke.
_calls = []
_pages = {
    f"{R.HUD_BASE}/listCounties/CT": _listing[:2],
    f"{R.HUD_USPS}?type=11&query=CT": _xw,
    f"{R.HUD_BASE}/data/0911002060": {"data": {"year": "2027", "basicdata": [
        {"zip_code": "MSA level", "Two-Bedroom": 1933}, {"zip_code": "06001", "Two-Bedroom": 2320}]}},
    f"{R.HUD_BASE}/data/0919073070": {"data": {"basicdata": {"Two-Bedroom": 2399}}},
    f"{R.HUD_BASE}/listCounties/NY": [{"fips_code": "3600199999", "county_name": "Albany County",
                                       "town_name": ""}],
    f"{R.HUD_BASE}/data/3600199999": {"data": {"basicdata": {"Two-Bedroom": 1450}}},
    f"{R.HUD_BASE}/listCounties/RI": [{"fips_code": "4400105140", "county_name": "Bristol County",
                                       "town_name": "Barrington town"}],
}


def _stub(url, headers, attempts=3):
    _calls.append(url)
    if url not in _pages:
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
    return _pages[url]


_real = R._get_json
R._get_json = _stub
try:
    _h = R.fetch_hud(["CT", "NY", "RI"], "t", pause=0)
finally:
    R._get_json = _real
check(f"{R.HUD_BASE}/data/0900302060" not in _calls and f"{R.HUD_BASE}/data/0911002060" in _calls,
      "FETCH_HUD ASKS FOR CONNECTICUT BY ITS 2022 CODES — never the stale ones that 404")
check(_h["town_fmr"] == {"0902060": {"bedrooms": {"2": 1933}, "year": "2027"},
                         "0973070": {"bedrooms": {"2": 2399}, "year": None}}
      and _h["safmr"]["06001"]["bedrooms"]["2"] == 2320,
      "town FMRs are stored by town, and Hartford's small-area ZIPs still land as SAFMR")
check(_h["fmr"] == {"36001": {"bedrooms": {"2": 1450}, "year": None}}
      and _h["counties"] == [("NY", "Albany County", "36001")],
      "a county-level state (New York) goes through the county path unchanged")
check(_h["town_states"] == {"CT"} and "state:RI" in _h["failed"] and _h["requested"] == 3,
      "a town state whose crosswalk fails is marked failed, and none of its towns is asked for")


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


# ── Census ACS (keyless bulk file, parsed by acs_bulk) ──
_a = R.acs_rents({"44107": {"B25064_001E": 1180}, "44126": {"B25064_001E": None},
                  "07030": {"B25064_001E": 980}, "99999": {"B25064_001E": 3},
                  "11111": {}})
check(_a == {"44107": 1180, "07030": 980},
      "an ordinary ZCTA rent is read; a suppressed one (None — acs_bulk turns "
      "Census's -666666666 into an absence) and an implausible $3 are dropped "
      "rather than read as free housing; leading-zero ZCTAs survive")
check(R.acs_rents({}) == {} and R.acs_rents(None) == {},
      "empty input yields nothing rather than raising")


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
        {}, {"44107": 1100, "44116": 1200, "44126": 1000},
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
check(len(_carried[0]) == 1 and len(_carried[1]) == 2 and len(_carried[2]) == 0
      and len(_carried[3]) == 3,
      "carry_stored reads back exactly what each source contributed")

# The partial run: ZORI fetched, HUD and ACS skipped.
R.apply(_c, {"44126": 1426}, _carried[1], _carried[2], _carried[3],
        "2026-09-01", dry_run=False)
check({z: _row(z) for z in ("44107", "44116", "44126")} == _before,
      "A PARTIAL RUN CHANGES NOTHING IT DID NOT FETCH. Carrying the stored "
      "columns for unreached sources is the whole difference — without it "
      "this same call blanked the rent, tier and cap rate of every "
      "SAFMR-tier row, 16,752 of them on the real database, silently")

# Without the carry, the damage is visible — this is the bug, pinned.
R.apply(_c, {"44126": 1426}, {}, {}, {}, "2026-09-01", dry_run=False)
check(_row("44107") == (None, None, None, None, None),
      "and dropping the carry reproduces it exactly, so the fix cannot be "
      "removed without this failing")
check(_row("44126")[0] == 1426,
      "while the ZIP the fetched source did cover is untouched")

# A source that DID fetch is authoritative, silences included.
R.apply(_c, {}, {"44107": {"bedrooms": {"2": 1433}}}, {}, {},
        "2026-09-01", dry_run=False)
check(_row("44126")[0] is None,
      "a ZIP the fetched source no longer lists loses its value rather than "
      "keeping a stale one forever — carrying is for sources that did not "
      "run, not for answers that changed")

# dry_run writes nothing at all.
_snap = _row("44107")
R.apply(_c, {"44107": 9999}, {}, {}, {}, "2099-01-01", dry_run=True)
check(_row("44107") == _snap, "--dry-run writes nothing")

# County FMR carried back as COUNTY FMR. The bedroom split is stored once,
# under rent_bedroom_tier; carrying it as SAFMR would relabel a county-wide
# figure as a ZIP-level one on the first run that couldn't reach HUD.
R.apply(_c, {}, {}, {"44116": {"bedrooms": {"1": 890, "2": 1100}}}, {},
        "2026-10-02", dry_run=False)
check(_row("44116")[:3] == (1100, "fmr", "voucher-floor"),
      "county FMR answers a ZIP with no ZORI and no SAFMR")
_c2 = R.carry_stored(_c)
check("44116" not in _c2[1] and _c2[2]["44116"]["bedrooms"] == {"1": 890, "2": 1100},
      "AND IS CARRIED BACK AS FMR, bedrooms and all — not promoted to SAFMR")
R.apply(_c, {}, _c2[1], _c2[2], _c2[3], "2026-11-02", dry_run=False)
check(_row("44116")[:2] == (1100, "fmr"),
      "so a HUD-less run leaves the row exactly as it was, tier included")
_c.close()


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} rent-refresh parser checks passed.")
sys.exit(0)
