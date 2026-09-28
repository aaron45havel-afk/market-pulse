"""The national city crime layer: FBI parsers and the rules for trusting a figure.

Run:  python tests/test_crime_build.py      (exit 0 = all pass)

Pure: no network. The fixtures copy the shapes cde.ucr.cjis.gov actually
returned in September 2026 (probed from GitHub's runners), so a parser that
passes here parses the real thing.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import build_crime as B                                    # noqa: E402
import crime_build as C                                    # noqa: E402
import fetch_fbi_crime as F                                # noqa: E402

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


# ══════════════════════════════════════════════════════════════════
# FETCH PARSERS — the real response shapes
# ══════════════════════════════════════════════════════════════════
LISTING = {  # /agency/byStateAbbr/OH is keyed by COUNTY
    "ERIE": [
        {"ori": "OH0220000", "agency_name": "Erie County Sheriff's Office",
         "agency_type_name": "County", "latitude": 41.43, "longitude": -82.69},
        {"ori": "OH0220200", "agency_name": "Huron Police Department",
         "agency_type_name": "City", "latitude": 41.39, "longitude": -82.55,
         "nibrs_start_date": "2019-01-01"}],
    "SUMMIT": [
        {"ori": "OH0770100", "agency_name": "Akron Police Department",
         "agency_type_name": "City", "latitude": 41.08, "longitude": -81.52},
        {"ori": "OH0770100", "agency_name": "Akron Police Department",
         "agency_type_name": "City"},                         # listed under two counties
        {"ori": "OHUNI0000", "agency_name": "University of Akron",
         "agency_type_name": "University or College"}],
}
_ca = F.city_agencies(LISTING, "OH")
check(sorted(a["ori"] for a in _ca) == ["OH0220200", "OH0770100"],
      "THE LISTING IS KEYED BY COUNTY and only City agencies are kept — the "
      "sheriff and the university are dropped, and an agency listed under "
      "two counties is kept once")
check(next(a for a in _ca if a["ori"] == "OH0220200")["lat"] == 41.39,
      "coordinates travel with the agency, for the distance check at matching")
check(raises(ValueError, F.city_agencies, [{"ori": "x"}], "OH"),
      "a flat list — the shape the old refresher assumed — is refused, not "
      "read as 'this state has no agencies'")


def months(y, vals):
    return {f"{m:02d}-{y}": v for m, v in zip(range(1, 13), vals)}


AKRON = {
    "offenses": {
        "rates": {"Ohio Offenses": months(2025, [21.0] * 12)},
        "actuals": {
            "Akron Police Department Offenses":
                {**months(2024, [88, 99, 118, 138, 131, 145, 134, 143, 153, 147, 141, 147]),
                 **months(2025, [91, 90, 167, 155, 151, 163, 161, 162, 172, None, None, None])},
            "Akron Police Department Clearances": months(2025, [15] * 12)}},
    "populations": {"population": {
        "Ohio": months(2025, [11_900_510] * 12),
        "Akron Police Department": {**months(2024, [188_200] * 12),
                                    **months(2025, [188_000] * 6 + [188_600] * 6)}}},
}
_y = F.yearly(AKRON, [2024, 2025])
check(_y["2024"] == {"n": 1584, "m": 12, "pop": 188200},
      "a complete year: the sum of twelve months, and the agency's population")
check(_y["2025"]["m"] == 9 and _y["2025"]["n"] == 1312,
      "A NULL MONTH IS NOT A ZERO: three unreported months leave m = 9, so the "
      "build can refuse the year instead of calling Akron 25% safer")
check(_y["2025"]["pop"] == 188300, "population is the mean of the months")
check(F.yearly(AKRON, [2023])["2023"] == {"n": 0, "m": 0, "pop": None},
      "a year outside the response is empty, not zero crime")
check(raises(ValueError, F.yearly, {"offenses": {"actuals": {}}}, [2025]),
      "a response with no agency series raises")
check(raises(ValueError, F.yearly,
             {"offenses": {"actuals": {"A Offenses": {}, "B Offenses": {}}}}, [2025]),
      "and so does one with two — it can't be told which is the agency")

good = {"agencies": [{}] * 11_000, "list_failures": [], "failed": [], "requests": 22_000}
for pull, limited, should, why in (
        (good, False, False, "a healthy national pull publishes"),
        (dict(good, agencies=[{}] * 3_000), False, True,
         "FAR FEWER AGENCIES THAN THE FBI LISTS IS REFUSED"),
        (dict(good, agencies=[{}] * 700), True, False,
         "but a pull limited with --states is judged on its failures only"),
        (dict(good, list_failures=["TX", "CA", "NY"]), False, True,
         "three states' agency lists failing is refused"),
        (dict(good, failed=[()] * 500), False, True,
         "more than 2% of requests failing is refused")):
    try:
        F.guard(pull, limited)
        refused = False
    except F.Refuse:
        refused = True
    check(refused is should, why)




# ── shards: a national pull split across runners, merged back ──
def shard(i, n, oris, matched, years=(2023, 2024, 2025), failed=0):
    return {"_meta": {"years": list(years), "matched": matched, "shard": f"{i}/{n}",
                      "shards": n, "list_failures": [], "failed_requests": failed,
                      "states": ["OH"]},
            "agencies": [{"ori": o} for o in oris]}


_ok = F.merge([shard(1, 2, ["A", "C"], 3), shard(2, 2, ["B"], 3)])
check(sorted(a["ori"] for a in _ok["agencies"]) == ["A", "B", "C"] and _ok["matched"] == 3,
      "two shards merge into the whole pull")
for parts, why in (
        ([shard(1, 2, ["A", "C"], 3)], "A MISSING SHARD IS REFUSED — not published as a smaller country"),
        ([shard(1, 2, ["A", "C"], 3), shard(1, 2, ["B"], 3)], "the same shard twice is refused"),
        ([shard(1, 2, ["A", "C"], 3), shard(2, 2, ["C"], 3)], "an agency in two shards is refused"),
        ([shard(1, 2, ["A", "C"], 3), shard(2, 2, ["B"], 3, years=(2022, 2023, 2024))],
         "shards covering different years are refused"),
        ([shard(1, 2, ["A"], 3), shard(2, 2, ["B"], 3)],
         "shards that don't add up to the matched total are refused")):
    try:
        F.merge(parts)
        check(False, why)
    except F.Refuse:
        check(True, why)

# ══════════════════════════════════════════════════════════════════
# NAMES — agency to place
# ══════════════════════════════════════════════════════════════════
for name, want in (
        ("Akron Police Department", ("Akron", False)),
        ("Colerain Township Police Department, Hamilton County", ("Colerain", True)),
        ("Jackson Township  Police Department, Franklin County", ("Jackson", True)),
        ("Village of Leesburg Police Department", ("Leesburg", False)),
        ("Bay View Village Police Department", ("Bay View", False)),
        ("Oxford City Police Department", ("Oxford", False)),
        ("Columbus Division of Police", ("Columbus", False)),
        ("Cherry Hill Township Police Department", ("Cherry Hill", True)),
        ("Bethel Park Borough Police Department", ("Bethel Park", False)),
        ("Hanover Police Dept.", ("Hanover", False)),
        ("Erie County Sheriff's Office", (None, False))):
    check(C.agency_place(name) == want, f"{name!r} → {want} (got {C.agency_place(name)})")
check(C.norm_place("St. Clair Shores") == C.norm_place("Saint Clair Shores")
      and C.norm_place("Mt. Vernon") == C.norm_place("Mount Vernon")
      and C.norm_place("Coeur d'Alene") == C.norm_place("Coeur dAlene"),
      "Saint/St., Mount/Mt. and apostrophes match; nothing else is merged")
check(C.norm_place("Springfield") != C.norm_place("Springfield Township"),
      "the words stay: Springfield is not Springfield Township")


# ══════════════════════════════════════════════════════════════════
# RATES — what a figure has to survive
# ══════════════════════════════════════════════════════════════════
def ag(v, p, pop=50_000, months=12, **kw):
    """v, p: {year: offenses}."""
    return dict({"ori": "XX0000000", "name": "Testville Police Department", "state": "OH",
                 "v": {str(y): {"n": n, "m": months, "pop": pop} for y, n in v.items()},
                 "p": {str(y): {"n": n, "m": months, "pop": pop} for y, n in p.items()}}, **kw)


_r = C.trusted_rate(ag({2023: 100, 2024: 90, 2025: 80}, {2023: 900, 2024: 850, 2025: 800}))
check(_r["violent"] == 160.0 and _r["property"] == 1600.0 and _r["years"] == ["2025"]
      and _r["basis"] == "latest" and _r["suspect"] is None,
      "a city of 50,000 is rated on its latest complete year: 80 / 50,000 = 160 per 100k")
_part = ag({2023: 100, 2024: 90, 2025: 80}, {2023: 900, 2024: 850, 2025: 800})
_part["v"]["2025"]["m"] = 9
_r = C.trusted_rate(_part)
check(_r["years"] == ["2024"] and _r["violent"] == 180.0,
      "A PARTIAL YEAR IS SKIPPED, NOT SCALED: nine months of 2025 fall back to "
      "complete 2024")
_pp = ag({2025: 80}, {2025: 800})
_pp["p"]["2025"]["m"] = 11
check(C.trusted_rate(_pp)["violent"] is None and "no complete year" in C.trusted_rate(_pp)["suspect"],
      "a year needs twelve months of property crime too — and no complete year "
      "at all is no figure, with the reason")
_small = C.trusted_rate(ag({2023: 3, 2024: 0, 2025: 1}, {2023: 20, 2024: 15, 2025: 10}, pop=2_000))
check(_small["basis"] == "pooled" and _small["years"] == ["2023", "2024", "2025"]
      and _small["violent"] == round(4 / 6_000 * 100_000, 1),
      "A SMALL TOWN POOLS ITS COMPLETE YEARS: 4 incidents over 3 years of 2,000 "
      "people is 66.7, not the 0 or 150 a single year would say")
_drop = C.trusted_rate(ag({2023: 100, 2024: 110, 2025: 30}, {2023: 900, 2024: 850, 2025: 800}))
check(_drop["suspect"] and "fell from an average of 105 to 30" in _drop["suspect"],
      "A COLLAPSE IS SUSPECT: 105 a year to 30 reads as a reporting change")
_pdrop = C.trusted_rate(ag({2023: 100, 2024: 110, 2025: 95}, {2023: 900, 2024: 850, 2025: 200}))
check(_pdrop["suspect"] and _pdrop["suspect"].startswith("property"),
      "and a property-crime collapse counts too — under-reporting rarely hits one")
_edge = C.trusted_rate(ag({2023: 100, 2024: 100, 2025: 40}, {2023: 900, 2024: 900, 2025: 900}))
check(_edge["suspect"] is None,
      "exactly 40% of the prior average is not a collapse — the line is under 40%")
check(C.trusted_rate(ag({2023: 9, 2024: 9, 2025: 1}, {2023: 5, 2024: 5, 2025: 5}, pop=12_000))["suspect"] is None,
      "a drop from a tiny base (under 10 a year) is noise, not a flag")
_zero = C.trusted_rate(ag({2023: 0, 2024: 0, 2025: 0}, {2023: 40, 2024: 50, 2025: 45}, pop=8_000))
check(_zero["suspect"] and "no violent offenses" in _zero["suspect"],
      "ZERO VIOLENT CRIME FOR YEARS IN A TOWN OF 8,000 IS A REPORTING GAP, not a record")
check(C.trusted_rate(ag({2023: 0, 2024: 0, 2025: 0}, {2023: 4, 2024: 5, 2025: 3}, pop=900))["suspect"] is None,
      "while a village of 900 can genuinely go three years without one")


# ══════════════════════════════════════════════════════════════════
# MATCHING — agency to ZIP city
# ══════════════════════════════════════════════════════════════════
CITIES = {("OH", "springfield"): {"key": "Springfield, OH", "lat": 39.92, "lng": -83.81},
          ("OH", "akron"): {"key": "Akron, OH", "lat": 41.08, "lng": -81.52},
          ("OH", "franklin"): {"key": "Franklin, OH", "lat": 39.56, "lng": -84.30},
          ("OH", "twinsburg"): {"key": "Twinsburg, OH", "lat": 41.31, "lng": -81.44},
          ("IN", "akron"): {"key": "Akron, IN", "lat": 41.04, "lng": -86.03}}
AGS = [
    {"ori": "OH1", "name": "Springfield Police Department", "state": "OH", "lat": 39.92, "lng": -83.80},
    {"ori": "OH2", "name": "Springfield Township Police Department, Clark County",
     "state": "OH", "lat": 39.90, "lng": -83.86},             # next door, same name
    {"ori": "OH3", "name": "Akron Police Department", "state": "OH", "lat": 41.08, "lng": -81.52},
    {"ori": "OH4", "name": "Franklin Police Department", "state": "OH", "lat": 41.90, "lng": -80.80},
    {"ori": "OH5", "name": "Twinsburg Police Department", "state": "OH", "lat": 41.31, "lng": -81.44},
    {"ori": "OH6", "name": "Twinsburg City Police Department", "state": "OH", "lat": 41.31, "lng": -81.44},
    {"ori": "IN1", "name": "Akron Police Department", "state": "IN", "lat": 41.04, "lng": -86.03}]
_m, _rep = C.match_agencies(AGS, CITIES)
check(_m["Springfield, OH"]["ori"] == "OH1",
      "A CITY AGENCY BEATS A TOWNSHIP AGENCY OF THE SAME NAME — Springfield "
      "Township's rate is not Springfield's")
check(_m["Akron, OH"]["ori"] == "OH3" and _m["Akron, IN"]["ori"] == "IN1",
      "same name, different state, different city")
check("Franklin, OH" not in _m and _rep["too_far"] >= 1,
      "A NAME MATCH 170 KM FROM THE CITY'S ZIPS IS REFUSED — that is another "
      "Franklin, not this one")
check("Twinsburg, OH" not in _m and _rep["ambiguous"] == 1,
      "two city agencies claiming one name is ambiguous, and gets no figure")


# ══════════════════════════════════════════════════════════════════
# MERGING — the researched table meets the FBI
# ══════════════════════════════════════════════════════════════════
EXIST = {"Marion, IN": {"violent_per_100k": 106.9, "confidence": "suspect", "note": "implausibly low"},
         "Columbus, OH": {"violent_per_100k": None, "confidence": "suspect", "note": "partial NIBRS"},
         "Savannah, GA": {"violent_per_100k": None, "confidence": "suspect", "note": "partial NIBRS"},
         "Warsaw, IN": {"violent_per_100k": 413.7, "confidence": "medium", "note": "aggregator"},
         "Peru, IN": {"violent_per_100k": 300.0, "confidence": "high", "note": "researched"}}
FBI = {"Marion, IN": {"violent_per_100k": 95.0, "confidence": "high", "ori": "IN2"},
       "Columbus, OH": {"violent_per_100k": 380.3, "confidence": "high", "ori": "OH2"},
       "Savannah, GA": {"violent_per_100k": 85.8, "confidence": "high", "ori": "GA1"},
       "Warsaw, IN": {"violent_per_100k": 390.0, "confidence": "high", "ori": "IN1"},
       "Akron, OH": {"violent_per_100k": 700.0, "confidence": "high", "ori": "OH1"}}
_t, _cnt = C.merge(EXIST, FBI)
check(_t["Marion, IN"]["confidence"] == "suspect" and _t["Marion, IN"]["violent_per_100k"] == 106.9
      and _t["Marion, IN"]["fbi"]["violent_per_100k"] == 95.0,
      "A RESEARCHER'S SUSPECT STAYS SUSPECT — the FBI figure rides along for "
      "review, never promoted to a label")
check(_t["Warsaw, IN"]["violent_per_100k"] == 390.0 and _t["Warsaw, IN"]["prior_note"] == "aggregator",
      "an FBI figure replaces an aggregator one, keeping the old note")
check(_t["Peru, IN"] == EXIST["Peru, IN"] and "Akron, OH" in _t,
      "a researched city the FBI didn't match is kept; a new city is added")
check(_t["Columbus, OH"]["violent_per_100k"] == 380.3 and _t["Columbus, OH"]["prior_note"] == "partial NIBRS",
      "A RESEARCHER'S DOUBT IS LIFTED BY A COMPLETE FBI FIGURE THAT PASSES EVERY "
      "CHECK AND ISN'T IMPLAUSIBLY LOW — Columbus, flagged for partial NIBRS "
      "reporting, now has a full year at 380 per 100k")
check(_t["Savannah, GA"]["confidence"] == "suspect" and _t["Savannah, GA"]["fbi"]["violent_per_100k"] == 85.8,
      "BUT NOT BY ONE UNDER 100: a city of 242,000 at 86 per 100k is exactly the "
      "under-reporting the doubt was about, and stays unverified")
check(_cnt == {"fbi": 3, "kept_suspect": 2, "lifted": 1, "replaced": 1, "kept_research": 1,
               "dropped_fbi": 0},
      f"and the merge counts what it did ({_cnt})")
# Next year's run starts from this year's table.
_t2, _c2 = C.merge(_t, {"Warsaw, IN": {"violent_per_100k": 400.0, "confidence": "high", "ori": "IN1"}})
check(_t2["Warsaw, IN"]["prior_note"] == "aggregator",
      "THE RESEARCHER'S NOTE SURVIVES A SECOND RUN — carried forward, not "
      "overwritten by last year's FBI entry")
check("Peru, IN" in _t2 and "Akron, OH" not in _t2 and _c2["dropped_fbi"] >= 1,
      "a researched-only city stays; last year's FBI-only city that this run "
      "didn't cover is dropped")
_t4, _c4 = C.merge(_t, FBI)
check(_t4 == _t,
      "RE-RUNNING ON ITS OWN OUTPUT CHANGES NOTHING — the yearly refresh starts "
      "from last year's table, so the merge must be stable")
_auto = {"Dropville, OH": {"violent_per_100k": 30.0, "confidence": "suspect", "ori": "OH7",
                           "note": "Not used: violent offenses fell"}}
_t5, _ = C.merge(_auto, {"Dropville, OH": {"violent_per_100k": 60.0, "confidence": "high", "ori": "OH7"}})
check(_t5["Dropville, OH"]["confidence"] == "high" and _t5["Dropville, OH"]["violent_per_100k"] == 60.0,
      "THE BUILD'S OWN SUSPECT FLAG DOESN'T STICK: next year's figures are "
      "re-judged, and a recovered reporting record is used")
_t3, _c3 = C.merge({"Gone, OH": {"violent_per_100k": 90.0, "confidence": "high", "ori": "OH9"}}, {})
check("Gone, OH" not in _t3 and _c3["dropped_fbi"] == 1,
      "A CITY ONLY THE FBI VOUCHED FOR, NOT MATCHED THIS YEAR, IS DROPPED — "
      "not kept on last year's figure forever")
_e = C.entry_for({"ori": "OH9", "name": "X Police Department"},
                 {"violent": 30.0, "property": 400.0, "basis": "latest", "years": ["2025"],
                  "population": 20_000, "suspect": "violent offenses fell"}, [2023, 2024, 2025])
check(_e["confidence"] == "suspect" and _e["note"].startswith("Not used"),
      "an FBI figure that failed a rule is written as suspect, with the reason")
_e = C.entry_for({"ori": "OH9", "name": "X Police Department"},
                 {"violent": None, "property": None, "basis": None, "years": [],
                  "population": None, "suspect": "no complete year"}, [2023, 2024, 2025])
check(_e["violent_per_100k"] is None and "no complete year" in _e["note"],
      "and a department with no complete year is written as no figure, saying why")


# ══════════════════════════════════════════════════════════════════
# BUILD — zips.db cities, every spelling, and the publish guard
# ══════════════════════════════════════════════════════════════════
import sqlite3                                               # noqa: E402

_db = sqlite3.connect(":memory:")
_db.execute("CREATE TABLE zips (zip TEXT, name TEXT, state TEXT, lat REAL, lng REAL)")
_db.executemany("INSERT INTO zips VALUES (?,?,?,?,?)", [
    ("44301", "Akron, OH", "OH", 41.04, -81.52), ("44310", "Akron", "OH", 41.12, -81.52),
    ("63101", "Saint Louis", "MO", 38.63, -90.19), ("63102", "St. Louis", "MO", 38.63, -90.18)])
_zc = B.zip_cities(_db)
check(_zc[("OH", "akron")]["keys"] == ["Akron, OH"] and abs(_zc[("OH", "akron")]["lat"] - 41.08) < 1e-9,
      "'Akron, OH' and 'Akron' in zips.db are one city, centred on its ZIPs")
check(_zc[("MO", "stlouis")]["keys"] == ["Saint Louis, MO", "St. Louis, MO"],
      "two spellings of one city are both kept as keys")
_raw = {"_meta": {"years": [2023, 2024, 2025]}, "agencies": [
    ag({2023: 1900, 2024: 1800, 2025: 1700}, {2023: 9000, 2024: 8500, 2025: 8000},
       pop=280_000, ori="MO0000001", name="St. Louis Police Department", state="MO",
       lat=38.63, lng=-90.20)]}
_tb, _rp = B.build(_raw, _zc, {})
check(_tb.get("Saint Louis, MO", {}).get("ori") == "MO0000001"
      and _tb.get("St. Louis, MO", {}).get("ori") == "MO0000001",
      "THE FIGURE IS WRITTEN UNDER EVERY SPELLING zips.db USES — safety.py "
      "looks a ZIP up by its own name, so a figure under one spelling alone "
      "would leave half the city's ZIPs unknown")
check(_tb["St. Louis, MO"]["violent_per_100k"] == round(1700 / 280_000 * 100_000, 1)
      and _rp["outcomes"] == {"usable": 1},
      "and the report counts it once, as one usable city")
_many = {f"C{i}, OH": {"violent_per_100k": 100.0, "confidence": "high"} for i in range(3_000)}
for table, prev, should, why in (
        (_many, {}, False, "3,000 usable cities publish"),
        (dict(list(_many.items())[:1_500]), {}, True, "1,500 is under the floor and is refused"),
        (dict(list(_many.items())[:2_200]), _many, True,
         "A 27% FALL IN USABLE CITIES AGAINST THE LAST TABLE IS REFUSED"),
        ({k: dict(v, confidence="suspect") for k, v in _many.items()}, {}, True,
         "suspect figures don't count as usable")):
    try:
        B.guard(table, prev, force=False)
        refused = False
    except B.Refuse:
        refused = True
    check(refused is should, why)

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} crime-build checks passed.")
sys.exit(0)
