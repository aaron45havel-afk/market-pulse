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


if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} crime-build checks passed.")
sys.exit(0)
