"""Weather and hazard figures per ZIP — the pure half (zip_env.py).

Run:  python tests/test_zip_env.py      (exit 0 = all pass)

Pure. Stations and tracts are dicts; no network.

The checks are weighted toward the two ways a joined figure lies: a ZIP that
takes its temperatures from a rain gauge or a station 200 km away, and a ZIP
that takes a flood figure from the one tract FEMA happened to score.
"""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import zip_env as E

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# DISTANCE
# ══════════════════════════════════════════════════════════════════
check(abs(E.haversine_km(41.4131, -81.86, 39.9914, -82.8808) - 178) < 3,
      "Cleveland Hopkins to Columbus airport is ~178 km")
check(E.haversine_km(40, -80, 40, -80) == 0, "a point is 0 km from itself")
check(abs(E.haversine_km(0, 0, 0, 1) - 111.2) < 0.5, "a degree of longitude at the equator is ~111 km")
check(abs(E.haversine_km(60, 0, 60, 1) - 55.6) < 0.5,
      "and half that at 60°N — the reason a lat/lng grid needs care near the pole")


# ══════════════════════════════════════════════════════════════════
# NEAREST STATION — only among stations that report the field
# ══════════════════════════════════════════════════════════════════
def st(sid, lat, lng, **vals):
    return {"id": sid, "name": sid, "lat": lat, "lng": lng, "values": vals}


stations = [
    st("RAIN_GAUGE", 40.001, -80.001, snow=30.0),                 # 0.1 km, no temps
    st("TEMPS_NEAR", 40.100, -80.000, winter_low=22.0, summer_high=84.0, snow=40.0),
    st("TEMPS_FAR", 40.300, -80.000, winter_low=10.0, summer_high=80.0),
]
temp = E.StationIndex(stations, ("winter_low", "summer_high"))
snow = E.StationIndex(stations, ("snow",))
s, km = temp.nearest(40.0, -80.0)
check(s and s["id"] == "TEMPS_NEAR" and 10 < km < 12,
      "A RAIN GAUGE IS NOT THE NEAREST TEMPERATURE STATION. The ZIP sits "
      "0.1 km from a gauge that reports only precipitation and snow; its "
      "temperatures come from the nearest station that actually measures "
      "them, 11 km away")
s, km = snow.nearest(40.0, -80.0)
check(s and s["id"] == "RAIN_GAUGE" and km < 1,
      "and SNOW, which the gauge does report, comes from the gauge — each "
      "variable group is matched on its own")
check(temp.count == 2 and snow.count == 2,
      "the index counts only the stations that report every required field")

s, km = temp.nearest(45.0, -80.0)
check(s is None and km is None,
      "A ZIP WITH NO QUALIFYING STATION IN RANGE HAS NO DATA — not the "
      "figures of a station 500 km away, and not a zero")
s, km = temp.nearest(40.0, -80.0, max_km=5)
check(s is None, "the distance cap is honoured")

bad = E.StationIndex([st("NAN", 40.0, -80.0, winter_low=float("nan"), summer_high=80.0),
                      st("BOOL", 40.0, -80.0, winter_low=True, summer_high=80.0),
                      st("NOLAT", None, -80.0, winter_low=20.0, summer_high=80.0)],
                     ("winter_low", "summer_high"))
check(bad.count == 0,
      "a NaN, a boolean or a missing coordinate disqualifies a station — "
      "True is not a temperature")
check(E.StationIndex([], ("x",)).nearest(40, -80) == (None, None), "an empty index finds nothing")
check(temp.nearest(None, -80) == (None, None) and temp.nearest(float("nan"), -80) == (None, None),
      "a ZIP with no coordinates finds nothing, rather than raising")

tie = E.StationIndex([st("B", 40.0, -80.1, x=1.0), st("A", 40.0, -79.9, x=2.0)], ("x",))
check(tie.nearest(40.0, -80.0)[0]["id"] == "A",
      "equidistant stations resolve by id, so the same ZIP gets the same "
      "station on every run")

# ── the grid search returns exactly what brute force returns ──
rng = random.Random(20260927)
pool = []
for i in range(1500):
    # Contiguous US plus Alaska, where a degree of longitude is under half
    # its equatorial length and a lazy ring bound stops too early.
    if i % 5 == 0:
        lat, lng = rng.uniform(55, 71), rng.uniform(-168, -141)
    else:
        lat, lng = rng.uniform(25, 49), rng.uniform(-124, -67)
    pool.append(st(f"S{i:04d}", lat, lng, v=1.0))
idx = E.StationIndex(pool, ("v",))
mismatch = 0
for j in range(600):
    if j % 5 == 0:
        qlat, qlng = rng.uniform(55, 71), rng.uniform(-168, -141)
    else:
        qlat, qlng = rng.uniform(25, 49), rng.uniform(-124, -67)
    got, got_km = idx.nearest(qlat, qlng, max_km=150)
    brute = [(E.haversine_km(qlat, qlng, p["lat"], p["lng"]), p["id"]) for p in pool]
    brute = [b for b in brute if b[0] <= 150]
    want = min(brute) if brute else None
    if (want is None) != (got is None) or (want and got["id"] != want[1]):
        mismatch += 1
check(mismatch == 0,
      f"THE GRID SEARCH AGREES WITH BRUTE FORCE on 600 random ZIPs, a fifth "
      f"of them in Alaska where longitude degrees are narrowest ({mismatch} "
      f"disagreed). The grid exists for speed; it must never trade accuracy")


# ══════════════════════════════════════════════════════════════════
# NOAA ROWS — as the real archive writes them
# ══════════════════════════════════════════════════════════════════
# Values copied from the Cleveland Hopkins row the probe printed: leading
# spaces, a completeness flag per value, and no column at all for a variable
# the station doesn't report.
cle = {"STATION": "USW00014820", "NAME": "CLEVELAND, OH US", "LATITUDE": " 41.4131",
       "LONGITUDE": " -81.8600", "ELEVATION": " 232.6",
       "DJF-TMIN-NORMAL": "    24.8", "comp_flag_DJF-TMIN-NORMAL": "S",
       "JJA-TMAX-NORMAL": "    81.8", "comp_flag_JJA-TMAX-NORMAL": "S",
       "ANN-TMAX-AVGNDS-GRTH090": "    12.4", "comp_flag_ANN-TMAX-AVGNDS-GRTH090": "S",
       "ANN-TMIN-AVGNDS-LSTH032": "   110.8", "comp_flag_ANN-TMIN-AVGNDS-LSTH032": "S",
       "ANN-SNOW-NORMAL": "    63.8", "comp_flag_ANN-SNOW-NORMAL": "S"}
s = E.station_from_normals(cle)
check(s["values"] == {"winter_low": 24.8, "summer_high": 81.8, "days_90": 12.4,
                      "days_32": 110.8, "snow_in": 63.8}
      and s["lat"] == 41.4131 and s["elev_m"] == 232.6,
      "the real Cleveland row parses — leading spaces and all")

gauge = {"STATION": "US1OHCY0001", "LATITUDE": "41.5", "LONGITUDE": "-81.7",
         "ANN-SNOW-NORMAL": " 55.0", "comp_flag_ANN-SNOW-NORMAL": "P"}
g = E.station_from_normals(gauge)
check(g["values"] == {"snow_in": 55.0},
      "a snow-only gauge yields snow and nothing else — absent columns are "
      "absent values, not zeros")
check(E.StationIndex([g], E.TEMP_FIELDS).count == 0
      and E.StationIndex([g], E.SNOW_FIELDS).count == 1,
      "so it can never be picked for temperatures, and can be for snow")

est = dict(cle, **{"comp_flag_DJF-TMIN-NORMAL": "E"})
e = E.station_from_normals(est)
check("winter_low" not in e["values"] and E.StationIndex([e], E.TEMP_FIELDS).count == 0,
      "AN ESTIMATED ('E') VALUE IS DROPPED — it was computed from neighbouring "
      "stations, not measured there — and a station missing one temperature "
      "field cannot serve as the temperature station at all")
check("snow_in" in e["values"], "while its measured snow still counts")
check(E.station_from_normals(dict(cle, **{"comp_flag_JJA-TMAX-NORMAL": ""}))["values"].get("summer_high") is None,
      "a value with no completeness flag is dropped rather than trusted")
check(E.station_from_normals(dict(cle, **{"DJF-TMIN-NORMAL": "  "}))["values"].get("winter_low") is None
      and E.station_from_normals(dict(cle, **{"DJF-TMIN-NORMAL": "nan"}))["values"].get("winter_low") is None,
      "blank and NaN values are dropped")
check(E.station_from_normals(dict(cle, LATITUDE="")) is None,
      "a row with no coordinates is not a station")


# ══════════════════════════════════════════════════════════════════
# FEMA TRACTS — the three kinds of empty
# ══════════════════════════════════════════════════════════════════
inland_ohio = {"IFLD_EALB": 1_997_486.0, "IFLD_EALR": "Relatively High",
               "CFLD_EALB": None, "CFLD_EALR": "Not Applicable"}
check(E.tract_loss(inland_ohio, E.HAZARD_GROUPS["flood"]) == 1_997_486.0,
      "'NOT APPLICABLE' IS ZERO: coastal flooding cannot happen in an Ohio "
      "river tract, so its flood loss is its inland loss. Reading the null "
      "as missing would give every inland ZIP no flood data at all")
check(E.tract_loss({"WFIR_EALB": 0.0, "WFIR_EALR": "No Expected Annual Losses"},
                   ("WFIR",)) == 0.0,
      "'No Expected Annual Losses' is zero")
check(E.tract_loss({"WFIR_EALB": None, "WFIR_EALR": "Insufficient Data"}, ("WFIR",)) is None,
      "'INSUFFICIENT DATA' IS MISSING — reading it as zero would call an "
      "unscored place safe")
check(E.tract_loss({"IFLD_EALB": 500.0, "IFLD_EALR": "Very Low",
                    "CFLD_EALB": None, "CFLD_EALR": "Insufficient Data"},
                   E.HAZARD_GROUPS["flood"]) is None,
      "and one missing component makes the whole group missing — inland "
      "loss alone would understate exactly the tracts where coastal is the risk")
check(E.tract_loss({"HRCN_EALB": 100.0, "HRCN_EALR": "Very Low",
                    "TRND_EALB": 250.0, "TRND_EALR": "Relatively Low"},
                   E.HAZARD_GROUPS["wind"]) == 350.0,
      "hurricane and tornado losses add into one wind figure")
check(E.tract_loss({"WFIR_EALB": None, "WFIR_EALR": None}, ("WFIR",)) is None,
      "a null with no rating at all is missing, not zero")
try:
    E.tract_loss({"WFIR_EALB": None, "WFIR_EALR": "Not Rated Yet"}, ("WFIR",))
    check(False, "an unseen rating must raise")
except ValueError:
    check(True, "A RATING THIS CODE HAS NEVER SEEN RAISES rather than being "
                "guessed at — FEMA renaming a category is exactly how a "
                "missing value would quietly become a zero")
check(not any(c in sum(E.HAZARD_GROUPS.values(), ()) for c in ("HWAV", "CWAV", "WNTW")),
      "heat, cold and winter weather are not hazard filters — their building "
      "losses are near zero nationally; their cost is comfort, which the "
      "weather filters cover")


# ══════════════════════════════════════════════════════════════════
# APPORTIONING TRACTS TO A ZIP
# ══════════════════════════════════════════════════════════════════
tracts = {"A": {"build": 1_000_000.0, "loss": 1_000.0},     # 0.1%
          "B": {"build": 1_000_000.0, "loss": 5_000.0},     # 0.5%
          "U": {"build": 1_000_000.0, "loss": None}}        # unscored
rate, share = E.zcta_loss_rate([("A", 100, 100)], tracts)
check(abs(rate - 0.001) < 1e-12 and share == 1.0, "one whole tract: its own rate")
rate, share = E.zcta_loss_rate([("A", 100, 100), ("B", 100, 100)], tracts)
check(abs(rate - 0.003) < 1e-12, "two whole tracts of equal value: the blended rate")
rate, share = E.zcta_loss_rate([("A", 100, 100), ("B", 10, 100)], tracts)
check(abs(rate - (1000 + 500) / (1_000_000 + 100_000)) < 1e-12,
      "A SLIVER OF A TRACT CONTRIBUTES A SLIVER of its buildings and losses: "
      "10% of B's land in the ZIP brings 10% of B's value and 10% of its loss")
rate, share = E.zcta_loss_rate([("A", 30, 100), ("U", 70, 100)], tracts)
check(rate is None and share == 0.3,
      "A ZIP MOSTLY IN AN UNSCORED TRACT HAS NO DATA — not the rate of the "
      "30% FEMA happened to score")
rate, share = E.zcta_loss_rate([("A", 60, 100), ("U", 40, 100)], tracts)
check(rate is not None and abs(rate - 0.001) < 1e-12 and share == 0.6,
      "above the coverage floor it is scored on what was scored, and says how much")
check(E.zcta_loss_rate([("A", 100, 0)], tracts) == (None, 0.0),
      "a tract with zero land cannot apportion anything")
check(E.zcta_loss_rate([("Z", 100, 100)], tracts) == (None, 0.0),
      "a tract FEMA doesn't list at all is uncovered, not zero")
check(E.zcta_loss_rate([("A", 0, 100)], tracts) == (None, 0.0) and E.zcta_loss_rate([], tracts) == (None, 0.0),
      "no land, no rate")
zero_bv = {"Z0": {"build": 0.0, "loss": 0.0}}
check(E.zcta_loss_rate([("Z0", 100, 100)], zero_bv)[0] is None,
      "a tract with no buildings has no loss RATE — 0/0 is not zero risk")


if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} zip-env checks passed.")
sys.exit(0)
