"""The weather and hazard fetch scripts' glue, driven with tiny fixtures.

Run:  python tests/test_zip_env_fetch.py      (exit 0 = all pass)

Pure: no network. zip_env.py's rules are tested in test_zip_env.py; this
file tests what the scripts wrap around them — a real tarball layout, the
Census relationship file's pipe format, the output keys the page reads, and
above all the refusals: a run that would publish a broken file must exit
without publishing.
"""
import io
import json
import os
import sys
import tarfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import refresh_zip_climate as C                             # noqa: E402
import refresh_zip_hazards as H                             # noqa: E402

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# CLIMATE — tarball in, per-ZIP normals out
# ══════════════════════════════════════════════════════════════════
def station_csv(sid, lat, lng, **cols):
    head = ["STATION", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "month", "day", "hour"]
    vals = [sid, f" {lat}", f" {lng}", " 200.0", f"{sid} NAME", "99", "99", "99"]
    for col, (v, flag) in cols.items():
        head += [col, f"meas_flag_{col}", f"comp_flag_{col}", f"years_{col}"]
        vals += [f"    {v}", " ", flag, "30"]
    return (",".join(f'"{h}"' for h in head) + "\n" + ",".join(f'"{v}"' for v in vals) + "\n").encode()


def tarball(files: dict) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


TEMPS = {"DJF-TMIN-NORMAL": (24.8, "S"), "JJA-TMAX-NORMAL": (81.8, "S"),
         "ANN-TMAX-AVGNDS-GRTH090": (12.4, "S"), "ANN-TMIN-AVGNDS-LSTH032": (110.8, "R")}
blob = tarball({
    "USW00014820.csv": station_csv("USW00014820", 41.4131, -81.86, **TEMPS,
                                   **{"ANN-SNOW-NORMAL": (63.8, "S")}),
    "US1OHCY0001.csv": station_csv("US1OHCY0001", 41.50, -81.69, **{"ANN-SNOW-NORMAL": (55.0, "P")}),
    "USC00999999.csv": station_csv("USC00999999", 41.49, -81.70,
                                   **{k: (v, "E") for k, (v, _) in TEMPS.items()}),
    "readme.txt": b"not a station",
})
stations = C.stations_from_tarball(blob)
check(len(stations) == 3, f"three station CSVs parse from the tarball; the txt is skipped (got {len(stations)})")

zips = [("44113", 41.4822, -81.6974),     # downtown Cleveland
        ("99501", 61.2181, -149.9003)]    # Anchorage — no fixture station in range
out = C.build(stations, zips, min_temp=1, min_snow=1)
z = out["zips"].get("44113", {})
check(z.get("ts") == "USW00014820" and z.get("wl") == 24.8 and z.get("sh") == 81.8
      and z.get("d90") == 12.4 and z.get("d32") == 110.8,
      "DOWNTOWN CLEVELAND TAKES HOPKINS' TEMPERATURES. The estimated ('E') "
      "station 0.8 km away is nearer but never qualifies")
check(z.get("ss") == "US1OHCY0001" and z.get("sn") == 55.0 and z.get("sk", 99) < 3,
      "and its snow from the nearer snow gauge, which measures snow")
check(10 < z.get("tk", 0) < 20, f"with the distance recorded ({z.get('tk')} km)")
check("99501" not in out["zips"],
      "A ZIP WITH NO STATION IN RANGE IS ABSENT, not given Cleveland's weather")
m = out["_meta"]
check(m["coverage"] == {"temperature": 0.5, "snow": 0.5} and m["zips"] == 2,
      "coverage is reported per variable")
check(set(out["stations"]) == {"USW00014820", "US1OHCY0001"},
      "only the stations actually used are listed, with name and elevation")
check(json.loads(json.dumps(out)) == out, "the payload is plain JSON")

try:
    C.build(stations, zips)
    check(False, "a tiny archive must be refused at the real floors")
except C.Refuse:
    check(True, "AN ARCHIVE WITH FAR TOO FEW STATIONS IS REFUSED — that is NOAA "
                "changing the file, not the weather changing")

good = {"_meta": {"coverage": {"temperature": 0.95}}}
bad = {"_meta": {"coverage": {"temperature": 0.60}}}
dropped = {"_meta": {"coverage": {"temperature": 0.86}}}
for payload, prev, should, why in (
        (good, None, False, "a healthy first run publishes"),
        (bad, None, True, "coverage under the floor is refused"),
        (dropped, good, True, "A NINE-POINT COVERAGE DROP AGAINST THE LAST FILE IS REFUSED"),
        (good, dropped, False, "a coverage RISE publishes")):
    try:
        C.guard(payload, prev, force=False)
        refused = False
    except C.Refuse:
        refused = True
    check(refused is should, why)
try:
    C.guard(bad, good, force=True)
    check(True, "--force overrides the guard for an expected change")
except C.Refuse:
    check(False, "--force must override")


# ══════════════════════════════════════════════════════════════════
# HAZARDS — tracts + relationship file in, per-ZIP loss out
# ══════════════════════════════════════════════════════════════════
rel = ("OID_ZCTA5_20|GEOID_ZCTA5_20|NAMELSAD_ZCTA5_20|AREALAND_ZCTA5_20|AREAWATER_ZCTA5_20|"
       "MTFCC_ZCTA5_20|CLASSFP_ZCTA5_20|FUNCSTAT_ZCTA5_20|OID_TRACT_20|GEOID_TRACT_20|"
       "NAMELSAD_TRACT_20|AREALAND_TRACT_20|AREAWATER_TRACT_20|MTFCC_TRACT_20|"
       "FUNCSTAT_TRACT_20|AREALAND_PART|AREAWATER_PART\n"
       "||||||||1|01003010100|Census Tract 101|971527026|37552076|G5020|S|207523057|32801540\n"
       "1|44113|ZCTA5 44113|200|0|G6350|B5|S|2|39035107101|T|100|0|G5020|S|100|0\n"
       "1|44113|ZCTA5 44113|200|0|G6350|B5|S|3|39035107102|T|400|0|G5020|S|100|0\n"
       "2|43215|ZCTA5 43215|100|0|G6350|B5|S|4|39049001000|T|100|0|G5020|S|100|0\n")
parts = H.parse_relationship(rel)
check(set(parts) == {"44113", "43215"},
      "the pipe-delimited relationship file parses; tract land outside any "
      "ZCTA (blank ZCTA id) is skipped")
check(parts["44113"] == [("39035107101", 100.0, 100.0), ("39035107102", 100.0, 400.0)],
      "each ZCTA keeps (tract, land in the ZCTA, tract's total land)")


def tract(fips, build, **haz):
    r = {"TRACTFIPS": fips, "STATEABBRV": "OH", "BUILDVALUE": build, "NRI_VER": "December 2025"}
    for code in ("IFLD", "CFLD", "WFIR", "HRCN", "TRND", "ERQK"):
        v, rating = haz.get(code, (0.0, "No Expected Annual Losses"))
        r[f"{code}_EALB"], r[f"{code}_EALR"] = v, rating
    return r


rows = [tract("39035107101", 1_000_000, IFLD=(1_000.0, "Relatively High"),
              CFLD=(None, "Not Applicable"), HRCN=(None, "Not Applicable")),
        tract("39035107102", 4_000_000, IFLD=(400.0, "Very Low"),
              CFLD=(None, "Not Applicable"), WFIR=(None, "Insufficient Data")),
        tract("39049001000", 2_000_000, IFLD=(0.0, "No Expected Annual Losses"),
              CFLD=(None, "Not Applicable"))]
out = H.build(rows, parts, ["44113", "43215", "00000"], min_tracts=1)
z = out["zips"]["44113"]
# 44113 holds all of tract 101 and a quarter of 102:
#   flood = (1000 + 0.25*400) / (1,000,000 + 0.25*4,000,000) = 1100 / 2,000,000
check(abs(z["fl"] - 1100 / 2_000_000 * 100_000) < 0.01,
      f"FLOOD APPORTIONS BY LAND: all of one tract, a quarter of the other — "
      f"$55 per $100k a year (got {z.get('fl')})")
check("wf" in z and z["wf"] == 0.0,
      "wildfire: half the ZIP's land is in a tract FEMA couldn't score, which "
      "is exactly the 50% floor, so it is scored on the half that was — zero")
check(z["wd"] == 0.0, "wind: 'Not Applicable' hurricane reads as zero, not missing")
check(out["zips"]["43215"]["fl"] == 0.0,
      "a ZIP whose tracts expect no flood loss reads $0, not missing")
check("00000" not in out["zips"], "a ZIP with no tracts is absent, not zero")
check(out["_meta"]["coverage"]["flood"] == round(2 / 3, 4) and out["_meta"]["nri_version"] == "December 2025",
      "coverage and the NRI release are recorded")
check(out["_meta"]["median"]["flood"] == round((0.0 + z["fl"]) / 2, 2),
      "the national median is over the ZIPs that have a figure — the ZIP "
      "with no tracts does not drag it toward zero")
# Three ZIPs, one whole tract each, $100k of building apiece: A floods, B
# blows down, C gets a little of both. The typical ZIP's all-hazards total is
# B's or A's $100; adding up the typical flood ($10) and typical wind ($10)
# figures would claim $20, a total no ZIP has.
_p3 = {z: [(t, 1.0, 1.0)] for z, t in (("A", "39000000001"), ("B", "39000000002"),
                                       ("C", "39000000003"))}
_r3 = [tract("39000000001", 100_000, IFLD=(100.0, "Very High"), CFLD=(None, "Not Applicable")),
       tract("39000000002", 100_000, HRCN=(100.0, "Very High"), CFLD=(None, "Not Applicable")),
       tract("39000000003", 100_000, IFLD=(10.0, "Very Low"), HRCN=(10.0, "Very Low"),
             CFLD=(None, "Not Applicable"))]
_m3 = H.build(_r3, _p3, ["A", "B", "C"], min_tracts=1)["_meta"]["median"]
check(_m3["total"] == 100.0 and _m3["flood"] == 10.0 and _m3["wind"] == 10.0,
      f"THE ALL-HAZARDS MEDIAN IS A MEDIAN OF TOTALS, NOT A SUM OF MEDIANS "
      f"($100, not $20 — got {_m3})")
check(set(z) == {"fl", "wf", "wd", "eq"},
      "the output keys are the ones the page reads — dollars only, no ranks")

for bad_rows, why in (
        (rows + [dict(rows[0])], "DUPLICATE TRACTS ARE REFUSED"),
        ([dict(r, NRI_VER="March 2023") if i == 0 else r for i, r in enumerate(rows)],
         "TWO NRI RELEASES MIXED IN ONE PULL ARE REFUSED"),
        ([dict(rows[0], WFIR_EALB=None, WFIR_EALR="Pending Review")] + rows[1:],
         "A RATING NOBODY HAS SEEN IS REFUSED, not guessed at")):
    try:
        H.build(bad_rows, parts, ["44113"], min_tracts=1)
        check(False, why)
    except H.Refuse:
        check(True, why)
try:
    H.build(rows, parts, ["44113"])
    check(False, "three tracts must be refused at the real floor")
except H.Refuse:
    check(True, "a pull with far too few tracts is refused")
try:
    H.guard({"_meta": {"coverage": {"flood": 0.90}}}, {"_meta": {"coverage": {"flood": 0.97}}}, False)
    check(False, "a coverage drop must be refused")
except H.Refuse:
    check(True, "a seven-point flood coverage drop against the last file is refused")


if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} fetch-glue checks passed.")
sys.exit(0)
