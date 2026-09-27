"""Weather per ZIP from NOAA's 1991–2020 U.S. Climate Normals.

    python scripts/refresh_zip_climate.py [--tarball PATH] [--force]

Writes data/zip_climate.json: for every ZIP in zips.db, the thirty-year
normals of its nearest weather station that measures each thing —

    winter average low (Dec–Feb, °F)     summer average high (Jun–Aug, °F)
    days a year at 90°F or hotter         nights a year at or below freezing
    snowfall a year (inches)

— with the station, its distance and its elevation, so a reader can see how
far the number travelled. The matching rules live in zip_env.py and are
tested there; this file downloads, validates and writes.

ONE REQUEST. NOAA publishes every station's annual/seasonal normals as a
single 54MB tarball, so this job makes one download rather than 15,616.
Normals are recomputed once a decade (next: 1991–2020 → 2001–2030 around
2031), so the figures barely move; the job re-runs to pick up ZIPs added to
zips.db and to notice if NOAA replaces the archive.

IT REFUSES TO PUBLISH A BROKEN FILE. If the archive has lost its temperature
columns, or a run would cover far fewer ZIPs than the last one, it exits
non-zero and leaves the committed file alone. A weather filter over a file
that quietly lost half its ZIPs would drop those ZIPs from every board as
"no data" — correct by the finder's rules, and invisible.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
import sys
import tarfile
import time
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import zip_env as E                                          # noqa: E402

ARCHIVE = ("https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/archive/"
           "us-climate-normals_1991-2020_v1.0.1_annualseasonal_multivariate_by-station_c20230404.tar.gz")
OUT = ROOT / "data" / "zip_climate.json"
ZIPS_DB = ROOT / "data" / "zips.db"
UA = {"User-Agent": "MarketPulse/1.0 (zip climate; invoice@archfms.com)"}

# Floors observed in the c20230404 archive (7,306 temperature stations and
# 5,749 snow stations before completeness filtering). Well below them means
# the archive changed shape, not that the weather did.
MIN_TEMP_STATIONS = 5_000
MIN_SNOW_STATIONS = 3_500
MIN_TEMP_COVERAGE = 0.80      # share of ZIPs that must get temperatures
MAX_COVERAGE_DROP = 0.05      # vs the file already committed


class Refuse(Exception):
    """A run that must not publish."""


def fetch(url: str, attempts: int = 3) -> bytes:
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=600) as r:
                return r.read()
        except Exception as e:                               # noqa: BLE001
            last = e
            time.sleep(5 * (i + 1))
    raise Refuse(f"could not download {url}: {last}")


def stations_from_tarball(blob: bytes) -> list:
    out = []
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tf:
        for m in tf.getmembers():
            if not (m.isfile() and m.name.endswith(".csv")):
                continue
            fh = tf.extractfile(m)
            if fh is None:
                continue
            for row in csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8", errors="replace")):
                s = E.station_from_normals(row)
                if s:
                    out.append(s)
    return out


def build(stations: list, zips: list, max_km: float = E.MAX_STATION_KM,
          min_temp: int = MIN_TEMP_STATIONS, min_snow: int = MIN_SNOW_STATIONS) -> dict:
    """stations: parsed station dicts. zips: [(zip, lat, lng), ...]."""
    temp = E.StationIndex(stations, E.TEMP_FIELDS)
    snow = E.StationIndex(stations, E.SNOW_FIELDS)
    if temp.count < min_temp or snow.count < min_snow:
        raise Refuse(f"only {temp.count} temperature and {snow.count} snow stations "
                     f"qualify (floors {min_temp} / {min_snow}) — "
                     f"the archive has changed shape; not publishing")

    used, out = {}, {}
    n_temp = n_snow = 0
    for z, lat, lng in zips:
        rec = {}
        t, t_km = temp.nearest(lat, lng, max_km)
        if t:
            v = t["values"]
            rec.update({"wl": round(v["winter_low"], 1), "sh": round(v["summer_high"], 1),
                        "d90": round(v["days_90"], 1),
                        "d32": round(v["days_32"], 1) if "days_32" in v else None,
                        "ts": t["id"], "tk": t_km})
            used[t["id"]] = t
            n_temp += 1
        s, s_km = snow.nearest(lat, lng, max_km)
        if s:
            rec.update({"sn": round(s["values"]["snow_in"], 1), "ss": s["id"], "sk": s_km})
            used[s["id"]] = s
            n_snow += 1
        if rec:
            out[z] = rec
    n = len(zips) or 1
    return {
        "_meta": {
            "as_of": date.today().isoformat(),
            "built": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "NOAA NCEI U.S. Climate Normals 1991–2020, annual/seasonal",
            "archive": ARCHIVE.rsplit("/", 1)[-1],
            "max_station_km": max_km,
            "accepted_completeness": sorted(E.ACCEPTED_COMP),
            "stations": {"temperature": temp.count, "snow": snow.count},
            "zips": len(zips),
            "coverage": {"temperature": round(n_temp / n, 4), "snow": round(n_snow / n, 4)},
            "fields": {"wl": "winter avg low °F (Dec–Feb)", "sh": "summer avg high °F (Jun–Aug)",
                       "d90": "days/yr ≥ 90°F", "d32": "nights/yr ≤ 32°F",
                       "sn": "snowfall in/yr", "ts/ss": "station id", "tk/sk": "km to station"},
        },
        "stations": {sid: {"name": s["name"], "elev_m": s["elev_m"]} for sid, s in sorted(used.items())},
        "zips": out,
    }


def guard(payload: dict, previous: dict | None, force: bool) -> None:
    cov = payload["_meta"]["coverage"]["temperature"]
    if cov < MIN_TEMP_COVERAGE and not force:
        raise Refuse(f"only {cov:.1%} of ZIPs got temperatures (floor {MIN_TEMP_COVERAGE:.0%})")
    if previous and not force:
        prev = (previous.get("_meta") or {}).get("coverage", {}).get("temperature")
        if isinstance(prev, (int, float)) and prev - cov > MAX_COVERAGE_DROP:
            raise Refuse(f"temperature coverage fell from {prev:.1%} to {cov:.1%} — more "
                         f"likely a broken run than a changed climate; re-run with --force "
                         f"if it is expected")


def load_zips() -> list:
    c = sqlite3.connect(str(ZIPS_DB))
    try:
        return [(z, lat, lng) for z, lat, lng in c.execute(
            "select zip, lat, lng from zips where lat is not null and lng is not null order by zip")]
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--tarball", help="parse a local copy instead of downloading")
    ap.add_argument("--force", action="store_true", help="publish despite the coverage guards")
    args = ap.parse_args()
    try:
        blob = Path(args.tarball).read_bytes() if args.tarball else fetch(ARCHIVE)
        print(f"archive: {len(blob):,} bytes")
        stations = stations_from_tarball(blob)
        zips = load_zips()
        payload = build(stations, zips)
        previous = json.loads(OUT.read_text()) if OUT.exists() else None
        guard(payload, previous, args.force)
    except Refuse as e:
        print(f"::error::REFUSING TO PUBLISH zip_climate.json — {e}")
        return 1
    m = payload["_meta"]
    OUT.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    print(f"wrote {OUT} ({OUT.stat().st_size:,} bytes)")
    print(f"  stations   {m['stations']['temperature']:,} temperature, {m['stations']['snow']:,} snow")
    print(f"  coverage   {m['coverage']['temperature']:.1%} temperature, "
          f"{m['coverage']['snow']:.1%} snow, of {m['zips']:,} ZIPs (within {m['max_station_km']:g} km)")
    known = {z for z, _, _ in zips}
    for z in ("44113", "43215", "94110", "33130", "80202", "99501"):
        r = payload["zips"].get(z)
        print(f"  {z}  {r}" if r else f"  {z}  " + ("(no station in range)" if z in known
                                                    else "(not in zips.db)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
