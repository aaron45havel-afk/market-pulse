"""Build data/headroom/crime.json from the FBI pull and the researched table.

    python scripts/build_crime.py [--force]

Reads data/headroom/fbi_agencies.json (scripts/fetch_fbi_crime.py), the ZIP
cities in data/zips.db, and the existing crime.json; applies the trust
rules in crime_build.py; writes crime.json. No network — so the rules can
be re-run and tuned against a committed pull.

It refuses to write a table with far fewer usable cities than the FBI pull
should give, or than the table it would replace.
"""
from __future__ import annotations

import argparse
import collections
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import crime_build as C                                      # noqa: E402

RAW = ROOT / "data" / "headroom" / "fbi_agencies.json"
CRIME = ROOT / "data" / "headroom" / "crime.json"
ZIPS_DB = ROOT / "data" / "zips.db"

MIN_USABLE = 2_000            # usable FBI city figures; the first pull gave far more
MAX_USABLE_DROP = 0.20        # vs the table being replaced


class Refuse(Exception):
    """A build that must not publish."""


def zip_cities(conn) -> dict:
    """{(state, norm_place): {"key", "keys", "lat", "lng"}} — one entry per
    ZIP city, keyed the way safety.py looks cities up ("City, ST")."""
    acc = collections.defaultdict(lambda: {"keys": set(), "lat": [], "lng": []})
    for name, st, lat, lng in conn.execute(
            "SELECT name, state, lat, lng FROM zips WHERE name IS NOT NULL AND state IS NOT NULL"):
        city = name[: -len(f", {st}")] if name.endswith(f", {st}") else name
        if not city.strip():
            continue
        c = acc[(st, C.norm_place(city))]
        c["keys"].add(f"{city.strip()}, {st}")
        if lat is not None and lng is not None:
            c["lat"].append(lat)
            c["lng"].append(lng)
    out = {}
    for k, c in acc.items():
        keys = sorted(c["keys"])
        out[k] = {"key": keys[0], "keys": keys,
                  "lat": sum(c["lat"]) / len(c["lat"]) if c["lat"] else None,
                  "lng": sum(c["lng"]) / len(c["lng"]) if c["lng"] else None}
    return out


def usable(table: dict) -> int:
    return sum(1 for r in table.values()
               if r.get("violent_per_100k") is not None and r.get("confidence") != "suspect")


def build(raw: dict, cities: dict, existing: dict) -> tuple[dict, dict]:
    years = raw["_meta"]["years"]
    matched, report = C.match_agencies(raw["agencies"], cities)
    by_key = {c["key"]: c for c in cities.values()}
    fbi, reasons = {}, collections.Counter()
    for key, agency in matched.items():
        rate = C.trusted_rate(agency)
        rec = C.entry_for(agency, rate, years)
        if rate["violent"] is None:
            reasons["no complete year"] += 1
        elif rate["suspect"]:
            reasons["suspect: " + rate["suspect"].split(" ")[0]] += 1
        else:
            reasons["usable"] += 1
        for k in by_key[key]["keys"]:       # every spelling zips.db uses for the city
            fbi[k] = rec
    table, counts = C.merge(existing, fbi)
    report.update({"matched_cities": len(matched), "outcomes": dict(reasons),
                   "merge": counts})
    return table, report


def guard(table: dict, previous: dict, force: bool) -> None:
    n = usable(table)
    if force:
        return
    if n < MIN_USABLE:
        raise Refuse(f"only {n:,} usable city figures (floor {MIN_USABLE:,})")
    prev = usable(previous)
    if prev >= MIN_USABLE and n < prev * (1 - MAX_USABLE_DROP):
        raise Refuse(f"usable cities would fall from {prev:,} to {n:,}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    raw = json.loads(RAW.read_text())
    blob = json.loads(CRIME.read_text())
    conn = sqlite3.connect(str(ZIPS_DB))
    try:
        cities = zip_cities(conn)
    finally:
        conn.close()
    table, report = build(raw, cities, blob.get("table", {}))
    try:
        guard(table, blob.get("table", {}), args.force)
    except Refuse as e:
        print(f"::error::REFUSING TO PUBLISH crime.json — {e}")
        return 1
    yrs = raw["_meta"]["years"]
    blob["table"] = dict(sorted(table.items()))
    blob["_meta"].update({
        "as_of": date.today().isoformat(),
        "status": (f"FBI Crime Data Explorer, every city police agency, {yrs[0]}–{yrs[-1]} "
                   f"(pulled {raw['_meta']['as_of']}); hand-researched entries kept where the "
                   f"FBI didn't match, and researcher-flagged suspects kept suspect"),
        "fbi_years": yrs,
        "fbi_rules": {"pool_below": C.POOL_BELOW, "drop_ratio": C.DROP_RATIO,
                      "drop_min_prior": C.DROP_MIN_PRIOR, "zero_pop": C.ZERO_POP,
                      "max_km": C.MAX_KM},
        "fbi_report": report,
    })
    CRIME.write_text(json.dumps(blob, indent=1, ensure_ascii=False))
    print(f"wrote {CRIME} ({CRIME.stat().st_size:,} bytes)")
    print(f"  cities {len(table):,}; usable {usable(table):,}")
    print(f"  match  {json.dumps({k: v for k, v in report.items() if k not in ('outcomes', 'merge')})}")
    print(f"  outcomes {report['outcomes']}")
    print(f"  merge  {report['merge']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
