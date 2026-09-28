"""Natural-hazard building loss per ZIP from FEMA's National Risk Index.

    python scripts/refresh_zip_hazards.py [--force]

Writes data/zip_hazards.json: for every ZIP in zips.db, FEMA's EXPECTED
ANNUAL BUILDING LOSS from flooding, wildfire, wind (hurricane + tornado) and
earthquake — as dollars a year per $100,000 of building value.

DOLLARS, NOT RANKS. Most of these distributions are piled up near zero —
the median ZIP's expected wildfire loss is 13 cents a year per $100k — so a
national rank turns a dollar a year into "worse than 80% of the country".
The page filters and displays the dollars themselves.

WHY LOSS PER DOLLAR, NOT FEMA'S RATINGS. The NRI's headline risk ratings
multiply expected loss by the local population's social vulnerability, so
the same flood rates riskier in a poorer tract — the income correlation this
board already threw crime_index out for — and they are relative, so a
wildfire score of 64 is rated "Very Low". A buyer's question is what the
hazard costs the building. Expected annual building loss ÷ building value
answers exactly that, and FEMA publishes both halves per tract.

TWO SOURCES, BOTH REACHABLE FROM ACTIONS. hazards.fema.gov and www.fema.gov
answer 403 to GitHub's runners; FEMA's own ArcGIS feature service (owner
FEMA_NationalRiskIndex) does not, and serves the same December 2025 release
tract by tract. Tracts become ZIPs through the Census Bureau's 2020
ZCTA-to-tract relationship file, apportioned by land area (zip_env.py).
Connecticut's tracts are renumbered to their 2020 IDs first (recode_tracts).

IT REFUSES TO PUBLISH A BROKEN FILE: too few tracts, more than one NRI
release mixed together, a rating it has never seen, or a coverage collapse
against the committed file all exit non-zero and leave that file alone.
"""
from __future__ import annotations

import argparse
import collections
import csv
import io
import json
import sqlite3
import statistics
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import zip_env as E                                          # noqa: E402

FEATURES = ("https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/"
            "National_Risk_Index_Census_Tracts/FeatureServer/0/query")
RELATIONSHIP = ("https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
                "tab20_zcta520_tract20_natl.txt")
OUT = ROOT / "data" / "zip_hazards.json"
ZIPS_DB = ROOT / "data" / "zips.db"
UA = {"User-Agent": "MarketPulse/1.0 (zip hazards; invoice@archfms.com)"}
PAGE = 2000                   # the layer's maxRecordCount

MIN_TRACTS = 80_000           # the December 2025 release has 85,154
MIN_FLOOD_COVERAGE = 0.85
MAX_COVERAGE_DROP = 0.05

# Short key in the output per hazard group: expected loss $/yr per $100k.
KEYS = {"flood": "fl", "wildfire": "wf", "wind": "wd", "quake": "eq"}


class Refuse(Exception):
    """A run that must not publish."""


def fetch(url: str, attempts: int = 4) -> bytes:
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=300) as r:
                return r.read()
        except Exception as e:                               # noqa: BLE001
            last = e
            time.sleep(5 * (i + 1))
    raise Refuse(f"could not download {url.split('?')[0]}: {last}")


def fetch_tracts() -> list:
    codes = sorted({c for g in E.HAZARD_GROUPS.values() for c in g})
    fields = ["TRACTFIPS", "STATEABBRV", "BUILDVALUE", "NRI_VER"]
    fields += [f"{c}_{s}" for c in codes for s in ("EALB", "EALR")]
    rows, offset = [], 0
    while True:
        qs = urllib.parse.urlencode({
            "where": "1=1", "outFields": ",".join(fields), "returnGeometry": "false",
            "orderByFields": "OBJECTID", "resultOffset": offset,
            "resultRecordCount": PAGE, "f": "json"})
        page = json.loads(fetch(FEATURES + "?" + qs))
        if "error" in page:
            raise Refuse(f"feature service error at offset {offset}: {page['error']}")
        feats = page.get("features", [])
        rows += [f["attributes"] for f in feats]
        if len(feats) < PAGE and not page.get("exceededTransferLimit"):
            return rows
        offset += len(feats)


def parse_relationship(text: str) -> dict:
    """{zcta: [(tract, land_in_zcta, tract_land_total), ...]}"""
    parts = collections.defaultdict(list)
    for r in csv.DictReader(io.StringIO(text), delimiter="|"):
        z = (r.get("GEOID_ZCTA5_20") or "").strip()
        t = (r.get("GEOID_TRACT_20") or "").strip()
        if not z or not t:
            continue           # tract land outside any ZCTA (water, parks)
        try:
            part, total = float(r["AREALAND_PART"]), float(r["AREALAND_TRACT_20"])
        except (KeyError, TypeError, ValueError):
            continue
        parts[z].append((t, part, total))
    return parts


# Connecticut replaced its eight counties with nine planning regions in 2022.
# FEMA's NRI carries the 2022 tract IDs (county part 110–190); the 2020
# relationship file carries the 2020 ones (001–015), so without this no CT
# tract joins. Only the county part changed: the tracts and their 6-digit
# codes did not, and the codes are unique statewide. Checked against the
# live data before this was written — all 879 of FEMA's CT tracts matched
# exactly one 2020 tract by code, and every 2020 CT tract was matched.
RECODED_STATES = {"09": "Connecticut's 2022 planning regions"}


def recode_tracts(tract_rows: list, parts: dict) -> tuple[list, dict]:
    """FEMA rows with each recoded state's tract IDs turned back into the 2020
    IDs the relationship file uses. Returns (rows, {"recoded", "unmatched"}).

    Matched on state + 6-digit tract code, and only where exactly one 2020
    tract of that state has the code and no other FEMA tract claims it — an
    ambiguous one is left as it was, and so unjoined, rather than guessed.
    Other states are never touched: a tract code repeats across counties
    almost everywhere else.
    """
    known = {t for p in parts.values() for t, _, _ in p}
    by_code = collections.defaultdict(set)          # recoded states only
    for t in known:
        if t[:2] in RECODED_STATES:
            by_code[(t[:2], t[5:])].add(t)
    target = {}
    for r in tract_rows:
        t = r.get("TRACTFIPS") or ""
        hits = by_code.get((t[:2], t[5:]), ()) if t not in known else ()
        if len(hits) == 1:
            target[t] = next(iter(hits))
    claims = collections.Counter(target.values())
    out, report = [], {"recoded": 0, "unmatched": 0}
    for r in tract_rows:
        t = r.get("TRACTFIPS") or ""
        if t[:2] not in RECODED_STATES or t in known:
            out.append(r)
        elif t in target and claims[target[t]] == 1:
            out.append(dict(r, TRACTFIPS=target[t], TRACTFIPS_NRI=t))
            report["recoded"] += 1
        else:
            out.append(r)
            report["unmatched"] += 1
    return out, report


def build(tract_rows: list, parts: dict, zips: list, min_tracts: int = MIN_TRACTS) -> dict:
    """tract_rows: NRI attribute dicts. parts: parse_relationship(). zips: [zip, ...]."""
    if len(tract_rows) < min_tracts:
        raise Refuse(f"only {len(tract_rows):,} tracts (floor {min_tracts:,})")
    versions = collections.Counter(r.get("NRI_VER") for r in tract_rows)
    if len(versions) != 1:
        raise Refuse(f"mixed NRI releases in one pull: {dict(versions)}")
    ids = [r.get("TRACTFIPS") for r in tract_rows]
    if len(set(ids)) != len(ids):
        raise Refuse("duplicate TRACTFIPS in the pull")
    tract_rows, recoded = recode_tracts(tract_rows, parts)

    by_group = {}
    for group, codes in E.HAZARD_GROUPS.items():
        table = {}
        for r in tract_rows:
            try:
                loss = E.tract_loss(r, codes)
            except ValueError as e:
                raise Refuse(f"{r.get('TRACTFIPS')}: {e} — FEMA changed its "
                             f"categories; not guessing what the new one means")
            table[r["TRACTFIPS"]] = {"build": r.get("BUILDVALUE"), "loss": loss}
        by_group[group] = table

    rates = {g: {} for g in E.HAZARD_GROUPS}
    shares = {g: {} for g in E.HAZARD_GROUPS}
    for z in zips:
        p = parts.get(z, [])
        for g in E.HAZARD_GROUPS:
            rate, share = E.zcta_loss_rate(p, by_group[g])
            rates[g][z] = rate
            shares[g][z] = share

    out = {z: {} for z in zips}
    coverage, median = {}, {}
    for g, key in KEYS.items():
        known = []
        for z in zips:
            r = rates[g][z]
            if r is None:
                continue
            out[z][key] = round(r * 100_000, 2)
            known.append(out[z][key])
        coverage[g] = round(len(known) / (len(zips) or 1), 4)
        median[g] = round(statistics.median(known), 2) if known else None
    out = {z: v for z, v in out.items() if v}
    # The typical ZIP's all-hazards figure, for the page's "typical" line.
    # Over ZIPs with all four scored; a sum of medians is not a median.
    totals = [sum(v.values()) for v in out.values() if len(v) == len(KEYS)]
    median["total"] = round(statistics.median(totals), 2) if totals else None

    # States where the join mostly failed are named, not averaged away. A
    # tract-ID scheme change like Connecticut's 2022 planning regions (now
    # handled by recode_tracts) shows up here rather than as a quietly
    # empty state.
    state_of = {r["TRACTFIPS"][:2]: r.get("STATEABBRV") for r in tract_rows if r.get("TRACTFIPS")}
    per_state = collections.defaultdict(lambda: [0, 0])
    for z in zips:
        st = None
        if parts.get(z):
            st = state_of.get(parts[z][0][0][:2])
        per_state[st or "??"][0] += 1
        per_state[st or "??"][1] += rates["flood"][z] is not None
    weak = {st: round(ok / n, 3) for st, (n, ok) in per_state.items() if n >= 20 and ok / n < 0.8}

    return {
        "_meta": {
            "as_of": date.today().isoformat(),
            "built": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "FEMA National Risk Index (census tracts) via FEMA's ArcGIS feature "
                      "service; Census 2020 ZCTA-to-tract relationship file",
            "nri_version": next(iter(versions)),
            "tracts": len(tract_rows),
            "zips": len(zips),
            "measure": "expected annual building loss per $100,000 of building value",
            "groups": {g: list(c) for g, c in E.HAZARD_GROUPS.items()},
            "min_scored_share": E.MIN_SCORED_SHARE,
            "coverage": coverage,
            "median": median,
            "weak_states": weak,
            "recoded_tracts": recoded,
            "fields": {k: f"{g} loss $/yr per $100k" for g, k in KEYS.items()},
        },
        "zips": out,
    }


def guard(payload: dict, previous: dict | None, force: bool) -> None:
    cov = payload["_meta"]["coverage"]["flood"]
    if cov < MIN_FLOOD_COVERAGE and not force:
        raise Refuse(f"only {cov:.1%} of ZIPs got a flood figure (floor {MIN_FLOOD_COVERAGE:.0%})")
    if previous and not force:
        prev = (previous.get("_meta") or {}).get("coverage", {}).get("flood")
        if isinstance(prev, (int, float)) and prev - cov > MAX_COVERAGE_DROP:
            raise Refuse(f"flood coverage fell from {prev:.1%} to {cov:.1%}; re-run with "
                         f"--force if it is expected")


def load_zips() -> list:
    c = sqlite3.connect(str(ZIPS_DB))
    try:
        return [z for (z,) in c.execute("select zip from zips order by zip")]
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--force", action="store_true", help="publish despite the coverage guards")
    args = ap.parse_args()
    try:
        t0 = time.time()
        tracts = fetch_tracts()
        print(f"tracts: {len(tracts):,} in {time.time() - t0:.0f}s")
        rel = fetch(RELATIONSHIP).decode("utf-8-sig", errors="replace")
        parts = parse_relationship(rel)
        print(f"relationship file: {len(parts):,} ZCTAs")
        payload = build(tracts, parts, load_zips())
        previous = json.loads(OUT.read_text()) if OUT.exists() else None
        guard(payload, previous, args.force)
    except Refuse as e:
        print(f"::error::REFUSING TO PUBLISH zip_hazards.json — {e}")
        return 1
    m = payload["_meta"]
    OUT.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    print(f"wrote {OUT} ({OUT.stat().st_size:,} bytes) — NRI {m['nri_version']}")
    print("  coverage  " + "  ".join(f"{g} {c:.1%}" for g, c in m["coverage"].items()))
    print("  median    " + "  ".join(f"{g} ${v}" for g, v in m["median"].items()))
    print(f"  weak states (<80% flood coverage): {m['weak_states'] or 'none'}")
    print(f"  Connecticut tracts renumbered to 2020 IDs: {m['recoded_tracts']}")
    for z in ("44113", "43215", "45202", "70112", "33139", "94110", "80202", "06103", "06902"):
        print(f"  {z}  {payload['zips'].get(z)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
