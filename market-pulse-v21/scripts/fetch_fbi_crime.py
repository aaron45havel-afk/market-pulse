"""Every city police department's reported crime, from the FBI Crime Data Explorer.

    python scripts/fetch_fbi_crime.py [--end-year 2025] [--states OH,IN] [--force]

Writes data/headroom/fbi_agencies.json: for every CITY agency the FBI lists,
three calendar years of violent and property offenses — per year, the sum
and how many of the twelve months the agency actually reported — with the
agency's population. Raw material only. Which figures are trustworthy
enough to call a city safe is decided in crime_build.py, offline and under
test, so the rules can change without another hour of requests.

NO KEY. The Crime Data Explorer's own web backend (cde.ucr.cjis.gov/LATEST)
answers without the api.data.gov key the public API gateway asks for, and
returns the same figures (checked side by side for Akron, 2024 and 2025).

ONE REQUEST PER AGENCY PER OFFENSE. There is no bulk file, so only the
agencies that match a ZIP city on the board are fetched (the build's own
matching rules) — the rest are villages with no ZIP here.

A BLANK MONTH IS NOT A ZERO. The FBI returns null for a month an agency
did not report; summing it as zero is how a department that sent four
months of data becomes the safest town in the state. Months are counted
separately so the build can demand a complete year.

IT REFUSES TO PUBLISH A BROKEN PULL: agency lists missing for more than a
few states, far fewer agencies than the FBI lists, or too many failed
requests all exit non-zero and leave the committed file alone.
"""
from __future__ import annotations

import argparse
import http.client
import json
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
OUT = ROOT / "data" / "headroom" / "fbi_agencies.json"
ZIPS_DB = ROOT / "data" / "zips.db"
UA = {"User-Agent": "MarketPulse/1.0 (city crime layer; invoice@archfms.com)"}
STATES = ("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS "
          "MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY").split()
OFFENSES = {"v": "violent-crime", "p": "property-crime"}
YEARS = 3
THREADS = 6

MIN_AGENCIES = 5_000           # that match a ZIP city; the FBI lists 11,784 city agencies
MAX_STATE_LIST_FAILURES = 2
MAX_REQUEST_FAILURE = 0.02


class Refuse(Exception):
    """A pull that must not publish."""


# One persistent HTTPS connection per worker thread. Measured from GitHub's
# runners in Sept 2026: a request answers in ~0.3s, but about one NEW
# connection in twenty-five hangs in connect until it times out. Opening a
# fresh connection per request — what urllib does — turned that into a
# stall every few seconds and a 90-minute run that never finished; reusing
# the connection makes new connects rare, and a stuck one is dropped after
# TIMEOUT seconds and retried on a fresh socket.
HOST = "cde.ucr.cjis.gov"
PREFIX = "/LATEST"
TIMEOUT = 10
_local = threading.local()


def _conn() -> http.client.HTTPSConnection:
    c = getattr(_local, "conn", None)
    if c is None:
        c = http.client.HTTPSConnection(HOST, timeout=TIMEOUT)
        _local.conn = c
    return c


def _drop() -> None:
    c = getattr(_local, "conn", None)
    if c is not None:
        c.close()
    _local.conn = None


def _get(path: str, attempts: int = 5):
    last = None
    for i in range(attempts):
        try:
            c = _conn()
            c.request("GET", PREFIX + path, headers={**UA, "Connection": "keep-alive"})
            r = c.getresponse()
            body = r.read()
            if r.status == 404:
                return None
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return json.loads(body)
        except Exception as e:                               # noqa: BLE001
            last = e
            _drop()
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"{path}: {last}")


# ─── pure parsers (tests/test_crime_build.py) ────────────────────────
def city_agencies(listing: dict, state: str) -> list:
    """/agency/byStateAbbr/{st} → [agency], city agencies only.

    The listing is keyed by COUNTY, each a list of agencies — not the flat
    list an earlier refresher assumed, which is why it never matched one
    city. A county sheriff's rate is not a city's, so only
    agency_type_name == "City" is kept.
    """
    if not isinstance(listing, dict):
        raise ValueError(f"{state}: agency listing is not an object keyed by county")
    out = {}
    for agencies in listing.values():
        for a in agencies or []:
            if not isinstance(a, dict) or a.get("agency_type_name") != "City":
                continue
            ori = (a.get("ori") or "").strip()
            if not ori:
                continue
            out[ori] = {"ori": ori, "name": (a.get("agency_name") or "").strip(),
                        "state": state, "lat": a.get("latitude"), "lng": a.get("longitude"),
                        "nibrs_start": a.get("nibrs_start_date")}
    return list(out.values())


def yearly(payload: dict, years: list) -> dict:
    """A /summarized/agency response → {year: {"n": sum, "m": months, "pop": population}}.

    The agency's own series is the one entry in `actuals` ending
    " Offenses" (the rest of the response is state and national context).
    A null month is not reported and does not count toward `m` or `n`.
    """
    off = (payload or {}).get("offenses") or {}
    actuals = off.get("actuals") or {}
    keys = [k for k in actuals if k.endswith(" Offenses")]
    if len(keys) != 1:
        raise ValueError(f"expected one agency series in actuals, got {keys}")
    agency = keys[0][: -len(" Offenses")]
    series = actuals[keys[0]] or {}
    pops = (((payload.get("populations") or {}).get("population") or {}).get(agency) or {})
    out = {}
    for y in years:
        months = [f"{m:02d}-{y}" for m in range(1, 13)]
        vals = [series.get(k) for k in months]
        got = [v for v in vals if isinstance(v, (int, float))]
        pv = [pops.get(k) for k in months if isinstance(pops.get(k), (int, float))]
        out[str(y)] = {"n": int(sum(got)), "m": len(got),
                       "pop": round(sum(pv) / len(pv)) if pv else None}
    return out


# ─── fetch ───────────────────────────────────────────────────────────
def matchable(agencies: list) -> list:
    """Only the agencies that tie to a ZIP city on the board — the same
    name, distance and ambiguity rules the build applies (crime_build.py).
    The rest are villages with no ZIP here; fetching them is wasted work."""
    import crime_build as C
    from build_crime import zip_cities
    conn = sqlite3.connect(str(ZIPS_DB))
    try:
        cities = zip_cities(conn)
    finally:
        conn.close()
    matched, _ = C.match_agencies(agencies, cities)
    keep = {a["ori"] for a in matched.values()}
    return [a for a in agencies if a["ori"] in keep]


def fetch(states: list, end_year: int) -> dict:
    years = list(range(end_year - YEARS + 1, end_year + 1))
    agencies, list_failures = [], []
    for st in states:
        try:
            agencies += city_agencies(_get(f"/agency/byStateAbbr/{st}") or {}, st)
        except Exception as e:                               # noqa: BLE001
            print(f"  agency list failed for {st}: {e}", flush=True)
            list_failures.append(st)
    listed = len(agencies)
    agencies = matchable(agencies)
    print(f"{listed:,} city agencies in {len(states) - len(list_failures)} states; "
          f"{len(agencies):,} match a ZIP city and will be fetched", flush=True)

    window = f"from=01-{years[0]}&to=12-{years[-1]}"
    failed = []

    def one(a):
        rec = dict(a)
        for key, offense in OFFENSES.items():
            try:
                payload = _get(f"/summarized/agency/{a['ori']}/{offense}?{window}")
                rec[key] = yearly(payload, years) if payload else None
            except Exception as e:                           # noqa: BLE001
                failed.append((a["ori"], offense, str(e)[:120]))
                rec[key] = None
        return rec

    t0 = time.time()
    done = [0]
    lock = threading.Lock()

    def tracked(a):
        rec = one(a)
        with lock:
            done[0] += 1
            if done[0] % 250 == 0 or done[0] == len(agencies):
                el = time.time() - t0
                print(f"  {done[0]:,}/{len(agencies):,} agencies · {el:.0f}s · "
                      f"{done[0] * len(OFFENSES) / el:.1f} req/s · {len(failed)} failed",
                      flush=True)
        return rec

    with ThreadPoolExecutor(THREADS) as ex:
        rows = list(ex.map(tracked, agencies))
    print(f"fetched in {time.time() - t0:.0f}s; {len(failed)} failed requests", flush=True)
    return {"agencies": rows, "years": years, "list_failures": list_failures,
            "failed": failed, "requests": len(agencies) * len(OFFENSES), "listed": listed}


def guard(pull: dict, limited: bool) -> None:
    if len(pull["list_failures"]) > MAX_STATE_LIST_FAILURES:
        raise Refuse(f"agency lists failed for {pull['list_failures']}")
    if not limited and len(pull["agencies"]) < MIN_AGENCIES:
        raise Refuse(f"only {len(pull['agencies']):,} city agencies (floor {MIN_AGENCIES:,})")
    if pull["requests"] and len(pull["failed"]) / pull["requests"] > MAX_REQUEST_FAILURE:
        raise Refuse(f"{len(pull['failed'])} of {pull['requests']} requests failed")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--end-year", type=int, default=date.today().year - 1,
                    help="last complete calendar year (default: last year)")
    ap.add_argument("--states", default="", help="comma-separated, for a test pull")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()] or STATES
    try:
        pull = fetch(states, args.end_year)
        if not args.force:
            guard(pull, limited=bool(args.states))
    except Refuse as e:
        print(f"::error::REFUSING TO PUBLISH fbi_agencies.json — {e}")
        return 1
    payload = {
        "_meta": {
            "source": "FBI Crime Data Explorer (cde.ucr.cjis.gov), summarized agency data",
            "built": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "as_of": date.today().isoformat(), "years": pull["years"],
            "agencies": len(pull["agencies"]), "states": states,
            "failed_requests": len(pull["failed"]), "list_failures": pull["list_failures"],
            "fields": {"v/p": "violent / property offenses per year",
                       "n": "offenses reported", "m": "months reported (of 12)",
                       "pop": "agency population, mean of the months"},
        },
        "agencies": sorted(pull["agencies"], key=lambda a: a["ori"]),
    }
    OUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"wrote {OUT} ({OUT.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
