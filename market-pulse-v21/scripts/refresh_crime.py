"""Refresh the crime layer from the FBI Crime Data Explorer API.

WHY THIS EXISTS AS AN ACTION AND NOT A LOCAL SCRIPT: the dev sandbox's
egress proxy blocks every .gov host (cde.ucr.cjis.gov, api.usa.gov all
return 403/000), so the crime layer had to be researched by hand. GitHub
Actions runners have unrestricted egress, so THEY can pull the FBI API
directly — this script runs in CI, not locally.

Needs a free api.data.gov key in the CDE_API_KEY repo secret.
Get one at https://api.data.gov/signup/ (instant, no approval).

Behaviour, in order of caution:
  * Only refreshes cities ALREADY in the table — this is a refresher, not
    a discovery tool; the researched notes and per-city caveats survive.
  * A city the API cannot match keeps its existing hand-researched entry
    rather than being blanked.
  * confidence "suspect" entries stay suspect: those were flagged because
    the published figure itself is untrustworthy (partial NIBRS
    submissions reading implausibly low), and a fresh pull of the same
    bad submission does not fix that.
  * Writes nothing if the API returns fewer than MIN_SUCCESS matches —
    a half-failed run must not silently gut the safety gate.

Usage:  python scripts/refresh_crime.py [--year 2024] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CRIME = ROOT / "data" / "headroom" / "crime.json"
API = "https://api.usa.gov/crime/fbi/cde"
MIN_SUCCESS = 40           # refuse to write a mostly-empty refresh
TIMEOUT = 30
RETRIES = 3


def _get(url: str, params: dict) -> dict | list | None:
    params = {**params, "API_KEY": os.environ.get("CDE_API_KEY", "")}
    full = f"{url}?{urllib.parse.urlencode(params)}"
    for attempt in range(RETRIES):
        try:
            with urllib.request.urlopen(full, timeout=TIMEOUT) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:                       # rate limited — back off
                time.sleep(2 ** attempt * 3)
                continue
            if e.code in (404, 400):
                return None
            if attempt == RETRIES - 1:
                print(f"  HTTP {e.code} on {url}", file=sys.stderr)
                return None
            time.sleep(2 ** attempt)
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            if attempt == RETRIES - 1:
                print(f"  {type(e).__name__} on {url}", file=sys.stderr)
                return None
            time.sleep(2 ** attempt)
    return None


def agencies_for_state(state: str) -> dict[str, dict]:
    """city-name(lower) -> agency record, for municipal police agencies."""
    data = _get(f"{API}/agency/byStateAbbr/{state}", {})
    if not data:
        return {}
    rows = data if isinstance(data, list) else data.get("results", data.get("data", []))
    out = {}
    for a in rows or []:
        if not isinstance(a, dict):
            continue
        name = (a.get("agency_name") or "").strip()
        ori = a.get("ori") or a.get("agency_ori")
        atype = (a.get("agency_type_name") or "").lower()
        if not name or not ori:
            continue
        # Municipal agencies only — a county sheriff's rate is not a city rate.
        if atype and "city" not in atype and "municipal" not in atype:
            continue
        key = name.lower().replace(" police department", "").replace(" pd", "").strip()
        out.setdefault(key, {"ori": ori, "name": name,
                             "population": a.get("population")})
    return out


def rate_for(ori: str, offense: str, year: int) -> tuple[int | None, int | None]:
    """(count, population) for one agency/offense/year."""
    d = _get(f"{API}/summarized/agency/{ori}/{offense}",
             {"from": f"01-{year}", "to": f"12-{year}"})
    if not d:
        return None, None
    # CDE shapes vary by release; accept the common ones.
    rows = d.get("offenses", d) if isinstance(d, dict) else d
    if isinstance(rows, dict):
        rows = rows.get("actuals", rows.get("data", []))
    total, pop = 0, None
    if isinstance(rows, dict):
        for _k, v in rows.items():
            if isinstance(v, dict):
                for _m, n in v.items():
                    try:
                        total += int(n)
                    except (TypeError, ValueError):
                        pass
    elif isinstance(rows, list):
        for r in rows:
            if not isinstance(r, dict):
                continue
            try:
                total += int(r.get("actual") or r.get("count") or 0)
            except (TypeError, ValueError):
                pass
            pop = pop or r.get("population")
    if isinstance(d, dict):
        pop = pop or d.get("population")
    return (total or None), pop


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=date.today().year - 2,
                    help="FBI data lags ~2 years at full coverage")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    if not os.environ.get("CDE_API_KEY"):
        print("CDE_API_KEY not set — get a free key at https://api.data.gov/signup/",
              file=sys.stderr)
        return 2
    if not CRIME.exists():
        print(f"{CRIME} missing", file=sys.stderr)
        return 2

    blob = json.loads(CRIME.read_text())
    table = blob["table"]
    by_state: dict[str, list[str]] = {}
    for key in table:
        if ", " not in key:
            continue
        by_state.setdefault(key.rsplit(", ", 1)[1], []).append(key)

    updated = kept = missed = 0
    for state, cities in sorted(by_state.items()):
        ags = agencies_for_state(state)
        if not ags:
            print(f"{state}: no agency list returned — keeping existing entries")
            kept += len(cities)
            continue
        for city_key in cities:
            city = city_key.rsplit(", ", 1)[0].lower()
            ag = ags.get(city)
            entry = table[city_key]
            if not ag:
                missed += 1
                continue
            if entry.get("confidence") == "suspect":
                kept += 1                     # a fresh pull of a bad submission is still bad
                continue
            v, pop = rate_for(ag["ori"], "violent-crime", a.year)
            p, _ = rate_for(ag["ori"], "property-crime", a.year)
            pop = pop or ag.get("population")
            if not (v and pop):
                missed += 1
                continue
            entry.update({
                "violent_per_100k": round(v / pop * 100_000, 1),
                "property_per_100k": (round(p / pop * 100_000, 1) if p else None),
                "year": a.year, "confidence": "high",
                "source": f"FBI Crime Data Explorer API, agency {ag['ori']} ({ag['name']}), {a.year}",
                "note": (entry.get("note") or "") + f" [auto-refreshed {date.today().isoformat()}]",
            })
            updated += 1
            time.sleep(0.15)                  # be polite to api.data.gov

    print(f"updated={updated} kept={kept} unmatched={missed} of {len(table)} cities")
    if updated < MIN_SUCCESS:
        print(f"REFUSING TO WRITE: only {updated} cities refreshed (min {MIN_SUCCESS}). "
              "Existing hand-researched data is safer than a half-failed pull.",
              file=sys.stderr)
        return 1
    blob["_meta"]["as_of"] = date.today().strftime("%Y-%m")
    blob["_meta"]["status"] = (f"auto-refreshed from FBI CDE {date.today().isoformat()} "
                               f"({updated} cities, data year {a.year}); "
                               "hand-researched entries retained where unmatched")
    if a.dry_run:
        print("dry run — not writing")
        return 0
    CRIME.write_text(json.dumps(blob, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
