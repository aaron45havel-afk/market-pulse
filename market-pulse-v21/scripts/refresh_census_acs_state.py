"""Refresh state-level Census ACS demographics annually.

Pulls median household income, % adults 25+ with a bachelor's+ degree,
and median age for all 50 states + DC from the Census ACS 5-year bulk
files (acs_bulk.py) — no API key.

Output: ``data/census_acs_state_overrides.json``. ``data_providers``
patches CHOROPLETH_STATES on import — refreshes the median_income +
median_age fields that have been hardcoded snapshots since launch.
Adds a new pct_bachelors_state field (the ZCTA-level pct_bachelors
already exists per ZIP, but state-level didn't).

Cadence: annual. ACS 5-year vintages release each December (2023
data releases Dec 2024). Cron is set to early January for the most
recent vintage.

Usage:
    python scripts/refresh_census_acs_state.py [--vintage 2023] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "data" / "census_acs_state_overrides.json"

# State FIPS → 2-letter code (50 + DC). Same map used in
# build_national_zips.py — keeping a copy here avoids tight coupling
# between the two scripts.
STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}

ACS_VARS = (
    "B19013_001E,"   # Median household income
    "B15003_001E,"   # Pop 25+ (denominator for bachelor's %)
    "B15003_022E,"   # Bachelor's
    "B15003_023E,"   # Master's
    "B15003_024E,"   # Professional
    "B15003_025E,"   # Doctorate
    "B01002_001E"    # Median age
)


def derive_state(fips_rows: dict) -> dict[str, dict]:
    """{state_fips: {var: value}} → {state_code: {median_income,
    pct_bachelors_state, median_age}}. Pure."""
    out: dict[str, dict] = {}
    for fips, v in fips_rows.items():
        code = STATE_FIPS.get(fips.zfill(2))
        if not code:
            continue
        income = v.get("B19013_001E")
        edu_total = v.get("B15003_001E")
        ba = sum((v.get(k) or 0) for k in ("B15003_022E", "B15003_023E",
                                           "B15003_024E", "B15003_025E"))
        age = v.get("B01002_001E")
        entry: dict = {}
        if income is not None:
            entry["median_income"] = int(income)
        if edu_total:
            entry["pct_bachelors_state"] = round(ba / edu_total * 100, 1)
        if age is not None:
            entry["median_age"] = round(float(age), 1)
        if entry:
            out[code] = entry
    return out


def fetch_state_acs(vintage: int | None) -> tuple[int, dict[str, dict]]:
    """51 state rows from the Census Bureau's keyless bulk files (acs_bulk.py)
    — the data API needs a key this repo couldn't activate. `vintage` None
    means the newest 5-year release on the server."""
    sys.path.insert(0, str(REPO_ROOT))
    import acs_bulk as AB
    year = vintage or AB.latest_year(date.today().year)
    log.info("Fetching ACS %d 5-year for all states (bulk files) …", year)
    try:
        raw = AB.fetch(("b19013", "b15003", "b01002"), year,
                       set(ACS_VARS.split(",")), prefix=AB.STATE_PREFIX, min_rows=51)
    except AB.AcsUnavailable as e:
        raise SystemExit(f"Census ACS bulk files unavailable: {e}")
    out = derive_state(raw)
    log.info("  → %d states with ACS data", len(out))
    return year, out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--vintage", type=int, default=None,
        help="ACS 5-year vintage year (default: the newest on the Census server).",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + parse but don't write the JSON file.")
    args = parser.parse_args(argv)

    vintage, overrides = fetch_state_acs(args.vintage)
    if not overrides:
        log.error("No state data returned — aborting.")
        return 1

    payload = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "vintage": vintage,
            "source": f"Census ACS {vintage} 5-year, state level (keyless bulk files)",
            "states_covered": len(overrides),
        },
        "overrides": overrides,
    }
    if args.dry_run:
        log.info("--dry-run: would write %d states to %s", len(overrides), OUTPUT_PATH)
        for k, v in list(overrides.items())[:3]:
            log.info("  sample %s: %s", k, v)
        return 0
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %s", OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
