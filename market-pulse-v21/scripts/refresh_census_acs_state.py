"""Refresh state-level Census ACS demographics annually.

Pulls median household income, % adults 25+ with a bachelor's+ degree,
and median age for all 50 states + DC from the Census ACS 5-year API.
Single bulk call, no API key required for this volume.

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
import os
import sys
import urllib.error
import urllib.request
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


def fetch_state_acs(vintage: int) -> dict[str, dict]:
    """Single bulk API call → 51 state rows. Returns
    {state_code: {median_income, pct_bachelors, median_age}}.
    Census null markers (negative ints) become None."""
    api_key = os.environ.get("CENSUS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit(
            "CENSUS_API_KEY is not set. Get one free at "
            "https://api.census.gov/data/key_signup.html and add it as a "
            "GitHub repo secret + workflow env var."
        )
    url = (
        f"https://api.census.gov/data/{vintage}/acs/acs5"
        f"?get={ACS_VARS}&for=state:*&key={api_key}"
    )
    log.info("Fetching ACS %d 5-year for all states …", vintage)
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            rows = json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Census ACS HTTP {e.code} — vintage {vintage} may not be released yet")
    except urllib.error.URLError as e:
        raise SystemExit(f"Network error: {e.reason}")

    headers = rows[0]
    inc_idx       = headers.index("B19013_001E")
    edu_total_idx = headers.index("B15003_001E")
    ba_idx        = headers.index("B15003_022E")
    ma_idx        = headers.index("B15003_023E")
    prof_idx      = headers.index("B15003_024E")
    doc_idx       = headers.index("B15003_025E")
    age_idx       = headers.index("B01002_001E")
    state_idx     = headers.index("state")

    def _int(v):
        try:
            n = int(float(v))
        except (ValueError, TypeError):
            return None
        return n if n >= 0 else None

    def _float(v):
        try:
            n = float(v)
        except (ValueError, TypeError):
            return None
        return n if n >= 0 else None

    out: dict[str, dict] = {}
    for row in rows[1:]:
        fips = row[state_idx].strip().zfill(2)
        code = STATE_FIPS.get(fips)
        if not code:
            continue
        income = _int(row[inc_idx])
        edu_total = _int(row[edu_total_idx])
        ba_count = sum((_int(row[i]) or 0) for i in (ba_idx, ma_idx, prof_idx, doc_idx))
        pct_bach = (ba_count / edu_total * 100) if (edu_total and edu_total > 0) else None
        age = _float(row[age_idx])
        entry: dict = {}
        if income is not None:        entry["median_income"]     = income
        if pct_bach is not None:      entry["pct_bachelors_state"] = round(pct_bach, 1)
        if age is not None:           entry["median_age"]        = round(age, 1)
        if entry:
            out[code] = entry
    log.info("  → %d states with ACS data", len(out))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--vintage", type=int, default=2022,
        help="ACS 5-year vintage year (default: 2022). Update annually after Dec release.",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + parse but don't write the JSON file.")
    args = parser.parse_args(argv)

    overrides = fetch_state_acs(args.vintage)
    if not overrides:
        log.error("No state data returned — aborting.")
        return 1

    payload = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "vintage": args.vintage,
            "source": f"Census ACS {args.vintage} 5-year API (state-level)",
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
