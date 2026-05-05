"""Refresh state-level unemployment rates from FRED.

Pulls each state's unemployment rate series (e.g. CAUR for California,
TXUR for Texas) from FRED, takes the latest monthly value, and writes
the 51-state result to ``data/bls_overrides.json``. ``data_providers``
applies this on import — the new ``unemployment_pct`` metric lands on
CHOROPLETH_STATES and shows up as a sidebar metric on /map.

Cadence: monthly. BLS publishes state unemployment ~3 weeks after
month-end (e.g. April data lands ~mid-May). The cron fires on the
15th to be safe.

Wage growth via BLS QCEW deferred to a follow-up — series ID format
varies by industry/ownership/size and the BLS API is rate-limited
without a key. Unemployment is the high-value, easy-to-fetch piece.

Requires FRED_API_KEY (already configured for refresh-rates.yml).

Usage:
    python scripts/refresh_bls.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "data" / "bls_overrides.json"

# FRED state-unemployment series naming: 2-letter state code + "UR".
# All 50 states + DC have one. AS/PR/etc not covered.
STATE_CODES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL",
    "GA","HI","ID","IL","IN","IA","KS","KY","LA","ME",
    "MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
    "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI",
    "SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
]


def fetch_latest_observation(series_id: str, api_key: str) -> tuple[float, str] | None:
    """Returns (value, observation_date) for the most recent non-empty
    obs of a FRED series, or None on failure / no data."""
    params = urllib.parse.urlencode({
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 4,
    })
    url = f"https://api.stlouisfed.org/fred/series/observations?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        log.warning("  %s: HTTP %s", series_id, e.code)
        return None
    except urllib.error.URLError as e:
        log.warning("  %s: network error %s", series_id, e.reason)
        return None
    for obs in data.get("observations", []):
        v = obs.get("value")
        if v not in (None, "", "."):
            try:
                return float(v), obs.get("date", "")
            except ValueError:
                continue
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + parse but don't write the JSON file.")
    args = parser.parse_args(argv)

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        log.error("FRED_API_KEY not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html")
        return 1

    log.info("Fetching state unemployment from FRED for %d states …", len(STATE_CODES))
    overrides: dict[str, dict] = {}
    latest_date = ""
    for code in STATE_CODES:
        result = fetch_latest_observation(f"{code}UR", api_key)
        if not result:
            continue
        rate, obs_date = result
        overrides[code] = {"unemployment_pct": round(rate, 1)}
        if obs_date > latest_date:
            latest_date = obs_date
    log.info("  → %d/%d states with data; latest obs %s",
             len(overrides), len(STATE_CODES), latest_date)

    if not overrides:
        log.error("No data returned — aborting.")
        return 1

    payload = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "latest_obs": latest_date,
            "source": "FRED state unemployment series ({STATE}UR), Bureau of Labor Statistics",
            "states_covered": len(overrides),
        },
        "overrides": overrides,
    }
    if args.dry_run:
        log.info("--dry-run: would write %d states to %s", len(overrides), OUTPUT_PATH)
        sample = list(overrides.items())[:3]
        for k, v in sample:
            log.info("  sample %s: %s", k, v)
        return 0
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %s", OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
