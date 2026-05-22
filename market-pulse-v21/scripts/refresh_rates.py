"""Refresh the current 30-year fixed mortgage rate from FRED.

Pulls the FRED series MORTGAGE30US (Freddie Mac PMMS, weekly average)
and writes the latest non-empty observation to ``data/rates.json``.
``data_providers`` reads this on module import + the /map header chip
+ /affordability default both pick it up.

Usage:
    python scripts/refresh_rates.py [--dry-run]

Run cadence: weekly is enough — Freddie Mac publishes Thursdays. The
GitHub Action cron fires Friday morning UTC.

Requires the FRED_API_KEY environment variable. Free key:
https://fred.stlouisfed.org/docs/api/api_key.html.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
RATES_PATH = REPO_ROOT / "data" / "rates.json"
SERIES_ID = "MORTGAGE30US"


def fetch_latest_rate(api_key: str) -> tuple[float, str] | None:
    """Pulls the most recent non-empty MORTGAGE30US observation. FRED
    returns weekly values; we grab the last 4 to be safe and walk back
    to the first non-empty. Returns (rate, date_str) or None on failure.

    Retries transient errors (FRED 5xx, 429, network blips) up to 3
    times with linear backoff. The old code called raise SystemExit
    on the very first error — a single FRED 503 was killing the whole
    weekly workflow."""
    params = urllib.parse.urlencode({
        "series_id": SERIES_ID,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 4,
    })
    url = f"https://api.stlouisfed.org/fred/series/observations?{params}"
    log.info("Fetching %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
    data = None
    last_err: str | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
            last_err = None
            break
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            # 4xx (e.g. invalid series ID) won't get better with a
            # retry. Only retry 5xx and rate-limit (429).
            if not (e.code >= 500 or e.code == 429):
                break
        except urllib.error.URLError as e:
            last_err = f"network error {e.reason}"
        if attempt < 2:
            time.sleep(2 * (attempt + 1))
    if data is None:
        log.error("FRED %s: %s (gave up after 3 attempts)", SERIES_ID, last_err)
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
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + parse but don't write the JSON file.",
    )
    args = parser.parse_args(argv)

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        log.error("FRED_API_KEY not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html")
        return 1

    result = fetch_latest_rate(api_key)
    if not result:
        log.error("No usable observations returned for %s", SERIES_ID)
        return 1
    rate, obs_date = result
    log.info("→ %s: %.2f%% (week ending %s)", SERIES_ID, rate, obs_date)

    payload = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "source": f"FRED {SERIES_ID} (Freddie Mac PMMS, weekly avg)",
        },
        "mortgage_30y": rate,
        "mortgage_30y_obs_date": obs_date,
    }
    if args.dry_run:
        log.info("--dry-run: would write %s", payload)
        return 0
    RATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    RATES_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %s", RATES_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
