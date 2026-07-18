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
# CPI ride-along: the landscaper rate cards were calibrated in July
# 2026; scaling them by CPIAUCSL(latest)/CPIAUCSL(base) keeps them in
# today's dollars automatically. Base month = last full CPI month
# before calibration.
CPI_SERIES = "CPIAUCSL"
CPI_BASE_MONTH = "2026-06-01"


def fetch_latest_rate(api_key: str, series_id: str = SERIES_ID,
                      extra: dict | None = None) -> tuple[float, str] | None:
    """Pulls the most recent non-empty MORTGAGE30US observation. FRED
    returns weekly values; we grab the last 4 to be safe and walk back
    to the first non-empty. Returns (rate, date_str) or None on failure.

    Retries transient errors (FRED 5xx, 429, network blips) up to 3
    times with linear backoff. The old code called raise SystemExit
    on the very first error — a single FRED 503 was killing the whole
    weekly workflow."""
    q = {"series_id": series_id, "api_key": api_key, "file_type": "json",
         "sort_order": "desc", "limit": 4}
    q.update(extra or {})
    params = urllib.parse.urlencode(q)
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
        log.error("FRED %s: %s (gave up after 3 attempts)", series_id, last_err)
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

    # CPI ride-along — failure here must never block the mortgage-rate
    # write (consumers fall back to a 1.0 multiplier without the block).
    try:
        latest = fetch_latest_rate(api_key, CPI_SERIES)
        base = fetch_latest_rate(api_key, CPI_SERIES, {
            "sort_order": "asc", "limit": 1,
            "observation_start": CPI_BASE_MONTH,
            "observation_end": CPI_BASE_MONTH,
        })
        if latest and base and base[0] > 0:
            payload["cpi"] = {
                "series": CPI_SERIES,
                "base_month": CPI_BASE_MONTH[:7],
                "base": base[0],
                "latest": latest[0],
                "latest_month": latest[1][:7],
            }
            log.info("→ %s: base %.1f (%s) latest %.1f (%s)", CPI_SERIES,
                     base[0], CPI_BASE_MONTH[:7], latest[0], latest[1][:7])
        else:
            log.warning("CPI fetch incomplete — writing rates without cpi block")
    except Exception as e:
        log.warning("CPI ride-along failed (%s) — writing rates without it", e)
    if args.dry_run:
        log.info("--dry-run: would write %s", payload)
        return 0
    RATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    RATES_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %s", RATES_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
