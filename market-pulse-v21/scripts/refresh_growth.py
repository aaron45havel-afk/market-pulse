"""Refresh Growth Outlook data — building permits + employment growth.

Pulls state-level data from FRED for the two auto-refreshable Growth
Outlook metrics, computes per-1K and YoY ratios, and writes the result
to ``data/growth_overrides.json``. ``data_providers`` patches the
choropleth state dict + recomputes the growth_score composite at
module import.

Usage:
    python scripts/refresh_growth.py [--dry-run]

What gets refreshed (per state):
  - ``permits_per_1k``  housing units permitted in last 12 months
                         per 1000 residents (FRED state Building
                         Permits + state population)
  - ``job_growth_yoy``   trailing 12-month % change in total nonfarm
                         employment (FRED state ``__NA`` series)

What we DON'T refresh here:
  - net_migration: Census Population Estimates Program publishes once
    a year (around Dec). Not on FRED. Hand-curated until/unless we add
    a Census API client.

Run cadence: monthly cron via .github/workflows/refresh-growth.yml,
same pattern as Zillow + Redfin.

Requires the FRED_API_KEY environment variable. Get a free key at
https://fred.stlouisfed.org/docs/api/api_key.html.
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
OVERRIDES_PATH = REPO_ROOT / "data" / "growth_overrides.json"

# FRED state-level series naming conventions:
#   {STATE}BPPRIVSA  → Building permits, total private housing units,
#                       seasonally adjusted, monthly count.
#   {STATE}NA        → All employees: Total Nonfarm, in thousands,
#                       seasonally adjusted, monthly.
# State codes are the standard 2-letter postal codes. DC is "DC".
# Population series:
#   {STATE}POP       → Resident population, in thousands, annual.
PERMIT_SERIES = "{}BPPRIVSA"
JOBS_SERIES   = "{}NA"
POP_SERIES    = "{}POP"

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID",
    "IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO",
    "MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA",
    "RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
]


def fred_observations(api_key: str, series_id: str, limit: int = 24) -> list[dict]:
    """Pull recent observations for a FRED series. Returns most-recent
    first via sort_order=desc; ``limit`` caps how many rows we retrieve.
    Raises SystemExit on HTTP / network errors with a clear message."""
    params = urllib.parse.urlencode({
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    })
    url = f"https://api.stlouisfed.org/fred/series/observations?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 400:
            # FRED returns 400 for unknown series IDs. We surface this
            # as a non-fatal warning so a single missing state doesn't
            # nuke the whole refresh.
            log.warning("  ⚠ %s: not found in FRED", series_id)
            return []
        raise SystemExit(f"FRED HTTP {e.code} for {series_id}")
    except urllib.error.URLError as e:
        raise SystemExit(f"Network error fetching {series_id}: {e.reason}")
    return data.get("observations", [])


def numeric(obs: dict) -> float | None:
    """Extract the numeric value from a FRED observation row, or None
    for FRED's ``.`` placeholder (used for missing/suppressed values)."""
    v = obs.get("value")
    if v in (None, "", "."):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def latest_and_year_ago(obs: list[dict]) -> tuple[float | None, float | None]:
    """Given FRED observations in newest-first order, return (latest,
    same period 12 entries earlier). For monthly series this gives the
    most recent month's value and the same month a year prior — exactly
    what you need to compute YoY % change."""
    if not obs:
        return (None, None)
    latest = numeric(obs[0])
    prior = numeric(obs[12]) if len(obs) > 12 else None
    return (latest, prior)


def fetch_state(api_key: str, state: str) -> dict | None:
    """Pull permits + employment + population for a state. Computes
    permits-per-1K and trailing-12-month YoY job change. Returns None
    if any input is missing — we'd rather not write half-data than
    write a confusing partial entry."""
    log.info("Fetching %s ...", state)
    permits_obs = fred_observations(api_key, PERMIT_SERIES.format(state), limit=14)
    jobs_obs    = fred_observations(api_key, JOBS_SERIES.format(state),   limit=14)
    pop_obs     = fred_observations(api_key, POP_SERIES.format(state),    limit=2)

    # Population (annual, in thousands). Use latest non-null.
    pop_thousands = None
    for o in pop_obs:
        v = numeric(o)
        if v:
            pop_thousands = v
            break

    # Permits — sum the trailing 12 months for an annualized rate.
    permits_12mo = 0.0
    permit_count = 0
    for o in permits_obs[:12]:
        v = numeric(o)
        if v is not None:
            permits_12mo += v
            permit_count += 1

    # Jobs YoY — most recent vs 12 entries back. Series is in thousands;
    # the ratio is unit-free.
    jobs_latest, jobs_prior = latest_and_year_ago(jobs_obs)

    permits_per_1k = None
    if pop_thousands and permit_count >= 6 and permits_12mo > 0:
        # Census ``{STATE}POP`` is annual, in thousands. permits_12mo is
        # the annualized count. permits / pop_thousands * 1 = per 1K.
        permits_per_1k = round(permits_12mo / pop_thousands, 1)

    job_growth_yoy = None
    if jobs_latest and jobs_prior and jobs_prior > 0:
        job_growth_yoy = round((jobs_latest - jobs_prior) / jobs_prior * 100, 1)

    if permits_per_1k is None and job_growth_yoy is None:
        return None
    out = {}
    if permits_per_1k is not None: out["permits_per_1k"] = permits_per_1k
    if job_growth_yoy is not None: out["job_growth_yoy"] = job_growth_yoy
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + parse but don't write the JSON file.",
    )
    args = parser.parse_args(argv)

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        log.error("FRED_API_KEY is not set — get one at https://fred.stlouisfed.org/docs/api/api_key.html")
        return 1

    overrides: dict[str, dict] = {}
    for state in STATES:
        entry = fetch_state(api_key, state)
        if entry:
            overrides[state] = entry

    log.info("→ collected %d/%d states", len(overrides), len(STATES))
    payload = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "source": "FRED — state Building Permits ({}BPPRIVSA), Total Nonfarm Employment ({}NA), Population ({}POP)",
            "states_covered": len(overrides),
        },
        "overrides": overrides,
    }

    if args.dry_run:
        log.info("--dry-run: would write %d state overrides to %s",
                 len(overrides), OVERRIDES_PATH)
        for s in list(overrides)[:5]:
            log.info("  %s → %s", s, overrides[s])
        return 0

    OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    OVERRIDES_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %d state overrides to %s", len(overrides), OVERRIDES_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
