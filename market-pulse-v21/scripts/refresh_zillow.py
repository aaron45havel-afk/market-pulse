"""Refresh Zillow ZHVI + ZORI data for ZIP codes + states.

Downloads four public CSVs from Zillow Research (ZIP-level ZHVI/ZORI +
state-level ZHVI/ZORI), matches the latest monthly value for each ZIP we
have hand-curated data for, parses state-level home_value / home_value_yoy
/ median_rent, and writes the merged result to ``data/zillow_overrides.json``.
The neighborhood modules apply the per-ZIP section at import time;
``data_providers`` applies the per-state section to CHOROPLETH_STATES so
cap-rate-driving numbers (home_value AND median_rent) stay current.

Usage:
    python scripts/refresh_zillow.py [--dry-run]

What gets refreshed (per ZIP):
  - ``median_home_value``    from ZHVI (Zillow Home Value Index, all homes)
  - ``median_rent_monthly``  from ZORI (Zillow Observed Rent Index, SFR+condo+MFR)

What gets refreshed (per state):
  - ``home_value`` + ``home_value_yoy``  from State ZHVI
  - ``median_rent``                       from State ZORI

Everything else (crime, walk score, restaurants, % bachelors, income, lat/lng,
tags) stays at the hand-curated snapshot — those move slowly and Zillow doesn't
publish them.

Run cadence: once a month is overkill since cap rates move slowly; quarterly
is plenty. Suitable for a GitHub Action that opens a PR with the JSON diff.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sys
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Zillow Research public CSV endpoints. Stable URLs maintained by Zillow.
# If these change, the script will fail with a clear download error.
ZHVI_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zhvi/"
    "Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
)
ZORI_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zori/"
    "Zip_zori_uc_sfrcondomfr_sm_month.csv"
)
# State-level ZHVI for the choropleth's "Median home value" + "Home
# value YoY" metrics. Same naming pattern as the ZIP-level ZHVI.
ZHVI_STATE_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zhvi/"
    "State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
)
# State-level ZORI for the median_rent input on the cash_on_cash
# composite. Without this, rent stays on the seed snapshot while
# home_value tracks live — half of cap-rate moves untracked.
ZORI_STATE_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zori/"
    "State_zori_uc_sfrcondomfr_sm_month.csv"
)

# Map full state names → 2-letter codes. Used by both ZHVI and ZORI
# state parsers since neither CSV exposes the abbreviation directly.
NAME_TO_CODE = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}

REPO_ROOT = Path(__file__).resolve().parent.parent
NEIGHBORHOOD_FILES = [
    REPO_ROOT / "dallas_neighborhoods.py",
    REPO_ROOT / "state_neighborhoods.py",
]
OVERRIDES_PATH = REPO_ROOT / "data" / "zillow_overrides.json"

# Format conventions matching the hand-curated dicts: home values rounded to
# the nearest $1K, monthly rent to the nearest $10. Keeps diffs scan-able and
# avoids silly precision (Zillow's underlying smoothing isn't accurate to $1).
HOME_VALUE_ROUND = 1_000
RENT_ROUND = 10


def fetch_csv(url: str, timeout: int = 60) -> str:
    """Download a CSV. Raises a clear error if the URL is broken or unreachable."""
    log.info("Fetching %s", url)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Zillow returned HTTP {e.code} for {url}. URL may have moved.")
    except urllib.error.URLError as e:
        raise SystemExit(f"Network error fetching {url}: {e.reason}")


def latest_value_per_zip(csv_text: str) -> dict[str, float]:
    """Parse a Zillow ZIP-level monthly CSV and return the most recent
    non-empty value per ZIP. Date columns look like 'YYYY-MM-DD'."""
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    try:
        region_idx = header.index("RegionName")
    except ValueError:
        raise SystemExit("Zillow CSV is missing the RegionName column — schema changed?")

    date_cols = sorted(
        ((i, h) for i, h in enumerate(header) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", h)),
        key=lambda x: x[1],
    )
    if not date_cols:
        raise SystemExit("Zillow CSV had no date columns — schema changed?")
    log.info("  → %d date columns; latest = %s", len(date_cols), date_cols[-1][1])

    out: dict[str, float] = {}
    for row in reader:
        zip_code = row[region_idx].zfill(5)
        # Walk newest → oldest; first non-empty cell wins.
        for i, _ in reversed(date_cols):
            if i < len(row) and row[i]:
                try:
                    out[zip_code] = float(row[i])
                except ValueError:
                    pass
                break
    log.info("  → parsed %d ZIPs with a latest value", len(out))
    return out


def parse_state_zhvi(csv_text: str) -> dict[str, dict]:
    """Parse the state-level Zillow ZHVI CSV. For each state's two-letter
    code (StateName column maps to a state), find:
      - latest non-empty monthly value  → home_value
      - same value 12 months prior      → 12mo prior reference
      - YoY % change between them       → home_value_yoy

    Returns: {state_code: {"home_value": int, "home_value_yoy": float}}
    """
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    try:
        # State CSVs use 'StateName' for the full name + 'RegionName' for
        # the same. The 2-letter abbreviation isn't always a column —
        # look up by full name via a state→code map.
        name_idx = header.index("RegionName")
    except ValueError:
        raise SystemExit("State ZHVI CSV missing RegionName column.")

    date_cols = sorted(
        ((i, h) for i, h in enumerate(header) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", h)),
        key=lambda x: x[1],
    )
    if not date_cols:
        raise SystemExit("State ZHVI CSV had no date columns — schema changed?")
    log.info("  → %d date columns; latest = %s", len(date_cols), date_cols[-1][1])

    out: dict[str, dict] = {}
    for row in reader:
        name = row[name_idx]
        code = NAME_TO_CODE.get(name)
        if not code:
            continue
        # Find the latest non-empty value (newest → oldest), and the
        # column 12 months earlier in the date-sorted list. We use the
        # latest column index, then walk back 12 months to find the
        # comparison value.
        latest_val = None
        latest_idx_pos = None  # index into date_cols
        for pos, (col_i, _) in enumerate(reversed(date_cols)):
            if col_i < len(row) and row[col_i]:
                try:
                    latest_val = float(row[col_i])
                    latest_idx_pos = len(date_cols) - 1 - pos
                    break
                except ValueError:
                    pass
        if latest_val is None or latest_idx_pos is None:
            continue
        prior_pos = latest_idx_pos - 12
        prior_val = None
        if prior_pos >= 0:
            col_i = date_cols[prior_pos][0]
            if col_i < len(row) and row[col_i]:
                try:
                    prior_val = float(row[col_i])
                except ValueError:
                    pass
        entry: dict = {"home_value": int(round(latest_val))}
        if prior_val and prior_val > 0:
            entry["home_value_yoy"] = round((latest_val - prior_val) / prior_val * 100, 1)
        out[code] = entry
    log.info("  → parsed %d states with home_value (and YoY where 12mo prior available)", len(out))
    return out


def parse_state_zori(csv_text: str) -> dict[str, dict]:
    """Parse the state-level ZORI CSV. For each state, take the latest
    non-empty monthly value and emit it as median_rent (rounded to the
    nearest $25). Returns {state_code: {"median_rent": int}}.

    No YoY here — rent_yoy isn't a metric the choropleth exposes today.
    Add a `median_rent_yoy` field if/when it does."""
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    try:
        name_idx = header.index("RegionName")
    except ValueError:
        raise SystemExit("State ZORI CSV missing RegionName column.")
    date_cols = sorted(
        ((i, h) for i, h in enumerate(header) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", h)),
        key=lambda x: x[1],
    )
    if not date_cols:
        raise SystemExit("State ZORI CSV had no date columns — schema changed?")
    log.info("  → %d date columns; latest = %s", len(date_cols), date_cols[-1][1])

    out: dict[str, dict] = {}
    for row in reader:
        name = row[name_idx]
        code = NAME_TO_CODE.get(name)
        if not code:
            continue
        for col_i, _ in reversed(date_cols):
            if col_i < len(row) and row[col_i]:
                try:
                    rent = float(row[col_i])
                    out[code] = {"median_rent": int(round(rent / 25) * 25)}
                    break
                except ValueError:
                    pass
    log.info("  → parsed %d states with median_rent", len(out))
    return out


def collect_target_zips() -> set[str]:
    """Find every 5-digit ZIP key in the neighborhood source files. The
    pattern is ``"75201":`` — keys are always 5-digit, quoted strings."""
    pattern = re.compile(r'"(\d{5})":\s*\{')
    zips: set[str] = set()
    for path in NEIGHBORHOOD_FILES:
        if not path.exists():
            log.warning("Skipping missing file: %s", path)
            continue
        text = path.read_text()
        zips.update(pattern.findall(text))
    log.info("Found %d unique ZIPs in neighborhood files", len(zips))
    return zips


def round_to(value: float, increment: int) -> int:
    """Round to the nearest `increment` (e.g. nearest $1K)."""
    return int(round(value / increment) * increment)


def build_overrides(target_zips: set[str], zhvi: dict, zori: dict, state_zhvi: dict) -> dict:
    overrides: dict[str, dict] = {}
    matched_value, matched_rent = 0, 0
    for z in sorted(target_zips):
        entry: dict[str, int] = {}
        if z in zhvi:
            entry["median_home_value"] = round_to(zhvi[z], HOME_VALUE_ROUND)
            matched_value += 1
        if z in zori:
            entry["median_rent_monthly"] = round_to(zori[z], RENT_ROUND)
            matched_rent += 1
        if entry:
            overrides[z] = entry

    log.info(
        "Coverage — home value: %d/%d  ·  rent: %d/%d",
        matched_value, len(target_zips), matched_rent, len(target_zips),
    )

    missing_value = sorted(z for z in target_zips if z not in zhvi)
    missing_rent = sorted(z for z in target_zips if z not in zori)
    if missing_value:
        log.info("ZIPs missing from ZHVI: %s", ", ".join(missing_value))
    if missing_rent:
        log.info("ZIPs missing from ZORI: %s", ", ".join(missing_rent))

    return {
        "_meta": {
            "as_of": date.today().isoformat(),
            "source": "Zillow Research (ZHVI all-homes; ZORI SFR+condo+MFR)",
            "zhvi_url": ZHVI_URL,
            "zori_url": ZORI_URL,
            "zhvi_state_url": ZHVI_STATE_URL,
            "zori_state_url": ZORI_STATE_URL,
            "zips_covered": len(overrides),
            "zips_targeted": len(target_zips),
            "states_covered": len(state_zhvi),
        },
        "overrides": overrides,
        "state_overrides": state_zhvi,
    }


def write_overrides(payload: dict, dry_run: bool) -> None:
    if dry_run:
        log.info("--dry-run: would write %d ZIP overrides to %s",
                 len(payload["overrides"]), OVERRIDES_PATH)
        log.info("Sample (first 5):")
        for z in list(payload["overrides"])[:5]:
            log.info("  %s → %s", z, payload["overrides"][z])
        return
    OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    OVERRIDES_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %d ZIP overrides to %s", len(payload["overrides"]), OVERRIDES_PATH)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + parse but don't write the JSON file.",
    )
    args = parser.parse_args(argv)

    target_zips = collect_target_zips()
    if not target_zips:
        log.error("No target ZIPs found — neighborhood files moved or empty?")
        return 1

    zhvi = latest_value_per_zip(fetch_csv(ZHVI_URL))
    zori = latest_value_per_zip(fetch_csv(ZORI_URL))
    state_zhvi = parse_state_zhvi(fetch_csv(ZHVI_STATE_URL))
    # Merge state-level ZORI rent into the same per-state dict so the
    # final state_overrides payload carries home_value, home_value_yoy,
    # and median_rent — the three inputs cash_on_cash needs from
    # market data. The data_providers loader already iterates
    # values.items() so any new key flows through to CHOROPLETH_STATES.
    state_zori = parse_state_zori(fetch_csv(ZORI_STATE_URL))
    for code, vals in state_zori.items():
        state_zhvi.setdefault(code, {}).update(vals)

    payload = build_overrides(target_zips, zhvi, zori, state_zhvi)
    write_overrides(payload, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
