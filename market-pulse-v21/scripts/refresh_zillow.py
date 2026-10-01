"""Refresh Zillow state-level ZHVI + ZORI for data_providers.

Downloads Zillow Research's state-level ZHVI and ZORI CSVs, parses
home_value / home_value_yoy / median_rent per state, and writes them to
``data/zillow_overrides.json`` under ``state_overrides``. ``data_providers``
applies that section to CHOROPLETH_STATES.

The per-ZIP section this file used to carry (ZHVI/ZORI for the ~480 ZIPs in
the hand-curated metro maps) went with those maps in map rebuild phase 5;
ZIP-level Zillow figures live in data/zip_profile.db
(scripts/build_zip_profile.py) and data/zips.db (build_national_zips.py).

Usage:
    python scripts/refresh_zillow.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Zillow Research public CSV endpoints. Stable URLs maintained by Zillow.
# If these change, the script will fail with a clear download error.
# State-level ZHVI for the choropleth's "Median home value" + "Home
# value YoY" metrics. Same naming pattern as the ZIP-level ZHVI.
ZHVI_STATE_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zhvi/"
    "State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
)
# State-level ZORI for median_rent. Without this, rent stays on the seed
# snapshot while home_value tracks live.
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
OVERRIDES_PATH = REPO_ROOT / "data" / "zillow_overrides.json"



def fetch_csv(url: str, timeout: int = 60, optional: bool = False) -> str | None:
    """Download a CSV.

    Retries 5xx/429 + network blips up to 3 times with linear backoff
    (the old code raised SystemExit on the first transient error, so
    a single Zillow S3 hiccup nuked the monthly workflow). 4xx still
    fails fast — that's a real URL or schema change, not flake.

    When ``optional=True``, a 4xx return logs a warning and returns
    ``None`` instead of exiting — used for feeds we can degrade past
    (e.g. state ZORI, which Zillow has renamed once before).
    """
    log.info("Fetching %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
    last_err: str | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if not (e.code >= 500 or e.code == 429):
                break
        except urllib.error.URLError as e:
            last_err = f"network error {e.reason}"
        if attempt < 2:
            time.sleep(3 * (attempt + 1))
    if optional:
        log.warning("Zillow %s: %s — skipping (optional feed).", url, last_err)
        return None
    raise SystemExit(f"Zillow {url}: {last_err} (after 3 attempts).")


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


def build_overrides(state_zhvi: dict) -> dict:
    return {
        "_meta": {
            "as_of": date.today().isoformat(),
            "source": "Zillow Research (state ZHVI all-homes; state ZORI SFR+condo+MFR)",
            "zhvi_state_url": ZHVI_STATE_URL,
            "zori_state_url": ZORI_STATE_URL,
            "states_covered": len(state_zhvi),
        },
        "state_overrides": state_zhvi,
    }


def write_overrides(payload: dict, dry_run: bool) -> None:
    n = len(payload["state_overrides"])
    if dry_run:
        log.info("--dry-run: would write %d state overrides to %s", n, OVERRIDES_PATH)
        for code in list(payload["state_overrides"])[:5]:
            log.info("  %s → %s", code, payload["state_overrides"][code])
        return
    OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    OVERRIDES_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %d state overrides to %s", n, OVERRIDES_PATH)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + parse but don't write the JSON file.",
    )
    args = parser.parse_args(argv)

    state_zhvi = parse_state_zhvi(fetch_csv(ZHVI_STATE_URL))
    if not state_zhvi:
        log.error("State ZHVI parsed to nothing — refusing to overwrite %s", OVERRIDES_PATH)
        return 1
    # Merge state-level ZORI rent into the same per-state dict so the
    # final state_overrides payload carries home_value, home_value_yoy,
    # and median_rent — the three inputs cash_on_cash needs from
    # market data. The data_providers loader already iterates
    # values.items() so any new key flows through to CHOROPLETH_STATES.
    state_zori_csv = fetch_csv(ZORI_STATE_URL, optional=True)
    if state_zori_csv is not None:
        state_zori = parse_state_zori(state_zori_csv)
        for code, vals in state_zori.items():
            state_zhvi.setdefault(code, {}).update(vals)
    else:
        # Carry over the prior median_rent values from the existing
        # overrides JSON so state rent doesn't silently disappear from
        # CHOROPLETH_STATES on the next deploy.
        carried = 0
        if OVERRIDES_PATH.exists():
            try:
                prev = json.loads(OVERRIDES_PATH.read_text())
                for code, vals in (prev.get("state_overrides") or {}).items():
                    if "median_rent" in vals:
                        state_zhvi.setdefault(code, {}).setdefault(
                            "median_rent", vals["median_rent"]
                        )
                        carried += 1
            except (OSError, ValueError) as e:
                log.warning("Could not read prior state_overrides: %s", e)
        log.warning("State ZORI unavailable — carried median_rent for %d states from prior JSON.", carried)

    payload = build_overrides(state_zhvi)
    write_overrides(payload, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
