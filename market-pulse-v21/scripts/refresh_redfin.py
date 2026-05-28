"""Refresh Redfin Home Sales + Days on Market data for the state choropleth.

Downloads the public Redfin state-level market tracker TSV, picks the most
recent month's `Homes Sold` count and `Median Days on Market` per state, and
writes the result to ``data/redfin_overrides.json``. ``data_providers`` patches
the choropleth state dict at module import so the site shows current data
without any code edits.

Usage:
    python scripts/refresh_redfin.py [--dry-run]

What gets refreshed (per state):
  - ``homes_sold``         most recent monthly count (All Residential)
  - ``dom``                most recent monthly median Days on Market
  - ``sale_to_list_pct``   avg sale-to-list ratio as %. >100 = bidding
                           above asking (seller's market); <100 = below
                           asking (buyer's market). The single best
                           "negotiating power" proxy in Redfin's feed.
  - ``price_drops_pct``    share of active listings with price drops,
                           as %. Spike = sellers capitulating; classic
                           leading indicator of a cooling market.
  - ``period_end``         the date these values are for (in _meta only)

Run cadence: monthly is appropriate. Suitable for a GitHub Action that
opens a PR (or auto-commits) with the JSON diff. The Redfin TSV is ~9MB
gzipped so this is well under any reasonable network/runtime budget.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import sys
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Redfin Data Center public S3 bucket. Stable URLs maintained by Redfin
# Research. These mirror what the data center webpage links to.
REDFIN_STATE_URL = (
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/"
    "redfin_market_tracker/state_market_tracker.tsv000.gz"
)

REPO_ROOT = Path(__file__).resolve().parent.parent
OVERRIDES_PATH = REPO_ROOT / "data" / "redfin_overrides.json"

# We pull "All Residential" — the aggregate across SF homes, condos,
# townhomes, and small multifamily. That matches what reventure /
# Zillow report at the headline level.
TARGET_PROPERTY_TYPE = "All Residential"


def fetch_gz_tsv(url: str, timeout: int = 90) -> str:
    """Download a gzipped TSV and return decoded text.

    Retries 5xx and network errors up to 3 times with linear backoff
    (the old code raised SystemExit on the first transient blip and
    killed the weekly workflow). 4xx still fails fast — that's a real
    URL or schema change, not flake."""
    log.info("Fetching %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1"})
    last_err: str | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
            return gzip.decompress(raw).decode("utf-8")
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if not (e.code >= 500 or e.code == 429):
                break
        except urllib.error.URLError as e:
            last_err = f"network error {e.reason}"
        if attempt < 2:
            time.sleep(3 * (attempt + 1))
    raise SystemExit(f"Redfin {url}: {last_err} (after 3 attempts).")


def parse_latest_per_state(tsv_text: str) -> dict[str, dict]:
    """Walk the TSV, keep the most recent row per state for the target
    property type, and return {state_code: {homes_sold, dom, period_end}}.

    The Redfin TSV is sorted unpredictably — same state appears across many
    months — so we track the latest period_end seen for each state and
    overwrite as newer rows come in.
    """
    reader = csv.reader(io.StringIO(tsv_text), delimiter="\t", quotechar='"')
    header = next(reader)
    # Some Redfin columns are quoted with embedded quotes that csv.reader
    # leaves intact — strip surrounding whitespace + quotes defensively.
    header = [h.strip().strip('"') for h in header]

    try:
        idx_period_end   = header.index("PERIOD_END")
        idx_region_type  = header.index("REGION_TYPE")
        idx_state_code   = header.index("STATE_CODE")
        idx_property     = header.index("PROPERTY_TYPE")
        idx_homes_sold   = header.index("HOMES_SOLD")
        idx_median_dom   = header.index("MEDIAN_DOM")
        idx_sale_to_list = header.index("AVG_SALE_TO_LIST")
        idx_price_drops  = header.index("PRICE_DROPS")
        idx_months_supply = header.index("MONTHS_OF_SUPPLY")
    except ValueError as e:
        raise SystemExit(f"Redfin TSV missing expected column: {e}")

    # Single max-index for the bounds check below — keeps the row-length
    # guard right whichever new column we add later.
    max_idx = max(idx_homes_sold, idx_median_dom, idx_sale_to_list,
                  idx_price_drops, idx_months_supply)

    latest: dict[str, dict] = {}
    rows_seen, rows_kept = 0, 0
    for row in reader:
        rows_seen += 1
        if len(row) <= max_idx:
            continue
        if (row[idx_region_type].strip('"') != "state" or
                row[idx_property].strip('"') != TARGET_PROPERTY_TYPE):
            continue
        state = row[idx_state_code].strip('"').upper()
        period_end = row[idx_period_end].strip('"')
        homes_sold_raw = row[idx_homes_sold]
        dom_raw = row[idx_median_dom]
        sale_to_list_raw = row[idx_sale_to_list]
        price_drops_raw = row[idx_price_drops]
        if not state or not period_end:
            continue
        # Skip rows with missing core values — Redfin uses '' for nulls.
        if homes_sold_raw in ("", "NA") or dom_raw in ("", "NA"):
            continue
        try:
            homes_sold = int(round(float(homes_sold_raw)))
            dom = int(round(float(dom_raw)))
        except ValueError:
            continue
        # AVG_SALE_TO_LIST + PRICE_DROPS are decimals (0.97 = 97%, 0.05 = 5%).
        # Optional — newly-added columns; tolerate missing values per state
        # rather than dropping the whole row so homes_sold / dom still land.
        def _to_pct(raw: str) -> float | None:
            if raw in ("", "NA"):
                return None
            try:
                return round(float(raw) * 100, 1)
            except ValueError:
                return None
        sale_to_list = _to_pct(sale_to_list_raw)
        price_drops = _to_pct(price_drops_raw)
        # months_of_supply is already a months value, not a fraction —
        # 1.5 means 1.5 months. Pass through as float.
        months_supply_raw = row[idx_months_supply]
        try:
            months_supply = round(float(months_supply_raw), 1) if months_supply_raw not in ("", "NA") else None
        except ValueError:
            months_supply = None
        existing = latest.get(state)
        if existing is None or period_end > existing["period_end"]:
            latest[state] = {
                "homes_sold": homes_sold,
                "dom": dom,
                "sale_to_list_pct": sale_to_list,
                "price_drops_pct": price_drops,
                "months_of_supply": months_supply,
                "period_end": period_end,
            }
            rows_kept += 1

    log.info("  → scanned %d rows, kept latest for %d states", rows_seen, len(latest))
    return latest


def build_overrides(per_state: dict[str, dict]) -> dict:
    if not per_state:
        raise SystemExit("No state-level rows passed the filter — Redfin schema may have shifted.")
    # Find the most common period_end across states. Some less-active
    # states lag a month behind the rest; we surface the modal date as
    # the headline "as of" so users see the freshest representative date.
    counts: dict[str, int] = {}
    for v in per_state.values():
        counts[v["period_end"]] = counts.get(v["period_end"], 0) + 1
    primary_period = max(counts, key=counts.get)
    log.info("  → primary period_end = %s (%d/%d states)",
             primary_period, counts[primary_period], len(per_state))

    overrides: dict[str, dict] = {}
    for state, v in sorted(per_state.items()):
        # None values get dropped so JSON doesn't carry nulls — the
        # data_providers loader only patches keys that are present.
        entry = {"homes_sold": v["homes_sold"], "dom": v["dom"]}
        for opt in ("sale_to_list_pct", "price_drops_pct", "months_of_supply"):
            val = v.get(opt)
            if val is not None:
                entry[opt] = val
        overrides[state] = entry
    return {
        "_meta": {
            "as_of": date.today().isoformat(),
            "source": "Redfin Data Center (state_market_tracker, All Residential)",
            "url": REDFIN_STATE_URL,
            "primary_period_end": primary_period,
            "states_covered": len(overrides),
        },
        "overrides": overrides,
    }


def write_overrides(payload: dict, dry_run: bool) -> None:
    if dry_run:
        log.info("--dry-run: would write %d state overrides to %s",
                 len(payload["overrides"]), OVERRIDES_PATH)
        log.info("Sample (first 5):")
        for s in list(payload["overrides"])[:5]:
            log.info("  %s → %s", s, payload["overrides"][s])
        return
    OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    OVERRIDES_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %d state overrides to %s",
             len(payload["overrides"]), OVERRIDES_PATH)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + parse but don't write the JSON file.",
    )
    args = parser.parse_args(argv)

    tsv = fetch_gz_tsv(REDFIN_STATE_URL)
    per_state = parse_latest_per_state(tsv)
    payload = build_overrides(per_state)
    write_overrides(payload, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
