"""Refresh the monthly Peter Lynch GARP snapshot.

Runs the Lynch screener (P/E < 10, EPS growth > 10%, mkt cap > $1B,
D/E < 0.5, CapEx/OCF < 0.5) against the SEC EDGAR universe + Stooq
prices and writes ``data/lynch_snapshots/YYYY-MM.json``.

Companion to refresh_screener.py (which snapshots net-nets). Same
retention strategy: keep 24 months on disk.

Usage:
    python scripts/refresh_lynch_screener.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

SNAPSHOT_DIR = REPO_ROOT / "data" / "lynch_snapshots"
MAX_SNAPSHOTS = 24


def prune_old_snapshots(directory: Path, keep: int) -> int:
    files = sorted(directory.glob("*.json"))
    removed = 0
    for old in files[:-keep] if len(files) > keep else []:
        log.info("Pruning old snapshot: %s", old.name)
        old.unlink()
        removed += 1
    return removed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="Build the snapshot and log it, but don't write to disk.")
    parser.add_argument("--max-companyfacts", type=int, default=None,
                        help="Cap on how many companies to deep-pull. For testing.")
    args = parser.parse_args(argv)

    from lynch_screener import LYNCH_RULES, build_lynch_screener

    log.info("Running Peter Lynch GARP screener …")
    rows = build_lynch_screener(max_companyfacts=args.max_companyfacts)
    log.info("  %d companies pass all Lynch filters", len(rows))

    today = date.today()
    snapshot_key = today.strftime("%Y-%m")
    payload = {
        "_meta": {
            "as_of": today.isoformat(),
            "snapshot_month": snapshot_key,
            "passing_count": len(rows),
            "source": "SEC EDGAR XBRL company facts + Stooq daily close",
            "rules": LYNCH_RULES,
        },
        "companies": rows,
    }

    if args.dry_run:
        log.info("--dry-run: would write %s with %d rows",
                 SNAPSHOT_DIR / f"{snapshot_key}.json", len(rows))
        for r in rows[:10]:
            log.info("  %s · PE=%s · 3yr=%s%% · D/E=%s · CapEx/OCF=%s",
                     r["ticker"], r["pe_ratio"], r["eps_3yr_cagr_pct"],
                     r["debt_to_equity"], r["capex_to_ocf"])
        return 0

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    out = SNAPSHOT_DIR / f"{snapshot_key}.json"
    out.write_text(json.dumps(payload, indent=2) + "\n")
    log.info("Wrote %s", out)

    pruned = prune_old_snapshots(SNAPSHOT_DIR, MAX_SNAPSHOTS)
    if pruned:
        log.info("Pruned %d snapshot(s) beyond %d-month cap.", pruned, MAX_SNAPSHOTS)

    return 0


if __name__ == "__main__":
    sys.exit(main())
