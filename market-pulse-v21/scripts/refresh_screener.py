"""Refresh the monthly net-net screener snapshot.

Runs the SEC EDGAR net-net screener and persists the *net-net pass*
subset of the result to ``data/screener_snapshots/YYYY-MM.json``.
The cron triggers on the 1st of each month (see
``.github/workflows/refresh-screener.yml``); the /finance UI
loads the latest snapshot by default and lets you browse history
via a month dropdown.

We only snapshot the net-nets (rows where ``is_net_net`` is True)
because that's the entire point of the screen — the wider candidate
set is generated each run from live SEC data anyway.

Retention: oldest snapshots are pruned to keep at most 24 months
on disk, since the per-snapshot file is small but unbounded growth
isn't useful — value-investing decisions look back ~2 years max.

Usage:
    python scripts/refresh_screener.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

# Make sec_edgar importable when the script runs from the repo root
# (GH Actions sets working-directory: market-pulse-v21).
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

SNAPSHOT_DIR = REPO_ROOT / "data" / "screener_snapshots"
MAX_SNAPSHOTS = 24


def prune_old_snapshots(directory: Path, keep: int) -> int:
    """Keep only the newest ``keep`` snapshots (by filename, which is
    YYYY-MM.json so lexicographic = chronological). Returns count
    removed. The GH Action commits the deletions alongside the new
    file."""
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
                        help="Run the screener and print what would be written, "
                             "but don't write the file or prune.")
    args = parser.parse_args(argv)

    # Imported lazily so the script still parses if the package isn't
    # installed (e.g. during local lint).
    from sec_edgar import SCREENER_RULES, build_net_net_screener

    log.info("Running net-net screener …")
    data = build_net_net_screener()
    if not isinstance(data, list):
        log.error("Screener returned non-list (%s). Aborting.", type(data).__name__)
        return 1

    net_nets = [d for d in data if isinstance(d, dict) and d.get("is_net_net")]
    log.info("  %d net-nets out of %d total candidates", len(net_nets), len(data))

    today = date.today()
    snapshot_key = today.strftime("%Y-%m")
    payload = {
        "_meta": {
            "as_of": today.isoformat(),
            "snapshot_month": snapshot_key,
            "net_nets_count": len(net_nets),
            "total_candidates": len(data),
            "source": "SEC EDGAR XBRL company facts",
            # Freeze the rules used at snapshot time so historical
            # views can show 'what counted as net-net then' if the
            # SCREENER_RULES dict changes later.
            "rules": SCREENER_RULES,
        },
        "net_nets": net_nets,
    }

    if args.dry_run:
        log.info("--dry-run: would write %s with %d rows",
                 SNAPSHOT_DIR / f"{snapshot_key}.json", len(net_nets))
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
