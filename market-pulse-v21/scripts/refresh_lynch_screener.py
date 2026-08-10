"""Refresh the monthly Peter Lynch GARP snapshot.

Writes data/lynch_snapshots/YYYY-MM.json — every screened company with a
verdict and a reason code, plus the census that makes the board
falsifiable.

TWO SAFETY RAILS THE OLD VERSION DID NOT HAVE, both of which the repo
already implements elsewhere and neither of which was wired here:

  * ATOMIC WRITE. The snapshot path is keyed by write month, and the old
    code wrote to it unconditionally. A bad partial run permanently
    destroyed that month's good snapshot. Write to a temp file in the
    same directory, then rename — rename is atomic on POSIX, so the
    published file is either the old one or a complete new one.

  * A DELTA GUARD. A run that loses most of the board is far more likely
    to be a broken parser than a repriced market.

    ANTICIPATE THIS: the guard WILL fire on the first run after the parser
    fixes land, because correcting the units, period and debt bugs is
    expected to shrink the board sharply. That is why it exits non-zero
    with an explicit override rather than skipping silently — a guard that
    quietly does nothing is not a guard.

Usage:
    python scripts/refresh_lynch_screener.py [--dry-run] [--force]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
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

# A board that loses more than this share of its passing names against the
# previous snapshot stops the run rather than publishing.
PASSING_DROP_MAX = 0.60


def prune_old_snapshots(directory: Path, keep: int) -> int:
    files = sorted(directory.glob("*.json"))
    removed = 0
    for old in files[:-keep] if len(files) > keep else []:
        log.info("Pruning old snapshot: %s", old.name)
        old.unlink()
        removed += 1
    return removed


def previous_passing(directory: Path, exclude: str) -> int | None:
    files = [p for p in sorted(directory.glob("*.json")) if p.stem != exclude]
    if not files:
        return None
    try:
        with open(files[-1], encoding="utf-8") as fh:
            d = json.load(fh)
    except (OSError, ValueError):
        return None
    meta = d.get("_meta") or {}
    if isinstance(meta.get("passing_count"), int):
        return meta["passing_count"]
    return None


def write_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    os.replace(tmp, path)          # atomic on POSIX


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--dry-run", action="store_true",
                    help="Build and log the snapshot but do not write it.")
    ap.add_argument("--max-companyfacts", type=int, default=None,
                    help="Cap how many companies to deep-pull. For testing.")
    ap.add_argument("--force", action="store_true",
                    help="Publish even if the passing count collapses. Use "
                         "on the run where parser fixes are expected to "
                         "shrink the board.")
    args = ap.parse_args(argv)

    from lynch_screener import LYNCH_RULES, build_lynch_screener

    log.info("Running the Peter Lynch GARP screener …")
    built = build_lynch_screener(max_companyfacts=args.max_companyfacts)
    rows, census = built["rows"], built["census"]
    passing = census["passing"]

    log.info("")
    log.info("  screened     %6d", census["screened"])
    log.info("  passing      %6d", passing)
    log.info("  unmeasurable %6d", census["unmeasured"])
    log.info("  rejections by cause:")
    for code, n in list(census["by_reason"].items())[:14]:
        if code == "pass":
            continue
        log.info("    %-18s %5d   %s", code, n,
                 census["labels"].get(code, ""))

    today = date.today()
    key = today.strftime("%Y-%m")

    prev = previous_passing(SNAPSHOT_DIR, key)
    if prev and passing < prev * (1 - PASSING_DROP_MAX) and not args.force:
        log.error("")
        log.error("STOPPING: passing count fell from %d to %d (%.0f%%).",
                  prev, passing, (1 - passing / prev) * 100)
        log.error("That is more likely a broken parser than a repriced "
                  "market. The previous snapshot is untouched.")
        log.error("Re-run with --force if the drop is expected — it is, on "
                  "the first run after a parser fix.")
        return 1

    payload = {
        "_meta": {
            "as_of": today.isoformat(),
            "snapshot_month": key,
            "passing_count": passing,
            "screened_count": census["screened"],
            "census": census,
            # The old snapshot recorded "Stooq daily close", a source
            # replaced long before — provenance that would misdirect the
            # next investigation.
            "source": f"SEC EDGAR XBRL companyfacts + {built['quote_source']} bulk quotes",
            "quote_source": built["quote_source"],
            "rules": LYNCH_RULES,
        },
        # Passing rows carry the full record; rejections carry enough to
        # render the funnel without a 6MB file.
        "companies": [r for r in rows if r["verdict"] == "pass"],
        "rejected": [
            {"ticker": r.get("ticker"), "name": r.get("name"),
             "reason": r.get("reason"), "market_cap": r.get("market_cap"),
             "size_band": r.get("size_band")}
            for r in rows if r["verdict"] != "pass"
        ],
    }

    if args.dry_run:
        log.info("--dry-run: would write %s (%d passing of %d)",
                 SNAPSHOT_DIR / f"{key}.json", passing, census["screened"])
        for r in payload["companies"][:10]:
            log.info("  %-6s PEG %-5s P/E %-6s growth %-6s D/E %-5s capex/OCF %s",
                     r["ticker"], r.get("peg"), r.get("pe_ratio"),
                     r.get("eps_3yr_cagr_pct"), r.get("debt_to_equity"),
                     r.get("capex_to_ocf"))
        return 0

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    write_atomic(SNAPSHOT_DIR / f"{key}.json", payload)
    log.info("Wrote %s", SNAPSHOT_DIR / f"{key}.json")

    pruned = prune_old_snapshots(SNAPSHOT_DIR, MAX_SNAPSHOTS)
    if pruned:
        log.info("Pruned %d snapshot(s) beyond the %d-month cap.",
                 pruned, MAX_SNAPSHOTS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
