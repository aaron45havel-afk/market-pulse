"""Refresh the monthly 100-bagger checklist snapshot.

Writes data/hundred_snapshots/YYYY-MM.json.

THE CHURN LINE IS THE POINT OF THIS SCRIPT, not an extra. The sister Lynch
board passed 10 names in June, 11 in July and 4 in August, and only
lululemon survived July into August — most of that turnover being a rules
change rather than a market. A page whose thesis is a fifteen-year hold
has to publish how long it has actually held anything, and it has to
publish it from the first run rather than after someone notices.

Usage:
    python scripts/refresh_hundred.py [--dry-run] [--force]
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

SNAPSHOT_DIR = REPO_ROOT / "data" / "hundred_snapshots"
MAX_SNAPSHOTS = 24

# A board that loses this share of its listed names against the previous
# published snapshot stops rather than publishing. Same guard as the Lynch
# refresh, and compared against the most recent file INCLUDING the current
# month — the file being overwritten is the one on the page.
LISTED_DROP_MAX = 0.60


def previous_snapshot(directory: Path) -> tuple[dict | None, str]:
    files = sorted(directory.glob("*.json"))
    if not files:
        return None, ""
    try:
        with open(files[-1], encoding="utf-8") as fh:
            return json.load(fh), files[-1].name
    except (OSError, ValueError):
        return None, files[-1].name


def write_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    os.replace(tmp, path)


def prune(directory: Path, keep: int) -> int:
    files = sorted(directory.glob("*.json"))
    removed = 0
    for old in files[:-keep] if len(files) > keep else []:
        log.info("Pruning %s", old.name)
        old.unlink()
        removed += 1
    return removed


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="Publish even if the listed count collapses.")
    ap.add_argument("--max-companyfacts", type=int, default=None)
    args = ap.parse_args(argv)

    import checklist as C
    from hundred_screener import RULES, build

    log.info("Running the 100-bagger checklist screen …")
    built = build(max_companyfacts=args.max_companyfacts)
    rows, census = built["rows"], built["census"]
    listed = [r for r in rows if r.get("verdict") == "list"]

    log.info("")
    log.info("  screened   %6d", census["screened"])
    log.info("  listed     %6d", census["listed"])
    log.info("  thin       %6d", census["thin"])
    log.info("  rejected   %6d", census["rejected"])
    log.info("  coverage by criterion (of %d screened):", census["screened"])
    for cid, n in sorted(census["coverage"].items()):
        log.info("    %-2s %-24s %6d  %5.1f%%", cid, C.CRITERION_NAMES[cid], n,
                 n / census["screened"] * 100 if census["screened"] else 0.0)
    log.info("  never scoreable: %s",
             ", ".join(str(c) for c in C.NEVER_SCORED_IDS))

    prev, prev_file = previous_snapshot(SNAPSHOT_DIR)
    prev_rows = [r for r in (prev or {}).get("companies", [])
                 if r.get("verdict") == "list"] if prev else None
    ch = C.churn(listed, prev_rows)

    log.info("")
    if ch["comparable"]:
        log.info("  CHURN against %s: %d held, %d entered, %d left",
                 prev_file, ch["held_n"], ch["entered_n"], ch["left_n"])
        if ch["held"]:
            log.info("    held: %s", ", ".join(ch["held"][:20]))
        if ch["left"]:
            log.info("    left: %s", ", ".join(ch["left"][:20]))
    else:
        log.info("  CHURN: first run, nothing to compare against")

    log.info("")
    log.info("  listed names:")
    for r in listed[:30]:
        led = r.get("ledger") or {}
        log.info("    %-6s %-26s %s", r.get("ticker"),
                 (r.get("name") or "")[:26], led.get("headline"))
    if len(listed) > 30:
        log.info("    … and %d more", len(listed) - 30)

    today = date.today()
    key = today.strftime("%Y-%m")

    if prev_rows is not None and prev_rows:
        drop = 1 - (len(listed) / len(prev_rows))
        if drop > LISTED_DROP_MAX and not args.force:
            log.error("")
            log.error("STOPPING: listed count fell from %d (%s) to %d (%.0f%%).",
                      len(prev_rows), prev_file, len(listed), drop * 100)
            log.error("Re-run with force checked if the drop is expected.")
            return 1

    payload = {
        "_meta": {
            "as_of": today.isoformat(), "snapshot_month": key,
            "listed_count": len(listed), "screened_count": census["screened"],
            "source": f"SEC EDGAR XBRL companyfacts + {built['quote_source']} bulk quotes",
            "rules": RULES, "census": census, "churn": ch,
            # PRINTED, NOT FIXED. Nothing in this repo can measure the exit
            # rate — a delisted company has no ticker and is not in
            # company_tickers.json at all — so every backward window here
            # is conditioned on the company still being listed today. The
            # only honest response is to say so on the page.
            "survivorship": (
                "The universe is current SEC registrants only. A delisted "
                "company has no ticker and does not appear in the source "
                "file at all, so every multi-year figure on this page is "
                "conditioned on the company having survived to "
                f"{today.isoformat()}. The exit rate is not merely unmeasured "
                "here, it is unmeasurable from free bulk sources — which is "
                "the same survivorship bias both source books are built on."),
        },
        "companies": rows,
    }

    if args.dry_run:
        log.info("--dry-run: would write %s (%d rows, %d listed)",
                 SNAPSHOT_DIR / f"{key}.json", len(rows), len(listed))
        return 0

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    write_atomic(SNAPSHOT_DIR / f"{key}.json", payload)
    log.info("Wrote %s", SNAPSHOT_DIR / f"{key}.json")
    n = prune(SNAPSHOT_DIR, MAX_SNAPSHOTS)
    if n:
        log.info("Pruned %d beyond the %d-month cap.", n, MAX_SNAPSHOTS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
