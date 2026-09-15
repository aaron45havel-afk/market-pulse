"""Monthly snapshot of the strict screen, for the change log on /norcal.

Run by .github/workflows/refresh-screen-history.yml on the 2nd of each
month — after Zillow (1st 12:00 UTC) and the national-zips rebuild
(1st 14:00 UTC) have moved the prices the screen reads.

Snapshots use the DEFAULT parameters, so the change log answers "what
changed for the standard buyer", not for one person's tuned query.

IT REFUSES TO RE-DATE AN UNCHANGED SCREEN. This script fetches nothing —
it reads committed files and runs the screen over them — which makes it
fast enough to finish before the refreshes it depends on have pushed. Run
it then and it writes last month's buyable list under this month's period,
which is worse here than almost anywhere else in the repo: the change log's
entire job is to say what moved, so a snapshot duplicating its predecessor
publishes "no change" as though that were an observation about the market.
freshness.verdict() decides per market before anything is written; see
freshness.py for why "the inputs are older than the snapshot on disk" is
NOT the rule that catches this.

Usage:
    python scripts/snapshot_screens.py [--force]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import freshness as F
import norcal
import regions as RG
import screen_history as H

ROOT = Path(__file__).resolve().parent.parent

# The code that decides what a snapshot says: norcal.py is the screen,
# screen_history.py shapes the stored rows, and this script picks the
# parameters. freshness.py is deliberately NOT hashed — a comment fix there
# would rebuild every joining snapshot in the repo at once.
LOGIC_FILES = (ROOT / "norcal.py", ROOT / "screen_history.py",
               Path(__file__).resolve())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--force", action="store_true",
                    help="write snapshots even where nothing has moved")
    args = ap.parse_args()

    period = H.current_period()
    inputs = dict(H.snapshot_inputs(), logic=F.logic_stamp(LOGIC_FILES))

    # PER MARKET, because markets are added over time and a new one has no
    # history to be redundant against. The inputs are shared, so in the
    # ordinary case every market reaches the same verdict — but a market
    # whose first snapshot is this one must not be skipped because the
    # others have nothing new.
    wrote, skipped, faulted = [], [], []
    for market in RG.MARKETS:
        previous = H.previous_inputs(market)
        v = F.verdict(inputs, previous)

        if v["action"] == "fault" and not args.force:
            print(f"{market} {period}: FAULT — {v['reason']}")
            for line in F.describe(inputs, previous):
                print(f"  {line}")
            faulted.append(market)
            continue

        if v["action"] == "skip" and not args.force:
            # Not an error. A fallback run against inputs that have not
            # moved is a normal Tuesday; the only wrong outcome is writing.
            print(f"{market} {period}: SKIP — nothing has moved since "
                  f"{(previous or {}).get('_file')}")
            skipped.append(market)
            continue

        mkt = RG.MARKETS[market]
        res = norcal.screen(region=mkt["default_region"], market=market)
        snap = H.snapshot(res, market, period,
                          params={"assets": res["power"]["assets"],
                                  "down_pct": res["power"]["down_pct"],
                                  "safety_tier": res.get("safety_tier"),
                                  "winter": res.get("winter")},
                          inputs=inputs)
        print(f"{market} {period}: buyable={len(snap['buyable'])} "
              f"aspirational={len(snap['aspirational'])} "
              f"universe={snap['universe_n']}  [{v['reason']}]")
        wrote.append(market)
        d = H.diff(market)
        if d:
            print(f"  vs {d['older']}: +{len(d['entered'])} entered, "
                  f"-{len(d['left'])} left, {len(d['cheaper'])} cheaper, "
                  f"{len(d['pricier'])} pricier")

    # This run's input dates, on every outcome including success: they are
    # what name the refresh that failed to land, where the verdict only
    # says one did. Only the current side — each market's own prior is
    # named on its own line above, and they can differ.
    print()
    print(F.describe(inputs, None)[0])
    print(f"wrote {len(wrote)}, skipped {len(skipped)}, "
          f"faulted {len(faulted)} of {len(RG.MARKETS)} markets")
    if skipped and not wrote:
        print("Nothing written. Re-run with --force to write anyway.")

    # The RESEARCHED layers are a separate question from the price layers
    # above: they age on a scale of years, are reported to the reader as an
    # age rather than gating anything, and a stale one is a research task
    # rather than a broken run. Printed, not enforced — as before.
    for f in H.layer_freshness():
        if f["stale"]:
            print(f"  STALE LAYER: {f['layer']} (as of {f['as_of']}, "
                  f"{f['age_days']}d > {f.get('ttl_days')}d TTL)")

    # A fault means this checkout predates a refresh that already landed.
    return 1 if faulted else 0


if __name__ == "__main__":
    raise SystemExit(main())
