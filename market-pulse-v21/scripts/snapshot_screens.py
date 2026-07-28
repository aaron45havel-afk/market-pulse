"""Monthly snapshot of the strict screen, for the change log on /norcal.

Run by .github/workflows/refresh-screen-history.yml on the 2nd of each
month — after Zillow (1st 12:00 UTC) and the national-zips rebuild
(1st 14:00 UTC) have moved the prices the screen reads.

Snapshots use the DEFAULT parameters, so the change log answers "what
changed for the standard buyer", not for one person's tuned query.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import norcal
import regions as RG
import screen_history as H

def main() -> int:
    period = H.current_period()
    for market in RG.MARKETS:
        mkt = RG.MARKETS[market]
        res = norcal.screen(region=mkt["default_region"], market=market)
        snap = H.snapshot(res, market, period,
                          params={"assets": res["power"]["assets"],
                                  "down_pct": res["power"]["down_pct"],
                                  "safety_tier": res.get("safety_tier"),
                                  "winter": res.get("winter")})
        print(f"{market} {period}: buyable={len(snap['buyable'])} "
              f"aspirational={len(snap['aspirational'])} universe={snap['universe_n']}")
        d = H.diff(market)
        if d:
            print(f"  vs {d['older']}: +{len(d['entered'])} entered, "
                  f"-{len(d['left'])} left, {len(d['cheaper'])} cheaper, "
                  f"{len(d['pricier'])} pricier")
    for f in H.layer_freshness():
        if f["stale"]:
            print(f"  STALE LAYER: {f['layer']} (as of {f['as_of']}, "
                  f"{f['age_days']}d > {f.get('ttl_days')}d TTL)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
