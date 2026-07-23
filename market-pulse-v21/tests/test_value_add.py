#!/usr/bin/env python3
"""Engine tests for the Value-Add MF SFR remodel budgeter (value_add.py).

Pure-function coverage of the California remodel cost model — no database,
no network. The cost table was research-built then hardened by an adversarial
review against a real change-of-occupancy conversion (516 Ward St, Martinez),
so these tests pin the calibrated ranges: a future edit that quietly lowballs
a rehab budget fails loudly here.

Run:  python tests/test_value_add.py      (exit 0 = all pass)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import value_add as V  # noqa: E402

_FAILS = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def between(x, lo, hi, msg):
    check(lo <= x <= hi, f"{msg}: got {x:,}, want {lo:,}–{hi:,}")


# ── 516 Ward St reference case: 1,703 sqft, 2bd/1ba, 1922, gut + conversion ──
ward = V.remodel_budget(1703, beds=2, baths=1, year_built=1922, scope="gut",
                        level="mid", conversion=True)
between(ward["total"], 600_000, 720_000, "516 Ward gut+conversion MID total")
between(ward["psf"], 350, 420, "516 Ward $/sqft (mid)")
check(ward["pre1978"] is True, "1922 auto-flags pre-1978 abatement")
check(ward["band"]["low"] < ward["total"] < ward["band"]["high"],
      "mid total sits inside the low–high band")
# conversion-only lines must be present
keys = {r["key"] for r in ward["hard_lines"]} | {r["key"] for r in ward["soft_lines"]}
for k in ("fire_sprinklers", "dwv_underslab", "storefront_infill", "windows_egress",
          "change_of_use", "sewer_capacity", "phase1_esa"):
    check(k in keys, f"conversion scope includes {k}")
# retrofit vs egress substitution: plain retrofit windows must be swapped out
check("windows" not in keys, "conversion swaps vinyl windows for egress openings")
# a conversion flag/warning surfaces
check(any("conversion" in f.lower() for f in ward["flags"]), "conversion warning present")

# ── URM masonry + full-foundation downside toggles push the number up ──
urm = V.remodel_budget(1703, 2, 1, 1922, "gut", "mid", conversion=True, masonry=True)
check(urm["total"] > ward["total"] + 100_000, "URM masonry retrofit materially raises cost")
check("urm_masonry_retrofit" in {r["key"] for r in urm["hard_lines"]}, "URM line present")
check("seismic_retrofit" not in {r["key"] for r in urm["hard_lines"]}, "URM replaces wood-frame seismic")
fnd = V.remodel_budget(1703, 2, 1, 1922, "gut", "mid", conversion=True, foundation_replace=True)
check(fnd["total"] > ward["total"], "full foundation replacement raises cost")
check("foundation_replacement" in {r["key"] for r in fnd["hard_lines"]}, "full-foundation line present")
check("foundation_repair" not in {r["key"] for r in fnd["hard_lines"]}, "full replaces partial foundation")

# ── conversion costs more than the same house gutted as a straight remodel ──
plain = V.remodel_budget(1703, 2, 1, 1922, "gut", "mid", conversion=False)
check(ward["total"] > plain["total"], "conversion scope costs more than a plain gut")
# non-conversion carries a permit % add-on; conversion carries change-of-use fees instead
check(any(a["key"] == "permit" for a in plain["addons"]), "plain gut has a permit add-on")
check(not any(a["key"] == "permit" for a in ward["addons"]), "conversion drops the permit % (uses change-of-use line)")

# ── scope tiers strictly increase in $/sqft ──
cos = V.remodel_budget(1500, 3, 2, 1960, "cosmetic", "mid")
mod = V.remodel_budget(1500, 3, 2, 1960, "moderate", "mid")
gut = V.remodel_budget(1500, 3, 2, 1960, "gut", "mid")
check(cos["total"] < mod["total"] < gut["total"], "cosmetic < moderate < gut")
between(cos["psf"], 35, 75, "cosmetic $/sqft near the ~$45 benchmark")
between(gut["psf"], 280, 420, "standard gut $/sqft in the Bay Area band")

# ── finish level low < mid < high for a fixed config ──
lo = V.remodel_budget(1500, 3, 2, 1960, "gut", "low")["total"]
mi = V.remodel_budget(1500, 3, 2, 1960, "gut", "mid")["total"]
hi = V.remodel_budget(1500, 3, 2, 1960, "gut", "high")["total"]
check(lo < mi < hi, "low < mid < high finish totals")

# ── invariants ──
check(all(r["amount"] > 0 for r in gut["hard_lines"]), "every hard line is a positive cost")
check(gut["hard"] < gut["total"], "soft costs add on top of hard cost")
check(V.remodel_budget(1000, 3, 1, 2010)["pre1978"] is False, "post-1978 skips abatement")
check(V.remodel_budget(1703, 2, 1, 1922, "gut", "mid")["windows"] == V._est_windows(1703),
      "window count auto-estimates from sqft")

# ── state regionalization (validated: Bay = 2.29× national remodel market) ──
check(V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="CA-BAY")["mult"] == 1.0,
      "CA-BAY is the 1.0 baseline (existing numbers unchanged)")
_in = V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="IN")
_tx = V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="TX")
_nyc = V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="NY-NYC")
between(_in["psf"], 115, 155, "Indiana gut $/sqft near the published ~$135")
between(_tx["psf"], 120, 160, "Texas gut $/sqft near the published ~$140")
between(_nyc["psf"], 175, 225, "NYC gut $/sqft near the published ~$200")
check(_in["total"] < _tx["total"] < _nyc["total"] < gut["total"],
      "IN < TX < NYC < Bay Area for the same house")
check(V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="NY")["total"] < _nyc["total"],
      "upstate NY cheaper than NYC metro")
check(V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="XX")["state"] == "CA-BAY",
      "unknown state code falls back to the default")
check(len(V.STATE_COST_FACTORS) == 53, "50 states + DC + CA-Bay + NYC breakouts present")
check(all(c in V.STATE_NAMES for c in V.STATE_COST_FACTORS), "every factor has a display name")

# ── buy / no-buy verdict ──
green = V.flip_verdict(100_000, 50_000, 300_000)
check(green["cls"] == "buy" and green["margin"] >= 20, "wide margin → green SHOULD BUY")
fair = V.flip_verdict(300_000, 60_000, 420_000)
check(fair["cls"] == "fair" and 8 <= fair["margin"] < 20, "thin margin → amber FAIR")
red = V.flip_verdict(450_000, 200_000, 500_000)
check(red["cls"] == "no" and red["margin"] < 8, "all-in over ARV → red TOO MUCH")
check(red["equity"] < 0, "red case shows negative equity")
# max offer lands exactly on the 20% green line
at_max = V.flip_verdict(green["max_offer"], 50_000, 300_000)
check(at_max is not None and abs(at_max["margin"] - 20.0) < 0.2, "max_offer hits the 20% line")
check(V.flip_verdict(0, 50_000, 300_000) is None, "no price → no verdict")
check(V.flip_verdict(100_000, 50_000, None) is None, "no ARV → no verdict")

# ── any-state ZIP lookup (ARV source) — guarded on the bundled zips.db ──
if V._ZIPS_DB.exists():
    fw = V.zip_market("46802")
    check(fw is not None and fw["state"] == "IN" and (fw["median_home_value"] or 0) > 0,
          "Fort Wayne 46802 resolves with a median value (national coverage)")
    check(V.zip_market("00000") is None, "unknown ZIP → None")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} value_add remodel-budget checks passed.")
print(f"   516 Ward St (gut+conversion): ${ward['total']:,} mid  ·  ${ward['psf']}/sqft  "
      f"·  band ${ward['band']['low']:,}–${ward['band']['high']:,}")
sys.exit(0)
