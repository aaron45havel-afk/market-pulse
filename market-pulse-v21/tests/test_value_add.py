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
check(len(V.STATE_COST_FACTORS) == 107, "50 states + DC + 56 metro breakouts present")
check(all(c in V.STATE_NAMES for c in V.STATE_COST_FACTORS), "every factor has a display name")
check(set(V.STATE_NAMES) == set(V.STATE_COST_FACTORS), "names and factors cover the same codes")
check(all(0.8 <= f <= 2.3 for f in V.STATE_COST_FACTORS.values()), "every factor in a sane 0.8-2.3 band")
check(all(c in V.STATE_COST_FACTORS for c in V.METRO_GEO), "every geo anchor has a cost factor")
# boom metros sit above their state; spot values from the research
check(V.STATE_COST_FACTORS["TN-BNA"] > V.STATE_COST_FACTORS["TN"] > V.STATE_COST_FACTORS["TN-MEM"],
      "Nashville above TN statewide, Memphis below")
check(V.STATE_COST_FACTORS["NC-RDU"] > V.STATE_COST_FACTORS["NC-CLT"] > V.STATE_COST_FACTORS["NC"],
      "Raleigh > Charlotte > rest-NC")
check(V.STATE_COST_FACTORS["CA-SD"] < V.STATE_COST_FACTORS["CA-BAY"], "San Diego below Bay Area")

# ── address → closest-metro locator ──
if V._ZIPS_DB.exists():
    loc_ward = V.locate_market("516 Ward St, Martinez, CA 94553")
    check(loc_ward is not None and loc_ward["code"] == "CA-BAY" and loc_ward["metro_matched"],
          "Martinez address resolves to the Bay Area metro")
    check(V.locate_market("98101")["code"] == "WA-SEA", "bare Seattle ZIP resolves to Seattle metro")
    check(V.locate_market("1310 W Jefferson Blvd, Fort Wayne, IN 46802")["code"] == "IN",
          "Fort Wayne falls back to Indiana statewide (no metro in range)")
    check(V.locate_market("37206")["code"] == "TN-BNA", "East Nashville ZIP → Nashville metro")
    check(V.locate_market("92104")["code"] == "CA-SD", "San Diego ZIP → San Diego metro")
    kck = V.locate_market("66101")
    check(kck is not None and kck["code"] == "MO-KC", "Kansas City KS ZIP crosses state line to the KC metro")
    esb = V.locate_market("350 5th Ave, New York, NY 10118")
    check(esb is not None and esb["code"] == "NY-NYC", "single-building Manhattan ZIP resolves via prefix fallback")
    check(V.locate_market("no zip here at all") is None, "address without a ZIP → None")
    check(V.extract_zip("12345 Main St, Austin TX 78704") == "78704", "last 5-digit group wins (house numbers lose)")

# ── metro breakouts: metro > rest-of-state, and the blend still ≈ statewide ──
METRO_PAIRS = [("IL-CHI", "IL"), ("WA-SEA", "WA"), ("TX-AUS", "TX"), ("MA-BOS", "MA"),
               ("PA-PHL", "PA"), ("FL-MIA", "FL"), ("CO-DEN", "CO"), ("GA-ATL", "GA"),
               ("NY-NYC", "NY"), ("CA-BAY", "CA")]
for metro, rest in METRO_PAIRS:
    check(V.STATE_COST_FACTORS[metro] > V.STATE_COST_FACTORS[rest],
          f"{metro} factor exceeds {rest} (rest of state)")
# researched population-weighted blends reproduce the statewide averages
BLENDS = [("IL-CHI", "IL", 0.67, 1.09), ("WA-SEA", "WA", 0.51, 1.12), ("TX-AUS", "TX", 0.08, 0.93),
          ("MA-BOS", "MA", 0.63, 1.16), ("PA-PHL", "PA", 0.31, 1.02), ("FL-MIA", "FL", 0.27, 0.97),
          ("CO-DEN", "CO", 0.51, 1.03), ("GA-ATL", "GA", 0.57, 0.96)]
for metro, rest, share, statewide in BLENDS:
    blend = share * V.STATE_COST_FACTORS[metro] + (1 - share) * V.STATE_COST_FACTORS[rest]
    check(abs(blend - statewide) < 0.02, f"{metro}+{rest} pop-weighted blend ≈ statewide {statewide} (got {blend:.3f})")
# metro $/sqft sanity (published-band midpoints via the validated transfer)
between(V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="WA-SEA")["psf"], 165, 215, "Seattle gut $/sqft near ~$188")
between(V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="MA-BOS")["psf"], 175, 220, "Boston gut $/sqft near ~$195")
between(V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="IL-CHI")["psf"], 155, 200, "Chicago gut $/sqft near ~$177")
between(V.remodel_budget(1500, 3, 2, 1960, "gut", "mid", state="TX-AUS")["psf"], 135, 170, "Austin gut $/sqft near ~$150")

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

# ── adjustable thresholds ──
# fair["margin"] is ~11.6% — flips class as the bar moves
mid_case = (300_000, 60_000, 420_000)
check(V.flip_verdict(*mid_case)["cls"] == "fair", "≈12% margin is FAIR at the default 20/8 bar")
check(V.flip_verdict(*mid_case, green_pct=10)["cls"] == "buy", "lowering the buy bar to 10% flips it green")
check(V.flip_verdict(*mid_case, green_pct=30, fair_pct=15)["cls"] == "no", "raising the pass bar to 15% flips it red")
strict = V.flip_verdict(100_000, 50_000, 300_000, green_pct=30)
at_strict_max = V.flip_verdict(strict["max_offer"], 50_000, 300_000, green_pct=30)
check(abs(at_strict_max["margin"] - 30.0) < 0.2, "max_offer tracks a custom 30% green bar")
check(strict["max_offer"] < green["max_offer"], "a stricter bar lowers the max offer")
clamped = V.flip_verdict(*mid_case, green_pct=200, fair_pct=90)
check(clamped["green_pct"] == 60 and clamped["fair_pct"] < 60, "thresholds clamp sanely (fair stays below green)")
check(V.flip_verdict(*mid_case, green_pct=20, fair_pct=8)["verdict"] == V.flip_verdict(*mid_case)["verdict"],
      "explicit defaults match implicit defaults")

# ── any-state ZIP lookup (ARV source) — guarded on the bundled zips.db ──
if V._ZIPS_DB.exists():
    fw = V.zip_market("46802")
    check(fw is not None and fw["state"] == "IN" and (fw["median_home_value"] or 0) > 0,
          "Fort Wayne 46802 resolves with a median value (national coverage)")
    check(V.zip_market("00000") is None, "unknown ZIP → None")

# ── contractor plan: phase mapping, reconciliation, PDF ──
import remodel_plan as P
from datetime import date as _date

mapped = [k for ph in P.PLAN_PHASES for k in ph["keys"]] + list(P.MGMT_KEYS)
check(len(mapped) == len(set(mapped)), "no line key is mapped to two phases")
item_keys = {it["key"] for it in V.REMODEL_ITEMS}
check(item_keys <= set(mapped),
      f"every REMODEL_ITEMS key has a phase (missing: {item_keys - set(mapped)})")

# maximal budget (gut + conversion + URM + full foundation + pre-1978):
# nothing lands in the leftover phase, dollars reconcile, order is the build order
big = V.remodel_budget(1703, 2, 1, 1922, "gut", "mid",
                       conversion=True, masonry=True, foundation_replace=True)
plan = P.build_plan(big)
check(all(p["title"] != "Additional scope items" for p in plan["phases"]),
      "maximal budget leaves no unphased leftovers")
check(abs(plan["phases_subtotal"] + plan["mgmt_subtotal"] - big["total"]) <= 5,
      "phase subtotals + management reconcile with the budget total")
check([p["n"] for p in plan["phases"]] == list(range(1, len(plan["phases"]) + 1)),
      "phases number contiguously from 1")
titles = [p["title"] for p in plan["phases"]]
def _idx(frag):
    return next(i for i, t in enumerate(titles) if frag in t)
check(_idx("Pre-construction") < _idx("abatement") < _idx("Foundation") < _idx("dry-in")
      < _idx("Rough-in") < _idx("Insulation") < _idx("Interior") < _idx("punch"),
      "maximal budget runs all 8 phases in build order")
check(len(plan["management"]) == 2 and plan["mgmt_subtotal"] > 0,
      "GC O&P + contingency carried as management, not phases")
phase_of = {r["key"]: p["title"] for p in plan["phases"] for r in p["items"]}
check("change_of_use" in phase_of and "Pre-construction" in phase_of["change_of_use"],
      "change-of-use permits land in pre-construction")
check("Rough-in" in phase_of.get("fire_sprinklers", ""), "sprinklers land in rough-in")
check("windows_egress" in phase_of and "windows" not in phase_of,
      "conversion swaps egress windows into the dry-in phase")

# cosmetic post-1978 job collapses to a short plan but still starts with permits
cos = V.remodel_budget(1200, 3, 2, 1995, "cosmetic", "mid", state="TX")
cplan = P.build_plan(cos)
check(2 <= len(cplan["phases"]) <= 5, "cosmetic job collapses to a short phase list")
check(cplan["phases"][0]["n"] == 1 and "Pre-construction" in cplan["phases"][0]["title"],
      "cosmetic plan renumbers from Phase 1 (permits first)")
check(abs(cplan["phases_subtotal"] + cplan["mgmt_subtotal"] - cos["total"]) <= 5,
      "cosmetic plan dollars reconcile too")

# HTML: escaping + no deal economics ever
h = P.plan_html(big, address='<script>alert(1)</script> 516 Ward St, Martinez, CA 94553')
check("<script>" not in h, "address is HTML-escaped in the plan")
for frag in ("Asking", "ARV", "margin", "max offer"):
    check(frag.lower() not in h.lower(), f"plan never leaks deal economics ({frag})")

# PDF: real bytes, both variants, deterministic date, phased content present
# (assertions avoid words with 'fi' — extraction renders them as ligatures)
pdf = P.plan_pdf(big, address="516 Ward St, Martinez, CA 94553",
                 generated=_date(2026, 1, 15))
check(pdf[:5] == b"%PDF-" and len(pdf) > 5000, "plan PDF renders with the PDF magic")
import fitz
_doc = fitz.open(stream=pdf, filetype="pdf")
_text = "".join(pg.get_text() for pg in _doc)
for frag in ("Renovation Scope of Work", "516 Ward St", "Phase 1", "Rough-in",
             "January 15, 2026", f"${big['total']:,}"):
    check(frag in _text, f"plan PDF contains {frag!r}")
nodollar = P.plan_pdf(big, address="516 Ward St, Martinez, CA 94553", dollars=False)
_text2 = "".join(pg.get_text() for pg in fitz.open(stream=nodollar, filetype="pdf"))
check("Allowance" not in _text2 and f"${big['total']:,}" not in _text2,
      "no-$ variant strips allowances and totals")
check("Phase 1" in _text2 and "Rough-in" in _text2, "no-$ variant keeps scope + sequence")

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
