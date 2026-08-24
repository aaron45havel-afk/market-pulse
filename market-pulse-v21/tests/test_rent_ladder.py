"""The rent ladder — proved without a network.

Run:  python tests/test_rent_ladder.py      (exit 0 = all pass)

WHY THIS EXISTS. median_rent_monthly used to be `home_value / 17 / 12`
for 17,358 of 25,774 ZIPs. That is not an estimate of rent, it is the
home value in different units, and every yield built on it was circular
— all 17,358 imputed ZIPs shared five distinct cap rates, each 5.88% by
construction.

So the checks that matter here are not arithmetic. They are the ones
that prove a number cannot be invented when no source has one, that the
tier which answered always travels with the answer, and that a source
handing back nonsense is dropped rather than passed along.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rent_ladder as R

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── plausibility ──
check(R.is_plausible(1467) and R.is_plausible(200.0) and R.is_plausible(25000),
      "ordinary rents and both bounds are plausible")
check(not R.is_plausible(116) and not R.is_plausible(41813),
      "the exact ends of the old imputed column ($116 and $41,813) are "
      "rejected — both were stored as ZIP medians and neither is possible")
check(not R.is_plausible(None) and not R.is_plausible("") and not R.is_plausible("abc")
      and not R.is_plausible(0) and not R.is_plausible(-500),
      "missing, junk, zero and negative are all implausible")
check(not R.is_plausible(True),
      "a bool is rejected — Python would float(True) to 1.0, which the "
      "floor catches anyway; the isinstance guard just says so out loud")
check(not R.is_plausible(float("nan")), "NaN is not a rent")

_alt = R.resolve(zori=41813, safmr=1180)
check(_alt["rent"] == 1180 and _alt["tier"] == "safmr",
      "an IMPLAUSIBLE top-tier value falls through to the next tier rather "
      "than being clamped to the ceiling — clamping would manufacture a "
      "believable number out of a source that is broken for that ZIP")
check("zori" not in _alt["alternatives"],
      "and the rejected value is not offered as an alternative either")


# ── precedence ──
_full = R.resolve(zori=1467, safmr=1180, fmr=1100, acs=980)
check(_full["rent"] == 1467 and _full["tier"] == "zori",
      "ZORI wins when present — it is the only tier that measures what is "
      "being asked today")
check(R.resolve(safmr=1180, fmr=1100, acs=980)["tier"] == "safmr",
      "without ZORI, the per-ZIP HUD figure beats the county one")
check(R.resolve(fmr=1100, acs=980)["tier"] == "fmr",
      "county FMR beats a two-year-old survey")
check(R.resolve(acs=980)["tier"] == "acs", "and ACS answers when it is all there is")
check(R.resolve()["rent"] is None and R.resolve()["tier"] is None,
      "WITH NOTHING MEASURED THE ANSWER IS NONE. No formula runs, nothing "
      "is derived from the home value — that would restate the price and "
      "call it a rent, which is the bug this module exists to end")
check("would restate the price" in R.resolve()["note"],
      "and the empty result says why it is empty, so a blank cell reads as "
      "a deliberate absence rather than a page that failed to load")


# ── the tier travels with the number ──
for k in R.TIER_ORDER:
    res = R.resolve(**{k: 1200})
    check(res["tier"] == k and res["label"] and res["basis"] and res["caveat"],
          f"a {k} answer carries its label, basis and caveat")
check(R.resolve(zori=1467)["basis"] == "asking"
      and R.resolve(safmr=1180)["basis"] == "voucher-floor"
      and R.resolve(acs=980)["basis"] == "occupied-gross",
      "and the BASIS is named — $1,467 of asking-rent index and $1,180 of "
      "voucher floor are not the same claim about the same thing")
check("FLOOR, NOT A MARKET RENT" in R.resolve(safmr=1180)["caveat"],
      "the 40th-percentile problem is stated in the SAFMR caveat rather "
      "than left for the reader to know")
check("includes utilities" in R.resolve(acs=980)["caveat"]
      and "lags" in R.resolve(acs=980)["caveat"],
      "and the ACS caveat names both of its distortions")
_keys = set(R.resolve().keys())
check(_keys == set(R.resolve(zori=1467).keys()),
      "an empty result and a full one carry EXACTLY the same keys — a "
      "ragged shape is what 500'd the Schloss page once already")


# ── bedrooms only ever come from HUD ──
_bed = R.resolve(zori=1467, safmr=1180,
                 safmr_bedrooms={"0": 905, "1": 1030, "2": 1433, "3": 1800, "4": 2473})
check(_bed["tier"] == "zori" and _bed["by_bedroom_tier"] == "safmr",
      "the headline rent can come from ZORI while the BEDROOM SPLIT comes "
      "from HUD — two labelled facts side by side, never one blended number")
check(_bed["by_bedroom"]["2"] == 1433, "and the bedroom figures survive")
check(R.resolve(zori=1467)["by_bedroom"] is None,
      "with no HUD data there is no bedroom split — ZORI's ZIP file has "
      "none, and inventing one from the headline would be the old bug in "
      "a new column")
_partial = R.clean_bedrooms({"0": 905, "1": 1030, "2": 1433, "3": None, "4": 9})
check(_partial == {"0": 905, "1": 1030, "2": 1433},
      "a partial bedroom set keeps what is real and drops what is not, "
      "rather than discarding the whole set because HUD omitted one")
check(R.clean_bedrooms({}) is None and R.clean_bedrooms(None) is None
      and R.clean_bedrooms("nope") is None,
      "and nothing usable yields None")
check(R.resolve(fmr=1100, fmr_bedrooms={"2": 1150})["by_bedroom_tier"] == "fmr",
      "county FMR supplies bedrooms when SAFMR has none")
check(R.resolve(safmr=1180, safmr_bedrooms={"2": 1433},
                fmr_bedrooms={"2": 1150})["by_bedroom"]["2"] == 1433,
      "and SAFMR's bedrooms beat the county's when both exist")


# ── the spread between tiers is itself the signal ──
_tight = R.resolve(zori=1800, safmr=1200)
check(_tight["spread"]["ratio"] == 1.5 and "tight" in _tight["spread"]["reading"],
      "asking rents far above the voucher floor read as a tight market")
_soft = R.resolve(zori=1100, safmr=1200)
check("soft" in _soft["spread"]["reading"],
      "and at or below the floor reads as a soft one")
check(R.resolve(zori=1467)["spread"] is None,
      "one source alone has no spread to report, rather than a spread of "
      "zero against itself")
check(R.resolve(zori=1467, acs=980)["spread"]["reading"] is None,
      "and two sources with no voucher floor between them give a ratio "
      "without a reading — the reading is about that specific comparison")
check(_full["alternatives"] == {"zori": 1467, "safmr": 1180, "fmr": 1100, "acs": 980},
      "every tier that answered is carried, not just the winner, so the "
      "spread can be checked rather than taken on faith")


# ── the yield that used to be circular ──
check(R.cap_rate_pct(1467, 300977) == 3.51,
      "a real rent and a real value give a real cap rate")
check(R.cap_rate_pct(None, 300977) is None,
      "NO RENT, NO CAP RATE. The board printed 5.88% for 17,358 ZIPs "
      "because the rent behind it was the home value over 204 — the yield "
      "was arithmetic performed on itself")
check(R.cap_rate_pct(1467, 0) is None and R.cap_rate_pct(1467, None) is None
      and R.cap_rate_pct(1467, "abc") is None,
      "and a missing or impossible home value yields None rather than "
      "raising or dividing by zero")
check(R.cap_rate_pct(116, 300977) is None,
      "an implausible rent cannot produce a cap rate either")
# The circularity, demonstrated: the old imputation forced one answer.
_imputed = [R.cap_rate_pct(hv / 17 / 12, hv) for hv in
            (120_000, 300_977, 415_000, 890_000)]
check(len(set(_imputed)) == 1,
      "PROOF OF THE OLD BUG: feeding the imputation back in gives the SAME "
      "cap rate for a $120k ZIP and an $890k one, because the rent was "
      "always the value over 204. Four wildly different markets, one yield")


# ── scoring a ZIP whose rent nobody measured ──
#
# compute_zip_metrics lives in dallas_neighborhoods and had never seen a
# missing rent, because rent was imputed for every ZIP and so was never
# absent. Deleting the imputation created this case; these checks cover
# it, and they are here rather than in a dallas test file because the
# case only exists because of the ladder.
import dallas_neighborhoods as DN

_BASE = {"median_home_value": 300977, "crime_index": 40, "pct_bachelors": 45,
         "median_household_income": 70000, "walk_score": 60,
         "restaurant_score": 55}
_with = DN.compute_zip_metrics({**_BASE, "median_rent_monthly": 1467})
_without = DN.compute_zip_metrics({**_BASE, "median_rent_monthly": None})

check(_with["cap_rate_pct"] == 5.85, "a ZIP with a rent gets a real cap rate")
check(_without["cap_rate_pct"] is None and _without["rent_to_price"] is None,
      "and one WITHOUT a rent gets None, not 0.0 — a zero cap rate is a "
      "claim that the property yields nothing, which nobody measured")
check(_without["sub_scores"]["cap_rate"] is None,
      "the cap-rate sub-score is absent too rather than scoring 0")
check(all(v is not None for k, v in _without["sub_scores"].items()
          if k != "cap_rate"),
      "while the six dimensions that ARE known still score normally")
check(all(0 < v <= 100 for v in _without["composite_by_persona"].values()),
      "SO THE ZIP IS STILL RANKED. Its cap-rate weight is redistributed "
      "across what is known; counted as zero it would sink to the bottom "
      "of the board, where an unmeasured ZIP reads as a bad one")
_inv = _without["composite_by_persona"]["investor"]
check(_inv > 20,
      f"the investor persona leans hardest on cap rate, so it is the one "
      f"that would collapse — it scores {_inv}, not single digits")
check(DN.compute_zip_metrics({**_BASE, "median_rent_monthly": 1467,
                              "median_home_value": 0})["cap_rate_pct"] is None,
      "a zero home value yields no cap rate either, rather than dividing "
      "by zero or reporting 0%")
# The weights sum to 1.0, so redistribution must not move a complete row.
check(_with["composite_by_persona"] == {
    k: round(sum(_with["sub_scores"][s] * w for s, w in p["weights"].items()), 1)
    for k, p in DN.PERSONAS.items()},
      "and for a row with every dimension known the result is IDENTICAL to "
      "the plain weighted sum — the weights already total 1.0, so nothing "
      "about the existing board's scores moved")


# ── coverage census ──
_cov = R.coverage([{"tier": "zori"}, {"tier": "zori"}, {"tier": "safmr"},
                   {"tier": "acs"}, {"tier": None}, {}])
check(_cov["total"] == 6 and _cov["have"] == 4 and _cov["none"] == 2,
      "coverage counts what is measured against what is missing")
check(_cov["by_tier"]["zori"] == 2 and _cov["by_tier"]["safmr"] == 1,
      "broken out by tier, so 'we have rents' cannot hide a board standing "
      "entirely on two-year-old survey data")
check(_cov["real_pct"] == 66.7, "and states the share as a percentage")
check(R.coverage([])["real_pct"] == 0.0 and R.coverage(None)["total"] == 0,
      "an empty set reports zero rather than dividing by zero")
check(R.coverage(["zori", "acs", None])["have"] == 2,
      "and bare tier strings are accepted alongside row dicts")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} rent-ladder checks passed.")
print(f"   {len(R.TIERS)} tiers: {' > '.join(R.TIER_ORDER)} > none")
print(f"   plausible band ${R.RENT_FLOOR:,.0f}–${R.RENT_CEILING:,.0f}/mo")
sys.exit(0)
