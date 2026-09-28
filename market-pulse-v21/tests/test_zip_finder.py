"""ZIP finder — filters, the funnel, and priority scoring for /multifamily.

Run:  python tests/test_zip_finder.py      (exit 0 = all pass)

Pure. Every outcome is reachable from a list of dicts.

The checks are weighted toward the ways a filter can lie. A filter that is
too strict is visible — the board is short and the funnel says why. A filter
that lets through a ZIP it could not measure is invisible: the row looks like
it passed. So the heaviest checks are on NO DATA, and on the funnel adding up,
because a removal the funnel did not record is a removal nobody can see.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import zip_env as E                                          # noqa: E402
import zip_finder as Z                                       # noqa: E402

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# AREA TYPE — measured density, named honestly
# ══════════════════════════════════════════════════════════════════
# zips.db stores people per km². 500/sq mi is ~193/km².
check(Z.area_type(100 / Z.SQMI_PER_KM2) == "rural", "100 people/sq mi is rural")
check(Z.area_type(499.9 / Z.SQMI_PER_KM2) == "rural",
      "just under 500/sq mi is still rural")
check(Z.area_type(500 / Z.SQMI_PER_KM2) == "suburban",
      "500/sq mi exactly is suburban — the lower bound is inclusive")
check(Z.area_type(2999 / Z.SQMI_PER_KM2) == "suburban", "2,999 is suburban")
check(Z.area_type(3000 / Z.SQMI_PER_KM2) == "urban", "3,000 is urban")
check(Z.area_type(80_000) == "urban", "Manhattan-scale density is urban")
check(Z.area_type(0) == "rural", "zero density is rural, not unknown")
check(Z.area_type(None) is None and Z.area_type(-5) is None
      and Z.area_type("dense") is None,
      "missing, negative or non-numeric density is UNKNOWN, not rural — "
      "an unmeasured ZIP must not be sorted into a bucket")
check(abs(Z.per_sq_mi(1.0) - 2.589988) < 1e-9,
      "the unit conversion is km² -> sq mi, not the reverse")


# ══════════════════════════════════════════════════════════════════
# PARSING — a URL cannot ask for a threshold the page never offered
# ══════════════════════════════════════════════════════════════════
f = Z.parse_filters({"min_income": "75000", "min_cap": "6", "max_cost": "0",
                     "min_trend": "0"})
check(f["min_income"] == 75000 and f["min_cap"] == 6,
      "offered thresholds parse")
check(f["max_cost"] == 0 and f["min_trend"] == 0,
      "ZERO IS A THRESHOLD, NOT 'OFF'. 'Tenants cover my cost' (max cost 0) "
      "and 'not falling' (trend >= 0) are the two most useful settings on "
      "the page, and a truthiness check would silently disable both")
check(len(Z.active_filters(f)) == 4,
      "and both zero-valued filters count as active")

f = Z.parse_filters({"min_income": "76000", "min_cap": "99", "min_degree": "abc",
                     "max_cost": "-500", "min_trend": "inf"})
check(all(f[k] is None for k in ("min_income", "min_cap", "min_degree",
                                  "max_cost", "min_trend")),
      "thresholds the page never offered are OFF, not honoured — a "
      "hand-edited URL cannot set a filter whose effect nobody has seen")
check(Z.parse_filters({})["area"] == [] and len(Z.active_filters(Z.parse_filters({}))) == 0,
      "no params, no filters")

check(Z.parse_filters({"area": ["urban", "rural"]})["area"] == ["rural", "urban"],
      "area picks come back in canonical order")
check(Z.parse_filters({"area": "suburban,urban"})["area"] == ["suburban", "urban"],
      "a comma-separated area string works too")
check(Z.parse_filters({"area": ["urban", "urban", "castle"]})["area"] == ["urban"],
      "duplicates and unknown area names are dropped")
check(Z.parse_filters({"area": ["rural", "suburban", "urban"]})["area"] == [],
      "ALL THREE TICKED IS NO FILTER, normalised to [] — otherwise the "
      "funnel shows an area stage that removed nothing and implies it did "
      "something")


# ══════════════════════════════════════════════════════════════════
# THE FUNNEL — every removal recorded, and it adds up
# ══════════════════════════════════════════════════════════════════
def _row(**kw):
    base = {"zip": kw.pop("zip", "00000"), "median_household_income": 80_000,
            "pct_bachelors": 40.0, "house_hack_net": 300, "cap_rate_pct": 6.0,
            "trend_3yr": 3.0, "area_type": "suburban"}
    base.update(kw)
    return base


rows = [_row(zip="A"), _row(zip="B", median_household_income=60_000),
        _row(zip="C", median_household_income=None), _row(zip="D", cap_rate_pct=3.0),
        _row(zip="E", area_type="urban"), _row(zip="F", area_type=None)]
fun = Z.Funnel("test ZIPs", rows)
kept = Z.apply_filters(fun, Z.parse_filters({"min_income": "75000",
                                             "area": ["suburban"]}))
check([r["zip"] for r in kept] == ["A", "D"],
      f"income and area filters keep exactly the rows that meet both "
      f"(got {[r['zip'] for r in kept]})")
check(fun.balances(),
      "THE FUNNEL ADDS UP: start minus everything removed is exactly what "
      "is left. A step that dropped rows without recording them would break "
      "this, which is the only way a silent removal gets caught")
_area = next(s for s in fun.stages if s["key"] == "area")
_inc = next(s for s in fun.stages if s["key"] == "min_income")
check(_area["removed"] == 2 and _area["no_data"] == 1,
      "the area stage removed the urban ZIP AND the one with no density — "
      "and counted the second separately, as a gap in the data")
check(_inc["removed"] == 2 and _inc["no_data"] == 1,
      "the income stage removed the $60k ZIP and the one with no figure, "
      "and says which was which")
check(_area["after"] == _inc["before"],
      "stages chain: each starts where the last ended")
check(_area["kind"] == "filter" and _inc["kind"] == "filter",
      "the user's own filters are marked as such, so the page shows them "
      "even when they removed nothing — 'your filter removed nothing' is an "
      "answer; a system step that removed nothing is not")
_sys = Z.Funnel("x", [_row()])
_sys.step("pop", "tiny", keep=lambda r: True)
check(_sys.stages[0]["kind"] == "system", "a plain step defaults to system")

# ── NO DATA NEVER PASSES ──
for f_spec in Z.FILTERS:
    t = f_spec["choices"][0]
    fun = Z.Funnel("x", [_row(**{f_spec["field"]: None})])
    out = Z.apply_filters(fun, {f_spec["key"]: t, "area": []})
    check(out == [] and fun.stages[-1]["no_data"] == 1,
          f"A ZIP WITH NO {f_spec['field']} FAILS the {f_spec['key']} filter "
          f"and is counted as no-data — it does not pass by default")
    fun = Z.Funnel("x", [_row(**{f_spec["field"]: True})])
    check(Z.apply_filters(fun, {f_spec["key"]: t, "area": []}) == [],
          f"and a boolean in {f_spec['field']} is not a number (True >= 0 "
          f"would otherwise pass it)")

# ── min and max run the right way, boundaries included ──
fun = Z.Funnel("x", [_row(zip="eq", cap_rate_pct=6.0), _row(zip="lo", cap_rate_pct=5.99)])
check([r["zip"] for r in Z.apply_filters(fun, {"min_cap": 6, "area": []})] == ["eq"],
      "a MIN filter keeps the boundary value and drops just below it")
fun = Z.Funnel("x", [_row(zip="eq", house_hack_net=0), _row(zip="hi", house_hack_net=1),
                     _row(zip="neg", house_hack_net=-400)])
check([r["zip"] for r in Z.apply_filters(fun, {"max_cost": 0, "area": []})] == ["eq", "neg"],
      "a MAX filter keeps the boundary and everything below it — negative "
      "cost is cash flow, the best case, and must pass 'tenants cover it'")
fun = Z.Funnel("x", [_row(zip="up", trend_3yr=0.0), _row(zip="dn", trend_3yr=-0.1)])
check([r["zip"] for r in Z.apply_filters(fun, {"min_trend": 0, "area": []})] == ["up"],
      "'not falling' keeps a flat market and drops a falling one")

fun = Z.Funnel("x", [_row(zip="A"), _row(zip="B")])
Z.apply_filters(fun, Z.parse_filters({}))
check(fun.stages == [] and fun.end == 2,
      "no active filters adds no stages — the funnel never shows a step "
      "that did not happen")

fun = Z.Funnel("x", [_row(zip="A", cap_rate_pct=3.0)])
Z.apply_filters(fun, {"min_cap": 6, "area": []})
check(fun.emptied_by() is not None and fun.emptied_by()["key"] == "min_cap",
      "an empty board names THE STEP THAT EMPTIED IT, so the page can say "
      "'your cap-rate filter removed the last one' instead of 'no results'")
check(Z.Funnel("x", [_row()]).emptied_by() is None,
      "and a board that isn't empty names nothing")

fun = Z.Funnel("x", [_row(zip="A"), _row(zip="B")])
fun.step("s1", "custom", keep=lambda r: r["zip"] == "A")
check(fun.stages[0]["removed"] == 1 and fun.stages[0]["no_data"] == 0 and fun.balances(),
      "a step with no no_data rule counts nothing as a gap")


# ══════════════════════════════════════════════════════════════════
# AVAILABILITY — Census filters light up only when there is data
# ══════════════════════════════════════════════════════════════════
_renter = Z.FILTER_BY_KEY["min_renter"]
check(not Z.available(_renter, set()),
      "RENTER SHARE IS NOT OFFERED OVER AN EMPTY COLUMN. Every row would be "
      "no-data, and the filter would empty the board while looking like a "
      "strict choice the user made")
check(Z.available(_renter, {"pct_renter_occupied"}),
      "and it lights up by itself once the Census column has data — no "
      "deploy needed when the fetch is fixed")
check(Z.available(Z.FILTER_BY_KEY["min_income"], set()),
      "filters on always-present columns are always available")
check({p["key"] for p in Z.PENDING} == {"schools"},
      "schools is still listed as PENDING — visible as unavailable rather than "
      "silently missing")
_yp, _24 = Z.FILTER_BY_KEY["min_young"], Z.FILTER_BY_KEY["min_2_4"]
check(_yp["field"] == "pct_age_25_34" and _24["field"] == "pct_2_4_units"
      and not Z.available(_yp, set()) and Z.available(_yp, {"pct_age_25_34"})
      and Z.available(_24, {"pct_2_4_units"}),
      "YOUNG ADULTS AND 2–4 UNIT STOCK ARE REAL FILTERS NOW, on the Census "
      "columns the keyless ACS pull fills — and like the other Census filters "
      "they are offered only where their column has data")
_fun = Z.Funnel("x", [_row(zip="young", pct_age_25_34=19.0), _row(zip="old", pct_age_25_34=8.0),
                      _row(zip="unk", pct_age_25_34=None)])
check([r["zip"] for r in Z.apply_filters(_fun, {"min_young": 15, "area": []})] == ["young"]
      and _fun.stages[-1]["label"] == "young adults (25–34) below 15%",
      "'young adults at least 15%' keeps 19%, drops 8% and the ZIP with no figure")
check(not {p["key"] for p in Z.PENDING} & {"weather", "hazards"},
      "weather and hazards are real filters now, not pending")
_wx = [f for f in Z.FILTERS if f["group"] in ("weather", "hazards")]
check(len(_wx) == 7 and all(f.get("needs") in ("zip_climate", "zip_hazards") for f in _wx),
      "the seven weather and hazard filters each name the file they need")
check(not any(Z.available(f, set()) for f in _wx)
      and all(Z.available(f, {"zip_climate", "zip_hazards"}) for f in _wx),
      "AND ARE OFFERED ONLY WHEN THAT FILE HAS DATA FOR THE STATE — a winter "
      "filter over a missing file would mark every ZIP no-data and empty the board")
_fl = Z.FILTER_BY_KEY["max_flood"]
_fun = Z.Funnel("x", [_row(zip="typical", flood_rate=83.65), _row(zip="edge", flood_rate=100.0),
                      _row(zip="bad", flood_rate=167.69), _row(zip="unk", flood_rate=None)])
_kept = Z.apply_filters(_fun, {"max_flood": 100, "area": []})
check([r["zip"] for r in _kept] == ["typical", "edge"] and _fun.stages[-1]["no_data"] == 1,
      "'flood damage at most $100/yr per $100k' keeps the typical ZIP and one at "
      "exactly $100, drops $168, and counts the ZIP with no flood figure as a data gap")
check(_fun.stages[-1]["label"] == "flood damage above $100/yr per $100k",
      f"and the funnel says so in dollars (got {_fun.stages[-1]['label']!r})")
_haz = [f for f in Z.FILTERS if f["group"] == "hazards"]
check(len(_haz) == 4 and all(f["unit"] == "$/100k" and f["op"] == "max"
                             and f["field"] == f"{f['hazard']}_rate" for f in _haz),
      "HAZARD FILTERS ARE DOLLARS, NOT RANKS. Most ZIPs expect almost no wildfire "
      "or earthquake damage, so a national rank turns $1 a year into 'worse than "
      "80% of the country'; every hazard filter reads the dollar rate")
check({f["hazard"] for f in _haz} == set(E.HAZARD_GROUPS),
      "one filter per hazard group the data file carries, each naming its group "
      "so the page can show that group's national median beside it")
check(all(list(f["choices"]) == sorted(f["choices"], reverse=True) for f in _haz),
      "hazard choices run loosest to strictest, like the other max filters")
_fun = Z.Funnel("x", [_row(zip="typ", wildfire_rate=0.13), _row(zip="dry", wildfire_rate=11.35)])
check([r["zip"] for r in Z.apply_filters(_fun, {"max_fire": 2, "area": []})] == ["typ"],
      "the strictest wildfire choice keeps the typical ZIP's 13 cents and drops $11")
_fun = Z.Funnel("x", [_row(zip="mild", winter_low=24.8), _row(zip="cold", winter_low=9.0)])
check([r["zip"] for r in Z.apply_filters(_fun, {"min_winter": 20, "area": []})] == ["mild"],
      "'winter low 20°F or milder' keeps Cleveland's 24.8 and drops a 9°F winter")
check(Z.choice_label(Z.FILTER_BY_KEY["max_snow"], 30) == "at most 30 in"
      and Z.choice_label(_fl, 150) == "at most $150/yr per $100k",
      "weather and hazard choices read as plain English")
check(all(f.get("group") in ("neighborhood", "money", "weather", "hazards") for f in Z.FILTERS),
      "every filter declares which panel group it belongs in")
check(Z.choice_label(Z.FILTER_BY_KEY["max_cost"], 0) == "Tenants cover it ($0 or less)"
      and Z.choice_label(Z.FILTER_BY_KEY["min_cap"], 6) == "at least 6%"
      and Z.choice_label(Z.FILTER_BY_KEY["max_cost"], 500) == "at most $500/mo",
      "dropdown labels read as plain English, and a max filter says 'at most'")
check(not any(f["field"] in ("walk_score", "restaurant_score", "crime_index")
              for f in Z.FILTERS),
      "NONE OF THE THREE PROXY COLUMNS IS A FILTER. walk_score and "
      "restaurant_score are both curves on density, crime_index is density "
      "+ income + education; offering them alongside area type would filter "
      "on density three times under three names")
check(not any(field in ("walk_score", "restaurant_score", "crime_index")
              for _k, _l, field, _lo in Z.PRIORITIES),
      "nor a ranking signal")


# ══════════════════════════════════════════════════════════════════
# PRIORITIES AND PRESETS
# ══════════════════════════════════════════════════════════════════
w, p = Z.parse_priorities(None, {})
check(p == "balanced" and w == Z.PRESETS["balanced"]["weights"],
      "a first visit gets the balanced preset")
w, p = Z.parse_priorities("cashflow", {"w_growth": "3"})
check(p == "cashflow" and w["growth"] == 0,
      "a preset button WINS over the weight fields it was submitted with — "
      "otherwise clicking a preset would change nothing")
w, p = Z.parse_priorities(None, {f"w_{k}": str(v) for k, v in
                                 Z.PRESETS["growth"]["weights"].items()})
check(p == "growth", "weights that match a preset exactly are shown as that preset")
w, p = Z.parse_priorities(None, {"w_cashflow": "3", "w_yield": "1", "w_growth": "0",
                                 "w_income": "0", "w_education": "0"})
check(p == "custom" and w["yield"] == 1, "anything else is custom")
w, p = Z.parse_priorities("nonsense", {"w_cashflow": "9", "w_yield": "x"})
check(w["cashflow"] == 0 and w["yield"] == 0,
      "out-of-range and junk weights are 0, not clamped to a guess")
check(all(set(pr["weights"]) == set(Z.PRIORITY_KEYS) for pr in Z.PRESETS.values()),
      "every preset sets every priority, so switching presets never leaves "
      "a stale weight behind")


# ══════════════════════════════════════════════════════════════════
# PERCENTILES AND SCORING
# ══════════════════════════════════════════════════════════════════
check(Z.percentiles([10, 20, 30]) == [0.0, 50.0, 100.0], "plain percentiles")
check(Z.percentiles([10, 20, 30], lower_is_better=True) == [100.0, 50.0, 0.0],
      "lower-is-better inverts")
check(Z.percentiles([5, 5, 9]) == [25.0, 25.0, 100.0],
      "TIES SHARE THE MIDPOINT: two ZIPs with identical income must score "
      "identically, and bisect_left alone would hand both the bottom rank")
check(Z.percentiles([7]) == [50.0],
      "a single row scores 50 — neither best nor worst of one")
check(Z.percentiles([None, 3, None, 1]) == [None, 100.0, None, 0.0],
      "missing values stay missing and do not move the others")
check(Z.percentiles([]) == [], "empty in, empty out")

rows = [_row(zip="cheap", house_hack_net=-200, cap_rate_pct=8.0),
        _row(zip="mid", house_hack_net=300, cap_rate_pct=6.0),
        _row(zip="dear", house_hack_net=900, cap_rate_pct=4.0)]
Z.score(rows, {"cashflow": 3, "yield": 3, "growth": 0, "income": 0, "education": 0})
check([r["mf_score"] for r in rows] == [100.0, 50.0, 0.0],
      "with cash flow and yield weighted, the ZIP best on both scores 100 "
      "and the worst on both scores 0")
check(rows[0]["score_parts"] == {"cashflow": 100.0, "yield": 100.0},
      "the parts that made the score are kept, so the page can show them")

rows = [_row(zip="a", median_household_income=50_000, house_hack_net=0),
        _row(zip="b", median_household_income=150_000, house_hack_net=0)]
Z.score(rows, {"cashflow": 0, "yield": 0, "growth": 0, "income": 3, "education": 0})
check(rows[1]["mf_score"] > rows[0]["mf_score"],
      "weighting income ranks the richer ZIP higher")
Z.score(rows, {"cashflow": 3, "yield": 0, "growth": 0, "income": 0, "education": 0})
check(rows[0]["mf_score"] == rows[1]["mf_score"],
      "and weighting only cash flow, where they tie, scores them equally — "
      "the weights actually change the answer")

rows = [_row(zip="full", trend_3yr=5.0), _row(zip="gap", trend_3yr=None),
        _row(zip="low", trend_3yr=1.0)]
Z.score(rows, {"cashflow": 1, "yield": 0, "growth": 3, "income": 0, "education": 0})
gap = rows[1]
check(gap["mf_score"] is not None and gap["score_basis"] == "1 of 2"
      and gap["score_complete"] is False,
      "A ROW MISSING ONE SIGNAL IS SCORED ON THE REST AND SAYS SO. The old "
      "board dropped such rows silently; scoring the gap as zero would "
      "punish the ZIP for our missing data")
check(rows[0]["score_complete"] is True and rows[0]["score_basis"] == "2 of 2",
      "a complete row says it is complete")

rows = [_row(zip="a"), _row(zip="b", house_hack_net=900)]
res = Z.score(rows, {k: 0 for k in Z.PRIORITY_KEYS})
check(res["defaulted"] is True and all(r["mf_score"] is not None for r in rows),
      "ALL-ZERO WEIGHTS FALL BACK to the default preset and report that "
      "they did — 'rank by nothing' is not a ranking, and returning no "
      "scores would empty the table")
res = Z.score(rows, Z.PRESETS["balanced"]["weights"])
check(res["defaulted"] is False, "and real weights are not reported as defaulted")

rows = [_row(zip="x", house_hack_net=None, cap_rate_pct=None)]
Z.score(rows, {"cashflow": 3, "yield": 3, "growth": 0, "income": 0, "education": 0})
check(rows[0]["mf_score"] is None,
      "a row with NONE of the weighted signals has no score, not a zero")

# Relative: percentiles are among the rows passed in, so filtering changes
# the curve. This is the intended semantics and it is asserted, not assumed.
big = [_row(zip=str(i), house_hack_net=i * 100) for i in range(10)]
Z.score(big, {"cashflow": 3, "yield": 0, "growth": 0, "income": 0, "education": 0})
mid_in_big = next(r for r in big if r["zip"] == "5")["mf_score"]
small = [dict(r) for r in big if int(r["zip"]) >= 5]
Z.score(small, {"cashflow": 3, "yield": 0, "growth": 0, "income": 0, "education": 0})
mid_in_small = next(r for r in small if r["zip"] == "5")["mf_score"]
check(mid_in_small == 100.0 and mid_in_big < 100.0,
      "SCORES ARE RELATIVE TO WHAT YOU'D CONSIDER: the same ZIP is the best "
      "of the ones left once the cheaper half is filtered out. Scoring "
      "against the whole state would let ZIPs you ruled out set the curve")


# ══════════════════════════════════════════════════════════════════
# BOARD ORDER — an unmeasured ZIP never outranks a measured one
# ══════════════════════════════════════════════════════════════════
_b = Z.board_order([
    {"zip": "unk_hi", "mf_score": 99.0, "safety": {"tier": "unknown"}},
    {"zip": "safe_lo", "mf_score": 10.0, "safety": {"tier": "safe"}},
    {"zip": "safe_hi", "mf_score": 80.0, "safety": {"tier": "very_safe"}},
    {"zip": "unk_lo", "mf_score": 5.0, "safety": {"tier": "unknown"}},
    {"zip": "safe_none", "mf_score": None, "safety": {"tier": "safe"}},
    {"zip": "no_safety_key", "mf_score": 90.0},
])
check([r["zip"] for r in _b] == ["safe_hi", "safe_lo", "safe_none",
                                 "unk_hi", "no_safety_key", "unk_lo"],
      f"VERIFIED ROWS FIRST, BY SCORE; UNMEASURED AFTER, BY SCORE. A 99 on a "
      f"ZIP nobody has a crime figure for must not sit above a verified 10 — "
      f"the high score would read as a recommendation the data can't back. "
      f"A row with no safety record at all counts as unmeasured, and a "
      f"missing score sorts last in its group (got {[r['zip'] for r in _b]})")
check(Z.board_order([]) == [], "empty in, empty out")


if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ZIP-finder checks passed.")
print("   No-data never passes a filter, the funnel adds up, zero is a real\n"
      "   threshold, and scores are relative to the ZIPs you'd actually consider.")
sys.exit(0)
