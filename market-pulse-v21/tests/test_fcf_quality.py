"""FCF quality — the VFLO funnel, proved against fixtures and its own shape.

Run:  python tests/test_fcf_quality.py      (exit 0 = all pass)

Pure. No network, no snapshot file. The screen is arithmetic over a
filing, and every failure it can have is reachable from a dict.

The checks are weighted towards the denominators. A yield is FCF over EV,
and every way this screen can be wrong is a way EV can be wrong — an
unfiled debt tag, a net-cash company, a unit error. Those do not produce
a wrong row in the middle of the board. They produce a HIGH yield, and a
board ranked by yield puts them on top.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fcf_quality as Q

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# ENTERPRISE VALUE — where the screen can lie to itself
# ══════════════════════════════════════════════════════════════════
ev = Q.enterprise_value(1_000_000_000, total_debt=200_000_000,
                        cash=50_000_000)
check(ev["ev"] == 1_150_000_000 and ev["basis"] == "ev" and ev["usable"],
      f"EV is market cap plus debt minus cash (got {ev['ev']:,})")
check(Q.enterprise_value(1e9, 2e8, 5e7, preferred=1e8, minority=5e7)["ev"]
      == 1_300_000_000,
      "preferred stock and minority interest are added, per VFLO's "
      "definition — both are claims on the enterprise that the equity "
      "market cap does not carry")

missing = Q.enterprise_value(1_000_000_000, total_debt=None, cash=5e7)
check(missing["basis"] == "market_cap" and missing["complete"] is False,
      "A MISSING DEBT TAG DOES NOT BECOME ZERO DEBT. It falls back to a "
      "market-cap basis and says so")
check(missing["ev"] is None and missing["usable"] is False,
      "AND IT PRODUCES NO EV AT ALL rather than a market-cap number "
      "wearing an EV label. `basis` records the only denominator this row "
      "COULD support; whether the cohort uses it is the cohort's call")
check("sort this row UP" in missing["reason"],
      "and the reason names the actual danger: zeroing the debt shrinks "
      "the denominator, which RAISES the yield, which moves the row "
      "toward the top of a board sorted by yield. The bad rows are "
      "exactly the ones the screen surfaces")
check(Q.enterprise_value(1e9, 2e8, None)["basis"] == "market_cap",
      "missing cash does the same")
check("debt and cash" in Q.enterprise_value(1e9, None, None)["reason"],
      "and both missing names both")

netcash = Q.enterprise_value(1_000_000_000, total_debt=0, cash=900_000_000)
check(netcash["usable"] is False,
      "A COMPANY REPORTED AS MOSTLY NET CASH IS NOT RANKED. EV of 10% of "
      "market cap turns any cash flow into a spectacular yield, and the "
      "filing cannot distinguish a genuine net-cash balance sheet from a "
      "debt tag that did not file")
check(Q.enterprise_value(1e9, 0, 2e9)["usable"] is False,
      "and a negative EV certainly is not — that is a division by "
      "something approaching zero from the wrong side")
check(Q.enterprise_value(1e9, 5e10, 0)["usable"] is False,
      "nor is 50x market cap in debt, which is a unit error far more "
      "often than a balance sheet")
check(Q.enterprise_value(1e9, 2e8, 5e7)["usable"] is True,
      "an ordinary balance sheet passes")
check(Q.enterprise_value(0, 1, 1)["ev"] is None
      and Q.enterprise_value(None, 1, 1)["ev"] is None,
      "no market cap is no EV")


# ══════════════════════════════════════════════════════════════════
# YIELD
# ══════════════════════════════════════════════════════════════════
check(Q.fcf_yield(100_000_000, 1_000_000_000) == 10.0,
      "a tenth of enterprise value in free cash flow is a 10% yield")
check(Q.fcf_yield(-5e7, 1e9) is None,
      "NEGATIVE FREE CASH FLOW HAS NO YIELD. It would sort to the bottom "
      "harmlessly, but it would still be COUNTED as measured — and "
      "'we looked at 1,200 companies' must not include ones with no cash "
      "flow to have a yield on")
check(Q.fcf_yield(1e8, 0) is None and Q.fcf_yield(1e8, -1e9) is None,
      "and a zero or negative denominator has none either")
check(Q.fcf_yield(None, 1e9) is None, "missing FCF is missing")

check(Q.yield_fault(8.0) is None, "an 8% yield is ordinary and usable")
check(Q.yield_fault(0.4) is None, "so is a thin one")
check("ceiling" in (Q.yield_fault(80.0) or ""),
      "AN 80% YIELD IS A DATA FAULT, NOT A BARGAIN. The market does not "
      "price a going concern to return its whole enterprise value inside "
      "two years")
check(Q.yield_fault(None) == "no yield" and Q.yield_fault(-3) is not None,
      "missing and negative both name themselves")


# ══════════════════════════════════════════════════════════════════
# TREND — VFLO's slope-over-mean construction
# ══════════════════════════════════════════════════════════════════
flat = Q.trend_score({2021: 100, 2022: 100, 2023: 100, 2024: 100, 2025: 100})
check(flat == 0.0, f"a flat series has no trend (got {flat})")

rising = Q.trend_score({2021: 80, 2022: 90, 2023: 100, 2024: 110, 2025: 120})
check(rising == 10.0,
      f"a series rising 10 a year on a mean of 100 scores the slope over "
      f"the mean: 10/100 = 10% (got {rising})")

# The property that makes the measure comparable at all.
small = Q.trend_score({2021: 8e6, 2022: 9e6, 2023: 10e6, 2024: 11e6,
                       2025: 12e6})
big = Q.trend_score({2021: 8e9, 2022: 9e9, 2023: 10e9, 2024: 11e9,
                     2025: 12e9})
check(small == big,
      f"A $12m REVENUE LINE AND A $12bn ONE GROWING AT THE SAME RATE "
      f"SCORE IDENTICALLY ({small} vs {big}). Dividing the slope by the "
      f"mean is what makes the number comparable across the whole market "
      f"rather than a restatement of company size — which matters more "
      f"here than it does for VFLO, whose universe is all large caps")

falling = Q.trend_score({2021: 120, 2022: 110, 2023: 100, 2024: 90,
                         2025: 80})
check(falling is not None and falling < 0, "a falling series scores negative")
check(abs(falling) == rising, "symmetrically")

check(Q.trend_score({2021: 1, 2022: 2, 2023: 3, 2024: 4}) is None,
      "FOUR POINTS IS NOT A TREND. A line through them is a guess, and a "
      "guess that looks like a measurement is worse than a gap")
check(Q.trend_score({}) is None and Q.trend_score(None) is None,
      "no series is no trend")
check(Q.trend_score({2021: -10, 2022: -8, 2023: -6, 2024: -4,
                     2025: -2}) is None,
      "A SERIES WITH A NEGATIVE MEAN SCORES NOTHING. Normalising by it "
      "flips the sign, so a shrinking loss would read as growth — which "
      "is exactly the company an unguarded screen would rank highest")
check(Q.trend_score({2021: 100, 2022: None, 2023: 100, 2024: 100,
                     2025: 100, 2026: 100}) == 0.0,
      "a gap in the middle is skipped rather than read as zero")


# ══════════════════════════════════════════════════════════════════
# RANKS — VFLO averages ranks, not raw measures
# ══════════════════════════════════════════════════════════════════
r = Q.percentile_ranks([1, 2, 3, 4, 5])
check(r[0] == 0.0 and r[-1] == 100.0, f"ranks span 0 to 100 (got {r})")
check(r == sorted(r), "and preserve order")
check(Q.percentile_ranks([5, 4, 3, 2, 1])[0] == 100.0,
      "the largest value ranks 100 wherever it sits in the list")

ties = Q.percentile_ranks([7, 7, 7, 7])
check(len(set(ties)) == 1 and ties[0] == 50.0,
      f"A COLUMN WHERE EVERY VALUE IS IDENTICAL GIVES EVERYBODY 50, not "
      f"an order decided by position in the list (got {ties})")

gaps = Q.percentile_ranks([1, None, 3])
check(gaps[1] is None and gaps[0] == 0.0 and gaps[2] == 100.0,
      "a missing value stays missing and does not take a rank")
check(Q.percentile_ranks([None, None]) == [None, None],
      "a column of nothing ranks nothing")
check(Q.percentile_ranks([42]) == [50.0],
      "a single value is the median of itself rather than both extremes")

# Why ranks and not the raw numbers.
raw_wide = [1.0, 2.0, 3.0]
raw_narrow = [1000.0, 1001.0, 1002.0]
check(Q.percentile_ranks(raw_wide) == Q.percentile_ranks(raw_narrow),
      "TWO COLUMNS WITH THE SAME ORDER BUT DIFFERENT SPREADS RANK THE "
      "SAME. Averaging the raw measures instead would let whichever "
      "component happened to have the wider spread decide the growth "
      "score on its own — which is why VFLO specifies the average RANK")


# ══════════════════════════════════════════════════════════════════
# GROWTH SCORE
# ══════════════════════════════════════════════════════════════════
check(Q.growth_score([100.0, 50.0]) == 75.0, "two components average")
check(Q.growth_score([100.0, 50.0, 0.0]) == 50.0, "three do too")
check(Q.growth_score([90.0, None]) is None,
      "A SCORE BUILT FROM ONE LEG IS NOT THE SAME MEASUREMENT as one "
      "built from two, and averaging whatever is present would let a "
      "company with a single strong number outrank one measured on both")
check(Q.growth_score([90.0, None], min_components=1) == 90.0,
      "the threshold is a parameter, so the choice is visible")
check(Q.growth_score([None, None]) is None, "nothing scores nothing")


# ══════════════════════════════════════════════════════════════════
# MEASURE — the floors, each naming itself
# ══════════════════════════════════════════════════════════════════
GOOD = {"ticker": "OK", "market_cap": 5e9, "fcf": 4e8, "revenue": 2e9,
        "total_debt": 1e9, "cash": 3e8,
        "growth_components": {"sales": 9.0, "fcf": 7.0}}
m = Q.measure(GOOD)
check(m["measured"] and m["fcf_yield"] is not None,
      f"an ordinary company measures (got {m.get('reason')})")
check(m["ev"] == 5.7e9 and m["basis"] == "ev" and m["rankable"],
      "with a real EV and a place in the ranking")
check(m["fcf_yield"] == round(4e8 / 5.7e9 * 100, 2),
      "and a yield computed on it")

tiny = Q.measure({**GOOD, "market_cap": 10e6})
check(not tiny["measured"] and "floor" in tiny["reason"],
      "A MICRO-CAP UNDER THE FLOOR IS REFUSED AND SAYS WHY. 'Whole "
      "market' means small, not fictional — below the floor a yield sort "
      "ranks shells, because any cash flow divided by a tiny denominator "
      "wins")
check(not Q.measure({**GOOD, "revenue": 1e6})["measured"],
      "and a revenue floor catches what market cap alone cannot — a "
      "registrant with a filing agent")
check(not Q.measure({**GOOD, "fcf": -1e8})["measured"],
      "a company with no free cash flow is OUTSIDE this question, not a "
      "low-ranked member of it")
check("outside the question" in Q.measure({**GOOD, "fcf": -1e8})["reason"],
      "and the reason says so in those terms")
_nc = Q.measure({**GOOD, "total_debt": 0, "cash": 4.9e9})
check(_nc["measured"] is True and _nc["rankable"] is False,
      "a net-cash balance sheet is SEEN but not ranked — the company "
      "exists and its cash flow is real, the denominator is what fails")

# ── the bug this design exists to prevent ──
# The first version fell back to a market-cap denominator whenever debt
# was unfiled. That looked careful and was the opposite: market cap is
# SMALLER than EV for any company carrying net debt, so the fallback
# produced a HIGHER yield — and on a board sorted by yield, the company
# that failed to file its debt outranked the identical company that
# filed. Measured on the same fixture: 8.0% against 7.02%.
nodebt = Q.measure({**GOOD, "total_debt": None}, basis="ev")
check(nodebt["measured"] is True and nodebt["rankable"] is False,
      "A COMPANY WITH NO DEBT TAG IS SEEN AND COUNTED BUT NOT RANKED. "
      "Dropping it silently would shrink the universe; ranking it on a "
      "different denominator would promote it for not filing")
check(nodebt.get("fcf_yield") is None,
      "it carries no yield at all, because a yield on a denominator the "
      "rest of the board is not using is not a comparable number")
check("outrank the one that did" in nodebt["reason"],
      "and the reason names the failure mode rather than saying 'missing "
      "data' — the point is not that a field is absent, it is which "
      "direction the absence moves the row")

_mixed = [
    {**GOOD, "ticker": "FILED"},
    {**GOOD, "ticker": "UNFILED", "total_debt": None},
]
_r = Q.screen(_mixed)
check(_r["basis"] == "ev",
      "with one row able to supply an EV, the cohort is ranked on EV")
check([x["ticker"] for x in _r["measured"]] == ["FILED"],
      "and ONLY the row that can supply one is ranked")
check(any(x["ticker"] == "UNFILED" for x in _r["rejected"]),
      "the other is reported, not discarded")

_none = Q.screen([{**GOOD, "ticker": "A", "total_debt": None},
                  {**GOOD, "ticker": "B", "total_debt": None}])
check(_none["basis"] == "market_cap" and _none["census"]["measured"] == 2,
      "BUT WHEN NOTHING IN THE COHORT CAN SUPPLY AN EV, the whole board "
      "drops to market cap together. Every row shares one denominator, so "
      "the ranking is still internally consistent — it is measuring a "
      "different thing, and the snapshot says which")


# ══════════════════════════════════════════════════════════════════
# THE FUNNEL — order, proportions, and the published shape
# ══════════════════════════════════════════════════════════════════
def company(i, mc, fcf, sales, fcftrend):
    return {"ticker": f"T{i:03d}", "name": f"Company {i}", "market_cap": mc,
            "fcf": fcf, "revenue": mc / 3, "total_debt": mc * 0.2,
            "cash": mc * 0.05,
            "growth_components": {"sales": sales, "fcf": fcftrend}}


# 400 synthetic companies with yield and growth deliberately ANTI-
# correlated, which is the real-world shape the second stage exists for:
# the cheapest cash flow is usually the least-loved growth.
UNIVERSE = []
for i in range(400):
    mc = 1e9 + i * 1e7
    yield_target = 1.0 + (i / 400) * 14.0          # 1% .. 15%
    fcf = mc * 1.15 * yield_target / 100
    growth = 25.0 - (i / 400) * 30.0               # +25% .. -5%
    UNIVERSE.append(company(i, mc, fcf, growth, growth * 0.8))

res = Q.screen(UNIVERSE)
c = res["census"]
check(c["input"] == 400 and c["measured"] == 400,
      f"all 400 synthetic companies measure (got {c})")
check(c["fcf_cut"] == 75,
      f"THE FIRST CUT TAKES 75 OF 400 — VFLO's own proportion, not a "
      f"round number chosen to look similar (got {c['fcf_cut']})")
check(c["final"] == 50,
      f"and the second takes 50 of those 75 (got {c['final']})")
check(len(res["missed_yield"]) == 325 and len(res["missed_growth"]) == 25,
      "the rows that did not survive each stage are kept and counted, so "
      "the funnel adds up")
check(all(r["reason"] for r in res["missed_yield"][:5]),
      "and each names the stage that cut it")

ycut = [r["fcf_yield"] for r in res["fcf_cut"]]
yall = [r["fcf_yield"] for r in res["measured"]]
check(min(ycut) >= max(y for y in yall if y not in ycut) - 1e-9,
      "the yield cut really is the top of the yield ranking")

# THE ORDER IS THE METHOD.
top_growth = max(res["measured"], key=lambda r: r["growth_score"])
check(top_growth not in res["final"],
      "THE FASTEST GROWER IN THE UNIVERSE IS NOT IN THE FINAL BOARD, "
      "because its yield did not clear the first stage. Ranking on a "
      "blend of yield and growth instead would let a spectacular grower "
      "with a 1% yield into a screen whose entire premise is buying cash "
      "flow cheaply")
best_yield = max(res["measured"], key=lambda r: r["fcf_yield"])
check(best_yield in res["fcf_cut"],
      "while the highest yielder clears stage one by construction")

# ── the published shape ──
# VFLO: yield 3.21 -> 7.43 -> 7.39, growth 16.74 -> 9.51 -> 13.41.
s = {x["label"]: x for x in res["stages"]}
u, f, g = (s["Starting universe"], s["Free cash flow screen"],
           s["Growth filter"])
check(f["median_fcf_yield"] > u["median_fcf_yield"] * 1.5,
      f"STAGE 1 ROUGHLY DOUBLES THE YIELD, as VFLO's 3.21 -> 7.43 does "
      f"({u['median_fcf_yield']} -> {f['median_fcf_yield']})")
check(f["median_growth_score"] < u["median_growth_score"],
      f"AND COSTS GROWTH, as their 16.74 -> 9.51 does "
      f"({u['median_growth_score']} -> {f['median_growth_score']}) — this "
      f"is the value trap the second stage exists to remove")
check(g["median_growth_score"] > f["median_growth_score"],
      f"STAGE 2 RESTORES GROWTH, as their 9.51 -> 13.41 does "
      f"({f['median_growth_score']} -> {g['median_growth_score']})")
check(g["median_fcf_yield"] > u["median_fcf_yield"] * 1.5,
      f"AT LITTLE COST IN YIELD, as their 7.43 -> 7.39 does "
      f"({f['median_fcf_yield']} -> {g['median_fcf_yield']}). All four "
      f"movements together are the signature of this funnel; a run that "
      f"does not reproduce them has a bug whatever its absolute numbers "
      f"look like")

# A company that cannot be growth-scored does not consume a yield slot.
NOSCORE = UNIVERSE[:10] + [
    {**company(999, 5e9, 1e9, None, None), "growth_components": {}}]
r2 = Q.screen(NOSCORE)
check(r2["census"]["unscorable"] == 1,
      "a company with no growth components is counted as unscorable")
check(all(x["ticker"] != "T999" for x in r2["fcf_cut"]),
      "AND DOES NOT TAKE ONE OF THE YIELD SLOTS despite having by far the "
      "highest yield in that cohort — VFLO's wording is 'the top 75 "
      "companies with the highest free cash flow yield THAT HAVE A GROWTH "
      "SCORE', and the order of those clauses is load-bearing")

check(Q.screen([])["census"]["input"] == 0,
      "an empty universe produces an empty funnel rather than an error")


# ══════════════════════════════════════════════════════════════════
# TWO INDEPENDENT PATHS TO THE DEBT, AND WHAT EACH CATCHES
# ══════════════════════════════════════════════════════════════════
# The balance-sheet extractor agrees with the pipeline's own
# net-debt/EBIT for the typical company — median ratio exactly 1.00 over
# 1,120 cross-checkable rows. It fails on foreign IFRS filers whose
# borrowings sit under element names the ladder does not carry.

check(Q.debt_cross_check(0.4e9, 4.0e9) is not None,
      "AN EXTRACTED NET DEBT UNDER HALF AN INDEPENDENT ESTIMATE IS "
      "REFUSED. AT&T, T-Mobile, Home Depot and Union Pacific all came "
      "through this way — real borrowings the tag ladder missed")
check("up a board sorted by yield" in Q.debt_cross_check(0.4e9, 4.0e9),
      "and the reason names the consequence, not just the discrepancy")
check(Q.debt_cross_check(3.6e9, 4.0e9) is None,
      "ordinary disagreement passes — the estimate approximates EBIT as "
      "revenue x operating margin and carries real error of its own, so "
      "the check is deliberately loose")

check(Q.debt_cross_check(8.0e9, 4.0e9) is None,
      "AND IT IS ONE-SIDED. An OVERSTATED debt inflates enterprise value, "
      "lowers the yield and loses a name — a cost nobody sees. An "
      "understated one promotes a row up the board. Only the second is "
      "refused: the screen fails toward missing something rather than "
      "toward recommending it")
check(Q.debt_cross_check(0.0, 1e6) is None,
      "and a tiny estimate cannot discriminate, so it abstains rather "
      "than firing on noise")
check(Q.debt_cross_check(1e9, None) is None, "no estimate is no check")

# ── what the cross-check structurally cannot catch ──
# When the extractor finds NOTHING, the independent estimate is usually
# built from the same nothing, so both paths agree and both are wrong.
# Agreement is only evidence when the paths are independent.
check(Q.inferred_zero_fault(True, 77e9) is not None,
      "A $77bn COMPANY WITH NO BORROWINGS TAG OF ANY KIND IS REFUSED. "
      "General Motors arrived exactly this way, and the cross-check could "
      "not see it: its net-debt estimate said net cash too")
check(Q.inferred_zero_fault(True, 200e6) is None,
      "but a small company with no debt tag is ordinary — plenty carry "
      "none, and refusing them would gut the small-cap half of a screen "
      "whose whole point is reaching below large cap")
check(Q.inferred_zero_fault(False, 77e9) is None,
      "a company with real debt tags is never touched by this")
check(Q.inferred_zero_fault(True, None) is None,
      "and an unknown size abstains")
check("tag ladder does not carry" in Q.inferred_zero_fault(True, 77e9),
      "the reason says what is actually wrong — an unmapped element, not "
      "a clean balance sheet")

# THE COST, ASSERTED SO IT IS NOT FORGOTTEN. This guard cannot tell a
# genuinely debt-free large cap from an unmapped tag, so it refuses both.
# Vertex, Intuitive Surgical and Datadog are really debt-light and are
# really excluded. That is accepted: a screen that omits them is worse
# than one that ranks General Motors as debt-free at an inflated yield.
check(Q.inferred_zero_fault(True, 130e9) is not None,
      "a genuinely debt-free large cap is refused TOO, and knowingly — "
      "size alone cannot separate the two cases, and the conservative "
      "direction is the one that does not promote a wrong row")


# ══════════════════════════════════════════════════════════════════
# LEVERAGE, ON A MARKET-CAP BASIS
# ══════════════════════════════════════════════════════════════════
# The sharpest edge on a board that cannot compute enterprise value.
# FCF/market cap equals FCF/EV only when net debt is zero; for a levered
# company market cap is much smaller, so the yield is much HIGHER — and
# most overstated for the most indebted names, which is the worst
# direction on a board sorted by yield.
check(Q.leverage_note(10.76, "market_cap") is not None,
      "A COMPANY AT ~11x EBIT IN NET DEBT IS FLAGGED on a market-cap "
      "basis. Bausch Health printed a 40.5% yield on the first real run; "
      "its enterprise-value yield is a fraction of that")
check("HIGHER than the enterprise-value yield"
      in Q.leverage_note(10.76, "market_cap"),
      "and the note says which DIRECTION the distortion runs, because "
      "'this number is approximate' would leave the reader guessing "
      "whether to adjust it up or down")
check(Q.leverage_note(0.2, "market_cap") is None,
      "an unlevered company is not flagged — for it the two denominators "
      "are nearly the same number")
check(Q.leverage_note(10.76, "ev") is None,
      "AND NOTHING IS FLAGGED ON AN EV BASIS, because there the debt is "
      "already in the denominator and there is nothing left to warn about")
check(Q.leverage_note(None, "market_cap") is None,
      "an unknown leverage abstains rather than asserting safety")


# ══════════════════════════════════════════════════════════════════
# THE POINT OF RUNNING IT WIDE
# ══════════════════════════════════════════════════════════════════
check(Q.size_band(500e9) == "mega" and Q.size_band(50e9) == "large"
      and Q.size_band(5e9) == "mid" and Q.size_band(1e9) == "small"
      and Q.size_band(1e8) == "micro",
      "size bands cover the market")
check(Q.size_band(None) is None and Q.size_band(0) is None,
      "and abstain on a missing cap")

MIXED = [
    {"ticker": "MEGA", "market_cap": 300e9},
    {"ticker": "MID", "market_cap": 5e9},
    {"ticker": "SMALL", "market_cap": 8e8},
]
under = [r["ticker"] for r in Q.below_large_cap(MIXED)]
check(under == ["MID", "SMALL"],
      f"below_large_cap isolates what a 400-name large-cap index cannot "
      f"hold (got {under}) — these are names that passed the SAME "
      f"two-stage test and are invisible to VFLO by construction rather "
      f"than by judgement, which is the entire reason to run the funnel "
      f"over the whole market")


# ══════════════════════════════════════════════════════════════════
# THE PUBLISHED COMPARISON IS DATA, NOT DECORATION
# ══════════════════════════════════════════════════════════════════
v = Q.VFLO_PUBLISHED
check(len(v["stages"]) == 4 and v["stages"][0]["fcf_yield"] == 3.21,
      "VFLO's published funnel is carried in the module so the page can "
      "show it beside ours")
check(v["stages"][-1]["label"] == "Final index"
      and "does not weight" in v["note"],
      "AND THE NOTE SAYS WHY WE HAVE NO FOURTH STAGE: their last step "
      "lifts yield by WEIGHTING holdings by free cash flow. This stops at "
      "selection, so comparing our third stage to their fourth would be "
      "comparing a screen to a portfolio")


# ══════════════════════════════════════════════════════════════════
# FRESHNESS — the guard, and the bug that shows why it is shaped this way
# ══════════════════════════════════════════════════════════════════
# The two source files do not agree on date format and never have.
check(Q._stamp("2026-09-15") is not None
      and Q._stamp("2026-09-07T13:45:58+00:00") is not None,
      "both real stamp formats parse — compounders writes a bare date, "
      "schloss a full timestamp")
check(Q._stamp("2026-09-15") > Q._stamp("2026-09-07T13:45:58+00:00"),
      "A BARE DATE AND A FULL TIMESTAMP COMPARE AT ALL. "
      "datetime.fromisoformat returns a naive value for one and an aware "
      "value for the other, and comparing those raises TypeError — so "
      "without normalising to UTC the guard would take the build down on "
      "the first month both files were read together")
check(Q._stamp("") is None and Q._stamp(None) is None
      and Q._stamp("last tuesday") is None and Q._stamp(20260915) is None,
      "an absent or unparseable stamp is None, not a guess")

# A NAIVE STAMP MEANS UTC, NOT THE RUNNER'S CLOCK. This needs TZ set to
# test at all: on a UTC runner — which is every GitHub runner — reading a
# bare date as local time and reading it as UTC give the same answer, so
# the bug is invisible until the day something runs somewhere else.
_TZ_WAS = os.environ.get("TZ")
try:
    os.environ["TZ"] = "America/Los_Angeles"
    time.tzset()
    check(Q._stamp("2026-09-15").isoformat() == "2026-09-15T00:00:00+00:00",
          "a bare date is midnight UTC even when the runner is not on UTC. "
          "datetime.astimezone() on a naive value assumes LOCAL time, which "
          "would shift compounders' stamp by the runner's offset while "
          "leaving schloss's aware stamp alone — a seven-hour disagreement "
          "between two files that are supposed to be compared")
finally:
    if _TZ_WAS is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = _TZ_WAS
    time.tzset()

_LOGIC_A, _LOGIC_B = "aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb"


def _v(cur_c, cur_s, prev_c, prev_s, cur_l=_LOGIC_A, prev_l=_LOGIC_A):
    return Q.refresh_verdict(
        {"compounders": cur_c, "schloss": cur_s, "logic": cur_l},
        {"compounders": prev_c, "schloss": prev_s, "logic": prev_l})


check(Q.refresh_verdict({"compounders": "2026-09-15"}, None)["action"]
      == "build",
      "no previous snapshot means build — the first run has nothing to "
      "be stale against")

check(_v("2026-09-15", "2026-09-07", "2026-08-12", "2026-09-07")["action"]
      == "build",
      "a forward input builds")
check(_v("2026-08-12", "2026-09-07", "2026-09-15", "2026-09-07")["action"]
      == "fault",
      "an input that went BACKWARD is a fault: the snapshot on disk was "
      "built from newer data than this checkout can see")
check(_v("2026-08-12", "2026-10-01", "2026-09-15", "2026-09-07")["action"]
      == "fault",
      "AND A BACKWARD INPUT OUTRANKS A FORWARD ONE. A run with one fresh "
      "input and one rolled back is not half right — it is a board whose "
      "numerator and denominator come from different months, which is the "
      "single worst thing a yield screen can print")

# ── the bug this guard exists for ────────────────────────────────────
# On the first real run the operator dispatched refresh-compounders (36
# minutes) and then this build (13 seconds). It read the tree as it stood
# BEFORE compounders pushed, so its inputs were not older than the
# snapshot already on disk — they were identical to them, because that
# snapshot had been built from the very same unrefreshed files minutes
# earlier. Both jobs went green.
_HISTORICAL = _v("2026-08-12", "2026-09-07T13:45:58+00:00",
                 "2026-08-12", "2026-09-07T13:45:58+00:00")
check(_HISTORICAL["action"] == "skip",
      "THE HISTORICAL FAILURE IS CAUGHT: identical inputs and an "
      "unchanged screen produce a skip, not a board")
check(_HISTORICAL["action"] != "build",
      "MUTATION — had the rule been the obvious 'refuse if the inputs are "
      "OLDER than the snapshot on disk', this exact case would have "
      "passed it, because nothing was older. Backward movement is a "
      "different and rarer fault. The load-bearing half of this guard is "
      "the one that refuses when nothing moved AT ALL")
check("has NOT" in _HISTORICAL["reason"],
      "and the message says a refresh did not land, because that — not "
      "'no changes' — is what an operator who dispatched one needs to read")

check(_v("2026-08-12", "2026-09-07", "2026-08-12", "2026-09-07",
         cur_l=_LOGIC_B)["action"] == "build",
      "unchanged inputs but CHANGED CODE builds: that is the push "
      "trigger's whole case, and a skip there would leave the page "
      "showing a board the current screen would not produce")
_NO_PREV_LOGIC = _v("2026-08-12", "2026-09-07", "2026-08-12", "2026-09-07",
                    prev_l=None)
check(_NO_PREV_LOGIC["action"] == "build"
      and "predates logic stamping" in _NO_PREV_LOGIC["reason"],
      "a snapshot written before logic stamping cannot prove the screen "
      "is unchanged, so it builds — AND SAYS WHY. Falling through to the "
      "hash comparison would also build, by accident of None never "
      "equalling a hash, and would report 'the code changed' about a "
      "snapshot that simply predates the field")
check(_v("2026-08-12", "2026-09-07", "2026-08-12", "2026-09-07",
         cur_l=None, prev_l=None)["action"] == "build",
      "AND IF BOTH HASHES ARE MISSING IT STILL BUILDS. That is the case "
      "the explicit branch exists for: None == None would otherwise read "
      "as 'the screen is unchanged' and skip, concluding from two absent "
      "values that nothing moved")

# ── unverifiable is not unchanged ────────────────────────────────────
_UNKNOWN = _v(None, "2026-09-07", "2026-08-12", "2026-09-07")
check(_UNKNOWN["action"] == "build" and _UNKNOWN["verified"] is False,
      "A MISSING STAMP DOES NOT EARN A SKIP. It cannot prove nothing "
      "moved, so the build proceeds and records that freshness was not "
      "verified — refusing instead would let a source file that stops "
      "writing as_of silently stop the board forever")
check(_v("garbage", "2026-09-07", "2026-08-12", "2026-09-07")["verified"]
      is False,
      "an unparseable stamp is unverifiable too, not silently equal")
check(_v("2026-08-12", "2026-09-07", "2026-08-12",
         "2026-09-07")["verified"] is True,
      "and a genuine skip IS verified, so the two cases are "
      "distinguishable in the snapshot rather than both reading as 'fine'")

check(set(Q.INPUT_KEYS) == {"compounders", "schloss"},
      "the guard covers BOTH inputs — the schloss half is the one the "
      "chain was rewired for, and a guard watching only compounders would "
      "have missed the stale-market-cap case entirely")


if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} FCF-quality checks passed.")
print("   Missing debt refused, net-cash refused, ranks not raw measures, "
      "and the\n   funnel reproduces VFLO's published yield-up/growth-down/"
      "growth-back shape.")
sys.exit(0)
