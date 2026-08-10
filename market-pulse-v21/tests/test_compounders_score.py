"""Expected-return decomposition checks.

Run:  python tests/test_compounders_score.py      (exit 0 = all pass)

Every term in E[r] is bounded except, until now, the dividend — which is
the one input that GROWS as the market loses confidence, because it
divides last year's payout by a collapsed price. The live board showed:

    SDEV  Stablecoin Development   E[r] 400.2   D = 388.4
    AD    Array Digital Infra      E[r] 121.1   D = 125.6
    SSTK  Shutterstock             E[r]  35.4   D =  23.1

None of those are forecasts. They are a special distribution, a company
paying 2,222% of its free cash flow, and a stock the market has marked
down 70%.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import compounders as C

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


BASE = {
    "name": "Test Co", "country": "TX", "roic_med": 25.0,
    "rev_cagr5": 8.0, "rev_cagr10": 8.0, "rev_up_years": 9, "rev_up_total": 10,
    "ni_pos_years": 10, "fcf_conv": 120.0, "nd_ebit": 1.0, "capex_ocf": 10.0,
    "shares_cagr5": -1.0, "div_yield": 0.0, "pfcf_now": 20.0, "pfcf_med": 20.0,
    "fcf_last": 1e8, "cyclical": False, "cycle_pos": 50,
}


def one(**over):
    return C.score({"tickers": {"T": {**BASE, **over}}})[0]


# ── the cap ─────────────────────────────────────────────────────────
plain = one(div_yield=3.0)
check(plain["er_div"] == 3.0, f"an ordinary 3% yield passes through (got {plain['er_div']})")
check(plain["div_capped"] is False, "and is not flagged as capped")

hot = one(div_yield=23.15, pfcf_now=1.7)          # Shutterstock's real figures
check(hot["er_div"] == C.DIV_YIELD_CAP,
      f"a 23% yield is capped at {C.DIV_YIELD_CAP} (got {hot['er_div']})")
check(hot["div_yield_raw"] == 23.15, "the raw yield is still reported")
check(hot["div_capped"] is True, "and the row says it was capped")
check(any(b["key"] == "divcap" for b in hot["badges"]),
      "a badge explains it on the page rather than the number changing silently")
check(hot["expected"] < 35.4,
      f"E[r] falls from the uncapped 35.4 (got {hot['expected']})")

absurd = one(div_yield=388.35)                     # SDEV
# At 20x P/FCF this is 7,767% of free cash flow, so the COVERAGE test
# binds first and gives the 5% FCF yield — stricter than the 8% level cap.
# Whichever bites, the point is that 388 never reaches the total.
check(absurd["er_div"] <= C.DIV_YIELD_CAP,
      f"a 388% yield never exceeds the cap (got {absurd['er_div']})")
check(abs(absurd["er_div"] - 5.0) < 0.05,
      f"and here the tighter FCF-yield test is what binds (got {absurd['er_div']})")
check(absurd["expected"] < 20.0,
      f"E[r] lands in single/low double digits, not 400 (got {absurd['expected']})")

# ── coverage: a dividend larger than the cash flow behind it ────────
# payout% = yield x P/FCF. At 8% yield and 20x P/FCF that is 160% of FCF,
# so the most it can be worth is the FCF yield itself: 100/20 = 5%.
unfunded = one(div_yield=8.0, pfcf_now=20.0)
check(unfunded["div_payout_fcf"] == 160.0,
      f"payout is computed as yield x P/FCF (got {unfunded['div_payout_fcf']})")
check(abs(unfunded["er_div"] - 5.0) < 0.05,
      f"an unfunded dividend is capped at the FCF yield (got {unfunded['er_div']})")
check(unfunded["div_capped"] is True, "and is flagged")

funded = one(div_yield=4.0, pfcf_now=20.0)         # 80% of FCF
check(funded["div_payout_fcf"] == 80.0, "80% payout is measured")
check(funded["er_div"] == 4.0, "a covered dividend under the cap is untouched")
check(funded["div_capped"] is False, "and is not flagged")

# Both tests can bite; the tighter one must win.
both = one(div_yield=91.41, pfcf_now=10.0)         # VISN: 914% of FCF
check(both["er_div"] <= 10.0,
      f"the FCF-yield cap binds below the level cap (got {both['er_div']})")

# ── the cap must not rescue a company through other terms ───────────
check(one(div_yield=388.35)["expected"] < one(div_yield=8.0)["expected"] + 0.01,
      "capping cannot make a 388% yielder score ABOVE an 8% yielder")

# ── nothing else changed ────────────────────────────────────────────
check(one(div_yield=0.0)["er_div"] == 0.0, "no dividend is still no dividend")
check(one(div_yield=0.0)["div_payout_fcf"] is None,
      "payout is unknown, not zero, when there is no dividend")
no_price = one(div_yield=5.0, pfcf_now=None)
check(no_price["er_div"] == 5.0,
      "with no P/FCF the coverage test cannot run and the level cap alone applies")
check(no_price["div_payout_fcf"] is None, "and payout stays unknown")

# The other three terms keep their existing bounds.
check(one(rev_cagr5=40.0, rev_cagr10=40.0)["er_growth"] <= 14.0 * C.GROWTH_HAIRCUT + 1.5,
      "growth stays clamped")
check(one(shares_cagr5=-20.0)["er_buyback"] == 4.0, "buyback stays clamped at +4")
check(one(pfcf_now=5.0, pfcf_med=500.0)["er_mult"] == C.MULT_DRIFT_CAP,
      "valuation drift stays clamped at +3 (P/FCF inside the sane band, so "
      "the clamp is what binds — not the plausibility guard)")


# ── evidence length: shown, never hidden ────────────────────────────
# Input that genuinely clears the 14% bar: g 9.8 + b 1.0 + d 2.0 + m 3.0.
STRONG = dict(rev_cagr5=15.0, rev_cagr10=15.0, div_yield=2.0,
              pfcf_now=15.0, pfcf_med=25.0)

proven = one(years=16, **STRONG)
check(proven["expected"] >= C.TARGET,
      f"the fixture really does clear the bar (got {proven['expected']})")
check(proven["status"] == "COMPOUNDER",
      f"a 16-year record can be a COMPOUNDER (got {proven['status']})")
check(proven["proven"] is True, "and is marked proven")

young = one(years=8, **STRONG)
check(young["status"] == "QUALITY",
      f"the same numbers on 8 years is QUALITY, not COMPOUNDER (got {young['status']})")
check(young["proven"] is False, "and is marked unproven")
check(young["expected"] == proven["expected"],
      "the EXPECTED RETURN is identical — the record changes the label, not the maths")
check(any(b["key"] == "short" for b in young["badges"]),
      "a badge states how many years there are, so 8 and 14 are distinguishable")
check("8" in next(b["label"] for b in young["badges"] if b["key"] == "short"),
      "and the badge carries the actual count")
check(young["status"] != "WATCH",
      "a short record demotes one tier at most — it does not bury the company")

# 14 years is a data-availability fact, not a business one. It still
# cannot carry a 15-year label, but the badge shows 14 rather than 8 so
# the difference is legible.
fourteen = one(years=14, **STRONG)
check(fourteen["status"] == "QUALITY", "14 years is short of the claim")
check("14" in next(b["label"] for b in fourteen["badges"] if b["key"] == "short"),
      "and the row says 14, not merely 'short'")

# ── the valuation term is withheld when P/FCF cannot be right ───────
odd = one(pfcf_now=0.1, pfcf_med=23.9)          # NVMI's real figures
check(odd["er_mult"] == 0.0,
      f"a P/FCF of 0.1 contributes no valuation drift (got {odd['er_mult']})")
check(any(b["key"] == "pfcfodd" for b in odd["badges"]), "and says so on the row")
sane = one(pfcf_now=15.0, pfcf_med=25.0)
check(sane["er_mult"] > 0, "an ordinary P/FCF still drives the term normally")
check(not any(b["key"] == "pfcfodd" for b in sane["badges"]), "and is not flagged")
huge = one(pfcf_now=34400.0, pfcf_med=25.0)
check(huge["er_mult"] == 0.0, "the top of the range is guarded too")

# ── the fitted trend is what the gate and the term use ──────────────
by_trend = one(rev_cagr5=40.0, rev_cagr10=None, rev_trend=7.0)
check(abs(by_trend["growth_blend"] - 7.0) < 0.01,
      f"the fitted trend wins over a distorted endpoint CAGR (got {by_trend['growth_blend']})")
no_trend = one(rev_cagr5=10.0, rev_cagr10=8.0, rev_trend=None)
check(abs(no_trend["growth_blend"] - 9.0) < 0.01,
      "without a trend it falls back to blending the endpoint CAGRs")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} compounders scoring checks passed.")
print(f"   dividend capped at {C.DIV_YIELD_CAP}% and at the FCF yield when "
      f"payout exceeds {C.DIV_PAYOUT_MAX:.0f}%")
print(f"   Shutterstock: 23.15% raw -> {hot['er_div']}% used, E[r] {hot['expected']}")
sys.exit(0)
