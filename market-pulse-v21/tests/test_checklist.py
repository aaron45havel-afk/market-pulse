"""The 100-bagger checklist — proved against the cases that broke its proxies.

Run:  python tests/test_checklist.py      (exit 0 = all pass)

This module's whole reason for existing is that SIX of the thirteen
criteria cannot be scored, so most of what is asserted below is about what
the code REFUSES to say. Each of these came from an adversarial review that
was asked to name a real company where a proposed proxy gives the wrong
answer, and found one every time:

    criterion 6   Intel FY2015-19 scored the TOP band on every capital
                  allocation proxy tested
    criterion 8   Tupperware scored "Great" on all three moat proxies,
                  unanimously, until it filed Chapter 11
    criterion 5   Papa John's scored "Great" on an equity-denominator ROIC
                  while the honest figure is two bands lower — years of
                  buybacks had left the equity base near zero
    criterion 7   Super Micro's $1 CEO salary reads as perfect alignment on
                  every pay-based proxy

The arithmetic tests use Carnival's revenue shape, which is the case that
motivated the drawdown guard: 29.5%/yr over three spans, 36.4% over five,
5.5% over nine — one company, one filing history, three answers.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import checklist as C
import lynch as L

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# THE DENOMINATOR IS A CONSTANT
# ══════════════════════════════════════════════════════════════════
check(C.CRITERIA == 13, "thirteen is a literal, in one place")
check(len(C.SCOREABLE_IDS) + len(C.NEVER_SCORED_IDS) == C.CRITERIA,
      "every criterion is either scoreable or explicitly never scoreable — "
      "no third category can hide a criterion nobody assigned")
check(not set(C.SCOREABLE_IDS) & set(C.NEVER_SCORED_IDS),
      "and the two sets are disjoint")
check(all(c in C.NOT_SCORED_BECAUSE for c in C.NEVER_SCORED_IDS),
      "every unscoreable criterion carries the REASON it cannot be scored, so "
      "a blank cell reads as a limit of the data rather than of the company")
check(set(C.CRITERION_NAMES) == set(range(1, 14)), "all thirteen are named")

# The bug this module is built to avoid, asserted as source text.
_src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "checklist.py"), encoding="utf-8").read()
for banned in ("/ len(", "/ measured_n", "sum(BAND_ORDINAL", "BAND_POINTS",
               'or "Meh"', "band or "):
    check(banned not in _src,
          f"no {banned!r} anywhere — a score divided by 'the criteria we "
          f"happened to get' ranks companies by how little is known about them")


# ══════════════════════════════════════════════════════════════════
# BANDS: no neutral, no default
# ══════════════════════════════════════════════════════════════════
check(C.band_for(1, 30.0) == "Great" and C.band_for(1, 20.0) == "Good",
      "sales growth bands run the checklist's way")
check(C.band_for(1, 12.0) == "Meh" and C.band_for(1, 4.0) == "Yikes", "and down")
check(C.band_for(1, None) is None,
      "an unmeasured value has NO band — not Meh, not a middle, not anything")
check(C.band_for(13, 0.5e9) == "Great" and C.band_for(13, 20e9) == "Yikes",
      "market cap is INVERTED: small is the good end, which is the whole "
      "reason a hundred-bagger has to start small")
check(C.band_for(13, 4e9) == "Good" and C.band_for(13, 7e9) == "Meh",
      "and its middle bands invert too")
check(C.band_for(99, 10.0) is None, "a criterion with no thresholds has no band")


# ══════════════════════════════════════════════════════════════════
# CRITERION 1 — Carnival, the case the drawdown guard exists for
# ══════════════════════════════════════════════════════════════════
CCL = {f"{2015 + i}-11-30": v for i, v in enumerate(
    [16.4, 17.5, 18.9, 20.8, 5.6, 1.9, 12.2, 21.6, 25.0, 26.5])}
m = C.sales_growth(CCL)
check(m.measured and abs(m.value - 5.5) < 0.2,
      f"ten points on file means a nine-span window and the honest 5.5%/yr "
      f"(got {m.value})")
check("9 spans" in m.basis, "and the row says how many spans it used")

check(m.value < 10.0,
      "the collapse and the recovery NET OUT over the full history — 16.4 to "
      "26.5 really is 5.5%/yr, and this answer has to survive every guard")

# Truncate to six points and the whole V now sits INSIDE the window, so
# there is no earlier peak to compare against and the base is no longer the
# low point. Every guard that interrogates the base against the window
# median is silent here, because the recovered years dragged that median up.
short = {k: v for k, v in sorted(CCL.items())[-6:]}
m2 = C.sales_growth(short)
check(not m2.measured and "drawdown" in m2.reason,
      f"the window OPENS with the collapse, so the base is the last normal "
      f"year rather than a starting level (got {m2.value}, {m2.reason!r})")
check(L.growth(short, spans=5, cap=60.0)["cagr"] is not None,
      "and without the guard that same series DOES produce a rate — the "
      "guard is load-bearing, not decorative")
shorter = {k: v for k, v in sorted(CCL.items())[-5:]}
check(not C.sales_growth(shorter).measured,
      "and with the base actually in the hole the existing trough test "
      "catches it, so the two guards cover different shapes")

# THE GUARD MUST NOT FIRE ON A GENUINE DECLINE. A company shrinking 5%/yr
# is a Yikes, which is a finding about the company; withholding would hide it.
decline = {f"{2015 + i}-12-31": v for i, v in
           enumerate([200, 190, 180, 170, 160, 150])}
md = C.sales_growth(decline)
check(md.measured and md.value < 0 and md.band == "Yikes",
      f"a steady decline is RATED and lands in Yikes (got {md.value}) — a bad "
      f"number is a finding, only an unreliable one is withheld")

# The guard must not touch honest compounders.
clean = {f"{2015 + i}-12-31": round(100 * 1.18 ** i, 1) for i in range(10)}
mc = C.sales_growth(clean)
check(mc.measured and abs(mc.value - 18.0) < 0.2,
      f"a steady 18%/yr compounder is untouched (got {mc.value})")
dip = {f"{2015 + i}-12-31": v for i, v in enumerate(
    [100, 118, 139, 164, 146, 193, 228, 269, 317, 374])}
check(C.sales_growth(dip).measured,
      "and so is a compounder carrying one real down year")

check(not C.sales_growth({}).measured, "no revenue is unmeasured")
check(not C.sales_growth({"2023-12-31": 1.0, "2024-12-31": 2.0}).measured,
      "and two points is too short to rate — which is a fact about the "
      "filing history, not a bad company")


# ══════════════════════════════════════════════════════════════════
# CRITERION 2 — the two tags must agree
# ══════════════════════════════════════════════════════════════════
check(C.gross_margin(1000.0, gross_profit=600.0).value == 60.0,
      "the filer's own subtotal is preferred")
check(C.gross_margin(1000.0, cost_of_revenue=350.0).value == 65.0,
      "and revenue less cost stands in when it is not filed")
agree = C.gross_margin(1000.0, gross_profit=600.0, cost_of_revenue=405.0)
check(agree.measured and agree.value == 60.0,
      "a small difference reconciles and the audited subtotal wins")
clash = C.gross_margin(1000.0, gross_profit=600.0, cost_of_revenue=250.0)
check(not clash.measured,
      "but a 15%-of-revenue disagreement means the two tags are not the same "
      "subtotal, and picking one would be choosing which number to believe")
check(not C.gross_margin(1000.0).measured,
      "a company with no cost concept at all is UNMEASURED — not zero-margin "
      "and not 100%-margin, both of which the arithmetic would happily print")
check(not C.gross_margin(0.0, gross_profit=5.0).measured,
      "and zero revenue does not become an infinite margin")


# ══════════════════════════════════════════════════════════════════
# CRITERION 4 — a median, and the equity trend beside it
# ══════════════════════════════════════════════════════════════════
ni = {f"{2019 + i}-12-31": v for i, v in enumerate([100, 110, 120, 130, 140])}
eq = {f"{2019 + i}-12-31": 600 for i in range(5)}
r = C.return_on_equity(ni, eq)
check(r.measured and abs(r.value - 20.0) < 0.1, f"steady 20% ROE (got {r.value})")
check("equity grew or held" in r.basis, "and the equity trend is stated")

shrink = C.return_on_equity(ni, {f"{2019 + i}-12-31": 600 - 100 * i for i in range(5)})
check("equity shrank" in shrink.basis,
      "a high ROE off a SHRINKING base is flagged — the source names this "
      "trap itself, and the number alone cannot tell buybacks from returns")
check(not C.return_on_equity(ni, {y: -50 for y in ni}).measured,
      "negative equity every year yields no ROE rather than a negative one")
check(C.return_on_equity({"2024-12-31": 900}, {"2024-12-31": 100}).value == 100.0,
      "and the ratio is bounded, because near-zero equity stops describing "
      "a business")


# ══════════════════════════════════════════════════════════════════
# CRITERION 5 — why Greenblatt, not the book's own definition
# ══════════════════════════════════════════════════════════════════
heavy = C.return_on_capital(300e6, 900e6, 400e6, 4.5e9)
light = C.return_on_capital(300e6, 500e6, 400e6, 120e6)
check(heavy.band == "Meh" and light.band == "Great",
      "identical operating income, opposite capital efficiency, opposite bands")
check(not C.return_on_capital(300e6, 400e6, 900e6, 200e6).measured,
      "a non-positive denominator reports nothing — the equity-denominator "
      "version scored Papa John's in the TOP band for exactly this reason, "
      "its equity base having been bought back to near zero")
check(not C.return_on_capital(300e6, 500e6, None, 120e6).measured,
      "and an unfiled input would SHRINK the denominator and print a better "
      "score, so it withholds instead")


# ══════════════════════════════════════════════════════════════════
# CRITERION 11 — a median needs peers
# ══════════════════════════════════════════════════════════════════
d = C.peer_discount(8.0, 16.0, 40)
check(d.measured and abs(d.value - 50.0) < 1e-9 and d.band == "Great",
      "half the peer median is a 50% discount")
check(not C.peer_discount(8.0, 16.0, 5).measured,
      f"five peers is not a peer group — a company in a thin SIC is not "
      f"thereby cheap (needs {C.PEER_MIN})")
check(not C.peer_discount(None, 16.0, 40).measured, "no P/E, no discount")
check(not C.peer_discount(8.0, -2.0, 40).measured,
      "and a non-positive peer median cannot produce a percentage")


# ══════════════════════════════════════════════════════════════════
# THE LEDGER — where the honesty is enforced
# ══════════════════════════════════════════════════════════════════
def led(**kw):
    return C.ledger({int(k[1:]): v for k, v in kw.items()})

full = led(c1=C.Measure(30.0, True, "", "Great", ""),
           c2=C.Measure(75.0, True, "", "Great", ""),
           c3=C.Measure(25.0, True, "", "Great", ""),
           c4=C.Measure(30.0, True, "", "Great", ""),
           c5=C.Measure(30.0, True, "", "Great", ""),
           c11=C.Measure(60.0, True, "", "Great", ""),
           c13=C.Measure(0.4e9, True, "", "Great", ""))
check(full["measured_n"] == 7 and full["passing_n"] == 7,
      "seven of thirteen is the CEILING — six criteria can never be scored, "
      "so no company can ever reach 13 and the page must not imply one could")
check(full["criteria_total"] == 13, "and the denominator stays thirteen")
check("of 13" in full["headline"], "which the headline states outright")

thin = led(c13=C.Measure(0.4e9, True, "", "Great", ""))
check(thin["measured_n"] == 1 and thin["passing_n"] == 1,
      "a company measured on ONE criterion and passing it")
check(thin["headline"] != full["headline"],
      "must not read the same as one measured on seven and passing seven — "
      "'100%' would be identical for both, which is why there is no percentage")
check(len(thin["unmeasured_ids"]) == 6,
      "and its six unmeasured scoreable criteria are listed BY NUMBER")

none = led()
check(none["measured_n"] == 0 and none["floor_band"] is None,
      "nothing measured yields no floor band rather than a Yikes")
check("nothing measured" in none["headline"], "and says so")

mixed = led(c1=C.Measure(30.0, True, "", "Great", ""),
            c2=C.Measure(10.0, True, "", "Yikes", ""),
            c13=C.Measure(0.4e9, True, "", "Great", ""))
check(mixed["floor_band"] == "Yikes" and mixed["floor_criterion"] == 2,
      "the floor names the criterion that owns it — on this repo's own data "
      "one criterion owns 60-84% of all floors, which is why the page does "
      "not rank on the floor")
check(mixed["great_n"] == 2 and mixed["passing_n"] == 2,
      "Great and Good both pass; they are counted separately as well")

unm = led(c1=C.unmeasured("revenue not filed"))
check(unm["measured_n"] == 0 and 1 in unm["unmeasured_ids"],
      "an unmeasured Measure counts as unmeasured, never as a band")
check(set(full["never_scored_ids"]) == set(C.NEVER_SCORED_IDS),
      "and every ledger carries the six that can never be scored, so no "
      "caller can shrink the denominator by forgetting them")


# ══════════════════════════════════════════════════════════════════
# CHURN — the finding that motivated shipping it in version one
# ══════════════════════════════════════════════════════════════════
# The Lynch board's real history: 10 names in June, 11 in July, 4 in
# August, and only lululemon survived July into August.
jul = [{"ticker": t} for t in
       ("XNET VIPS ANF BZ CALM ATAT PDD ASO LDOS FISV LULU".split())]
aug = [{"ticker": t} for t in ("BOSC LRN LULU INGR".split())]
ch = C.churn(aug, jul)
check(ch["held"] == ["LULU"] and ch["held_n"] == 1,
      "one of eleven survived a single month on the sister board — a page "
      "whose thesis is a fifteen-year hold owes the reader this number "
      "before it owes them a ranking")
check(ch["left_n"] == 10 and ch["entered_n"] == 3, "and both directions")
check(C.churn(aug, None)["comparable"] is False,
      "a first run has nothing to compare against and says so rather than "
      "reporting zero churn, which would be the friendliest possible lie")
check(C.churn(aug, None)["held_n"] is None, "with no count at all")


# ══════════════════════════════════════════════════════════════════
# CENSUS — partitions, like the Lynch funnel now does
# ══════════════════════════════════════════════════════════════════
rows = ([{"verdict": "list", "ledger": {"measured_ids": [1, 2, 13]}}] * 3
        + [{"verdict": "thin", "reason": "too few measured",
            "ledger": {"measured_ids": [13]}}] * 5
        + [{"verdict": "reject", "reason": "too_big", "ledger": {"measured_ids": []}}] * 7)
c = C.census(rows)
check(c["screened"] == c["listed"] + c["thin"] + c["rejected"],
      "screened = listed + thin + rejected, exactly — `thin` is its own "
      "bucket because folding it into `rejected` is how the Lynch funnel "
      "came to count 975 companies twice")
check(c["coverage"][13] == 8 and c["coverage"][1] == 3,
      "coverage is counted over the WHOLE universe, not only the survivors — "
      "the question the page is built around is how much could be seen")
check(set(c["never_scored"]) == {str(x) for x in C.NEVER_SCORED_IDS},
      "and the census carries the six reasons too, so the page cannot render "
      "a blank without one")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} checklist checks passed.")
print(f"   {len(C.SCOREABLE_IDS)} of {C.CRITERIA} criteria scoreable; "
      f"{len(C.NEVER_SCORED_IDS)} never scoreable "
      f"({', '.join(str(c) for c in C.NEVER_SCORED_IDS)})")
print(f"   windows up to {C.MAX_SPANS} spans · peer median needs "
      f"{C.PEER_MIN} peers · no division, no points, no total")
sys.exit(0)
