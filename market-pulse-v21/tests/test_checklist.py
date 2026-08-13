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
import schloss as S

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
_ALL = (set(C.SCOREABLE_IDS) | set(C.GATE_IDS) | set(C.VETO_IDS)
        | set(C.NEVER_SCORED_IDS))
check(len(C.SCOREABLE_IDS) + len(C.GATE_IDS) + len(C.VETO_IDS)
      + len(C.NEVER_SCORED_IDS) == C.CRITERIA and _ALL == set(range(1, 14)),
      "every criterion is in exactly one of the four categories — scoreable, "
      "gate, veto, never — and no criterion can hide between them")
check(len(_ALL) == C.CRITERIA, "the four categories are disjoint")
check(13 in C.GATE_IDS and 13 not in C.SCOREABLE_IDS,
      "criterion 13 GATES and does not SCORE. It used to do both: 99 of the "
      "111 companies at the four-pass minimum were counting 'it is small' as "
      "one of their four passes, having cleared three quality gates rather "
      "than four. The board read 148 where the honest figure is 49")
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

# ── criterion 13 is a PRECONDITION, and the first run proved it ──
# NVIDIA was listed at $5.2 TRILLION because it cleared four other
# criteria while 13 sat marked Yikes — the right band, the wrong
# consequence. A hundred-bagger from there is $520tn, several times world
# GDP. The gate invents no threshold: >$10bn is where the checklist itself
# puts Yikes, and the source's prose argues for exactly this treatment.
check(C.MARKET_CAP_CEILING == C.THRESHOLDS[13][0],
      "the size ceiling IS the checklist's own Yikes boundary — promoted "
      "from a band to a precondition, not invented")
check(C.band_for(13, C.MARKET_CAP_CEILING + 1) == "Yikes",
      "and anything above it was already the worst band")


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
# ARISTA arrived with a P/E of 69,913,371 against $245.5bn of market cap —
# about $3,500 of net income — and printed a peer discount of -198,956,563%.
# lynch.price_earnings bounds the LOW end and never needed an upper one,
# because the Lynch screen gates at PE_MAX = 10 right afterwards. Nothing
# gates it here.
anet = C.peer_discount(69913371.44, 35.14, 900)
check(not anet.measured and "denominator" in anet.reason,
      f"a P/E of 69.9 million is a near-zero denominator, not a valuation "
      f"(got {anet.value})")
check(C.peer_discount(200.0, 30.0, 40).measured,
      "but 200x is a real multiple and stays — the ceiling is at 500x, not "
      "at whatever looks expensive")
check(C.peer_discount(200.0, 30.0, 40).band == "Yikes",
      "and lands in Yikes, which is a finding about the price rather than "
      "about the parser")

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
check(full["measured_n"] == 6 and full["passing_n"] == 6,
      "SIX of thirteen is the ceiling now: three can never be scored, three "
      "can only veto, and the gate does not vote")
check(full["gate_bands"] == {13: "Great"} and 13 not in full["bands"],
      "the gate's band is still SHOWN — it is a real measurement and belongs "
      "in the grid — it simply does not count toward the headline")
check(full["criteria_total"] == 13, "and the denominator stays thirteen")
check("of 13" in full["headline"], "which the headline states outright")

thin = led(c1=C.Measure(30.0, True, "", "Great", ""))
check(thin["measured_n"] == 1 and thin["passing_n"] == 1,
      "a company measured on ONE criterion and passing it")
check(led(c13=C.Measure(0.4e9, True, "", "Great", ""))["measured_n"] == 0,
      "and being small, alone, is measured ZERO — it is the price of "
      "admission, not evidence of quality")
check(thin["headline"] != full["headline"],
      "must not read the same as one measured on seven and passing seven — "
      "'100%' would be identical for both, which is why there is no percentage")
check(len(thin["unmeasured_ids"]) == 5,
      "and its five unmeasured scoreable criteria are listed BY NUMBER")

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
check(mixed["great_n"] == 1 and mixed["passing_n"] == 1,
      "criterion 13's Great is NOT among them — the gate is shown and not "
      "counted, which is the whole point of splitting it out")
_gg = led(c1=C.Measure(30.0, True, "", "Great", ""),
          c2=C.Measure(55.0, True, "", "Good", ""))
check(_gg["great_n"] == 1 and _gg["passing_n"] == 2,
      "Great and Good both pass, and are counted separately")

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
rows = ([{"verdict": "list", "sic": "7372", "ledger": {"measured_ids": [1, 2, 11]}}] * 3
        + [{"verdict": "thin", "reason": "too few measured", "sic": "3674",
            "ledger": {"measured_ids": [1]}}] * 5
        + [{"verdict": "reject", "reason": "too_big", "sic": "2834",
            "ledger": {"measured_ids": []}}] * 7)
c = C.census(rows)
check(c["screened"] == c["listed"] + c["thin"] + c["rejected"],
      "screened = listed + thin + rejected, exactly — `thin` is its own "
      "bucket because folding it into `rejected` is how the Lynch funnel "
      "came to count 975 companies twice")
check(c["coverage"][1] == 8 and 13 not in c["coverage"],
      "coverage counts every row that reached the criterion — 3 listed plus "
      "5 thin — because the question is how much could be SEEN, not how much "
      "survived. And the gate is absent: it would report over a different "
      "population from every other row in the table")
check(c["concentration"]["effective_sectors"] == 1.0,
      "three listed names in one SIC is ONE effective sector — 148 names "
      "sounds diversified and the live board measures 6.7, with software "
      "alone at 36%")
check(set(c["never_scored"]) == {str(x) for x in C.NEVER_SCORED_IDS},
      "and the census carries the six reasons too, so the page cannot render "
      "a blank without one")


# ══════════════════════════════════════════════════════════════════
# THE VETOES — they can only subtract
# ══════════════════════════════════════════════════════════════════
# A count-of-passes rule is MONOTONE: growing the scoreable tuple can only
# ever add companies to the board. Measured against the live snapshot,
# folding 6, 7 and 8 in as scoreable criteria would have taken it from 148
# names to roughly 468 — the exact opposite of what the three criteria the
# checklist uses to say NO are for.
_Y = C.Measure(-5.0, True, C.VETO_REASONS[8], "Yikes", "")
_Q = C.Measure(12.0, True, "", "quiet", "")
_pass4 = dict(c1=C.Measure(30.0, True, "", "Great", ""),
              c2=C.Measure(75.0, True, "", "Great", ""),
              c3=C.Measure(25.0, True, "", "Great", ""),
              c4=C.Measure(30.0, True, "", "Great", ""))
check(led(**_pass4)["passing_n"] == 4 and not led(**_pass4)["vetoed"],
      "four passes and no veto")
check(led(**_pass4, c8=_Y)["vetoed"] and led(**_pass4, c8=_Y)["vetoed_by"] == [8],
      "a Yikes on a veto criterion removes the company and names which one")
check(led(**_pass4, c8=_Y)["passing_n"] == 4,
      "and does NOT change the pass count — the veto subtracts, it never "
      "votes, so it cannot promote anything either")
check(led(**_pass4, c8=_Q)["passing_n"] == 4 and not led(**_pass4, c8=_Q)["vetoed"],
      "a quiet veto changes nothing at all: 'nothing visible is wrong' is "
      "not evidence that anything is right, and there is no positive band")
check("Great" not in C.VETO_BANDS and "Good" not in C.VETO_BANDS,
      "there is NO positive band on a veto criterion, deliberately — no free "
      "source proves a moat, and offering Great would be the fabrication")

# TUPPERWARE. Every proxy the design review tested scored it Great,
# unanimously, until it filed Chapter 11: margin held BECAUSE volume was
# being given up.
_YR = [f"{2016 + i}-12-31" for i in range(9)]
_tup_r = dict(zip(_YR, [2300, 2200, 2000, 1800, 1740, 1600, 1300, 1150, 1050]))
_tup_m = dict(zip(_YR, [68, 68, 67, 68, 69, 68, 67, 66, 67]))
check(C.moat_veto(_tup_r, _tup_m).band == "Yikes",
      "a harvest is caught: revenue falling while margin holds")
check(C.moat_veto(dict(zip(_YR, [round(100 * 1.18 ** i, 1) for i in range(9)])),
                  dict(zip(_YR, [60, 61, 60, 62, 61, 62, 61, 62, 62]))).band == "quiet",
      "a compounder with the SAME margin is not")
check(C.moat_veto(_tup_r, dict(zip(_YR, [68, 64, 60, 55, 50, 45, 40, 36, 32]))).band
      == "quiet",
      "and revenue falling WITH margin is an ordinary decline, not a harvest "
      "— the pair is the signal, neither leg alone")
check(not C.moat_veto({"2024-12-31": 1.0}, {"2024-12-31": 50.0}).measured,
      "too few years is unmeasured, never quiet")

# INTEL FY2015-19, which every capital-allocation proxy put in the TOP band.
# Its LEVEL of return was healthy the whole time.
_iop = dict(zip(_YR, [13.1e9, 12.9e9, 17.9e9, 23.3e9, 22.0e9, 23.7e9, 19.5e9, 2.2e9, -11.7e9]))
_icap = dict(zip(_YR, [60e9, 70e9, 80e9, 95e9, 110e9, 130e9, 150e9, 175e9, 190e9]))
check(C.capital_veto(_iop, _icap).band == "Yikes",
      "capital ballooning while operating income falls is caught by the "
      "INCREMENTAL return, which is the only measure that could see it")
check(C.capital_veto(dict(zip(_YR, [10e9 * 1.27 ** i for i in range(9)])),
                     dict(zip(_YR, [50e9 * 1.16 ** i for i in range(9)]))).band == "quiet",
      "a compounder reinvesting well is quiet")
check(C.capital_veto(_iop, {y: 60e9 for y in _YR}).band == "quiet",
      "and a company that invested nothing cannot be judged on how well it "
      "invested — that is quiet, not a veto")

# TWO DIFFERENT THINGS LAND IN THAT EXEMPTION and were told one sentence.
_flat = C.capital_veto(_iop, dict(zip(_YR, [60e9 + i * 1e8 for i in range(9)])))
_gone = C.capital_veto(_iop, dict(zip(_YR, [60e9 - i * 3e9 for i in range(9)])))
check(_flat.band == "quiet" and "too little invested" in _flat.basis
      and "FELL" not in _flat.basis,
      "a company that barely moved its capital is told it invested too "
      "little to judge, which is what happened to it")
check(_gone.band == "quiet" and "FELL" in _gone.basis
      and "too little" not in _gone.basis,
      "and a company that SHRANK its capital base by a third is told that "
      "instead — 376 companies on the live board did the second thing and "
      "read the first sentence, and 'too little' describes smallness, not "
      "a business returning or writing off half of what it employed")
check(_gone.value is None and _gone.measured,
      "both stay quiet with no value: incremental return on a capital base "
      "that is leaving is undefined, not bad")

# CRITERION 7, and the hole no budget can fill.
check(C.ownership_veto(2.0).band == "Yikes" and C.ownership_veto(18.0).band == "quiet",
      f"under {C.INSIDER_MIN_PCT:.0f}% is no skin in the game")
check(C.INSIDER_MIN_PCT == S.INSIDER_ALIGNED_MIN * 100,
      "and the floor is schloss's own constant, imported not retyped")
_fpi = C.ownership_veto(None, "foreign private issuer — files 20-F, exempt "
                              "from Regulation 14A; no proxy exists")
check(not _fpi.measured and "20-F" in _fpi.reason,
      "a foreign private issuer files NO proxy statement, so its ownership is "
      "unmeasured — never zero. Reading an absent filing obligation as "
      "absent ownership is `country or \"United States\"` in a new hat")

# THE DEF 14A PARSER
check(C.parse_group_ownership(
      "All executive officers and directors as a group (12 persons) 1,234,567 24.3%")[0]
      == 24.3, "the group row's percent is the company's own number")
check(C.parse_group_ownership(
      "All of our current directors and executive officers as a group 4,010 3.1%")[0]
      == 3.1, "and the wording varies by filing agent")
check(C.parse_group_ownership(
      "All directors and officers as a group (5 persons) 120,000 less than 1%")[0] == 0.5,
      "'less than 1%' is a real measurement — and must not read as 1.0%, "
      "which is what matching the percent regex first produces")
check(C.parse_group_ownership(
      "All executive officers and directors as a group 1,000 —")[0] is None,
      "an em-dash is NOT zero — a parse failure returning 0.0 would band as "
      "the worst score for a founder who may own 45%")
check(C.parse_group_ownership("<p>a different table</p>")[0] is None
      and C.parse_group_ownership("")[0] is None,
      "and no table at all is unmeasured, with a reason")

# ── the shapes a REAL proxy has, which the fixtures above never had ──
# Every check below returned a wrong number before the walk-all-matches fix,
# and every wrong number landed under INSIDER_MIN_PCT and vetoed a company
# off the board. Nine of the sixteen figures on the live August run were
# exactly 5.00%.
_INTRO = (
    "<p>The following table sets forth information regarding beneficial "
    "ownership of our common stock by each person known to us to be the "
    "beneficial owner of more than 5% of our common stock, by each of our "
    "directors, by each named executive officer, and by all executive "
    "officers and directors as a group.</p>"
    "<p>Beneficial ownership is determined in accordance with the rules of "
    "the Securities and Exchange Commission. A person is deemed to be the "
    "beneficial owner of securities that can be acquired within 60 days.</p>")

check(C.parse_group_ownership(_INTRO)[0] is None,
      "the section's INTRO paragraph contains the group phrase too, and the "
      "SEC boilerplate after it opens with the 5% reporting threshold — "
      "reading that as a holding is where nine identical 5.00%s came from")

check(C.parse_group_ownership(
      _INTRO + "<table><tr><td>All executive officers and directors as a "
      "group (9 persons)</td><td>2,145,880</td><td>23.4%</td></tr></table>")[0]
      == 23.4,
      "and with the real table present, the intro must be walked past to "
      "reach the row that actually carries the number")

check(C.parse_group_ownership(
      "All executive officers and directors as a group (7 persons) held "
      "shares representing more than 5% of the outstanding common stock")[0] is None,
      "a percent introduced by a comparative is a THRESHOLD, not a holding — "
      "a table cell never says 'more than 24.3%'")

check(C.parse_group_ownership(
      "All executive officers and directors as a group (4 persons) 900,100 "
      + "x" * 200 + " 12.5%")[0] is None,
      "and a percent 200 characters downstream belongs to whatever came "
      "next, not to this row — unmeasured beats a confident wrong number")

_pct, _why = C.parse_group_ownership(
    "and by all executive officers and directors as a group. Each holder of "
    "more than 5% of our common stock is listed above.")
check(_pct is None and "threshold" in _why,
      "when the boilerplate percent IS close enough to look like a cell, the "
      "comparative in front of it is what gives it away — and the refusal "
      "names that, so the page can say why the cell is empty")

_pct, _why = C.parse_group_ownership(_INTRO)
check(_pct is None and _why and "5" not in _why,
      "and a refusal never carries the number it refused — a reason string "
      "reading '5%' is one copy-paste from being shown as the value")

# ── THE ASTERISK, WHICH MEANS TWO THINGS IN THE SAME TABLE ──
# Fixing the intro-paragraph bug moved the scan onto the real row, where a
# footnote marker was waiting. Nine companies then reported exactly 0.50% —
# this module's own "less than 1%" sentinel — which was 45% of every value
# the criterion produced, and eight of the ten names that left the board
# that run were removed by this criterion.
check(C.parse_group_ownership(
      "All executive officers and directors as a group (12 persons)* "
      "4,512,880 18.7%")[0] == 18.7,
      "an asterisk WELDED to a token is a footnote marker, and reading it "
      "as the percent cell throws away the real number four words later")
check(C.parse_group_ownership(
      "All executive officers and directors as a group (9 persons) (*) "
      "2,145,880 23.4%")[0] == 23.4,
      "including when the filing agent wraps the marker in parentheses")
check(C.parse_group_ownership(
      "All executive officers and directors as a group (6 persons) "
      "1,234,567* 14.2%")[0] == 14.2,
      "and when it hangs off the share count instead of the person count")
check(C.parse_group_ownership(
      "All directors and officers as a group (5 persons) 120,000 *")[0] == 0.5,
      "but a LONE asterisk with whitespace either side is the percent cell "
      "itself — the standard legend for under one percent, and a real "
      "measurement this must keep reading")
check(C.parse_group_ownership(
      "All executive officers and directors as a group (7 persons) 1,200,000 "
      "11.4% * Includes shares subject to options.")[0] == 11.4,
      "and a footnote that follows a real percent does not overwrite it")

# ── THE DETECTOR, so the next one announces itself ──
_rows = [{"measures": {"7": {"measured": True, "value": 0.5}}} for _ in range(9)]
_rows += [{"measures": {"7": {"measured": True, "value": v}}}
          for v in (2.9, 8.86, 1.08, 3.8, 5.9, 1.49, 1.18, 99.99, 2.0, 5.0, 5.0)]
_cl = C.value_clustering(_rows)
check(_cl[7]["modal"] == 0.5 and _cl[7]["modal_n"] == 9 and _cl[7]["flagged"],
      "nine values on one number out of twenty is flagged — this is the "
      "shape the last four fabrications all had, and every one of them was "
      "caught by a human squinting at a board instead of by the code")
check(C.value_clustering(
      [{"measures": {"5": {"measured": True, "value": 100.0}}} for _ in range(53)]
      )[5]["modal"] == 100.0,
      "and the modal VALUE comes back beside the share, because a capped "
      "criterion piles up on its bound honestly — 100.0 where the cap is "
      "100 is a bound, 0.50 where there is no cap at all is a sentinel")
check(not C.value_clustering(
      [{"measures": {"1": {"measured": True, "value": float(i)}}}
       for i in range(40)])[1]["flagged"],
      "a real spread is not flagged")
check(C.value_clustering([]) == {} and C.value_clustering(
      [{"measures": {"1": {"measured": False, "value": None}}}]) == {},
      "and nothing measured yields nothing to report, rather than a "
      "zero that looks like a finding")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} checklist checks passed.")
print(f"   {len(C.SCOREABLE_IDS)} scoreable · {len(C.GATE_IDS)} gate "
      f"· {len(C.VETO_IDS)} veto-only · {len(C.NEVER_SCORED_IDS)} never "
      f"= {C.CRITERIA}")
print(f"   windows up to {C.MAX_SPANS} spans · peer median needs "
      f"{C.PEER_MIN} peers · no division, no points, no total")
sys.exit(0)
