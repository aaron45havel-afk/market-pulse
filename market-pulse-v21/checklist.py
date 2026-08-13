"""The 100-bagger checklist, scored only where it can honestly be scored.

Mayer's "100 Baggers" and Phelps's "100 to 1" reduce to two instructions —
buy right, hold on — and a thirteen-point checklist for the first half.
This module implements that checklist against SEC filings and a bulk quote
file, and its single most important output is not a score. It is the count
of how many of the thirteen were actually measured for each company.

WHY THAT IS THE HEADLINE. Six of the thirteen cannot be answered from any
free machine-readable source, and two more cannot be answered at all:

    6  sound capital allocation   no free source, and every quantitative
                                  proxy tested scored INTEL FY2015-19 in
                                  the top band
    7  incentives aligned         insider ownership lives in Forms 3/4/5,
                                  one request per company, which the
                                  monthly run has no budget for. Pay-based
                                  proxies read Super Micro's $1 CEO salary
                                  as perfect alignment
    8  long-term moat             every proxy scored TUPPERWARE "Great"
                                  unanimously, for the whole window before
                                  it filed Chapter 11
    9  analyst coverage           sell-side counts are licensed terminal
                                  data. Note this criterion is INVERTED —
                                  zero analysts is the best score — so its
                                  absence removes the thesis's own
                                  explanation of why a quality company
                                  would be cheap
   10  work culture               Glassdoor and Comparably are licensed;
                                  SEC human-capital disclosure is
                                  company-authored prose
   12  forward PEG                needs analyst estimates. Trailing PEG is
                                  a different quantity and substituting it
                                  silently would be the house bug

So the best any company can reach here is SEVEN of thirteen. A composite
that divided by "the criteria we happened to get" would rank companies by
how little was known about them — which is the failure this codebase keeps
paying for, promoted to a ranking function. There is therefore NO DIVISION
ANYWHERE in this module, no points, and no total.

WHAT THIS PAGE IS: a reading list. It says "these companies clear the
seven gates that filings can answer, and here are the six you must answer
yourself by reading a proxy statement." It does not rank them, because a
measured ranking over an unmeasured majority is a horoscope with tabular
numerals.

TWO STATEMENTS THE PAGE MUST CARRY, both established by measurement
against this repo's own snapshots rather than argued:

  * SURVIVORSHIP IS IN THE UNIVERSE, not just the analysis. The universe
    comes from company_tickers.json, and a delisted company has no ticker
    and is not in the file. Every backward window here is conditioned on
    the company still being listed today. The exit rate is not merely
    unmeasured, it is unmeasurable from free bulk sources, and no guard in
    this module can fix it — only saying so can.

  * THE BOARD CHURNS FASTER THAN THE THESIS. The Lynch snapshots passed 10
    companies in June, 11 in July and 4 in August, and ONLY LULULEMON
    survived July into August. Most of that turnover was a rules change,
    not a market. A screen whose premise is a fifteen-year hold owes the
    reader a churn count before it owes them a ranking, so `churn()` and
    `months_on_board` ship in the first version rather than a later one.
"""
from __future__ import annotations

import json
import re
from collections import namedtuple
from datetime import date
from pathlib import Path

import lynch as L
import schloss as S

_DATA_DIR = Path(__file__).resolve().parent / "data" / "hundred_snapshots"

# ── The checklist, as a frozen fact about the source material ────────
#
# THIRTEEN IS A LITERAL, in one place, forever. It is the denominator the
# reader is owed and it must never be computed from "how many we managed",
# which would make the fraction shrink to fit the evidence.
CRITERIA = 13

# Scoreable from a companyfacts blob plus one bulk quote file, and
# COUNTED toward the "k of m" headline.
SCOREABLE_IDS = (1, 2, 3, 4, 5, 11)

# MEASURED AND SHOWN, BUT NOT COUNTED. Criterion 13 was in SCOREABLE_IDS
# and was also the size gate, so it decided membership twice: 99 of the 111
# companies sitting at exactly the four-pass minimum counted "it is small"
# as one of their four qualifying passes. They cleared THREE quality gates,
# not four, and the board read 148 names where the honest figure is 49.
#
# A criterion cannot both admit a company and be evidence that it deserved
# admission. Size still gates, still renders its band in the grid, and no
# longer votes.
GATE_IDS = (13,)

# CAN ONLY SAY NO. These three are the checklist's veto criteria — moat,
# capital allocation, incentives — and no free source can establish any of
# them POSITIVELY. What free filings can sometimes establish is the
# negative: revenue falling while margin holds is a harvest; capital going
# in while incremental returns fall is destruction; a proxy showing
# management owns almost nothing is not alignment.
#
# THE COUNT-OF-PASSES RULE COULD NOT EXPRESS THAT. `passing_n >= 4` over a
# growing tuple is monotone — adding criteria can only ever ADD companies
# to the board, never remove one. Measured against the live snapshot,
# folding 6, 7 and 8 into SCOREABLE_IDS would have taken it from 148 names
# to roughly 468, which is the exact opposite of what those three
# criteria are for. So they veto instead of voting: a Yikes here removes a
# company, and anything else changes nothing.
VETO_IDS = (6, 7, 8)

# Never scoreable, for the reasons in the module docstring. These are not
# "missing data" — they are permanently outside what this page can see,
# and they are rendered differently from a company that simply did not
# file a tag.
# Never scoreable in either direction, positive or negative.
NEVER_SCORED_IDS = (9, 10, 12)

CRITERION_NAMES = {
    1: "Sales growth CAGR", 2: "Gross margin", 3: "EPS growth",
    4: "Return on equity", 5: "Return on capital", 6: "Sound capital allocation",
    7: "Incentives aligned", 8: "Long-term moat", 9: "Analyst coverage",
    10: "Work culture", 11: "Discount to peer P/E", 12: "Forward PEG",
    13: "Market capitalisation",
}

# Why each unscoreable criterion is unscoreable, shown on the row so the
# blank is legible as a limit of the data rather than of the company.
NOT_SCORED_BECAUSE = {
    9: "sell-side counts are licensed terminal data — and this criterion is "
       "INVERTED, so its absence removes the thesis's own reason a quality "
       "company would be cheap",
    10: "employee sentiment is licensed; SEC human-capital disclosure is prose",
    12: "needs analyst estimates; trailing PEG is a different quantity",
}

# What a veto criterion can say. There is no positive band, deliberately:
# no free source proves a moat or proves good capital allocation, and
# offering "Great" here would be the fabrication this whole module exists
# to prevent. The strongest available statement is "nothing visible is
# wrong", which is not evidence of anything.
VETO_BANDS = ("Yikes", "quiet")
VETO_REASONS = {
    6: "capital going in while incremental returns fall",
    7: "management owns almost none of the company",
    8: "margin held while revenue fell — a harvest, not a moat",
}

BANDS = ("Yikes", "Meh", "Good", "Great")
PASS_BANDS = ("Good", "Great")

# Comparable, never summable. Adding an ordinal asserts that the step from
# Yikes to Meh is the same size as Good to Great, which the source material
# does not claim and the arithmetic cannot support.
BAND_ORDINAL = {b: i for i, b in enumerate(BANDS)}

# ── Thresholds, straight off the checklist ───────────────────────────
# (lower, upper) cut points between the four bands, ascending = better,
# except where `higher_is_worse` inverts it.
THRESHOLDS = {
    1: (10.0, 15.0, 25.0),          # sales growth %/yr
    2: (20.0, 50.0, 70.0),          # gross margin %
    3: (5.0, 10.0, 20.0),           # EPS growth %/yr
    4: (5.0, 15.0, 25.0),           # ROE %
    5: (5.0, 15.0, 25.0),           # return on capital %
    11: (15.0, 30.0, 50.0),         # discount to peer median P/E, %
    13: (10e9, 5e9, 1e9),           # market cap $ — DESCENDING, smaller is better
}
DESCENDING = (13,)

# ── Windows ──────────────────────────────────────────────────────────
#
# TEN YEARS IS THE KNEE, and the choice is not neutral. The SEC's XBRL
# phase-in only reached smaller reporting companies for periods ending
# after 15 June 2011, so fifteen years of tagged revenue is structurally
# impossible for most of the sub-$250m tier — which is precisely the tier
# the thesis points at, Mayer's median company being about $500m. A short
# window is systematically biased HIGH for young companies, so a fixed
# five-year window would manufacture "Great" exactly where the evidence is
# thinnest. Take the longest available up to ten, and print how many were
# used on every row.
MAX_SPANS = 10
MIN_SPANS_REVENUE = 4
MIN_SPANS_EPS = L.MIN_EPS_YEARS          # 3, matching the Lynch board

# Display bound only. It sits far above the top band (25%), so it can
# never move a company between bands — unlike the EPS cap, which can.
REV_GROWTH_CAP = 60.0

# A peer median needs enough peers to be a median. With bands 15 points
# wide and the standard error of a median running about 0.75/sqrt(n) of a
# spread, fewer than this and the "discount" is noise with a decimal point.
PEER_MIN = 25

# GrossProfit and Revenue - Cost disagreeing by more than this share of
# revenue means the two tags are not describing the same subtotal.
GM_RECONCILE_FRAC = 0.02

# Anchored on schloss.INSIDER_ALIGNED_MIN (0.10), imported rather than
# retyped so the two boards cannot drift apart on what alignment means.
INSIDER_MIN_PCT = S.INSIDER_ALIGNED_MIN * 100

# A P/E ABOVE THIS IS A NEAR-ZERO DENOMINATOR, not a valuation.
# lynch.price_earnings bounds the LOW end at PE_SANE[0] — below 3 is a
# currency or share-basis artefact — but never needed an upper bound,
# because the Lynch screen gates at PE_MAX = 10 immediately afterwards.
# Nothing gates it here, and Arista arrived with a P/E of 69,913,371
# against $245.5bn of market cap, which is about $3,500 of net income.
# It printed a peer discount of -198,956,563%.
#
# Real companies do trade at 200x. Above 500x the ratio has stopped
# describing the business, which is the same failure as below 3x and gets
# the same treatment: withheld, not clamped to a pretty number.
PE_PLAUSIBLE_MAX = 500.0

# ── Criterion 13 is a PRECONDITION, not a quality ────────────────────
#
# The other twelve describe how good a business is. This one describes
# whether a hundred-bagger is arithmetically available from here, and the
# source material is explicit about it: "Is Microsoft a great company?
# Yes. Can it become a 100-bagger from now on? Probably not."
#
# The first run listed NVIDIA at $5.2 TRILLION. A hundred-bagger from
# there is $520tn, several times world GDP. It was listed because it
# cleared four of the other criteria while criterion 13 sat there marked
# Yikes — which is the correct band and the wrong consequence.
#
# So the checklist's own worst band becomes a gate. This invents no
# threshold: >$10bn is where the source puts Yikes, and its prose argues
# for exactly this treatment.
MARKET_CAP_CEILING = 10e9


# A measurement, or an explicit absence of one. `value` may not be read
# unless `measured` is True — every consumer in this module goes through
# band() or the ledger, both of which check.
Measure = namedtuple("Measure", "value measured reason band basis")


def unmeasured(reason: str, basis: str = "") -> Measure:
    return Measure(None, False, reason, None, basis)


def _num(v):
    return L._num(v)


def band_for(cid: int, value: float | None) -> str | None:
    """Which of the four bands a value falls in. None when unmeasured.

    No default band. There is no such thing as a neutral score here: a
    company we could not measure is absent from the criterion, not average
    at it.
    """
    if value is None or cid not in THRESHOLDS:
        return None
    lo, mid, hi = THRESHOLDS[cid]
    if cid in DESCENDING:
        if value > lo:
            return "Yikes"
        if value > mid:
            return "Meh"
        if value > hi:
            return "Good"
        return "Great"
    if value < lo:
        return "Yikes"
    if value < mid:
        return "Meh"
    if value < hi:
        return "Good"
    return "Great"


def _measure(cid: int, value, basis: str) -> Measure:
    v = _num(value)
    if v is None:
        return unmeasured("not filed", basis)
    return Measure(v, True, "", band_for(cid, v), basis)


# ═══════════════════════════════════════════════════════════════════
# THE SEVEN THAT CAN BE MEASURED
# ═══════════════════════════════════════════════════════════════════

def sales_growth(revenue_by_year: dict) -> Measure:
    """Criterion 1. Longest available window up to ten spans.

    Reuses lynch.growth with `drawdown_base=True`, which is the one guard
    the EPS board does not want and the revenue board cannot do without:
    Carnival's revenue over 3 spans compounds at 29.5%, over 5 at a capped
    36.4% and over 9 at 5.5% — Great, Great, Yikes, one company and one
    filing history, with no other guard firing at any window.
    """
    pts = sorted((y, v) for y, v in (revenue_by_year or {}).items()
                 if _num(v) is not None)
    if not pts:
        return unmeasured("revenue not filed")
    spans = min(MAX_SPANS, len(pts) - 1)
    if spans < MIN_SPANS_REVENUE:
        return unmeasured(f"only {len(pts)} annual periods, need "
                          f"{MIN_SPANS_REVENUE + 1}")
    g = L.growth(dict(pts), spans=spans, cap=REV_GROWTH_CAP, drawdown_base=True)
    basis = f"{spans} spans, {g.get('from_year', '?')} to {g.get('to_year', '?')}"
    if g["cagr"] is None:
        # MEASURED, BUT NO RATE. A third state, and not a failure: we read
        # the company and what we read says no honest rate exists. It is
        # not a band and it is not "not filed".
        why = ("base is a drawdown from a prior peak" if g.get("drawdown_base")
               else "base year is a trough" if g["trough"]
               else "one year does all the growing" if g["step"]
               else "a loss year inside the window" if g["loss_window"]
               else "no rate extractable")
        return unmeasured(why, basis)
    return Measure(g["cagr"], True, "", band_for(1, g["cagr"]), basis)


def gross_margin(revenue, gross_profit=None, cost_of_revenue=None) -> Measure:
    """Criterion 2. The filer's own subtotal, or revenue less cost.

    GrossProfit is preferred because it is the company's audited subtotal.
    When both are available they must RECONCILE — a disagreement wider than
    2% of revenue means the two tags are describing different things, and
    the honest answer is to withhold rather than to pick one.

    A company with no cost-of-revenue concept at all (many financials, some
    software) is unmeasured here, not zero-margin and not 100%-margin.
    """
    rev = _num(revenue)
    if rev is None or rev <= 0:
        return unmeasured("revenue not filed")
    gp, cost = _num(gross_profit), _num(cost_of_revenue)
    derived = rev - cost if cost is not None else None
    if gp is not None and derived is not None:
        if abs(gp - derived) > GM_RECONCILE_FRAC * rev:
            return unmeasured(
                "GrossProfit and revenue-less-cost disagree by more than 2% "
                "of revenue — the two tags are not the same subtotal",
                f"filed {gp:,.0f} vs derived {derived:,.0f}")
    use, how = (gp, "GrossProfit") if gp is not None else (derived, "revenue - cost")
    if use is None:
        return unmeasured("neither GrossProfit nor a cost-of-revenue tag filed")
    return _measure(2, use / rev * 100.0, how)


def eps_growth(eps_by_year: dict) -> Measure:
    """Criterion 3. The Lynch engine, on the longest window available.

    THIS WILL BE BLANK OFTEN, and that is the correct outcome rather than
    a defect to engineer around. A ten-span window has to clear the
    loss-window guard across ten years; on the most mature slice of this
    repo's own data only 22.9% of companies have eleven consecutive
    loss-free net-income years. Every one of the four companies on the
    current Lynch board is rated over exactly three spans despite carrying
    seven to seventeen years on file.
    """
    pts = sorted((y, v) for y, v in (eps_by_year or {}).items()
                 if _num(v) is not None)
    if not pts:
        return unmeasured("EPS not filed")
    spans = min(MAX_SPANS, len(pts) - 1)
    if spans < MIN_SPANS_EPS:
        return unmeasured(f"only {len(pts)} annual periods, need {MIN_SPANS_EPS + 1}")
    g = L.growth(dict(pts), spans=spans)
    basis = f"{spans} spans, {g.get('from_year', '?')} to {g.get('to_year', '?')}"
    if g["cagr"] is None:
        why = ("base year is a trough" if g["trough"]
               else "one year does all the growing" if g["step"]
               else "a loss year inside the window" if g["loss_window"]
               else "no rate extractable")
        return unmeasured(why, basis)
    return Measure(g["cagr"], True, "", band_for(3, g["cagr"]), basis)


def return_on_equity(net_income_by_year: dict, equity_by_year: dict) -> Measure:
    """Criterion 4. Median ROE over the available years, not an average.

    A MEDIAN BECAUSE ONE YEAR CAN OWN A MEAN. And bounded at 100 for the
    reason lynch.moat bounds it: as equity approaches zero the ratio stops
    describing a business and starts describing the denominator.

    The source material names the trap this cannot see — a high ROE from
    SHRINKING equity (buybacks, or leverage) is not the same fact as a high
    ROE from rising returns. The equity trend is returned in the basis so a
    reader can tell which they are looking at.
    """
    ni, eq = net_income_by_year or {}, equity_by_year or {}
    years = sorted(set(ni) & set(eq))
    roes = []
    for y in years:
        n, e = _num(ni[y]), _num(eq[y])
        if n is not None and e is not None and e > 0:
            roes.append(n / e * 100.0)
    if not roes:
        return unmeasured("no year with both net income and positive equity")
    med = L.median(roes)
    capped = med > L.ROC_CAP
    first, last = _num(eq[years[0]]), _num(eq[years[-1]])
    trend = ("equity shrank — check whether the return rose or the base fell"
             if (first and last and last < first) else "equity grew or held")
    return Measure(min(med, L.ROC_CAP), True, "", band_for(4, min(med, L.ROC_CAP)),
                   f"median of {len(roes)} yrs{', capped' if capped else ''}; {trend}")


def return_on_capital(op_income, current_assets, current_liabilities, ppe) -> Measure:
    """Criterion 5. Greenblatt's pre-tax return on capital.

    THIS IS NOT THE SOURCE'S DEFINITION and the difference is stated rather
    than smoothed over. The book asks for NOPAT / (equity + debt - cash).
    That needs an effective tax rate — two more tags that go missing, and a
    statutory 21% substituted for them would be a fabricated number with a
    decimal point. Worse, its denominator uses EQUITY, and an adversarial
    check found Papa John's scoring in the top band on it while the honest
    figure is two bands lower, because years of buybacks left the equity
    base near zero.

    Greenblatt's form needs no tax rate and no equity, so neither failure
    is reachable. It measures the same thing the criterion is asking about
    — what the operating business earns on the capital it employs.
    """
    r = L.return_on_capital(op_income, current_assets, current_liabilities, ppe)
    if r["roc_pct"] is None:
        return unmeasured(r["reason"] or "capital employed not measurable")
    basis = f"EBIT over ${r['capital'] / 1e6:,.0f}M of capital employed"
    if r["capped"]:
        basis += f" (raw {r['roc_raw_pct']}%, bounded)"
    return Measure(r["roc_pct"], True, "", band_for(5, r["roc_pct"]), basis)


def peer_discount(pe_ratio, peer_median_pe, peer_count) -> Measure:
    """Criterion 11. How far below its peer group's median P/E this trades.

    The peer group is the screen's own universe grouped by SIC, which is
    free and already fetched. A median over too few peers is not a median,
    so below PEER_MIN the criterion is unmeasured and says how many peers
    it found — a company in a thinly-populated SIC is not thereby cheap.
    """
    pe, med, n = _num(pe_ratio), _num(peer_median_pe), _num(peer_count)
    if pe is None:
        return unmeasured("no usable P/E")
    if pe > PE_PLAUSIBLE_MAX:
        return unmeasured(f"P/E of {pe:,.0f} is a near-zero denominator, not a "
                          f"valuation — above the {PE_PLAUSIBLE_MAX:,.0f}x "
                          f"plausibility ceiling")
    if med is None or n is None or n < PEER_MIN:
        return unmeasured(f"only {int(n) if n is not None else 0} peers in this "
                          f"SIC group, need {PEER_MIN} for a median")
    if med <= 0:
        return unmeasured("peer median P/E is not positive")
    disc = (med - pe) / med * 100.0
    return Measure(disc, True, "", band_for(11, disc),
                   f"P/E {pe:.1f} against a {int(n)}-peer median of {med:.1f}")


def market_cap(cap) -> Measure:
    """Criterion 13. Smaller is better — the only inverted scoreable one.

    Mayer's dataset had a median market cap around $500m. A hundred-bagger
    has to start small enough to become one, which is why this band runs
    the other way from every other criterion on the page.
    """
    return _measure(13, cap, "from the bulk market file")


# ═══════════════════════════════════════════════════════════════════
# THE VETO CRITERIA — they can only say no
# ═══════════════════════════════════════════════════════════════════

# A harvest needs BOTH legs: revenue genuinely falling, and margin not
# falling with it. Either alone is ordinary.
HARVEST_REV_DECLINE = -2.0        # %/yr, revenue CAGR below this
HARVEST_MARGIN_HOLD = -2.0        # pp of margin lost per year, above this
HARVEST_MIN_YEARS = 4

# Incremental return, not the level. Intel's LEVEL of ROIC was fine.
INCREMENTAL_RETURN_MIN = 5.0      # % on incremental capital
INCREMENTAL_MIN_CAPITAL_GROWTH = 0.20   # capital must have grown 20%+ to judge


def moat_veto(revenue_by_year: dict, margin_by_year: dict) -> Measure:
    """Criterion 8, as a veto only. Detects a HARVEST, never a moat.

    THE SPECIFICATION IS TUPPERWARE. Every proxy the design review tested
    scored it "Great" unanimously — high margin, high return on capital —
    for the entire window before it filed Chapter 11. It was not defending
    a franchise; it was harvesting a shrinking one, and margin held
    precisely BECAUSE volume was being given up.

    So the signal is not the level, it is the pair. A high margin with
    rising revenue is a moat. The same margin with falling revenue is a
    company selling less at a better price each year, which ends.

    THIS FUNCTION CANNOT SAY A COMPANY HAS A MOAT, and no free source can.
    It says "nothing visible is wrong", which is not the same claim and is
    rendered differently on the page.
    """
    rev = {y: v for y, v in (revenue_by_year or {}).items() if _num(v) is not None}
    mar = {y: v for y, v in (margin_by_year or {}).items() if _num(v) is not None}
    years = sorted(set(rev) & set(mar))
    if len(years) < HARVEST_MIN_YEARS:
        return unmeasured(f"needs {HARVEST_MIN_YEARS} years of revenue and margin "
                          f"together, has {len(years)}")
    y0, y1 = years[0], years[-1]
    spans = len(years) - 1
    r0, r1 = rev[y0], rev[y1]
    if r0 <= 0 or r1 <= 0:
        return unmeasured("revenue not positive at both ends")
    rev_cagr = ((r1 / r0) ** (1.0 / spans) - 1.0) * 100.0
    margin_drift = (mar[y1] - mar[y0]) / spans          # pp per year

    basis = (f"revenue {rev_cagr:+.1f}%/yr, margin {margin_drift:+.1f}pp/yr "
             f"over {spans} spans")
    if rev_cagr < HARVEST_REV_DECLINE and margin_drift > HARVEST_MARGIN_HOLD:
        return Measure(rev_cagr, True, VETO_REASONS[8], "Yikes", basis)
    return Measure(rev_cagr, True, "", "quiet", basis)


def capital_veto(op_income_by_year: dict, invested_capital_by_year: dict) -> Measure:
    """Criterion 6, as a veto only. Incremental return, not the level.

    THE SPECIFICATION IS INTEL FY2015-19, which every proxy the design
    review tested placed in the TOP band. Its level of return was healthy
    throughout. What was happening was that enormous capital was going in
    at a return far below what the existing base earned — the process-node
    failure — and only an INCREMENTAL measure can see that.

    delta operating income over delta invested capital, across the window.
    Judged only when capital actually grew enough for the ratio to mean
    something; a company that invested nothing cannot be judged on how well
    it invested.

    Still not a verdict on capital allocation, which needs a human reading
    a decade of letters. It is one specific way of being wrong, detected.
    """
    op = {y: v for y, v in (op_income_by_year or {}).items() if _num(v) is not None}
    cap = {y: v for y, v in (invested_capital_by_year or {}).items()
           if _num(v) is not None}
    years = sorted(set(op) & set(cap))
    if len(years) < HARVEST_MIN_YEARS:
        return unmeasured(f"needs {HARVEST_MIN_YEARS} years of operating income and "
                          f"capital together, has {len(years)}")
    y0, y1 = years[0], years[-1]
    d_cap = cap[y1] - cap[y0]
    if cap[y0] <= 0:
        return unmeasured("opening invested capital not positive")
    if d_cap <= cap[y0] * INCREMENTAL_MIN_CAPITAL_GROWTH:
        # Nothing was invested, so nothing can be said about how well — but
        # TWO DIFFERENT THINGS LAND HERE and they were being told the same
        # sentence. On the current board 78 companies barely moved their
        # capital and 376 shrank it, and all 454 read "too little to judge".
        # "Too little" describes smallness; a company that returned or wrote
        # off 40% of its capital base did something substantial, and calling
        # that too little to judge is the same species of error as the rest
        # of this file's history — a real event described by a sentence that
        # does not fit it. Both stay quiet, because incremental return on a
        # capital base that is leaving is undefined rather than bad. Only
        # the description changes, and it should.
        moved = d_cap / cap[y0] * 100.0
        if d_cap < 0:
            return Measure(None, True, "", "quiet",
                           f"capital FELL {abs(moved):.0f}% over "
                           f"{y0[:4]}-{y1[:4]} — incremental return is "
                           f"undefined when capital is leaving, so this "
                           f"says nothing either way about the allocation")
        return Measure(None, True, "", "quiet",
                       f"capital grew {moved:+.0f}% over {y0[:4]}-{y1[:4]} "
                       f"— too little invested to judge how well")
    inc = (op[y1] - op[y0]) / d_cap * 100.0
    basis = (f"{inc:+.1f}% on ${d_cap / 1e6:,.0f}M of incremental capital, "
             f"{y0[:4]}-{y1[:4]}")
    if inc < INCREMENTAL_RETURN_MIN:
        return Measure(inc, True, VETO_REASONS[6], "Yikes", basis)
    return Measure(inc, True, "", "quiet", basis)


def ownership_veto(pct: float | None, reason: str = "") -> Measure:
    """Criterion 7, as a veto only, from the DEF 14A group ownership row.

    Bands would be presumptuous — 30% insider ownership is alignment in one
    company and entrenchment in another, and the row prints the raw percent
    so a reader can tell which. What IS safe to say is the negative: a
    management team owning almost nothing has no skin in a fifteen-year
    outcome, which is the criterion's own "None" band.

    A MISSING PERCENT IS NOT ZERO. Foreign private issuers file no proxy
    statement at all — they are exempt from Regulation 14A — so roughly one
    row in eight can never be scored here at any budget. That is a fact
    about the filing regime, and reading it as "no insider ownership" would
    be `country or "United States"` wearing a new hat.
    """
    if pct is None:
        return unmeasured(reason or "ownership not read")
    v = _num(pct)
    if v is None:
        return unmeasured(reason or "ownership not numeric")
    basis = f"officers and directors as a group hold {v:.1f}%"
    if v < INSIDER_MIN_PCT:
        return Measure(v, True, VETO_REASONS[7], "Yikes", basis)
    return Measure(v, True, "", "quiet", basis)


# ── The DEF 14A group-ownership row, parsed ─────────────────────────
#
# Matches the standard beneficial-ownership summary line. Filing agents
# word it differently and every variant below appears in real proxies:
#   "All executive officers and directors as a group (12 persons)"
#   "All directors and executive officers as a group"
#   "All of our current directors and executive officers as a group"
_GROUP_ROW = re.compile(
    r"all\s+(?:of\s+)?(?:our\s+|the\s+)?(?:current\s+)?"
    r"(?:(?:executive\s+)?officers?\s+and\s+directors?"
    r"|directors?\s+and\s+(?:executive\s+)?officers?)"
    r"[^\n]{0,120}?as\s+a\s+group", re.I)

# The percent cell that follows it. An asterisk or "less than 1%" is a
# REAL measurement meaning under one percent; an em-dash or a blank is not.
_PCT = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*%")

# AN ASTERISK MEANS TWO THINGS IN THE SAME TABLE and this used to accept
# both. In the percent column it is the standard "less than 1%" legend. On
# any other token it is a footnote marker: "(12 persons)*", "(*)",
# "1,234,567*". Matching a bare `\*` anywhere read the footnote marker as
# the holding, and because 0.5 sits under INSIDER_MIN_PCT it vetoed the
# company — discarding the real percent sitting a few characters later.
#
# It replaced one fabrication with another. Walking past the intro
# paragraph (which fixed nine identical 5.00%s) landed the scan on the real
# table row, where the footnote marker was waiting: nine companies then
# reported exactly 0.50%, which is 45% of every value the criterion
# produced, and eight of the ten names that left the board that run were
# removed by this criterion.
#
# The discriminator is position, not content. A legend asterisk STANDS
# ALONE — whitespace on both sides, because it is the whole cell. A
# footnote marker is welded to the token it annotates.
_LESS_THAN_ONE = re.compile(
    r"(?:(?:^|(?<=\s))\*(?=\s|$)|less\s+than\s+1\s*%|<\s*1\s*%)", re.I)

# A PERCENT INTRODUCED BY A COMPARATIVE IS A THRESHOLD, NOT A HOLDING.
# Every proxy carries the Section 13(d) boilerplate — "owners of more than
# 5% of our common stock" — and it sits within a few hundred characters of
# the ownership table, which is to say inside any window wide enough to
# catch a real value. Taking the first percent in that window read the
# boilerplate as the holding: on the live board NINE of the sixteen
# companies that produced a figure produced exactly 5.00%, every one of
# them under the 10% floor, every one vetoed and dropped from the list.
# A table cell never says "more than 24.3%".
#
# Deliberately excludes "less than", which IS a real cell value and is
# handled by _LESS_THAN_ONE above.
_THRESHOLD_LEAD = re.compile(
    r"(?:more\s+than|at\s+least|in\s+excess\s+of|greater\s+than|"
    r"not\s+less\s+than|exceeds?|exceeding|owners?\s+of)\s*$", re.I)

# How far past the group row a real percent cell can sit. The flattened
# row is "(12 persons) 1,234,567 24.3%" — under thirty characters. Room
# for footnote markers and a second share column still leaves this far
# short of a sentence of prose. Beyond it we are reading the next section,
# not the row, so the answer is "not read" rather than a number.
_CELL_GAP = 120


def parse_group_ownership(text: str) -> tuple[float | None, str]:
    """(percent, reason) from a DEF 14A's beneficial-ownership table.

    THE COMPANY'S OWN NUMBER, not one this module computes. Proxies state
    the group total directly, already netted for options exercisable within
    sixty days, and recomputing it from Forms 4 would be worse — see below.

    Returns (None, reason) on anything it cannot read. NEVER (0.0, ...):
    a parse failure that returned zero would band as the worst possible
    score for a founder who may own 45%, which is precisely the shape of
    the unfiled-debt-tag bug this repo has already paid for twice.

    WHY NOT FORMS 3/4/5, which are structured XML and would parse cleanly:
    `sharesOwnedFollowingTransaction` is current only as of the filing. An
    insider who has not traded since 2019 contributes a 2019 count against
    today's share denominator, and one who never traded contributes their
    Form 3 count or nothing. The measure is therefore worst for the
    never-sold founder the criterion exists to find, and best for the
    executive who sells constantly. It is more accurate the less it
    matters, so it is refused on accuracy rather than on cost.
    """
    if not text:
        return None, "no document"
    flat = re.sub(r"<[^>]+>", " ", text)
    flat = re.sub(r"&nbsp;?", " ", flat)
    flat = re.sub(r"\s+", " ", flat)
    # EVERY OCCURRENCE, NOT THE FIRST. The phrase appears twice in a normal
    # proxy: once in the paragraph introducing the section ("…by each person
    # known to us to be the beneficial owner of more than 5% of our common
    # stock, by each director, and by all executive officers and directors
    # as a group"), and again as the table's last row. `search` took the
    # intro, and what follows the intro is the SEC's beneficial-ownership
    # boilerplate — whose first percent is the 5% reporting threshold.
    # That is where nine identical 5.00%s came from, and each one vetoed a
    # company off the board. Walk the matches and take the first that
    # actually yields a cell.
    seen = False
    reason = "ownership table found, group row not located"
    for m in _GROUP_ROW.finditer(flat):
        seen = True
        tail = flat[m.end():m.end() + 400]
        pct = _PCT.search(tail)
        lt1 = _LESS_THAN_ONE.search(tail)
        # WHICHEVER COMES FIRST, not whichever is checked first. "less than
        # 1%" contains a numeric percent, so testing _PCT first reads it as
        # 1.0% — a company reported as UNDER one percent, published as
        # exactly one.
        if lt1 and (not pct or lt1.start() <= pct.start()):
            if lt1.start() > _CELL_GAP:
                reason = ("group row found, nearest '<1%' is "
                          f"{lt1.start()} characters away — too far to be its cell")
                continue
            # A real measurement and a real finding: under one percent.
            return 0.5, ""
        if not pct:
            reason = "group row found, no percent cell"
            continue
        # THE CELL, OR NOTHING. Two ways the nearest percent is not the
        # group's holding, and both used to return a number.
        if pct.start() > _CELL_GAP:
            reason = ("group row found, nearest percent is "
                      f"{pct.start()} characters away — too far to be its cell")
            continue
        if _THRESHOLD_LEAD.search(tail[:pct.start()]):
            reason = ("group row found, but the nearest percent is a "
                      "disclosure threshold rather than a holding")
            continue
        try:
            v = float(pct.group(1))
        except ValueError:
            reason = "group row found, percent cell unparseable"
            continue
        if 0.0 <= v <= 100.0:
            return v, ""
        reason = f"percent out of range: {v}"
    return None, (reason if seen else "ownership table found, group row not located")


# ═══════════════════════════════════════════════════════════════════
# THE LEDGER — the actual output
# ═══════════════════════════════════════════════════════════════════

def ledger(measures: dict) -> dict:
    """The k-of-13 object. No score, no total, no division.

    `measures` is {criterion_id: Measure} for the scoreable ones. The six
    that can never be scored are added here rather than by the caller, so
    no caller can forget them and shrink the denominator.

    THE FRACTION ALWAYS SHOWS BOTH NUMBERS. "Good or Great on 4 of the 6
    measured, of 13" is the sentence. "67%" is not, because it is the same
    figure for a company measured on six criteria and one measured on
    three, and the difference between those two companies is the entire
    point of this page.
    """
    scored, bands, unmeasured_ids = {}, {}, []
    for cid in SCOREABLE_IDS:
        m = measures.get(cid)
        if m is not None and m.measured and m.band:
            scored[cid] = m
            bands[cid] = m.band
        else:
            unmeasured_ids.append(cid)

    # THE GATE IS SHOWN, NEVER COUNTED. Criterion 13 decides membership
    # and must not also be evidence for it — 99 of the 111 companies at
    # the four-pass minimum were counting "it is small" as one of their
    # four passes, having cleared three quality gates rather than four.
    gate_bands = {}
    for cid in GATE_IDS:
        m = measures.get(cid)
        if m is not None and m.measured and m.band:
            gate_bands[cid] = m.band

    # VETOES ONLY SUBTRACT. A Yikes here removes the company; anything
    # else changes nothing at all, because no free source can establish
    # any of these three positively.
    vetoes, veto_quiet = [], []
    for cid in VETO_IDS:
        m = measures.get(cid)
        if m is None or not m.measured:
            continue
        (vetoes if m.band == "Yikes" else veto_quiet).append(cid)

    passing = [c for c, b in bands.items() if b in PASS_BANDS]
    great = [c for c, b in bands.items() if b == "Great"]
    floors = sorted(bands, key=lambda c: BAND_ORDINAL[bands[c]])
    return {
        "criteria_total": CRITERIA,
        "measured_ids": sorted(scored),
        "measured_n": len(scored),
        "unmeasured_ids": sorted(unmeasured_ids),
        "never_scored_ids": list(NEVER_SCORED_IDS),
        "gate_ids": list(GATE_IDS),
        "gate_bands": gate_bands,
        "veto_ids": list(VETO_IDS),
        "vetoed_by": sorted(vetoes),
        "veto_quiet": sorted(veto_quiet),
        "vetoed": bool(vetoes),
        "bands": bands,
        "passing_ids": sorted(passing),
        "passing_n": len(passing),
        "great_ids": sorted(great),
        "great_n": len(great),
        # The weakest band present, and which criterion owns it. On this
        # repo's own data one criterion owns 60-84% of all floors, which is
        # exactly why the page does not rank on it.
        "floor_band": bands[floors[0]] if floors else None,
        "floor_criterion": floors[0] if floors else None,
        "headline": (f"{len(passing)} of the {len(scored)} measured, "
                     f"of {CRITERIA}") if scored else f"nothing measured, of {CRITERIA}",
    }


def churn(current: list, previous: list | None) -> dict:
    """Who entered, who left, and how much of the board is the same.

    SHIPPED IN THE FIRST VERSION, not a later one, because the thesis is a
    fifteen-year hold and this repo's other board turned over almost
    completely in a month: 10 names in June, 11 in July, 4 in August, with
    only lululemon surviving July into August. Most of that was a rules
    change rather than a market — which is the finding. A reader deciding
    whether to hold something for a decade is owed the number that says how
    long this page has held it.
    """
    cur = {r.get("ticker") for r in current or [] if r.get("ticker")}
    if previous is None:
        return {"comparable": False, "entered": [], "left": [], "held": [],
                "held_n": None, "entered_n": None, "left_n": None}
    prev = {r.get("ticker") for r in previous or [] if r.get("ticker")}
    return {
        "comparable": True,
        "entered": sorted(cur - prev), "left": sorted(prev - cur),
        "held": sorted(cur & prev),
        "entered_n": len(cur - prev), "left_n": len(prev - cur),
        "held_n": len(cur & prev), "previous_n": len(prev),
    }


# How much of a criterion's output may sit on one value before the shape
# stops looking like a measurement. Deliberately loose: a real signal
# trips it only for a criterion with a hard cap, and those are obvious
# from the value itself.
CLUSTER_FLAG_PCT = 10.0
CLUSTER_MIN_N = 20


def value_clustering(rows: list[dict]) -> dict:
    """Per criterion: how much of its output lands on a single number.

    A DETECTOR FOR THE ONE BUG THIS REPO KEEPS SHIPPING. Four times now a
    parser has emitted a constant where a measurement belonged, and every
    time it was found by a human noticing an odd-looking board rather than
    by anything in the code. The signature is always identical and always
    visible in one line: nine companies at exactly 5.00% when a regex read
    the SEC's 5% reporting threshold as a holding, then — after that was
    fixed — nine at exactly 0.50%, the module's own "less than 1%"
    sentinel, when a footnote asterisk was read as the percent cell. That
    second cluster was 45% of everything the criterion produced.

    Reported, not enforced. A capped criterion legitimately piles up on its
    bound: return on capital stops at ROC_CAP and dozens of companies sit
    there honestly. The modal VALUE is returned alongside the share
    precisely so the distinction stays with the reader — 100.0 on a
    criterion capped at 100 is a bound, 0.50 on a criterion with no bound
    at all is a sentinel escaping as data.
    """
    out = {}
    for cid in range(1, CRITERIA + 1):
        vals = []
        for r in rows or []:
            m = (r.get("measures") or {}).get(str(cid)) or {}
            if m.get("measured") and m.get("value") is not None:
                try:
                    vals.append(round(float(m["value"]), 4))
                except (TypeError, ValueError):
                    continue
        n = len(vals)
        if not n:
            continue
        counts = {}
        for v in vals:
            counts[v] = counts.get(v, 0) + 1
        modal = max(counts, key=counts.get)
        modal_n = counts[modal]
        # The count is bound to a name before dividing. Dividing by a
        # length inline is banned at source level by the test suite, for
        # the composite-score reason — and the ban is a raw substring scan
        # that reads comments too, so this one says it in words.
        pct = round(modal_n / n * 100.0, 1)
        out[cid] = {
            "n": n, "modal": modal, "modal_n": modal_n, "modal_pct": pct,
            "distinct": len(counts),
            "flagged": bool(pct >= CLUSTER_FLAG_PCT and n >= CLUSTER_MIN_N),
        }
    return out


def census(rows: list[dict]) -> dict:
    """The funnel. Partitions — screened = rejected + thin + listed.

    `thin` is its own bucket rather than part of `rejected`: a company we
    could not measure enough of is a different fact from one that failed a
    gate, and folding them together is how the Lynch funnel came to count
    975 companies twice.
    """
    listed = [r for r in rows if r.get("verdict") == "list"]
    thin = [r for r in rows if r.get("verdict") == "thin"]
    rejected = [r for r in rows if r.get("verdict") == "reject"]
    by_reason: dict[str, int] = {}
    for r in rejected + thin:
        code = r.get("reason") or "unknown"
        by_reason[code] = by_reason.get(code, 0) + 1
    # How many rows reached each criterion at all — the coverage question
    # the whole page is built around, answered for the universe and not
    # only for the survivors.
    coverage = {}
    for cid in SCOREABLE_IDS:
        coverage[cid] = sum(1 for r in rows
                            if cid in ((r.get("ledger") or {}).get("measured_ids") or []))

    # THE VETOES MEASURE THINGS AND APPEARED NOWHERE. They are not in
    # `coverage` because that loop walks SCOREABLE_IDS, and they are not in
    # `never_scored` because they are not never-scored. A criterion that
    # removed 18 companies from this board and shows on no panel is exactly
    # the kind of silent machinery this page exists to expose.
    veto_coverage = {}
    for cid in VETO_IDS:
        fired = quiet = unread = 0
        for r in rows:
            m = (r.get("measures") or {}).get(str(cid)) or {}
            if not m:
                continue
            if not m.get("measured"):
                unread += 1
            elif m.get("band") == "Yikes":
                fired += 1
            else:
                quiet += 1
        veto_coverage[cid] = {"fired": fired, "quiet": quiet, "unread": unread}
    # CONCENTRATION, over the LISTED set. A hundred and forty-eight names
    # sounds diversified and is not: measured on the live board the SIC-2
    # Herfindahl is 0.150, which is 6.7 effective sectors, and software
    # alone is 36% of the list. A basket built from it is one bet wearing
    # many tickers, and the reader cannot see that from any other number
    # on the page.
    sic_counts: dict[str, int] = {}
    for r in listed:
        k = str(r.get("sic") or "")[:2] or "??"
        sic_counts[k] = sic_counts.get(k, 0) + 1
    n_listed = len(listed)
    hhi = sum((c / n_listed) ** 2 for c in sic_counts.values()) if n_listed else None
    ranked = sorted(sic_counts.items(), key=lambda kv: -kv[1])
    concentration = {
        # AN ARRAY, NOT A DICT. The dict is ordered correctly here and
        # JavaScript throws that ordering away: `Object.entries` returns
        # integer-like keys in ASCENDING NUMERIC order regardless of
        # insertion, and SIC codes are integer-like strings. The page
        # rendered "SIC 13: 1, SIC 15: 1, SIC 28: 2 …" — the six SMALLEST
        # sectors — while the 45% one sat off the end of the slice. The
        # data was right and the format silently reordered it, which is
        # this codebase's oldest failure wearing a new hat.
        "by_sic_ranked": [[k, v] for k, v in ranked],
        "by_sic": dict(ranked),
        "hhi": round(hhi, 4) if hhi else None,
        "effective_sectors": round(1 / hhi, 1) if hhi else None,
        "largest_sic": max(sic_counts, key=sic_counts.get) if sic_counts else None,
        "largest_share_pct": (round(max(sic_counts.values()) / n_listed * 100, 1)
                              if n_listed else None),
    }

    return {
        "screened": len(rows),
        "listed": len(listed), "thin": len(thin), "rejected": len(rejected),
        "concentration": concentration,
        "by_reason": dict(sorted(by_reason.items(), key=lambda kv: -kv[1])),
        "coverage": coverage,
        "veto_coverage": veto_coverage,
        "clustering": value_clustering(rows),
        "criterion_names": CRITERION_NAMES,
        "never_scored": {str(c): NOT_SCORED_BECAUSE[c] for c in NEVER_SCORED_IDS},
    }


# ═══════════════════════════════════════════════════════════════════
# PAGE SIDE
# ═══════════════════════════════════════════════════════════════════

def available_months() -> list[str]:
    if not _DATA_DIR.exists():
        return []
    return sorted(p.stem for p in _DATA_DIR.glob("*.json"))


def load(month: str | None = None) -> dict:
    months = available_months()
    if not months:
        return {}
    key = month if month in months else months[-1]
    try:
        with open(_DATA_DIR / f"{key}.json", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}
