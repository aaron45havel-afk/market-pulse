"""FCF quality — the VictoryShares VFLO method, run over the whole market.

VFLO screens the largest 400 profitable US companies, keeps the 75 with
the highest free cash flow yield, then keeps the 50 of those with the
best growth score. BofA's factor work puts that construction at 18.2%/yr
since 1999 against 8.5% for the Russell 1000. The interesting part is
not the yield or the growth on their own — it is the ORDER. Cheap on
cash first, growing second. Reversed, you buy expensive growth; without
the second stage, you buy value traps.

THE ONLY CHANGE HERE IS THE UNIVERSE. VFLO is a large-cap index and
cannot hold what it cannot weight; this runs the same funnel over every
filer the pipeline can measure, which is where a screen has an edge an
index structurally does not. A $300m company with an 11% FCF yield and a
rising sales trend is invisible to VFLO by construction.

────────────────────────────────────────────────────────────────────
WHAT THIS CANNOT DO, said once and repeated on the page
────────────────────────────────────────────────────────────────────
VFLO's yield is AVERAGE(trailing 12m FCF, FORWARD 12m FCF) / EV, and its
growth score includes a 3-5 year consensus EPS growth estimate. Both
forward legs come from a FactSet consensus feed. This pipeline reads SEC
filings, which are by definition backward-looking.

So: the yield here is TRAILING ONLY, and the growth score has two legs
where VFLO has three. That is a real difference and not a detail —
VFLO's own factsheet leads with "considers a company's expected free
cash flow, not just trailing data" as the thing that distinguishes it.
A company whose cash flow is about to fall off a cliff looks identical
to one whose is about to double. The guards below catch bad DATA; they
cannot catch a bad FUTURE, and nothing in a filing can.

Where the construction is faithful it is faithful exactly — the two-stage
proportions are VFLO's own 75/400 and 50/75, not round numbers chosen to
look similar.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from statistics import median

# ── the funnel, in VFLO's own proportions ────────────────────────────
#
# 75 of 400 is 18.75%, then 50 of 75 is 66.67%, so the index is the top
# 12.5% of its starting universe. Expressed as FRACTIONS rather than
# counts because the universe here is not 400 — it is whatever the
# pipeline could measure, which moves every quarter. Fixed counts would
# silently change the selectivity as coverage grew.
FCF_TAKE = 75 / 400          # 0.1875
GROWTH_TAKE = 50 / 75        # 0.6667

# ── universe floors ──────────────────────────────────────────────────
#
# "Whole market" means small, not fictional. These are deliberately far
# below VFLO's large-cap floor — that is the entire point — but a screen
# with no floor at all ranks shells and registrants with filing agents,
# and on a yield sort those sort FIRST because a $2m market cap divided
# into any cash flow at all is a huge number.
MIN_MARKET_CAP = 50_000_000
MIN_REVENUE = 10_000_000

# ── sanity bounds, each one a data fault rather than a judgement ─────
#
# An FCF yield above this is not a bargain. At 50% the market is saying
# the cash flow ends within two years, and on the live join the rows up
# there are debt tags that did not file, not businesses.
FCF_YIELD_CEILING = 50.0
# EV below a fraction of market cap means a company is being reported as
# mostly net cash. That is real for a handful of names and a missing
# debt tag for far more of them, and the two are indistinguishable from
# the filing alone — so the row is kept and BADGED rather than ranked on
# a yield its denominator cannot support.
EV_TO_MCAP_FLOOR = 0.25
EV_TO_MCAP_CEILING = 20.0

# A trend fitted through fewer points than this is a line through noise.
MIN_TREND_POINTS = 5


class Unmeasurable(ValueError):
    """Raised only for programmer error. A company that cannot be measured
    gets a REASON on its row, never an exception — a screen that throws on
    bad data is a screen that reports a smaller universe than it saw."""


def _num(v):
    """A float, or None. NaN and inf are None: both propagate silently
    through a sort and put themselves at one end of the board."""
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


# ═══════════════════════════════════════════════════════════════════
# ENTERPRISE VALUE
# ═══════════════════════════════════════════════════════════════════

def enterprise_value(market_cap, total_debt=None, cash=None,
                     preferred=None, minority=None) -> dict:
    """VFLO's EV definition, with missing components refused rather than
    zeroed.

        EV = diluted shares x price      (market cap)
           + preferred stock
           + total debt
           + accumulated minority interest
           - cash & cash equivalents

    MISSING DEBT IS NOT ZERO DEBT, and this is the whole reason the
    function returns a dict instead of a number. An unfiled debt tag read
    as 0 makes EV smaller, which makes the yield HIGHER, which sorts the
    company toward the top of a board ranked by yield. The failure is not
    that one row is wrong — it is that the wrong rows are exactly the ones
    the screen surfaces. lynch.py learned this on the debt/equity gate and
    the lesson transfers unchanged.

    So a row with no debt figure gets basis="market_cap": a real,
    computable, clearly-labelled number that is not pretending to be EV.
    """
    mc = _num(market_cap)
    if mc is None or mc <= 0:
        return {"ev": None, "basis": None, "complete": False,
                "reason": "no market capitalisation"}

    debt, csh = _num(total_debt), _num(cash)
    pref = _num(preferred) or 0.0
    mino = _num(minority) or 0.0

    if debt is None or csh is None:
        missing = " and ".join(
            [n for n, v in (("debt", debt), ("cash", csh)) if v is None])
        return {"ev": None, "basis": "market_cap", "complete": False,
                "usable": False,
                "reason": f"{missing} not filed, so enterprise value "
                          f"cannot be computed. Treating an unfiled tag as "
                          f"zero would shrink the denominator, RAISE the "
                          f"yield, and sort this row UP a board ranked by "
                          f"yield — the company that failed to file would "
                          f"outrank the one that did."}

    ev = mc + pref + debt + mino - csh
    ratio = ev / mc
    if ev <= 0:
        return {"ev": ev, "basis": "ev", "complete": True, "usable": False,
                "reason": f"enterprise value is not positive ({ev:,.0f}) — "
                          f"more net cash than market value. A yield "
                          f"computed on it is a division by something "
                          f"approaching zero."}
    if ratio < EV_TO_MCAP_FLOOR:
        return {"ev": ev, "basis": "ev", "complete": True, "usable": False,
                "reason": f"enterprise value is {ratio:.0%} of market cap — "
                          f"reported as mostly net cash. Real for a few "
                          f"names and an unfiled debt tag for more of "
                          f"them, and the filing cannot tell them apart."}
    if ratio > EV_TO_MCAP_CEILING:
        return {"ev": ev, "basis": "ev", "complete": True, "usable": False,
                "reason": f"enterprise value is {ratio:.0f}x market cap — "
                          f"debt that large against equity that small is a "
                          f"unit error far more often than a balance sheet."}
    return {"ev": ev, "basis": "ev", "complete": True, "usable": True,
            "reason": ""}


# ── a second opinion on the debt, and why it is one-sided ────────────
#
# The balance-sheet extractor agrees with the pipeline's independently
# computed net-debt/EBIT for the typical company — median ratio exactly
# 1.00, p25 to p90 of 0.79 to 1.09 across 1,120 cross-checkable rows. It
# fails on a specific group: foreign IFRS filers whose debt sits under
# element names the tag ladder does not carry. Korea Electric, Ecopetrol,
# Toyota, POSCO, Takeda and Wipro all came through with a fraction of
# their real borrowings, and 13.5% of rows disagree in SIGN — the
# extractor reporting net cash where the ratio says net debt.
#
# THE GUARD IS ASYMMETRIC AND THAT IS THE POINT. An overstated debt
# figure inflates enterprise value, which LOWERS the yield, which loses a
# name — a cost paid in missed opportunities nobody sees. An understated
# one shrinks enterprise value, RAISES the yield, and promotes the row up
# a board sorted by yield. Only the second is refused. The screen should
# fail toward missing something rather than toward recommending it.
DEBT_UNDERSTATEMENT_LIMIT = 0.5     # of the independent estimate
DEBT_ORACLE_FLOOR = 100_000_000     # below this the estimate cannot discriminate


# A company this large with NO debt tag at all has an unmapped tag, not a
# clean balance sheet. General Motors and BP both arrived with zero debt
# inferred, and the cross-check cannot catch them: when the extractor
# finds nothing, the independent estimate is usually built from the same
# nothing, so both paths agree and both are wrong. Agreement is only
# evidence when the two paths are genuinely independent.
#
# Size is the discriminator that does not depend on either path. A small
# company with no debt tag is ordinary — plenty of them carry no debt. A
# company worth tens of billions that filed no borrowings at all is not
# debt-free; it is filed under an element name the ladder does not have.
# Market cap, not revenue, because it comes from a USD quote file and a
# foreign filer's revenue is in its own currency.
INFERRED_DEBT_FREE_MAX_CAP = 10_000_000_000


def inferred_zero_fault(debt_inferred_zero, market_cap) -> str | None:
    """Why an inferred debt-free balance sheet is not credible, or None."""
    if not debt_inferred_zero:
        return None
    mc = _num(market_cap)
    if mc is None or mc < INFERRED_DEBT_FREE_MAX_CAP:
        return None
    return (f"no borrowings tag of any kind on a company worth "
            f"${mc/1e9:.0f}bn. Below ${INFERRED_DEBT_FREE_MAX_CAP/1e9:.0f}bn "
            f"an absent debt tag is ordinary; at this size it is an "
            f"element name the tag ladder does not carry. Reading it as "
            f"zero would set enterprise value to market cap and inflate "
            f"the yield — and the independent net-debt check cannot catch "
            f"it, because when the extractor finds nothing the estimate is "
            f"usually built from the same nothing and agrees.")


def debt_cross_check(net_debt, oracle_net_debt) -> str | None:
    """Why this row's extracted debt cannot be trusted, or None.

    `oracle_net_debt` is an independent estimate from a different code
    path — net-debt/EBIT multiplied back out by EBIT. It carries its own
    error, so this is deliberately loose: it fires on a company whose
    debt is understated by half or more, not on ordinary disagreement.
    """
    o = _num(oracle_net_debt)
    if o is None or o < DEBT_ORACLE_FLOOR:
        return None                 # nothing to check against
    n = _num(net_debt)
    if n is None:
        return None
    if n < o * DEBT_UNDERSTATEMENT_LIMIT:
        return (f"the debt tags found sum to ${n/1e9:.2f}bn of net debt "
                f"against ${o/1e9:.2f}bn implied by this company's own "
                f"net-debt/EBIT — under half. Two independent paths "
                f"disagreeing that far means the tag ladder missed "
                f"borrowings, which would shrink enterprise value, RAISE "
                f"the yield and move this row up a board sorted by yield.")
    return None


def fcf_yield(fcf, ev) -> float | None:
    """Free cash flow over enterprise value, as a percent.

    Negative FCF returns None rather than a negative yield. This screen
    ranks by yield descending and a negative number sorts to the bottom
    harmlessly — but it would still be COUNTED as measured, and "we
    looked at 1,200 companies" should not include the ones that have no
    free cash flow to have a yield on.
    """
    f, e = _num(fcf), _num(ev)
    if f is None or e is None or e <= 0:
        return None
    if f <= 0:
        return None
    return round(f / e * 100, 2)


def yield_fault(y) -> str | None:
    """Why a yield cannot be used, or None. Named, per this repo's habit
    of never dropping a row without saying why."""
    v = _num(y)
    if v is None:
        return "no yield"
    if v <= 0:
        return "no positive free cash flow"
    if v > FCF_YIELD_CEILING:
        return (f"{v:.1f}% yield is past the {FCF_YIELD_CEILING:.0f}% "
                f"ceiling — the market does not price a going concern to "
                f"return its whole enterprise value in two years, so this "
                f"is a denominator that did not file")
    return None


# ═══════════════════════════════════════════════════════════════════
# GROWTH
# ═══════════════════════════════════════════════════════════════════

def trend_score(series: dict) -> float | None:
    """VFLO's trend construction: the slope of the series over its own mean.

    The factsheet defines it precisely — "the slope of two forward years
    of sales and five years of trailing sales divided by the average of
    those years". Dividing by the mean is what makes it comparable across
    companies: a $40bn revenue line growing $2bn a year and a $400m line
    growing $20m a year both score 5, which is the point.

    The forward years are not available here, so this fits the trailing
    years alone. Fewer points, same construction.

    Returns a percent. None when there are too few points to fit a line
    through — four points is a guess, and a guess that looks like a
    measurement is worse than a gap.
    """
    if not series:
        return None
    pts = []
    for k in sorted(series):
        v = _num(series[k])
        y = _num(k)
        if v is not None and y is not None:
            pts.append((y, v))
    if len(pts) < MIN_TREND_POINTS:
        return None

    n = len(pts)
    mean_x = sum(x for x, _ in pts) / n
    mean_y = sum(y for _, y in pts) / n
    # A mean at or below zero cannot normalise a slope — the sign flips
    # and a shrinking loss reads as growth. Real case: any company whose
    # EBITDA averaged negative across the window.
    if mean_y <= 0:
        return None
    denom = sum((x - mean_x) ** 2 for x, _ in pts)
    if denom == 0:
        return None
    slope = sum((x - mean_x) * (y - mean_y) for x, y in pts) / denom
    return round(slope / mean_y * 100, 2)


def percentile_ranks(values: list) -> list:
    """Rank each value 0-100 against the others. None stays None.

    VFLO's growth score is "the average RANK of the growth rate measures",
    not the average of the measures themselves — which matters, because a
    sales trend and an EPS growth estimate are not in the same units and
    averaging them directly would let whichever has the wider spread
    decide the score on its own.

    Ties share the midpoint of the ranks they span, so a column where
    every value is identical contributes 50 to everybody rather than an
    order determined by dict insertion.
    """
    idx = [(i, _num(v)) for i, v in enumerate(values)]
    present = sorted((v, i) for i, v in idx if v is not None)
    out = [None] * len(values)
    if not present:
        return out
    n = len(present)
    if n == 1:
        out[present[0][1]] = 50.0
        return out
    i = 0
    while i < n:
        j = i
        while j + 1 < n and present[j + 1][0] == present[i][0]:
            j += 1
        # midpoint rank for the whole tied block
        share = (i + j) / 2
        pct = round(share / (n - 1) * 100, 2)
        for k in range(i, j + 1):
            out[present[k][1]] = pct
        i = j + 1
    return out


def growth_score(component_ranks: list, min_components: int = 2):
    """Average of the available component ranks, or None.

    VFLO averages three: sales trend, EBITDA trend, and a long-term EPS
    growth CONSENSUS ESTIMATE. The third has no equivalent in a filing and
    is simply absent here.

    `min_components` is why this is a function and not a mean. A score
    built from one leg is not the same measurement as one built from
    three, and averaging whatever happens to be present would let a
    company with a single strong number outrank one measured on both.
    VFLO's own funnel requires a growth score to exist at all — "select
    the top 75 companies with the highest free cash flow yield THAT HAVE
    A GROWTH SCORE" — so absence is disqualifying there too.
    """
    vals = [_num(v) for v in component_ranks]
    have = [v for v in vals if v is not None]
    if len(have) < min_components:
        return None
    return round(sum(have) / len(have), 2)


# ═══════════════════════════════════════════════════════════════════
# THE FUNNEL
# ═══════════════════════════════════════════════════════════════════

REASONS = {
    "no_market_cap": "no market capitalisation",
    "too_small": f"market cap under ${MIN_MARKET_CAP/1e6:.0f}m",
    "too_little_revenue": f"revenue under ${MIN_REVENUE/1e6:.0f}m",
    "no_fcf": "no positive free cash flow",
    "ev_unusable": "enterprise value could not be used",
    "yield_fault": "free cash flow yield is not usable",
    "no_growth_score": "not enough growth components to score",
    "cut_on_yield": "did not make the free cash flow cut",
    "cut_on_growth": "made the yield cut but not the growth cut",
}


def measure(row: dict, basis: str = "ev") -> dict:
    """One company, measured on ONE stated basis. Never raises.

    `basis` is not a preference, it is the denominator every row in the
    cohort is being divided by, and it is a parameter because MIXING TWO
    BASES IN ONE RANKING IS THE BUG THIS SCREEN IS MOST LIKELY TO HAVE.

    The first version of this function fell back to market cap whenever
    debt was unfiled, which looked careful and was not: market cap is
    SMALLER than enterprise value for any company with net debt, so the
    fallback produced a HIGHER yield. On a board sorted by yield, the
    company that failed to file its debt outranked the identical company
    that filed. Same inputs, 8.0% against 7.02%.

    So the cohort picks one basis and rows that cannot supply it are
    measured, shown, and NOT RANKED — visible, with a reason, rather than
    quietly promoted or quietly dropped.

    Expects: ticker, name, market_cap, fcf, revenue, and optionally
    total_debt, cash, preferred, minority, plus growth component values
    under `growth_components`.
    """
    out = dict(row)
    out["reason"] = ""
    out["measured"] = False
    out["rankable"] = False

    mc = _num(row.get("market_cap"))
    if mc is None or mc <= 0:
        out["reason"] = REASONS["no_market_cap"]
        return out
    if mc < MIN_MARKET_CAP:
        out["reason"] = (f"market cap of ${mc/1e6:.0f}m is under the "
                         f"${MIN_MARKET_CAP/1e6:.0f}m floor — below it a "
                         f"yield sort ranks shells, because any cash flow "
                         f"at all divided by a tiny denominator wins")
        return out

    rev = _num(row.get("revenue"))
    if rev is not None and rev < MIN_REVENUE:
        out["reason"] = (f"revenue of ${rev/1e6:.1f}m is under the "
                         f"${MIN_REVENUE/1e6:.0f}m floor — market cap alone "
                         f"cannot tell a small business from a registrant "
                         f"with a filing agent")
        return out

    fcf = _num(row.get("fcf"))
    if fcf is None:
        out["reason"] = "free cash flow not filed"
        return out
    if fcf <= 0:
        out["reason"] = ("free cash flow is not positive — this screen "
                         "ranks companies BY their cash generation, so a "
                         "company without any is not a low-ranked member "
                         "of it, it is outside the question")
        return out

    ev_info = enterprise_value(mc, row.get("total_debt"), row.get("cash"),
                               row.get("preferred"), row.get("minority"))
    out["ev"] = ev_info["ev"]
    out["ev_complete"] = ev_info["complete"]
    out["ev_note"] = ev_info["reason"]

    # The denominator this cohort is being ranked on.
    if basis == "ev":
        if not ev_info.get("usable"):
            # Measured — we saw the company and know its cash flow — but
            # not comparable to rows divided by a real EV.
            out["measured"] = True
            out["basis"] = None
            out["reason"] = ev_info["reason"]
            return out
        denom = ev_info["ev"]
    elif basis == "market_cap":
        denom = mc
    else:
        raise Unmeasurable(f"unknown basis {basis!r}")

    # Two independent paths to the same quantity. When they disagree in
    # the direction that flatters, the row is not ranked.
    if basis == "ev":
        bogus = inferred_zero_fault(row.get("debt_inferred_zero"), mc)
        if bogus:
            out["measured"] = True
            out["basis"] = None
            out["reason"] = bogus
            return out
        disagree = debt_cross_check(
            (_num(row.get("total_debt")) or 0) - (_num(row.get("cash")) or 0),
            row.get("oracle_net_debt"))
        if disagree:
            out["measured"] = True
            out["basis"] = None
            out["reason"] = disagree
            return out

    out["basis"] = basis
    y = fcf_yield(fcf, denom)
    fault = yield_fault(y)
    if fault:
        out["measured"] = True
        out["reason"] = fault
        return out

    out["fcf_yield"] = y
    out["measured"] = True
    out["rankable"] = True
    return out


def choose_basis(rows: list) -> str:
    """The denominator the whole cohort will be ranked on.

    Enterprise value when ANY row can supply one, because those rows then
    form a board that is internally comparable and the rest are listed
    unranked with a reason. Market cap only when NOTHING can supply an EV
    — a pipeline with no debt or cash data at all, where every row shares
    the same denominator and the board is therefore still internally
    consistent, just measuring a different thing and labelled as such.

    What never happens is a board with some rows on each.
    """
    for r in rows:
        info = enterprise_value(r.get("market_cap"), r.get("total_debt"),
                                r.get("cash"), r.get("preferred"),
                                r.get("minority"))
        if info.get("usable"):
            return "ev"
    return "market_cap"


def screen(rows: list, fcf_take: float = FCF_TAKE,
           growth_take: float = GROWTH_TAKE, basis: str | None = None) -> dict:
    """The two-stage funnel. Returns every stage, not just the survivors.

    A board of fifty names out of an unstated universe cannot be argued
    with — the same file is produced by a strict screen over a rich market
    and by a parser that rejected everything. Every count below is
    reported so the shape of the funnel is visible, and every rejected
    company carries the reason it was rejected.

    THE ORDER IS THE METHOD. Yield first, then growth among the survivors.
    Ranking on a blend of the two instead would let a spectacular grower
    with a 1% yield into a screen whose entire premise is buying cash flow
    cheaply, and that is the fund this is not.
    """
    basis = basis or choose_basis(rows)
    measured, rejected = [], []
    for r in rows:
        m = measure(r, basis=basis)
        (measured if m["rankable"] else rejected).append(m)

    # ── growth scoring happens across the MEASURED cohort ──
    # Ranks are relative, so the cohort defines them. Scoring against the
    # whole input instead would rank a company against rows that were
    # thrown out for having no cash flow, which is not a peer group.
    comp_names = []
    for m in measured:
        for k in (m.get("growth_components") or {}):
            if k not in comp_names:
                comp_names.append(k)
    ranks_by_comp = {}
    for c in comp_names:
        ranks_by_comp[c] = percentile_ranks(
            [(m.get("growth_components") or {}).get(c) for m in measured])
    for i, m in enumerate(measured):
        per = {c: ranks_by_comp[c][i] for c in comp_names}
        m["growth_ranks"] = per
        m["growth_score"] = growth_score(list(per.values()))

    # ── stage 1: the free cash flow screen ──
    # VFLO requires a growth score to EXIST before the yield cut, so a
    # company that cannot be scored does not consume one of the 75 slots.
    scorable = [m for m in measured if m["growth_score"] is not None]
    unscorable = [m for m in measured if m["growth_score"] is None]
    for m in unscorable:
        m["reason"] = REASONS["no_growth_score"]

    by_yield = sorted(scorable, key=lambda m: m["fcf_yield"], reverse=True)
    n_fcf = max(1, round(len(by_yield) * fcf_take)) if by_yield else 0
    fcf_cut, missed_yield = by_yield[:n_fcf], by_yield[n_fcf:]
    for m in missed_yield:
        m["reason"] = REASONS["cut_on_yield"]

    # ── stage 2: the growth filter ──
    by_growth = sorted(fcf_cut, key=lambda m: m["growth_score"], reverse=True)
    n_growth = max(1, round(len(by_growth) * growth_take)) if by_growth else 0
    final, missed_growth = by_growth[:n_growth], by_growth[n_growth:]
    for m in missed_growth:
        m["reason"] = REASONS["cut_on_growth"]

    return {
        "final": final,
        "fcf_cut": fcf_cut,
        "measured": measured,
        "rejected": rejected + unscorable,
        "missed_yield": missed_yield,
        "missed_growth": missed_growth,
        "basis": basis,
        "census": {
            "input": len(rows),
            "measured": len(measured),
            "unmeasurable": len(rejected),
            "seen_but_unrankable": sum(1 for r in rejected if r["measured"]),
            "scorable": len(scorable),
            "unscorable": len(unscorable),
            "fcf_cut": len(fcf_cut),
            "final": len(final),
        },
        "stages": [
            stage_summary("Starting universe", scorable),
            stage_summary("Free cash flow screen", fcf_cut),
            stage_summary("Growth filter", final),
        ],
    }


def stage_summary(label: str, rows: list) -> dict:
    """Median yield and growth at one stage of the funnel.

    VFLO publishes exactly this table, which is what makes an
    implementation checkable rather than merely plausible: their funnel
    runs 3.21% -> 7.43% -> 7.39% on yield and 16.74% -> 9.51% -> 13.41%
    on growth. The SHAPE is the signature — yield roughly doubles at the
    first cut while growth falls, then the second cut restores growth at
    almost no cost in yield. A run of this module that does not reproduce
    that shape has a bug, whatever its absolute numbers look like on a
    different universe.

    Medians, not means: one 40% yield in a cohort of forty moves a mean
    enough to hide the stage it is describing.
    """
    ys = [r["fcf_yield"] for r in rows if _num(r.get("fcf_yield")) is not None]
    gs = [r["growth_score"] for r in rows
          if _num(r.get("growth_score")) is not None]
    # THE RANK AND THE RATE ARE NOT THE SAME COLUMN, and reporting only
    # the rank makes this table incomparable to VFLO's. A percentile rank
    # has a median of ~50 within its own cohort BY CONSTRUCTION, so the
    # starting universe always reads about 50 and the funnel looks flat
    # whatever it actually did. VFLO publishes a growth RATE — 16.74%
    # falling to 9.51%, recovering to 13.41% — so the underlying rate is
    # what has to sit beside it.
    #
    # On the first real run this was the difference between a stage that
    # appeared to do nothing (median rank 48.19 -> 62.97) and one that
    # more than doubled the sales trend of the names it kept
    # (4.69% -> 10.07%). Same stage, same rows, one of the two readings
    # useless.
    rates = {}
    for comp in ("sales_trend", "fcf_trend"):
        vals = [_num((r.get("growth_components") or {}).get(comp))
                for r in rows]
        vals = [v for v in vals if v is not None]
        rates[comp] = round(median(vals), 2) if vals else None
    return {
        "label": label,
        "count": len(rows),
        "median_fcf_yield": round(median(ys), 2) if ys else None,
        "median_growth_score": round(median(gs), 2) if gs else None,
        "median_growth_rates": rates,
    }


# ── VFLO's own published funnel, for comparison on the page ──────────
# From the 06/11/2026 factsheet rebalance. Kept here rather than in the
# template because it is data about the method, and because a number in
# a template is a number nobody tests.
VFLO_PUBLISHED = {
    "as_of": "2026-06-11",
    "stages": [
        {"label": "Starting universe", "fcf_yield": 3.21, "growth_rate": 16.74},
        {"label": "Free cash flow screen", "fcf_yield": 7.43, "growth_rate": 9.51},
        {"label": "Growth filter", "fcf_yield": 7.39, "growth_rate": 13.41},
        {"label": "Final index", "fcf_yield": 8.66, "growth_rate": 13.16},
    ],
    "note": ("VFLO's final stage lifts yield again by WEIGHTING the 50 "
             "holdings by free cash flow. This is a screen, not a fund — "
             "it stops at selection and does not weight, so there is no "
             "fourth stage to compare."),
}


# ── the market-cap basis flatters leverage, and by how much is knowable ──
#
# This is the sharpest edge on a board that cannot compute enterprise
# value. FCF / market cap and FCF / EV are the same number only for a
# company with no net debt. For a levered one the market cap is far
# smaller than the enterprise value, so the yield printed here is far
# HIGHER than the yield VFLO would compute — and it is highest for the
# most indebted companies, which is the worst direction for the error to
# run on a board sorted by yield.
#
# Bausch Health on the first real run: 40.5% on market cap, against about
# ten times its EBIT in net debt. Its enterprise-value yield is a
# fraction of that. Nothing is WRONG — the market cap is right and the
# cash flow is right — but a reader who takes 40.5% as "the FCF yield"
# has been misled by a denominator, and the row can say so itself.
LEVERAGE_FLATTERS_AT = 3.0          # net debt / EBIT


def leverage_note(nd_ebit, basis: str) -> str | None:
    """Why this row's yield is overstated, or None.

    Fires only on a market-cap basis: with a real enterprise value the
    debt is already in the denominator and there is nothing to warn about.
    """
    if basis != "market_cap":
        return None
    v = _num(nd_ebit)
    if v is None or v < LEVERAGE_FLATTERS_AT:
        return None
    return (f"carries about {v:.1f}x EBIT in net debt, and this yield is "
            f"computed on market cap rather than enterprise value — so it "
            f"is materially HIGHER than the enterprise-value yield VFLO "
            f"would report. The more levered the company, the larger the "
            f"overstatement, and on a board sorted by yield that pushes "
            f"the most indebted names toward the top.")


SIZE_BANDS = [
    ("mega", 200e9, None),
    ("large", 10e9, 200e9),
    ("mid", 2e9, 10e9),
    ("small", 300e6, 2e9),
    ("micro", 0, 300e6),
]


def size_band(market_cap) -> str | None:
    mc = _num(market_cap)
    if mc is None or mc <= 0:
        return None
    for name, lo, hi in SIZE_BANDS:
        if mc >= lo and (hi is None or mc < hi):
            return name
    return None


def below_large_cap(rows: list) -> list:
    """The rows a large-cap index could not hold — the reason to run this
    over the whole market rather than reading VFLO's holdings.

    VFLO's universe is the largest 400 US companies. Anything here in the
    mid, small or micro band is a name that passed the same two-stage test
    and is invisible to the index by construction, not by judgement.
    """
    return [r for r in rows if size_band(r.get("market_cap"))
            in ("mid", "small", "micro")]


# ═══════════════════════════════════════════════════════════════════
# FRESHNESS — is there anything new to say?
# ═══════════════════════════════════════════════════════════════════
# This build makes no network calls; it joins two files the repo already
# has. That is its strength and its one failure mode. It finishes in 13
# seconds while the compounders refresh it depends on takes 36 minutes,
# so an operator who dispatches both by hand in sequence gets a board
# built from the PREVIOUS month's inputs — silently, with a green tick.
# It happened on the first real run.
#
# THE OBVIOUS GUARD DOES NOT CATCH IT, and that is worth stating plainly
# because it was the first thing tried. "Refuse if the inputs are older
# than the snapshot on disk" sounds like the rule, but on that run the
# inputs were not older — they were IDENTICAL, because the snapshot on
# disk had been built from the very same unrefreshed files minutes
# earlier. Backward movement is a different fault, and a rarer one.
#
# What actually distinguishes a real rebuild from that one is whether
# ANYTHING changed: the inputs, or the code that reads them. If neither
# moved, re-running cannot produce a different board — it can only
# re-date the old one, which is the house failure exactly (a value nobody
# measured rendered as a confident number, here a month nobody measured
# printed as a fresh board). So:
#
#   an input went BACKWARD          fault — something is wrong, fail loudly
#   an input moved forward          build
#   nothing moved, logic changed    build  (the push trigger's whole case)
#   nothing moved, logic unchanged  skip   — say so, write nothing, exit 0
#
# The skip is not an error and must not be red: a monthly fallback cron
# firing against inputs that have not moved is a normal Tuesday. It is
# only ever wrong to write the file.

INPUT_KEYS = ("compounders", "schloss")


def _stamp(value):
    """Parse a source file's date stamp to an aware UTC datetime.

    The two files disagree on format — schloss writes a full
    '2026-09-07T13:45:58+00:00', compounders a bare '2026-09-15' — and a
    naive datetime cannot be compared against an aware one without
    raising TypeError. Normalising both to UTC makes the comparison a
    real one rather than a lexical accident that happens to work while
    the formats happen to match.

    A naive stamp is stamped UTC EXPLICITLY, not handed to astimezone(),
    which would read it as the runner's LOCAL time. Both do the same
    thing on a UTC runner, which is why the difference survives a
    mutation test unless the test sets TZ — and the tests do.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def refresh_verdict(current: dict, previous: dict | None) -> dict:
    """Should this build write a snapshot? -> {action, reason, moved}

    `current` and `previous` are {compounders, schloss, logic}; the first
    two are the source files' own date stamps, `logic` a content hash of
    the code that decides what the board says.

    action is "build", "skip" or "fault". Only "fault" is an error.

    UNVERIFIABLE IS NOT UNCHANGED. A stamp that is missing or unparseable
    cannot prove that nothing moved, so it does not earn a skip — the
    build proceeds and the snapshot records that freshness was not
    verified. Refusing instead would mean a source file that stops
    writing `as_of` silently stops the board forever, which trades a
    stale snapshot for no snapshot and is not obviously the better trade.
    """
    if not previous:
        return {"action": "build", "reason": "no previous snapshot to compare",
                "moved": {}, "verified": False}

    moved = {}
    for key in INPUT_KEYS:
        cur, prev = _stamp(current.get(key)), _stamp(previous.get(key))
        if cur is None or prev is None:
            moved[key] = None
        elif cur > prev:
            moved[key] = "newer"
        elif cur < prev:
            moved[key] = "older"
        else:
            moved[key] = "same"

    # Backward first: a fault outranks anything else that moved. A run
    # that reads one fresh input and one rolled-back one is not half
    # right, it is a board with a denominator from a different month.
    backward = [k for k in INPUT_KEYS if moved[k] == "older"]
    if backward:
        detail = ", ".join(
            f"{k} {previous.get(k)} -> {current.get(k)}" for k in backward)
        return {
            "action": "fault",
            "reason": (f"input went BACKWARD ({detail}). The snapshot on "
                       f"disk was built from newer data than this run can "
                       f"see, which means this checkout predates a refresh "
                       f"that already landed. Writing would overwrite good "
                       f"numbers with old ones."),
            "moved": moved, "verified": True,
        }

    forward = [k for k in INPUT_KEYS if moved[k] == "newer"]
    if forward:
        return {"action": "build", "moved": moved, "verified": True,
                "reason": f"fresh input: {', '.join(sorted(forward))}"}

    unverifiable = [k for k in INPUT_KEYS if moved[k] is None]
    if unverifiable:
        return {"action": "build", "moved": moved, "verified": False,
                "reason": (f"cannot verify freshness of "
                           f"{', '.join(sorted(unverifiable))} — no usable "
                           f"date stamp, so 'unchanged' cannot be proved")}

    cur_logic, prev_logic = current.get("logic"), previous.get("logic")
    if not prev_logic:
        return {"action": "build", "moved": moved, "verified": True,
                "reason": ("previous snapshot predates logic stamping, so "
                           "an unchanged screen cannot be proved")}
    if cur_logic != prev_logic:
        return {"action": "build", "moved": moved, "verified": True,
                "reason": "inputs unchanged but the screen's own code changed"}

    return {
        "action": "skip",
        "reason": ("nothing to rebuild: both inputs and the screen are "
                   "unchanged since the last snapshot. If you expected a "
                   "refresh to have landed, it has NOT — check the input "
                   "dates below against the job you dispatched."),
        "moved": moved, "verified": True,
    }
