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

# Scoreable from a companyfacts blob plus one bulk quote file.
SCOREABLE_IDS = (1, 2, 3, 4, 5, 11, 13)

# Never scoreable, for the reasons in the module docstring. These are not
# "missing data" — they are permanently outside what this page can see,
# and they are rendered differently from a company that simply did not
# file a tag.
NEVER_SCORED_IDS = (6, 7, 8, 9, 10, 12)

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
    6: "no free source; every proxy tested put Intel FY2015-19 in the top band",
    7: "ownership is in Forms 3/4/5, one request per company; pay proxies read "
       "Super Micro's $1 CEO salary as perfect alignment",
    8: "every proxy tested scored Tupperware 'Great' until it filed Chapter 11",
    9: "sell-side counts are licensed terminal data — and this criterion is "
       "INVERTED, so its absence removes the thesis's own reason a quality "
       "company would be cheap",
    10: "employee sentiment is licensed; SEC human-capital disclosure is prose",
    12: "needs analyst estimates; trailing PEG is a different quantity",
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

    passing = [c for c, b in bands.items() if b in PASS_BANDS]
    great = [c for c, b in bands.items() if b == "Great"]
    floors = sorted(bands, key=lambda c: BAND_ORDINAL[bands[c]])
    return {
        "criteria_total": CRITERIA,
        "measured_ids": sorted(scored),
        "measured_n": len(scored),
        "unmeasured_ids": sorted(unmeasured_ids),
        "never_scored_ids": list(NEVER_SCORED_IDS),
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
    return {
        "screened": len(rows),
        "listed": len(listed), "thin": len(thin), "rejected": len(rejected),
        "by_reason": dict(sorted(by_reason.items(), key=lambda kv: -kv[1])),
        "coverage": coverage,
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
