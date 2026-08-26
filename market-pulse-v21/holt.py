"""
The starting multiple is a choice. The growth is a bet.

UBS HOLT published a grid of median 5-year annualised excess returns for
US large caps 1990-2024, cut by starting Economic P/E (rows) against
5-year FORWARD sales growth (columns). Its headline:

    "It's hard to overcome a high starting multiple, even when future
     growth is robust. The average 50x multiple stock underperforms,
     even when you only sample companies that also delivered at least
     20% p.a. growth."

THE COLUMN AXIS CANNOT BE SCREENED ON. It is growth that has not
happened yet. A screen that sorts by it is sorting on hindsight, and
would have told you to buy the winners after they won. Everything below
exists to make that limitation explicit rather than to paper over it.

What this module does instead:

  1. Puts a company in a ROW from its multiple, which is knowable today.

  2. Treats the COLUMN as a probability distribution, not a fact —
     measured from how trailing growth actually rolled forward in our own
     universe over two DISJOINT five-year windows.

  3. Scores the expected excess return as the row weighted by that
     distribution. The result is what you can honestly expect from a
     starting multiple given what the company has done, not what you
     would have earned knowing what it went on to do.

The arithmetic makes HOLT's point sharper than the raw table does. Every
cell of the 50x+ row is negative after weighting, and so is every cell of
35-50x: the best available outcome at 50x — a company already compounding
20%+ that keeps doing it — is still negative. Reading DOWN a column spans
7-10 points; reading ACROSS a row spans about 4. Choosing the multiple is
worth roughly twice as much as guessing the growth right, and it is the
half you control.

TWO BIASES THAT BOTH RUN THE SAME WAY, and neither is corrected here
because correcting them would mean inventing a number:

  * HOLT's own footnote: the table includes only companies that survived
    the five subsequent years. Its excess returns are upper bounds.

  * Our transition matrix has the same shape of hole. It is measured on
    companies alive today with fifteen years of filings, so the 20%+
    grower that collapsed and delisted is not in it. The 36% persistence
    figure is an upper bound too.

Pure: no network, no database, no clock.
"""
from __future__ import annotations

# ── the grid, transcribed ────────────────────────────────────────────
# Rows: starting multiple. Columns: 5-year FORWARD sales CAGR.
# Values: median annualised excess return over the following 5 years.
# Source: UBS HOLT, largest 1000 US companies by TTM market cap, 1990-2024.
GROWTH_BANDS = ("neg", "0-5", "5-10", "10-15", "15-20", "20+")
GROWTH_LABELS = {
    "neg": "Negative", "0-5": "0-5%", "5-10": "5-10%",
    "10-15": "10-15%", "15-20": "15-20%", "20+": "20%+",
}
GRID = {
    "50x+":   (-12.7, -7.6, -4.8, -3.4, -2.0, -2.2),
    "35-50x": (-9.7, -4.4, -1.4, -1.1, -0.7, 2.8),
    "30-35x": (-7.9, -2.4, 0.1, 1.1, 2.3, 4.5),
    "25-30x": (-6.1, -2.4, 0.7, 0.9, 2.5, 4.6),
    "20-25x": (-6.6, -1.6, 0.9, 1.4, 3.2, 4.7),
    "15-20x": (-7.5, -2.1, 1.7, 3.3, 3.0, 6.9),
    "10-15x": (-7.1, -0.5, 2.0, 5.4, 6.7, 11.6),
    "0-10x":  (-6.4, 1.6, 3.0, 5.1, 11.2, 9.6),
    "negative": (-11.5, -7.0, -4.5, -6.2, -11.6, -9.5),
}
# Cheapest first — the order a reader should scan, and the order the
# board sorts in.
MULTIPLE_BANDS = ("0-10x", "10-15x", "15-20x", "20-25x", "25-30x",
                  "30-35x", "35-50x", "50x+", "negative")
_BAND_EDGES = ((10.0, "0-10x"), (15.0, "10-15x"), (20.0, "15-20x"),
               (25.0, "20-25x"), (30.0, "25-30x"), (35.0, "30-35x"),
               (50.0, "35-50x"))

# HOLT's rows are Economic P/E — a CFROI-based, inflation-adjusted,
# gross-asset measure that is proprietary and not reproducible from
# public filings. We stand P/FCF in its place. Both are cash-based, so
# the ordering carries; the BOUNDARIES DO NOT TRANSFER LITERALLY. A 50x
# Economic P/E is not a 50x P/FCF, and this is stated on the page rather
# than buried here.
MULTIPLE_PROXY = "P/FCF"


# ── refusing a multiple that cannot be true ──────────────────────────
#
# THIS IS THE LOAD-BEARING GUARD ON THIS WHOLE BOARD. The screen ranks by
# cheapness, so a multiple that is wrong-low does not sit harmlessly in
# the middle of the list — it sorts to number one. In the shipped
# compounders file, Booking Holdings carries 0.8x P/FCF and Cable One
# 0.8x; 31 companies sit under 2x and 29 sit below a fifth of their own
# fifteen-year median. Those are data faults wearing the costume of
# bargains, and without this guard they would be the entire top of the
# board.
MULTIPLE_FLOOR = 2.0
MULTIPLE_CEILING = 2000.0
# A current multiple this far below the company's own 15-year median is
# a fault signal, not a bargain. Real de-ratings of 80% happen; dozens at
# once, clustered in the names a screen would surface, do not.
MEDIAN_DIVERGENCE = 5.0
# ...but only when the median is itself a usable baseline. A fifteen-year
# median P/FCF of 1,280x (Inspire Medical) or 428x (Snap) does not mean
# the company was ever priced at 1,280x free cash flow — it means free
# cash flow was near zero through most of those years, so the ratio
# exploded. Comparing today's honest 21.9x against that would refuse the
# name for the crime of having finally started generating cash. Fifteen
# of the twenty-two names this guard first caught were this, not a fault.
MEDIAN_USABLE_MAX = 60.0


def multiple_fault(now, median=None) -> str | None:
    """Why this multiple cannot be used, or None if it can.

    Returns a reason rather than a boolean so the board can say what is
    wrong with a name instead of silently dropping it — a company that
    vanishes from a screen with no explanation is one the user will
    assume was never considered.
    """
    if now is None:
        return "no multiple"
    try:
        v = float(now)
    except (TypeError, ValueError):
        return "multiple is not a number"
    if v != v:
        return "multiple is not a number"
    if v < 0:
        return None                      # genuinely negative FCF — a real row
    if v < MULTIPLE_FLOOR:
        return (f"{v:.1f}x is below the {MULTIPLE_FLOOR:.0f}x floor — no going "
                f"concern trades there, so this is a data fault, and on a "
                f"screen that ranks by cheapness it would sort first")
    if v > MULTIPLE_CEILING:
        return f"{v:,.0f}x is beyond any usable range"
    if median:
        try:
            m = float(median)
        except (TypeError, ValueError):
            m = 0.0
        if 0 < m <= MEDIAN_USABLE_MAX and v < m / MEDIAN_DIVERGENCE:
            return (f"{v:.1f}x is under a fifth of its own {m:.1f}x "
                    f"fifteen-year median — a de-rating that large is "
                    f"possible, but it is far more often a bad price or a "
                    f"bad share count")
    return None


def multiple_band(now, median=None) -> str | None:
    """Which HOLT row a company sits in. None when the multiple is unusable."""
    if multiple_fault(now, median):
        return None
    v = float(now)
    if v < 0:
        return "negative"
    for edge, band in _BAND_EDGES:
        if v < edge:
            return band
    return "50x+"


def growth_band(cagr) -> str | None:
    """Which column a growth rate falls in."""
    if cagr is None:
        return None
    try:
        g = float(cagr)
    except (TypeError, ValueError):
        return None
    if g != g:
        return None
    if g < 0:
        return "neg"
    for edge, band in ((5.0, "0-5"), (10.0, "5-10"),
                       (15.0, "10-15"), (20.0, "15-20")):
        if g < edge:
            return band
    return "20+"


# ── how trailing growth actually rolls forward ───────────────────────
#
# Measured on 1,300 companies over two DISJOINT five-year windows: sales
# CAGR across years -10..-5 against years -5..0. Disjoint matters. The
# obvious version — comparing the stored 5-year and 10-year CAGRs —
# double-counts the most recent five years on both sides and inflates
# persistence badly: it reports 61% of 20%+ growers still growing 20%+,
# where the honest figure below is 36%.
#
# Recompute with transition_matrix() whenever the universe refreshes;
# these are the fallback and the thing the tests pin.
DEFAULT_TRANSITIONS = {
    "neg":   {"neg": 0.465,  "0-5": 0.2428, "5-10": 0.144,  "10-15": 0.0494, "15-20": 0.0453, "20+": 0.0535},
    "0-5":   {"neg": 0.2051, "0-5": 0.3933, "5-10": 0.2978, "10-15": 0.0534, "15-20": 0.0309, "20+": 0.0197},
    "5-10":  {"neg": 0.106,  "0-5": 0.3179, "5-10": 0.4106, "10-15": 0.096,  "15-20": 0.043,  "20+": 0.0265},
    "10-15": {"neg": 0.1007, "0-5": 0.2081, "5-10": 0.2886, "10-15": 0.2685, "15-20": 0.047,  "20+": 0.0872},
    "15-20": {"neg": 0.1325, "0-5": 0.0843, "5-10": 0.1928, "10-15": 0.3133, "15-20": 0.1325, "20+": 0.1446},
    "20+":   {"neg": 0.1317, "0-5": 0.0419, "5-10": 0.2275, "10-15": 0.1018, "15-20": 0.1377, "20+": 0.3593},
}
TRANSITION_N = 1300
MIN_BUCKET_N = 15          # below this a row is noise, not a probability


def early_cagr(rev_last, cagr5, cagr10):
    """Sales CAGR over years -10..-5, backed out of the two stored rates.

        rev_5  = rev_0 / (1+g5)^5
        rev_10 = rev_0 / (1+g10)^10
        early  = (rev_5 / rev_10)^(1/5) - 1

    None when the inputs cannot support it. Never raises: this runs over
    a whole universe and one pathological filer must not stop the board.
    """
    try:
        r0, g5, g10 = float(rev_last), float(cagr5), float(cagr10)
    except (TypeError, ValueError):
        return None
    if r0 <= 0:
        return None
    try:
        r5 = r0 / (1 + g5 / 100) ** 5
        r10 = r0 / (1 + g10 / 100) ** 10
    except (ZeroDivisionError, OverflowError, ValueError):
        return None
    if r5 <= 0 or r10 <= 0:
        return None
    try:
        e = ((r5 / r10) ** 0.2 - 1) * 100
    except (ZeroDivisionError, OverflowError, ValueError):
        return None
    return e if -100 < e < 500 else None


def transition_matrix(pairs) -> dict:
    """{from_band: {to_band: probability}} from (early, late) CAGR pairs.

    A band with fewer than MIN_BUCKET_N observations is OMITTED rather
    than published as a probability. Four companies do not make a base
    rate, and a row built from them would carry the same visual weight
    as one built from four hundred.
    """
    out = {}
    for src in GROWTH_BANDS:
        outs = [growth_band(b) for a, b in (pairs or [])
                if growth_band(a) == src]
        outs = [o for o in outs if o]
        if len(outs) < MIN_BUCKET_N:
            continue
        n = len(outs)
        out[src] = {c: outs.count(c) / n for c in GROWTH_BANDS}
    return out


# ── the score ────────────────────────────────────────────────────────
def expected_excess(m_band, g_band, transitions=None):
    """The HOLT row weighted by where growth actually goes. None if unknown.

    This is the whole point of the module. HOLT's raw +9.6% for a cheap
    20%+ grower assumes you KNOW it will grow 20%; weighted by the 36%
    chance it actually does, the honest expectation is about +5.4%.
    """
    if m_band not in GRID:
        return None
    t = (transitions or DEFAULT_TRANSITIONS).get(g_band)
    if not t:
        return None
    payoffs = GRID[m_band]
    return sum(t[c] * payoffs[i] for i, c in enumerate(GROWTH_BANDS))


# ── quality gates ────────────────────────────────────────────────────
#
# The grid alone surfaces junk. Ranked by expected excess return with no
# gates, the top of this board fills with negative-ROIC shells whose
# "growth" is a tiny base tripling (CorMedix at -195% ROIC, Nutex at
# -110%) and with Permian oil producers whose five-year sales CAGR is a
# commodity price, not a business compounding. HOLT's sample was the
# largest 1000 US companies; ours is not, so the gates do the work that
# their universe construction did.
GATES = (
    ("roic", "ROIC below 8% — the growth is not being funded by returns"),
    ("fcf_conv", "FCF conversion under 50% — the sales growth is not "
                 "reaching cash"),
    ("cyclical", "Flagged cyclical — a 5-year sales CAGR here is a price "
                 "cycle, not compounding"),
    ("profitable", "Losses in more than a third of the last 15 years"),
)
ROIC_MIN = 8.0
FCF_CONV_MIN = 50.0


def quality_flags(row) -> list[str]:
    """Reasons to distrust a high score. Empty means nothing objected."""
    r = row or {}
    out = []
    roic = r.get("roic_med")
    if roic is not None and roic < ROIC_MIN:
        out.append(f"ROIC {roic:.1f}% is below {ROIC_MIN:.0f}% — the growth "
                   f"is not being funded by returns")
    conv = r.get("fcf_conv")
    if conv is not None and conv < FCF_CONV_MIN:
        out.append(f"FCF conversion {conv:.0f}% — the sales growth is not "
                   f"reaching cash")
    if r.get("cyclical"):
        out.append("Flagged cyclical — a five-year sales CAGR here is a "
                   "price cycle, not compounding")
    seen, pos = r.get("ni_years_seen"), r.get("ni_pos_years")
    if seen and pos is not None and seen >= 5 and pos < seen * 2 / 3:
        out.append(f"Profitable in only {pos} of {seen} years")
    return out


def score(row, transitions=None) -> dict:
    """Everything the board shows for one company.

    Every key is present whatever happens, so a caller reading a field on
    a refused row gets None rather than a KeyError.
    """
    r = row or {}
    now, med = r.get("pfcf_now"), r.get("pfcf_med")
    fault = multiple_fault(now, med)
    m_band = multiple_band(now, med)
    g_band = growth_band(r.get("rev_cagr5"))
    ev = expected_excess(m_band, g_band, transitions)
    flags = quality_flags(r)
    t = (transitions or DEFAULT_TRANSITIONS).get(g_band) or {}

    return {
        "ticker": r.get("ticker"), "name": r.get("name"),
        "multiple": None if fault else (float(now) if now is not None else None),
        "multiple_band": m_band, "multiple_fault": fault,
        "multiple_median": med,
        "growth": r.get("rev_cagr5"), "growth_band": g_band,
        "expected_excess": None if ev is None else round(ev, 2),
        "grid_if_realised": (GRID[m_band][GROWTH_BANDS.index(g_band)]
                             if m_band in GRID and g_band else None),
        # The bet, stated as a probability rather than implied as a fact.
        "p_repeat": round(t.get(g_band, 0.0), 4) if g_band else None,
        "p_decline": round(sum(v for k, v in t.items()
                               if GROWTH_BANDS.index(k) < GROWTH_BANDS.index(g_band)),
                           4) if g_band and t else None,
        "flags": flags, "clean": not flags and not fault,
    }


def rank(rows, transitions=None, require_clean: bool = True,
         max_band: str | None = None) -> dict:
    """Score a universe and sort it. Refusals are RETURNED, not dropped.

    `max_band` caps the multiple — the one axis a buyer actually chooses.
    `refused` carries every name the multiple guard rejected, because a
    board that silently omits them looks like a board that considered
    them and found them wanting.
    """
    scored, refused, flagged = [], [], []
    allowed = None
    if max_band:
        if max_band not in MULTIPLE_BANDS:
            raise ValueError(f"unknown band {max_band!r}")
        allowed = set(MULTIPLE_BANDS[:MULTIPLE_BANDS.index(max_band) + 1])
        allowed.discard("negative")

    for r in rows or []:
        s = score(r, transitions)
        if s["multiple_fault"]:
            refused.append(s)
            continue
        if s["expected_excess"] is None:
            continue
        if allowed and s["multiple_band"] not in allowed:
            continue
        (flagged if s["flags"] else scored).append(s)

    scored.sort(key=lambda s: (-s["expected_excess"], s["multiple"] or 0))
    flagged.sort(key=lambda s: (-s["expected_excess"], s["multiple"] or 0))
    return {
        "rows": scored if require_clean else scored + flagged,
        "clean": scored, "flagged": flagged, "refused": refused,
        "counts": {"scored": len(scored) + len(flagged), "clean": len(scored),
                   "flagged": len(flagged), "refused": len(refused)},
    }


def grid_census(scored) -> dict:
    """How many companies sit in each cell — the board's own shape."""
    out = {}
    for s in scored or []:
        mb, gb = (s or {}).get("multiple_band"), (s or {}).get("growth_band")
        if mb and gb:
            out[f"{mb}|{gb}"] = out.get(f"{mb}|{gb}", 0) + 1
    return out
