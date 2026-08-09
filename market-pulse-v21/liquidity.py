"""Liquidity as a screening axis — turnover, and whether YOU can trade it.

The motivating research is Ibbotson, Chen, Kim & Hu, "Liquidity as an
Investment Style": sort stocks into size quartiles, then into turnover
quartiles WITHIN each size quartile, rebalance annually, equal weight.
The micro-cap / low-turnover cell returned ~16% geometric while the
micro-cap / HIGH-turnover cell returned ~0.1%. Two things follow, and
this module is built around both:

  1. The primary metric must be ANNUAL TURNOVER — trailing 12-month share
     volume divided by shares outstanding. Not dollar volume, which is
     mechanically correlated with market cap and would partly re-measure
     the size axis we already cut on. Turnover is unitless ("the share
     count changed hands N times this year"), which is exactly why it
     survives as a style independent of size.

  2. The finding is as much "avoid churned micro-caps" as "buy quiet
     ones". A screen that only rewards low turnover captures half of it;
     the top quartile is an EXCLUSION, and is treated as one here.

WHAT THE RESEARCH NUMBER DOES NOT INCLUDE, and why this module reports
more than turnover: that ~16% is gross of trading costs, on an equal-
weighted basket of ~345 names. Round-trip spread on a low-turnover
micro-cap is routinely 2-5%. So alongside the academic metric we compute
what decides whether a real person can build and exit a position —
median daily dollar volume, and the number of days the stock did not
trade at all. For this cohort, zero-trade days are often the truer
illiquidity signal: median daily volume is frequently literally zero.

A DENOMINATOR CAVEAT WORTH KNOWING: turnover here is on shares
OUTSTANDING, matching the research so the historical quartile returns
mean something. Micro-caps often carry 40-70% insider ownership, so a
closely-held company whose actual float trades briskly will still look
illiquid on this measure. Float would answer "can I trade it"; shares
outstanding answers "is this the cohort the study describes". We report
turnover on the study's basis and let the tradeability metrics speak to
the other question, rather than blending two different questions into
one number.
"""
from __future__ import annotations

import statistics

# Ibbotson's buckets are quartiles of the cross-section, not fixed
# thresholds — the cut points move with the market. We compute them from
# the universe each run, and only fall back to these when a caller has
# too few names to form meaningful quartiles. They are approximate
# annual-turnover levels observed in US micro-caps, not published
# constants, and are labelled as a fallback wherever they are used.
FALLBACK_CUTS = (0.35, 0.90, 2.20)

QUARTILE_NAMES = ("low", "mid_low", "mid_high", "high")

# A year of trading. Used to annualise when a ticker has a partial
# history (recent IPO, or a feed that returned a short window).
TRADING_DAYS = 252

# Below this many observed sessions we do not claim a turnover figure at
# all. A stock with three weeks of history cannot be placed in an annual
# turnover quartile without inventing the other eleven months.
MIN_SESSIONS = 60


def annual_turnover(volumes: list[float | None], shares_outstanding: float | None,
                    sessions_expected: int = TRADING_DAYS) -> dict | None:
    """Trailing-12-month turnover from a daily volume series.

    Returns None rather than a guess when the inputs cannot support the
    figure — no share count, or too little history. A screen that ranks
    on a fabricated turnover is worse than one that omits the name.

    Missing days (None) are treated as NOT TRADED rather than dropped:
    for an illiquid stock the absence of a print is the signal, and
    silently skipping those days would flatter the average.
    """
    if not shares_outstanding or shares_outstanding <= 0:
        return None
    clean = [float(v) if v is not None else 0.0 for v in (volumes or [])]
    clean = [v for v in clean if v >= 0]
    sessions = len(clean)
    if sessions < MIN_SESSIONS:
        return None

    total = sum(clean)
    zero_days = sum(1 for v in clean if v == 0)
    # Annualise a short window rather than understating a recent listing.
    scale = (sessions_expected / sessions) if sessions < sessions_expected else 1.0
    turnover = (total * scale) / shares_outstanding

    return {
        "turnover": round(turnover, 4),
        "total_volume": int(total),
        "sessions": sessions,
        "zero_days": zero_days,
        "zero_day_pct": round(zero_days / sessions * 100, 1),
        "median_daily_volume": int(statistics.median(clean)),
        "annualised": scale != 1.0,
        "shares_outstanding": int(shares_outstanding),
    }


def tradeability(turn: dict | None, price: float | None,
                 position_usd: float = 25_000.0) -> dict | None:
    """What the turnover figure does not tell you: whether you can get in
    and out at a size you actually intend to trade.

    days_to_build is deliberately measured against a share of median
    daily volume, not the whole day's volume — being the entire tape for
    a session is how you move the price against yourself.
    """
    if not turn or not price or price <= 0:
        return None
    mdv_shares = turn["median_daily_volume"]
    mdv_usd = mdv_shares * price
    # Assume you are willing to be at most 20% of a normal day's volume.
    participation = 0.20
    daily_capacity = mdv_usd * participation
    days = (position_usd / daily_capacity) if daily_capacity > 0 else None
    return {
        "median_daily_usd": int(mdv_usd),
        "position_usd": int(position_usd),
        "days_to_build": (round(days, 1) if days is not None else None),
        # Untradeable in any practical sense: no median print, or it would
        # take more than a month of being a fifth of the tape.
        "impractical": daily_capacity <= 0 or (days is not None and days > 21),
        "participation_assumed": participation,
    }


def quartile_cuts(turnovers: list[float]) -> tuple[float, float, float] | None:
    """The three cut points splitting a universe into turnover quartiles.

    Returns None when there are too few names to cut meaningfully — the
    caller then falls back to FALLBACK_CUTS and must say so in the UI.
    Twenty is not a magic number; it is the point below which quartiles
    of four or five names stop being a description of anything.
    """
    vals = sorted(v for v in turnovers if v is not None and v >= 0)
    if len(vals) < 20:
        return None
    q = statistics.quantiles(vals, n=4, method="inclusive")
    return (round(q[0], 4), round(q[1], 4), round(q[2], 4))


def bucket(turnover: float | None, cuts: tuple[float, float, float] | None) -> str | None:
    """Which turnover quartile a name falls in. 'low' is the quiet end —
    the cell the research favours."""
    if turnover is None:
        return None
    c = cuts or FALLBACK_CUTS
    if turnover <= c[0]:
        return "low"
    if turnover <= c[1]:
        return "mid_low"
    if turnover <= c[2]:
        return "mid_high"
    return "high"


# Size buckets. Ibbotson cuts size by cross-sectional quartile too, but
# for a screening tool the familiar dollar bands are more useful than a
# relative cut that shifts every run — a user asking for "micro-cap"
# means roughly this, not "the smallest quartile of whatever I loaded".
SIZE_BANDS = (
    ("nano", 0, 50_000_000),
    ("micro", 50_000_000, 300_000_000),
    ("small", 300_000_000, 2_000_000_000),
    ("mid", 2_000_000_000, 10_000_000_000),
    ("large", 10_000_000_000, float("inf")),
)


def size_bucket(market_cap: float | None) -> str | None:
    if not market_cap or market_cap <= 0:
        return None
    for name, lo, hi in SIZE_BANDS:
        if lo <= market_cap < hi:
            return name
    return "large"


def classify(rows: list[dict], within_size: bool = True) -> list[dict]:
    """Attach a turnover quartile to every row, cutting WITHIN each size
    bucket by default.

    Cutting within size is the whole point: the research forms turnover
    quartiles inside each size quartile, because turnover and size are
    correlated and a single global cut would put most micro-caps in the
    'low' bucket by construction. That would turn the liquidity axis into
    a second, noisier size axis and the effect would vanish.

    Each row needs 'turnover' and 'market_cap'; both may be None. Rows
    are returned with 'liq_bucket', 'size_bucket' and 'liq_cuts' added,
    and 'liq_cuts_fallback' flagged when the cohort was too small to cut.
    """
    out = [dict(r) for r in rows]
    for r in out:
        r["size_bucket"] = size_bucket(r.get("market_cap"))

    groups: dict[str | None, list[dict]] = {}
    for r in out:
        key = r["size_bucket"] if within_size else "_all"
        groups.setdefault(key, []).append(r)

    for key, members in groups.items():
        cuts = quartile_cuts([m.get("turnover") for m in members])
        fallback = cuts is None
        use = cuts or FALLBACK_CUTS
        for m in members:
            m["liq_bucket"] = bucket(m.get("turnover"), use)
            m["liq_cuts"] = use
            m["liq_cuts_fallback"] = fallback
            m["liq_cohort"] = key
            m["liq_cohort_n"] = len(members)
    return out


def passes(row: dict, max_bucket: str = "low", exclude_high: bool = True) -> bool:
    """Does this name clear the liquidity bar?

    Fail-closed on an unknown bucket: a name we could not measure is not
    quietly admitted to a screen whose entire premise is a measurement.

    exclude_high is separate from max_bucket on purpose. The research's
    strongest single result is that the HIGH-turnover micro-cap cell
    earned ~0.1%/yr — so even a user who widens the bar to 'mid_high'
    should still be excluding the churned end unless they turn that off
    deliberately.
    """
    b = row.get("liq_bucket")
    if b is None:
        return False
    if exclude_high and b == "high":
        return False
    order = {name: i for i, name in enumerate(QUARTILE_NAMES)}
    return order.get(b, 99) <= order.get(max_bucket, 0)


def coverage(rows: list[dict]) -> dict:
    """How much of a universe we can actually place. Surfaced in the UI so
    a short board reads as missing data rather than a market with no
    quiet stocks in it."""
    total = len(rows)
    measured = sum(1 for r in rows if r.get("turnover") is not None)
    placed = sum(1 for r in rows if r.get("liq_bucket"))
    return {
        "total": total,
        "measured": measured,
        "placed": placed,
        "unmeasured": total - measured,
        "pct": (round(measured / total * 100) if total else 0),
    }
