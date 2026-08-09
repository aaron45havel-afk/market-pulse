"""Special-situation triage: which filing should I read first?

This module does NOT tell you what to buy. It ranks candidates so the
reading happens in the right order, and it kills the ones that cannot pay
for the evening they would cost. Everything it produces is a component
you can inspect and argue with — there is deliberately no single blended
"score", because a composite built on a guessed completion probability
looks like a measurement and is not one.

THE FOUR THINGS IT GETS RIGHT THAT HAND ANALYSIS USUALLY GETS WRONG:

  1. It ranks on ANNUALIZED return, not headline spread. A 3% spread
     closing in 30 days is ~44%/yr; a 20% spread resolving over three
     years is ~6%/yr. The second one looks better and is not close.

  2. It subtracts the round-trip bid-ask BEFORE ranking. A 6% tender
     spread in a name with a 4% round trip is a 2% trade. This is where
     retail special-situations analysis usually dies, and it is worst in
     exactly the illiquid names these situations show up in.

  3. It applies TAX at the holding period and account type. A 60-day
     event is ordinary income. For a 15%-after-tax mandate, that means a
     two-month play needs roughly 25%+ gross to beat a boring long hold —
     and the same play is worth materially more inside an IRA. That
     difference is frequently the entire decision.

  4. It shows DOLLARS AT STAKE next to the percentage. An odd-lot tender
     is capped at 99 shares: 99 x $2 = $198. That can annualize to 60%
     and still be worth two hundred dollars. Rank on percentage alone and
     the queue fills with mathematically beautiful, economically
     irrelevant plays.
"""
from __future__ import annotations

import math

# ── What each filing means, how long it usually takes, and how often it
#    completes.
#
# THESE COMPLETION RATES ARE PRIORS, NOT MEASUREMENTS. They encode the
# structural differences that actually drive outcomes — whether cash is
# already on the balance sheet, whether a financing condition exists,
# whether the buyer controls the vote — and they are exposed on every
# result so they can be overridden per deal. They are never folded
# silently into a return figure.
CATALYSTS: dict[str, dict] = {
    "SC TO-I": {
        "label": "Issuer tender offer",
        "note": "Company buying its own shares. Check for an odd-lot provision — "
                "it's the one that lets a small account get filled in full.",
        "typical_days": 30, "completion": 0.95, "actionable": True,
    },
    "SC TO-T": {
        "label": "Third-party tender offer",
        "note": "Outside buyer. Financing and minimum-tender conditions are where these break.",
        "typical_days": 45, "completion": 0.85, "actionable": True,
    },
    "SC 13E-3": {
        "label": "Going private",
        "note": "Controlling holder taking it private. Frequently RE-CUT LOWER after "
                "the first offer — the initial price is a negotiating position.",
        "typical_days": 120, "completion": 0.75, "actionable": True,
    },
    "SC 13D": {
        "label": "Activist stake (>5%, active intent)",
        "note": "13D means intent to influence. Not 13G, which is passive — do not "
                "confuse them; only one is a catalyst.",
        "typical_days": 270, "completion": 0.40, "actionable": True,
    },
    "DEFM14A": {
        "label": "Merger vote scheduled",
        "note": "Definitive proxy. The vote date is a real deadline you can annualize against.",
        "typical_days": 45, "completion": 0.90, "actionable": True,
    },
    "10-12B": {
        "label": "Spin-off registration",
        "note": "Forced selling by index funds at separation is the classic entry.",
        "typical_days": 90, "completion": 0.85, "actionable": True,
    },
    "DEF 14A/LIQUIDATION": {
        "label": "Plan of liquidation / dissolution",
        "note": "Completes, but timelines slip badly — hard assets take longer to sell "
                "than the proxy says. Annualize on the LONG end of the estimate.",
        "typical_days": 400, "completion": 0.80, "actionable": True,
    },
    "4": {
        "label": "Insider open-market purchase",
        "note": "Only transaction code P is a buy. Grants and option exercises are noise.",
        "typical_days": 180, "completion": 0.50, "actionable": True,
    },
    # ── Warnings. Not opportunities; things you want to know FAST if held.
    "15": {
        "label": "Deregistering — going dark",
        "note": "Filing stops. Usually BAD if you hold it: you lose disclosure and "
                "most of the liquidity. This is an exit alarm, not an entry.",
        "typical_days": 90, "completion": 1.0, "actionable": False, "warning": True,
    },
    "8-K/4.01": {
        "label": "Auditor change",
        "note": "Sometimes routine, sometimes the first visible crack. Read the letter.",
        "typical_days": 0, "completion": 1.0, "actionable": False, "warning": True,
    },
    "NT 10-K": {
        "label": "Late annual filing",
        "note": "Cannot file on time. Read why before anything else.",
        "typical_days": 0, "completion": 1.0, "actionable": False, "warning": True,
    },
    "NT 10-Q": {
        "label": "Late quarterly filing",
        "note": "Same signal, smaller. Repeated NTs are a pattern.",
        "typical_days": 0, "completion": 1.0, "actionable": False, "warning": True,
    },
}

# An odd lot is fewer than 100 shares. The whole point of the provision is
# that odd-lot holders get filled in FULL, ahead of proration — which is
# why it's the one tender structure a small account can actually rely on,
# and why it's capped at an amount that will not change your life.
ODD_LOT_MAX_SHARES = 99

LONG_TERM_DAYS = 366        # US: MORE than one year for long-term treatment


def classify(form: str, text: str = "") -> dict | None:
    """Map an EDGAR form type to a catalyst. `text` catches the cases where
    the form alone is ambiguous — a DEF 14A is usually routine, and is only
    interesting when it carries a plan of liquidation."""
    f = (form or "").strip().upper()
    t = (text or "").lower()
    if f.startswith("DEF 14A") and any(k in t for k in ("plan of liquidation", "plan of dissolution", "dissolution")):
        return {"form": f, **CATALYSTS["DEF 14A/LIQUIDATION"]}
    if f.startswith("8-K") and "4.01" in t:
        return {"form": f, **CATALYSTS["8-K/4.01"]}
    for key, meta in CATALYSTS.items():
        if "/" in key:
            continue
        # Suffixes matter: amendments are "SC TO-I/A", and Form 15 is filed
        # as 15-12B / 15-12G / 15-15D and essentially NEVER as bare "15".
        # Matching only the exact string meant the going-dark alarm — the
        # most important warning for anyone holding micro-caps — could
        # never fire.
        if f == key or f.startswith((key + "/", key + " ", key + "-")):
            return {"form": f, **meta}
    return None


def annualized(spread_pct: float, days: float) -> float | None:
    """Compound a holding-period return to an annual rate.

    None when the horizon is unusable. Sub-one-day horizons are excluded
    deliberately: compounding a 2% gain over 0.5 days produces a number in
    the millions of percent, which is arithmetically true and completely
    useless for ranking.
    """
    if days is None or days < 1:
        return None
    base = 1 + (spread_pct / 100.0)
    if base <= 0:
        return None                     # a total loss does not annualize
    try:
        return round((base ** (365.0 / days) - 1) * 100, 1)
    except (OverflowError, ValueError):
        return None


def after_tax(gross_pct: float, days: float, account: str = "taxable",
              ordinary_rate: float = 0.37, ltcg_rate: float = 0.20) -> dict:
    """Apply the tax that the holding period actually implies.

    The reason this is not a footnote: a 60-day event is ordinary income.
    At a 37% marginal rate, a 25% gross return is 15.75% net — which is
    barely at a 15%-after-tax mandate, while the same 25% held 13 months
    nets 20%. The tax code is paying you to be slow, and any ranking that
    ignores it will systematically over-rate short events.
    """
    acct = account if account in ("taxable", "ira", "roth") else "taxable"
    if acct in ("ira", "roth"):
        rate = 0.0
    else:
        rate = ordinary_rate if (days is not None and days < LONG_TERM_DAYS) else ltcg_rate
    net = gross_pct * (1 - rate)
    return {
        "gross_pct": round(gross_pct, 2),
        "rate": rate,
        "net_pct": round(net, 2),
        "treatment": ("tax-deferred" if rate == 0 else
                      "short-term / ordinary" if rate == ordinary_rate else "long-term"),
        "account": acct,
    }


def evaluate(*, price: float, consideration: float, days: float,
             roundtrip_spread_pct: float = 0.0, shares: int | None = None,
             odd_lot: bool = False, account: str = "taxable",
             completion: float | None = None, downside: float | None = None,
             ordinary_rate: float = 0.37, ltcg_rate: float = 0.20) -> dict | None:
    """Score one situation. Returns components, never a blended number.

    `downside` is where you think it trades if the deal breaks. Supplying
    it turns the headline into an expected value instead of a best case,
    which for anything below ~90% completion is a different number
    entirely — and the deals that look most attractive on raw spread are
    usually the ones with the widest gap between the two.
    """
    p, c = _pos(price), _pos(consideration)
    if p is None or c is None:
        return None

    gross_spread = (c - p) / p * 100
    net_spread = gross_spread - max(0.0, roundtrip_spread_pct or 0.0)

    ann_gross = annualized(gross_spread, days)
    ann_net = annualized(net_spread, days)
    tax = after_tax(ann_net, days, account, ordinary_rate, ltcg_rate) if ann_net is not None else None

    # Capacity. The odd-lot cap is the point of the structure and the
    # reason a 60% annualized tender can be worth two hundred dollars.
    capped = ODD_LOT_MAX_SHARES if odd_lot else shares
    dollars = None
    if capped and capped > 0:
        dollars = round(capped * p * (net_spread / 100.0), 2)

    # Expected value, when a break price is supplied.
    prob = completion if completion is not None else None
    ev_pct = None
    if prob is not None and downside is not None:
        d = _pos(downside)
        if d is not None:
            loss = (d - p) / p * 100
            ev_pct = round(prob * net_spread + (1 - prob) * loss, 2)

    return {
        "price": p, "consideration": c, "days": days,
        "gross_spread_pct": round(gross_spread, 2),
        "trading_cost_pct": round(max(0.0, roundtrip_spread_pct or 0.0), 2),
        "net_spread_pct": round(net_spread, 2),
        "annualized_gross_pct": ann_gross,
        "annualized_net_pct": ann_net,
        "after_tax": tax,
        "annualized_after_tax_pct": (tax["net_pct"] if tax else None),
        "shares_assumed": capped,
        "odd_lot_capped": bool(odd_lot),
        "dollars_at_stake": dollars,
        "completion_assumed": prob,
        "downside": downside,
        "expected_value_pct": ev_pct,
    }


def triage(result: dict | None, *, target_after_tax_pct: float = 15.0,
           min_dollars: float = 2_000.0) -> dict:
    """Read first / worth a look / skip — and say WHY in one line.

    Two independent bars, because a play has to clear both to be worth an
    evening: the return has to beat what you'd get doing nothing clever,
    and the dollars have to justify the hours. Most alerts die on the
    second one, which is exactly the filter hand analysis never applies.
    """
    if not result:
        return {"verdict": "skip", "reason": "not enough information to price it"}

    ann = result.get("annualized_after_tax_pct")
    dollars = result.get("dollars_at_stake")
    reasons: list[str] = []

    if ann is None:
        return {"verdict": "skip", "reason": "no usable horizon — cannot annualize"}
    if result["net_spread_pct"] <= 0:
        return {"verdict": "skip",
                "reason": f"the {result['trading_cost_pct']}% round-trip spread eats "
                          f"the entire {result['gross_spread_pct']}% gap"}

    # Negative expected value is a HARDER rejection than "too small" and has
    # to be tested first. Reporting it as too_small would read as "good, just
    # not at your size" — which is exactly the wrong lesson to carry to the
    # same deal with more money behind it.
    ev = result.get("expected_value_pct")
    if ev is not None and ev <= 0:
        return {"verdict": "skip",
                "reason": f"negative expected value ({ev}%) once the break price is weighed — "
                          f"the {result['net_spread_pct']}% upside does not pay for the downside"}

    beats = ann >= target_after_tax_pct
    big_enough = dollars is None or dollars >= min_dollars

    if not beats:
        reasons.append(f"{ann}% after tax is below your {target_after_tax_pct}% bar")
    if not big_enough:
        reasons.append(f"only ${dollars:,.0f} at stake — under your ${min_dollars:,.0f} floor")

    if beats and big_enough and not reasons:
        return {"verdict": "read_first",
                "reason": f"{ann}% annualized after tax on ${dollars:,.0f}"
                          if dollars else f"{ann}% annualized after tax"}
    if beats and not big_enough:
        return {"verdict": "too_small", "reason": " · ".join(reasons)}
    if reasons:
        return {"verdict": "skip", "reason": " · ".join(reasons)}
    return {"verdict": "worth_a_look", "reason": f"{ann}% annualized after tax"}


def _pos(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f) or f <= 0:
        return None
    return f


def account_comparison(**kw) -> dict:
    """The same play, taxable vs tax-deferred.

    Surfaced because for short-duration events it is often the largest
    single lever available — larger than finding a better deal. If a lot
    of the edge is special situations, running them in a taxable account
    gives away a third of it before any analysis begins.
    """
    taxable = evaluate(**{**kw, "account": "taxable"})
    sheltered = evaluate(**{**kw, "account": "ira"})
    if not taxable or not sheltered:
        return {}
    gap = (sheltered["annualized_after_tax_pct"] or 0) - (taxable["annualized_after_tax_pct"] or 0)
    return {
        "taxable_pct": taxable["annualized_after_tax_pct"],
        "sheltered_pct": sheltered["annualized_after_tax_pct"],
        "gap_pct": round(gap, 2),
        "treatment": taxable["after_tax"]["treatment"],
    }
