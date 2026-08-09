"""Cash-rich, low-debt, low-capex, high-margin, dividend-paying, cheap.

Seven tests, run over EDGAR fundamentals plus a price. Each returns a
pass/fail with the number behind it, because "this scored 71" is not a
reason to buy anything — "net cash is 34% of the market cap and it trades
at 8x earnings" is.

TWO RULES THIS MODULE HOLDS TO:

  Missing is not passing. A company that does not report capex has not
  demonstrated low capex. Every test returns None for "unknown", and an
  unknown never counts toward the score. The alternative — treating a
  missing field as zero — systematically flatters exactly the companies
  with the sparsest disclosure, which in a micro-cap universe is a large
  and adversely-selected group.

  A cheap price on collapsing earnings is not value. Low P/E is the
  easiest of these to satisfy for the worst reason, so it is scored
  alongside margin and balance-sheet strength rather than on its own,
  and a negative-earnings name is marked unknown rather than "infinitely
  expensive" or, worse, skipped so it can pass on the other six.
"""
from __future__ import annotations

# Defaults chosen to be demanding but not empty. Every one is a dial the
# caller can move; nothing here is a law of finance.
DEFAULTS = {
    "min_net_cash_pct": 10.0,    # net cash (cash - total debt) as % of market cap
    "max_debt_to_equity": 0.50,
    "max_capex_to_ocf": 0.25,    # capital-light: reinvestment eats little of the cash
    "min_operating_margin": 15.0,
    "min_dividend_yield": 1.5,
    "max_pe": 15.0,
    "max_pb": 2.0,
}

TESTS = ("net_cash", "debt", "capex", "margin", "dividend", "pe", "pb")

LABELS = {
    "net_cash": "Cash rich",
    "debt": "Low debt",
    "capex": "Low capex",
    "margin": "High margin",
    "dividend": "Pays a dividend",
    "pe": "Low P/E",
    "pb": "Low P/B",
}


def _num(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f and f not in (float("inf"), float("-inf")) else None


def net_cash_pct(cash, total_debt, market_cap) -> float | None:
    """Net cash as a share of the market cap. The interesting version of
    'cash rich': gross cash next to a pile of debt is not a fortress, and
    cash next to a huge market cap is a rounding error."""
    c, d, m = _num(cash), _num(total_debt), _num(market_cap)
    if c is None or m is None or m <= 0:
        return None
    return round((c - (d or 0.0)) / m * 100, 1)


def debt_to_equity(total_debt, equity) -> float | None:
    d, e = _num(total_debt), _num(equity)
    if d is None or e is None:
        return None
    # Negative equity is a real condition, not a low ratio. Report it as
    # unknown so it can never satisfy a "low debt" test by arithmetic
    # accident (a negative denominator flips the sign).
    if e <= 0:
        return None
    return round(d / e, 2)


def capex_to_ocf(capex, ocf) -> float | None:
    """How much of the cash the business generates goes straight back into
    keeping it running. Unknown when operating cash flow is negative —
    the ratio is meaningless against a negative denominator, and a
    cash-burning company is not capital-light."""
    c, o = _num(capex), _num(ocf)
    if c is None or o is None or o <= 0:
        return None
    return round(abs(c) / o, 2)


def operating_margin(operating_income, revenue) -> float | None:
    oi, rev = _num(operating_income), _num(revenue)
    if oi is None or rev is None or rev <= 0:
        return None
    return round(oi / rev * 100, 1)


def dividend_yield(dividends_per_share, price) -> float | None:
    d, p = _num(dividends_per_share), _num(price)
    if d is None or p is None or p <= 0:
        return None
    return round(d / p * 100, 2)


def pe_ratio(price, eps) -> float | None:
    """None for a loss-maker. A negative P/E is not a cheap one, and
    printing a negative number in a column sorted ascending would put
    every loss-making company at the top of a 'cheapest' list."""
    p, e = _num(price), _num(eps)
    if p is None or e is None or e <= 0:
        return None
    return round(p / e, 2)


def pb_ratio(price, book_value_per_share) -> float | None:
    p, b = _num(price), _num(book_value_per_share)
    if p is None or b is None or b <= 0:
        return None
    return round(p / b, 2)


def evaluate(f: dict, limits: dict | None = None) -> dict:
    """Run all seven tests over one company.

    `f` carries whatever EDGAR and the price feed produced; anything
    absent simply yields an unknown test. Returns per-test verdicts, the
    metric behind each, and a score that is honest about its own
    denominator — 4/5 with two unknowns is a different statement from
    4/7, and the UI shows both numbers rather than one percentage.
    """
    lim = {**DEFAULTS, **(limits or {})}
    price = _num(f.get("price"))
    mcap = _num(f.get("market_cap"))
    total_debt = _num(f.get("total_debt"))

    metrics = {
        "net_cash": net_cash_pct(f.get("cash"), total_debt, mcap),
        "debt": debt_to_equity(total_debt, f.get("equity")),
        "capex": capex_to_ocf(f.get("capex"), f.get("ocf")),
        "margin": operating_margin(f.get("operating_income"), f.get("revenue")),
        "dividend": dividend_yield(f.get("dividends_per_share"), price),
        "pe": pe_ratio(price, f.get("eps")),
        "pb": pb_ratio(price, f.get("book_value_per_share")),
    }

    # Direction differs per test: three want a floor, four want a ceiling.
    verdicts: dict[str, bool | None] = {
        "net_cash": None if metrics["net_cash"] is None else metrics["net_cash"] >= lim["min_net_cash_pct"],
        "debt": None if metrics["debt"] is None else metrics["debt"] <= lim["max_debt_to_equity"],
        "capex": None if metrics["capex"] is None else metrics["capex"] <= lim["max_capex_to_ocf"],
        "margin": None if metrics["margin"] is None else metrics["margin"] >= lim["min_operating_margin"],
        "dividend": None if metrics["dividend"] is None else metrics["dividend"] >= lim["min_dividend_yield"],
        "pe": None if metrics["pe"] is None else metrics["pe"] <= lim["max_pe"],
        "pb": None if metrics["pb"] is None else metrics["pb"] <= lim["max_pb"],
    }

    known = [k for k in TESTS if verdicts[k] is not None]
    passed = [k for k in known if verdicts[k]]
    unknown = [k for k in TESTS if verdicts[k] is None]

    return {
        "metrics": metrics,
        "verdicts": verdicts,
        "passed": len(passed),
        "known": len(known),
        "unknown": unknown,
        "passed_keys": passed,
        "failed_keys": [k for k in known if not verdicts[k]],
        # Share of the tests we could actually run. Reported next to the
        # raw count so a name that passes 3/3 with four unknowns can
        # never be mistaken for one that passes 7/7.
        "pct_of_known": (round(len(passed) / len(known) * 100) if known else None),
        "limits": lim,
    }


def meets(result: dict, min_passed: int = 5, min_known: int = 5,
          require: tuple[str, ...] = ()) -> bool:
    """Screening verdict.

    min_known exists because passing every test you happen to have data
    for is not an achievement when you only have two. A name has to be
    measurable on most of the criteria before it can clear a screen built
    on them.

    `require` names tests that must pass outright — for a dividend-income
    mandate you would pass ('dividend',) and no amount of cheapness
    substitutes for one that does not pay.
    """
    if result["known"] < min_known:
        return False
    for key in require:
        if not result["verdicts"].get(key):
            return False
    return result["passed"] >= min_passed


def summarize(result: dict) -> str:
    """A one-line reason, for the row itself. Numbers, not adjectives —
    the whole point is that the user can disagree with the screen."""
    m = result["metrics"]
    bits = []
    if m["pe"] is not None:
        bits.append(f"{m['pe']:g}x earnings")
    if m["pb"] is not None:
        bits.append(f"{m['pb']:g}x book")
    if m["net_cash"] is not None and m["net_cash"] > 0:
        bits.append(f"net cash {m['net_cash']:g}% of cap")
    if m["margin"] is not None:
        bits.append(f"{m['margin']:g}% op margin")
    if m["dividend"] is not None and m["dividend"] > 0:
        bits.append(f"{m['dividend']:g}% yield")
    return " · ".join(bits) if bits else "not enough reported data to judge"
