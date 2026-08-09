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
import re

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
    #
    # Form 15 is THREE different events wearing one number, and collapsing
    # them overstates the alarm on the two common ones. 12(b) is the
    # exchange registration: withdrawing it is the delisting, the real
    # going-dark event. 12(g) and 15(d) are reporting obligations that
    # frequently attach to a secondary class — notes, warrants, a SPAC's
    # units — so an NYSE-listed company can file a 15-12G with its common
    # stock entirely unaffected. Labelling that "going dark" cries wolf.
    "15-12B": {
        "label": "Deregistering from the exchange — going dark",
        "note": "Withdrawing the exchange registration. This is the real one: filing "
                "stops, you lose disclosure and most of the liquidity. Exit alarm.",
        "typical_days": 90, "completion": 1.0, "actionable": False, "warning": True,
    },
    "15-12G": {
        "label": "Deregistering a 12(g) class",
        "note": "Often a SECONDARY class — notes, warrants, SPAC units — not the listed "
                "common. Check which security before reacting. Alarming only if it is the stock.",
        "typical_days": 90, "completion": 1.0, "actionable": False, "warning": True,
    },
    "15-15D": {
        "label": "Suspending 15(d) reporting",
        "note": "Reporting duty from a registration statement lapses. Usually a class that "
                "was never exchange-listed. Read which security it covers.",
        "typical_days": 90, "completion": 1.0, "actionable": False, "warning": True,
    },
    "15F": {
        "label": "Foreign issuer deregistering",
        "note": "A foreign private issuer withdrawing from US reporting. ADR holders lose "
                "disclosure and usually the listing with it.",
        "typical_days": 90, "completion": 1.0, "actionable": False, "warning": True,
    },
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
    # LONGEST KEY WINS. Suffixes matter: amendments are "SC TO-I/A", and
    # Form 15 is filed as 15-12B / 15-12G / 15-15D and essentially NEVER as
    # bare "15" — matching only the exact string meant the going-dark alarm
    # could never fire at all. But once the specific variants exist, a
    # shortest-first scan would let the generic "15" swallow every one of
    # them and re-flatten the distinction. Specificity has to be ordered
    # explicitly; dict order is not a safe proxy for it.
    for key in sorted((k for k in CATALYSTS if "/" not in k), key=len, reverse=True):
        if f == key or f.startswith((key + "/", key + " ", key + "-")):
            return {"form": f, **CATALYSTS[key]}
    return None


# ── Form 4: the transaction code IS the signal ──────────────────────
#
# The daily index tells you a Form 4 exists. It does not tell you what
# happened — and "an insider filed a Form 4" is not information. The large
# majority of Form 4s are restricted stock vesting, options being
# exercised, shares withheld to pay the tax on a grant, and scheduled
# 10b5-1 sales. Exactly one code means a person chose to buy stock with
# their own money at the prevailing price.
#
# So the queue has to OPEN the filing. Classifying Form 4s from the index
# alone publishes noise under a caption claiming it is a purchase, which
# is worse than publishing nothing: it looks like a filter ran.
TXN_CODES: dict[str, str] = {
    "P": "Open-market or private purchase",
    "S": "Open-market or private sale",
    "A": "Grant, award or other acquisition from the issuer",
    "D": "Disposition back to the issuer",
    "F": "Shares withheld to cover tax on a grant",
    "M": "Exercise or conversion of a derivative held",
    "X": "Exercise of an in- or at-the-money option",
    "C": "Conversion of a derivative security",
    "G": "Bona fide gift",
    "V": "Transaction voluntarily reported early",
    "J": "Other (explained in a footnote)",
    "K": "Equity swap or similar",
    "U": "Tendered into a merger or acquisition",
    "W": "Acquired or disposed by will or inheritance",
    "I": "Discretionary transaction",
    "E": "Expiration of a short derivative position",
    "H": "Expiration of a long derivative position",
    "O": "Out-of-the-money option exercise",
    "L": "Small acquisition",
    "Z": "Deposit into or withdrawal from a voting trust",
}

BUY_CODE = "P"          # the only code that is somebody buying stock

# ── Sanity bounds on the arithmetic ─────────────────────────────────
#
# The first live run put "Reborn Coffee, Inc. — $23,649,660,000" at the
# top of the board: 131,387 shares at exactly $180,000.00 each, in a stock
# that trades near a dollar. The filer had put the AGGREGATE value in the
# per-share field. Nothing in the filing is malformed, so no parser can
# catch it by being stricter — the only defence is checking whether the
# result describes a trade a human could have made.
#
# These bounds are deliberately far outside the real range (median implied
# price on that same board was $15.25, second-highest $370.79). They are
# not a view on what is expensive. Above them, the arithmetic has stopped
# describing a purchase, and the row is marked for checking rather than
# dropped — a filter you cannot see is indistinguishable from a bug.
SUSPECT_PRICE_ABOVE = 10_000.0          # only BRK.A trades above this
SUSPECT_DOLLARS_ABOVE = 5_000_000_000.0  # larger than any real insider buy

# Form 4 uses these where an issuer has no trading symbol. Rendering them
# verbatim puts the literal word NONE in a ticker column.
PLACEHOLDER_TICKERS = frozenset({"NONE", "N/A", "NA", "N.A.", "-", "--", "NULL"})


def implausible(shares: float | None, dollars: float | None) -> str | None:
    """Why this doesn't look like a trade — or None if it does.

    Returns a sentence meant to be shown to the reader, because the point
    is to send them to the filing, not to make the row disappear.
    """
    if not shares or not dollars or shares <= 0 or dollars <= 0:
        return None                     # unknown is handled elsewhere
    pps = dollars / shares
    if pps > SUSPECT_PRICE_ABOVE:
        return (f"implied price of ${pps:,.2f} a share — almost certainly the "
                f"aggregate value reported in the per-share field")
    if dollars > SUSPECT_DOLLARS_ABOVE:
        return f"${dollars:,.0f} is larger than any real insider purchase"
    return None

_OWNERSHIP_DOC = re.compile(r"<ownershipDocument>.*?</ownershipDocument>", re.S | re.I)

# Issuer tender offers from interval funds, non-traded BDCs and similar
# vehicles are SCHEDULED REPURCHASES, not events. They buy back a slice of
# shares at net asset value on a calendar, quarter after quarter. There is
# no premium to capture and usually no exchange to buy them on — and there
# are enough of them to bury every real tender in the queue. Scoped to
# SC TO-I on purpose: a fund appearing in a merger vote or a third-party
# tender is a genuinely different event and stays.
_FUND_VEHICLE = re.compile(r"\b(funds?|bdc|sicav|ucits)\b", re.I)


def is_routine_repurchase(form: str, company: str) -> bool:
    """True for the quarterly at-NAV repurchase offers that flood SC TO-I.

    Word-boundary matching matters: "Fundamental Industries" is a company,
    "Ares Private Markets Fund" is a calendar entry.
    """
    f = (form or "").strip().upper()
    if not f.startswith("SC TO-I"):
        return False
    return bool(_FUND_VEHICLE.search(company or ""))


def _val(el) -> str:
    """Form 4 XML wraps most leaves in <value>, but not all of them —
    transactionCode is bare inside transactionCoding. Handle both, and
    ignore the <footnoteId> siblings that sit alongside real values."""
    if el is None:
        return ""
    inner = el.find("value")
    text = (inner.text if inner is not None else el.text) or ""
    return text.strip()


def _flag(el) -> bool:
    v = _val(el).lower()
    return v in ("1", "true", "y", "yes")


def _num(el) -> float | None:
    try:
        return float(_val(el).replace(",", ""))
    except (TypeError, ValueError):
        return None


def parse_form4(raw: str) -> dict | None:
    """Pull issuer, owners and transactions out of a Form 4 submission.

    Takes the full .txt submission (SGML wrapper and all) or a bare XML
    document. Returns None if there is no ownership document in it.

    THE ISSUER IS THE COMPANY. EDGAR's daily index emits one row per filer
    CIK, and a Form 4 has the issuer plus every reporting owner as filers —
    so a fund group reporting through ten entities appears as ten rows with
    the LP names in the company column. Reading the document is the only
    way to know that "ROSS SCOTT I" and nine Hill Path partnerships are one
    purchase in one company.
    """
    import xml.etree.ElementTree as ET

    m = _OWNERSHIP_DOC.search(raw or "")
    if not m:
        return None
    try:
        root = ET.fromstring(m.group(0))
    except ET.ParseError:
        return None

    issuer = root.find("issuer")
    owners = []
    for o in root.findall("reportingOwner"):
        ident, rel = o.find("reportingOwnerId"), o.find("reportingOwnerRelationship")
        title = _val(rel.find("officerTitle")) if rel is not None else ""
        roles = []
        if rel is not None:
            if _flag(rel.find("isOfficer")):
                roles.append(title or "Officer")
            if _flag(rel.find("isDirector")):
                roles.append("Director")
            if _flag(rel.find("isTenPercentOwner")):
                roles.append("10% owner")
            if _flag(rel.find("isOther")):
                roles.append(_val(rel.find("otherText")) or "Other")
        owners.append({
            "name": _val(ident.find("rptOwnerName")) if ident is not None else "",
            "cik": (_val(ident.find("rptOwnerCik")) if ident is not None else "").lstrip("0"),
            "roles": roles,
        })

    txns = []
    for table, derivative in (("nonDerivativeTable", False), ("derivativeTable", True)):
        node = root.find(table)
        if node is None:
            continue
        tag = "nonDerivativeTransaction" if not derivative else "derivativeTransaction"
        for t in node.findall(tag):
            coding, amounts = t.find("transactionCoding"), t.find("transactionAmounts")
            if coding is None or amounts is None:
                continue
            code = _val(coding.find("transactionCode")).upper()
            ad = _val(amounts.find("transactionAcquiredDisposedCode")).upper()
            txns.append({
                "code": code,
                "meaning": TXN_CODES.get(code, "Unrecognized code"),
                "shares": _num(amounts.find("transactionShares")),
                "price": _num(amounts.find("transactionPricePerShare")),
                "acquired": ad == "A",
                "date": _val(t.find("transactionDate")),
                "security": _val(t.find("securityTitle")),
                "derivative": derivative,
            })

    ticker = _val(issuer.find("issuerTradingSymbol")).upper() if issuer is not None else ""
    if ticker in PLACEHOLDER_TICKERS:
        ticker = ""                     # an issuer with no symbol has no symbol
    return {
        "issuer": _val(issuer.find("issuerName")) if issuer is not None else "",
        "issuer_cik": (_val(issuer.find("issuerCik")) if issuer is not None else "").lstrip("0"),
        "ticker": ticker,
        "owners": owners,
        "transactions": txns,
    }


def insider_buy(doc: dict | None) -> dict | None:
    """Reduce a parsed Form 4 to a purchase, or None if it isn't one.

    Two rules carry the weight:

      * CODE P AND ACQUIRED. A code-P line disposing shares is not a buy,
        and derivative-table entries are excluded — an option grant priced
        at a nominal strike is not a person buying stock, however it codes.

      * A MISSING PRICE IS UNKNOWN, NOT ZERO. Form 4 leaves the price blank
        on some purchases, and treating that as $0 would silently sort a
        real buy to the bottom of a dollar ranking. The shares are reported
        separately as unpriced so the gap is visible rather than absorbed.
    """
    if not doc:
        return None
    buys = [t for t in doc.get("transactions", [])
            if t["code"] == BUY_CODE and t["acquired"] and not t["derivative"]]
    if not buys:
        return None

    dollars = 0.0
    shares = unpriced = 0.0
    for t in buys:
        s = t["shares"] or 0.0
        shares += s
        if t["price"] and t["price"] > 0:
            dollars += s * t["price"]
        else:
            unpriced += s
    if shares <= 0:
        return None

    roles = [r for o in doc.get("owners", []) for r in o["roles"]]
    total = round(dollars, 2) if dollars > 0 else None
    priced_shares = shares - unpriced
    return {
        "shares": round(shares, 2),
        "dollars": total,
        "unpriced_shares": round(unpriced, 2),
        "fully_priced": unpriced == 0,
        # Shown on the page beside the total, because the total alone hides
        # the one error a reader could otherwise catch instantly.
        "price_per_share": (round(dollars / priced_shares, 4)
                            if total and priced_shares > 0 else None),
        "suspect": implausible(priced_shares, total),
        "buyers": [o["name"] for o in doc.get("owners", []) if o["name"]],
        "roles": roles,
        "by_insider": any(r for r in roles if r not in ("10% owner",)),
        "dates": sorted({t["date"] for t in buys if t["date"]}),
    }


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
