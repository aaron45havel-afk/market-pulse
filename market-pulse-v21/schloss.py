"""Walter Schloss's method, as far as filings alone can carry it.

Schloss ran 1955-2002 at roughly 21% gross, about 15.3% net to his limited
partners, against ~10% for the index. The gross figure is the one that
circulates; the net one is what an investor actually received, and it is
the honest comparison for anyone with a 15%-after-fees target.

WHAT HE DID, in his own words, reduced to what a filing can answer:

    "companies with real assets with little or no debt"
    "20% or more discount to book value ... cash, fixed assets, and
     other tangibles"
    "a good dividend yield"
    "managements that own a lot of stock"
    "honest management that does not overpay itself"
    "buy stocks that manufactured products ... long histories (20+ years)"
    "do not be afraid to hold cash"
    "buying after a dividend cut"

THIS MODULE DELIBERATELY TAKES NO PRICE. Everything here comes out of SEC
filings, which are free, official and complete. Price-dependent tests —
the 52-week low he started from, price/tangible book, price/NCAV, the
discount itself — are genuinely blocked on a market-data feed, and are
handled by returning UNKNOWN rather than a guess. A screen that quietly
substitutes an assumption for a missing price is worse than one that says
it cannot answer.

That split is not a compromise. The balance-sheet half is most of the
method and the half Schloss considered the real work; the price half is
the trigger, not the thesis.

TWO PLACES THIS DISAGREES WITH THE COMPOUNDERS SCREEN, ON PURPOSE:

  * Compounders gates on ROIC, growth consistency and cash conversion.
    Schloss ignored all three. He bought businesses that would fail every
    quality gate, because he was buying the assets, not the engine. The
    two screens must never be blended: the average of them is a portfolio
    neither approach would hold.

  * Compounders CAPS a high dividend yield, on the reasoning that the
    market is pricing a cut. Schloss bought AFTER the cut, when the
    overreaction had already happened. Same data, opposite conclusion —
    and the discriminator is simply whether the cut is in the past.
"""
from __future__ import annotations

import json
from pathlib import Path

_DATA_PATH = Path(__file__).resolve().parent / "data" / "schloss.json"

# ── Thresholds (his numbers where he gave them) ──────────────────────
BOOK_DISCOUNT_MIN = 0.20       # "20% or more discount to book value"
NCAV_MAX_PRICE = 2 / 3         # Graham: buy at <= 2/3 of net current assets
DEBT_TO_EQUITY_MAX = 0.50      # "little or no debt", "avoid debt like the plague"
SURVIVAL_YEARS_MIN = 20        # "long histories of operations (20+ years)"

# "honest management that does not overpay itself". Stock compensation is
# the machine-readable part of that: it is a real transfer from owners to
# managers, it is tagged in XBRL, and paired with a rising share count it
# is the modern version of the self-dealing he watched for. These are
# generous — the aim is to catch the egregious, not to police normal pay.
SBC_TO_REVENUE_MAX = 0.10      # 10% of revenue paid in stock
DILUTION_MAX = 2.0             # share count growing >2%/yr
INSIDER_ALIGNED_MIN = 0.10     # "managements that own a lot of stock"


def _num(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f and abs(f) != float("inf") else None


def tangible_book(equity, goodwill=None, intangibles=None) -> float | None:
    """Book value with the un-sellable parts removed.

    Schloss defined book as "cash, fixed assets, and other tangibles" — a
    liquidation-flavoured figure. Goodwill is the accounting residue of
    prices paid for past acquisitions; it backs nothing and realises
    nothing. A company at 0.9x reported book and 3x tangible book is not
    a Schloss stock, and without this subtraction the two are
    indistinguishable.

    Goodwill and intangibles missing is treated as ZERO, not unknown:
    companies that carry neither routinely omit the tags, and refusing to
    score them would silently drop exactly the asset-heavy industrials
    this screen exists to find.
    """
    e = _num(equity)
    if e is None:
        return None
    return e - (_num(goodwill) or 0.0) - (_num(intangibles) or 0.0)


def asset_backing(cash=None, receivables=None, inventory=None, ppe=None,
                  goodwill=None, intangibles=None) -> dict:
    """What the book value actually consists of.

    He wanted to know what he was buying, not merely that it was cheap.
    Cash realises at 100%, receivables mostly, inventory sometimes, plant
    rarely at carrying value, goodwill never. Reported as shares of the
    named total so a reader can apply their own haircuts rather than
    inherit ours — no liquidation discounts are assumed here.
    """
    parts = {"cash": _num(cash) or 0.0, "receivables": _num(receivables) or 0.0,
             "inventory": _num(inventory) or 0.0, "ppe": _num(ppe) or 0.0,
             "goodwill": _num(goodwill) or 0.0,
             "intangibles": _num(intangibles) or 0.0}
    total = sum(parts.values())
    if total <= 0:
        return {"total": None, "mix": {}, "hard_pct": None}
    mix = {k: round(v / total * 100, 1) for k, v in parts.items()}
    # "Hard" = cash and receivables: the part that becomes money without
    # finding a buyer for a factory.
    hard = (parts["cash"] + parts["receivables"]) / total * 100
    return {"total": round(total, 2), "mix": mix, "hard_pct": round(hard, 1)}


def ncav(current_assets, total_liabilities, preferred=None) -> float | None:
    """Net current asset value: current assets less ALL liabilities.

    Graham's original, and the one Schloss started from. Note it charges
    every liability against current assets alone and gives no credit at
    all for plant — which is why it is a liquidation floor rather than a
    valuation.
    """
    ca, tl = _num(current_assets), _num(total_liabilities)
    if ca is None or tl is None:
        return None
    return ca - tl - (_num(preferred) or 0.0)


def debt_ok(short_term_debt, long_term_debt, equity,
            total_liabilities=None) -> tuple[bool | None, float | None]:
    """(passes, ratio). "Avoid debt like the plague."

    Negative equity returns (False, None): the ratio is undefined but the
    verdict is not — a company owing more than it owns has no margin of
    safety to buy, which is the entire point of the test.

    NEITHER DEBT TAG PRESENT IS NOT ZERO DEBT. Companies with no borrowings
    routinely omit the tags, so the tempting reading is "absent means
    none" — but that is precisely how a leveraged company walks through
    the one gate built to stop it. Instead of assuming, this uses the
    bound that always exists: debt can never exceed total liabilities. If
    the ENTIRE liability stack sits inside the limit, no-debt is proved
    rather than presumed, and the ratio is returned as unknown because we
    genuinely did not measure it. If it does not, the answer is unknown
    and the row says so.
    """
    e = _num(equity)
    if e is None:
        return None, None
    if e <= 0:
        return False, None

    st, lt = _num(short_term_debt), _num(long_term_debt)
    if st is None and lt is None:
        tl = _num(total_liabilities)
        if tl is None:
            return None, None
        return (True, None) if tl / e <= DEBT_TO_EQUITY_MAX else (None, None)

    ratio = round(((st or 0.0) + (lt or 0.0)) / e, 3)
    return ratio <= DEBT_TO_EQUITY_MAX, ratio


def dividend_history(per_share_by_year: dict) -> dict:
    """Does it pay, and has it just cut?

    Two different signals from one series, and the screen wants both.

    PAYING is a governance check, not a yield: "I like the idea of
    company-paid dividends, because I think it makes management a little
    more aware of stockholders." A cheque that leaves the building every
    quarter is a constraint on self-dealing.

    A CUT is an entry signal — "investors usually overreact to dividend
    cuts which provides a golden opportunity". This is the exact inverse
    of the cap in compounders.py, and both are right: before a cut, a
    swollen yield is the market forecasting the cut; after one, the
    forced selling is done and the reason is public.
    """
    series = {}
    for y, v in (per_share_by_year or {}).items():
        try:
            yr, val = int(y), _num(v)
        except (TypeError, ValueError):
            continue
        if val is not None and val >= 0:
            series[yr] = val
    if not series:
        return {"pays": None, "cut": None, "latest": None, "cut_pct": None,
                "years": 0, "from_year": None, "to_year": None}

    years = sorted(series)
    latest = series[years[-1]]
    prior = series[years[-2]] if len(years) > 1 else None
    # A zero base makes the PERCENTAGE undefined; it does not make the
    # verdict undefined. Nothing to nothing is not a cut, and neither is
    # initiating a dividend — reporting those as unknown would put every
    # non-payer in the "may have just cut" bucket this screen shops in.
    # (`cut` is only ever True when prior > 0, so the division is safe.)
    cut = cut_pct = None
    if prior is not None:
        cut = latest < prior
        cut_pct = round((prior - latest) / prior * 100, 1) if cut else 0.0
    # The two years compared are reported so the caller can judge
    # recency itself. A 2019-to-2025 pair is a real decline but not the
    # fresh overreaction he was buying into, and only the dates say which
    # of the two this is.
    return {"pays": latest > 0, "cut": cut, "latest": latest,
            "cut_pct": cut_pct, "years": len(series),
            "from_year": years[-2] if len(years) > 1 else None,
            "to_year": years[-1]}


def self_dealing(sbc=None, revenue=None, shares_cagr=None,
                 insider_pct=None) -> dict:
    """"Honest management that does not overpay itself."

    Executive pay tables are prose; stock compensation is a number. It is
    a real transfer from owners to managers, and paired with a rising
    share count it is self-dealing that shows up on the cash flow
    statement rather than in a proxy footnote.

    Insider OWNERSHIP is scored the other way, which is the unusual part.
    Most screens read heavy insider holdings as entrenchment. Schloss read
    Potlatch's 40% as alignment — "management has an interest in seeing
    that this company is a success" — and dismissed the objection that it
    blocked a takeover. Followed here deliberately.

    Every component returns None when unmeasured. An unreported figure is
    never scored as a pass; a company that does not disclose is a company
    we cannot clear.
    """
    s, r = _num(sbc), _num(revenue)
    sbc_pct = round(s / r * 100, 2) if (s is not None and r and r > 0) else None
    dil = _num(shares_cagr)
    ins = _num(insider_pct)

    checks = {
        "pay": None if sbc_pct is None else sbc_pct <= SBC_TO_REVENUE_MAX * 100,
        "dilution": None if dil is None else dil <= DILUTION_MAX,
        "aligned": None if ins is None else ins >= INSIDER_ALIGNED_MIN * 100,
    }
    known = [v for v in checks.values() if v is not None]
    return {"sbc_pct_revenue": sbc_pct, "shares_cagr": dil,
            "insider_pct": ins, "checks": checks,
            "passed": sum(1 for v in known if v), "known": len(known)}


def survival(first_filing_year, this_year) -> tuple[bool | None, int | None]:
    """(passes, years). "Long histories of operations (20+ years)."

    Deliberately measured from the FILING record, not the financial one.
    XBRL begins around 2010, so no company can show more than ~16 years of
    machine-readable financials — but EDGAR's submissions index reaches
    back to the mid-1990s, and a company that has been filing for thirty
    years has demonstrably survived thirty years. Operating history and
    data history are different questions, and only one of them is capped
    by the SEC's technology roadmap.
    """
    try:
        first, now = int(first_filing_year), int(this_year)
    except (TypeError, ValueError):
        return None, None
    if first <= 0 or now < first:
        return None, None
    yrs = now - first
    return yrs >= SURVIVAL_YEARS_MIN, yrs


def evaluate(f: dict, this_year: int) -> dict:
    """Score one company from filings alone.

    `price_ready` is False whenever the cheapness half could not be
    computed. The screen still shows the row — the balance sheet is the
    thesis and it is fully measured — but a reader must be able to see
    that the discount is unknown rather than assume it was checked.
    """
    tb = tangible_book(f.get("stockholders_equity"), f.get("goodwill"),
                       f.get("intangibles"))
    shares = _num(f.get("shares"))
    tb_ps = round(tb / shares, 4) if (tb is not None and shares and shares > 0) else None

    n = ncav(f.get("current_assets"), f.get("total_liabilities"),
             f.get("preferred_stock"))
    ncav_ps = round(n / shares, 4) if (n is not None and shares and shares > 0) else None

    debt_pass, debt_ratio = debt_ok(f.get("short_term_debt"),
                                    f.get("long_term_debt"),
                                    f.get("stockholders_equity"),
                                    f.get("total_liabilities"))
    # Presence of the key is the fetcher saying "I looked". An absent
    # dividend series is ambiguous — a non-payer files no dividend facts,
    # and so does a company we failed to fetch — but only the caller knows
    # which. So the refresh script sets the key to {} when it searched and
    # found nothing, and that is read as a definite no.
    raw_divs = f.get("div_per_share_by_year")
    divs = dividend_history(raw_divs)
    if raw_divs is not None and divs["pays"] is None:
        divs = {**divs, "pays": False}
    mgmt = self_dealing(f.get("sbc"), f.get("revenue"), f.get("shares_cagr"),
                        f.get("insider_pct"))
    surv_pass, surv_yrs = survival(f.get("first_filing_year"), this_year)
    backing = asset_backing(f.get("cash"), f.get("receivables"),
                            f.get("inventory"), f.get("ppe"),
                            f.get("goodwill"), f.get("intangibles"))

    # A price we have and a book value we do not are different failures,
    # and so are a book value that is negative and one that is missing.
    # `price_ready` answers only "did a price arrive"; the cheapness tests
    # resolve to False — not unknown — when the denominator is known and
    # non-positive, because a company with no tangible assets left has
    # definitively failed the discount test rather than dodged it.
    price = _num(f.get("price"))
    priced = price is not None and price > 0
    p_tb = round(price / tb_ps, 3) if (priced and tb_ps and tb_ps > 0) else None
    p_ncav = round(price / ncav_ps, 3) if (priced and ncav_ps and ncav_ps > 0) else None

    below = net_net = None
    if priced and tb_ps is not None:
        below = (p_tb <= 1 - BOOK_DISCOUNT_MIN) if tb_ps > 0 else False
    if priced and ncav_ps is not None:
        net_net = (p_ncav <= NCAV_MAX_PRICE) if ncav_ps > 0 else False

    gates = {
        "assets": None if tb is None else tb > 0,
        "debt": debt_pass,
        "survival": surv_pass,
        "pays_dividend": divs["pays"],
    }
    # `gates_pass` demands every gate be KNOWN and true, not merely that
    # nothing known failed. One measured gate out of four passing is a
    # company we could barely read, and "all of the gates we happened to
    # manage" is how such a row ends up presented as having cleared the
    # screen. `gates_known` is what tells a reader how thin the evidence
    # behind a rejection was.
    known_gates = [v for v in gates.values() if v is not None]

    return {
        "tangible_book": None if tb is None else round(tb, 2),
        "tangible_book_ps": tb_ps,
        "ncav": None if n is None else round(n, 2),
        "ncav_ps": ncav_ps,
        "debt_ratio": debt_ratio,
        "dividend": divs,
        "management": mgmt,
        "survival_years": surv_yrs,
        "backing": backing,
        "gates": gates,
        "gates_passed": sum(1 for v in known_gates if v),
        "gates_known": len(known_gates),
        "gates_pass": all(v is True for v in gates.values()),
        # ── the price half, honestly absent ──
        "price_ready": priced,
        "p_tangible_book": p_tb,
        "p_ncav": p_ncav,
        "below_book": below,
        "is_net_net": net_net,
    }


def census(rows: list[dict]) -> dict:
    """How many of these exist right now — his one permitted market call.

    "You used to be able to tell when the market was too high by the fact
    that the working capital stocks disappeared."

    That is a measurement, not a forecast, and it is the only market
    timing he allowed himself. Tracked over time it becomes a temperature
    series built from filings rather than sentiment. Counts are reported
    separately for what filings alone can establish and what needs a
    price, so a thin board is never ambiguous between "the market is
    expensive" and "we could not check".
    """
    total = len(rows)
    qualifying = sum(1 for r in rows if r.get("gates_pass"))
    positive_ncav = sum(1 for r in rows if (r.get("ncav") or 0) > 0)
    ncav_unknown = sum(1 for r in rows if r.get("ncav") is None)
    # Rejected and unreadable are different populations, and the gap
    # between screened and the sum of the two is how much of the board is
    # actually being judged.
    unreadable = sum(1 for r in rows
                     if not r.get("gates_pass")
                     and not any(v is False
                                 for v in (r.get("gates") or {}).values()))
    priced = sum(1 for r in rows if r.get("price_ready"))
    below = sum(1 for r in rows if r.get("below_book"))
    netnets = sum(1 for r in rows if r.get("is_net_net"))
    cuts = sum(1 for r in rows if (r.get("dividend") or {}).get("cut"))
    return {
        "screened": total,
        "balance_sheet_qualifying": qualifying,
        "positive_ncav": positive_ncav,
        "ncav_unknown": ncav_unknown,
        "unreadable": unreadable,
        "recent_dividend_cuts": cuts,
        "priced": priced,
        "below_book": below if priced else None,
        "net_nets": netnets if priced else None,
        "price_coverage_pct": round(priced / total * 100, 1) if total else 0.0,
    }


# ═══════════════════════════════════════════════════════════════════
# PAGE SIDE — reading what the monthly build wrote
# ═══════════════════════════════════════════════════════════════════

def load(data: dict | None = None) -> dict:
    """data/schloss.json if present and well-formed, else {}."""
    if data is not None:
        return data
    try:
        with open(_DATA_PATH, encoding="utf-8") as fh:
            payload = json.load(fh)
        if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            return payload
    except (OSError, ValueError):
        pass
    return {}


def data_source_label(data: dict | None = None) -> str:
    d = load(data)
    if not d:
        return "no data yet — run the refresh workflow"
    when = (d.get("generated") or "")[:10]
    return f"SEC EDGAR frames — {when} · {len(d.get('rows') or []):,} companies"


# The lists the page is built from. Each is a different question, and the
# ordering inside each is what makes it useful, so they are defined here
# rather than assembled in a template where nobody can test them.
def board(data: dict | None = None) -> dict:
    """Four lists, in the order Schloss would have worked through them."""
    rows = load(data).get("rows") or []

    def _f(x):
        return x if isinstance(x, (int, float)) else float("-inf")

    qualify = [r for r in rows if r.get("gates_pass")]

    # THE ENTRY SIGNAL, and the one thing here that is genuinely
    # actionable without a price. He bought after the cut, when the
    # forced selling was done and the reason was public. Biggest cut
    # first, but only among companies whose balance sheet already clears.
    cuts = sorted(
        [r for r in qualify if (r.get("dividend") or {}).get("cut")],
        key=lambda r: -_f((r.get("dividend") or {}).get("cut_pct")))

    # Net current assets above zero is what he meant by working-capital
    # stocks. Without a price we cannot say they are CHEAP, only that the
    # asset base exists — so this is sorted by how much of it there is
    # relative to what is owed, not by any discount.
    working = sorted([r for r in rows if _f(r.get("ncav")) > 0],
                     key=lambda r: -_f(r.get("ncav")))

    # The long-lived, low-debt, dividend-paying, asset-backed core. Most
    # hard assets first: he wanted to know what he was buying.
    core = sorted(qualify,
                  key=lambda r: -_f((r.get("backing") or {}).get("hard_pct")))

    # COULD NOT READ, AS OPPOSED TO REJECTED. Nothing here failed a gate;
    # something simply was not filed. Without this list they appear on no
    # list at all, and a company we could not measure would silently
    # vanish while a company we measured and rejected at least leaves a
    # trace. That is the same error as scoring the unknown as a pass, run
    # the other way. Closest to readable first.
    unread = sorted(
        [r for r in rows
         if not r.get("gates_pass")
         and not any(v is False for v in (r.get("gates") or {}).values())
         and (r.get("gates_known") or 0) < 4],
        key=lambda r: (-(r.get("gates_known") or 0), r.get("ticker") or ""))

    return {"qualify": qualify, "cuts": cuts, "working": working,
            "core": core, "unread": unread}


# Which gate is missing, in words, for the "could not read" list.
GATE_LABELS = {
    "assets": "shareholders' equity",
    "debt": "debt (and the liability total could not settle it)",
    "survival": "filing history",
    "pays_dividend": "dividend record",
}


def missing_gates(row: dict) -> list[str]:
    """The unmeasured gates, named. A '?' on a page is only honest if the
    reader can find out what was not filed."""
    gates = row.get("gates") or {}
    return [GATE_LABELS.get(k, k) for k, v in gates.items() if v is None]
