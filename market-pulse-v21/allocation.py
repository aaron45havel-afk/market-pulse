"""Now vs optimal — what the owner holds and how they split their pay today,
against the path the capital board would take with the same money.

Both comparisons are in the board's own unit, an AFTER-TAX ANNUAL RETURN
NET OF FRICTION, so a holding and a board row are read the same way:

  1. WHAT YOU HOLD. Every holding — cash, funds, picks, in every account,
     and property you own — gets the return the board would give it: a
     ticker on a screen gets that screen's blended estimate, an index or
     plan fund (or a stock no screen covers) the market return, cash the
     rate the owner gives (no rate, no number — it is not guessed), and a
     rental its own numbers on the equity that could actually be taken out
     after 7% selling costs. That last one is what "lazy equity" means: as
     a property is paid down and rises, the same cash flow is a smaller
     return on the equity sitting in it.

     Then the optimal path for the same money. MONEY MOVES ONLY WHERE ITS
     ACCOUNT LETS IT: a 401(k) holds the plan's funds; an IRA, Roth or HSA
     holds picks or an index fund; taxable money and cash can go anywhere
     on the board. And IT MOVES ONLY WHEN IT PAYS AFTER TAX: selling a
     taxable holding pays tax on its gain now, a rental pays depreciation
     recapture, capital-gains tax and 7% to sell. Over the owner's hold,
     what is left after that, compounding at the destination's return, must
     beat keeping — and keeping is credited with deferring the tax for good
     (held to a step-up, or forever), the cautious reading, which tilts
     toward keeping. Inside a 401(k), IRA, Roth or HSA a switch costs no tax.

  2. YOUR PAY. The split the owner uses now, line by line, against the
     waterfall's split of the same amount, each line at the after-tax return
     of a new dollar in that account (the employer match counted).

Percentages are in PERCENT (7.0 means 7%).
"""
from __future__ import annotations

import math
import re
from datetime import date

import capital as K

# ── accounts ─────────────────────────────────────────────────────────
ACCOUNT_LABEL = {"taxable": "taxable", "k401": "401(k)", "roth401k": "Roth 401(k)", "ira": "IRA",
                 "roth": "Roth IRA", "hsa": "HSA", "cash": "cash", "debt": "debt",
                 "rental": "rental", "home": "your home", "house_hack": "house hack you live in"}
_ALIASES = {
    "taxable": "taxable", "brokerage": "taxable", "individual": "taxable", "joint": "taxable", "trust": "taxable",
    "401k": "k401", "403b": "k401", "457": "k401", "457b": "k401", "tsp": "k401", "traditional401k": "k401",
    "trad401k": "k401",
    "roth401k": "roth401k", "roth403b": "roth401k", "rothtsp": "roth401k",
    "ira": "ira", "traditionalira": "ira", "tradira": "ira", "rollover": "ira", "rolloverira": "ira",
    "sep": "ira", "sepira": "ira", "simpleira": "ira",
    "roth": "roth", "rothira": "roth",
    "hsa": "hsa",
    "cash": "cash", "checking": "cash", "savings": "cash", "hysa": "cash", "highyieldsavings": "cash",
    "moneymarket": "cash", "mm": "cash", "cd": "cash",
    "tbill": "tbill", "tbills": "tbill", "treasury": "tbill", "treasuries": "tbill",
    "debt": "debt", "loan": "debt", "paydown": "debt",
}
PLAN = {"k401", "roth401k"}                       # the plan's own funds only
TRADITIONAL = {"k401", "ira"}                     # taxed on the way out
SHELTERED = {"k401", "roth401k", "ira", "roth", "hsa"}
LIQUID = {"taxable", "cash", "rental"}            # can go anywhere on the board

CATEGORY = {"cash": "Cash & savings", "k401": "401(k)", "roth401k": "401(k)", "debt": "Debt paydown",
            "hsa": "HSA", "ira": "IRA", "roth": "IRA", "taxable": "Taxable investing"}
STEP_CATEGORY = {"cash": "Cash & savings", "tbill": "Cash & savings", "match": "401(k)", "wrapper": "401(k)",
                 "debt": "Debt paydown", "hsa": "HSA", "ira": "IRA", "stock": "Taxable investing",
                 "re": "Down-payment fund"}
CATEGORY_ORDER = ["Cash & savings", "401(k)", "Debt paydown", "HSA", "IRA", "Taxable investing",
                  "Down-payment fund", "Unassigned"]
BUCKET_LABEL = {"cash": "Cash & T-bills", "funds": "Index & plan funds", "picks": "Stock picks",
                "re": "Real estate equity", "debt": "Debt paid off", "cost": "Tax & costs to move"}

# US total-market and S&P 500 index funds — the market return, not a pick.
INDEX_TICKERS = frozenset({
    "VTI", "VTSAX", "ITOT", "SCHB", "SWTSX", "FSKAX", "FZROX",
    "VOO", "VFIAX", "SPY", "IVV", "SPLG", "SCHX", "FXAIX", "FNILX", "SWPPX", "IWB", "VV",
})
INDEX_DIV = 1.2            # the S&P 500's dividend yield, about 1.2% in 2025-26 — the taxed-yearly share
# A move must gain at least this share of the money moved over the whole
# hold: a 0.1%-a-year edge is not worth a trade and a tax bill.
MIN_GAIN = 0.01
MAX_LINES = 200
_TICKER_RE = re.compile(r"^[A-Z]{1,5}(?:[.\-][A-Z]{1,2})?$")


# ═══════════════════════════════════════════════════════════════════
# PARSING — what the owner typed
# ═══════════════════════════════════════════════════════════════════

def account_of(text) -> tuple[str, bool] | None:
    """(account, state_exempt) for what was typed, or None. T-bills are cash
    whose interest the state does not tax."""
    a = _ALIASES.get(re.sub(r"[^a-z0-9]", "", str(text or "").lower()))
    if a is None:
        return None
    return ("cash", True) if a == "tbill" else (a, False)


def _money(s) -> float | None:
    s = str(s or "").strip().replace("$", "").replace(" ", "")
    return K._f(s, None) if s else None


def _pct(tok: str) -> float | None:
    v = K._f(tok[:-1].strip(), None) if tok.endswith("%") else None
    return None if v is None else max(-50.0, min(100.0, v))


TOP_PICKS = "TOP PICKS"
_TOP_PICKS_NAMES = {"top picks", "the top picks"}


def _ticker(name: str, account: str) -> str | None:
    """The ticker a line names, if it names one. "Top picks" stands for the
    board's current top picks, as a group — a monthly split can say where new
    money goes without listing eight tickers that change."""
    if account in ("cash", "debt"):
        return None
    if name.strip().lower() in _TOP_PICKS_NAMES:
        return TOP_PICKS
    up = name.strip().upper()
    return up if _TICKER_RE.match(up) else None


def _lines(text):
    """(line number in the text, the line, its comma-separated parts) for each
    line that is not blank or a # note — the number lets a step edit the
    owner's own line in place."""
    for n, raw in enumerate(str(text or "").splitlines()[:MAX_LINES]):
        line = raw.strip()
        if line and not line.startswith("#"):
            yield n, line, [x.strip() for x in line.split(",")]


def parse_holdings(text) -> tuple[list[dict], list[str]]:
    """'name or ticker, account, value[, basis][, return%]' per line → (rows,
    lines that could not be read). A bare number after the value, or
    'basis 18000', is the cost basis; a number with % is the owner's own
    return (for cash, its interest rate)."""
    rows, bad = [], []
    for n, line, parts in _lines(text):
        acct = account_of(parts[1]) if len(parts) >= 3 else None
        value = _money(parts[2]) if len(parts) >= 3 else None
        basis = rate = None
        ok = bool(parts[0]) and acct is not None and acct[0] != "debt" and value is not None and value >= 0
        for tok in parts[3:] if ok else []:
            if not tok:
                continue
            if tok.endswith("%"):
                rate = _pct(tok)
                ok = ok and rate is not None
                continue
            m = re.fullmatch(r"(?:basis|cost)?\s*[:=]?\s*\$?\s*(\d+(?:\.\d+)?)", tok, re.I)
            if not m or basis is not None:
                ok = False
            else:
                basis = float(m.group(1))
        if not ok:
            bad.append(line[:120])
            continue
        name = parts[0][:40]
        rows.append({"name": name, "ticker": _ticker(name, acct[0]), "account": acct[0],
                     "state_exempt": acct[1], "value": float(value), "basis": basis, "rate": rate, "line": n})
    return rows, bad


def parse_flows(text) -> tuple[list[dict], list[str]]:
    """'what, account, amount a month[, return%]' per line. The account can be
    'debt' (what = the debt's name, or give its APR)."""
    rows, bad = [], []
    for n, line, parts in _lines(text):
        acct = account_of(parts[1]) if len(parts) >= 3 else None
        amt = _money(parts[2]) if len(parts) >= 3 else None
        rate = None
        ok = bool(parts[0]) and acct is not None and amt is not None and amt >= 0
        for tok in parts[3:] if ok else []:
            if tok:
                rate = _pct(tok)
                ok = ok and rate is not None
        if not ok:
            bad.append(line[:120])
            continue
        name = parts[0][:40]
        rows.append({"name": name, "ticker": _ticker(name, acct[0]), "account": acct[0],
                     "state_exempt": acct[1], "amount": float(amt), "rate": rate})
    return rows, bad


USES = ("rental", "home", "house_hack")
_RE_NUM = {"value": (0, 1e9), "loan": (0, 1e9), "rate": (0, 20), "payment": (0, 1e6), "rent": (0, 1e6),
           "shelter_rent": (0, 1e6), "costs": (0, 1e6), "basis": (0, 1e9), "year": (1900, 2100),
           "hours": (0, 200)}
_RE_OPTIONAL = ("basis", "year")


def parse_owned_re(items) -> list[dict]:
    """Property the owner owns, from the form's records. A record without a
    value is dropped; basis and year bought may be blank (then selling it is
    not tested)."""
    out = []
    for it in (items if isinstance(items, list) else [])[:20]:
        if not isinstance(it, dict):
            continue
        rec = {"name": str(it.get("name") or "").strip()[:40] or "Property",
               "use": it.get("use") if it.get("use") in USES else "rental"}
        for k, (lo, hi) in _RE_NUM.items():
            v = it.get(k)
            x = (K._f(str(v).replace(",", "").replace("$", "").replace("%", "").strip(), None)
                 if v not in (None, "") else None)
            rec[k] = min(hi, max(lo, x)) if x is not None else (None if k in _RE_OPTIONAL else 0.0)
        if rec["year"] is not None:
            rec["year"] = int(rec["year"])
        if rec["value"] > 0:
            out.append(rec)
    return out


def _fund_ticker(t: str | None) -> bool:
    """An index fund, or a mutual fund (five letters ending in X: FXAIX)."""
    return bool(t) and (t in INDEX_TICKERS or (len(t) == 5 and t.endswith("X")))


def is_pick(h: dict) -> bool:
    """An individual stock — research time applies — rather than a fund. A
    401(k) holds the plan's funds, whatever the line is called."""
    return (h["account"] not in ("cash", "debt") and h["account"] not in PLAN
            and bool(h.get("ticker")) and not _fund_ticker(h["ticker"]))


# ═══════════════════════════════════════════════════════════════════
# WHAT FOLLOWS FROM WHAT YOU HOLD — read by capital.profile_with_defaults
# ═══════════════════════════════════════════════════════════════════

def derive(p: dict) -> dict:
    """One source of truth: once holdings are listed, cash on hand is their
    cash lines and picks held are their individual stocks. Net worth is
    everything listed plus property equity less the debts. Owning where you
    live means no rent to stop paying."""
    hold, _ = parse_holdings(p.get("holdings"))
    owned = parse_owned_re(p.get("owned_re"))
    out: dict = {}
    cash_lines = [h for h in hold if h["account"] == "cash"]
    if cash_lines:
        out["cash"] = round(sum(h["value"] for h in cash_lines), 2)
        out["_cash_from_holdings"] = True
    invested = [h for h in hold if h["account"] != "cash"]
    if invested:
        out["stock_holdings"] = round(sum(h["value"] for h in invested if is_pick(h)), 2)
        out["_picks_from_holdings"] = True
    if any(r["use"] in ("home", "house_hack") for r in owned):
        out["housing_cost"] = 0.0
        out["_owns_shelter"] = True
    eq = sum(r["value"] - r["loan"] for r in owned)
    cash = out.get("cash", K._f(p.get("cash")))
    inv = sum(h["value"] for h in invested) if invested else K._f(p.get("stock_holdings"))
    debts = sum(K._f(d.get("balance")) for d in p.get("debts") or [] if isinstance(d, dict))
    out["_net_worth"] = round(cash + inv + eq - debts, 2)
    out["_re_equity"] = round(eq, 2)
    return out


# ═══════════════════════════════════════════════════════════════════
# RETURNS — every holding in the board's unit
# ═══════════════════════════════════════════════════════════════════

def _H(p: dict) -> int:
    return max(1, int(K._f(p.get("hold_years"), 5)))


def _hsa_state(p: dict) -> float:
    """California and New Jersey tax HSA growth as ordinary income."""
    return K._f(p.get("state_rate")) / 100 if (p.get("home_state") or "").upper() in ("CA", "NJ") else 0.0


def stock_time_drag(p: dict, monthly_free: float) -> float:
    """The research time the board charges every pick, in points a year."""
    probe = K.row(id="probe", kind="stock", label="", source="", ret_pre=0.0, ret_after=0.0, basis="")
    return K.friction(probe, p, monthly_free=monthly_free, cash_free=0.0)["time_drag"] or 0.0


def estimate(item: dict, p: dict, stock_rows: dict) -> tuple[float, float, str, bool] | None:
    """(pre-tax return, dividend share, where it came from, a single stock?)
    for a holding or a pay line. None when it cannot be measured: cash
    without a rate."""
    mr = K._f(p.get("market_return"), 7.0)
    t = item.get("ticker")
    pick = is_pick(item)
    if item.get("rate") is not None:
        return item["rate"], (0.0 if pick else INDEX_DIV), "your rate", pick
    if item["account"] == "cash":
        return None
    if t and t in stock_rows and item["account"] not in PLAN:
        r = stock_rows[t]
        srcs = ", ".join(dict.fromkeys(r.get("sources") or [r["source"]]))
        return r["ret_pre"], r["div_pct"], f"{srcs} estimate, blended toward the market", True
    if _fund_ticker(t):
        return mr, INDEX_DIV, "the market return (an index fund)" if t in INDEX_TICKERS else \
            "a mutual fund — the market return assumed", False
    if pick:
        return mr, 0.0, "on no screen — the market return assumed", True
    return mr, INDEX_DIV, "a fund — the market return assumed", False


def held_after(pre: float, div: float, account: str, p: dict, state_exempt: bool = False) -> float:
    """After-tax return of money ALREADY in this account. A traditional
    balance returns its pre-tax rate on its after-tax value (the tax on the
    way out is proportional), so it is counted at that value (weq)."""
    t = K.tax_rates(p)
    if account == "cash":
        return pre * (1 - (t["tbill"] if state_exempt else t["ordinary"]))
    if account == "taxable":
        return K.after_tax_taxable(pre, div, _H(p), t["qualified"], t["qualified"])
    if account == "hsa":
        return pre * (1 - _hsa_state(p))
    return pre


def _weq(account: str, p: dict) -> float:
    return 1 - K.tax_rates(p)["retire"] if account in TRADITIONAL else 1.0


def measure_holding(h: dict, p: dict, stock_rows: dict, td: float, sleeve: set) -> dict:
    t = K.tax_rates(p)
    acct = h["account"]
    item = {"id": f"h:{h['name']}:{acct}", "label": h["name"], "account": acct, "line": h.get("line"),
            "state_exempt": bool(h.get("state_exempt")), "ticker": h.get("ticker"),
            "account_label": "T-bills" if h.get("state_exempt") else ACCOUNT_LABEL[acct],
            "value": h["value"], "weq": _weq(acct, p),
            "bucket": "cash" if acct == "cash" else ("picks" if is_pick(h) else "funds"),
            "h": None, "keep_reason": None, "tau": 0.0, "c": 0.0, "basis": h.get("basis")}
    est = estimate(h, p, stock_rows)
    if est is None:
        item["why_not"] = "no rate given — add it (e.g. 4.1%) to compare this cash"
        return item
    pre, div, src, pick = est
    after = held_after(pre, div, acct, p, h.get("state_exempt"))
    time = td if pick else 0.0
    row = stock_rows.get(h.get("ticker") or "")
    rt = row["round_trip_pct"] if row else (K.ROUND_TRIP_PCT["stock_large"] if pick else 0.0)
    item.update(pre=pre, after=round(after, 2), time=round(time, 2), h=round(after - time, 2), src=src,
                c=rt / 2 / 100)
    if acct == "taxable":
        b = h.get("basis")
        item["tau"] = (None if b is None else
                       max(0.0, h["value"] - b) * t["qualified"] / h["value"] if h["value"] > 0 else 0.0)
        if b is None:
            item["keep_reason"] = "add its cost basis to test selling it"
    if h.get("ticker") in sleeve:
        item["keep_reason"] = "already one of the top picks"
    return item


def _year_one(loan: float, rate_pct: float, payment: float) -> tuple[float, float]:
    """(interest, principal) paid over the next twelve payments."""
    bal, i, interest, principal = loan, rate_pct / 100 / 12, 0.0, 0.0
    for _ in range(12):
        if bal <= 0:
            break
        it = bal * i
        pr = min(bal, max(0.0, payment - it))
        interest, principal, bal = interest + it, principal + pr, bal - pr
    return interest, principal


def rental_sale_tax(r: dict, p: dict, years: float) -> float:
    """Tax on selling a rental now: the depreciation taken is recaptured at up
    to 25% federal (plus state, plus NIIT when it applies); the rest of the
    gain at the capital-gains rate. /headroom's building share and schedule."""
    import headroom as HR
    t = K.tax_rates(p)
    net_sale = r["value"] * (1 - HR.SELL_COST_PCT)
    dep = r["basis"] * HR.BUILDING_SHARE / HR.DEP_YEARS * min(max(years, 0.0), HR.DEP_YEARS)
    gain = net_sale - (r["basis"] - dep)
    if gain <= 0:
        return 0.0
    recap = min(dep, gain)
    rate = (min(25.0, K._f(p.get("fed_rate"))) + K._f(p.get("state_rate")) + (3.8 if p.get("niit") else 0.0)) / 100
    return recap * rate + (gain - recap) * t["qualified"]


def measure_property(r: dict, p: dict, today: date, index: int | None = None) -> dict:
    """A property's return on the equity that could be taken out today — its
    value less 7% to sell, less the loan. Rentals: cash flow after the full
    payment and the income tax depreciation does not shelter, plus the year's
    principal and appreciation after the gains tax. A home or a house hack
    you live in adds the rent you would otherwise pay, untaxed."""
    import headroom as HR
    t = K.tax_rates(p)
    use = r["use"]
    E = r["value"] * (1 - HR.SELL_COST_PCT) - r["loan"]
    item = {"id": f"re:{r['name']}", "label": r["name"], "account": "rental" if use == "rental" else use,
            "account_label": ACCOUNT_LABEL[use], "value": round(max(E, 0.0), 2), "weq": 1.0, "bucket": "re",
            "h": None, "keep_reason": None, "tau": None, "c": 0.0, "property": True, "prop": index,
            "gross_equity": r["value"] - r["loan"]}
    if E <= 0:
        item["why_not"] = "no equity left to take out after 7% selling costs"
        return item
    if r["loan"] > 0 and r["payment"] <= 0:
        item["why_not"] = "add the monthly loan payment to measure it"
        return item
    interest, principal = _year_one(r["loan"], r["rate"], r["payment"])
    noi = (r["rent"] - r["costs"]) * 12
    shelter = r["shelter_rent"] * 12 if use != "rental" else 0.0
    years = (today.year - r["year"]) if r.get("year") else None
    dep = (r["basis"] * HR.BUILDING_SHARE / HR.DEP_YEARS
           if use == "rental" and r.get("basis") and (years is None or years < HR.DEP_YEARS) else 0.0)
    op_tax = max(0.0, noi - interest - dep) * t["ordinary"] if use == "rental" else 0.0
    appr = K._f(p.get("appreciation")) / 100 * r["value"]
    appr_after = appr * (1 - t["qualified"]) if use == "rental" else appr
    cash_flow = noi + shelter - r["payment"] * 12
    after = (cash_flow - op_tax + principal + appr_after) / E * 100
    time = r["hours"] * 12 * K._f(p.get("hourly_value")) / E * 100
    item.update(after=round(after, 2), time=round(time, 2), h=round(after - time, 2),
                src=(f"${cash_flow:,.0f}/yr after the payment{' incl. the rent you would pay' if shelter else ''}"
                     f"{f', ${op_tax:,.0f} tax' if op_tax else ''}, + ${principal:,.0f} principal"
                     f"{f', + ${appr_after:,.0f} appreciation' if appr_after else ''}, on ${E:,.0f} you could "
                     f"take out (value less 7% to sell, less the ${r['loan']:,.0f} loan)"))
    if use != "rental":
        item["keep_reason"] = "the place you live — selling it is not tested"
    elif r.get("basis") is None or r.get("year") is None:
        item["keep_reason"] = "add its purchase price and year bought to test selling it"
    else:
        tax = rental_sale_tax(r, p, years)
        item.update(tau=tax / E, sale_tax=round(tax, 2))
    return item


# ═══════════════════════════════════════════════════════════════════
# THE OPTIMAL PATH FOR WHAT YOU HOLD
# ═══════════════════════════════════════════════════════════════════

def sleeve_of(board: dict) -> list[dict]:
    p = board["profile"]
    picks = [r for r in board["rows"] if r["kind"] == "stock" and r.get("ret_net") is not None]
    return picks[:max(1, int(K._f(p.get("picks_n"), 8)))]


def stock_rows_of(board: dict) -> dict:
    """Each pick on the board by ticker, plus TOP PICKS: the top picks as one
    equal-weighted group, at their average estimate."""
    out = {r["detail"]["ticker"]: r for r in board["rows"] if r["kind"] == "stock"}
    sl = sleeve_of(board)
    if sl:
        n = len(sl)
        avg = lambda k: sum(K._f(r.get(k)) for r in sl) / n  # noqa: E731
        out[TOP_PICKS] = {"ret_pre": avg("ret_pre"), "div_pct": avg("div_pct"), "ret_net": avg("ret_net"),
                          "round_trip_pct": avg("round_trip_pct"), "source": "the top picks",
                          "sources": ["the top picks"], "detail": {"ticker": TOP_PICKS}}
    return out


def destinations(board: dict, sleeve: list[dict]) -> list[dict]:
    """Where held money can go, best first, each with how much it can take
    (in dollars arriving) and which kinds of money may go there. Real estate
    is lumpy: one deal's cash, all at once, or not at all."""
    p = board["profile"]
    inf = math.inf
    out = []
    for r in board["rows"]:
        if r.get("ret_net") is None or r.get("blocked"):
            continue
        if r["kind"] == "debt":
            out.append({"id": r["id"], "label": r["label"], "kind": "debt", "bucket": "debt", "net": r["ret_after"],
                        "cap": K._f(r["detail"].get("balance")), "lumpy": False, "from": LIQUID})
        elif r["kind"] == "tbill":
            out.append({"id": r["id"], "label": r["label"], "kind": "tbill", "bucket": "cash", "net": r["ret_after"],
                        "cap": inf, "lumpy": False, "from": LIQUID | {"reserve"}})
        elif r["kind"] == "re":
            # Held money is there now: no months of saving, only the months to
            # close.
            ready = K.friction(r, p, monthly_free=0.0, cash_free=inf)
            out.append({"id": r["id"], "label": r["label"], "kind": "re", "bucket": "re", "net": ready["ret_net"],
                        "cap": r["min_capital"], "lumpy": True, "from": LIQUID, "conditional": r.get("conditional")})
    if sleeve:
        n = len(sleeve)
        names = ", ".join(r["detail"]["ticker"] for r in sleeve)
        taxed = sum(r["ret_after"] - r["cost_drag"] - r["time_drag"] for r in sleeve) / n
        pre = sum(r["ret_pre"] - r["cost_drag"] - r["time_drag"] for r in sleeve) / n
        out.append({"id": "picks", "label": f"The top {n} picks ({names})", "kind": "picks", "bucket": "picks",
                    "net": round(taxed, 2), "cap": inf, "lumpy": False, "from": LIQUID})
        out.append({"id": "picks_in", "label": f"The top {n} picks, inside the same account", "kind": "picks",
                    "bucket": "picks", "net": round(pre, 2), "cap": inf, "lumpy": False,
                    "from": {"ira", "roth", "hsa"}, "sheltered": True})
    out.append({"id": "index_in", "label": "An index fund, inside the same account", "kind": "index",
                "bucket": "funds", "net": K._f(p.get("market_return"), 7.0), "cap": inf, "lumpy": False,
                "from": SHELTERED, "sheltered": True})
    # When the month's plan is saving for a property, the board already
    # counted free cash toward that down payment (it is why "ready in" is
    # what it is), so free cash may go there at that row's return — waiting
    # months included — rather than being spent elsewhere.
    win = (board.get("plan") or {}).get("winner")
    if win and win.get("kind") == "re" and win.get("ret_net") is not None:
        out.append({"id": f"dp:{win['id']}", "for_id": win["id"], "kind": "dpfund", "bucket": "cash",
                    "label": f"Down-payment fund for {win['label']} (T-bills until it closes)",
                    "net": win["ret_net"], "cap": win["min_capital"], "lumpy": False, "from": {"cash"},
                    "conditional": win.get("conditional")})
    return sorted(out, key=lambda d: -d["net"])


def _per_dollar_gain(s: dict, a: float, H: int) -> float:
    """What a dollar moved is worth at the end of the hold, less what it would
    have been worth kept: (1 − cost − tax)(1 + a)^H − (1 + h)^H."""
    return (1 - s["c"] - s["tau"]) * (1 + a / 100) ** H - (1 + s["h"] / 100) ** H


def optimize(items: list[dict], board: dict, sleeve: list[dict]) -> list[dict]:
    """Moves, destination by destination from the best: each takes the money
    allowed to go there that gains most by going, until it is full. Cash
    first fills the emergency fund (which may only move to T-bills), from the
    best-paying cash."""
    p = board["profile"]
    H = _H(p)
    hsa_st = _hsa_state(p)
    ef = K._f(p.get("monthly_expenses")) * K._f(p.get("emergency_months"), 6.0)
    sources = []
    cash = sorted([i for i in items if i["account"] == "cash" and i["h"] is not None], key=lambda i: -i["h"])
    for i in cash:
        res = min(i["value"], ef)
        ef -= res
        if res > 0:
            sources.append({**i, "key": "reserve", "left": res, "item": i, "label": f"{i['label']} — emergency fund"})
        if i["value"] - res > 0:
            sources.append({**i, "key": "cash", "left": i["value"] - res, "item": i})
    for i in items:
        if i["account"] == "cash" or i["h"] is None or i["keep_reason"] or i["tau"] is None:
            continue
        sources.append({**i, "key": i["account"], "left": i["value"], "item": i})

    def net_for(d, s):
        return d["net"] * (1 - hsa_st) if d.get("sheltered") and s["account"] == "hsa" else d["net"]

    moves = []
    funded: set[str] = set()
    dests = destinations(board, sleeve)
    picks = next((d for d in dests if d["id"] == "picks"), None)

    def eligible(s, d):
        if s["left"] <= 0.005 or s["key"] not in d["from"] or 1 - s["c"] - s["tau"] <= 0:
            return False
        # A property is sold whole or not at all: only when what is left after
        # a smaller destination fills still has a home that beats keeping it
        # (the picks, which take any amount). Selling a duplex to clear a
        # $4,000 card is not a move.
        if s.get("property") and (picks is None or _per_dollar_gain(s, picks["net"], H) < MIN_GAIN):
            return False
        # Cash already in T-bills IS a down-payment fund; "moving" it there
        # would be the same money, suggested again every time.
        if d["kind"] == "dpfund" and s.get("state_exempt"):
            return False
        return _per_dollar_gain(s, net_for(d, s), H) >= MIN_GAIN

    for d in dests:
        if d.get("for_id") in funded:      # the deal itself was bought outright
            continue
        elig = [s for s in sources if eligible(s, d)]
        if not elig:
            continue
        elig.sort(key=lambda s: -_per_dollar_gain(s, net_for(d, s), H))
        cap = d["cap"]
        if d["lumpy"]:
            if sum(s["left"] * (1 - s["c"] - s["tau"]) for s in elig) + 0.005 < cap:
                continue
            funded.add(d["id"])
        for s in elig:
            if cap <= 0.005:
                break
            keep = 1 - s["c"] - s["tau"]
            sold = min(s["left"], cap / keep)
            a = net_for(d, s)
            moves.append({"from": s["label"], "from_id": s["id"], "account": s["account_label"], "to": d["label"],
                          "to_kind": d["kind"], "to_id": d["id"], "bucket": d["bucket"],
                          "conditional": d.get("conditional"), "key": s["key"], "acct": s["account"],
                          "name": s["item"]["label"], "line": s.get("line"), "prop": s.get("prop"),
                          "state_exempt": bool(s.get("state_exempt")),
                          "sold": round(sold, 2), "tax": round(sold * s["tau"], 2), "cost": round(sold * s["c"], 2),
                          "proceeds": round(sold * keep, 2), "h": s["h"], "a": round(a, 2), "weq": s["weq"],
                          "gain": round(s["weq"] * sold * _per_dollar_gain(s, a, H), 2)})
            s["left"] -= sold
            s["item"]["moved"] = s["item"].get("moved", 0.0) + sold
            cap -= sold * keep
    return moves


def compare_holdings(board: dict, today: date) -> dict:
    p = board["profile"]
    rows = board["rows"]
    stock_rows = stock_rows_of(board)
    sleeve = sleeve_of(board)
    td = stock_time_drag(p, board.get("monthly_free", 0.0))
    hold, bad = parse_holdings(p.get("holdings"))
    in_sleeve = {r["detail"]["ticker"] for r in sleeve} | {TOP_PICKS}
    items = ([measure_holding(h, p, stock_rows, td, in_sleeve) for h in hold]
             + [measure_property(r, p, today, n) for n, r in enumerate(parse_owned_re(p.get("owned_re")))])
    for n, i in enumerate(items):
        i["id"] = f"{i['id']}:{n}"           # two lines may name the same fund
    moves = optimize(items, board, sleeve)
    measured = [i for i in items if i["h"] is not None]
    base = sum(i["weq"] * i["value"] for i in measured)
    now = sum(i["weq"] * i["value"] * i["h"] / 100 for i in measured)
    opt = (sum(i["weq"] * (i["value"] - i.get("moved", 0.0)) * i["h"] / 100 for i in measured)
           + sum(m["weq"] * m["proceeds"] * m["a"] / 100 for m in moves))
    one_time = sum(m["weq"] * (m["tax"] + m["cost"]) for m in moves)

    def bars(after: bool) -> list[dict]:
        tot: dict[str, float] = {}
        for i in items:
            v = i["weq"] * (i["value"] - (i.get("moved", 0.0) if after else 0.0))
            tot[i["bucket"]] = tot.get(i["bucket"], 0.0) + v
        if after:
            for m in moves:
                tot[m["bucket"]] = tot.get(m["bucket"], 0.0) + m["weq"] * m["proceeds"]
                tot["cost"] = tot.get("cost", 0.0) + m["weq"] * (m["tax"] + m["cost"])
        whole = sum(tot.values()) or 1.0
        return [{"bucket": b, "label": BUCKET_LABEL[b], "amount": round(v, 2), "pct": round(v / whole * 100, 1)}
                for b, v in sorted(tot.items(), key=lambda kv: list(BUCKET_LABEL).index(kv[0])) if v > 0.5]

    for i in items:
        i["status"] = ("not measured" if i["h"] is None else
                       "move" if i.get("moved", 0) > 0.5 else "keep")
    return {"items": items, "moves": moves, "bad_lines": bad, "hold_years": _H(p),
            "base": round(base, 2), "now_dollars": round(now, 2), "opt_dollars": round(opt, 2),
            "now_pct": round(now / base * 100, 2) if base > 0 else None,
            "opt_pct": round(opt / base * 100, 2) if base > 0 else None,
            "gap_dollars": round(opt - now, 2), "one_time": round(one_time, 2),
            "gain_hold": round(sum(m["gain"] for m in moves), 2),
            "unmeasured": [i for i in items if i["h"] is None],
            "bars_now": bars(False), "bars_opt": bars(True)}


# ═══════════════════════════════════════════════════════════════════
# YOUR PAY vs THE WATERFALL
# ═══════════════════════════════════════════════════════════════════

def contribution_return(account: str, pre: float, p: dict, *, div: float = 0.0, pick: bool = False,
                        matched: bool = False, state_exempt: bool = False, td: float = 0.0,
                        round_trip: float = 0.0) -> float:
    """After-tax annual return of a NEW dollar put into this account at this
    pre-tax return, over the hold. A traditional dollar is deducted now and
    taxed on the way out; a matched dollar brings the employer's match with
    it (into the pre-tax side, taxed on the way out); an HSA dollar is
    deducted and grows untaxed (federally — California and New Jersey tax
    both); a Roth dollar grows untaxed."""
    t = K.tax_rates(p)
    H = _H(p)
    drag = round_trip / H + (td if pick else 0.0)
    m = K._f(p.get("match_rate"), 100.0) / 100 if matched else 0.0
    g = (1 + pre / 100) ** H
    if account == "cash":
        return pre * (1 - (t["tbill"] if state_exempt else t["ordinary"]))
    if account == "taxable":
        return K.after_tax_taxable(pre, div, H, t["qualified"], t["qualified"]) - drag
    if account == "roth":
        return pre - drag
    if account == "roth401k":
        grown = g * (1 + m * (1 - t["retire"]))
    elif account in ("k401", "ira"):
        grown = g * (1 + m) * (1 - t["retire"]) / max(1e-9, 1 - t["ordinary"])
    elif account == "hsa":
        st = _hsa_state(p)
        grown = (1 + pre * (1 - st) / 100) ** H / max(1e-9, 1 - (t["ordinary"] - st))
    else:
        return pre - drag
    return ((grown ** (1 / H) - 1) * 100 if grown > 0 else -100.0) - drag


def _debt_apr(name: str, debts: list[dict]) -> float | None:
    n = name.strip().lower()
    for d in debts or []:
        dn = str(d.get("name") or "").strip().lower()
        if dn and (dn == n or dn in n or n in dn):
            return K._f(d.get("apr"))
    return None


def measure_flows(flows: list[dict], p: dict, stock_rows: dict, td: float) -> list[dict]:
    match_left = K._f(p.get("salary")) * K._f(p.get("match_pct")) / 100 / 12 if K._f(p.get("k401_room")) > 0 else 0.0
    out = []
    for f in flows:
        acct = f["account"]
        line = {"label": f["name"], "account_label": "T-bills" if f.get("state_exempt") else ACCOUNT_LABEL[acct],
                "category": CATEGORY[acct], "amount": f["amount"], "ret": None}
        if acct == "debt":
            apr = f["rate"] if f["rate"] is not None else _debt_apr(f["name"], p.get("debts"))
            if apr is None:
                line["why_not"] = "no APR — add it (e.g. 6.5%) or use the name from your debts"
            else:
                line.update(ret=apr, src=f"a guaranteed {apr:g}%")
            out.append(line)
            continue
        est = estimate(f, p, stock_rows)
        if est is None:
            line["why_not"] = "no rate given — add it (e.g. 4.1%)"
            out.append(line)
            continue
        pre, div, src, pick = est
        row = stock_rows.get(f.get("ticker") or "")
        if acct == "taxable" and row is not None and f.get("rate") is None:
            line.update(ret=row["ret_net"], src=src)
            out.append(line)
            continue
        rt = row["round_trip_pct"] if row else (K.ROUND_TRIP_PCT["stock_large"] if pick else 0.0)
        kw = dict(div=div, pick=pick, state_exempt=f.get("state_exempt"), td=td, round_trip=rt)
        if acct in PLAN and match_left > 0:
            mtd = min(match_left, f["amount"])
            match_left -= mtd
            r_m = contribution_return(acct, pre, p, matched=True, **kw)
            r_u = contribution_return(acct, pre, p, **kw)
            ret = (mtd * r_m + (f["amount"] - mtd) * r_u) / f["amount"] if f["amount"] > 0 else r_u
            src += f"; the employer match on the first ${mtd:,.0f}"
        else:
            ret = contribution_return(acct, pre, p, **kw)
        line.update(ret=round(ret, 2), src=src)
        out.append(line)
    return out


def step_returns(steps: list[dict], left: float, board: dict, sleeve: list[dict]) -> list[dict]:
    """Each waterfall step at the after-tax return of the dollars in it."""
    p = board["profile"]
    t = K.tax_rates(p)
    mr = K._f(p.get("market_return"), 7.0)
    tbill = K._f(p.get("rf_rate"), 4.0) * (1 - t["tbill"])
    by_id = {r["id"]: r for r in board["rows"]}
    roth = (sum(r["ret_pre"] - r["cost_drag"] - r["time_drag"] for r in sleeve) / len(sleeve)) if sleeve else mr
    out = []
    for s in steps:
        k = s["kind"]
        if k == "cash":
            ret = tbill
        elif k == "match":
            ret = contribution_return("k401", mr, p, matched=True)
        elif k == "debt":
            ret = _debt_apr(str(s.get("ref") or ""), p.get("debts"))
        elif k == "hsa":
            ret = contribution_return("hsa", mr, p)
        elif k == "ira":
            ret = roth
        else:
            ret = (by_id.get(s.get("ref")) or {}).get("ret_net")
        out.append({"label": s["to"], "category": STEP_CATEGORY.get(k, "Taxable investing"),
                    "amount": s["amount"], "ret": round(ret, 2) if ret is not None else None})
    if left > 0:
        out.append({"label": "Unassigned (counted at the T-bill rate)", "category": "Unassigned",
                    "amount": left, "ret": round(tbill, 2)})
    return out


def compare_pay(board: dict, today: date) -> dict | None:
    p = board["profile"]
    flows, bad = parse_flows(p.get("current_monthly"))
    if not flows and not bad:
        return None
    stock_rows = stock_rows_of(board)
    sleeve = sleeve_of(board)
    lines = measure_flows(flows, p, stock_rows, stock_time_drag(p, board.get("monthly_free", 0.0)))
    total = round(sum(l["amount"] for l in lines), 2)
    # The waterfall for the SAME amount, so the two splits compare like for like.
    wf = K.waterfall({**p, "monthly_invest": total}, board["rows"], today) if total > 0 else {"steps": [], "left": 0}
    opt = step_returns(wf["steps"], wf["left"], board, sleeve)
    meas = [l for l in lines if l["ret"] is not None]
    unmeasured = [l for l in lines if l["ret"] is None]
    # Dollars a year earned on ONE month's money. Not times twelve: this
    # month's waterfall includes one-time steps (the card paid off, the
    # emergency fund topped up) that a normal month will not repeat.
    now_amt = sum(l["amount"] for l in meas)
    now = sum(l["amount"] * l["ret"] / 100 for l in meas)
    opt_known = [o for o in opt if o["ret"] is not None]
    optd = sum(o["amount"] * o["ret"] / 100 for o in opt_known)
    opt_amt = sum(o["amount"] for o in opt_known)
    cats = []
    for c in CATEGORY_ORDER:
        a = sum(l["amount"] for l in lines if l["category"] == c)
        b = sum(o["amount"] for o in opt if o["category"] == c)
        if a > 0.5 or b > 0.5:
            cats.append({"category": c, "now": round(a, 2), "opt": round(b, 2)})
    return {"lines": lines, "opt": opt, "cats": cats, "bad_lines": bad, "total": total,
            "profile_total": K._f(p.get("monthly_invest")),
            "now_pct": round(now / now_amt * 100, 2) if now_amt > 0 else None,
            "opt_pct": round(optd / opt_amt * 100, 2) if opt_amt > 0 else None,
            "now_dollars": round(now, 2), "opt_dollars": round(optd, 2),
            # A gap is only claimed when every line of the current split is measured.
            "gap_dollars": round(optd - now, 2) if not unmeasured else None,
            "unmeasured": unmeasured}


# ═══════════════════════════════════════════════════════════════════

def compare(board: dict, today: date | None = None) -> dict:
    """Everything the "Now vs optimal" card shows."""
    today = today or date.today()
    p = board["profile"]
    nw, eq = K._f(p.get("_net_worth")), K._f(p.get("_re_equity"))
    cap = K.re_cap_pct(p, nw)
    hold = compare_holdings(board, today)
    has_hold = bool(hold["items"] or hold["bad_lines"])
    pay = compare_pay(board, today)
    return {"has_any": has_hold or pay is not None,
            "holdings": hold if has_hold else None,
            "pay": pay,
            "net_worth": nw, "re_equity": eq,
            "re_share": round(eq / nw * 100, 1) if nw > 0 else None,
            "re_cap": cap, "re_room": round(cap / 100 * nw - eq, 2) if cap is not None else None,
            "owns_shelter": bool(p.get("_owns_shelter"))}


# ═══════════════════════════════════════════════════════════════════
# DOING IT — a step marked done edits the saved profile
# ═══════════════════════════════════════════════════════════════════
# The owner's own lines are edited in place (a holding sold down is
# rewritten, one sold out is removed), new holdings are appended, and
# notes and unreadable lines are left exactly as typed.

ACCOUNT_TEXT = {"taxable": "taxable", "k401": "401k", "roth401k": "roth 401k", "ira": "ira", "roth": "roth",
                "hsa": "hsa"}


def _n(x: float) -> str:
    return f"{x:.2f}".rstrip("0").rstrip(".")


def holding_line(name: str, account: str, value: float, basis: float | None = None, rate: float | None = None,
                 state_exempt: bool = False) -> str:
    """One holdings line in the form parse_holdings reads back."""
    acct = ("tbills" if state_exempt else "cash") if account == "cash" else ACCOUNT_TEXT[account]
    parts = [str(name).replace(",", " ").strip(), acct, _n(value)]
    if basis is not None:
        parts.append(_n(basis))
    if rate is not None:
        parts.append(f"{_n(rate)}%")
    return ", ".join(parts)


class CannotApply(ValueError):
    """A step the dashboard cannot carry out for the owner (buying a
    property: its price, loan and rent are theirs to enter)."""


def apply_moves(saved: dict, moves: list[dict], board: dict) -> dict:
    """The saved profile after these moves are made. Sources: a holding is
    sold down by what moved (its basis in proportion) or removed; a property
    is removed (it is sold whole). Destinations: a debt's balance falls by
    what arrived (a debt paid off is removed); T-bills and a down-payment fund
    become T-bill lines; the picks become one line per top pick, split as the
    waterfall splits them (what a per-name cap leaves goes to T-bills, or to
    the account's index fund inside an IRA, Roth or HSA); the index fund
    becomes an index-fund line in the same account."""
    p = board["profile"]
    rf = K._f(p.get("rf_rate"), 4.0)
    prof = {k: v for k, v in saved.items() if not str(k).startswith("_")}
    lines: list[str | None] = str(prof.get("holdings") or "").splitlines()
    rows = {r["line"]: r for r in parse_holdings(prof.get("holdings"))[0]}
    owned = parse_owned_re(prof.get("owned_re"))
    debts = [dict(d) for d in K.parse_debts(prof.get("debts") or [])]
    sleeve = sleeve_of(board)
    sold_lines: dict[int, float] = {}
    sold_props: set[int] = set()
    adds: list[str] = []
    for m in moves:
        kind, got = m["to_kind"], float(m["proceeds"])
        if kind == "re":
            raise CannotApply("Buying a property is recorded under Real estate you own, with its own numbers.")
        if m.get("prop") is not None:
            sold_props.add(m["prop"])
        elif m.get("line") is not None and m["line"] in rows:
            sold_lines[m["line"]] = sold_lines.get(m["line"], 0.0) + float(m["sold"])
        else:
            raise CannotApply("That holding is no longer in your profile.")
        if kind == "debt":
            name = str(m["to_id"]).split(":", 1)[1]
            hit = [d for d in debts if d["name"] == name]
            if not hit:
                raise CannotApply(f"{name} is no longer in your debts.")
            hit[0]["balance"] = max(0.0, hit[0]["balance"] - got)
        elif kind in ("tbill", "dpfund"):
            adds.append(holding_line("Down payment fund" if kind == "dpfund" else "T-bills", "cash", got,
                                     rate=rf, state_exempt=True))
        elif kind == "index":
            adds.append(holding_line("Index fund", m["acct"], got))
        elif kind == "picks":
            acct = "taxable" if m["to_id"] == "picks" else m["acct"]
            n = len(sleeve)
            if not n:
                raise CannotApply("There are no top picks on the board right now.")
            cap = K._f(p.get("pick_max_pct"), 20.0) / 100
            w = min(1.0 / n, cap) if cap > 0 else 1.0 / n
            for r in sleeve:
                amt = round(got * w, 2)
                adds.append(holding_line(r["detail"]["ticker"], acct, amt, basis=amt if acct == "taxable" else None))
            rest = round(got - got * w * n, 2)
            if rest > 0.5:
                adds.append(holding_line("T-bills", "cash", rest, rate=rf, state_exempt=True) if acct == "taxable"
                            else holding_line("Index fund", acct, rest))
        else:
            raise CannotApply(f"Unknown destination: {kind}")
    for ln, sold in sold_lines.items():
        r = rows[ln]
        left = r["value"] - sold
        if left < 1:
            lines[ln] = None
        else:
            basis = r["basis"] * left / r["value"] if r["basis"] is not None and r["value"] > 0 else r["basis"]
            lines[ln] = holding_line(r["name"], r["account"], left, basis, r["rate"], r["state_exempt"])
    prof["holdings"] = "\n".join([x for x in lines if x is not None] + adds)
    prof["debts"] = [d for d in debts if d["balance"] > 0.5]
    prof["owned_re"] = [r for i, r in enumerate(owned) if i not in sold_props]
    return prof


def split_text(steps: list[dict], left: float, p: dict) -> str:
    """A waterfall's steps as monthly-split lines that measure_flows reads
    back at the same returns: the match and any 401(k) money into the plan's
    fund, the Roth IRA and the taxable buys as "Top picks", cash steps and a
    down-payment fund as T-bills, debt by its name."""
    rf = K._f(p.get("rf_rate"), 4.0)
    tot: dict[tuple, float] = {}
    for s in steps:
        k, amt = s["kind"], float(s["amount"])
        if k == "match" or k == "wrapper":
            key = ("Plan fund", "401k", None)
        elif k == "debt":
            key = (str(s.get("ref") or s["to"]).split(":", 1)[-1].replace("Pay down ", ""), "debt", None)
        elif k == "hsa":
            key = ("HSA fund", "hsa", None)
        elif k == "ira":
            key = ("Top picks", "roth", None)
        elif k == "stock":
            key = ("Top picks", "taxable", None)
        elif k == "re":
            key = ("Down payment fund", "tbills", rf)
        else:                                   # cash, tbill
            key = ("T-bills", "tbills", rf)
        tot[key] = tot.get(key, 0.0) + amt
    if left > 0.5:
        key = ("T-bills", "tbills", rf)
        tot[key] = tot.get(key, 0.0) + left
    return "\n".join(f"{name}, {acct}, {_n(v)}" + (f", {_n(rate)}%" if rate is not None else "")
                     for (name, acct, rate), v in tot.items() if v > 0.005)
