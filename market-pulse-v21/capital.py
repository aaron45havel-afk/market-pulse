"""The highest and best use of the next dollar — every use of capital on one
board, compared after tax and after friction, and this month's pay split
down a waterfall.

Two questions, one page (/capital):

  1. WHERE DOES THE NEXT DOLLAR EARN THE MOST? Debt paydown, T-bills, stock
     picks from the screens, and real estate from the underwriting engines,
     each reduced to the same unit: an AFTER-TAX ANNUAL RETURN NET OF
     FRICTION. Friction is how hard the asset is to get and to hold —
     transaction costs spread over the hold, your hours at your own hourly
     rate, and the months the money sits in T-bills while a down payment
     is saved. A 16% house hack that needs $40,000 you will not have for a
     year, and six hours a month, is not a 16% use of this month's pay.

  2. WHAT DOES THIS MONTH'S PAY DO? A fixed order for the uses that win by
     construction — a starter cushion, the employer match (an instant
     return), debt dearer than the market, the full emergency fund, the
     HSA and the IRA — then whatever is left goes to the top of the board.

NOTHING HERE IS A FORECAST DRESSED AS A FACT. Every stock return is the
screen's own estimate (Compounders' growth + buybacks + dividend ± multiple
drift, the Aristocrats' yield + dividend growth, Lynch's earnings yield +
growth), blended toward the market return by a weight the owner sets,
because one company's estimate is noisier than the market's. BRRRR and
flips have no "typical fixer price" in the data (/headroom removed that
guess on purpose), so they are shown as CONDITIONAL: the after-tax target
is reached if a fixer is bought at or below the price the engine solves.
Every assumption is on the page, in the profile, and editable.

Percentages are in PERCENT throughout (7.0 means 7%).
"""
from __future__ import annotations

import math
from datetime import date

# ── contribution limits ──────────────────────────────────────────────
# IRS figures for tax year 2026 (Notice 2025-67 for 401(k) and IRA; Rev.
# Proc. 2025-19 for HSA). Defaults only — the profile holds the room the
# owner actually has left this year.
LIMITS_2026 = {"k401": 24_500, "ira": 7_500, "hsa_self": 4_400, "hsa_family": 8_750}

PROFILE_DEFAULTS: dict = {
    # cash flow
    "monthly_invest": 0.0,        # $ a month left after living costs
    "cash": 0.0,                  # liquid cash today
    "monthly_expenses": 0.0,      # sizes the emergency fund
    "starter_months": 1.0,
    "emergency_months": 6.0,
    # taxes (percent)
    "fed_rate": 24.0,
    "state_rate": 9.3,
    "ltcg_rate": 15.0,
    "niit": False,
    "retire_rate": None,          # traditional-account withdrawals; None = today's fed + state
    # accounts and the room left this year
    "salary": 0.0,
    "match_pct": 0.0,             # employer matches up to this % of salary...
    "match_rate": 100.0,          # ...at this % of what you put in
    "k401_room": float(LIMITS_2026["k401"]),
    "hsa_eligible": False,
    "hsa_room": float(LIMITS_2026["hsa_self"]),
    "ira_room": float(LIMITS_2026["ira"]),
    # debts: [{"name", "balance", "apr"}]
    "debts": [],
    # your time
    "hourly_value": 50.0,
    # real estate
    "home_state": "CA",           # house hacks are owner-occupied: where you would live
    "home_zip": "",               # ...and how far from here
    "max_miles": 40.0,
    "housing_cost": 0.0,          # rent you pay today; a house hack or a home replaces it
    "house_hack_budget": 750_000.0,
    "re_target": 14.0,            # after-tax target the BRRRR and flip engines solve to
    "appreciation": 0.0,          # annual home-price change at exit (0 unless you choose one)
    "mortgage_rate": None,        # None = the latest 30-year rate from rates.json
    # stocks
    "picks_n": 8,
    "pick_max_pct": 20.0,         # of the stock sleeve, per name
    "market_return": 7.0,         # the hurdle a pick must beat
    "estimate_weight": 50.0,      # % on the screen's own estimate; the rest on the market return
    "picks_hours": 2.0,           # research hours a month for the whole stock sleeve
    "stock_holdings": 0.0,        # what the picks are worth today
    "rf_rate": 4.0,               # T-bills / high-yield savings
    "hold_years": 5,
}

# Hours a month each real-estate path takes, at a steady state. Editable
# through the profile under the same keys.
HOURS_DEFAULT = {"house_hack": 6.0, "brrrr": 10.0, "flip": 25.0, "home": 2.0}
# Months from "the money is there" to "the asset is owned".
DEPLOY_MONTHS = {"stock": 0, "house_hack": 3, "brrrr": 3, "flip": 3, "home": 3, "tbill": 0, "debt": 0}
# Round trip as % of the capital in it. Real-estate returns below come out of
# engines that already charge buying and selling, so theirs is zero here.
ROUND_TRIP_PCT = {"stock_large": 0.2, "stock_small": 2.0}
SMALL_SOURCES = {"Quiet Value", "Schloss"}

# Sanity bounds on any one estimate before blending.
EST_BOUNDS = (-20.0, 40.0)
GROWTH_CAP = 10.0


def profile_with_defaults(p: dict | None) -> dict:
    out = dict(PROFILE_DEFAULTS)
    for k, v in (p or {}).items():
        if k in PROFILE_DEFAULTS or k.startswith("hours_"):
            out[k] = v
    return out


# What the form may set, and how. Numbers are clamped to a sane range so a
# typo cannot make the board nonsense (a 900% tax rate, a negative hold).
_NUMBERS = {
    "monthly_invest": (0, 1e7), "cash": (0, 1e9), "monthly_expenses": (0, 1e7),
    "starter_months": (0, 24), "emergency_months": (0, 36),
    "fed_rate": (0, 60), "state_rate": (0, 20), "ltcg_rate": (0, 40), "retire_rate": (0, 70),
    "salary": (0, 1e8), "match_pct": (0, 50), "match_rate": (0, 300),
    "k401_room": (0, 1e6), "hsa_room": (0, 1e5), "ira_room": (0, 1e5),
    "hourly_value": (0, 10_000), "housing_cost": (0, 1e6), "max_miles": (1, 500),
    "house_hack_budget": (0, 1e8), "re_target": (0, 50), "appreciation": (-10, 10),
    "mortgage_rate": (0, 20), "picks_n": (1, 50), "pick_max_pct": (1, 100),
    "market_return": (-10, 30), "estimate_weight": (0, 100), "picks_hours": (0, 200),
    "stock_holdings": (0, 1e10), "rf_rate": (0, 20), "hold_years": (1, 30),
    "hours_house_hack": (0, 200), "hours_brrrr": (0, 200), "hours_flip": (0, 200), "hours_home": (0, 200),
}
_BOOLS = ("niit", "hsa_eligible")
_TEXT = {"home_state": 2, "home_zip": 5}
_OPTIONAL = ("retire_rate", "mortgage_rate")      # blank means "use the default rule"


def parse_profile(form: dict) -> dict:
    """The profile from what the form sent: numbers clamped, booleans read,
    debts parsed from "name, balance, apr" lines. Unknown keys are dropped."""
    out: dict = {}
    for k, (lo, hi) in _NUMBERS.items():
        if k not in form:
            continue
        v = form[k]
        if k in _OPTIONAL and (v is None or str(v).strip() == ""):
            out[k] = None
            continue
        x = _f(str(v).replace(",", "").replace("$", "").replace("%", "") if v is not None else None, None)
        if x is not None:
            out[k] = min(hi, max(lo, x))
    for k in _BOOLS:
        if k in form:
            out[k] = str(form[k]).lower() in ("1", "true", "on", "yes")
    for k, n in _TEXT.items():
        if k in form:
            out[k] = "".join(ch for ch in str(form[k] or "") if ch.isalnum())[:n].upper()
    if "debts" in form:
        out["debts"] = parse_debts(form["debts"])
    return out


def parse_debts(text) -> list[dict]:
    """'Car loan, 12000, 6.5' per line → [{"name", "balance", "apr"}]. A line
    that does not parse is skipped, not guessed at."""
    if isinstance(text, list):
        return [d for d in text if isinstance(d, dict) and d.get("name")]
    out = []
    for line in str(text or "").splitlines():
        parts = [s.strip() for s in line.split(",")]
        if len(parts) < 3 or not parts[0]:
            continue
        bal = _f(parts[1].replace("$", "").replace(" ", ""), None)
        apr = _f(parts[2].replace("%", ""), None)
        if bal is None or apr is None or bal < 0 or apr < 0:
            continue
        out.append({"name": parts[0][:40], "balance": bal, "apr": min(apr, 100.0)})
    return out


def debts_text(debts: list[dict]) -> str:
    return "\n".join(f"{d['name']}, {d['balance']:g}, {d['apr']:g}" for d in debts or [])


def _f(v, default=0.0) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


# ═══════════════════════════════════════════════════════════════════
# TAXES
# ═══════════════════════════════════════════════════════════════════

def tax_rates(p: dict) -> dict:
    """Marginal rates as fractions. T-bill interest is exempt from state tax;
    qualified dividends and long-term gains share one rate (+3.8% NIIT when
    the owner says it applies)."""
    fed, st = _f(p.get("fed_rate")), _f(p.get("state_rate"))
    niit = 3.8 if p.get("niit") else 0.0
    ret = p.get("retire_rate")
    return {"ordinary": (fed + st) / 100, "tbill": fed / 100,
            "qualified": (_f(p.get("ltcg_rate")) + st + niit) / 100,
            "retire": (_f(ret) if ret is not None else fed + st) / 100}


def after_tax_taxable(total_pct: float, div_pct: float, years: int,
                      t_div: float, t_cg: float) -> float:
    """Annualized after-tax return of $1 in a taxable account: dividends taxed
    each year and reinvested (raising the basis), the price gain taxed once,
    at the exit. A loss is not taxed (and its deduction is not counted)."""
    years = max(1, int(years))
    r, d = total_pct / 100, max(0.0, min(div_pct, max(total_pct, 0.0))) / 100
    price = r - d
    value = basis = 1.0
    for _ in range(years):
        div = value * d
        value = value * (1 + price) + div * (1 - t_div)
        basis += div * (1 - t_div)
    if value <= 0:
        return -100.0
    after = value - max(0.0, value - basis) * t_cg
    return (after ** (1 / years) - 1) * 100


def after_tax_traditional(total_pct: float, years: int, t_now: float, t_later: float) -> float:
    """The return on an after-tax dollar put into a traditional account: the
    deduction now buys 1/(1-t_now) dollars, which grow untaxed and are taxed at
    t_later on the way out. Equal rates give back the pre-tax return."""
    years = max(1, int(years))
    grown = (1 + total_pct / 100) ** years * (1 - t_later) / max(1e-9, 1 - t_now)
    return (grown ** (1 / years) - 1) * 100 if grown > 0 else -100.0


# ═══════════════════════════════════════════════════════════════════
# ROWS — one shape for every use of capital
# ═══════════════════════════════════════════════════════════════════

def row(*, id: str, kind: str, label: str, source: str, ret_pre: float | None,
        ret_after: float | None, basis: str, min_capital: float = 0.0,
        deploy_months: int = 0, round_trip_pct: float = 0.0, hours_month: float = 0.0,
        risk: str = "market", conditional: str | None = None, qualifies: bool | None = None,
        detail: dict | None = None, div_pct: float = 0.0, link: str | None = None) -> dict:
    return {"id": id, "kind": kind, "label": label, "source": source, "link": link or source,
            "ret_pre": ret_pre, "ret_after": ret_after, "basis": basis,
            "min_capital": float(min_capital or 0.0), "deploy_months": int(deploy_months or 0),
            "round_trip_pct": float(round_trip_pct or 0.0), "hours_month": float(hours_month or 0.0),
            "risk": risk, "conditional": conditional, "qualifies": qualifies,
            "detail": detail or {}, "div_pct": div_pct}


def blend(est: float, p: dict) -> float:
    """A screen's estimate for one company, pulled toward the market return
    by the owner's weight — one company's estimate is noisier than the
    market's, and a weight of 100 trusts the screen outright."""
    w = max(0.0, min(100.0, _f(p.get("estimate_weight"), 50.0))) / 100
    e = max(EST_BOUNDS[0], min(EST_BOUNDS[1], est))
    return w * e + (1 - w) * _f(p.get("market_return"), 7.0)


def stock_row(p: dict, *, ticker: str, name: str, source: str, est: float, div: float,
              basis: str, link: str, small: bool = False, detail: dict | None = None) -> dict:
    t = tax_rates(p)
    pre = blend(est, p)
    after = after_tax_taxable(pre, div, _f(p.get("hold_years"), 5), t["qualified"], t["qualified"])
    return row(id=f"stock:{ticker}", kind="stock", label=f"{ticker} — {name}", source=source,
               ret_pre=round(pre, 2), ret_after=round(after, 2),
               basis=f"{basis}; {p.get('estimate_weight', 50):g}% on that, the rest on the "
                     f"{_f(p.get('market_return'), 7):g}% market return",
               min_capital=1.0, deploy_months=0,
               round_trip_pct=ROUND_TRIP_PCT["stock_small" if small else "stock_large"],
               hours_month=0.0, risk="single stock", div_pct=div, link=link,
               detail={"ticker": ticker, "estimate": round(est, 2), **(detail or {})})


# ── stocks, from each screen's own estimate ─────────────────────────

def stocks_from_compounders(rows: list[dict], p: dict) -> list[dict]:
    out = []
    for r in rows:
        if r.get("status") not in ("COMPOUNDER", "QUALITY") or r.get("expected") is None:
            continue
        div = _f(r.get("er_div"), _f(r.get("div_yield")))
        out.append(stock_row(p, ticker=r["ticker"], name=r.get("name") or "", source="Compounders",
                             est=_f(r["expected"]), div=div, link="/compounders",
                             basis=(f"Compounders' estimate {r['expected']:g}%: growth {r.get('er_growth')}, "
                                    f"buybacks {r.get('er_buyback')}, dividend {r.get('er_div')}, "
                                    f"multiple drift {r.get('er_mult')}"),
                             detail={"status": r.get("status")}))
    return out


def stocks_from_aristocrats(rows: list[dict], p: dict) -> list[dict]:
    out = []
    for r in rows:
        if r.get("status") not in ("BUY", "VALUE") or r.get("y") is None:
            continue
        y, g = _f(r.get("y")), min(GROWTH_CAP, _f(r.get("dg5")))
        out.append(stock_row(p, ticker=r["t"], name=r.get("n") or "", source="Aristocrats",
                             est=y + g, div=y, link="/aristocrats",
                             basis=f"yield {y:g}% + dividend growth {g:g}% (5-yr, capped at {GROWTH_CAP:g}%)",
                             detail={"status": r.get("status")}))
    return out


def stocks_from_lynch(companies: list[dict], p: dict) -> list[dict]:
    out = []
    for r in companies:
        pe, g = _f(r.get("pe_ratio"), 0.0), r.get("growth")
        # Lynch's growth is a record: the EPS CAGR and how it was measured.
        if isinstance(g, dict):
            g = g.get("cagr")
        if pe <= 0 or g is None:
            continue
        g = max(0.0, min(GROWTH_CAP, _f(g)))
        out.append(stock_row(p, ticker=r["ticker"], name=r.get("name") or "", source="Lynch",
                             est=100 / pe + g, div=0.0, link="/lynch",
                             basis=f"earnings yield {100 / pe:.1f}% (P/E {pe:g}) + growth {g:g}% (capped)"))
    return out


def stocks_from_quiet_value(rows: list[dict], p: dict) -> list[dict]:
    out = []
    for r in rows:
        pe = (r.get("metrics") or {}).get("pe")
        if not pe or pe <= 0:
            continue
        div = _f((r.get("metrics") or {}).get("dividend"))
        out.append(stock_row(p, ticker=r["ticker"], name=r.get("name") or "", source="Quiet Value",
                             est=100 / pe, div=div, link="/quiet-value", small=True,
                             basis=f"earnings yield {100 / pe:.1f}% (P/E {pe:g}), no growth assumed",
                             detail={"days_to_build": r.get("days_to_build")}))
    return out


# Schloss's deepest discounts were about a third of book. Below that the
# market is saying the assets are not there (Franklin Street at 0.07x is an
# office landlord in distress), and a reversion estimate would rank the
# distress first. A yield above DIV_YIELD_CAP is a special dividend or a
# cut coming — the cap Compounders uses (SITE Centers read 193% on its
# liquidation distributions).
SCHLOSS_PTB_FLOOR = 1 / 3
DIV_YIELD_CAP = 8.0


def stocks_from_schloss(rows: list[dict], p: dict) -> list[dict]:
    """Below tangible book and clearing every gate. The estimate is HALF the
    discount closing over the owner's hold, plus the dividend — a reversion
    assumption, labelled as one, and deliberately the cautious half."""
    H = max(1, int(_f(p.get("hold_years"), 5)))
    out = []
    for r in rows:
        ptb = r.get("p_tangible_book")
        if (not r.get("gates_pass") or not r.get("below_book") or not ptb
                or ptb < SCHLOSS_PTB_FLOOR):
            continue
        price, dps = r.get("price"), (r.get("dividend") or {}).get("latest")
        y = min(DIV_YIELD_CAP, (dps / price * 100) if (price and dps) else 0.0)
        to = ptb + (1 - ptb) / 2
        rev = ((to / ptb) ** (1 / H) - 1) * 100
        out.append(stock_row(p, ticker=r["ticker"], name=r.get("name") or "", source="Schloss",
                             est=rev + y, div=y, link="/schloss", small=True,
                             basis=f"{ptb:g}x tangible book closing halfway, to {to:.2f}x, over {H} years "
                                   f"({rev:.1f}%/yr) + yield {y:.1f}% (capped at {DIV_YIELD_CAP:g}%) — a reversion "
                                   f"assumption"))
    return out


def merge_stocks(rows: list[dict]) -> list[dict]:
    """One row per ticker: the highest estimate wins, and every screen that
    named it is listed."""
    by: dict[str, dict] = {}
    for r in rows:
        t = r["detail"]["ticker"]
        if t not in by:
            by[t] = {**r, "sources": [r["source"]]}
            continue
        keep = by[t]
        srcs = keep["sources"] + [r["source"]]
        if (r["ret_after"] or -1e9) > (keep["ret_after"] or -1e9):
            keep = {**r}
        keep["sources"] = srcs
        by[t] = keep
    return list(by.values())


# ── real estate ──────────────────────────────────────────────────────

def house_hack_rows(hh: list[dict], p: dict, *, top: int = 3) -> list[dict]:
    """Owner-occupied 2-4 units in the state you would live in. The return is
    the year's cash benefit — the other units' rent less the full payment,
    vacancy and repairs, PLUS the rent you stop paying — over the cash to
    close. Rental income is mostly sheltered by depreciation; the sale costs
    are charged over the hold."""
    st = (p.get("home_state") or "").upper()
    H = max(1, int(_f(p.get("hold_years"), 5)))
    hrs = _f(p.get("hours_house_hack"), HOURS_DEFAULT["house_hack"])
    avoided = _f(p.get("housing_cost"))
    near = [x for x in hh if (x.get("state") or "").upper() == st]
    # A STATE IS NOT WHERE YOU LIVE. The first board put Calipatria — Imperial
    # County, 500 miles from the Bay — at the top of a NorCal owner's list.
    # With a home ZIP, only ZIPs within the commute radius stay.
    home = str(p.get("home_zip") or "").strip()
    if home:
        miles = _f(p.get("max_miles"), 40.0)
        coords = _zip_coords([home] + [str(x.get("zip")) for x in near])
        if home in coords:
            near = [x for x in near if str(x.get("zip")) in coords
                    and _miles(coords[home], coords[str(x.get("zip"))]) <= miles]
    out = []
    for r in near[:top]:
        cash = _f(r.get("cash_to_close"))
        price = _f(r.get("max_offer"))
        if cash <= 0 or price <= 0:
            continue
        benefit = (_f(r.get("monthly_surplus")) + avoided) * 12
        sale_drag = 0.07 * price / H
        ret = (benefit - sale_drag) / cash * 100
        out.append(row(id=f"hh:{r.get('zip')}", kind="re", label=f"House hack — {r.get('place')} ({r.get('zip')})",
                       source="Headroom", link=f"/headroom?mode=hh&state={st}",
                       ret_pre=round(ret, 1), ret_after=round(ret, 1),
                       basis=(f"(${_f(r.get('monthly_surplus')):,.0f}/mo after the full payment + ${avoided:,.0f}/mo "
                              f"rent you stop paying) × 12, less 7% selling costs over {H} years, "
                              f"on ${cash:,.0f} to close at ${price:,.0f}"),
                       min_capital=cash, deploy_months=DEPLOY_MONTHS["house_hack"], hours_month=hrs,
                       risk="leveraged, owner-occupied, one at a time",
                       detail={"price": price, "zip": r.get("zip"), "rent_label": r.get("rent_label")}))
    return out


def conditional_re_rows(board: list[dict], p: dict, *, mode: str, top: int = 3) -> list[dict]:
    """BRRRR or flip markets where some price clears the after-tax target. No
    fixer prices exist in the data, so the return IS the target, conditional
    on buying at or below the solved price — the share of the median it
    needs says how hard that deal is to find."""
    tgt = _f(p.get("re_target"), 14.0)
    hrs = _f(p.get(f"hours_{mode}"), HOURS_DEFAULT[mode])
    feas = sorted([r for r in board if r.get("feasible") and r.get("detail")],
                  key=lambda r: -(r.get("max_pct_median") or 0))[:top]
    out = []
    for r in feas:
        d = r["detail"]
        cash = _f(d.get("cash_in_peak") if mode == "brrrr" else d.get("equity"))
        label = "BRRRR" if mode == "brrrr" else "Flip"
        out.append(row(id=f"{mode}:{r['code']}", kind="re", label=f"{label} — {r.get('name') or r['code']}",
                       source="Headroom", link=f"/headroom?mode={mode}",
                       ret_pre=tgt, ret_after=tgt,
                       basis=(f"the {tgt:g}% after-tax target, reached at a purchase of "
                              f"${r['max_price']:,.0f} or less ({r['max_pct_median']:.0f}% of the "
                              f"${r['median_value']:,.0f} median)"),
                       min_capital=cash, deploy_months=DEPLOY_MONTHS[mode], hours_month=hrs,
                       risk="leveraged, renovation, remote" if mode == "brrrr" else "short hold, ordinary-income tax",
                       conditional=f"only if you buy at ≤ {r['max_pct_median']:.0f}% of the median",
                       detail={"max_price": r["max_price"], "pct_median": r["max_pct_median"]}))
    return out


def home_rows(buyable: list[dict], p: dict, *, rate_pct: float, top: int = 2) -> list[dict]:
    """Buying the home you live in (NorCal screen). The return is the rent you
    stop paying less the full cost of owning, plus the first year's principal,
    over the cash to close — negative where owning costs more than renting."""
    import re_assumptions as RA
    import underwrite as U
    H = max(1, int(_f(p.get("hold_years"), 5)))
    hrs = _f(p.get("hours_home"), HOURS_DEFAULT["home"])
    out = []
    for r in buyable[:top]:
        price, rent = _f(r.get("entry_price")), _f(r.get("median_rent"))
        if price <= 0 or rent <= 0:
            continue
        # The owner engine invents nothing: property tax and insurance come
        # from the same state tables /norcal and /multifamily use. Without
        # them it returns no costs at all, and this row silently vanished.
        st = (r.get("state") or p.get("home_state") or "CA").upper()
        tax = RA.tax_defaults(st, None, None)["owner_pct"]
        ins = RA.scaled_premium(RA.insurance_defaults(st)["owner_300k"], price)
        o = U.owner(price, rate_pct=rate_pct, market_rent=rent, tax_rate_pct=tax, insurance_annual=ins)
        cash, own = _f(o.get("cash_to_close")), _f(o.get("cost_of_owning_monthly"))
        loan = _f(o.get("loan_amount"))
        if cash <= 0:
            continue
        principal = loan - _balance_after(loan, rate_pct, 12)
        appr = _f(p.get("appreciation")) / 100 * price
        sale_drag = 0.07 * price / H
        ret = ((rent - own) * 12 + principal + appr - sale_drag) / cash * 100
        out.append(row(id=f"home:{r.get('zip')}", kind="re", label=f"Buy your home — {r.get('name')} ({r.get('zip')})",
                       source="NorCal", link="/norcal", ret_pre=round(ret, 1), ret_after=round(ret, 1),
                       basis=(f"rent ${rent:,.0f}/mo against ${own:,.0f}/mo to own, + ${principal:,.0f} "
                              f"principal in year one, appreciation {p.get('appreciation', 0):g}%, "
                              f"7% selling costs over {H} years, on ${cash:,.0f} to close at ${price:,.0f}"),
                       min_capital=cash, deploy_months=DEPLOY_MONTHS["home"], hours_month=hrs,
                       risk="leveraged, one home",
                       detail={"price": price, "zip": r.get("zip")}))
    return out


def _miles(a: tuple, b: tuple) -> float:
    lat1, lng1, lat2, lng2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lng2 - lng1) / 2) ** 2)
    return 3958.8 * 2 * math.asin(math.sqrt(h))


def _zip_coords(zips: list[str]) -> dict[str, tuple]:
    """{zip: (lat, lng)} from data/zips.db; missing ZIPs are absent."""
    import sqlite3
    from pathlib import Path
    db = Path(__file__).resolve().parent / "data" / "zips.db"
    if not db.exists() or not zips:
        return {}
    con = sqlite3.connect(str(db))
    try:
        q = ",".join("?" * len(zips))
        return {z: (lat, lng) for z, lat, lng in
                con.execute(f"SELECT zip, lat, lng FROM zips WHERE zip IN ({q})", list(zips))
                if lat is not None and lng is not None}
    finally:
        con.close()


def _balance_after(loan: float, rate_pct: float, months: int, years: int = 30) -> float:
    i = rate_pct / 100 / 12
    n = years * 12
    if i <= 0:
        return loan * (1 - months / n)
    pmt = loan * i / (1 - (1 + i) ** -n)
    return loan * (1 + i) ** months - pmt * ((1 + i) ** months - 1) / i


# ── guaranteed uses and tax wrappers ─────────────────────────────────

def guaranteed_rows(p: dict) -> list[dict]:
    t = tax_rates(p)
    rf = _f(p.get("rf_rate"), 4.0)
    out = [row(id="tbill", kind="tbill", label="T-bills / high-yield savings", source="Rates",
               ret_pre=rf, ret_after=round(rf * (1 - t["tbill"]), 2), risk="guaranteed",
               basis=f"{rf:g}%, federal tax only (T-bill interest is state-exempt)")]
    for d in p.get("debts") or []:
        apr, bal = _f(d.get("apr")), _f(d.get("balance"))
        if bal <= 0:
            continue
        out.append(row(id=f"debt:{d.get('name')}", kind="debt", label=f"Pay down {d.get('name')}",
                       source="Profile", ret_pre=apr, ret_after=apr, risk="guaranteed",
                       basis=f"a guaranteed {apr:g}% — the interest you stop paying",
                       detail={"balance": bal}))
    mr = _f(p.get("market_return"), 7.0)
    H = int(_f(p.get("hold_years"), 5))
    if _f(p.get("k401_room")) > 0:
        out.append(row(id="k401", kind="wrapper", label="401(k) beyond the match (plan funds)",
                       source="Profile", ret_pre=mr,
                       ret_after=round(after_tax_traditional(mr, H, t["ordinary"], t["retire"]), 2),
                       risk="market",
                       basis=(f"the {mr:g}% market return, deducted at {t['ordinary'] * 100:.1f}% now and taxed at "
                              f"{t['retire'] * 100:.1f}% later — most plans hold funds, not individual stocks")))
    return out


# ═══════════════════════════════════════════════════════════════════
# FRICTION — how hard the asset is to get and to hold
# ═══════════════════════════════════════════════════════════════════

def friction(r: dict, p: dict, *, monthly_free: float, cash_free: float,
             delay_months: int = 0) -> dict:
    """The row with `ret_net`: after-tax return less the round trip spread over
    the hold, less your hours at your hourly rate as a share of the capital
    in it, blended with T-bills for the average month the money waits while
    the minimum is saved. `monthly_free` is a normal month's share for the
    board; `delay_months` the one-time steps that come first.
    `months_to_fund` is None when the pay can never reach the minimum."""
    H = max(1.0, _f(p.get("hold_years"), 5))
    t = tax_rates(p)
    rf_after = _f(p.get("rf_rate"), 4.0) * (1 - t["tbill"])
    if r["ret_after"] is None:
        return {**r, "ret_net": None, "months_to_fund": None, "time_drag": None, "cost_drag": None}
    cost = r["round_trip_pct"] / H
    hours = r["hours_month"]
    if r["kind"] == "stock":
        # Research time is spent on the whole sleeve, so it is charged against
        # the money the sleeve manages on average over the hold: what is held
        # now plus half of what the hold adds.
        hours = _f(p.get("picks_hours"), 2.0)
        capital = max(_f(p.get("stock_holdings")) + monthly_free * 12 * H / 2, 1.0)
    else:
        capital = max(r["min_capital"], 1.0)
    time = hours * 12 * _f(p.get("hourly_value")) / capital * 100
    need = max(0.0, r["min_capital"] - cash_free)
    # A $1 minimum is not something to save up for: a stock is bought with
    # whatever this month has, so it is ready now.
    if need <= 1.0:
        months = 0
    elif monthly_free > 0:
        months = delay_months + math.ceil(need / monthly_free)
    else:
        months = None
    if months is None:
        return {**r, "ret_net": None, "months_to_fund": None, "time_drag": round(time, 2),
                "cost_drag": round(cost, 2)}
    wait_years = (months / 2 + r["deploy_months"]) / 12
    w = min(1.0, wait_years / (H + wait_years))
    net = r["ret_after"] - cost - time
    eff = (1 - w) * net + w * rf_after
    return {**r, "ret_net": round(eff, 2), "months_to_fund": months,
            "time_drag": round(time, 2), "cost_drag": round(cost, 2), "wait_weight": round(w, 3)}


def rank(rows: list[dict]) -> list[dict]:
    """Best friction-adjusted return first; rows that cannot be funded last.
    A guaranteed use wins a tie."""
    order = {"debt": 0, "tbill": 1, "wrapper": 2, "re": 3, "stock": 4}
    return sorted(rows, key=lambda r: (r.get("ret_net") is None, -(r.get("ret_net") or 0),
                                       order.get(r["kind"], 9)))


# ═══════════════════════════════════════════════════════════════════
# THE MONTH — a waterfall
# ═══════════════════════════════════════════════════════════════════

class _Purse:
    """The month's money, handed out one step at a time."""

    def __init__(self, amount: float):
        self.left = round(max(0.0, amount), 2)
        self.steps: list[dict] = []

    def take(self, amount, to, why, kind) -> float:
        amt = round(max(0.0, min(self.left, amount)), 2)
        if amt > 0:
            self.steps.append({"to": to, "amount": amt, "why": why, "kind": kind})
            self.left = round(self.left - amt, 2)
        return amt


def fixed_steps(p: dict, today: date | None = None) -> _Purse:
    """The steps that win by construction, in order. What is left is what the
    board competes for — and what a down payment is saved from."""
    today = today or date.today()
    months_left = 13 - today.month
    purse = _Purse(_f(p.get("monthly_invest")))
    take = purse.take
    cash = _f(p.get("cash"))
    exp = _f(p.get("monthly_expenses"))
    mr = _f(p.get("market_return"), 7.0)

    starter = exp * _f(p.get("starter_months"), 1.0)
    if cash < starter:
        take(starter - cash, "Starter cushion (high-yield savings)",
             "One month of expenses first: without it the next surprise goes on a card at its rate.", "cash")

    match_monthly = _f(p.get("salary")) * _f(p.get("match_pct")) / 100 / 12
    if match_monthly > 0 and _f(p.get("k401_room")) > 0:
        take(min(match_monthly, _f(p.get("k401_room")) / months_left), "401(k) up to the employer match",
             f"An instant {_f(p.get('match_rate'), 100):g}% return before the money is even invested.", "match")

    for d in sorted(p.get("debts") or [], key=lambda d: -_f(d.get("apr"))):
        if _f(d.get("apr")) >= mr and _f(d.get("balance")) > 0:
            take(_f(d.get("balance")), f"Pay down {d.get('name')}",
                 f"A guaranteed {_f(d.get('apr')):g}% beats the {mr:g}% the market is expected to pay.", "debt")

    ef = exp * _f(p.get("emergency_months"), 6.0)
    if cash < ef:
        take(ef - cash - sum(s["amount"] for s in purse.steps if s["kind"] == "cash"),
             "Emergency fund (high-yield savings)",
             f"{_f(p.get('emergency_months'), 6):g} months of expenses, so nothing below has to be sold at a bad time.",
             "cash")

    if p.get("hsa_eligible") and _f(p.get("hsa_room")) > 0:
        ca = (p.get("home_state") or "").upper() in ("CA", "NJ")
        take(_f(p.get("hsa_room")) / months_left, "HSA (invested)",
             "Deductible going in, untaxed growth, untaxed out for medical costs"
             + (" — federally; your state taxes HSAs." if ca else "."), "hsa")

    if _f(p.get("ira_room")) > 0:
        take(_f(p.get("ira_room")) / months_left, "Roth IRA — your top pick(s)",
             "Growth never taxed. The highest-return pick belongs here; contributions can be withdrawn if a "
             "down payment needs them.", "ira")
    purse.months_left = months_left
    return purse


def steady_free(p: dict) -> float:
    """What a normal month leaves for the board: the pay less the RECURRING
    fixed steps (the match, a twelfth of the HSA and IRA room). The one-time
    steps — topping up the cushion and the emergency fund, clearing debt above
    the hurdle — are timed separately (one_time_months), so a month spent
    clearing a card does not read as "never" for every down payment."""
    match = _f(p.get("salary")) * _f(p.get("match_pct")) / 100 / 12
    if _f(p.get("k401_room")) <= 0:
        match = 0.0
    hsa = _f(p.get("hsa_room")) / 12 if p.get("hsa_eligible") else 0.0
    ira = _f(p.get("ira_room")) / 12
    return max(0.0, _f(p.get("monthly_invest")) - match - hsa - ira)


def one_time_months(p: dict) -> int | None:
    """Months the one-time steps take before the board's share starts: the
    emergency fund's shortfall and every debt above the hurdle, paid from the
    steady free amount. None when nothing is free to pay them."""
    need = max(0.0, _f(p.get("monthly_expenses")) * _f(p.get("emergency_months"), 6.0) - _f(p.get("cash")))
    mr = _f(p.get("market_return"), 7.0)
    need += sum(_f(d.get("balance")) for d in p.get("debts") or [] if _f(d.get("apr")) >= mr)
    if need <= 0:
        return 0
    free = steady_free(p)
    return math.ceil(need / free) if free > 0 else None


def _fixed_debt(r: dict, p: dict) -> bool:
    """A debt the fixed steps already pay — not a candidate for what is left."""
    return r["kind"] == "debt" and _f(r.get("ret_pre")) >= _f(p.get("market_return"), 7.0)


def waterfall(p: dict, ranked: list[dict], today: date | None = None) -> dict:
    """This month's pay, step by step: the fixed steps, then the remainder to
    the top of the board. Returns {"steps", "left" (unassigned), "winner"}."""
    purse = fixed_steps(p, today)
    take = purse.take
    winner = next((r for r in ranked if r.get("ret_net") is not None and not _fixed_debt(r, p)), None)
    if purse.left > 0 and winner:
        if winner["kind"] == "re":
            m = winner.get("months_to_fund")
            take(purse.left, f"Down-payment fund for {winner['label']} (T-bills)",
                 (f"The best use after friction. About {m} month{'s' if m != 1 else ''} to the "
                  f"${winner['min_capital']:,.0f} it takes.") if m else
                 f"The best use after friction, and the ${winner['min_capital']:,.0f} it takes is already there.",
                 "re")
        elif winner["kind"] == "stock":
            picks = [r for r in ranked if r["kind"] == "stock" and r.get("ret_net") is not None]
            n = max(1, min(int(_f(p.get("picks_n"), 8)), len(picks)))
            cap = _f(p.get("pick_max_pct"), 20.0) / 100
            weight = min(1.0 / n, cap) if cap > 0 else 1.0 / n
            each = round(purse.left * weight, 2)
            for r in picks[:n]:
                take(each, f"Buy {r['detail']['ticker']} (taxable)",
                     f"{r['ret_net']:g}% after tax and friction — {r['source']}.", "stock")
        else:
            take(purse.left, winner["label"],
                 f"{winner['ret_net']:g}% after tax and friction — the best use left.", winner["kind"])
    return {"steps": purse.steps, "left": purse.left, "winner": winner,
            "months_left_in_year": purse.months_left}


# ═══════════════════════════════════════════════════════════════════
# THE BOARD — read every page
# ═══════════════════════════════════════════════════════════════════

def build(p: dict, *, today: date | None = None, sources: dict | None = None) -> dict:
    """Every use of capital, scored, ranked, and this month's waterfall.
    `sources` lets tests pass each page's rows; otherwise each page's own
    module is read, and a page that fails is named, not hidden."""
    p = profile_with_defaults(p)
    errors: dict[str, str] = {}
    src = dict(sources or {})

    def load(name, fn):
        if name in src:
            return src[name]
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — one broken page must not blank the board
            errors[name] = f"{type(e).__name__}: {e}"
            return []

    rate = _f(p.get("mortgage_rate")) or _latest_mortgage_rate()
    rows: list[dict] = guaranteed_rows(p)
    stocks = (stocks_from_compounders(load("compounders", _compounders_rows), p)
              + stocks_from_aristocrats(load("aristocrats", _aristocrats_rows), p)
              + stocks_from_lynch(load("lynch", _lynch_rows), p)
              + stocks_from_quiet_value(load("quiet_value", _quiet_value_rows), p)
              + stocks_from_schloss(load("schloss", _schloss_rows), p))
    rows += merge_stocks(stocks)
    rows += house_hack_rows(load("house_hack", lambda: _hh_rows(p, rate)), p)
    rows += conditional_re_rows(load("brrrr", lambda: _re_board("brrrr", p, rate)), p, mode="brrrr")
    rows += conditional_re_rows(load("flip", lambda: _re_board("flip", p, rate)), p, mode="flip")
    rows += home_rows(load("home", lambda: _home_rows(p)), p, rate_pct=rate)

    ef = _f(p.get("monthly_expenses")) * _f(p.get("emergency_months"), 6.0)
    cash_free = max(0.0, _f(p.get("cash")) - ef)
    # What the board competes for in a normal month: the pay left after the
    # recurring fixed steps. A down payment is saved from this, starting once
    # the one-time steps (cushion, emergency fund, dear debt) are done.
    monthly_free = steady_free(p)
    delay = one_time_months(p)
    scored = rank([friction(r, p, monthly_free=monthly_free if delay is not None else 0.0,
                            cash_free=cash_free, delay_months=delay or 0) for r in rows])
    return {"profile": p, "rows": scored, "plan": waterfall(p, scored, today),
            "errors": errors, "mortgage_rate": rate,
            "counts": {k: sum(1 for r in scored if r["kind"] == k) for k in ("stock", "re", "debt", "tbill", "wrapper")}}


# ── page readers (each page's own module, never a copy of its logic) ──

def _latest_mortgage_rate() -> float:
    import json
    from pathlib import Path
    try:
        d = json.loads((Path(__file__).resolve().parent / "data" / "rates.json").read_text())
        return float(d["mortgage_30y"])
    except Exception:  # noqa: BLE001
        return 7.0


def _compounders_rows():
    import compounders as C
    return C.score()


def _aristocrats_rows():
    import aristocrats as A
    return A.score()


def _lynch_rows():
    import json
    from pathlib import Path
    d = Path(__file__).resolve().parent / "data" / "lynch_snapshots"
    latest = sorted(d.glob("*.json"))[-1]
    return json.loads(latest.read_text()).get("companies") or []


def _quiet_value_rows():
    import json
    from pathlib import Path
    import main
    blob = json.loads((Path(__file__).resolve().parent / "data" / "quiet_value.json").read_text())
    out, _counts = main.quiet_value_board(blob.get("rows") or [])
    return out


def _schloss_rows():
    import schloss as S
    return S.board()["qualify"]


def _hh_rows(p, rate):
    import headroom as H
    # Wide enough that the commute-radius filter has something to choose from.
    return H.zip_board_hh(state=(p.get("home_state") or "").upper() or None, rate_pct=rate,
                          max_price=_f(p.get("house_hack_budget"), 750_000.0), top=1000)


def _re_board(mode, p, rate):
    import headroom as H
    return H.build_board(mode=mode, rate_pct=rate, target=_f(p.get("re_target"), 14.0),
                         appreciation=_f(p.get("appreciation")) / 100)


def _home_rows(p):
    import norcal as N
    ef = _f(p.get("monthly_expenses")) * _f(p.get("emergency_months"), 6.0)
    return N.screen(assets=max(0.0, _f(p.get("cash"))), reserves=ef).get("buyable") or []
