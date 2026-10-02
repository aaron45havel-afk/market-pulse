"""Headroom — the remodel deal engine. /headroom

Answers, per market: at current rates and costs, what is the MAXIMUM
purchase price per sqft at which a levered remodel value-add still clears
the owner's AFTER-TAX compounded-return target (default 14%/yr) — and how
does that compare with what fixers actually trade for (headroom)?

Modes:
  BRRRR (default) — hard-money acquisition (purchase + rehab funded, LTARV
    capped), renovate, DSCR cash-out refi at 75% of ARV around month 6,
    rent at the market median, 5-yr hold, sell. Return = after-tax IRR on
    a monthly cash-flow grid (Sec 469 passive-loss suspension, 27.5-yr
    depreciation, non-taxable refi proceeds, LTCG + recapture + NIIT at
    exit, state exclusions). Max price found by bisection.
  FLIP — dealer sale: ordinary income + SE tax. Bisection on the same
    after-tax basis; the pre-tax closed form is kept as a cross-check.

Calibration tables live in data/headroom/*.json (51-state property tax at
INVESTOR/new-purchase effective rates, landlord + renovation insurance,
vacant-home utilities, state income tax + exit rules, July-2026 financing
terms). Researched by a web swarm; verification status is stamped in each
file's _meta.status. Rehab costs come from value_add.remodel_budget's
validated 107-market model.

All rates in PERCENT form in the tables; normalized to fractions here.
First-pass underwriting for ranking markets — not tax or investment advice.
"""
from __future__ import annotations

import json
import os
import sqlite3
import statistics
import tempfile
import threading
from collections import OrderedDict
from functools import lru_cache
from pathlib import Path

import househack as HH
import rent_ladder as RL

_DATA = Path(__file__).resolve().parent / "data" / "headroom"

TARGET_DEFAULT = 14.0          # after-tax compounded %/yr
PROFIT_FLOOR = 25_000          # min forced equity (BRRRR) / net profit (flip), owner lock
HOLD_YEARS = 5
FED_ORDINARY = 0.24            # OBBBA-permanent bracket at the owner persona
FED_LTCG = 0.15
NIIT = 0.038                   # exit year only (gain lifts MAGI over threshold)
RECAPTURE = min(0.25, FED_ORDINARY)
BUILDING_SHARE = 0.80          # of (purchase + buy closing) depreciable, 27.5-yr
DEP_YEARS = 27.5
BUY_CLOSING_PCT = 0.015
SELL_COST_PCT = 0.07
VACANCY = 0.08                 # mirrors structural.VACANCY_FACTOR = 0.92
MAINTENANCE_PCT = 0.015        # of home value / yr, mirrors structural
RENO_MONTHS = {"cosmetic": 2, "moderate": 4, "gut": 6}
REFI_MONTH_MIN = 6             # DSCR full-ARV programs at 3-6 mo seasoning

# Exit-tax specials from the inctax research (state LTCG exclusions apply to
# the LTCG slice; MO/OK/WA exempt the whole gain per the research notes).
_STATE_LTCG_EXCL = {"MO": 1.0, "OK": 1.0, "ID": 0.60, "AR": 0.50, "SC": 0.44,
                    "ND": 0.40, "VT": 0.40, "WI": 0.30, "AZ": 0.25}
_STATE_EXIT_RATE_OVERRIDE = {"WA": 0.0, "HI": 7.25, "MT": 4.1, "MA": 5.0}
_FULL_GAIN_EXEMPT = {"MO", "OK", "WA"}
_NO_STATE_LOSS_CARRYFORWARD = {"PA", "NJ"}


@lru_cache(maxsize=None)
def _cal(item: str) -> dict:
    with open(_DATA / f"{item}.json") as f:
        return json.load(f)


def _state_of(market_code: str) -> str:
    return "DC" if market_code.upper() == "DC" else market_code.split("-")[0].upper()


def state_costs(market_code: str) -> dict:
    """Per-state carry/tax constants for a market code, normalized to
    fractions (tables are percent-form by convention)."""
    st = _state_of(market_code)
    pt = _cal("proptax")["table"].get(st, {"rate_pct": 1.1})
    ins = _cal("insurance")["table"].get(st, {"landlord_annual": 1800, "reno_multiplier": 1.6})
    utl = _cal("utilities")["table"].get(st, {"monthly_usd": 250})
    inc = _cal("inctax")["table"].get(st, {"marginal_pct": 4.5})
    return {
        "state": st,
        "proptax": pt["rate_pct"] / 100.0,          # of purchase price / yr
        "ins_landlord": float(ins["landlord_annual"]),
        "ins_reno_mult": float(ins.get("reno_multiplier", 1.6)),
        "utilities_mo": float(utl["monthly_usd"]),
        "state_income": inc["marginal_pct"] / 100.0,
    }


def financing_terms(rate_pct: float) -> dict:
    """July-2026 product terms anchored to the live 30-yr rate (percent in,
    fractions out)."""
    t = _cal("financing")["table"]
    hm, dscr = t["HARD_MONEY"], t["DSCR"]
    return {
        "hm_rate": (rate_pct + hm["rate_spread_over_mortgage30us_pct"]) / 100.0,
        "hm_points": hm["origination_points_pct"] / 100.0,
        "hm_fixed": 2_000.0 + 1_000.0,               # fixed fees + ~4 draws
        "hm_max_ltc": float(hm["max_ltc"]),
        "hm_max_purchase_adv": float(hm["max_purchase_advance"]),
        "hm_max_ltarv": float(hm["max_ltarv"]),
        # DSCR cash-out: spread + cash-out add (per research rules)
        "refi_rate": (rate_pct + dscr["spread_over_mortgage30us_pct"] + 0.25) / 100.0,
        "refi_points": 0.02,
        "refi_fixed": float(dscr["fixed_closing_costs_usd"]),
        "refi_max_ltv": float(dscr["typical_ltv"]),
        "refi_min_dscr": 1.0,                        # floor; <1.0 → BRRRR fails
    }


def _pmt(loan: float, annual_rate: float, years: int = 30) -> float:
    r = annual_rate / 12.0
    n = years * 12
    if r <= 0:
        return loan / n
    return loan * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def _amort(loan: float, annual_rate: float, months: int, years: int = 30):
    """Yield (interest, principal) per month for `months` months."""
    r = annual_rate / 12.0
    pay = _pmt(loan, annual_rate, years)
    bal = loan
    out = []
    for _ in range(months):
        i = bal * r
        p = pay - i
        bal -= p
        out.append((i, p))
    return out, bal


def brrrr_after_tax_irr(price: float, market: dict, inputs: dict) -> dict | None:
    """Monthly-grid after-tax cash-flow simulation of one BRRRR deal.

    market: {code, arv, rent, appreciation} — arv/rent are deal-level
      dollars (metro median × calibration happens upstream); appreciation
      is the annual fraction the caller chose (the page's input, 0% unless
      the user sets one — a trailing price trend is not a forecast).
    inputs: {rehab, sqft, scope, rate_pct, target, fed_ordinary}

    Timeline: buy with hard money; renovate for RENO_MONTHS (vacant:
    renovation insurance, utilities on the owner); RENTED FROM THE MONTH
    AFTER THE REMODEL at market rent less VACANCY, landlord insurance and
    maintenance, hard-money interest still running, while the DSCR cash-out
    refi seasons; refi; hold to month 60; sell.

    Taxes: placed in service when first rented, so rental income and
    depreciation start then. A year's rental loss is suspended (Sec 469)
    and offsets later years' rental income before that is taxed — except
    for state tax in PA and NJ, which allow no carryforward — and what is
    left is released at the sale. Refi points amortize over the loan's 30
    years, prorated by month, and the unamortized rest is deducted when the
    sale pays the loan off.

    Returns metrics, or None when the deal is structurally infeasible (rent
    can't cover the refi's tax and insurance — no DSCR loan at all)."""
    R = inputs["rehab"]
    arv, rent = market["arv"], market["rent"]
    app = market.get("appreciation", 0.0)
    sc = state_costs(market["code"])
    fin = financing_terms(inputs["rate_pct"])
    fed = inputs.get("fed_ordinary", FED_ORDINARY)
    s_inc = sc["state_income"]
    st = sc["state"]
    state_carries = st not in _NO_STATE_LOSS_CARRYFORWARD

    m_reno = RENO_MONTHS.get(inputs.get("scope", "moderate"), 4)
    refi_m = max(REFI_MONTH_MIN, m_reno + 1)
    in_service = m_reno + 1                  # the first month rented
    hold_m = HOLD_YEARS * 12
    cf = [0.0] * (hold_m + 1)

    # ── Acquisition: hard money, purchase advance + rehab draws ──
    commitment = min(fin["hm_max_ltc"] * (price + R),
                     fin["hm_max_purchase_adv"] * price + R,
                     fin["hm_max_ltarv"] * arv)
    purchase_adv = min(fin["hm_max_purchase_adv"] * price, commitment)
    rehab_funded = max(0.0, min(R, commitment - purchase_adv))
    hm_costs = fin["hm_points"] * commitment + fin["hm_fixed"]
    closing = BUY_CLOSING_PCT * price
    cf[0] = -((price - purchase_adv) + closing + hm_costs)

    # Operating figures once rented, before and after the refi.
    tax_yr = sc["proptax"] * price
    ins_yr = sc["ins_landlord"]
    maint_yr = MAINTENANCE_PCT * arv
    noi_mo = (rent * 12 * (1 - VACANCY) - tax_yr - ins_yr - maint_yr) / 12.0

    # Renovation (draws, carry), then rented while the refi seasons.
    drawn = purchase_adv
    carry_paid = 0.0                         # cash the owner adds after closing, before the refi
    reno_ins_mo = sc["ins_landlord"] * sc["ins_reno_mult"] / 12.0
    tax_mo = tax_yr / 12.0
    seasoning_taxable = 0.0                  # rental result of the months rented before the refi
    for m in range(1, refi_m + 1):
        if m <= m_reno:
            drawn += rehab_funded / m_reno
            interest = drawn * fin["hm_rate"] / 12.0
            carry = interest + tax_mo + reno_ins_mo + sc["utilities_mo"]
            carry_paid += carry
            cf[m] -= carry + (R - rehab_funded) / m_reno
        else:
            interest = drawn * fin["hm_rate"] / 12.0
            net = noi_mo - interest
            seasoning_taxable += net
            carry_paid += max(0.0, -net)
            cf[m] += net

    # ── Refi: DSCR cash-out at 75% ARV, gated by DSCR at market rent ──
    max_by_ltv = fin["refi_max_ltv"] * arv
    # DSCR = gross rent / PITIA ≥ floor → loan cap from the payment side
    pitia_cap = rent / fin["refi_min_dscr"] - (tax_yr + ins_yr) / 12.0
    if pitia_cap <= 0:
        return None
    r12 = fin["refi_rate"] / 12.0
    max_by_dscr = pitia_cap * ((1 + r12) ** 360 - 1) / (r12 * (1 + r12) ** 360)
    refi_loan = min(max_by_ltv, max_by_dscr)
    refi_costs = fin["refi_points"] * refi_loan + fin["refi_fixed"]
    cash_out = refi_loan - drawn - refi_costs
    cf[refi_m] += cash_out

    # ── Hold: rent less operating costs, financed by the refi loan ──
    sched, exit_balance = _amort(refi_loan, fin["refi_rate"], hold_m - refi_m)
    dep_basis = BUILDING_SHARE * (price + closing) + R
    annual_dep = dep_basis / DEP_YEARS
    points = fin["refi_points"] * refi_loan

    susp_fed = susp_st = 0.0                 # suspended losses, federal and state
    accum_dep = points_taken = 0.0
    years = []
    mi = 0                                   # months consumed from the schedule
    for y in range(1, HOLD_YEARS + 1):
        year_end = min(12 * y, hold_m)
        months = max(0, year_end - max(refi_m, 12 * (y - 1)))          # after the refi
        rented = max(0, year_end - max(in_service - 1, 12 * (y - 1)))  # in service
        seg = sched[mi:mi + months]
        mi += months
        interest_y = sum(i for i, _ in seg)
        principal_y = sum(p for _, p in seg)
        cfy = noi_mo * months - (interest_y + principal_y)
        dep_y = annual_dep * rented / 12.0
        accum_dep += dep_y
        pts_y = points * months / 360.0
        points_taken += pts_y
        taxable = (noi_mo * months - interest_y - dep_y - pts_y
                   + (seasoning_taxable if y == 1 else 0.0))
        use_f = use_s = 0.0
        if taxable >= 0:
            use_f = min(susp_fed, taxable)
            use_s = min(susp_st, taxable)
            susp_fed -= use_f
            susp_st -= use_s
            tax_bill = (taxable - use_f) * fed + (taxable - use_s) * s_inc
        else:
            tax_bill = 0.0
            susp_fed += -taxable
            if state_carries:
                susp_st += -taxable
        cf[year_end] += cfy - tax_bill
        years.append({"taxable": taxable, "loss_used": use_f, "loss_used_state": use_s,
                      "tax": tax_bill, "dep": dep_y, "points": pts_y, "rented_months": rented})

    # ── Exit at month 60 ──
    sale = arv * (1 + app) ** HOLD_YEARS
    amount_realized = sale * (1 - SELL_COST_PCT)
    adj_basis = price + closing + R - accum_dep
    gain = amount_realized - adj_basis
    if gain >= 0:
        recap = min(gain, accum_dep)
        ltcg = gain - recap
        fed_tax = recap * RECAPTURE + ltcg * FED_LTCG + gain * NIIT
        if st in _FULL_GAIN_EXEMPT:
            state_tax = gain * (_STATE_EXIT_RATE_OVERRIDE.get(st, 0.0) / 100.0 if st == "WA" else 0.0)
        else:
            s_exit = (_STATE_EXIT_RATE_OVERRIDE[st] / 100.0) if st in _STATE_EXIT_RATE_OVERRIDE else s_inc
            excl = _STATE_LTCG_EXCL.get(st, 0.0)
            state_tax = (recap + ltcg * (1 - excl)) * s_exit
    else:
        fed_tax = gain * (fed)               # Sec 1231 ordinary loss benefit (negative tax)
        state_tax = gain * s_inc
    unamortized = points - points_taken      # deducted when the sale pays the loan off
    release = susp_fed * fed + susp_st * s_inc + unamortized * (fed + s_inc)
    exit_cf = amount_realized - exit_balance - fed_tax - state_tax + release
    cf[hold_m] += exit_cf

    irr_m = _irr_monthly(cf)
    if irr_m is None:
        return None
    forced_equity = arv - (price + closing + R + carry_paid + hm_costs)
    return {
        "irr_annual": (1 + irr_m) ** 12 - 1,
        # Net after-tax dollars the deal makes over the full hold (all cash
        # out minus all cash in). The $25k floor gates on THIS for a hold:
        # yield-driven pockets earn their $25k from five years of cash flow,
        # not from a day-one equity pop.
        "total_profit": sum(cf),
        "cash_in_peak": -cf[0] + carry_paid + (R - rehab_funded),
        "cash_out": cash_out,
        "forced_equity": forced_equity,
        "dscr": rent / (_pmt(refi_loan, fin["refi_rate"]) + (tax_yr + ins_yr) / 12.0),
        "refi_loan": refi_loan,
        # Which cap sized the refi: the rent (DSCR) or 75% of ARV.
        "refi_cap_ltv": max_by_ltv,
        "refi_cap_rent": max_by_dscr,
        "refi_by": "rent" if max_by_dscr < max_by_ltv else "ltv",
        "rented_from_month": in_service,
        "suspended_released": release,
        "points_at_payoff": unamortized,
        "suspended_left_fed": susp_fed,
        "suspended_left_state": susp_st,
        "points_total": points,
        "exit_value": sale,
        "noi_month": noi_mo,
        "years": years,
        "cashflows": cf,
    }


def _irr_monthly(cf: list[float]) -> float | None:
    """Bisection IRR on a monthly cash-flow list. Bracket [-0.9, 1.0]/mo.
    numpy-vectorized NPV — the national board runs ~60 bisections × 107
    markets per build, so this is the hot loop."""
    import numpy as np
    c = np.asarray(cf)
    idx = np.arange(len(cf))

    def npv(r):
        return float(np.dot(c, (1 + r) ** (-idx)))
    lo, hi = -0.9, 1.0
    flo, fhi = npv(lo), npv(hi)
    if flo * fhi > 0:
        return None
    for _ in range(80):
        mid = (lo + hi) / 2
        fm = npv(mid)
        if flo * fm <= 0:
            hi = mid
        else:
            lo, flo = mid, fm
    return (lo + hi) / 2


# The limits that can stop the price going higher, and what the page calls them.
LIMIT_LABEL = {"target": "return target", "floor": "$25k profit floor",
               "refi": "rent can't carry the refi", "search": "search ceiling"}
NO_PATH_LABEL = {"target": "no price reaches the return target",
                 "floor": "no price clears the $25k floor",
                 "refi": "rent can't carry a refi at any price"}


def _brrrr_limits(m: dict | None, target: float, floor: float) -> list[str]:
    """Which limits a simulated BRRRR deal fails (target is a fraction)."""
    if m is None:
        return ["refi"]
    out = []
    if m["irr_annual"] < target:
        out.append("target")
    if m["total_profit"] < floor:
        out.append("floor")
    return out


def brrrr_max_price(market: dict, inputs: dict) -> dict | None:
    """Highest purchase price where after-tax IRR ≥ target AND the deal's
    total after-tax profit over the hold ≥ the $25k floor. Bisection (IRR
    is monotone-decreasing in P). `binding` names the limit a dollar more
    would break; None when no price works."""
    target = inputs.get("target", TARGET_DEFAULT) / 100.0
    floor = inputs.get("profit_floor", PROFIT_FLOOR)

    def ok(p):
        return not _brrrr_limits(brrrr_after_tax_irr(p, market, inputs), target, floor)

    lo, hi = 1_000.0, market["arv"] * 1.5
    if not ok(lo):
        return None                          # even ~free fails → infeasible market
    if ok(hi):
        hi = market["arv"] * 3
    for _ in range(60):
        mid = (lo + hi) / 2
        if ok(mid):
            lo = mid
        else:
            hi = mid
    limits = _brrrr_limits(brrrr_after_tax_irr(hi, market, inputs), target, floor)
    metrics = brrrr_after_tax_irr(lo, market, inputs)
    return {"max_price": lo, "max_psf": lo / inputs["sqft"],
            "binding": limits[0] if limits else "search", "limits": limits, **(metrics or {})}


# ── FLIP mode (secondary): dealer after-tax, bisection + pre-tax closed form ──

def flip_pretax_max_price(arv: float, rehab: float, market_code: str,
                          rate_pct: float, months: int = 6, *, target_pct: float = 14.0,
                          down: float = 0.25, sqft: float = 1500.0) -> float:
    """The design-swarm closed form (pre-tax) — kept as the cross-check the
    tests pin; conventional investor variant, rehab in cash."""
    sc = state_costs(market_code)
    g = (1 + target_pct / 100.0) ** (months / 12.0) - 1
    r = (rate_pct + 0.75) / 100.0
    alpha = BUY_CLOSING_PCT + 0.01 * (1 - down)
    kappa = (months / 12.0) * (r * (1 - down) + sc["proptax"])
    F = months * (sc["utilities_mo"] + sc["ins_landlord"] * sc["ins_reno_mult"] / 12.0)
    n_s = 1 - SELL_COST_PCT
    num = n_s * arv - (1 + g) * (rehab + F)
    den = (1 + alpha + kappa) + g * (down + alpha + kappa)
    return max(0.0, num / den)


def _flip_after_tax_profit(pretax: float, market_code: str, fed: float = FED_ORDINARY,
                           w2_wages: float = 150_000.0) -> float:
    """Dealer profit after federal ordinary + SE + state (research rules)."""
    if pretax <= 0:
        return pretax
    sc = state_costs(market_code)
    se_base = 0.9235 * pretax
    sswb_room = max(0.0, 184_500.0 - w2_wages)
    se = 0.124 * min(se_base, sswb_room) + 0.029 * se_base \
        + 0.009 * max(0.0, se_base + w2_wages - 200_000.0)
    fed_tax = pretax * fed - 0.5 * se * fed
    state_tax = pretax * sc["state_income"]
    return pretax - fed_tax - se - state_tax


def calibration(scope: str) -> dict:
    """Fixer entry discount x and renovated premium y for a scope (verified
    July-2026 calibration: Zillow hedonic fixer discounts + distressed-sale
    literature + JBREC/Kiavi 65%-of-ARV cross-check; conservative vs the
    ATTOM 25.4% observed national flip ROI)."""
    t = _cal("calibration")["table"]["defaults"]
    return {"x": t[f"x_{scope}"], "y": t[f"y_{scope}"]}


def market_headroom(code: str, median_value: float, median_rent: float,
                    appreciation: float = 0.0, *, scope: str = "moderate",
                    level: str = "mid", sqft: float = 1500.0,
                    rate_pct: float = 6.55, target: float = TARGET_DEFAULT,
                    mode: str = "brrrr", rehab_total: float | None = None) -> dict:
    """One market's answer: the MOST YOU CAN PAY for a fixer there and still
    clear the after-tax target and the $25k floor — in dollars, per sqft
    and as a share of the market's median home — and which limit sets it
    (the return target, the floor, or rent too thin to carry a refi).

    No verdict against a "typical fixer price". Fixer sales aren't in the
    data; the old comparator was the median × one national discount while
    the remodel is priced per square foot, so every market read PRICED OUT
    and the ranking tracked remodel cost ÷ home value. What is left is what
    the arithmetic actually knows: the price ceiling and why.

    median_value/median_rent: the market's aggregates (1,500-sqft prototype:
    the median-value home IS the prototype); ARV = median × the scope's
    renovated premium. appreciation: annual fraction for the exit, the
    caller's (−5%..+5%). rehab_total overrides the cost model (tests)."""
    calib = calibration(scope)
    arv = calib["y"] * median_value
    if rehab_total is None:
        from value_add import remodel_budget
        rehab_total = remodel_budget(sqft, 3, 2, 1965, scope, level, state=code)["total"]
    market = {"code": code, "arv": arv, "rent": median_rent,
              "appreciation": max(-0.05, min(0.05, appreciation or 0.0))}
    inputs = {"rehab": rehab_total, "sqft": sqft, "scope": scope,
              "rate_pct": rate_pct, "target": target}
    base = {"code": code, "arv_psf": arv / sqft, "rehab_psf": rehab_total / sqft,
            "rehab": rehab_total, "median_psf": median_value / sqft,
            "rtv_pct": median_rent * 12 / median_value * 100 if median_value else None}
    sol = brrrr_max_price(market, inputs) if mode == "brrrr" else flip_max_price(market, inputs)
    if sol is None:
        if mode == "brrrr":
            lim = _brrrr_limits(brrrr_after_tax_irr(1_000.0, market, inputs), target / 100.0, PROFIT_FLOOR)
        else:
            lim = _flip_limits(1_000.0, market, inputs)
        why = lim[0] if lim else "refi"
        return {**base, "feasible": False, "max_price": 0.0, "max_psf": 0.0, "max_pct_median": None,
                "binding": why, "binding_label": NO_PATH_LABEL[why], "detail": None}
    return {**base, "feasible": True, "max_price": sol["max_price"], "max_psf": sol["max_psf"],
            "max_pct_median": sol["max_price"] / median_value * 100,
            "binding": sol["binding"], "binding_label": LIMIT_LABEL[sol["binding"]], "detail": sol}


def _flip_eval(p: float, market: dict, inputs: dict) -> tuple:
    """(equity in, pre-tax profit, after-tax profit, hurdle growth, floor)
    for a flip bought at p."""
    arv, code = market["arv"], market["code"]
    R = inputs["rehab"]
    months = FLIP_MONTHS.get(inputs.get("scope", "moderate"), 6)
    target = inputs.get("target", TARGET_DEFAULT) / 100.0
    sc = state_costs(code)
    rate = (inputs["rate_pct"] + 0.75) / 100.0
    down = 0.25
    g = (1 + target) ** (months / 12.0) - 1
    alpha = BUY_CLOSING_PCT + 0.01 * (1 - down)
    kappa = (months / 12.0) * (rate * (1 - down) + sc["proptax"])
    F = months * (sc["utilities_mo"] + sc["ins_landlord"] * sc["ins_reno_mult"] / 12.0)
    E = (down + alpha + kappa) * p + R + F
    pretax = (1 - SELL_COST_PCT) * arv - R - F - (1 + alpha + kappa) * p
    at = _flip_after_tax_profit(pretax, code, inputs.get("fed_ordinary", FED_ORDINARY))
    return E, pretax, at, g, inputs.get("profit_floor", PROFIT_FLOOR)


def _flip_limits(p: float, market: dict, inputs: dict) -> list[str]:
    E, _, at, g, floor = _flip_eval(p, market, inputs)
    out = []
    if at < g * E:
        out.append("target")
    if at < floor:
        out.append("floor")
    return out


FLIP_MONTHS = {"cosmetic": 4, "moderate": 6, "gut": 9}


def flip_max_price(market: dict, inputs: dict) -> dict | None:
    """After-tax flip max price by bisection; $25k floor on after-tax profit.
    `binding` names the limit a dollar more would break."""
    arv = market["arv"]
    lo, hi = 1_000.0, arv * 1.2
    if _flip_limits(lo, market, inputs):
        return None
    for _ in range(60):
        mid = (lo + hi) / 2
        if not _flip_limits(mid, market, inputs):
            lo = mid
        else:
            hi = mid
    limits = _flip_limits(hi, market, inputs)
    E, pretax, at, _g, _f = _flip_eval(lo, market, inputs)
    months = FLIP_MONTHS.get(inputs.get("scope", "moderate"), 6)
    return {"max_price": lo, "max_psf": lo / inputs["sqft"], "months": months,
            "equity": E, "pretax_profit": pretax, "after_tax_profit": at,
            "annualized": (1 + at / E) ** (12 / months) - 1 if E > 0 else 0.0,
            "binding": limits[0] if limits else "search", "limits": limits}


# ── National board: metro aggregation + ranked solve ─────────────────

# The research tables this page reads, for its freshness line (crime too,
# on the house-hack board).
LAYERS = ("calibration", "financing", "proptax", "insurance", "utilities", "inctax")

_ZIPS_DB = Path(__file__).resolve().parent / "data" / "zips.db"
_PROFILE_DB = Path(__file__).resolve().parent / "data" / "zip_profile.db"
_AGG_PATH = _DATA / "market_aggregates.json"
# Bump when the aggregation changes, so a cache written by older code is
# never read as current.
AGG_VERSION = 3
# Zillow ZIPs a market needs before it gets a median rent.
MIN_MARKET_RENTS = 10


def _connect(db: Path):
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


_FP_MEMO: dict = {}


def _fingerprint(db: Path) -> str:
    """zips.db's CONTENT, for cache keys. Not its file time: every checkout
    and deploy resets that, so a cache keyed on it was never reused and
    every local run rewrote the committed copy. Memoized on (mtime, size)
    so a request does not rescan the table."""
    st = db.stat()
    memo = (str(db), st.st_mtime_ns, st.st_size)
    if memo not in _FP_MEMO:
        conn = _connect(db)
        try:
            r = conn.execute("SELECT MAX(as_of), MAX(rent_as_of), COUNT(*), SUM(median_home_value), "
                             "SUM(median_rent_monthly), SUM(LENGTH(history_zhvi)) FROM zips").fetchone()
        finally:
            conn.close()
        _FP_MEMO[memo] = f"v{AGG_VERSION}:" + "|".join("" if x is None else str(x) for x in r)
    return _FP_MEMO[memo]


def _write_atomic(path: Path, payload) -> None:
    """Write then rename, so a concurrent reader never sees half a file. A
    read-only data directory is not an error: the caller still has its answer."""
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, sort_keys=True)
        os.replace(tmp, path)
    except OSError:
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _profile_fingerprint(db: Path) -> str:
    """zip_profile.db's build and Realtor.com month, for cache keys; "none"
    without the file (the listing columns then show dashes)."""
    try:
        st = db.stat()
    except OSError:
        return "none"
    memo = ("profile", str(db), st.st_mtime_ns, st.st_size)
    if memo not in _FP_MEMO:
        conn = _connect(db)
        try:
            meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        except sqlite3.Error:
            meta = {}
        finally:
            conn.close()
        _FP_MEMO[memo] = f"{meta.get('built_at', '')}|{meta.get('rdc_last_month', '')}"
    return _FP_MEMO[memo]


_LISTINGS: dict = {}


def zip_listings(db: Path | None = None) -> dict:
    """{zip: {month, active, dom, dom_yoy_pct, cut_pct, cut_yoy_pp, thin}} —
    Realtor.com's latest month per ZIP from zip_profile.db: median days on
    market, the share of listings with a price cut, each against a year
    earlier. What buyers are actually facing, where the page used to guess
    competition from the price trend. {} without the file."""
    db = Path(db or _PROFILE_DB)
    key = (str(db), _profile_fingerprint(db))
    hit = _LISTINGS.get(key)
    if hit is not None:
        return hit
    out: dict = {}
    if db.exists():
        conn = _connect(db)
        try:
            rows = conn.execute(
                "SELECT zip, rdc_month, rdc_active, rdc_dom, rdc_dom_yoy_pct, rdc_price_cut_pct, "
                "rdc_price_cut_yoy_pp, rdc_thin FROM zip_profile WHERE rdc_month IS NOT NULL").fetchall()
        except sqlite3.Error:
            rows = []
        finally:
            conn.close()
        out = {r["zip"]: {"month": r["rdc_month"], "active": r["rdc_active"], "dom": r["rdc_dom"],
                          "dom_yoy_pct": r["rdc_dom_yoy_pct"], "cut_pct": r["rdc_price_cut_pct"],
                          "cut_yoy_pp": r["rdc_price_cut_yoy_pp"], "thin": bool(r["rdc_thin"])}
               for r in rows}
    _LISTINGS[key] = out
    return out


def market_listings(recs: list[dict]) -> dict | None:
    """One market's listings from its ZIPs': averages weighted by each ZIP's
    active listings, over ZIPs with enough listings to read (not thin), so
    a ZIP with 300 listings counts for more than one with 12. None when no
    ZIP qualifies."""
    use = [r for r in recs if r and not r["thin"] and r["active"]]
    if not use:
        return None

    def wavg(k):
        xs = [(r[k], r["active"]) for r in use if r[k] is not None]
        tw = sum(a for _, a in xs)
        return round(sum(v * a for v, a in xs) / tw, 1) if tw else None
    return {"month": max(r["month"] for r in use), "active": int(sum(r["active"] for r in use)),
            "n_zips": len(use), "dom": wavg("dom"), "dom_yoy_pct": wavg("dom_yoy_pct"),
            "cut_pct": wavg("cut_pct"), "cut_yoy_pp": wavg("cut_yoy_pp")}


_ZIP_MARKET: dict = {}


def zip_markets(db: Path | None = None) -> dict:
    """{zip: market code}: each ZIP's nearest metro within that metro's
    radius, else its rest-of-state bucket, else None. One pass over every
    ZIP against ~100 anchors — seconds — so it is kept per database content."""
    from value_add import METRO_GEO, STATE_COST_FACTORS, _haversine_mi
    db = Path(db or _ZIPS_DB)
    key = (str(db), _fingerprint(db))
    hit = _ZIP_MARKET.get(key)
    if hit is not None:
        return hit
    conn = _connect(db)
    try:
        rows = conn.execute("SELECT zip, state, lat, lng FROM zips WHERE lat IS NOT NULL").fetchall()
    finally:
        conn.close()
    anchors = [(code, lat, lng, rad) for code, (lat, lng, rad) in METRO_GEO.items()
               if code in STATE_COST_FACTORS]
    out = {}
    for r in rows:
        best, best_d = None, 1e12
        for code, lat, lng, rad in anchors:
            d = _haversine_mi(r["lat"], r["lng"], lat, lng)
            if d <= rad and d < best_d:
                best, best_d = code, d
        out[r["zip"]] = best or (r["state"] if r["state"] in STATE_COST_FACTORS else None)
    _ZIP_MARKET[key] = out
    return out


def market_aggregates(force: bool = False, db: Path | None = None,
                      path: Path | None = None, profile_db: Path | None = None) -> dict:
    """Per-market aggregates from zips.db, cached to disk under the
    database's content fingerprint. Every ZIP goes to its market
    (zip_markets) and rolls up: median home value, median rent, population,
    value P25/P75, the median 3-year price change from each ZIP's 60-month
    history (shown as history, not used as a forecast), and the market's
    Realtor.com listings (market_listings).

    RENT IS ZILLOW'S ONLY — the rent ladder's ZORI answer, an asking rent.
    The ladder also answers with HUD voucher figures (gross, utilities in,
    40th percentile) and Census rents; a median across ZIPs that mixes
    those has no basis anyone can name, and the page says "ZORI". A market
    with fewer than MIN_MARKET_RENTS Zillow ZIPs has no rent."""
    db = Path(db or _ZIPS_DB)
    path = Path(path or _AGG_PATH)
    profile_db = Path(profile_db or _PROFILE_DB)
    key = _fingerprint(db) + "#" + _profile_fingerprint(profile_db)
    if path.exists() and not force:
        try:
            cached = json.loads(path.read_text())
            if cached.get("_key") == key:
                return cached["markets"]
        except (OSError, ValueError):
            pass

    from structural import trajectory_from_history
    conn = _connect(db)
    try:
        rows = conn.execute(
            "SELECT zip, population, median_home_value, median_rent_monthly, rent_tier, history_zhvi "
            "FROM zips WHERE lat IS NOT NULL AND median_home_value IS NOT NULL AND population >= 1500"
        ).fetchall()
    finally:
        conn.close()
    market_of = zip_markets(db)
    buckets: dict[str, dict] = {}
    for r in rows:
        code = market_of.get(r["zip"])
        if code is None:
            continue
        b = buckets.setdefault(code, {"values": [], "rents": [], "pop": 0, "cagr3": []})
        b["values"].append(r["median_home_value"])
        b["pop"] += r["population"] or 0
        if r["rent_tier"] == "zori" and r["median_rent_monthly"]:
            b["rents"].append(r["median_rent_monthly"])
        if r["history_zhvi"]:
            try:
                t = trajectory_from_history(json.loads(r["history_zhvi"]))
            except (ValueError, TypeError):
                t = None
            if t:
                b["cagr3"].append(t["cagr_3yr_pct"])

    listings = zip_listings(profile_db)
    by_market: dict[str, list] = {}
    for z, rec in listings.items():
        code = market_of.get(z)
        if code is not None:
            by_market.setdefault(code, []).append(rec)

    markets = {}
    for code, b in buckets.items():
        vals = sorted(b["values"])
        n = len(vals)
        markets[code] = {
            "value": statistics.median(vals), "p25": vals[n // 4], "p75": vals[(3 * n) // 4],
            "rent": (statistics.median(b["rents"]) if len(b["rents"]) >= MIN_MARKET_RENTS else None),
            "n_zips": n, "n_zori": len(b["rents"]), "population": b["pop"],
            "cagr3_pct": (round(statistics.median(b["cagr3"]), 2) if b["cagr3"] else None),
            "listings": market_listings(by_market.get(code, [])),
        }
    _write_atomic(path, {"_key": key, "markets": markets})
    return markets


# Solved boards, newest last. Bounded: the key is built from whatever the
# query string says, and an unbounded dict grew by one ~100-row board per
# distinct URL anyone sent.
BOARD_CACHE_MAX = 64
_BOARD_CACHE: OrderedDict = OrderedDict()
_CACHE_LOCK = threading.Lock()
# One national solve at a time: each is seconds of pure-Python CPU, and a
# pile of them in worker threads starves the event loop of the GIL.
_SOLVE_LOCK = threading.Lock()


def _cache_get(key):
    with _CACHE_LOCK:
        hit = _BOARD_CACHE.get(key)
        if hit is not None:
            _BOARD_CACHE.move_to_end(key)
        return hit


def _cache_put(key, value) -> None:
    with _CACHE_LOCK:
        _BOARD_CACHE[key] = value
        _BOARD_CACHE.move_to_end(key)
        while len(_BOARD_CACHE) > BOARD_CACHE_MAX:
            _BOARD_CACHE.popitem(last=False)


def build_board(*, mode: str = "brrrr", scope: str = "moderate", level: str = "low",
                target: float = TARGET_DEFAULT, rate_pct: float = 6.55,
                sqft: float = 1500.0, appreciation: float = 0.0,
                metros_only: bool = False, min_pop: int = 100_000) -> list[dict]:
    """Solve every market and rank by the most you can pay as a share of its
    median home. appreciation is the exit's annual price change, the
    user's (0 by default). Results cached in-process per input set."""
    key = (mode, scope, level, round(target, 1), round(rate_pct, 2),
           int(sqft), round(appreciation, 4), metros_only, min_pop)
    hit = _cache_get(key)
    if hit is not None:
        return hit
    with _SOLVE_LOCK:
        hit = _cache_get(key)              # solved while this request waited
        if hit is not None:
            return hit
        out = _solve_board(mode=mode, scope=scope, level=level, target=target, rate_pct=rate_pct,
                           sqft=sqft, appreciation=appreciation, metros_only=metros_only, min_pop=min_pop)
        _cache_put(key, out)
        return out


def _solve_board(*, mode, scope, level, target, rate_pct, sqft, appreciation, metros_only, min_pop):
    from value_add import STATE_NAMES
    aggs = market_aggregates()
    out = []
    for code, a in aggs.items():
        if a["population"] < min_pop or not a["rent"]:
            continue
        if metros_only and "-" not in code and code not in ("DC",):
            continue
        r = market_headroom(code, a["value"], a["rent"], appreciation, scope=scope,
                            level=level, sqft=sqft, rate_pct=rate_pct, target=target, mode=mode)
        out.append({**r, "name": STATE_NAMES.get(code, code),
                    "median_value": a["value"], "rent": a["rent"],
                    "population": a["population"], "n_zips": a["n_zips"],
                    "n_zori": a["n_zori"], "cagr3_pct": a["cagr3_pct"],
                    "listings": a.get("listings")})
    out.sort(key=lambda r: (not r["feasible"], -(r["max_pct_median"] or 0)))
    return out


def board_summary(board: list[dict]) -> dict | None:
    """What the page's headline sentence says: how many markets have any
    price that works, the range of the most you can pay (as a share of the
    median), how many each limit sets, and — for BRRRR — how many refis the
    rent sizes rather than 75% of ARV."""
    if not board:
        return None
    feas = [r for r in board if r["feasible"]]
    limits: dict = {}
    for r in feas:
        limits[r["binding"]] = limits.get(r["binding"], 0) + 1
    refi_by_rent = sum(1 for r in feas if (r.get("detail") or {}).get("refi_by") == "rent")
    return {"n": len(board), "n_feasible": len(feas), "n_no_path": len(board) - len(feas),
            "best": feas[0] if feas else None, "worst": feas[-1] if feas else None,
            "limits": limits, "refi_by_rent": refi_by_rent}


def zip_drilldown(metro_code: str, *, mode: str = "brrrr", scope: str = "moderate",
                  level: str = "low", target: float = TARGET_DEFAULT,
                  rate_pct: float = 6.55, sqft: float = 1500.0, appreciation: float = 0.0,
                  top: int = 15, db: Path | None = None, profile_db: Path | None = None) -> list[dict]:
    """The market's top ZIPs by rent-to-value (the BRRRR fuel), each solved
    with its own median value and rent at the metro's construction costs,
    with its own Realtor.com listings. Zillow rents only, as on the board:
    a county-wide HUD figure divided by each ZIP's value would rank the
    county's cheapest ZIPs first by construction."""
    from value_add import METRO_GEO, _haversine_mi
    if metro_code not in METRO_GEO:
        return []
    lat0, lng0, rad = METRO_GEO[metro_code]
    conn = _connect(Path(db or _ZIPS_DB))
    try:
        rows = conn.execute(
            "SELECT zip, name, state, lat, lng, population, median_home_value, "
            "median_rent_monthly FROM zips WHERE lat IS NOT NULL "
            "AND median_home_value IS NOT NULL AND median_rent_monthly IS NOT NULL "
            "AND rent_tier = 'zori' AND population >= 5000").fetchall()
    finally:
        conn.close()
    members = [r for r in rows if _haversine_mi(r["lat"], r["lng"], lat0, lng0) <= rad]
    members.sort(key=lambda r: r["median_rent_monthly"] / r["median_home_value"],
                 reverse=True)
    listings = zip_listings(profile_db)
    out = []
    for r in members[:top]:
        h = market_headroom(metro_code, r["median_home_value"], r["median_rent_monthly"],
                            appreciation, scope=scope, level=level, sqft=sqft,
                            rate_pct=rate_pct, target=target, mode=mode)
        out.append({**h, "zip": r["zip"], "place": r["name"],
                    "state": r["state"], "population": r["population"],
                    "median_value": r["median_home_value"],
                    "rent": r["median_rent_monthly"],
                    "listings": listings.get(r["zip"])})
    return out


# ── House-hack mode: owner-occupant FHA 203(k), ZIP-level ────────────

FHA_DOWN = 0.035
FHA_UFMIP = 0.0175             # upfront MIP, financed into the loan
FHA_MIP_ANNUAL = 0.0055        # annual MIP on the base loan (LTV > 95%)
HH_UNIT_SQFT = 900.0
# What set the offer, in the order the page names them.
HH_BINDING = {"value": "building value", "live_free": "cash flow",
              "fha_self_sufficiency": "FHA 75% test", "budget": "your budget"}


@lru_cache(maxsize=4096)
def _hh_rehab(code: str, units: int, scope: str, level: str) -> float:
    from value_add import remodel_budget
    return remodel_budget(units * HH_UNIT_SQFT, 3, max(2.0, units * 1.0), 1965, scope, level,
                          state=code)["total"]


def house_hack_max_offer(median_value: float, rent_unit: float, code: str,
                         *, units: int = 4, scope: str = "cosmetic",
                         level: str = "low", rate_pct: float = 6.55,
                         max_price: float = 300_000.0) -> dict | None:
    """The offer number for an owner-occupant house-hacker: the LOWEST of

      value      what the building is likely worth — the ZIP's single-family
                 median × the 2-4 unit factor /multifamily prices it at
                 (househack.est_building_price; an estimate, not a listing).
                 Rents alone once set offers at 4.8x the local median: a
                 cash-flow ceiling no appraisal would support.
      live_free  the price where the other units' rent, less 8% vacancy and
                 repairs at 1.5% a year of (price + remodel), covers the
                 entire PITI — the same vacancy and maintenance the BRRRR
                 side underwrites with.
      fha_self_sufficiency  (3-4 units) FHA's funding rule: 75% of ALL
                 units' rent ≥ PITI. Owner-occupants face this, not a DSCR.
      budget     the user's cap.

    PITI after a 203(k) remodel: 3.5% down on (price + remodel), the 1.75%
    upfront MIP financed, 0.55% annual MIP, property tax at the investor
    table (homestead on the owner's unit would only trim it), insurance at
    the state landlord premium × /multifamily's unit factor. Every term is
    linear in the price, so each cap is a closed form.

    rent_unit is the ZIP rent per unit — same convention as /multifamily;
    the caller names its source."""
    if not rent_unit or rent_unit <= 0 or units < 2 or not median_value or median_value <= 0:
        return None
    sc = state_costs(code)
    R = _hh_rehab(code, units, scope, level)
    est = HH.est_building_price(median_value, units)
    r = rate_pct / 100.0
    k12 = (r / 12) * (1 + r / 12) ** 360 / ((1 + r / 12) ** 360 - 1)
    # Monthly cost per dollar of BASE loan: P&I on the base plus financed
    # UFMIP, and the annual MIP on the base.
    per_base = k12 * (1 + FHA_UFMIP) + FHA_MIP_ANNUAL / 12
    ins_mo = sc["ins_landlord"] * HH.UNIT_INSURANCE_FACTOR.get(units, 1.45) / 12
    # PITI(P) = a·P + b
    a = per_base * (1 - FHA_DOWN) + sc["proptax"] / 12
    b = per_base * (1 - FHA_DOWN) * R + ins_mo
    m = MAINTENANCE_PCT / 12                       # repairs per dollar of (P + R), monthly
    gross_other = (units - 1) * rent_unit
    kept = gross_other * (1 - VACANCY)
    caps = {"value": float(est["estimated_price"]),
            "live_free": (kept - m * R - b) / (a + m),
            "budget": float(max_price)}
    if units >= HH.FHA_SELF_SUFF_UNITS:
        caps["fha_self_sufficiency"] = (HH.FHA_RENT_CREDIT * units * rent_unit - b) / a
    if min(caps["live_free"], caps.get("fha_self_sufficiency", caps["live_free"])) <= 0:
        return None                                # the rents carry no price at all
    binding = min(caps, key=caps.get)
    offer = caps[binding]
    piti = a * offer + b
    repairs = m * (offer + R)
    cash = FHA_DOWN * (offer + R) + 0.03 * offer   # down + ~3% closing
    return {"max_offer": round(offer), "binding": binding, "binding_label": HH_BINDING[binding],
            "rehab": round(R), "est_value": est["estimated_price"], "est_basis": est["basis"],
            "piti": round(piti), "rent_offset": round(gross_other),
            "vacancy": round(gross_other * VACANCY), "repairs": round(repairs),
            "cash_to_close": round(cash), "capped_at_budget": binding == "budget",
            "monthly_surplus": round(kept - repairs - piti),
            "fha_75_passes": (HH.FHA_RENT_CREDIT * units * rent_unit >= piti - 0.5
                              if units >= HH.FHA_SELF_SUFF_UNITS else None)}


def zip_board_hh(*, state: str | None = None, units: int = 4, scope: str = "cosmetic",
                 level: str = "low", rate_pct: float = 6.55,
                 max_price: float = 300_000.0, min_pop: int = 5_000,
                 top: int = 40, max_tier: str = "safe",
                 allow_unknown: bool = False, db: Path | None = None,
                 profile_db: Path | None = None) -> list[dict]:
    """ZIP-level house-hack board: every ZIP with a measured rent (optionally
    one state), solved for the max offer; safest first, then rent-to-value.

    Rents: whatever the rent ladder measured for the ZIP — Zillow, HUD
    (small-area or county voucher figures, utilities included) or Census —
    each row labelled with its source, and a HUD rent that would take a
    large share of local income flagged, as /multifamily does. ZIPs the
    ladder could not answer for are left out.

    Safety gate (safety.py, real FBI city-level figures): rows above
    `max_tier` are dropped, and cities we have no verified figure for are
    dropped too unless allow_unknown — the yield leaders are exactly the
    places most likely to be screened out, which is the point."""
    from safety import zip_safety, passes, TIER_ORDER
    db = Path(db or _ZIPS_DB)
    tiers = RL.TIER_ORDER
    q = ("SELECT zip, name, state, population, median_home_value, median_rent_monthly, rent_tier, "
         "median_household_income FROM zips WHERE median_home_value IS NOT NULL "
         "AND median_rent_monthly IS NOT NULL AND population >= ? "
         f"AND rent_tier IN ({', '.join('?' * len(tiers))})")
    args: list = [min_pop, *tiers]
    if state:
        q += " AND state = ?"
        args.append(state.upper())
    conn = _connect(db)
    try:
        rows = conn.execute(q, args).fetchall()
    finally:
        conn.close()
    market_of = zip_markets(db)
    out = []
    for rr in rows:
        code = market_of.get(rr["zip"])
        if code is None:
            continue
        sf = zip_safety(rr["name"], rr["state"])
        if not passes(sf, max_tier, allow_unknown):
            continue
        mv, rent = rr["median_home_value"], rr["median_rent_monthly"]
        h = house_hack_max_offer(mv, rent, code, units=units, scope=scope,
                                 level=level, rate_pct=rate_pct, max_price=max_price)
        if h is None:
            continue
        tier = RL.TIER_BY_KEY[rr["rent_tier"]]
        out.append({**h, "zip": rr["zip"], "place": rr["name"], "state": rr["state"],
                    "market": code, "population": rr["population"],
                    "median_value": mv, "rent": rent,
                    "rent_tier": rr["rent_tier"], "rent_label": tier["label"],
                    "rent_basis": tier["basis"], "rent_caveat": tier["caveat"],
                    "rent_strains_income": RL.hud_rent_strains_income(
                        rent, rr["rent_tier"], rr["median_household_income"]),
                    "rtv_pct": round(rent * 12 / mv * 100, 1),
                    "safety": sf})
    # Safest first, then yield — a house-hack is where the family lives, so
    # safety outranks rent-to-value in the ordering.
    out.sort(key=lambda x: (TIER_ORDER[x["safety"]["tier"]], -x["rtv_pct"], -x["max_offer"]))
    out = out[:top]
    listings = zip_listings(profile_db)
    for z in out:
        z["listings"] = listings.get(z["zip"])
    return out
