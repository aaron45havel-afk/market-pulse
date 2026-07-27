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
from functools import lru_cache
from pathlib import Path

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
      is an annual fraction, already trajectory-vetoed/clamped upstream.
    inputs: {rehab, sqft, scope, rate_pct, target, fed_ordinary}
    Returns metrics dict, or None when the deal is structurally infeasible
    (DSCR floor fails — no refi market for the rent level).
    """
    R = inputs["rehab"]
    arv, rent = market["arv"], market["rent"]
    app = market.get("appreciation", 0.0)
    sc = state_costs(market["code"])
    fin = financing_terms(inputs["rate_pct"])
    fed = inputs.get("fed_ordinary", FED_ORDINARY)
    s_inc = sc["state_income"]
    st = sc["state"]

    m_reno = RENO_MONTHS.get(inputs.get("scope", "moderate"), 4)
    refi_m = max(REFI_MONTH_MIN, m_reno + 1)
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

    # Renovation + seasoning months: draws, interest on drawn balance, carry
    drawn = purchase_adv
    carry_paid = 0.0
    reno_ins_mo = sc["ins_landlord"] * sc["ins_reno_mult"] / 12.0
    tax_mo = sc["proptax"] * price / 12.0
    for m in range(1, refi_m + 1):
        cash_rehab = 0.0
        if m <= m_reno:
            drawn += rehab_funded / m_reno
            cash_rehab = (R - rehab_funded) / m_reno
        interest = drawn * fin["hm_rate"] / 12.0
        carry = interest + tax_mo + reno_ins_mo + sc["utilities_mo"]
        carry_paid += carry
        cf[m] -= carry + cash_rehab

    # ── Refi: DSCR cash-out at 75% ARV, gated by DSCR at market rent ──
    tax_yr = sc["proptax"] * price
    ins_yr = sc["ins_landlord"]
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
    maint_yr = MAINTENANCE_PCT * arv
    noi_yr = rent * 12 * (1 - VACANCY) - tax_yr - ins_yr - maint_yr
    points_amort_yr = (fin["refi_points"] * refi_loan) / 30.0

    suspended = 0.0
    accum_dep = 0.0
    mi = 0                                   # months consumed from the schedule
    for y in range(1, HOLD_YEARS + 1):
        months = min(12 * y, hold_m) - max(refi_m, 12 * (y - 1))
        months = max(0, months)              # year 1 is post-refi months only
        seg = sched[mi:mi + months]
        mi += months
        interest_y = sum(i for i, _ in seg)
        principal_y = sum(p for _, p in seg)
        share = months / 12.0 if y == 1 else 1.0
        cfy = noi_yr * share - (interest_y + principal_y)
        dep_y = annual_dep * share
        accum_dep += dep_y
        taxable = noi_yr * share - interest_y - dep_y - points_amort_yr
        if taxable >= 0:
            tax_bill = taxable * (fed + s_inc)
        else:
            tax_bill = 0.0
            suspended += -taxable
        cf[min(12 * y, hold_m)] += cfy - tax_bill

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
    release = suspended * (fed + (0.0 if st in _NO_STATE_LOSS_CARRYFORWARD else s_inc))
    exit_cf = amount_realized - exit_balance - fed_tax - state_tax + release
    cf[hold_m] += exit_cf

    irr_m = _irr_monthly(cf)
    if irr_m is None:
        return None
    forced_equity = arv - (price + closing + R + carry_paid + hm_costs)
    return {
        "irr_annual": (1 + irr_m) ** 12 - 1,
        "cash_in_peak": -cf[0] + carry_paid + (R - rehab_funded),
        "cash_out": cash_out,
        "forced_equity": forced_equity,
        "dscr": rent / (_pmt(refi_loan, fin["refi_rate"]) + (tax_yr + ins_yr) / 12.0),
        "refi_loan": refi_loan,
        "suspended_released": release,
        "exit_value": sale,
    }


def _irr_monthly(cf: list[float]) -> float | None:
    """Bisection IRR on a monthly cash-flow list. Bracket [-0.9, 1.0]/mo."""
    def npv(r):
        return sum(c / (1 + r) ** i for i, c in enumerate(cf))
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


def brrrr_max_price(market: dict, inputs: dict) -> dict | None:
    """Highest purchase price where after-tax IRR ≥ target AND forced
    equity ≥ the $25k floor. Bisection (IRR is monotone-decreasing in P)."""
    target = inputs.get("target", TARGET_DEFAULT) / 100.0
    floor = inputs.get("profit_floor", PROFIT_FLOOR)

    def ok(p):
        m = brrrr_after_tax_irr(p, market, inputs)
        return m is not None and m["irr_annual"] >= target and m["forced_equity"] >= floor

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
    metrics = brrrr_after_tax_irr(lo, market, inputs)
    return {"max_price": lo, "max_psf": lo / inputs["sqft"], **(metrics or {})}


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


def flip_max_price(market: dict, inputs: dict) -> dict | None:
    """After-tax flip max price by bisection; $25k floor on after-tax profit."""
    arv, code = market["arv"], market["code"]
    R = inputs["rehab"]
    months = {"cosmetic": 4, "moderate": 6, "gut": 9}.get(inputs.get("scope", "moderate"), 6)
    target = inputs.get("target", TARGET_DEFAULT) / 100.0
    floor = inputs.get("profit_floor", PROFIT_FLOOR)
    sc = state_costs(code)
    rate = (inputs["rate_pct"] + 0.75) / 100.0
    down = 0.25
    g = (1 + target) ** (months / 12.0) - 1

    def metrics(p):
        alpha = BUY_CLOSING_PCT + 0.01 * (1 - down)
        kappa = (months / 12.0) * (rate * (1 - down) + sc["proptax"])
        F = months * (sc["utilities_mo"] + sc["ins_landlord"] * sc["ins_reno_mult"] / 12.0)
        E = (down + alpha + kappa) * p + R + F
        pretax = (1 - SELL_COST_PCT) * arv - R - F - (1 + alpha + kappa) * p
        at = _flip_after_tax_profit(pretax, code, inputs.get("fed_ordinary", FED_ORDINARY))
        return E, pretax, at

    def ok(p):
        E, _, at = metrics(p)
        return at >= g * E and at >= floor

    lo, hi = 1_000.0, arv * 1.2
    if not ok(lo):
        return None
    for _ in range(60):
        mid = (lo + hi) / 2
        if ok(mid):
            lo = mid
        else:
            hi = mid
    E, pretax, at = metrics(lo)
    return {"max_price": lo, "max_psf": lo / inputs["sqft"], "months": months,
            "equity": E, "pretax_profit": pretax, "after_tax_profit": at,
            "annualized": (1 + at / E) ** (12 / months) - 1 if E > 0 else 0.0}
