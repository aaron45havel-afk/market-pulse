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
        "suspended_released": release,
        "exit_value": sale,
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


def brrrr_max_price(market: dict, inputs: dict) -> dict | None:
    """Highest purchase price where after-tax IRR ≥ target AND the deal's
    total after-tax profit over the hold ≥ the $25k floor. Bisection (IRR
    is monotone-decreasing in P)."""
    target = inputs.get("target", TARGET_DEFAULT) / 100.0
    floor = inputs.get("profit_floor", PROFIT_FLOOR)

    def ok(p):
        m = brrrr_after_tax_irr(p, market, inputs)
        return m is not None and m["irr_annual"] >= target and m["total_profit"] >= floor

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


def calibration(scope: str) -> dict:
    """Fixer entry discount x and renovated premium y for a scope (verified
    July-2026 calibration: Zillow hedonic fixer discounts + distressed-sale
    literature + JBREC/Kiavi 65%-of-ARV cross-check; conservative vs the
    ATTOM 25.4% observed national flip ROI)."""
    t = _cal("calibration")["table"]["defaults"]
    return {"x": t[f"x_{scope}"], "y": t[f"y_{scope}"]}


def market_headroom(code: str, median_value: float, median_rent: float,
                    appreciation: float, *, scope: str = "moderate",
                    level: str = "mid", sqft: float = 1500.0,
                    rate_pct: float = 6.55, target: float = TARGET_DEFAULT,
                    mode: str = "brrrr", trajectory: str | None = None,
                    rehab_total: float | None = None) -> dict | None:
    """One market's answer: max buy $/sqft at the after-tax target vs what
    fixers actually cost there → headroom % → verdict tier.

    median_value/median_rent: the market's aggregates (1,500-sqft prototype:
    the median-value home IS the prototype). appreciation: annual fraction,
    already clamped/vetoed upstream is fine — a 'declining' trajectory also
    demotes the verdict here. rehab_total overrides the cost model (tests)."""
    calib = calibration(scope)
    arv = calib["y"] * median_value
    entry_psf = calib["x"] * median_value / sqft
    if rehab_total is None:
        from value_add import remodel_budget
        rehab_total = remodel_budget(sqft, 3, 2, 1965, scope, level, state=code)["total"]
    market = {"code": code, "arv": arv, "rent": median_rent,
              "appreciation": max(0.0, min(0.04, appreciation or 0.0))}
    inputs = {"rehab": rehab_total, "sqft": sqft, "scope": scope,
              "rate_pct": rate_pct, "target": target}
    sol = brrrr_max_price(market, inputs) if mode == "brrrr" else flip_max_price(market, inputs)
    if sol is None:
        return {"code": code, "feasible": False, "verdict": "NO PATH",
                "entry_psf": entry_psf, "arv_psf": arv / sqft,
                "rehab_psf": rehab_total / sqft, "max_psf": 0.0, "headroom": None}
    headroom = (sol["max_psf"] - entry_psf) / entry_psf
    if headroom >= 0.05:
        verdict = "PRIMED"
    elif headroom >= -0.20:
        verdict = "DEAL-DEPENDENT"
    else:
        verdict = "PRICED OUT"
    vetoed = False
    if trajectory == "declining" and verdict == "PRIMED":
        verdict, vetoed = "DEAL-DEPENDENT", True
    return {"code": code, "feasible": True, "verdict": verdict, "vetoed": vetoed,
            "headroom": headroom, "max_psf": sol["max_psf"],
            "entry_psf": entry_psf, "arv_psf": arv / sqft,
            "rehab_psf": rehab_total / sqft, "detail": sol}


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


# ── National board: metro aggregation + ranked solve ─────────────────

COMPETITION_LABEL = {
    # Display-only competition read from the price-trajectory signal; the
    # calibration's months-of-supply x-adjustment is a separate, manual
    # knob until a real inventory feed lands (see calibration.json
    # tightness_rule). Shown so a hot market is never mistaken for a
    # negotiable one.
    "accelerating": ("HOT — buyers competing", "+"),
    "steady": ("BALANCED", "="),
    "decelerating": ("COOLING — ask for discounts", "−"),
    "declining": ("BUYER'S MARKET — but values falling", "!"),
}

_AGG_PATH = _DATA / "market_aggregates.json"


def market_aggregates(force: bool = False) -> dict:
    """Per-market aggregates from zips.db, cached to disk (rebuilt when the
    db as_of changes or force=True). One pass assigns every ZIP to its
    nearest metro within radius (else its rest-of-state bucket) and rolls
    up: median home value, ZORI-only median rent (imputed rows — rent ==
    value/17/12 — are excluded), population, value P25/P75, median 3-yr
    CAGR and the modal trajectory label from each ZIP's 60-mo history."""
    import sqlite3 as _sq
    import statistics as _st
    from value_add import METRO_GEO, STATE_COST_FACTORS, _haversine_mi
    from structural import trajectory_from_history

    db = Path(__file__).resolve().parent / "data" / "zips.db"
    as_of = str(int(db.stat().st_mtime))
    if _AGG_PATH.exists() and not force:
        cached = json.loads(_AGG_PATH.read_text())
        if cached.get("_as_of") == as_of:
            return cached["markets"]

    conn = _sq.connect(str(db))
    conn.row_factory = _sq.Row
    rows = conn.execute(
        "SELECT zip, state, lat, lng, population, median_home_value, "
        "median_rent_monthly, history_zhvi FROM zips WHERE lat IS NOT NULL "
        "AND median_home_value IS NOT NULL AND population >= 1500").fetchall()
    conn.close()

    anchors = [(code, lat, lng, rad) for code, (lat, lng, rad) in METRO_GEO.items()
               if code in STATE_COST_FACTORS]
    buckets: dict[str, dict] = {}
    for r in rows:
        best, best_d = None, 1e12
        for code, lat, lng, rad in anchors:
            d = _haversine_mi(r["lat"], r["lng"], lat, lng)
            if d <= rad and d < best_d:
                best, best_d = code, d
        code = best or (r["state"] if r["state"] in STATE_COST_FACTORS else None)
        if code is None:
            continue
        b = buckets.setdefault(code, {"values": [], "rents": [], "pop": 0,
                                      "cagr3": [], "traj": {}})
        b["values"].append(r["median_home_value"])
        b["pop"] += r["population"] or 0
        rent = r["median_rent_monthly"]
        if rent and abs(rent * 17 * 12 / r["median_home_value"] - 1) > 0.02:
            b["rents"].append(rent)
        if r["history_zhvi"]:
            try:
                t = trajectory_from_history(json.loads(r["history_zhvi"]))
            except (ValueError, TypeError):
                t = None
            if t:
                b["cagr3"].append(t["cagr_3yr_pct"])
                b["traj"][t["label"]] = b["traj"].get(t["label"], 0) + 1

    markets = {}
    for code, b in buckets.items():
        vals = sorted(b["values"])
        n = len(vals)
        traj = max(b["traj"], key=b["traj"].get) if b["traj"] else None
        markets[code] = {
            "value": _st.median(vals), "p25": vals[n // 4], "p75": vals[(3 * n) // 4],
            "rent": (_st.median(b["rents"]) if len(b["rents"]) >= 10 else None),
            "n_zips": n, "n_zori": len(b["rents"]), "population": b["pop"],
            "cagr3_pct": (round(_st.median(b["cagr3"]), 2) if b["cagr3"] else None),
            "trajectory": traj,
        }
    _AGG_PATH.write_text(json.dumps({"_as_of": as_of, "markets": markets}))
    return markets


_BOARD_CACHE: dict = {}


def build_board(*, mode: str = "brrrr", scope: str = "moderate", level: str = "low",
                target: float = TARGET_DEFAULT, rate_pct: float = 6.55,
                sqft: float = 1500.0, x_adjust: float = 0.0,
                metros_only: bool = False, min_pop: int = 100_000) -> list[dict]:
    """Solve every market and rank by headroom. x_adjust is the manual
    competition knob (added to the fixer entry discount, bounds per the
    calibration tightness rule). Results cached in-process per input set."""
    from value_add import STATE_NAMES
    key = (mode, scope, level, round(target, 1), round(rate_pct, 2),
           int(sqft), round(x_adjust, 3), metros_only, min_pop)
    if key in _BOARD_CACHE:
        return _BOARD_CACHE[key]
    aggs = market_aggregates()
    x_adj = max(-0.02, min(0.05, x_adjust))
    out = []
    for code, a in aggs.items():
        if a["population"] < min_pop or not a["rent"]:
            continue
        if metros_only and "-" not in code and code not in ("DC",):
            continue
        app = (a["cagr3_pct"] or 0.0) / 100.0
        if a["trajectory"] == "declining":
            app = 0.0
        r = market_headroom(code, a["value"], a["rent"], app, scope=scope,
                            level=level, sqft=sqft, rate_pct=rate_pct,
                            target=target, mode=mode, trajectory=a["trajectory"])
        if r is None:
            continue
        if r["feasible"] and x_adj:
            entry = r["entry_psf"] * (1 + x_adj / calibration(scope)["x"])
            r["entry_psf"] = entry
            r["headroom"] = (r["max_psf"] - entry) / entry
            r["verdict"] = ("PRIMED" if r["headroom"] >= 0.05 else
                            "DEAL-DEPENDENT" if r["headroom"] >= -0.20 else "PRICED OUT")
            if a["trajectory"] == "declining" and r["verdict"] == "PRIMED":
                r["verdict"], r["vetoed"] = "DEAL-DEPENDENT", True
        comp = COMPETITION_LABEL.get(a["trajectory"] or "steady", ("BALANCED", "="))
        out.append({**r, "name": STATE_NAMES.get(code, code),
                    "median_value": a["value"], "rent": a["rent"],
                    "population": a["population"], "n_zips": a["n_zips"],
                    "n_zori": a["n_zori"], "cagr3_pct": a["cagr3_pct"],
                    "trajectory": a["trajectory"], "competition": comp[0],
                    "median_psf": a["value"] / sqft})
    out.sort(key=lambda r: (r["headroom"] is None, -(r["headroom"] or -9)))
    _BOARD_CACHE[key] = out
    return out


def zip_drilldown(metro_code: str, *, mode: str = "brrrr", scope: str = "moderate",
                  level: str = "low", target: float = TARGET_DEFAULT,
                  rate_pct: float = 6.55, sqft: float = 1500.0,
                  top: int = 15) -> list[dict]:
    """Best ZIPs inside one market: rank by rent-to-value (the BRRRR fuel),
    solve headroom per ZIP using the ZIP's own median value/rent priced at
    the metro's construction-cost code."""
    import sqlite3 as _sq
    from value_add import METRO_GEO, _haversine_mi
    if metro_code not in METRO_GEO:
        return []
    lat0, lng0, rad = METRO_GEO[metro_code]
    db = Path(__file__).resolve().parent / "data" / "zips.db"
    conn = _sq.connect(str(db))
    conn.row_factory = _sq.Row
    rows = conn.execute(
        "SELECT zip, name, state, lat, lng, population, median_home_value, "
        "median_rent_monthly, history_zhvi FROM zips WHERE lat IS NOT NULL "
        "AND median_home_value IS NOT NULL AND median_rent_monthly IS NOT NULL "
        "AND population >= 5000").fetchall()
    conn.close()
    from structural import trajectory_from_history
    members = []
    for r in rows:
        if _haversine_mi(r["lat"], r["lng"], lat0, lng0) > rad:
            continue
        if abs(r["median_rent_monthly"] * 17 * 12 / r["median_home_value"] - 1) <= 0.02:
            continue                     # imputed rent — no real signal
        members.append(r)
    members.sort(key=lambda r: r["median_rent_monthly"] / r["median_home_value"],
                 reverse=True)
    out = []
    for r in members[:top]:
        t = None
        if r["history_zhvi"]:
            try:
                t = trajectory_from_history(json.loads(r["history_zhvi"]))
            except (ValueError, TypeError):
                t = None
        app = max(0.0, min(0.04, (t["cagr_3yr_pct"] / 100.0 if t else 0.0)))
        if t and t["label"] == "declining":
            app = 0.0
        h = market_headroom(metro_code, r["median_home_value"], r["median_rent_monthly"],
                            app, scope=scope, level=level, sqft=sqft,
                            rate_pct=rate_pct, target=target, mode=mode,
                            trajectory=(t["label"] if t else None))
        out.append({**(h or {}), "zip": r["zip"], "place": r["name"],
                    "state": r["state"], "population": r["population"],
                    "median_value": r["median_home_value"],
                    "rent": r["median_rent_monthly"],
                    "rtv_pct": round(r["median_rent_monthly"] * 12 / r["median_home_value"] * 100, 2),
                    "trajectory": (t["label"] if t else None)})
    return out


# ── House-hack mode: owner-occupant FHA 203(k), ZIP-level ────────────

def house_hack_max_offer(median_value: float, rent_unit: float, code: str,
                         *, units: int = 4, scope: str = "cosmetic",
                         level: str = "low", rate_pct: float = 6.55,
                         max_price: float = 300_000.0) -> dict | None:
    """The offer number for an owner-occupant house-hacker: the highest
    purchase price at which, after a 203(k) remodel (rehab rolled into the
    loan at 3.5% down, owner rate, MIP), the OTHER units' rent covers the
    entire PITI (live-free) AND the FHA self-sufficiency gate passes
    (3-4 units: 75% x ALL units' rent >= PITI — the funding rule that
    replaces an investor loan's DSCR; owner-occupants don't have DSCR).

    Both gates are linear in P -> closed form. rent_unit uses the ZIP
    median rent per unit — same convention as /multifamily. Insurance
    scales +25% per extra unit (estimate). Property tax at the investor
    table (conservative: homestead would trim the owner's unit share)."""
    if not rent_unit or rent_unit <= 0 or units < 2:
        return None
    sc = state_costs(code)
    sqft = units * 900.0
    from value_add import remodel_budget
    R = remodel_budget(sqft, 3, max(2.0, units * 1.0), 1965, scope, level, state=code)["total"]
    r = rate_pct / 100.0
    # PITI(P) = pmt(0.965(P+R)) + MIP + tax + ins  — all linear in P.
    k12 = (r / 12) * (1 + r / 12) ** 360 / ((1 + r / 12) ** 360 - 1)
    pay_per_loan = k12 + 0.0055 / 12                     # P&I + annual MIP /12, per $ of loan
    ins_mo = sc["ins_landlord"] * (1 + 0.25 * (units - 1)) / 12
    # PITI(P) = pay_per_loan*0.965*(P+R) + tax_mo*P + ins_mo
    a = pay_per_loan * 0.965 + sc["proptax"] / 12
    b = pay_per_loan * 0.965 * R + ins_mo
    caps = {"live_free": ((units - 1) * rent_unit - b) / a}
    if units >= 3:
        caps["fha_self_sufficiency"] = (0.75 * units * rent_unit - b) / a
    binding = min(caps, key=caps.get)
    offer = min(min(caps.values()), max_price)
    if offer <= 0:
        return None
    piti = a * offer + b
    cash = 0.035 * (offer + R) + 0.03 * offer            # down + ~3% closing
    return {"max_offer": round(offer), "binding": binding, "rehab": round(R),
            "piti": round(piti), "rent_offset": round((units - 1) * rent_unit),
            "cash_to_close": round(cash), "capped_at_budget": offer >= max_price - 1,
            "monthly_surplus": round((units - 1) * rent_unit - piti)}


def zip_board_hh(*, state: str | None = None, units: int = 4, scope: str = "cosmetic",
                 level: str = "low", rate_pct: float = 6.55,
                 max_price: float = 300_000.0, min_pop: int = 5_000,
                 top: int = 40, max_tier: str = "safe",
                 allow_unknown: bool = False) -> list[dict]:
    """ZIP-level house-hack board: every real-rent ZIP (optionally one
    state), solved for the max offer; ranked by rent-to-value with the
    live-free ZIPs first.

    Safety gate (safety.py, real FBI city-level figures): rows above
    `max_tier` are dropped, and cities we have no verified figure for are
    dropped too unless allow_unknown — the yield leaders are exactly the
    places most likely to be screened out, which is the point."""
    import sqlite3 as _sq
    from value_add import METRO_GEO, STATE_COST_FACTORS, _haversine_mi
    from structural import trajectory_from_history
    db = Path(__file__).resolve().parent / "data" / "zips.db"
    conn = _sq.connect(str(db))
    conn.row_factory = _sq.Row
    q = ("SELECT zip, name, state, lat, lng, population, median_home_value, "
         "median_rent_monthly, history_zhvi FROM zips WHERE lat IS NOT NULL AND "
         "median_home_value IS NOT NULL AND median_rent_monthly IS NOT NULL "
         "AND population >= ?")
    args: list = [min_pop]
    if state:
        q += " AND state = ?"
        args.append(state.upper())
    rows = conn.execute(q, args).fetchall()
    conn.close()
    anchors = [(c, la, ln, rd) for c, (la, ln, rd) in METRO_GEO.items()
               if c in STATE_COST_FACTORS]
    out = []
    for rr in rows:
        mv, rent = rr["median_home_value"], rr["median_rent_monthly"]
        if abs(rent * 17 * 12 / mv - 1) <= 0.02:         # imputed rent
            continue
        best, bd = None, 1e12
        for c, la, ln, rd in anchors:
            d = _haversine_mi(rr["lat"], rr["lng"], la, ln)
            if d <= rd and d < bd:
                best, bd = c, d
        code = best or (rr["state"] if rr["state"] in STATE_COST_FACTORS else None)
        if code is None:
            continue
        h = house_hack_max_offer(mv, rent, code, units=units, scope=scope,
                                 level=level, rate_pct=rate_pct, max_price=max_price)
        if h is None:
            continue
        label = None
        if rr["history_zhvi"]:
            try:
                t = trajectory_from_history(json.loads(rr["history_zhvi"]))
                label = t["label"] if t else None
            except (ValueError, TypeError):
                label = None
        from safety import zip_safety, passes
        sf = zip_safety(rr["name"], rr["state"])
        if not passes(sf, max_tier, allow_unknown):
            continue
        out.append({**h, "zip": rr["zip"], "place": rr["name"], "state": rr["state"],
                    "market": code, "population": rr["population"],
                    "median_value": mv, "rent": rent,
                    "rtv_pct": round(rent * 12 / mv * 100, 1), "trajectory": label,
                    "safety": sf,
                    "competition": COMPETITION_LABEL.get(label or "steady", ("BALANCED", "="))[0]})
    # Safest first, then yield — a house-hack is where the family lives, so
    # safety outranks rent-to-value in the ordering.
    from safety import TIER_ORDER as _TO
    out.sort(key=lambda x: (_TO[x["safety"]["tier"]], -x["rtv_pct"], -x["max_offer"]))
    return out[:top]
