"""Underwriting arithmetic for one property in one ZIP. Pure, no I/O.

Two readers, one set of inputs:

  investor(...)  — buy-and-hold rental, single-family or 2-4 units: the
                   operating statement line by line, NOI, cap rate, debt
                   service, cash flow, cash-on-cash, DSCR, break-even rent
                   and occupancy, and whether the debt is positive leverage.
  owner(...)     — owner-occupant: the full monthly payment (P&I, PMI,
                   tax, insurance, HOA), the income it takes under the
                   28/36 rules, and the monthly cost of owning against
                   renting the same home.

NOTHING IS INVENTED. A missing input that a figure depends on makes that
figure None and names the input in `missing`. A rent of zero is a claim;
a missing rent is not, and the two are never confused.

Rates and percentages are passed in PERCENT (7.03 means 7.03%).
"""
from __future__ import annotations

# ── defaults, each with the convention it comes from ─────────────────
DEFAULTS = {
    "vacancy_pct": 5.0,
    "management_pct": 8.0,
    "maintenance_pct": 5.0,
    "capex_pct": 5.0,
    "investor_down_pct": 25.0,
    "investor_rate_premium": 0.75,
    "closing_cost_pct": 3.0,
    "term_years": 30,
    "owner_down_pct": 20.0,
    "pmi_annual_pct": 0.5,
    "front_end_dti_pct": 28.0,
    "back_end_dti_pct": 36.0,
    "owner_maintenance_pct_of_value": 1.0,
    "opportunity_return_pct": 4.0,
}

PROVENANCE = {
    "vacancy_pct": "5% of scheduled rent: the common underwriting floor; lenders "
                   "typically require at least this whatever the local rate",
    "management_pct": "8% of collected rent: mid-range of the 8-10% single-family "
                      "fee (small multifamily runs 6-8%)",
    "maintenance_pct": "5% of scheduled rent for repairs",
    "capex_pct": "5% of scheduled rent reserved for roofs, systems and turnover",
    "investor_down_pct": "25%: conventional minimum for 2-4 unit investment loans "
                         "(single-family investment allows 15-20% at a higher price)",
    "investor_rate_premium": "+0.75 points over the owner-occupied 30-year average: "
                             "investment-property loan-level price adjustments",
    "closing_cost_pct": "3% of price paid in cash at closing",
    "term_years": "30-year fixed",
    "owner_down_pct": "20%: the NAR/HSH affordability convention (no PMI)",
    "pmi_annual_pct": "0.5% of the loan per year while the loan is over 80% of value "
                      "(typical range 0.3-1.5% by credit score and down payment)",
    "front_end_dti_pct": "28% of gross income for housing (conventional front-end ratio)",
    "back_end_dti_pct": "36% of gross income for housing plus other debt",
    "owner_maintenance_pct_of_value": "1% of value per year, the homeowner rule of thumb",
    "opportunity_return_pct": "4%/yr forgone on the down payment and closing costs",
}


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def monthly_payment(principal, annual_rate_pct, years=30) -> float | None:
    """Level monthly principal and interest. None when it cannot be priced."""
    p, r, n = _num(principal), _num(annual_rate_pct), _num(years)
    if p is None or r is None or n is None or p < 0 or n <= 0 or r < 0:
        return None
    if p == 0:
        return 0.0
    months = int(round(n * 12))
    i = r / 100.0 / 12.0
    if i == 0:
        return p / months
    return p * i / (1 - (1 + i) ** -months)


def first_year_interest(principal, annual_rate_pct, years=30) -> float | None:
    """Interest paid in the first 12 payments — the part of a mortgage
    payment that is a cost rather than saving."""
    pay = monthly_payment(principal, annual_rate_pct, years)
    p, r = _num(principal), _num(annual_rate_pct)
    if pay is None or p is None or r is None:
        return None
    i, bal, paid = r / 100.0 / 12.0, p, 0.0
    for _ in range(12):
        interest = bal * i
        paid += interest
        bal -= pay - interest
    return paid


def _pct(x):
    return None if x is None else x / 100.0


def investor(price, rent_per_unit, *, units: int = 1, tax_rate_pct=None,
             insurance_annual=None, hoa_monthly=0.0, utilities_monthly=0.0,
             vacancy_pct=None, management_pct=None, maintenance_pct=None,
             capex_pct=None, rate_pct=None, down_pct=None,
             closing_cost_pct=None, term_years=None) -> dict:
    """One rental purchase, underwritten. See module docstring.

    `rent_per_unit` is the monthly market rent of ONE unit; `units` 1-4.
    `rate_pct` is the loan's rate — pass the owner-occupied average plus
    DEFAULTS['investor_rate_premium'] for an investment loan.
    """
    d = DEFAULTS
    vac = d["vacancy_pct"] if vacancy_pct is None else vacancy_pct
    mgmt = d["management_pct"] if management_pct is None else management_pct
    mnt = d["maintenance_pct"] if maintenance_pct is None else maintenance_pct
    cpx = d["capex_pct"] if capex_pct is None else capex_pct
    dn = d["investor_down_pct"] if down_pct is None else down_pct
    cc = d["closing_cost_pct"] if closing_cost_pct is None else closing_cost_pct
    term = d["term_years"] if term_years is None else term_years

    price, rent = _num(price), _num(rent_per_unit)
    units = int(units) if units else 1
    missing = [k for k, v in (("price", price), ("rent", rent), ("tax_rate", _num(tax_rate_pct)),
                              ("insurance", _num(insurance_annual)), ("rate", _num(rate_pct)))
               if v is None]
    out = {"inputs": {"price": price, "rent_per_unit": rent, "units": units,
                      "tax_rate_pct": _num(tax_rate_pct), "insurance_annual": _num(insurance_annual),
                      "hoa_monthly": _num(hoa_monthly) or 0.0,
                      "utilities_monthly": _num(utilities_monthly) or 0.0,
                      "vacancy_pct": vac, "management_pct": mgmt, "maintenance_pct": mnt,
                      "capex_pct": cpx, "rate_pct": _num(rate_pct), "down_pct": dn,
                      "closing_cost_pct": cc, "term_years": term},
           "missing": missing}
    if price is None or price <= 0 or rent is None or rent < 0:
        return out

    gsi = rent * units * 12.0                       # gross scheduled income
    vacancy = gsi * vac / 100.0
    egi = gsi - vacancy                             # effective gross income
    out.update({"gross_rent_annual": round(gsi, 2), "vacancy_loss": round(vacancy, 2),
                "effective_gross_income": round(egi, 2),
                "grm": round(price / gsi, 2) if gsi else None,
                "rent_to_price_monthly_pct": round(rent * units / price * 100, 3),
                "one_percent_rule": (rent * units / price) >= 0.01})

    tax = None if _num(tax_rate_pct) is None else price * _num(tax_rate_pct) / 100.0
    ins = _num(insurance_annual)
    fixed_lines = {
        "property_tax": tax,
        "insurance": ins,
        "hoa": (_num(hoa_monthly) or 0.0) * 12.0,
        "utilities": (_num(utilities_monthly) or 0.0) * 12.0,
    }
    variable_lines = {
        "management": egi * mgmt / 100.0,
        "maintenance": gsi * mnt / 100.0,
        "capex_reserve": gsi * cpx / 100.0,
    }
    lines = {**fixed_lines, **variable_lines}
    out["expenses"] = {k: (None if v is None else round(v, 2)) for k, v in lines.items()}
    if tax is None or ins is None:
        return out
    opex = sum(lines.values())
    noi = egi - opex
    out.update({"operating_expenses": round(opex, 2), "noi": round(noi, 2),
                "expense_ratio_pct": round(opex / egi * 100, 1) if egi else None,
                "cap_rate_pct": round(noi / price * 100, 2)})

    rate = _num(rate_pct)
    if rate is None:
        return out
    loan = price * (1 - dn / 100.0)
    pay = monthly_payment(loan, rate, term)
    ds = pay * 12.0
    cash_in = price * dn / 100.0 + price * cc / 100.0
    cf = noi - ds
    k = ds / loan if loan > 0 else None             # mortgage constant
    fixed = sum(v for v in fixed_lines.values())
    # Break-even rent: cash flow = 0, with management on collected rent and
    # maintenance/capex on scheduled rent.
    per_dollar = (1 - vac / 100.0) * (1 - mgmt / 100.0) - (mnt + cpx) / 100.0
    be_gsi = (fixed + ds) / per_dollar if per_dollar > 0 else None
    out.update({
        "loan_amount": round(loan, 2), "monthly_pi": round(pay, 2),
        "annual_debt_service": round(ds, 2), "cash_invested": round(cash_in, 2),
        "cash_flow_annual": round(cf, 2), "cash_flow_monthly": round(cf / 12.0, 2),
        "cash_on_cash_pct": round(cf / cash_in * 100, 2) if cash_in > 0 else None,
        "dscr": round(noi / ds, 2) if ds > 0 else None,
        "mortgage_constant_pct": round(k * 100, 2) if k is not None else None,
        "leverage_spread_pct": (round((noi / price - k) * 100, 2) if k is not None else None),
        "break_even_rent_per_unit": (round(be_gsi / 12.0 / units, 2) if be_gsi is not None else None),
        # Share of scheduled rent that must be collected to pay operating
        # expenses and the mortgage — the lender's break-even occupancy.
        "break_even_occupancy_pct": round((opex + ds) / gsi * 100, 1) if gsi else None,
    })
    return out


def owner(price, *, tax_rate_pct=None, insurance_annual=None, hoa_monthly=0.0,
          rate_pct=None, down_pct=None, pmi_annual_pct=None, term_years=None,
          closing_cost_pct=None, other_debt_monthly=0.0, area_income=None,
          market_rent=None, maintenance_pct_of_value=None,
          opportunity_return_pct=None, front_end_dti_pct=None,
          back_end_dti_pct=None) -> dict:
    """One owner-occupied purchase. See module docstring.

    `area_income` is the comparison income (the ZIP's median household
    income, or owners' median income) for payment-to-income.
    `market_rent` is what renting a comparable home costs per month.
    """
    d = DEFAULTS
    dn = d["owner_down_pct"] if down_pct is None else down_pct
    pmi_r = d["pmi_annual_pct"] if pmi_annual_pct is None else pmi_annual_pct
    term = d["term_years"] if term_years is None else term_years
    cc = d["closing_cost_pct"] if closing_cost_pct is None else closing_cost_pct
    mnt = d["owner_maintenance_pct_of_value"] if maintenance_pct_of_value is None else maintenance_pct_of_value
    opp = d["opportunity_return_pct"] if opportunity_return_pct is None else opportunity_return_pct
    fe = d["front_end_dti_pct"] if front_end_dti_pct is None else front_end_dti_pct
    be = d["back_end_dti_pct"] if back_end_dti_pct is None else back_end_dti_pct

    price, rate = _num(price), _num(rate_pct)
    tax_r, ins = _num(tax_rate_pct), _num(insurance_annual)
    hoa = _num(hoa_monthly) or 0.0
    missing = [k for k, v in (("price", price), ("rate", rate), ("tax_rate", tax_r),
                              ("insurance", ins)) if v is None]
    out = {"inputs": {"price": price, "rate_pct": rate, "down_pct": dn, "pmi_annual_pct": pmi_r,
                      "tax_rate_pct": tax_r, "insurance_annual": ins, "hoa_monthly": hoa,
                      "term_years": term, "other_debt_monthly": _num(other_debt_monthly) or 0.0},
           "missing": missing}
    if missing:
        return out

    loan = price * (1 - dn / 100.0)
    pi = monthly_payment(loan, rate, term)
    ltv = loan / price
    pmi = loan * pmi_r / 100.0 / 12.0 if ltv > 0.80 else 0.0
    tax = price * tax_r / 100.0 / 12.0
    ins_m = ins / 12.0
    total = pi + pmi + tax + ins_m + hoa
    other = _num(other_debt_monthly) or 0.0
    need_fe = total * 12.0 / (fe / 100.0)
    need_be = (total + other) * 12.0 / (be / 100.0)
    out.update({
        "loan_amount": round(loan, 2), "ltv_pct": round(ltv * 100, 1),
        "monthly": {"principal_interest": round(pi, 2), "pmi": round(pmi, 2),
                    "property_tax": round(tax, 2), "insurance": round(ins_m, 2),
                    "hoa": round(hoa, 2)},
        "monthly_total": round(total, 2),
        "cash_to_close": round(price * (dn + cc) / 100.0, 2),
        "income_needed": round(max(need_fe, need_be)),
        "income_needed_basis": ("back-end" if need_be > need_fe else "front-end"),
    })
    inc = _num(area_income)
    out["payment_to_income_pct"] = (round(total * 12 / inc * 100, 1)
                                    if inc and inc > 0 else None)

    # The monthly COST of owning: interest (not principal, which is saving),
    # PMI, tax, insurance, HOA, upkeep, and the return the cash would earn.
    # No appreciation is assumed in either direction.
    interest = first_year_interest(loan, rate, term) / 12.0
    upkeep = price * mnt / 100.0 / 12.0
    opp_cost = price * (dn + cc) / 100.0 * opp / 100.0 / 12.0
    own_cost = interest + pmi + tax + ins_m + hoa + upkeep + opp_cost
    out["cost_of_owning_monthly"] = round(own_cost, 2)
    rent = _num(market_rent)
    out["market_rent"] = rent
    out["own_minus_rent_monthly"] = round(own_cost - rent, 2) if rent is not None else None
    return out
