"""Underwriting arithmetic (underwrite.py) and its tax/insurance defaults
(re_assumptions.py).

Run: python tests/test_underwrite.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re_assumptions as RA  # noqa: E402
import underwrite as U  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def near(a, b, tol=0.01):
    return a is not None and b is not None and abs(a - b) <= tol


# ══════════════════════════════════════════════════════════════════
# THE MORTGAGE
# ══════════════════════════════════════════════════════════════════
check(near(U.monthly_payment(300_000, 7.0, 30), 1995.91),
      "the textbook payment: $300k at 7% over 30 years is $1,995.91")
check(near(U.monthly_payment(360_000, 0, 30), 1000.0), "a zero rate divides evenly")
check(U.monthly_payment(0, 7, 30) == 0.0, "no loan, no payment")
check(U.monthly_payment(None, 7, 30) is None and U.monthly_payment(1e5, None) is None,
      "a missing input prices nothing")
_pay = U.monthly_payment(300_000, 7.0, 30)
_i = 0.07 / 12
_bal12 = 300_000 * (1 + _i) ** 12 - _pay * ((1 + _i) ** 12 - 1) / _i
check(near(U.first_year_interest(300_000, 7.0, 30), 12 * _pay - (300_000 - _bal12), 0.05),
      "first-year interest = twelve payments less the principal they retired")

# ══════════════════════════════════════════════════════════════════
# INVESTOR — one worked deal, every line checked by hand
# ══════════════════════════════════════════════════════════════════
DEAL = dict(units=1, tax_rate_pct=1.2, insurance_annual=1800, rate_pct=7.78,
            down_pct=25, closing_cost_pct=3)
inv = U.investor(300_000, 2500, **DEAL)
check(inv["gross_rent_annual"] == 30_000 and inv["vacancy_loss"] == 1_500
      and inv["effective_gross_income"] == 28_500,
      "scheduled rent $30,000, 5% vacancy, $28,500 collected")
check(inv["expenses"] == {"property_tax": 3600.0, "insurance": 1800.0, "hoa": 0.0,
                          "utilities": 0.0, "management": 2280.0,
                          "maintenance": 1500.0, "capex_reserve": 1500.0},
      f"every expense line: tax on PRICE, management on COLLECTED rent, "
      f"repairs and capex on SCHEDULED rent (got {inv['expenses']})")
check(inv["operating_expenses"] == 10_680 and inv["noi"] == 17_820
      and inv["cap_rate_pct"] == 5.94,
      "NOI $17,820 on $300k is a 5.94% cap rate")
check(inv["grm"] == 10.0 and inv["one_percent_rule"] is False
      and inv["rent_to_price_monthly_pct"] == 0.833,
      "a gross rent multiplier of 10, and $2,500 on $300k misses the 1% rule")
check(inv["loan_amount"] == 225_000 and inv["cash_invested"] == 84_000,
      "25% down plus 3% closing is $84,000 of cash")
_ds = U.monthly_payment(225_000, 7.78, 30) * 12
check(near(inv["annual_debt_service"], _ds) and near(inv["cash_flow_annual"], 17_820 - _ds),
      "cash flow is NOI less twelve payments")
check(near(inv["dscr"], round(17_820 / _ds, 2)) and inv["dscr"] < 1.0,
      f"DSCR under 1.0 at today's rates — this deal does not cover its debt (got {inv['dscr']})")
check(inv["leverage_spread_pct"] < 0,
      "and the cap rate is below the mortgage constant: negative leverage")
check(near(inv["break_even_occupancy_pct"], round((10_680 + _ds) / 30_000 * 100, 1), 0.05),
      "break-even occupancy = (expenses + debt service) / scheduled rent")
_be = inv["break_even_rent_per_unit"]
_at_be = U.investor(300_000, _be, **DEAL)
check(abs(_at_be["cash_flow_annual"]) < 1.0,
      f"AT THE BREAK-EVEN RENT THE DEAL CASH-FLOWS TO ZERO — solved, not "
      f"guessed (rent {_be}, cash flow {_at_be['cash_flow_annual']})")
_rich = U.investor(300_000, 3400, **DEAL)
check(_rich["cash_flow_annual"] > 0 and _rich["dscr"] > 1.0 and _rich["one_percent_rule"],
      "a $3,400 rent clears the 1% rule, a 1.0 DSCR, and cash-flows")

duplex = U.investor(400_000, 1600, units=2, tax_rate_pct=1.5, insurance_annual=2400,
                    utilities_monthly=150, rate_pct=7.78)
check(duplex["gross_rent_annual"] == 38_400 and duplex["expenses"]["utilities"] == 1800,
      "a duplex: two units of rent, and the owner-paid utilities a 2-4 unit carries")
check(duplex["break_even_rent_per_unit"] is not None
      and abs(U.investor(400_000, duplex["break_even_rent_per_unit"], units=2,
                         tax_rate_pct=1.5, insurance_annual=2400, utilities_monthly=150,
                         rate_pct=7.78)["cash_flow_annual"]) < 1.0,
      "break-even rent is per unit")

# ── nothing is invented ──
norent = U.investor(300_000, None, **DEAL)
check("rent" in norent["missing"] and "noi" not in norent and "cap_rate_pct" not in norent,
      "A MISSING RENT IS NOT A ZERO RENT: no NOI, no cap rate, and it says why")
notax = U.investor(300_000, 2500, units=1, insurance_annual=1800, rate_pct=7.78)
check("tax_rate" in notax["missing"] and "noi" not in notax
      and notax["gross_rent_annual"] == 30_000,
      "a missing tax rate stops at the income lines")
norate = U.investor(300_000, 2500, units=1, tax_rate_pct=1.2, insurance_annual=1800)
check(norate["cap_rate_pct"] == 5.94 and "dscr" not in norate,
      "with no loan rate the property still has a cap rate, but no debt metrics")
check(U.investor(300_000, 0, **DEAL)["gross_rent_annual"] == 0,
      "while a rent of zero is a claim, and is underwritten as one")

# ══════════════════════════════════════════════════════════════════
# OWNER-OCCUPANT
# ══════════════════════════════════════════════════════════════════
own = U.owner(400_000, tax_rate_pct=1.0, insurance_annual=1500, rate_pct=7.03,
              down_pct=10, area_income=95_000, market_rent=2600)
_pi = U.monthly_payment(360_000, 7.03, 30)
check(own["loan_amount"] == 360_000 and own["ltv_pct"] == 90.0
      and own["monthly"]["pmi"] == 150.0,
      "10% down: a 90% loan carries PMI, 0.5% a year = $150 a month")
check(near(own["monthly_total"], _pi + 150 + 333.33 + 125, 0.02),
      "the monthly total is P&I + PMI + tax + insurance")
check(own["income_needed"] == round(own["monthly_total"] * 12 / 0.28)
      and own["income_needed_basis"] == "front-end",
      "income needed: housing within 28% of gross income")
check(own["payment_to_income_pct"] == round(own["monthly_total"] * 12 / 95_000 * 100, 1),
      "payment-to-income against the area's income")
check(own["cash_to_close"] == 52_000, "10% down plus 3% closing")
_debt = U.owner(400_000, tax_rate_pct=1.0, insurance_annual=1500, rate_pct=7.03,
                down_pct=10, other_debt_monthly=900)
check(_debt["income_needed_basis"] == "back-end"
      and _debt["income_needed"] == round((_debt["monthly_total"] + 900) * 12 / 0.36),
      "with $900 of other debt the 36% back-end ratio binds")
check(U.owner(400_000, tax_rate_pct=1.0, insurance_annual=1500, rate_pct=7.03,
              down_pct=20)["monthly"]["pmi"] == 0.0, "20% down: no PMI")
_interest = U.first_year_interest(360_000, 7.03, 30) / 12
_cost = _interest + 150 + 400_000 * 0.01 / 12 + 333.33 + 125 + 52_000 * 0.04 / 12
check(near(own["cost_of_owning_monthly"], _cost, 0.05)
      and near(own["own_minus_rent_monthly"], own["cost_of_owning_monthly"] - 2600, 0.01),
      "COST OF OWNING counts interest, not principal: principal is saving. "
      "It adds upkeep and the return the cash would have earned")
check("tax_rate" in U.owner(400_000, insurance_annual=1500, rate_pct=7)["missing"],
      "owner: a missing tax rate is named, not assumed")

# ══════════════════════════════════════════════════════════════════
# TAX AND INSURANCE DEFAULTS
# ══════════════════════════════════════════════════════════════════
check(RA.zip_effective_rate(3000, 300_000) == 1.0, "taxes $3,000 on $300k is 1.0%")
check(RA.zip_effective_rate(50, 500_000) is None and RA.zip_effective_rate(None, 1) is None,
      "a rate outside the plausible band is a data fault, not a regime")
_inv_ca = RA._tables()["investor_tax"]["CA"]
ca = RA.tax_defaults("CA", 0.62, 0.75)
check(ca["investor_pct"] == _inv_ca and "resets" in ca["investor_basis"],
      "CALIFORNIA RESETS AT SALE: an investor pays the purchase rate, not the "
      "Prop 13 rate the neighbours pay")
check(ca["owner_pct"] == 1.10 and "floor" in ca["owner_basis"],
      "and so does an owner-occupant, floored at 1.10%")
_inv_tx = RA._tables()["investor_tax"]["TX"]
hi = RA.tax_defaults("TX", 2.2, 1.8)
lo = RA.tax_defaults("TX", 1.4, 1.8)
check(hi["investor_pct"] > _inv_tx > lo["investor_pct"],
      "within a state, a ZIP that taxes above the state median stays above "
      "the state investor rate")
check(hi["investor_pct"] >= hi["owner_pct"] and lo["investor_pct"] >= lo["owner_pct"],
      "an investor never pays less than owners")
nozip = RA.tax_defaults("OH", None, 1.5)
check(nozip["investor_pct"] == RA._tables()["investor_tax"]["OH"]
      and "no ZIP" in nozip["investor_basis"] and "no ZIP" in nozip["owner_basis"],
      "no Census rate: the state rates, labelled as such")
check(RA.tax_defaults("ZZ", None, None)["investor_basis"] == "national fallback",
      "an unknown state says it fell back")
ins = RA.insurance_defaults("FL")
check(ins["landlord_300k"] == RA._tables()["landlord_ins"]["FL"] and ins["owner_300k"] > 0,
      "Florida's landlord and homeowner premiums come from their tables")
check(RA.scaled_premium(3000, 300_000) == 3000 and RA.scaled_premium(3000, 100_000) == 1800
      and RA.scaled_premium(3000, 1_000_000) == 6000,
      "premiums scale with price, floored at 0.6x and capped at 2x — land is not insured")
check(RA.state_medians({"a": ("TX", 1.0), "b": ("TX", 3.0), "c": ("OH", None)}) == {"TX": 2.0},
      "state medians skip ZIPs with no rate")

# ── a year's rent over 20% of the price is flagged, not dropped ──
_ok = U.investor(300_000, 2_000, tax_rate_pct=1.0, insurance_annual=1_500)
_bad = U.investor(47_000, 1_316, tax_rate_pct=1.0, insurance_annual=1_500)
check(_ok["gross_yield_pct"] == 8.0 and _ok["implausible"] is False,
      "gross yield: $24,000 of rent on $300k is 8%")
check(_bad["implausible"] is True and _bad["cap_rate_pct"] is not None,
      "DETROIT'S $47k HOME AT $1,316 RENT (34%) IS FLAGGED — and still computed, not dropped")
check(U.owner(47_000, tax_rate_pct=1.0, insurance_annual=900, rate_pct=6.3,
              market_rent=1_316)["rent_implausible"] is True
      and U.owner(300_000, tax_rate_pct=1.0, insurance_annual=900, rate_pct=6.3,
                  market_rent=2_000)["rent_implausible"] is False,
      "the owner's own-vs-rent comparison carries the same flag")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for f in _FAILS:
        print("  ✗", f)
    sys.exit(1)
print(f"OK — all {_COUNT} underwriting checks passed.")
print("   operating statement, debt, break-even solved; owner PITI, DTI, cost of owning;")
print("   tax: purchase rate where assessments reset, ZIP-relative elsewhere")
