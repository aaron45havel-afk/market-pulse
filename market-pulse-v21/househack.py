"""Owner-occupant house-hack realism for /multifamily.

The ZIP board was ranking on yield alone, which sorts *toward* the crime
discount — the same trap the strict screen had. It also carried four
number problems worth naming, because each one flattered or maligned a
deal in a specific direction:

  1. The scenario priced the FILTER CAP, not the properties returned.
     A $350k cap over ZIPs whose stock is $75-160k overstates PITI, cash
     to close, and required income across the whole page.
  2. Rent medians are ~69% IMPUTED nationally (value/17/12). Ohio is 73%.
     A "state median rent" built from those is largely circular
     arithmetic restating home values, not observed rent.
  3. "Net cost" was benchmarked against zero. The real benchmark for a
     house-hack is what you'd otherwise pay in rent — paying $485 to
     live somewhere that rents for $1,193 is a $708/mo win being
     displayed in red.
  4. Home value is the SINGLE-FAMILY median. A triplex does not cost the
     SFR median, and insurance on an owner-occupied 3-unit with tenants
     is not an SFR premium.

Everything here is an explicit, labelled assumption rather than a hidden
one — the point is that the user can see and override each.
"""
from __future__ import annotations

# 2-4 unit buildings trade above the single-family median in the same
# ZIP: more roof, more kitchens, and they're priced partly on income.
# Published per-unit multiples vary widely by market, so this is a
# deliberately conservative, EDITABLE default rather than a claim.
UNIT_PRICE_FACTOR = {2: 1.25, 3: 1.55, 4: 1.85}

# Landlord/owner-occupied multi-unit insurance vs an SFR policy: more
# units means more liability exposure and higher rebuild cost.
UNIT_INSURANCE_FACTOR = {2: 1.25, 3: 1.45, 4: 1.65}

# FHA underwriting
FHA_RENT_CREDIT = 0.75        # share of gross rent counted as income
FHA_DTI_MAX = 0.45            # back-end ratio most lenders will approve
FHA_SELF_SUFF_UNITS = 3       # the 75%-of-all-rents test applies at 3-4

# Reserves a first-time small landlord actually needs: FHA requires 3
# months PITI reserves on 3-4 units, and the real failure mode is a
# vacancy plus one mechanical failure, not the monthly spread.
RESERVE_MONTHS_PITI = 3
MECHANICAL_RESERVE = 8_000    # furnace / roof patch / water heater


def est_building_price(sfr_median: float | None, units: int,
                       factor: float | None = None) -> dict | None:
    """SFR median -> estimated 2-4 unit building price, labelled as an
    estimate. 2-4 unit ask prices are not in the ZIP dataset, so this is
    an adjustment with its assumption on the surface, not a measurement."""
    if not sfr_median or sfr_median <= 0:
        return None
    f = factor if factor and factor > 0 else UNIT_PRICE_FACTOR.get(units, 1.55)
    return {"sfr_median": round(sfr_median), "factor": f,
            "estimated_price": round(sfr_median * f),
            "basis": f"SFR median x {f:g} ({units}-unit adjustment — estimate, not a listing)"}


def apply_unit_insurance(piti: dict | None, units: int) -> dict | None:
    """Scale an SFR insurance premium up to an owner-occupied 2-4 unit
    policy, keeping PITI consistent, in place.

    This exists so the page cannot price the same building three
    different ways. The table, the scenario card and the deal checker all
    run through here; when only the card did the uplift, a ZIP could show
    a passing FHA badge in the table and a failing one in the card, which
    is worse than either number alone."""
    if not piti:
        return piti
    f = UNIT_INSURANCE_FACTOR.get(units, 1.45)
    bump = piti["monthly_ins"] * (f - 1)
    piti["monthly_ins"] += bump
    piti["piti"] += bump
    piti["insurance_factor"] = f
    return piti


def rent_is_observed(rent: float | None, home_value: float | None) -> bool:
    """False when the rent is the imputed value/17/12 placeholder rather
    than an observed ZORI figure. Imputed rents make cap rate exactly
    5.88% by construction, so any yield built on them is circular."""
    if not rent or not home_value:
        return False
    return abs(rent * 17 * 12 / home_value - 1) > 0.02


def income_to_qualify(piti_monthly: float, units: int, rent_unit: float,
                      other_debts: float = 0.0,
                      dti_max: float = FHA_DTI_MAX) -> dict:
    """W-2 income needed for FHA approval, per Handbook 4000.1.

    THE EASY VERSION OF THIS IS WRONG IN THE DANGEROUS DIRECTION.
    Subtracting the rent credit from PITI and dividing by the DTI cap
    understates the required income badly, because that is not how a
    lender runs the ratio. Rental income is added to the borrower's
    EFFECTIVE INCOME (the denominator); the full PITI stays in the
    numerator. It is never netted out of the payment.

      2 units   — rental income = 75% of gross rent on the unit you do
                  not occupy.
      3-4 units — rental income = NET SELF-SUFFICIENCY rental income,
                  i.e. 75% of ALL units' rent MINUS the full PITI. When
                  that is negative it is not a smaller credit, it is a
                  LIABILITY added to the numerator — the same arithmetic
                  that fails the self-sufficiency gate also raises the
                  income you need, so a failing building punishes twice.

    Required W-2 = (PITI + debts + liability) / DTI − rental income."""
    rented = max(0, units - 1)
    rent_unit = max(0.0, rent_unit)
    if units >= FHA_SELF_SUFF_UNITS:
        net = FHA_RENT_CREDIT * units * rent_unit - piti_monthly
        basis = "net self-sufficiency rental income: 75% of ALL units' rent − PITI"
    else:
        net = FHA_RENT_CREDIT * rented * rent_unit
        basis = "75% of gross rent on the unit you don't occupy"
    add_income = max(0.0, net)      # positive → added to effective income
    liability = max(0.0, -net)      # negative → added to monthly debts
    need = ((piti_monthly + other_debts + liability) / dti_max
            if dti_max > 0 else 0.0)
    monthly_income = max(0.0, need - add_income)
    return {"rent_credit": round(net), "rent_income": round(add_income),
            "rent_liability": round(liability), "basis": basis,
            "qualifying_housing": round(piti_monthly + other_debts + liability),
            "monthly_income": round(monthly_income),
            "annual_income": round(monthly_income * 12),
            "dti_max": dti_max, "rented_units": rented}


def vs_renting(piti_monthly: float, units: int, rent_unit: float) -> dict:
    """The honest headline for a house-hack: your net housing cost versus
    what renting the equivalent unit would cost you. Zero is not the
    benchmark — the local rent is."""
    rented = max(0, units - 1)
    offset = rented * max(0.0, rent_unit)
    net = piti_monthly - offset
    delta = rent_unit - net          # positive = cheaper than renting
    return {"net_cost": round(net), "rent_equivalent": round(rent_unit),
            "monthly_advantage": round(delta),
            "annual_advantage": round(delta * 12),
            "beats_renting": delta > 0,
            "live_free": net <= 0}


def reserves_needed(piti_monthly: float, closing: float,
                    down: float, units: int = 0) -> dict:
    """Cash to close plus the reserve a first-time landlord actually
    needs. The deal does not fail on the monthly spread; it fails on one
    vacancy landing the same month as a furnace.

    We hold 3 months of PITI regardless of unit count, but the REASON
    differs and the UI should not misstate it: on 3-4 units FHA requires
    it, on a duplex it is our recommendation, not a rule."""
    required = units >= FHA_SELF_SUFF_UNITS
    reserve = RESERVE_MONTHS_PITI * piti_monthly + MECHANICAL_RESERVE
    return {"cash_to_close": round(down + closing),
            "reserve_months": RESERVE_MONTHS_PITI,
            "reserve_required": required,
            "reserve_basis": (
                f"FHA requires {RESERVE_MONTHS_PITI} months' PITI in reserves on "
                f"{FHA_SELF_SUFF_UNITS}-4 units" if required else
                f"{RESERVE_MONTHS_PITI} months' PITI — our recommendation; FHA's "
                "reserve requirement on a 2-unit is lighter than this"),
            "piti_reserve": round(RESERVE_MONTHS_PITI * piti_monthly),
            "mechanical_reserve": MECHANICAL_RESERVE,
            "reserve_total": round(reserve),
            "all_in_cash": round(down + closing + reserve)}


def fha_self_sufficiency(piti_monthly: float, units: int,
                         rent_unit: float) -> dict | None:
    """FHA's hard funding gate on 3-4 units: 75% of ALL units' market
    rent must cover PITI. Returns None for duplexes, which are exempt."""
    if units < FHA_SELF_SUFF_UNITS:
        return None
    qualifying = FHA_RENT_CREDIT * units * max(0.0, rent_unit)
    required = piti_monthly / (FHA_RENT_CREDIT * units) if units else 0.0
    return {"qualifying_rent": round(qualifying), "piti": round(piti_monthly),
            "passes": qualifying >= piti_monthly,
            "required_rent_per_unit": round(required),
            "gap_per_unit": round(max(0.0, required - rent_unit))}


def _one_percent(price: float, units: int, rent_unit: float) -> dict:
    """The old 1% screen: total monthly rent >= 1% of price. It is a
    30-second sniff test, not a law — it was calibrated in a 5-6% rate
    era and fails almost everything at today's rates. Reported as
    context, never as a gate."""
    gross = units * max(0.0, rent_unit)
    pct = (gross / price * 100) if price > 0 else 0.0
    return {"gross_monthly_rent": round(gross), "pct_of_price": round(pct, 2),
            "grm_years": round(price / (gross * 12), 1) if gross else None,
            "passes": pct >= 1.0}


def check_deal(piti_monthly: float, units: int, rent_unit: float,
               price: float, *, insurance_monthly: float = 0.0,
               closing: float = 0.0, down: float = 0.0,
               your_income: float | None = None,
               other_debts: float = 0.0) -> dict:
    """Go / no-go on ONE actual listing, with ACTUAL rents.

    The board answers "which ZIPs", which is the easier half. The half
    that decides anything is a specific building at a specific ask with
    the rents the seller says it collects — and that is where the board's
    two weakest inputs (an estimated building price, an often-imputed
    rent) get replaced by real numbers the user typed in.

    Ordering matters: the FHA self-sufficiency test is a HARD funding
    gate on 3-4 units. A deal that fails it does not close, no matter how
    good the spread looks, so it is checked first and it alone can sink
    the verdict."""
    vs = vs_renting(piti_monthly, units, rent_unit)
    ss = fha_self_sufficiency(piti_monthly, units, rent_unit)
    q = income_to_qualify(piti_monthly, units, rent_unit, other_debts)
    res = reserves_needed(piti_monthly, closing, down, units)
    onepct = _one_percent(price, units, rent_unit)

    # Same stress the table column uses: rents 10% light, insurance
    # reprices +30%, one vacant month a year.
    stressed_piti = piti_monthly + 0.30 * insurance_monthly
    stressed_rent = max(0, units - 1) * max(0.0, rent_unit) * 0.90 * (11 / 12)
    stressed_net = stressed_piti - stressed_rent
    stressed = {"net_cost": round(stressed_net),
                "beats_renting": stressed_net < rent_unit,
                "advantage": round(rent_unit - stressed_net)}

    checks: list[dict] = []
    if ss is not None:
        checks.append({
            "key": "self_suff", "hard": True,
            "label": f"FHA self-sufficiency ({units} units)",
            "status": "pass" if ss["passes"] else "fail",
            "detail": (f"75% × {units} units × ${rent_unit:,.0f} = "
                       f"${ss['qualifying_rent']:,.0f} vs PITI ${ss['piti']:,.0f}"
                       + ("" if ss["passes"] else
                          f" — short ${ss['gap_per_unit']:,.0f}/unit; "
                          f"needs ${ss['required_rent_per_unit']:,.0f}/unit to fund")),
        })
    else:
        checks.append({
            "key": "self_suff", "hard": False,
            "label": "FHA self-sufficiency", "status": "na",
            "detail": "2-unit purchases are exempt from this test.",
        })
    checks.append({
        "key": "vs_rent", "hard": False,
        "label": "Beats renting the same unit",
        "status": "pass" if vs["beats_renting"] else "fail",
        "detail": (f"net ${vs['net_cost']:,.0f}/mo vs ${vs['rent_equivalent']:,.0f} rent "
                   f"= {'+' if vs['monthly_advantage'] >= 0 else '−'}"
                   f"${abs(vs['monthly_advantage']):,.0f}/mo"),
    })
    checks.append({
        "key": "stressed", "hard": False,
        "label": "Survives the stress case",
        "status": "pass" if stressed["beats_renting"] else "warn",
        "detail": (f"rents −10%, insurance +30%, 1 vacant month/yr → "
                   f"net ${stressed['net_cost']:,.0f}/mo"),
    })
    # Rental income is added to income, or — when 75% of rents can't
    # cover PITI on a 3-4 unit — subtracted as a liability. Say which.
    if q["rent_liability"]:
        rent_txt = (f"${q['rent_liability']:,.0f}/mo of NEGATIVE rental income "
                    f"counts against you as a liability")
    else:
        rent_txt = f"${q['rent_income']:,.0f}/mo of rental income added to your income"
    if your_income is not None and your_income > 0:
        ok = your_income >= q["annual_income"]
        checks.append({
            "key": "income", "hard": True,
            "label": f"You qualify at {q['dti_max'] * 100:.0f}% DTI",
            "status": "pass" if ok else "fail",
            "detail": (f"needs ${q['annual_income']:,.0f}/yr W-2 — {rent_txt}; "
                       f"you entered ${your_income:,.0f}"),
        })
    else:
        checks.append({
            "key": "income", "hard": False,
            "label": "Income to qualify", "status": "info",
            "detail": (f"${q['annual_income']:,.0f}/yr W-2 at {q['dti_max'] * 100:.0f}% DTI — "
                       f"{rent_txt}. Full PITI stays in the ratio; rent is never "
                       "netted out of the payment."),
        })
    checks.append({
        "key": "cash", "hard": False,
        "label": "Cash you need on hand", "status": "info",
        "detail": (f"${res['cash_to_close']:,.0f} to close + "
                   f"${res['reserve_total']:,.0f} reserve = "
                   f"${res['all_in_cash']:,.0f} all-in"),
    })
    grm_txt = (f" · GRM {onepct['grm_years']} yrs" if onepct["grm_years"] else "")
    # INFO, never warn. A "warn" downgrades the verdict to marginal, and
    # this rule was calibrated in a 5-6% rate era — at today's rates it
    # misses on most real listings, so gating on it meant a deal that
    # cleared every funding and DTI test could never come back WORKS.
    # It is reported because it is a useful sniff test, not because it
    # decides anything.
    checks.append({
        "key": "one_pct", "hard": False,
        "label": "1% rule (context only)",
        "status": "info",
        "detail": (f"gross rent ${onepct['gross_monthly_rent']:,.0f} = "
                   f"{onepct['pct_of_price']}% of ${price:,.0f}{grm_txt} — "
                   + ("clears" if onepct["passes"] else "misses")
                   + " the 1% line, which is a rate-era heuristic and does not"
                     " affect the verdict either way"),
    })

    hard_fail = any(c["hard"] and c["status"] == "fail" for c in checks)
    soft_fail = any(not c["hard"] and c["status"] == "fail" for c in checks)
    warn = any(c["status"] == "warn" for c in checks)
    verdict = ("no" if hard_fail else
               "marginal" if (soft_fail or warn) else "yes")
    if hard_fail:
        headline = "This does not fund or does not qualify as entered."
    elif soft_fail:
        headline = "It funds, but it costs you more than renting the same unit."
    elif warn:
        headline = "It works today, with less margin than you'd want."
    else:
        headline = "It funds, it qualifies, and it beats renting."
    return {"verdict": verdict, "headline": headline, "checks": checks,
            "vs_rent": vs, "selfsuff": ss, "qualify": q, "reserves": res,
            "stressed": stressed, "one_pct": onepct, "price": round(price)}
