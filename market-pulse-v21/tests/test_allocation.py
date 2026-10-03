"""Now vs optimal (/capital): what the owner holds and how they split their pay
today, against the path the board would take. Parsing, what follows from the
holdings (cash, net worth, shelter), every holding's return in the board's
unit, rentals on the equity that could be taken out, the switch test (tax
and costs to move), account limits, the emergency fund, lumpy real estate,
the pay comparison, and the page. Offline — boards are built by hand or from
injected pages.

Run: python tests/test_allocation.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import allocation as A  # noqa: E402
import capital as K  # noqa: E402
import headroom as HR  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def near(a, b, tol=0.01):
    return a is not None and b is not None and abs(a - b) <= tol


OCT = date(2026, 10, 3)
BASE = {"fed_rate": 24, "state_rate": 9.3, "ltcg_rate": 15, "hold_years": 5, "market_return": 7.0,
        "estimate_weight": 50, "rf_rate": 4.0, "home_state": "CA", "hourly_value": 50, "picks_hours": 2,
        "monthly_expenses": 1000, "emergency_months": 6}

# ── parsing ────────────────────────────────────────────────────────
rows, bad = A.parse_holdings(
    "VTI, taxable, 50000, 30000\nAAPL, Roth IRA, 12000\nAlly, savings, 20000, 3.8%\nT, T-bills, 5000, 4%\n"
    "Plan fund, 401(k), 80000\nX, brokerage, 100, basis 50, 9%\n# a note\n\nbad\nY, nowhere, 5\nZ, taxable, abc\n"
    "W, taxable, 10, 5, 6\nCard, debt, 100\nQ, taxable, 10, ten%")
check([r["name"] for r in rows] == ["VTI", "AAPL", "Ally", "T", "Plan fund", "X"],
      "holdings parse line by line; notes and blank lines are skipped")
check(bad == ["bad", "Y, nowhere, 5", "Z, taxable, abc", "W, taxable, 10, 5, 6", "Card, debt, 100", "Q, taxable, 10, ten%"],
      "A LINE THAT CANNOT BE READ IS KEPT AND NAMED, never guessed at: no account, an unknown account, a value that "
      "is not a number, two bases, a debt (debts have their own box), a bad %")
check(rows[0]["ticker"] == "VTI" and rows[0]["basis"] == 30000 and rows[0]["rate"] is None,
      "a bare number after the value is the cost basis")
check(rows[1]["account"] == "roth" and rows[4]["account"] == "k401" and rows[4]["ticker"] is None,
      "'Roth IRA' and '401(k)' are read; a fund's name is not a ticker")
check(rows[2]["account"] == "cash" and rows[2]["ticker"] is None and rows[2]["rate"] == 3.8
      and rows[3]["state_exempt"] and not rows[2]["state_exempt"],
      "savings is cash with its rate; T-bills are cash whose interest the state does not tax")
check(rows[5]["account"] == "taxable" and rows[5]["basis"] == 50 and rows[5]["rate"] == 9,
      "'basis 50' and '9%' in either order")
fl, fbad = A.parse_flows("401k, 401k, 500\nCard, debt, 300\nLoan, debt, 100, 6.5%\nVTI, taxable, x\nS, savings, 5, 7")
check([f["name"] for f in fl] == ["401k", "Card", "Loan"] and fl[2]["rate"] == 6.5 and fbad == ["VTI, taxable, x", "S, savings, 5, 7"],
      "the pay split reads what, account, amount, %; debt is an account there")
re_ = A.parse_owned_re([{"name": "", "use": "weird", "value": "$900,000", "loan": "300000", "rate": "50%",
                         "year": "2015", "basis": ""}, {"value": 0}, "junk", {"name": "Home", "use": "home", "value": 1}])
check(len(re_) == 2 and re_[0]["name"] == "Property" and re_[0]["use"] == "rental" and re_[0]["value"] == 900_000
      and re_[0]["rate"] == 20 and re_[0]["basis"] is None and re_[0]["year"] == 2015 and re_[0]["hours"] == 0.0,
      "property: money strings read, the rate clamped, a blank basis kept blank (not 0), an unknown use is a rental, "
      "no value no record")
check(A.parse_owned_re("not a list") == [] and re_[1]["use"] == "home", "junk is no property")

# ── what follows from the holdings ─────────────────────────────────
D = A.derive({"holdings": "Chk, checking, 1000, 0%\nSav, savings, 2000, 4%\nVTI, taxable, 5000\nAAPL, roth, 3000\n"
                          "MSFT, taxable, 4000\nPlan, 401k, 10000",
              "cash": 99, "stock_holdings": 99, "debts": [{"name": "Car", "balance": 6000, "apr": 5}],
              "owned_re": [{"name": "Rental", "use": "rental", "value": 500_000, "loan": 300_000}]})
check(D["cash"] == 3000 and D["_cash_from_holdings"], "ONE SOURCE OF TRUTH: the cash lines are cash on hand")
check(D["stock_holdings"] == 7000 and D["_picks_from_holdings"], "and the individual stocks are the picks held (not VTI, not the plan fund)")
check(not A.is_pick({"account": "taxable", "ticker": "FXAIX"}) and not A.is_pick({"account": "k401", "ticker": "AAPL"})
      and A.is_pick({"account": "roth", "ticker": "AAPL"}),
      "a mutual-fund ticker is a fund; anything in a 401(k) is a plan fund")
check(D["_net_worth"] == 3000 + 22000 + 200_000 - 6000 and D["_re_equity"] == 200_000 and not D.get("_owns_shelter"),
      "net worth: cash + everything invested + property equity − debts")
D2 = A.derive({"cash": 5000, "stock_holdings": 1000, "owned_re": [{"name": "H", "use": "house_hack", "value": 800_000,
                                                                     "loan": 700_000}], "housing_cost": 2500})
check(D2["_owns_shelter"] and D2["housing_cost"] == 0 and "cash" not in D2 and D2["_net_worth"] == 106_000,
      "owning where you live (a house hack counts) means no rent to stop paying; with no holdings listed, the "
      "profile's own cash and picks count")
check(K.profile_with_defaults({"cash": 99, "holdings": "Chk, cash, 1000, 0%"})["cash"] == 1000,
      "the profile reads it through")

# ── a board, by hand ───────────────────────────────────────────────
P = K.profile_with_defaults({**BASE, "debts": [{"name": "Card", "balance": 4000, "apr": 22.9}]})
t = K.tax_rates(P)


def stock(tk, pre, after, cost=0.04, time=1.0):
    return {"id": f"stock:{tk}", "kind": "stock", "label": tk, "source": "Compounders", "sources": ["Compounders"],
            "ret_pre": pre, "ret_after": after, "cost_drag": cost, "time_drag": time, "ret_net": after - cost - time,
            "detail": {"ticker": tk}, "div_pct": 1.0, "round_trip_pct": 0.2, "min_capital": 1.0}


HHROW = {**K.row(id="hh:1", kind="re", label="House hack — Benicia", source="Headroom", ret_pre=30.0, ret_after=30.0,
                 basis="", min_capital=40_000, deploy_months=3, hours_month=6), "ret_net": 25.0}
TBILL = {**K.guaranteed_rows(P)[0], "ret_net": 3.04, "cost_drag": 0, "time_drag": 0}
DEBT = {**K.guaranteed_rows(P)[1], "ret_net": 22.9}


def board(p, rows=None, winner=None):
    rows = rows if rows is not None else [DEBT, stock("AAA", 12.0, 9.0), stock("BBB", 11.0, 8.0), TBILL, HHROW]
    return {"profile": p, "rows": K.rank(rows), "plan": {"winner": winner, "steps": [], "left": 0},
            "monthly_free": 2000.0}


def hold(text, **kw):
    p = K.profile_with_defaults({**BASE, "debts": [{"name": "Card", "balance": 4000, "apr": 22.9}], "picks_n": 2,
                                 "holdings": text, **kw})
    return p


# ── each holding's return ──────────────────────────────────────────
td = A.stock_time_drag(P, 2000.0)
check(near(td, 2 * 12 * 50 / (2000 * 12 * 5 / 2) * 100), "research time: the board's own charge per pick")
srows = {"AAA": stock("AAA", 12.0, 9.0)}
h = {r["name"]: r for r in A.parse_holdings(
    "VTI, taxable, 50000, 30000\nChk, checking, 1000, 0.5%\nT, tbills, 1000, 4%\nMyst, cash, 500\nPlan, 401k, 10000\n"
    "AAPL, roth, 3000\nAAA, taxable, 2000, 2000\nHSA fund, hsa, 1000\nNoB, taxable, 100")[0]}
m = {k: A.measure_holding(v, P, srows, td, set()) for k, v in h.items()}
check(near(m["VTI"]["h"], K.after_tax_taxable(7.0, A.INDEX_DIV, 5, t["qualified"], t["qualified"]), 0.01)
      and m["VTI"]["bucket"] == "funds",
      "an index fund earns the market return, taxed as held in a taxable account, no research time")
check(near(m["VTI"]["tau"], 20_000 * t["qualified"] / 50_000, 1e-6),
      "TAX TO SELL: the gain over the basis at the capital-gains rate, per dollar sold")
check(near(m["Chk"]["h"], 0.5 * (1 - t["ordinary"])) and near(m["T"]["h"], 4 * (1 - t["tbill"])),
      "bank interest is taxed federal + state; T-bill interest federal only")
check(m["Myst"]["h"] is None and "no rate" in m["Myst"]["why_not"], "CASH WITHOUT A RATE IS NOT MEASURED, not guessed")
check(m["Plan"]["h"] == 7.0 and near(m["Plan"]["weq"], 1 - t["retire"]),
      "a traditional balance earns its pre-tax rate on its after-tax value — counted at that value")
check(near(m["AAPL"]["h"], 7.0 - td) and m["AAPL"]["bucket"] == "picks" and "no screen" in m["AAPL"]["src"],
      "a stock on no screen: the market return, untaxed in a Roth, less the research time")
check(m["AAA"]["pre"] == 12.0 and near(m["AAA"]["c"], 0.001) and "Compounders" in m["AAA"]["src"],
      "a ticker on a screen gets that screen's blended estimate, and half the round trip to sell")
check(near(m["HSA fund"]["h"], 7.0 * (1 - 0.093)), "California taxes HSA growth")
check(m["NoB"]["tau"] is None and "cost basis" in m["NoB"]["keep_reason"],
      "no basis, no switch test — it is kept and the owner is told why")
check(A.measure_holding(h["AAA"], P, srows, td, {"AAA"})["keep_reason"] == "already one of the top picks",
      "a holding already among the top picks is where it should be")

# ── property: return on the equity that could be taken out ─────────
RENT = {"name": "Duplex", "use": "rental", "value": 900_000, "loan": 300_000, "rate": 3.1, "payment": 1900,
        "rent": 5200, "shelter_rent": 0, "costs": 1500, "basis": 500_000, "year": 2015, "hours": 4}
pr = A.measure_property(RENT, P, OCT)
E = 900_000 * 0.93 - 300_000
interest, principal = A._year_one(300_000, 3.1, 1900)
dep = 500_000 * HR.BUILDING_SHARE / HR.DEP_YEARS
op_tax = max(0, 44_400 - interest - dep) * t["ordinary"]
after = (21_600 - op_tax + principal) / E * 100
check(near(pr["value"], E) and near(pr["after"], after, 0.01) and near(pr["h"], after - 4 * 12 * 50 / E * 100, 0.01),
      "LAZY EQUITY, MEASURED: cash flow after the payment and the tax depreciation does not shelter, plus principal, "
      "on the $537k that could be taken out — not on the original down payment")
check(near(interest + principal, 1900 * 12, 0.01) and principal > 0, "twelve payments split into interest and principal")
dep_taken = dep * 11
gain = 900_000 * 0.93 - (500_000 - dep_taken)
want_tax = dep_taken * (24 + 9.3) / 100 + (gain - dep_taken) * t["qualified"]
check(near(pr["sale_tax"], want_tax, 1) and near(pr["tau"], want_tax / E, 1e-6),
      "SELLING A RENTAL: the depreciation taken is recaptured (25% federal cap, plus state), the rest of the gain at the "
      "capital-gains rate")
check(near(A.rental_sale_tax({**RENT, "value": 300_000}, P, 11), 0.0), "a sale at a loss owes nothing")
check(near(A.rental_sale_tax(RENT, {**P, "niit": True}, 11) - want_tax, gain * 0.038, 1),
      "NIIT on the whole gain when the owner says it applies")
check(A.measure_property({**RENT, "basis": None}, P, OCT)["keep_reason"].startswith("add its purchase price"),
      "no purchase price: measured, not tested for sale")
check(A.measure_property({**RENT, "loan": 900_000}, P, OCT)["h"] is None, "no equity left: not measured")
check("payment" in A.measure_property({**RENT, "payment": 0}, P, OCT)["why_not"],
      "a loan without its payment is not measured (the cash flow would be invented)")
home = A.measure_property({**RENT, "use": "home", "rent": 0, "shelter_rent": 3500}, P, OCT)
check(near(home["after"], ((3500 - 1500 - 1900) * 12 + principal) / E * 100, 0.01)
      and "never" not in (home["keep_reason"] or "") and "selling it is not tested" in home["keep_reason"],
      "THE PLACE YOU LIVE: the rent you would pay counts, untaxed — and it is never put up for sale")

# ── the switch test ────────────────────────────────────────────────
def run(text, rows=None, winner=None, owned=None, **kw):
    p = hold(text, owned_re=owned or [], **kw)
    b = board(p, rows, winner)
    return A.compare_holdings(b, OCT)


NODEBT = [stock("AAA", 12.0, 9.0), stock("BBB", 11.0, 8.0), TBILL]
r1 = run("VTI, taxable, 50000, 50000", rows=NODEBT)
mv = r1["moves"]
check(len(mv) == 1 and mv[0]["to_kind"] == "picks" and mv[0]["tax"] == 0 and mv[0]["sold"] == 50_000,
      "an index fund with no gain moves to the picks when they earn more after tax and friction")
r2 = run("VTI, taxable, 50000, 5000", rows=NODEBT)
check(not r2["moves"] and r2["items"][0]["status"] == "keep",
      "THE SAME FUND WITH A BIG GAIN STAYS: the tax to sell outweighs the better return over the hold")
s = r2["items"][0]
a_picks = (9.0 - 1.04 + 8.0 - 1.04) / 2
check((1 - s["tau"]) * (1 + a_picks / 100) ** 5 < (1 + s["h"] / 100) ** 5 and
      (1 - 0) * (1 + a_picks / 100) ** 5 > (1 + s["h"] / 100) ** 5,
      "and the reason is exactly the rule: (1 − tax)(1 + new)^hold against (1 + old)^hold")
r3 = run("Stable, 401k, 20000, 3%\nAAPL, roth, 15000\nHS, hsa, 1000, 3%")
to = {m_["from"]: m_ for m_ in r3["moves"]}
check(to["Stable"]["to"] == "An index fund, inside the same account" and to["Stable"]["tax"] == 0,
      "A 401(k) HOLDS THE PLAN'S FUNDS: a 3% stable-value fund moves to the index fund, never to picks, debt or property")
check(to["AAPL"]["to_kind"] == "picks" and near(to["AAPL"]["a"], (12 - 1.04 + 11 - 1.04) / 2),
      "a Roth can hold the picks, at their pre-tax return")
check(near(to["HS"]["a"], to["AAPL"]["a"] * (1 - 0.093), 0.01), "in an HSA, California's tax on growth comes off")
check(near(run("Stable, 401k, 20000, 3%")["moves"][0]["gain"],
           (1 - t["retire"]) * 20_000 * ((1.07 ** 5) - (1.03 ** 5)), 1),
      "a traditional account's gain counts at its after-tax value")
FLAT = [stock("AAA", 8.0, 6.0, cost=0, time=0)]
check(not run("Some fund, roth, 1000, 7.9%", rows=FLAT)["moves"]
      and run("Some fund, roth, 1000, 7.5%", rows=FLAT)["moves"],
      "A TRIVIAL EDGE IS NOT A TRADE: 7.9% → 8% gains under 1% over the whole hold and stays; 7.5% → 8% moves")
check(run("VTI, taxable, 50000, 5000")["moves"][0]["to_kind"] == "debt",
      "but the same big-gain fund still pays off a 22.9% card")

# ── cash: the emergency fund first, from the best-paying cash ──────
r5 = run("Chk, checking, 10000, 0%\nHY, savings, 3000, 4%")
res = [m_ for m_ in r5["moves"] if "emergency fund" in m_["from"]]
check(res and all(m_["to_kind"] == "tbill" for m_ in res) and near(sum(m_["sold"] for m_ in res), 6000),
      "THE EMERGENCY FUND STAYS CASH: six months of $1,000 may only move to T-bills, never to picks or property")
check(any(m_["from"] == "HY — emergency fund" and m_["sold"] == 3000 for m_ in res),
      "the fund is held first in the best-paying cash (savings at 4% before checking at 0%)")
debt_m = [m_ for m_ in r5["moves"] if m_["to_kind"] == "debt"]
check(len(debt_m) == 1 and debt_m[0]["sold"] == 4000 and debt_m[0]["from"] == "Chk",
      "free cash pays the 22.9% card — exactly its balance, no more")
check(any(m_["to_kind"] == "picks" and near(m_["sold"], 3000) for m_ in r5["moves"]),
      "and the rest goes to the next best use")

# ── lumpy real estate ──────────────────────────────────────────────
r6 = run("Chk, checking, 56000, 0%")
re_m = [m_ for m_ in r6["moves"] if m_["to_kind"] == "re"]
check(len(re_m) == 1 and near(re_m[0]["proceeds"], 40_000),
      "ONE DEAL, ALL AT ONCE: with $50k free after the fund, the card takes $4k and the house hack takes exactly its $40k")
r7 = run("Chk, checking, 30000, 0%")
check(not [m_ for m_ in r7["moves"] if m_["to_kind"] == "re"], "with less than the deal needs, no property is bought")
r8 = run("Chk, checking, 30000, 0%", winner={**HHROW, "ret_net": 20.0})
dp = [m_ for m_ in r8["moves"] if m_["to_kind"] == "dpfund"]
check(len(dp) == 1 and near(dp[0]["sold"], 30_000 - 6000 - 4000) and "Down-payment fund" in dp[0]["to"],
      "WHEN THE MONTH IS SAVING FOR A PROPERTY, free cash joins that down payment at the property's return")
r9 = run("Chk, checking, 56000, 0%", winner={**HHROW, "ret_net": 20.0})
check(not [m_ for m_ in r9["moves"] if m_["to_kind"] == "dpfund"],
      "and once the deal is bought outright, there is no down payment left to save for")
blocked = run("Chk, checking, 56000, 0%", rows=[DEBT, stock("AAA", 12.0, 9.0), TBILL, {**HHROW, "blocked": "over cap"}])
check(not [m_ for m_ in blocked["moves"] if m_["to_kind"] == "re"], "THE CAP HOLDS FOR HELD MONEY: a blocked property takes none")

# ── rentals: lazy equity moves when the tax allows ─────────────────
LAZY = {"name": "Paid-off", "use": "rental", "value": 500_000, "loan": 0, "rate": 0, "payment": 0, "rent": 2000,
        "shelter_rent": 0, "costs": 800, "basis": 480_000, "year": 2025, "hours": 2}
r10 = run("", owned=[LAZY], rows=[stock("AAA", 12.0, 9.0), TBILL])
lz = r10["items"][0]
check(r10["moves"] and r10["moves"][0]["from"] == "Paid-off" and near(r10["moves"][0]["tax"], lz["sale_tax"], 1)
      and r10["moves"][0]["to_kind"] == "picks",
      "A PAID-OFF RENTAL EARNING LITTLE ON ITS EQUITY, bought recently (little tax to sell), is redeployed")
r11 = run("", owned=[{**LAZY, "basis": 150_000, "year": 2000}], rows=[stock("AAA", 12.0, 9.0), TBILL])
check(not r11["moves"], "the same rental held 26 years, with its gain and recapture, stays")
r12 = run("", owned=[{**LAZY, "use": "home", "shelter_rent": 100}], rows=[stock("AAA", 12.0, 9.0), TBILL])
check(not r12["moves"], "the home you live in is never sold, however low its return")

# ── totals, bars ───────────────────────────────────────────────────
r = run("Chk, checking, 20000, 0%\nVTI, taxable, 50000, 40000\nStable, 401k, 20000, 3%\nMyst, cash, 500")
meas = [i for i in r["items"] if i["h"] is not None]
check(near(r["now_dollars"], sum(i["weq"] * i["value"] * i["h"] / 100 for i in meas), 0.05),
      "now: every measured holding at its return, in after-tax dollars")
check(near(r["gap_dollars"], sum(m_["weq"] * (m_["proceeds"] * m_["a"] - m_["sold"] * m_["h"]) / 100 for m_ in r["moves"]), 0.05),
      "the gap is exactly what the moves change")
check(near(r["one_time"], sum(m_["weq"] * (m_["tax"] + m_["cost"]) for m_ in r["moves"]), 0.01) and r["one_time"] > 0,
      "the one-time cost is the tax and trading costs of the moves")
check(near(sum(b["amount"] for b in r["bars_now"]), sum(b["amount"] for b in r["bars_opt"]), 1.0),
      "THE BARS ADD UP: what moves arrives somewhere or is paid in tax and costs")
check([u["label"] for u in r["unmeasured"]] == ["Myst"] and any(b["bucket"] == "cost" for b in r["bars_opt"]),
      "unmeasured money is listed, kept, and in the bars at its value")

# ── your pay vs the waterfall ──────────────────────────────────────
H = 5
check(near(A.contribution_return("k401", 7.0, P), K.after_tax_traditional(7.0, H, t["ordinary"], t["retire"])),
      "a 401(k) dollar: deducted now, taxed later")
P20 = {**P, "retire_rate": 20}
check(near(A.contribution_return("k401", 7.0, P20), K.after_tax_traditional(7.0, H, t["ordinary"], 0.20))
      and A.contribution_return("k401", 7.0, P20) > 7.5,
      "and more than the market when withdrawals will be taxed lower than today's deduction")
check(near(A.contribution_return("k401", 7.0, P, matched=True), ((2 * 1.07 ** 5) ** 0.2 - 1) * 100),
      "a matched dollar at a 100% match doubles before it grows (equal tax rates in and out)")
check(near(A.contribution_return("roth", 7.0, P), 7.0) and near(A.contribution_return("cash", 4.0, P, state_exempt=True), 3.04),
      "a Roth dollar grows untaxed; T-bill interest is taxed federally")
hsa = A.contribution_return("hsa", 7.0, P)
check(near(hsa, (((1 + 0.07 * 0.907) ** 5 / 0.76) ** 0.2 - 1) * 100),
      "an HSA dollar in California: deducted federally only, growth taxed by the state")
check(near(A.contribution_return("taxable", 7.0, P, div=1.2, pick=True, td=1.0, round_trip=0.2),
           K.after_tax_taxable(7.0, 1.2, 5, t["qualified"], t["qualified"]) - 0.04 - 1.0),
      "a taxable pick: after tax, less its round trip over the hold and the research time")

PP = K.profile_with_defaults({**BASE, "salary": 150_000, "match_pct": 4, "k401_room": 20_000, "monthly_invest": 4000,
                              "debts": [{"name": "Card", "balance": 4000, "apr": 22.9}], "picks_n": 2, "cash": 6000,
                              "current_monthly": "401k, 401k, 800\nRoth 401k, roth401k, 200\nCard, debt, 100\n"
                                                 "Car loan, debt, 100\nAlly, savings, 900, 4%\nStuff, taxable, 900"})
lines = A.measure_flows(A.parse_flows(PP["current_monthly"])[0], PP, {}, 1.0)
L = {l_["label"]: l_ for l_ in lines}
r_m = A.contribution_return("k401", 7.0, PP, matched=True, div=A.INDEX_DIV)
r_u = A.contribution_return("k401", 7.0, PP, div=A.INDEX_DIV)
check(near(L["401k"]["ret"], (500 * r_m + 300 * r_u) / 800, 0.01),
      "THE MATCH IS ON THE FIRST $500 (4% of $150k a month) — the rest of the line at the plain 401(k) return")
check(near(L["Roth 401k"]["ret"], A.contribution_return("roth401k", 7.0, PP), 0.01),
      "and a later 401(k) line gets no second match")
check(L["Card"]["ret"] == 22.9 and L["Car loan"]["ret"] is None and "APR" in L["Car loan"]["why_not"],
      "a debt line takes its APR from the debts box by name; an unknown debt without a % is not measured")

bp = K.build(PP, today=OCT, sources={k: [] for k in ("compounders", "aristocrats", "lynch", "quiet_value", "schloss",
                                                    "house_hack", "brrrr", "flip", "home")})
pay = bp["current"]["pay"]
check(pay["total"] == 3000 and near(sum(o["amount"] for o in pay["opt"]), 3000, 0.01),
      "THE WATERFALL SPLITS THE SAME AMOUNT as the owner's split ($3,000, not the profile's $4,000)")
check(pay["gap_dollars"] is None and pay["unmeasured"],
      "with a line unmeasured, no gap is claimed")
check(near(pay["now_dollars"], sum(l_["amount"] * l_["ret"] / 100 for l_ in pay["lines"] if l_["ret"] is not None), 0.01),
      "dollars are a year's return on ONE month's money — this month's waterfall has one-time steps")
steps = {o["label"]: o for o in pay["opt"]}
check(near(steps["401(k) up to the employer match"]["ret"],
           A.contribution_return("k401", 7.0, bp["profile"], matched=True), 0.01)
      and steps["Pay down Card"]["ret"] == 22.9,
      "each waterfall step at its own after-tax return: the match, the card's APR")
check([c["category"] for c in pay["cats"]][:3] == ["Cash & savings", "401(k)", "Debt paydown"],
      "side by side, by where the money goes")
ok = K.build({**PP, "current_monthly": "Card, debt, 3000"}, today=OCT,
             sources={k: [] for k in ("compounders", "aristocrats", "lynch", "quiet_value", "schloss",
                                      "house_hack", "brrrr", "flip", "home")})["current"]["pay"]
check(ok["gap_dollars"] is not None and near(ok["gap_dollars"], ok["opt_dollars"] - ok["now_dollars"], 0.01),
      "every line measured: the gap is claimed")

# ── the whole thing through build, and the page ────────────────────
SRC = {"compounders": [{"ticker": "LEN", "name": "Lennar", "status": "COMPOUNDER", "expected": 17.7, "er_div": 2.5}],
       "aristocrats": [], "lynch": [], "quiet_value": [], "schloss": [], "house_hack": [], "brrrr": [], "flip": [],
       "home": []}
FULL = {**BASE, "monthly_invest": 4000, "salary": 150_000, "match_pct": 4, "k401_room": 20_000,
        "debts": [{"name": "Card", "balance": 4000, "apr": 22.9}],
        "holdings": "Chk, checking, 40000, 0.01%\nVTI, taxable, 50000, 30000\nLEN, taxable, 5000, 5000\noops",
        "current_monthly": "Ally, savings, 3000, 4%\nVTI, taxable, 1000",
        "owned_re": [RENT]}
FB = K.build(FULL, today=OCT, sources=SRC)
cur = FB["current"]
check(not FB["errors"] and cur["has_any"] and cur["holdings"]["moves"] and cur["pay"],
      "build carries the comparison")
check(cur["re_cap"] == 60.0 and near(cur["net_worth"], 40000 + 55000 + 600_000 - 4000),
      "net worth and the cap at it are on the card")
import allocation  # noqa: E402
saved_cmp = allocation.compare
allocation.compare = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
try:
    FB2 = K.build(FULL, today=OCT, sources=SRC)
finally:
    allocation.compare = saved_cmp
check(FB2["current"] is None and FB2["errors"]["now vs optimal"].startswith("RuntimeError") and FB2["rows"],
      "A BROKEN COMPARISON IS NAMED, AND THE BOARD STANDS")

from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")), autoescape=True)
env.globals.update(is_admin=lambda r: True, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/capital"), query_params={})


import capital_view as V  # noqa: E402


def render(b):
    return env.get_template("capital.html").render(
        request=req, board=b, p=b["profile"], view=V.build_view(b, OCT), saved=True, updated_at="2026-10-03",
        last_step=None, debts_text=K.debts_text(b["profile"]["debts"]), limits=K.LIMITS_2026,
        hours_default=K.HOURS_DEFAULT, accounts=V.EDIT_ACCOUNT_LABELS)


html = render(FB)
check("Do next" in html and "What you hold" in html and "Your monthly pay" in html and "Left on the table" in html,
      "the comparison is on the page: the gap, the steps, the holdings, the pay")
check("Could not read 1 line" in html and "oops" in html, "an unreadable line is named on the page")
check("Duplex" in html and 'data-k="basis" value="500000' in html and 'id="cpPropTpl"' in html,
      "the property form shows the saved property, and a blank one to add")
check('data-editor="holdings"' in html and 'value="VTI"' in html and 'data-editor="flows"' in html
      and 'value="Ally"' in html and 'data-bad="holdings"' in html,
      "the holdings and the pay split are in the profile's editors, and an unreadable line stays where it can be fixed")
check('name="cash" value="40000.0" step="100" readonly' in html, "cash on hand is read-only once the cash lines set it")
for b_ in (">None<", "None%", "Undefined"):
    check(b_ not in html, f"no '{b_}' leaks into the page")
check("over cap" in render(K.build({**FULL, "owned_re": [RENT, {**RENT, "name": "Second"}]}, today=OCT,
                                   sources={**SRC, "house_hack": [{"zip": "94510", "place": "Benicia", "state": "CA",
                                                                    "max_offer": 700_000, "cash_to_close": 40_000,
                                                                    "monthly_surplus": 100}]})),
      "a property over the cap reads 'over cap' on the board")
blank = render(K.build({}, today=OCT, sources={k: [] for k in SRC}))
check("List what you hold" in blank and 'data-editor="holdings"' in blank, "with nothing listed, the page says how to start")

# ── saving ─────────────────────────────────────────────────────────
pp = K.parse_profile({"holdings": "VTI, taxable, 1\r\nnot a line", "current_monthly": "x" * 20_000,
                      "owned_re": [{"name": "A", "value": "100000"}, {"value": ""}], "re_cap_mid": "150"})
check(pp["holdings"] == "VTI, taxable, 1\nnot a line" and len(pp["current_monthly"]) == 10_000,
      "the boxes are saved as typed (an unreadable line stays where the owner can fix it), within a length limit")
check([r_["name"] for r_ in pp["owned_re"]] == ["A"] and pp["re_cap_mid"] == 100,
      "property is parsed on save; a cap is clamped to 100%")

import database  # noqa: E402
import main  # noqa: E402


class _Req(SimpleNamespace):
    async def json(self):
        return self.body


store = {"profile": {"cash": 1}}
saved_fns = (main._check_admin_token, database.get_capital_profile, database.save_capital_profile)
main._check_admin_token = lambda request: True
database.get_capital_profile = lambda owner="owner": dict(store["profile"])
database.save_capital_profile = lambda prof, owner="owner": store.update(profile=prof) or True
try:
    res = asyncio.run(main.capital_profile_save(_Req(body={"owned_re": [{"name": "B", "value": 5, "use": "home"}],
                                                           "holdings": "Chk, cash, 5, 1%"})))
    check(res.status_code == 200 and store["profile"]["owned_re"][0]["use"] == "home"
          and store["profile"]["holdings"] == "Chk, cash, 5, 1%" and store["profile"]["cash"] == 1,
          "the save endpoint stores property and holdings with the rest of the profile")
finally:
    main._check_admin_token, database.get_capital_profile, database.save_capital_profile = saved_fns

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} now-vs-optimal checks passed.")
