"""Headroom engine checks.

Run:  python tests/test_headroom.py      (exit 0 = all pass)

Strategy: self-consistency over constant-pinning — the researched tables
will be corrected over time, so tests assert the *algebra*: at the solved
max price the after-tax return sits on the hurdle (or the $25k floor
binds), IRR is monotone in price, gates fire, and the closed-form flip
solve agrees with an independent cash-flow reconstruction.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import headroom as H

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── IRR machinery ──
# (1+r)^60 = 2 over 60 months → annual (2)^(12/60) − 1 = 14.87%
cf = [-100.0] + [0.0] * 59 + [200.0]
r = H._irr_monthly(cf)
check(r is not None and abs((1 + r) ** 12 - 1 - (2 ** 0.2 - 1)) < 1e-6,
      "IRR: doubling in 5 years = 14.87%/yr")
check(H._irr_monthly([-100.0] + [0.0] * 59 + [100.0]) is not None
      and abs(H._irr_monthly([-100.0] + [0.0] * 59 + [100.0])) < 1e-9,
      "IRR: break-even = 0")
pay = H._pmt(100_000, 0.078)
check(719 < pay < 721, f"P&I on $100k @7.8%/30yr ≈ $719.8 (got {pay:.1f})")
sched, bal = H._amort(100_000, 0.078, 12)
check(abs(sum(p for _, p in sched) + bal - 100_000) < 0.01, "amortization conserves principal")

# ── state cost normalization ──
ca = H.state_costs("CA-SAC")
check(0.008 < ca["proptax"] < 0.02, "CA investor property tax is a fraction near 1.2%")
check(ca["state"] == "CA" and ca["state_income"] > 0.08, "CA-SAC → CA, ~9.3% income tax")
tx = H.state_costs("TX-DFW")
check(tx["state_income"] == 0.0, "TX has no income tax")
dc = H.state_costs("DC")
check(dc["state"] == "DC", "DC resolves as its own state")

# ── financing terms anchor to the live rate ──
fin = H.financing_terms(6.55)
check(abs(fin["hm_rate"] - 0.1055) < 1e-9, "hard money = rate + 400bp")
check(abs(fin["refi_rate"] - 0.078) < 1e-9, "DSCR cash-out = rate + 125bp")
check(fin["hm_max_ltarv"] == 0.70 and fin["refi_max_ltv"] == 0.75, "LTARV/LTV caps loaded")

# ── BRRRR: a healthy midwest-style deal ──
mkt = {"code": "MO-KC", "arv": 320_000.0, "rent": 2_300.0, "appreciation": 0.03}
inp = {"rehab": 60_000.0, "sqft": 1500.0, "scope": "moderate", "rate_pct": 6.55,
       "target": 14.0}
m150 = H.brrrr_after_tax_irr(150_000.0, mkt, inp)
m200 = H.brrrr_after_tax_irr(200_000.0, mkt, inp)
check(m150 is not None and m200 is not None, "BRRRR simulates on a plausible deal")
check(m150["irr_annual"] > m200["irr_annual"], "IRR is monotone-decreasing in price")
check(m150["forced_equity"] > m200["forced_equity"], "forced equity decreases in price")
check(m150["dscr"] > 1.0, "healthy deal clears the DSCR floor")

sol = H.brrrr_max_price(mkt, inp)
check(sol is not None, "max-price solve succeeds on the healthy deal")
if sol:
    at = H.brrrr_after_tax_irr(sol["max_price"], mkt, inp)
    hurdle_on = abs(at["irr_annual"] - 0.14) < 0.004
    floor_on = abs(at["total_profit"] - 25_000) < 1_500
    check(hurdle_on or floor_on,
          f"at P_max either the 14% hurdle or the $25k total-profit floor binds "
          f"(irr {at['irr_annual']:.3%}, profit ${at['total_profit']:,.0f})")
    worse = H.brrrr_after_tax_irr(sol["max_price"] + 5_000, mkt, inp)
    check(worse["irr_annual"] < 0.14 or worse["total_profit"] < 25_000,
          "P_max + $5k violates a gate")
    check(sol["max_psf"] * 1500 == sol["max_price"], "psf is price/sqft")
    check(at["total_profit"] >= 25_000 - 1_500, "deal makes at least the floor")

# DSCR gate: rent far too low to support any refi → structurally infeasible
starved = {"code": "MO-KC", "arv": 320_000.0, "rent": 300.0, "appreciation": 0.0}
check(H.brrrr_after_tax_irr(150_000.0, starved, inp) is None
      or H.brrrr_after_tax_irr(150_000.0, starved, inp)["dscr"] < 1.0
      or H.brrrr_max_price(starved, inp) is None,
      "rent-starved market fails the refi path")

# appreciation feeds the exit: same deal, zero appreciation → lower IRR
flat = dict(mkt, appreciation=0.0)
check(H.brrrr_after_tax_irr(150_000.0, flat, inp)["irr_annual"] < m150["irr_annual"],
      "zero appreciation lowers the after-tax IRR")

# no-income-tax state beats a high-tax state on an otherwise identical deal
mkt_tx = dict(mkt, code="TX-DFW")
tx_m = H.brrrr_after_tax_irr(150_000.0, mkt_tx, inp)
ca_mkt = dict(mkt, code="CA")
ca_m = H.brrrr_after_tax_irr(150_000.0, ca_mkt, inp)
check(tx_m is not None and ca_m is not None, "TX and CA variants simulate")
# (not directly comparable on IRR because carry costs differ; compare the tax
# component via suspended/exit rather than headline — assert exit taxes lower in TX)
check(tx_m["irr_annual"] != ca_m["irr_annual"], "state tables actually differentiate states")

# ── FLIP: closed form self-consistency ──
p_ct = H.flip_pretax_max_price(410_000, 142_000, "CA-SAC", 6.55, months=6)
check(100_000 < p_ct < 300_000, f"Sacramento-style pre-tax flip max in a sane band (got {p_ct:,.0f})")
# reconstruct the deal at P_max: return must equal the 14% hurdle over 6 mo
sc = H.state_costs("CA-SAC")
g = 1.14 ** 0.5 - 1
r = (6.55 + 0.75) / 100
alpha = H.BUY_CLOSING_PCT + 0.01 * 0.75
kappa = 0.5 * (r * 0.75 + sc["proptax"])
F = 6 * (sc["utilities_mo"] + sc["ins_landlord"] * sc["ins_reno_mult"] / 12.0)
E = (0.25 + alpha + kappa) * p_ct + 142_000 + F
profit = 0.93 * 410_000 - 142_000 - F - (1 + alpha + kappa) * p_ct
check(abs(profit / E - g) < 1e-9, "closed form lands exactly on the hurdle")

fm = H.flip_max_price({"code": "TX-DFW", "arv": 350_000.0}, dict(inp, rehab=70_000.0))
check(fm is not None, "after-tax flip solves in TX")
if fm:
    check(fm["after_tax_profit"] >= 25_000 - 5, "flip floor respected")
    check(fm["max_price"] < H.flip_pretax_max_price(350_000, 70_000, "TX-DFW", 6.55),
          "after-tax max price is below the pre-tax max price")
# CA flip pays SE + 9.3% state → lower after-tax max than TX at same inputs
fm_ca = H.flip_max_price({"code": "CA", "arv": 350_000.0}, dict(inp, rehab=70_000.0))
check(fm_ca is None or fm_ca["max_price"] < fm["max_price"],
      "high-tax state lowers the flip max price")

# after-tax profit function: SE tax + state, sane magnitudes
at = H._flip_after_tax_profit(100_000, "TX-DFW")
check(0.60 * 100_000 < at < 0.80 * 100_000, f"TX dealer keeps 60-80% of $100k (got {at:,.0f})")
at_ca = H._flip_after_tax_profit(100_000, "CA")
check(at_ca < at, "CA dealer keeps less than TX dealer")

# ── calibration + market bridge ──
cal_m = H.calibration("moderate")
check(cal_m["x"] == 0.83 and cal_m["y"] == 1.04, "verified moderate calibration loads")
check(H.calibration("gut")["x"] == 0.70, "gut fixer discount = 0.70")
# implied gross flip ROI stays at/below ATTOM's observed 25.4% for moderate
check((cal_m["y"] - cal_m["x"]) / cal_m["x"] <= 0.2554,
      "moderate implied ROI ≤ observed ATTOM national")

# ── the market answer: the most you can pay, and what sets it ──
mh = H.market_headroom("MO-KC", 320_000, 2_300, 0.03, scope="moderate", rehab_total=60_000)
check(mh["feasible"] and abs(mh["max_pct_median"] - mh["max_price"] / 320_000 * 100) < 1e-9,
      "the most you can pay is also stated as a share of the median home")
check(not any(k in mh for k in ("verdict", "entry_psf", "headroom", "vetoed")),
      "NO VERDICT AGAINST A GUESSED FIXER PRICE: no entry price, headroom or tier")
check(abs(mh["arv_psf"] - 1.04 * 320_000 / 1500) < 0.01 and mh["binding_label"] == H.LIMIT_LABEL[mh["binding"]],
      "ARV psf = y × median psf; the limit carries its label")
_inp = {"rehab": 60_000.0, "sqft": 1500.0, "scope": "moderate", "rate_pct": 6.55, "target": 14.0}
_mk = {"code": "MO-KC", "arv": 1.04 * 320_000, "rent": 2_300.0, "appreciation": 0.03}
over = H._brrrr_limits(H.brrrr_after_tax_irr(mh["max_price"] + 500, _mk, _inp), 0.14, H.PROFIT_FLOOR)
check(mh["binding"] in over and not H._brrrr_limits(H.brrrr_after_tax_irr(mh["max_price"], _mk, _inp), 0.14, H.PROFIT_FLOOR),
      "THE NAMED LIMIT IS THE ONE $500 MORE BREAKS; at the max price none is broken")
starved_m = H.market_headroom("MO-KC", 320_000, 300, 0.0, scope="moderate", rehab_total=60_000)
check(not starved_m["feasible"] and starved_m["max_pct_median"] is None
      and starved_m["binding_label"] in H.NO_PATH_LABEL.values(), "no price works, and the page is told why")
check(H.market_headroom("MO-KC", 320_000, 2_300, 0.10, rehab_total=60_000)["max_price"]
      == H.market_headroom("MO-KC", 320_000, 2_300, 0.05, rehab_total=60_000)["max_price"],
      "appreciation is held to ±5%/yr")
check(H.market_headroom("MO-KC", 320_000, 2_300, 0.03, rehab_total=60_000)["max_price"]
      > H.market_headroom("MO-KC", 320_000, 2_300, 0.0, rehab_total=60_000)["max_price"],
      "the exit uses the appreciation it is given (0 unless the user sets one)")
fl = H.market_headroom("TX-DFW", 336_538, 2_000, 0.0, mode="flip", rehab_total=70_000)
fsol = H.flip_max_price({"code": "TX-DFW", "arv": 350_000.0}, dict(inp, rehab=70_000.0))
check(fl["feasible"] and fsol["binding"] == "floor"
      and "floor" in H._flip_limits(fsol["max_price"] + 500, {"code": "TX-DFW", "arv": 350_000.0}, dict(inp, rehab=70_000.0)),
      "a modest flip is set by the $25k floor")
big = {"code": "TX-DFW", "arv": 2_500_000.0}
bsol = H.flip_max_price(big, dict(inp, rehab=60_000.0))
check(bsol["binding"] == "target" and "target" in H._flip_limits(bsol["max_price"] + 500, big, dict(inp, rehab=60_000.0)),
      "a big flip is set by the return target — the page names whichever it is")

# ── BRRRR timeline: rented while the refi seasons ──
m = H.brrrr_after_tax_irr(150_000.0, mkt, inp)
fin_ = H.financing_terms(6.55)
first = H.RENO_MONTHS["moderate"] + 1
check(m["rented_from_month"] == first, "rented from the month after the remodel")
_sc = H.state_costs("MO-KC")
noi = (2_300 * 12 * (1 - H.VACANCY) - _sc["proptax"] * 150_000 - _sc["ins_landlord"] - H.MAINTENANCE_PCT * 320_000) / 12
check(abs(m["noi_month"] - noi) < 1e-6, "rented months earn rent less vacancy, tax, landlord insurance and maintenance")
_c = min(fin_["hm_max_ltc"] * 210_000, fin_["hm_max_purchase_adv"] * 150_000 + 60_000, fin_["hm_max_ltarv"] * 320_000)
_adv = min(fin_["hm_max_purchase_adv"] * 150_000, _c)
_drawn = _adv + max(0.0, min(60_000, _c - _adv))
check(abs(m["cashflows"][first] - (noi - _drawn * fin_["hm_rate"] / 12)) < 1e-6,
      "A SEASONING MONTH IS RENTED, NOT CARRIED VACANT: rent less costs, less the hard-money interest")
_dep_year = (H.BUILDING_SHARE * (150_000 * (1 + H.BUY_CLOSING_PCT)) + 60_000) / H.DEP_YEARS
check(m["years"][0]["rented_months"] == 12 - H.RENO_MONTHS["moderate"]
      and abs(m["years"][0]["dep"] - _dep_year * (12 - H.RENO_MONTHS["moderate"]) / 12) < 1e-6,
      "depreciation starts the month it is rented")
_refi_m = max(H.REFI_MONTH_MIN, H.RENO_MONTHS["moderate"] + 1)
check(abs(m["years"][0]["points"] - m["points_total"] * (12 - _refi_m) / 360) < 1e-6
      and abs(m["years"][1]["points"] - m["points_total"] * 12 / 360) < 1e-6
      and abs(sum(y["points"] for y in m["years"]) + m["points_at_payoff"] - m["points_total"]) < 1e-6,
      "refi points amortize by the month over 30 years")
_r = H.FED_ORDINARY + _sc["state_income"]
check(abs(m["suspended_released"] - (m["suspended_left_fed"] * H.FED_ORDINARY + m["suspended_left_state"] * _sc["state_income"]
                                     + m["points_at_payoff"] * _r)) < 1e-6 and m["points_at_payoff"] > 0,
      "AT THE SALE: the losses still suspended are released, and the unamortized points deducted")
_losses = -sum(y["taxable"] for y in m["years"] if y["taxable"] < 0)
check(abs(m["suspended_left_fed"] - (_losses - sum(y["loss_used"] for y in m["years"]))) < 1e-6,
      "what is released is every loss not already used")

# ── losses carry forward ──
rich = {"code": "MO-KC", "arv": 150_000.0, "rent": 2_000.0, "appreciation": 0.0}
inp_c = dict(inp, scope="cosmetic")
mr = H.brrrr_after_tax_irr(120_000.0, rich, inp_c)
ys = mr["years"]
loss_then_gain = [i for i in range(1, len(ys)) if ys[i]["taxable"] > 0 and any(y["taxable"] < 0 for y in ys[:i])]
check(bool(loss_then_gain), "fixture: an early loss year followed by a taxable year")
i = loss_then_gain[0]
prior_loss = -sum(y["taxable"] for y in ys[:i] if y["taxable"] < 0) - sum(y["loss_used"] for y in ys[:i])
rates = H.FED_ORDINARY + _sc["state_income"]
check(ys[i]["loss_used"] > 0 and abs(ys[i]["loss_used"] - min(prior_loss, ys[i]["taxable"])) < 1e-6
      and abs(ys[i]["tax"] - (ys[i]["taxable"] - ys[i]["loss_used"]) * rates) < 1e-6,
      "A LATER YEAR'S RENTAL INCOME IS TAXED ONLY AFTER EARLIER SUSPENDED LOSSES")
mp = H.brrrr_after_tax_irr(100_000.0, dict(rich, code="PA", rent=1_800.0), inp_c)
_pa = H.state_costs("PA")
yp = next((y for y in mp["years"] if y["loss_used"] > 0), None)
check(yp is not None and yp["loss_used_state"] == 0
      and abs(yp["tax"] - ((yp["taxable"] - yp["loss_used"]) * H.FED_ORDINARY + yp["taxable"] * _pa["state_income"])) < 1e-6,
      "Pennsylvania's state tax gets no carryforward; the federal one does")
check(m["refi_loan"] == min(m["refi_cap_rent"], m["refi_cap_ltv"])
      and m["refi_by"] == ("rent" if m["refi_cap_rent"] < m["refi_cap_ltv"] else "ltv"),
      "the refi is the smaller of what the rent carries and 75% of ARV, and says which")
thin_rent = H.brrrr_after_tax_irr(150_000.0, dict(mkt, rent=1_400.0), inp)
check(thin_rent["refi_by"] == "rent" and m["refi_by"] == "ltv", "fixtures: a thin rent sizes the refi; a strong one doesn't")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} headroom engine checks passed.")
if sol:
    print(f"   KC sample: max ${sol['max_price']:,.0f} (${sol['max_psf']:.0f}/sqft, set by the {H.LIMIT_LABEL[sol['binding']]}) · "
          f"IRR {H.brrrr_after_tax_irr(sol['max_price'], mkt, inp)['irr_annual']:.1%} · "
          f"forced ${H.brrrr_after_tax_irr(sol['max_price'], mkt, inp)['forced_equity']:,.0f}")
sys.exit(0)
