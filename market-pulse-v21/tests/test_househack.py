"""House-hack underwriting checks for /multifamily.

Run:  python tests/test_househack.py      (exit 0 = all pass)

The properties under test are the ones that decide whether a user buys a
building. Two of them are safety-critical in the money sense:

  * FHA self-sufficiency on 3-4 units is a HARD funding gate — a deal
    that fails it must never come back with an encouraging verdict, no
    matter how good the monthly spread looks.
  * An IMPUTED rent (value/17/12) must be detectable, because the cap
    rate built on one is 5.88% by construction and says nothing about
    income. Silently treating it as observed is how a board ends up
    ranking arithmetic.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import househack as H

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── imputed vs observed rent detection ──────────────────────────────
# The placeholder is exactly value / 17 / 12, so it round-trips to 1.0.
hv = 135_029
check(not H.rent_is_observed(hv / 17 / 12, hv),
      "the exact value/17/12 placeholder is detected as imputed")
check(not H.rent_is_observed(hv / 17 / 12 * 1.01, hv),
      "1% off the placeholder is still within float/rounding noise -> imputed")
check(H.rent_is_observed(1_450, hv),
      "a rent well off the 5.88% construction line reads as observed")
check(not H.rent_is_observed(None, hv) and not H.rent_is_observed(1200, None),
      "missing inputs are never claimed as observed (fail closed)")
# The whole point: an imputed rent forces the cap rate to one number.
imputed_cap = (hv / 17 / 12) * 12 / hv * 100
check(abs(imputed_cap - 5.88) < 0.01,
      "imputed rents make cap rate 5.88% by construction")

# ── building price is an adjustment, and says so ────────────────────
b = H.est_building_price(135_029, 3)
check(b["estimated_price"] == round(135_029 * 1.55), "3-unit uses the 1.55 factor")
check(b["estimated_price"] > b["sfr_median"], "a triplex costs more than the SFR median")
check("estimate" in b["basis"].lower(), "the price basis labels itself an estimate")
check(H.est_building_price(0, 3) is None and H.est_building_price(None, 2) is None,
      "no home value -> no invented price")
check(H.UNIT_PRICE_FACTOR[2] < H.UNIT_PRICE_FACTOR[3] < H.UNIT_PRICE_FACTOR[4],
      "price factors rise monotonically with unit count")
check(H.UNIT_INSURANCE_FACTOR[2] < H.UNIT_INSURANCE_FACTOR[4],
      "insurance uplift rises with unit count")

# ── vs-renting: zero is not the benchmark ───────────────────────────
# Screenshot case: $209k triplex, PITI $2,871, rents $1,193/unit.
v = H.vs_renting(2871, 3, 1193)
check(v["net_cost"] == 485, f"net cost is PITI minus 2 rents (got {v['net_cost']})")
check(v["monthly_advantage"] == 708,
      f"paying 485 to live where rent is 1193 is a +708 win (got {v['monthly_advantage']})")
check(v["beats_renting"] and not v["live_free"],
      "beats renting without living free — both flags are distinct")
check(v["annual_advantage"] == 708 * 12, "annual advantage is 12x monthly")
# A deal that costs more than renting must not read as a win.
bad = H.vs_renting(4000, 2, 900)
check(not bad["beats_renting"] and bad["monthly_advantage"] < 0,
      "a deal costing more than local rent is reported negative, not spun")
# Full cash flow.
free = H.vs_renting(1000, 4, 800)
check(free["live_free"] and free["net_cost"] < 0,
      "rents exceeding PITI is live-free + cash flow")

# ── FHA self-sufficiency: the hard gate ─────────────────────────────
ss = H.fha_self_sufficiency(2871, 3, 1193)
check(ss is not None and not ss["passes"], "the screenshot triplex FAILS self-sufficiency")
check(ss["required_rent_per_unit"] == 1276,
      f"needs $1,276/unit to fund (got {ss['required_rent_per_unit']})")
check(ss["gap_per_unit"] == 83, f"short $83/unit (got {ss['gap_per_unit']})")
check(H.fha_self_sufficiency(2871, 2, 1193) is None,
      "duplexes are exempt from the self-sufficiency test")
check(H.fha_self_sufficiency(2000, 4, 800)["passes"],
      "75% x 4 x 800 = 2400 >= 2000 passes")

# ── income to qualify: the gate the board never showed ──────────────
# FHA 4000.1: rental income is added to EFFECTIVE INCOME, and the full
# PITI stays in the ratio. Netting rent out of the payment instead --
# the intuitive version -- understates the required income by a third,
# which is the direction that gets someone to an application they fail.
q = H.income_to_qualify(2871, 3, 1193)
naive = (2871 - 0.75 * 2 * 1193) / 0.45 * 12      # the wrong way
check(q["annual_income"] > naive * 1.3,
      f"required income is far above the netted-out figure "
      f"(${q['annual_income']:,} vs naive ${naive:,.0f})")
check(q["annual_income"] == 81_540,
      f"$81,540/yr at 45% DTI (got {q['annual_income']})")
# On a 3-4 unit, a self-sufficiency shortfall becomes a LIABILITY.
check(q["rent_liability"] == 187 and q["rent_income"] == 0,
      "negative net self-sufficiency rental income counts against you")
check(q["qualifying_housing"] == 2871 + 187,
      "the liability is added to the numerator, not deducted from PITI")
# A duplex uses plain 75% of the one rented unit, added to income.
dq = H.income_to_qualify(2600, 2, 900)
check(dq["rent_income"] == round(0.75 * 900) and dq["rent_liability"] == 0,
      "2-unit rental income is 75% of the non-occupied unit's rent")
check(dq["annual_income"] == 61_233,
      f"duplex needs $61,233/yr (got {dq['annual_income']})")
# A self-sufficient triplex turns the same figure into added income.
pq = H.income_to_qualify(1500, 3, 1200)
check(pq["rent_income"] == 1200 and pq["rent_liability"] == 0,
      "a passing 3-unit adds net self-sufficiency income to the denominator")
check(pq["annual_income"] < q["annual_income"],
      "a building that funds needs less income than one that doesn't")
check(H.income_to_qualify(2871, 3, 1193, other_debts=500)["annual_income"]
      > q["annual_income"], "car and card payments raise the income needed")
check(H.income_to_qualify(500, 4, 5000)["monthly_income"] == 0,
      "rental income far exceeding PITI floors required income at zero, never negative")

# ── reserves: where deals actually die ──────────────────────────────
r = H.reserves_needed(2871, 6_000, 7_325)
check(r["cash_to_close"] == 13_325, "cash to close is down + closing")
check(r["piti_reserve"] == 3 * 2871, "three months PITI, per FHA on 3-4 units")
check(r["all_in_cash"] == r["cash_to_close"] + r["reserve_total"],
      "all-in cash is close + reserve, no double count")
check(r["reserve_total"] > r["piti_reserve"],
      "a mechanical reserve sits on top of the PITI reserve")

# ── deal checker: composite verdict ─────────────────────────────────
d = H.check_deal(2871, 3, 1193, 209_295, insurance_monthly=180,
                 closing=6_000, down=7_325)
check(d["verdict"] == "no",
      f"a self-sufficiency failure sinks the verdict (got {d['verdict']})")
ssc = [c for c in d["checks"] if c["key"] == "self_suff"][0]
check(ssc["hard"] and ssc["status"] == "fail",
      "self-sufficiency is marked a HARD gate and fails here")
check("1,276" in ssc["detail"], "the failure says what rent WOULD fund it")
# The critical anti-spin property: it beats renting AND still says no.
check(d["vs_rent"]["beats_renting"] and d["verdict"] == "no",
      "a good monthly spread cannot override a funding gate")

# A clean deal must come back yes.
ok = H.check_deal(1500, 3, 1200, 180_000, insurance_monthly=150,
                  closing=5_000, down=6_300)
check(ok["verdict"] == "yes", f"a funding, qualifying, rent-beating deal is yes (got {ok['verdict']})")
check(all(c["status"] != "fail" for c in ok["checks"]), "no failed checks on a clean deal")

# Duplex path: exempt from self-sufficiency, so nothing hard can fail
# on the rent side, but it must still be honest about vs-renting.
dup = H.check_deal(2600, 2, 900, 240_000, insurance_monthly=140)
sd = [c for c in dup["checks"] if c["key"] == "self_suff"][0]
check(sd["status"] == "na" and not sd["hard"], "duplex self-sufficiency check is n/a, not a pass")
check(not dup["vs_rent"]["beats_renting"] and dup["verdict"] in ("marginal", "no"),
      "a duplex costing $1,700 net against $900 rent is not sold as fine")

# Income entered -> becomes a hard pass/fail rather than an FYI.
poor = H.check_deal(1500, 3, 1200, 180_000, your_income=10_000)
ic = [c for c in poor["checks"] if c["key"] == "income"][0]
check(ic["hard"] and ic["status"] == "fail" and poor["verdict"] == "no",
      "an income too small to qualify is a hard fail")
check("W-2" in ic["detail"], "the income check says it means W-2 income")
rich = H.check_deal(1500, 3, 1200, 180_000, your_income=250_000)
ic2 = [c for c in rich["checks"] if c["key"] == "income"][0]
check(ic2["status"] == "pass", "ample income passes the DTI check")
# And when the building itself doesn't fund, the income line must show
# the liability rather than an encouraging credit.
lia = H.check_deal(2871, 3, 1193, 209_295, your_income=200_000)
il = [c for c in lia["checks"] if c["key"] == "income"][0]
check("liability" in il["detail"],
      "a negative-rental-income building says so on the income line")

# The stress case must be strictly worse than the base case.
check(d["stressed"]["net_cost"] > d["vs_rent"]["net_cost"],
      "stressed net is worse than base net (rents down, insurance up, one vacancy)")

# 1% rule is reported, never a gate.
op = [c for c in d["checks"] if c["key"] == "one_pct"][0]
check(not op["hard"], "the 1% rule is context, not a gate")
check("heuristic" in op["detail"], "the 1% rule labels itself a heuristic")

# Every check carries the fields the template renders.
for c in d["checks"]:
    check({"key", "hard", "label", "status", "detail"} <= set(c),
          f"check {c.get('key')} has all render fields")
    check(c["status"] in ("pass", "fail", "warn", "info", "na"),
          f"check {c.get('key')} has a known status ({c.get('status')})")

# ── degenerate inputs must not raise ────────────────────────────────
for args in ((0, 2, 0, 0), (1e9, 4, 0, 1), (100, 2, 1e6, 100)):
    try:
        H.check_deal(*args)
    except Exception as e:                                # noqa: BLE001
        _FAILS.append(f"check_deal{args} raised {type(e).__name__}: {e}")
    _COUNT += 1

# ── the 1% rule must not secretly gate the verdict ────────────────
# It was "context only" in the docstring and a warn in the code, and a
# warn downgrades to marginal — so any listing under ~1% rent-to-price,
# which is most real listings at today's rates, could never come back
# WORKS however cleanly it funded.
clean = H.check_deal(2200, 4, 1200, 600_000, insurance_monthly=200,
                     closing=18_000, down=21_000, your_income=200_000)
op2 = [c for c in clean["checks"] if c["key"] == "one_pct"][0]
check(not clean["one_pct"]["passes"], "this fixture genuinely misses the 1% line")
check(op2["status"] == "info", "a 1% miss is INFO, never a warn")
check(clean["verdict"] == "yes",
      f"missing 1% cannot downgrade a deal that funds, qualifies and beats rent "
      f"(got {clean['verdict']})")
check("does not affect the verdict" in op2["detail"],
      "the 1% line says outright that it doesn't affect the verdict")

# ── reserves: state the rule that actually applies ─────────────────
r2 = H.reserves_needed(2000, 5_000, 7_000, 2)
r3 = H.reserves_needed(2000, 5_000, 7_000, 3)
check(not r2["reserve_required"] and r3["reserve_required"],
      "the FHA reserve requirement is flagged only on 3-4 units")
check("recommendation" in r2["reserve_basis"],
      "on a duplex the 3-month reserve is labelled our recommendation, not FHA's rule")
check("FHA requires" in r3["reserve_basis"],
      "on 3-4 units it is labelled as FHA's requirement")
check(r2["reserve_total"] == r3["reserve_total"],
      "we hold the same reserve either way — only the justification differs")

# ── one insurance basis for the whole page ─────────────────────────
# The table, the scenario card and the deal checker priced the same
# building three different ways; a ZIP could show a passing FHA badge in
# the table and a failing one in the card.
base = {"monthly_ins": 100.0, "piti": 1000.0}
out = H.apply_unit_insurance(dict(base), 3)
check(out["monthly_ins"] == 145.0, "3-unit insurance carries the 1.45x uplift")
check(out["piti"] == 1045.0, "the uplift flows through to PITI, not just the line item")
check(out["insurance_factor"] == 1.45, "the factor is exposed so the UI can show it")
check(H.apply_unit_insurance(None, 3) is None, "a missing PITI stays missing")
check(H.apply_unit_insurance(dict(base), 4)["piti"]
      > H.apply_unit_insurance(dict(base), 2)["piti"],
      "more units, more insurance")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} house-hack checks passed.")
print(f"   screenshot triplex: net ${v['net_cost']}/mo vs ${v['rent_equivalent']} rent "
      f"(+${v['monthly_advantage']}/mo), but FHA needs ${ss['required_rent_per_unit']}/unit → {d['verdict'].upper()}")
sys.exit(0)
