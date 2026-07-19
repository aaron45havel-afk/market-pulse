#!/usr/bin/env python3
"""Engine tests for the household finance tool.

Pure-function coverage of household.py — no database, no network — so a
future change that quietly breaks her numbers fails loudly here instead.

Run:  python tests/test_household.py      (exit 0 = all pass)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import household as H  # noqa: E402


# ── tiny test harness (no pytest dependency) ───────────────────────
_FAILS = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def eq(got, want, msg):
    check(got == want, f"{msg}: got {got!r}, want {want!r}")


def approx(got, want, msg, tol=0.5):
    check(abs(got - want) <= tol, f"{msg}: got {got!r}, want ~{want!r}")


def txn(date, amount, bucket, project_id=None, desc=""):
    """A ledger row shaped like household_all_txns rows, with cls derived."""
    return {"date": date, "amount": amount, "bucket": bucket,
            "cls": H.bucket_class(bucket), "project_id": project_id,
            "desc": desc or bucket}


# ── categorization ─────────────────────────────────────────────────
def test_categorize():
    cases = {
        # her real statement's trouble spots
        "ACH-A- LOANDEPOT (MSP ACH)": "Mortgage",
        "THE HOME DEPOT # SAN LEANDRO": "Home Improvement",
        "CREATIVE PAINT #8": "Home Improvement",
        "ZELLE MONEY TRANSFER 04JUN ZELLE W/D - TO ROSALES,FE": "Uncategorized",
        'G1 Online Transfer "DTL" 669.45 to loan 1': "Transfer",
        "G1 Online LV2 Transfer 750.00 to credit card tracking": "Transfer",
        "Interest Charge on Cash Advances": "Fees & Interest",
        # common merchants stay put
        "AMAZON MKTPL*OF8": "Shopping",
        "TARGET T-1234": "Household Goods",
        "SAFEWAY #123": "Groceries",
        "COSTCO WHSE #0441": "Groceries",       # warehouse -> essential groceries
        "COSTCO GAS #0441 SAN LEANDRO": "Gas & Fuel",  # but the gas station is fuel
        "CHEVRON 12345": "Gas & Fuel",
        "GEICO ONLINE PMT": "Insurance",
        "PAYROLL ST OF CA": "Income",
    }
    for desc, want in cases.items():
        eq(H.categorize(desc), want, f"categorize({desc[:28]!r})")
    # a learned rule wins over the built-ins
    eq(H.categorize("MYSTERY LLC", {"mystery": "Groceries"}), "Groceries",
       "learned rule overrides")


def test_bucket_class():
    eq(H.bucket_class("Mortgage"), "fixed", "mortgage is fixed")
    eq(H.bucket_class("Transfer"), "transfer", "transfer class")
    eq(H.bucket_class("Home Improvement"), "variable", "home improvement variable")
    eq(H.bucket_class("Uncategorized"), "review", "uncategorized is review")


# ── labor payees ───────────────────────────────────────────────────
def test_payees():
    eq(H.payee_fragment("ZELLE W/D - TO ROSALES,FE"), "rosales", "zelle surname")
    eq(H.payee_fragment("ZELLE MONEY TRANSFER - TO .,ARNULFO"), "arnulfo", "zelle punct name")
    check(not H.is_p2p("THE HOME DEPOT"), "home depot not p2p")
    check(H.is_p2p("ZELLE MONEY TRANSFER"), "zelle is p2p")
    # whole-word matching — the P1 fix
    check(H.payee_matches("ZELLE W/D - TO LEE,JO", "lee"), "lee matches TO LEE")
    check(not H.payee_matches("SLEEP NUMBER STORE", "lee"), "lee does NOT match SLEEP")
    check(not H.payee_matches("CANADA GOOSE", "ada"), "ada does NOT match CANADA")
    check(not H.payee_matches("anything at all", "wu"), "<3 char payee ignored")


def test_suggest_reno_and_candidates():
    rows = [
        txn("2026-06-04", -5000, "Uncategorized", desc="ZELLE W/D - TO ROSALES,FE"),
        txn("2026-06-19", -5000, "Uncategorized", desc="ZELLE W/D - TO ROSALES,FE"),
        txn("2026-06-15", -350, "Uncategorized", desc="ZELLE W/D - TO .,ARNULFO"),
        txn("2026-06-10", -73, "Home Improvement", desc="THE HOME DEPOT # SAN LEANDRO"),
        txn("2026-06-20", -100, "Uncategorized", desc="ZELLE W/D - TO GRANDSON,JO"),
    ]
    # vendors only when no payees learned
    ids = [t["desc"] for t in H.suggest_reno(rows, "2026-06-01", "2026-06-30")]
    eq(len(ids), 1, "only home depot suggested without payees")
    # with a learned payee, both Rosales + the vendor
    got = H.suggest_reno(rows, "2026-06-01", "2026-06-30", ["rosales"])
    eq(len(got), 3, "rosales x2 + home depot")
    # labor candidates: the unrecognized zelles (arnulfo, grandson), not rosales/vendor
    cands = H.labor_candidates(rows, "2026-06-01", "2026-06-30", ["rosales"])
    eq(sorted(c["payee"] for c in cands), ["arnulfo", "grandson"], "labor candidates")


# ── monthly figures / normalization ────────────────────────────────
def test_monthly_and_essential():
    rows = [
        txn("2026-06-01", 12000, "Income"),
        txn("2026-06-02", -2664, "Mortgage"),
        txn("2026-06-03", -200, "Utilities"),
        txn("2026-06-04", -500, "Groceries"),
        txn("2026-06-05", -400, "Dining"),                 # discretionary
        txn("2026-06-06", -5000, "Home Improvement"),      # one-time reno
        txn("2026-06-07", -5000, "Uncategorized", project_id=1),  # reno-tagged
        txn("2026-06-08", -669, "Transfer"),               # excluded entirely
    ]
    m = H.monthly_figures(rows)
    # total spend includes dining + reno; essential excludes both
    approx(m["spend"], 2664 + 200 + 500 + 400 + 5000 + 5000, "total spend includes reno")
    approx(m["essential"], 2664 + 200 + 500, "essential = mortgage+utils+groceries")
    approx(m["spend_recurring"], 2664 + 200 + 500 + 400, "recurring excludes reno")


# ── vital signs: savings source + essential cushion ────────────────
def test_vital_signs_savings():
    rows = [txn("2026-06-02", -2664, "Mortgage"), txn("2026-06-03", -500, "Groceries"),
            txn("2026-06-01", 12000, "Income")]
    # manual fallback (no accounts)
    vs = H.vital_signs(rows, {"savings": 1500})
    eq(vs["savings_source"], "manual", "manual when no accounts")
    eq(vs["savings"], 1500, "manual savings value")
    # derived from liquid balances, manual ignored
    accts = [{"kind": "checking", "balance": 3800, "name": "Checking"},
             {"kind": "savings", "balance": 157.43, "name": "Savings"},
             {"kind": "credit_card", "balance": 7454, "name": "Card"}]
    vs = H.vital_signs(rows, {"savings": 1500}, accts)
    eq(vs["savings_source"], "accounts", "accounts source when balances present")
    approx(vs["savings"], 3957.43, "savings = checking + savings only (card excluded)")
    # savings_extra stacks on top
    vs = H.vital_signs(rows, {"savings_extra": 20000}, accts)
    approx(vs["savings"], 23957.43, "savings_extra adds")
    # cushion measured against essential bills, not total spend
    approx(vs["cushion_base"], vs["essential"], "cushion base is essential")


def test_vital_signs_reno_not_overspend():
    rows = [
        txn("2026-06-01", 8000, "Income"),
        txn("2026-06-02", -3000, "Mortgage"),
        txn("2026-06-03", -10000, "Home Improvement"),   # HELOC-funded remodel
    ]
    vs = H.vital_signs(rows, {})
    # living-within-means should NOT count the one-time reno as overspending
    check(vs["avg_net"] >= 0, f"reno excluded from avg_net (got {vs['avg_net']})")


# ── roadmap ────────────────────────────────────────────────────────
def _vitals(**over):
    base = {"spend": 7000, "essential": 4600, "spend_recurring": 6000,
            "savings": 0, "avg_net": 1500, "card_balance": 7454, "card_apr": 18.24,
            "heloc_balance": 117648, "heloc_apr": 7.12}
    base.update(over)
    return base


def test_roadmap_order_and_status():
    reno = {"active": True, "budget_total": 72000, "can_fund": 72000}
    retire = {"configured": True, "year": 2027, "covered": True, "surplus": 2335}
    r = H.money_roadmap(_vitals(), reno, retire)
    keys = [s["key"] for s in r["steps"]]
    eq(keys, ["cover", "starter", "card", "safety", "kitchen", "heloc", "retire"],
       "roadmap milestone order")
    byk = {s["key"]: s for s in r["steps"]}
    eq(byk["cover"]["status"], "done", "cover done (net positive)")
    eq(byk["starter"]["status"], "now", "starter is the current step")
    eq(byk["card"]["status"], "later", "card later")
    eq(byk["kitchen"]["status"], "goal", "kitchen is a parallel goal")
    eq(byk["retire"]["status"], "done", "retirement covered")
    # safety net is sized to ESSENTIAL bills, not total spend
    approx(byk["safety"]["progress"] or 0, 0, "no savings yet -> 0 progress")


def test_roadmap_safety_target_essential():
    # essential ($4,600) drives the 3-month target, not total spend ($16k)
    r = H.money_roadmap(_vitals(spend=16000, essential=4600, savings=5000), {}, {})
    safety = next(s for s in r["steps"] if s["key"] == "safety")
    check("13,800" in safety["metric"] or "13,80" in safety["metric"],
          f"safety target ~3x essential, got {safety['metric']!r}")


def test_roadmap_current_advances():
    # card cleared -> current focus moves to the safety net
    r = H.money_roadmap(_vitals(savings=3000, card_balance=0), {}, {})
    eq(r["current_key"], "safety", "focus advances past a cleared card")


# ── retirement ─────────────────────────────────────────────────────
def test_retirement_plan():
    settings = {"retirement": H.retirement_seed(),
                "heloc_payment": 669, "card_payment": 250}
    p = H.retirement_plan(settings)
    check(p["configured"], "seeded plan is configured")
    # need = living + mortgage(2664 PITI) + heloc + card
    approx(p["need"], 7000 + 2664 + 669 + 250, "monthly need includes real mortgage")
    # chosen 2027 @ SS 67: pension 8685 + ss 3405 - need
    approx(p["chosen"]["surplus_with_ss"], 8685 + 3405 - p["need"],
           "surplus once SS is on")
    eq(p["chosen"]["bridge_years"], 0, "no bridge gap at FRA")
    # unconfigured book
    eq(H.retirement_plan({}).get("configured"), False, "empty settings not configured")


def test_retirement_bridge_gap():
    ret = H.retirement_seed()
    ret["retire_year"] = 2026   # retire at 66, but claim SS at 67
    ret["ss_claim_age"] = 67
    p = H.retirement_plan({"retirement": ret})
    eq(p["chosen"]["bridge_years"], 1, "one bridge year before SS starts")


# ── budget ─────────────────────────────────────────────────────────
def test_budget_options_and_owned():
    seed = H.kitchen_seed_template()
    groups = {it["opt_group"] for it in seed if it.get("opt_group")}
    check("Countertop material" in groups and "Cabinet finish" in groups,
          "seed ships the two option groups")
    total0 = H.budget_summary(seed)["subtotal"]
    # switching the chosen cabinet to the pricier option raises the total
    walnut = next(it for it in seed if "walnut" in it["name"].lower())
    painted = next(it for it in seed if it.get("opt_group") == "Cabinet finish" and it["chosen"])
    painted["chosen"] = False
    walnut["chosen"] = True
    total1 = H.budget_summary(seed)["subtotal"]
    check(total1 > total0, "pricier chosen option raises subtotal")
    # only ONE option per group counts
    manual = sum(it["qty"] * it["unit_cost"] + it["labor"] for it in seed
                 if not (it.get("opt_group") and not it.get("chosen")) and not it.get("owned"))
    approx(H.budget_summary(seed)["subtotal"], manual, "only chosen options count")


def test_owned_zeroes_cost():
    items = [{"section": "Appliances", "name": "Fridge", "qty": 1, "unit": "ea",
              "unit_cost": 2800, "labor": 0, "owned": True},
             {"section": "Appliances", "name": "Range", "qty": 1, "unit": "ea",
              "unit_cost": 2800, "labor": 0, "owned": False}]
    s = H.budget_summary(items)
    approx(s["subtotal"], 2800, "owned item excluded from subtotal")
    approx(s["owned_total"], 2800, "owned_total reported")


# ── debt math ──────────────────────────────────────────────────────
def test_payoff_months():
    n, interest = H.payoff_months(1000, 0, 100)
    eq(n, 10, "zero-APR payoff months")
    eq(interest, 0.0, "zero-APR no interest")
    n, _ = H.payoff_months(10000, 18.24, 250)
    check(n and n > 0, "card payoff terminates")
    eq(H.payoff_months(5000, 18.24, 10)[0], None, "payment below interest never pays off")
    eq(H.payoff_months(0, 5, 100), (0, 0.0), "no balance -> done")


def test_net_worth():
    accts = [{"kind": "checking", "balance": 3800, "name": "Checking"},
             {"kind": "savings", "balance": 200, "name": "Savings"}]
    settings = {
        "home_value": 950000, "heloc_balance": 117648, "card_balance": 7454,
        "retirement": {"mortgage_balance": 377000},
        "assets": [{"id": 1, "name": "Fidelity", "value": 8000, "kind": "investment"},
                   {"id": 2, "name": "401k", "value": 120000, "kind": "investment"}],
    }
    nw = H.net_worth(settings, accts)
    approx(nw["total_assets"], 950000 + 4000 + 8000 + 120000, "assets = home+cash+investments")
    approx(nw["total_liabilities"], 377000 + 117648 + 7454, "liabilities = mortgage+heloc+card")
    approx(nw["net_worth"], nw["total_assets"] - nw["total_liabilities"], "net = assets - liab")
    approx(nw["home_equity"], 950000 - 377000 - 117648, "home equity net of liens")
    approx(nw["investments"], 128000, "investment total")
    approx(nw["cash"], 4000, "cash from liquid balances")
    # empty book still returns a sane shape (home default only)
    nw0 = H.net_worth({})
    approx(nw0["home_value"], H.DEFAULT_HOME_VALUE, "default home value")
    eq(nw0["total_liabilities"], 0, "no liabilities on empty book")


def test_rental_scenario():
    settings = {"home_value": 950000, "heloc_payment": 669,
                "retirement": {"mortgage_payment": 2664}}
    r = H.rental_scenario(settings)
    eq(r["rent"], 5000, "default rent")
    # operating = mgmt 8% + vacancy 5% of rent + maintenance 1%/yr of value
    approx(r["mgmt"], 400, "management 8% of rent")
    approx(r["vacancy"], 250, "vacancy 5% of rent")
    approx(r["maintenance"], 950000 * 0.01 / 12, "maintenance 1%/yr of value")
    approx(r["debt_service"], 2664 + 669, "debt service = mortgage + heloc")
    approx(r["net_monthly"], 5000 - (2664 + 669) - r["operating"], "net = rent - costs")
    approx(r["net_annual"], r["net_monthly"] * 12, "annual = 12x monthly")
    # override the rent and management
    r2 = H.rental_scenario(settings, {"rental_rent": 6000, "rental_mgmt_pct": 0})
    eq(r2["rent"], 6000, "rent override")
    eq(r2["mgmt"], 0, "self-manage -> no management fee")
    check(r2["net_monthly"] > r["net_monthly"], "higher rent + self-manage nets more")
    # gross yield
    approx(r["gross_yield_pct"], 5000 * 12 / 950000 * 100, "gross yield")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        try:
            t()
        except Exception as e:  # a throw is a failure, not a crash
            _FAILS.append(f"{t.__name__} raised {type(e).__name__}: {e}")
    print(f"ran {len(tests)} test groups, {_COUNT} checks")
    if _FAILS:
        print(f"\nFAILED ({len(_FAILS)}):")
        for f in _FAILS:
            print("  -", f)
        sys.exit(1)
    print("all passed")


if __name__ == "__main__":
    main()
