"""The owner's review of /capital (2026-10-03): a Roth's negative cash made a
"margin loan", the Debts panel disagreed with the monthly plan, every loss sale
claimed the whole $3,000 against pay, ETFs counted as stock picks, dividends
were missing, and thirty Do-next steps buried the ones that matter. Each fix,
checked. Fixtures are made up.

Run: python tests/test_capital_review.py
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import allocation as A  # noqa: E402
import capital as K  # noqa: E402
import capital_view as V  # noqa: E402
import positions as P  # noqa: E402

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
COMP = [{"ticker": t, "name": t, "status": "COMPOUNDER", "expected": e, "er_div": 1.0}
        for t, e in (("AAA", 30.0), ("BBB", 26.0), ("CCC", 24.0))]
SRC = {"compounders": COMP, "aristocrats": [], "lynch": [], "quiet_value": [], "schloss": [], "house_hack": [],
       "brrrr": [], "flip": [], "home": []}


def build(prof, **kw):
    return K.build(prof, today=OCT, sources={**SRC, **kw})


# ── 1. a retirement account's negative cash is not a loan ──────────
SCHWAB_HEAD = ('"Symbol","Description","Mkt Val (Market Value)","Cost Basis","Qty (Quantity)","Asset Type",'
               '"Div Yld (Dividend Yield)",')


def srow(*c):
    return ",".join(f'"{x}"' for x in c) + ","


ROTH = "\n".join(['"Positions for account Roth Contributory IRA ...321 as of 04:00 PM ET, 2026/10/02"', "", SCHWAB_HEAD,
                  srow("DDD", "DDD CORP", "$1,000.00", "$900.00", "10", "Equity", "1.5%"),
                  srow("Cash & Cash Investments", "--", "-$29.00", "--", "--", "Cash and Money Market", "--"),
                  srow("Positions Total", "", "$971.00", "--", "--", "--", "--")])
r = P.parse_export(ROTH)["accounts"][0]
check(r["account"] == "roth" and r["margin"] == 0 and r["skipped"][-1]["value"] == -29 and r["check"] == "ok",
      "A ROTH CANNOT BORROW: −$29 of cash is a trade settling — left out (still counted in the check), not a margin loan")
prof, _ = P.apply_import({"debts": [{"name": "Schwab margin …321", "balance": 29, "apr": 0}]}, P.parse_export(ROTH), {})
check(prof["debts"] == [], "re-importing removes the 'margin loan' the earlier import made")
r2 = P.parse_export(ROTH.replace("Roth Contributory IRA", "Individual"))
check(r2["accounts"][0]["margin"] == 29, "the same −$29 in a taxable account is still a margin loan")
ty, _ = P.apply_import({}, P.parse_export(ROTH.replace("Roth Contributory IRA", "Account")),
                       {"accounts": {"schwab-321": {"account": "roth"}}})
check(ty["debts"] == [], "an account the file did not type, chosen as a Roth in the preview: no loan either")

# ── 2. the Debts panel speaks the plan's dates ─────────────────────
PROF = {"monthly_invest": 6000, "monthly_expenses": 3000, "home_state": "CA", "housing_cost": 2000, "age": 34,
        "ira_room": 7500, "market_return": 8,
        "debts": [{"name": "Visa", "balance": 1300, "apr": 23, "payment": 500},
                  {"name": "Margin", "balance": 20000, "apr": 12},
                  {"name": "Zero", "balance": 29, "apr": 0}],
        "holdings": "AAA, taxable, 20000, 25000\nIdle, cash, 1000, 0.5%"}
B = build(PROF)
dp = {d["name"]: d for d in V.debt_plan(B, V.build_view(B, OCT)["todo"], OCT)}
check(dp["Visa"]["plan_payoff"] == "Oct 2026" and dp["Visa"]["plan_months"] == 0 and dp["Visa"]["payoff"] == "Jan 2027"
      and "extra_payoff" not in dp["Visa"],
      "THE PLAN'S DATE: Visa is cleared this month by the plan (its $500 payment alone would take to January), "
      "and the 'pay $100 extra' advice is gone — the plan already pays it")
check(dp["Margin"].get("plan_payoff") and not dp["Margin"]["payment"],
      "a margin loan with no payment: the plan's payoff month, not 'add the monthly payment'")
check(dp["Zero"]["below_hurdle"] and "plan_payoff" not in dp["Zero"], "a 0% balance: below the hurdle, the plan leaves it")


def render(b):
    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")), autoescape=True)
    env.globals.update(is_admin=lambda r: True, current_user=lambda r: None, pipeline_access=lambda r: False)
    req = SimpleNamespace(url=SimpleNamespace(path="/capital"), query_params={})
    return env.get_template("capital.html").render(
        request=req, board=b, p=b["profile"], view=V.build_view(b, OCT), saved=True, updated_at="2026-10-03",
        last_step=None, debts_text="", limits=K.LIMITS_2026, hours_default=K.HOURS_DEFAULT,
        accounts=V.EDIT_ACCOUNT_LABELS)


html = render(B)
check("Your monthly plan clears it <b>this month</b> — at the $500 payment alone it would take until Jan 2027" in html
      and "it has no fixed payment" in html and "so the plan leaves it" in html
      and "Add the monthly payment (Profile) to see when it is paid off" not in html,
      "on the page: the plan's month first; margin has no payment; the 0% balance is left")

# ── 3. $3,000 a year of losses against pay, in all ─────────────────
LOSS = {"monthly_invest": 500, "monthly_expenses": 3000, "home_state": "CA", "age": 34, "fed_rate": 24, "state_rate": 9.3,
        "holdings": ("Chk, checking, 18000, 4%\nLLL, taxable, 4000, 9000\nMMM, taxable, 3000, 6000\n"
                     "NNN, taxable, 2000, 2500")}
LB = build(LOSS)
lsteps = [s for s in V.build_view(LB, OCT)["todo"] if s.get("loss")]
rate = K.tax_rates(LB["profile"])["ordinary"]
check(len(lsteps) == 1 and lsteps[0]["loss"] == 8500 and near(lsteps[0]["harvest"], 3000 * rate)
      and lsteps[0]["carry"] == 5500,
      "one step selling $8,500 of losses: $3,000 of it against pay this year, $5,500 carries forward")
ss = [{"loss": 2500.0}, {"loss": 0.0}, {"loss": 2000.0}, {"loss": 900.0}]
V.share_losses(ss, 0.3)
check([(s.get("harvest"), s.get("carry")) for s in ss] == [(750.0, 0.0), (None, None), (150.0, 1500.0), (0.0, 900.0)],
      "LOSSES SHARE THE $3,000 in step order: $2,500, then the last $500 of it, then nothing — the rest carries")
lh = render(LB)
check("of loss carries forward" in lh and "Saves ≈" in lh, "the chips: what it saves this year, and what carries")
tax_tab = V.build_view(LB, OCT)["taxes"]["harvest"]
check(near(sum(x["saves"] for x in tax_tab), 3000 * rate, 0.02) and tax_tab[0]["loss"] >= tax_tab[-1]["loss"],
      "THE TAXES TAB: the same $3,000 in all, biggest loss first — not $3,000 per holding")

# ── 4. ETFs are funds; 5. dividends come from the exports ──────────
rows, bad = A.parse_holdings("ETFX, taxable, 1000, 1200, 40 sh, 6.25% div, fund\nETFX, ira, 500, 20 sh\n"
                             "NKE, taxable, 300, 250, 2.1% div, 12%\nX, taxable, 1, 2% div, 3% div")
by = {(r["name"], r["account"]): r for r in rows}
check(by[("ETFX", "taxable")]["fund"] and by[("ETFX", "ira")]["fund"] and not A.is_pick(by[("ETFX", "taxable")])
      and not A.is_pick(by[("ETFX", "ira")]),
      "'fund' marks an ETF — and a ticker marked a fund on one line is a fund on every line (one broker calls it a "
      "trust, another an ETF)")
check(by[("ETFX", "taxable")]["div"] == 6.25 and by[("NKE", "taxable")]["div"] == 2.1 and by[("NKE", "taxable")]["rate"] == 12
      and len(bad) == 1, "'6.25% div' is the yield; '12%' is still the owner's own return; two yields cannot be read")
check(A.holding_line("ETFX", "taxable", 1000, 1200, qty=40, div=6.25, fund=True)
      == "ETFX, taxable, 1000, 1200, 40 sh, 6.25% div, fund", "written back in that order")
est = A.estimate(by[("ETFX", "taxable")], K.profile_with_defaults({}), {})
check(est[1] == 6.25 and not est[3] and "ETF" in est[2], "an ETF at the market return, its own 6.25% as the dividend")
pick = A.estimate(by[("NKE", "taxable")], K.profile_with_defaults({}), {})
plain = A.estimate(A.parse_holdings("ZZZ, taxable, 300, 250, 2.1% div")[0][0], K.profile_with_defaults({}), {})
check(pick[1] == 2.1 and plain[1] == 2.1 and plain[3],
      "a stock's own yield is used too — with the owner's return, or on no screen at the market return")
mix = build({**PROF, "debts": [], "holdings": "ETFX, taxable, 10000, 12000, 400 sh, fund\nAAA, taxable, 10000, 10000"})
check({b_["bucket"]: b_["pct"] for b_ in mix["current"]["holdings"]["bars_now"]} == {"funds": 50.0, "picks": 50.0},
      "the overview's mix: the ETF under funds, not stock picks")
inc = V.passive_income(build({**PROF, "debts": [], "holdings": "ETFX, taxable, 10000, 12000, 8.5% div, fund"}))
check(near(inc["parts"]["Dividends"], 850.0), "passive income: $10,000 at 8.5% is $850 a year (it had been $0)")
_, info = A.reprice("QQQ, taxable, 1000, 900, 2 sh", {"QQQ": {"price": 600.0, "instrument": "ETF"}})
new, _ = A.reprice("QQQ, taxable, 1000, 900, 2 sh", {"QQQ": {"price": 600.0, "instrument": "ETF"}})
check(new == "QQQ, taxable, 1200, 900, 2 sh, fund" and
      A.reprice("AAPL, taxable, 1000, 900, 2 sh", {"AAPL": {"price": 600.0, "instrument": "EQUITY"}})[0].endswith("2 sh"),
      "THE PRICE FEED KNOWS AN ETF the export did not name: the line is marked a fund; a stock is left alone")
X = "\n".join(['"Positions for account Individual ...111 as of 04:00 PM ET, 2026/10/02"', "", SCHWAB_HEAD,
               srow("ETFX", "ACME GLOBAL INTERNET", "$1,000.00", "$900.00", "40", "ETFs & Closed End Funds", "6.25%"),
               srow("ZZZ", "ZZZ HOLDINGS", "$500.00", "$400.00", "5", "Equity", "N/A"),
               srow("Positions Total", "", "$1,500.00", "--", "--", "--", "--")])
xa = P.parse_export(X)["accounts"][0]
xp = {p_["symbol"]: p_ for p_ in xa["positions"]}
check(xp["ETFX"]["fund"] and xp["ETFX"]["div"] == 6.25 and not xp["ZZZ"]["fund"] and xp["ZZZ"]["div"] is None,
      "SCHWAB: 'ETFs & Closed End Funds' is a fund; 'Div Yld' 6.25% is the yield; N/A is no yield")
check(P._is_fund("ACME WORLD TECH INDEX ETF", "Equity") and not P._is_fund("FUNDAMENTAL CORP", "Equity")
      and P._yield("0", "1,250.00", 20000.00) == 6.25 and P._yield("4.1%", "", 100) == 4.1,
      "a fund by its name (ETF, INDEX — not 'FUNDAMENTAL'); CHASE: estimated income over value (its own yield "
      "column reads 0 for a payer); FIDELITY: the distribution rate")
blk = P.block_lines({**xa, "account": "taxable"}, "2026-10-02", {})
check("ETFX, taxable, 1000, 900, 40 sh, 6.25% div, fund" in blk and "ZZZ, taxable, 500, 400, 5 sh" in blk,
      "the synced lines carry the yield and the fund mark")
ed = V.build_view(build({**PROF, "debts": [], "holdings": "ETFX, taxable, 1000, 900, 4 sh, 6.25% div, fund"}), OCT)["editor"]
check(ed["holdings"][0]["extra"] == "6.25% div, fund", "the row editor keeps what it has no column for")
eh = render(build({**PROF, "debts": [], "holdings": "ETFX, taxable, 1000, 900, 4 sh, 6.25% div, fund"}))
check('data-c="extra" value="6.25% div, fund"' in eh and "g('extra')" in eh, "…and writes it back on save")

# ── where a debt's payment comes from ──────────────────────────────
check(K.parse_profile({"debt_payments_from": "expenses"})["debt_payments_from"] == "expenses"
      and "debt_payments_from" not in K.parse_profile({"debt_payments_from": "nonsense"})
      and K.profile_with_defaults({})["debt_payments_from"] == "invest",
      "a setting, out of what is put aside by default")
check('name="debt_payments_from"' in html, "and on the profile, under pay")
car = {**PROF, "debts": [{"name": "Car", "balance": 9000, "apr": 5, "payment": 300}], "ira_room": 0, "k401_room": 0}
check(near(K.steady_free(K.profile_with_defaults(car)), 6000 - 300)
      and near(K.steady_free(K.profile_with_defaults({**car, "debt_payments_from": "expenses"})), 6000),
      "the board's free money: less the car loan's payment when it comes out of what is put aside")

# ── 6. small steps fold ────────────────────────────────────────────
vw = V.build_view(LB, OCT)
small, main = vw["todo_small"], vw["todo_main"]
check(all(s["impact"] < V.SMALL_STEP and s["kind"] != "debt" for s in small)
      and len(small) + len(main) == len(vw["todo"]) and all(s["impact"] >= V.SMALL_STEP or s["kind"] == "debt"
                                                             or not small for s in main),
      "steps under $100 a year fold into the clean-ups; the rest — and every debt — stay on top")
SM = {"monthly_invest": 4000, "monthly_expenses": 1000, "home_state": "CA", "age": 34, "ira_room": 0,
      "holdings": "Big, cash, 60000, 0.01%\nAAPL, roth, 900\nMSFT, roth, 700\nNVDA, roth, 500"}
sv = V.build_view(build(SM), OCT)
sh = render(build(SM))
check(len(sv["todo_small"]) >= 2 and all(s["impact"] < V.SMALL_STEP for s in sv["todo_small"])
      and sv["todo_main"] and sv["todo_main"][0]["impact"] >= V.SMALL_STEP,
      "a profile with a big move and several tiny Roth switches: the tiny ones fold")
check(f"{len(sv['todo_small'])} small clean-ups" in sh and sh.count('class="cx-small"') == 1
      and sh.count('class="cx-task') == len(sv["todo"]),
      "the page: one fold, every step still a card (Mark done and the filters reach it)")
one = V.build_view(build({**SM, "holdings": "Big, cash, 60000, 0.01%\nAAPL, roth, 900"}), OCT)
check(not one["todo_small"], "a single small step is not folded on its own")
visible = re.sub(r"<script.*?</script>", "", html, flags=re.S)
for bad_ in (">None<", "None%", "Undefined", "NaN", "Infinity"):
    check(bad_ not in visible, f"no '{bad_}' leaks into the page")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} review-fix checks passed.")
