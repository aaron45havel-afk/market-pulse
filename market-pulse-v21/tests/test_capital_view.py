"""The /capital dashboard (capital_view.py and the step and undo routes): the
Do-next list (grouping, titles, ranking, stable ids), marking a step done
(the profile edited as if the move were made, notes and unreadable lines
kept, no step suggested twice), the normal month, independence, debts,
passive income, liquidity, property, taxes, the profile's five steps, and
the page. Offline — every page's rows are passed in.

Run: python tests/test_capital_view.py
"""
from __future__ import annotations

import asyncio
import math
import os
import sys
from datetime import date
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import allocation as A  # noqa: E402
import capital as K  # noqa: E402
import capital_view as V  # noqa: E402

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
RENT = {"name": "Duplex", "use": "rental", "value": 900_000, "loan": 300_000, "rate": 3.1, "payment": 1900,
        "rent": 5200, "shelter_rent": 0, "costs": 1500, "basis": 500_000, "year": 2015, "hours": 4}
PROF = {"monthly_invest": 4000, "monthly_expenses": 5000, "salary": 150_000, "match_pct": 4, "k401_room": 20_000,
        "ira_room": 7500, "home_state": "CA", "housing_cost": 2800, "hourly_value": 75, "mortgage_rate": 7.0,
        "picks_n": 3, "pick_max_pct": 50, "age": 34, "take_home": 9000,
        "debts": [{"name": "Card", "balance": 4000, "apr": 22.9}, {"name": "Car loan", "balance": 12000, "apr": 6.5,
                                                                  "payment": 350}],
        "holdings": "Checking, checking, 60000, 0.01%\nSavings, savings, 20000, 3.8%\nVTI, taxable, 50000, 30000\n"
                    "NKE, taxable, 6000, 9000\nAAPL, roth, 15000\nPlan fund, 401k, 80000\nStable value, 401k, 20000, 3%\n"
                    "# my note\nnot a real line",
        "current_monthly": "401k, 401k, 500\nSavings, savings, 2000, 3.8%\nVTI, taxable, 1000\nCard, debt, 500",
        "owned_re": [RENT]}


def build(prof):
    return K.build(prof, today=OCT, sources=SRC)


B = build(PROF)
VW = V.build_view(B, OCT)
P = B["profile"]
T = K.tax_rates(P)
steps = VW["todo"]
by_title = {s["title"]: s for s in steps}

# ── the profile's new fields ───────────────────────────────────────
check(K.parse_debts("Car, 12000, 6.5, 350\nCard, 4000, 22.9\nBad, x, 1") ==
      [{"name": "Car", "balance": 12000.0, "apr": 6.5, "payment": 350.0}, {"name": "Card", "balance": 4000.0, "apr": 22.9}],
      "a debt line takes an optional monthly payment")
check(K.debts_text(K.parse_debts("Car, 12000, 6.5, 350\nCard, 4000, 22.9")) == "Car, 12000, 6.5, 350\nCard, 4000, 22.9",
      "and prints back the way it was typed")
pp = K.parse_profile({"age": "", "filing_status": "Married", "fi_multiple": "99", "take_home": "$9,000",
                      "retire_age": "70"})
check(pp["age"] is None and pp["filing_status"] == "married" and pp["fi_multiple"] == 50 and pp["take_home"] == 9000
      and pp["retire_age"] == 70, "age may be blank; filing status is one of two; the multiple is clamped")
check(K.parse_profile({"filing_status": "head of household"}).get("filing_status") is None,
      "an unknown filing status is not saved")
check(K.LIMITS_2026["roth_phaseout"]["single"] == (153_000, 168_000)
      and K.LIMITS_2026["roth_phaseout"]["married"] == (242_000, 252_000), "the 2026 Roth IRA phase-outs")

# ── the do-next list ───────────────────────────────────────────────
check(steps[0]["kind"] == "debt" and steps[0]["title"] == "Pay off Card",
      "DEBT ABOVE THE HURDLE COMES FIRST — a guaranteed return — and paying all of it reads 'Pay off'")
rest = [s for s in steps if s["kind"] != "debt"]
check(all(a["impact"] >= b["impact"] for a, b in zip(rest, rest[1:])), "then every step by what it adds a year")
check([s["rank"] for s in steps] == list(range(1, len(steps) + 1)), "ranked 1, 2, 3…")
ef = [s for s in steps if s["kind"] == "tbill"]
check(len(ef) == 1 and ef[0]["title"] == "Keep the emergency fund in T-bills" and len(ef[0]["parts"]) == 2
      and near(ef[0]["sold"], 30_000), "THE EMERGENCY FUND IS ONE STEP, from both cash accounts — six months of $5,000")
nke = next(s for s in steps if "NKE" in s["what"])
check(nke["title"] == "Sell NKE at a loss, buy the picks" and near(nke["harvest"], 3000 * T["ordinary"], 0.5),
      "a sale at a loss says so, with the tax it saves (a $3,000 loss against pay)")
vti = by_title.get("Switch VTI to the top picks")
check(vti and vti["tax"] > 0 and near(vti["tax"], 20_000 * T["qualified"], 1), "a sale at a gain shows its tax")
check(sum(s["impact"] for s in steps if s["type"] == "once") - B["current"]["holdings"]["gap_dollars"] < 1
      and abs(sum(s["impact"] for s in steps if s["type"] == "once") - B["current"]["holdings"]["gap_dollars"]) < 1,
      "THE STEPS ADD UP TO THE GAP on the headline")
check([s["id"] for s in V.build_view(build(PROF), OCT)["todo"]] == [s["id"] for s in steps],
      "a step's id is the same every time the same profile is built (what Mark done finds it by)")
mstep = next((s for s in steps if s["type"] == "monthly"), None)
check(mstep and mstep["note"].startswith("From January: $500 to the 401(k) match") and "across the top picks" in mstep["note"],
      "the monthly step says the new split in words")

# ── the normal month and the monthly step ──────────────────────────
st = VW["steady"]
check(st["total"] == 4000 and not [s for s in st["raw"] if s["kind"] in ("debt", "cash")],
      "A NORMAL MONTH has no one-time steps: the card is cleared and the emergency fund full")
check(any(s["kind"] == "ira" and near(s["amount"], 7500 / 12) for s in st["raw"]),
      "and a full year's IRA room, a twelfth a month")
flows = A.parse_flows(PROF["current_monthly"])[0]
lines = A.measure_flows(flows, P, A.stock_rows_of(B), A.stock_time_drag(P, B["monthly_free"]))
tb = P["rf_rate"] * (1 - T["tbill"])
now = sum(f["amount"] * (tb if f["account"] == "debt" else l["ret"]) / 100 for f, l in zip(flows, lines)) * 12
check(near(mstep["now_yr"], now, 0.05), "THE CARD PAYMENT COUNTS AS FREED CASH (T-bill rate), not as 22.9% forever")
check(near(mstep["impact"], st["per_year"] - now, 0.05), "the step is worth the difference, a year")
check(V.monthly_step(build({**PROF, "current_monthly": A.split_text(st["raw"], st["left"], P)}), st) is None,
      "ALREADY ON THE WATERFALL'S SPLIT: no monthly step")
check(V.monthly_step(build({**PROF, "current_monthly": "Mystery, savings, 4000"}), st) is None,
      "a split with an unmeasured line claims nothing")
paid = build({**PROF, "debts": PROF["debts"][1:]})
check(V.monthly_step(paid, V.steady_plan(paid, OCT)) is not None,
      "a payment to a debt no longer listed (paid off) is freed money, not a reason to drop the step")

# ── marking a step done ────────────────────────────────────────────
def apply(title):
    s = by_title[title]
    prof, step = V.apply_step(PROF, B, OCT, s["id"])
    return prof, step


card, _ = apply("Pay off Card")
check(card["debts"] == [PROF["debts"][1]], "PAYING OFF THE CARD REMOVES IT from the debts")
check(card["holdings"].splitlines()[0] == "Checking, cash, 56000, 0.01%",
      "and takes the $4,000 from checking, rewriting that line in place")
check("# my note" in card["holdings"] and "not a real line" in card["holdings"],
      "THE OWNER'S NOTES AND UNREADABLE LINES ARE LEFT EXACTLY AS TYPED")
vti_p, _ = apply("Switch VTI to the top picks")
hl = vti_p["holdings"].splitlines()
check(not any(x.startswith("VTI") for x in hl), "a holding sold out is removed")
new = [x for x in hl if x.split(",")[0] in ("AAA", "BBB", "CCC")]
got = vti["parts"][0]["proceeds"]
check(len(new) == 3 and all(x.endswith(f"{A._n(round(got / 3, 2))}, {A._n(round(got / 3, 2))}") for x in new),
      "the picks arrive one line each, split evenly, at their cost (the after-tax proceeds)")
two = A.apply_moves(PROF, [{**vti["parts"][0], "sold": 20_000, "proceeds": 20_000 * (1 - vti["parts"][0]["tax"] / 50_000)}], B)
check("VTI, taxable, 30000, 18000" in two["holdings"], "a holding sold DOWN keeps its basis in proportion")
roth_p, roth_s = apply("Hold the top picks in your Roth IRA")
third = A._n(round(roth_s["parts"][0]["proceeds"] / 3, 2))
check(f"AAA, roth, {third}" in roth_p["holdings"] and not any(x.startswith("AAPL") for x in roth_p["holdings"].splitlines()),
      "inside a Roth the picks land in the Roth, with no basis")
sv = next(s for s in steps if s["kind"] == "index")
sv_p, _ = V.apply_step(PROF, B, OCT, sv["id"])
check("Index fund, 401k, 20000" in sv_p["holdings"], "a 401(k) switch lands in the plan's index fund")
ef_p, _ = V.apply_step(PROF, B, OCT, ef[0]["id"])
check(ef_p["holdings"].count("T-bills, tbills") == 2, "the emergency fund lands in T-bills (state-exempt)")
capped = A.apply_moves(PROF, [{**vti["parts"][0]}], {**B, "profile": {**P, "pick_max_pct": 20}})
check(any(x.startswith("T-bills, tbills") for x in capped["holdings"].splitlines()),
      "what a per-name cap leaves goes to T-bills, not lost")
m_p, _ = V.apply_step(PROF, B, OCT, mstep["id"])
check(m_p["current_monthly"] == A.split_text(st["raw"], st["left"], P) and "Top picks, taxable" in m_p["current_monthly"],
      "the monthly step rewrites the split to the waterfall's, the picks as 'Top picks'")
for s in steps:
    try:
        prof, _ = V.apply_step(PROF, B, OCT, s["id"])
    except A.CannotApply:
        continue
    after = V.build_view(build(prof), OCT)["todo"]
    check(not any(x["title"] == s["title"] for x in after),
          f"NO STEP IS SUGGESTED TWICE: '{s['title']}' is gone once it is done")
try:
    V.apply_step(PROF, B, OCT, "nope")
    check(False, "an unknown step id is refused")
except A.CannotApply as e:
    check("no longer on the list" in str(e), "an unknown step id is refused, with a reason")
try:
    A.apply_moves(PROF, [{"to_kind": "re", "proceeds": 1, "sold": 1, "line": 0}], B)
    check(False, "buying a property is not applied")
except A.CannotApply:
    check(True, "buying a property is recorded by the owner, with its own numbers")

# ── the "Top picks" shorthand ──────────────────────────────────────
rows = A.stock_rows_of(B)
sl = A.sleeve_of(B)
check(A._ticker("Top picks", "taxable") == A.TOP_PICKS and A._ticker("Top picks", "cash") is None,
      "'Top picks' names the board's top picks as a group (never in cash)")
check(near(rows[A.TOP_PICKS]["ret_pre"], sum(r["ret_pre"] for r in sl) / len(sl)),
      "at their average estimate")
mf = A.measure_flows(A.parse_flows(A.split_text(st["raw"], st["left"], P))[0], P, rows, A.stock_time_drag(P, B["monthly_free"]))
check(near(sum(l["amount"] * l["ret"] for l in mf) / 100 * 12, st["per_year"], 0.5),
      "THE WRITTEN SPLIT READS BACK AT THE WATERFALL'S OWN RETURNS — that is why the step disappears once done")

# ── the optimizer's two guards ─────────────────────────────────────
lazy = {"name": "Paid-off", "use": "rental", "value": 500_000, "loan": 0, "rate": 0, "payment": 0, "rent": 2000,
        "shelter_rent": 0, "costs": 800, "basis": 480_000, "year": 2025, "hours": 1}
sold = build({**PROF, "holdings": "", "owned_re": [lazy], "debts": [{"name": "Card", "balance": 4000, "apr": 22.9}]})
sv = [s for s in V.build_view(sold, OCT)["todo"] if s["title"].startswith("Sell Paid-off")]
eq = sold["current"]["holdings"]["items"][0]["value"]
check(len(sv) == 1 and {m["to_kind"] for m in sv[0]["parts"]} == {"debt", "picks"} and near(sv[0]["sold"], eq, 1),
      "A PROPERTY SOLD IS ONE STEP — all of it, its money split between the card and the picks")
weak = {**SRC, "compounders": [{**c, "expected": e} for c, e in zip(COMP, (20.0, 16.0, 14.0))]}
kept = K.build({**PROF, "holdings": "", "debts": [{"name": "Card", "balance": 4000, "apr": 22.9}]}, today=OCT, sources=weak)
check(not [m for m in kept["current"]["holdings"]["moves"] if m.get("prop") is not None],
      "A PROPERTY IS SOLD WHOLE OR NOT AT ALL — the duplex beats paying the card but not the picks, so no slice of it "
      "is sold to clear $4,000")
dp = A.optimize([{"id": "x", "label": "T-bills", "account": "cash", "account_label": "T-bills", "value": 50_000,
                  "weq": 1.0, "h": 3.0, "keep_reason": None, "tau": 0.0, "c": 0.0, "state_exempt": True, "line": 0,
                  "bucket": "cash"}],
                {**B, "profile": {**P, "monthly_expenses": 0},
                 "plan": {"winner": {"kind": "re", "id": "hh:1", "label": "House hack", "ret_net": 20.0,
                                     "min_capital": 80_000}}}, sl)
check(not [m for m in dp if m["to_kind"] == "dpfund"],
      "CASH ALREADY IN T-BILLS IS NOT 'MOVED' INTO THE DOWN-PAYMENT FUND — it would be suggested again forever")

# ── the slower questions ───────────────────────────────────────────
fi = VW["fi"]
check(fi["number"] == 60_000 * 25 and near(fi["progress"], P["_net_worth"] / 1_500_000 * 100),
      "independence: 25 years of expenses, and how far net worth is toward it")
check(fi["now"]["years"] > fi["opt"]["years"] and near(fi["opt"]["age"], 34 + fi["opt"]["years"]),
      "the optimal path gets there sooner, at a stated age")
r = (1 + B["current"]["holdings"]["opt_pct"] / 100) / 1.025 - 1
check(near(fi["opt"]["coast"], 1_500_000 / (1 + r) ** (65 - 34), 1),
      "Coast FI: what today must be to grow to the number by 65 with no more saving, at the real return")
home = V.independence(build({**PROF, "owned_re": [{**RENT, "use": "home", "shelter_rent": 3000}]}),
                      B["current"]["holdings"], OCT)
check(near(home["home_left_out"], 600_000) and near(home["start"], P["_net_worth"] - 600_000),
      "the home you live in is left out — it houses you, it does not pay for anything")
check(V.independence(build({**PROF, "monthly_expenses": 0}), None, OCT) is None, "no expenses, no number")
check(V.independence(build({**PROF, "age": None}), B["current"]["holdings"], OCT)["opt"]["coast"] is None,
      "no age, no Coast FI")

debts = {d["name"]: d for d in VW["debts"]}
check(debts["Card"]["step"] == 1, "a debt a step pays off says which step")
car = debts["Car loan"]
n = -math.log(1 - (6.5 / 1200) * 12_000 / 350) / math.log(1 + 6.5 / 1200)
check(car["months"] == math.ceil(n) and car["payoff"] == "Jan 2030" and car["extra_months"] > 0
      and car["extra_verdict"] == "keep the minimum",
      "the car is paid off when the annuity says, and $100 more a month loses to the picks")
check(V._payoff(10_000, 24, 100) == (None, 0.0) or V._payoff(10_000, 24, 100)[0] is None,
      "a payment below the interest never pays it off — said, not looped")

pi = VW["passive"]
check(pi["parts"]["Rent after the loan"] == (5200 - 1500 - 1900) * 12 and near(pi["parts"]["Interest"], 60000 * 0.0001 + 20000 * 0.038)
      and near(pi["parts"]["Dividends"], 50_000 * A.INDEX_DIV / 100 + 6_000 * 0.0, 0.01),
      "passive income: rent after the loan, interest at the owner's rates, taxable dividends only (not the Roth's or 401(k)'s)")
check(near(pi["cover"], pi["total"] / 60_000 * 100), "and the share of expenses it covers")
lq = {t["when"]: t["amount"] for t in VW["liquidity"]["tiers"]}
check(lq == {"Today": 80_000, "In days": 56_000, "With a penalty": 115_000, "In months": 900_000 * 0.93 - 300_000},
      "how fast it can be reached: cash, taxable, retirement accounts, property equity")

pr = VW["properties"][0]
check(near(pr["dscr"], 44_400 / 22_800) and near(pr["cap_rate"], 44_400 / 900_000 * 100) and near(pr["ltv"], 100 / 3),
      "property: debt coverage, cap rate, loan-to-value")
check(pr["calc"] and pr["calc"]["E"] == pr["equity_out"] and near(pr["calc"]["refi_rate"], 7.5)
      and near(pr["calc"]["second_rate"], 8.25) and pr["calc"]["sale_tax"] == pr["sale_tax"],
      "the calculator's inputs: today's rate + 0.5 to refinance an investment property, + 1.25 for a second loan")
check(V.properties(build({**PROF, "owned_re": [{**RENT, "use": "home"}]}), OCT)[0]["calc"] is None,
      "the place you live gets no sell calculator")

tx = VW["taxes"]
check(len(tx["harvest"]) == 1 and tx["harvest"][0]["name"] == "NKE" and tx["harvest"][0]["in_plan"]
      and near(tx["harvest"][0]["saves"], 3000 * T["ordinary"]), "the loss to harvest, its tax saved, and that a step sells it")
big = V.taxes(build({**PROF, "holdings": "NKE, taxable, 1000, 9000"}), [])
check(near(big["harvest"][0]["saves"], 3000 * T["ordinary"]) and big["harvest"][0]["carry"] == 5000,
      "a loss past $3,000 is used $3,000 a year; the rest carries forward")
rl = tx["roth_limit"]
check(rl["magi"] == 150_000 - 500 * 12 and rl["state"] == "below" and rl["start"] == 153_000,
      "the Roth IRA test: pay less traditional 401(k) contributions, against the 2026 single phase-out")
check(V.taxes(build({**PROF, "salary": 160_000}), [])["roth_limit"]["state"] == "phasing"
      and V.taxes(build({**PROF, "salary": 300_000}), [])["roth_limit"]["state"] == "above"
      and V.taxes(build({**PROF, "salary": 200_000, "filing_status": "married"}), [])["roth_limit"]["state"] == "below",
      "inside the phase-out, above it, and a married couple's higher line")

# ── vitals, setup ──────────────────────────────────────────────────
vit = {x["key"]: x for x in VW["vitals"]}
check(vit["runway"]["value"] == "16" and vit["runway"]["chip"] == ("warn", "10 mo idle"), "16 months of cash: 10 idle")
check(V.vitals(build({**PROF, "holdings": "Chk, cash, 10000, 1%"}), None)[0]["chip"][0] == "bad",
      "two months against a six-month target is short")
check(vit["saving"]["value"] == "44%" and vit["re"]["chip"] == ("bad", "Over cap"), "saving rate; real estate over its cap")
setup = {s["key"]: s for s in VW["setup"]}
check(not setup["hold"]["done"] and "could not be read" in " ".join(setup["hold"]["todo"]),
      "the holdings step is not done while a line cannot be read")
check(not setup["debts"]["done"] and "Card" in setup["debts"]["todo"][0], "nor the debts step while a payment is missing")
check(setup["goals"]["done"] and not {s["key"]: s for s in V.setup(K.profile_with_defaults({}))}["goals"]["done"],
      "the last step needs your age")

# ── the routes ─────────────────────────────────────────────────────
import database  # noqa: E402
import main  # noqa: E402


class _Req(SimpleNamespace):
    async def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body


STORE: dict = {}
saved_fns = (main._check_admin_token, database.get_capital_profile, database.save_capital_profile, K.build)
main._check_admin_token = lambda request: True
database.get_capital_profile = lambda owner="owner": ({**STORE[owner], "_updated_at": "x"} if owner in STORE else None)
database.save_capital_profile = lambda prof, owner="owner": STORE.update({owner: {k: v for k, v in prof.items()
                                                                                  if not k.startswith("_")}}) or True
_real_build = K.build
K.build = lambda prof, today=None, sources=None: _real_build(prof, today=OCT, sources=SRC)
try:
    STORE["owner"] = dict(PROF)
    first = steps[0]
    res = asyncio.run(main.capital_step_done(_Req(body={"id": first["id"]})))
    check(res.status_code == 200 and STORE["owner"]["debts"] == [PROF["debts"][1]]
          and STORE["owner"]["last_step"]["title"] == "Pay off Card",
          "MARK DONE: the profile is edited and the step recorded for the banner")
    check(STORE["owner:undo"]["debts"] == PROF["debts"], "and the profile before it is kept for one undo")
    again = asyncio.run(main.capital_step_done(_Req(body={"id": first["id"]})))
    check(again.status_code == 409, "the same step twice: it is no longer on the list (409)")
    bad = asyncio.run(main.capital_step_done(_Req(body={})))
    check(bad.status_code == 400, "no id: 400")
    undo = asyncio.run(main.capital_step_undo(_Req(body={})))
    check(undo.status_code == 200 and STORE["owner"]["debts"] == PROF["debts"] and "last_step" not in STORE["owner"]
          and STORE["owner:undo"] == {}, "UNDO PUTS IT BACK and clears the banner; there is nothing left to undo")
    none = asyncio.run(main.capital_step_undo(_Req(body={})))
    check(none.status_code == 404, "a second undo: nothing to undo")
    STORE["owner"]["last_step"] = {"title": "x"}
    asyncio.run(main.capital_profile_save(_Req(body={"age": "40"})))
    check("last_step" not in STORE["owner"] and STORE["owner"]["age"] == 40, "an edit by hand closes the undo banner")
    main._check_admin_token = lambda request: False
    no = asyncio.run(main.capital_step_done(_Req(body={"id": "x"}, cookies={}, query_params={}, headers={})))
    nu = asyncio.run(main.capital_step_undo(_Req(body={}, cookies={}, query_params={}, headers={})))
    check(no.status_code == 401 and nu.status_code == 401, "only the admin can mark steps done or undo")
finally:
    main._check_admin_token, database.get_capital_profile, database.save_capital_profile, K.build = saved_fns

# ── the page ───────────────────────────────────────────────────────
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")), autoescape=True)
env.globals.update(is_admin=lambda r: True, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/capital"), query_params={})


def render(b, **kw):
    ctx = dict(request=req, board=b, p=b["profile"], view=V.build_view(b, OCT), saved=True, updated_at="2026-10-03",
               last_step=None, debts_text=K.debts_text(b["profile"]["debts"]), limits=K.LIMITS_2026,
               hours_default=K.HOURS_DEFAULT, accounts=V.EDIT_ACCOUNT_LABELS)
    ctx.update(kw)
    return env.get_template("capital.html").render(**ctx)


html = render(B, last_step={"title": "Pay off Card", "impact": 916})
for tab in ("overview", "plan", "holdings", "property", "taxes", "board", "profile"):
    check(f'id="v-{tab}"' in html and f'id="t-{tab}"' in html, f"the {tab} tab is on the page")
check(f'data-step="{steps[0]["id"]}"' in html and "Mark done" in html, "every step has its Mark done button, by id")
check("Done:</b> Pay off Card" in html and 'id="undoBtn"' in html, "after a step, the banner offers Undo")
check("Financial independence" in html and "Coast FI" in html and "Jan 2030" in html and "covers" in html,
      "the rail: independence, the car's payoff month, passive income")
check("data-calc=" in html and "Keep, borrow or sell?" in html, "the property calculator, with its inputs")
check("Harvest a loss" in html and "wash-sale" in html and "153,000" in html, "the tax helpers")
check('data-editor="holdings"' in html and 'data-editor="flows"' in html and 'data-editor="debts"' in html
      and 'value="350"' in html, "the profile's editors, the car's payment in its row")
for bad in (">None<", "None%", "Undefined", "nan%", "{{", "{%"):
    check(bad not in html, f"no '{bad}' leaks into the page")
blank = render(_real_build({}, today=OCT, sources={k: [] for k in SRC}), saved=False)
check("Start with your profile" in blank and "List what you hold" in blank and "Add your monthly expenses" in blank,
      "a blank profile gets prompts, not empty numbers")
for bad in (">None<", "None%", "Undefined"):
    check(bad not in blank, f"no '{bad}' leaks into the blank page")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} dashboard checks passed.")
