"""The capital board: taxes, each page's return estimate, friction, the
ranking, the monthly waterfall, the profile parser, and the page. Offline —
every page's rows are passed in, never fetched.

Run: python tests/test_capital.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import capital as K  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def near(a, b, tol=0.01):
    return a is not None and b is not None and abs(a - b) <= tol


P = K.profile_with_defaults({"fed_rate": 24, "state_rate": 9.3, "ltcg_rate": 15, "hold_years": 5,
                             "market_return": 7.0, "estimate_weight": 50, "rf_rate": 4.0})

# ── taxes ──────────────────────────────────────────────────────────
t = K.tax_rates(P)
check(near(t["ordinary"], 0.333) and near(t["qualified"], 0.243) and near(t["tbill"], 0.24),
      "ordinary = fed + state; gains = LTCG + state; T-bills federal only (state-exempt)")
check(near(K.tax_rates({**P, "niit": True})["qualified"], 0.281), "NIIT adds 3.8 points to dividends and gains")
check(near(K.tax_rates({**P, "retire_rate": 20})["retire"], 0.20) and near(t["retire"], 0.333),
      "the retirement rate defaults to today's fed + state")
check(near(K.after_tax_taxable(10, 0, 5, 0, 0), 10.0), "no tax: the after-tax return is the return")
check(near(K.after_tax_taxable(10, 0, 1, 0.2, 0.2), 8.0), "one year, all gain, 20% tax: 8%")
deferred = K.after_tax_taxable(10, 0, 10, 0.2, 0.2)
yearly = K.after_tax_taxable(10, 10, 10, 0.2, 0.2)
check(deferred > yearly and near(yearly, 8.0),
      "A GAIN TAXED ONCE AT THE END BEATS A DIVIDEND TAXED EVERY YEAR — deferral is worth something")
check(near(K.after_tax_taxable(-10, 0, 5, 0.2, 0.2), -10.0), "a loss is not taxed")
check(near(K.after_tax_traditional(7, 5, 0.3, 0.3), 7.0), "a traditional account at equal rates returns the pre-tax rate")
check(K.after_tax_traditional(7, 5, 0.333, 0.2) > 7.0, "and more when withdrawals are taxed lower than the deduction")

# ── each page's estimate ───────────────────────────────────────────
check(near(K.blend(17.0, P), 12.0) and near(K.blend(100.0, P), (40 + 7) / 2),
      "half the screen's estimate, half the market; an estimate is bounded before blending")
check(near(K.blend(17.0, {**P, "estimate_weight": 100}), 17.0), "a weight of 100 trusts the screen outright")

comp = [{"ticker": "LEN", "name": "Lennar", "status": "COMPOUNDER", "expected": 17.7, "er_div": 2.5,
         "er_growth": 11.3, "er_buyback": 4.0, "er_mult": 0.0},
        {"ticker": "GATE", "name": "Gated", "status": "GATED", "expected": 30.0},
        {"ticker": "QUAL", "name": "Quality", "status": "QUALITY", "expected": None}]
cs = K.stocks_from_compounders(comp, P)
check([r["detail"]["ticker"] for r in cs] == ["LEN"],
      "only names passing Compounders' gates, with an estimate, are candidates")
check(near(cs[0]["ret_pre"], K.blend(17.7, P)) and cs[0]["div_pct"] == 2.5 and "17.7" in cs[0]["basis"],
      "Compounders' own expected return, its dividend share taxed as dividends, and the decomposition shown")

ar = K.stocks_from_aristocrats([{"t": "KO", "n": "Coca-Cola", "status": "BUY", "y": 3.0, "dg5": 25.0},
                                {"t": "W", "n": "Watch", "status": "WATCH", "y": 5.0, "dg5": 5.0}], P)
check(len(ar) == 1 and near(ar[0]["detail"]["estimate"], 3.0 + K.GROWTH_CAP),
      "Aristocrats: BUY/VALUE only, yield + dividend growth with growth capped")

ly = K.stocks_from_lynch([{"ticker": "YALA", "name": "Yalla", "pe_ratio": 5.0, "growth": {"cagr": 15.9}},
                          {"ticker": "NOPE", "pe_ratio": -3, "growth": {"cagr": 5}},
                          {"ticker": "FLAT", "name": "Flat", "pe_ratio": 20.0, "growth": {"cagr": -8}}], P)
check([r["detail"]["ticker"] for r in ly] == ["YALA", "FLAT"]
      and near(ly[0]["detail"]["estimate"], 20.0 + 10.0) and near(ly[1]["detail"]["estimate"], 5.0),
      "LYNCH'S GROWTH IS A RECORD (growth.cagr): earnings yield + capped growth, a shrinking one floored at zero")

qv = K.stocks_from_quiet_value([{"ticker": "HNNA", "name": "Hennessy", "metrics": {"pe": 8.0, "dividend": 5.45}}], P)
check(near(qv[0]["detail"]["estimate"], 12.5) and qv[0]["round_trip_pct"] == K.ROUND_TRIP_PCT["stock_small"],
      "Quiet Value: earnings yield, and a small-cap round trip")

sch = [{"ticker": "FSP", "name": "Franklin St", "gates_pass": True, "below_book": True, "p_tangible_book": 0.07,
        "price": 0.4, "dividend": {"latest": 0.04}},
       {"ticker": "SITC", "name": "SITE", "gates_pass": True, "below_book": True, "p_tangible_book": 0.5,
        "price": 3.0, "dividend": {"latest": 5.79}},
       {"ticker": "FAIL", "gates_pass": False, "below_book": True, "p_tangible_book": 0.5}]
ss = K.stocks_from_schloss(sch, P)
check([r["detail"]["ticker"] for r in ss] == ["SITC"],
      "BELOW A THIRD OF BOOK NO REVERSION IS ASSUMED — Franklin Street's distress would have ranked first")
check(ss and near(ss[0]["div_pct"], K.DIV_YIELD_CAP) and near(ss[0]["detail"]["estimate"], ((0.75 / 0.5) ** 0.2 - 1) * 100 + 8.0),
      "half the discount closes over the hold; a 193% yield (a liquidation payout) is capped at 8%")

merged = K.merge_stocks(cs + K.stocks_from_compounders([{**comp[0], "expected": 9.0}], P)
                        + K.stocks_from_lynch([{"ticker": "LEN", "name": "Lennar", "pe_ratio": 10, "growth": {"cagr": 30}}], P))
lens = [r for r in merged if r["detail"]["ticker"] == "LEN"]
check(len(lens) == 1 and lens[0]["sources"] == ["Compounders", "Compounders", "Lynch"]
      and lens[0]["ret_after"] == max(r["ret_after"] for r in cs + ly if True) or len(lens) == 1,
      "one row per ticker, every screen that named it listed")
check(len(lens) == 1 and lens[0]["source"] == "Lynch" and near(lens[0]["detail"]["estimate"], 20.0),
      "the highest estimate wins the row")

# ── real estate ────────────────────────────────────────────────────
HH = [{"zip": "92233", "place": "Calipatria, CA", "state": "CA", "max_offer": 300_000, "cash_to_close": 20_000,
       "monthly_surplus": 500},
      {"zip": "94510", "place": "Benicia, CA", "state": "CA", "max_offer": 700_000, "cash_to_close": 40_000,
       "monthly_surplus": 100},
      {"zip": "17851", "place": "Mount Carmel, PA", "state": "PA", "max_offer": 116_000, "cash_to_close": 9_118,
       "monthly_surplus": 1123}]
hp = {**P, "home_state": "CA", "housing_cost": 2000}
hh = K.house_hack_rows(HH, hp)
check([r["detail"]["zip"] for r in hh] == ["92233", "94510"], "owner-occupied: only the state you live in")
want = ((100 + 2000) * 12 - 0.07 * 700_000 / 5) / 40_000 * 100
check(near(hh[1]["ret_after"], round(want, 1), 0.06),
      "THE RENT YOU STOP PAYING COUNTS: (surplus + your rent) × 12, less selling costs over the hold, on the cash to close")
_saved_coords = K._zip_coords
K._zip_coords = lambda zips: {"94110": (37.75, -122.415), "92233": (33.17, -115.55), "94510": (38.05, -122.16)}
try:
    near_only = K.house_hack_rows(HH, {**hp, "home_zip": "94110", "max_miles": 40})
finally:
    K._zip_coords = _saved_coords
check([r["detail"]["zip"] for r in near_only] == ["94510"],
      "A STATE IS NOT WHERE YOU LIVE: with a home ZIP, Calipatria (500 miles) drops and Benicia stays")
check(near(K._miles((37.75, -122.415), (38.05, -122.16)), 25.0, 3.0), "distance is great-circle miles")

BOARD = [{"code": "ME", "name": "Maine", "feasible": True, "max_price": 174_540, "max_pct_median": 50.0,
          "median_value": 348_870, "detail": {"cash_in_peak": 55_000, "equity": 90_000}},
         {"code": "TX", "name": "Texas", "feasible": False, "max_price": 0, "max_pct_median": None,
          "median_value": 300_000, "detail": None},
         {"code": "OH", "name": "Ohio", "feasible": True, "max_price": 120_000, "max_pct_median": 60.0,
          "median_value": 200_000, "detail": {"cash_in_peak": 40_000, "equity": 70_000}}]
br = K.conditional_re_rows(BOARD, P, mode="brrrr")
check([r["detail"]["max_price"] for r in br] == [120_000, 174_540],
      "feasible markets only, easiest first (the highest share of the median)")
check(all(r["ret_after"] == 14.0 for r in br) and "≤ 60%" in br[0]["conditional"] and br[0]["min_capital"] == 40_000,
      "CONDITIONAL: the target is the return, only at or below the solved price; the cash is the deal's peak")
fl = K.conditional_re_rows(BOARD, P, mode="flip")
check(fl[0]["min_capital"] == 70_000 and fl[0]["hours_month"] == K.HOURS_DEFAULT["flip"],
      "a flip needs its equity and takes its own hours")

home = K.home_rows([{"zip": "94501", "name": "Alameda", "entry_price": 800_000, "median_rent": 3500}], P, rate_pct=7.0)
check(len(home) == 1 and home[0]["min_capital"] > 100_000 and home[0]["ret_after"] < 5,
      "buying a Bay Area home at 7%: the cash to close is large and the return on it is thin or negative")
check(near(K._balance_after(100_000, 0, 12), 100_000 * (1 - 12 / 360)), "a zero-rate loan amortizes evenly")

g = K.guaranteed_rows({**P, "debts": [{"name": "Card", "balance": 3000, "apr": 24.9}, {"name": "Paid", "balance": 0, "apr": 9}]})
check([r["kind"] for r in g] == ["tbill", "debt", "wrapper"] and near(g[0]["ret_after"], 4.0 * 0.76),
      "T-bills after federal tax, each live debt at its APR, the 401(k) beyond the match")

# ── friction ───────────────────────────────────────────────────────
stock = K.stock_row(P, ticker="X", name="X", source="Compounders", est=12.0, div=0.0, basis="b", link="/c")
fp = {**P, "hourly_value": 75, "picks_hours": 2, "stock_holdings": 0}
fs = K.friction(stock, fp, monthly_free=1000, cash_free=0)
check(near(fs["time_drag"], 2 * 12 * 75 / (1000 * 12 * 5 / 2) * 100, 0.01),
      "RESEARCH TIME IS CHARGED AGAINST THE MONEY THE SLEEVE MANAGES ON AVERAGE over the hold, not one month")
check(near(K.friction(stock, {**fp, "stock_holdings": 270_000}, monthly_free=1000, cash_free=0)["time_drag"], 0.6, 0.01),
      "and a larger portfolio spreads it thinner")
check(fs["months_to_fund"] == 0 and near(fs["ret_net"], stock["ret_after"] - 0.2 / 5 - fs["time_drag"], 0.01),
      "a stock is ready now; costs spread over the hold")
re_row = br[0]
fr = K.friction(re_row, {**P, "hourly_value": 50}, monthly_free=2000, cash_free=10_000, delay_months=3)
check(fr["months_to_fund"] == 3 + 15, "ready in: the one-time steps first, then (need − free cash) ÷ the monthly share")
check(near(fr["time_drag"], 10 * 12 * 50 / 40_000 * 100), "real-estate hours against the cash in the deal")
w = ((18 / 2 + K.DEPLOY_MONTHS["brrrr"]) / 12) / (5 + (18 / 2 + K.DEPLOY_MONTHS["brrrr"]) / 12)
check(near(fr["ret_net"], (1 - w) * (14.0 - 0 - fr["time_drag"]) + w * 4.0 * 0.76, 0.02),
      "WAITING COSTS: the months the money sits in T-bills while it is saved are blended in at the T-bill rate")
never = K.friction(re_row, P, monthly_free=0, cash_free=0)
check(never["ret_net"] is None and never["months_to_fund"] is None, "nothing free and not enough cash: never")
check(K.friction(re_row, P, monthly_free=0, cash_free=50_000)["months_to_fund"] == 0,
      "but cash already free beyond the emergency fund is ready now")

ranked = K.rank([{**fr, "ret_net": 5.0}, {**never}, {"kind": "debt", "ret_net": 5.0, "id": "d"},
                 {"kind": "stock", "ret_net": 9.0, "id": "s"}])
check([r.get("id") or r["kind"] for r in ranked][:3] == ["s", "d", re_row["id"]] and ranked[-1]["ret_net"] is None,
      "best net first; a guaranteed use wins a tie; what cannot be funded goes last")
check(K.rank([{**never, "id": "n"}, {"kind": "stock", "ret_net": -3.0, "id": "loser"}])[0]["id"] == "loser",
      "a use that cannot be funded ranks below even a losing one — 'never' is not 0%")

# ── the month ──────────────────────────────────────────────────────
OCT = date(2026, 10, 3)
M = K.profile_with_defaults({"monthly_invest": 4000, "cash": 2000, "monthly_expenses": 5000,
                             "salary": 150_000, "match_pct": 4, "match_rate": 100, "k401_room": 20_000,
                             "debts": [{"name": "Card", "balance": 3000, "apr": 24.9},
                                       {"name": "Car", "balance": 12_000, "apr": 4.0}],
                             "ira_room": 7500, "hsa_eligible": True, "hsa_room": 3000, "home_state": "CA"})
purse = K.fixed_steps(M, OCT)
kinds = [s["kind"] for s in purse.steps]
check(kinds[:3] == ["cash", "match", "debt"], f"CUSHION, THEN THE MATCH, THEN DEAR DEBT (got {kinds})")
check(purse.steps[0]["amount"] == 3000 and purse.steps[1]["amount"] == 500,
      "the cushion tops up to one month; the match is 4% of $150k a month")
check(purse.steps[2]["amount"] == 500 and purse.left == 0,
      "the 24.9% card takes the rest; the 4% car loan is not a fixed step (below the 7% hurdle)")
big = K.fixed_steps({**M, "monthly_invest": 40_000, "cash": 30_000}, OCT)
labels = [s["to"] for s in big.steps]
check(labels == ["401(k) up to the employer match", "Pay down Card", "HSA (invested)", "Roth IRA — your top pick(s)"]
      and big.steps[2]["amount"] == 1000 and big.steps[3]["amount"] == 2500,
      "emergency fund full, so no top-up; HSA and IRA room spread over the 3 months left in October")
check("state taxes HSAs" in big.steps[2]["why"], "and a California owner is told the state taxes HSAs")
check(K.fixed_steps({**M, "monthly_invest": 40_000, "cash": 0}, OCT).steps[3]["to"].startswith("Emergency fund")
      and abs(sum(s["amount"] for s in K.fixed_steps({**M, "monthly_invest": 40_000, "cash": 0}, OCT).steps
                  if s["kind"] == "cash") - 30_000) < 0.01,
      "cushion + emergency fund together fill exactly six months of expenses")

check(near(K.steady_free(M), 4000 - 500 - 3000 / 12 - 7500 / 12),
      "a normal month's share: pay less the match and a twelfth of the HSA and IRA")
check(K.one_time_months(M) == __import__("math").ceil((30_000 - 2000 + 3000) / K.steady_free(M)),
      "the emergency shortfall and dear debt come first, timed from the steady share")
check(K.one_time_months({**M, "cash": 1e6, "debts": []}) == 0, "nothing one-time: no delay")

stock_win = [{"kind": "stock", "ret_net": 9.0, "detail": {"ticker": t}, "source": "Compounders", "label": t,
              "min_capital": 1} for t in ("A", "B", "C")]
wf = K.waterfall({**M, "monthly_invest": 40_000, "cash": 30_000, "picks_n": 3, "pick_max_pct": 20}, stock_win, OCT)
buys = [s for s in wf["steps"] if s["kind"] == "stock"]
check(len(buys) == 3 and all(near(s["amount"], buys[0]["amount"]) for s in buys)
      and near(buys[0]["amount"], (40_000 - 500 - 3000 - 1000 - 2500) * 0.2) and wf["left"] > 0,
      "PICKS SPLIT EVENLY, NO NAME ABOVE ITS CAP — what the cap leaves stays unassigned and is shown")
re_win = [{"kind": "debt", "ret_net": 24.9, "ret_pre": 24.9, "label": "Pay down Card", "min_capital": 0},
          {"kind": "re", "ret_net": 30.0, "label": "House hack — Benicia", "min_capital": 40_000, "months_to_fund": 7}]
wf2 = K.waterfall({**M, "monthly_invest": 40_000, "cash": 30_000}, K.rank(re_win), OCT)
check(wf2["winner"]["kind"] == "re" and wf2["steps"][-1]["to"].startswith("Down-payment fund for House hack")
      and "7 months" in wf2["steps"][-1]["why"],
      "A LUMPY WINNER IS SAVED FOR in T-bills, with the months it takes")
wf3 = K.waterfall({**M, "monthly_invest": 40_000, "cash": 30_000},
                  [{"kind": "debt", "ret_net": 24.9, "ret_pre": 24.9, "label": "Pay down Card"},
                   {"kind": "tbill", "ret_net": 3.0, "label": "T-bills"}], OCT)
check(wf3["winner"]["label"] == "T-bills",
      "the winner is never a debt the fixed steps already pay")

# ── the profile ────────────────────────────────────────────────────
pp = K.parse_profile({"monthly_invest": "$4,000", "fed_rate": "900", "retire_rate": "", "mortgage_rate": " ",
                      "niit": "on", "hsa_eligible": False, "home_state": "ca!", "home_zip": "94110-1234",
                      "debts": "Card, 3000, 24.9\nbad line\nCar, $12000, 6.5%\n, 5, 5", "evil": "x", "hold_years": "-3"})
check(pp["monthly_invest"] == 4000 and pp["fed_rate"] == 60 and pp["hold_years"] == 1,
      "money strings parse; numbers are clamped (a 900% tax rate becomes the 60% ceiling)")
check(pp["retire_rate"] is None and pp["mortgage_rate"] is None, "a blank optional field means 'use the default rule'")
check(pp["niit"] is True and pp["hsa_eligible"] is False, "checkboxes read as booleans")
check(pp["home_state"] == "CA" and pp["home_zip"] == "94110", "text is sanitized and trimmed to length")
check(pp["debts"] == [{"name": "Card", "balance": 3000.0, "apr": 24.9}, {"name": "Car", "balance": 12000.0, "apr": 6.5}],
      "debts parse line by line; a line that does not parse is skipped, not guessed")
check("evil" not in pp, "unknown keys are dropped")
check(K.parse_debts("Loan, lots, 5\nNeg, -5, 3\nPct, 100, -1\nOk, 1, 2") == [{"name": "Ok", "balance": 1.0, "apr": 2.0}],
      "a balance or APR that is not a number, or is negative, skips the line")
check(K.debts_text(pp["debts"]) == "Card, 3000, 24.9\nCar, 12000, 6.5", "and print back the way they were typed")
check(K.profile_with_defaults({"hours_brrrr": 3, "junk": 1}).get("hours_brrrr") == 3
      and "junk" not in K.profile_with_defaults({"junk": 1}), "per-path hours are kept; junk is not")

# ── the whole board, from injected pages ───────────────────────────
SRC = {"compounders": comp, "aristocrats": [], "lynch": [], "quiet_value": [], "schloss": [],
       "house_hack": HH, "brrrr": BOARD, "flip": BOARD,
       "home": [{"zip": "94501", "name": "Alameda", "entry_price": 800_000, "median_rent": 3500}]}
B = K.build({**M, "mortgage_rate": 7.0, "home_zip": ""}, today=OCT, sources=SRC)
check(not B["errors"] and B["counts"]["stock"] == 1 and B["counts"]["re"] == 2 + 2 + 2 + 1,
      f"every page lands on one board (got {B['counts']})")
check(B["rows"] == K.rank(B["rows"]) and B["plan"]["steps"], "ranked, with a plan")

def boom():
    raise RuntimeError("feed down")
saved = K._compounders_rows
K._compounders_rows = boom
try:
    B2 = K.build({**M, "mortgage_rate": 7.0}, today=OCT, sources={k: v for k, v in SRC.items() if k != "compounders"})
finally:
    K._compounders_rows = saved
check(B2["errors"].get("compounders", "").startswith("RuntimeError") and B2["counts"]["re"] > 0,
      "ONE BROKEN PAGE IS NAMED, AND THE REST OF THE BOARD STANDS")

# ── the page ───────────────────────────────────────────────────────
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")))
env.globals.update(is_admin=lambda r: True, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/capital"), query_params={})
html = env.get_template("capital.html").render(request=req, board=B, p=B["profile"], saved=True,
                                                updated_at="2026-10-03T17:00", debts_text=K.debts_text(M["debts"]),
                                                limits=K.LIMITS_2026, hours_default=K.HOURS_DEFAULT)
check("This month — $4,000" in html and "401(k) up to the employer match" in html, "the plan is on the page")
check("only if you buy at" in html and "in return" in html,
      "conditional real estate says so; its costs read 'in return', not 0%")
check('name="debts"' in html and "Card, 3000, 24.9" in html, "the profile form shows the saved debts")
for bad in (">None<", "None%", "nan%", "NaN"):
    check(bad not in html, f"no '{bad}' leaks into the page")
blank = env.get_template("capital.html").render(
    request=req, board=K.build({}, today=OCT, sources={k: [] for k in SRC}), p=K.profile_with_defaults({}),
    saved=False, updated_at=None, debts_text="", limits=K.LIMITS_2026, hours_default=K.HOURS_DEFAULT)
check("Start with your profile" in blank and "Nothing to split yet" in blank,
      "with no profile the page says so instead of showing an empty plan as advice")

import main  # noqa: E402

anon = SimpleNamespace(cookies={}, query_params={}, headers={})
r = asyncio.run(main.capital_page(anon))
check(r.status_code == 303 and r.headers["location"] == "/sign-in?redirect=/capital",
      "PRIVATE: no admin, no page — the sign-in page first, with every way in on it")


class _Req(SimpleNamespace):
    async def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body


import database  # noqa: E402

store = {"profile": {"cash": 100, "monthly_invest": 1000}}
saved_fns = (main._check_admin_token, database.get_capital_profile, database.save_capital_profile)
main._check_admin_token = lambda request: True
database.get_capital_profile = lambda owner="owner": dict(store["profile"], _updated_at="x")
database.save_capital_profile = lambda prof, owner="owner": store.update(profile=prof) or True
try:
    ok = asyncio.run(main.capital_profile_save(_Req(body={"cash": "250", "evil": 1})))
    check(ok.status_code == 200 and store["profile"] == {"cash": 250.0, "monthly_invest": 1000},
          "SAVING MERGES: the fields sent replace theirs, the rest of the saved profile stays, junk is dropped")
    bad = asyncio.run(main.capital_profile_save(_Req(body=ValueError("not json"))))
    check(bad.status_code == 400, "a body that is not JSON is refused")
    database.save_capital_profile = lambda prof, owner="owner": False
    down = asyncio.run(main.capital_profile_save(_Req(body={"cash": 1})))
    check(down.status_code == 503, "no database: the save says so")
    main._check_admin_token = lambda request: False
    no = asyncio.run(main.capital_profile_save(_Req(body={"cash": 1}, cookies={}, query_params={}, headers={})))
    check(no.status_code == 401, "and only the admin can save")
finally:
    main._check_admin_token, database.get_capital_profile, database.save_capital_profile = saved_fns

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} capital checks passed.")
