"""What if (whatif.py and /api/capital/move): a move weighed against leaving
the money where it is and against the best use of the same money — the
arithmetic, each destination's account rules, the warnings, saved
scenarios, and carrying a move out. Offline — every page's rows are
passed in.

Run: python tests/test_whatif.py
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
import whatif as W  # noqa: E402

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
PROF = {"monthly_invest": 4000, "monthly_expenses": 5000, "salary": 150_000, "match_pct": 4, "k401_room": 20_000,
        "home_state": "CA", "hourly_value": 75, "mortgage_rate": 7.0, "picks_n": 3, "pick_max_pct": 50,
        "debts": [{"name": "Card", "balance": 4000, "apr": 22.9}],
        "holdings": "Checking, checking, 60000, 0.01%\nVTI, taxable, 50000, 30000\nNKE, taxable, 6000, 9000\n"
                    "AAPL, roth, 15000\nPlan fund, 401k, 80000\nHSA cash, hsa, 3000\nNoBasis, taxable, 1000\n"
                    "Mystery, savings, 500",
        "owned_re": [{"name": "Duplex", "use": "rental", "value": 900_000, "loan": 300_000, "rate": 3.1,
                      "payment": 1900, "rent": 5200, "costs": 1500, "basis": 500_000, "year": 2015}],
        "scenarios": [{"id": "a", "name": "VTI to AAA", "src_name": "VTI", "src_account": "taxable", "amount": None,
                       "dest": {"kind": "ticker", "name": "AAA"}},
                      {"id": "b", "name": "Gone", "src_name": "Nope", "src_account": "taxable",
                       "dest": {"kind": "picks"}}]}


def build(prof):
    return K.build(prof, today=OCT, sources=SRC)


B = build(PROF)
KT = W.kit(B)
P = B["profile"]
T = K.tax_rates(P)
H = KT["hold"]
SRCS = {s["label"]: s for s in KT["sources"]}


def ev(label, kind, amount=None, **dest):
    return W.evaluate(KT, {"line": SRCS[label]["line"], "amount": amount, "dest": {"kind": kind, **dest}})


# ── the kit ────────────────────────────────────────────────────────
check(W.account_class("cash") == "taxable" and W.account_class("taxable") == "taxable"
      and W.account_class("k401") == "plan" and W.account_class("roth401k") == "plan"
      and W.account_class("roth") == "sheltered" and W.account_class("ira") == "sheltered"
      and W.account_class("hsa") == "hsa", "where money can go, by the account it is in")
check("Duplex" not in SRCS and SRCS["Mystery"]["h"] is None and len(KT["sources"]) == 8,
      "every holding is a source (property is the Property tab's), the unmeasured ones too, marked")
q = T["qualified"]
check(near(KT["nets"]["index"]["taxable"], K.after_tax_taxable(7.0, A.INDEX_DIV, H, q, q))
      and KT["nets"]["index"]["sheltered"] == 7.0 and near(KT["nets"]["tbill"], 4.0 * (1 - T["tbill"])),
      "an index fund at the market return (taxed in taxable, whole in a Roth); T-bills after federal tax")
sl = A.sleeve_of(B)
check(near(KT["nets"]["picks"]["taxable"], sum(r["ret_net"] for r in sl) / len(sl))
      and set(KT["tickers"]) == {"AAA", "BBB", "CCC"} and KT["sleeve"] == ["AAA", "BBB", "CCC"],
      "the top picks at their average net; every pick on the board by ticker")

# ── the arithmetic ─────────────────────────────────────────────────
r = ev("Checking", "picks", 20_000)
a = KT["nets"]["picks"]["taxable"]
h = SRCS["Checking"]["h"]
check(r["ok"] and near(r["keep_w"], 20_000 * (1 + h / 100) ** H) and near(r["move_w"], 20_000 * (1 + a / 100) ** H)
      and near(r["diff"], r["move_w"] - r["keep_w"]) and near(r["per_year"], 20_000 * (a - h) / 100),
      "KEEP vs MOVE over the hold, and the difference a year — the board's arithmetic")
check(r["verdict"] == "better" and r["payback"] == 0.0 and r["tax"] == 0, "cash into the picks: better, nothing to earn back")
v = ev("VTI", "ticker", None, name="AAA")
tau = 20_000 * q / 50_000
check(near(v["tax"], 20_000 * q, 1) and near(v["proceeds"], 50_000 * (1 - tau), 1)
      and near(v["a"], KT["tickers"]["AAA"]["taxable"]),
      "SELLING VTI PAYS TAX ON ITS $20,000 GAIN; the rest buys AAA at AAA's own net")
pb = math.log(1 / (1 - tau)) / math.log((1 + v["a"] / 100) / (1 + v["h"] / 100))
check(near(v["payback"], pb, 1e-6), "and the years it takes to earn that tax back")
same = ev("Plan fund", "index")
check(same["verdict"] == "same" and abs(same["diff"]) < 1, "a plan fund into the plan's index fund: about the same")
worse = ev("VTI", "ticker", 10_000, name="ZZZZ")
check(worse["verdict"] == "worse" and worse["diff"] < 0 and worse["payback"] is None,
      "VTI into a stock on no screen (the market return, less tax and research time): worse, never earned back")
band = W.SAME_BAND * 10_000
check(worse["diff"] <= -band, "worse means behind by at least 1% of the money over the hold")

# ── account rules ──────────────────────────────────────────────────
check(not ev("AAPL", "tbill")["ok"] and "retirement account" in ev("AAPL", "tbill")["error"],
      "money in a Roth does not go to T-bills")
check(not ev("AAPL", "debt", name="Card")["ok"] and "withdrawal" in ev("AAPL", "debt", name="Card")["error"],
      "nor pays a debt — that is a withdrawal")
rp = ev("AAPL", "picks")
check(rp["ok"] and near(rp["a"], KT["nets"]["picks"]["sheltered"]), "inside a Roth the picks earn their pre-tax net")
hs = ev("HSA cash", "index")
check(near(hs["a"], 7.0 * (1 - 0.093)), "inside a California HSA the state's tax on growth comes off")
pk = ev("Plan fund", "ticker", name="AAA")
check(pk["ok"] and any("brokerage window" in w for w in pk["warnings"]), "a ticker inside a 401(k) is flagged")
check(near(ev("Plan fund", "index")["weq"], 1 - T["retire"]), "a 401(k) dollar counts at its after-tax value")
check(not ev("Checking", "custom", name="X")["ok"], "something else needs its return")
cu = ev("Checking", "custom", 10_000, name="Private fund", rate=14)
check(cu["ok"] and near(cu["a"], K.after_tax_taxable(14, 0, H, q, q)) and cu["dest"] == "Private fund",
      "something else: the owner's return, taxed as growth taken at the end")
check(not ev("Mystery", "picks")["ok"] and "rate" in ev("Mystery", "picks")["error"], "cash without a rate cannot be weighed")

# ── amounts and warnings ───────────────────────────────────────────
db = ev("Checking", "debt", 50_000, name="Card")
check(near(db["amount"], 4000) and near(db["proceeds"], 4000) and any("pays it off" in w for w in db["warnings"]),
      "PAYING MORE THAN THE DEBT: the move stops at the balance")
check(db["is_best"] and db["verdict"] == "better", "and paying a 22.9% card is the best use of that money")
big = ev("Checking", "picks", 75_000)
check(near(big["amount"], 60_000) and any("Only $60,000" in w for w in big["warnings"]),
      "more than is there: the move uses all of it, and says so")
check(any("emergency fund" in w for w in big["warnings"]), "moving checking below six months of expenses is flagged")
check(not ev("Checking", "picks", 0)["ok"], "no amount, no move")
nb = ev("NoBasis", "picks")
check(nb["ok"] and nb["tax"] == 0 and any("No cost basis" in w for w in nb["warnings"]),
      "NO BASIS: the tax is not counted and the page says this may look better than it is")
conc = ev("VTI", "ticker", None, name="BBB")
check(any("of your stock picks" in w for w in conc["warnings"]), "one name above the per-name cap is flagged")
nk = ev("NKE", "ticker", name="NKE")
check(any("already is" in w for w in nk["warnings"]) and any("wash-sale" in w for w in nk["warnings"])
      and not any("of your stock picks" in w for w in nk["warnings"]),
      "selling NKE to buy NKE: that is where it already is, and the wash-sale rule")
check(nk["loss_note"] and near(nk["tax"], 0) and "$999" in nk["loss_note"],
      "a sale at a loss says what the loss saves this year (not counted in the verdict)")

# ── the best use of the same money ─────────────────────────────────
fill = W.best_use(KT, "taxable", 20_000)
check([f["label"] for f in fill] == ["Pay down Card", "The top picks"] and near(fill[0]["amount"], 4000),
      "THE BEST USE FILLS THE BOARD BEST FIRST: the card's $4,000, then the picks")
check(r["best"]["label"] == "Pay down Card, then The top picks" and r["shortfall"] > 0 and not r["is_best"],
      "so $20,000 of checking into the picks falls short of paying the card first")
check(near(r["shortfall"], r["best"]["w"] - r["move_w"]), "by exactly the difference at the end of the hold")
keep_best = W.evaluate({**KT, "nets": {**KT["nets"], "picks": {"taxable": 0.5, "sheltered": 0.5},
                                       "index": {"taxable": 0.5, "sheltered": 0.5}}, "debts": []},
                       {"line": SRCS["VTI"]["line"], "amount": None, "dest": {"kind": "index"}})
check(keep_best["best"]["label"] == "Keeping it where it is" and keep_best["verdict"] == "worse",
      "when nothing beats where the money is, keeping it is the best use")
check([f["label"] for f in W.best_use(KT, "plan", 1000)] == ["An index fund"], "a 401(k)'s best use is its index fund")

# ── saved scenarios ────────────────────────────────────────────────
sc = W.parse_scenarios([{"name": "x" * 99, "src_name": "VTI", "src_account": "taxable", "amount": "$1,000",
                         "dest": {"kind": "custom", "name": "Fund", "rate": "250%"}},
                        {"name": "bad kind", "src_name": "VTI", "dest": {"kind": "lottery"}},
                        {"name": "no source", "dest": {"kind": "picks"}}, "junk"] + [
                       {"src_name": "VTI", "dest": {"kind": "picks"}}] * 30)
check(len(sc) == 20 and len(sc[0]["name"]) == 60 and sc[0]["amount"] == 1000 and sc[0]["dest"]["rate"] == 100
      and sc[1]["amount"] is None and sc[1]["name"] == "A move" and all(s["id"] for s in sc),
      "scenarios: names cut to 60, money strings read, a return clamped, blank = all, junk dropped, at most 20")
saved = W.scenarios(B, KT)
check(saved[0]["result"]["ok"] and near(saved[0]["result"]["per_year"], v["per_year"])
      and saved[1]["result"]["error"] == "That holding is no longer in your profile.",
      "A SAVED MOVE IS WEIGHED AGAIN against today's profile; one whose holding is gone says so")
check(K.parse_profile({"scenarios": [{"src_name": "VTI", "dest": {"kind": "index"}}]})["scenarios"][0]["dest"]["kind"] == "index",
      "the profile saves them")

# ── carrying a move out ────────────────────────────────────────────
mv = W.as_move(v)
prof = A.apply_moves(PROF, [mv], B)
lines = prof["holdings"].splitlines()
check(not any(x.startswith("VTI") for x in lines)
      and f"AAA, taxable, {A._n(round(v['proceeds'], 2))}, {A._n(round(v['proceeds'], 2))}" in lines,
      "VTI sold, AAA bought with what is left after tax, at its cost")
cp = A.apply_moves(PROF, [W.as_move(cu)], B)
check(f"Private fund, taxable, 10000, 10000, 14%" in cp["holdings"] and "Checking, cash, 50000, 0.01%" in cp["holdings"],
      "something else lands with the owner's return, and checking is drawn down")
ip = A.apply_moves(PROF, [W.as_move(ev("Checking", "index", 5000))], B)
check("Index fund, taxable, 5000, 5000" in ip["holdings"], "cash into an index fund lands in taxable, not in cash")
rp_p = A.apply_moves(PROF, [W.as_move(rp)], B)
check("AAA, roth, " in rp_p["holdings"] and "AAPL, roth" not in rp_p["holdings"], "a Roth move stays in the Roth")
dp = A.apply_moves(PROF, [W.as_move(db)], B)
check(dp["debts"] == [] and "Checking, cash, 56000, 0.01%" in dp["holdings"], "paying the card off removes it")

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
    ok = asyncio.run(main.capital_move(_Req(body={"move": {"src_name": "VTI", "src_account": "taxable",
                                                           "dest": {"kind": "ticker", "name": "AAA"}}})))
    check(ok.status_code == 200 and "AAA, taxable" in STORE["owner"]["holdings"]
          and STORE["owner"]["last_step"]["title"].startswith("What if: $50,000 from VTI")
          and STORE["owner:undo"]["holdings"] == PROF["holdings"],
          "APPLY: the move is made in the profile, named on the banner, with the profile before it kept for undo")
    gone = asyncio.run(main.capital_move(_Req(body={"move": {"src_name": "VTI", "src_account": "taxable",
                                                             "dest": {"kind": "picks"}}})))
    check(gone.status_code == 409, "the same move again: VTI is gone, so it is refused")
    bad = asyncio.run(main.capital_move(_Req(body={"move": "x"})))
    check(bad.status_code == 400, "a move that is not a move: 400")
    nope = asyncio.run(main.capital_move(_Req(body={"move": {"line": 3, "dest": {"kind": "tbill"}}})))
    check(nope.status_code == 409, "a move the account rules forbid is refused")
    main._check_admin_token = lambda request: False
    no = asyncio.run(main.capital_move(_Req(body={"move": {}}, cookies={}, query_params={}, headers={})))
    check(no.status_code == 401, "only the admin can apply a move")
finally:
    main._check_admin_token, database.get_capital_profile, database.save_capital_profile, K.build = saved_fns

# ── the page ───────────────────────────────────────────────────────
import capital_view as V  # noqa: E402
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")), autoescape=True)
env.globals.update(is_admin=lambda r: True, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/capital"), query_params={})
html = env.get_template("capital.html").render(
    request=req, board=B, p=P, view=V.build_view(B, OCT), saved=True, updated_at="2026-10-03", last_step=None,
    debts_text=K.debts_text(P["debts"]), limits=K.LIMITS_2026, hours_default=K.HOURS_DEFAULT,
    accounts=V.EDIT_ACCOUNT_LABELS)
check('id="v-whatif"' in html and 'id="wiKit"' in html and 'id="wiSrc"' in html and "Try a move" in html,
      "the What if tab, its calculator and its data, and a way in from Do next")
check(f'data-py-year="{v["per_year"]:.2f}"' in html and "That holding is no longer in your profile." in html,
      "saved moves carry the server's numbers (the browser check compares the live calculator to them)")
import re  # noqa: E402
visible = re.sub(r"<script.*?</script>", "", html, flags=re.S)
for bad_ in (">None<", "None%", "Undefined", "NaN", "Infinity"):
    check(bad_ not in visible, f"no '{bad_}' leaks into the page")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} what-if checks passed.")
