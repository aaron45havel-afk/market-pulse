"""The monthly plan, month by month (timeline.py), and what it fixed: property
returns annualized over the hold (capital.hold_return), a debt paid early
saving only the months until pay would clear it (capital.debt_hold_rate),
and the Monthly plan tab.

Every expected number is worked by hand from the rules — a month's interest
at the APR, the waterfall's order, room reset in January — not read back
from the code.

Run: python tests/test_timeline.py
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import capital as K  # noqa: E402
import capital_view as V  # noqa: E402
import timeline as T  # noqa: E402

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
PROF = {"monthly_invest": 6000, "monthly_expenses": 3000, "home_state": "CA", "housing_cost": 2800, "hold_years": 15,
        "mortgage_rate": 6.3, "age": 34, "ira_room": 7500, "market_return": 8, "picks_n": 3,
        "debts": [{"name": "Visa", "balance": 3500, "apr": 23}, {"name": "Store", "balance": 1000, "apr": 23},
                  {"name": "Personal", "balance": 1800, "apr": 20, "payment": 90},
                  {"name": "Car", "balance": 9000, "apr": 5, "payment": 300}],
        "holdings": "AAA, taxable, 20000, 25000\nBBB, taxable, 10000, 9000"}


def build(prof, src=None):
    return K.build(prof, today=OCT, sources=src or SRC)


B = build(PROF)
tl = B["timeline"]
mo = tl["months"]
ms = {(m["kind"], m.get("name")): m for m in tl["milestones"]}

# ── month by month ─────────────────────────────────────────────────
check(mo[0]["label"] == "Oct 2026" and mo[0]["total"] == 6000
      and mo[0]["by_category"] == {"Savings": 3000.0, "Debt": 3000.0},
      "THIS MONTH: the $3,000 cushion (a month of expenses) first, then $3,000 to the dearest card")
visa, store = 500 * (1 + 23 / 1200), 1000 * (1 + 23 / 1200)
personal = 1800 * (1 + 20 / 1200) - 90
check(near(mo[1]["by_category"]["Debt"], round(visa + store + personal, 2)) and mo[1]["total"] == 6000,
      "NOVEMBER: each card takes a month's interest at its APR, the personal loan its $90 payment from expenses, then the plan "
      "clears all three (the month's amount is what came in, before anything it frees)")
check(near(mo[1]["by_category"]["Savings"], round(6000 - (visa + store + personal), 2)),
      "and the rest starts the emergency fund")
check(mo[2]["total"] == 6090 and mo[2]["by_category"] == {"Savings": 6090.0},
      "DECEMBER: the personal loan's $90 payment joins the monthly amount; all of it fills the emergency fund")
ef_left = 18000 - 3000 - (6000 - (visa + store + personal)) - 6090 - 6090
ira = 7500 / 11
check(near(mo[4]["by_category"]["Savings"], round(ef_left, 2)) and near(mo[4]["by_category"]["Roth IRA"], round(ira, 2)),
      "FEBRUARY: the last of the $18,000 fund, then the Roth IRA's room for the year spread over the 11 months left")
picks = round((6090 - ef_left - ira) * 0.2, 2) * 3
check(near(mo[4]["by_category"]["Stock picks"], picks, 0.02),
      "then the board's best use: three picks at the 20% per-name cap each")
check(ms[("debt", "Personal")]["label"] == "Nov 2026" and "$90/mo payment joins" in ms[("debt", "Personal")]["detail"]
      and ms[("cushion", None)]["label"] == "Oct 2026" and ms[("ef", None)]["label"] == "Feb 2027",
      "MILESTONES: dated, each saying what it changes")
car = next(r for r in B["rows"] if r["id"] == "debt:Car")
check(("debt", "Car") not in ms and car["detail"]["payoff_months"] is None,
      "a 5% car loan is not paid early (below the market return); its own payments run past the window")
jan = next(m for m in mo if m["date"] == "2028-01")
check(near(jan["by_category"]["Roth IRA"], 625.0), "JANUARY: the room resets — $7,500 over twelve months")
check(tl["steady"] and tl["steady"]["from_label"] == "Mar 2027" and tl["steady"]["date"] == "2028-01"
      and near(tl["steady"]["per_year"], round(sum(s["amount"] * s["ret"] / 100 for s in jan["steps"])
                                               + jan["left"] * T._tbill(B["profile"]) / 100, 2) * 12, 1),
      "STEADY from the first month the split holds once every one-time goal is met — shown as a January (a normal "
      "year's room), with what it earns a year after tax")

# ── a property: saved for, closed, moved into ─────────────────────
HH = [{"zip": "94510", "place": "Benicia, CA", "state": "CA", "max_offer": 700_000, "cash_to_close": 35_000,
       "monthly_surplus": -600}]
LOW = [{**c, "expected": c["expected"] - 16} for c in COMP]
R = build({**PROF, "debts": [], "holdings": "Chk, checking, 30000, 4%"}, {**SRC, "compounders": LOW, "house_hack": HH})
rt = R["timeline"]
rms = {m["kind"]: m for m in rt["milestones"]}
check(R["plan"]["winner"]["id"] == "hh:94510" and rt["deal"]["cash"] == 35_000,
      "the board's best use is the house hack — the plan saves for its cash to close")
check(rms["dp_start"]["label"] == "Oct 2026" and "$12,000 of cash above the emergency fund" in rms["dp_start"]["detail"],
      "CASH ABOVE THE EMERGENCY FUND starts the down payment ($30,000 held, $18,000 kept)")
first = rt["months"][0]
check(near(first["by_category"]["Down payment"], 6000 - 7500 / 3) and near(first["by_category"]["Roth IRA"], 2500),
      "each month: the Roth IRA's room (October: $7,500 over three months), the rest to the down payment")
ready = rms["dp_ready"]
check(ready["label"] == "Mar 2027" and rms["move_in"]["label"] == "Jun 2027",
      "READY once the fund holds the cash to close ($12,000 + 3 × $3,500 + 2 × $5,375 + $1,750 = $35,000); "
      "CLOSE AND MOVE IN three months later")
check("$2,800/mo rent stops" in rms["move_in"]["detail"] and "cost $600/mo" in rms["move_in"]["detail"]
      and "+$2,200/mo" in rms["move_in"]["detail"],
      "at move-in the rent stops and the other units' shortfall after the full payment starts: +$2,200 a month")
after = next(m for m in rt["months"] if m["date"] == "2027-06")
check(after["total"] == 8200 and "Down payment" not in after["by_category"] and after["by_category"].get("Stock picks", 0) > 3000
      and not any(m["by_category"].get("Down payment") for m in rt["months"][6:]),
      "AFTER: $8,200 a month to the next best use (the picks), and no second down payment (one deal at a time)")
check(rt["steady"]["from_label"] == "Jun 2027" and rms["steady"]["label"] == "Jun 2027", "steady from the move-in")
dp_total = sum(m["by_category"].get("Down payment", 0) for m in rt["months"])
check(near(dp_total + 12_000, 35_000, 0.02), "the fund is filled exactly — the month it fills, the rest is left over")

# ── a loan that ends on its own schedule; a cheap debt the board ranks top
own = build({**PROF, "debts": [{"name": "Loan", "balance": 900, "apr": 6, "payment": 300}]})["timeline"]
paid = next(m for m in own["milestones"] if m.get("name") == "Loan")
check(paid["label"] == "Feb 2027" and own["months"][4]["total"] == 6300 and own["months"][3]["total"] == 6000,
      "A LOAN'S OWN PAYMENTS clear it ($900 at 6% less $300 a month leaves $9.06 after January: gone in February), "
      "and its $300 joins the monthly amount")
oms = [m["kind"] for m in own["milestones"]]
check(oms.index("steady") > oms.index("ef") and own["steady"]["from_label"] == "Feb 2027",
      "STEADY never comes before the emergency fund is full — and starts once the loan's $300 is in the split")
cheap = build({**PROF, "debts": [{"name": "Cheap", "balance": 1000, "apr": 7.5}], "holdings": "Chk, checking, 18000, 1%",
               "k401_room": 0},
              {**SRC, "compounders": []})
ct = cheap["timeline"]
check(cheap["plan"]["winner"]["id"] == "debt:Cheap" and near(ct["months"][0]["by_category"]["Debt"], 1000)
      and near(ct["months"][0]["left"], 6000 - 3000 * 0 - 1000 - 7500 / 3, 0.02),
      "a 7.5% debt the board ranks first takes its $1,000 balance, not the whole month — the rest is left over")

# ── never steady inside ten years ──────────────────────────────────
slow = build({**PROF, "monthly_invest": 150, "debts": [{"name": "Big", "balance": 60_000, "apr": 24}],
              "holdings": ""})["timeline"]
check(slow["steady"] is None and slow["horizon"] == T.MAX_MONTHS and len(slow["months"]) >= T.SHOW_MONTHS,
      "$150 a month against $60,000 at 24%: no steady month in ten years, and the page says so")
check(build({**PROF, "monthly_invest": 0})["timeline"] is None, "nothing put aside: no plan")

# ── a debt paid early saves only the months until pay clears it ───
m_ = K.market_after_tax(B["profile"]) / 100
check(near(K.debt_hold_rate(23, None, B["profile"]), 23.0) and near(K.debt_hold_rate(23, 0, B["profile"]), m_ * 100),
      "a debt pay never clears earns its APR for the whole hold; one this month's pay clears earns what money earns")
want = ((1.23 ** 1) * (1 + m_) ** 14) ** (1 / 15) - 1
check(near(K.debt_hold_rate(23, 12, B["profile"]), round(want * 100, 2)),
      "cleared in a year: a year at 23%, fourteen at the market, annualized")
visa_row = next(r for r in B["rows"] if r["id"] == "debt:Visa")
check(visa_row["detail"]["payoff_months"] == 1 and visa_row["detail"]["payoff_label"] == "Nov 2026"
      and visa_row["detail"]["hold_rate"] < 9 and visa_row["ret_after"] == 23,
      "Visa is cleared by November's pay: paid early it saves a month of 23%, not 23% a year for fifteen")
idle = build({**PROF, "holdings": PROF["holdings"] + "\nChk, checking, 30000, 0.5%"})
todo = V.build_view(idle, OCT)["todo"]
check(not [s for s in todo if s["kind"] == "debt"] and [s for s in todo if s["kind"] == "picks"],
      "DO NEXT does not move $12,000 of idle checking to a card this month's pay clears — it goes to the picks")
big = build({**PROF, "monthly_invest": 500, "debts": [{"name": "Big", "balance": 40_000, "apr": 24}],
             "holdings": "Chk, checking, 60000, 0.5%"})
bstep = [s for s in V.build_view(big, OCT)["todo"] if s["kind"] == "debt"]
check(bstep and bstep[0]["title"] == "Pay off Big" and bstep[0]["rank"] == 1 and next(r for r in big["rows"] if r["id"] == "debt:Big")["detail"]["payoff_months"] is None,
      "a debt pay can't clear ($500 a month against $800 of interest) is still the first step, at its full APR")

# ── property: annualized over the hold ─────────────────────────────
row = next(r for r in R["rows"] if r["id"] == "hh:94510")
check(row["detail"]["year_one"] > 60 and row["ret_after"] < 30 and "year one alone" in row["basis"],
      "a house hack returning 60%+ on its cash in year one is under 30% a year over a 15-year hold")
hold = R["current"]["holdings"]
check(hold["opt_pct"] is not None and hold["opt_pct"] < 30,
      "THE OVERVIEW: the optimal path's return is the hold's, not year one compounded")

# ── the page ───────────────────────────────────────────────────────
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")), autoescape=True)
env.globals.update(is_admin=lambda r: True, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/capital"), query_params={})


def render(b):
    return env.get_template("capital.html").render(
        request=req, board=b, p=b["profile"], view=V.build_view(b, OCT), saved=True, updated_at="2026-10-03",
        last_step=None, debts_text="", limits=K.LIMITS_2026, hours_default=K.HOURS_DEFAULT,
        accounts=V.EDIT_ACCOUNT_LABELS)


html = render(R)
check("Your monthly plan" in html and "One-time goals done" in html and ">Apr 2027<" in html,
      "the summary: what is put aside, when the one-time goals are done, then each month")
check(html.count('class="cx-tlb"') == len(rt["months"]) and html.count('role="tabpanel" aria-labelledby="tlb') == len(rt["months"])
      and 'aria-selected="true"' in html, "a bar and a split for every month shown, the first selected")
check("Close and move in" in html and "Down payment ready" in html and "tl-dp" in html,
      "the milestones and the chart's categories are on the page")
check("Once everything" in html and "Add your split" in html, "the steady split, and a way to add the current one")
held = build({**PROF, "debts": [], "holdings": "VTI, taxable, 60000, 60000"},
             {**SRC, "compounders": LOW, "house_hack": HH})
check("Do next can buy it today" in render(held) and "Do next can buy it today" not in html,
      "when Do next would fund the deal from what is held, the plan says so — it saves from pay alone")
slow_html = render(build({**PROF, "monthly_invest": 150, "debts": [{"name": "Big", "balance": 60_000, "apr": 24}],
                          "holdings": ""}))
check("Not within 10 years" in slow_html, "no steady month: said plainly")
visible = re.sub(r"<script.*?</script>", "", html, flags=re.S)
for bad_ in (">None<", "None%", "Undefined", "NaN", "Infinity"):
    check(bad_ not in visible, f"no '{bad_}' leaks into the page")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} monthly-plan checks passed.")
