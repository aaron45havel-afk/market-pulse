"""Housing affordability (affordability.py, templates/housing_affordability.html):
the payment is the ZIP page's, the affordable price solves exactly, the
decomposition adds up, states are like-for-like, and the page renders.

Run: python tests/test_affordability.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import affordability as AF  # noqa: E402
import build_zip_profile as B  # noqa: E402
import re_assumptions as RA  # noqa: E402
import underwrite as U  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# THE ARITHMETIC IS THE ZIP PAGE'S
# ══════════════════════════════════════════════════════════════════
for price in (120_000, 350_000, 900_000):        # below the premium floor, inside, above the cap
    o = U.owner(price, tax_rate_pct=1.2, insurance_annual=RA.scaled_premium(1600, price), rate_pct=7.03)
    check(abs(AF.payment(price, 7.03, 1.2, 1600) - o["monthly_total"]) < 0.01,
          f"THE PAYMENT ON ${price:,} IS underwrite.owner's monthly_total (20% down, tax, insurance)")
    check(abs(AF.ratio(price, 7.03, 90_000, 1.2, 1600) - o["monthly_total"] * 12 / 90_000 * 100) < 1e-3,
          "and payment-to-income is twelve payments over the income")
for income in (25_000, 90_000, 400_000):         # answers in each premium band
    ap = AF.affordable_price(income, 7.03, 1.2, 1600)
    check(ap and abs(AF.ratio(ap, 7.03, income, 1.2, 1600) - 30.0) < 1e-6,
          f"THE AFFORDABLE PRICE FOR ${income:,} TAKES EXACTLY 30% (got {ap and round(ap)})")
check(AF.affordable_price(None, 7.03, 1.2, 1600) is None and AF.ratio(300_000, None, 1, 1, 1) is None,
      "a missing input gives no figure, not a guess")

# ══════════════════════════════════════════════════════════════════
# THE DECOMPOSITION ADDS UP
# ══════════════════════════════════════════════════════════════════
add = AF.shapley(lambda v: 2 * v["a"] + 3 * v["b"], {"a": 1, "b": 1}, {"a": 2, "b": 3})
check(add == {"a": 2.0, "b": 6.0}, "an additive change is credited to each factor exactly")
mul = AF.shapley(lambda v: v["a"] * v["b"], {"a": 1, "b": 1}, {"a": 3, "b": 5})
check(abs(mul["a"] - 6.0) < 1e-9 and abs(mul["b"] - 8.0) < 1e-9,
      "an interaction is split evenly between the factors that make it (Shapley: 2+4 and 4+4)")
MACRO = {"baseline_year": 2019, "pmms": {"2019": 3.94},
         "cpi": {"2019": 255.0, "2024": 313.0, "latest": 334.0, "latest_month": "2026-08"}}
inp = {"price": 300_000, "price19": 230_000, "income": 70_000, "income19": 60_000,
       "tax_rate": 1.1, "ins_300k": 1500}
m = AF.measure(inp, MACRO, 7.0)
check(abs(sum(m["parts"].values()) - m["change_pts"]) < 1e-9,
      "THE FOUR PARTS SUM TO THE WHOLE CHANGE — prices, rate, incomes, insurance")
check(m["parts"]["price"] > 0 and m["parts"]["rate"] > 0 and m["parts"]["income"] < 0,
      "dearer homes and a higher rate raise the share; higher incomes lower it")
check(abs(m["income_today"] - 70_000 * 334 / 313) < 1e-6,
      "TODAY'S CENSUS INCOME (2024 DOLLARS) IS BROUGHT TO TODAY'S DOLLARS BY CPI")
check(abs(m["ratio19"] - AF.ratio(230_000, 3.94, 60_000, 1.1, 1500, 255 / 334)) < 1e-9,
      "2019 uses 2019's price, rate and income, and insurance deflated to 2019 by CPI")
check(abs(m["gap_pct"] - (300_000 / m["affordable"] - 1) * 100) < 1e-9, "the gap is price over affordable price")
no19 = AF.measure({**inp, "price19": None}, MACRO, 7.0)
check(no19["ratio"] == m["ratio"] and "ratio19" not in no19 and "parts" not in no19,
      "no 2019 price: today stands, 2019 and the change are not invented")
check(AF.measure(inp, MACRO, None) is None and AF.measure(inp, None, 7.0) is None,
      "no mortgage rate or no CPI constants: nothing is computed")
check(AF.macro_constants({**MACRO, "cpi": {"2019": 255.0, "latest": 334.0}}) is None,
      "constants without the ACS end year's CPI are incomplete")

# ══════════════════════════════════════════════════════════════════
# THE TYPICAL HOUSEHOLD — like-for-like, weighted by households
# ══════════════════════════════════════════════════════════════════
check(AF.weighted_median([(1, 1), (2, 1), (10, 5)]) == 10 and AF.weighted_median([]) is None,
      "a household-weighted median")


def row(**kw):
    r = {k: None for k in B.COLUMN_NAMES}
    r.update(tax_rate_owner=1.0, ins_owner_300k=1500, zhvi_month="2026-08")
    r.update(kw)
    return r


ROWS = [
    row(zip="44107", state="OH", city="Lakewood", county="Cuyahoga County", households=24_000,
        zhvi=300_000, zhvi_2019=180_000, acs_median_income=69_000, acs19_median_income=53_000),
    row(zip="44101", state="OH", city="Cleveland", county="Cuyahoga County", households=8_000,
        zhvi=90_000, zhvi_2019=60_000, acs_median_income=30_000, acs19_median_income=24_000),
    # today only: no 2019 price — kept for today's map, out of the like-for-like state figure
    row(zip="44999", state="OH", city="Newtown", county="Cuyahoga County", households=50_000,
        zhvi=900_000, acs_median_income=200_000, acs19_median_income=150_000),
    row(zip="10007", state="NY", city="New York", county="New York County", households=4_000,
        zhvi=3_100_000, zhvi_2019=2_990_000, acs_median_income=None,
        acs_flags=json.dumps({"acs_median_income": {"bound": 250001, "side": "top"}}),
        acs19_median_income=224_000),
]
tmp = Path(tempfile.mkdtemp())
DB = tmp / "zip_profile.db"
B.write_db(ROWS, {"built_at": "2026-10-01T18:18Z", "zhvi_last_month": "2026-08",
                  "acs_vintage": "2020-2024 5-year", "acs19_vintage": "2015-2019 5-year",
                  "afford_macro": json.dumps(MACRO)}, DB)
b = AF.build(7.03, DB)
oh = b["states"]["OH"]
check(oh["typical"]["n_zips"] == 2 and oh["typical"]["price"] == 300_000,
      "THE STATE IS ITS TYPICAL HOUSEHOLD OF THE SAME ZIPS IN BOTH YEARS — the 50,000-household ZIP "
      "with no 2019 price stays out, and Lakewood's 24,000 households outweigh Cleveland's 8,000")
check(oh["coverage_pct"] == round(32_000 / 82_000 * 100, 1), "and says what share of households that covers")
ny = next(z for z in b["zips"] if z["zip"] == "10007")
check(ny["inp"]["income"] == 250001 and ny["inp"]["coded"] == "top" and ny["m"]["ratio"] is not None,
      "A '$250,000+' INCOME IS USED AS ITS BOUND AND MARKED — the share is 'at most'")
nw = next(z for z in b["zips"] if z["zip"] == "44999")
check(nw["m"]["ratio"] is not None and "ratio19" not in nw["m"], "a ZIP with no 2019 price still has today")
check(b["nation"]["typical"]["n_zips"] == 3, "the nation is the same rule across every state")
zv = AF.zip_values("change_pts", 7.03, DB)
check(set(zv) == {"44107", "44101", "10007"} and AF.zip_values("ratio", 7.03, DB).get("44999"),
      "the map reads the page's own per-ZIP figures")
check(abs(AF.income_factor(DB) - 334 / 313) < 1e-12, "the ZIP page's income factor is the page's")

# ══════════════════════════════════════════════════════════════════
# THE PAGE
# ══════════════════════════════════════════════════════════════════
INFO = {"OH": {"name": "Ohio", "fips": "39"}, "NY": {"name": "New York", "fips": "36"}}
ctx = AF.page(7.03, "Sep 24, 2026", "oh", INFO, DB)
check(ctx["state"] == "OH" and ctx["picked"]["name"] == "Ohio" and len(ctx["zip_rows"]) == 3,
      "a picked state carries every one of its ZIPs with a figure")
check(AF.page(7.03, "", "ZZ", INFO, DB)["state"] == "" and not AF.page(7.03, "", "", INFO, DB)["zip_rows"],
      "an unknown state is no state, not an error")

from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")))
env.globals.update(is_admin=lambda r: False, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/housing-affordability"), query_params={})
html = env.get_template("housing_affordability.html").render(request=req, **ctx)
for needle in ("Housing affordability", "Payment ÷ income, US", "Why it changed", "Ohio", "2015-2019 5-year",
               "2020-2024 5-year", "Sep 24, 2026", "Shapley", "aff_change&st=OH", "fair value"):
    check(needle in html, f"the page shows '{needle}'")
for bad in (">None<", " None ", "nan%", "undefined"):
    check(bad not in html, f"no '{bad}' leaks into the page")
bare = env.get_template("housing_affordability.html").render(
    request=req, **{**ctx, "const": None, "nation": None})
check("could not be computed" in bare, "missing constants: the page says so instead of printing zeros")

import main  # noqa: E402

r = asyncio.run(main.fair_value_redirect(state="tx"))
check(r.status_code == 301 and r.headers["location"] == "/housing-affordability?state=TX",
      "/fair-value?state=tx MOVES PERMANENTLY to the affordability page, keeping the state")
r = asyncio.run(main.fair_value_redirect(state="<script>"))
check(r.headers["location"] == "/housing-affordability", "and drops a junk state")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} housing-affordability checks passed.")
print("   the ZIP page's payment; exact 30% price; parts sum to the change; like-for-like typical household")
