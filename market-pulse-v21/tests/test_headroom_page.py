"""Headroom phase 1 (headroom.py, templates/headroom.html): the house-hack
offer is the lowest of building value, cash flow after vacancy and repairs,
FHA's 75% test and the budget; rents are the ladder's, labelled; the market
board's rents are Zillow's only; the aggregates cache is keyed on content and
written atomically; the solved-board cache is bounded; the page names its
research tables' dates and never calls a modelled fixer price an observed one.

Run: python tests/test_headroom_page.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import headroom as HR  # noqa: E402
import househack as HH  # noqa: E402
from value_add import METRO_GEO  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# THE HOUSE-HACK OFFER
# ══════════════════════════════════════════════════════════════════
RATE = 7.03


def independent(offer, rent, median, code, units=4, scope="cosmetic", level="low"):
    """PITI and surplus at a price, worked out from the stated rules."""
    sc = HR.state_costs(code)
    R = HR._hh_rehab(code, units, scope, level)
    base = 0.965 * (offer + R)
    pi = HR._pmt(base * 1.0175, RATE / 100)
    piti = pi + base * 0.0055 / 12 + sc["proptax"] * offer / 12 \
        + sc["ins_landlord"] * HH.UNIT_INSURANCE_FACTOR[units] / 12
    surplus = (units - 1) * rent * 0.92 - 0.015 * (offer + R) / 12 - piti
    return piti, surplus


cheap = HR.house_hack_max_offer(62_790, 1_014, "PA", units=4, rate_pct=RATE, max_price=300_000)
check(cheap["binding"] == "value" and cheap["max_offer"] == round(62_790 * 1.85)
      and cheap["est_value"] == round(62_790 * 1.85) and cheap["binding_label"] == "building value",
      "A $62,790 TOWN GETS AN OFFER AT WHAT A 4-UNIT THERE IS LIKELY WORTH (1.85x), not $300k of cash-flow ceiling")
p, s = independent(cheap["max_offer"], 1_014, 62_790, "PA")
check(abs(cheap["piti"] - p) <= 1 and abs(cheap["monthly_surplus"] - s) <= 1,
      "PITI carries the 1.75% upfront MIP financed and 0.55% annual MIP; surplus is after 8% vacancy and repairs")
check(cheap["vacancy"] == round(3 * 1_014 * 0.08) and cheap["rent_offset"] == 3 * 1_014,
      "vacancy is 8% of the other units' rent")

dear = HR.house_hack_max_offer(400_000, 1_500, "OH", units=4, rate_pct=RATE, max_price=900_000)
p, s = independent(dear["max_offer"], 1_500, 400_000, "OH")
check(dear["binding"] == "live_free" and abs(dear["monthly_surplus"]) <= 1 and abs(s) <= 1,
      "WHERE RENTS ARE THIN THE CASH-FLOW LIMIT SETS THE OFFER: zero surplus after vacancy, repairs and PITI")
check(dear["fha_75_passes"] is True, "at a cash-flow-limited offer FHA's 75% test passes too (it is the looser rule)")

budget = HR.house_hack_max_offer(62_790, 1_014, "PA", units=4, rate_pct=RATE, max_price=60_000)
check(budget["binding"] == "budget" and budget["max_offer"] == 60_000 and budget["capped_at_budget"],
      "the user's budget can set the offer, and says so")
duplex = HR.house_hack_max_offer(200_000, 1_400, "OH", units=2, rate_pct=RATE, max_price=900_000)
check(duplex["fha_75_passes"] is None and duplex["est_value"] == round(200_000 * 1.25),
      "a duplex has no 75% test and is valued at 1.25x")
check(HR.house_hack_max_offer(400_000, 100, "OH", units=4, rate_pct=RATE) is None,
      "rents that carry no price give no offer")
ins_ratio = (HR.house_hack_max_offer(62_790, 1_014, "PA", units=3, rate_pct=RATE, max_price=60_000)["piti"]
             - HR.house_hack_max_offer(62_790, 1_014, "PA", units=2, rate_pct=RATE, max_price=60_000)["piti"])
check(ins_ratio != 0, "insurance and remodel differ by unit count")

# ══════════════════════════════════════════════════════════════════
# A SMALL ZIPS.DB
# ══════════════════════════════════════════════════════════════════
tmp = Path(tempfile.mkdtemp())
DB = tmp / "zips.db"
HIST = json.dumps([200_000 + 500 * i for i in range(60)])
COLS = ("zip", "name", "state", "lat", "lng", "population", "median_home_value", "median_rent_monthly",
        "rent_tier", "median_household_income", "history_zhvi", "as_of", "rent_as_of")


def make_db(rows):
    if DB.exists():
        DB.unlink()
    c = sqlite3.connect(DB)
    c.execute(f"CREATE TABLE zips ({', '.join(COLS)})")
    c.executemany(f"INSERT INTO zips VALUES ({', '.join('?' * len(COLS))})", rows)
    c.commit()
    c.close()


def zrow(z, state, value, rent, tier, lat=0.0, lng=0.0, pop=8_000, income=60_000, name=None):
    return (z, name or f"Town{z}", state, lat, lng, pop, value, rent, tier, income, HIST, "2026-10-01", "2026-10-01")


ROWS = [zrow(f"43{i:03d}", "OH", 200_000, 1_000 + 10 * i, "zori") for i in range(12)]
ROWS += [zrow(f"44{i:03d}", "OH", 200_000, 5_000, "fmr") for i in range(5)]          # would lift a blended median
ROWS += [zrow("45000", "OH", 204_000, 1_000, "zori")]                               # a real rent AT value ÷ 204
ROWS += [zrow(f"46{i:03d}", "IN", 150_000, 1_100, "zori") for i in range(9)]          # too few Zillow ZIPs
ROWS += [zrow("47000", "OH", 90_000, 1_200, "fmr", income=20_000),                    # a HUD rent that strains income
         zrow("47001", "OH", 90_000, 1_150, None)]                                    # nothing measured
lat0, lng0, _rad = METRO_GEO["OH-CLE"]
ROWS += [zrow("48000", "OH", 180_000, 1_600, "zori", lat=lat0, lng=lng0, pop=20_000),
         zrow("48001", "OH", 120_000, 1_500, "fmr", lat=lat0, lng=lng0, pop=20_000)]
make_db(ROWS)
AGG = tmp / "agg" / "market_aggregates.json"
AGG.parent.mkdir()

aggs = HR.market_aggregates(db=DB, path=AGG)
zori_rents = sorted([1_000 + 10 * i for i in range(12)] + [1_000])
check(aggs["OH"]["n_zori"] == 13 and aggs["OH"]["rent"] == zori_rents[6],
      "THE MARKET RENT IS ZILLOW'S ONLY — HUD's $5,000 rows are not in the median; a real rent at value÷204 is")
check(aggs["IN"]["rent"] is None and aggs["IN"]["n_zori"] == 9,
      "a market with fewer than 10 Zillow ZIPs has no rent (and so no board row)")
check("OH-CLE" in aggs and aggs["OH-CLE"]["n_zori"] == 1, "ZIPs go to their metro when inside its radius")
saved = json.loads(AGG.read_text())
check(saved["_key"].startswith(f"v{HR.AGG_VERSION}:") and saved["markets"] == aggs
      and not [f for f in AGG.parent.iterdir() if f.suffix == ".tmp"],
      "the cache is written whole, under a content key, with no temp file left behind")
before = AGG.stat().st_mtime_ns
time.sleep(0.01)
check(HR.market_aggregates(db=DB, path=AGG) == aggs and AGG.stat().st_mtime_ns == before,
      "an unchanged database reads the cache and does not rewrite it")
os.utime(DB, None)                                    # a fresh checkout: new file time, same content
check(HR.market_aggregates(db=DB, path=AGG) == aggs and AGG.stat().st_mtime_ns == before,
      "A NEW FILE TIME WITH THE SAME CONTENT STILL HITS THE CACHE (deploys no longer rebuild it)")
c = sqlite3.connect(DB)
c.execute("UPDATE zips SET median_rent_monthly = 2000 WHERE rent_tier = 'zori' AND state = 'OH' AND lat = 0")
c.commit()
c.close()
check(HR.market_aggregates(db=DB, path=AGG)["OH"]["rent"] == 2000, "changed content rebuilds the cache")
check(HR.market_aggregates(db=DB, path=tmp / "missing-dir" / "x.json")["OH"]["rent"] == 2000,
      "an unwritable cache path still returns the answer")
make_db(ROWS)

# ── the drill-down is Zillow-only too ──
drill = HR.zip_drilldown("OH-CLE", rate_pct=RATE, db=DB)
check([d["zip"] for d in drill] == ["48000"],
      "THE DRILL-DOWN RANKS ZILLOW RENTS ONLY: a county HUD figure ÷ a cheap ZIP's value is not a pocket")

# ── the house-hack board ──
board = HR.zip_board_hh(state="OH", units=4, rate_pct=RATE, max_price=300_000, max_tier="high",
                        allow_unknown=True, db=DB, top=100)
by = {z["zip"]: z for z in board}
check("47001" not in by, "a ZIP the rent ladder could not answer for is left out")
check("45000" in by, "a real Zillow rent that happens to sit at value ÷ 204 is kept (the old filter dropped it)")
check(by["44000"]["rent_label"] == "HUD FMR (county)" and by["44000"]["rent_basis"] == "voucher-floor"
      and by["43000"]["rent_label"] == "Zillow ZORI", "EVERY ROW NAMES ITS RENT'S SOURCE")
check(by["47000"]["rent_strains_income"] and not by["43000"]["rent_strains_income"],
      "a HUD rent that takes a large share of local income is flagged; a Zillow one never is")
check(all(z["max_offer"] <= z["est_value"] for z in board) and all(z["monthly_surplus"] >= -1 for z in board),
      "no offer above the building's estimated value, no negative surplus")
check(all("_history" not in z and "trajectory" in z for z in board), "trend computed for the rows shown")
check(HR.zip_markets(DB) is HR.zip_markets(DB), "ZIP-to-market assignment is computed once per database")

# ══════════════════════════════════════════════════════════════════
# THE SOLVED-BOARD CACHE IS BOUNDED
# ══════════════════════════════════════════════════════════════════
small = HR.market_aggregates(db=DB, path=AGG)
_orig = HR.market_aggregates
HR.market_aggregates = lambda force=False: {"OH": {**small["OH"], "population": 200_000},
                                            "IN": {**small["IN"], "population": 200_000}}
try:
    HR._BOARD_CACHE.clear()
    first = HR.build_board(target=10.0, rate_pct=RATE)
    check([r["code"] for r in first] == ["OH"], "a market with no rent is left off the board")
    check(HR.build_board(target=10.0, rate_pct=RATE) is first, "a repeat request is served from the cache")
    for i in range(HR.BOARD_CACHE_MAX + 5):
        HR.build_board(target=11.0 + i / 10, rate_pct=RATE)
    check(len(HR._BOARD_CACHE) == HR.BOARD_CACHE_MAX, "THE CACHE NEVER HOLDS MORE THAN ITS LIMIT")
    check(next(iter(HR._BOARD_CACHE))[3] != 10.0, "the least recently used board is the one evicted")
finally:
    HR.market_aggregates = _orig
    HR._BOARD_CACHE.clear()

# ══════════════════════════════════════════════════════════════════
# THE PAGE
# ══════════════════════════════════════════════════════════════════
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")))
env.globals.update(is_admin=lambda r: False, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/headroom"), query_params={})
FRESH = [{"layer": "calibration", "as_of": "2026-07", "stale": False},
         {"layer": "financing", "as_of": "2026-03", "stale": True}]
common = {"request": req, "scope": "cosmetic", "level": "low", "target": 14.0, "rate": RATE, "sqft": 1500.0,
          "xadj": 0.0, "universe": "all", "calib": HR.calibration("cosmetic"), "fin": HR.financing_terms(RATE),
          "profit_floor": HR.PROFIT_FLOOR, "hold_years": HR.HOLD_YEARS, "freshness": FRESH}
t = env.get_template("headroom.html")
hh_html = t.render(**common, mode="hh", board=[], hh_board=board, n_primed=0, metro="", drill=None, metro_name="",
                   hstate="OH", units=4, maxprice=300_000.0, safetier="high", allow_unknown=True,
                   crime_cov={"with_data": 1, "cities": 2}, us_violent=364.0, vacancy=HR.VACANCY,
                   maint=HR.MAINTENANCE_PCT, unit_factor=HH.UNIT_PRICE_FACTOR[4])
for needle in ("Set by", "Est. value", "building value", "HUD FMR (county)", "Zillow ZORI", "8% vacancy",
               "1.75% upfront MIP", "× 1.85", "financing 2026-03", "STALE"):
    check(needle in hh_html, f"the house-hack page shows '{needle}'")
check("supports more than your budget" not in hh_html, "the old '+ supports more' claim is gone")
board_rows = [{**HR.market_headroom("OH", 200_000, 1_400, 0.02, scope="cosmetic", rehab_total=30_000),
               "name": "Ohio", "median_value": 200_000, "rent": 1_400, "population": 1, "n_zips": 3,
               "n_zori": 3, "cagr3_pct": 2.0, "trajectory": "steady", "competition": "BALANCED",
               "median_psf": 133.0}]
bd_html = t.render(**common, mode="brrrr", board=board_rows, n_primed=0, metro="", drill=None, metro_name="")
for needle in ("modelled fixer price", "Modelled fixer $/sf", "not sales observed", "Zillow ZORI asking rents only",
               "All markets"):
    check(needle in bd_html, f"the board shows '{needle}'")
for bad in ("actually trade for", "villain", "Fixer ask", "fixer ask", "All 107 markets", ">None<", "nan%"):
    check(bad not in bd_html and bad not in hh_html, f"no '{bad}' on the page")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} headroom page checks passed.")
print("   house-hack offer = min(value, cash flow after vacancy + repairs, FHA 75%, budget); rents labelled;"
      " board rents Zillow-only; caches keyed and bounded")
