"""Market conditions (conditions.py, templates/conditions.html) and the state
listing history it reads (scripts/zip_market.parse_rdc_history, written by
build_zip_profile): one measure at a time, each against the same month a
year earlier, no blended score, Redfin's sales figures dated and frozen.

Run: python tests/test_conditions.py
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import build_zip_profile as B  # noqa: E402
import conditions as CD  # noqa: E402
import zip_market as ZM  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


HEAD = ("month_date_yyyymm,state,state_id,median_listing_price,active_listing_count,median_days_on_market,"
        "new_listing_count,price_reduced_share,pending_listing_count,median_listing_price_per_square_foot,"
        "pending_ratio,quality_flag")


def state_row(ym, name, sid, price, active, dom, new, cut, pending, ppsf, ratio, q=0):
    return f"{ym},{name},{sid},{price},{active},{dom},{new},{cut},{pending},{ppsf},{ratio},{q}"


def months(start_y, start_m, n):
    out = []
    for i in range(n):
        t = start_y * 12 + start_m - 1 + i
        out.append(f"{t // 12}{t % 12 + 1:02d}")
    return out


MS = months(2024, 6, 28)            # 2024-06 … 2026-09
lines = [HEAD]
for i, ym in enumerate(MS):
    # Ohio: days on market rise 1 a month; price cuts 20% → 0.20 + 0.001/month
    lines.append(state_row(ym, "Ohio", "OH", 250000 + 1000 * i, 10000 + 100 * i, 40 + i, 3000, 0.20 + 0.001 * i,
                           4000, 160, 0.40 - 0.002 * i))
    if ym != "202609":              # Montana misses the latest month
        lines.append(state_row(ym, "Montana", "MT", 600000, 6000, 70, 1500, 0.18, 1600, 320, 0.27, 1 if ym == "202608" else 0))
lines.append("Note: figures are listings, not sales,,,,,,,,,,")
STATE_CSV = "\n".join(lines)
hist = ZM.parse_rdc_history(STATE_CSV, "state_id")
oh = hist["OH"]
check(set(hist) == {"OH", "MT"}, "states keyed by their two letters; the note row is skipped")
check(len(oh) == ZM.STATE_MONTHS and oh[0]["month"] == "2024-09" and oh[-1]["month"] == "2026-09",
      "THE LAST 25 MONTHS, OLDEST FIRST — this month and the same month a year earlier, with every month between")
check(oh[-1]["price_cut_pct"] == round((0.20 + 0.001 * 27) * 100, 2) and oh[-1]["dom"] == 67,
      "a share is stored as percent; days on market as published")
US_CSV = ("month_date_yyyymm,country,median_listing_price,active_listing_count,median_days_on_market,"
          "new_listing_count,price_reduced_share,pending_listing_count,median_listing_price_per_square_foot,"
          "pending_ratio,quality_flag\n202609,United States,419250.0,1161615,61,394830,0.2081,422936,223.0,0.3641,0\n"
          "202509,United States,425000.0,1101979,62,397400,0.1994,440830,227.0,0.40,0")
us = ZM.parse_rdc_history(US_CSV, "country")
check(set(us) == {"US"} and us["US"][-1]["active"] == 1161615, "the national file keys on 'US'")
check(us["US"][-1]["pending_ratio"] == 0.3641, "the pending ratio keeps four decimals (it moves in the third)")
withus = ZM.parse_rdc_history(STATE_CSV + "\n" + state_row("202609", "United States", "US", 1, 1, 1, 1, 0.1, 1, 1, 0.3),
                              "state_id")
check(set(withus) == {"OH", "MT"}, "a 'US' row in the state file is skipped, not merged with the national series")

# ══════════════════════════════════════════════════════════════════
# A DOWNLOAD REPLACES THE TABLE ONLY IF IT LOOKS RIGHT
# ══════════════════════════════════════════════════════════════════
CODES = [chr(65 + i // 26) + chr(65 + i % 26) for i in range(51)]


def rec(month, **kw):
    r = {"month": month, "active": 1000, "new": 300, "pending": 400, "pending_ratio": 0.4, "dom": 50,
         "price_cut_pct": 20.0, "list_price": 400000, "list_ppsf": 200, "quality_flag": 0}
    r.update(kw)
    return r


def fresh(n=51, month="2026-09", us_month="2026-09", **kw):
    return ({c: [rec("2025-09", **kw), rec(month, **kw)] for c in CODES[:n]},
            {"US": [rec("2025-09"), rec(us_month)]})


PRIOR = [{"geo": "OH", "month": "2026-08"}]
calls = []


def prior_rows():
    calls.append(1)
    return PRIOR


def carry(fetch, prior_month=""):
    return B.state_market_or_carry(fetch, prior_rows, prior_month)


good, carried = carry(lambda: fresh())
check(not carried and len(good) == 52 * 2 and not calls, "a complete download replaces the table")
check(B.check_state_history(*fresh(), "2026-09") == "2026-09", "the same month as last build is fine (a rebuild)")


def boom():
    raise OSError("connection reset")


for why, fetch, pm in (
        ("the download fails", boom, ""),
        ("fewer than 51 states", lambda: fresh(n=50), ""),
        ("the national file is on another month", lambda: fresh(us_month="2026-08"), ""),
        ("the download is older than the table", lambda: fresh(), "2026-10"),
        ("a renamed column leaves a measure blank", lambda: fresh(dom=None), ""),
        ("the nation's measure is blank", lambda: (fresh()[0], {"US": [rec("2026-09", list_price=None)]}), "")):
    rows_, carried = carry(fetch, pm)
    check(carried and rows_ is PRIOR, f"CARRIED FORWARD WHEN {why.upper()}")
lag = fresh()[0]
for c in CODES[:10]:
    lag[c] = lag[c][:1]
rows_, carried = carry(lambda: (lag, fresh()[1]))
try:
    B.check_state_history(lag, fresh()[1])
    reason = ""
except ValueError as e:
    reason = str(e)
check(carried and reason == "only 41 states have 2026-09",
      "carried forward when most states lack the latest month — and the warning says that, not 'blank'")

# ══════════════════════════════════════════════════════════════════
# THE BUILD WRITES THE TABLE
# ══════════════════════════════════════════════════════════════════
tmp = Path(tempfile.mkdtemp())
DB = tmp / "zip_profile.db"
rows = B.state_market_rows(hist, us)
check(len(rows) == 25 + 25 + 2 and all(r["geo"] in ("OH", "MT", "US") for r in rows), "one row per geo and month")
B.write_db([], {"built_at": "2026-10-01T18:18Z", "state_market_carried": ""}, DB, rows)
EMPTY = tmp / "empty.db"
B.write_db([], {"built_at": "x"}, EMPTY)
check(CD.page({}, {}, None, EMPTY)["rows"] == [], "no state history yet: an empty page, not an error")

# ══════════════════════════════════════════════════════════════════
# CHANGES ARE AGAINST THE SAME MONTH A YEAR EARLIER
# ══════════════════════════════════════════════════════════════════
check(CD.change(110, 100, "pct") == 10.0 and CD.change(21.5, 20.0, "pts") == 1.5 and CD.change(67, 55, "diff") == 12,
      "counts and prices in percent, shares in points, days in days")
check(CD.change(None, 1, "pct") is None and CD.change(1, 0, "pct") is None, "no prior month (or a zero base): no change")
INFO = {"OH": {"name": "Ohio", "fips": "39"}, "MT": {"name": "Montana", "fips": "30"}}
RF = {"OH": {"sale_to_list_pct": 99.1, "months_of_supply": 3.4}, "MT": {"sale_to_list_pct": None, "months_of_supply": 5.2}}
ctx = CD.page(INFO, RF, "2026-05-31", DB)
o = next(r for r in ctx["rows"] if r["code"] == "OH")
check(o["month"] == "2026-09" and o["year_ago_month"] == "2025-09",
      "OHIO'S SEPTEMBER IS COMPARED WITH LAST SEPTEMBER, not with August")
check(o["dom"] == 67 and o["dom_chg"] == 12, "days on market: 67, up 12 days on the year (55 then)")
check(o["price_cut_pct_chg"] == 1.2, "a share's change is in points: 24.7% against 23.5%")
check(o["active_chg"] == round((12700 / 11500 - 1) * 100, 1), "inventory's change is a percent")
check(abs(o["pending_ratio_chg"] - (-0.024)) < 1e-9, "the pending ratio's change is in its own unit")
m = next(r for r in ctx["rows"] if r["code"] == "MT")
check(m["month"] == "2026-08" and ctx["stale_months"] == ["MT"],
      "A STATE MISSING THE LATEST MONTH SHOWS ITS OWN MONTH AND IS NAMED, not passed off as current")
check(m["quality_flag"] == 1 and o["quality_flag"] == 0, "Realtor.com's quality flag rides along")
check(m["rf_stl"] is None and m["rf_supply"] == 5.2 and o["rf_stl"] == 99.1,
      "Redfin's May sales figures join by state; a missing one is missing, not dropped with its state")
check(ctx["us"]["active_chg"] == round((1161615 / 1101979 - 1) * 100, 1) and ctx["month_label"] == "Sep 2026"
      and ctx["year_ago_label"] == "Sep 2025" and ctx["redfin_label"] == "May 2026", "the nation and the labels")
ch = ctx["charts"]["OH"]
check(len(ch["months"]) == 13 and ch["months"][-1] == "2026-09" and ch["dom"]["now"][-1] == 67
      and ch["dom"]["prior"][-1] == 55, "the chart pairs each of the last 13 months with the same month a year earlier")
gap = {f"{y}-{mo:02d}": {"dom": y * 100 + mo} for y in (2024, 2025, 2026) for mo in range(1, 13)
       if f"{y}-{mo:02d}" <= "2026-09" and f"{y}-{mo:02d}" != "2026-03"}
gc = CD.chart_series(gap)
i = gc["months"].index("2026-03")
check(gc["months"][0] == "2025-09" and gc["dom"]["now"][i] is None and gc["dom"]["prior"][i] == 202503
      and gc["dom"]["now"][i + 1] == 202604 and gc["dom"]["prior"][i + 1] == 202504,
      "A MISSING MONTH IS A GAP IN THE CHART; the months after it still pair with their own year-ago month")
check(m["month_label"] == "Aug 2026" and m["year_ago_label"] == "Aug 2025" and o["year_ago_label"] == "Sep 2025",
      "a lagging state carries its own months for the tooltip and the detail card")
check(not any("climate" in k or "score" in k for k in CD.MEASURE_KEYS), "no blended score among the measures")

RJ = tmp / "redfin.json"
RJ.write_text('{"_meta": {"primary_period_end": "2026-05-31"}, "overrides": {"OH": {"sale_to_list_pct": 99.1, '
              '"months_of_supply": 3.4, "dom": 30}, "XX": "junk"}}')
rf, period = CD.load_redfin(RJ)
check(period == "2026-05-31" and rf == {"OH": {"sale_to_list_pct": 99.1, "months_of_supply": 3.4}},
      "Redfin's frozen figures load by state with their period end")
check(CD.load_redfin(tmp / "missing.json") == ({}, None), "no Redfin file: dashes, not an error")
(tmp / "bad.json").write_text("[1, 2")
check(CD.load_redfin(tmp / "bad.json") == ({}, None), "an unreadable Redfin file: dashes, not an error")

import data_providers as D  # noqa: E402
check(all("market_climate_pct" not in s for s in D.CHOROPLETH_STATES.values()),
      "THE 0-100 'MARKET CLIMATE' BLEND IS NO LONGER COMPUTED")

# ══════════════════════════════════════════════════════════════════
# THE PAGE RENDERS
# ══════════════════════════════════════════════════════════════════
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")))
env.globals.update(is_admin=lambda r: False, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/conditions"), query_params={})
html = env.get_template("conditions.html").render(request=req, **ctx)
for needle in ("Market conditions", "Sep 2026", "Sep 2025", "May 2026", "frozen", "Ohio", "Montana",
               "+12 d", "+1.2 pts", "MT: latest figures are from an earlier month", "No blended score"):
    check(needle in html, f"the page shows '{needle}'")
check('class="mc-old"' in html and "Aug 2026</span>" in html, "the lagging state's row shows its month")
for bad in (">None<", " None ", ">nan", "nan%", ">undefined<", "Market Climate", "Buyer's mkt"):
    check(bad not in html, f"no '{bad}' on the page")
carried = env.get_template("conditions.html").render(request=req, **{**ctx, "carried": True})
check("did not pass its checks" in carried, "a carried month says so")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} market-conditions checks passed.")
print("   Realtor.com by state, each against the same month a year earlier; no blend; Redfin dated and frozen")
