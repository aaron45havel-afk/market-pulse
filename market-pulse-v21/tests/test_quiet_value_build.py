"""Quiet Value build helpers (scripts/refresh_quiet_value.py): flows come from
the last complete year's EDGAR frames — not one quarter's — and revenue is
merged across the tags companies actually file. Offline: no network.

Run: python tests/test_quiet_value_build.py
"""
from __future__ import annotations

import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import refresh_quiet_value as Q  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


check(Q.annual_period(date(2026, 10, 2)) == "CY2025" and Q.annual_period(date(2027, 1, 15)) == "CY2026",
      "the flows are the LAST COMPLETE CALENDAR YEAR's frame, never a single quarter's")
check(set(Q.ANNUAL_INPUTS) == {"revenue", "net_income", "operating_income", "ocf", "capex",
                                "div_per_share", "div_paid"},
      "every flow the seven tests read is annual: P/E, dividend yield, margin and capex/OCF")
check(Q.ANNUAL_INPUTS["div_paid"] == (("PaymentsOfDividendsCommonStock", "PaymentsOfDividends"), "USD"),
      "dividends paid in dollars, the common-stock line preferred over the all-classes one")
check(len(Q.ANNUAL_INPUTS["revenue"][0]) >= 4 and Q.ANNUAL_INPUTS["revenue"][0][0] == "Revenues",
      "revenue is merged across the tags companies use (Revenues alone covered 1,655 filers)")
check(Q.ANNUAL_INPUTS["div_per_share"][1] == "USD-per-shares", "dividends per share are in their own unit")

frames = {
    "revenue": [{"1": {"val": 100.0}}, {"1": {"val": 999.0}, "2": {"val": 50.0}}, {}, {"3": {"val": None}}],
    "net_income": [{"1": {"val": 8.0}, "2": {"val": -3.0}}],
}
m = Q.merge_annual(frames)
check(m["1"]["revenue"] == 100.0 and m["2"]["revenue"] == 50.0,
      "THE FIRST TAG A COMPANY FILES WINS; later tags only fill companies the earlier ones lack")
check(m.get("3", {}).get("revenue") is None, "a null value is not a value")
check(m["1"]["net_income"] == 8.0 and m["2"]["net_income"] == -3.0, "a loss is kept as a loss")
check(Q.merge_annual({}) == {}, "no frames: nothing")

import inspect  # noqa: E402
src = inspect.getsource(Q.main)
check('"revenue",\n                                     "net_income", "div_per_share")' not in src
      and "for k in ANNUAL_INPUTS" in src,
      "candidates take their flows from the annual frames, not the quarterly screener payload")
check("default=4000" in inspect.getsource(Q.main),
      "every candidate is priced — the 400 smallest by assets were mostly pre-revenue shells")

# ── dividends: per share as filed, else dollars paid over shares ──
check(Q.dividend_per_share(0.50, 9e9, 1e6) == 0.50, "a filed per-share dividend wins over the dollars paid")
check(Q.dividend_per_share(None, 2_000_000, 4_000_000) == 0.50,
      "WITHOUT A PER-SHARE TAG, the year's dividends paid over the share count")
check(Q.dividend_per_share(None, -2_000_000, 4_000_000) == 0.50, "a payment filed with a sign is still a payment")
check(Q.dividend_per_share(None, 0.0, 4_000_000) == 0.0, "a filed zero is a measured zero, not a gap")
check(Q.dividend_per_share(0.0, 2_000_000, 4_000_000) == 0.0, "and a filed per-share zero is not overridden")
check(Q.dividend_per_share(None, None, 4_000_000) is None, "nothing filed: unknown")
check(Q.dividend_per_share(None, 2_000_000, 0) is None and Q.dividend_per_share(None, 2_000_000, None) is None,
      "no share count: unknown, never a division by zero")

# ── industry codes: fetched, paced, and refused when SEC will not answer ──
calls: list[str] = []


def fake_get(url):
    calls.append(url)
    cik = url.rsplit("CIK", 1)[1].split(".")[0]
    return {"0000000001": {"sic": "6022"}, "0000000002": {"sic": 3571},
            "0000000003": {"sic": ""}, "0000000004": None}.get(cik, {"sic": "7372"})


got = Q.fetch_sic(["1", "2", "3", "4", "0000000005"], get=fake_get, interval=0)
check(got == {"1": "6022", "2": "3571", "3": None, "0000000005": "7372"},
      f"code per CIK as a string; a blank code is None; an unanswered CIK is ABSENT (got {got})")
check(sorted(calls)[0] == Q.SUBMISSIONS.format(cik="0000000001")
      and Q.SUBMISSIONS == "https://data.sec.gov/submissions/CIK{cik}.json",
      "SEC's submissions record, by ten-digit CIK")

import time as _t  # noqa: E402
t0 = _t.monotonic()
Q.fetch_sic([str(i) for i in range(12)], get=lambda u: {"sic": "1"}, interval=0.05)
check(_t.monotonic() - t0 >= 0.5,
      "the pool is paced as a whole: four workers do not make four times SEC's rate")

dead: list[str] = []
Q.fetch_sic([str(i) for i in range(400)], get=lambda u: dead.append(u), interval=0)
check(len(dead) == Q.SIC_CHECK_AFTER,
      f"NOTHING ANSWERING IN THE FIRST {Q.SIC_CHECK_AFTER} STOPS THE PULL (asked {len(dead)} of 400)")
slow: list[str] = []
got = Q.fetch_sic([str(i) for i in range(120)],
                  get=lambda u: slow.append(u) or ({"sic": "6022"} if len(slow) == 30 else None), interval=0)
check(len(slow) == 120 and len(got) == 1,
      "one answer in the first fifty is a flaky source, not a dead one: it keeps asking")

check(Q.sic_coverage_ok(90, 100) and not Q.sic_coverage_ok(89, 100) and Q.sic_coverage_ok(0, 0),
      "90% of the board must have a code; below that a bank is scored as an operating company again")
msrc = inspect.getsource(Q.main)
check("sic_coverage_ok(len(sics), len(done))" in msrc and "industry=r[\"industry\"]" in msrc,
      "the run refuses to write without the codes, and every row is scored with its industry")
check(msrc.index("fetch_sic(") > msrc.index("sized = ["),
      "codes are asked only for names in the size band — the ones that can reach the board")

# ── the page ──
import asyncio  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from jinja2 import Environment, FileSystemLoader  # noqa: E402

import quality_value as QV  # noqa: E402
import main  # noqa: E402


def row(t, industry=None, liq="low", impractical=False, **kw):
    q = QV.evaluate(kw, industry=industry)
    return {"ticker": t, "name": f"{t} Corp", "industry": industry, "liq_bucket": liq,
            "impractical": impractical, "turnover": 0.1, "zero_day_pct": 5.0, "days_to_build": 1.0,
            "market_cap": 1e8, "price": 10.0, "size_bucket": "micro",
            "metrics": q["metrics"], "verdicts": q["verdicts"], "passed": q["passed"], "known": q["known"],
            "unknown": q["unknown"], "not_applicable": q["not_applicable"], "why": QV.summarize(q)}


GOOD = dict(price=10.0, market_cap=1e8, cash=4e7, total_debt=5e6, equity=6e7, capex=1e6, ocf=1.2e7,
            operating_income=9e6, revenue=4e7, dividends_per_share=0.4, eps=1.2, book_value_per_share=6.0)
rows = [row("OPCO", **GOOD), row("BANK", "bank", **GOOD), row("INSR", "insurer", **GOOD),
        row("REIT", "reit", **GOOD), row("LOUD", "bank", liq="high", **GOOD)]
out, counts = main.quiet_value_board(rows)
check([r["ticker"] for r in out] == ["OPCO", "REIT"],
      f"THE SAME FIGURES CLEAR THE SCREEN FOR AN OPERATING COMPANY AND A REIT, NOT A BANK OR AN INSURER "
      f"(got {[r['ticker'] for r in out]})")
check(counts["financials"] == 2 and counts["after_tradeable"] == 4,
      "the funnel counts the banks and insurers that reached the quality step (not the churned one)")
check(main.quiet_value_board(rows, minpass_n=0)[0][-1]["ticker"] != "BANK",
      "even at zero tests passed, a bank cannot reach five measured tests")

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")))
env.globals.update(is_admin=lambda r: False, current_user=lambda r: None, pipeline_access=lambda r: False)
ctx = dict(request=SimpleNamespace(url=SimpleNamespace(path="/quiet-value"), query_params={}),
           meta={"as_of": "2026-10-02", "universe_candidates": 2754, "in_size_band": 1417,
                 "volume_coverage_pct": 99, "price_source": "yahoo", "sic_coverage_pct": 98},
           pending=False, liq="low", size="", minpass=5, require="", tradeable=True, exclude_high=True,
           tests=QV.TESTS, labels=QV.LABELS, na_reason=QV.NA_REASON)
html = env.get_template("quiet_value.html").render(**ctx, rows=rows[:4], counts=counts, shown=len(out))
check("2 of the 4 are banks or insurers" in html, "the funnel says how many banks and insurers stopped, and why")
check(html.count('class="t t-na"') == 4 + 4 + 1,
      "every withheld test is drawn as not applicable: four for the bank, four for the insurer, one for the REIT")
check(QV.NA_REASON["bank"] in html and QV.NA_REASON["reit"] in html,
      "and its tooltip says why it does not apply, not that it was unreported")
check("industry code on 98%" in html, "the run's industry-code coverage is printed with the other coverage figures")
html0 = env.get_template("quiet_value.html").render(**ctx, rows=rows[:1],
                                                     counts={**counts, "financials": 0}, shown=1)
check("banks or insurers" not in html0.split("Read this before acting")[0],
      "no banks reached the step: the funnel says nothing about them")
for bad in (">None<", "None%", "nan%", "NaN"):
    check(bad not in html, f"no '{bad}' leaks into the page")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} quiet-value build checks passed.")
