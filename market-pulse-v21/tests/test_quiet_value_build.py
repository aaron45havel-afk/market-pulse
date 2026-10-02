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
check(set(Q.ANNUAL_INPUTS) == {"revenue", "net_income", "operating_income", "ocf", "capex", "div_per_share"},
      "every flow the seven tests read is annual: P/E, dividend yield, margin and capex/OCF")
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

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} quiet-value build checks passed.")
