"""/aristocrats — dividend safety is measured, and unmeasured is never safe.

Audit item #3: the page called four of its five BUYs "payout/leverage safe"
with neither figure on file — the gate read a missing payout or leverage
as a pass. Payout and net debt/EBITDA now come from SEC filings in the
monthly refresh, and a cheap name whose safety could not be measured is
UNMEASURED, never BUY. Fixtures are the companies' own filed figures
(FY2023-25, from a probe of SEC companyfacts).
"""
import os
import sys

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import aristocrats as A                 # noqa: E402
import refresh_aristocrats as RA        # noqa: E402

_FAILS, _COUNT = [], 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def yrs(a, b, c):
    return {2023: a, 2024: b, 2025: c}


# ── payout: the better of two three-year covers ──────────────────────
# AbbVie: acquired R&D sinks GAAP earnings; cash covers the dividend.
_abbv = RA.payout_cover(div=yrs(10.539e9, 11.025e9, 11.657e9), ni=yrs(4.863e9, 4.278e9, 4.226e9),
                        ocf=yrs(22.839e9, 18.806e9, 19.030e9), capex=yrs(0.777e9, 0.974e9, 1.214e9),
                        last_fy=2025)
check(_abbv[1] == "free cash flow" and 57 < _abbv[0] < 58 and _abbv[2] == [2023, 2024, 2025],
      f"ABBVIE PAYS 58% OF THREE YEARS' FREE CASH FLOW — not the 248% its charge-laden GAAP "
      f"earnings give (got {_abbv})")
# Coca-Cola: a one-off tax deposit sinks cash; earnings cover it.
_ko = RA.payout_cover(div=yrs(7.952e9, 8.359e9, 8.779e9), ni=yrs(10.714e9, 10.631e9, 13.107e9),
                      ocf=yrs(11.599e9, 6.805e9, 7.408e9), capex=yrs(1.852e9, 2.064e9, 2.112e9),
                      last_fy=2025)
check(_ko[1] == "earnings" and 72 < _ko[0] < 74,
      f"Coca-Cola is judged on earnings when a tax payment sinks one year's cash (got {_ko})")
# Realty Income: GAAP earnings are after depreciation; the FFO proxy is the fair cover.
_o_div, _o_ni = yrs(2.112e9, 2.692e9, 2.921e9), yrs(0.872e9, 0.861e9, 1.059e9)
_o_da = yrs(1.895e9, 2.396e9, 2.524e9)
_o = RA.payout_cover(_o_div, _o_ni, {}, {}, 2025, ffo_da=_o_da)
check(_o[1] == "ffo" and 80 < _o[0] < 81,
      f"A REIT is covered by earnings plus D&A — Realty Income ~80%, not 275% (got {_o})")
check(RA.payout_cover(_o_div, _o_ni, {}, {}, 2025)[1] == "earnings",
      "without the REIT flag the FFO proxy is not offered")
# Albemarle: three years of dividends while earnings and FCF were both negative.
_alb = RA.payout_cover(div=yrs(0.187e9, 0.189e9, 0.191e9), ni=yrs(1.573e9, -1.179e9, -0.511e9),
                       ocf=yrs(1.325e9, 0.702e9, 1.282e9), capex=yrs(2.149e9, 1.686e9, 0.590e9),
                       last_fy=2025)
check(_alb == (None, "negative", []),
      f"A DIVIDEND PAID OUT OF NOTHING EARNED IS A FAILURE, not an unknown (got {_alb})")
check(RA.payout_cover({}, {}, {}, {}, 2025) == (None, None, []),
      "no filings at all is unknown")
check(RA.payout_cover(yrs(1, 1, 1), {2025: 10.0}, {}, {}, 2025) == (None, None, []),
      "one year is not a window: a single year's charge must not decide it")
check(RA.payout_cover({2023: 1.0, 2024: 1.0}, {2023: 5.0, 2024: 5.0}, {}, {}, 2025) == (None, None, []),
      "and a window that stops before the latest year is stale, not current")

# ── EBITDA: amortization counts, and one bad year does not decide ─────
_e = RA.ebitda_by_year({2025: 15.075e9}, {}, {2025: 0.762e9}, {2025: 7.377e9})
check(abs(_e[2025] - 23.214e9) < 1e6,
      "AbbVie tags depreciation and intangible amortization apart; EBITDA adds both")
check(RA.ebitda_by_year({2025: 10.0}, {2025: 3.0}, {2025: 1.0}, {2025: 9.0})[2025] == 13.0,
      "a combined D&A tag is used alone — amortization is already in it")
check(RA.ebitda_by_year({2025: 10.0}, {}, {}, {}) == {},
      "no D&A at all is no EBITDA, not EBIT")
_apd = RA.net_debt_to_ebitda(16e9, 1e9, {2023: 4.4e9, 2024: 4.5e9, 2025: 0.65e9}, 2025)
check(_apd == (round(15e9 / 4.4e9, 2), "measured"),
      f"one impairment year does not decide the gate: the 3-year median stands in (got {_apd})")
check(RA.net_debt_to_ebitda(1e9, 3e9, {2025: 2e9}, 2025) == (-1.0, "net cash"),
      "net cash passes on its own")
check(RA.net_debt_to_ebitda(5e9, 1e9, {}, 2025, insurer=True) == (None, "n/a"),
      "EBITDA is not how an underwriter's leverage is read: not applicable, not unknown")
check(RA.net_debt_to_ebitda(0.55e9, 0.05e9, {2025: 3.0e9}, 2025,
                            notes=["one kind of debt is the only long-term figure filed"])[1] == "floor",
      "Realty Income's debt read is one kind of its borrowing — a floor")
check(RA.net_debt_to_ebitda(5e9, 1e9, {2023: -1.0, 2024: -1.0, 2025: -2.0}, 2025) == (None, "no positive EBITDA"),
      "net debt with no positive EBITDA to carry it")
check(RA.net_debt_to_ebitda(None, 1e9, {2025: 1e9}, 2025) == (None, "unknown"), "no debt figure is unknown")
check(RA.net_debt_to_ebitda(5e9, 1e9, {2024: 2e9}, 2025) == (None, "unknown"),
      "no EBITDA for the balance-sheet year is unknown")
check(RA.ticker_ciks({"0": {"ticker": "BF-B", "cik_str": 14693}, "1": {"ticker": "abt", "cik_str": 1800}})
      == {"BF-B": 14693, "ABT": 1800}, "every ticker maps to its CIK, share classes included")


# ── end to end over a companyfacts payload ───────────────────────────
def _dur(vals, unit="USD"):
    return {"units": {unit: [{"start": f"{y - 1}-01-01", "end": f"{y - 1}-12-31", "val": v,
                              "fy": y - 1, "fp": "FY", "form": "10-K", "filed": f"{y}-02-15"}
                             for y, v in vals.items()]}}


def _inst(end, v):
    return {"units": {"USD": [{"end": end, "val": v, "fy": int(end[:4]), "fp": "FY",
                               "form": "10-K", "filed": end}]}}


def _facts(bs_end="2025-12-31"):
    return {"facts": {"us-gaap": {
        "Revenues": _dur({2024: 20e9, 2025: 21e9, 2026: 22e9}),
        "NetIncomeLoss": _dur({2024: 4e9, 2025: 4.2e9, 2026: 4.4e9}),
        "NetCashProvidedByUsedInOperatingActivities": _dur({2024: 5e9, 2025: 5.2e9, 2026: 5.4e9}),
        "PaymentsToAcquirePropertyPlantAndEquipment": _dur({2024: 1e9, 2025: 1e9, 2026: 1e9}),
        "PaymentsOfDividendsCommonStock": _dur({2024: 2e9, 2025: 2.1e9, 2026: 2.2e9}),
        "OperatingIncomeLoss": _dur({2024: 6e9, 2025: 6.2e9, 2026: 6.4e9}),
        "DepreciationDepletionAndAmortization": _dur({2024: 1e9, 2025: 1e9, 2026: 1e9}),
        "LongTermDebtNoncurrent": _inst(bs_end, 12e9),
        "CashAndCashEquivalentsAtCarryingValue": _inst(bs_end, 2e9),
        "StockholdersEquity": _inst(bs_end, 10e9),
    }}}


_s = RA.sec_safety(_facts(), "Staples", False, "2026-09-28")
check(_s.get("po_basis") == "earnings" and abs(_s["po"] - 50.0) < 0.1 and _s["po_years"] == "FY2023–2025"
      and _s.get("nd") == round(10e9 / 7.4e9, 2) and _s.get("nd_basis") == "measured"
      and _s.get("safety_as_of") == "2025-12-31",
      f"a filer's payout and leverage come out of its own companyfacts (got {_s})")
_old = RA.sec_safety(_facts("2023-12-31"), "Staples", False, "2026-09-28")
check("po" not in _old and "nd" not in _old and "21 months" in (_old.get("safety_note") or ""),
      f"a balance sheet over 21 months old is not today's: nothing measured, and it says why (got {_old})")
_reit = _facts()
_reit["facts"]["us-gaap"]["NetIncomeLoss"] = _dur({2024: 1e9, 2025: 1e9, 2026: 1e9})
_reit["facts"]["us-gaap"]["PaymentsToAcquirePropertyPlantAndEquipment"] = _dur({2024: 4.5e9, 2025: 4.5e9, 2026: 4.5e9})
_reit["facts"]["us-gaap"]["DepreciationDepletionAndAmortization"] = _dur({2024: 2e9, 2025: 2e9, 2026: 2e9})
check(RA.sec_safety(_reit, "REIT", False, "2026-09-28").get("po_basis") == "ffo"
      and RA.sec_safety(_reit, "Staples", False, "2026-09-28").get("po_basis") != "ffo",
      "the refresh offers the FFO cover to REITs and midstream only")
check(RA.sec_safety({"facts": {}}, "Staples", False, "2026-09-28") == {},
      "no statements, no figures — not zeros")


# ── the page: BUY needs both figures measured ────────────────────────
def board(**tickers):
    return {r["t"]: r for r in A.score({"tickers": tickers})}


CHEAP = {"y": 3.0, "median_y5": 2.0, "dg5": 10.0}           # +50% premium, Chowder 13
_b = board(LOW=dict(CHEAP), ADP=dict(CHEAP, po=55.0, po_basis="earnings", po_years="FY2023–2025",
                                     nd=1.2, nd_basis="measured"))
check(_b["LOW"]["status"] == "UNMEASURED" and _b["LOW"]["payout_state"] == "unknown",
      f"LOWE'S IS CHEAP AND CLEARS CHOWDER, BUT WITH NO PAYOUT OR LEVERAGE IT IS UNMEASURED — "
      f"it was BUY (got {_b['LOW']['status']})")
check(_b["ADP"]["status"] == "BUY" and _b["ADP"]["safety_known"] and _b["ADP"]["po_src"] == "sec",
      "with both figures measured and within their lines, BUY")
check("FY2023–2025" in _b["ADP"]["payout_basis"], "and the page can say what the payout was measured on")
_g = board(UVV=dict(CHEAP, po=96.8, nd=3.96, nd_basis="measured"))
check(_g["UVV"]["status"] == "GATED" and _g["UVV"]["payout_state"] == "fail",
      "Universal Corp at 97% of three years' earnings is gated")
_n = board(ALB=dict(CHEAP, po_basis="negative", nd=5.9, nd_basis="measured"))
check(_n["ALB"]["payout_state"] == "fail" and _n["ALB"]["status"] == "GATED",
      "a dividend paid out of losses is a measured failure")
_i = board(CB=dict(CHEAP, po=15.0, po_basis="earnings", po_years="FY2023–2025", nd_basis="n/a"))
check(_i["CB"]["leverage_state"] == "n/a" and _i["CB"]["status"] == "BUY",
      "an insurer's leverage is not applicable — it does not keep Chubb out of the buy zone")
_c = board(WST=dict(CHEAP, po=11.0, po_basis="earnings", po_years="FY2023–2025", nd=-0.8, nd_basis="net cash"))
check(_c["WST"]["leverage_state"] == "pass" and _c["WST"]["status"] == "BUY", "net cash passes")
_u = board(CWT=dict(CHEAP, po=53.0, po_basis="earnings", po_years="FY2023–2025", nd=4.9, nd_basis="measured"))
check(_u["CWT"]["leverage_state"] == "pass" and _u["CWT"]["status"] == "BUY",
      "a water utility at 4.9x is inside the 6.5x line for utilities, REITs and midstream")
check(board(PEP=dict(CHEAP, nd=4.9, nd_basis="measured"))["PEP"]["leverage_state"] == "fail",
      "while a staples company at 4.9x is past the 3.5x line")
check(board(NEE=dict(CHEAP, nd=6.5, nd_basis="measured"))["NEE"]["leverage_state"] == "fail"
      and board(NEE=dict(CHEAP, nd=5.5, nd_basis="measured"))["NEE"]["leverage_state"] == "warn",
      "the asset-heavy lines are 6.5x to hide and 5.5x to warn")
_f = board(O=dict(CHEAP, po=77.0, po_basis="ffo", po_years="FY2023–2025", nd=0.2, nd_basis="floor"))
check(_f["O"]["leverage_state"] == "unknown" and _f["O"]["status"] == "UNMEASURED",
      "a floor under the hide line proves nothing: unknown, so not BUY")
check(board(O=dict(CHEAP, nd=7.0, nd_basis="floor"))["O"]["leverage_state"] == "fail",
      "a floor already past the line proves the failure")
check(board(APD=dict(CHEAP, nd_basis="no positive EBITDA"))["APD"]["leverage_state"] == "fail",
      "net debt with no positive EBITDA is a failure")
# Seeds: kept where SEC has nothing, on their own (forward-basis) lines.
_s1 = board(MSEX=dict(CHEAP))["MSEX"]
check(_s1["po_src"] == "seed" and _s1["payout_state"] == "pass" and _s1["leverage_state"] == "unknown"
      and _s1["status"] == "UNMEASURED",
      "Middlesex Water's seeded payout survives, but with no leverage it is not BUY")
check(A.payout_state({"po": 78.0, "po_src": "seed"}) == "warn"
      and A.payout_state({"po": 78.0, "po_src": "sec"}) == "warn"
      and A.payout_state({"po": 82.0, "po_src": "seed"}) == "fail"
      and A.payout_state({"po": 82.0, "po_src": "sec"}) == "warn"
      and A.payout_state({"po": 90.0, "po_src": "sec"}) == "fail",
      "a seed (forward) is judged on 65/80, a trailing SEC cover on 75/90")
_p = board(PEP=dict(CHEAP, po=63.0, po_basis="earnings", po_years="FY2023–2025"))["PEP"]
check(_p["po_src"] == "sec" and _p["po"] == 63.0, "an SEC figure replaces the seed")
check(A.leverage_state({"nd": 3.0, "nd_src": "seed"}) == "warn"
      and A.leverage_state({"nd": None, "nd_src": None}) == "unknown",
      "no leverage figure from anywhere is unknown, never a pass")
_order = [r["status"] for r in A.score({"tickers": {
    "LOW": dict(CHEAP), "ADP": dict(CHEAP, po=55.0, po_basis="earnings", po_years="FY2023–2025",
                                    nd=1.2, nd_basis="measured")}})][:2]
check(_order == ["BUY", "UNMEASURED"], f"UNMEASURED sorts right after BUY (got {_order})")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} aristocrats checks passed.")
print(f"   payout {A.PAYOUT_WARN:.0f}/{A.PAYOUT_HIDE:.0f}% (seeds {A.SEED_PAYOUT_WARN:.0f}/{A.SEED_PAYOUT_HIDE:.0f}) · "
      f"ND/EBITDA {A.ND_EBITDA_WARN}/{A.ND_EBITDA_HIDE}x, asset-heavy {A.ND_EBITDA_WARN_ASSET}/{A.ND_EBITDA_HIDE_ASSET}x")
