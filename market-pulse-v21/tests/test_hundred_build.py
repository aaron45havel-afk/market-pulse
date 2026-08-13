"""The 100-bagger builder — the seam between filings and the checklist.

NOTHING COVERED THIS FILE BEFORE, which is how `invested_capital_by_year`
came to carry a comment reading "Greenblatt's basis so it matches criterion
5's denominator" over code that used StockholdersEquity. The comment was
checked by nobody and the code was checked by nobody, so they were free to
disagree for as long as they liked.

checklist.py is pure and well covered; every bug that has survived in this
feature has lived in the assembly step instead.
"""
import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import checklist as C           # noqa: E402
import hundred_screener as H    # noqa: E402

_FAILS, _COUNT = [], 0


def check(cond, label):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(label)


YEARS = ["2019-12-31", "2020-12-31", "2021-12-31", "2022-12-31",
         "2023-12-31", "2024-12-31"]


def _instants(vals):
    """Balance-sheet facts: an end date and no start, per lynch._instant."""
    return [{"end": y, "val": v} for y, v in zip(YEARS, vals) if v is not None]


def _durations(vals):
    """Income-statement facts: a ~365-day window, per lynch.annual_series."""
    out = []
    for y, v in zip(YEARS, vals):
        if v is None:
            continue
        out.append({"start": f"{int(y[:4]) - 1}-12-31", "end": y, "val": v})
    return out


def facts(**tags):
    return {"facts": {"us-gaap": {
        name: {"units": {"USD": entries}} for name, entries in tags.items()}}}


ROW = {"ticker": "BUYB", "name": "Buyback Corp", "cik": "0000000001",
       "sic": "3670", "state": "DE", "exchange": "Nasdaq"}
QUOTE = {"price": 40.0, "market_cap": 2.0e9}


# ── THE DENOMINATOR ──────────────────────────────────────────────────
# A company buying back stock: equity collapses while the operating base it
# actually employs grows. These are opposite facts about the same company
# and the veto has to run on the second one.
BUYBACK = facts(
    StockholdersEquity=_instants([1000, 850, 700, 500, 350, 200]),
    AssetsCurrent=_instants([700, 760, 820, 880, 940, 1000]),
    LiabilitiesCurrent=_instants([400, 400, 400, 400, 400, 400]),
    PropertyPlantAndEquipmentNet=_instants([200, 200, 200, 200, 200, 200]),
    OperatingIncomeLoss=_durations([50, 52, 54, 56, 58, 60]),
    Revenues=_durations([900, 950, 1000, 1050, 1100, 1150]),
)
rec = H.facts_to_record(ROW, QUOTE, BUYBACK, "2025-01-31")
cap = rec["invested_capital_by_year"]

check(cap.get("2019-12-31") == 500 and cap.get("2024-12-31") == 800,
      "invested capital is (current assets - current liabilities) + net PP&E, "
      "which is what criterion 5 uses and what the comment always claimed")

check(rec["equity_by_year"].get("2024-12-31") == 200,
      "equity is still carried separately — criterion 4 is return on EQUITY "
      "and that is the right denominator there")

check(cap != rec["equity_by_year"],
      "and the two are different quantities. When they were the same object "
      "the capital veto was measuring return on equity under another name")

_v = C.capital_veto(rec["op_income_by_year"], cap)
check(_v.measured and _v.band == "Yikes",
      "$300 of new capital earning $10 more is a 3.3% incremental return and "
      "the veto fires. On the equity basis this company's capital SHRANK 80%, "
      "so it was excused from the test and reported quiet — 405 companies on "
      "the August board were excused exactly this way")

# The old behaviour, pinned so it cannot come back by accident.
_old = C.capital_veto(rec["op_income_by_year"], rec["equity_by_year"])
check(_old.band == "quiet" and _old.value is None,
      "and the equity series still exempts the very same company — which is "
      "why this was a fixed denominator and not a fixed threshold. Asserted "
      "on the outcome rather than the wording: this check pinned the old "
      "sentence and broke when that sentence was corrected, which is a test "
      "guarding prose instead of behaviour")


# ── EVERY INPUT MUST BE FILED ────────────────────────────────────────
GAPPY = facts(
    StockholdersEquity=_instants([1000, 850, 700, 500, 350, 200]),
    AssetsCurrent=_instants([700, 760, 820, 880, 940, 1000]),
    # 2021 and 2022 never filed a current-liabilities tag
    LiabilitiesCurrent=_instants([400, 400, None, None, 400, 400]),
    PropertyPlantAndEquipmentNet=_instants([200, 200, 200, 200, 200, 200]),
    OperatingIncomeLoss=_durations([50, 52, 54, 56, 58, 60]),
)
_gap = H.facts_to_record(ROW, QUOTE, GAPPY, "2025-01-31")["invested_capital_by_year"]
check("2021-12-31" not in _gap and "2022-12-31" not in _gap,
      "a year missing any input is DROPPED, not defaulted — a missing "
      "liability would shrink the denominator and flatter the return, which "
      "is the shape of every bug this repo has paid for")
check(len(_gap) == 4 and _gap["2023-12-31"] == 740,
      "and the years that did file all three are still measured")


# ── NOT POSITIVE IS UNDEFINED, NOT SMALL ─────────────────────────────
UNDERWATER = facts(
    AssetsCurrent=_instants([300, 300, 300, 300, 300, 300]),
    LiabilitiesCurrent=_instants([900, 900, 900, 900, 900, 900]),
    PropertyPlantAndEquipmentNet=_instants([100, 100, 100, 100, 100, 100]),
    OperatingIncomeLoss=_durations([50, 52, 54, 56, 58, 60]),
)
_uw = H.facts_to_record(ROW, QUOTE, UNDERWATER, "2025-01-31")["invested_capital_by_year"]
check(_uw == {},
      "capital employed at or below zero is refused outright — a negative "
      "denominator flips the sign and ranks a struggling company at the top")

_uwv = C.capital_veto({y: 50.0 for y in YEARS}, _uw)
check(not _uwv.measured,
      "so the veto reports it as unmeasured, never as quiet — 'we could not "
      "look' is a different fact from 'nothing is wrong'")


# ── NO FILINGS AT ALL ────────────────────────────────────────────────
_empty = H.facts_to_record(ROW, QUOTE, {}, "2025-01-31")
check(_empty["invested_capital_by_year"] == {}
      and _empty["equity_by_year"] == {}
      and _empty["ticker"] == "BUYB",
      "an empty companyfacts blob yields empty series and still returns a "
      "row — the company is unmeasured, not absent")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for f in _FAILS:
        print("  ✗", f)
    sys.exit(1)
print(f"OK — all {_COUNT} builder checks passed.")
print("   invested capital = (CA - CL) + PP&E, per year, every input filed")
print("   equity stays with criterion 4, where equity is the right question")
