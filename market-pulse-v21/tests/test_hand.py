"""Hand-entered criteria — the six a person answers by opening a document.

The property under test throughout is that a hand entry CANNOT reach the
headline. Not "does not currently"; cannot, because `ledger()` counts
`measures` and hand entries are never put there.
"""
import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import checklist as C   # noqa: E402
import hand as H        # noqa: E402

_FAILS, _COUNT = [], 0


def check(cond, label):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(label)


def invalid(fn, label):
    try:
        fn()
    except H.Invalid:
        check(True, label)
        return
    except Exception as e:                      # pragma: no cover
        check(False, f"{label} (raised {type(e).__name__} instead)")
        return
    check(False, f"{label} (accepted it)")


# ── VALIDATION ───────────────────────────────────────────────────────
e = H.validate(7, 23.4, note="net of 60-day options", source="DEF 14A 2025-04-02")
check(e["criterion"] == 7 and e["value"] == 23.4 and e["source"],
      "a percent of class, with the document that carries it")

invalid(lambda: H.validate(7, 23.4, source=""),
        "an unsourced entry is refused — that is the same unmeasured number "
        "this page exists to expose, just typed by a human")
invalid(lambda: H.validate(7, 4512880, source="DEF 14A"),
        "4,512,880 is a share COUNT in the neighbouring column, not a "
        "percent, and 0-100 is the guard that catches the wrong column")
invalid(lambda: H.validate(1, 30.0, source="10-K"),
        "criterion 1 is measured from filings and is not hand-enterable")
invalid(lambda: H.validate(9, -2, source="broker page"),
        "a negative analyst count is not a reading")
invalid(lambda: H.validate(12, -0.4, source="broker page"),
        "PEG at or below zero growth is undefined — unmeasured beats a "
        "negative, which is Lynch's own refusal in lynch.peg")
invalid(lambda: H.validate(6, 12.0, source="10-K"),
        "capital allocation has no agreed scale, so a bare number is "
        "refused rather than stored as invented precision")

n = H.validate(6, note="bought back at 4x book in 2021, wrote it off in 2023",
               source="10-K FY2023 Item 5")
check(n["value"] is None and n["note"],
      "and the same criterion takes a written finding instead")

check(H.validate(9, 0, source="Koyfin")["value"] == 0.0,
      "zero analysts is a real reading, and on an INVERTED criterion it is "
      "the best one — it must not be confused with unmeasured")


# ── THE HEADLINE CANNOT SEE IT ───────────────────────────────────────
row = {"ticker": "TEST", "verdict": "list",
       "measures": {"1": {"measured": True, "value": 30.0, "band": "Great"}},
       "ledger": C.ledger({1: C.sales_growth({"2020-12-31": 100.0,
                                              "2024-12-31": 300.0})})}
before = dict(row["ledger"])
H.merge([row], {"TEST": {9: H.validate(9, 0, source="Koyfin")}})
check(row["ledger"] == before,
      "attaching a hand entry does not touch the ledger at all")
check("9" in row["hand"] and "9" not in row["measures"],
      "it lands in `hand`, never in `measures` — ledger() counts measures, "
      "so this is arithmetic rather than discipline")
check(C.ledger({1: C.sales_growth({"2020-12-31": 100.0, "2024-12-31": 300.0})})
      ["measured_n"] == before["measured_n"],
      "and the ledger is a pure function of measures, so nothing a person "
      "types can move the denominator")


# ── THE ONE EXCEPTION: CRITERION 7 ───────────────────────────────────
# Shaped after EPAM on the live board: rejected on criterion 7 alone, with
# every other test already passed, because evaluate() runs vetoes LAST.
def vetoed_row(tkr="EPAM", vetoed_by=(7,)):
    return {"ticker": tkr, "verdict": "reject",
            "reason": "veto: management owns almost none of the company",
            "measures": {}, "ledger": {"vetoed": True,
                                       "vetoed_by": list(vetoed_by),
                                       "passing_n": 5, "measured_n": 6}}

r = vetoed_row()
out = H.merge([r], {"EPAM": {7: H.validate(7, 23.4, source="DEF 14A")}})
check(r["verdict"] == "list" and out["retracted"] == ["EPAM"],
      "reading the actual proxy retracts a criterion-7 veto — the row had "
      "already cleared both thresholds before the veto ran, so cancelling "
      "it restores the verdict the build would have reached")
check("retracted" in r.get("hand_verdict", ""),
      "and the row says so, rather than quietly appearing")

r = vetoed_row()
H.merge([r], {"EPAM": {7: H.validate(7, 2.0, source="DEF 14A")}})
check(r["verdict"] == "reject",
      "a hand reading BELOW the floor leaves the veto standing")

r = vetoed_row(vetoed_by=(6, 7))
H.merge([r], {"EPAM": {7: H.validate(7, 23.4, source="DEF 14A")}})
check(r["verdict"] == "reject",
      "and a company vetoed on capital allocation TOO stays rejected — "
      "retracting one veto cannot speak for another")

r = vetoed_row(vetoed_by=(8,))
H.merge([r], {"EPAM": {7: H.validate(7, 23.4, source="DEF 14A")}})
check(r["verdict"] == "reject",
      "a criterion-7 reading has nothing to say about a moat veto")

# Symmetric, and the safe direction: it removes a name you researched.
r = {"ticker": "IPAR", "verdict": "list", "reason": "listed", "measures": {},
     "ledger": {"vetoed": False, "vetoed_by": [], "passing_n": 4}}
out = H.merge([r], {"IPAR": {7: H.validate(7, 0.4, source="DEF 14A 2025")}})
check(r["verdict"] == "reject" and out["hand_vetoed"] == ["IPAR"],
      "and reading a genuinely tiny stake takes a LISTED company off — the "
      "direction that cannot flatter anything, since it only ever removes "
      "names the reader went looking at")

# The other five never move a verdict, whatever they say.
for cid, kw in ((6, {"note": "serial acquirer, all written off"}),
                (8, {"note": "no moat, commodity pricing"}),
                (9, {"value": 40}), (10, {"note": "2.1 of 5 on Glassdoor"}),
                (12, {"value": 9.9})):
    r = vetoed_row("X", vetoed_by=(7,))
    H.merge([r], {"X": {cid: H.validate(cid, source="read it", **kw)}})
    check(r["verdict"] == "reject",
          f"criterion {cid} is recorded but moves no verdict — only 7 does, "
          f"because only 7 is the company's own compelled number")

r = {"ticker": "T", "verdict": "thin", "measures": {},
     "ledger": {"vetoed": False, "vetoed_by": [], "measured_n": 2}}
H.merge([r], {"T": {7: H.validate(7, 40.0, source="DEF 14A")}})
check(r["verdict"] == "thin",
      "a company too thin to judge stays thin — the veto never ran on it, "
      "so there is nothing for a reading to retract")


# ── MERGE HYGIENE ────────────────────────────────────────────────────
rows = [{"ticker": "AAA", "verdict": "list", "measures": {}, "ledger": {}}]
out = H.merge(rows, {})
check(out["attached"] == 0 and "hand" not in rows[0],
      "no entries means no key added — an empty dict on every row would "
      "read as 'looked at and found nothing'")
check(H.merge([], {"AAA": {7: H.validate(7, 1, source="x")}})["attached"] == 0,
      "and entries for companies not in the snapshot are ignored quietly")

rows = [{"ticker": "bbb", "verdict": "list", "measures": {}, "ledger": {}}]
H.merge(rows, {"BBB": {9: H.validate(9, 3, source="Koyfin")}})
check("hand" in rows[0], "ticker matching is case-insensitive")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for f in _FAILS:
        print("  ✗", f)
    sys.exit(1)
print(f"OK — all {_COUNT} hand-entry checks passed.")
print(f"   {len(H.MANUAL_IDS)} hand-enterable · {len(H.NUMERIC_IDS)} numeric · "
      f"{len(H.NOTE_ONLY_IDS)} written findings")
print("   hand values live outside `measures`, so the headline cannot count them")
