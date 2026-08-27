"""Money is integer minor units — proved.

Run:  python tests/test_ops_money.py      (exit 0 = all pass)

This repo already stores money as DOUBLE PRECISION for the analysis boards.
That is fine for a market-cap estimate and fatal for a rent ledger. Most of
these checks are about the doors that let a float back in, and about
division — because a split that drops the odd cent is how a ledger stops
reconciling two years later with nobody able to say when it started.
"""
import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.ops import money as M

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def raises(fn, *a, **k):
    try:
        fn(*a, **k)
        return False
    except M.MoneyError:
        return True


# ── parse ──
check(M.parse("19.99") == 1999, "a string amount becomes cents")
check(M.parse("1,234.56") == 123456 and M.parse("$1,234.56") == 123456,
      "commas and a dollar sign are tolerated — people paste from statements")
check(M.parse(Decimal("19.99")) == 1999, "a Decimal is exact and accepted")
check(M.parse(20) == 2000, "a plain int is major units, not minor")
check(M.parse("-45.50") == -4550 and M.parse("(45.50)") == -4550,
      "negatives in both conventions, including accounting parentheses")
check(M.parse("0") == 0, "zero is a real amount, not a missing one")

check(raises(M.parse, 19.99),
      "A FLOAT IS REFUSED, NOT ROUNDED. By the time a float arrives the "
      "value may already be 19.989999999999998, and rounding it here would "
      "launder an upstream bug into a plausible-looking cent")
check(raises(M.parse, None) and raises(M.parse, ""),
      "missing is refused rather than becoming zero — zero is a payment of "
      "nothing, which is a different claim from no payment")
check(raises(M.parse, True), "a boolean is not an amount")
check(raises(M.parse, "abc") and raises(M.parse, "12.3.4"), "junk is refused")
check(raises(M.parse, "19.999"),
      "more precision than the currency holds is refused rather than "
      "silently rounded — the caller rounds deliberately or not at all")
check(raises(M.parse, "19.99", "XYZ"),
      "an unknown currency raises. Defaulting to 2 decimals would be a 100x "
      "error against a zero-decimal currency")


# ── format, and only at the edge ──
check(M.format_minor(123456) == "$1,234.56", "cents render as a display string")
check(M.format_minor(-4550) == "-$45.50", "negatives keep their sign")
check(M.format_minor(0) == "$0.00", "zero renders as zero, not blank")
check(M.format_minor(5) == "$0.05", "and a nickel is not five dollars")
check(M.format_minor(123456, symbol=False) == "1,234.56", "symbol is optional")
check(raises(M.format_minor, 12.34),
      "formatting a float is refused too — it would mean a float reached "
      "the presentation edge, which means it was in the arithmetic")


# ── division loses nothing ──
check(M.split(1000, 3) == [334, 333, 333],
      "a three-way split of $10 gives the odd cent to the first part")
check(sum(M.split(1000, 3)) == 1000, "and the parts sum to the whole EXACTLY")
for total in (1, 7, 99, 100, 12345, 999999):
    for parts in (2, 3, 7, 12, 30, 31):
        check(sum(M.split(total, parts)) == total,
              f"split({total},{parts}) is lossless")
check(M.split(-1000, 3) == [-334, -333, -333], "negatives split symmetrically")
check(M.split(0, 5) == [0, 0, 0, 0, 0], "zero splits into zeros")
check(raises(M.split, 1000, 0) and raises(M.split, 1000, -1),
      "a zero or negative number of parts is refused")
check(raises(M.split, 10.0, 2), "and split refuses a float total")

check(M.allocate(1000, [1, 1, 1]) == [334, 333, 333],
      "equal weights match a plain split")
check(sum(M.allocate(10_000, [3, 5, 11, 2])) == 10_000,
      "weighted allocation is lossless too")
check(M.allocate(100, [1, 0]) == [100, 0],
      "a zero weight gets nothing rather than a rounding crumb")
check(raises(M.allocate, 100, [0, 0]),
      "weights summing to zero raise rather than dividing by zero")
check(raises(M.allocate, 100, []) and raises(M.allocate, 100, [1, -1]),
      "an empty or negative weight list is refused")
_alloc = M.allocate(1001, [1, 1, 1])
check(sum(_alloc) == 1001 and _alloc == [334, 334, 333],
      "leftover units go to the largest remainders, deterministically, so a "
      "test can pin which charge got the extra cent")


# ── percentages in basis points ──
check(M.pct(100_000, 500) == 5000, "5% of $1,000 is $50")
check(M.pct(1999, 500) == 100,
      "5% of $19.99 rounds HALF-UP to $1.00 — what a lease says and what a "
      "court expects, not banker's rounding")
check(M.pct(100, 50) == 1, "half a percent of a dollar is a cent, rounded up")
check(M.pct(0, 500) == 0 and M.pct(100_000, 0) == 0, "zero either side is zero")
check(M.pct(-10_000, 500) == -500, "a percentage of a negative stays negative")
check(raises(M.pct, 100.0, 500), "pct refuses a float amount")


# ── clamping, and saying that it clamped ──
_v, _did = M.clamp(7500, hi=5000)
check(_v == 5000 and _did is True,
      "a fee above the jurisdiction ceiling is clamped AND reports that it "
      "was — a fee silently reduced to the legal maximum looks identical to "
      "one computed correctly, and the difference matters if disputed")
check(M.clamp(3000, hi=5000) == (3000, False),
      "a fee under the ceiling passes through untouched and unflagged")
check(M.clamp(-100, lo=0) == (0, True), "a floor clamps too")
check(M.clamp(500) == (500, False), "with no bounds nothing happens")


# ── the property that matters most ──
# Round-tripping through the module must never drift, at any magnitude.
for s in ("0.01", "0.99", "1.00", "19.99", "1234.56", "999999.99", "-0.01"):
    check(M.format_minor(M.parse(s), symbol=False).replace(",", "")
          == (s if not s.startswith("-") else s),
          f"{s} round-trips exactly")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ops-money checks passed.")
sys.exit(0)
