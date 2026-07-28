"""Safety / crime-gate checks.

Run:  python tests/test_safety.py      (exit 0 = all pass)

The safety-critical property under test is FAIL-CLOSED: a ZIP we have no
verified crime figure for must never be presented as safe, and the
house-hack board must drop it unless the user explicitly opts in.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import safety as S
import headroom as H

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── tier thresholds are anchored to the real US rate ──
check(340 <= S.US_VIOLENT <= 400, "US violent benchmark is the real ~364/100k")
tiers = {t[0]: t for t in S.TIERS}
check(tiers["very_safe"][2] == 150 and tiers["safe"][2] == 250,
      "very safe <150, safe <250 per 100k")
check(S.TIERS[2][1] <= S.US_VIOLENT < S.TIERS[2][2],
      "the US average lands inside the AVERAGE band, not in a 'safe' band")
check(S.TIER_ORDER["very_safe"] < S.TIER_ORDER["safe"] < S.TIER_ORDER["average"]
      < S.TIER_ORDER["elevated"] < S.TIER_ORDER["high"] < S.TIER_ORDER["unknown"],
      "tier ordering is monotone and unknown sorts last")

# ── a user-supplied tier must never DISABLE the gate ──
# TIER_ORDER doubles as a sort key and contains "unknown": 9. Validating a
# query param against it let ?safetier=unknown through, and every verified
# tier compares <= 9, so the gate turned off while the UI still showed
# "Very safe". That is the one failure mode this module exists to prevent.
check("unknown" not in S.SELECTABLE, "the 'unknown' sentinel is not a selectable bar")
check(S.SELECTABLE == {t[0] for t in S.TIERS}, "selectable bars are exactly the real tiers")
for junk in ("unknown", "", "UNKNOWN", "safest", "9", None, "high "):
    check(S.valid_tier(junk) == S.DEFAULT_TIER,
          f"a junk tier {junk!r} falls back to the default bar, not wide open")
check(S.valid_tier("high") == "high" and S.valid_tier("very_safe") == "very_safe",
      "real tiers pass through valid_tier unchanged")
_hi = {"tier": "high", "violent": 1031.0}
for bad in ("unknown", "", "nonsense", None):
    check(not S.passes(_hi, bad),
          f"a HIGH-crime ZIP does not clear a {bad!r} bar (gate tightens, never opens)")
check(S.passes(_hi, "high"), "a HIGH ZIP clears an explicit 'no filter' bar")

# ── coverage must not count what the gate will reject ──
cov = S.coverage()
check(cov["usable"] <= cov["with_data"], "usable coverage never exceeds rows with a figure")
check(cov["with_data"] - cov["usable"] == cov["suspect"], "suspect count reconciles")
check(all(S.zip_safety(k.rsplit(", ", 1)[0], k.rsplit(", ", 1)[1])["tier"] == "unknown"
          for k, v in list(S._crime_table().items())
          if v.get("confidence") == "suspect")
      if any(v.get("confidence") == "suspect" for v in S._crime_table().values()) else True,
      "every suspect city resolves to unknown, so 'usable' is the honest headline")

# ── classification of real-shaped records ──
tbl = S._crime_table()
have_data = sum(1 for v in tbl.values() if v.get("violent_per_100k") is not None)
check(isinstance(tbl, dict), "crime table loads (may be empty before research lands)")

# direct classification via a temporary injected table
_orig = S._crime_table
S._crime_table = lambda: {
    "Testville, IN": {"violent_per_100k": 90, "property_per_100k": 900,
                      "year": 2024, "source": "FBI test", "confidence": "high"},
    "Midtown, IN": {"violent_per_100k": 364, "property_per_100k": 1954,
                    "year": 2024, "source": "FBI test", "confidence": "high"},
    "Roughtown, IN": {"violent_per_100k": 1500, "property_per_100k": 4000,
                      "year": 2024, "source": "FBI test", "confidence": "high"},
    "Nodata, IN": {"violent_per_100k": None, "property_per_100k": None,
                   "year": None, "source": "", "confidence": None,
                   "note": "agency does not report separately"},
}
try:
    vs = S.zip_safety("Testville", "IN")
    check(vs["tier"] == "very_safe" and vs["violent"] == 90, "90/100k → VERY SAFE")
    check(abs(vs["vs_us"] - 90 / S.US_VIOLENT) < 0.01, "vs_us ratio computed")
    avg = S.zip_safety("Midtown", "IN")
    check(avg["tier"] == "average", "the national rate classifies as AVERAGE")
    hi = S.zip_safety("Roughtown", "IN")
    check(hi["tier"] == "high", "1500/100k → HIGH")
    nod = S.zip_safety("Nodata", "IN")
    check(nod["tier"] == "unknown" and nod["violent"] is None, "null rate → UNKNOWN")
    missing = S.zip_safety("Nowhere At All", "IN")
    check(missing["tier"] == "unknown", "absent city → UNKNOWN, never a tier")

    # name normalization: zips.db stores either "Gary" or "Gary, IN"
    check(S._norm("Testville", "IN") == "Testville, IN"
          and S._norm("Testville, IN", "IN") == "Testville, IN",
          "city key normalizes both stored name forms")
    check(S.zip_safety("Testville, IN", "IN")["tier"] == "very_safe",
          "already-suffixed name resolves")

    # ── FAIL-CLOSED gate ──
    check(S.passes(vs, "very_safe") and S.passes(vs, "safe"),
          "very safe passes a very-safe bar")
    check(not S.passes(avg, "safe"), "average does NOT pass a safe bar")
    check(not S.passes(hi, "average"), "high does NOT pass an average bar")
    check(not S.passes(nod, "high"), "UNKNOWN fails even the loosest bar by default")
    check(not S.passes(missing, "high"), "absent city fails by default")
    check(S.passes(nod, "safe", allow_unknown=True),
          "unknown passes only with the explicit opt-in")
finally:
    S._crime_table = _orig
    S._crime_table.cache_clear() if hasattr(S._crime_table, "cache_clear") else None

# ── board integration: the gate actually filters rows ──
loose = H.zip_board_hh(state="IN", units=4, top=40, max_tier="high", allow_unknown=True)
check(len(loose) > 0, "board returns rows when unverified markets are allowed")
check(all("safety" in r for r in loose), "every row carries a safety record")
strict = H.zip_board_hh(state="IN", units=4, top=40, max_tier="safe",
                        allow_unknown=False)
check(len(strict) <= len(loose), "the safety gate can only remove rows, never add")
check(all(r["safety"]["tier"] != "unknown" for r in strict),
      "no UNVERIFIED row survives the default gate")
check(all(S.TIER_ORDER[r["safety"]["tier"]] <= S.TIER_ORDER["safe"] for r in strict),
      "every surviving row is at or below the requested tier")
if len(strict) > 1:
    order = [S.TIER_ORDER[r["safety"]["tier"]] for r in strict]
    check(order == sorted(order), "board is ordered safest-first")

# the fake socioeconomic proxy must not leak into the safety layer
src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "safety.py")).read()
check("crime_index" not in src.split('"""')[2] if src.count('"""') > 2 else True,
      "crime_index is referenced only in the warning docstring, never used")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} safety checks passed.")
print(f"   crime table: {have_data} cities with verified figures; "
      f"IN board {len(strict)} rows at 'safe' vs {len(loose)} unfiltered")
sys.exit(0)
