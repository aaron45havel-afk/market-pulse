"""The HOLT multiple/growth grid — proved without a network.

Run:  python tests/test_holt.py      (exit 0 = all pass)

TWO THINGS HERE CAN DO REAL DAMAGE, and most of these checks are about
them rather than about arithmetic.

The first is the multiple guard. This board ranks by cheapness, so a
multiple that is wrong-LOW does not sit harmlessly mid-list — it sorts to
number one. The shipped compounders file has Booking Holdings at 0.8x
P/FCF and 31 names under 2x. Without the guard those data faults are the
entire top of the board, dressed as the best ideas on it.

The second is the column axis. It is forward growth — growth that has not
happened. Any code path that lets it be read as a fact rather than a
probability turns this screen into a record of what you would have earned
knowing the answer in advance.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import holt as H

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── the grid, as transcribed ──
check(len(H.GRID) == 9 and all(len(v) == 6 for v in H.GRID.values()),
      "nine multiple rows by six growth columns")
check(H.GRID["50x+"][5] == -2.2 and H.GRID["0-10x"][4] == 11.2,
      "the two cells the source calls out are transcribed correctly")
check(all(v < 0 for v in H.GRID["50x+"]),
      "EVERY cell of the 50x+ row is negative in the raw table — the "
      "source's whole point, and the thing the board must not soften")
check(all(v < 0 for v in H.GRID["negative"]),
      "and so is every cell of the negative-earnings row")


# ── refusing a multiple that cannot be true ──
check(H.multiple_fault(18.7) is None, "an ordinary multiple is usable")
check(H.multiple_fault(0.8) is not None and "sort first" in H.multiple_fault(0.8),
      "0.8x IS REFUSED — Booking Holdings carries that in the shipped file, "
      "and on a board that ranks by cheapness it would be the top row")
check(H.multiple_fault(0.1) is not None, "and 0.1x, which three names carry")
check("data fault" in H.multiple_fault(1.5),
      "the refusal says it is a data fault rather than implying the "
      "company is that cheap")
check(H.multiple_fault(None) == "no multiple"
      and H.multiple_fault("abc") is not None
      and H.multiple_fault(float("nan")) is not None,
      "missing and junk multiples are refused with a reason, not silently")
check(H.multiple_fault(-4.0) is None,
      "a NEGATIVE multiple is allowed through — negative FCF is a real "
      "state and HOLT gives it its own row; it is small values near zero "
      "that are impossible")
check(H.multiple_fault(7.0, 40.0) is not None,
      "7x against its own 40x fifteen-year median is refused — a de-rating "
      "that large happens, but far more often it is a bad price or a bad "
      "share count")
check(H.multiple_fault(8.0, 40.0) is None,
      "exactly a fifth is allowed; the guard is UNDER a fifth, and the "
      "boundary belongs to the company rather than to the refusal")
check(H.multiple_fault(8.0, 30.0) is None,
      "and 8x against a 30x median is comfortably inside the band")
check(H.multiple_fault(21.9, 1280.7) is None,
      "21.9x against a 1,280x median is ALLOWED. That median does not mean "
      "the company once traded at 1,280x free cash flow — it means free "
      "cash flow was near zero for most of those years, so the ratio "
      "exploded. Refusing on it would drop a name for finally generating "
      "cash, and fifteen of the twenty-two this guard first caught were "
      "exactly that")
check(H.multiple_fault(5.0, 55.0) is not None,
      "while a median inside the usable range still refuses a divergent "
      "current value")
# The ceiling was 60x on the first pass and let the two names at the TOP
# of the live board straight through — both had rich-but-real medians
# that happened to clear it.
check(H.multiple_fault(7.8, 76.3) is not None,
      "Globant at 7.8x against a 76x median is REFUSED. A 60x ceiling sat "
      "around the 93rd percentile of medians and waved this through to "
      "number one on the board — 'rich' is not the same as 'meaningless'")
check(H.multiple_fault(8.7, 63.3) is not None,
      "and Trade Desk at 8.7x against 63x, which was number three")
check(H.multiple_fault(21.9, 428.1) is None
      and H.multiple_fault(21.9, 1280.7) is None,
      "while Snap's 428x and Inspire's 1,280x medians are still treated as "
      "the broken numbers they are — FCF near zero, not a price ever paid")
check(H.multiple_fault(24.2, 0.0) is None,
      "a median of zero disables the divergence test rather than tripping "
      "it — twenty-one companies carry one. There is no low ceiling to "
      "match the high one, because with a median under 2x the test could "
      "only fire below 0.4x, which the absolute floor already refuses; a "
      "guard there would be unreachable code claiming to do a job")
check(H.MEDIAN_USABLE_MAX == 150.0,
      "the usable band is pinned, since every check above references the "
      "constants and would survive the numbers drifting")
check(H.multiple_fault(18.0, None) is None,
      "and with no median to compare against, the absolute floor decides "
      "alone rather than the name being dropped")
check(H.multiple_fault(3000) is not None, "an absurd high multiple is refused too")


# ── bands ──
check(H.multiple_band(8) == "0-10x" and H.multiple_band(12) == "10-15x"
      and H.multiple_band(60) == "50x+" and H.multiple_band(-3) == "negative",
      "multiples land in the right HOLT row")
check(H.multiple_band(10) == "10-15x" and H.multiple_band(50) == "50x+",
      "and the boundaries belong to the upper band, matching the table's "
      "own labels")
check(H.multiple_band(0.8) is None,
      "a refused multiple has NO band — it cannot be placed on the grid at "
      "all, rather than being placed in the cheapest one")
check(H.growth_band(-2) == "neg" and H.growth_band(0) == "0-5"
      and H.growth_band(19.9) == "15-20" and H.growth_band(20) == "20+",
      "growth lands in the right column")
check(H.growth_band(None) is None and H.growth_band("x") is None,
      "and an unknown growth rate has no column rather than defaulting to "
      "the worst or the best one")


# ── the disjoint-window arithmetic ──
# A company at 100 revenue growing 10% then 20% over two five-year windows.
_r5 = 100 * 1.1 ** 5
_r0 = _r5 * 1.2 ** 5
_g5 = 20.0
_g10 = ((_r0 / 100) ** 0.1 - 1) * 100
check(abs(H.early_cagr(_r0, _g5, _g10) - 10.0) < 0.01,
      "early_cagr recovers the FIRST window's rate from the two stored "
      "overlapping ones — 10% here, not the 14.9% blend the overlap gives")
check(H.early_cagr(0, 10, 10) is None and H.early_cagr(None, 10, 10) is None
      and H.early_cagr(100, None, 10) is None,
      "and refuses rather than raising when the inputs cannot support it")
check(H.early_cagr(100, -100, 5) is None,
      "a -100% CAGR would divide by zero; it returns None instead")


# ── the transition matrix ──
check(set(H.DEFAULT_TRANSITIONS) == set(H.GROWTH_BANDS),
      "every growth band has a measured forward distribution")
for src, row in H.DEFAULT_TRANSITIONS.items():
    check(abs(sum(row.values()) - 1.0) < 0.005,
          f"the {src} row is a probability distribution (sums to 1)")
check(H.DEFAULT_TRANSITIONS["20+"]["20+"] == 0.3593,
      "36% OF 20%+ GROWERS STAY THERE. Not the 61% the overlapping "
      "windows suggest — that version credits the same five years to both "
      "sides of the comparison and is the single easiest way to make this "
      "screen look far better than it is")
_base = H.DEFAULT_TRANSITIONS
check(_base["20+"]["20+"] > _base["10-15"]["20+"] > _base["0-5"]["20+"],
      "past growth does predict future growth — monotonically, which is "
      "why the proxy is worth using at all")
check(_base["20+"]["neg"] > 0.1,
      "and 13% of them go outright negative, which is why it is a bet")

_m = H.transition_matrix([(25, 25), (30, 22), (21, 5), (2, 3)] * 10)
check(_m["20+"]["20+"] == 2 / 3, "transition_matrix measures what it is given")
check("0-5" not in H.transition_matrix([(2, 3)] * 5),
      f"a band with fewer than {H.MIN_BUCKET_N} observations is OMITTED "
      f"rather than published — four companies are not a base rate, and a "
      f"row built from them would look identical to one built from four "
      f"hundred")
check(H.transition_matrix([]) == {} and H.transition_matrix(None) == {},
      "and an empty universe yields no matrix rather than raising")


# ── the score: HOLT's row, weighted by the bet ──
_ev = H.expected_excess("0-10x", "20+")
check(5.0 < _ev < 5.8,
      f"a cheap 20%+ grower expects about +5.4%, NOT the +9.6% the raw "
      f"table shows — that number assumes you already know it will grow, "
      f"and 36% of the time it does (got {_ev:.2f}%)")
check(_ev < H.GRID["0-10x"][5],
      "weighting always pulls the best cell DOWN, because the bet is not "
      "a certainty")
check(all(H.expected_excess("50x+", g) < 0 for g in H.GROWTH_BANDS),
      "EVERY CELL OF THE 50x+ ROW IS STILL NEGATIVE after weighting — the "
      "source's headline, and the one thing this board exists to say")
check(all(H.expected_excess("35-50x", g) < 0 for g in H.GROWTH_BANDS),
      "and so is every cell of 35-50x")
check(H.expected_excess("50x+", "20+") > H.expected_excess("50x+", "neg"),
      "growth still helps within a row — it just cannot rescue it")

_col_span = H.expected_excess("0-10x", "20+") - H.expected_excess("50x+", "20+")
_row_span = H.expected_excess("0-10x", "20+") - H.expected_excess("0-10x", "neg")
check(_col_span > _row_span * 1.4,
      f"THE MULTIPLE MATTERS ABOUT TWICE AS MUCH AS THE GROWTH: choosing "
      f"the row is worth {_col_span:.1f} points, guessing the column right "
      f"{_row_span:.1f}. The row is the half you control")
check(H.expected_excess("bogus", "20+") is None
      and H.expected_excess("0-10x", None) is None,
      "an unknown band scores nothing rather than zero")


# ── quality gates ──
_junk = {"ticker": "NUTX", "pfcf_now": 4.6, "rev_cagr5": 193.6,
         "roic_med": -110.1, "fcf_conv": 0.0}
_s = H.score(_junk)
check(_s["expected_excess"] is not None and _s["flags"],
      "a negative-ROIC shell still SCORES — the grid does not know about "
      "quality — but it is flagged, which is what keeps it off the board")
check(any("ROIC" in f for f in _s["flags"])
      and any("reaching cash" in f for f in _s["flags"]),
      "and the flags name what is wrong, in words")
check(H.quality_flags({"cyclical": True})[0].startswith("Flagged cyclical"),
      "a cyclical name is flagged — its five-year sales CAGR is a "
      "commodity price, not a business compounding")
check(H.quality_flags({"ni_years_seen": 15, "ni_pos_years": 6}),
      "so is one that loses money in most years")
check(H.quality_flags({"roic_med": 20.1, "fcf_conv": 188.0,
                       "cyclical": False}) == [],
      "and a genuinely clean name draws no objection")
check(H.quality_flags({}) == [],
      "missing quality data raises no flag — an absence is not an "
      "accusation, and every field here is optional upstream")


# ── the full verdict ──
_good = {"ticker": "MELI", "name": "MercadoLibre", "pfcf_now": 18.6,
         "pfcf_med": 19.0, "rev_cagr5": 43.8, "roic_med": 50.3,
         "fcf_conv": 470.0, "cyclical": False}
_v = H.score(_good)
check(_v["multiple_band"] == "15-20x" and _v["growth_band"] == "20+",
      "a clean name lands in its cell")
check(_v["clean"] is True and _v["flags"] == [], "and passes the gates")
check(_v["p_repeat"] == 0.3593,
      "the board carries the PROBABILITY it repeats, so the column reads "
      "as a bet the user is taking rather than a fact about the company")
check(0 < _v["p_decline"] < 1 and _v["p_decline"] > 0.5,
      f"and the probability growth DECLINES from here — {_v['p_decline']:.0%}, "
      f"the number a screen like this normally hides")
check(_v["grid_if_realised"] == 6.9 and _v["expected_excess"] < 6.9,
      "both numbers are shown: what the cell pays IF the growth lands, and "
      "what it is worth given it probably will not")
_ref = H.score({"ticker": "BKNG", "pfcf_now": 0.8, "pfcf_med": 22.5,
                "rev_cagr5": 12.0})
check(_ref["multiple"] is None and _ref["multiple_band"] is None
      and _ref["expected_excess"] is None and _ref["multiple_fault"],
      "a refused name carries NO multiple, NO band and NO score — nothing "
      "downstream can accidentally rank it")
check(set(H.score({}).keys()) == set(_v.keys()),
      "an empty row and a full one return the same keys, so a caller "
      "reading a field on a refusal gets None rather than a KeyError")


# ── ranking a universe ──
UNIVERSE = [
    {"ticker": "CHEAP", "pfcf_now": 8.0, "pfcf_med": 9.0, "rev_cagr5": 22.0,
     "roic_med": 18.0, "fcf_conv": 110.0},
    {"ticker": "RICH", "pfcf_now": 62.0, "pfcf_med": 60.0, "rev_cagr5": 25.0,
     "roic_med": 25.0, "fcf_conv": 120.0},
    {"ticker": "MID", "pfcf_now": 17.0, "pfcf_med": 18.0, "rev_cagr5": 12.0,
     "roic_med": 14.0, "fcf_conv": 95.0},
    {"ticker": "JUNK", "pfcf_now": 5.0, "pfcf_med": 6.0, "rev_cagr5": 190.0,
     "roic_med": -95.0, "fcf_conv": 0.0},
    {"ticker": "BROKEN", "pfcf_now": 0.8, "pfcf_med": 22.5, "rev_cagr5": 12.0},
]
_r = H.rank(UNIVERSE)
check([s["ticker"] for s in _r["clean"]] == ["CHEAP", "MID", "RICH"],
      "the clean board sorts by expected excess, cheapest-and-growing first")
check(_r["clean"][-1]["ticker"] == "RICH"
      and _r["clean"][-1]["expected_excess"] < 0,
      "and a 62x name lands LAST with a negative expectation despite 25% "
      "growth — which is the entire finding")
check([s["ticker"] for s in _r["flagged"]] == ["JUNK"],
      "the negative-ROIC shell is held out of the clean board")
check([s["ticker"] for s in _r["refused"]] == ["BROKEN"],
      "and the broken multiple is REFUSED — not ranked first, which is "
      "where its 0.8x would otherwise have put it")
check(_r["counts"]["refused"] == 1 and _r["counts"]["flagged"] == 1,
      "refusals and flags are counted, not quietly dropped")
check(all(s["ticker"] != "BROKEN" for s in _r["rows"]),
      "and nothing refused reaches the rendered rows under any setting")
check([s["ticker"] for s in H.rank(UNIVERSE, require_clean=False)["rows"]]
      == ["CHEAP", "MID", "RICH", "JUNK"],
      "asking for the flagged ones puts them after the clean ones, never "
      "interleaved where a reader would miss the distinction")

_capped = H.rank(UNIVERSE, max_band="10-15x")
check([s["ticker"] for s in _capped["clean"]] == ["CHEAP"],
      "capping the multiple is the one filter the buyer actually controls")
check(all(s["multiple"] < 15 for s in _capped["clean"]), "and it bites")
try:
    H.rank(UNIVERSE, max_band="nonsense")
    check(False, "an unknown cap should raise")
except ValueError:
    check(True, "an unknown cap raises rather than silently filtering nothing")

_cen = H.grid_census(_r["clean"])
check(_cen.get("0-10x|20+") == 1 and _cen.get("50x+|20+") == 1,
      "the census counts each occupied cell so the board can show its own "
      "shape")
check(H.grid_census([]) == {}, "and an empty board has an empty census")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} HOLT-grid checks passed.")
print(f"   {len(H.GRID)} multiple rows x {len(H.GROWTH_BANDS)} growth columns; "
      f"transitions from {H.TRANSITION_N:,} companies")
print(f"   multiple floor {H.MULTIPLE_FLOOR:.0f}x, "
      f"median-divergence guard 1/{H.MEDIAN_DIVERGENCE:.0f}")
sys.exit(0)
