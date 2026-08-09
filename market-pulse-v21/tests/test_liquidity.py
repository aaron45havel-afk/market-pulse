"""Liquidity + quality/value screen checks.

Run:  python tests/test_liquidity.py      (exit 0 = all pass)

Neither module can be verified against live data from the dev sandbox —
sec.gov, Yahoo and Finnhub are all unreachable here, which is why the
fetcher runs in CI. So the domain logic is written to be provable
offline, and these are the properties that matter:

  * A metric we cannot compute is NEVER silently treated as passing.
    In a micro-cap universe, sparse disclosure is not random — it
    correlates with exactly the companies you would least like a screen
    to wave through.
  * Turnover quartiles are cut WITHIN a size bucket. Cut globally, the
    liquidity axis collapses into a second, noisier size axis and the
    effect the screen exists to capture disappears.
  * The high-turnover cell is an exclusion, not merely a low rank. It
    returned ~0.1%/yr in the research; being fourth-best is not the
    finding.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import liquidity as L
import quality_value as QV

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── turnover: the academic metric ───────────────────────────────────
# 10M shares out, 40k traded every session for a year -> 10.08M shares
# traded -> just over 1.0x turnover.
vols = [40_000] * 252
t = L.annual_turnover(vols, 10_000_000)
check(t is not None and abs(t["turnover"] - 1.008) < 0.001,
      f"turnover is total volume / shares outstanding (got {t and t['turnover']})")
check(t["zero_days"] == 0 and t["median_daily_volume"] == 40_000,
      "a fully-traded year has no zero days and a clean median")
check(not t["annualised"], "a full year of sessions is not scaled")

# Half the sessions dark — the micro-cap norm.
quiet = L.annual_turnover([0, 5_000] * 126, 10_000_000)
check(quiet["zero_days"] == 126 and quiet["zero_day_pct"] == 50.0,
      "days with no print are counted, not dropped")
check(quiet["median_daily_volume"] == 2_500,
      f"median across a half-dark year sits between (got {quiet['median_daily_volume']})")
# Turnover is a SUM, so it is invariant to whether dark days are zeroed
# or dropped — which is worth knowing, because it means the academic
# metric is robust to that choice while the tradeability metrics are not.
dropped = L.annual_turnover([5_000] * 126, 10_000_000, sessions_expected=126)
check(abs(dropped["turnover"] - quiet["turnover"]) < 1e-9,
      "turnover is a sum, so zeroing vs dropping dark days cannot change it")
check(dropped["median_daily_volume"] == 2 * quiet["median_daily_volume"],
      "but dropping them DOUBLES the median daily volume — the tradeability lie")
check(dropped["zero_days"] == 0 and quiet["zero_days"] == 126,
      "and erases the zero-day count entirely, which for this cohort is the "
      "truer illiquidity signal")

# ── refusing to invent a number ─────────────────────────────────────
check(L.annual_turnover(vols, None) is None, "no share count -> no turnover")
check(L.annual_turnover(vols, 0) is None, "zero shares outstanding -> no turnover")
check(L.annual_turnover([40_000] * 10, 10_000_000) is None,
      "ten sessions cannot support an ANNUAL turnover figure")
check(L.annual_turnover([], 10_000_000) is None, "no history -> no turnover")
# A recent listing is annualised rather than reported as artificially quiet.
part = L.annual_turnover([40_000] * 126, 10_000_000)
check(part["annualised"] and abs(part["turnover"] - 1.008) < 0.001,
      "a half-year history is scaled up, not read as half the turnover")

# ── quartiles must be cut within size ───────────────────────────────
# Micro-caps trade less than large-caps in absolute terms. A global cut
# would file nearly every micro-cap under 'low' by construction.
rows = []
for i in range(40):
    rows.append({"ticker": f"MICRO{i}", "market_cap": 100_000_000, "turnover": 0.1 + i * 0.02})
for i in range(40):
    rows.append({"ticker": f"BIG{i}", "market_cap": 50_000_000_000, "turnover": 2.0 + i * 0.05})

within = L.classify(rows, within_size=True)
glob = L.classify(rows, within_size=False)
micro_low_within = sum(1 for r in within if r["size_bucket"] == "micro" and r["liq_bucket"] == "low")
micro_low_global = sum(1 for r in glob if r["size_bucket"] == "micro" and r["liq_bucket"] == "low")
check(micro_low_global > micro_low_within,
      f"a global cut floods micro-caps into 'low' ({micro_low_global} vs {micro_low_within})")
check(micro_low_within <= 12,
      f"cutting within size keeps 'low' to roughly a quarter of the cohort (got {micro_low_within})")
check(all(r["liq_cohort"] == r["size_bucket"] for r in within),
      "each name is ranked against its own size cohort")
check(all(not r["liq_cuts_fallback"] for r in within),
      "40 names per cohort is enough to cut real quartiles")

# All four buckets are populated within a cohort.
micro_buckets = {r["liq_bucket"] for r in within if r["size_bucket"] == "micro"}
check(micro_buckets == set(L.QUARTILE_NAMES),
      f"every quartile is represented within a cohort (got {sorted(micro_buckets)})")

# ── too few names: say so rather than pretend ───────────────────────
tiny = L.classify([{"market_cap": 1e8, "turnover": 0.5}] * 5)
check(all(r["liq_cuts_fallback"] for r in tiny),
      "a 5-name cohort is flagged as using fallback cut points")
check(L.quartile_cuts([0.1, 0.2, 0.3]) is None,
      "three values do not define quartiles")

# ── the gate ────────────────────────────────────────────────────────
check(L.passes({"liq_bucket": "low"}), "the quiet quartile passes")
check(not L.passes({"liq_bucket": "high"}), "the churned quartile does not")
check(not L.passes({"liq_bucket": None}),
      "an UNMEASURED name never passes a screen built on the measurement")
check(not L.passes({}), "a row with no bucket at all fails closed")
# The exclusion is independent of where the bar is set.
check(not L.passes({"liq_bucket": "high"}, max_bucket="high"),
      "even at the widest bar, the high-turnover cell stays excluded by default")
check(L.passes({"liq_bucket": "high"}, max_bucket="high", exclude_high=False),
      "...unless the user turns the exclusion off deliberately")
check(L.passes({"liq_bucket": "mid_low"}, max_bucket="mid_low"),
      "widening the bar admits the next quartile up")
check(not L.passes({"liq_bucket": "mid_high"}, max_bucket="mid_low"),
      "but not the one above that")

# ── size bands ──────────────────────────────────────────────────────
check(L.size_bucket(40_000_000) == "nano", "under $50M is nano")
check(L.size_bucket(100_000_000) == "micro", "$100M is micro")
check(L.size_bucket(1_000_000_000) == "small", "$1B is small")
check(L.size_bucket(None) is None and L.size_bucket(0) is None,
      "no market cap -> no size bucket")

# ── tradeability: the number the research does not give you ─────────
# 2,500 shares/day median at $4 = $10k/day. At 20% participation that is
# $2k/day of capacity, so a $25k position takes ~12 sessions to build.
tr = L.tradeability(quiet, 4.0, position_usd=25_000)
check(tr["median_daily_usd"] == 10_000, "median daily dollar volume")
check(abs(tr["days_to_build"] - 12.5) < 0.1,
      f"days to build at 20% of the tape (got {tr['days_to_build']})")
check(not tr["impractical"], "twelve sessions is slow but doable")
thin = L.tradeability(L.annual_turnover([0, 0, 0, 100] * 63, 10_000_000), 2.0)
check(thin["impractical"], "a name that barely prints is flagged impractical")
check(L.tradeability(None, 5.0) is None and L.tradeability(quiet, None) is None,
      "no turnover or no price -> no tradeability claim")

# ── coverage ────────────────────────────────────────────────────────
cov = L.coverage(within + [{"turnover": None, "liq_bucket": None}])
check(cov["measured"] == 80 and cov["unmeasured"] == 1,
      "coverage separates what we measured from what we could not")

# ═══ quality / value ════════════════════════════════════════════════
# A cash-rich, capital-light, cheap dividend payer: passes everything.
good = {
    "price": 10.0, "market_cap": 100_000_000,
    "cash": 40_000_000, "total_debt": 5_000_000, "equity": 60_000_000,
    "capex": 1_000_000, "ocf": 12_000_000,
    "operating_income": 9_000_000, "revenue": 40_000_000,
    "dividends_per_share": 0.40, "eps": 1.20, "book_value_per_share": 6.0,
}
g = QV.evaluate(good)
check(g["passed"] == 7 and g["known"] == 7, f"the ideal name passes all seven (got {g['passed']}/{g['known']})")
check(g["metrics"]["net_cash"] == 35.0, f"net cash is (cash-debt)/mcap (got {g['metrics']['net_cash']})")
check(g["metrics"]["pe"] == 8.33 and g["metrics"]["pb"] == 1.67, "P/E and P/B")
check(g["metrics"]["capex"] == 0.08, "capex as a share of operating cash flow")
check(g["metrics"]["dividend"] == 4.0, "dividend yield")
check(QV.meets(g), "and it clears the screen")

# ── missing data must never read as passing ─────────────────────────
sparse = {"price": 10.0, "market_cap": 100_000_000, "eps": 1.0}
s = QV.evaluate(sparse)
check(s["verdicts"]["capex"] is None and s["verdicts"]["margin"] is None,
      "unreported capex and margin are UNKNOWN, not passes")
check("capex" in s["unknown"] and "dividend" in s["unknown"],
      "unknowns are named so the UI can show what's missing")
check(s["known"] < 5 and not QV.meets(s),
      "a name measurable on almost nothing cannot clear the screen")
check(s["passed"] <= s["known"], "you cannot pass more tests than you ran")

# Passing everything you happen to have data for is not the same thing.
two_of_two = QV.evaluate({"price": 10.0, "eps": 1.0, "book_value_per_share": 8.0})
check(two_of_two["passed"] == 2 and two_of_two["known"] == 2,
      "two tests run, two passed")
check(not QV.meets(two_of_two),
      "2-for-2 does not clear a screen that needs five measurable criteria")
check(two_of_two["pct_of_known"] == 100,
      "the percentage is of what was KNOWN — which is why the raw count is shown too")

# ── the traps in each individual metric ─────────────────────────────
check(QV.debt_to_equity(5_000_000, -2_000_000) is None,
      "negative equity is unknown, not a flatteringly low debt ratio")
check(QV.capex_to_ocf(1_000_000, -5_000_000) is None,
      "a cash-burning company is not capital-light")
check(QV.pe_ratio(10.0, -2.0) is None,
      "a loss-maker has no P/E — never a negative one that sorts as 'cheapest'")
check(QV.pe_ratio(10.0, 0) is None, "zero earnings is not an infinite bargain")
check(QV.pb_ratio(10.0, -1.0) is None, "negative book value is unknown")
check(QV.net_cash_pct(10_000_000, 30_000_000, 100_000_000) == -20.0,
      "more debt than cash reports NEGATIVE net cash rather than hiding it")
check(QV.evaluate({"cash": 1e7, "total_debt": 3e7, "market_cap": 1e8})["verdicts"]["net_cash"] is False,
      "and that fails the cash-rich test")
check(QV.dividend_yield(0.40, 0) is None, "no price -> no yield")
check(QV.net_cash_pct(1e7, None, 1e8) == 10.0,
      "absent debt is treated as zero debt only in the net-cash sum, where the filing implies it")

# ── the classic value trap shape ────────────────────────────────────
# Cheap on P/E and P/B, but levered, capital-hungry and thin-margined.
trap = {
    "price": 5.0, "market_cap": 50_000_000,
    "cash": 1_000_000, "total_debt": 40_000_000, "equity": 20_000_000,
    "capex": 9_000_000, "ocf": 10_000_000,
    "operating_income": 1_000_000, "revenue": 80_000_000,
    "dividends_per_share": 0.0, "eps": 0.60, "book_value_per_share": 5.0,
}
tp = QV.evaluate(trap)
check(tp["verdicts"]["pe"] and tp["verdicts"]["pb"],
      "the trap is genuinely cheap on both price multiples")
check(not tp["verdicts"]["debt"] and not tp["verdicts"]["capex"] and not tp["verdicts"]["margin"],
      "and fails on leverage, reinvestment and margin")
check(not QV.meets(tp), "so cheapness alone does not clear the screen")
check(tp["passed"] == 2, f"it passes exactly the two price tests (got {tp['passed']})")

# ── required tests ──────────────────────────────────────────────────
no_div = {**good, "dividends_per_share": 0.0}
nd = QV.evaluate(no_div)
check(nd["passed"] == 6, "a non-payer still passes the other six")
check(QV.meets(nd), "and clears a screen that does not require income")
check(not QV.meets(nd, require=("dividend",)),
      "but never one that does — no amount of cheapness substitutes for a dividend")

# ── the reason line is numbers, not adjectives ──────────────────────
line = QV.summarize(g)
for frag in ("8.33x earnings", "1.67x book", "35% of cap", "22.5% op margin", "4% yield"):
    check(frag in line, f"summary states {frag} (got: {line})")
check("not enough reported data" in QV.summarize(QV.evaluate({})),
      "an unmeasurable name says so rather than printing an empty verdict")

# ── dials move, and moving them changes outcomes ────────────────────
strict = QV.evaluate(good, {"max_pe": 5.0})
check(not strict["verdicts"]["pe"] and strict["passed"] == 6,
      "tightening the P/E ceiling flips exactly that one test")
check(QV.evaluate(good, {"max_pe": 5.0})["limits"]["max_pe"] == 5.0,
      "the limits actually used are returned, so the UI can show the bar")

# ── degenerate inputs must not raise ────────────────────────────────
for bad_input in ({}, {"price": None}, {"price": "abc", "eps": "xyz"},
                  {"market_cap": float("inf")}, {"eps": float("nan"), "price": 10.0}):
    try:
        QV.evaluate(bad_input)
    except Exception as e:                                    # noqa: BLE001
        _FAILS.append(f"evaluate({bad_input}) raised {type(e).__name__}: {e}")
    _COUNT += 1
for bad_vols in ([None] * 300, [0] * 300, [-5] * 300):
    try:
        L.annual_turnover(bad_vols, 1_000_000)
    except Exception as e:                                    # noqa: BLE001
        _FAILS.append(f"annual_turnover raised {type(e).__name__}: {e}")
    _COUNT += 1

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} liquidity + quality/value checks passed.")
print(f"   micro-cap cohort: {micro_low_within} of 40 in the quiet quartile cut within size, "
      f"vs {micro_low_global} under a global cut")
print(f"   value trap: cheap on P/E and P/B, {tp['passed']}/7 overall → screened out")
sys.exit(0)
