"""Peter Lynch GARP screen — the arithmetic, proved against the board it broke.

Run:  python tests/test_lynch.py      (exit 0 = all pass)

There was no test file for this screen, and it published three monthly
snapshots of numbers that were not what their names said. Every fixture
below is a real row from data/lynch_snapshots/2026-08.json — the committed
snapshot was already the fixture, which is why none of this needed a
network call to find.

    ANF   493.6% "EPS growth"   from a base year of $0.05
    XNET  273.1% "EPS growth"   from a base year of $0.06
    VIPS  P/E 0.22              renminbi EPS over a dollar ADS price
    ASO    41.6% "3-yr CAGR"    measured between two QUARTER ends
    LULU  48 positive EPS years against a 2007 IPO
    LULU  D/E 0.00              global store fleet, debt tags unfiled
    PDD   capex/OCF 0.0004      $6.6m of capex on $15.3bn of cash flow

Each of those is a real figure from real filings. That is the point: none
of them looked like an error on the page.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lynch as L

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def facts(concept, entries, taxonomy="us-gaap", unit="USD"):
    return {"facts": {taxonomy: {concept: {"units": {unit: entries}}}}}


# ══════════════════════════════════════════════════════════════════
# A PERIOD IS NOT A FISCAL YEAR BECAUSE THE FILING SAYS FY
# ══════════════════════════════════════════════════════════════════
# `fp` and `form` describe the FILING. Academy Sports came through with
# three quarter-ends inside its five-year "annual" EPS history, and a
# 41.6% CAGR measured between two of them.

ASO = facts("EarningsPerShareDiluted", [
    {"start": "2023-01-29", "end": "2024-02-03", "val": 7.34, "fp": "FY", "form": "10-K"},
    {"start": "2024-02-04", "end": "2024-05-04", "val": 1.95, "fp": "FY", "form": "10-K"},
    {"start": "2024-05-05", "end": "2024-08-03", "val": 2.03, "fp": "FY", "form": "10-K"},
    {"start": "2024-08-04", "end": "2024-11-02", "val": 1.32, "fp": "FY", "form": "10-K"},
    {"start": "2024-02-04", "end": "2025-02-01", "val": 6.27, "fp": "FY", "form": "10-K"},
    {"start": "2025-02-02", "end": "2026-01-31", "val": 5.54, "fp": "FY", "form": "10-K"},
], unit="USD/shares")

s, concept, unit = L.eps_series(ASO)
check(sorted(s) == ["2024-02-03", "2025-02-01", "2026-01-31"],
      f"only the three genuinely annual periods survive (got {sorted(s)})")
check("2024-05-04" not in s and "2024-08-03" not in s and "2024-11-02" not in s,
      "the three quarter-ends are gone — every one of them carried fp=FY "
      "and form=10-K, so nothing but the duration could tell them apart")
check(unit == "USD/shares", "and the unit key is reported")

# A 52/53-week retail calendar must still pass. 2024-02-03 is a 53-week year.
check(L.ANNUAL_DAYS[0] <= 370 <= L.ANNUAL_DAYS[1],
      "the band admits a 53-week fiscal year")
check(L.ANNUAL_DAYS[0] > 100, "and excludes every quarter")

# An annual and a quarterly fact ending the SAME day were overwriting each
# other arbitrarily, because the dedupe keyed on end alone.
clash = facts("EarningsPerShareDiluted", [
    {"start": "2024-01-01", "end": "2024-12-31", "val": 9.00, "fp": "FY", "form": "10-K"},
    {"start": "2024-10-01", "end": "2024-12-31", "val": 2.25, "fp": "FY", "form": "10-K"},
], unit="USD/shares")
cs, _, _ = L.eps_series(clash)
check(cs == {"2024-12-31": 9.00},
      f"the annual fact wins over a quarter ending the same day (got {cs})")


# ══════════════════════════════════════════════════════════════════
# EPS IS READ IN WHATEVER CURRENCY EDGAR LISTS FIRST
# ══════════════════════════════════════════════════════════════════
# Line 275 of the old module accepted any unit key containing "shares", so
# CNY/shares passed while every other figure on the row was pinned to USD.

CNY = {"facts": {"us-gaap": {"EarningsPerShareDiluted": {"units": {
    "CNY/shares": [{"start": "2024-01-01", "end": "2024-12-31", "val": 70.74,
                    "fp": "FY", "form": "20-F"}],
}}}}}
s, _, unit = L.eps_series(CNY)
check(unit == "CNY/shares",
      f"a renminbi EPS is reported AS renminbi, not silently as dollars "
      f"(got {unit!r})")
check(s.get("2024-12-31") == 70.74, "the value is still available to display")

BOTH = {"facts": {"us-gaap": {"EarningsPerShareDiluted": {"units": {
    "CNY/shares": [{"start": "2024-01-01", "end": "2024-12-31", "val": 70.74,
                    "fp": "FY", "form": "20-F"}],
    "USD/shares": [{"start": "2024-01-01", "end": "2024-12-31", "val": 9.83,
                    "fp": "FY", "form": "20-F"}],
}}}}}
s, _, unit = L.eps_series(BOTH)
check(unit == "USD/shares" and s["2024-12-31"] == 9.83,
      "when both are filed, dollars win — the old code took whichever the "
      "dict happened to yield first")


# ══════════════════════════════════════════════════════════════════
# THE UNITS INVARIANT — one identity, both failure modes
# ══════════════════════════════════════════════════════════════════
# EPS x shares should reconstruct net income. On the live board the four
# US filers land at 0.82-1.52 and the five foreign filers at 2.81-7.52.

ok = L.units_check(eps=10.46, shares=48_800_000, net_income=510_000_000)
check(ok["ok"] is True, f"a US filer reconstructs (ratio {ok['ratio']})")

vips = L.units_check(eps=70.74, shares=520_000_000, net_income=5_100_000_000)
check(vips["ok"] is False,
      f"Vipshop's renminbi EPS against a dollar net income does not "
      f"(ratio {vips['ratio']})")

check(L.units_check(None, 1e6, 1e6)["ok"] is None, "a missing EPS abstains")
check(L.units_check(1.0, None, 1e6)["ok"] is None, "a missing share count abstains")
check(L.units_check(1.0, 1e6, None)["ok"] is None, "missing net income abstains")
check(L.units_check(1.0, 1e6, 0)["ok"] is None,
      "and a zero denominator abstains rather than dividing by it")
check(L.units_check(1.0, 1e6, 1e6)["ok"] is True,
      "the test FIRES or ABSTAINS — it never certifies, because it needs a "
      "share count and that is the figure most often wrong")

# It catches an ADS mismatch that a currency check alone would miss.
xnet = L.units_check(eps=3.31, shares=314_277_001, net_income=272_000_000)
check(xnet["ok"] is False,
      f"Xunlei fails the invariant even though its market cap agrees with "
      f"the feed to 3% (ratio {xnet['ratio']})")


# ══════════════════════════════════════════════════════════════════
# THE ADS RATIO — not in EDGAR, recoverable from the bulk feed
# ══════════════════════════════════════════════════════════════════
pdd = L.ads_ratio(market_cap=126_000_000_000, price=88.56, filed_shares=5_690_000_000)
check(pdd["ratio"] == 4 and pdd["clean"] is True,
      f"PDD's feed quotes per-ADS, four ordinary shares to one (got {pdd['ratio']})")

xn = L.ads_ratio(market_cap=1_700_000_000, price=5.34, filed_shares=314_277_001)
check(xn["ratio"] == 1 and xn["clean"] is True,
      f"Xunlei's feed quotes per-ordinary (got {xn['ratio']})")
check(pdd["ratio"] != xn["ratio"],
      "the two conventions differ between companies, which is exactly why "
      "this has to be measured per row rather than assumed once")

junk = L.ads_ratio(market_cap=1e9, price=10.0, filed_shares=137_000_000)
check(junk["clean"] is False,
      "a quotient that is neither ~1 nor a clean integer means the cap and "
      "the share count are not describing the same instrument")
check(L.ads_ratio(None, 10.0, 1e6)["ratio"] is None, "missing inputs abstain")
check(L.ads_ratio(1e9, 0, 1e6)["ratio"] is None, "and a zero price does not divide")


# ══════════════════════════════════════════════════════════════════
# A TROUGH BASE IS NOT A GROWTH RATE
# ══════════════════════════════════════════════════════════════════
ANF = L.growth({"2021-01-30": 4.20, "2022-01-29": 0.05, "2023-01-28": 6.22,
                "2024-02-03": 10.69, "2025-02-01": 10.46})
check(ANF["trough"] is True,
      "Abercrombie's $0.05 base against a $6.22 median is a trough")
check(ANF["cagr"] is None and ANF["cagr_raw"] is None,
      "so NO rate is reported — 493.6% was a recovery being sold as growth")

# Xunlei is the OTHER failure, and the trough test structurally cannot
# catch it: the base of 0.06 sits ABOVE the median of 0.04, because the
# whole history is near zero. Nothing is depressed — the company has no
# history at its current level.
XNET = L.growth({"2021-12-31": 0.00, "2022-12-31": 0.06, "2023-12-31": 0.04,
                 "2024-12-31": 0.00, "2025-12-31": 3.31})
check(XNET["trough"] is False,
      "Xunlei's base is not below the median — the whole series is near zero")
check(XNET["step"] is True,
      "but one year at 83x the median is a step change, not a rate")
check(XNET["cagr"] is None,
      "so 273.1% disappears. Without this test it would pass at the capped "
      "35% — over the 10% gate, and a fabricated number that looks "
      "reasonable is worse than an absurd one that does not")

# A real grower must survive untouched.
real = L.growth({"2021-12-31": 4.00, "2022-12-31": 4.60, "2023-12-31": 5.20,
                 "2024-12-31": 5.90, "2025-12-31": 6.60})
check(real["trough"] is False and real["step"] is False,
      "a steady climb is neither a trough nor a step")
check(12.0 < real["cagr"] < 13.0,
      f"and reports its real rate (got {real['cagr']})")
check(real["capped"] is False, "uncapped")

# ── the cap ──
hot = L.growth({"2021-12-31": 1.00, "2022-12-31": 2.00, "2023-12-31": 3.00,
                "2024-12-31": 4.00, "2025-12-31": 8.00})
check(hot["step"] is False,
      "hyper-growth with real history behind it is not a step change — "
      "8.00 against a median of 3.00 is 2.7x, well inside the multiple")
check(hot["cagr"] == L.EPS_GROWTH_CAP,
      f"growth past the cap is shown at the cap (got {hot['cagr']})")
check(hot["cagr_raw"] > L.EPS_GROWTH_CAP, "the raw rate is kept")
check(hot["capped"] is True, "and the row says it was capped")
check(L.EPS_GROWTH_CAP > L.EPS_GROWTH_MIN,
      "the cap sits above the gate, so bounding it cannot reject a company "
      "that would otherwise have passed")

# ── deceleration, which an endpoint CAGR cannot see ──
# Four of the six live rows had FALLING earnings and were rendered as
# growth stocks.
fading = L.growth({"2021-12-31": 4.00, "2022-12-31": 6.00, "2023-12-31": 9.00,
                   "2024-12-31": 10.69, "2025-12-31": 10.46})
check(fading["cagr"] is not None and fading["cagr"] > 20,
      "the CAGR still reads strongly")
check(fading["latest_yoy"] < 0,
      f"while the latest year was NEGATIVE (got {fading['latest_yoy']}%) — "
      "deceleration was the only thing that mattered to Lynch for a fast "
      "grower, and the endpoint rate is structurally blind to it")
check(fading["up_years"] == 3 and fading["of_years"] == 4,
      "and the up-years count carries the shape")

# ── the span comes from the dates, not a hardcoded 3.0 ──
gap = L.growth({"2019-12-31": 1.00, "2021-12-31": 2.00, "2023-12-31": 3.00,
                "2025-12-31": 4.00})
check(gap["span"] and gap["span"] > 5.5,
      f"a six-year span is measured as six years, not three (got {gap['span']})")

check(L.growth({})["cagr"] is None, "no series is no rate")
check(L.growth({"2025-12-31": 1.0})["cagr"] is None, "one point is no rate")
check(L.growth({"2024-12-31": -1.0, "2025-12-31": 2.0})["cagr"] is None,
      "and a swing through zero is not a growth rate either")


# ══════════════════════════════════════════════════════════════════
# THE MULTIPLE — cap over earnings, no share count anywhere
# ══════════════════════════════════════════════════════════════════
pe = L.price_earnings(market_cap=126_000_000_000, net_income=13_000_000_000)
check(abs(pe["pe"] - 9.69) < 0.05,
      f"PDD's honest multiple is about 9.7, against a printed 5.37 "
      f"(got {pe['pe']})")
check(pe["suspect"] is False, "and it is plausible")

low = L.price_earnings(market_cap=1_000_000_000, net_income=5_000_000_000)
check(low["suspect"] is True, "a P/E of 0.2 is not a valuation")
check(low["pe"] is None, "so the figure is withheld")
check(low["pe_raw"] == 0.2,
      "with the raw value kept, because deleting the row would hide the "
      "artefact instead of reporting it")

loss = L.price_earnings(1e9, -5e7)
check(loss["profitable"] is False and loss["pe"] is None,
      "a loss-maker has no P/E — Lynch wanted positive earnings")
check(L.price_earnings(None, 1e6)["pe"] is None, "no cap, no multiple")

check(L.peg(9.0, 30.0) == 0.3, "PEG is P/E over growth")
check(L.peg(9.0, None) is None, "and needs both")
check(L.peg(L.PE_SANE[0], L.EPS_GROWTH_CAP) >= 0.08,
      "with both inputs bounded, PEG has a hard floor instead of running "
      "to 0.01 — the nine live PEGs were 0.01 to 0.49, so DEEP fired on "
      "100% of rows while the default sort was PEG ascending")


# ══════════════════════════════════════════════════════════════════
# THE FULL ROW — every rejection names itself
# ══════════════════════════════════════════════════════════════════
GOOD = {
    "ticker": "TEST", "name": "Test Co", "price": 40.0,
    "market_cap": 4_000_000_000, "total_assets": 3_000_000_000,
    "equity": 1_500_000_000, "revenue": 2_000_000_000,
    "last_filing": "2026-02-01", "as_of": "2026-08-01",
    "net_income": 400_000_000, "shares": 100_000_000,
    "eps_by_year": {"2022-12-31": 2.60, "2023-12-31": 3.00,
                    "2024-12-31": 3.50, "2025-12-31": 4.00},
    "eps_unit": "USD/shares",
    "short_term_debt": 100_000_000, "long_term_debt": 200_000_000,
    "total_liabilities": 1_500_000_000,
    "capex": 100_000_000, "ocf": 500_000_000, "ppe": 800_000_000,
    "op_income": 500_000_000,
    "net_income_by_year": {"2023-12-31": 3e8, "2024-12-31": 3.5e8, "2025-12-31": 4e8},
    "equity_by_year": {"2023-12-31": 1.2e9, "2024-12-31": 1.35e9, "2025-12-31": 1.5e9},
}

r = L.evaluate(GOOD)
check(r["verdict"] == "pass", f"a clean company passes (got {r['reason']})")
check(r["pe_ratio"] == 10.0, "P/E from cap over earnings")
check(r["eps_3yr_cagr_pct"] is not None, "with a growth rate")
check(r["debt_to_equity"] == 0.2, "and a measured debt ratio")
check(r["size_band"] == "MID", "and a size band")

# EVERY rejection carries a reason code. Ten bare `return None` exits used
# to produce a file a strict screen and a broken parser would both write.
def why(**over):
    return L.evaluate({**GOOD, **over})["reason"]

check(why(market_cap=10_000_000) == "too_small", "below the floor")
check(why(price=0.40) == "penny", "sub-$1 is a delisting-risk proxy")
check(why(equity=-5e8) == "negative_equity", "negative equity")
check(why(equity=None) == "no_equity", "unfiled equity is NOT negative equity")
check(why(revenue=None) == "no_revenue", "no revenue tag")
check(why(revenue=2_000_000) == "tiny_revenue", "below the operating-business floor")
check(why(last_filing="2023-01-01") == "dormant", "a dormant registrant")
check(why(total_assets=100) == "cap_implausible", "cap orders of magnitude off assets")
check(why(net_income=-1e8, eps_by_year={"2022-12-31": -2.60, "2023-12-31": -3.00,
                                        "2024-12-31": -3.50, "2025-12-31": -1.00})
      == "unprofitable",
      "a loss-maker — with an EPS series that agrees with it, or the units "
      "invariant fires first and correctly")
check(why(market_cap=400_000_000) == "pe_suspect", "a P/E of 1.0 is withheld")
check(why(market_cap=8_000_000_000) == "pe_high", "a P/E of 20 is over the ceiling")
check(why(eps_by_year={"2024-12-31": 3.5, "2025-12-31": 4.0}) == "short_history",
      "fewer than four annual periods")
check(why(eps_by_year={"2022-12-31": 0.02, "2023-12-31": 3.00,
                       "2024-12-31": 3.50, "2025-12-31": 4.00}) == "trough_base",
      "a trough base")
check(why(eps_by_year={"2022-12-31": 3.90, "2023-12-31": 3.94,
                       "2024-12-31": 3.97, "2025-12-31": 4.00}) == "no_growth",
      "1% growth is under the floor")
check(why(capex=None) == "capex_unknown", "unfiled capex")
check(why(capex=400_000_000) == "capex_high", "capex over the ceiling")

# ── missing is never a pass ──
# LULU rendered D/E 0.00 while operating a global store fleet, because an
# unfiled tag became 0.0 and 0.0/equity is the best possible score on a
# gate that decides membership.
nodebt = L.evaluate({**GOOD, "short_term_debt": None, "long_term_debt": None,
                     "total_liabilities": 5_000_000_000})
check(nodebt["reason"] == "debt_unknown",
      f"neither debt tag filed and a liability stack too large to settle it "
      f"is UNKNOWN, not zero (got {nodebt['reason']})")
check(nodebt["debt_known"] is False, "and the row records that")

proved = L.evaluate({**GOOD, "short_term_debt": None, "long_term_debt": None,
                     "total_liabilities": 500_000_000})
check(proved["verdict"] == "pass",
      "but a liability stack inside the limit PROVES low debt without the "
      "tags — the schloss.debt_ok bound, reused rather than reimplemented")

# PDD passed "asset-light" on $6.6m of capex against $15.3bn of cash flow.
tiny_capex = L.evaluate({**GOOD, "capex": 100_000, "ppe": 800_000_000})
check(tiny_capex["reason"] == "capex_unknown",
      "capex at 0.01% of net PP&E is an unmeasured numerator, not an "
      "asset-light business")
check(tiny_capex.get("capex_suspect") is True, "and is flagged as such")

# The units invariant rejects before the multiple is ever computed.
bad_units = L.evaluate({**GOOD, "eps_by_year": {"2022-12-31": 18.0, "2023-12-31": 20.0,
                                                "2024-12-31": 24.0, "2025-12-31": 28.0}})
check(bad_units["reason"] == "units_unverified",
      "28.00 x 100m shares is $2.8bn against a filed $400m — the row is "
      "refused before any multiple is published")

check(L.evaluate({"ticker": "X"})["reason"] == "no_price",
      "an empty record is rejected by name, not by silence")
check(L.evaluate({"ticker": "X"})["verdict"] == "reject", "with a verdict")


# ══════════════════════════════════════════════════════════════════
# MOAT — bounded, and finally rendered
# ══════════════════════════════════════════════════════════════════
m = L.moat(GOOD)
check(m["avg_roe_3yr_pct"] is not None, "ROE computes")
check(m["op_margin_pct"] == 25.0,
      f"operating margin is RETURNED (got {m['op_margin_pct']}) — it was "
      "computed and discarded for three builds, which is why Xunlei's 1.4% "
      "beside a 27% ROE never showed")
check(m["years_positive_eps"] == 4, "consecutive positive years count")

wild = L.moat({**GOOD, "equity_by_year": {"2025-12-31": 1000.0},
               "net_income_by_year": {"2025-12-31": 4e8}})
check(wild["avg_roe_3yr_pct"] == 100.0,
      "ROE is bounded as equity approaches zero, the same guard compounders "
      "puts on ROIC")
check(wild["roe_capped"] is True, "and says it was bounded")


# ══════════════════════════════════════════════════════════════════
# CENSUS AND MEDIAN
# ══════════════════════════════════════════════════════════════════
rows = [L.evaluate(GOOD),
        L.evaluate({**GOOD, "market_cap": 10_000_000}),
        L.evaluate({**GOOD, "market_cap": 11_000_000}),
        L.evaluate({**GOOD, "capex": None})]
c = L.census(rows)
check(c["screened"] == 4 and c["passing"] == 1, "the funnel counts")
check(c["by_reason"]["too_small"] == 2, "grouped by cause")
check(c["unmeasured"] == 1,
      "and separates what we could not measure from what we rejected — "
      "without this, nine rows out of an unstated universe is unfalsifiable")
check("too_small" in c["labels"], "with a human label for each code")
check(L.census([])["screened"] == 0, "an empty board does not divide by zero")

# The page used the UPPER middle value and called it a median.
check(L.median([1, 2, 3]) == 2, "odd counts take the middle")
check(L.median([1, 2, 3, 4]) == 2.5,
      "even counts AVERAGE the two middles — the old code took the upper, "
      "so on six rows all three tiles printed Atour's row exactly")
check(abs(L.median([8.87, 8.96, 9.52, 1.61, 0.22, 5.37]) - 7.12) < 1e-9,
      "the six live P/Es have a true median of 7.12, not the 8.9 printed")
check(L.median([]) is None, "and nothing is not zero")


# ── the universe filter ──
# This function was unreachable from any test until its three fetches were
# made injectable, which is precisely why a null exchange took down a
# production run: `.get("exchange", "")` returns the default only when the
# key is ABSENT, and SEC ships it present-and-null for OTC filers.
import sec_edgar as SE
from lynch_screener import build_universe

_TICK = {
    "1": {"ticker": "AAA", "name": "Alpha Corp"},
    "2": {"ticker": "BBB", "name": "Beta Inc"},
    "3": {"ticker": "CCC", "name": "Gamma Ltd"},
    "4": {"ticker": "DDD", "name": "Delta Co"},
}
_EXCH = {
    "1": {"exchange": "Nasdaq"},
    "2": {"exchange": None},       # the crash: key present, value null
    "3": {"exchange": ""},         # SEC named no exchange
    "4": {"exchange": "OTC"},      # named, but not a major exchange
}
_DET = {c: {"sic": "7372", "state_desc": "California"} for c in ("1", "2", "3", "4")}

_got = {r["ticker"] for r in build_universe(_TICK, _EXCH, _DET)}
check(_got == {"AAA"},
      "a null exchange is rejected rather than raising AttributeError on "
      ".upper() — the crash that killed run #7")
check("CCC" not in _got, "an unnamed exchange is not a pass — fail closed")
check("DDD" not in _got, "OTC is named but is not a major exchange")

_named = sum(1 for v in _EXCH.values() if (v or {}).get("exchange"))
check(_named == 2,
      "2 of 4 exchanges are usable — the old log line printed len(exchanges) "
      "and claimed '7998 of 7998' one line before crashing on a null")

# Same defect one layer down: normalise at the parser so no consumer has
# to know the cell can be null.
_parsed = SE.parse_exchanges({
    "fields": ["cik", "name", "ticker", "exchange"],
    "data": [[1, "Alpha", "AAA", "Nasdaq"],
             [2, "Beta", "BBB", None],
             [3, "Gamma", "CCC", "  NYSE  "],
             [4, "Delta", "DDD"]],          # row short of the exchange cell
})
check(_parsed["2"]["exchange"] == "", "a null cell parses to empty, never None")
check(_parsed["3"]["exchange"] == "NYSE", "and is stripped")
check(_parsed["4"]["exchange"] == "", "a short row does not raise IndexError")
check(all(isinstance(v["exchange"], str) for v in _parsed.values()),
      "every parsed exchange is a string, so .upper() is always safe")
check(SE.parse_exchanges({}) == {} and SE.parse_exchanges(None) == {},
      "a failed fetch yields an empty map, not a crash")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} Lynch checks passed.")
print(f"   floor ${L.MARKET_CAP_MIN:,} · P/E {L.PE_SANE[0]}-{L.PE_MAX} · "
      f"growth >={L.EPS_GROWTH_MIN}% capped at {L.EPS_GROWTH_CAP}%")
print(f"   annual duration {L.ANNUAL_DAYS[0]}-{L.ANNUAL_DAYS[1]} days · "
      f"units invariant at {L.UNITS_TOLERANCE}")
sys.exit(0)
