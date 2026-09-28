"""XBRL series-extraction checks for the compounders build.

Run:  python tests/test_compounders_series.py      (exit 0 = all pass)

There was no test here, and two bugs lived in one 30-line function for as
long as the screen existed. Both came from taking the first thing found:

  * FIRST TAG WINS. ASC 606 moved essentially every US company off
    `us-gaap:Revenues` and onto `RevenueFromContractWithCustomer...` for
    fiscal years beginning after December 2017. Returning the first tag
    with three years meant locking onto the dead one. 114 of 901 names —
    13% of the board — were frozen at 2017 or earlier, Broadridge and
    Maximus among them, both showing a headline growth rate computed from
    a series that ended nine years before the page was rendered.

  * FIRST UNIT WINS. companyfacts keys values by unit. Foreign filers came
    through in TWD, JPY, CNY and INR and were then divided by a USD ADR
    price. Toyota's P/FCF read 0.2. Sony's read 0.0.

Neither was visible in the output as an error. Both produced numbers.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))

import refresh_compounders as R

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def fy(year, val, form="10-K"):
    return {"fy": year, "val": val, "form": form, "fp": "FY",
            "start": f"{year}-01-01", "end": f"{year}-12-31"}


def facts(**tags):
    """facts(**{"us-gaap:Revenues": {"USD": [fy(2015, 10)]}}) style."""
    out = {}
    for key, units in tags.items():
        tax, tag = key.split("__", 1)
        out.setdefault(tax.replace("_", "-"), {})[tag] = {"units": units}
    return out


REV = R.TAGS["revenue"]

# ── the ASC 606 cutover: the bug that froze 13% of the board ────────
# Old tag runs 2013-2017, new tag picks up 2018-2025. Neither alone is
# the company's history; only together are they.
asc606 = facts(
    us_gaap__Revenues={"USD": [fy(y, 100 + y - 2013) for y in range(2013, 2018)]},
    us_gaap__RevenueFromContractWithCustomerExcludingAssessedTax={
        "USD": [fy(y, 200 + y - 2018) for y in range(2018, 2026)]},
)
series, unit = R._annual_series(asc606, REV)
check(series, "an ASC 606 cutover company yields a series at all")
check(max(series) == 2025,
      f"the series reaches 2025, not the pre-606 tag's last year (got {max(series)})")
check(min(series) == 2013, f"and still starts at 2013 (got {min(series)})")
check(len(series) == 13, f"all 13 years survive the merge (got {len(series)})")
check(unit == "USD", f"unit reported as USD (got {unit!r})")

# The precise regression: the OLD behaviour returned the first tag with
# >=3 years and stopped. That is what produced Broadridge at fy2017.
check(max(series) != 2017,
      "REGRESSION GUARD: the series must not stop at the pre-606 tag's "
      "final year — that is the Broadridge/Maximus bug exactly")

# ── the DOMINANT basis wins overlapping years ───────────────────────
# This rule replaced "earlier slot wins", which let a tag with a couple of
# scattered years overwrite the middle of a real series — the Xerox bug
# below. Coverage decides; slot order only breaks ties.
overlap = facts(
    us_gaap__Revenues={"USD": [fy(y, 111) for y in (2018, 2019, 2020)]},
    us_gaap__RevenueFromContractWithCustomerExcludingAssessedTax={
        "USD": [fy(y, 999) for y in (2019, 2020, 2021, 2022)]},
)
s2, _ = R._annual_series(overlap, REV)
check(s2[2019] == 999 and s2[2020] == 999,
      f"the tag with 4 years supplies the overlap, not the one with 3 "
      f"(got {s2.get(2019)})")
check(s2[2018] == 111, "and the shorter tag still contributes the year only it has")
check(sorted(s2) == [2018, 2019, 2020, 2021, 2022], "coverage is still the union")

# Equal coverage falls back to the documented slot preference.
tie = facts(
    us_gaap__Revenues={"USD": [fy(y, 111) for y in (2019, 2020, 2021)]},
    us_gaap__RevenueFromContractWithCustomerExcludingAssessedTax={
        "USD": [fy(y, 999) for y in (2019, 2020, 2021)]},
)
check(R._annual_series(tie, REV)[0][2020] == 111,
      "with equal coverage the earlier slot still wins")

# ── currency: yen must never be divided by dollars ──────────────────
jpy = facts(us_gaap__Revenues={"JPY": [fy(y, 29_000_000_000_000) for y in range(2018, 2026)]})
s3, u3 = R._annual_series(jpy, REV)
check(u3 == "JPY", f"a yen-only filer reports its unit as JPY (got {u3!r})")
check(s3, "and still yields a series — the ratios remain valid")

# When BOTH are present, USD wins regardless of JSON ordering. This is
# the whole defence: dict order in the SEC's payload must not decide a
# company's currency.
both = facts(us_gaap__Revenues={
    "JPY": [fy(y, 29_000_000_000_000) for y in range(2018, 2026)],
    "USD": [fy(y, 250_000_000_000) for y in range(2018, 2026)],
})
s4, u4 = R._annual_series(both, REV)
check(u4 == "USD", f"USD is preferred when offered (got {u4!r})")
check(s4[2025] == 250_000_000_000, "and the USD values are the ones returned")

both_rev = facts(us_gaap__Revenues={
    "USD": [fy(y, 250_000_000_000) for y in range(2018, 2026)],
    "JPY": [fy(y, 29_000_000_000_000) for y in range(2018, 2026)],
})
check(R._annual_series(both_rev, REV)[1] == "USD",
      "…and the reverse insertion order gives the same answer")

# Two currencies are NEVER spliced into one series.
split = facts(
    us_gaap__Revenues={"JPY": [fy(y, 29e12) for y in (2015, 2016, 2017)]},
    us_gaap__RevenueFromContractWithCustomerExcludingAssessedTax={
        "USD": [fy(y, 250e9) for y in (2018, 2019, 2020)]},
)
s5, u5 = R._annual_series(split, REV)
check(u5 == "USD", "with a choice, USD is taken")
check(set(s5) == {2018, 2019, 2020},
      f"only the USD years are returned — a JPY year must never be "
      f"appended to a USD series (got {sorted(s5)})")

# Fallback is deterministic, so a company cannot change currency between
# runs because the SEC reordered a JSON object.
a = facts(us_gaap__Revenues={"TWD": [fy(y, 1) for y in (2020, 2021, 2022)],
                             "CNY": [fy(y, 2) for y in (2020, 2021, 2022)]})
b = facts(us_gaap__Revenues={"CNY": [fy(y, 2) for y in (2020, 2021, 2022)],
                             "TWD": [fy(y, 1) for y in (2020, 2021, 2022)]})
check(R._annual_series(a, REV)[1] == R._annual_series(b, REV)[1],
      "unit choice does not depend on JSON key order")

# ── share counts ask for a different unit entirely ──────────────────
sh = facts(us_gaap__WeightedAverageNumberOfDilutedSharesOutstanding={
    "shares": [fy(y, 1_000_000) for y in (2020, 2021, 2022)]})
s6, u6 = R._annual_series(sh, R.TAGS["shares_diluted"], R.WANT_UNIT["shares_diluted"])
check(u6 == "shares", f"share counts request the shares unit (got {u6!r})")
check(len(s6) == 3, "and resolve normally")
check(R.WANT_UNIT.get("shares_diluted") == "shares",
      "the want-unit map covers share counts — asking USD of a share count "
      "would fall through to the deterministic fallback and pick something wrong")

# ── the pre-existing filters must still hold ────────────────────────
mixed = facts(us_gaap__Revenues={"USD": [
    fy(2020, 100), fy(2021, 110), fy(2022, 120),
    {"fy": 2023, "val": 30, "form": "10-Q", "fp": "Q1",
     "start": "2023-01-01", "end": "2023-03-31"},
    {"fy": 2024, "val": 40, "form": "8-K", "fp": "FY",
     "start": "2024-01-01", "end": "2024-12-31"},
]})
s7, _ = R._annual_series(mixed, REV)
check(2023 not in s7, "a 10-Q value is not an annual figure")
check(2024 not in s7, "and an 8-K is not an annual form")
check(sorted(s7) == [2020, 2021, 2022], "only the annual 10-K years remain")

# 20-F and 40-F are annual forms — that is how foreign filers get in.
f20 = facts(us_gaap__Revenues={"USD": [fy(y, 100, form="20-F") for y in (2020, 2021, 2022)]})
check(len(R._annual_series(f20, REV)[0]) == 3, "20-F counts as annual")
f40 = facts(us_gaap__Revenues={"USD": [fy(y, 100, form="40-F") for y in (2020, 2021, 2022)]})
check(len(R._annual_series(f40, REV)[0]) == 3, "40-F counts as annual")

# Amended filings overwrite, keeping the last value for a year.
amended = facts(us_gaap__Revenues={"USD": [
    fy(2020, 100), fy(2021, 110), fy(2022, 120), fy(2022, 125, form="10-K/A")]})
check(R._annual_series(amended, REV)[0][2022] == 125,
      "an amendment supersedes the original for that year")

# ── degenerate input must not raise ─────────────────────────────────
for bad, label in (({}, "empty facts"),
                   ({"us-gaap": {}}, "taxonomy with no tags"),
                   ({"us-gaap": {"Revenues": {}}}, "tag with no units"),
                   ({"us-gaap": {"Revenues": {"units": {}}}}, "empty units"),
                   ({"us-gaap": {"Revenues": {"units": {"USD": []}}}}, "empty unit list")):
    try:
        got, u = R._annual_series(bad, REV)
        check(got == {}, f"{label} yields an empty series")
    except Exception as e:                                   # noqa: BLE001
        _FAILS.append(f"_annual_series raised {type(e).__name__} on {label}: {e}")
    _COUNT += 1

# Fewer than three years is not a series.
thin = facts(us_gaap__Revenues={"USD": [fy(2024, 100), fy(2025, 110)]})
check(R._annual_series(thin, REV)[0] == {}, "two years is too thin to use")


# ── a thin stray unit must not evict the real history ───────────────
#
# Run #6 regression: preferring USD outright lost Toyota, Novo Nordisk and
# SAP outright, and gave Taiwan Semi a P/FCF of 410. A 20-F filer whose
# history is in its own currency can also carry a couple of years tagged
# in dollars; taking those either drops the company under the 3-year
# minimum or keeps a truncated series and divides it by a dollar price.
thin_usd = facts(us_gaap__Revenues={
    "JPY": [fy(y, 29e12) for y in range(2015, 2026)],   # 11 real years
    "USD": [fy(y, 250e9) for y in (2024, 2025)],        # 2 stray years
})
s_thin, u_thin = R._annual_series(thin_usd, REV)
check(u_thin == "JPY",
      f"an 11-year JPY history beats a 2-year USD stray (got {u_thin!r})")
check(len(s_thin) == 11, f"and all 11 years survive (got {len(s_thin)})")

# But a genuine USD reporter still gets USD, even when another unit is
# present with the same depth. The preference is not abandoned, only
# subordinated to actually having the history.
real_usd = facts(us_gaap__Revenues={
    "USD": [fy(y, 250e9) for y in range(2015, 2026)],
    "EUR": [fy(y, 230e9) for y in range(2015, 2026)],
})
check(R._annual_series(real_usd, REV)[1] == "USD",
      "with equal coverage the wanted unit still wins")

# Within one year counts as equal — a filer who tagged one extra year in
# a secondary currency is still a USD reporter.
off_by_one = facts(us_gaap__Revenues={
    "USD": [fy(y, 250e9) for y in range(2016, 2026)],   # 10
    "CAD": [fy(y, 330e9) for y in range(2015, 2026)],   # 11
})
check(R._annual_series(off_by_one, REV)[1] == "USD",
      "a one-year shortfall does not flip the reporting currency")

# Two years behind does flip it.
off_by_two = facts(us_gaap__Revenues={
    "USD": [fy(y, 250e9) for y in range(2017, 2026)],   # 9
    "CAD": [fy(y, 330e9) for y in range(2015, 2026)],   # 11
})
check(R._annual_series(off_by_two, REV)[1] == "CAD",
      "a two-year shortfall means USD was not the reporting currency")

# The merge still spans tags within the chosen unit.
thin_usd_606 = facts(
    us_gaap__Revenues={"JPY": [fy(y, 29e12) for y in range(2013, 2018)],
                       "USD": [fy(y, 250e9) for y in (2024, 2025)]},
    us_gaap__RevenueFromContractWithCustomerExcludingAssessedTax={
        "JPY": [fy(y, 30e12) for y in range(2018, 2026)]},
)
s_both, u_both = R._annual_series(thin_usd_606, REV)
check(u_both == "JPY" and len(s_both) == 13,
      f"unit choice and tag merging compose (got {u_both}, {len(s_both)} years)")

# Ties break deterministically when neither unit is the wanted one.
tie_a = facts(us_gaap__Revenues={"TWD": [fy(y, 1) for y in range(2020, 2026)],
                                 "CNY": [fy(y, 2) for y in range(2020, 2026)]})
tie_b = facts(us_gaap__Revenues={"CNY": [fy(y, 2) for y in range(2020, 2026)],
                                 "TWD": [fy(y, 1) for y in range(2020, 2026)]})
check(R._annual_series(tie_a, REV)[1] == R._annual_series(tie_b, REV)[1],
      "equal-length non-preferred units still resolve deterministically")


# ── the base year must not be the shutdown ──────────────────────────
#
# Fixing the ASC 606 freeze pushed 1,540 of 1,833 rows onto a 2020 base at
# once, turning recovery into "growth": Southwest -16.6 -> 69.4, Cintas
# -16.6 -> 41.7, Royal Caribbean -> 250.0. Cintas is a steady
# high-single-digit grower; that number describes 2020, not Cintas.
airline = {2017: 100.0, 2018: 106.0, 2019: 112.0,
           2020: 30.0,                       # the hole
           2021: 55.0, 2022: 95.0, 2023: 110.0, 2024: 118.0, 2025: 125.0}
naive = ((125.0 / 30.0) ** (1 / 5) - 1) * 100
got = R._cagr(airline, 5)
check(naive > 30, f"the endpoint-to-2020 figure really is absurd ({naive:.1f}%)")
check(got is not None, "a company with pre-2020 history still gets a growth number")
check(got < 5, f"anchored at 2019 it reads like the business, not the hole "
               f"(got {got}, naive {naive:.1f})")

# 2021 is only half a base year and is excluded too.
part = {2018: 100.0, 2019: 112.0, 2020: 30.0, 2021: 55.0,
        2022: 95.0, 2023: 110.0, 2024: 118.0, 2025: 125.0, 2026: 130.0}
check(R._cagr(part, 5) == R._cagr(part, 5), "deterministic")
c2 = R._cagr(part, 5)                       # base would be 2021 -> walk to 2019
check(c2 is not None and c2 < 5,
      f"a 2021 base is walked back as well (got {c2})")

# A company whose entire record is the rebound has no measurable durable
# growth. UNKNOWN beats a recovery rate — it is the difference between
# "no answer" and "admitted to the board on the strength of the bounce".
rebound_only = {2021: 55.0, 2022: 95.0, 2023: 110.0, 2024: 118.0, 2025: 125.0}
check(R._cagr(rebound_only, 5) is None,
      "with nothing before 2020 the answer is None, not the rebound rate")

# Untouched: a normal series still measures exactly as before.
steady = {y: 100.0 * (1.08 ** (y - 2015)) for y in range(2015, 2026)}
check(abs(R._cagr(steady, 5) - 8.0) < 0.05,
      f"a steady 8% grower still reads 8% (got {R._cagr(steady, 5)})")
check(abs(R._cagr(steady, 10) - 8.0) < 0.05, "and over ten years too")

# ── one substituted year must not rewrite a history ─────────────────
# Xerox: fiscal year unchanged between runs, 5-yr CAGR 0.0 -> 126.8,
# because slot order handed one base year to a different tag.
primary_wins = facts(
    us_gaap__Revenues={"USD": [fy(2020, 1.0)]},                    # 1 stray year
    us_gaap__RevenueFromContractWithCustomerExcludingAssessedTax={
        "USD": [fy(y, 100.0) for y in range(2018, 2026)]},         # 8 real years
)
s_p, _ = R._annual_series(primary_wins, REV)
check(s_p[2020] == 100.0,
      f"the tag with 8 years supplies 2020, not the one with a single "
      f"stray value (got {s_p.get(2020)})")
check(len(s_p) == 8, f"and the series is the primary basis (got {len(s_p)})")

# Gaps in the primary ARE still filled from the others.
gapfill = facts(
    us_gaap__Revenues={"USD": [fy(2015, 50.0), fy(2016, 55.0), fy(2017, 60.0)]},
    us_gaap__RevenueFromContractWithCustomerExcludingAssessedTax={
        "USD": [fy(y, 100.0) for y in range(2018, 2026)]},
)
s_g, _ = R._annual_series(gapfill, REV)
check(len(s_g) == 11, f"primary + gap fill spans 2015-2025 (got {len(s_g)})")
check(s_g[2015] == 50.0 and s_g[2025] == 100.0, "both bases contribute their own years")


# ── fitted growth: every year counts, no year decides ───────────────
#
# A fifteen-year endpoint CAGR still rests on two of the fifteen numbers.
# One restated year, one 53-week year, one acquisition or one pandemic
# sets the whole answer. The fit uses all of them.
steady15 = {y: 100.0 * (1.07 ** (y - 2011)) for y in range(2011, 2026)}
t = R._trend_growth(steady15)
check(abs(t - 7.0) < 0.05, f"a clean 7% compounder fits at 7% (got {t})")

# Same company, with 2020 collapsing 60% and recovering to trend.
covid15 = dict(steady15)
covid15[2020] = steady15[2020] * 0.40
t_c = R._trend_growth(covid15)
check(abs(t_c - 7.0) < 1.5,
      f"one pandemic year moves a fifteen-year fit by約 a point, not by ten "
      f"(got {t_c} vs clean {t})")
endpoint = R._cagr(covid15, 15)
check(t_c is not None and endpoint is not None, "both measures return something")

# The endpoint measure is the fragile one: put the shock at the START and
# it swings hard, while the fit barely notices.
shock_base = dict(steady15)
shock_base[2011] = steady15[2011] * 0.40
check(abs(R._trend_growth(shock_base) - 7.0) < 3.0,
      f"a wrecked FIRST year still fits near trend (got {R._trend_growth(shock_base)})")
naive_base = ((shock_base[2025] / shock_base[2011]) ** (1 / 14) - 1) * 100
check(naive_base > 12,
      f"...where the endpoint measure would read {naive_base:.1f}% off the same data")

# Guardrails.
check(R._trend_growth({2023: 1.0, 2024: 2.0, 2025: 3.0}) is None,
      "a slope through three points is a guess, not a trend")
check(R._trend_growth({}) is None, "empty series is None")
check(R._trend_growth({y: 100.0 for y in range(2015, 2026)}) == 0.0,
      "a flat series fits at 0%")
neg = {y: -5.0 for y in range(2015, 2026)}
check(R._trend_growth(neg) is None, "all-negative revenue cannot be fitted in logs")
decl = {y: 100.0 * (0.95 ** (y - 2011)) for y in range(2011, 2026)}
check(abs(R._trend_growth(decl) + 5.0) < 0.05,
      f"a shrinking business fits negative (got {R._trend_growth(decl)})")


# ── a quarter is not a year, even when it ends in December ──────────
#
# The old guard read: same calendar year AND does not end in December AND
# spans under nine months. That middle clause whitelisted every period
# ENDING in December, so Q4 and H2 walked in and overwrote the real annual
# figure. It only bit the OLDEST year in a window — which is exactly the
# year a CAGR divides by — so it hid for as long as the screen existed:
#
#     MA    ten-year revenue CAGR 31.67% against a true ~11%
#           implied 2015 base $2.09bn = one quarter, not the $9.7bn year
#     IDXX  implied base 37% of a year · POOL 31% · GPI 47%
#
# `fp` cannot catch it: companyfacts reports the fiscal period of the
# FILING, so every fact in a 10-K carries FY whatever it actually spans.
def dur(start, end, val, fy, form="10-K"):
    return {"fy": fy, "val": val, "form": form, "fp": "FY",
            "start": start, "end": end}

leaky = {"units": {"USD": [
    dur("2015-01-01", "2015-12-31", 9_667_000_000, 2015),   # the real year
    dur("2015-10-01", "2015-12-31", 2_093_000_000, 2015),   # Q4, ends in December
    dur("2015-07-01", "2015-12-31", 4_900_000_000, 2015),   # H2, ends in December
]}}
got = R._rows_for(leaky, "USD")
check(got.get(2015) == 9_667_000_000,
      f"the ANNUAL figure survives a Q4 and an H2 tagged to the same year "
      f"(got {got.get(2015)})")

# Order must not matter: the annual value can appear first or last.
reordered = {"units": {"USD": [
    dur("2015-10-01", "2015-12-31", 2_093_000_000, 2015),
    dur("2015-01-01", "2015-12-31", 9_667_000_000, 2015),
]}}
check(R._rows_for(reordered, "USD").get(2015) == 9_667_000_000,
      "and the quarter loses regardless of JSON order")

# Quarters at every position are rejected, December or not.
for a, b, label in (("2015-01-01", "2015-03-31", "Q1"),
                    ("2015-04-01", "2015-06-30", "Q2"),
                    ("2015-07-01", "2015-09-30", "Q3"),
                    ("2015-10-01", "2015-12-31", "Q4"),
                    ("2015-01-01", "2015-06-30", "H1"),
                    ("2015-07-01", "2015-12-31", "H2")):
    only = {"units": {"USD": [dur(a, b, 1.0, 2015)]}}
    check(R._rows_for(only, "USD") == {},
          f"{label} ({a}..{b}) is not an annual period")

# Real annual shapes must all survive.
for a, b, label in (("2015-01-01", "2015-12-31", "calendar year"),
                    ("2016-01-03", "2016-12-31", "52-week retail year"),
                    ("2017-01-01", "2018-01-02", "53-week year crossing Dec 31"),
                    ("2014-07-01", "2015-06-30", "June fiscal year end"),
                    ("2015-02-01", "2016-01-31", "January fiscal year end")):
    only = {"units": {"USD": [dur(a, b, 5.0, 2015)]}}
    check(R._rows_for(only, "USD") == {2015: 5.0},
          f"a {label} ({a}..{b}) IS an annual period")

# Instants (balance-sheet items) carry no start and must pass untouched.
inst = {"units": {"USD": [{"fy": 2015, "val": 42.0, "form": "10-K",
                           "fp": "FY", "end": "2015-12-31"}]}}
check(R._rows_for(inst, "USD") == {2015: 42.0},
      "an instant has no duration to check and is kept")

# Unparseable dates are not evidence of anything.
bad = {"units": {"USD": [dur("not-a-date", "2015-12-31", 7.0, 2015)]}}
check(R._rows_for(bad, "USD") == {}, "a malformed date is dropped, not guessed at")

# ══════════════════════════════════════════════════════════════════
# BALANCE SHEET — the enterprise-value inputs
# ══════════════════════════════════════════════════════════════════
# Instants, not durations, and the difference is not cosmetic. A 10-K
# carries TWO balance sheets and companyfacts stamps both with the fiscal
# year of the FILING, so keying on `fy` the way the duration extractor
# does puts last year's debt on this year's row.


def inst(taxonomy, tag, rows, unit="USD"):
    return {taxonomy: {tag: {"units": {unit: rows}}}}


def merge(*ds):
    out = {}
    for d in ds:
        for tax, tags in d.items():
            out.setdefault(tax, {}).update(tags)
    return out


TWO_YEARS = inst("us-gaap", "CashAndCashEquivalentsAtCarryingValue", [
    {"end": "2024-09-28", "val": 29_943, "fy": 2024, "fp": "FY", "form": "10-K"},
    {"end": "2023-09-30", "val": 30_737, "fy": 2024, "fp": "FY", "form": "10-K"},
])
val, as_of, unit = R._latest_instant(TWO_YEARS, R.BALANCE_TAGS["cash"])
check(val == 29_943 and as_of == "2024-09-28",
      f"THE LATEST BALANCE SHEET WINS, NOT THE LAST ONE LISTED. Both facts "
      f"carry fy=2024 because that is the FILING's year, so `fy` cannot "
      f"tell them apart and the `end` date is the only identifier that "
      f"can (got {val} at {as_of})")

REVERSED = inst("us-gaap", "CashAndCashEquivalentsAtCarryingValue", [
    {"end": "2023-09-30", "val": 30_737, "fy": 2024, "fp": "FY", "form": "10-K"},
    {"end": "2024-09-28", "val": 29_943, "fy": 2024, "fp": "FY", "form": "10-K"},
])
check(R._latest_instant(REVERSED, R.BALANCE_TAGS["cash"])[0] == 29_943,
      "and the answer does not depend on the order the SEC listed them in")

DURATION_MIXED = inst("us-gaap", "CashAndCashEquivalentsAtCarryingValue", [
    {"start": "2023-10-01", "end": "2024-09-28", "val": 999, "fy": 2024,
     "fp": "FY", "form": "10-K"},
    {"end": "2024-09-28", "val": 29_943, "fy": 2024, "fp": "FY", "form": "10-K"},
])
check(R._latest_instant(DURATION_MIXED, R.BALANCE_TAGS["cash"])[0] == 29_943,
      "a duration fact under a balance-sheet tag is skipped — it has a "
      "start, and a balance does not")

check(R._latest_instant({}, R.BALANCE_TAGS["cash"]) == (None, None, None),
      "nothing filed is an explicit absence, not a zero")
check(R._latest_instant(inst("us-gaap", "CashAndCashEquivalentsAtCarryingValue", [
        {"end": "2024-06-30", "val": 5, "fy": 2024, "fp": "Q2", "form": "10-Q"}]),
      R.BALANCE_TAGS["cash"])[0] is None,
      "and a quarterly balance sheet is not an annual one")


# ── total debt is ASSEMBLED, and the assembly is where it goes wrong ──
def bs(**kw):
    """Build a facts dict from {tag: value} plus an `end` date."""
    end = kw.pop("end", "2025-12-31")
    parts = [inst("us-gaap", tag, [{"end": end, "val": v, "fy": 2025,
                                    "fp": "FY", "form": "10-K"}])
             for tag, v in kw.items()]
    return merge(*parts) if parts else {}


combined = R.balance_sheet(bs(DebtLongtermAndShorttermCombinedAmount=5_000,
                              LongTermDebtNoncurrent=4_000,
                              LongTermDebtCurrent=500,
                              CashAndCashEquivalentsAtCarryingValue=900))
check(combined["total_debt"] == 5_000,
      f"A SINGLE COMBINED DEBT TAG IS USED ALONE, not added to the "
      f"components it already contains (got {combined['total_debt']})")

summed = R.balance_sheet(bs(LongTermDebtNoncurrent=4_000,
                            LongTermDebtCurrent=500,
                            ShortTermBorrowings=250,
                            CashAndCashEquivalentsAtCarryingValue=900))
check(summed["total_debt"] == 4_750,
      f"otherwise it is noncurrent + current portion + short-term "
      f"borrowings (got {summed['total_debt']})")
check(summed["cash"] == 900, "and cash comes across")

# `LongTermDebt` IS THE TOTAL, current portion included. It used to be
# thrown away whenever a current portion was also filed — which kept only
# the current portion. From the SEC's own figures (2025 10-Ks, $bn):
#   Union Pacific  LongTermDebt 31.81 · LTDACLO 30.29 · current 1.52  -> read 1.52
#   AbbVie         LongTermDebt 64.50 · LTDACLO 58.94 · current 6.06 · STB 2.50 -> read 8.55
unp = R.balance_sheet(bs(LongTermDebt=31.81e9, LongTermDebtAndCapitalLeaseObligations=30.29e9,
                         LongTermDebtAndCapitalLeaseObligationsCurrent=1.52e9, CommercialPaper=0.0,
                         CashAndCashEquivalentsAtCarryingValue=1.27e9))
check(abs(unp["total_debt"] - 31.81e9) < 1e6,
      f"UNION PACIFIC'S DEBT IS $31.8bn, NOT ITS $1.5bn CURRENT PORTION: the noncurrent "
      f"line (LongTermDebtAndCapitalLeaseObligations) plus the current portion "
      f"(got {unp['total_debt'] / 1e9:.2f}bn)")
abbv = R.balance_sheet(bs(LongTermDebt=64.50e9, LongTermDebtAndCapitalLeaseObligations=58.94e9,
                          LongTermDebtAndCapitalLeaseObligationsCurrent=6.06e9,
                          ShortTermBorrowings=2.50e9, StockholdersEquity=3e9))
check(abs(abbv["total_debt"] - 67.50e9) < 1e6,
      f"AbbVie: noncurrent + current + short-term borrowings, $67.5bn "
      f"(got {abbv['total_debt'] / 1e9:.2f}bn; the old rule read $8.55bn)")
ltd_only = R.balance_sheet(bs(LongTermDebt=4_500, LongTermDebtCurrent=500,
                              CashAndCashEquivalentsAtCarryingValue=900))
check(ltd_only["total_debt"] == 4_500,
      f"WITH NO NONCURRENT LINE, LongTermDebt IS USED AS THE TOTAL and the current "
      f"portion it contains is not added again (got {ltd_only['total_debt']})")
check(R.balance_sheet(bs(LongTermDebt=4_500, ShortTermBorrowings=250,
                         StockholdersEquity=1))["total_debt"] == 4_750,
      "short-term borrowings, which LongTermDebt cannot contain, are added")
check(R.balance_sheet(bs(LongTermDebt=300, LongTermDebtCurrent=500,
                         StockholdersEquity=1))["total_debt"] == 500,
      "a LongTermDebt below its own current portion is not a total — the larger stands")

# ONE DATE FOR EVERYTHING. Home Depot stopped tagging LongTermDebtNoncurrent
# in 2011; the old reader took that $8.71bn beside this year's figures.
def dated(tag, end, val, unit="USD", filed=None):
    return inst("us-gaap", tag, [{"end": end, "val": val, "fy": int(end[:4]), "fp": "FY",
                                  "form": "10-K", "filed": filed or end}], unit=unit)


HD = merge(dated("LongTermDebtNoncurrent", "2011-01-30", 8.71e9),
           dated("LongTermDebtCurrent", "2011-01-30", 1.04e9),
           dated("LongTermDebt", "2026-02-01", 49.40e9),
           dated("LongTermDebtAndCapitalLeaseObligations", "2026-02-01", 46.34e9),
           dated("LongTermDebtAndCapitalLeaseObligationsCurrent", "2026-02-01", 4.97e9),
           dated("CommercialPaper", "2026-02-01", 4.46e9),
           dated("StockholdersEquity", "2026-02-01", 12e9),
           dated("CashAndCashEquivalentsAtCarryingValue", "2026-02-01", 1.39e9))
hd = R.balance_sheet(HD)
check(abs(hd["total_debt"] - 55.77e9) < 1e6 and hd["as_of"] == "2026-02-01",
      f"HOME DEPOT'S DEBT IS READ FROM ITS 2026 BALANCE SHEET — $46.3bn + $5.0bn + "
      f"$4.5bn of commercial paper — never the $8.7bn it filed in 2011 "
      f"(got {hd['total_debt'] / 1e9:.2f}bn as of {hd['as_of']})")
MU = merge(dated("LongTermDebtNoncurrent", "2012-08-30", 3.04e9),
           dated("LongTermDebtCurrent", "2012-08-30", 0.22e9),
           dated("LongTermDebt", "2025-08-28", 11.53e9),
           dated("LongTermDebtAndCapitalLeaseObligations", "2025-08-28", 14.02e9),
           dated("DebtCurrent", "2025-08-28", 0.56e9),
           dated("StockholdersEquity", "2025-08-28", 54e9))
mu = R.balance_sheet(MU)
check(abs(mu["total_debt"] - 14.58e9) < 1e6,
      f"Micron: this year's noncurrent line plus DebtCurrent, $14.6bn — not $3.3bn "
      f"from 2012 (got {mu['total_debt'] / 1e9:.2f}bn)")
_dc =R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2025-12-31", 4_000.0),
                            dated("DebtCurrent", "2025-12-31", 600.0),
                            dated("ShortTermBorrowings", "2025-12-31", 250.0),
                            dated("StockholdersEquity", "2025-12-31", 1.0)))
check(_dc["total_debt"] == 4_600.0,
      f"DebtCurrent already holds short-term borrowings, so they are not added beside it "
      f"(got {_dc['total_debt']})")
_gone = R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2015-12-31", 4_000.0),
                              dated("StockholdersEquity", "2025-12-31", 1.0),
                              dated("CashAndCashEquivalentsAtCarryingValue", "2025-12-31", 900.0)))
check(_gone["total_debt"] is None and not _gone["debt_inferred_zero"],
      "A COMPANY THAT FILED DEBT TAGS ONCE AND NONE ON ITS LATEST BALANCE SHEET HAS "
      "UNKNOWN DEBT — it moved to a tag this list doesn't read; it is not debt-free")
_amend = R.balance_sheet(merge(
    inst("us-gaap", "LongTermDebtNoncurrent", [
        {"end": "2025-12-31", "val": 4_000.0, "fy": 2025, "fp": "FY", "form": "10-K/A", "filed": "2026-05-01"},
        {"end": "2025-12-31", "val": 3_000.0, "fy": 2025, "fp": "FY", "form": "10-K", "filed": "2026-02-01"}]),
    dated("StockholdersEquity", "2025-12-31", 1.0)))
check(_amend["total_debt"] == 4_000.0, "a restated figure (10-K/A, filed later) wins")
# The second probe's cases (latest balance sheets, $bn).
def ifrs(tag, end, val):
    return inst("ifrs-full", tag, [{"end": end, "val": val, "fy": int(end[:4]), "fp": "FY",
                                    "form": "20-F", "filed": end}])


_apd = R.balance_sheet(merge(dated("DebtAndCapitalLeaseObligations", "2025-09-30", 17.698e9),
                             dated("LongTermDebtAndCapitalLeaseObligationsCurrent", "2025-09-30", 0.716e9),
                             dated("ShortTermBorrowings", "2025-09-30", 0.035e9),
                             dated("StockholdersEquity", "2025-09-30", 17e9)))
check(abs(_apd["total_debt"] - 17.698e9) < 1e6,
      f"AIR PRODUCTS' $17.7bn IS READ FROM ITS TOTAL TAG — not its $0.75bn of current debt "
      f"(got {_apd['total_debt'] / 1e9:.2f}bn)")
_pbr = R.balance_sheet(merge(ifrs("LongtermBorrowings", "2024-12-31", 20.596e9),
                             ifrs("CurrentBorrowingsAndCurrentPortionOfNoncurrentBorrowings", "2024-12-31", 2.566e9),
                             ifrs("ShorttermBorrowings", "2024-12-31", 0.010e9),
                             ifrs("Equity", "2024-12-31", 70e9)))
check(abs(_pbr["total_debt"] - 23.162e9) < 1e6,
      f"PETROBRAS (IFRS): long-term borrowings plus current borrowings, $23.2bn — its short-term "
      f"$0.01bn alone had been read as all its debt (got {_pbr['total_debt'] / 1e9:.3f}bn)")
_bud = R.balance_sheet(merge(ifrs("Borrowings", "2025-12-31", 73.013e9), ifrs("Equity", "2025-12-31", 80e9)))
check(_bud["total_debt"] == 73.013e9, "Anheuser-Busch (IFRS): the Borrowings total")
_de = R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2019-11-03", 30e9),
                            dated("DebtCurrent", "2025-11-02", 13.796e9),
                            dated("StockholdersEquity", "2025-11-02", 25e9)))
check(_de["total_debt"] is None and not _de["debt_inferred_zero"],
      "DEERE: ONLY ITS CURRENT DEBT IS ON A READ TAG, AND IT FILED A LONG-TERM LINE BEFORE — "
      "unknown, not $13.8bn read as the whole")
check(sum("unknown" in n for n in _de["notes"]) == 1 and any("short-term" in n for n in _de["notes"]),
      "and the row gives that one reason, not a second, contradictory one")
check(R.balance_sheet(merge(dated("ShortTermBorrowings", "2025-12-31", 250.0),
                            dated("StockholdersEquity", "2025-12-31", 1.0)))["total_debt"] == 250.0,
      "a company that only ever had short-term debt is measured on it")
_panw = R.balance_sheet(merge(dated("ConvertibleDebtNoncurrent", "2026-07-31", 1.774e9),
                              dated("StockholdersEquity", "2026-07-31", 7e9)))
check(_panw["total_debt"] == 1.774e9, "Palo Alto: its converts are its long-term debt")
_both = R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2025-12-31", 5_000.0),
                              dated("ConvertibleDebtNoncurrent", "2025-12-31", 1_000.0),
                              dated("StockholdersEquity", "2025-12-31", 1.0)))
check(_both["total_debt"] == 5_000.0,
      "A FALLBACK LINE IS NEVER ADDED BESIDE THE MAIN ONE — the converts inside a filed "
      "LongTermDebtNoncurrent are not counted twice")
_part = R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2025-12-31", 1_000.0),
                              dated("LongTermDebt", "2025-12-31", 9_000.0),
                              dated("LongTermDebtCurrent", "2025-12-31", 500.0),
                              dated("StockholdersEquity", "2025-12-31", 1.0)))
check(_part["total_debt"] == 9_000.0,
      "WHEN THE LINE TAG IS ONLY PART OF THE DEBT, THE FILER'S OWN TOTAL STANDS — the larger route")

_late = R.balance_sheet(merge(dated("StockholdersEquity", "2025-12-31", 1.0),
                              dated("LongTermDebtNoncurrent", "2025-12-31", 4_000.0),
                              dated("ShortTermBorrowings", "2026-02-15", 999.0)))
check(_late["total_debt"] == 4_000.0 and _late["as_of"] == "2025-12-31",
      "the date is the balance sheet's: a footnote figure dated after it doesn't move it")

# The third probe: what 304 unknown-debt rows filed ($bn, latest 10-Ks).
_zs = R.balance_sheet(merge(dated("ConvertibleLongTermNotesPayable", "2026-07-31", 1.696e9),
                            dated("ConvertibleNotesPayableCurrent", "2025-07-31", 0.0),
                            dated("StockholdersEquity", "2026-07-31", 2e9)))
check(_zs["total_debt"] == 1.696e9, "Zscaler's converts are read from ConvertibleLongTermNotesPayable")
_teva = R.balance_sheet(merge(dated("SeniorNotes", "2025-12-31", 16.85e9),
                              dated("LongTermDebtCurrent", "2025-12-31", 1.798e9),
                              dated("StockholdersEquity", "2025-12-31", 7e9)))
check(_teva["total_debt"] == 16.85e9,
      f"TEVA'S SENIOR NOTES ARE ITS TOTAL, current portion inside — $16.85bn, not the $1.8bn "
      f"current portion read as partial (got {_teva['total_debt']})")
_ava = R.balance_sheet(merge(dated("SecuredDebt", "2025-12-31", 2.759e9),
                             dated("ShortTermBorrowings", "2025-12-31", 0.388e9),
                             dated("StockholdersEquity", "2025-12-31", 2.6e9)))
check(abs(_ava["total_debt"] - 3.147e9) < 1e3,
      "Avista: its secured bonds plus short-term borrowings")
_nvgs = R.balance_sheet(merge(dated("UnsecuredLongTermDebt", "2025-12-31", 0.138e9),
                              dated("LongTermLoansPayable", "2025-12-31", 0.594e9),
                              dated("LongTermDebtCurrent", "2025-12-31", 0.168e9),
                              dated("StockholdersEquity", "2025-12-31", 1.2e9)))
check(_nvgs["total_debt"] == 0.594e9
      and not any("LongTermDebt" in n for n in _nvgs["notes"]),
      f"NAVIGATOR FILES TWO KINDS: THE LARGER STANDS ($594m of loans), not whichever the "
      f"list names first ($138m of bonds) (got {_nvgs['total_debt']}, {_nvgs['notes']})")
_csv = R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2025-12-31", 6e6),
                             dated("LongTermDebtCurrent", "2025-12-31", 1e6),
                             dated("SeniorNotes", "2025-12-31", 397e6),
                             dated("StockholdersEquity", "2025-12-31", 1.0)))
check(_csv["total_debt"] == 397e6,
      f"A ONE-KIND TOTAL LARGER THAN THE MAIN LINE IS A FLOOR THE LINE MISSED — Carriage "
      f"Services' $397m of senior notes, not its $7m noncurrent line (got {_csv['total_debt']})")
check(R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2025-12-31", 6e6),
                            dated("SeniorNotes", "2025-12-31", 397e6),
                            dated("ShortTermBorrowings", "2025-12-31", 50e6),
                            dated("StockholdersEquity", "2025-12-31", 1.0)))["total_debt"] == 447e6,
      "and short-term borrowings, which no long-term total holds, are added to it")
# Every route is a floor; the combined tag is one of them, not an override.
_on = R.balance_sheet(merge(dated("DebtLongtermAndShorttermCombinedAmount", "2025-12-31", 0.9e6),
                            dated("LongTermDebt", "2025-12-31", 2.9805e9),
                            dated("LongTermDebtNoncurrent", "2025-12-31", 2.9805e9),
                            dated("StockholdersEquity", "2025-12-31", 8e9)))
check(_on["total_debt"] == 2.9805e9 and "single combined debt tag" not in _on["notes"],
      f"ON SEMICONDUCTOR'S $0.9m COMBINED TAG DOES NOT OVERRIDE ITS $2.98bn OF LongTermDebt "
      f"(got {_on['total_debt']})")
_lite = R.balance_sheet(merge(dated("DebtLongtermAndShorttermCombinedAmount", "2026-06-27", 1.637e9),
                              dated("LongTermDebtNoncurrent", "2026-06-27", 0.040e9),
                              dated("LongTermDebtCurrent", "2026-06-27", 1.597e9),
                              dated("ShortTermBorrowings", "2026-06-27", 1.597e9),
                              dated("StockholdersEquity", "2026-06-27", 1e9)))
check(_lite["total_debt"] == 1.637e9,
      f"LUMENTUM TAGS ITS $1.6bn OF CURRENT CONVERTS TWICE — its own $1.64bn total stands, not "
      f"the $3.23bn the parts add to (got {_lite['total_debt']})")
_cve = R.balance_sheet(merge(ifrs("Borrowings", "2025-12-31", 11.000e9),
                             ifrs("LongtermBorrowings", "2025-12-31", 11.032e9),
                             ifrs("CurrentPortionOfLongtermBorrowings", "2025-12-31", 0.0),
                             ifrs("CurrentBorrowingsAndCurrentPortionOfNoncurrentBorrowings", "2025-12-31", 11.032e9),
                             ifrs("Equity", "2025-12-31", 30e9)))
check(_cve["total_debt"] == 11.0e9,
      f"Cenovus's current-borrowings figure repeats its long-term one: its C$11bn total stands "
      f"(got {_cve['total_debt']})")
_skm = R.balance_sheet(merge(ifrs("Borrowings", "2024-12-31", 305e6),
                             ifrs("CurrentPortionOfLongtermBorrowings", "2024-12-31", 2.46e12),
                             ifrs("LongtermBorrowings", "2019-12-31", 5e12),
                             ifrs("Equity", "2024-12-31", 12e12)))
check(_skm["total_debt"] is None and any("combined" in n for n in _skm["notes"]),
      f"SK TELECOM'S 305m 'Borrowings' BESIDE 2.46tn DUE THIS YEAR IS NOT A TOTAL — ignored, and "
      f"what is left is a current portion alone: unknown (got {_skm['total_debt']}, {_skm['notes']})")
_sgrp = R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2025-12-31", 1e6),
                              dated("LongTermDebtCurrent", "2025-12-31", 0.0),
                              dated("DebtCurrent", "2025-12-31", 20e6),
                              dated("StockholdersEquity", "2025-12-31", 1.0)))
check(_sgrp["total_debt"] == 21e6,
      f"a zero current portion does not hide $20m of DebtCurrent (got {_sgrp['total_debt']})")
check(R.balance_sheet(merge(dated("SeniorNotes", "2019-12-31", 2e9),
                            dated("ShortTermBorrowings", "2025-12-31", 100.0),
                            dated("StockholdersEquity", "2025-12-31", 1.0)))["total_debt"] is None,
      "senior notes filed before count as long-term debt filed before: short-term alone is partial")
_de2 = R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2019-11-03", 30e9),
                             dated("SecuredDebt", "2025-11-02", 6.596e9),
                             dated("DebtCurrent", "2025-11-02", 13.796e9),
                             dated("StockholdersEquity", "2025-11-02", 25e9)))
check(_de2["total_debt"] is None and sum("unknown" in n for n in _de2["notes"]) == 1,
      f"DEERE'S $6.6bn OF SECURITISATION DEBT, SMALLER THAN THE $13.8bn DUE THIS YEAR, IS ONE "
      f"SLICE — unknown, not its total (got {_de2['total_debt']}, {_de2['notes']})")
check(R.balance_sheet(merge(dated("SeniorNotes", "2025-12-31", 1e9),
                            dated("CommercialPaper", "2025-12-31", 5e9),
                            dated("StockholdersEquity", "2025-12-31", 1.0)))["total_debt"] == 6e9,
      "commercial paper is not debt 'due this year' out of a long-term total — notes plus paper")


def dur(tag, end, val):
    y = int(end[:4])
    return inst("us-gaap", tag, [{"start": f"{y - 1}{end[4:]}", "end": end, "val": val, "fy": y,
                                  "fp": "FY", "form": "10-K", "filed": end}])


# Copart: paid off its notes, filed the line at zero in 2024, and stopped.
CPRT = merge(dated("LongTermDebtAndCapitalLeaseObligations", "2022-07-31", 376.5e6),
             dated("LongTermDebtAndCapitalLeaseObligations", "2024-07-31", 0.0),
             dated("StockholdersEquity", "2025-07-31", 8e9),
             dur("InterestPaidNet", "2025-07-31", 2.0e6))
_cprt = R.balance_sheet(CPRT, revenue=4.647e9)
check(_cprt["total_debt"] == 0.0 and not _cprt["debt_inferred_zero"]
      and _cprt["debt_zero_as_of"] == "2024-07-31",
      f"COPART LAST REPORTED ITS DEBT AS ZERO AND HAS FILED NONE SINCE: debt-free, with the "
      f"date it said so — not unknown (got {_cprt['total_debt']}, {_cprt['notes']})")
# Caleres: LongTermDebt at zero in 2022, then a revolver under a tag not read.
CAL = merge(dated("LongTermDebt", "2022-01-29", 0.0),
            dated("StockholdersEquity", "2026-01-31", 0.6e9),
            dur("InterestPaidNet", "2026-01-31", 17.7e6))
_cal = R.balance_sheet(CAL, revenue=2.76e9)
check(_cal["total_debt"] is None and _cal["debt_zero_as_of"] is None
      and any("0.64%" in n for n in _cal["notes"]),
      f"BUT NOT WHEN THE INTEREST BILL SAYS OTHERWISE: Caleres paid 0.64% of revenue in "
      f"interest — unknown (got {_cal['total_debt']}, {_cal['notes']})")
check(R.balance_sheet(CAL, revenue=17.7e6 / 0.002)["total_debt"] == 0.0,
      "interest under DEBT_FREE_INTEREST_MAX of revenue does not block the zero")
_q4 = merge(dated("LongTermDebt", "2022-01-29", 0.0), dated("StockholdersEquity", "2026-01-31", 0.6e9),
            inst("us-gaap", "InterestExpense", [{"start": "2025-11-01", "end": "2026-01-31", "val": 17.7e6,
                                                 "fy": 2025, "fp": "FY", "form": "10-K"}]))
check(R.balance_sheet(_q4, revenue=2.76e9)["total_debt"] == 0.0,
      "a quarter's interest inside a 10-K is not the year's — only a year-long figure is evidence")
check(R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2023-12-31", 0.0),
                            dated("ShortTermBorrowings", "2023-12-31", 200.0),
                            dated("StockholdersEquity", "2025-12-31", 1.0)))["total_debt"] is None,
      "nor a zero line beside nonzero short-term borrowings")
# Each one-kind total and short-term fallback is read when it is all a filer tags.
for _tag, _key in (("ConvertibleNotesPayable", "lt"), ("UnsecuredDebt", "lt"),
                   ("UnsecuredLongTermDebt", "lt"), ("SecuredLongTermDebt", "lt"),
                   ("LongTermLoansPayable", "lt"), ("LoansPayableCurrent", "st"),
                   ("ShortTermBankLoansAndNotesPayable", "st")):
    check(R.balance_sheet(merge(dated(_tag, "2025-12-31", 700.0),
                                dated("StockholdersEquity", "2025-12-31", 1.0)))["total_debt"] == 700.0,
          f"{_tag} is read ({_key})")
check(R.balance_sheet(merge(dated("LongTermDebtNoncurrent", "2022-12-31", 1.488e9),
                            dated("LongTermDebtCurrent", "2022-12-31", 0.0),
                            dated("StockholdersEquity", "2025-12-31", 9e9)))["total_debt"] is None,
      "A ZERO CURRENT PORTION BESIDE A NONZERO LINE IS NOT A REPORTED ZERO — every figure "
      "on the last date must be")
_shop = R.balance_sheet(merge(dated("ConvertibleDebtNoncurrent", "2024-12-31", 0.92e9),
                              dated("ConvertibleDebtCurrent", "2025-12-31", 0.0),
                              dated("StockholdersEquity", "2025-12-31", 12e9)), revenue=11.6e9)
check(_shop["total_debt"] == 0.0 and _shop["debt_zero_as_of"] == "2025-12-31",
      f"Shopify's converts matured: a zero current line on the latest balance sheet is a "
      f"reported zero, not a partial figure (got {_shop['total_debt']}, {_shop['notes']})")
_owes = R.balance_sheet(merge(dated("StockholdersEquity", "2025-12-31", 1e9),
                              dur("InterestExpense", "2025-12-31", 40e6)), revenue=1e9)
check(_owes["total_debt"] is None and not _owes["debt_inferred_zero"],
      "A COMPANY THAT NEVER TAGGED DEBT BUT PAYS 4% OF REVENUE IN INTEREST IS NOT DEBT-FREE")
check(R.balance_sheet(merge(dated("StockholdersEquity", "2025-12-31", 1e9),
                            dur("InterestExpense", "2025-12-31", 1e6)), revenue=1e9)["debt_inferred_zero"],
      "one paying 0.1% still reads as inferred zero")

lone = R.balance_sheet(bs(LongTermDebt=4_500,
                          CashAndCashEquivalentsAtCarryingValue=900))
check(lone["total_debt"] == 4_500,
      "but with no current portion filed there is nothing to double-count, "
      "so the ambiguous tag is used")

# ── absence of a debt tag is not absence of information ──
debtfree = R.balance_sheet(bs(CashAndCashEquivalentsAtCarryingValue=900,
                              StockholdersEquity=12_000))
check(debtfree["total_debt"] == 0.0 and debtfree["debt_inferred_zero"],
      "A COMPANY THAT FILED A BALANCE SHEET AND NO DEBT TAG IS READ AS "
      "DEBT-FREE, flagged. Calling it unknown would discard exactly the "
      "balance sheets an FCF-quality screen most wants")
check(summed["debt_inferred_zero"] is False,
      "while a company with real debt tags is not flagged")

nothing = R.balance_sheet({})
check(nothing["total_debt"] is None and not nothing["filed_balance_sheet"],
      "but a company with NO balance sheet at all has unknown debt — the "
      "inference needs evidence that a balance sheet exists, and absence "
      "of everything is not that evidence")

# ── currency, the bug this repo has already paid for once ──
JPY = inst("us-gaap", "LongTermDebtNoncurrent",
           [{"end": "2025-03-31", "val": 8_000_000, "fy": 2025, "fp": "FY",
             "form": "20-F"}], unit="JPY")
jp = R.balance_sheet(JPY, want_unit="USD")
check(jp["unit"] == "JPY",
      f"a yen balance sheet reports itself as yen (got {jp['unit']}) — the "
      f"caller must refuse to subtract it from a dollar market cap, which "
      f"is the mistake that once put Toyota's P/FCF at 0.2")

MIXED = merge(
    inst("us-gaap", "LongTermDebtNoncurrent",
         [{"end": "2025-12-31", "val": 4_000, "fy": 2025, "fp": "FY", "form": "10-K"}]),
    inst("us-gaap", "CashAndCashEquivalentsAtCarryingValue",
         [{"end": "2025-12-31", "val": 700_000, "fy": 2025, "fp": "FY", "form": "10-K"}],
         unit="EUR"))
mx = R.balance_sheet(MIXED, want_unit="USD")
check(mx["total_debt"] == 4_000 and mx["cash"] is None,
      f"AND A COMPONENT IN A DIFFERENT UNIT IS DROPPED RATHER THAN MIXED "
      f"IN. Subtracting 700,000 euros of cash from a dollar enterprise "
      f"value would not error — it would just produce a company that "
      f"looks like net cash (got debt={mx['total_debt']}, "
      f"cash={mx['cash']})")

check(R.balance_sheet(bs(LongTermDebtNoncurrent=4_000,
                         CashAndCashEquivalentsAtCarryingValue=900,
                         PreferredStockValue=300,
                         MinorityInterest=150))["preferred"] == 300,
      "preferred stock and minority interest come across for VFLO's EV "
      "definition, which adds both")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} compounders series checks passed.")
print(f"   ASC 606 cutover: {min(series)}–{max(series)} merged across tags "
      f"(old code stopped at 2017)")
print("   currency: USD preferred, never spliced, fallback deterministic")
sys.exit(0)
