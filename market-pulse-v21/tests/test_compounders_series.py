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
