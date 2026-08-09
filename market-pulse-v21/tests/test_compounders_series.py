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

# ── earlier slots still win the years they cover ────────────────────
# Merging must not reorder preference, only extend coverage.
overlap = facts(
    us_gaap__Revenues={"USD": [fy(y, 111) for y in (2018, 2019, 2020)]},
    us_gaap__RevenueFromContractWithCustomerExcludingAssessedTax={
        "USD": [fy(y, 999) for y in (2019, 2020, 2021, 2022)]},
)
s2, _ = R._annual_series(overlap, REV)
check(s2[2019] == 111 and s2[2020] == 111,
      f"an earlier slot keeps the years it covers (got {s2.get(2019)})")
check(s2[2021] == 999 and s2[2022] == 999,
      "a later slot supplies only the years the earlier one lacks")
check(sorted(s2) == [2018, 2019, 2020, 2021, 2022], "coverage is the union")

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
