"""Stage C of the compounders build: prices against SEC share counts.

Run:  python tests/test_compounders_market.py      (exit 0 = all pass)

THE FAULT. Yahoo's prices are split-adjusted to today's share basis; SEC
share counts are as filed, and a filing never restates for a split that came
after it. Booking Holdings split 25-for-1 on 2026-04-06, two months after
its FY2025 10-K reported 32.6M diluted shares, so its post-split $164 came
out at 0.6x free cash flow — the cheapest large company on every board that
ranks by cheapness. Chipotle's 50-for-1 split in 2024 gave it a 1.2x
"historical median" P/FCF and a share count growing 90% a year: the maximum
dilution penalty for a company that buys stock back.

The figures below are the real ones where the probe read them (Booking's
split date, its 10-K filing date and share count; Nvidia's two splits).
"""
import os
import sys
from datetime import datetime, timezone

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


def ts(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())


def split(day, num, den):
    return {str(ts(day)): {"date": ts(day), "numerator": num, "denominator": den,
                           "splitRatio": f"{num}:{den}"}}


def chart(price_of, splits=(), divs=(), start=(2016, 10), end=(2026, 9)):
    """A Yahoo v8 chart result: monthly closes from `start` to `end`."""
    stamps, closes = [], []
    y, m = start
    while (y, m) <= end:
        stamps.append(ts(f"{y:04d}-{m:02d}-01"))
        closes.append(price_of(y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    events = {"splits": {}, "dividends": {}}
    for s in splits:
        events["splits"].update(s)
    for day, amt in divs:
        events["dividends"][str(ts(day))] = {"date": ts(day), "amount": amt}
    return {"timestamp": stamps, "indicators": {"quote": [{"close": closes}]}, "events": events}


def fyv(year, val, filed):
    return {"fy": year, "fp": "FY", "form": "10-K", "start": f"{year}-01-01",
            "end": f"{year}-12-31", "val": val, "filed": filed}


# ── parse_splits ──
BKNG_SPLIT = split("2026-04-06", 25, 1)
check(R.parse_splits({"events": {"splits": BKNG_SPLIT}}) == [("2026-04-06", 25.0)],
      "Booking's 25-for-1 reads as a ratio of 25 on its ex-date")
check(R.parse_splits({"events": {"splits": {**split("2021-07-20", 4, 1), **split("2024-06-10", 10, 1)}}})
      == [("2021-07-20", 4.0), ("2024-06-10", 10.0)], "several splits come back oldest first")
check(R.parse_splits({"events": {"splits": split("2023-03-01", 1, 10)}}) == [("2023-03-01", 0.1)],
      "a 1-for-10 reverse split is a ratio of 0.1")
_bad = {"a": {"date": ts("2022-01-03"), "numerator": 2}, "b": {"date": ts("2022-01-04"), "numerator": 3,
        "denominator": 0}, "c": {"date": ts("2022-01-05"), "numerator": 1, "denominator": 1},
        "d": {"date": "x", "numerator": 2, "denominator": 1}}
check(R.parse_splits({"events": {"splits": _bad}}) == [],
      "a malformed, zero-denominator or 1:1 event is skipped, never read as a ratio")
check(R.parse_splits({}) == [] and R.parse_splits({"events": {"splits": None}}) == []
      and R.parse_splits({"events": None}) == [] and R.parse_splits(None) == [],
      "no events at all is no splits")

# ── restate_shares ──
_nv = [("2021-07-20", 4.0), ("2024-06-10", 10.0)]
_sh = {2020: 2.5e9, 2021: 2.5e9, 2024: 2.49e10, 2025: 2.45e10}
_fd = {2020: "2021-02-26", 2021: "2022-03-18", 2024: "2025-02-26", 2025: "2026-02-25"}
_r = R.restate_shares(_sh, _fd, _nv, "2016-10-01")
check(_r[2020] == 2.5e9 * 40 and _r[2021] == 2.5e9 * 10 and _r[2024] == 2.49e10 and _r[2025] == 2.45e10,
      f"EACH COUNT TAKES EVERY SPLIT AFTER ITS FILING DATE, and only those: Nvidia's FY2020 "
      f"count x40, FY2021 x10, FY2024 as filed (got {_r})")
check(R.restate_shares({2025: 100.0}, {2025: "2026-04-06"}, [("2026-04-06", 25.0)], "2016-10-01")
      == {2025: 100.0},
      "a split on the filing date is already in the filing's number")
check(R.restate_shares({2022: 100.0}, {2022: "2023-02-01"}, [("2023-03-01", 0.1)], "2016-10-01")
      == {2022: 10.0}, "a reverse split shrinks the earlier count")
check(R.restate_shares({2015: 1.0, 2016: 2.0, 2019: 3.0}, {2015: "2016-02-01", 2019: "2020-02-01"},
                       [], "2016-10-01") == {2019: 3.0},
      "A COUNT FILED BEFORE THE SPLIT HISTORY BEGINS, OR WITH NO FILING DATE, IS DROPPED — "
      "a split before the window could be missing from it")

# ── market_metrics: Booking ──
# FCF per year in dollars; share counts as the 10-Ks filed them, all before
# the April 2026 split. The price is post-split throughout, as Yahoo gives it.
BK_FCF = {2019: 4.9e9, 2020: 0.1e9, 2021: 2.6e9, 2022: 6.4e9, 2023: 7.3e9, 2024: 8.4e9, 2025: 9.0e9}
BK_SH = {2019: 43.8e6, 2020: 41.1e6, 2021: 41.4e6, 2022: 39.4e6, 2023: 36.5e6, 2024: 34.1e6, 2025: 32.639e6}
BK_FILED = {y: f"{y + 1}-02-2{y % 3}" for y in BK_SH}
BK = chart(lambda y, m: 164.0 if (y, m) >= (2026, 4) else 164.0 * (0.6 + 0.05 * (y - 2016)),
           splits=[BKNG_SPLIT])
bk = R.market_metrics(BK, BK_FCF, BK_SH, BK_FILED)
fcf_ps_now = 9.0e9 / (32.639e6 * 25)
check(abs(bk["pfcf_now"] - round(164.0 / fcf_ps_now, 1)) < 0.05 and bk["pfcf_now"] > 10,
      f"BOOKING'S P/FCF IS THE POST-SPLIT PRICE OVER POST-SPLIT FCF PER SHARE — "
      f"{164.0 / fcf_ps_now:.1f}x, not 0.7x (got {bk['pfcf_now']})")
check(bk["pfcf_med"] is not None and bk["pfcf_med"] > 5,
      f"and its history is on the same basis (median {bk['pfcf_med']}x, was 0.7x)")
check(bk["price"] == 164.0, "the price itself was never wrong: it is Yahoo's last close")
check(bk["shares_cagr5"] == R._cagr(BK_SH, 5),
      "a split after EVERY count scales them all alike, so the buyback trend is unchanged")
check(bk["_restated"] is True, "and the row is counted as restated")
_as_filed = R.market_metrics(BK, BK_FCF, BK_SH, BK_FILED, apply_splits=False)
check(_as_filed["pfcf_now"] < 1,
      f"(the fixture reproduces the fault when splits are ignored: {_as_filed['pfcf_now']}x)")

# ── market_metrics: Chipotle — a split in the middle of the history ──
CMG_SPLIT = split("2024-06-26", 50, 1)
CM_SH = {2019: 28.0e6, 2020: 28.3e6, 2021: 28.4e6, 2022: 27.9e6, 2023: 27.7e6,
         2024: 1.37e9, 2025: 1.35e9}
CM_FILED = {y: f"{y + 1}-02-05" for y in CM_SH}
CM_FCF = {2019: 0.5e9, 2020: 0.3e9, 2021: 0.9e9, 2022: 1.0e9, 2023: 1.2e9, 2024: 1.5e9, 2025: 1.6e9}
CM = chart(lambda y, m: 35.0 + (y - 2016), splits=[CMG_SPLIT])
cm = R.market_metrics(CM, CM_FCF, CM_SH, CM_FILED)
check(cm["shares_cagr5"] is not None and -3 < cm["shares_cagr5"] < 1,
      f"CHIPOTLE'S SHARE COUNT SHRINKS SLIGHTLY, not grows 90% a year: the pre-split "
      f"counts are restated x50 (got {cm['shares_cagr5']}%, as filed "
      f"{R._cagr(CM_SH, 5)}%)")
check(cm["pfcf_med"] is not None and cm["pfcf_med"] > 20,
      f"and its historical median P/FCF is on today's basis ({cm['pfcf_med']}x, "
      f"not the 1.2x the unrestated counts gave)")
check(R.market_metrics(CM, CM_FCF, CM_SH, CM_FILED, apply_splits=False)["pfcf_med"] < 5,
      "(ignoring the split reproduces the 1.2x-style median)")

# ── the other paths ──
_plain = R.market_metrics(chart(lambda y, m: 50.0), BK_FCF, BK_SH, BK_FILED)
check(_plain["_restated"] is False and _plain["shares_cagr5"] == R._cagr(BK_SH, 5)
      and _plain["pfcf_now"] == round(50.0 / (9.0e9 / 32.639e6), 1),
      "a company with no splits is untouched")
_foreign = R.market_metrics(BK, BK_FCF, BK_SH, BK_FILED, apply_splits=False)
check("shares_cagr5" not in _foreign and "_restated" not in _foreign,
      "A FOREIGN FILER'S COUNTS ARE LEFT AS FILED — an ADR's splits and ratio changes are "
      "not its ordinary shares' — and its as-filed share trend is not replaced")
_nonusd = R.market_metrics(BK, {}, {}, {})
check(_nonusd["pfcf_now"] is None and _nonusd["pfcf_med"] is None and "shares_cagr5" not in _nonusd,
      "a non-USD reporter (FCF and shares withheld) gets no P/FCF, as before")
check(R.market_metrics(chart(lambda y, m: 1.0, start=(2026, 1)), BK_FCF, BK_SH, BK_FILED) is None,
      "under a year of prices is no market data")
# The P/FCF median uses the last PFCF_YEARS of prices, as the 7-year chart did;
# the longer chart is for the split history only.
_old = {2016: 1e9, 2017: 1e9, 2018: 1e9, 2023: 1e9, 2024: 1e9, 2025: 1e9}
_osh = {y: 1e8 for y in _old}
_ofd = {y: f"{y + 1}-02-01" for y in _old}
check(R.market_metrics(chart(lambda y, m: 100.0), _old, _osh, _ofd)["pfcf_med"] is None,
      "YEARS BEFORE THE 7-YEAR WINDOW DON'T ENTER THE MEDIAN — three in-window years are "
      "too few for one")
_mi = {"currency": "USD", "_fcf": {2025: 1.0}, "_shares": {2025: 2.0}, "_shares_filed": {2025: "2026-02-01"}}
check(R.market_inputs(_mi, {"foreign_filer": False}) == ({2025: 1.0}, {2025: 2.0}, {2025: "2026-02-01"}, True),
      "a domestic USD filer goes to Stage C with its FCF, shares and dates, splits applied")
check(R.market_inputs(_mi, {"foreign_filer": True})[3] is False,
      "A FOREIGN FILER'S SHARES ARE NOT RESTATED BY ITS ADR'S SPLITS")
check(R.market_inputs(dict(_mi, currency="JPY"), {})[:2] == ({}, {}),
      "a yen reporter's FCF and shares are withheld, as before")
check(R.PFCF_YEARS == 7 and R.CHART_RANGE == "10y",
      "the chart reaches ten years back for splits; the multiple history stays at seven")

# ── share counts filed at the wrong scale (the SEC's own figures) ──
MCD = {2015: 919_900_000, 2016: 829_700_000, 2017: 803_000_000, 2018: 776_600_000,
       2019: 755_600_000, 2020: 750_100_000, 2021: 751_800_000, 2022: 741_300_000,
       2023: 732.3, 2024: 721.9, 2025: 716.4}
_f, _did = R.fix_share_scale(MCD)
check(_did and abs(_f[2025] - 716.4e6) < 1 and _f[2022] == 741_300_000,
      f"MCDONALD'S 732.3 / 721.9 / 716.4 ARE MILLIONS: put back on the scale of the rest "
      f"(got {_f[2025]:,.0f}) — the fault behind its 0.0x P/FCF")
COP = {2015: 1_241_919, 2016: 1_245_440, 2017: 1_221_038, 2018: 1_175_538, 2019: 1_123_536,
       2020: 1_078_030, 2021: 1_328_151, 2022: 1_278_163_000, 2023: 1_205_675_000,
       2024: 1_180_871_000, 2025: 1_253_446_000}
_f, _ = R.fix_share_scale(COP)
check(_f[2015] == 1_241_919_000 and _f[2025] == 1_253_446_000 and -2 < R._cagr(_f, 5) < 5,
      f"CONOCOPHILLIPS' 2015–21 COUNTS WERE THOUSANDS: rescaled, its share count no longer "
      f"'grows' {R._cagr(COP, 5)}% a year (now {R._cagr(_f, 5)}%)")
DDS = {2019: 25_364_000, 2020: 22_697_000, 2021: 20_592_000, 2022: 17_549, 2023: 16_517,
       2024: 16_120, 2025: 15_655}
_f, _ = R.fix_share_scale(DDS)
check(_f[2025] == 15_655_000 and _f[2019] == 25_364_000,
      "Dillard's switch to thousands in FY2022 is undone, anchored on a listable latest count")
UCTT = {2016: 33_150_000, 2017: 34_303_000, 2018: 38_919_000, 2019: 39.5, 2020: 44.4,
        2022: 45.7, 2023: 44.7, 2024: 45_300_000, 2025: 45_300_000}
_f, _ = R.fix_share_scale(UCTT)
check(abs(_f[2020] - 44.4e6) < 1 and _f[2025] == 45_300_000 and _f[2016] == 33_150_000,
      "a run of millions in the MIDDLE of a series (Ultra Clean 2019–23) is rescaled, both "
      "breaks read")
_steady = {y: 1e8 * (0.97 ** i) for i, y in enumerate(range(2015, 2026))}
check(R.fix_share_scale(_steady) == (_steady, False), "an ordinary buyback series is untouched")
_dilute = {2019: 1e6, 2020: 5e6, 2021: 4e7, 2022: 3e8, 2023: 2.5e9}
check(R.fix_share_scale(_dilute) == (_dilute, False),
      "HEAVY BUT REAL DILUTION (5–8x A YEAR) IS NOT MISTAKEN FOR A CHANGE OF UNIT")
_merger = {2020: 2e6, 2021: 2.1e6, 2022: 3.15e8, 2023: 3.2e8}
check(R.fix_share_scale(_merger) == (_merger, False),
      "a 150x jump in one year (a merger issuance) is real: only within 3x of a thousand "
      "is a jump read as a change of unit")
_tiny = {2021: 900.0, 2022: 850.0, 2023: 800.0}
check(R.fix_share_scale(_tiny) == (_tiny, False),
      "a series with no break is left alone whatever its size: nothing to anchor it to")
check(R.fix_share_scale({2025: 5.0}) == ({2025: 5.0}, False) and R.fix_share_scale({}) == ({}, False),
      "one year or none: nothing to compare")
_m_mc = R.market_metrics(chart(lambda y, m: 236.5), {y: 7e9 for y in MCD}, MCD,
                         {y: f"{y + 1}-02-25" for y in MCD})
check(10 < _m_mc["pfcf_now"] < 40 and _m_mc["_rescaled"] is True,
      f"McDonald's P/FCF comes out as a real multiple ({_m_mc['pfcf_now']}x, was 0.0x)")
_rs = R.market_metrics(chart(lambda y, m: 5.0, splits=[split("2024-03-01", 1, 1000)]),
                       {2021: 1e6, 2022: 1e6, 2023: 1e6, 2024: 1e6},
                       {2021: 5e9, 2022: 5e9, 2023: 5e9, 2024: 5e6},
                       {2021: "2022-02-01", 2022: "2023-02-01", 2023: "2024-02-01", 2024: "2025-02-01"})
check(_rs["_rescaled"] is False and _rs["pfcf_now"] == 25.0,
      "A 1-FOR-1,000 REVERSE SPLIT IS RESTATED BEFORE THE SCALE CHECK, so it is never "
      "mistaken for a change of unit")
_mf = R.compute_metrics({"us-gaap": {
    "Revenues": {"units": {"USD": [fyv(y, 1e9, f"{y + 1}-02-10") for y in MCD]}},
    "NetIncomeLoss": {"units": {"USD": [fyv(y, 2e8, f"{y + 1}-02-10") for y in MCD]}},
    "NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": [fyv(y, 3e8, f"{y + 1}-02-10") for y in MCD]}},
    "PaymentsToAcquirePropertyPlantAndEquipment": {"units": {"USD": [fyv(y, 1e8, f"{y + 1}-02-10") for y in MCD]}},
    "WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": [
        fyv(y, v, f"{y + 1}-02-10") for y, v in MCD.items()]}}}})
check(-3 < _mf["shares_cagr5"] < 0 and _mf["_shares"][2025] == 716.4,
      f"Stage B's own share trend (used when there is no price) is rescaled too "
      f"({_mf['shares_cagr5']}%), while Stage C still gets the counts as filed")

# ── compute_metrics hands Stage C the as-filed counts and their filing dates ──
YEARS = range(2016, 2026)
facts = {"us-gaap": {
    "Revenues": {"units": {"USD": [fyv(y, 1e9 + y, f"{y + 1}-02-10") for y in YEARS]}},
    "NetIncomeLoss": {"units": {"USD": [fyv(y, 2e8, f"{y + 1}-02-10") for y in YEARS]}},
    "NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": [fyv(y, 3e8, f"{y + 1}-02-10") for y in YEARS]}},
    "PaymentsToAcquirePropertyPlantAndEquipment": {"units": {"USD": [fyv(y, 1e8, f"{y + 1}-02-10") for y in YEARS]}},
    "WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": [
        fyv(y, 1e7, f"{y + 1}-02-10") for y in YEARS if y >= 2018]}},
    "WeightedAverageNumberOfSharesOutstandingBasic": {"units": {"shares": [
        fyv(y, 9e6, f"{y + 1}-03-01") for y in YEARS if y < 2018]}},
}}
_m = R.compute_metrics(facts)
check(_m["_shares"][2019] == 1e7 and _m["_shares_filed"][2019] == "2020-02-10",
      "each year's share count arrives with the filing date of the value kept")
check(_m["_shares"][2016] == 9e6 and _m["_shares_filed"][2016] == "2017-03-01",
      "a year filled from a second tag carries THAT tag's filing date")
check(_m["_fcf"][2025] == 2e8 and "fcf_ps" not in _m,
      "FCF goes across whole; FCF per share is no longer built before the splits are known")
# ── capex and operating income filed under tags the main lists don't hold ──
def co(extra, years=range(2016, 2026)):
    base = {
        "Revenues": {"units": {"USD": [fyv(y, 1e10, f"{y + 1}-02-10") for y in years]}},
        "NetIncomeLoss": {"units": {"USD": [fyv(y, 1e9, f"{y + 1}-02-10") for y in years]}},
        "NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": [fyv(y, 2e9, f"{y + 1}-02-10") for y in years]}},
        "WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": [fyv(y, 1e8, f"{y + 1}-02-10") for y in years]}},
        "LongTermDebtNoncurrent": {"units": {"USD": [dict(fyv(y, 3e9, f"{y + 1}-02-10"), start=None) for y in years]}},
    }
    base.update(extra)
    return {"us-gaap": base}


def usd(ys, v, tag_start=True):
    return {"units": {"USD": [fyv(y, v, f"{y + 1}-02-10") for y in ys]}}


# Verizon: PaymentsToAcquireProductiveAssets to 2018, ...OtherProductiveAssets since.
_vz = R.compute_metrics(co({"PaymentsToAcquireProductiveAssets": usd(range(2016, 2019), 5e8),
                            "PaymentsToAcquireOtherProductiveAssets": usd(range(2019, 2026), 8e8)}))
check(_vz["fcf_last"] == 2e9 - 8e8 and _vz["capex_ocf"] == 40.0,
      f"VERIZON'S CAPEX IS READ THROUGH ITS 2019 TAG CHANGE: this year's free cash flow "
      f"exists and the capex ratio is on the last five years, not 2014-18 "
      f"(got fcf {_vz['fcf_last']}, capex/OCF {_vz['capex_ocf']})")
# Eli Lilly: only the "Other" tag.
_lly = R.compute_metrics(co({"PaymentsToAcquireOtherPropertyPlantAndEquipment": usd(range(2016, 2026), 6e8)}))
check(_lly["fcf_last"] == 1.4e9 and _lly["capex_ocf"] == 30.0,
      "a filer whose whole capex line is the 'Other' tag has free cash flow (Lilly, ADP)")
# A minor "Other" line filed for MORE years than the main capex must not take over.
_minor = R.compute_metrics(co({"PaymentsToAcquirePropertyPlantAndEquipment": usd(range(2018, 2026), 9e8),
                               "PaymentsToAcquireOtherPropertyPlantAndEquipment": usd(range(2012, 2026), 1e7)}))
check(_minor["capex_ocf"] == 45.0 and _minor["fcf_last"] == 2e9 - 9e8,
      "THE 'OTHER' TAGS ONLY FILL YEARS THE MAIN CAPEX TAG LACKS — a small side line "
      "filed for longer never replaces the real capex figure")
# TJX: OperatingIncomeLoss stops in 2019; pre-tax income + interest continue.
_tjx = R.compute_metrics(co({"OperatingIncomeLoss": usd(range(2016, 2020), 1.5e9),
                             "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": usd(range(2016, 2026), 1.3e9),
                             "InterestExpense": usd(range(2016, 2026), 1e8)}))
check(_tjx["nd_ebit"] == round(3e9 / 1.4e9, 2),
      f"TJX HAS A NET DEBT/EBIT AGAIN: pre-tax income plus interest stands in for the "
      f"operating line it stopped filing in 2019 (got {_tjx['nd_ebit']})")
check(_tjx["op_margin_med"] is not None,
      "and its margins and ROIC are measured on the recent years too")
_keep = R.compute_metrics(co({"OperatingIncomeLoss": usd(range(2016, 2026), 1.5e9),
                              "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": usd(range(2016, 2026), 9e9)}))
check(_keep["nd_ebit"] == 2.0, "a year with a filed operating line keeps it — the proxy only fills")
_eur = R.compute_metrics(co({"IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest":
                             {"units": {"EUR": [fyv(y, 1.3e9, f"{y + 1}-02-10") for y in range(2016, 2026)]}}}))
check(_eur["nd_ebit"] is None, "a filler in another currency is not spliced into dollars")


# Debt last filed as zero in 2020; the interest bill is checked against REVENUE.
def _zero_then(interest):
    return co({"LongTermDebtNoncurrent": {"units": {"USD": [
                   dict(fyv(y, 3e9 if y < 2020 else 0.0, f"{y + 1}-02-10"), start=None)
                   for y in range(2016, 2021)]}},
               "StockholdersEquity": {"units": {"USD": [
                   dict(fyv(y, 5e9, f"{y + 1}-02-10"), start=None) for y in range(2016, 2026)]}},
               "InterestExpense": usd(range(2016, 2026), interest)})


check(R.compute_metrics(_zero_then(1e8))["total_debt"] is None
      and R.compute_metrics(_zero_then(1e6))["total_debt"] == 0.0
      and R.compute_metrics(_zero_then(1e6))["debt_zero_as_of"] == "2020-12-31",
      "compute_metrics hands revenue to the balance sheet: 1% of revenue in interest keeps a "
      "reported zero from standing, 0.01% does not")

_amend = {"us-gaap": {"WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": [
    fyv(2023, 5.0, "2024-02-01"), fyv(2024, 5.0, "2025-02-01"), fyv(2025, 5.0, "2026-02-01"),
    dict(fyv(2025, 6.0, "2026-05-01"), form="10-K/A")]}}}}
_fo = {}
_s, _ = R._annual_series(_amend, R.TAGS["shares_diluted"], "shares", _fo)
check(_s[2025] == 6.0 and _fo[2025] == "2026-05-01",
      "an amended 10-K's count comes with the amendment's date")
_fo2 = {}
R._annual_series({"us-gaap": {"WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": [
    fyv(2024, 5.0, "2025-02-01"), fyv(2025, 5.0, "2026-02-01")]}}}},
    R.TAGS["shares_diluted"], "shares", _fo2)
check(_fo2 == {}, "a series too short to use reports no dates either")


# ── whose cash flow: the parent's share where minority holders own part ──
PS = R.parent_shares
check(PS({2025: 352.9}, {2025: 684.6}, {}) == {2025: 352.9 / 684.6},
      "the parent's share is its profit over the group's (Hess Midstream 2025: 52%)")
check(PS({2025: 100.0}, {2025: 100.0}, {}) == {} and PS({2025: 99.0}, {2025: 100.0}, {}) == {},
      "a company with no material minority holders is the parent's whole")
check(PS({2025: 109.0}, {2025: 83.0}, {2025: -26.0}) == {},
      "a minority LOSS (parent profit above the group's) gives the parent no extra cash: whole")
check(PS({2024: -5.0}, {2024: 10.0}, {}) == {2024: None} and PS({2024: -8.0}, {2024: -5.0}, {}) == {2024: None},
      "A LOSS YEAR WITH A MATERIAL MINORITY AND NO NEIGHBOUR TO TAKE A SHARE FROM IS NONE, never guessed")
check(PS({2024: 50.0, 2025: -67.0}, {2024: 100.0, 2025: 53.0}, {}) == {2024: 0.5, 2025: 0.5},
      "A LOSS YEAR TAKES THE OWNERSHIP SHARE OF THE YEAR BEFORE (Omnicom's merger-charge year)")
check(PS({2024: 100.0, 2025: -5.0}, {2024: 100.0, 2025: -4.0}, {}) == {},
      "a neighbour that was the parent's whole makes the loss year whole too (Hyatt, General Mills)")
check(PS({2020: 50.0, 2025: -67.0}, {2020: 100.0, 2025: 53.0}, {}) == {2020: 0.5, 2025: None},
      "a neighbour more than three years away is too far to borrow ownership from")
check(PS({2023: 40.0, 2025: -67.0, 2026: 80.0}, {2023: 100.0, 2025: 53.0, 2026: 100.0}, {})[2025] == 0.8,
      "the nearest year wins")
check(PS({2025: -5_840.0}, {2025: -5_830.0}, {2025: 10.0}) == {} and PS({2025: -100.0}, {2025: -99.0}, {}) == {},
      "A LOSS YEAR WITH AN IMMATERIAL MINORITY LINE IS THE PARENT'S WHOLE (Kraft Heinz 2025): only a material "
      "minority share in a loss year is unattributable")
check(PS({}, {2023: 200.0}, {2023: 120.0}) == {2023: 0.4} and PS({2023: 80.0}, {}, {2023: 120.0}) == {2023: 0.4},
      "a missing parent or total line is rebuilt from the minority line")
check(abs(PS({2021: 54.585}, {2021: 141.902}, {2021: 141.902})[2021] - 0.3847) < 1e-3,
      "Formula Systems tagged its whole 2021 profit as the minority's; parent over total still reads 38%")

HY = range(2019, 2026)
NI = {2019: 16.1, 2020: 24.0, 2021: 46.4, 2022: 83.9, 2023: 118.6, 2024: 223.1, 2025: 352.9}
PL = {2019: 75.1, 2020: 484.9, 2021: 617.8, 2022: 620.6, 2023: 607.7, 2024: 659.0, 2025: 684.6}
OCF = {2019: 470.7, 2020: 641.7, 2021: 795.5, 2022: 861.1, 2023: 866.4, 2024: 940.3, 2025: 983.8}
CAP = {2019: 306.4, 2020: 301.1, 2021: 163.2, 2022: 238.2, 2023: 223.5, 2024: 306.1, 2025: 255.6}


def hesm(ni=NI, pl=PL):
    return {"us-gaap": {
        "Revenues": {"units": {"USD": [fyv(y, 1e9 + y, f"{y + 1}-02-10") for y in HY]}},
        "NetIncomeLoss": {"units": {"USD": [fyv(y, ni[y] * 1e6, f"{y + 1}-02-10") for y in HY]}},
        "ProfitLoss": {"units": {"USD": [fyv(y, pl[y] * 1e6, f"{y + 1}-02-10") for y in HY]}},
        "NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": [fyv(y, OCF[y] * 1e6, f"{y + 1}-02-10") for y in HY]}},
        "PaymentsToAcquirePropertyPlantAndEquipment": {"units": {"USD": [fyv(y, CAP[y] * 1e6, f"{y + 1}-02-10") for y in HY]}},
        "WeightedAverageNumberOfSharesOutstandingBasic": {"units": {"shares": [fyv(y, 1e8, f"{y + 1}-02-10") for y in HY]}}}}


_h = R.compute_metrics(hesm())
check(all(abs(_h["_fcf"][y] - (OCF[y] - CAP[y]) * 1e6 * NI[y] / PL[y]) < 1 for y in HY),
      "HESS MIDSTREAM: EACH YEAR'S FREE CASH FLOW IS CUT TO THE PUBLIC SHARES' SHARE OF THAT YEAR "
      "(5% in 2020, 52% in 2025) before it is divided by their count")
check(_h["parent_share"] == round(352.9 / 684.6, 3) and _h["nci_years"] == 7 and not _h["nci_unattributed"],
      "the latest share and the years adjusted are recorded")
check(abs(_h["fcf_last"] - (OCF[2025] - CAP[2025]) * 1e6) < 1,
      "the company's own free cash flow stays whole in fcf_last (it is a fact about the business)")
_conv = sum((OCF[y] - CAP[y]) * NI[y] / PL[y] for y in HY) / sum(NI.values()) * 100
check(abs(_h["fcf_conv"] - round(_conv, 1)) < 0.05 and _h["fcf_conv"] < 150,
      f"conversion is the parent's cash over the parent's profit ({_h['fcf_conv']}%, was 410%)")
# ProfitLoss filed for more years than NetIncomeLoss: the build's net-income series is then the
# group's total, and conversion must still use the parent's profit wherever it is filed.
_short = hesm()
_short["us-gaap"]["NetIncomeLoss"]["units"]["USD"] = [v for v in _short["us-gaap"]["NetIncomeLoss"]["units"]["USD"]
                                                     if v["fy"] >= 2022]
_hs = R.compute_metrics(_short)
_num = sum((OCF[y] - CAP[y]) * (NI[y] / PL[y] if y >= 2022 else 1.0) for y in HY)
_den = sum(NI[y] if y >= 2022 else PL[y] for y in HY)
check(abs(_hs["fcf_conv"] - round(_num / _den * 100, 1)) < 0.05,
      f"CONVERSION DIVIDES BY THE PARENT'S PROFIT wherever it is filed, even when the group total is "
      f"the longer series ({_hs['fcf_conv']}%)")
_loss = R.compute_metrics(hesm(ni={**NI, 2025: -50.0}))
check(not _loss["nci_unattributed"] and _loss["parent_share"] == round(NI[2024] / PL[2024], 3)
      and abs(_loss["_fcf"][2025] - (OCF[2025] - CAP[2025]) * 1e6 * NI[2024] / PL[2024]) < 1,
      "a loss year takes the ownership share of the year before")
# Three years of group profit whose parent side is a loss each year: no year has a split to borrow.
_ln = R.compute_metrics({"us-gaap": {**hesm()["us-gaap"],
    "ProfitLoss": {"units": {"USD": [fyv(y, 600e6, f"{y + 1}-02-10") for y in (2023, 2024, 2025)]}},
    "NetIncomeLoss": {"units": {"USD": [fyv(y, (-30.0 if y >= 2023 else NI[y]) * 1e6, f"{y + 1}-02-10")
                                        for y in HY]}}}})
check(_ln["nci_unattributed"] and _ln["_fcf"] == {} and _ln["parent_share"] is None,
      "WHEN THE LATEST YEAR CAN'T BE ATTRIBUTED AND HAS NO NEIGHBOUR TO BORROW FROM, NO MULTIPLE IS BUILT — "
      "not one from a stale year")
check(_m["parent_share"] is None and _m["nci_years"] == 0 and _m["_fcf"][2025] == 2e8,
      "a company with no minority holders is untouched")
_mm = R.market_metrics(chart(lambda y, m: 38.25), _h["_fcf"], _h["_shares"], _h["_shares_filed"])
check(_mm["pfcf_now"] == round(38.25 / (_h["_fcf"][2025] / 1e8), 1),
      "and P/FCF divides the price by the shareholders' own cash flow per share")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} compounders market checks passed.")
print(f"   Booking: {bk['pfcf_now']}x P/FCF on post-split shares (as filed: {_as_filed['pfcf_now']}x)")
sys.exit(0)
