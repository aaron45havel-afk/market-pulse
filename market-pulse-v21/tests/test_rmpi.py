"""Freshness reporting for the real-mortgage-payment index.

Run:  python tests/test_rmpi.py      (exit 0 = all pass)

WHY THIS FILE EXISTS. The page showed "as of May 2026" in mid-August and
read as broken. It was not: the three inputs are inner-joined, and
Case-Shiller publishes about two months in arrears, so the newest month
all three share IS two months back. The page just never said so.

Everything here is pure date arithmetic and dictionary comparison, which
matters because FRED is unreachable from the sandbox this was written in.
The fetch was already a thin wrapper; the part that can be wrong — which
input is blamed, and when the chart will next advance — is separated out
so it can be proved offline.
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import real_mortgage_index as R

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── last Tuesday of the month ──
check(R.last_tuesday(2026, 8) == date(2026, 8, 25),
      "August 2026's last Tuesday is the 25th")
check(R.last_tuesday(2026, 7) == date(2026, 7, 28),
      "July 2026's last Tuesday is the 28th")
check(R.last_tuesday(2024, 12) == date(2024, 12, 31),
      "a month ENDING on a Tuesday returns that day, not the week before — "
      "the off-by-one that would name a release seven days early")
check(R.last_tuesday(2026, 2) == date(2026, 2, 24),
      "February works like any other month")
check(R.last_tuesday(2024, 2) == date(2024, 2, 27),
      "including a leap February, where the 29th shifts the last week")
for _y in range(2020, 2035):
    for _m in range(1, 13):
        _d = R.last_tuesday(_y, _m)
        check(_d.weekday() == 1 and _d.month == _m and _d.year == _y,
              f"last_tuesday({_y},{_m}) lands on a Tuesday inside that month")
        check((_d + __import__("datetime").timedelta(days=7)).month != _m,
              f"and there is no later Tuesday in {_y}-{_m:02d}")


# ── the next release, and what it will carry ──
_n = R.next_case_shiller_release(date(2026, 8, 19))
check(_n["date"] == "2026-08-25" and _n["covers"] == "2026-06",
      "asked on 19 Aug 2026, the next release is 25 Aug and it brings JUNE — "
      "which is the whole answer to why the chart sat on May")
check(R.next_case_shiller_release(date(2026, 8, 25))["date"] == "2026-08-25",
      "on release day the answer is today, not next month — a reader "
      "refreshing that morning is told to look now")
check(R.next_case_shiller_release(date(2026, 8, 26))["date"] == "2026-09-29",
      "the day after, it rolls to September")
_dec = R.next_case_shiller_release(date(2026, 12, 30))
check(_dec["date"] == "2027-01-26" and _dec["covers"] == "2026-11",
      "the year boundary rolls both the release and the month it covers — "
      "January's report carries the previous NOVEMBER")
_nov = R.next_case_shiller_release(date(2026, 1, 5))
check(_nov["covers"] == "2025-11",
      "and a January release names a month in the prior year, which is where "
      "a naive month-minus-two would produce '2026-11' or month zero")


# ── which input is actually holding the chart back ──
check(R.binding_source({"hpi": "2026-05", "rate": "2026-08",
                        "cpi": "2026-07"}) == "hpi",
      "the OLDEST series gates an inner join, so Case-Shiller is named")
check(R.binding_source({"hpi": "2026-07", "rate": "2026-08",
                        "cpi": "2026-04"}) == "cpi",
      "but this is MEASURED, not assumed — if CPI stalls, CPI is named. A "
      "page that always blamed Case-Shiller would send a reader to check "
      "the wrong feed on the one day it mattered")
check(R.binding_source({}) is None
      and R.binding_source({"hpi": None, "rate": None, "cpi": None}) is None,
      "with nothing known it returns None rather than picking a scapegoat")
check(R.binding_source({"hpi": None, "rate": "2026-08"}) == "rate",
      "a series that reported nothing is not treated as infinitely old")


# ── month arithmetic ──
check(R.months_between("2026-05", "2026-08") == 3, "May to August is 3 months")
check(R.months_between("2025-11", "2026-01") == 2, "spanning a year boundary")
check(R.months_between("2026-08", "2026-08") == 0, "same month is 0")
check(R.months_between("2026-09", "2026-08") == -1,
      "and it can go negative rather than clamping, so a future-dated "
      "series shows as wrong instead of as fine")
check(R.months_between("", "2026-08") is None
      and R.months_between(None, "2026-08") is None
      and R.months_between("garbage", "2026-08") is None,
      "junk yields None, never 0 — 'zero months behind' is the reassuring "
      "reading and would be a lie")


# ── the assembled block ──
_f = R.freshness({"hpi": "2026-05", "rate": "2026-08", "cpi": "2026-07"},
                 "2026-05", date(2026, 8, 19))
check(_f["binding_source"] == "hpi"
      and _f["binding_label"] == "Case-Shiller home prices",
      "the block names the gating input in words a reader can act on")
check(_f["months_behind"] == 3,
      "and says how far back the newest point sits from the current month")
check(_f["next_case_shiller"]["covers"] == "2026-06",
      "and when that will change")
check(_f["last_observation"] == {"hpi": "2026-05", "rate": "2026-08",
                                 "cpi": "2026-07"},
      "the per-series dates travel too, so the claim can be checked rather "
      "than taken on faith")
_f["last_observation"]["hpi"] = "1999-01"
check(R.freshness({"hpi": "2026-05"}, "2026-05",
                  date(2026, 8, 19))["last_observation"]["hpi"] == "2026-05",
      "the block copies its input — mutating what came back cannot reach "
      "into the cached payload it was built from")


# ── re-dating a cached payload ──
_cached = {"as_of": "2026-05-31",
           "freshness": R.freshness({"hpi": "2026-05", "rate": "2026-08",
                                     "cpi": "2026-07"},
                                    "2026-05", date(2026, 8, 19))}
check(_cached["freshness"]["next_case_shiller"]["date"] == "2026-08-25",
      "a payload built on 19 Aug names the 25th")
_re = R._refresh_freshness(_cached)
check(_re["freshness"]["next_case_shiller"]["date"]
      == R.next_case_shiller_release(date.today())["date"],
      "and re-dating moves it to today's answer — served straight from a "
      "24h cache it would eventually name a release already in the past, "
      "which is the page being confidently wrong about the one thing a "
      "reader came to it for")
check(_re["as_of"] == "2026-05-31",
      "while the series and every other field are left exactly alone")
check(_cached["freshness"]["next_case_shiller"]["date"] == "2026-08-25",
      "and the cached dict is not mutated in place")
_old = {"as_of": "2026-05-31"}
check(R._refresh_freshness(_old) == _old
      and R._refresh_freshness({"freshness": {}}) == {"freshness": {}}
      and R._refresh_freshness({}) == {},
      "a payload written before this block existed passes through "
      "untouched — an unexplained date is a smaller lie than an invented "
      "explanation, and the next fetch replaces it anyway")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} real-mortgage-index freshness checks passed.")
print("   Case-Shiller publishes on the last Tuesday, "
      f"{R.CASE_SHILLER_LAG_MONTHS} months in arrears.")
sys.exit(0)
