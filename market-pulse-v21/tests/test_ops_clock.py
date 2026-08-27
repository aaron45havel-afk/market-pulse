"""Legal dates in the property's timezone — proved across the cases that break.

Run:  python tests/test_ops_clock.py      (exit 0 = all pass)

Railway runs UTC. The operator is in California. The buildings are in two
timezones. Those are three different answers to "what day is it", and only
one of them — the property's — is the legal one.

Every check below is pinned to a fixed instant. A clock test that uses the
real clock silently skips the DST transition on 364 days of the year, and
the DST transition is where the bugs are.
"""
import os
import sys
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.ops import clock as C

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
    except C.ClockError:
        return True


UTC = timezone.utc
CA = "America/Los_Angeles"
RI = "America/New_York"


# ── TimeService is the only source of now ──
_fixed = datetime(2026, 8, 26, 17, 30, tzinfo=UTC)
_ts = C.TimeService(_fixed)
check(_ts.now() == _fixed, "a frozen clock returns exactly what it was given")
check(C.TimeService().now().tzinfo is not None,
      "the live clock is timezone-AWARE — a naive 'now' is a bug waiting for "
      "the first comparison against a stored timestamptz")
check(raises(C.TimeService, datetime(2026, 1, 1)),
      "a naive fixed clock is refused; its meaning depends on an assumption "
      "nobody wrote down")


# ── the case this module exists for ──
_late = datetime(2026, 11, 1, 0, 30, tzinfo=UTC)
_ca = C.JurisdictionClock(CA, C.TimeService(_late))
_ri = C.JurisdictionClock(RI, C.TimeService(_late))
check(_ca.local_date() == date(2026, 10, 31),
      "2026-11-01 00:30 UTC is 31 OCTOBER in California. A rent charge "
      "stamped with the UTC date posts to the wrong month, and the tenant is "
      "late on a month they paid")
check(_ri.local_date() == date(2026, 10, 31),
      "and 31 October in Rhode Island too, four hours behind UTC")
check(_late.date() == date(2026, 11, 1) and _ca.local_date() == date(2026, 10, 31),
      "the UTC date and the legal date genuinely differ here — which is the "
      "whole point, and why nothing may use the server's date")

_noon = datetime(2026, 6, 15, 19, 0, tzinfo=UTC)   # 12:00 PDT / 15:00 EDT
check(C.JurisdictionClock(CA, C.TimeService(_noon)).local_date() == date(2026, 6, 15)
      and C.JurisdictionClock(RI, C.TimeService(_noon)).local_date() == date(2026, 6, 15),
      "mid-day both coasts agree — the divergence is only near midnight, "
      "which is exactly why it survives casual testing")
check(raises(_ca.local_date, datetime(2026, 6, 15, 12, 0)),
      "localising a NAIVE datetime is refused rather than assumed to be UTC")


# ── deadline arithmetic across DST ──
# US DST 2026: spring forward 8 March, fall back 1 November.
_c = C.JurisdictionClock(CA)
check(_c.add_days(date(2026, 3, 7), 1) == date(2026, 3, 8),
      "one day across spring-forward is one CALENDAR day")
check(_c.add_days(date(2026, 3, 1), 30) == date(2026, 3, 31),
      "thirty days spanning spring-forward lands on the 31st — done in "
      "86,400-second chunks it would drift to the 30th, and a notice period "
      "one day short is void, not approximately right")
check(_c.add_days(date(2026, 10, 20), 30) == date(2026, 11, 19),
      "and thirty days spanning fall-back does not gain a day either")
check(_c.add_days(date(2026, 2, 27), 2) == date(2026, 3, 1),
      "February 2026 has 28 days")
check(_c.add_days(date(2024, 2, 27), 2) == date(2024, 2, 29),
      "February 2024 has 29 — leap years are not special-cased, they are "
      "just calendar arithmetic")
check(raises(_c.add_days, datetime(2026, 3, 1, tzinfo=UTC), 30),
      "add_days refuses a datetime — a legal deadline has no time of day, "
      "and accepting one invites a timezone into a date calculation")


# ── month arithmetic clamps rather than rolling over ──
check(_c.add_months(date(2026, 1, 31), 1) == date(2026, 2, 28),
      "31 January + 1 month is 28 FEBRUARY, not 3 March. Rolling over is how "
      "a monthly lease silently skips a month")
check(_c.add_months(date(2024, 1, 31), 1) == date(2024, 2, 29),
      "and 29 February in a leap year")
check(_c.add_months(date(2026, 1, 31), 12) == date(2027, 1, 31),
      "a full year returns to the same day")
check(_c.add_months(date(2026, 3, 31), -1) == date(2026, 2, 28),
      "backwards clamps the same way")
check(_c.add_months(date(2026, 12, 15), 1) == date(2027, 1, 15),
      "crossing a year boundary works")


# ── rent due dates ──
check(C.due_date_in_month(2026, 2, 31) == date(2026, 2, 28),
      "a lease with rent_due_day = 31 is due 28 February — NOT 3 March, and "
      "not skipped. This single clamp is the whole February rent-posting bug")
check(C.due_date_in_month(2024, 2, 31) == date(2024, 2, 29), "leap February")
check(C.due_date_in_month(2026, 4, 31) == date(2026, 4, 30), "and 30-day months")
check(C.due_date_in_month(2026, 1, 1) == date(2026, 1, 1), "the 1st is the 1st")
check(raises(C.due_date_in_month, 2026, 1, 0)
      and raises(C.due_date_in_month, 2026, 1, 32),
      "an impossible due day is refused")


# ── counting modes are explicit ──
check(_c.deadline(date(2026, 8, 26), 30) == date(2026, 9, 25),
      "a 30-day calendar window from a Wednesday")
check(_c.deadline(date(2026, 8, 26), 3, "business") == date(2026, 8, 31),
      "three business days from Wednesday 26 Aug skips the weekend and lands "
      "on Monday the 31st")
check(_c.deadline(date(2026, 8, 28), 1, "business") == date(2026, 8, 31),
      "one business day from a Friday is the following Monday")
check(raises(_c.deadline, date(2026, 8, 26), 5, "banking"),
      "an unrecognised counting mode raises rather than quietly falling back "
      "to calendar days")


# ── two jurisdictions, one instant, two answers ──
_evening = datetime(2026, 8, 27, 3, 0, tzinfo=UTC)  # 20:00 PDT 26th / 23:00 EDT 26th
check(C.JurisdictionClock(CA, C.TimeService(_evening)).local_date() == date(2026, 8, 26)
      and C.JurisdictionClock(RI, C.TimeService(_evening)).local_date() == date(2026, 8, 26),
      "both still on the 26th at 03:00 UTC")
_later = datetime(2026, 8, 27, 5, 0, tzinfo=UTC)    # 22:00 PDT 26th / 01:00 EDT 27th
check(C.JurisdictionClock(CA, C.TimeService(_later)).local_date() == date(2026, 8, 26)
      and C.JurisdictionClock(RI, C.TimeService(_later)).local_date() == date(2026, 8, 27),
      "TWO HOURS LATER THE TWO PROPERTIES ARE ON DIFFERENT DATES. A single "
      "server-side 'today' is wrong for one of them every single night")


# ── is_past compares local to local ──
_pc = C.JurisdictionClock(CA, C.TimeService(datetime(2026, 8, 27, 4, 0, tzinfo=UTC)))
check(_pc.is_past(date(2026, 8, 26)) is False,
      "at 21:00 local on the 26th, a deadline of the 26th has NOT passed — "
      "comparing against the UTC instant would expire it up to eight hours "
      "early, on the wrong calendar day")
check(_pc.is_past(date(2026, 8, 25)) is True, "but the 25th has")


# ── helpers ──
check(C.days_in_month(2026, 2) == 28 and C.days_in_month(2024, 2) == 29
      and C.days_in_month(2026, 12) == 31 and C.days_in_month(2026, 4) == 30,
      "days_in_month handles February, leap February, and December")
check(C.month_end(date(2026, 2, 10)) == date(2026, 2, 28), "month_end clamps")
check(_c.days_between(date(2026, 1, 1), date(2026, 3, 1)) == 59,
      "days_between counts calendar days across a short February")
check(_c.days_between(date(2026, 3, 1), date(2026, 1, 1)) == -59,
      "and goes negative backwards rather than returning an absolute value")


# ── timezone validation ──
check(raises(C.zone, "America/Nowhere") and raises(C.zone, "") and raises(C.zone, None),
      "an unknown IANA zone raises. A wrong one silently shifts every legal "
      "date it touches, which is the least detectable failure in this module")
check(C.zone(CA).key == CA and C.zone(RI).key == RI, "both real zones resolve")

_sod = _c.start_of_day(date(2026, 3, 8))     # spring-forward day in the US
check(_sod.tzinfo is not None and _sod.astimezone(C.zone(CA)).hour == 0,
      "local midnight resolves to a real instant even on a spring-forward "
      "date, rather than constructing a wall-clock time that does not exist")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ops-clock checks passed.")
print("   DST both directions, leap February, month-end clamping, and two "
      "properties on different dates at the same instant.")
sys.exit(0)
