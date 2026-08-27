"""
Time, and the difference between an instant and a legal date.

CLAUDE.md: "Store timestamps as UTC timestamptz. Store legal dates (notice
served, lease start, rent due) as date in the PROPERTY's local timezone,
computed from the property's jurisdiction, never from the server's timezone
or the browser's."

That distinction is the whole module. A rent charge posted at 2026-11-01
00:30 UTC is 2026-10-31 in California. Post it against the wrong date and
the tenant is late on a month they paid, or a notice is served a day early
and is void. Railway runs UTC; the operator is in California; the buildings
are in two timezones. Every one of those is a different answer, and the only
correct one is the property's.

  * TimeService is the ONLY source of "now". Injectable, so a test can pin
    the clock instead of skipping the DST cases that actually break things.
  * JurisdictionClock converts an instant to a legal date IN A NAMED ZONE,
    and does deadline arithmetic on calendar days rather than on 86,400
    second chunks — because across a DST boundary a day is 23 or 25 hours,
    and "30 days from service" means thirty calendar days.

Pure: no database, no network. zoneinfo only, which ships with Python.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

UTC = timezone.utc


class ClockError(ValueError):
    pass


class TimeService:
    """The only thing in ops that knows what time it is.

    Everything takes one of these rather than calling datetime.now(). A
    test that cannot pin the clock is a test that will not cover the DST
    transition, and the DST transition is exactly where the bugs are.
    """

    def __init__(self, fixed: datetime | None = None):
        if fixed is not None and fixed.tzinfo is None:
            raise ClockError("A fixed clock must be timezone-aware.")
        self._fixed = fixed

    def now(self) -> datetime:
        """Always timezone-aware UTC. Never naive."""
        return self._fixed if self._fixed else datetime.now(UTC)

    def today_utc(self) -> date:
        """Rarely what you want. If it is a LEGAL date, use JurisdictionClock."""
        return self.now().date()

    def freeze(self, at: datetime) -> "TimeService":
        return TimeService(at)


def zone(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        raise ClockError(
            f"Unknown timezone {tz_name!r}. A property's timezone comes from "
            f"its jurisdiction row and must be a valid IANA name — a wrong "
            f"one silently shifts every legal date it touches."
        )


class JurisdictionClock:
    """Legal dates and deadlines in one property's local timezone."""

    def __init__(self, tz_name: str, time_service: TimeService | None = None):
        self.tz_name = tz_name
        self.tz = zone(tz_name)
        self.ts = time_service or TimeService()

    # ── instants to legal dates ──
    def local_date(self, instant: datetime | None = None) -> date:
        """The calendar date it is (or was) HERE, not on the server.

        The case this exists for: 2026-11-01T00:30Z is 31 October in
        America/Los_Angeles. A rent charge stamped with the UTC date would
        post to the wrong month.
        """
        i = instant or self.ts.now()
        if i.tzinfo is None:
            raise ClockError("Refusing to localise a naive datetime — its "
                             "meaning depends on an assumption nobody wrote "
                             "down.")
        return i.astimezone(self.tz).date()

    def start_of_day(self, on: date) -> datetime:
        """Local midnight as a UTC instant. Handles DST gaps.

        On a spring-forward date some zones have no 00:00. fold/normalise
        via a known-safe hour rather than constructing an impossible time.
        """
        naive = datetime(on.year, on.month, on.day, 12, 0)
        noon_local = naive.replace(tzinfo=self.tz)
        midnight = noon_local - timedelta(hours=12)
        # Re-anchor: if the wall clock is not 00:00 the zone skipped it.
        local = midnight.astimezone(self.tz)
        if local.hour != 0:
            midnight += timedelta(hours=(0 - local.hour) % 24)
        return midnight.astimezone(UTC)

    # ── deadline arithmetic ──
    def add_days(self, start: date, days: int) -> date:
        """Calendar days. Not 86,400-second multiples.

        Across a DST boundary a day is 23 or 25 hours, so doing this in
        seconds drifts by one across the boundary — and a notice period
        that is one day short is void, not approximately right.
        """
        if not isinstance(start, date) or isinstance(start, datetime):
            raise ClockError("add_days takes a date, not a datetime — a legal "
                             "deadline has no time of day.")
        return start + timedelta(days=int(days))

    def add_months(self, start: date, months: int) -> date:
        """Calendar months, clamping to the last valid day.

        31 January + 1 month is 28 February (29 in a leap year), which is
        what a lease means by "monthly" and what every court has assumed
        since before software. Never rolls into the next month.
        """
        if not isinstance(start, date) or isinstance(start, datetime):
            raise ClockError("add_months takes a date.")
        m = start.month - 1 + int(months)
        y = start.year + m // 12
        m = m % 12 + 1
        return date(y, m, min(start.day, days_in_month(y, m)))

    def deadline(self, event_date: date, days: int,
                 counting: str = "calendar") -> date:
        """The date a `days`-day window from `event_date` expires.

        `counting` is explicit because jurisdictions differ and the default
        must never be a guess:
          calendar        — every day counts
          business        — Mon-Fri only (public holidays are NOT modelled;
                            see the note below)
        The event day itself is day zero — the count starts the following
        day, which is the near-universal convention. Where a jurisdiction
        counts differently, that belongs in its rule row, not here.

        HOLIDAYS ARE NOT MODELLED. A real business-day count needs a
        per-jurisdiction holiday calendar, and inventing one would produce
        confidently wrong dates. Callers asking for business days get
        weekday-only counting and must treat the result as provisional
        until Phase 7 loads real calendars.
        """
        if counting not in ("calendar", "business"):
            raise ClockError(f"Unknown counting mode {counting!r}.")
        if counting == "calendar":
            return self.add_days(event_date, days)
        out, left = event_date, int(days)
        while left > 0:
            out += timedelta(days=1)
            if out.weekday() < 5:
                left -= 1
        return out

    def days_between(self, a: date, b: date) -> int:
        """Calendar days from a to b. Negative if b precedes a."""
        return (b - a).days

    def is_past(self, deadline_date: date, instant: datetime | None = None) -> bool:
        """Has the local day rolled past this deadline?

        Compares LOCAL date to local date. Comparing a deadline to a UTC
        instant makes a Californian deadline expire up to eight hours early.
        """
        return self.local_date(instant) > deadline_date


def days_in_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    return (date(year, month + 1, 1) - timedelta(days=1)).day


def month_end(on: date) -> date:
    return date(on.year, on.month, days_in_month(on.year, on.month))


def due_date_in_month(year: int, month: int, due_day: int) -> date:
    """The rent due date for a month, clamped to a day that exists.

    A lease with rent_due_day = 31 is due on 28 February, not on 3 March.
    This is a one-line function and it is here rather than inline because
    the inline version is where the off-by-one lives.
    """
    if not 1 <= int(due_day) <= 31:
        raise ClockError("rent_due_day must be 1-31.")
    return date(year, month, min(int(due_day), days_in_month(year, month)))
