"""Place a GTFS-Flex booking rule on one dated on-demand window.

Pure: the calendar comes in as two callables, so the index supplies its
own service days and tests supply theirs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from .models import BookingRule, BookingType, BookingWindow

# How far back a count of service days may look before it reads unknown:
# a year of the counted service's days covers any real booking notice.
LOOK_BACK_DAYS = 366


def counted_day(
    service_date: date,
    days: int,
    service_id: str | None,
    runs: Callable[[str, date], bool],
) -> date | None:
    """Return the ``days``-th day before ``service_date``, or None.

    Calendar days without ``service_id``; with it, only the days that
    service runs count (``prior_notice_service_id``), looking back at most
    :data:`LOOK_BACK_DAYS` days. Zero days is the service date itself.
    None when the count does not end in range.
    """
    if service_id is None:
        try:
            return service_date - timedelta(days=days)
        except OverflowError:
            return None
    day = service_date
    for _ in range(LOOK_BACK_DAYS):
        if days == 0:
            return day
        try:
            day -= timedelta(days=1)
        except OverflowError:
            return None
        if runs(service_id, day):
            days -= 1
    return day if days == 0 else None


def booking_window(
    rule: BookingRule,
    service_date: date,
    window_start: datetime,
    window_end: datetime,
    *,
    day_start: Callable[[date], datetime],
    runs: Callable[[str, date], bool],
) -> BookingWindow | None:
    """Return when a ride in ``[window_start, window_end]`` can be booked.

    See :class:`~.models.BookingWindow` for the reading of each
    ``booking_type``. ``day_start`` gives a service day's start instant
    (raising OverflowError when it can't be dated) and ``runs`` whether a
    service runs on a day. None when the rule's window can't be placed.
    """
    window = _Window(service_date, window_start, window_end, day_start, runs)
    reader = None if rule.booking_type is None else _READERS.get(rule.booking_type)
    if reader is None or set(rule.unreadable_fields) & _read_fields(rule):
        # An unknown booking_type says nothing placeable; a field this type
        # reads that the file states but can't be read is unknown, not
        # absent. A field the spec forbids for the type is never read.
        return None
    try:
        bounds = reader(rule, window)
    except OverflowError:
        return None
    if bounds is None:
        return None
    earliest, latest = bounds
    if latest > window_end or (earliest is not None and earliest > latest):
        # A deadline after every ride (the spec's "before travel"), or
        # booking that for this window opens after it closes: no window,
        # never one reading "never open" or open too late.
        return None
    return BookingWindow(rule=rule, earliest=earliest, latest=latest)


def _read_fields(rule: BookingRule) -> set[str]:
    """Return the prior-notice fields the rule's booking_type reads.

    Only those the spec allows it: none for real-time booking; for
    same-day booking the durations, and a start day (with its time) only
    without a duration max; for prior-day booking the last and start days
    and times. A time is read only with its day. Every other field the
    file states -- forbidden for the type -- is ignored.
    """
    unreadable = set(rule.unreadable_fields)

    def stated(field: str) -> bool:
        return getattr(rule, field) is not None or field in unreadable

    fields: set[str] = set()
    if rule.booking_type is BookingType.SAME_DAY:
        fields |= {"prior_notice_duration_min", "prior_notice_duration_max"}
        if not stated("prior_notice_duration_max"):
            fields.add("prior_notice_start_day")
    elif rule.booking_type is BookingType.PRIOR_DAYS:
        fields |= {
            "prior_notice_last_day",
            "prior_notice_last_time",
            "prior_notice_start_day",
        }
    if "prior_notice_start_day" in fields and stated("prior_notice_start_day"):
        fields.add("prior_notice_start_time")
    return fields


@dataclass(frozen=True)
class _Window:
    """One dated window and the calendar to count days on."""

    service_date: date
    start: datetime
    end: datetime
    day_start: Callable[[date], datetime]
    runs: Callable[[str, date], bool]

    def at(
        self, days: int | None, offset: timedelta | None, service_id: str | None
    ) -> datetime | None:
        """Return the GTFS time ``days`` counted days before travel, or None.

        The time is required wherever its day is given, so a missing one
        (or a count that does not end) leaves the bound unknown.
        """
        if days is None or offset is None:
            return None
        day = counted_day(self.service_date, days, service_id, self.runs)
        return None if day is None else self.day_start(day) + offset


_Bounds = tuple[datetime | None, datetime] | None


def _real_time(_rule: BookingRule, window: _Window) -> _Bounds:
    """booking_type=0: up to the window's end, from any time."""
    return None, window.end


def _same_day(rule: BookingRule, window: _Window) -> _Bounds:
    """booking_type=1: minutes before travel, or a start day on the calendar."""
    if rule.prior_notice_duration_min is None:
        return None  # Required for booking_type=1.
    latest = window.end - timedelta(minutes=rule.prior_notice_duration_min)
    if rule.prior_notice_duration_max is not None:
        if rule.prior_notice_duration_max < rule.prior_notice_duration_min:
            return None  # No ride's notice can be both: contradictory.
        return window.start - timedelta(minutes=rule.prior_notice_duration_max), latest
    # Without a duration max, a start day may say when booking opens, in
    # calendar days (prior_notice_service_id is type 2's alone).
    if rule.prior_notice_start_day is None:
        return None, latest
    earliest = window.at(
        rule.prior_notice_start_day, rule.prior_notice_start_time, None
    )
    return None if earliest is None else (earliest, latest)


def _prior_days(rule: BookingRule, window: _Window) -> _Bounds:
    """booking_type=2: days before travel, counted on the service's days."""
    service_id = rule.prior_notice_service_id
    latest = window.at(
        rule.prior_notice_last_day, rule.prior_notice_last_time, service_id
    )
    if latest is None:
        return None  # Both required for booking_type=2.
    if rule.prior_notice_start_day is None:
        return None, latest
    earliest = window.at(
        rule.prior_notice_start_day, rule.prior_notice_start_time, service_id
    )
    return None if earliest is None else (earliest, latest)


_READERS: dict[BookingType, Callable[[BookingRule, _Window], _Bounds]] = {
    BookingType.REAL_TIME: _real_time,
    BookingType.SAME_DAY: _same_day,
    BookingType.PRIOR_DAYS: _prior_days,
}
