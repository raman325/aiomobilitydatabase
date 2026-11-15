"""GTFS-Flex booking rules placed on a dated window (booking_rules.txt).

``booking_window`` reads only the fields each ``booking_type`` allows, and
gives no window at all -- never a guessed one -- where a field it needs is
missing or a day count does not end.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

from aiomobilitydatabase.feeds._booking import (
    LOOK_BACK_DAYS,
    booking_window,
    counted_day,
)
from aiomobilitydatabase.feeds.models import BookingRule, BookingType, BookingWindow

_D = date(2026, 11, 13)  # a Friday
_WS = datetime(2026, 11, 13, 9, tzinfo=UTC)
_WE = datetime(2026, 11, 13, 17, tzinfo=UTC)
_WEEKDAYS = "WEEKDAYS"


def _rule(booking_type: BookingType | None, **fields: object) -> BookingRule:
    blank = BookingRule(
        id="R",
        booking_type=booking_type,
        prior_notice_duration_min=None,
        prior_notice_duration_max=None,
        prior_notice_last_day=None,
        prior_notice_last_time=None,
        prior_notice_start_day=None,
        prior_notice_start_time=None,
        prior_notice_service_id=None,
        message=None,
        pickup_message=None,
        drop_off_message=None,
        phone_number=None,
        info_url=None,
        booking_url=None,
    )
    return replace(blank, **fields)  # type: ignore[arg-type]


def _midnight(day: date) -> datetime:
    return datetime.combine(day, time(0), tzinfo=UTC)


def _weekdays(service_id: str, day: date) -> bool:
    return service_id == _WEEKDAYS and day.weekday() < 5


def _place(
    rule: BookingRule,
    *,
    service_date: date = _D,
    window: tuple[datetime, datetime] = (_WS, _WE),
    runs: object = _weekdays,
) -> tuple[datetime | None, datetime] | None:
    placed = booking_window(
        rule,
        service_date,
        *window,
        day_start=_midnight,
        runs=runs,  # type: ignore[arg-type]
    )
    if placed is None:
        return None
    assert placed.rule is rule
    return placed.earliest, placed.latest


# -- booking_type=0 -----------------------------------------------------------


def test_real_time_booking_runs_to_the_window_end() -> None:
    """Real-time booking has no earliest time, and a ride late in the window
    can be booked up to its end."""
    assert _place(_rule(BookingType.REAL_TIME)) == (None, _WE)


# -- booking_type=1 -----------------------------------------------------------


def test_same_day_booking_counts_minutes_back_from_the_window() -> None:
    """At least ``duration_min`` before travel, at most ``duration_max``:
    the latest ride is booked by its end less the minimum, the earliest
    from its start less the maximum."""
    rule = _rule(BookingType.SAME_DAY, prior_notice_duration_min=30)
    assert _place(rule) == (None, _WE - timedelta(minutes=30))
    rule = replace(rule, prior_notice_duration_max=120)
    assert _place(rule) == (_WS - timedelta(hours=2), _WE - timedelta(minutes=30))


def test_same_day_booking_may_open_on_a_start_day() -> None:
    """Without a duration max, a start day and time may say when booking
    opens, counted in calendar days (a service id is forbidden here)."""
    rule = _rule(
        BookingType.SAME_DAY,
        prior_notice_duration_min=30,
        prior_notice_start_day=3,
        prior_notice_start_time=timedelta(hours=8),
    )
    assert _place(rule) == (
        datetime(2026, 11, 10, 8, tzinfo=UTC),
        _WE - timedelta(minutes=30),
    )


_FORBIDDEN = {
    "prior_notice_duration_min": 30,
    "prior_notice_duration_max": 60,
    "prior_notice_last_day": 1,
    "prior_notice_last_time": timedelta(hours=17),
    "prior_notice_start_day": 3,
    "prior_notice_start_time": timedelta(hours=8),
    "prior_notice_service_id": _WEEKDAYS,
}
_VALID = {
    BookingType.REAL_TIME: {},
    BookingType.SAME_DAY: {"prior_notice_duration_min": 30},
    BookingType.PRIOR_DAYS: {
        "prior_notice_last_day": 1,
        "prior_notice_last_time": timedelta(hours=17),
    },
}


@pytest.mark.parametrize(
    ("booking_type", "field", "extra"),
    [
        *(
            (BookingType.REAL_TIME, field, {})
            for field in _FORBIDDEN
            if field != "prior_notice_start_time"
        ),
        (BookingType.SAME_DAY, "prior_notice_last_day", {}),
        (BookingType.SAME_DAY, "prior_notice_service_id", {}),
        (
            BookingType.SAME_DAY,
            "prior_notice_start_day",
            {"prior_notice_duration_max": 60},
        ),
        (BookingType.SAME_DAY, "prior_notice_start_time", {}),
        (BookingType.PRIOR_DAYS, "prior_notice_duration_min", {}),
        (BookingType.PRIOR_DAYS, "prior_notice_duration_max", {}),
        (BookingType.PRIOR_DAYS, "prior_notice_start_time", {}),
    ],
)
def test_a_forbidden_field_leaves_the_rule_unknown(
    booking_type: BookingType, field: str, extra: dict[str, object]
) -> None:
    """The spec forbids it for this booking_type, so the rule contradicts
    itself and which field is wrong is unknowable: no window, never one
    built by ignoring a field (real-time booking ignoring a stated notice
    would read bookable up to the ride)."""
    valid = _rule(booking_type, **_VALID[booking_type], **extra)
    assert _place(valid) is not None
    assert _place(replace(valid, **{field: _FORBIDDEN[field]})) is None


def test_a_contradictory_rule_is_unknown() -> None:
    """A duration max under the min allows no ride; booking opening after
    its deadline never opens: both contradictions, not windows."""
    assert (
        _place(
            _rule(
                BookingType.SAME_DAY,
                prior_notice_duration_min=120,
                prior_notice_duration_max=30,
            ),
            window=(_WS, _WS + timedelta(hours=4)),
        )
        is None
    )
    assert (
        _place(
            _rule(
                BookingType.PRIOR_DAYS,
                prior_notice_last_day=7,
                prior_notice_last_time=timedelta(hours=17),
                prior_notice_start_day=1,
                prior_notice_start_time=timedelta(hours=8),
            )
        )
        is None
    )
    assert (
        _place(
            _rule(
                BookingType.SAME_DAY,
                prior_notice_duration_min=30,
                prior_notice_start_day=0,
                prior_notice_start_time=timedelta(hours=20),
            )
        )
        is None
    )


def test_same_day_booking_without_its_minimum_is_unknown() -> None:
    """``prior_notice_duration_min`` is required for booking_type=1."""
    assert _place(_rule(BookingType.SAME_DAY, prior_notice_duration_max=60)) is None


def test_a_start_day_without_its_time_is_unknown() -> None:
    """The start time is required with a start day: no earliest is
    guessed, so no window."""
    rule = _rule(
        BookingType.SAME_DAY, prior_notice_duration_min=30, prior_notice_start_day=3
    )
    assert _place(rule) is None


# -- booking_type=2 -----------------------------------------------------------


def test_prior_day_booking_counts_service_days() -> None:
    """ "Book by 5PM the business day before, at the earliest a week of
    business days ahead at midnight": counted on the service's days, so a
    Monday ride books by Friday."""
    rule = _rule(
        BookingType.PRIOR_DAYS,
        prior_notice_last_day=1,
        prior_notice_last_time=timedelta(hours=17),
        prior_notice_start_day=5,
        prior_notice_start_time=timedelta(0),
        prior_notice_service_id=_WEEKDAYS,
    )
    monday = date(2026, 11, 16)
    assert _place(rule, service_date=monday) == (
        datetime(2026, 11, 9, tzinfo=UTC),
        datetime(2026, 11, 13, 17, tzinfo=UTC),
    )


def test_prior_day_booking_counts_calendar_days_without_a_service() -> None:
    """With no service id, ``prior_notice_last_day=2`` is two calendar days."""
    rule = _rule(
        BookingType.PRIOR_DAYS,
        prior_notice_last_day=2,
        prior_notice_last_time=timedelta(hours=17),
    )
    assert _place(rule, service_date=date(2026, 11, 16)) == (
        None,
        datetime(2026, 11, 14, 17, tzinfo=UTC),
    )


def test_a_last_day_of_zero_is_the_service_date() -> None:
    rule = _rule(
        BookingType.PRIOR_DAYS,
        prior_notice_last_day=0,
        prior_notice_last_time=timedelta(hours=7),
        prior_notice_service_id=_WEEKDAYS,
    )
    assert _place(rule, service_date=date(2026, 11, 15)) == (
        None,
        datetime(2026, 11, 15, 7, tzinfo=UTC),
    )


def test_prior_day_booking_without_its_last_time_is_unknown() -> None:
    for fields in (
        {"prior_notice_last_day": 1},
        {"prior_notice_last_time": timedelta(hours=17)},
        {},
    ):
        assert _place(_rule(BookingType.PRIOR_DAYS, **fields)) is None


def test_a_count_of_days_a_service_never_runs_is_unknown() -> None:
    """A year of looking back finds no day the service runs: no window."""
    rule = _rule(
        BookingType.PRIOR_DAYS,
        prior_notice_last_day=1,
        prior_notice_last_time=timedelta(hours=17),
        prior_notice_service_id="NEVER",
    )
    assert _place(rule) is None


def test_an_unknown_booking_type_places_nothing() -> None:
    assert _place(_rule(None, prior_notice_duration_min=30)) is None


def test_a_date_past_the_calendar_is_unknown() -> None:
    """Counting back from 0001-01-01, or minutes beyond datetime, gives no
    window instead of raising."""
    rule = _rule(
        BookingType.PRIOR_DAYS,
        prior_notice_last_day=1,
        prior_notice_last_time=timedelta(hours=17),
    )
    early = datetime(1, 1, 1, 9, tzinfo=UTC)
    assert _place(rule, service_date=date.min, window=(early, early)) is None
    huge = _rule(BookingType.SAME_DAY, prior_notice_duration_min=10**12)
    assert _place(huge) is None


def test_booking_times_are_measured_from_noon_minus_twelve_hours() -> None:
    """A GTFS time on a spring-forward day is measured from noon minus 12
    hours, so 17:00:00 on Chicago's 2026-03-08 is 17:00 CDT."""
    chicago = ZoneInfo("America/Chicago")

    def day_start(day: date) -> datetime:
        noon = datetime.combine(day, time(12), tzinfo=chicago)
        return noon.astimezone(UTC) - timedelta(hours=12)

    rule = _rule(
        BookingType.PRIOR_DAYS,
        prior_notice_last_day=1,
        prior_notice_last_time=timedelta(hours=17),
    )
    placed = booking_window(
        rule, date(2026, 3, 9), _WS, _WE, day_start=day_start, runs=_weekdays
    )
    assert placed is not None
    assert placed.latest == datetime(2026, 3, 8, 17, tzinfo=chicago)


# -- is_open ------------------------------------------------------------------

_INSTANTS = st.datetimes(
    min_value=datetime(2026, 1, 1),
    max_value=datetime(2027, 1, 1),
    timezones=st.just(UTC),
)


@given(earliest=st.none() | _INSTANTS, latest=_INSTANTS, at=_INSTANTS)
def test_is_open_is_its_definition(
    earliest: datetime | None, latest: datetime, at: datetime
) -> None:
    window = BookingWindow(_rule(BookingType.REAL_TIME), earliest, latest)
    assert window.is_open(at) == ((earliest is None or at >= earliest) and at <= latest)


def test_is_open_includes_both_ends() -> None:
    window = BookingWindow(_rule(BookingType.REAL_TIME), _WS, _WE)
    assert window.is_open(_WS)
    assert window.is_open(_WE)
    assert not window.is_open(_WE + timedelta(microseconds=1))
    assert not window.is_open(_WS - timedelta(microseconds=1))


# -- properties ---------------------------------------------------------------

_MINUTES = st.integers(0, 3 * 24 * 60)


@given(
    minimum=_MINUTES,
    maximum=st.none() | _MINUTES,
    start=_INSTANTS,
    length=st.integers(0, 24 * 3600),
)
def test_same_day_bounds_are_exact(
    minimum: int, maximum: int | None, start: datetime, length: int
) -> None:
    end = start + timedelta(seconds=length)
    rule = _rule(
        BookingType.SAME_DAY,
        prior_notice_duration_min=minimum,
        prior_notice_duration_max=maximum,
    )
    placed = _place(rule, service_date=start.date(), window=(start, end))
    if maximum is not None and maximum < minimum:
        assert placed is None  # no ride's notice can be both
        return
    earliest = None if maximum is None else start - timedelta(minutes=maximum)
    latest = end - timedelta(minutes=minimum)
    if earliest is not None and earliest > latest:
        assert placed is None
        return
    assert placed == (earliest, latest)


_DAY0 = date(2026, 1, 1)


@given(
    running=st.frozensets(st.integers(0, 120)),
    offset=st.integers(0, 120),
    days=st.integers(0, 12),
)
def test_prior_days_land_on_the_counted_service_day(
    running: frozenset[int], offset: int, days: int
) -> None:
    """The booking day is the ``days``-th day the service runs before the
    service date (the service date itself for zero): exactly ``days``
    running days lie in ``[booking day, service date)``. None when fewer
    run in range."""
    service_date = _DAY0 + timedelta(days=offset)
    run_days = sorted(
        (_DAY0 + timedelta(days=n) for n in running if n < offset), reverse=True
    )

    def runs(service_id: str, day: date) -> bool:
        return service_id == "B" and (day - _DAY0).days in running

    rule = _rule(
        BookingType.PRIOR_DAYS,
        prior_notice_last_day=days,
        prior_notice_last_time=timedelta(hours=17),
        prior_notice_service_id="B",
    )
    placed = _place(rule, service_date=service_date, runs=runs)
    if days == 0:
        expected: date | None = service_date
    elif len(run_days) >= days:
        expected = run_days[days - 1]
    else:
        expected = None
    assert placed == (
        None if expected is None else (None, _midnight(expected) + timedelta(hours=17))
    )


@given(
    running=st.frozensets(st.integers(0, 60)),
    days=st.integers(0, 10),
    more=st.integers(1, 5),
)
def test_more_notice_never_moves_the_deadline_later(
    running: frozenset[int], days: int, more: int
) -> None:
    service_date = _DAY0 + timedelta(days=60)

    def runs(service_id: str, day: date) -> bool:
        return (day - _DAY0).days in running

    base = _rule(
        BookingType.PRIOR_DAYS,
        prior_notice_last_day=days,
        prior_notice_last_time=timedelta(hours=17),
        prior_notice_service_id="B",
    )
    near = _place(base, service_date=service_date, runs=runs)
    far = _place(
        replace(base, prior_notice_last_day=days + more),
        service_date=service_date,
        runs=runs,
    )
    if near is not None and far is not None:
        assert far[1] <= near[1]
    if near is None:
        assert far is None  # fewer days in range for the nearer count


def test_the_look_back_is_bounded() -> None:
    """A service running once, a year and a day back, is out of reach."""
    service_date = date(2026, 11, 13)
    far = service_date - timedelta(days=LOOK_BACK_DAYS)

    def runs(service_id: str, day: date) -> bool:
        return day == far

    assert counted_day(service_date, 1, "B", runs) == far
    assert (
        counted_day(service_date, 1, "B", lambda s, d: d == far - timedelta(1)) is None
    )
