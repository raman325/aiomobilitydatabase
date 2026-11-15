"""GTFS-Flex windows on dates: ``get_on_demand_windows``.

Each on-demand call of :meth:`get_on_demand_services` placed on every
service date its trip runs, where the window overlaps ``[start, end]``,
with its booking rules placed on it.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from functools import partial
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.geo import Circle
from aiomobilitydatabase.feeds.models import BookingType, OnDemandWindow
from aiomobilitydatabase.feeds.static_index import StaticIndex
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.fixtures import build_flex_gtfs_zip_bytes
from tests.feeds.test_gtfs_schedule_conformance import _build, _feed
from tests.feeds.test_time_window_properties import _run

_AT_H2 = Circle(41.01, -87.04, 10.0)
_AT_Z = Circle(0.0, 0.0, 10.0)
_JULY_30 = datetime(2026, 7, 30, tzinfo=UTC)
_HEADER = (
    "trip_id,stop_id,location_id,start_pickup_drop_off_window,"
    "end_pickup_drop_off_window,stop_sequence,pickup_type,drop_off_type,"
    "pickup_booking_rule_id,drop_off_booking_rule_id\n"
)
_ZONE = json.dumps(
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "Z",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]],
                },
            }
        ],
    }
)


def _zone_feed(stop_times: str, **files: str) -> dict[str, str]:
    """Trip T (route R, daily service S) calling at zone Z by ``stop_times``."""
    feed = _feed(stop_times=_HEADER + stop_times, **files)
    feed["locations.geojson"] = _ZONE
    return feed


def _windows(
    index: StaticIndex | Callable[[], StaticIndex], zone: Circle, **kwargs: object
) -> list[OnDemandWindow]:
    """Query once; a handle closes its index, so pass a factory to reuse a feed."""

    async def body(handle: TransitFeedHandle) -> list[OnDemandWindow]:
        return await handle.get_on_demand_windows(zone, **kwargs)  # type: ignore[arg-type]

    return _run(index if isinstance(index, StaticIndex) else index(), body)


def _spans(windows: list[OnDemandWindow]) -> list[tuple[object, ...]]:
    return [
        (w.service.trip_id, w.service_date, w.window_start, w.window_end)
        for w in windows
    ]


def test_the_fixture_dates_each_call_on_its_weekdays(tmp_path: Path) -> None:
    """Friday's three calls, then Monday's (WEEKDAYS skips the weekend),
    in Chicago time; DAY_BEFORE books by 5PM the business day before, from
    midnight seven business days ahead."""
    zip_path = tmp_path / "flex.zip"
    zip_path.write_bytes(build_flex_gtfs_zip_bytes())
    index = StaticIndex.build(zip_path, ":memory:", "ds", None)
    chicago = ZoneInfo("America/Chicago")
    friday, monday = date(2026, 11, 13), date(2026, 11, 16)

    def at(day: date, hour: int, minute: int = 0) -> datetime:
        return datetime.combine(day, time(hour, minute), tzinfo=chicago)

    windows = _windows(
        index, _AT_H2, start=datetime(2026, 11, 13, 12, tzinfo=UTC), limit=4
    )
    assert [
        (w.service.trip_id, w.service.stop_sequence, *_spans([w])[0][1:])
        for w in windows
    ] == [
        ("DEV1", 2, friday, at(friday, 8, 5), at(friday, 8, 25)),
        ("ZONE1", 1, friday, at(friday, 9), at(friday, 17)),
        ("ZONE1", 2, friday, at(friday, 9), at(friday, 17)),
        ("DEV1", 2, monday, at(monday, 8, 5), at(monday, 8, 25)),
    ]
    now = windows[0].pickup_booking
    assert now is not None
    assert (now.rule.id, now.earliest, now.latest) == ("NOW", None, at(friday, 8, 25))
    day_before = windows[1].pickup_booking
    assert day_before is not None
    assert (day_before.earliest, day_before.latest) == (
        at(date(2026, 11, 4), 0),
        at(date(2026, 11, 12), 17),
    )
    assert windows[1].drop_off_booking is None  # ZONE1 #1 names no drop-off rule
    assert windows[2].drop_off_booking == day_before


def test_both_ends_of_the_query_are_inclusive(tmp_path: Path) -> None:
    """A window ending exactly at ``start`` counts, as does one opening
    exactly at ``end``; a second either side, they don't."""
    index = partial(_build, tmp_path, _zone_feed("T,,Z,08:00:00,09:00:00,1,2,2,,\n"))
    nine = _JULY_30.replace(hour=9)
    assert [w.service_date for w in _windows(index, _AT_Z, start=nine, limit=1)] == [
        date(2026, 7, 30)
    ]
    later = nine + timedelta(seconds=1)
    july_31 = date(2026, 7, 31)
    eight = datetime.combine(july_31, time(8), tzinfo=UTC)
    assert [w.service_date for w in _windows(index, _AT_Z, start=later, end=eight)] == [
        july_31
    ]
    assert _windows(index, _AT_Z, start=later, end=eight - timedelta(seconds=1)) == []


def test_a_window_from_the_day_before_is_still_under_way(tmp_path: Path) -> None:
    """A window running past midnight (23:00:00-26:00:00) belongs to its
    service date: at 01:00 it is the previous day's that is open."""
    index = _build(tmp_path, _zone_feed("T,,Z,23:00:00,26:00:00,1,2,2,,\n"))
    [window] = _windows(index, _AT_Z, start=_JULY_30.replace(hour=1), limit=1)
    assert _spans([window]) == [
        (
            "T",
            date(2026, 7, 29),
            datetime(2026, 7, 29, 23, tzinfo=UTC),
            datetime(2026, 7, 30, 2, tzinfo=UTC),
        )
    ]


def test_a_window_is_placed_from_noon_minus_twelve_hours(tmp_path: Path) -> None:
    """On Chicago's spring-forward day, 08:00:00 is 08:00 CDT: GTFS times
    count from noon minus 12 hours, not from local midnight."""
    feed = _zone_feed("T,,Z,08:00:00,09:00:00,1,2,2,,\n")
    feed["agency.txt"] = (
        "agency_id,agency_name,agency_url,agency_timezone\n"
        "A,T,https://e.com,America/Chicago\n"
    )
    index = _build(tmp_path, feed)
    chicago = ZoneInfo("America/Chicago")
    [window] = _windows(
        index, _AT_Z, start=datetime(2026, 3, 8, 6, tzinfo=chicago), limit=1
    )
    assert window.window_start == datetime(2026, 3, 8, 8, tzinfo=chicago)
    assert window.window_end == datetime(2026, 3, 8, 9, tzinfo=chicago)


def test_an_open_ended_query_reaches_a_far_service_day(tmp_path: Path) -> None:
    """A service added on one date a year away, and removed on the one
    date its calendar would run, is found by an open-ended query."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,,\n",
        calendar="service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
        "sunday,start_date,end_date\nS,1,1,1,1,1,1,1,20260730,20260730\n",
        calendar_dates="service_id,date,exception_type\nS,20260730,2\nS,20270601,1\n",
    )
    index = _build(tmp_path, feed)
    assert [
        w.service_date for w in _windows(index, _AT_Z, start=_JULY_30, limit=5)
    ] == [date(2027, 6, 1)]


def test_an_unknown_window_is_listed_but_never_dated(tmp_path: Path) -> None:
    """A frequencies.txt trip of Flex calls alone has no window of its own:
    get_on_demand_services lists it, unknown; it has no dated window."""
    index = partial(
        _build,
        tmp_path,
        _zone_feed(
            "T,,Z,08:00:00,09:00:00,1,2,2,,\n",
            frequencies="trip_id,start_time,end_time,headway_secs\n"
            "T,06:00:00,07:00:00,600\n",
        ),
    )

    async def body(handle: TransitFeedHandle) -> int:
        return len(await handle.get_on_demand_services(_AT_Z))

    assert _run(index(), body) == 1
    assert _windows(index, _AT_Z, start=_JULY_30, limit=5) == []


def test_headway_repetitions_are_dated_and_say_so(tmp_path: Path) -> None:
    """A headway-based repetition's window is dated like any other, its
    service saying it is approximate."""
    feed = _feed(
        stop_times="trip_id,arrival_time,departure_time,stop_id,location_id,"
        "start_pickup_drop_off_window,end_pickup_drop_off_window,stop_sequence,"
        "pickup_type,drop_off_type\n"
        "T,08:00:00,08:00:00,S0,,,,0,,\n"
        "T,,,,Z,08:05:00,09:00:00,1,2,2\n",
        frequencies="trip_id,start_time,end_time,headway_secs,exact_times\n"
        "T,10:00:00,10:01:00,600,0\n",
    )
    feed["locations.geojson"] = _ZONE
    [window] = _windows(_build(tmp_path, feed), _AT_Z, start=_JULY_30, limit=1)
    assert (window.service.trip_id, window.service.exact_times) == ("T#36000", False)
    assert window.window_start == _JULY_30.replace(hour=10, minute=5)


def test_route_ids_narrow_the_windows(tmp_path: Path) -> None:
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,,\nU,,Z,08:00:00,09:00:00,1,2,2,,\n",
        routes="route_id,route_short_name,route_type\nR,R,3\nQ,Q,3\n",
        trips="route_id,service_id,trip_id\nR,S,T\nQ,S,U\n",
    )
    index = partial(_build, tmp_path, feed)
    assert {
        w.service.trip_id for w in _windows(index, _AT_Z, start=_JULY_30, limit=4)
    } == {
        "T",
        "U",
    }
    assert {
        w.service.trip_id
        for w in _windows(index, _AT_Z, start=_JULY_30, limit=4, route_ids=["Q"])
    } == {"U"}


def test_a_rule_the_dataset_lacks_places_no_booking(tmp_path: Path) -> None:
    """The service keeps the id; there is no rule to place."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,MISSING,NOW\n",
        booking_rules="booking_rule_id,booking_type\nNOW,0\n",
    )
    [window] = _windows(_build(tmp_path, feed), _AT_Z, start=_JULY_30, limit=1)
    assert window.service.pickup_booking_rule_id == "MISSING"
    assert window.pickup_booking is None
    assert window.drop_off_booking is not None
    assert window.drop_off_booking.latest == window.window_end


def test_stop_windows_and_stop_groups_are_dated_and_tied_by_sequence(
    tmp_path: Path,
) -> None:
    """A stop served within a window and a group with a stop in the zone
    are dated like a zone; equal window starts order by trip, then by
    stop_sequence."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,2,2,2,,\n"
        "T,S0,,08:00:00,09:00:00,1,2,2,,\n"
        "U,,,08:00:00,09:00:00,1,2,2,,\n",
        trips="route_id,service_id,trip_id\nR,S,T\nR,S,U\n",
        location_groups="location_group_id,location_group_name\nG,G\n",
        location_group_stops="location_group_id,stop_id\nG,S1\n",
    )
    feed["stop_times.txt"] = (
        feed["stop_times.txt"]
        .replace(
            "trip_id,stop_id,location_id,",
            "trip_id,stop_id,location_id,location_group_id,",
        )
        .replace("T,,Z,", "T,,Z,,")
        .replace("T,S0,,", "T,S0,,,")
        .replace("U,,,", "U,,,G,")
    )
    windows = _windows(_build(tmp_path, feed), _AT_Z, start=_JULY_30, limit=3)
    assert [
        (w.service.trip_id, w.service.stop_sequence, w.service_date) for w in windows
    ] == [
        ("T", 1, date(2026, 7, 30)),
        ("T", 2, date(2026, 7, 30)),
        ("U", 1, date(2026, 7, 30)),
    ]
    assert (windows[0].service.stop_id, windows[2].service.location_group_id) == (
        "S0",
        "G",
    )


def test_added_dates_alone_are_each_dated(tmp_path: Path) -> None:
    """A service of calendar_dates alone, on consecutive days and from the
    query's first day: each added date is a running day."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,,\n",
        calendar="service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
        "sunday,start_date,end_date\n",
        calendar_dates="service_id,date,exception_type\n"
        "S,20260730,1\nS,20260731,1\nS,20260802,1\n",
    )
    windows = _windows(_build(tmp_path, feed), _AT_Z, start=_JULY_30, limit=5)
    assert [w.service_date for w in windows] == [
        date(2026, 7, 30),
        date(2026, 7, 31),
        date(2026, 8, 2),
    ]


def test_a_window_ending_before_it_starts_is_never_dated(tmp_path: Path) -> None:
    """Service "becomes available" and then "ends": 10:00-08:00 says
    neither when, so it gets no date (get_on_demand_services keeps it)."""
    feed = _zone_feed("T,,Z,10:00:00,08:00:00,1,2,2,,\n")
    assert _windows(_build(tmp_path, feed), _AT_Z, start=_JULY_30, limit=3) == []


def test_a_sparse_far_calendar_is_walked_by_its_running_days(tmp_path: Path) -> None:
    """Mondays in January 2026 and one date in 9000: an open-ended query
    reads a handful of days, not the 7,000 years between."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,,\n",
        calendar="service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
        "sunday,start_date,end_date\nS,1,0,0,0,0,0,0,20260101,20260131\n",
        calendar_dates="service_id,date,exception_type\nS,90000101,1\n",
    )
    index = _build(tmp_path, feed)
    reads = 0
    active = index.active_service_ids

    def counted(service_date: date) -> set[str]:
        nonlocal reads
        reads += 1
        return active(service_date)

    with patch.object(index, "active_service_ids", counted):
        [window] = _windows(index, _AT_Z, start=_JULY_30, limit=1)
    assert window.service_date == date(9000, 1, 1)
    assert reads <= 2


def test_an_unreadable_or_late_booking_is_unknown(tmp_path: Path) -> None:
    """A duration max the feed states but can't be read is unknown, not
    "no earliest time"; a deadline at 23:00 on the travel day is after the
    08:00-09:00 window it is "before": contradictory, so no booking."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,GARBLED,LATE\n",
        booking_rules="booking_rule_id,booking_type,prior_notice_duration_min,"
        "prior_notice_duration_max,prior_notice_last_day,prior_notice_last_time\n"
        "GARBLED,1,30,1440.0,,\nLATE,2,,,0,23:00:00\n",
    )
    [window] = _windows(_build(tmp_path, feed), _AT_Z, start=_JULY_30, limit=1)
    rule = window.service.pickup_booking_rule
    assert rule is not None
    assert rule.unreadable_fields == ("prior_notice_duration_max",)
    assert (window.pickup_booking, window.drop_off_booking) == (None, None)


def test_booking_counts_on_a_service_no_call_runs(tmp_path: Path) -> None:
    """The rule counts business days on W, a service no trip uses: a
    Monday ride books by 17:00 the Friday before."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,P,\n",
        calendar="service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
        "sunday,start_date,end_date\n"
        "S,1,1,1,1,1,1,1,20260101,20271231\n"
        "W,1,1,1,1,1,0,0,20260101,20271231\n",
        booking_rules="booking_rule_id,booking_type,prior_notice_last_day,"
        "prior_notice_last_time,prior_notice_service_id\nP,2,1,17:00:00,W\n",
    )
    monday = datetime(2026, 8, 3, tzinfo=UTC)
    [window] = _windows(_build(tmp_path, feed), _AT_Z, start=monday, limit=1)
    assert window.pickup_booking is not None
    assert window.pickup_booking.latest == datetime(2026, 7, 31, 17, tzinfo=UTC)


def test_a_window_ending_before_start_is_skipped_in_year_one(
    tmp_path: Path,
) -> None:
    """Near the first datable instant no first reaching day can be placed
    by arithmetic, so the walk starts at the service's first day -- and
    still returns only windows that reach start."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,,\n",
        calendar="service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
        "sunday,start_date,end_date\nS,1,1,1,1,1,1,1,00010101,00010110\n",
    )
    start = datetime(1, 1, 3, 8, 30, tzinfo=UTC)
    windows = _windows(_build(tmp_path, feed), _AT_Z, start=start, limit=2)
    assert [w.service_date for w in windows] == [date(1, 1, 3), date(1, 1, 4)]


def test_a_blank_service_id_counts_calendar_days(tmp_path: Path) -> None:
    """A prior_notice_service_id of whitespace alone is blank: calendar
    days, so a Monday ride books by 17:00 on Sunday."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,P,\n",
        booking_rules="booking_rule_id,booking_type,prior_notice_last_day,"
        "prior_notice_last_time,prior_notice_service_id\nP,2,1,17:00:00, \n",
    )
    monday = datetime(2026, 8, 3, tzinfo=UTC)
    [window] = _windows(_build(tmp_path, feed), _AT_Z, start=monday, limit=1)
    assert window.pickup_booking is not None
    assert window.pickup_booking.latest == datetime(2026, 8, 2, 17, tzinfo=UTC)


def test_times_past_a_duration_read_unknown_not_a_crash(tmp_path: Path) -> None:
    """A booking time or a window end too large for a duration (garbage
    that still fits the column) reads unknown: the feed opens, the call is
    listed with no window and never dated, and the rule is unreadable."""
    feed = _zone_feed(
        "T,,Z,08:00:00,9000000000000:00:00,1,2,2,P,\nT,,Z,08:00:00,09:00:00,2,2,2,P,\n",
        booking_rules="booking_rule_id,booking_type,prior_notice_last_day,"
        "prior_notice_last_time\nP,2,1,99999999999:00:00\n",
    )

    def index() -> StaticIndex:
        return _build(tmp_path, feed)

    async def body(handle: TransitFeedHandle) -> list[object]:
        return [
            (s.stop_sequence, s.end_pickup_drop_off_window)
            for s in await handle.get_on_demand_services(_AT_Z)
        ]

    assert _run(index(), body) == [(1, None), (2, timedelta(hours=9))]
    [window] = _windows(index, _AT_Z, start=_JULY_30, limit=1)
    assert window.service.stop_sequence == 2
    rule = window.service.pickup_booking_rule
    assert rule is not None
    assert rule.unreadable_fields == ("prior_notice_last_time",)
    assert rule.prior_notice_last_time is None
    assert window.pickup_booking is None


def test_a_one_day_calendar_range_runs_that_day(tmp_path: Path) -> None:
    """calendar.txt with start_date == end_date runs on that one day."""
    feed = _zone_feed(
        "T,,Z,08:00:00,09:00:00,1,2,2,,\n",
        calendar="service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
        "sunday,start_date,end_date\nS,1,1,1,1,1,1,1,20260731,20260731\n",
    )
    windows = _windows(_build(tmp_path, feed), _AT_Z, start=_JULY_30, limit=3)
    assert [w.service_date for w in windows] == [date(2026, 7, 31)]


def test_bad_arguments_are_refused(tmp_path: Path) -> None:
    index = partial(_build, tmp_path, _zone_feed("T,,Z,08:00:00,09:00:00,1,2,2,,\n"))
    with pytest.raises(TypeError):
        _windows(index, _AT_Z, start=_JULY_30, limit=1, route_ids="R")
    with pytest.raises(ValueError, match="non-negative"):
        _windows(index, _AT_Z, start=_JULY_30, limit=-1)
    with pytest.raises(ValueError, match="needs a limit"):
        _windows(index, _AT_Z, start=_JULY_30)
    with pytest.raises(ValueError, match="timezone-aware"):
        _windows(index, _AT_Z, start=datetime(2026, 7, 30), limit=1)
    assert _windows(index, _AT_Z, start=_JULY_30, limit=0) == []
    assert (
        _windows(index, _AT_Z, start=_JULY_30, end=_JULY_30 - timedelta(seconds=1))
        == []
    )


# -- the oracle ---------------------------------------------------------------

_ZONES = ["UTC", "America/Chicago", "Australia/Lord_Howe", "Europe/London"]
_FIRST = date(2026, 3, 1)
_SPAN_DAYS = 45  # through the US, UK and Lord Howe DST changes
_WEEKDAY_COLUMNS = "monday,tuesday,wednesday,thursday,friday,saturday,sunday"


def _gtfs(day: date) -> str:
    return day.strftime("%Y%m%d")


def _clock(secs: int) -> str:
    return f"{secs // 3600:02d}:{secs // 60 % 60:02d}:{secs % 60:02d}"


@st.composite
def _flex_feeds(draw: st.DrawFn) -> dict[str, object]:
    """Services with random weekdays (none: calendar_dates alone), ranges
    and exceptions; Flex calls with random windows (some past midnight,
    some blank, some ending before they start); a random query."""
    zone_name = draw(st.sampled_from(_ZONES))
    services: dict[
        str, tuple[tuple[bool, ...], date, date, dict[date, frozenset[int]]]
    ] = {}
    for service_id in ("A", "B", "C")[: draw(st.integers(1, 3))]:
        mask = (
            tuple(draw(st.lists(st.booleans(), min_size=7, max_size=7)))
            if draw(st.booleans())
            else (False,) * 7  # calendar_dates alone
        )
        lo = _FIRST + timedelta(days=draw(st.integers(0, 20)))
        hi = lo + timedelta(days=draw(st.integers(0, _SPAN_DAYS)))
        # A date may be both ADDED and REMOVED: the removal wins.
        exceptions: dict[date, frozenset[int]] = {}
        for offset, kind in draw(
            st.lists(
                st.tuples(st.integers(0, _SPAN_DAYS + 5), st.sampled_from([1, 1, 2])),
                max_size=8,
            )
        ):
            day = _FIRST + timedelta(days=offset)
            exceptions[day] = exceptions.get(day, frozenset()) | {kind}
        services[service_id] = (mask, lo, hi, exceptions)
    calls = []
    for n in range(draw(st.integers(1, 5))):
        start_secs = draw(st.integers(0, 30 * 3600))
        end_secs = max(0, start_secs + draw(st.integers(-3 * 3600, 20 * 3600)))
        known = draw(st.booleans()) or n == 0
        calls.append(
            (
                f"T{n}",
                draw(st.sampled_from(sorted(services))),
                start_secs if known else None,
                end_secs if known else None,
            )
        )
    start = datetime.combine(
        _FIRST + timedelta(days=draw(st.integers(-2, _SPAN_DAYS - 10))),
        time(0),
        tzinfo=UTC,
    ) + timedelta(seconds=draw(st.integers(0, 86399)))
    end = draw(
        st.none()
        | st.integers(0, 20 * 86400).map(lambda secs: start + timedelta(seconds=secs))
    )
    limit = draw(st.integers(1, 12) if end is None else st.none() | st.integers(1, 12))
    return {
        "zone": zone_name,
        "services": services,
        "calls": calls,
        "start": start,
        "end": end,
        "limit": limit,
    }


def _feed_files(case: dict[str, object]) -> dict[str, str]:
    services = case["services"]
    calls = case["calls"]
    assert isinstance(services, dict)
    assert isinstance(calls, list)
    calendar = f"service_id,{_WEEKDAY_COLUMNS},start_date,end_date\n" + "".join(
        f"{sid},{','.join(str(int(on)) for on in mask)},{_gtfs(lo)},{_gtfs(hi)}\n"
        for sid, (mask, lo, hi, _) in services.items()
    )
    calendar_dates = "service_id,date,exception_type\n" + "".join(
        f"{sid},{_gtfs(day)},{kind}\n"
        for sid, (_, _, _, exceptions) in services.items()
        for day, kinds in exceptions.items()
        for kind in sorted(kinds)
    )
    trips = "route_id,service_id,trip_id\n" + "".join(
        f"R,{service_id},{trip_id}\n" for trip_id, service_id, _, _ in calls
    )
    stop_times = "".join(
        f"{trip_id},,Z,{'' if lo is None else _clock(lo)},"
        f"{'' if hi is None else _clock(hi)},1,2,2,,\n"
        for trip_id, _, lo, hi in calls
    )
    feed = _zone_feed(
        stop_times, calendar=calendar, calendar_dates=calendar_dates, trips=trips
    )
    feed["agency.txt"] = (
        "agency_id,agency_name,agency_url,agency_timezone\n"
        f"A,T,https://e.com,{case['zone']}\n"
    )
    return feed


def _runs(
    service: tuple[tuple[bool, ...], date, date, dict[date, frozenset[int]]],
    day: date,
) -> bool:
    """calendar.txt with calendar_dates.txt over it, a removal winning."""
    mask, lo, hi, exceptions = service
    kinds = exceptions.get(day, frozenset())
    if 2 in kinds:
        return False
    if 1 in kinds:
        return True
    return lo <= day <= hi and mask[day.weekday()]


def _expected(case: dict[str, object]) -> list[tuple[object, ...]]:
    """Every call on every day its service runs, the window overlapping
    [start, end], sorted and cut -- by brute force over the whole span."""
    tz = ZoneInfo(str(case["zone"]))
    services = case["services"]
    calls = case["calls"]
    start = case["start"]
    end = case["end"]
    limit = case["limit"]
    assert isinstance(services, dict)
    assert isinstance(calls, list)
    assert isinstance(start, datetime)
    found = []
    for trip_id, service_id, lo, hi in calls:
        if lo is None or hi < lo:
            continue  # unknown, or ending before it starts: never dated
        for offset in range(-3, 2 * _SPAN_DAYS + 10):
            day = _FIRST + timedelta(days=offset)
            if not _runs(services[service_id], day):
                continue
            day_start = datetime.combine(day, time(12), tzinfo=tz).astimezone(
                UTC
            ) - timedelta(hours=12)
            ws = day_start + timedelta(seconds=lo)
            we = day_start + timedelta(seconds=hi)
            if we >= start and (end is None or ws <= end):
                found.append((ws, trip_id, day, we))
    found.sort()
    cut = found if limit is None else found[: int(str(limit))]
    return [(trip_id, day, ws, we) for ws, trip_id, day, we in cut]


@given(case=_flex_feeds())
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_windows_are_exactly_the_brute_force_ones(
    tmp_path_factory: pytest.TempPathFactory, case: dict[str, object]
) -> None:
    """Against a brute-force oracle with its own calendar: every window
    whose service runs on its date and that overlaps [start, end] is
    there, nothing else is, in order, cut to the limit -- so open-ended
    equals bounded at the feed's end, and every window is one of
    get_on_demand_services's calls."""
    index = _build(tmp_path_factory.mktemp("feed"), _feed_files(case))

    async def body(
        handle: TransitFeedHandle,
    ) -> tuple[list[OnDemandWindow], list[object]]:
        windows = await handle.get_on_demand_windows(
            _AT_Z,
            start=case["start"],  # type: ignore[arg-type]
            end=case["end"],  # type: ignore[arg-type]
            limit=case["limit"],  # type: ignore[arg-type]
        )
        return windows, list(await handle.get_on_demand_services(_AT_Z))

    windows, services = _run(index, body)
    assert _spans(windows) == _expected(case)
    assert all(window.service in services for window in windows)


@given(case=_flex_feeds(), days=st.integers(0, 3), more=st.integers(1, 3))
@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_booking_is_placed_on_each_window(
    tmp_path_factory: pytest.TempPathFactory,
    case: dict[str, object],
    days: int,
    more: int,
) -> None:
    """Each window's pickup booking: by 17:00 on the ``days``-th day
    service A runs before the service date, from 08:00 on the
    ``days + more``-th -- counted by hand on the oracle's calendar, a year
    back at most, and unknown for a deadline after the window ends. Its
    drop-off booking: same-day, from 2 hours before the window's start to
    30 minutes before its end."""
    feed = _feed_files(case)
    feed["booking_rules.txt"] = (
        "booking_rule_id,booking_type,prior_notice_duration_min,"
        "prior_notice_duration_max,prior_notice_last_day,prior_notice_last_time,"
        "prior_notice_start_day,prior_notice_start_time,prior_notice_service_id\n"
        f"P,2,,,{days},17:00:00,{days + more},08:00:00,A\n"
        "D,1,30,120,,,,,\n"
    )
    feed["stop_times.txt"] = feed["stop_times.txt"].replace(
        ",1,2,2,,\n", ",1,2,2,P,D\n"
    )
    index = _build(tmp_path_factory.mktemp("feed"), feed)
    services = case["services"]
    assert isinstance(services, dict)
    tz = ZoneInfo(str(case["zone"]))

    def day_start(day: date) -> datetime:
        return datetime.combine(day, time(12), tzinfo=tz).astimezone(UTC) - timedelta(
            hours=12
        )

    def nth_running_day(service_date: date, n: int) -> date | None:
        if n == 0:
            return service_date
        running = [
            day
            for back in range(1, 367)
            if _runs(services["A"], day := service_date - timedelta(days=back))
        ]
        return running[n - 1] if len(running) >= n else None

    windows = _windows(
        index, _AT_Z, start=case["start"], end=case["end"], limit=case["limit"] or 5
    )
    for window in windows:
        drop_off = window.drop_off_booking
        assert drop_off is not None
        assert drop_off.rule.booking_type is BookingType.SAME_DAY
        assert (drop_off.earliest, drop_off.latest) == (
            window.window_start - timedelta(hours=2),
            window.window_end - timedelta(minutes=30),
        )
        last = nth_running_day(window.service_date, days)
        first = nth_running_day(window.service_date, days + more)
        booking = window.pickup_booking
        if (
            last is None
            or first is None
            or day_start(last) + timedelta(hours=17) > window.window_end
        ):
            assert booking is None
            continue
        assert booking is not None
        assert booking.rule.booking_type is BookingType.PRIOR_DAYS
        assert (booking.earliest, booking.latest) == (
            day_start(first) + timedelta(hours=8),
            day_start(last) + timedelta(hours=17),
        )
