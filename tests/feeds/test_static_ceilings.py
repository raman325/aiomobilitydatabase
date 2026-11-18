"""Per-query scan ceilings, canonical calendar dates, and current runs."""

import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.models import ArrivalsQuery
from aiomobilitydatabase.feeds.static_index import StaticIndex
from aiomobilitydatabase.feeds.transit import TransitFeedHandle, _DirectUrls

_CALENDAR_HEADER = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
    "start_date,end_date\n"
)


def _index(
    tmp_path: Path,
    *,
    calendar: str,
    stop_times: str,
    trips: str = "R1,ALL,T1,Downtown\nR1,ALL,T2,Uptown\n",
    calendar_dates: str = "",
    timezone: str = "UTC",
) -> StaticIndex:
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n"
        f"A,A,https://a.example,{timezone}\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,One,1,1\nS2,Two,1,1\n",
        "routes.txt": "route_id,route_short_name,route_type\nR1,1,3\n",
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\n" + trips,
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        + stop_times,
        "calendar.txt": _CALENDAR_HEADER + calendar,
        "calendar_dates.txt": "service_id,date,exception_type\n" + calendar_dates,
    }
    path = tmp_path / "feed.zip"
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return StaticIndex.build(path, ":memory:", "ds", None)


_TWO_STOPS = "T1,08:00:00,08:00:00,S1,1\nT1,08:10:00,08:10:00,S2,2\n"


def test_departure_ceiling_follows_the_serving_services_only(tmp_path: Path) -> None:
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,20260101,20261231\n"
        "SUMMER,1,1,1,1,1,1,1,20260601,20260831\n",
        trips="R1,ALL,T1,Downtown\nR1,SUMMER,T2,Beach\n",
        stop_times=_TWO_STOPS
        + "T2,09:00:00,09:00:00,S1,1\nT2,09:30:00,09:30:00,S2,2\n",
    )
    year_end = datetime(2026, 12, 31, 8, tzinfo=UTC)
    summer_end = datetime(2026, 8, 31, 9, tzinfo=UTC)
    assert index.departures_ceiling(["S1"]) == year_end
    # A headsign served only by the seasonal trip ends with its season.
    assert index.departures_ceiling(["S1"], headsigns=["Beach"]) == summer_end
    assert index.departures_ceiling(["S1"], route_ids=["R1"]) == year_end
    # Nothing matches: nothing to scan for.
    assert index.departures_ceiling(["S1"], headsigns=["Nowhere"]) is None
    assert index.departures_ceiling(["S1"], route_ids=["R9"]) is None
    assert index.departures_ceiling([]) is None
    # Journeys need the right direction.
    assert index.journeys_ceiling("S1", "S2") == year_end
    assert index.journeys_ceiling("S2", "S1") is None


def test_ceilings_ignore_services_that_can_never_run(tmp_path: Path) -> None:
    index = _index(
        tmp_path,
        # No weekday set, and start after end: neither can ever be active.
        calendar="ALL,0,0,0,0,0,0,0,20260101,20991231\n"
        "BACKWARDS,1,1,1,1,1,1,1,20270101,20260101\n",
        trips="R1,ALL,T1,Downtown\nR1,BACKWARDS,T2,Uptown\n",
        stop_times=_TWO_STOPS + "T2,09:00:00,09:00:00,S1,1\n",
        # ...but an added date makes ALL run once.
        calendar_dates="ALL,20260315,1\n",
    )
    assert index.departures_ceiling(["S1"]) == datetime(2026, 3, 15, 8, tzinfo=UTC)


def test_a_calendar_running_forever_does_not_overflow(tmp_path: Path) -> None:
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,20260101,99991231\n",
        # 25:00 on the last day would land past the end of datetime.
        stop_times="T1,25:00:00,25:00:00,S1,1\n",
    )
    ceiling = index.departures_ceiling(["S1"])
    assert ceiling is not None
    assert ceiling.year == 9999


def test_unparseable_calendar_dates_are_never_active(tmp_path: Path) -> None:
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,20260101,2026-12-31\n",
        stop_times=_TWO_STOPS,
        calendar_dates="ALL,2026-07-04,1\n",
    )
    assert index.active_service_ids(date(2026, 7, 1)) == set()
    assert index.active_service_ids(date(2026, 7, 4)) == set()
    assert index.departures_ceiling(["S1"]) is None


def test_realtime_index_helpers_accept_empty_input(tmp_path: Path) -> None:
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,20260101,20261231\n",
        stop_times=_TWO_STOPS,
    )
    assert index.rt_instances([]) == []
    assert index.instance_departures([], ["S1"]) == []
    assert index.instance_departures([("T1", date(2026, 7, 30))], []) == []
    assert index.instance_trips([], "S1", "S2") == []
    assert index.instance_trip_calls([]) == []
    # A trip with calls, addressed on a day its service runs and one it doesn't.
    assert index.rt_instances(
        [("T1", date(2026, 7, 30), None), ("T1", date(2027, 1, 1), None)]
    ) == [(("T1", date(2026, 7, 30)), ("T1", date(2026, 7, 30), None))]
    # A key without a date addresses nothing: matching dates every key first.
    assert index.rt_instances([("T1", None, None)]) == []
    # A run on a day its service doesn't operate doesn't exist.
    gone = [("T1", date(2027, 1, 1))]
    assert index.instance_departures(gone, ["S1"]) == []
    assert index.instance_trips(gone, "S1", "S2") == []
    assert index.instance_trip_calls(gone) == []
    [run] = index.instance_trip_calls([("T1", date(2026, 7, 30))])
    assert [call.stop_id for call in run.calls] == ["S1", "S2"]


def test_a_garbage_feed_time_has_no_current_run(tmp_path: Path) -> None:
    """A representable but absurd feed timestamp (year 9999) east of UTC
    would overflow local-date arithmetic; it simply means no current run.
    """
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,20260101,99991231\n",
        stop_times=_TWO_STOPS,
        timezone="Australia/Sydney",
    )
    at = datetime(9999, 12, 31, 23, tzinfo=UTC)
    assert index.current_service_dates(["T1"], at) == {}


def test_current_service_dates_edges(tmp_path: Path) -> None:
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,20260101,20261231\n",
        # T2 has no timed stop at all: it can have no current run.
        stop_times=_TWO_STOPS + "T2,,,S1,1\n",
    )
    at = datetime(2026, 7, 30, 7, tzinfo=UTC)
    assert index.current_service_dates([], at) == {}
    assert index.current_service_dates(["T1", "T2"], at) == {"T1": date(2026, 7, 30)}
    # After today's run finished it is still the nearest run up to today:
    # a date-less update never moves onto tomorrow's.
    later = datetime(2026, 7, 30, 23, tzinfo=UTC)
    assert index.current_service_dates(["T1"], later) == {"T1": date(2026, 7, 30)}


@pytest.mark.parametrize("end_date", ["20991231", "99991231"])
async def test_handles_answer_promptly_for_calendars_running_forever(
    tmp_path: Path, end_date: str
) -> None:
    index = _index(
        tmp_path,
        calendar=f"ALL,1,1,1,1,1,1,1,20260101,{end_date}\n",
        stop_times=_TWO_STOPS,
    )
    start = datetime(2026, 7, 30, 9, tzinfo=UTC)
    async with MobilityFeedsClient() as client:
        handle = TransitFeedHandle(
            client,
            None,
            [],
            index,
            None,
            direct=_DirectUrls(static_url="https://e.com/g.zip", headers=None),
        )
        try:
            open_ended, empty, unmatched = await handle.get_scheduled_departures(
                [
                    ArrivalsQuery(["S1"], limit=2),
                    ArrivalsQuery([], limit=2),
                    ArrivalsQuery(["S1"], headsigns=["Nowhere"], limit=2),
                ],
                start=start,
            )
            assert [row.scheduled_departure for row in open_ended] == [
                datetime(2026, 7, 31, 8, tzinfo=UTC),
                datetime(2026, 8, 1, 8, tzinfo=UTC),
            ]
            assert empty == unmatched == []
            # An explicit end at the very end of datetime is fine too.
            [far] = await handle.get_arrivals(
                [ArrivalsQuery(["S1"], limit=1)],
                start=start,
                end=datetime.max.replace(tzinfo=UTC) - timedelta(days=1),
            )
            assert [row.trip_id for row in far] == ["T1"]
        finally:
            handle.close()


_WEEK = "ALL,1,1,1,1,1,1,1,20260105,20260111\n"
_WEEK_DAYS = [date(2026, 1, 5) + timedelta(days=k) for k in range(7)]


def _counting_reads(index: StaticIndex) -> tuple[list[date], object]:
    """Record each service day the index reads, through active_service_ids."""
    read: list[date] = []
    active = index.active_service_ids

    def counted(service_date: date) -> set[str]:
        read.append(service_date)
        return active(service_date)

    return read, patch.object(index, "active_service_ids", counted)


@settings(max_examples=40, deadline=None)
@given(
    before=st.integers(min_value=0, max_value=730_000),
    after=st.integers(min_value=0, max_value=2_000_000),
)
@example(before=739_000, after=0)  # from 0001-01-03, as close as start may be
@example(before=0, after=2_914_000)  # to 9999-11-30
def test_a_window_reaching_past_the_feed_reads_only_the_feed_s_days(
    tmp_path_factory: pytest.TempPathFactory, before: int, after: int
) -> None:
    """A week of service read through a window opening ``before`` days ahead
    of it and closing ``after`` days past it: every run in the week, and
    only the week's days (and a day of margin each side) read -- never one
    per day of the window."""
    index = _index(
        tmp_path_factory.mktemp("feed"), calendar=_WEEK, stop_times=_TWO_STOPS
    )
    try:
        # Clamped to the range a window may span (see _utc_window).
        opens = datetime.combine(
            date.fromordinal(max(_WEEK_DAYS[0].toordinal() - before, 3)),
            datetime.min.time(),
            UTC,
        )
        closes = datetime.combine(
            date.fromordinal(
                min(
                    _WEEK_DAYS[-1].toordinal() + after + 1,
                    date(9999, 11, 30).toordinal(),
                )
            ),
            datetime.min.time(),
            UTC,
        )
        read, counting = _counting_reads(index)
        with counting:
            got = index.upcoming_departures(["S1"], None, opens, closes - opens)
        assert [dep.departure for dep in got] == [
            datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=8)
            for day in _WEEK_DAYS
        ]
        assert len(read) <= len(_WEEK_DAYS) + 2
    finally:
        index.close()


def test_a_feed_whose_service_never_runs_reads_no_day(tmp_path: Path) -> None:
    """No weekday set: no day has a service to run, so a window across all
    of datetime's range reads none of them."""
    index = _index(
        tmp_path,
        calendar="ALL,0,0,0,0,0,0,0,20260101,20261231\n",
        stop_times=_TWO_STOPS,
    )
    try:
        opens = datetime(1, 1, 3, tzinfo=UTC)
        read, counting = _counting_reads(index)
        with counting:
            got = index.upcoming_departures(
                ["S1"], None, opens, datetime(9999, 11, 30, tzinfo=UTC) - opens
            )
        assert (got, read) == ([], [])
    finally:
        index.close()


async def test_a_board_opening_long_before_the_feed_starts_reads_its_days_only(
    tmp_path: Path,
) -> None:
    """Hegelator 1678: a start at 0001-01-03 before a feed starting in 2026
    once read every service day between them, one by one."""
    index = _index(tmp_path, calendar=_WEEK, stop_times=_TWO_STOPS)
    read, counting = _counting_reads(index)
    async with MobilityFeedsClient() as client:
        handle = TransitFeedHandle(
            client,
            None,
            [],
            index,
            None,
            direct=_DirectUrls(static_url="https://e.com/g.zip", headers=None),
        )
        try:
            with counting:
                [board] = await handle.get_scheduled_departures(
                    [ArrivalsQuery(["S1"], limit=2)],
                    start=datetime(1, 1, 3, tzinfo=UTC),
                )
            assert [row.scheduled_departure for row in board] == [
                datetime(2026, 1, 5, 8, tzinfo=UTC),
                datetime(2026, 1, 6, 8, tzinfo=UTC),
            ]
            assert len(read) < 100
        finally:
            handle.close()


def test_detour_index_helpers_accept_empty_input(tmp_path: Path) -> None:
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,20260101,20261231\n",
        stop_times=_TWO_STOPS,
    )
    assert index.trips_serving([], ["S1"]) == set()
    assert index.trips_serving(["T1"], []) == set()
    assert index.trips_serving(["T1", "T2"], ["S2"]) == {"T1"}
    assert index.run_days([]) == []


def test_a_trip_longer_than_a_day_names_no_dateless_run(tmp_path: Path) -> None:
    """GTFS-RT requires start_date for a trip lasting more than 24 hours:
    without one, a date-less update names none of its runs -- even a
    garbage-length one whose last call falls past the end of datetime.
    """
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,99990101,99990101\n",
        stop_times="T1,08:00:00,08:00:00,S1,1\nT1,300000:00:00,300000:00:00,S2,2\n",
    )
    at = datetime(9999, 11, 1, tzinfo=UTC)
    assert index.current_service_dates(["T1"], at) == {}


def test_a_run_whose_day_cannot_be_dated_is_no_current_run(tmp_path: Path) -> None:
    """Tokyo, service only on 0001-01-01: that day's start lies before
    datetime itself, so its run can't be placed -- no current run, no error.
    """
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,00010101,00010101\n",
        stop_times=_TWO_STOPS,
        timezone="Asia/Tokyo",
    )
    at = datetime(1, 1, 5, tzinfo=UTC)
    assert index.current_service_dates(["T1"], at) == {}
