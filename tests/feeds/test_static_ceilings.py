"""Per-query scan ceilings, canonical calendar dates, and current runs."""

import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

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
    at = datetime(2026, 7, 30, 7, tzinfo=UTC)
    assert index.rt_instances([], at) == []
    assert index.instance_departures([], ["S1"]) == []
    assert index.instance_departures([("T1", date(2026, 7, 30))], []) == []
    assert index.instance_trips([], "S1", "S2") == []
    assert index.instance_trip_calls([]) == []
    # A trip with calls, addressed on a day its service runs and one it doesn't.
    assert index.rt_instances(
        [("T1", date(2026, 7, 30), None), ("T1", date(2027, 1, 1), None)], at
    ) == [(("T1", date(2026, 7, 30)), ("T1", date(2026, 7, 30), None))]
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
    assert index.rt_instances([("T1", None, None)], at) == []


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
    # After today's run finished, there is no current run -- not tomorrow's.
    later = datetime(2026, 7, 30, 9, tzinfo=UTC)
    assert index.current_service_dates(["T1"], later) == {}


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


def test_a_stop_time_no_date_can_carry_is_still_under_way(tmp_path: Path) -> None:
    """A garbage stop time (300000:00:00, decades past its day) on service
    that only runs in year 9999: the first run's last call falls past the
    end of datetime, so that run is still under way -- found, not an
    OverflowError.
    """
    index = _index(
        tmp_path,
        calendar="ALL,1,1,1,1,1,1,1,99990101,99991231\n",
        stop_times="T1,08:00:00,08:00:00,S1,1\nT1,300000:00:00,300000:00:00,S2,2\n",
    )
    at = datetime(9999, 11, 1, tzinfo=UTC)
    assert index.current_service_dates(["T1"], at) == {"T1": date(9999, 1, 1)}
