"""Runs that reach days past their service day, and runs no date can carry.

GTFS times are offsets from the service day's start and may pass 24:00:00
-- or 48:00:00, for a multi-day trip -- so the window scan must look back as
far as the feed's latest time reaches (plus an hour of DST). A run dated on
the last representable day (a "forever" calendar's 9999-12-31) can't be
dated at all, and must be skipped rather than overflow.
"""

import io
import zipfile
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds import transit as transit_module
from aiomobilitydatabase.feeds.models import ArrivalsQuery, StopArrival, TripQuery
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.test_properties import _index_from_zip_bytes
from tests.feeds.test_time_window_properties import _run


def _feed(
    stop_times: str,
    *,
    timezone: str = "UTC",
    calendar: str = "DAILY,1,1,1,1,1,1,1,20260101,20261231",
    trips: str = "R,DAILY,T\n",
) -> bytes:
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n"
        f"A,A,https://a.example,{timezone}\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nA,A,0,0\nB,B,0,0\nX,X,0,0\n",
        "routes.txt": "route_id,agency_id,route_short_name,route_type\nR,A,R,3\n",
        "trips.txt": "route_id,service_id,trip_id\n" + trips,
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        + stop_times,
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,"
        "saturday,sunday,start_date,end_date\n" + calendar + "\n",
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def _times(rows: Sequence[StopArrival]) -> list[datetime | None]:
    return [row.scheduled_departure for row in rows]


def test_a_multi_day_trips_late_calls_are_read() -> None:
    """A at 00:00, B at 60:00:00: the runs of two and three days ago reach B
    inside a 48h window, with or without realtime naming them.
    """
    index = _index_from_zip_bytes(
        _feed("T,00:00:00,00:00:00,A,1\nT,60:00:00,60:00:00,B,2\n")
    )
    start = datetime(2026, 6, 10, tzinfo=UTC)
    end = start + timedelta(hours=48)
    expected = [
        datetime(2026, 6, 10, 12, tzinfo=UTC),
        datetime(2026, 6, 11, 12, tzinfo=UTC),
    ]

    async def body(handle: TransitFeedHandle) -> None:
        [scheduled] = await handle.get_scheduled_departures(
            [ArrivalsQuery(["B"], limit=None)], start=start, end=end
        )
        [arrivals] = await handle.get_arrivals(
            [ArrivalsQuery(["B"], limit=None)], start=start, end=end
        )
        assert _times(scheduled) == expected
        assert _times(arrivals) == expected

    _run(index, body)


def test_a_night_run_across_spring_forward_is_read() -> None:
    """Saturday's run calls at 47:30:00 -- Monday 00:30 EDT, after a 23-hour
    Sunday -- two days past its service day, though under 48 hours.
    """
    index = _index_from_zip_bytes(
        _feed(
            "N,47:30:00,47:30:00,X,1\n",
            timezone="America/New_York",
            calendar="SAT,0,0,0,0,0,1,0,20260101,20261231",
            trips="R,SAT,N\n",
        )
    )
    start = datetime(2026, 3, 9, 4, tzinfo=UTC)  # Monday 00:00 EDT

    async def body(handle: TransitFeedHandle) -> None:
        for board in (
            await handle.get_scheduled_departures(
                [ArrivalsQuery(["X"], limit=1)], start=start
            ),
            await handle.get_arrivals([ArrivalsQuery(["X"], limit=1)], start=start),
        ):
            assert _times(board[0]) == [datetime(2026, 3, 9, 4, 30, tzinfo=UTC)]

    _run(index, body)


def test_realtime_dated_on_the_last_representable_day_is_skipped() -> None:
    """A "forever" calendar (99991231) in Los Angeles: a TripUpdate and a
    detour dated 9999-12-31 name a run whose 20:00 call falls in year 10000,
    which no datetime can carry -- skipped, not an OverflowError for the
    whole batch.
    """
    index = _index_from_zip_bytes(
        _feed(
            "T,00:00:00,00:00:00,A,1\nT,20:00:00,20:00:00,B,2\n",
            timezone="America/Los_Angeles",
            calendar="DAILY,1,1,1,1,1,1,1,20260101,99991231",
        )
    )
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    update = message.entity.add()
    update.id = "tu"
    update.trip_update.trip.trip_id = "T"
    update.trip_update.trip.start_date = "99991231"
    update.trip_update.delay = 60
    detour = message.entity.add()
    detour.id = "mod"
    detour.trip_modifications.selected_trips.add().trip_ids.append("T")
    detour.trip_modifications.service_dates.append("99991231")
    modification = detour.trip_modifications.modifications.add()
    modification.start_stop_selector.stop_sequence = 2
    modification.end_stop_selector.stop_sequence = 2
    start = datetime(2026, 6, 10, tzinfo=UTC)

    async def body(handle: TransitFeedHandle) -> None:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["A", "B"], limit=2)], start=start
        )
        [trips] = await handle.get_upcoming_trips(
            [TripQuery("A", "B", limit=2)], start=start
        )
        assert len(board) == 2
        assert len(trips) == 2

    _run(index, body, message)


def _detour_message(trip_id: str, days: int) -> gtfs_realtime_pb2.FeedMessage:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "mod"
    entity.trip_modifications.selected_trips.add().trip_ids.append(trip_id)
    for offset in range(days):
        day = date(2026, 6, 10) + timedelta(days=offset)
        entity.trip_modifications.service_dates.append(day.strftime("%Y%m%d"))
    modification = entity.trip_modifications.modifications.add()
    modification.start_stop_selector.stop_sequence = 1
    modification.end_stop_selector.stop_sequence = 1
    modification.propagated_modification_delay = 600
    return message


def test_a_detour_is_rebuilt_once_and_only_where_it_can_show() -> None:
    """Cost, not just correctness: a week of service dates rebuilds the run
    once (then shifts it to each day), and a detour on a trip that serves
    none of the queried stops isn't rebuilt at all.
    """
    feed = _feed(
        "T,08:00:00,08:00:00,A,1\nT,08:10:00,08:10:00,B,2\n"
        "U,09:00:00,09:00:00,X,1\nU,09:10:00,09:10:00,X,2\n",
        trips="R,DAILY,T\nR,DAILY,U\n",
    )
    rebuilds: list[str] = []
    original = transit_module._modified_calls

    def counting(instance, *args, **kwargs):  # type: ignore[no-untyped-def]
        rebuilds.append(instance.trip_id)
        return original(instance, *args, **kwargs)

    start = datetime(2026, 6, 10, tzinfo=UTC)

    async def body(handle: TransitFeedHandle) -> None:
        transit_module._modified_calls = counting  # type: ignore[assignment]
        try:
            [board] = await handle.get_arrivals(
                [ArrivalsQuery(["B"], limit=None)],
                start=start,
                end=start + timedelta(days=7),
            )
        finally:
            transit_module._modified_calls = original
        # Seven days of T, each shifted +10 minutes by the detour.
        assert _times(board) == [
            datetime(2026, 6, 10 + offset, 8, 20, tzinfo=UTC) for offset in range(7)
        ]
        assert rebuilds == ["T"]

    message = _detour_message("T", days=7)
    _run(_index_from_zip_bytes(feed), body, message)

    rebuilds.clear()
    other = _detour_message("U", days=7)

    async def unrelated(handle: TransitFeedHandle) -> None:
        transit_module._modified_calls = counting  # type: ignore[assignment]
        try:
            [board] = await handle.get_arrivals(
                [ArrivalsQuery(["B"], limit=1)], start=start
            )
        finally:
            transit_module._modified_calls = original
        assert _times(board) == [datetime(2026, 6, 10, 8, 10, tzinfo=UTC)]
        assert rebuilds == []  # U never serves B

    _run(_index_from_zip_bytes(feed), unrelated, other)


def test_a_repeated_stop_sequence_keeps_both_rows_on_every_path() -> None:
    """An (invalid) feed repeating a stop_sequence within a trip keeps both
    rows -- whether the run is read up front (realtime names it) or by the
    scan -- so the answer can't depend on which path read it.
    """
    feed = _feed(
        "T,00:00:00,00:00:00,A,0\nT,08:00:00,08:00:00,A,0\nT,09:00:00,09:00:00,B,1\n"
    )
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "tu"
    entity.trip_update.trip.trip_id = "T"
    entity.trip_update.trip.start_date = "20260611"
    entity.trip_update.delay = 0
    start = datetime(2026, 6, 10, 23, tzinfo=UTC)
    end = start + timedelta(hours=12)
    expected = [
        datetime(2026, 6, 11, 0, tzinfo=UTC),
        datetime(2026, 6, 11, 8, tzinfo=UTC),
    ]

    async def body(handle: TransitFeedHandle) -> None:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["A"], limit=None)], start=start, end=end
        )
        assert _times(board) == expected

    _run(_index_from_zip_bytes(feed), body, message)
    _run(_index_from_zip_bytes(feed), body)
