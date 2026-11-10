"""Open-ended dated queries, batch trip queries, and vehicle parity.

Fixture calendar (see tests/feeds/data/gtfs): at S1, WKDY runs T1 08:00:30
and T2 08:30:30 on weekdays, NIGHT runs T3 at 25:31:00 (01:31 the next
morning) every day, SPECIAL runs T4 09:00:30 only on 2026-07-04 and
2026-07-31, and WKDY is removed on 2026-07-03. The feed's agency runs on
America/Los_Angeles (PDT = UTC-7 in July/August).
"""

from datetime import UTC, datetime, timedelta

import pytest
from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.geo import Circle
from aiomobilitydatabase.feeds.models import ArrivalsQuery, StopArrival, TripQuery

from tests.feeds.fixtures import (
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    TRIP_UPDATES_BASELINE,
    VEHICLE_POSITIONS,
    build_gtfs_zip_bytes,
    build_trip_query_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

PB = "application/octet-stream"
PDT = timedelta(hours=-7)


def _pdt(month: int, day: int, hour: int, minute: int, sec: int = 0) -> datetime:
    """A 2026 Pacific Daylight Time wall-clock instant, as aware UTC."""
    return datetime(2026, month, day, hour, minute, sec, tzinfo=UTC) - PDT


def _mock_catalog(
    mock_api: MockApi, *, rt: bool, trip_query_feed: bool = False
) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    rt_feeds = [with_base(GTFS_RT_FEED, base)] if rt else []
    mock_api.get("/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=rt_feeds)
    zip_bytes = (
        build_trip_query_gtfs_zip_bytes() if trip_query_feed else build_gtfs_zip_bytes()
    )
    mock_api.get("/hosted/mdb-100.zip", body=zip_bytes, content_type="application/zip")


def _bundled_message() -> bytes:
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(TRIP_UPDATES_BASELINE)
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.ParseFromString(VEHICLE_POSITIONS)
    message.entity.extend(vehicles.entity)
    return message.SerializeToString()


def _times(board: list[StopArrival]) -> list[tuple[str | None, datetime | None]]:
    return [(row.trip_id, row.scheduled_departure) for row in board]


async def test_overnight_board_fills_from_the_next_service_day(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    start = _pdt(7, 30, 9, 0)  # Thursday, after T2
    expected = [
        ("T3", _pdt(7, 31, 1, 31)),  # Thursday's NIGHT service
        ("T1", _pdt(7, 31, 8, 0, 30)),
        ("T2", _pdt(7, 31, 8, 30, 30)),
    ]
    [scheduled] = await handle.get_scheduled_departures(
        [ArrivalsQuery(["S1"], limit=3)], start=start
    )
    assert _times(scheduled) == expected
    # The realtime family answers the same with no realtime sources.
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"], limit=3)], start=start
    )
    assert _times(arrivals) == expected


async def test_weekend_gap_is_crossed(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [board] = await handle.get_scheduled_departures(
        [ArrivalsQuery(["S1"], limit=4)], start=_pdt(7, 31, 9, 30)
    )
    assert _times(board) == [
        ("T3", _pdt(8, 1, 1, 31)),
        ("T3", _pdt(8, 2, 1, 31)),
        ("T3", _pdt(8, 3, 1, 31)),
        ("T1", _pdt(8, 3, 8, 0, 30)),  # Monday: weekday service again
    ]


async def test_removed_service_day_is_skipped(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [board] = await handle.get_scheduled_departures(
        [ArrivalsQuery(["S1"], limit=3)], start=_pdt(7, 2, 9, 0)
    )
    # WKDY is removed on Friday 2026-07-03, so no T1/T2 that day; the
    # SPECIAL T4 runs on Saturday 2026-07-04.
    assert _times(board) == [
        ("T3", _pdt(7, 3, 1, 31)),
        ("T3", _pdt(7, 4, 1, 31)),
        ("T4", _pdt(7, 4, 9, 0, 30)),
    ]


async def test_open_ended_scan_stops_at_the_end_of_service(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    start = _pdt(7, 30, 9, 0)
    # A stop nothing serves scans to the ceiling and comes back empty.
    assert await handle.get_scheduled_departures(
        [ArrivalsQuery(["NOT-A-STOP"])], start=start
    ) == [[]]
    assert await handle.get_arrivals([ArrivalsQuery(["NOT-A-STOP"])], start=start) == [
        []
    ]
    # After the calendars end (2027-12-31) there is nothing left.
    after = datetime(2028, 6, 1, tzinfo=UTC)
    assert await handle.get_scheduled_departures(
        [ArrivalsQuery(["S1"])], start=after
    ) == [[]]
    # The last departure before the ceiling is still found.
    # (Pacific STANDARD time in January: 01:31 PST is 09:31 UTC.)
    [last] = await handle.get_scheduled_departures(
        [ArrivalsQuery(["S1"], limit=None)],
        start=datetime(2027, 12, 31, 20, 0, tzinfo=UTC),
        end=datetime(2028, 1, 2, tzinfo=UTC),
    )
    assert _times(last) == [("T3", datetime(2028, 1, 1, 9, 31, tzinfo=UTC))]


async def test_open_ended_queries_need_a_limit(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    unlimited = [ArrivalsQuery(["S1"]), ArrivalsQuery(["S2"], limit=None)]
    with pytest.raises(ValueError, match="open-ended"):
        await handle.get_arrivals(unlimited)
    with pytest.raises(ValueError, match="open-ended"):
        await handle.get_scheduled_departures(unlimited)
    with pytest.raises(ValueError, match="open-ended"):
        await handle.get_upcoming_trips([TripQuery("S1", "S2", limit=None)])
    # With an end, an unlimited query is fine.
    start = _pdt(7, 30, 7, 0)
    [_, everything] = await handle.get_scheduled_departures(
        unlimited, start=start, end=start + timedelta(days=1)
    )
    assert [row.trip_id for row in everything] == ["T1"]


async def test_an_empty_window_returns_empty_boards(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    start = _pdt(7, 30, 7, 0)
    for end in (start, start - timedelta(minutes=1)):
        assert await handle.get_arrivals(
            [ArrivalsQuery(["S1"])], start=start, end=end
        ) == [[]]
        assert await handle.get_scheduled_departures(
            [ArrivalsQuery(["S1"])], start=start, end=end
        ) == [[]]
        assert await handle.get_upcoming_trips(
            [TripQuery("S1", "S2")], start=start, end=end
        ) == [[]]


async def test_an_early_running_trip_beyond_the_first_horizon_is_caught(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The early-running margin keeps the open-ended board exact.

    The first horizon is start + 2h = 08:10:30, which holds only T1
    (08:00:30). T2 is scheduled at 08:30:30, outside that horizon, but
    runs 40 minutes early -- effective 07:50:30, ahead of T1. Settling on
    the first horizon would return T1; the board must say T2.
    """
    _mock_catalog(mock_api, rt=True)
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "t2-early"
    entity.trip_update.trip.trip_id = "T2"
    entity.trip_update.delay = -40 * 60
    mock_api.get("/rt/all", body=message.SerializeToString(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [board] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"], limit=1)], start=_pdt(7, 30, 6, 10, 30)
    )
    assert [(row.trip_id, row.predicted_departure) for row in board] == [
        ("T2", _pdt(7, 30, 7, 50, 30))
    ]


async def test_scheduled_departures_never_touch_the_network(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    # Realtime sources are configured but /rt/all is NOT scripted: any
    # realtime request would fail the call.
    _mock_catalog(mock_api, rt=True)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [board] = await handle.get_scheduled_departures(
        [ArrivalsQuery(["S1"], limit=2)], start=_pdt(7, 30, 7, 0)
    )
    assert [row.trip_id for row in board] == ["T1", "T2"]
    assert not any(request.path == "/rt/all" for request in mock_api.requests)


async def test_get_vehicles_filters_by_zone(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    for _ in range(2):
        mock_api.get("/rt/all", body=_bundled_message(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    everywhere = await handle.get_vehicles()
    near_v1 = await handle.get_vehicles(
        zone=Circle(latitude=34.055, longitude=-118.245, radius_m=50)
    )
    assert len(everywhere) > 1
    assert [vehicle.vehicle_id for vehicle in near_v1] == ["V1"]


async def test_upcoming_trips_open_ended_across_the_weekend(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False, trip_query_feed=True)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [trips] = await handle.get_upcoming_trips(
        [TripQuery("S1", "S3", limit=2)], start=_pdt(7, 31, 9, 0)
    )
    assert [(trip.trip_id, trip.scheduled_departure) for trip in trips] == [
        ("T1", _pdt(8, 3, 8, 0, 30)),
        ("T1", _pdt(8, 4, 8, 0, 30)),
    ]


async def test_upcoming_trip_batch_fetches_realtime_once_and_attaches_vehicles(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True, trip_query_feed=True)
    for _ in range(4):  # spares: a per-query fetch would record more requests
        mock_api.get("/rt/all", body=_bundled_message(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    start = _pdt(7, 30, 7, 45)
    s1_s3, s1_s2 = await handle.get_upcoming_trips(
        [TripQuery("S1", "S3", limit=1), TripQuery("S1", "S2", limit=1)],
        start=start,
        end=start + timedelta(hours=2),
        with_vehicles=True,
    )
    assert sum(1 for r in mock_api.requests if r.path == "/rt/all") == 1
    assert (
        [trip.trip_id for trip in s1_s3] == [trip.trip_id for trip in s1_s2] == ["T1"]
    )
    # T1's TripUpdate names V1, whose position is in the same message.
    for [trip] in (s1_s3, s1_s2):
        assert trip.vehicle_id == "V1"
        assert trip.vehicle is not None
        assert trip.vehicle.vehicle_id == "V1"


async def test_upcoming_trip_vehicles_need_the_flag(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True, trip_query_feed=True)
    mock_api.get("/rt/all", body=_bundled_message(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    start = _pdt(7, 30, 7, 45)
    [[trip]] = await handle.get_upcoming_trips(
        [TripQuery("S1", "S3", limit=1)], start=start, end=start + timedelta(hours=2)
    )
    assert trip.vehicle_id == "V1"  # from the prediction, flag or not
    assert trip.vehicle is None


def test_trip_query_rejects_a_negative_limit() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        TripQuery("S1", "S2", limit=-1)


async def test_a_feed_without_calendars_has_no_scheduled_answers(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """No calendar means no scheduled service, and nothing to scan toward.

    RT-added trips still surface on arrival boards: they need no schedule.
    """
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds",
        payload=[with_base(GTFS_RT_FEED, base)],
    )
    mock_api.get(
        "/hosted/mdb-100.zip",
        body=build_gtfs_zip_bytes(
            omit=frozenset({"calendar.txt", "calendar_dates.txt"})
        ),
        content_type="application/zip",
    )
    for _ in range(2):  # trips and arrivals each fetch realtime once
        mock_api.get("/rt/all", body=TRIP_UPDATES_BASELINE, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    start = _pdt(7, 30, 7, 45)
    assert await handle.get_scheduled_departures(
        [ArrivalsQuery(["S1"])], start=start
    ) == [[]]
    assert await handle.get_upcoming_trips([TripQuery("S1", "S2")], start=start) == [[]]
    [board] = await handle.get_arrivals([ArrivalsQuery(["S1", "S2"])], start=start)
    assert [row.trip_id for row in board] == ["ADDED-9"]
