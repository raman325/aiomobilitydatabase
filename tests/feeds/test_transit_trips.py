"""Tests for TransitFeedHandle.upcoming_trips: schedule + RT overlay."""

from datetime import UTC, datetime, timedelta

from aiomobilitydatabase.feeds.client import MobilityFeedsClient

from tests.feeds.fixtures import (
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    TRIP_UPDATES_T1_BOTH_ENDS,
    TRIP_UPDATES_T1_CANCELED,
    TRIP_UPDATES_T1_DELAYED,
    TRIP_UPDATES_T1_DEST_ARRIVAL,
    build_trip_query_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)  # Thursday 07:45 PDT
T1_DEPARTURE_EPOCH = int(datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC).timestamp())
T1_S3_ARRIVAL_EPOCH = int(datetime(2026, 7, 30, 15, 20, tzinfo=UTC).timestamp())
PB = "application/octet-stream"


def _mock_catalog(mock_api: MockApi, *, rt: bool) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    rt_feeds = [with_base(GTFS_RT_FEED, base)] if rt else []
    mock_api.get("/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=rt_feeds)
    mock_api.get(
        "/hosted/mdb-100.zip",
        body=build_trip_query_gtfs_zip_bytes(),
        content_type="application/zip",
    )


async def test_schedule_only_trips(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is False
    assert trip.predicted_departure is None
    assert trip.predicted_arrival is None
    assert trip.delay_seconds is None
    assert trip.scheduled_departure == datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
    assert trip.scheduled_arrival == datetime(2026, 7, 30, 15, 20, tzinfo=UTC)
    assert trip.origin_stop_id == "S1"
    assert trip.destination_stop_id == "S3"
    assert trip.route_name == "10 Main Line"
    assert trip.headsign == "Downtown"


async def test_rt_origin_prediction_only(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100", api_key="secret123")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    # The RT feed also carries an ADDED trip (ADDED-9): added trips never
    # appear here because their full stop sequence is unknown.
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.delay_seconds == 300
    assert trip.predicted_departure == datetime.fromtimestamp(
        T1_DEPARTURE_EPOCH + 330, tz=UTC
    )
    # The TU names S1 only: no prediction at the destination end.
    assert trip.predicted_arrival is None
    assert trip.scheduled_arrival == datetime(2026, 7, 30, 15, 20, tzinfo=UTC)


async def test_rt_destination_prediction_only(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DEST_ARRIVAL, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.predicted_arrival == datetime.fromtimestamp(
        T1_S3_ARRIVAL_EPOCH + 120, tz=UTC
    )
    assert trip.predicted_departure is None
    # delay_seconds reports the ORIGIN departure delay only, so a
    # destination-end prediction must not populate it.
    assert trip.delay_seconds is None


async def test_rt_predictions_at_both_ends(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_BOTH_ENDS, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.delay_seconds == 300
    assert trip.predicted_departure == datetime.fromtimestamp(
        T1_DEPARTURE_EPOCH + 330, tz=UTC
    )
    assert trip.predicted_arrival == datetime.fromtimestamp(
        T1_S3_ARRIVAL_EPOCH + 300, tz=UTC
    )


async def test_rt_cancellation_drops_trip(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_CANCELED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    # T1 is the only scheduled S1→S3 candidate in the window; canceling it
    # must drop the trip entirely (cancellation-wins, as in get_arrivals).
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert trips == []
