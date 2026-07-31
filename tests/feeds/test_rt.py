"""Tests for GTFS-RT fetching (auth, errors) and protobuf parsing."""

from datetime import UTC, datetime

import aiohttp
import pytest
from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds.exceptions import (
    FeedParseError,
    SourceAuthenticationError,
    SourceConnectionError,
)
from aiomobilitydatabase.feeds.rt import (
    alerts_from_message,
    fetch_feed_message,
    trip_updates_from_message,
    vehicles_from_message,
)

from tests.feeds.fixtures import ALERTS, TRIP_UPDATES_BASELINE, VEHICLE_POSITIONS
from tests.mock_server import MockApi

PB = "application/octet-stream"


async def _fetch(
    mock_api: MockApi, path: str = "/rt", **kwargs: object
) -> gtfs_realtime_pb2.FeedMessage:
    async with aiohttp.ClientSession() as session:
        return await fetch_feed_message(session, mock_api.url(path), **kwargs)  # type: ignore[arg-type]


async def test_fetch_plain(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    msg = await _fetch(mock_api)
    assert len(msg.entity) == 2


async def test_fetch_header_auth(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    await _fetch(mock_api, auth_type=2, api_key_name="X-Api-Key", api_key="secret123")
    assert mock_api.requests[-1].headers.get("X-Api-Key") == "secret123"


async def test_fetch_query_param_auth(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    await _fetch(mock_api, auth_type=1, api_key_name="api_key", api_key="secret123")
    assert mock_api.requests[-1].query.get("api_key") == "secret123"


async def test_fetch_401_maps_to_source_auth_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", status=401, body=b"denied", content_type="text/plain")
    with pytest.raises(SourceAuthenticationError):
        await _fetch(mock_api)


async def test_fetch_500_maps_to_source_connection_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", status=500, body=b"oops", content_type="text/plain")
    with pytest.raises(SourceConnectionError) as excinfo:
        await _fetch(mock_api)
    assert excinfo.value.status == 500


async def test_fetch_unreachable_maps_to_source_connection_error() -> None:
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError):
            await fetch_feed_message(session, "http://127.0.0.1:1/rt")


async def test_fetch_garbage_maps_to_feed_parse_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=b"\x00\x01 not protobuf \xff" * 10, content_type=PB)
    with pytest.raises(FeedParseError):
        await _fetch(mock_api)


def test_vehicles_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(VEHICLE_POSITIONS)
    vehicles = vehicles_from_message(
        msg,
        route_names={"R1": "10 Main Line", "R2": "20 Night Owl"},
        trip_routes={"T3": "R2"},
    )
    assert len(vehicles) == 2
    v1 = next(v for v in vehicles if v.vehicle_id == "V1")
    assert v1.route_id == "R1"
    assert v1.route_name == "10 Main Line"
    assert v1.occupancy_status == "MANY_SEATS_AVAILABLE"
    assert v1.timestamp == datetime.fromtimestamp(1_785_500_000, tz=UTC)
    v2 = next(v for v in vehicles if v.vehicle_id == "V2")
    assert v2.route_id == "R2"  # resolved via trip_routes fallback
    assert v2.route_name == "20 Night Owl"


def test_trip_updates_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(TRIP_UPDATES_BASELINE)
    updates = trip_updates_from_message(msg)
    assert updates.canceled_trips == {"T2"}
    prediction = updates.predictions[("T1", "S1")]
    assert prediction.delay_seconds == 300
    assert prediction.departure == datetime.fromtimestamp(1_785_500_330, tz=UTC)
    assert prediction.vehicle_id == "V1"
    added = updates.added[0]
    assert added.trip_id == "ADDED-9"
    assert added.route_id == "R1"
    assert added.stop_id == "S2"
    assert added.departure == datetime.fromtimestamp(1_785_500_630, tz=UTC)


def _build_conflicting_trip_update(
    *, canceled_first: bool
) -> gtfs_realtime_pb2.FeedMessage:
    """A CANCELED entity plus a stale prediction and a stale ADDED stop time,
    all for the same trip_id, in the given order.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"

    def add_canceled(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "cancel"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )

    def add_prediction(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "stale-update"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.arrival.time = 1_785_500_000

    def add_stale_added(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "stale-added"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ADDED
        )
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S2"
        stu.arrival.time = 1_785_500_100

    if canceled_first:
        add_canceled(msg)
        add_prediction(msg)
        add_stale_added(msg)
    else:
        add_prediction(msg)
        add_stale_added(msg)
        add_canceled(msg)
    return msg


@pytest.mark.parametrize("canceled_first", [True, False])
def test_trip_updates_cancellation_wins_regardless_of_entity_order(
    canceled_first: bool,
) -> None:
    """A CANCELED trip_update plus a stale prediction and a stale ADDED stop
    time for the same trip_id can arrive in either entity order; cancellation
    must always win so the parsed result never claims both statuses for one
    trip.
    """
    msg = _build_conflicting_trip_update(canceled_first=canceled_first)
    updates = trip_updates_from_message(msg)
    assert updates.canceled_trips == {"STALE-TRIP"}
    assert ("STALE-TRIP", "S1") not in updates.predictions
    assert all(added.trip_id != "STALE-TRIP" for added in updates.added)


def test_alerts_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(ALERTS)
    alerts = alerts_from_message(msg)
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.id == "alert-1"
    assert alert.header == "Detour on Main"
    assert alert.description == "Use Second Ave"
    assert alert.cause == "CONSTRUCTION"
    assert alert.effect == "DETOUR"
    assert alert.route_ids == ["R1"]
    assert alert.stop_ids == ["S1"]
    assert alert.active_periods == [
        (datetime.fromtimestamp(1_785_400_000, tz=UTC), None)
    ]
