"""GTFS Realtime conformance: VehiclePositions, the stops realtime defines,
and the VehicleDescriptor's accessibility on boards and journeys."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest
from google.transit import gtfs_realtime_pb2
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds import Circle
from aiomobilitydatabase.feeds.models import (
    ArrivalsQuery,
    OccupancyStatus,
    StopLocationType,
    TripQuery,
    VehicleStopStatus,
    WheelchairAccess,
)
from aiomobilitydatabase.feeds.rt import stops_from_message, vehicles_from_message
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.test_gtfs_rt_conformance import (
    _EIGHT,
    _descriptor_message,
    _run_feeds,
)
from tests.feeds.test_gtfs_schedule_conformance import _feed

_VEHICLE = gtfs_realtime_pb2.VehiclePosition
_DESCRIPTOR = gtfs_realtime_pb2.VehicleDescriptor


def _vehicle_message(
    *, header_timestamp: int | None = None
) -> tuple[gtfs_realtime_pb2.FeedMessage, gtfs_realtime_pb2.VehiclePosition]:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    if header_timestamp is not None:
        message.header.timestamp = header_timestamp
    entity = message.entity.add()
    entity.id = "v"
    entity.vehicle.SetInParent()
    return message, entity.vehicle


# --- D17: a vehicle without a position ---------------------------------------


def test_a_vehicle_without_a_position_is_kept_and_in_no_zone() -> None:
    """``position`` is Optional: a vehicle saying only which trip it runs
    and where along it is still a vehicle. It has no coordinates (as a
    docked GBFS vehicle has none), so no zone includes it."""
    message, vehicle = _vehicle_message()
    vehicle.trip.trip_id = "T"
    vehicle.trip.start_date = "20260730"
    vehicle.current_stop_sequence = 2
    vehicle.current_status = _VEHICLE.STOPPED_AT
    positioned = message.entity.add()
    positioned.id = "p"
    positioned.vehicle.position.latitude = 0.0
    positioned.vehicle.position.longitude = 0.0

    [bare, placed] = vehicles_from_message(
        message, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert (bare.latitude, bare.longitude, bare.bearing) == (None, None, None)
    assert bare.trip_id == "T"
    assert bare.current_status is VehicleStopStatus.STOPPED_AT

    async def body(handle: TransitFeedHandle) -> tuple[int, int]:
        everywhere = await handle.get_vehicles()
        near = await handle.get_vehicles(Circle(0.0, 0.0, 1000.0))
        return len(everywhere), len(near)

    assert _run_feeds(_feed(), body, [], [message]) == (2, 1)
    assert (placed.latitude, placed.longitude) == (0.0, 0.0)


# --- D16: the message timestamp stands in ------------------------------------


@pytest.mark.parametrize(
    ("own", "header", "expected"),
    [
        (1_785_500_000, 1_785_500_100, 1_785_500_000),
        (None, 1_785_500_100, 1_785_500_100),
        (None, None, None),
    ],
)
def test_a_vehicle_without_a_timestamp_takes_the_messages(
    own: int | None, header: int | None, expected: int | None
) -> None:
    """``vehicle.timestamp``: "Otherwise, consumers must use the message
    timestamp"."""
    message, vehicle = _vehicle_message(header_timestamp=header)
    if own is not None:
        vehicle.timestamp = own
    [parsed] = vehicles_from_message(
        message, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert parsed.timestamp == (
        None if expected is None else datetime.fromtimestamp(expected, tz=UTC)
    )


# --- D15 / D27: carriages ----------------------------------------------------


@settings(max_examples=200, deadline=None)
@given(
    sequences=st.lists(st.none() | st.integers(0, 6), max_size=5),
)
def test_carriages_are_kept_only_when_numbered_one_to_n(
    sequences: list[int | None],
) -> None:
    """ "The first carriage in the direction of travel must have a value of
    1 ... If the second carriage in the direction of travel has a value of
    3, consumers will discard data for all carriages". Kept carriages come
    front first, and an unset occupancy_status is its declared default,
    NO_DATA_AVAILABLE."""
    message, vehicle = _vehicle_message()
    for index, sequence in enumerate(sequences):
        carriage = vehicle.multi_carriage_details.add()
        carriage.label = f"c{index}"
        if sequence is not None:
            carriage.carriage_sequence = sequence
    [parsed] = vehicles_from_message(
        message, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    numbered = (
        sorted(s for s in sequences if s is not None)
        == list(range(1, len(sequences) + 1))
        and None not in sequences
    )
    if not numbered:
        assert parsed.carriages == ()
        return
    assert [c.carriage_sequence for c in parsed.carriages] == list(
        range(1, len(sequences) + 1)
    )
    by_label = {f"c{index}": s for index, s in enumerate(sequences)}
    assert all(by_label[c.label] == c.carriage_sequence for c in parsed.carriages)  # type: ignore[index]
    assert all(
        c.occupancy_status is OccupancyStatus.NO_DATA_AVAILABLE
        for c in parsed.carriages
    )


def test_a_carriages_stated_occupancy_is_kept() -> None:
    message, vehicle = _vehicle_message()
    carriage = vehicle.multi_carriage_details.add()
    carriage.carriage_sequence = 1
    carriage.occupancy_status = _VEHICLE.FULL
    [parsed] = vehicles_from_message(
        message, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert [c.occupancy_status for c in parsed.carriages] == [OccupancyStatus.FULL]


# --- D18: a stop realtime defines ---------------------------------------------


def test_a_realtime_stops_translated_fields_are_text() -> None:
    """Stop: "All fields are as described in the (CSV) GTFS
    specification"; ``stop_code`` and ``platform_code`` are
    TranslatedStrings, read as text like the name, and None when unset.
    "The location type of the new stop is 0"."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    coded = message.entity.add()
    coded.id = "s1"
    coded.stop.stop_id = "N1"
    coded.stop.stop_name.translation.add(text="Temporary stop", language="en")
    coded.stop.stop_code.translation.add(text="4021")
    coded.stop.platform_code.translation.add(text="B")
    coded.stop.stop_desc.translation.add(text="Outside the library")
    coded.stop.stop_url.translation.add(text="https://example.com/n1")
    bare = message.entity.add()
    bare.id = "s2"
    bare.stop.stop_id = "N2"

    stops = stops_from_message(message)
    assert (
        stops["N1"].name,
        stops["N1"].stop_code,
        stops["N1"].platform_code,
        stops["N1"].description,
        stops["N1"].url,
    ) == (
        "Temporary stop",
        "4021",
        "B",
        "Outside the library",
        "https://example.com/n1",
    )
    assert (stops["N2"].name, stops["N2"].stop_code, stops["N2"].platform_code) == (
        None,
        None,
        None,
    )
    assert {stop.location_type for stop in stops.values()} == {StopLocationType.STOP}


# --- D19: the VehicleDescriptor's accessibility --------------------------------

_ACCESSIBLE_TRIP = _feed(
    trips=(
        "route_id,service_id,trip_id,trip_headsign,wheelchair_accessible\n"
        "R,S,T,Downtown,1\n"
    )
)


def _with_descriptor(
    message: gtfs_realtime_pb2.FeedMessage, wheelchair: int | None
) -> gtfs_realtime_pb2.FeedMessage:
    for entity in message.entity:
        entity.trip_update.vehicle.id = "V1"
        if wheelchair is not None:
            entity.trip_update.vehicle.wheelchair_accessible = wheelchair
    return message


def _vehicle_running_t(wheelchair: int | None) -> gtfs_realtime_pb2.FeedMessage:
    message, vehicle = _vehicle_message()
    vehicle.vehicle.id = "V1"
    vehicle.trip.trip_id = "T"
    vehicle.trip.start_date = "20260730"
    vehicle.position.latitude = 0.0
    vehicle.position.longitude = 0.0
    if wheelchair is not None:
        vehicle.vehicle.wheelchair_accessible = wheelchair  # type: ignore[assignment]
    return message


def _accessibility() -> Callable[
    [TransitFeedHandle], Awaitable[tuple[set[object], set[object]]]
]:
    async def body(handle: TransitFeedHandle) -> tuple[set[object], set[object]]:
        start = _EIGHT - timedelta(minutes=30)
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1"], limit=None)],
            start=start,
            end=start + timedelta(hours=2),
            with_vehicles=True,
        )
        [journeys] = await handle.get_upcoming_trips(
            [TripQuery("S0", "S1", limit=None)],
            start=start,
            end=start + timedelta(hours=2),
            with_vehicles=True,
        )
        return (
            {(row.trip_id, row.wheelchair_accessible) for row in board},
            {(row.trip_id, row.wheelchair_accessible) for row in journeys},
        )

    return body


_STATED: dict[int | None, WheelchairAccess | None] = {
    None: None,
    _DESCRIPTOR.NO_VALUE: None,
    _DESCRIPTOR.UNKNOWN: WheelchairAccess.UNKNOWN,
    _DESCRIPTOR.WHEELCHAIR_ACCESSIBLE: WheelchairAccess.POSSIBLE,
    _DESCRIPTOR.WHEELCHAIR_INACCESSIBLE: WheelchairAccess.NOT_POSSIBLE,
}


@pytest.mark.parametrize("from_vehicle", list(_STATED))
@pytest.mark.parametrize("from_update", list(_STATED))
def test_realtime_accessibility_overwrites_the_static_value(
    from_update: int | None, from_vehicle: int | None
) -> None:
    """WheelchairAccessible: "When available, this value should overwrite
    the wheelchair_accessible value from the static GTFS" -- UNKNOWN,
    ACCESSIBLE and INACCESSIBLE each "will overwrite the value from the
    GTFS", and NO_VALUE "won't". The row's vehicle's descriptor states it
    first, then the TripUpdate's; T is accessible in trips.txt."""
    update = _with_descriptor(
        _descriptor_message({"trip_id": "T", "start_date": "20260730", "delay": 60}),
        from_update,
    )
    vehicles = [_vehicle_running_t(from_vehicle)]
    expected = next(
        (
            value
            for value in (_STATED[from_vehicle], _STATED[from_update])
            if value is not None
        ),
        WheelchairAccess.POSSIBLE,
    )
    board, journeys = _run_feeds(_ACCESSIBLE_TRIP, _accessibility(), [update], vehicles)
    assert board == {("T", expected)}
    assert journeys == {("T", expected)}


def test_a_new_trip_shows_the_accessibility_its_update_states() -> None:
    """A NEW trip has no static value to overwrite: its rows show what its
    descriptor states."""
    update = _with_descriptor(
        _descriptor_message(
            {
                "trip_id": "N",
                "start_date": "20260730",
                "route_id": "R",
                "relationship": gtfs_realtime_pb2.TripDescriptor.NEW,
                "calls": [
                    ("S0", _EIGHT + timedelta(minutes=5), None, False),
                    ("S1", _EIGHT + timedelta(minutes=15), None, False),
                ],
            }
        ),
        _DESCRIPTOR.WHEELCHAIR_INACCESSIBLE,
    )
    board, journeys = _run_feeds(_ACCESSIBLE_TRIP, _accessibility(), [update])
    assert ("N", WheelchairAccess.NOT_POSSIBLE) in board
    assert ("N", WheelchairAccess.NOT_POSSIBLE) in journeys


def test_a_copy_shows_the_accessibility_its_update_states() -> None:
    """A DUPLICATED copy takes its original's descriptors, which its own
    update's descriptor overwrites like any static value."""
    update = _with_descriptor(
        _descriptor_message(
            {
                "trip_id": "T",
                "start_date": "20260730",
                "relationship": gtfs_realtime_pb2.TripDescriptor.DUPLICATED,
                "copy": ("T-copy", "20260730", "08:30:00"),
            }
        ),
        _DESCRIPTOR.WHEELCHAIR_INACCESSIBLE,
    )
    board, journeys = _run_feeds(_ACCESSIBLE_TRIP, _accessibility(), [update])
    assert ("T-copy", WheelchairAccess.NOT_POSSIBLE) in board
    assert ("T-copy", WheelchairAccess.NOT_POSSIBLE) in journeys
    assert ("T", WheelchairAccess.POSSIBLE) in board
