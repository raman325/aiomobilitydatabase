"""Tests for public snapshot models."""

import dataclasses
import io
import zipfile
from datetime import UTC, datetime

import pytest
from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds.models import (
    Agency,
    AlertCause,
    AlertEffect,
    ArrivalsQuery,
    BikesAllowed,
    CongestionLevel,
    GbfsVehicle,
    OccupancyStatus,
    PickupDropOffType,
    Route,
    ServiceAlert,
    Station,
    Stop,
    StopArrival,
    SystemInfo,
    VehiclePosition,
    VehicleStopStatus,
    WheelchairAccess,
)

from tests.feeds.fixtures import (
    ALERTS,
    ALERTS_TRIP_SCOPED,
    TRIP_UPDATES_BASELINE,
    VEHICLE_POSITIONS,
    VEHICLE_POSITIONS_STATUS,
    build_gtfs_zip_bytes,
)


def _route(route_id: str, short_name: str | None, long_name: str | None) -> Route:
    return Route(
        id=route_id,
        short_name=short_name,
        long_name=long_name,
        type=3,
        agency_id="A1",
        color="FFD700",
        text_color="000000",
        url=None,
        description=None,
        sort_order=None,
    )


def _stop(stop_id: str, name: str) -> Stop:
    return Stop(
        id=stop_id,
        name=name,
        latitude=34.05,
        longitude=-118.25,
        parent_station=None,
        location_type=None,
        stop_code=None,
        platform_code="A",
        wheelchair_boarding=None,
        description=None,
        url=None,
        zone_id=None,
        timezone=None,
    )


def test_stop_arrival_realtime_flags() -> None:
    scheduled = datetime(2026, 7, 31, 15, 0, tzinfo=UTC)
    arrival = StopArrival(
        stop_id="S1",
        stop=_stop("S1", "Main St"),
        route_id="R1",
        route=_route("R1", "10", "Main Line"),
        trip_id="T1",
        headsign="Downtown",
        scheduled_arrival=scheduled,
        scheduled_departure=scheduled,
        predicted_arrival=None,
        predicted_departure=None,
        delay_seconds=None,
        realtime=False,
        vehicle_id=None,
        vehicle=None,
        wheelchair_accessible=WheelchairAccess.POSSIBLE,
        bikes_allowed=BikesAllowed.NOT_ALLOWED,
        direction_id=0,
        pickup_type=PickupDropOffType.REGULAR,
        drop_off_type=PickupDropOffType.NONE,
        timepoint_exact=True,
        stop_headsign="Downtown via 5th",
        trip_short_name="42",
        block_id="B1",
    )
    assert arrival.realtime is False
    assert arrival.scheduled_departure == scheduled
    assert arrival.wheelchair_accessible is WheelchairAccess.POSSIBLE
    assert arrival.stop_headsign == "Downtown via 5th"
    assert arrival.trip_short_name == "42"
    assert arrival.block_id == "B1"


def test_models_are_frozen() -> None:
    stop = Stop(
        id="S1",
        name="Main St",
        latitude=34.05,
        longitude=-118.25,
        parent_station=None,
        location_type=None,
        stop_code=None,
        platform_code=None,
        wheelchair_boarding=None,
        description=None,
        url=None,
        zone_id=None,
        timezone=None,
    )
    try:
        stop.name = "x"  # type: ignore[misc]
        raise AssertionError("Stop must be frozen")
    except AttributeError:
        pass


def test_vehicle_and_station_construct() -> None:
    vehicle = VehiclePosition(
        vehicle_id="V1",
        label=None,
        latitude=34.0,
        longitude=-118.0,
        bearing=90.0,
        speed=None,
        route_id="R1",
        route_name="10 Main Line",
        trip_id="T1",
        occupancy_status=OccupancyStatus.MANY_SEATS_AVAILABLE,
        timestamp=datetime(2026, 7, 31, 15, 0, tzinfo=UTC),
        current_status=VehicleStopStatus.STOPPED_AT,
        congestion_level=CongestionLevel.RUNNING_SMOOTHLY,
        stop_id="S1",
        current_stop_sequence=3,
        license_plate="8ABC123",
    )
    # StrEnum members compare equal to their raw protobuf-name strings, so
    # pre-typing consumers keep working.
    assert vehicle.occupancy_status == "MANY_SEATS_AVAILABLE"
    assert vehicle.occupancy_status is OccupancyStatus.MANY_SEATS_AVAILABLE
    assert vehicle.current_status == "STOPPED_AT"
    assert vehicle.current_status is VehicleStopStatus.STOPPED_AT
    assert vehicle.congestion_level is CongestionLevel.RUNNING_SMOOTHLY
    assert vehicle.license_plate == "8ABC123"
    station = Station(
        id="st1",
        name="Dock A",
        latitude=34.0,
        longitude=-118.0,
        capacity=20,
        bikes_available=5,
        docks_available=15,
        is_renting=True,
        is_returning=True,
        vehicle_types_available=None,
        rental_uris={"web": "https://example.com/stations/st1"},
    )
    assert station.bikes_available == 5
    gbfs_vehicle = GbfsVehicle(
        id="b1",
        latitude=34.0,
        longitude=-118.0,
        is_reserved=False,
        is_disabled=False,
        vehicle_type_id=None,
        current_range_m=None,
        rental_uris=None,
    )
    assert gbfs_vehicle.id == "b1"
    info = SystemInfo(
        system_id="sys",
        name="Test Bikes",
        operator=None,
        timezone="America/Los_Angeles",
    )
    assert info.system_id == "sys"
    alert = ServiceAlert(
        id="a1",
        header="Detour",
        description=None,
        cause=AlertCause.CONSTRUCTION,
        effect=AlertEffect.DETOUR,
        severity=None,
        route_ids=["R1"],
        stop_ids=[],
        trip_ids=["T1"],
        active_periods=[(datetime(2026, 7, 1, tzinfo=UTC), None)],
        url=None,
    )
    assert alert.route_ids == ["R1"]
    assert alert.trip_ids == ["T1"]
    route = _route("R1", short_name="10", long_name="Main Line")
    assert route.display_name == "10 Main Line"
    assert route.color == "FFD700"
    owl_route = _route("R2", short_name=None, long_name="Owl")
    assert owl_route.display_name == "Owl"
    assert _route("R3", short_name="7", long_name=None).display_name == "7"
    agency = Agency(
        id="A1",
        name="Test Transit",
        url="https://example.com",
        timezone="America/Los_Angeles",
        lang="en",
        phone=None,
        fare_url=None,
        email=None,
    )
    assert agency.name == "Test Transit"


def test_fixtures_are_valid() -> None:
    """Sanity-check the checked-in fixture data: well-formed zip + protobuf."""
    zip_bytes = build_gtfs_zip_bytes()
    names = set(zipfile.ZipFile(io.BytesIO(zip_bytes)).namelist())
    assert "stop_times.txt" in names and "calendar.txt" in names
    for raw in (
        VEHICLE_POSITIONS,
        VEHICLE_POSITIONS_STATUS,
        TRIP_UPDATES_BASELINE,
        ALERTS,
        ALERTS_TRIP_SCOPED,
    ):
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.ParseFromString(raw)
        assert msg.entity


def test_arrivals_query_defaults() -> None:
    query = ArrivalsQuery(["S1", "S2"])
    assert query.stop_ids == ["S1", "S2"]
    assert query.route_ids is None
    assert query.headsigns is None
    assert query.limit == 10


def test_arrivals_query_is_frozen() -> None:
    query = ArrivalsQuery(["S1"])
    with pytest.raises(dataclasses.FrozenInstanceError):
        query.limit = 5  # type: ignore[misc]


def test_arrivals_query_accepts_a_tuple_of_stop_ids() -> None:
    # StationGroup.stop_ids is a tuple, so the canonical flow passes one in.
    query = ArrivalsQuery(("S1", "S2"))
    assert tuple(query.stop_ids) == ("S1", "S2")


@pytest.mark.parametrize("field", ["stop_ids", "route_ids", "headsigns"])
def test_arrivals_query_rejects_a_bare_string(field: str) -> None:
    kwargs: dict[str, object] = {"stop_ids": ["S1"], field: "S1"}
    with pytest.raises(TypeError, match=field):
        ArrivalsQuery(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("limit", [-1, -10])
def test_arrivals_query_rejects_a_negative_limit(limit: int) -> None:
    with pytest.raises(ValueError, match="non-negative"):
        ArrivalsQuery(["S1"], limit=limit)


def test_arrivals_query_allows_a_zero_limit() -> None:
    assert ArrivalsQuery(["S1"], limit=0).limit == 0
