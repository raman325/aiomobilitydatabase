"""Tests for public snapshot models."""

import io
import zipfile
from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds.models import (
    GbfsVehicle,
    Route,
    ServiceAlert,
    Station,
    Stop,
    StopArrival,
    SystemInfo,
    VehiclePosition,
)

from tests.feeds.fixtures import build_gtfs_zip_bytes
from tests.feeds.rt_fixture import (
    build_alerts,
    build_trip_updates,
    build_vehicle_positions,
)


def test_stop_arrival_realtime_flags() -> None:
    scheduled = datetime(2026, 7, 31, 15, 0, tzinfo=UTC)
    arrival = StopArrival(
        stop_id="S1",
        stop_name="Main St",
        route_id="R1",
        route_name="10 Main Line",
        trip_id="T1",
        headsign="Downtown",
        scheduled_arrival=scheduled,
        scheduled_departure=scheduled,
        predicted_arrival=None,
        predicted_departure=None,
        delay_seconds=None,
        realtime=False,
        vehicle_id=None,
    )
    assert arrival.realtime is False
    assert arrival.scheduled_departure == scheduled


def test_models_are_frozen() -> None:
    stop = Stop(id="S1", name="Main St", latitude=34.05, longitude=-118.25)
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
        occupancy_status="MANY_SEATS_AVAILABLE",
        timestamp=datetime(2026, 7, 31, 15, 0, tzinfo=UTC),
    )
    assert vehicle.occupancy_status == "MANY_SEATS_AVAILABLE"
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
        cause="CONSTRUCTION",
        effect="DETOUR",
        severity=None,
        route_ids=["R1"],
        stop_ids=[],
        active_periods=[(datetime(2026, 7, 1, tzinfo=UTC), None)],
        url=None,
    )
    assert alert.route_ids == ["R1"]
    route = Route(id="R1", short_name="10", long_name="Main Line", type=3)
    assert route.display_name == "10 Main Line"
    owl_route = Route(id="R2", short_name=None, long_name="Owl", type=3)
    assert owl_route.display_name == "Owl"
    assert Route(id="R3", short_name="7", long_name=None, type=3).display_name == "7"


def test_fixture_builders_produce_bytes() -> None:
    zip_bytes = build_gtfs_zip_bytes()
    names = set(zipfile.ZipFile(io.BytesIO(zip_bytes)).namelist())
    assert "stop_times.txt" in names and "calendar.txt" in names
    for raw in (
        build_vehicle_positions(),
        build_trip_updates(base_epoch=1_785_500_000),
        build_alerts(),
    ):
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.ParseFromString(raw)
        assert msg.entity
