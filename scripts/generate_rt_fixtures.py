"""Regenerates the checked-in GTFS-RT protobuf fixtures under tests/feeds/data/rt/.

Not a test (pytest never collects anything outside tests/) -- run manually
with ``uv run python scripts/generate_rt_fixtures.py`` if a fixture needs to
change. Each ``_build_*`` function below is a fixed-parameter snapshot of
what used to be a parameterized fixture builder; every call site across the
test suite passes fixed, deterministic arguments (test "now" values are
hardcoded datetimes, never wall-clock time), so baking one .pb file per
distinct call is exact and reproducible.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from google.transit import gtfs_realtime_pb2

_OUTPUT_DIR = Path(__file__).parent.parent / "tests" / "feeds" / "data" / "rt"

# Matches test_transit_arrivals.py's NOW / T1_DEPARTURE_EPOCH constants.
_NOW_EPOCH = int(datetime(2026, 7, 30, 14, 45, tzinfo=UTC).timestamp())
_T1_DEPARTURE_EPOCH = int(datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC).timestamp())
# Arbitrary fixed epoch used by test_rt.py/test_models.py's non-arrivals
# protobuf-parsing tests, unrelated to any particular "now".
_BASELINE_EPOCH = 1_785_500_000


def _build_vehicle_positions() -> bytes:
    """Build a VehiclePositions FeedMessage with two vehicles."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = 1_785_500_000
    entity = msg.entity.add()
    entity.id = "vp-1"
    vehicle = entity.vehicle
    vehicle.vehicle.id = "V1"
    vehicle.vehicle.label = "Bus 42"
    vehicle.trip.trip_id = "T1"
    vehicle.trip.route_id = "R1"
    vehicle.position.latitude = 34.055
    vehicle.position.longitude = -118.245
    vehicle.position.bearing = 90.0
    vehicle.position.speed = 11.5
    vehicle.occupancy_status = gtfs_realtime_pb2.VehiclePosition.MANY_SEATS_AVAILABLE
    vehicle.timestamp = 1_785_500_000
    entity2 = msg.entity.add()
    entity2.id = "vp-2"
    vehicle2 = entity2.vehicle
    vehicle2.vehicle.id = "V2"
    vehicle2.trip.trip_id = "T3"  # route resolved via static index (no route_id set)
    vehicle2.position.latitude = 34.06
    vehicle2.position.longitude = -118.24
    return msg.SerializeToString()


def _build_trip_updates(*, base_epoch: int) -> bytes:
    """TripUpdates: T1 delayed 300s at S1, T2 canceled, added trip on R1 at S2."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = base_epoch
    e1 = msg.entity.add()
    e1.id = "tu-1"
    tu1 = e1.trip_update
    tu1.trip.trip_id = "T1"
    tu1.vehicle.id = "V1"
    stu = tu1.stop_time_update.add()
    stu.stop_id = "S1"
    stu.arrival.delay = 300
    stu.arrival.time = base_epoch + 300
    stu.departure.delay = 300
    stu.departure.time = base_epoch + 330
    e2 = msg.entity.add()
    e2.id = "tu-2"
    tu2 = e2.trip_update
    tu2.trip.trip_id = "T2"
    tu2.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    e3 = msg.entity.add()
    e3.id = "tu-3"
    tu3 = e3.trip_update
    tu3.trip.trip_id = "ADDED-9"
    tu3.trip.route_id = "R1"
    tu3.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.ADDED
    stu3 = tu3.stop_time_update.add()
    stu3.stop_id = "S2"
    stu3.arrival.time = base_epoch + 600
    stu3.departure.time = base_epoch + 630
    return msg.SerializeToString()


def _build_added_trips(*, base_epoch: int, stop_id: str, count: int) -> bytes:
    """TripUpdates with `count` distinct ADDED trips at one stop.

    Trips are named ADDED-A, ADDED-B, ... and their departures are spaced
    60s apart starting at ``base_epoch + 60``, so callers can assert a
    stable nearest-departure-first ordering.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = base_epoch
    for i in range(count):
        entity = msg.entity.add()
        entity.id = f"added-{i}"
        trip_update = entity.trip_update
        trip_update.trip.trip_id = f"ADDED-{chr(ord('A') + i)}"
        trip_update.trip.route_id = "R1"
        trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.ADDED
        stu = trip_update.stop_time_update.add()
        stu.stop_id = stop_id
        offset = 60 * (i + 1)
        stu.arrival.time = base_epoch + offset
        stu.departure.time = base_epoch + offset + 30
    return msg.SerializeToString()


def _build_alerts() -> bytes:
    """Build an Alert FeedMessage with one active alert."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = 1_785_500_000
    entity = msg.entity.add()
    entity.id = "alert-1"
    alert = entity.alert
    period = alert.active_period.add()
    period.start = 1_785_400_000
    informed = alert.informed_entity.add()
    informed.route_id = "R1"
    informed_stop = alert.informed_entity.add()
    informed_stop.stop_id = "S1"
    alert.cause = gtfs_realtime_pb2.Alert.CONSTRUCTION
    alert.effect = gtfs_realtime_pb2.Alert.DETOUR
    text = alert.header_text.translation.add()
    text.text = "Detour on Main"
    text.language = "en"
    desc = alert.description_text.translation.add()
    desc.text = "Use Second Ave"
    desc.language = "en"
    return msg.SerializeToString()


def main() -> None:
    """Write every fixture .pb file used by the feeds test suite."""
    fixtures = {
        "vehicle_positions.pb": _build_vehicle_positions(),
        "alerts.pb": _build_alerts(),
        "trip_updates_baseline.pb": _build_trip_updates(base_epoch=_BASELINE_EPOCH),
        "trip_updates_t1_delayed.pb": _build_trip_updates(
            base_epoch=_T1_DEPARTURE_EPOCH
        ),
        "added_trips_s1.pb": _build_added_trips(
            base_epoch=_NOW_EPOCH, stop_id="S1", count=3
        ),
    }
    for filename, content in fixtures.items():
        (_OUTPUT_DIR / filename).write_bytes(content)
        print(f"wrote {filename} ({len(content)} bytes)")


if __name__ == "__main__":
    main()
