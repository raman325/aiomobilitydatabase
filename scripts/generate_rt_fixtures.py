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
# Matches test_transit_trips.py's T1_S3_ARRIVAL_EPOCH: T1 reaches S3 (the
# trip-query variant zip's terminal call) at 08:20:00 PDT on the fixture
# Thursday.
_T1_S3_ARRIVAL_EPOCH = int(datetime(2026, 7, 30, 15, 20, 0, tzinfo=UTC).timestamp())
# Arbitrary fixed epoch used by test_rt.py/test_models.py's non-arrivals
# protobuf-parsing tests, unrelated to any particular "now".
_BASELINE_EPOCH = 1_785_500_000
# Matches test_transit_frequencies.py: the frequencies variant zip's
# F1#22200 repetition (start_time 06:10:00 PDT) departs S1 at 13:10 UTC on
# the fixture Thursday.
_F1_REP_22200_S1_EPOCH = int(datetime(2026, 7, 30, 13, 10, tzinfo=UTC).timestamp())


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
    return bytes(msg.SerializeToString())


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
    return bytes(msg.SerializeToString())


def _build_trip_updates_dest_arrival() -> bytes:
    """TripUpdates: T1 predicted 120s late at S3 only (destination end)."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-dest"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "T1"
    stu = trip_update.stop_time_update.add()
    stu.stop_id = "S3"
    stu.arrival.delay = 120
    stu.arrival.time = _T1_S3_ARRIVAL_EPOCH + 120
    return bytes(msg.SerializeToString())


def _build_trip_updates_both_ends() -> bytes:
    """TripUpdates: T1 delayed 300s at both S1 (origin) and S3 (destination)."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-both"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "T1"
    origin = trip_update.stop_time_update.add()
    origin.stop_id = "S1"
    origin.arrival.delay = 300
    origin.arrival.time = _T1_DEPARTURE_EPOCH + 300
    origin.departure.delay = 300
    origin.departure.time = _T1_DEPARTURE_EPOCH + 330
    dest = trip_update.stop_time_update.add()
    dest.stop_id = "S3"
    dest.arrival.delay = 300
    dest.arrival.time = _T1_S3_ARRIVAL_EPOCH + 300
    return bytes(msg.SerializeToString())


def _build_trip_updates_t1_skipped(*, skipped_stop_id: str, delay_at_s1: bool) -> bytes:
    """TripUpdates: T1 has one stop SKIPPED (optionally plus a 300s S1 delay).

    Against the trip-query variant zip (T1: S1 -> S2 -> S3). The S1 delay
    variant is used for the skipped-INTERMEDIATE case so the test can prove
    propagation continues past a skipped stop (S3 still turns realtime).
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-skip"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "T1"
    if delay_at_s1:
        stu = trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.departure.delay = 300
    skipped = trip_update.stop_time_update.add()
    skipped.stop_id = skipped_stop_id
    skipped.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    return bytes(msg.SerializeToString())


def _build_trip_updates_t1_no_data_cut() -> bytes:
    """TripUpdates: T1 delayed 300s at S1, NO_DATA at S2.

    Against the trip-query variant zip: S1 gets scheduled+300 predictions,
    while S2 AND S3 (propagation cut) stay schedule-only.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-no-data"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "T1"
    stu = trip_update.stop_time_update.add()
    stu.stop_id = "S1"
    stu.departure.delay = 300
    no_data = trip_update.stop_time_update.add()
    no_data.stop_id = "S2"
    no_data.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.NO_DATA
    return bytes(msg.SerializeToString())


def _build_trip_updates_t1_trip_delay() -> bytes:
    """TripUpdates: T1 carries ONLY a trip-level delay of 180s (no STUs)."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-trip-delay"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "T1"
    trip_update.delay = 180
    return bytes(msg.SerializeToString())


def _build_trip_updates_t1_canceled() -> bytes:
    """TripUpdates: T1 canceled outright (no stop_time_updates)."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-cancel"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "T1"
    trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    return bytes(msg.SerializeToString())


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
    return bytes(msg.SerializeToString())


def _build_freq_trip_updates_matched() -> bytes:
    """TripUpdates addressing SPECIFIC F1 repetitions via start_time.

    Against the frequencies variant zip: the 06:10:00 repetition
    (F1#22200) is delayed 120s at S1, and the 06:20:00 repetition
    (F1#22800) is canceled -- each must affect exactly its own repetition.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    e1 = msg.entity.add()
    e1.id = "tu-freq-rep"
    tu1 = e1.trip_update
    tu1.trip.trip_id = "F1"
    tu1.trip.start_time = "06:10:00"
    tu1.vehicle.id = "V9"
    stu = tu1.stop_time_update.add()
    stu.stop_id = "S1"
    stu.departure.delay = 120
    stu.departure.time = _F1_REP_22200_S1_EPOCH + 120
    e2 = msg.entity.add()
    e2.id = "tu-freq-cancel"
    tu2 = e2.trip_update
    tu2.trip.trip_id = "F1"
    tu2.trip.start_time = "06:20:00"
    tu2.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    return bytes(msg.SerializeToString())


def _build_freq_trip_updates_unmatched() -> bytes:
    """TripUpdates that must attach to NO frequency repetition.

    One prediction WITHOUT start_time (repetition-ambiguous) and one with a
    start_time (06:05:00) matching no materialized repetition.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    e1 = msg.entity.add()
    e1.id = "tu-freq-bare"
    tu1 = e1.trip_update
    tu1.trip.trip_id = "F1"
    stu1 = tu1.stop_time_update.add()
    stu1.stop_id = "S1"
    stu1.departure.delay = 300
    stu1.departure.time = _F1_REP_22200_S1_EPOCH + 300
    e2 = msg.entity.add()
    e2.id = "tu-freq-ghost"
    tu2 = e2.trip_update
    tu2.trip.trip_id = "F1"
    tu2.trip.start_time = "06:05:00"
    stu2 = tu2.stop_time_update.add()
    stu2.stop_id = "S1"
    stu2.departure.delay = 300
    stu2.departure.time = _F1_REP_22200_S1_EPOCH + 300
    return bytes(msg.SerializeToString())


def _build_freq_trip_updates_bare_cancel() -> bytes:
    """Build a start_time-less cancellation of frequency trip F1.

    Must cancel NO repetition: which one was meant is unknowable without
    start_time.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-freq-bare-cancel"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "F1"
    trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    return bytes(msg.SerializeToString())


def _build_trip_updates_t1_canceled_tomorrow() -> bytes:
    """TripUpdates: T1 canceled with start_date NAMING THE NEXT SERVICE DAY.

    The motivating start_date case: an early-posted "T1 is canceled
    tomorrow" (2026-07-31, the fixture Friday) must cancel ONLY Friday's
    instance -- never the in-window Thursday departure.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-cancel-tomorrow"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "T1"
    trip_update.trip.start_date = "20260731"
    trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    return bytes(msg.SerializeToString())


def _build_trip_updates_t1_dated_tomorrow_delay() -> bytes:
    """TripUpdates: T1 delayed 300s at S1, start_date = the NEXT service day.

    In a 30h arrivals window holding both the Thursday and Friday
    instances of T1, the dated prediction must attach to Friday's
    (second) instance only.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-dated-tomorrow"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "T1"
    trip_update.trip.start_date = "20260731"
    stu = trip_update.stop_time_update.add()
    stu.stop_id = "S1"
    stu.departure.delay = 300
    return bytes(msg.SerializeToString())


def _build_freq_trip_updates_canceled_tomorrow() -> bytes:
    """TripUpdates: the F1 06:10:00 repetition canceled FOR TOMORROW only.

    start_time addresses the repetition and start_date (2026-07-31) the
    service day: today's in-window F1#22200 repetition must survive.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = _NOW_EPOCH
    entity = msg.entity.add()
    entity.id = "tu-freq-cancel-tomorrow"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "F1"
    trip_update.trip.start_time = "06:10:00"
    trip_update.trip.start_date = "20260731"
    trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    return bytes(msg.SerializeToString())


def _build_vehicle_positions_status() -> bytes:
    """VehiclePositions exercising the descriptive status surface.

    Three vehicles against the fixture static feed:

    - V7: EXPLICIT current_status (STOPPED_AT) with a full stop referent
      (stop_id + current_stop_sequence), congestion_level, license_plate.
    - V8: a stop referent (current_stop_sequence only) but NO explicit
      current_status -- the spec default IN_TRANSIT_TO must surface.
    - V9: neither current_status nor any stop referent -- current_status
      must be None (no stop to be in transit to), congestion None.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = 1_785_500_000
    e1 = msg.entity.add()
    e1.id = "vp-s1"
    v1 = e1.vehicle
    v1.vehicle.id = "V7"
    v1.vehicle.license_plate = "8ABC123"
    v1.trip.trip_id = "T1"
    v1.position.latitude = 34.056
    v1.position.longitude = -118.246
    v1.current_status = gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
    v1.stop_id = "S2"
    v1.current_stop_sequence = 2
    v1.congestion_level = gtfs_realtime_pb2.VehiclePosition.SEVERE_CONGESTION
    e2 = msg.entity.add()
    e2.id = "vp-s2"
    v2 = e2.vehicle
    v2.vehicle.id = "V8"
    v2.trip.trip_id = "T3"
    v2.position.latitude = 34.061
    v2.position.longitude = -118.241
    v2.current_stop_sequence = 1  # referent present, status unset -> default
    e3 = msg.entity.add()
    e3.id = "vp-s3"
    v3 = e3.vehicle
    v3.vehicle.id = "V9"
    v3.position.latitude = 34.062
    v3.position.longitude = -118.242
    return bytes(msg.SerializeToString())


def _build_alerts_trip_scoped() -> bytes:
    """Build an alert whose ONLY scoping is informed_entity trip descriptors.

    Pins the semantic fix: with trip_ids populated, a trip-scoped alert
    (route_ids and stop_ids both empty) no longer reads as agency-wide.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = 1_785_500_000
    entity = msg.entity.add()
    entity.id = "alert-trip"
    alert = entity.alert
    informed = alert.informed_entity.add()
    informed.trip.trip_id = "T1"
    informed2 = alert.informed_entity.add()
    informed2.trip.trip_id = "T3"
    alert.effect = gtfs_realtime_pb2.Alert.SIGNIFICANT_DELAYS
    text = alert.header_text.translation.add()
    text.text = "T1 and T3 running late"
    text.language = "en"
    return bytes(msg.SerializeToString())


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
    return bytes(msg.SerializeToString())


def main() -> None:
    """Write every fixture .pb file used by the feeds test suite."""
    fixtures = {
        "vehicle_positions.pb": _build_vehicle_positions(),
        "vehicle_positions_status.pb": _build_vehicle_positions_status(),
        "alerts.pb": _build_alerts(),
        "alerts_trip_scoped.pb": _build_alerts_trip_scoped(),
        "trip_updates_baseline.pb": _build_trip_updates(base_epoch=_BASELINE_EPOCH),
        "trip_updates_t1_delayed.pb": _build_trip_updates(
            base_epoch=_T1_DEPARTURE_EPOCH
        ),
        "trip_updates_t1_dest_arrival.pb": _build_trip_updates_dest_arrival(),
        "trip_updates_t1_both_ends.pb": _build_trip_updates_both_ends(),
        "trip_updates_t1_canceled.pb": _build_trip_updates_t1_canceled(),
        "trip_updates_t1_skip_s1.pb": _build_trip_updates_t1_skipped(
            skipped_stop_id="S1", delay_at_s1=False
        ),
        "trip_updates_t1_skip_s2.pb": _build_trip_updates_t1_skipped(
            skipped_stop_id="S2", delay_at_s1=True
        ),
        "trip_updates_t1_skip_s3.pb": _build_trip_updates_t1_skipped(
            skipped_stop_id="S3", delay_at_s1=False
        ),
        "trip_updates_t1_no_data_cut.pb": _build_trip_updates_t1_no_data_cut(),
        "trip_updates_t1_trip_delay.pb": _build_trip_updates_t1_trip_delay(),
        "added_trips_s1.pb": _build_added_trips(
            base_epoch=_NOW_EPOCH, stop_id="S1", count=3
        ),
        "trip_updates_t1_canceled_tomorrow.pb": (
            _build_trip_updates_t1_canceled_tomorrow()
        ),
        "trip_updates_t1_dated_tomorrow_delay.pb": (
            _build_trip_updates_t1_dated_tomorrow_delay()
        ),
        "trip_updates_freq_canceled_tomorrow.pb": (
            _build_freq_trip_updates_canceled_tomorrow()
        ),
        "trip_updates_freq_matched.pb": _build_freq_trip_updates_matched(),
        "trip_updates_freq_unmatched.pb": _build_freq_trip_updates_unmatched(),
        "trip_updates_freq_bare_cancel.pb": _build_freq_trip_updates_bare_cancel(),
    }
    for filename, content in fixtures.items():
        (_OUTPUT_DIR / filename).write_bytes(content)
        print(f"wrote {filename} ({len(content)} bytes)")


if __name__ == "__main__":
    main()
