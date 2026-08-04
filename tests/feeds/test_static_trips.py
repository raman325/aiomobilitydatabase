"""Tests for origin→destination scheduled trip queries (upcoming_trips)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from aiomobilitydatabase.feeds.static_index import StaticIndex

from tests.feeds.fixtures import build_trip_query_gtfs_zip_bytes

DATASET = "mdb-100-202607310000"
TZ = "America/Los_Angeles"

# Thursday 2026-07-30 07:45 PDT == 14:45 UTC, the same instant the arrivals
# tests use. In the trip-query variant zip, T1 runs S1 (dep 08:00:30) ->
# S2 (08:10) -> S3 (arr 08:20, terminal) and T9 runs the reverse S3
# (dep 08:05:30) -> S2 -> S1 (arr 08:25).
NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)


def _index(tmp_path: Path) -> StaticIndex:
    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(build_trip_query_gtfs_zip_bytes())
    return StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def test_origin_to_destination_happy_path(tmp_path: Path) -> None:
    index = _index(tmp_path)
    trips = index.upcoming_trips("S1", "S3", NOW, timedelta(hours=1), 10)
    # T2 also departs S1 in-window with a LATER S3 call in its sequence, but
    # that call has no arrival time: the d.arrival_secs IS NOT NULL clause
    # excludes it (which is why ScheduledTrip.arrival is non-optional).
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.route_id == "R1"
    assert trip.headsign == "Downtown"
    assert trip.origin_stop_id == "S1"
    assert trip.destination_stop_id == "S3"
    # 08:00:30 PDT == 15:00:30 UTC; 08:20:00 PDT == 15:20:00 UTC.
    assert trip.departure == datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
    assert trip.arrival == datetime(2026, 7, 30, 15, 20, tzinfo=UTC)
    index.close()


def test_wrong_direction_trip_excluded(tmp_path: Path) -> None:
    """The reverse trip T9 serves S1 AND S3 with valid departure/arrival
    times, so ONLY the o.stop_sequence < d.stop_sequence predicate keeps it
    out of the S1→S3 result -- and, symmetrically, keeps T1 out of S3→S1.
    """
    index = _index(tmp_path)
    outbound = index.upcoming_trips("S1", "S3", NOW, timedelta(hours=1), 10)
    assert "T9" not in [trip.trip_id for trip in outbound]
    inbound = index.upcoming_trips("S3", "S1", NOW, timedelta(hours=1), 10)
    assert [trip.trip_id for trip in inbound] == ["T9"]
    assert inbound[0].headsign == "Uptown"
    # 08:05:30 PDT == 15:05:30 UTC; 08:25:00 PDT == 15:25:00 UTC.
    assert inbound[0].departure == datetime(2026, 7, 30, 15, 5, 30, tzinfo=UTC)
    assert inbound[0].arrival == datetime(2026, 7, 30, 15, 25, tzinfo=UTC)
    index.close()


def test_origin_served_but_not_destination_excluded(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # T2 departs S1 at 08:30:30 PDT (in-window) but never calls at S2.
    trips = index.upcoming_trips("S1", "S2", NOW, timedelta(hours=1), 10)
    assert [trip.trip_id for trip in trips] == ["T1"]
    assert trips[0].arrival == datetime(2026, 7, 30, 15, 10, tzinfo=UTC)
    index.close()


def test_null_origin_departure_excluded(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # T9 runs S2→S1 in the right direction with a valid S1 arrival, but its
    # S2 call has no departure time: the o.departure_secs IS NOT NULL clause
    # excludes it (which is why ScheduledTrip.departure is non-optional).
    assert index.upcoming_trips("S2", "S1", NOW, timedelta(hours=1), 10) == []
    index.close()


def test_lookahead_window_bounds_origin_departure(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # T1 departs S1 at 15:00:30 UTC: a 15-minute lookahead from 14:45 UTC
    # ends at 15:00, 30 seconds too early; one more minute admits it.
    assert index.upcoming_trips("S1", "S3", NOW, timedelta(minutes=15), 10) == []
    got = index.upcoming_trips("S1", "S3", NOW, timedelta(minutes=16), 10)
    assert [trip.trip_id for trip in got] == ["T1"]
    index.close()


def test_limit_truncates_after_sort(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # 26 hours from Thursday 14:45 UTC spans today's AND Friday's T1 run
    # (WKDY is active both days); limit=1 must keep the nearest departure.
    both = index.upcoming_trips("S1", "S3", NOW, timedelta(hours=26), 10)
    assert [trip.trip_id for trip in both] == ["T1", "T1"]
    assert both[0].departure < both[1].departure
    limited = index.upcoming_trips("S1", "S3", NOW, timedelta(hours=26), 1)
    assert limited == [both[0]]
    index.close()


def test_past_midnight_trip_crosses_service_day(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # T3 (NIGHT) runs S1 25:31:00 -> S2 25:40:00 on THURSDAY's service day,
    # i.e. Friday 01:31 -> 01:40 PDT. Querying Friday 01:00 PDT (08:00 UTC)
    # must surface it from the previous service day.
    now = datetime(2026, 7, 31, 8, 0, tzinfo=UTC)
    trips = index.upcoming_trips("S1", "S2", now, timedelta(hours=1), 10)
    assert [trip.trip_id for trip in trips] == ["T3"]
    assert trips[0].departure == datetime(2026, 7, 31, 8, 31, tzinfo=UTC)
    assert trips[0].arrival == datetime(2026, 7, 31, 8, 40, tzinfo=UTC)
    index.close()
