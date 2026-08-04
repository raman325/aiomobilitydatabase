"""Tests for service-day resolution and scheduled departure queries."""

import io
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from aiomobilitydatabase.feeds.static_index import StaticIndex

from tests.feeds.fixtures import build_gtfs_zip_bytes

DATASET = "mdb-100-202607310000"
TZ = "America/Los_Angeles"


def _index(tmp_path: Path) -> StaticIndex:
    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(build_gtfs_zip_bytes())
    return StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def _tied_departures_index(tmp_path: Path) -> StaticIndex:
    """A minimal UTC feed where two trips depart stop S1 at the identical
    instant. Rows are written TB-before-TA (reverse of the expected total
    order) so a departure-only sort key -- which is stable and would
    therefore preserve this insertion/scan order -- produces the WRONG
    ([TB, TA]) sequence; only an explicit trip_id tiebreak yields [TA, TB].
    """
    files = {
        "agency.txt": (
            "agency_id,agency_name,agency_url,agency_timezone\nA1,T,https://e.com,UTC\n"
        ),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign\nR1,ALL,TB,H\nR1,ALL,TA,H\n"
        ),
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "TB,08:00:00,08:00:00,S1,1\nTA,08:00:00,08:00:00,S1,1\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\nALL,1,1,1,1,1,1,1,20260101,20271231\n"
        ),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    zip_path = tmp_path / "tied.zip"
    zip_path.write_bytes(buf.getvalue())
    return StaticIndex.build(zip_path, ":memory:", DATASET, "UTC")


def test_tied_departures_total_order(tmp_path: Path) -> None:
    """Two trips departing the same stop at the identical instant must sort
    by trip_id when departure ties, identically across repeated calls (HA
    sensors must not flap between tied departures).
    """
    index = _tied_departures_index(tmp_path)
    now = datetime(2026, 7, 30, 7, 0, tzinfo=UTC)
    first = index.upcoming_departures(
        ["S1"], None, now, timedelta(hours=2), per_stop_limit=10
    )
    second = index.upcoming_departures(
        ["S1"], None, now, timedelta(hours=2), per_stop_limit=10
    )
    assert [dep.trip_id for dep in first] == ["TA", "TB"]
    assert first == second
    index.close()


def test_active_service_ids_weekday_vs_weekend(tmp_path: Path) -> None:
    index = _index(tmp_path)
    thursday = index.active_service_ids(date(2026, 7, 30))
    assert thursday == {"WKDY", "NIGHT"}
    saturday = index.active_service_ids(date(2026, 8, 1))
    assert saturday == {"NIGHT"}
    index.close()


def test_calendar_dates_exceptions(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # 2026-07-03 is a Friday, but WKDY is removed by exception_type 2.
    assert index.active_service_ids(date(2026, 7, 3)) == {"NIGHT"}
    # 2026-07-04 is a Saturday: SPECIAL added by exception_type 1.
    assert index.active_service_ids(date(2026, 7, 4)) == {"NIGHT", "SPECIAL"}
    index.close()


def _index_with_calendar_dates_conflict(
    tmp_path: Path, csv_row_order: str
) -> StaticIndex:
    """A minimal feed where SVC has BOTH a type-1 (added) and a type-2
    (removed) calendar_dates row for the SAME date, in the given CSV row
    order -- deliberately outside the shared build_gtfs_zip_bytes fixture,
    which has no same-date conflict.
    """
    if csv_row_order == "added_row_first":
        calendar_dates = (
            "service_id,date,exception_type\nSVC,20260810,1\nSVC,20260810,2\n"
        )
    else:
        assert csv_row_order == "removed_row_first"
        calendar_dates = (
            "service_id,date,exception_type\nSVC,20260810,2\nSVC,20260810,1\n"
        )
    files = {
        "agency.txt": (
            "agency_id,agency_name,agency_url,agency_timezone\nA1,T,https://e.com,UTC\n"
        ),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\nR1,SVC,T1,H\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,08:00:00,08:00:00,S1,1\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\nSVC,0,0,0,0,0,0,0,20260101,20271231\n"
        ),
        "calendar_dates.txt": calendar_dates,
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    zip_path = tmp_path / f"conflict-{csv_row_order}.zip"
    zip_path.write_bytes(buf.getvalue())
    return StaticIndex.build(zip_path, ":memory:", DATASET, "UTC")


def test_calendar_dates_same_date_conflict_removed_wins(tmp_path: Path) -> None:
    """Task 15R-b item 3: a service with BOTH an added (type 1) and a
    removed (type 2) calendar_dates exception for the SAME date must
    resolve to inactive -- deterministically, regardless of which row the
    CSV lists first. Pre-fix, ``active_service_ids`` processed exceptions in
    a single pass in row-fetch order, so whichever exception type happened
    to be LAST in the CSV silently won; this pins the desired two-pass
    "removed always wins" semantics for both row orders.
    """
    for order in ("added_row_first", "removed_row_first"):
        index = _index_with_calendar_dates_conflict(tmp_path, order)
        try:
            assert index.active_service_ids(date(2026, 8, 10)) == set(), order
        finally:
            index.close()


def test_upcoming_departures_basic_window(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # Thursday 2026-07-30 07:45 PDT == 14:45 UTC. T1 departs S1 08:00:30, T2 08:30:30.
    now = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["S1"], None, now, timedelta(hours=1), per_stop_limit=10
    )
    trip_ids = [dep.trip_id for dep in departures]
    assert trip_ids == ["T1", "T2"]
    first = departures[0]
    assert first.route_id == "R1"
    assert first.headsign == "Downtown"
    # 08:00:30 PDT == 15:00:30 UTC
    assert first.departure == datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
    # The RT service-day identity: this row belongs to Thursday's service day.
    assert first.service_date == date(2026, 7, 30)
    index.close()


def test_route_filter(tmp_path: Path) -> None:
    index = _index(tmp_path)
    now = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["S1"], ["R2"], now, timedelta(hours=24), per_stop_limit=10
    )
    assert {dep.route_id for dep in departures} == {"R2"}
    index.close()


def test_past_midnight_trip_from_previous_service_day(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # T3 departs S1 at 25:31:00 on the NIGHT service = 01:31 local next clock day.
    # Friday 2026-07-31 01:00 PDT == 08:00 UTC: T3 (from Thursday's service day)
    # should appear at 01:31 PDT == 08:31 UTC.
    now = datetime(2026, 7, 31, 8, 0, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["S1"], None, now, timedelta(hours=1), per_stop_limit=10
    )
    assert [dep.trip_id for dep in departures] == ["T3"]
    assert departures[0].departure == datetime(2026, 7, 31, 8, 31, tzinfo=UTC)
    # service_date pins the GENERATING service day across the >24:00:00
    # boundary: Thursday, even though the departure lands on Friday's clock
    # day -- the date a producer's TripDescriptor.start_date would name.
    assert departures[0].service_date == date(2026, 7, 30)
    index.close()


def test_lookahead_crossing_midnight_catches_tomorrow(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # Thursday 2026-07-30 23:30 PDT == Friday 06:30 UTC. A 10h lookahead spans
    # into Friday's service day: expect Thursday's T3 (25:31 -> Fri 01:31 PDT)
    # AND Friday's own T1/T2/T4 (08:00/08:30/09:00 PDT).
    now = datetime(2026, 7, 31, 6, 30, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["S1"], None, now, timedelta(hours=10), per_stop_limit=10
    )
    assert [dep.trip_id for dep in departures] == ["T3", "T1", "T2", "T4"]
    index.close()


def test_per_stop_limit(tmp_path: Path) -> None:
    index = _index(tmp_path)
    now = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["S1"], None, now, timedelta(hours=24), per_stop_limit=1
    )
    assert len(departures) == 1
    assert departures[0].trip_id == "T1"
    index.close()


def test_dst_spring_forward_uses_elapsed_seconds(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # 2027-03-14 (Sunday) is US spring-forward. NIGHT's T3 at 25:31:00 elapsed
    # from that service day's noon-12h anchor (08:00 UTC) is 2027-03-15T09:31Z.
    # Wall-clock arithmetic would wrongly yield 08:31Z.
    now = datetime(2027, 3, 15, 7, 0, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["S1"], ["R2"], now, timedelta(hours=4), per_stop_limit=10
    )
    assert [dep.trip_id for dep in departures] == ["T3"]
    assert departures[0].departure == datetime(2027, 3, 15, 9, 31, tzinfo=UTC)
    index.close()


def test_long_lookahead_spans_all_service_days(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # Hypothesis-found: the old scan was hardcoded to exactly local_today-1,
    # local_today, local_today+1 regardless of `lookahead`, so a lookahead
    # long enough to require a THIRD future service day silently dropped
    # instances beyond it. now=Sat 2026-01-31 19:00 PST (local_today);
    # T3 (NIGHT, 25:31:00 elapsed) departs S1 at (service_date + 1) 09:31 UTC.
    # A 60h lookahead's correct window [Feb1 03:00Z, Feb3 15:00Z] requires
    # service_date up to local_today+2 (2026-02-02 -> Feb3 09:31Z, in-window)
    # -- one day beyond what the old `local_today+1` cap ever scanned. The old
    # code returned only 2 instances (Feb1 09:31Z, Feb2 09:31Z) here; verified
    # empirically against the pre-fix implementation before writing this
    # assertion. (The plan's original 48h/29h suggestions were checked and
    # do NOT reproduce the bug against this fixture -- at 48h the old
    # local_today+1 day already supplies the second instance, so len==2 both
    # before and after the fix; 60h was chosen as the smallest round number
    # that actually distinguishes old (2) from correct (3) behavior.)
    now = datetime(2026, 2, 1, 3, 0, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["S1"], ["R2"], now, timedelta(hours=60), per_stop_limit=10
    )
    assert len(departures) == 3
    assert [dep.trip_id for dep in departures] == ["T3", "T3", "T3"]
    assert departures[0].departure < departures[1].departure < departures[2].departure
    assert departures[0].departure == datetime(2026, 2, 1, 9, 31, tzinfo=UTC)
    assert departures[1].departure == datetime(2026, 2, 2, 9, 31, tzinfo=UTC)
    assert departures[2].departure == datetime(2026, 2, 3, 9, 31, tzinfo=UTC)
    index.close()


def test_dst_fall_back_uses_elapsed_seconds(tmp_path: Path) -> None:
    index = _index(tmp_path)
    # 2027-11-07 (Sunday) is US fall-back. T3 elapsed from that day's anchor
    # (07:00 UTC) is 2027-11-08T08:31Z; wall-clock would wrongly yield 09:31Z.
    now = datetime(2027, 11, 8, 6, 0, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["S1"], ["R2"], now, timedelta(hours=4), per_stop_limit=10
    )
    assert [dep.trip_id for dep in departures] == ["T3"]
    assert departures[0].departure == datetime(2027, 11, 8, 8, 31, tzinfo=UTC)
    index.close()


def test_trip_stop_calls_ordering_and_empty(tmp_path: Path) -> None:
    """The RT-propagation seam query: per-trip (stop_sequence, stop_id)
    calls come back in stop_sequence order, unknown trips are simply
    absent, and the no-ids fast path returns an empty map without touching
    SQL (an empty IN () list is a SQLite syntax error).
    """
    index = _index(tmp_path)
    calls = index.trip_stop_calls(["T1", "T2", "missing"])
    assert calls == {
        "T1": [(1, "S1"), (2, "S2")],
        "T2": [(1, "S1")],
    }
    assert index.trip_stop_calls([]) == {}
    index.close()
