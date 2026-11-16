"""Index reads: from concurrent calls, and over id lists of any length."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, event, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds import static_index
from aiomobilitydatabase.feeds.static_index import StaticIndex
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.fixtures import build_gtfs_zip_bytes
from tests.feeds.test_gtfs_schedule_conformance import _feed
from tests.feeds.test_properties import (
    _build_index_from_files,
    _index_from_zip_bytes,
)
from tests.feeds.test_time_window_properties import _run


def test_concurrent_index_reads_never_interleave() -> None:
    """Every read shares the index's one SQLite connection. Two queries run
    on it at once by different worker threads return each other's rows, or
    empty ones; concurrent calls (arrivals and vehicles polled together)
    must each still read exactly what a lone call reads."""
    index = _build_index_from_files(_feed())

    async def body(handle: TransitFeedHandle) -> int:
        reads = (handle._index.routes_by_id, handle._index.stops_by_id)
        alone = [await handle._index_read(read) for read in reads]
        wrong = 0
        for _ in range(100):
            together = await asyncio.gather(
                *(handle._index_read(reads[i % 2]) for i in range(16)),
                return_exceptions=True,
            )
            wrong += sum(result != alone[i % 2] for i, result in enumerate(together))
        return wrong

    assert _run(index, body) == 0


# -- Id lists longer than SQLite binds in one statement ---------------------------

# Past SQLite's default variable limit (32766 since 3.32; 999 before).
_MANY = 40_001
_DAY = date(2026, 6, 10)  # a Wednesday: WKDY runs
_NOW = datetime(2026, 6, 10, 14, tzinfo=UTC)  # 07:00 in Los Angeles


def _ghosts(prefix: str, count: int = _MANY) -> list[str]:
    return [f"{prefix}{n}" for n in range(count)]


@pytest.fixture
def fixture_index() -> Iterator[StaticIndex]:
    index = _index_from_zip_bytes(build_gtfs_zip_bytes())
    yield index
    index.close()


def _runs(trip_ids: list[str]) -> list[tuple[str, date]]:
    return [(trip_id, _DAY) for trip_id in trip_ids]


def test_trip_lists_of_any_length(fixture_index: StaticIndex) -> None:
    """Each read naming trips answers over 40,001 of them as over the real
    ones alone (it raised OperationalError: too many SQL variables)."""
    index = fixture_index
    many = ["T1", *_ghosts("ghost")]
    assert index.routes_for_trips(many) == {"T1": "R1"}
    assert index.concrete_trip_ids(many) == {"T1": ["T1"]}
    assert index.trip_summaries(many) == index.trip_summaries(["T1"])
    assert index.plain_trip_starts(many) == {"T1": 8 * 3600 + 30}
    assert index.run_days(_runs(many)) == index.run_days(_runs(["T1"])) != []
    assert index.instance_trip_calls(_runs(many)) == index.instance_trip_calls(
        _runs(["T1"])
    )
    keys = [(trip_id, _DAY, None) for trip_id in many]
    assert index.rt_instances(keys) == index.rt_instances(keys[:1]) != []
    assert index.current_service_dates(many, _NOW) == index.current_service_dates(
        ["T1"], _NOW
    )
    assert index.slot_runs([(trip_id, _DAY, 0) for trip_id in many]) == {}


def test_concrete_trips_come_in_id_order() -> None:
    """A template's repetitions are listed by id, as before slicing --
    not by start, the order the trips index would read them in."""
    files = _feed()
    files["frequencies.txt"] = (
        "trip_id,start_time,end_time,headway_secs\nT,01:00:00,07:00:00,18000\n"
    )
    index = _build_index_from_files(files)
    try:
        concrete = index.concrete_trip_ids(["T", *_ghosts("ghost")])
    finally:
        index.close()
    assert concrete == {"T": ["T", "T#21600", "T#3600"]}


def test_the_first_and_last_journey_are_the_days_across_slices() -> None:
    """Five services, bound three at a time: the day's first journey runs
    on the last service and its last on the first, so each extreme is in
    another slice than the other -- and still exactly one row each."""
    services = ["V0", "V1", "V2", "V3", "V4"]
    files = _feed(
        trips="route_id,service_id,trip_id,trip_headsign\n"
        "R,V0,LATE,X\nR,V4,EARLY,X\nR,V2,MIDDLE,X\n",
        stop_times="trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "LATE,10:00:00,10:00:00,A,1\nLATE,10:30:00,10:30:00,B,2\n"
        "EARLY,08:00:00,08:00:00,A,1\nEARLY,08:30:00,08:30:00,B,2\n"
        "MIDDLE,09:00:00,09:00:00,A,1\nMIDDLE,09:30:00,09:30:00,B,2\n",
        calendar="service_id,monday,tuesday,wednesday,thursday,friday,"
        "saturday,sunday,start_date,end_date\n"
        + "".join(f"{v},1,1,1,1,1,1,1,20260101,20271231\n" for v in services),
    )
    index = _build_index_from_files(files)
    # A journeys day query binds four scalars and the services: three a slice.
    index._conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 5)
    try:
        journeys = index.upcoming_trips(
            "A", "B", datetime(2026, 6, 10, tzinfo=UTC), timedelta(days=1)
        )
    finally:
        index.close()
    assert [(j.trip_id, j.is_first, j.is_last) for j in journeys] == [
        ("EARLY", True, False),
        ("MIDDLE", False, False),
        ("LATE", False, True),
    ]


def test_trips_serving_with_both_lists_long(fixture_index: StaticIndex) -> None:
    index = fixture_index
    assert index.trips_serving(
        ["T1", *_ghosts("ghost")], ["S2", *_ghosts("nowhere")]
    ) == {"T1"}


def test_instance_reads_of_any_length(fixture_index: StaticIndex) -> None:
    """instance_departures and instance_trips over 40,001 runs, and over as
    many stops: the same rows as over the real ones."""
    index = fixture_index
    runs = _runs(["T1", "T2", *_ghosts("ghost")])
    real = _runs(["T1", "T2"])
    departures = index.instance_departures(real, ["S1"])
    assert len(departures) == 2
    assert index.instance_departures(runs, ["S1"]) == departures
    assert (
        index.instance_departures(runs, ["S1", *_ghosts("nowhere")], ["R1"])
        == departures
    )
    journeys = index.instance_trips(real, "S1", "S2")
    assert len(journeys) == 1
    assert index.instance_trips(runs, "S1", "S2") == journeys


def test_stop_route_and_headsign_lists_of_any_length(
    fixture_index: StaticIndex,
) -> None:
    index = fixture_index
    stops = ["S1", *_ghosts("nowhere")]
    routes = ["R1", *_ghosts("route")]
    signs = ["Downtown", *_ghosts("sign")]
    assert index.departures_ceiling(stops, routes, signs) == index.departures_ceiling(
        ["S1"], ["R1"], ["Downtown"]
    )
    lookahead = timedelta(hours=4)
    board = index.upcoming_departures(["S1"], ["R1"], _NOW, lookahead)
    assert board != []
    assert (
        index.upcoming_departures(stops, routes, _NOW, lookahead, headsigns=signs)
        == board
    )
    assert index.on_demand_rows(stops, stops, stops) == []


def _every_read(index: StaticIndex, day: date, now: datetime) -> list[Any]:
    """Every read that binds an id list, over everything the feed holds."""
    stops = [row[0] for row in index._conn.execute("SELECT id FROM stops")]
    trips = [row[0] for row in index._conn.execute("SELECT id FROM trips")]
    routes = [row[0] for row in index._conn.execute("SELECT id FROM routes")]
    runs = [(trip_id, day) for trip_id in trips]
    pair = (stops * 2)[:2]
    lookahead = timedelta(days=2)
    return [
        index.routes_for_trips(trips),
        index.concrete_trip_ids(trips),
        index.trip_summaries(trips),
        index.plain_trip_starts(trips),
        index.trips_serving(trips, stops),
        index.run_days(runs),
        index.instance_trip_calls(runs),
        index.rt_instances([(trip_id, day, None) for trip_id in trips]),
        index.current_service_dates(trips, now),
        index.departures_ceiling(stops, routes),
        index.instance_departures(runs, stops, routes),
        index.instance_trips(runs, *pair),
        index.upcoming_departures(stops, routes, now, lookahead),
        index.upcoming_trips(*pair, now, lookahead),
        index.on_demand_rows(stops, stops, stops),
    ]


@st.composite
def _busy_feed(draw: st.DrawFn) -> dict[str, str]:
    """Trips of several daily services, routes and headsigns over a few
    stops (loops included), so every read binds several ids per list."""
    stops = [f"S{n}" for n in range(draw(st.integers(2, 6)))]
    services = [f"V{n}" for n in range(draw(st.integers(1, 8)))]
    routes = [f"R{n}" for n in range(draw(st.integers(1, 3)))]
    trips_txt = "route_id,service_id,trip_id,trip_headsign\n"
    calls = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
    for trip in range(draw(st.integers(1, 10))):
        trips_txt += (
            f"{draw(st.sampled_from(routes))},{draw(st.sampled_from(services))},"
            f"T{trip},{draw(st.sampled_from(['North', 'South']))}\n"
        )
        minute = draw(st.integers(0, 47 * 60))
        visits = draw(st.lists(st.sampled_from(stops), min_size=1, max_size=5))
        for sequence, stop in enumerate(visits):
            minute += draw(st.integers(0, 30))
            time = f"{minute // 60:02d}:{minute % 60:02d}:00"
            calls += f"T{trip},{time},{time},{stop},{sequence}\n"
    return {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n"
        "A,A,https://a.example,America/Los_Angeles\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"
        + "".join(f"{stop},{stop},0,0\n" for stop in stops),
        "routes.txt": "route_id,agency_id,route_short_name,route_type\n"
        + "".join(f"{route},A,{route},3\n" for route in routes),
        "trips.txt": trips_txt,
        "stop_times.txt": calls,
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,"
        "saturday,sunday,start_date,end_date\n"
        + "".join(
            f"{service},1,1,1,1,1,1,1,20260101,20271231\n" for service in services
        ),
    }


@given(feed=_busy_feed(), limit=st.integers(6, 9), hour=st.integers(0, 23))
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_reads_in_slices_answer_as_reads_in_one_statement(
    feed: dict[str, str], limit: int, hour: int
) -> None:
    """Under a variable limit of a few -- enforced by SQLite itself, so a
    statement binding more fails -- every read returns exactly what it
    returns in one statement: the rows, their order, aggregates and
    first/last flags across slices. (A board's day query binds two
    scalars and up to four lists: 6 is the least limit any query needs.)"""
    now = datetime(2026, 6, 10, hour, tzinfo=UTC)
    index = _build_index_from_files(feed)
    try:
        whole = _every_read(index, _DAY, now)
    finally:
        index.close()
    # A connection of its own, limited before any statement is prepared:
    # sqlite3 reuses a cached statement without checking the limit again.
    index = _build_index_from_files(feed)
    try:
        index._conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, limit)
        sliced = _every_read(index, _DAY, now)
    finally:
        index.close()
    event(f"journeys read: {bool(whole[11])}")
    assert sliced == whole


def test_an_id_list_reaches_sql_through_select_only() -> None:
    """No query builds an IN list of its own: every id list goes through
    StaticIndex._select, which slices it."""
    code = Path(static_index.__file__).read_text(encoding="utf-8")
    assert code.count("'?' * len(") == 1  # _select's own
    assert "IN ({" not in code
