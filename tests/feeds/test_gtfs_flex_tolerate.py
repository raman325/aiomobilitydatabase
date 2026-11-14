"""GTFS-Flex feeds load: zone-only datasets, and Flex calls among fixed ones.

stops.txt is "Conditionally Required: Optional if demand-responsive zones
are defined in locations.geojson". In stop_times.txt, ``stop_id`` is
"Forbidden if stop_times.location_group_id or stop_times.location_id are
defined", and a pickup/drop-off window forbids arrival and departure
times. Such a call is stored apart from the fixed calls (flex_stop_times):
every fixed-route answer is what the feed gives without it, but a GTFS-RT
update or a TripModifications selector naming its stop_sequence still
finds its place in the trip.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import timedelta
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.models import ArrivalsQuery
from aiomobilitydatabase.feeds.rt import stated_copy_calls, trip_updates_from_message
from aiomobilitydatabase.feeds.static_index import StaticIndex, TripCall
from aiomobilitydatabase.feeds.transit import TransitFeedHandle, _run_calls

from tests.feeds.test_gtfs_rt_conformance import _detour, _run_feeds
from tests.feeds.test_gtfs_schedule_conformance import (
    _MIDNIGHT,
    _board,
    _build,
    _feed,
    _journeys,
)
from tests.feeds.test_properties import _index_from_zip_bytes, _random_gtfs_zip

_STU = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
_EIGHT = _MIDNIGHT.replace(hour=8)

_ZONES = json.dumps(
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "Z",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                },
            }
        ],
    }
)

_FIXED_HEADER = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
_FLEX_HEADER = (
    "trip_id,arrival_time,departure_time,stop_id,location_id,location_group_id,"
    "start_pickup_drop_off_window,end_pickup_drop_off_window,stop_sequence,"
    "pickup_type,drop_off_type\n"
)


def _zone_only(**files: str) -> dict[str, str]:
    """A demand-responsive dataset: zones, no stops.txt, calls by location."""
    feed = _feed(
        stop_times=(
            "trip_id,location_id,start_pickup_drop_off_window,"
            "end_pickup_drop_off_window,stop_sequence,pickup_type,drop_off_type\n"
            "T,Z,08:00:00,18:00:00,1,2,1\nT,Z,08:00:00,18:00:00,2,1,2\n"
        ),
        **files,
    )
    del feed["stops.txt"]
    feed["locations.geojson"] = _ZONES
    return feed


def _deviation(**files: str) -> dict[str, str]:
    """T runs S0 (08:00) -> a zone window -> S1 (08:30): route deviation."""
    feed = _feed(
        stop_times=_FLEX_HEADER
        + "T,08:00:00,08:00:00,S0,,,,,1,,\n"
        + "T,,,,Z,,08:05:00,08:25:00,2,2,2\n"
        + "T,08:30:00,08:30:00,S1,,,,,3,,\n",
        **files,
    )
    feed["locations.geojson"] = _ZONES
    return feed


def _stop_time_rows(index: StaticIndex) -> list[tuple[object, ...]]:
    return index._conn.execute(
        "SELECT trip_id, stop_id, arrival_secs, departure_secs, stop_sequence "
        "FROM stop_times ORDER BY trip_id, stop_sequence"
    ).fetchall()


def _flex_rows(index: StaticIndex) -> list[tuple[object, ...]]:
    return index._conn.execute(
        "SELECT trip_id, stop_sequence, stop_id, location_id, location_group_id, "
        "start_window_secs, end_window_secs, pickup_type, drop_off_type "
        "FROM flex_stop_times ORDER BY trip_id, stop_sequence"
    ).fetchall()


# -- stops.txt is optional when zones are defined -----------------------------


def test_a_zone_only_dataset_builds(tmp_path: Path) -> None:
    """A valid demand-responsive dataset yields a handle with its routes and
    agency, no fixed calls, and its zone calls stored apart."""
    index = _build(tmp_path, _zone_only())
    assert [route.id for route in index.routes()] == ["R"]
    assert [agency.id for agency in index.agencies()] == ["A"]
    assert index.stops() == []
    assert _stop_time_rows(index) == []
    assert _flex_rows(index) == [
        ("T", 1, None, "Z", None, 28800, 64800, 2, 1),
        ("T", 2, None, "Z", None, 28800, 64800, 1, 2),
    ]
    index.close()


def test_a_zone_only_dataset_with_an_empty_stops_file_builds(tmp_path: Path) -> None:
    feed = _zone_only()
    feed["stops.txt"] = "stop_id,stop_name,stop_lat,stop_lon\n"
    index = _build(tmp_path, feed)
    assert _stop_time_rows(index) == []
    index.close()


def test_stops_txt_is_required_without_zones(tmp_path: Path) -> None:
    feed = _zone_only()
    del feed["locations.geojson"]
    with pytest.raises(FeedParseError, match=r"missing required file: stops\.txt"):
        _build(tmp_path, feed)


# -- Flex calls are stored apart, fixed calls kept ----------------------------


def test_flex_calls_among_fixed_ones_are_stored_apart(tmp_path: Path) -> None:
    """A route-deviation trip (fixed stops around a zone window), a trip of
    zones and stop groups, an on-demand window at a stop, and a row naming
    no place: the fixed calls alone are in stop_times, the Flex ones in
    flex_stop_times, the placeless row nowhere -- and the deviation trip
    still runs S0 -> S1 on the board and as a journey."""
    feed = _deviation(
        trips=(
            "route_id,service_id,trip_id,trip_headsign\n"
            "R,S,T,Downtown\nR,S,AREA,Anywhere\nR,S,ATSTOP,Here\n"
        ),
    )
    feed["stop_times.txt"] += (
        "AREA,,,,Z,,06:00:00,22:00:00,1,2,1\n"
        "AREA,,,,,G,06:00:00,22:00:00,2,1,2\n"
        "ATSTOP,,,S0,,,09:00:00,10:00:00,1,2,1\n"
        "ATSTOP,,,,,,,,2,,\n"
    )
    feed["location_groups.txt"] = "location_group_id\nG\n"
    feed["location_group_stops.txt"] = "location_group_id,stop_id\nG,S1\n"
    index = _build(tmp_path, feed)
    assert _stop_time_rows(index) == [
        ("T", "S0", 28800, 28800, 1),
        ("T", "S1", 30600, 30600, 3),
    ]
    assert _flex_rows(index) == [
        ("AREA", 1, None, "Z", None, 21600, 79200, 2, 1),
        ("AREA", 2, None, None, "G", 21600, 79200, 1, 2),
        ("ATSTOP", 1, "S0", None, None, 32400, 36000, 2, 1),
        ("T", 2, None, "Z", None, 29100, 30300, 2, 2),
    ]
    assert [row[:2] for row in _board(index, ["S0", "S1"])] == [
        ("T", "S0"),
        ("T", "S1"),
    ]
    assert _journeys(index, "S0", "S1") == ["T"]
    index.close()


def test_a_stop_times_file_without_a_stop_id_column_builds(tmp_path: Path) -> None:
    feed = _zone_only()
    feed["stops.txt"] = _feed()["stops.txt"]
    index = _build(tmp_path, feed)
    assert _stop_time_rows(index) == []
    assert len(_flex_rows(index)) == 2
    index.close()


# -- frequencies.txt ----------------------------------------------------------


def test_a_frequency_template_anchors_on_its_first_fixed_call(tmp_path: Path) -> None:
    """A window ahead of a frequency trip's first stop is not that stop:
    repetitions depart S0 at their start, and each carries the window
    shifted alike (05:00-06:00 on an 08:00 template, so 03:00-04:00 for the
    06:00 run); the template keeps none of its calls."""
    feed = _feed(
        stop_times=_FLEX_HEADER
        + "T,,,,Z,,05:00:00,06:00:00,0,2,1\n"
        + "T,08:00:00,08:00:00,S0,,,,,1,,\n"
        + "T,08:10:00,08:10:00,S1,,,,,2,,\n",
        frequencies="trip_id,start_time,end_time,headway_secs\nT,06:00:00,06:20:00,600\n",
    )
    feed["locations.geojson"] = _ZONES
    index = _build(tmp_path, feed)
    assert [(row[0], row[3]) for row in _board(index, ["S0"])] == [
        ("T#21600", _MIDNIGHT.replace(hour=6)),
        ("T#22200", _MIDNIGHT.replace(hour=6, minute=10)),
    ]
    assert _flex_rows(index) == [
        ("T#21600", 0, None, "Z", None, 10800, 14400, 2, 1),
        ("T#22200", 0, None, "Z", None, 11400, 15000, 2, 1),
    ]
    index.close()


def test_a_window_first_trip_anchors_on_its_first_timed_call(tmp_path: Path) -> None:
    """With a window first, the first fixed call is no longer the trip's
    first stop, so its times are optional: an untimed S0 leaves the
    anchor at the first timed call (S1), instead of failing the feed."""
    feed = _feed(
        stop_times=_FLEX_HEADER
        + "T,,,,Z,,07:00:00,07:50:00,1,2,1\n"
        + "T,,,S0,,,,,2,,\n"
        + "T,08:10:00,08:10:00,S1,,,,,3,,\n",
        frequencies="trip_id,start_time,end_time,headway_secs\nT,06:00:00,06:10:00,600\n",
    )
    feed["locations.geojson"] = _ZONES
    index = _build(tmp_path, feed)
    assert [(row[0], row[3]) for row in _board(index, ["S1"])] == [
        ("T#21600", _MIDNIGHT.replace(hour=6)),
    ]
    assert _flex_rows(index) == [
        ("T#21600", 1, None, "Z", None, 25200 - 7800, 28200 - 7800, 2, 1)
    ]
    index.close()


def test_a_frequency_trip_of_flex_calls_alone_is_one_trip(tmp_path: Path) -> None:
    """A template of Flex calls alone has no departure to repeat from: it
    stays one unrepeated trip, its calls under its own id."""
    feed = _zone_only(
        frequencies="trip_id,start_time,end_time,headway_secs\nT,06:00:00,07:00:00,600\n"
    )
    index = _build(tmp_path, feed)
    assert [row[:2] for row in _flex_rows(index)] == [("T", 1), ("T", 2)]
    assert index._conn.execute("SELECT id FROM trips").fetchall() == [("T",)]
    index.close()


# -- GTFS-RT: a Flex call keeps its place in the trip -------------------------


def _deviation_board(message: gtfs_realtime_pb2.FeedMessage) -> dict[str, object]:
    async def body(handle: TransitFeedHandle) -> dict[str, object]:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1"], limit=None)],
            start=_EIGHT - timedelta(minutes=30),
            end=_EIGHT + timedelta(hours=2),
        )
        return {
            row.stop_id: (row.predicted_arrival, row.delay_seconds) for row in board
        }

    feed = _deviation()
    feed["locations.geojson"] = _ZONES
    return _run_feeds(feed, body, [message])


def _deviation_update(
    *updates: tuple[int, int | None, int],
) -> gtfs_realtime_pb2.FeedMessage:
    """One TripUpdate for T: (stop_sequence, delay, relationship) each."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "T"
    entity.trip_update.trip.trip_id = "T"
    entity.trip_update.trip.start_date = "20260730"
    for sequence, delay, relationship in updates:
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = sequence
        stu.schedule_relationship = relationship
        if delay is not None:
            stu.arrival.delay = stu.departure.delay = delay
    return message


def test_a_delay_at_a_flex_call_propagates_past_it() -> None:
    """An update naming the zone call's stop_sequence is placed there, and
    its delay "is propagated to all subsequent stops": S1 is ten minutes
    late, not the one minute S0 carried."""
    message = _deviation_update((1, 60, _STU.SCHEDULED), (2, 600, _STU.SCHEDULED))
    assert _deviation_board(message) == {
        "S0": (_EIGHT + timedelta(minutes=1), 60),
        "S1": (_EIGHT + timedelta(minutes=40), 600),
    }


def test_no_data_at_a_flex_call_stops_propagation() -> None:
    """NO_DATA at the zone call cuts propagation: S1 is schedule-only."""
    message = _deviation_update((1, 60, _STU.SCHEDULED), (2, None, _STU.NO_DATA))
    assert _deviation_board(message) == {
        "S0": (_EIGHT + timedelta(minutes=1), 60),
        "S1": (None, None),
    }


def test_a_detour_may_start_at_a_flex_call() -> None:
    """A TripModifications selector naming the zone call resolves: the
    detour's five minutes move S1, after it."""
    message = _deviation_update()
    _detour(message, start=2, end=2, delay=300)

    async def body(handle: TransitFeedHandle) -> list[tuple[object, ...]]:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1"], limit=None)],
            start=_EIGHT - timedelta(minutes=30),
            end=_EIGHT + timedelta(hours=2),
        )
        return [(row.stop_id, row.scheduled_departure) for row in board]

    assert _run_feeds(_deviation(), body, [message]) == [
        ("S0", _EIGHT),
        ("S1", _EIGHT + timedelta(minutes=35)),
    ]


def _call(sequence: int, stop_id: str, *, on_demand: bool = False) -> TripCall:
    when = None if on_demand else _EIGHT + timedelta(minutes=sequence)
    return TripCall(
        stop_sequence=sequence,
        stop_id=stop_id,
        arrival=when,
        departure=when,
        pickup_type=None,
        drop_off_type=None,
        timepoint_exact=None,
        stop_headsign=None,
        on_demand=on_demand,
    )


def test_a_flex_call_is_never_shown_as_a_stop() -> None:
    """An update giving a time at a window-at-stop call does not make it a
    timed stop: a run's shown calls, and a copy's stated ones, leave it
    out."""
    message = _deviation_update()
    stu = message.entity[0].trip_update.stop_time_update.add()
    stu.stop_sequence = 2
    stu.arrival.time = stu.departure.time = int(_EIGHT.timestamp())
    [entry] = trip_updates_from_message(message).trips.values()
    calls = [_call(1, "S0"), _call(2, "S0", on_demand=True), _call(3, "S1")]
    assert [call.stop_id for call in _run_calls(entry, calls)] == ["S0", "S1"]
    assert stated_copy_calls(entry, calls) == ()


# -- Property: Flex rows change nothing a fixed-route query can see ------------


def _dump(index: StaticIndex) -> dict[str, list[tuple[object, ...]]]:
    """Every stored row of every table: what any query can read."""
    tables = [
        name
        for (name,) in index._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
    ]
    return {
        table: sorted(
            index._conn.execute(f"SELECT * FROM {table}").fetchall(),
            key=repr,
        )
        for table in tables
    }


def _outcome(zip_bytes: bytes) -> dict[str, list[tuple[object, ...]]] | str:
    try:
        index = _index_from_zip_bytes(zip_bytes)
    except FeedParseError as err:
        return f"FeedParseError: {err}"
    try:
        return _dump(index)
    finally:
        index.close()


def _rezip(base: bytes, **replace: str) -> bytes:
    buf = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(base)) as source,
        zipfile.ZipFile(buf, "w") as target,
    ):
        for name in source.namelist():
            if name not in replace:
                target.writestr(name, source.read(name))
        for name, content in replace.items():
            target.writestr(name, content)
    return buf.getvalue()


_WINDOWS = {"": None, "06:00:00": 21600, "08:30:00": 30600, "25:00:00": 90000}
_FLEX_TIMES = st.sampled_from(sorted(_WINDOWS))
_FLEX_IDS = st.sampled_from(["Z", "Z2", "G", "🚌"])
_FLEX_KINDS = [
    "location",
    "group",
    "window_at_stop",
    "end_window_at_stop",
    "stop_and_location",
    "no_place",
    "blank_stop",
]


@st.composite
def _flex_row(
    draw: st.DrawFn, trip_ids: list[str], stop_ids: list[str]
) -> dict[str, str]:
    """A Flex-shaped stop_times row: a zone, a stop group, a window (or an
    end alone) at a stop, a stop and a zone at once (Forbidden, but seen),
    or no place at all (blank or whitespace stop_id); windows in place of
    times, sometimes both."""
    kind = draw(st.sampled_from(_FLEX_KINDS))
    start = draw(_FLEX_TIMES)
    end = draw(_FLEX_TIMES)
    if kind == "window_at_stop":
        start = start or "06:00:00"  # a window is what makes it on-demand
    if kind == "end_window_at_stop":
        start, end = "", end or "08:30:00"
    at_stop = kind in {"window_at_stop", "end_window_at_stop", "stop_and_location"}
    row = {
        "trip_id": draw(st.sampled_from([*trip_ids, "ghost"])),
        "stop_id": (
            draw(st.sampled_from(stop_ids))
            if at_stop
            else " "
            if kind == "blank_stop"
            else ""
        ),
        "location_id": (
            draw(_FLEX_IDS) if kind in {"location", "stop_and_location"} else ""
        ),
        "location_group_id": draw(_FLEX_IDS) if kind == "group" else "",
        "start_pickup_drop_off_window": start,
        "end_pickup_drop_off_window": end,
        "stop_sequence": str(draw(st.integers(0, 50))),
        "pickup_type": draw(st.sampled_from(["", "1", "2", "3"])),
        "drop_off_type": draw(st.sampled_from(["", "1", "2", "3"])),
    }
    if draw(st.integers(0, 4)) == 0:  # Forbidden alongside a window, but seen
        row["arrival_time"] = row["departure_time"] = draw(_FLEX_TIMES)
    return row


def _stored_flex(row: dict[str, str]) -> tuple[object, ...] | None:
    """The flex_stop_times record a drawn row should become, if any."""
    stop_id = row.get("stop_id", "").strip() and row["stop_id"]
    flex = any(
        row[column]
        for column in (
            "location_id",
            "location_group_id",
            "start_pickup_drop_off_window",
            "end_pickup_drop_off_window",
        )
    )
    if not flex or not (stop_id or row["location_id"] or row["location_group_id"]):
        return None
    return (
        row["trip_id"],
        int(row["stop_sequence"]),
        stop_id or None,
        row["location_id"] or None,
        row["location_group_id"] or None,
        _WINDOWS[row["start_pickup_drop_off_window"]],
        _WINDOWS[row["end_pickup_drop_off_window"]],
        int(row["pickup_type"] or 0),
        int(row["drop_off_type"] or 0),
        None,
        None,
        None,
    )


def _csv(fieldnames: list[str], rows: list[dict[str, str]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def _without_flex(
    outcome: dict[str, list[tuple[object, ...]]] | str,
) -> tuple[object, list[tuple[object, ...]]]:
    """Split an outcome into everything but flex_stop_times, and that."""
    if isinstance(outcome, str):
        return outcome, []
    rest = dict(outcome)
    return rest, rest.pop("flex_stop_times")


@given(base=_random_gtfs_zip(), data=st.data())
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_flex_rows_change_no_fixed_route_answer(
    base: bytes, data: st.DataObject
) -> None:
    """Injecting Flex-shaped rows anywhere in stop_times.txt (with extra
    columns in any order, a zones file, and maybe a frequencies.txt) stores
    exactly what the feed without them stores in every other table -- so
    every fixed-route query answers the same -- or fails the build with the
    same error. Without frequencies, every row with a Flex column and a
    place lands in flex_stop_times, and no other row does. Without a
    stop_id column, a file of Flex rows alone stores what an empty
    stop_times.txt does elsewhere."""
    with zipfile.ZipFile(io.BytesIO(base)) as archive:
        fixed = list(
            csv.DictReader(io.StringIO(archive.read("stop_times.txt").decode()))
        )
        stop_ids = [
            row["stop_id"]
            for row in csv.DictReader(io.StringIO(archive.read("stops.txt").decode()))
        ]
    trip_ids = sorted({row["trip_id"] for row in fixed})
    repeated = data.draw(st.booleans())
    if repeated:
        base = _rezip(
            base,
            **{
                "frequencies.txt": "trip_id,start_time,end_time,headway_secs\n"
                f"{data.draw(st.sampled_from(trip_ids))},06:00:00,07:00:00,1800\n"
            },
        )
    flex = data.draw(st.lists(_flex_row(trip_ids, stop_ids), min_size=1, max_size=8))
    rows = list(fixed)
    for row in flex:
        rows.insert(data.draw(st.integers(0, len(rows))), row)
    fieldnames = data.draw(
        st.permutations(
            [
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "location_id",
                "location_group_id",
                "start_pickup_drop_off_window",
                "end_pickup_drop_off_window",
                "stop_sequence",
                "pickup_type",
                "drop_off_type",
            ]
        )
    )
    expected, no_flex = _without_flex(_outcome(base))
    assert no_flex == []
    got, stored = _without_flex(
        _outcome(
            _rezip(
                base,
                **{
                    "stop_times.txt": _csv(fieldnames, rows),
                    "locations.geojson": _ZONES,
                },
            )
        )
    )
    # A frequency template whose first stop is untimed fails to anchor, but
    # with a window ahead of it that stop is not the trip's first.
    if not (repeated and isinstance(expected, str) and "anchor" in expected):
        assert got == expected
    if not repeated and not isinstance(expected, str):
        assert stored == sorted(
            (record for row in flex if (record := _stored_flex(row)) is not None),
            key=repr,
        )

    no_stop_id = [name for name in fieldnames if name != "stop_id"]
    zone_rows = [
        {key: value for key, value in row.items() if key != "stop_id"} for row in flex
    ]
    got, _ = _without_flex(
        _outcome(
            _rezip(
                base,
                **{
                    "stop_times.txt": _csv(no_stop_id, zone_rows),
                    "locations.geojson": _ZONES,
                },
            )
        )
    )
    assert (
        got
        == _without_flex(_outcome(_rezip(base, **{"stop_times.txt": _FIXED_HEADER})))[0]
    )
