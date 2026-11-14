"""GTFS-Flex feeds load: zone-only datasets, and Flex calls among fixed ones.

stops.txt is "Conditionally Required: Optional if demand-responsive zones
are defined in locations.geojson". In stop_times.txt, ``stop_id`` is
"Forbidden if stop_times.location_group_id or stop_times.location_id are
defined", and a pickup/drop-off window forbids arrival and departure
times. The index does not model those calls yet: it skips them, and every
fixed-route answer is what the feed gives without them.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.static_index import StaticIndex

from tests.feeds.test_gtfs_schedule_conformance import (
    _MIDNIGHT,
    _board,
    _build,
    _feed,
    _journeys,
)
from tests.feeds.test_properties import _index_from_zip_bytes, _random_gtfs_zip

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


def _stop_time_rows(index: StaticIndex) -> list[tuple[object, ...]]:
    return index._conn.execute(
        "SELECT trip_id, stop_id, arrival_secs, departure_secs, stop_sequence "
        "FROM stop_times ORDER BY trip_id, stop_sequence"
    ).fetchall()


# -- stops.txt is optional when zones are defined -----------------------------


def test_a_zone_only_dataset_builds(tmp_path: Path) -> None:
    """A valid demand-responsive dataset yields a handle with its routes and
    agency, and no fixed calls."""
    index = _build(tmp_path, _zone_only())
    assert [route.id for route in index.routes()] == ["R"]
    assert [agency.id for agency in index.agencies()] == ["A"]
    assert index.stops() == []
    assert _stop_time_rows(index) == []
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


# -- Flex calls are skipped, fixed calls kept ---------------------------------


def test_flex_calls_among_fixed_ones_are_skipped(tmp_path: Path) -> None:
    """A route-deviation trip (fixed stops around a zone window), a trip of
    zones and stop groups, an on-demand window at a stop, and a row naming
    no place: only the fixed calls are stored, and the deviation trip still
    runs S0 -> S1 on the board and as a journey."""
    feed = _feed(
        trips=(
            "route_id,service_id,trip_id,trip_headsign\n"
            "R,S,DEV,Downtown\nR,S,AREA,Anywhere\nR,S,ATSTOP,Here\n"
        ),
        stop_times=_FLEX_HEADER
        + "DEV,08:00:00,08:00:00,S0,,,,,1,,\n"
        + "DEV,,,,Z,,08:05:00,08:25:00,2,2,2\n"
        + "DEV,08:30:00,08:30:00,S1,,,,,3,,\n"
        + "AREA,,,,Z,,06:00:00,22:00:00,1,2,1\n"
        + "AREA,,,,,G,06:00:00,22:00:00,2,1,2\n"
        + "ATSTOP,,,S0,,,09:00:00,10:00:00,1,2,1\n"
        + "ATSTOP,,,,,,,,2,,\n",
    )
    feed["locations.geojson"] = _ZONES
    feed["location_groups.txt"] = "location_group_id\nG\n"
    feed["location_group_stops.txt"] = "location_group_id,stop_id\nG,S1\n"
    index = _build(tmp_path, feed)
    assert _stop_time_rows(index) == [
        ("DEV", "S0", 28800, 28800, 1),
        ("DEV", "S1", 30600, 30600, 3),
    ]
    assert [row[:2] for row in _board(index, ["S0", "S1"])] == [
        ("DEV", "S0"),
        ("DEV", "S1"),
    ]
    assert _journeys(index, "S0", "S1") == ["DEV"]
    index.close()


def test_a_stop_times_file_without_a_stop_id_column_builds(tmp_path: Path) -> None:
    feed = _zone_only()
    feed["stops.txt"] = _feed()["stops.txt"]
    index = _build(tmp_path, feed)
    assert _stop_time_rows(index) == []
    index.close()


def test_a_frequency_template_anchors_on_its_first_fixed_call(tmp_path: Path) -> None:
    """A window ahead of a frequency trip's first timed stop is not that
    trip's first stop: repetitions depart S0 at their start."""
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
    index.close()


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


_FLEX_TIMES = st.sampled_from(["", "06:00:00", "08:30:00", "25:00:00"])
_FLEX_IDS = st.sampled_from(["Z", "Z2", "G", "🚌"])


@st.composite
def _flex_row(
    draw: st.DrawFn, trip_ids: list[str], stop_ids: list[str]
) -> dict[str, str]:
    """A Flex-shaped stop_times row: a zone, a stop group, a window at a stop,
    or no place at all; windows in place of times, sometimes both."""
    kind = draw(st.sampled_from(["location", "group", "window_at_stop", "no_place"]))
    start = draw(_FLEX_TIMES)
    end = draw(_FLEX_TIMES)
    if kind == "window_at_stop":
        # A window is what makes a stop's call on-demand.
        start = start or "06:00:00"
    row = {
        "trip_id": draw(st.sampled_from([*trip_ids, "ghost"])),
        "stop_id": draw(st.sampled_from(stop_ids)) if kind == "window_at_stop" else "",
        "location_id": draw(_FLEX_IDS) if kind == "location" else "",
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


def _csv(fieldnames: list[str], rows: list[dict[str, str]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


@given(base=_random_gtfs_zip(), data=st.data())
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_flex_rows_change_no_fixed_route_answer(
    base: bytes, data: st.DataObject
) -> None:
    """Injecting Flex-shaped rows anywhere in stop_times.txt (with extra
    columns in any order, a zones file, and maybe a frequencies.txt) stores
    exactly what the feed without them stores -- so every fixed-route query
    answers the same -- or fails the build with the same error. Without a
    stop_id column, a file of Flex rows alone stores what an empty
    stop_times.txt does."""
    with zipfile.ZipFile(io.BytesIO(base)) as archive:
        fixed = list(
            csv.DictReader(io.StringIO(archive.read("stop_times.txt").decode()))
        )
        stop_ids = [
            row["stop_id"]
            for row in csv.DictReader(io.StringIO(archive.read("stops.txt").decode()))
        ]
    trip_ids = sorted({row["trip_id"] for row in fixed})
    if data.draw(st.booleans()):
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
    expected = _outcome(base)
    injected = _rezip(
        base,
        **{"stop_times.txt": _csv(fieldnames, rows), "locations.geojson": _ZONES},
    )
    assert _outcome(injected) == expected

    no_stop_id = [name for name in fieldnames if name != "stop_id"]
    zone_rows = [
        {key: value for key, value in row.items() if key != "stop_id"} for row in flex
    ]
    assert _outcome(
        _rezip(
            base,
            **{
                "stop_times.txt": _csv(no_stop_id, zone_rows),
                "locations.geojson": _ZONES,
            },
        )
    ) == _outcome(
        _rezip(
            base,
            **{"stop_times.txt": _FIXED_HEADER},
        )
    )
