"""GTFS-Flex records: zones, stop groups, booking rules, on the handle.

locations.geojson "must contain a FeatureCollection"; "every GeoJSON
Feature must have an id", and its geometry "must be of type" Polygon or
MultiPolygon. location_groups.txt and location_group_stops.txt name groups
of stops; booking_rules.txt says how a ride is booked. Each loads into a
model the handle exposes and replaces on refresh.
"""

from __future__ import annotations

import csv
import io
import json
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.models import (
    BookingRule,
    BookingType,
    FlexLocation,
    FlexPolygon,
    LocationGroup,
)
from aiomobilitydatabase.feeds.static_index import StaticIndex, repetition_trip_id
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.fixtures import build_flex_gtfs_zip_bytes
from tests.feeds.test_gtfs_schedule_conformance import _build, _feed, _zip
from tests.feeds.test_time_window_properties import _run

_SETTINGS = settings(
    max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)


def _flex_index(tmp_path: Path) -> StaticIndex:
    zip_path = tmp_path / "flex.zip"
    zip_path.write_bytes(build_flex_gtfs_zip_bytes())
    return StaticIndex.build(zip_path, ":memory:", "ds", None)


def _with_zones(document: object, **files: str) -> dict[str, str]:
    feed = _feed(**files)
    feed["locations.geojson"] = (
        document if isinstance(document, str) else json.dumps(document)
    )
    return feed


def _collection(*features: object) -> dict[str, object]:
    return {"type": "FeatureCollection", "features": list(features)}


_SQUARE = [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]
_SQUARE_LATLON = ((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0))


def _zone(
    zone_id: object = "Z", geometry: object = None, **extra: object
) -> dict[str, Any]:
    return {
        "type": "Feature",
        "id": zone_id,
        "properties": {},
        "geometry": geometry or {"type": "Polygon", "coordinates": [_SQUARE]},
        **extra,
    }


# -- The fixture, end to end --------------------------------------------------


_TOWN = FlexLocation(
    id="TOWN",
    name="Town",
    description="Anywhere in town but the park",
    polygons=(
        FlexPolygon(
            exterior=(
                (40.99, -87.05),
                (40.99, -86.99),
                (41.03, -86.99),
                (41.03, -87.05),
                (40.99, -87.05),
            ),
            holes=(
                (
                    (41.0, -87.02),
                    (41.01, -87.02),
                    (41.01, -87.01),
                    (41.0, -87.01),
                    (41.0, -87.02),
                ),
            ),
        ),
    ),
)
_HOSPITAL = LocationGroup(id="HOSPITAL", name="Hospital stops", stop_ids=("H1", "H2"))
_DAY_BEFORE = BookingRule(
    id="DAY_BEFORE",
    booking_type=BookingType.PRIOR_DAYS,
    prior_notice_duration_min=None,
    prior_notice_duration_max=None,
    prior_notice_last_day=1,
    prior_notice_last_secs=17 * 3600,
    prior_notice_start_day=7,
    prior_notice_start_secs=0,
    prior_notice_service_id="WEEKDAYS",
    message="Book by 5PM the business day before",
    pickup_message=None,
    drop_off_message=None,
    phone_number="555-0101",
    info_url=None,
    booking_url="https://flex.example/book",
)
_NOW = BookingRule(
    id="NOW",
    booking_type=BookingType.REAL_TIME,
    prior_notice_duration_min=None,
    prior_notice_duration_max=None,
    prior_notice_last_day=None,
    prior_notice_last_secs=None,
    prior_notice_start_day=None,
    prior_notice_start_secs=None,
    prior_notice_service_id=None,
    message="Call when ready",
    pickup_message=None,
    drop_off_message=None,
    phone_number="555-0100",
    info_url="https://flex.example/info",
    booking_url=None,
)


def test_the_flex_fixture_loads_every_record(tmp_path: Path) -> None:
    """The zone (hole kept, [lon, lat] swapped), the stop group, both
    booking rules, and every Flex call naming them."""
    index = _flex_index(tmp_path)
    assert index.flex_locations() == [_TOWN]
    assert index.location_groups() == [_HOSPITAL]
    assert index.booking_rules() == [_DAY_BEFORE, _NOW]
    assert index._conn.execute(
        "SELECT trip_id, stop_sequence, location_id, location_group_id, "
        "pickup_booking_rule_id, drop_off_booking_rule_id "
        "FROM flex_stop_times ORDER BY trip_id, stop_sequence"
    ).fetchall() == [
        ("DEV1", 2, "TOWN", None, "NOW", "NOW"),
        ("ZONE1", 1, None, "HOSPITAL", "DAY_BEFORE", None),
        ("ZONE1", 2, "TOWN", None, None, "DAY_BEFORE"),
    ]
    index.close()


def test_the_handle_exposes_flex_records_and_replaces_them_on_refresh(
    tmp_path: Path,
) -> None:
    """Loaded with the handle; a refresh to a dataset without Flex files
    publishes none, together with the new index."""
    replacement = _build(tmp_path, _feed())

    async def body(handle: TransitFeedHandle) -> list[tuple[object, ...]]:
        before = (handle.flex_locations, handle.location_groups, handle.booking_rules)
        await handle._swap_index(replacement)
        after = (handle.flex_locations, handle.location_groups, handle.booking_rules)
        return [before, after]

    assert _run(_flex_index(tmp_path), body) == [
        ((_TOWN,), (_HOSPITAL,), (_DAY_BEFORE, _NOW)),
        ((), (), ()),
    ]


def test_a_dataset_without_flex_files_has_no_flex_records(tmp_path: Path) -> None:
    index = _build(tmp_path, _feed())
    assert (index.flex_locations(), index.location_groups(), index.booking_rules()) == (
        [],
        [],
        [],
    )
    index.close()


def test_build_progress_counts_the_zones_file(tmp_path: Path) -> None:
    """locations.geojson contributes its size to build progress, which
    still ends at the total."""
    zip_path = tmp_path / "flex.zip"
    zip_path.write_bytes(build_flex_gtfs_zip_bytes())
    with zipfile.ZipFile(zip_path) as archive:
        total = sum(info.file_size for info in archive.infolist())
    calls: list[tuple[int, int | None]] = []
    StaticIndex.build(
        zip_path, ":memory:", "ds", None, lambda done, of: calls.append((done, of))
    ).close()
    assert calls[-1] == (total, total)
    assert all(done <= total and of == total for done, of in calls)


# -- locations.geojson: what fails the build ---------------------------------


@pytest.mark.parametrize(
    "document",
    [
        pytest.param("{", id="truncated"),
        pytest.param(
            '{"type": "FeatureCollection", "features": [{"type": "Feature", '
            '"id": "Z", "properties": {}, "geometry": {"type": "Polygon", '
            '"coordinates": [[[NaN, 0], [1, 0], [1, 1], [NaN, 0]]]}}]}',
            id="NaN is not JSON",
        ),
        pytest.param("[" * 100_000 + "]" * 100_000, id="nested past the stack"),
        pytest.param(b"\xff\xfe\xff", id="not text"),
        pytest.param(json.dumps(_zone()), id="a lone Feature"),
        pytest.param(json.dumps([_zone()]), id="a bare list"),
        pytest.param(json.dumps({"type": "FeatureCollection"}), id="no features"),
        pytest.param(
            json.dumps({"type": "FeatureCollection", "features": {}}),
            id="features not a list",
        ),
        pytest.param(json.dumps({"features": [_zone()]}), id="no type"),
    ],
)
def test_a_zones_file_that_is_not_a_feature_collection_fails_the_build(
    tmp_path: Path, document: str | bytes
) -> None:
    zip_path = tmp_path / "feed.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for name, content in _feed().items():
            archive.writestr(name, content)
        archive.writestr("locations.geojson", document)
    with pytest.raises(FeedParseError, match=r"locations\.geojson"):
        StaticIndex.build(zip_path, ":memory:", "ds", None)


# -- locations.geojson: what a Feature needs to name a zone -------------------


def _polygon(*rings: object) -> dict[str, object]:
    return {"type": "Polygon", "coordinates": list(rings)}


@pytest.mark.parametrize(
    "feature",
    [
        pytest.param({k: v for k, v in _zone().items() if k != "id"}, id="no id"),
        pytest.param(_zone(7), id="numeric id"),
        pytest.param(_zone(""), id="empty id"),
        pytest.param(
            _zone(geometry={"type": "Point", "coordinates": [0, 0]}), id="Point"
        ),
        pytest.param(_zone(geometry={"type": "Polygon"}), id="no coordinates"),
        pytest.param(_zone(geometry=_polygon()), id="no rings"),
        pytest.param(_zone(geometry=_polygon(_SQUARE[:-1])), id="unclosed ring"),
        pytest.param(
            _zone(geometry=_polygon([[0, 0], [1, 0], [0, 0]])), id="three positions"
        ),
        pytest.param(
            _zone(geometry=_polygon([[0, 0], [1, 0], [1, 91], [0, 0]])),
            id="latitude past 90",
        ),
        pytest.param(
            _zone(geometry=_polygon([[0, 0], [181, 0], [1, 1], [0, 0]])),
            id="longitude past 180",
        ),
        pytest.param(
            _zone(geometry=_polygon([[0, 0], [10**400, 0], [1, 1], [0, 0]])),
            id="past float range",
        ),
        pytest.param(
            _zone(geometry=_polygon([[0, 0], [True, 0], [1, 1], [0, 0]])),
            id="boolean coordinate",
        ),
        pytest.param(
            _zone(geometry=_polygon([[0, 0], ["1", 0], [1, 1], [0, 0]])),
            id="text coordinate",
        ),
        pytest.param(
            _zone(geometry=_polygon([[0, 0], [1], [1, 1], [0, 0]])),
            id="one-number position",
        ),
        pytest.param(
            _zone(geometry=_polygon(_SQUARE, [[0.2, 0.2], [0.4, 0.2], [0.2, 0.2]])),
            id="a bad hole",
        ),
        pytest.param(
            _zone(
                geometry={
                    "type": "MultiPolygon",
                    "coordinates": [[_SQUARE], [_SQUARE[:-1]]],
                }
            ),
            id="one bad MultiPolygon part",
        ),
        pytest.param(
            _zone(geometry={"type": "MultiPolygon", "coordinates": []}),
            id="empty MultiPolygon",
        ),
        pytest.param(
            _zone(geometry={"type": "MultiPolygon", "coordinates": [[]]}),
            id="a MultiPolygon part of no rings",
        ),
        pytest.param("Z", id="not an object"),
    ],
)
def test_a_feature_that_names_no_valid_zone_is_skipped(
    tmp_path: Path, feature: object
) -> None:
    """Skipped whole -- the dataset still builds, its other zones kept. One
    bad ring or part drops the Feature: the rest would be a different zone."""
    index = _build(tmp_path, _with_zones(_collection(feature, _zone("KEPT"))))
    assert [location.id for location in index.flex_locations()] == ["KEPT"]
    index.close()


def test_a_multipolygon_keeps_every_part_and_hole(tmp_path: Path) -> None:
    hole = [[0.2, 0.2], [0.2, 0.4], [0.4, 0.4], [0.2, 0.2]]
    far = [[10, 10], [11, 10], [11, 11], [10, 10]]
    index = _build(
        tmp_path,
        _with_zones(
            _collection(
                _zone(
                    geometry={
                        "type": "MultiPolygon",
                        "coordinates": [[_SQUARE, hole], [far]],
                    }
                )
            )
        ),
    )
    assert index.flex_locations()[0].polygons == (
        FlexPolygon(
            exterior=_SQUARE_LATLON,
            holes=(((0.2, 0.2), (0.4, 0.2), (0.4, 0.4), (0.2, 0.2)),),
        ),
        FlexPolygon(
            exterior=((10.0, 10.0), (10.0, 11.0), (11.0, 11.0), (10.0, 10.0)), holes=()
        ),
    )
    index.close()


def test_feature_descriptions_are_text_or_none(tmp_path: Path) -> None:
    """stop_name/stop_desc are optional text: anything else, or properties
    that are not an object, reads as no description, never a failure. An
    altitude after a position is ignored."""
    features = [
        _zone("A", properties={"stop_name": "Alpha", "stop_desc": "First"}),
        _zone("B", properties={"stop_name": 5, "stop_desc": ""}),
        _zone("C", properties=None),
        _zone(
            "D",
            geometry=_polygon([[0, 0, 9], [1, 0, 9], [1, 1, 9], [0, 0, 9]]),
        ),
    ]
    index = _build(tmp_path, _with_zones(_collection(*features)))
    locations = index.flex_locations()
    assert [(loc.id, loc.name, loc.description) for loc in locations] == [
        ("A", "Alpha", "First"),
        ("B", None, None),
        ("C", None, None),
        ("D", None, None),
    ]
    assert locations[3].polygons[0].exterior == (
        (0.0, 0.0),
        (0.0, 1.0),
        (1.0, 1.0),
        (0.0, 0.0),
    )
    index.close()


def test_a_repeated_zone_id_keeps_the_last_feature(tmp_path: Path) -> None:
    """Ids "must be unique"; where they are not, the last wins, as in
    stops.txt."""
    index = _build(
        tmp_path,
        _with_zones(
            _collection(
                _zone("Z", properties={"stop_name": "first"}),
                _zone("Z", properties={"stop_name": "second"}),
            )
        ),
    )
    assert [loc.name for loc in index.flex_locations()] == ["second"]
    index.close()


def test_a_zones_file_with_a_byte_order_mark_loads(tmp_path: Path) -> None:
    feed = _with_zones("﻿" + json.dumps(_collection(_zone())))
    index = _build(tmp_path, feed)
    assert [loc.id for loc in index.flex_locations()] == ["Z"]
    index.close()


# -- location_groups.txt, location_group_stops.txt ---------------------------


def test_location_groups_list_their_stops_in_file_order_once(tmp_path: Path) -> None:
    """A group's stops come from location_group_stops.txt in file order,
    repeats dropped (the primary key is the whole row); a group with none
    has none; a row missing either id, or naming a group
    location_groups.txt lacks, assigns nothing."""
    index = _build(
        tmp_path,
        _feed(
            location_groups="location_group_id,location_group_name\nG2,\nG1,One\n,X\n",
            location_group_stops=(
                "location_group_id,stop_id\nG1,S1\nG1,S0\nG1,S1\n,S0\nG1,\nGHOST,S0\n"
            ),
        ),
    )
    assert index.location_groups() == [
        LocationGroup(id="G1", name="One", stop_ids=("S1", "S0")),
        LocationGroup(id="G2", name=None, stop_ids=()),
    ]
    index.close()


# -- booking_rules.txt --------------------------------------------------------


def test_booking_rule_cells_parse_leniently_and_are_kept_as_stated(
    tmp_path: Path,
) -> None:
    """Unparseable cells and an unknown booking_type read as None, never a
    failure; a cell the spec forbids for the rule's type (a duration on a
    real-time rule) is still kept as the file states it; a row without a
    booking_rule_id is no rule."""
    index = _build(
        tmp_path,
        _feed(
            booking_rules=(
                "booking_rule_id,booking_type,prior_notice_duration_min,"
                "prior_notice_last_time,prior_notice_start_time\n"
                "BAD,9,soon,5PM,25:00:00\n"
                "RT,0,30,,\n"
                ",1,30,,\n"
            )
        ),
    )
    rules = {rule.id: rule for rule in index.booking_rules()}
    assert sorted(rules) == ["BAD", "RT"]
    bad, real_time = rules["BAD"], rules["RT"]
    assert (
        bad.booking_type,
        bad.prior_notice_duration_min,
        bad.prior_notice_last_secs,
        bad.prior_notice_start_secs,
    ) == (None, None, None, 25 * 3600)
    assert (real_time.booking_type, real_time.prior_notice_duration_min) == (
        BookingType.REAL_TIME,
        30,
    )
    index.close()


# -- Property strategies ------------------------------------------------------

_IDS = st.sampled_from(["A", "B", "C", "D"])
_LONGITUDES = st.one_of(st.floats(-180, 180, allow_nan=False), st.integers(-180, 180))
_LATITUDES = st.one_of(st.floats(-90, 90, allow_nan=False), st.integers(-90, 90))


@st.composite
def _rings(draw: st.DrawFn) -> list[list[float]]:
    """A closed GeoJSON ring of [lon, lat] (maybe with an altitude)."""
    positions = draw(
        st.lists(st.tuples(_LONGITUDES, _LATITUDES), min_size=3, max_size=6)
    )
    altitude = draw(st.booleans())
    ring: list[list[Any]] = [
        [lon, lat, 0] if altitude else [lon, lat] for lon, lat in positions
    ]
    return [*ring, ring[0]]


def _swapped(ring: list[list[Any]]) -> tuple[tuple[float, float], ...]:
    return tuple((float(position[1]), float(position[0])) for position in ring)


@st.composite
def _valid_features(draw: st.DrawFn) -> tuple[dict[str, Any], FlexLocation]:
    """A Feature naming a zone, and the FlexLocation it must load as."""
    parts = draw(
        st.lists(st.lists(_rings(), min_size=1, max_size=3), min_size=1, max_size=3)
    )
    multi = len(parts) > 1 or draw(st.booleans())
    geometry = (
        {"type": "MultiPolygon", "coordinates": parts}
        if multi
        else {"type": "Polygon", "coordinates": parts[0]}
    )
    name = draw(st.one_of(st.none(), st.text(max_size=5)))
    zone_id = draw(_IDS)
    feature = {
        "type": "Feature",
        "id": zone_id,
        "properties": {} if name is None else {"stop_name": name},
        "geometry": geometry,
    }
    location = FlexLocation(
        id=zone_id,
        name=name or None,
        description=None,
        polygons=tuple(
            FlexPolygon(
                exterior=_swapped(rings[0]),
                holes=tuple(_swapped(ring) for ring in rings[1:]),
            )
            for rings in parts
        ),
    )
    return feature, location


def _break_ring(ring: list[list[Any]], how: str) -> list[list[Any]]:
    if how == "unclosed":
        lon, lat = ring[0][0], ring[0][1]
        return [*ring[:-1], [lon + 1 if lon < 0 else lon - 1, lat]]
    if how == "short":
        return [ring[0], ring[1], ring[0]]
    if how == "range":
        return [ring[0], [ring[1][0], 95], *ring[2:]]
    return [ring[0], [True, ring[1][1]], *ring[2:]]


@st.composite
def _invalid_features(draw: st.DrawFn) -> dict[str, Any]:
    """A Feature that names no zone: the loader must skip it whole."""
    feature, _location = draw(_valid_features())
    how = draw(
        st.sampled_from(
            ["no id", "number id", "point", "unclosed", "short", "range", "bool"]
        )
    )
    if how == "no id":
        del feature["id"]
    elif how == "number id":
        feature["id"] = 1
    elif how == "point":
        feature["geometry"] = {"type": "Point", "coordinates": [0, 0]}
    else:
        geometry = feature["geometry"]
        parts = (
            geometry["coordinates"]
            if geometry["type"] == "MultiPolygon"
            else [geometry["coordinates"]]
        )
        part = draw(st.sampled_from(parts))
        index = draw(st.integers(0, len(part) - 1))
        part[index] = _break_ring(part[index], how)
    return feature


@st.composite
def _zones_documents(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], list[FlexLocation]]:
    """A FeatureCollection, and the zones it must load as (by id, last wins)."""
    entries = draw(
        st.lists(
            st.one_of(
                _valid_features(),
                _invalid_features().map(lambda feature: (feature, None)),
            ),
            max_size=6,
        )
    )
    expected: dict[str, FlexLocation] = {}
    for _feature, location in entries:
        if location is not None:
            expected[location.id] = location
    document = _collection(*(feature for feature, _ in entries))
    return document, [expected[zone_id] for zone_id in sorted(expected)]


def _cells[T](values: st.SearchStrategy[T], render: Any) -> st.SearchStrategy[Any]:
    return values.map(lambda value: (render(value), value))


# A descriptive integer cell and the value it must load as.
_INT_CELLS = st.one_of(
    _cells(st.integers(0, 10**6), str),
    _cells(st.integers(0, 999), lambda n: f" {n} "),
    st.sampled_from([("", None), ("x", None), ("-1", None), ("1.5", None)]),
)
_TIME_CELLS = st.one_of(
    st.tuples(st.integers(0, 47), st.integers(0, 59), st.integers(0, 59)).map(
        lambda hms: (
            f"{hms[0]:02d}:{hms[1]:02d}:{hms[2]:02d}",
            hms[0] * 3600 + hms[1] * 60 + hms[2],
        )
    ),
    st.sampled_from([("", None), ("17:60:00", None), ("5PM", None), ("8:00", None)]),
)
_TYPE_CELLS = st.one_of(
    st.integers(0, 5).map(
        lambda n: (str(n), BookingType(n) if n <= BookingType.PRIOR_DAYS else None)
    ),
    st.sampled_from([("", None), ("real", None)]),
)
# Text survives a CSV round trip except a bare carriage return (read as a
# newline) and control characters.
_TEXT = st.text(alphabet=st.characters(blacklist_categories=("Cs", "Cc")), max_size=6)
_TEXT_CELLS = _TEXT.map(lambda text: (text, text or None))

_RULE_COLUMNS: dict[str, st.SearchStrategy[Any]] = {
    "booking_type": _TYPE_CELLS,
    "prior_notice_duration_min": _INT_CELLS,
    "prior_notice_duration_max": _INT_CELLS,
    "prior_notice_last_day": _INT_CELLS,
    "prior_notice_last_time": _TIME_CELLS,
    "prior_notice_start_day": _INT_CELLS,
    "prior_notice_start_time": _TIME_CELLS,
    "prior_notice_service_id": _TEXT_CELLS,
    "message": _TEXT_CELLS,
    "pickup_message": _TEXT_CELLS,
    "drop_off_message": _TEXT_CELLS,
    "phone_number": _TEXT_CELLS,
    "info_url": _TEXT_CELLS,
    "booking_url": _TEXT_CELLS,
}
# The model field each column loads into, where it is not the column name.
_RULE_FIELDS = {
    "prior_notice_last_time": "prior_notice_last_secs",
    "prior_notice_start_time": "prior_notice_start_secs",
}


def _csv(columns: list[str], rows: list[dict[str, str]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


@st.composite
def _booking_rules_files(draw: st.DrawFn) -> tuple[str, list[BookingRule]]:
    """A booking_rules.txt (columns shuffled, some absent) and its rules."""
    present = draw(st.permutations(list(_RULE_COLUMNS)))
    present = present[: draw(st.integers(0, len(present)))]
    columns = draw(st.permutations(["booking_rule_id", *present]))
    rows: list[dict[str, str]] = []
    expected: dict[str, BookingRule] = {}
    for _ in range(draw(st.integers(0, 5))):
        rule_id = draw(st.one_of(_IDS, st.just("")))
        row = {"booking_rule_id": rule_id}
        values: dict[str, Any] = dict.fromkeys(
            (_RULE_FIELDS.get(column, column) for column in _RULE_COLUMNS), None
        )
        for column in present:
            row[column], value = draw(_RULE_COLUMNS[column])
            values[_RULE_FIELDS.get(column, column)] = value
        rows.append(row)
        if rule_id:
            expected[rule_id] = BookingRule(id=rule_id, **values)
    return _csv(columns, rows), [expected[key] for key in sorted(expected)]


@st.composite
def _location_group_files(
    draw: st.DrawFn,
) -> tuple[str, str, list[LocationGroup]]:
    """location_groups.txt, location_group_stops.txt, and their groups."""
    groups = draw(st.lists(st.tuples(st.one_of(_IDS, st.just("")), _TEXT), max_size=4))
    members = draw(
        st.lists(
            st.tuples(
                st.one_of(_IDS, st.just(""), st.just("GHOST")),
                st.sampled_from(["", "S0", "S1", "S2"]),
            ),
            max_size=8,
        )
    )
    names = {group_id: name or None for group_id, name in groups if group_id}
    stops: dict[str, dict[str, None]] = {}
    for group_id, stop_id in members:
        if group_id and stop_id:
            stops.setdefault(group_id, {})[stop_id] = None
    expected = [
        LocationGroup(
            id=group_id, name=names[group_id], stop_ids=tuple(stops.get(group_id, ()))
        )
        for group_id in sorted(names)
    ]
    return (
        _csv(
            ["location_group_id", "location_group_name"],
            [{"location_group_id": g, "location_group_name": n} for g, n in groups],
        ),
        _csv(
            ["location_group_id", "stop_id"],
            [{"location_group_id": g, "stop_id": s} for g, s in members],
        ),
        expected,
    )


def _flex_records(index: StaticIndex) -> tuple[list[Any], list[Any], list[Any]]:
    return index.flex_locations(), index.location_groups(), index.booking_rules()


# -- Properties ---------------------------------------------------------------


@given(zones=_zones_documents())
@_SETTINGS
def test_zones_load_as_drawn_with_coordinates_swapped(
    zones: tuple[dict[str, Any], list[FlexLocation]],
) -> None:
    """Every Feature naming a valid zone loads with each ring's [lon, lat]
    swapped to (latitude, longitude), holes and parts kept in order; every
    other Feature is skipped whole; a repeated id keeps the last valid one."""
    document, expected = zones
    with tempfile.TemporaryDirectory() as directory:
        index = _build(Path(directory), _with_zones(document))
    assert index.flex_locations() == expected
    index.close()


@given(rules=_booking_rules_files(), groups=_location_group_files())
@_SETTINGS
def test_booking_rules_and_location_groups_load_as_drawn(
    rules: tuple[str, list[BookingRule]],
    groups: tuple[str, str, list[LocationGroup]],
) -> None:
    """Every cell of every rule as drawn (lenient: garbage is None, an
    absent column None, the last row of an id wins); every group with its
    stops in file order, once each."""
    rules_text, expected_rules = rules
    groups_text, members_text, expected_groups = groups
    files = _feed(
        booking_rules=rules_text,
        location_groups=groups_text,
        location_group_stops=members_text,
    )
    with tempfile.TemporaryDirectory() as directory:
        index = _build(Path(directory), files)
    assert index.booking_rules() == expected_rules
    assert index.location_groups() == expected_groups
    index.close()


@given(
    zones=_zones_documents(),
    rules=_booking_rules_files(),
    groups=_location_group_files(),
)
@_SETTINGS
def test_a_cached_index_returns_the_flex_records_of_its_build(
    zones: tuple[dict[str, Any], list[FlexLocation]],
    rules: tuple[str, list[BookingRule]],
    groups: tuple[str, str, list[LocationGroup]],
) -> None:
    """open_cached over a built file serves exactly what the build served."""
    files = _with_zones(
        zones[0],
        booking_rules=rules[0],
        location_groups=groups[0],
        location_group_stops=groups[1],
    )
    with tempfile.TemporaryDirectory() as directory:
        zip_path = Path(directory) / "feed.zip"
        zip_path.write_bytes(_zip(files))
        db_path = Path(directory) / "feed.db"
        built = StaticIndex.build(zip_path, str(db_path), "ds", None)
        fresh = _flex_records(built)
        built.close()
        cached = StaticIndex.open_cached(db_path, "ds")
        assert cached is not None
        assert _flex_records(cached) == fresh == (zones[1], groups[2], rules[1])
        cached.close()


@st.composite
def _frequency_feeds(
    draw: st.DrawFn,
) -> tuple[dict[str, str], list[tuple[object, ...]]]:
    """A frequency trip whose template mixes timed fixed calls and Flex
    windows, and the flex_stop_times rows its repetitions must carry."""
    sequences = draw(st.lists(st.integers(0, 30), min_size=2, max_size=6, unique=True))
    flex_count = draw(st.integers(1, len(sequences) - 1))
    flex_sequences = set(draw(st.permutations(sequences))[:flex_count])
    fixed_sequences = sorted(set(sequences) - flex_sequences)
    first_departure = draw(st.integers(0, 20_000))
    header = (
        "trip_id,arrival_time,departure_time,stop_id,location_id,"
        "start_pickup_drop_off_window,end_pickup_drop_off_window,stop_sequence\n"
    )

    def clock(secs: int) -> str:
        return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}"

    rows = []
    for offset, sequence in enumerate(fixed_sequences):
        when = clock(first_departure + 60 * offset)
        rows.append(f"T,{when},{when},S0,,,,{sequence}\n")
    windows: dict[int, tuple[int, int]] = {}
    for sequence in sorted(flex_sequences):
        start = draw(st.integers(0, 30_000))
        windows[sequence] = (start, start + draw(st.integers(0, 3_600)))
        rows.append(
            f"T,,,,Z,{clock(windows[sequence][0])},{clock(windows[sequence][1])},{sequence}\n"
        )
    first = draw(st.integers(0, 30_000))
    headway = draw(st.integers(60, 3_600))
    end = first + draw(st.integers(1, 4)) * headway - draw(st.integers(0, headway - 1))
    files = _with_zones(
        _collection(_zone()),
        stop_times=header + "".join(draw(st.permutations(rows))),
        frequencies=(
            "trip_id,start_time,end_time,headway_secs\n"
            f"T,{clock(first)},{clock(end)},{headway}\n"
        ),
    )

    def shifted(secs: int, shift: int) -> int | None:
        return secs + shift if secs + shift >= 0 else None

    expected = [
        (
            repetition_trip_id("T", start),
            sequence,
            shifted(window[0], start - first_departure),
            shifted(window[1], start - first_departure),
        )
        for start in range(first, end, headway)
        for sequence, window in windows.items()
    ]
    return files, sorted(expected, key=repr)


@given(feed=_frequency_feeds())
@_SETTINGS
def test_every_repetition_carries_the_templates_flex_calls_shifted(
    feed: tuple[dict[str, str], list[tuple[object, ...]]],
) -> None:
    """Each repetition holds every Flex call of its template, at its
    stop_sequence, its window moved by exactly the repetition's offset from
    the first fixed departure (a time before its service day is blank); the
    template's own Flex calls are gone."""
    files, expected = feed
    with tempfile.TemporaryDirectory() as directory:
        index = _build(Path(directory), files)
    stored = index._conn.execute(
        "SELECT trip_id, stop_sequence, start_window_secs, end_window_secs "
        "FROM flex_stop_times"
    ).fetchall()
    assert sorted(stored, key=repr) == expected
    index.close()


def test_handle_records_are_read_once_per_index(tmp_path: Path) -> None:
    """The properties hand back the records loaded with the index -- the
    same tuple on every read, no query per access."""

    async def body(handle: TransitFeedHandle) -> bool:
        return handle.flex_locations is handle.flex_locations

    assert _run(_flex_index(tmp_path), body)
