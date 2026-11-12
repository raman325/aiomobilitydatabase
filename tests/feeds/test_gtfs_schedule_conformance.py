"""GTFS Schedule conformance: the spec's rules a consumer must apply.

Each section quotes the rule it pins. Together with the descriptive-cell
oracles in ``test_properties`` these cover the empty-value defaults, parent
inheritance, the agency timezone, lone stop times, boardable journeys, the
per-stop headsign, frequency anchoring, and the file-format MUSTs.
"""

from __future__ import annotations

import codecs
import csv
import io
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.models import (
    ArrivalsQuery,
    PickupDropOffType,
    StopLocationType,
    WheelchairAccess,
)
from aiomobilitydatabase.feeds.rt import stops_from_message
from aiomobilitydatabase.feeds.static_index import StaticIndex
from aiomobilitydatabase.feeds.transit import TransitFeedHandle, _inherited_rt_stops

from tests.feeds.test_properties import _build_index_from_files
from tests.feeds.test_time_window_properties import _run

_AGENCY = "agency_id,agency_name,agency_url,agency_timezone\nA,T,https://e.com,UTC\n"
_ROUTES = "route_id,route_short_name,route_type\nR,R,3\n"
_DAILY = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
    "start_date,end_date\nS,1,1,1,1,1,1,1,20260101,20271231\n"
)
_MIDNIGHT = datetime(2026, 7, 30, tzinfo=UTC)


def _feed(**files: str) -> dict[str, str]:
    """A one-route UTC feed; keyword files (``stop_times``, ...) override."""
    base = {
        "agency.txt": _AGENCY,
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS0,A,0,0\nS1,B,0,0\n",
        "routes.txt": _ROUTES,
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\nR,S,T,Downtown\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T,08:00:00,08:00:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
        ),
        "calendar.txt": _DAILY,
    }
    base.update({f"{name}.txt": content for name, content in files.items()})
    return base


def _zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buf.getvalue()


def _build(
    tmp_path: Path, files: dict[str, str], timezone_name: str | None = None
) -> StaticIndex:
    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(_zip(files))
    return StaticIndex.build(zip_path, ":memory:", "ds", timezone_name)


def _board(
    index: StaticIndex, stop_ids: list[str], hours: int = 24
) -> list[tuple[object, ...]]:
    return [
        (dep.trip_id, dep.stop_id, dep.arrival, dep.departure)
        for dep in index.upcoming_departures(
            stop_ids, None, _MIDNIGHT, timedelta(hours=hours), 100
        )
    ]


def _journeys(index: StaticIndex, origin: str, destination: str) -> list[str]:
    return [
        trip.trip_id
        for trip in index.upcoming_trips(
            origin, destination, _MIDNIGHT, timedelta(hours=24), 100
        )
    ]


# -- D1: frequencies.txt start_time is a departure ----------------------------


def test_repetitions_depart_the_first_stop_at_their_start(tmp_path: Path) -> None:
    """start_time: "Time at which the first vehicle departs from the first
    stop". A terminal layover (arrive 08:00, depart 08:02) must not make
    every repetition two minutes late, and the run's identity (its start)
    is its first departure, as GTFS-RT's start_time reads it."""
    index = _build(
        tmp_path,
        _feed(
            stop_times=(
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T,08:00:00,08:02:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
            ),
            frequencies=(
                "trip_id,start_time,end_time,headway_secs\nT,06:00:00,06:30:00,600\n"
            ),
        ),
    )
    assert _board(index, ["S0"]) == [
        (
            f"T#{start}",
            "S0",
            _MIDNIGHT + timedelta(seconds=start - 120),
            _MIDNIGHT + timedelta(seconds=start),
        )
        for start in (21600, 22200, 22800)
    ]
    index.close()


def test_a_repetition_at_midnight_arrives_when_it_departs(tmp_path: Path) -> None:
    """A 00:00:00 repetition of a template that dwells at its first stop
    would arrive before its service day starts, a time GTFS cannot
    express; the call arrives when it departs instead."""
    index = _build(
        tmp_path,
        _feed(
            stop_times=(
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T,08:00:00,08:02:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
            ),
            frequencies=(
                "trip_id,start_time,end_time,headway_secs\nT,00:00:00,00:10:00,600\n"
            ),
        ),
    )
    eight = _MIDNIGHT + timedelta(minutes=8)
    assert _board(index, ["S0", "S1"], hours=1) == [
        ("T#0", "S0", _MIDNIGHT, _MIDNIGHT),
        ("T#0", "S1", eight, eight),
    ]
    index.close()


# -- D3: a blank cell is the value GTFS gives "empty" -------------------------

_DEFAULTED = {
    # column: (its file, the spec's value for "empty")
    "location_type": ("stops", StopLocationType.STOP),
    "wheelchair_boarding": ("stops", WheelchairAccess.UNKNOWN),
    "pickup_type": ("stop_times", PickupDropOffType.REGULAR),
    "drop_off_type": ("stop_times", PickupDropOffType.REGULAR),
}


@pytest.mark.parametrize("column", sorted(_DEFAULTED))
@pytest.mark.parametrize("form", ["absent", "blank", "spaces", "zero"])
def test_absent_blank_and_zero_cells_read_the_same(
    tmp_path: Path, column: str, form: str
) -> None:
    """ "0 or empty": an absent column, a blank cell and an explicit 0 are
    one value, while garbage stays unknown (None)."""
    table, expected = _DEFAULTED[column]
    cell = {"absent": None, "blank": "", "spaces": "  ", "zero": "0"}[form]
    files = _feed()
    if cell is not None and table == "stops":
        files["stops.txt"] = (
            f"stop_id,stop_name,stop_lat,stop_lon,{column}\n"
            f"S0,A,0,0,{cell}\nS1,B,0,0,{cell}\n"
        )
    if cell is not None and table == "stop_times":
        files["stop_times.txt"] = (
            f"trip_id,arrival_time,departure_time,stop_id,stop_sequence,{column}\n"
            f"T,08:00:00,08:00:00,S0,1,{cell}\nT,08:10:00,08:10:00,S1,2,{cell}\n"
        )
    index = _build(tmp_path, files)
    if table == "stops":
        values = [getattr(stop, column) for stop in index.stops()]
    else:
        values = [
            getattr(dep, column)
            for dep in index.upcoming_departures(
                ["S0", "S1"], None, _MIDNIGHT, timedelta(hours=24), 10
            )
        ]
    assert values == [expected, expected]
    index.close()


# -- D4: parent-station inheritance -------------------------------------------

_HIERARCHY = (
    "stop_id,stop_name,stop_lat,stop_lon,parent_station,location_type,"
    "wheelchair_boarding,stop_timezone\n"
    "STA,Station,0,0,,1,1,Europe/Berlin\n"
    "P1,Platform 1,0,0,STA,0,0,Europe/Lisbon\n"
    "P2,Platform 2,0,0,STA,0,,\n"
    "P3,Platform 3,0,0,STA,0,2,\n"
    "BA,Boarding area,0,0,P2,4,,\n"
    "BARE,Bare station,0,0,,1,0,\n"
    "Q1,Under bare,0,0,BARE,0,0,\n"
    "BAD,Bad station,0,0,,1,x,\n"
    "Q2,Under bad,0,0,BAD,0,0,\n"
    "LONE,Lone stop,0,0,,0,,\n"
    "ORPHAN,Orphan,0,0,NOWHERE,0,0,Asia/Tokyo\n"
    "C1,Cycle 1,0,0,C2,0,0,Europe/Madrid\n"
    "C2,Cycle 2,0,0,C1,0,0,Europe/Rome\n"
)


def test_child_stops_inherit_from_their_parent_station(tmp_path: Path) -> None:
    """wheelchair_boarding "0 or empty - Stop will inherit its
    wheelchair_boarding behavior from the parent station, if specified in
    the parent"; stop_timezone: "If the location has a parent station, it
    inherits the parent station's timezone instead of applying its own.
    Stations and parentless stops with empty stop_timezone inherit the
    timezone specified by agency.agency_timezone"."""
    index = _build(tmp_path, _feed(stops=_HIERARCHY))
    stops = {stop.id: stop for stop in index.stops()}
    boarding = {stop_id: stops[stop_id].wheelchair_boarding for stop_id in stops}
    zone = {stop_id: stops[stop_id].timezone for stop_id in stops}
    assert boarding["P1"] is WheelchairAccess.POSSIBLE  # 0: inherits
    assert boarding["P2"] is WheelchairAccess.POSSIBLE  # empty: inherits
    assert boarding["P3"] is WheelchairAccess.NOT_POSSIBLE  # its own
    assert boarding["BA"] is WheelchairAccess.POSSIBLE  # via its platform
    assert boarding["Q1"] is WheelchairAccess.UNKNOWN  # the parent says 0
    assert boarding["BAD"] is None  # garbage stays unknown
    assert boarding["Q2"] is WheelchairAccess.UNKNOWN  # nothing specified
    assert boarding["LONE"] is WheelchairAccess.UNKNOWN
    assert boarding["ORPHAN"] is WheelchairAccess.UNKNOWN  # no such parent
    assert zone["P1"] == "Europe/Berlin"  # the parent's, not its own
    assert zone["P2"] == "Europe/Berlin"
    assert zone["BA"] == "Europe/Berlin"
    assert zone["BARE"] == "UTC"  # a station without one: the agency's
    assert zone["Q1"] == "UTC"
    assert zone["LONE"] == "UTC"
    assert zone["ORPHAN"] == "Asia/Tokyo"  # a missing parent is no parent
    # A parent_station cycle ends instead of looping, at the last stop
    # before it would repeat.
    assert zone["C1"] == "Europe/Rome"
    assert zone["C2"] == "Europe/Madrid"
    assert boarding["C1"] is WheelchairAccess.UNKNOWN
    index.close()


def test_realtime_defined_stops_inherit_from_a_static_parent() -> None:
    """A stop only a GTFS-RT feed defines (a detour's replacement stop) is a
    stop -- RT's Stop has no location_type -- with the message's own
    wheelchair_boarding, and inherits from a static parent station by the
    same rules as a static stop."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    for stop_id, parent, boarding, zone in (
        ("UNDER", "STA", None, "Europe/Lisbon"),
        ("OWN", "STA", gtfs_realtime_pb2.Stop.NOT_AVAILABLE, ""),
        ("LONE", "", gtfs_realtime_pb2.Stop.AVAILABLE, ""),
        ("ZONED", "NOWHERE", None, "Asia/Tokyo"),
        # A realtime station and its realtime platform.
        ("RTSTA", "", gtfs_realtime_pb2.Stop.AVAILABLE, "Europe/Oslo"),
        ("RTPLAT", "RTSTA", None, "Europe/Lisbon"),
        # A realtime parent_station cycle.
        ("C3", "C4", None, "Europe/Madrid"),
        ("C4", "C3", None, "Europe/Rome"),
    ):
        entity = message.entity.add()
        entity.id = stop_id
        entity.stop.stop_id = stop_id
        entity.stop.parent_station = parent
        entity.stop.stop_timezone = zone
        if boarding is not None:
            entity.stop.wheelchair_boarding = boarding
    static = _build_index_from_files(_feed(stops=_HIERARCHY))
    try:
        stops = _inherited_rt_stops(
            stops_from_message(message), static.stops_by_id(), "UTC"
        )
    finally:
        static.close()
    assert {stop.location_type for stop in stops.values()} == {StopLocationType.STOP}
    assert stops["UNDER"].wheelchair_boarding is WheelchairAccess.POSSIBLE
    assert stops["UNDER"].timezone == "Europe/Berlin"
    assert stops["OWN"].wheelchair_boarding is WheelchairAccess.NOT_POSSIBLE
    assert stops["LONE"].wheelchair_boarding is WheelchairAccess.POSSIBLE
    assert stops["LONE"].timezone == "UTC"
    assert stops["ZONED"].wheelchair_boarding is WheelchairAccess.UNKNOWN
    assert stops["ZONED"].timezone == "Asia/Tokyo"
    assert stops["RTPLAT"].wheelchair_boarding is WheelchairAccess.POSSIBLE
    assert stops["RTPLAT"].timezone == "Europe/Oslo"
    # As for static stops: a cycle ends at the last stop before it repeats.
    assert stops["C3"].timezone == "Europe/Rome"
    assert stops["C4"].timezone == "Europe/Madrid"


# -- D6: agency.txt's timezone ------------------------------------------------


@pytest.mark.parametrize(
    ("agency_zone", "catalog_zone", "expected"),
    [
        ("America/New_York", "UTC", "America/New_York"),  # agency.txt wins
        ("America/New_York", None, "America/New_York"),
        ("", "Europe/Paris", "Europe/Paris"),  # blank: the catalog's
        ("Not/AZone", "Europe/Paris", "Europe/Paris"),  # unusable: the catalog's
    ],
)
def test_stop_times_are_in_the_agency_timezone(
    tmp_path: Path, agency_zone: str, catalog_zone: str | None, expected: str
) -> None:
    """Stop times are "in the time zone specified by agency.agency_timezone":
    a catalog's (possibly stale) zone is only a fallback for a dataset
    that names no usable one."""
    agency = (
        "agency_id,agency_name,agency_url,agency_timezone\n"
        f"A,T,https://e.com,{agency_zone}\n"
    )
    index = _build(tmp_path, _feed(agency=agency), catalog_zone)
    assert index.timezone_name == expected
    index.close()


def test_an_unreadable_agency_txt_fails_as_a_parse_error(tmp_path: Path) -> None:
    """agency.txt is read for its timezone even when a catalog supplies
    one; a file that is not UTF-8 fails the way every loader does."""
    zip_path = tmp_path / "latin1.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for name, content in _feed().items():
            if name != "agency.txt":
                archive.writestr(name, content)
        archive.writestr(
            "agency.txt",
            "agency_id,agency_name,agency_url,agency_timezone\n"
            "A,Caf\xe9,https://e.com,UTC\n".encode("latin-1"),
        )
    with pytest.raises(FeedParseError, match=r"Malformed agency\.txt"):
        StaticIndex.build(zip_path, ":memory:", "ds", "America/New_York")


def test_no_usable_timezone_anywhere_fails_the_build(tmp_path: Path) -> None:
    agency = "agency_id,agency_name,agency_url,agency_timezone\nA,T,https://e.com,X/Y\n"
    with pytest.raises(FeedParseError, match="No valid agency timezone"):
        _build(tmp_path, _feed(agency=agency), "Also/Wrong")


# -- D7: a call timed by one of arrival/departure -----------------------------


def test_a_lone_time_is_both_arrival_and_departure(tmp_path: Path) -> None:
    """departure_time is required for no stop but timepoints, so a first
    stop may carry only its (required) arrival_time; "If there are not
    separate times for arrival and departure at a stop, arrival_time and
    departure_time should be the same"."""
    index = _build(
        tmp_path,
        _feed(
            stop_times=(
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T,08:00:00,,S0,1\nT,,08:10:00,S1,2\n"
            )
        ),
    )
    eight = _MIDNIGHT + timedelta(hours=8)
    assert _board(index, ["S0", "S1"]) == [
        ("T", "S0", eight, eight),
        ("T", "S1", eight + timedelta(minutes=10), eight + timedelta(minutes=10)),
    ]
    assert _journeys(index, "S0", "S1") == ["T"]
    index.close()


@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    blanks=st.lists(
        st.sampled_from(["", "arrival", "departure"]), min_size=3, max_size=3
    )
)
def test_blanking_one_of_two_equal_times_changes_nothing(blanks: list[str]) -> None:
    """Metamorphic: on calls whose arrival equals their departure, blanking
    either one leaves every board and journey as it was."""

    def build(blank: list[str]) -> StaticIndex:
        times = ("08:00:00", "08:10:00", "08:20:00")
        rows = "".join(
            f"T,{'' if b == 'arrival' else t},"
            f"{'' if b == 'departure' else t},S{i},{i}\n"
            for i, (t, b) in enumerate(zip(times, blank, strict=True))
        )
        stops = "".join(f"S{i},Stop,0,0\n" for i in range(3))
        return _build_index_from_files(
            _feed(
                stops="stop_id,stop_name,stop_lat,stop_lon\n" + stops,
                stop_times="trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                + rows,
            )
        )

    plain, blanked = build(["", "", ""]), build(blanks)
    try:
        stops = ["S0", "S1", "S2"]
        assert _board(blanked, stops) == _board(plain, stops)
        for origin, destination in (("S0", "S1"), ("S0", "S2"), ("S1", "S2")):
            assert _journeys(blanked, origin, destination) == _journeys(
                plain, origin, destination
            )
    finally:
        plain.close()
        blanked.close()


# -- D8: journeys need a pickup and a drop-off --------------------------------


@pytest.mark.parametrize(
    ("pickup", "drop_off", "offered"),
    [
        ("", "", True),
        ("1", "", False),  # "No pickup available" at the origin
        ("", "1", False),  # "No drop off available" at the destination
        ("2", "3", True),  # phone the agency / coordinate with the driver
        ("x", "x", True),  # unknown is no evidence against boarding
    ],
)
def test_journeys_need_a_pickup_and_a_drop_off(
    tmp_path: Path, pickup: str, drop_off: str, offered: bool
) -> None:
    index = _build(
        tmp_path,
        _feed(
            stop_times=(
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence,"
                "pickup_type,drop_off_type\n"
                f"T,08:00:00,08:00:00,S0,1,{pickup},\n"
                f"T,08:10:00,08:10:00,S1,2,,{drop_off}\n"
            )
        ),
    )
    assert _journeys(index, "S0", "S1") == (["T"] if offered else [])
    # Boards keep every call: they expose pickup_type/drop_off_type.
    assert [row[0] for row in _board(index, ["S0", "S1"])] == ["T", "T"]
    assert (
        index.journeys_ceiling("S0", "S1") is not None
    ) is offered  # the open-ended scan agrees on what a journey is
    index.close()


# -- D9: the headsign at a stop -----------------------------------------------

_SIGNED = (
    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign\n"
    "T,08:00:00,08:00:00,S0,1,Airport\nT,08:10:00,08:10:00,S1,2,\n"
)


def test_the_headsign_at_a_stop_is_its_override(tmp_path: Path) -> None:
    """stop_headsign "overrides the default trips.trip_headsign when the
    headsign changes between stops": the picker offers, and the filters
    match, what the sign at that stop reads."""
    index = _build(tmp_path, _feed(stop_times=_SIGNED))
    assert index.headsigns_serving("S0") == ["Airport"]
    assert index.headsigns_serving("S1") == ["Downtown"]

    def signed(stop_id: str, sign: str) -> list[str | None]:
        return [
            dep.trip_id
            for dep in index.upcoming_departures(
                [stop_id], None, _MIDNIGHT, timedelta(hours=24), 10, headsigns=[sign]
            )
        ]

    assert signed("S0", "Airport") == ["T"]
    assert signed("S0", "Downtown") == []
    assert signed("S1", "Downtown") == ["T"]
    assert index.departures_ceiling(["S0"], headsigns=["Downtown"]) is None
    assert index.departures_ceiling(["S0"], headsigns=["Airport"]) is not None
    index.close()


def test_the_board_filters_on_the_headsign_at_the_stop() -> None:
    built = _build_index_from_files(_feed(stop_times=_SIGNED))

    async def body(handle: TransitFeedHandle) -> list[list[str | None]]:
        boards = await handle.get_arrivals(
            [
                ArrivalsQuery(["S0", "S1"], headsigns=["Airport"]),
                ArrivalsQuery(["S0", "S1"], headsigns=["Downtown"]),
            ],
            start=_MIDNIGHT,
            end=_MIDNIGHT + timedelta(hours=24),
        )
        return [[row.stop_id for row in board] for board in boards]

    assert _run(built, body) == [["S0"], ["S1"]]


# -- S6: stop_timezone never moves a time -------------------------------------


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    zones=st.lists(
        st.sampled_from(["", "Asia/Tokyo", "America/Denver", "Pacific/Kiritimati"]),
        min_size=2,
        max_size=2,
    )
)
def test_stop_timezone_never_moves_a_stop_time(zones: list[str]) -> None:
    """Stop times are "in the time zone specified by agency.agency_timezone,
    not stops.stop_timezone": any stop_timezone leaves every instant
    where it was."""
    stops = "stop_id,stop_name,stop_lat,stop_lon,stop_timezone\n" + "".join(
        f"S{i},Stop,0,0,{zone}\n" for i, zone in enumerate(zones)
    )
    plain = _build_index_from_files(_feed())
    zoned = _build_index_from_files(_feed(stops=stops))
    try:
        assert _board(zoned, ["S0", "S1"]) == _board(plain, ["S0", "S1"])
        assert _journeys(zoned, "S0", "S1") == _journeys(plain, "S0", "S1")
    finally:
        plain.close()
        zoned.close()


# -- C8: service defined by calendar_dates.txt alone --------------------------


@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    days=st.sets(st.integers(0, 20), max_size=6),
    removed=st.sets(st.integers(0, 20), max_size=3),
    probe=st.integers(-2, 22),
)
def test_service_from_calendar_dates_alone(
    days: set[int], removed: set[int], probe: int
) -> None:
    """The alternate model: "Omit calendar.txt, and specify each date of
    service in calendar_dates.txt". A service runs exactly on its added
    dates; a removal for a service with no calendar row removes nothing it
    did not add."""
    first = date(2026, 7, 1)
    rows = "".join(
        f"S,{(first + timedelta(days=d)).strftime('%Y%m%d')},1\n" for d in sorted(days)
    ) + "".join(
        f"OTHER,{(first + timedelta(days=d)).strftime('%Y%m%d')},2\n"
        for d in sorted(removed)
    )
    files = _feed(calendar_dates="service_id,date,exception_type\n" + rows)
    del files["calendar.txt"]
    index = _build_index_from_files(files)
    try:
        when = first + timedelta(days=probe)
        assert index.active_service_ids(when) == ({"S"} if probe in days else set())
    finally:
        index.close()


# -- F1-F5: file format -------------------------------------------------------


def _render(files: dict[str, str], *, bom: bool, crlf: bool) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for name, content in files.items():
        rows = list(csv.reader(io.StringIO(content)))
        buf = io.StringIO()
        csv.writer(buf, lineterminator="\r\n" if crlf else "\n").writerows(rows)
        data = buf.getvalue().encode()
        out[name] = (codecs.BOM_UTF8 if bom else b"") + data
    return out


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    bom=st.booleans(),
    crlf=st.booleans(),
    name=st.sampled_from(["Main St", 'Main "Central" St', "Main, North", "Ünïcode"]),
)
def test_bom_line_endings_and_quoting_read_the_same(
    tmp_path_factory: pytest.TempPathFactory, bom: bool, crlf: bool, name: str
) -> None:
    """Files are comma-delimited (RFC 4180 quoting), lines end in CRLF or
    LF, and a UTF-8 byte-order mark "is acceptable": none of them changes
    what is read."""
    stops = io.StringIO()
    csv.writer(stops, lineterminator="\n").writerows(
        [
            ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            ["S0", name, "0", "0"],
            ["S1", "B", "0", "0"],
        ]
    )
    files = _feed(stops=stops.getvalue())
    tmp = tmp_path_factory.mktemp("render")
    zip_path = tmp / "feed.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for file_name, data in _render(files, bom=bom, crlf=crlf).items():
            archive.writestr(file_name, data)
    index = StaticIndex.build(zip_path, ":memory:", "ds", None)
    try:
        assert {stop.id: stop.name for stop in index.stops()} == {"S0": name, "S1": "B"}
        assert _journeys(index, "S0", "S1") == ["T"]
    finally:
        index.close()


def test_field_names_are_case_sensitive(tmp_path: Path) -> None:
    """ "All file and field names are case-sensitive": a required column
    spelled with other capitals is missing, and the build says so rather
    than reading an empty column."""
    stops = "Stop_ID,stop_name,stop_lat,stop_lon\nS0,A,0,0\n"
    with pytest.raises(FeedParseError, match=r"stops\.txt"):
        _build(tmp_path, _feed(stops=stops))


def test_files_in_a_subfolder_are_missing(tmp_path: Path) -> None:
    """ "The files must reside at the root level directly, not in a
    subfolder": a zip of gtfs/*.txt is missing its required files, and
    says which -- not that it lacks a timezone."""
    zip_path = tmp_path / "nested.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for name, content in _feed().items():
            archive.writestr(f"gtfs/{name}", content)
    with pytest.raises(FeedParseError, match=r"missing required file: routes\.txt"):
        StaticIndex.build(zip_path, ":memory:", "ds", None)


def test_a_failed_file_build_leaves_no_partial_database(tmp_path: Path) -> None:
    zip_path = tmp_path / "bad.zip"
    zip_path.write_bytes(_zip(_feed(stops="stop_name\nA\n")))
    db_path = tmp_path / "index.db"
    with pytest.raises(FeedParseError):
        StaticIndex.build(zip_path, str(db_path), "ds", None)
    assert not db_path.exists()
    assert not Path(f"{db_path}.building").exists()
