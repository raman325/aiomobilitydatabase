"""Descriptive GTFS attribute loading and its typed model boundary.

Covers the attribute-surface contract: every descriptive column the models
expose (stop codes/platforms/wheelchair boarding, route agency/colors/url,
agency records, trip wheelchair/bikes/direction, stop_time pickup/drop-off/
timepoint/headsign) loads leniently -- absent columns and blank values are
None, out-of-vocabulary or garbage values are None, and NOTHING descriptive
ever fails a build -- and converts to the closed-vocabulary enums exactly
at the model boundary.
"""

import io
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from aiomobilitydatabase.feeds.models import (
    Agency,
    BikesAllowed,
    FeedInfo,
    PickupDropOffType,
    StopLocationType,
    WheelchairAccess,
)
from aiomobilitydatabase.feeds.static_index import StaticIndex

from tests.feeds.fixtures import build_gtfs_zip_bytes

DATASET = "mdb-100-202607310000"
TZ = "America/Los_Angeles"

# Thursday 2026-07-30 07:00 PDT == 14:00 UTC.
NOW = datetime(2026, 7, 30, 14, 0, tzinfo=UTC)

_CALENDAR = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
    "start_date,end_date\nWKDY,1,1,1,1,1,1,1,20260101,20271231\n"
)

# Every descriptive column populated, blanked, out-of-vocabulary, and
# garbage at least once -- the lenient-boundary matrix in one feed.
_DESCRIPTIVE_FILES = {
    "agency.txt": (
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang,"
        "agency_phone,agency_fare_url\n"
        "A1,Test Transit,https://example.com,America/Los_Angeles,en,"
        "555-0100,https://example.com/fares\n"
        "A2,Bare Transit,,America/Los_Angeles,,,\n"
    ),
    "stops.txt": (
        "stop_id,stop_name,stop_lat,stop_lon,parent_station,location_type,"
        "stop_code,platform_code,wheelchair_boarding,stop_desc,stop_url,"
        "zone_id,stop_timezone\n"
        "S1,Main St,34.05,-118.25,,0,MAIN,1A,1,Northeast corner,"
        "https://example.com/s1,Z1,America/Denver\n"
        "S2,Second Ave,34.06,-118.24,,,,,2,,,,\n"
        "S3,Depot,34.07,-118.23,,4,,,0,,,,\n"
        "S4,Odd,34.08,-118.22,,9,,,9,,,,\n"
        "S5,Bad,34.09,-118.21,,,,,x,,,,\n"
    ),
    "routes.txt": (
        "route_id,route_short_name,route_long_name,route_type,agency_id,"
        "route_color,route_text_color,route_url,route_desc,route_sort_order\n"
        "R1,10,Main Line,3,A1,FFD700,000000,https://example.com/r1,"
        "Runs along Main,5\n"
        "R2,20,Bare,3,,,,,,x\n"
    ),
    "trips.txt": (
        "route_id,service_id,trip_id,trip_headsign,wheelchair_accessible,"
        "bikes_allowed,direction_id,trip_short_name,block_id\n"
        "R1,WKDY,T1,Downtown,1,2,0,42,B1\n"
        "R1,WKDY,T2,Uptown,9,x,1,,\n"
        "R2,WKDY,T3,Loop,,,,,\n"
    ),
    "stop_times.txt": (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,"
        "pickup_type,drop_off_type,timepoint,stop_headsign\n"
        "T1,08:00:00,08:00:30,S1,1,0,1,1,Via 5th\n"
        "T1,08:10:00,08:10:30,S2,2,2,3,0,\n"
        "T2,09:00:00,09:00:30,S1,1,7,x,2,\n"
        "T2,09:10:00,09:10:30,S2,2,,,x,\n"
        "T3,10:00:00,10:00:30,S1,1,,,,\n"
        "T3,10:10:00,10:10:30,S2,2,1,0,1,Loop End\n"
    ),
    "calendar.txt": _CALENDAR,
}

# One frequency template whose trip AND stop_time descriptors must survive
# materialization into every repetition (2 reps: 06:00 and 06:10 PDT).
_FREQUENCY_DESCRIPTOR_FILES = {
    "agency.txt": _DESCRIPTIVE_FILES["agency.txt"],
    "stops.txt": _DESCRIPTIVE_FILES["stops.txt"],
    "routes.txt": _DESCRIPTIVE_FILES["routes.txt"],
    "trips.txt": (
        "route_id,service_id,trip_id,trip_headsign,wheelchair_accessible,"
        "bikes_allowed,direction_id,trip_short_name,block_id\n"
        "R1,WKDY,F1,Loop,1,2,1,7X,BLK-9\n"
    ),
    "stop_times.txt": (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,"
        "pickup_type,drop_off_type,timepoint,stop_headsign\n"
        "F1,08:00:00,08:00:00,S1,1,2,3,0,Loop Start\n"
        "F1,08:10:00,08:10:30,S2,2,1,1,1,Loop End\n"
    ),
    "calendar.txt": _CALENDAR,
    "frequencies.txt": (
        "trip_id,start_time,end_time,headway_secs\nF1,06:00:00,06:20:00,600\n"
    ),
}


def _index_from_files(
    tmp_path: Path, files: dict[str, str], timezone_name: str | None = None
) -> StaticIndex:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(buf.getvalue())
    return StaticIndex.build(zip_path, ":memory:", DATASET, timezone_name)


def test_enum_vocabularies_match_spec_ints() -> None:
    """The closed vocabularies pin the GTFS spec ints, so consumers can keep
    comparing against raw numbers (IntEnum) while getting named members.
    """
    assert WheelchairAccess.UNKNOWN == 0
    assert WheelchairAccess.POSSIBLE == 1
    assert WheelchairAccess.NOT_POSSIBLE == 2
    assert BikesAllowed.UNKNOWN == 0
    assert BikesAllowed.ALLOWED == 1
    assert BikesAllowed.NOT_ALLOWED == 2
    assert PickupDropOffType.REGULAR == 0
    assert PickupDropOffType.NONE == 1
    assert PickupDropOffType.PHONE_AGENCY == 2
    assert PickupDropOffType.COORDINATE_WITH_DRIVER == 3
    assert StopLocationType.STOP == 0
    assert StopLocationType.STATION == 1
    assert StopLocationType.ENTRANCE_EXIT == 2
    assert StopLocationType.GENERIC_NODE == 3
    assert StopLocationType.BOARDING_AREA == 4


def test_stop_descriptive_columns_and_lenient_boundary(tmp_path: Path) -> None:
    index = _index_from_files(tmp_path, _DESCRIPTIVE_FILES)
    stops = {stop.id: stop for stop in index.stops()}
    assert stops["S1"].stop_code == "MAIN"
    assert stops["S1"].platform_code == "1A"
    assert stops["S1"].wheelchair_boarding is WheelchairAccess.POSSIBLE
    assert stops["S1"].location_type is StopLocationType.STOP
    assert stops["S1"].description == "Northeast corner"
    assert stops["S1"].url == "https://example.com/s1"
    assert stops["S1"].zone_id == "Z1"
    # stop_timezone is display metadata only: stored verbatim, never used
    # in time computation (stop_times values are always agency-timezone).
    assert stops["S1"].timezone == "America/Denver"
    assert stops["S2"].stop_code is None  # blank value
    assert stops["S2"].platform_code is None
    assert stops["S2"].description is None
    assert stops["S2"].url is None
    assert stops["S2"].zone_id is None
    assert stops["S2"].timezone is None
    assert stops["S2"].wheelchair_boarding is WheelchairAccess.NOT_POSSIBLE
    assert stops["S2"].location_type is None  # blank value
    assert stops["S3"].wheelchair_boarding is WheelchairAccess.UNKNOWN
    assert stops["S3"].location_type is StopLocationType.BOARDING_AREA
    # Out-of-vocabulary ints and garbage text are None, never an error.
    assert stops["S4"].location_type is None
    assert stops["S4"].wheelchair_boarding is None
    assert stops["S5"].wheelchair_boarding is None
    index.close()


def test_route_descriptive_columns(tmp_path: Path) -> None:
    index = _index_from_files(tmp_path, _DESCRIPTIVE_FILES)
    routes = {route.id: route for route in index.routes()}
    assert routes["R1"].agency_id == "A1"
    assert routes["R1"].color == "FFD700"
    assert routes["R1"].text_color == "000000"
    assert routes["R1"].url == "https://example.com/r1"
    assert routes["R1"].description == "Runs along Main"
    assert routes["R1"].sort_order == 5
    assert routes["R2"].agency_id is None
    assert routes["R2"].color is None
    assert routes["R2"].text_color is None
    assert routes["R2"].url is None
    assert routes["R2"].description is None
    assert routes["R2"].sort_order is None  # "x": garbage, lenient -> None
    # routes_serving carries the same full record as routes().
    assert index.routes_serving("S1") == sorted(
        routes.values(), key=lambda route: route.id
    )
    index.close()


def test_agency_records_full_and_blank_fields(tmp_path: Path) -> None:
    index = _index_from_files(tmp_path, _DESCRIPTIVE_FILES)
    assert index.agencies() == [
        Agency(
            id="A1",
            name="Test Transit",
            url="https://example.com",
            timezone="America/Los_Angeles",
            lang="en",
            phone="555-0100",
            fare_url="https://example.com/fares",
        ),
        Agency(
            id="A2",
            name="Bare Transit",
            url=None,
            timezone="America/Los_Angeles",
            lang=None,
            phone=None,
            fare_url=None,
        ),
    ]
    index.close()


def test_agency_without_id_column_and_timezone_fallback_kept(tmp_path: Path) -> None:
    """A single-agency feed omitting the optional agency_id column loads with
    id None -- and the pre-existing timezone-from-agency.txt fallback still
    works off the same file (timezone_name=None here).
    """
    files = dict(_DESCRIPTIVE_FILES)
    files["agency.txt"] = (
        "agency_name,agency_url,agency_timezone\n"
        "Solo Transit,https://solo.example.com,America/Los_Angeles\n"
    )
    index = _index_from_files(tmp_path, files, timezone_name=None)
    assert index.timezone_name == "America/Los_Angeles"
    assert index.agencies() == [
        Agency(
            id=None,
            name="Solo Transit",
            url="https://solo.example.com",
            timezone="America/Los_Angeles",
            lang=None,
            phone=None,
            fare_url=None,
        )
    ]
    index.close()


def test_agencies_empty_when_agency_txt_missing(tmp_path: Path) -> None:
    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(build_gtfs_zip_bytes(omit=frozenset({"agency.txt"})))
    index = StaticIndex.build(zip_path, ":memory:", DATASET, TZ)
    assert index.agencies() == []
    index.close()


def test_absent_descriptive_columns_load_as_none(tmp_path: Path) -> None:
    """The base fixture feed predates every descriptive column: absent
    columns are None across stops/routes/departures -- except timepoint,
    whose absence means the times ARE exact.
    """
    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(build_gtfs_zip_bytes())
    index = StaticIndex.build(zip_path, ":memory:", DATASET, TZ)
    stops = {stop.id: stop for stop in index.stops()}
    assert stops["S1"].stop_code is None
    assert stops["S1"].platform_code is None
    assert stops["S1"].wheelchair_boarding is None
    assert stops["S1"].description is None
    assert stops["S1"].url is None
    assert stops["S1"].zone_id is None
    assert stops["S1"].timezone is None
    assert stops["ST1"].location_type is StopLocationType.STATION
    routes = {route.id: route for route in index.routes()}
    assert routes["R1"].agency_id is None
    assert routes["R1"].color is None
    assert routes["R1"].text_color is None
    assert routes["R1"].url is None
    assert routes["R1"].description is None
    assert routes["R1"].sort_order is None
    departures = index.upcoming_departures(
        ["S1"], None, datetime(2026, 7, 30, 14, 45, tzinfo=UTC), timedelta(hours=1), 10
    )
    assert departures
    for dep in departures:
        assert dep.wheelchair_accessible is None
        assert dep.bikes_allowed is None
        assert dep.pickup_type is None
        assert dep.drop_off_type is None
        assert dep.stop_headsign is None
        assert dep.timepoint_exact is True
        assert dep.trip_short_name is None
        assert dep.block_id is None
    # No feed_info.txt in the base fixture zip: the accessor is None, not
    # an empty record.
    assert index.feed_info() is None
    index.close()


def test_departure_stop_time_and_trip_descriptors(tmp_path: Path) -> None:
    index = _index_from_files(tmp_path, _DESCRIPTIVE_FILES)
    departures = index.upcoming_departures(
        ["S1", "S2"], None, NOW, timedelta(hours=4), 10
    )
    by_key = {(dep.trip_id, dep.stop_id): dep for dep in departures}
    t1_s1 = by_key[("T1", "S1")]
    assert t1_s1.wheelchair_accessible is WheelchairAccess.POSSIBLE
    assert t1_s1.bikes_allowed is BikesAllowed.NOT_ALLOWED
    assert t1_s1.trip_short_name == "42"
    assert t1_s1.block_id == "B1"
    assert t1_s1.pickup_type is PickupDropOffType.REGULAR
    assert t1_s1.drop_off_type is PickupDropOffType.NONE
    assert t1_s1.timepoint_exact is True
    assert t1_s1.stop_headsign == "Via 5th"
    t1_s2 = by_key[("T1", "S2")]
    assert t1_s2.pickup_type is PickupDropOffType.PHONE_AGENCY
    assert t1_s2.drop_off_type is PickupDropOffType.COORDINATE_WITH_DRIVER
    assert t1_s2.timepoint_exact is False
    assert t1_s2.stop_headsign is None  # blank value
    # T2: out-of-vocabulary/garbage descriptive values are all None.
    t2_s1 = by_key[("T2", "S1")]
    assert t2_s1.wheelchair_accessible is None  # 9: out of vocabulary
    assert t2_s1.bikes_allowed is None  # "x": garbage
    assert t2_s1.pickup_type is None  # 7: out of vocabulary
    assert t2_s1.drop_off_type is None  # "x": garbage
    assert t2_s1.timepoint_exact is None  # 2: outside the 0/1 vocabulary
    t2_s2 = by_key[("T2", "S2")]
    assert t2_s2.pickup_type is None  # blank value
    assert t2_s2.timepoint_exact is None  # "x": garbage
    # T3: blank trip-level values; blank timepoint defaults to exact.
    t3_s1 = by_key[("T3", "S1")]
    assert t3_s1.wheelchair_accessible is None
    assert t3_s1.bikes_allowed is None
    assert t3_s1.timepoint_exact is True
    assert t3_s1.trip_short_name is None  # blank value
    assert t3_s1.block_id is None
    t3_s2 = by_key[("T3", "S2")]
    assert t3_s2.pickup_type is PickupDropOffType.NONE
    assert t3_s2.drop_off_type is PickupDropOffType.REGULAR
    assert t3_s2.stop_headsign == "Loop End"
    index.close()


def test_trip_query_descriptors_both_ends(tmp_path: Path) -> None:
    """upcoming_trips carries trip-level descriptors plus the origin AND
    destination stop_time descriptors (the destination's from the arrival
    row actually ridden to).
    """
    index = _index_from_files(tmp_path, _DESCRIPTIVE_FILES)
    trips = {
        trip.trip_id: trip
        for trip in index.upcoming_trips("S1", "S2", NOW, timedelta(hours=4), 10)
    }
    t1 = trips["T1"]
    assert t1.wheelchair_accessible is WheelchairAccess.POSSIBLE
    assert t1.bikes_allowed is BikesAllowed.NOT_ALLOWED
    assert t1.direction_id == 0
    assert t1.trip_short_name == "42"
    assert t1.block_id == "B1"
    assert t1.origin_pickup_type is PickupDropOffType.REGULAR
    assert t1.origin_drop_off_type is PickupDropOffType.NONE
    assert t1.origin_timepoint_exact is True
    assert t1.origin_stop_headsign == "Via 5th"
    assert t1.destination_pickup_type is PickupDropOffType.PHONE_AGENCY
    assert t1.destination_drop_off_type is PickupDropOffType.COORDINATE_WITH_DRIVER
    assert t1.destination_timepoint_exact is False
    assert t1.destination_stop_headsign is None
    t2 = trips["T2"]
    assert t2.direction_id == 1
    assert t2.wheelchair_accessible is None
    assert t2.origin_timepoint_exact is None
    t3 = trips["T3"]
    assert t3.direction_id is None
    assert t3.destination_stop_headsign == "Loop End"
    assert t3.trip_short_name is None
    assert t3.block_id is None
    # The three trips are the day's only S1->S2 candidates, in that order.
    assert (t1.is_first, t1.is_last) == (True, False)
    assert (t2.is_first, t2.is_last) == (False, False)
    assert (t3.is_first, t3.is_last) == (False, True)
    index.close()


def test_frequency_repetitions_carry_all_descriptors(tmp_path: Path) -> None:
    """Materialization must copy trip-level AND per-stop_time descriptors
    into every repetition -- the INSERT..SELECT and template copy would
    silently drop them otherwise.
    """
    index = _index_from_files(tmp_path, _FREQUENCY_DESCRIPTOR_FILES)
    departures = index.upcoming_departures(
        ["S1", "S2"],
        None,
        datetime(2026, 7, 30, 12, 45, tzinfo=UTC),  # Thursday 05:45 PDT
        timedelta(hours=2),
        10,
    )
    by_key = {(dep.trip_id, dep.stop_id): dep for dep in departures}
    assert set(by_key) == {
        ("F1#21600", "S1"),
        ("F1#21600", "S2"),
        ("F1#22200", "S1"),
        ("F1#22200", "S2"),
    }
    for rep in ("F1#21600", "F1#22200"):
        origin = by_key[(rep, "S1")]
        assert origin.wheelchair_accessible is WheelchairAccess.POSSIBLE
        assert origin.bikes_allowed is BikesAllowed.NOT_ALLOWED
        assert origin.trip_short_name == "7X"  # template identifiers ride along
        assert origin.block_id == "BLK-9"
        assert origin.pickup_type is PickupDropOffType.PHONE_AGENCY
        assert origin.drop_off_type is PickupDropOffType.COORDINATE_WITH_DRIVER
        assert origin.timepoint_exact is False
        assert origin.stop_headsign == "Loop Start"
        terminal = by_key[(rep, "S2")]
        assert terminal.pickup_type is PickupDropOffType.NONE
        assert terminal.drop_off_type is PickupDropOffType.NONE
        assert terminal.timepoint_exact is True
        assert terminal.stop_headsign == "Loop End"
    trips = index.upcoming_trips(
        "S1", "S2", datetime(2026, 7, 30, 12, 45, tzinfo=UTC), timedelta(hours=2), 10
    )
    assert [
        (trip.trip_id, trip.direction_id, trip.is_first, trip.is_last) for trip in trips
    ] == [
        ("F1#21600", 1, True, False),
        ("F1#22200", 1, False, True),
    ]
    assert trips[0].wheelchair_accessible is WheelchairAccess.POSSIBLE
    assert trips[0].origin_stop_headsign == "Loop Start"
    assert trips[0].destination_stop_headsign == "Loop End"
    assert trips[0].trip_short_name == "7X"
    assert trips[0].block_id == "BLK-9"
    index.close()


# -- feed_info.txt ------------------------------------------------------------

_FEED_INFO_BASE = dict(_DESCRIPTIVE_FILES)


def test_feed_info_full_record(tmp_path: Path) -> None:
    files = dict(_FEED_INFO_BASE)
    files["feed_info.txt"] = (
        "feed_publisher_name,feed_publisher_url,feed_lang,feed_version,"
        "feed_start_date,feed_end_date\n"
        "Example Transit,https://example.com,en,2026.07,20260101,20271231\n"
    )
    index = _index_from_files(tmp_path, files)
    info = index.feed_info()
    assert info == FeedInfo(
        publisher_name="Example Transit",
        publisher_url="https://example.com",
        lang="en",
        version="2026.07",
        start_date=date(2026, 1, 1),
        end_date=date(2027, 12, 31),
    )
    index.close()


def test_feed_info_lenient_dates_and_blank_cells(tmp_path: Path) -> None:
    """Malformed/blank date cells and blank text cells are None -- feed_info
    is descriptive metadata and must never fail a build (wrong-length,
    non-digit, and calendar-invalid dates all degrade)."""
    for bad_start, bad_end in (
        ("garbage!", "2026073"),
        ("20261332", ""),
        ("00000101", "202607301"),
    ):
        files = dict(_FEED_INFO_BASE)
        files["feed_info.txt"] = (
            "feed_publisher_name,feed_publisher_url,feed_lang,feed_version,"
            "feed_start_date,feed_end_date\n"
            f"Example Transit,,,,{bad_start},{bad_end}\n"
        )
        index = _index_from_files(tmp_path, files)
        info = index.feed_info()
        assert info is not None
        assert info.publisher_name == "Example Transit"
        assert info.publisher_url is None
        assert info.lang is None
        assert info.version is None
        assert info.start_date is None
        assert info.end_date is None
        index.close()


def test_feed_info_missing_columns_are_none(tmp_path: Path) -> None:
    """A minimal feed_info.txt (only the required publisher columns) loads
    with every absent field None."""
    files = dict(_FEED_INFO_BASE)
    files["feed_info.txt"] = "feed_publisher_name,feed_publisher_url\nSolo Publisher,\n"
    index = _index_from_files(tmp_path, files)
    assert index.feed_info() == FeedInfo(
        publisher_name="Solo Publisher",
        publisher_url=None,
        lang=None,
        version=None,
        start_date=None,
        end_date=None,
    )
    index.close()


def test_feed_info_first_data_row_wins(tmp_path: Path) -> None:
    """GTFS defines feed_info.txt as single-record; a producer shipping
    several rows gets the FIRST one, the rest ignored (documented)."""
    files = dict(_FEED_INFO_BASE)
    files["feed_info.txt"] = (
        "feed_publisher_name,feed_publisher_url,feed_lang,feed_version,"
        "feed_start_date,feed_end_date\n"
        "First Publisher,https://first.example.com,en,1,20260101,20260131\n"
        "Second Publisher,https://second.example.com,fr,2,20260201,20260228\n"
    )
    index = _index_from_files(tmp_path, files)
    info = index.feed_info()
    assert info is not None
    assert info.publisher_name == "First Publisher"
    assert info.version == "1"
    assert info.start_date == date(2026, 1, 1)
    index.close()


def test_feed_info_header_only_file_is_none(tmp_path: Path) -> None:
    """A feed_info.txt with a header but no data rows behaves like an
    absent file: no record to expose."""
    files = dict(_FEED_INFO_BASE)
    files["feed_info.txt"] = "feed_publisher_name,feed_publisher_url,feed_lang\n"
    index = _index_from_files(tmp_path, files)
    assert index.feed_info() is None
    index.close()
