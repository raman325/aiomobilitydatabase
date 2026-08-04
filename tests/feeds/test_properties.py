"""Property-based tests (hypothesis) for timing and geo invariants."""

import asyncio
import csv
import io
import math
import tempfile
import zipfile
from datetime import UTC, date, datetime, time, timedelta
from enum import IntEnum
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import pytest
from google.transit import gtfs_realtime_pb2
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import (
    FeedParseError,
    SourceAuthenticationError,
    SourceConnectionError,
)
from aiomobilitydatabase.feeds.gbfs import (
    GbfsFeedHandle,
    _as_bool,
    _localized,
    _version_key,
)
from aiomobilitydatabase.feeds.geo import Circle, haversine_m, in_circle
from aiomobilitydatabase.feeds.models import (
    BikesAllowed,
    PickupDropOffType,
    StaticBuildProgress,
    StopArrival,
    WheelchairAccess,
)
from aiomobilitydatabase.feeds.rt import (
    _epoch_to_utc,
    _first_translation,
    alerts_from_message,
    fetch_feed_message,
    trip_updates_from_message,
    vehicles_from_message,
)
from aiomobilitydatabase.feeds.static_index import (
    ScheduledTrip,
    StaticIndex,
    parse_gtfs_time,
)
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.fixtures import (
    _FILES,
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    build_frequencies_gtfs_zip_bytes,
    build_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

# One-hour-DST zones, a no-DST zone, a half-hour-offset zone, and UTC.
TIMEZONES = [
    "America/Los_Angeles",
    "America/New_York",
    "Europe/Berlin",
    "Australia/Sydney",
    "Asia/Kolkata",
    "UTC",
]

CAL_START = date(2026, 1, 1)
CAL_END = date(2027, 12, 31)


def _build_index_from_files(files: dict[str, str]) -> StaticIndex:
    """Build a StaticIndex from in-memory GTFS files via a spooled temp zip."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as fp:
        fp.write(buf.getvalue())
        zip_path = Path(fp.name)
    try:
        return StaticIndex.build(zip_path, ":memory:", "ds-prop", None)
    finally:
        zip_path.unlink(missing_ok=True)


def _single_trip_index(tz_name: str, dep_secs: int) -> StaticIndex:
    """Minimal daily-service feed with one stop and one trip at dep_secs."""
    hours, rem = divmod(dep_secs, 3600)
    minutes, seconds = divmod(rem, 60)
    dep = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    files = {
        "agency.txt": (
            "agency_id,agency_name,agency_url,agency_timezone\n"
            f"A1,T,https://e.com,{tz_name}\n"
        ),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\nR1,ALL,T1,H\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            f"T1,{dep},{dep},S1,1\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            f"ALL,1,1,1,1,1,1,1,{CAL_START.strftime('%Y%m%d')},{CAL_END.strftime('%Y%m%d')}\n"
        ),
    }
    return _build_index_from_files(files)


def _index_from_zip_bytes(zip_bytes: bytes) -> StaticIndex:
    """Build a StaticIndex straight from raw zip bytes via a spooled temp file."""
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as fp:
        fp.write(zip_bytes)
        zip_path = Path(fp.name)
    try:
        return StaticIndex.build(zip_path, ":memory:", "ds-det", None)
    finally:
        zip_path.unlink(missing_ok=True)


def _oracle_anchor_utc(service_date: date, tz: ZoneInfo) -> datetime:
    """Independent restatement of the GTFS rule: noon minus 12h, in UTC."""
    noon = datetime.combine(service_date, time(12, 0), tzinfo=tz)
    return (noon - timedelta(hours=12)).astimezone(UTC)


@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(
    tz_name=st.sampled_from(TIMEZONES),
    query_date=st.dates(min_value=date(2026, 2, 1), max_value=date(2027, 11, 30)),
    dep_secs=st.integers(min_value=0, max_value=30 * 3600 - 1),
    lookahead_hours=st.integers(min_value=1, max_value=72),
)
def test_departures_match_elapsed_seconds_oracle(
    tz_name: str, query_date: date, dep_secs: int, lookahead_hours: int
) -> None:
    tz = ZoneInfo(tz_name)
    index = _single_trip_index(tz_name, dep_secs)
    try:
        now = datetime.combine(query_date, time(3, 0), tzinfo=UTC)
        lookahead = timedelta(hours=lookahead_hours)
        got = {
            dep.departure
            for dep in index.upcoming_departures(["S1"], None, now, lookahead, 1000)
        }
        # Oracle: every active service day whose anchor+secs falls in the window.
        expected = set()
        for offset in range(-2, lookahead_hours // 24 + 3):
            service_date = query_date + timedelta(days=offset)
            if not (CAL_START <= service_date <= CAL_END):
                continue
            instant = _oracle_anchor_utc(service_date, tz) + timedelta(seconds=dep_secs)
            if now <= instant <= now + lookahead:
                expected.add(instant)
        assert got == expected
    finally:
        index.close()


@given(
    hours=st.integers(min_value=0, max_value=47),
    minutes=st.integers(min_value=0, max_value=59),
    seconds=st.integers(min_value=0, max_value=59),
)
def test_parse_gtfs_time_round_trip(hours: int, minutes: int, seconds: int) -> None:
    value = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    assert parse_gtfs_time(value) == hours * 3600 + minutes * 60 + seconds


@given(text=st.text(alphabet=st.characters(categories=["L"]), min_size=1, max_size=8))
def test_parse_gtfs_time_rejects_garbage(text: str) -> None:
    # Letters-only: "0:00:00"-style single-digit hours are VALID GTFS, so the
    # original numeric-string strategy was a broken oracle (found 2026-07-31).
    with pytest.raises(FeedParseError):
        parse_gtfs_time(f"{text}:00:00")


def test_parse_gtfs_time_rejects_out_of_range_components() -> None:
    for bad in ("08:75:00", "08:00:99", "-1:00:00", "08:-5:00"):
        with pytest.raises(FeedParseError):
            parse_gtfs_time(bad)


@given(
    zone_lat=st.floats(min_value=-84.0, max_value=84.0),
    zone_lon=st.floats(min_value=-179.0, max_value=179.0),
    radius_m=st.floats(min_value=1.0, max_value=100_000.0),
    dlat=st.floats(min_value=-2.0, max_value=2.0),
    dlon=st.floats(min_value=-2.0, max_value=2.0),
)
def test_in_circle_equivalent_to_exact_distance(
    zone_lat: float, zone_lon: float, radius_m: float, dlat: float, dlon: float
) -> None:
    zone = Circle(latitude=zone_lat, longitude=zone_lon, radius_m=radius_m)
    lat, lon = zone_lat + dlat, zone_lon + dlon
    # The bbox prefilter may only admit candidates, never change the answer.
    assert in_circle(zone, lat, lon) == (
        haversine_m(zone_lat, zone_lon, lat, lon) <= radius_m
    )


def _normalize_lon(lon_deg: float) -> float:
    """Wrap a longitude to (-180, 180], matching real-world GPS coordinates."""
    return (lon_deg + 180.0) % 360.0 - 180.0


def _point_at(
    lat: float, lon: float, bearing_deg: float, distance_m: float
) -> tuple[float, float]:
    """Exact spherical destination point (independent forward great-circle formula).

    The returned longitude is normalized to (-180, 180] -- real-world
    coordinates are always normalized, and an earlier version of this helper
    returned the raw (potentially >180 or <-180) angle, which is precisely
    why ``test_bbox_safety_factor_admits_boundary_points`` never tripped the
    dateline-wraparound bug in ``in_circle``'s bbox prefilter (Task 15R-b
    item 1): an unnormalized point near +180 landing at e.g. 180.4 degrees
    never lands anywhere close to a zone whose longitude is a normalized
    -179.6, so the naive linear bbox comparison happened to still "work" by
    accident.
    """
    radius = 6_371_000.0  # must match geo._EARTH_RADIUS_M
    delta = distance_m / radius
    theta = math.radians(bearing_deg)
    phi1 = math.radians(lat)
    lambda1 = math.radians(lon)
    phi2 = math.asin(
        math.sin(phi1) * math.cos(delta)
        + math.cos(phi1) * math.sin(delta) * math.cos(theta)
    )
    lambda2 = lambda1 + math.atan2(
        math.sin(theta) * math.sin(delta) * math.cos(phi1),
        math.cos(delta) - math.sin(phi1) * math.sin(phi2),
    )
    return math.degrees(phi2), _normalize_lon(math.degrees(lambda2))


@settings(max_examples=500)
@given(
    zone_lat=st.floats(min_value=-84.0, max_value=84.0),
    zone_lon=st.floats(min_value=-179.0, max_value=179.0),
    radius_m=st.floats(min_value=1.0, max_value=100_000.0),
    bearing=st.floats(min_value=0.0, max_value=360.0),
    fraction=st.floats(min_value=0.90, max_value=0.999),
)
def test_bbox_safety_factor_admits_boundary_points(
    zone_lat: float, zone_lon: float, radius_m: float, bearing: float, fraction: float
) -> None:
    """Stress the 1.01 prefilter padding: points just inside the true circle
    boundary, at any bearing/latitude/radius, must NEVER be excluded. If this
    ever fails, the shrunk counterexample tells us the padding is too small --
    raise _BBOX_SAFETY rather than weakening the test.
    """
    zone = Circle(latitude=zone_lat, longitude=zone_lon, radius_m=radius_m)
    lat, lon = _point_at(zone_lat, zone_lon, bearing, fraction * radius_m)
    assert in_circle(zone, lat, lon)


# Zone longitude close enough to +-180 that an eastbound/westbound point a
# fraction of the radius away crosses the antimeridian and wraps sign.
_ZONE_LON_NEAR_DATELINE = st.builds(
    lambda abs_lon, sign: abs_lon * sign,
    st.floats(min_value=178.0, max_value=179.9),
    st.sampled_from([1.0, -1.0]),
)


@settings(max_examples=200, deadline=None)
@given(
    zone_lat=st.floats(min_value=-60.0, max_value=60.0),
    zone_lon=_ZONE_LON_NEAR_DATELINE,
    radius_m=st.floats(min_value=10_000.0, max_value=100_000.0),
    bearing=st.sampled_from(
        [90.0, 270.0]
    ),  # due east / due west: crosses the antimeridian
    # Capped at 0.995 (not 0.999) so the true-distance sanity check below has
    # headroom over floating-point rounding in the independent _point_at oracle.
    fraction=st.floats(min_value=0.90, max_value=0.995),
)
def test_in_circle_admits_points_across_the_antimeridian(
    zone_lat: float,
    zone_lon: float,
    radius_m: float,
    bearing: float,
    fraction: float,
) -> None:
    """Task 15R-b item 1: zones sitting near +-180 longitude, with a point a
    true distance well inside the radius but on the OTHER side of the
    antimeridian (e.g. zone at 179.5, point at -179.9), must still be
    admitted. ``in_circle``'s bbox prefilter compared raw (non-wrapped)
    longitude deltas, so a point that wrapped from ~180.4 to ~-179.6 looked
    (falsely) like it was ~360 degrees away instead of ~0.8 degrees away,
    and got rejected by the prefilter before haversine ever ran.

    Falsifying example captured pre-fix (verbatim, via a standalone repro
    script, not shrunk by hypothesis): zone_lat=20.0, zone_lon=179.5,
    radius_m=100_000.0, bearing=90.0 (due east), fraction=0.95 ->
    point normalizes to lon=-179.590823..., true distance=95000.0m (0.95 *
    radius, i.e. comfortably inside) but ``in_circle`` returned False.
    """
    zone = Circle(latitude=zone_lat, longitude=zone_lon, radius_m=radius_m)
    lat, lon = _point_at(zone_lat, zone_lon, bearing, fraction * radius_m)
    # Sanity-check the oracle itself: the point must actually be well inside
    # the circle by true great-circle distance, independent of in_circle.
    true_distance = haversine_m(zone_lat, zone_lon, lat, lon)
    assert true_distance <= 0.999 * radius_m
    assert in_circle(zone, lat, lon)


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    weekdays=st.lists(st.booleans(), min_size=7, max_size=7),
    added=st.lists(
        st.dates(min_value=CAL_START, max_value=CAL_END), max_size=3, unique=True
    ),
    removed=st.lists(
        st.dates(min_value=CAL_START, max_value=CAL_END), max_size=3, unique=True
    ),
    probe=st.dates(min_value=date(2025, 12, 1), max_value=date(2028, 1, 31)),
)
def test_active_service_ids_matches_naive_oracle(
    weekdays: list[bool],
    added: list[date],
    removed: list[date],
    probe: date,
) -> None:
    bits = ",".join("1" if flag else "0" for flag in weekdays)
    exceptions = [(d, 1) for d in added] + [(d, 2) for d in removed if d not in added]
    exception_rows = "".join(
        f"SVC,{d.strftime('%Y%m%d')},{etype}\n" for d, etype in exceptions
    )
    files = {
        "agency.txt": (
            "agency_id,agency_name,agency_url,agency_timezone\nA1,T,https://e.com,UTC\n"
        ),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,L,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\nR1,SVC,T1,H\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,08:00:00,08:00:00,S1,1\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            f"SVC,{bits},{CAL_START.strftime('%Y%m%d')},{CAL_END.strftime('%Y%m%d')}\n"
        ),
        "calendar_dates.txt": "service_id,date,exception_type\n" + exception_rows,
    }
    index = _build_index_from_files(files)
    try:
        # Naive oracle, restated from the GTFS rules.
        in_range = CAL_START <= probe <= CAL_END
        base_active = in_range and weekdays[probe.weekday()]
        exception_map = dict(exceptions)
        if exception_map.get(probe) == 1:
            oracle_active = True
        elif exception_map.get(probe) == 2:
            oracle_active = False
        else:
            oracle_active = base_active
        assert index.active_service_ids(probe) == ({"SVC"} if oracle_active else set())
    finally:
        index.close()


# --- messy-format properties (Task 13b) ---

_JSONISH = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False)
    | st.text(max_size=20),
    lambda children: (
        st.lists(children, max_size=4)
        | st.dictionaries(st.text(max_size=8), children, max_size=4)
    ),
    max_leaves=10,
)


@given(value=_JSONISH)
def test_localized_is_total(value: object) -> None:
    result = _localized(value)
    assert result is None or isinstance(result, str)


@given(
    entries=st.lists(
        st.fixed_dictionaries(
            {
                "text": st.text(min_size=1, max_size=20),
                "language": st.sampled_from(["de", "fr", "es"]),
            }
        ),
        min_size=1,
        max_size=4,
    ),
    en_text=st.text(min_size=1, max_size=20),
    include_en=st.booleans(),
)
def test_localized_prefers_en_else_first(
    entries: list[dict[str, str]], en_text: str, include_en: bool
) -> None:
    payload = list(entries)
    if include_en:
        payload.insert(len(payload) // 2, {"text": en_text, "language": "en"})
    result = _localized(payload)
    assert result == (en_text if include_en else entries[0]["text"])


@given(version=st.text(max_size=12))
def test_version_key_is_total(version: str) -> None:
    key = _version_key(version)
    assert isinstance(key, tuple)


def test_version_key_orders_numerically() -> None:
    assert _version_key("10.0") > _version_key("9.5")
    assert _version_key("3.0") > _version_key("2.3")


@given(
    info_ids=st.lists(st.text(min_size=1, max_size=6), max_size=5, unique=True),
    status_ids=st.lists(st.text(min_size=1, max_size=6), max_size=5, unique=True),
    data=st.data(),
)
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_station_merge_invariants(
    info_ids: list[str], status_ids: list[str], data: st.DataObject
) -> None:
    """Merged stations: ids come only from information; never raises."""
    info_rows = [
        {
            "station_id": sid,
            "name": data.draw(st.text(max_size=10) | st.none()),
            "lat": data.draw(st.floats(-85, 85) | st.none()),
            "lon": data.draw(st.floats(-179, 179) | st.none()),
        }
        for sid in info_ids
    ]
    status_rows = [
        {
            "station_id": sid,
            "num_bikes_available": data.draw(st.integers(0, 50) | st.none()),
            "is_renting": data.draw(st.sampled_from([0, 1, True, False]) | st.none()),
        }
        for sid in status_ids
    ]
    handle = GbfsFeedHandle.__new__(GbfsFeedHandle)
    handle._doc_cache = {
        "station_information": (
            float("inf"),
            float("inf"),
            {"data": {"stations": info_rows}},
        ),
        "station_status": (
            float("inf"),
            float("inf"),
            {"data": {"stations": status_rows}},
        ),
    }
    handle._endpoints = {"station_information": "x", "station_status": "x"}
    stations = asyncio.run(handle.get_stations())
    assert {s.id for s in stations} == {str(sid) for sid in info_ids}


def _maybe_fill_vehicle_entity(
    entity: gtfs_realtime_pb2.FeedEntity, data: st.DataObject
) -> bool:
    """Fill a vehicle entity; return whether a position was set (i.e.
    whether vehicles_from_message should surface this entity at all).
    """
    has_position = data.draw(st.booleans())
    if has_position:
        entity.vehicle.position.latitude = data.draw(st.floats(-90, 90))
        entity.vehicle.position.longitude = data.draw(st.floats(-180, 180))
    if data.draw(st.booleans()):
        entity.vehicle.trip.trip_id = data.draw(st.text(max_size=6))
    if data.draw(st.booleans()):
        entity.vehicle.vehicle.id = data.draw(st.text(max_size=6))
    return has_position


# Sampled TripDescriptor.start_time values with their expected parsed
# start_secs component: absent and garbage both key as None.
_START_TIME_SAMPLES: dict[str, int | None] = {
    "": None,
    "06:10:00": 22200,
    "25:00:00": 90000,
    "not-a-time": None,
}


def _maybe_fill_trip_update_entity(
    entity: gtfs_realtime_pb2.FeedEntity, data: st.DataObject
) -> set[tuple[str, int | None, str]]:
    """Fill a trip_update entity; return the (trip_id, start_secs, stop_id)
    keys it could contribute as predictions (empty when canceled or added,
    since those entities never populate ``updates.predictions``).
    """
    trip_id = data.draw(st.text(max_size=6))
    entity.trip_update.trip.trip_id = trip_id
    start_time = data.draw(st.sampled_from(sorted(_START_TIME_SAMPLES)))
    if start_time:
        entity.trip_update.trip.start_time = start_time
    start_secs = _START_TIME_SAMPLES[start_time]
    relationship = data.draw(st.sampled_from([0, 1, 2, 3, 5]))
    entity.trip_update.trip.schedule_relationship = relationship
    is_prediction_eligible = relationship not in (
        gtfs_realtime_pb2.TripDescriptor.CANCELED,
        gtfs_realtime_pb2.TripDescriptor.ADDED,
    )
    possible_keys: set[tuple[str, int | None, str]] = set()
    for _ in range(data.draw(st.integers(0, 2))):
        stu = entity.trip_update.stop_time_update.add()
        stop_id = ""
        if data.draw(st.booleans()):
            stop_id = data.draw(st.text(max_size=6))
            stu.stop_id = stop_id
        if data.draw(st.booleans()):
            stu.arrival.time = data.draw(st.integers(0, 2_000_000_000))
        if data.draw(st.booleans()):
            stu.departure.delay = data.draw(st.integers(-3600, 3600))
        if is_prediction_eligible:
            possible_keys.add((trip_id, start_secs, stop_id))
    return possible_keys


def _maybe_fill_alert_entity(
    entity: gtfs_realtime_pb2.FeedEntity, data: st.DataObject
) -> None:
    if data.draw(st.booleans()):
        translation = entity.alert.header_text.translation.add()
        translation.text = data.draw(st.text(max_size=10))
    if data.draw(st.booleans()):
        informed = entity.alert.informed_entity.add()
        informed.route_id = data.draw(st.text(max_size=6))


@given(data=st.data())
@settings(max_examples=100, deadline=None)
def test_rt_parsers_are_total_over_field_presence(data: st.DataObject) -> None:
    """Random field-presence combinations must never crash any RT parser."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    fillers = {
        "vehicle": _maybe_fill_vehicle_entity,
        "trip_update": _maybe_fill_trip_update_entity,
        "alert": _maybe_fill_alert_entity,
        "empty": None,
    }
    expected_vehicle_count = 0
    possible_prediction_keys: set[tuple[str, int | None, str]] = set()
    for i in range(data.draw(st.integers(0, 4))):
        entity = msg.entity.add()
        entity.id = f"e{i}"
        kind = data.draw(st.sampled_from(list(fillers)))
        filler = fillers[kind]
        if filler is None:
            continue
        result = filler(entity, data)
        if kind == "vehicle":
            if result:
                expected_vehicle_count += 1
        elif kind == "trip_update":
            possible_prediction_keys |= result
    vehicles = vehicles_from_message(msg, route_names={}, trip_routes={})
    assert len(vehicles) == expected_vehicle_count
    for vehicle in vehicles:
        assert vehicle.latitude is not None and vehicle.longitude is not None
    updates = trip_updates_from_message(msg)
    assert updates.canceled_trips.isdisjoint(
        {(trip_id, start_secs) for trip_id, start_secs, _ in updates.predictions}
    )
    assert set(updates.predictions.keys()) <= possible_prediction_keys
    alerts_from_message(msg)  # must simply not raise


_GARBAGE_NUMERICS = ["abc", "1.2.3", "NaN?", "--", "1e999x", " ", "12a"]


@given(
    target=st.sampled_from(
        [
            ("stops.txt", "stop_lat"),
            ("routes.txt", "route_type"),
            ("stop_times.txt", "stop_sequence"),
            ("calendar_dates.txt", "exception_type"),
        ]
    ),
    garbage=st.sampled_from(_GARBAGE_NUMERICS),
)
@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_build_error_contract_on_garbage_values(
    target: tuple[str, str], garbage: str
) -> None:
    """build() either succeeds or raises FeedParseError — never raw ValueError."""
    filename, column = target
    original = _FILES[filename]
    reader = csv.DictReader(io.StringIO(original))
    rows = list(reader)
    assert reader.fieldnames is not None
    rows[0][column] = garbage
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(reader.fieldnames))
    writer.writeheader()
    writer.writerows(rows)
    corrupted = dict(_FILES)
    corrupted[filename] = out.getvalue()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in corrupted.items():
            zf.writestr(name, content)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as fp:
        fp.write(buf.getvalue())
        zip_path = Path(fp.name)
    try:
        try:
            index = StaticIndex.build(zip_path, ":memory:", "ds-garbage", "UTC")
            index.close()
        except FeedParseError:
            pass  # the ONLY acceptable failure type
    finally:
        zip_path.unlink(missing_ok=True)


# --- status-code totality (Task 13c): the input space is NOT finite, so prove
# --- totality over it instead of enumerating the handled subset.

_FEED_FETCH_ALLOWED = (SourceAuthenticationError, SourceConnectionError, FeedParseError)


def _run_fetch_probe(status: int, body: bytes, content_type: str) -> str:
    """Serve one scripted response and classify the fetch outcome."""

    async def scenario() -> str:
        api = MockApi()
        await api.start()
        try:
            api.get("/rt", status=status, body=body, content_type=content_type)
            async with aiohttp.ClientSession() as session:
                try:
                    await fetch_feed_message(session, api.url("/rt"))
                except _FEED_FETCH_ALLOWED:
                    return "ours"
                return "success"
        finally:
            await api.stop()

    return asyncio.run(scenario())


# 1xx statuses are excluded: aiohttp's web.Response (which the in-repo mock
# server uses) does not support serving informational responses as a normal
# handler return value, so status < 200 is untestable through a real HTTP
# round-trip here. 200-599 is the servable range and is what a real producer
# can actually send.
@given(
    status=st.integers(min_value=200, max_value=599),
    body=st.binary(max_size=64),
    content_type=st.sampled_from(
        ["application/octet-stream", "application/json", "text/html", "text/plain"]
    ),
)
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_rt_fetch_total_over_status_and_body(
    status: int, body: bytes, content_type: str
) -> None:
    outcome = _run_fetch_probe(status, body, content_type)
    assert outcome in ("success", "ours")  # anything else already raised out


def _run_gbfs_probe(status: int, body: bytes, content_type: str) -> str:
    """Serve one scripted response and classify the _document outcome."""

    async def scenario() -> str:
        api = MockApi()
        await api.start()
        try:
            api.get(
                "/gbfs/doc.json", status=status, body=body, content_type=content_type
            )
            async with MobilityFeedsClient("t", base_url=api.url()) as client:
                handle = GbfsFeedHandle(
                    client,
                    feed=None,
                    endpoints={"system_information": api.url("/gbfs/doc.json")},
                )
                try:
                    await handle._document("system_information")
                except (SourceConnectionError, FeedParseError):
                    return "ours"
                return "success"
        finally:
            await api.stop()

    return asyncio.run(scenario())


@given(
    status=st.integers(min_value=200, max_value=599),
    body=st.binary(max_size=64),
    content_type=st.sampled_from(["application/json", "text/html"]),
)
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_gbfs_document_total_over_status_and_body(
    status: int, body: bytes, content_type: str
) -> None:
    outcome = _run_gbfs_probe(status, body, content_type)
    assert outcome in ("success", "ours")


# --- generative feed-data properties (Task 13d) ---


@given(epoch=st.integers(min_value=0, max_value=2**63 - 1))
def test_epoch_to_utc_is_total(epoch: int) -> None:
    result = _epoch_to_utc(epoch)
    assert result is None or result.tzinfo is not None


@given(
    raw=st.sampled_from([True, False, 0, 1, 2, "true", "false", "yes", "", None, 1.0])
)
def test_station_bool_coercion_never_lies(raw: object) -> None:
    result = _as_bool(raw)
    assert result in (True, False, None)
    if isinstance(raw, str):
        assert result is None  # bool("false") is True — strings are UNKNOWN, not truthy


_IDS = st.text(
    # EN DASH is intentional: exercising unicode-in-id robustness, not a typo.
    alphabet=st.characters(categories=["L", "N"], include_characters="–_🚌"),  # noqa: RUF001
    min_size=1,
    max_size=8,
)


@st.composite
def _random_gtfs_zip(draw: st.DrawFn) -> bytes:
    """A structurally coherent-but-messy GTFS feed: orphans, dupes, unicode,
    shuffled columns, boundary times, calendar absurdities.
    """
    stop_ids = draw(st.lists(_IDS, min_size=1, max_size=5, unique=True))
    route_ids = draw(st.lists(_IDS, min_size=1, max_size=3, unique=True))
    service_ids = draw(st.lists(_IDS, min_size=1, max_size=3, unique=True))
    trip_ids = draw(st.lists(_IDS, min_size=1, max_size=6, unique=True))
    fake = draw(_IDS)

    def maybe_fake(pool: list[str]) -> str:
        return draw(st.sampled_from([*pool, fake]))  # referential orphans

    def clean_name() -> str:
        # Strip CSV-framing-breaking characters: comma (field sep) and
        # bare CR/LF (row terminators) would otherwise shift columns.
        raw = draw(st.text(max_size=12))
        for bad in (",", "\n", "\r"):
            raw = raw.replace(bad, " ")
        return raw

    stops_rows = [
        f"{sid},{clean_name()},"
        f"{draw(st.sampled_from(['34.05', '999', '-999', '']))},"
        f"{draw(st.sampled_from(['-118.25', '181', '']))},,"
        for sid in stop_ids
    ]
    if draw(st.booleans()):
        stops_rows.append(stops_rows[0])  # duplicate stop row
    trips_rows = [
        f"{maybe_fake(route_ids)},{maybe_fake(service_ids)},{tid},"
        f"{draw(st.sampled_from(['Downtown', '終点🚌', '']))}"
        for tid in trip_ids
    ]
    times = ["00:00:00", "08:00:00", "24:00:00", "47:59:59", ""]
    stop_times_rows = []
    for tid in trip_ids:
        for seq in range(draw(st.integers(1, 3))):
            stop_times_rows.append(
                f"{maybe_fake([tid])},{draw(st.sampled_from(times))},"
                f"{draw(st.sampled_from(times))},{maybe_fake(stop_ids)},{seq}"
            )
    cal_rows = [
        f"{sid},1,1,1,1,1,{draw(st.sampled_from(['0', '1']))},1,"
        f"{draw(st.sampled_from(['20260101', '20270101']))},"
        f"{draw(st.sampled_from(['20271231', '20250101']))}"  # end may precede start
        for sid in service_ids[:-1] or service_ids
    ]
    files = {
        "agency.txt": (
            "agency_id,agency_name,agency_url,agency_timezone\nA1,T,https://e.com,UTC\n"
        ),
        "stops.txt": (
            "stop_id,stop_name,stop_lat,stop_lon,parent_station,location_type\n"
            + "\n".join(stops_rows)
            + "\n"
        ),
        "routes.txt": "route_id,route_short_name,route_long_name,route_type,extra_col\n"
        + "\n".join(f"{rid},{rid},Line {rid},3,x" for rid in route_ids)
        + "\n",
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\n"
        + "\n".join(trips_rows)
        + "\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        + "\n".join(stop_times_rows)
        + "\n",
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            + "\n".join(cal_rows)
            + "\n"
        ),
        "calendar_dates.txt": "service_id,date,exception_type\n"
        + (f"{service_ids[-1]},20260704,1\n" if len(service_ids) > 1 else ""),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


# --- exhaustive property sweep (Task 13e) ---


@given(zip_bytes=_random_gtfs_zip(), data=st.data())
@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_upcoming_departures_deterministic(
    zip_bytes: bytes, data: st.DataObject
) -> None:
    """Identical queries must return identical row sequences (HA sensors must
    not flap between tied departures).
    """
    try:
        index = _index_from_zip_bytes(zip_bytes)
    except FeedParseError:
        return  # acceptable outcome for genuinely unbuildable feeds
    try:
        stops = [s.id for s in index.stops()]
        if not stops:
            return
        queried = data.draw(
            st.lists(st.sampled_from(stops), min_size=1, max_size=3, unique=True)
        )
        now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
        first = index.upcoming_departures(queried, None, now, timedelta(hours=30), 5)
        second = index.upcoming_departures(queried, None, now, timedelta(hours=30), 5)
        assert first == second
        assert first == sorted(
            first, key=lambda dep: (dep.departure, dep.trip_id, dep.stop_id)
        )
    finally:
        index.close()


@given(zip_bytes=_random_gtfs_zip(), data=st.data())
@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_upcoming_trips_invariants(zip_bytes: bytes, data: st.DataObject) -> None:
    """Over messy generated feeds: identical origin→destination queries are
    deterministic, echo the queried stop pair, keep every origin departure
    tz-aware and inside the window, stay sorted by the total key, and
    respect the limit. (Departure <= arrival is deliberately NOT asserted:
    generated feeds may contain non-monotonic stop times, and the query
    reports the schedule as-is rather than repairing producer errors.)
    """
    try:
        index = _index_from_zip_bytes(zip_bytes)
    except FeedParseError:
        return  # acceptable outcome for genuinely unbuildable feeds
    try:
        stops = [s.id for s in index.stops()]
        if not stops:
            return
        origin = data.draw(st.sampled_from(stops))
        destination = data.draw(st.sampled_from(stops))
        now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
        lookahead = timedelta(hours=30)
        first = index.upcoming_trips(origin, destination, now, lookahead, 5)
        second = index.upcoming_trips(origin, destination, now, lookahead, 5)
        assert first == second
        assert len(first) <= 5
        for trip in first:
            assert trip.origin_stop_id == origin
            assert trip.destination_stop_id == destination
            assert trip.departure.tzinfo is not None
            assert trip.arrival.tzinfo is not None
            assert now <= trip.departure <= now + lookahead
        assert first == sorted(
            first, key=lambda trip: (trip.departure, trip.trip_id, trip.arrival)
        )
    finally:
        index.close()


# --- descriptive attribute-surface properties --------------------------------

# Cells a real producer might put in a DESCRIPTIVE integer column: ints in
# and around every vocabulary (incl. negative), huge ints, decimals,
# garbage text, empty, whitespace-only.
_INT_CELL = st.one_of(
    st.integers(min_value=-5, max_value=12).map(str),
    st.sampled_from(["", " ", "x", "1.5", "abc", "999999999999", "--"]),
)
# Text-column cells: empty (-> None) or CSV-safe text kept verbatim
# (including whitespace-only and digit-only values).
_TEXT_CELL = st.one_of(
    st.just(""),
    st.text(
        alphabet=st.characters(categories=["L", "N"], include_characters=" -_"),
        min_size=1,
        max_size=8,
    ),
)


def _expected_lenient_int(cell: str) -> int | None:
    """Oracle for descriptive int cells: blank/garbage -> None, parseable
    ints kept as-is (even outside every vocabulary)."""
    cell = cell.strip()
    if not cell:
        return None
    try:
        return int(cell)
    except ValueError:
        return None


def _expected_enum(enum_cls: type[IntEnum], cell: str) -> IntEnum | None:
    """Oracle for closed-vocabulary cells: in-vocabulary int -> the exact
    member; anything else -> None."""
    parsed = _expected_lenient_int(cell)
    members = {member.value: member for member in enum_cls}
    return None if parsed is None else members.get(parsed)


def _expected_text(cell: str) -> str | None:
    """Oracle for text cells: verbatim pass-through, empty -> None."""
    return cell or None


def _expected_timepoint(cell: str | None) -> bool | None:
    """Oracle for the timepoint tri-state: absent column (None) or blank ->
    True (GTFS default: times are exact), 0 -> False, 1 -> True, anything
    else -> None."""
    if cell is None or not cell.strip():
        return True
    parsed = _expected_lenient_int(cell)
    return {0: False, 1: True}.get(parsed) if parsed is not None else None


_ONE_DAY_CALENDAR = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
    "start_date,end_date\nONE,1,1,1,1,1,1,1,20260730,20260730\n"
)
_UTC_AGENCY = (
    "agency_id,agency_name,agency_url,agency_timezone\nA1,T,https://e.com,UTC\n"
)

_DESCRIPTIVE_ROW_CELLS = st.fixed_dictionaries(
    {
        "wheelchair_boarding": _INT_CELL,
        "wheelchair_accessible": _INT_CELL,
        "bikes_allowed": _INT_CELL,
        "direction_id": _INT_CELL,
        "pickup_type": _INT_CELL,
        "drop_off_type": _INT_CELL,
        "timepoint": _INT_CELL,
        "stop_code": _TEXT_CELL,
        "platform_code": _TEXT_CELL,
        "stop_headsign": _TEXT_CELL,
        "agency_id": _TEXT_CELL,
        "route_color": _TEXT_CELL,
        "route_text_color": _TEXT_CELL,
        "route_url": _TEXT_CELL,
    }
)


@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(cells=_DESCRIPTIVE_ROW_CELLS)
def test_descriptive_cells_parse_total_and_map_per_rules(cells: dict[str, str]) -> None:
    """Arbitrary producer cells in EVERY descriptive column at once: the
    build never raises, in-vocabulary ints surface as the exact enum member
    (matching .value), everything else (out-of-vocabulary, negative, huge,
    decimal, garbage, blank) is None, open-vocabulary direction_id keeps
    any parseable int verbatim, and text columns pass through verbatim with
    empty -> None (whitespace-only text is kept, not blanked).
    """
    files = {
        "agency.txt": _UTC_AGENCY,
        "stops.txt": (
            "stop_id,stop_name,stop_lat,stop_lon,stop_code,platform_code,"
            "wheelchair_boarding\n"
            f"S1,A,0,0,{cells['stop_code']},{cells['platform_code']},"
            f"{cells['wheelchair_boarding']}\n"
            "S2,B,0,0,,,\n"
        ),
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type,agency_id,"
            "route_color,route_text_color,route_url\n"
            f"R1,1,Line,3,{cells['agency_id']},{cells['route_color']},"
            f"{cells['route_text_color']},{cells['route_url']}\n"
        ),
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,wheelchair_accessible,"
            "bikes_allowed,direction_id\n"
            f"R1,ONE,T1,H,{cells['wheelchair_accessible']},"
            f"{cells['bikes_allowed']},{cells['direction_id']}\n"
        ),
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,"
            "pickup_type,drop_off_type,timepoint,stop_headsign\n"
            f"T1,08:00:00,08:00:00,S1,1,{cells['pickup_type']},"
            f"{cells['drop_off_type']},{cells['timepoint']},"
            f"{cells['stop_headsign']}\n"
            "T1,08:10:00,08:10:00,S2,2,,,,\n"
        ),
        "calendar.txt": _ONE_DAY_CALENDAR,
    }
    index = _build_index_from_files(files)  # totality: must never raise
    try:
        stop = next(s for s in index.stops() if s.id == "S1")
        assert stop.stop_code == _expected_text(cells["stop_code"])
        assert stop.platform_code == _expected_text(cells["platform_code"])
        assert stop.wheelchair_boarding is _expected_enum(
            WheelchairAccess, cells["wheelchair_boarding"]
        )
        (route,) = index.routes()
        assert route.agency_id == _expected_text(cells["agency_id"])
        assert route.color == _expected_text(cells["route_color"])
        assert route.text_color == _expected_text(cells["route_text_color"])
        assert route.url == _expected_text(cells["route_url"])
        now = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)
        (trip,) = index.upcoming_trips("S1", "S2", now, timedelta(hours=24), 10)
        assert trip.wheelchair_accessible is _expected_enum(
            WheelchairAccess, cells["wheelchair_accessible"]
        )
        assert trip.bikes_allowed is _expected_enum(
            BikesAllowed, cells["bikes_allowed"]
        )
        assert trip.direction_id == _expected_lenient_int(cells["direction_id"])
        assert trip.origin_pickup_type is _expected_enum(
            PickupDropOffType, cells["pickup_type"]
        )
        assert trip.origin_drop_off_type is _expected_enum(
            PickupDropOffType, cells["drop_off_type"]
        )
        assert trip.origin_timepoint_exact == _expected_timepoint(cells["timepoint"])
        assert trip.origin_stop_headsign == _expected_text(cells["stop_headsign"])
        # Non-None enum results are exact members whose .value round-trips
        # to the raw cell int.
        for member, cell in (
            (stop.wheelchair_boarding, cells["wheelchair_boarding"]),
            (trip.bikes_allowed, cells["bikes_allowed"]),
            (trip.origin_pickup_type, cells["pickup_type"]),
        ):
            if member is not None:
                assert member.value == int(cell)
    finally:
        index.close()


@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    column_present=st.booleans(),
    cells=st.lists(
        st.sampled_from(["", " ", "0", "1", "01", "2", "7", "-1", "x", "1.5"]),
        min_size=1,
        max_size=5,
    ),
)
def test_timepoint_tristate_oracle(column_present: bool, cells: list[str]) -> None:
    """timepoint is a tri-state at the model boundary: True for an absent
    COLUMN (whole file without the header) and for blank cells (GTFS
    default: times are exact), False for 0, True for 1, None for anything
    outside the 0/1 vocabulary.
    """
    if column_present:
        header = "trip_id,arrival_time,departure_time,stop_id,stop_sequence,timepoint\n"
        rows = "".join(
            f"T{i},08:00:00,08:00:00,S1,1,{cell}\n" for i, cell in enumerate(cells)
        )
    else:
        header = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        rows = "".join(f"T{i},08:00:00,08:00:00,S1,1\n" for i in range(len(cells)))
    files = {
        "agency.txt": _UTC_AGENCY,
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,A,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\n"
        + "".join(f"R1,ONE,T{i},H\n" for i in range(len(cells))),
        "stop_times.txt": header + rows,
        "calendar.txt": _ONE_DAY_CALENDAR,
    }
    index = _build_index_from_files(files)
    try:
        now = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)
        departures = index.upcoming_departures(
            ["S1"], None, now, timedelta(hours=24), 100
        )
        got = {dep.trip_id: dep.timepoint_exact for dep in departures}
        assert got == {
            f"T{i}": _expected_timepoint(cell if column_present else None)
            for i, cell in enumerate(cells)
        }
    finally:
        index.close()


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    trip_cells=st.fixed_dictionaries(
        {"wheelchair": _INT_CELL, "bikes": _INT_CELL, "direction": _INT_CELL}
    ),
    stop_cells=st.lists(
        st.fixed_dictionaries(
            {
                "pickup": _INT_CELL,
                "drop_off": _INT_CELL,
                "timepoint": _INT_CELL,
                "headsign": _TEXT_CELL,
            }
        ),
        min_size=2,
        max_size=3,
    ),
    start_offset=st.integers(min_value=0, max_value=7200),
    headway=st.sampled_from([600, 1800]),
    reps=st.integers(min_value=1, max_value=4),
)
def test_frequency_repetitions_carry_template_descriptors(
    trip_cells: dict[str, str],
    stop_cells: list[dict[str, str]],
    start_offset: int,
    headway: int,
    reps: int,
) -> None:
    """EVERY materialized repetition carries, at EVERY stop, descriptors
    equal to what the DRAWN template cells map to (oracle from the drawn
    values, not another repetition) -- materialization may never drop or
    garble trip-level or stop_time descriptive metadata.
    """
    template_base = 28800  # template anchored at 08:00:00 (arbitrary)
    start = 21600 + start_offset
    end = start + headway * reps  # strict-<: exactly `reps` repetitions
    files = {
        "agency.txt": _UTC_AGENCY,
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"
        + "".join(f"S{i},Stop,0,0\n" for i in range(len(stop_cells))),
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,wheelchair_accessible,"
            "bikes_allowed,direction_id\n"
            f"R1,ONE,F1,H,{trip_cells['wheelchair']},{trip_cells['bikes']},"
            f"{trip_cells['direction']}\n"
        ),
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,"
            "pickup_type,drop_off_type,timepoint,stop_headsign\n"
            + "".join(
                f"F1,{_format_gtfs_time(template_base + i * 300)},"
                f"{_format_gtfs_time(template_base + i * 300)},S{i},{i + 1},"
                f"{cell['pickup']},{cell['drop_off']},{cell['timepoint']},"
                f"{cell['headsign']}\n"
                for i, cell in enumerate(stop_cells)
            )
        ),
        "calendar.txt": _ONE_DAY_CALENDAR,
        "frequencies.txt": (
            "trip_id,start_time,end_time,headway_secs\n"
            f"F1,{_format_gtfs_time(start)},{_format_gtfs_time(end)},{headway}\n"
        ),
    }
    index = _build_index_from_files(files)
    try:
        now = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)
        departures = index.upcoming_departures(
            [f"S{i}" for i in range(len(stop_cells))],
            None,
            now,
            timedelta(hours=30),
            1000,
        )
        expected_ids = {f"F1#{start + n * headway}" for n in range(reps)}
        assert {dep.trip_id for dep in departures} == expected_ids
        assert len(departures) == reps * len(stop_cells)
        by_key = {(dep.trip_id, dep.stop_id): dep for dep in departures}
        for rep_id in expected_ids:
            for i, cell in enumerate(stop_cells):
                dep = by_key[(rep_id, f"S{i}")]
                assert dep.wheelchair_accessible is _expected_enum(
                    WheelchairAccess, trip_cells["wheelchair"]
                )
                assert dep.bikes_allowed is _expected_enum(
                    BikesAllowed, trip_cells["bikes"]
                )
                assert dep.pickup_type is _expected_enum(
                    PickupDropOffType, cell["pickup"]
                )
                assert dep.drop_off_type is _expected_enum(
                    PickupDropOffType, cell["drop_off"]
                )
                assert dep.timepoint_exact == _expected_timepoint(cell["timepoint"])
                assert dep.stop_headsign == _expected_text(cell["headsign"])
        trip_rows = index.upcoming_trips("S0", "S1", now, timedelta(hours=30), 1000)
        assert len(trip_rows) == reps
        for trip in trip_rows:
            assert trip.direction_id == _expected_lenient_int(trip_cells["direction"])
    finally:
        index.close()


@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    base_offsets=st.lists(
        st.integers(min_value=0, max_value=86_399), min_size=1, max_size=5
    ),
    spill_offset=st.integers(min_value=86_400, max_value=107_999),
)
def test_exactly_one_first_and_one_last_per_service_day_pair(
    base_offsets: list[int], spill_offset: int
) -> None:
    """Over generated TWO-service-day schedules -- duplicate departure times
    allowed, always including one guaranteed >24:00:00 spillover trip --
    every service day flags EXACTLY one is_first and one is_last row for
    the pair, the flagged rows ARE the min/max of the (departure, trip_id)
    total order WITHIN their own service day, and the spillover row (which
    runs on the next clock day, interleaved with that day's departures) is
    flagged as its OWN day's last, never the next day's.
    """
    dep_offsets = [*base_offsets, spill_offset]
    trip_rows = "".join(f"R1,TWO,T{i},H\n" for i in range(len(dep_offsets)))
    stop_time_rows = "".join(
        f"T{i},{_format_gtfs_time(secs)},{_format_gtfs_time(secs)},S1,1\n"
        f"T{i},{_format_gtfs_time(secs + 300)},{_format_gtfs_time(secs + 300)},S2,2\n"
        for i, secs in enumerate(dep_offsets)
    )
    files = {
        "agency.txt": _UTC_AGENCY,
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,A,0,0\nS2,B,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\n" + trip_rows,
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + stop_time_rows
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\nTWO,1,1,1,1,1,1,1,20260730,20260731\n"
        ),
    }
    index = _build_index_from_files(files)
    try:
        now = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)  # day 1's UTC anchor
        # 55h reaches 31h of elapsed seconds even on day 2, so BOTH service
        # days are fully inside the window and nothing is clipped.
        trips = index.upcoming_trips("S1", "S2", now, timedelta(hours=55), 1000)
        assert len(trips) == 2 * len(dep_offsets)
        offset_of = {f"T{i}": secs for i, secs in enumerate(dep_offsets)}
        by_day: dict[datetime, list[ScheduledTrip]] = {}
        for trip in trips:
            anchor = trip.departure - timedelta(seconds=offset_of[trip.trip_id])
            by_day.setdefault(anchor, []).append(trip)
        assert set(by_day) == {
            datetime(2026, 7, 30, 0, 0, tzinfo=UTC),
            datetime(2026, 7, 31, 0, 0, tzinfo=UTC),
        }
        ordered = sorted((secs, trip_id) for trip_id, secs in offset_of.items())
        first_key, last_key = ordered[0], ordered[-1]
        # Base offsets stay below 24:00:00, so the guaranteed spillover is
        # the strict maximum: each day's last IS its spillover departure.
        assert last_key == (spill_offset, f"T{len(base_offsets)}")
        for day_trips in by_day.values():
            assert len(day_trips) == len(dep_offsets)
            for trip in day_trips:
                key = (offset_of[trip.trip_id], trip.trip_id)
                assert trip.is_first == (key == first_key)
                assert trip.is_last == (key == last_key)
    finally:
        index.close()


@given(zip_bytes=_random_gtfs_zip(), data=st.data())
@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_cache_roundtrip_equivalent(zip_bytes: bytes, data: st.DataObject) -> None:
    """A reopened cached index answers every query identically to the builder."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = Path(tmp_dir) / "feed.zip"
        zip_path.write_bytes(zip_bytes)
        db_path = Path(tmp_dir) / "static.db"
        try:
            built = StaticIndex.build(zip_path, str(db_path), "ds-rt", None)
        except FeedParseError:
            return
        try:
            baseline_stops = built.stops()
            baseline_routes = built.routes()
            stops = [s.id for s in baseline_stops]
            queried = (
                data.draw(
                    st.lists(
                        st.sampled_from(stops), min_size=1, max_size=3, unique=True
                    )
                )
                if stops
                else ["none"]
            )
            now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
            baseline_deps = built.upcoming_departures(
                queried, None, now, timedelta(hours=30), 5
            )
        finally:
            built.close()
        reopened = StaticIndex.open_cached(db_path, "ds-rt")
        assert reopened is not None
        try:
            assert reopened.stops() == baseline_stops
            assert reopened.routes() == baseline_routes
            assert (
                reopened.upcoming_departures(queried, None, now, timedelta(hours=30), 5)
                == baseline_deps
            )
        finally:
            reopened.close()


@given(zip_bytes=_random_gtfs_zip(), data=st.data())
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_generated_feeds_end_to_end_totality(
    zip_bytes: bytes, data: st.DataObject
) -> None:
    """build + every query is total over messy-but-structured feeds; arrivals
    only ever reference queried stops and carry tz-aware datetimes.
    """
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as fp:
        fp.write(zip_bytes)
        zip_path = Path(fp.name)
    try:
        try:
            index = StaticIndex.build(zip_path, ":memory:", "ds-gen", None)
        except FeedParseError:
            return  # acceptable outcome for genuinely unbuildable feeds
    finally:
        zip_path.unlink(missing_ok=True)
    try:
        all_stops = index.stops()
        index.routes()
        probe_stop = data.draw(
            st.sampled_from([s.id for s in all_stops] + ["missing-stop"])
        )
        index.routes_serving(probe_stop)
        index.headsigns_serving(probe_stop)
        queried = data.draw(
            st.lists(
                st.sampled_from([s.id for s in all_stops] + ["missing-stop"]),
                min_size=1,
                max_size=3,
                unique=True,
            )
        )
        now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
        departures = index.upcoming_departures(
            queried, None, now, timedelta(hours=30), per_stop_limit=5
        )
        for dep in departures:
            assert dep.stop_id in queried
            assert dep.departure.tzinfo is not None
            assert now <= dep.departure <= now + timedelta(hours=30)
        assert departures == sorted(
            departures, key=lambda dep: (dep.departure, dep.trip_id, dep.stop_id)
        )
        per_stop: dict[str, int] = {}
        for dep in departures:
            per_stop[dep.stop_id] = per_stop.get(dep.stop_id, 0) + 1
        assert all(count <= 5 for count in per_stop.values())
        route_ids = {route.id for route in index.routes()}
        assert {r.id for r in index.routes_serving(probe_stop)} <= route_ids
        # Two-sided zone law against the REAL TransitFeedHandle.stops_in (a
        # detached instance, per the plan note — reuses production logic
        # rather than re-deriving it, so a bug in stops_in's filter (e.g. a
        # dropped coordinate-null check) would be caught here).
        zone = Circle(latitude=34.05, longitude=-118.25, radius_m=500_000.0)
        handle = TransitFeedHandle.__new__(TransitFeedHandle)
        handle.stops = all_stops
        returned = handle.stops_in(zone)
        assert all(
            s.latitude is not None
            and s.longitude is not None
            and in_circle(zone, s.latitude, s.longitude)
            for s in returned
        )
        in_circle_with_coords = {
            s.id
            for s in all_stops
            if s.latitude is not None
            and s.longitude is not None
            and in_circle(zone, s.latitude, s.longitude)
        }
        assert {s.id for s in returned} == in_circle_with_coords
    finally:
        index.close()


_FIXTURE_TRIPS = ["T1", "T2", "T4"]


def _run_arrivals_merge_scenario(
    msg: gtfs_realtime_pb2.FeedMessage,
) -> list[StopArrival]:
    async def scenario() -> list[StopArrival]:
        api = MockApi()
        await api.start()
        try:
            base = api.url()
            api.post("/v1/tokens", payload=TOKEN_RESPONSE)
            api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
            api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
            api.get(
                "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds",
                payload=[with_base(GTFS_RT_FEED, base)],
            )
            api.get(
                "/hosted/mdb-100.zip",
                body=build_gtfs_zip_bytes(),
                content_type="application/zip",
            )
            api.get(
                "/rt/all",
                body=msg.SerializeToString(),
                content_type="application/octet-stream",
            )
            async with MobilityFeedsClient("t", base_url=base) as client:
                handle = await client.get_transit_feed("mdb-100")
                return await handle.get_arrivals(
                    ["S1", "S2"],
                    lookahead=timedelta(hours=1),
                    limit=3,
                    now_utc=datetime(2026, 7, 30, 14, 45, tzinfo=UTC),
                )
        finally:
            await api.stop()

    return asyncio.run(scenario())


@given(data=st.data())
@settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_arrivals_merge_invariants(data: st.DataObject) -> None:
    """Over random cancellation/prediction/added sets: canceled trips absent,
    realtime <=> prediction existed, rows sorted, per-stop <= limit.

    The cancelable pool includes the generated ADDED trip ids too (not just
    the fixture trips) — otherwise cancellation-over-added can never be
    exercised, since ``GEN-ADDED-*`` ids are disjoint from ``_FIXTURE_TRIPS``.
    A forced boolean additionally guarantees GEN-ADDED-0 is canceled on some
    fraction of examples rather than relying on sampling luck.
    """
    added_count = data.draw(st.integers(0, 3))
    added_ids = [f"GEN-ADDED-{i}" for i in range(added_count)]
    cancelable_pool = [*_FIXTURE_TRIPS, *added_ids]
    canceled = set(
        data.draw(st.lists(st.sampled_from(cancelable_pool), max_size=3, unique=True))
    )
    cancel_first_added = data.draw(st.booleans())
    if added_count > 0 and cancel_first_added:
        canceled.add(added_ids[0])
    predicted = set(
        data.draw(st.lists(st.sampled_from(_FIXTURE_TRIPS), max_size=3, unique=True))
    )
    base_epoch = int(datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC).timestamp())
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    for trip_id in canceled:
        entity = msg.entity.add()
        entity.id = f"c-{trip_id}"
        entity.trip_update.trip.trip_id = trip_id
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )
    for trip_id in predicted:
        entity = msg.entity.add()
        entity.id = f"p-{trip_id}"
        entity.trip_update.trip.trip_id = trip_id
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.departure.time = base_epoch + 300
    for i, trip_id in enumerate(added_ids):
        entity = msg.entity.add()
        entity.id = f"a-{i}"
        entity.trip_update.trip.trip_id = trip_id
        entity.trip_update.trip.route_id = "R1"
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ADDED
        )
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.departure.time = base_epoch + 60 * (i + 1)

    arrivals = _run_arrivals_merge_scenario(msg)
    for row in arrivals:
        assert row.trip_id not in canceled
        if row.scheduled_departure is not None:
            expected_rt = (
                row.trip_id in predicted
                and row.stop_id == "S1"
                and row.trip_id not in canceled
            )
            assert row.realtime == expected_rt
    keys = [
        ((a.predicted_departure or a.scheduled_departure), a.trip_id or "", a.stop_id)
        for a in arrivals
    ]
    assert keys == sorted(keys)
    per_stop: dict[str, int] = {}
    for row in arrivals:
        per_stop[row.stop_id] = per_stop.get(row.stop_id, 0) + 1
    assert all(count <= 3 for count in per_stop.values())


_GBFS_ZONE = Circle(latitude=34.05, longitude=-118.25, radius_m=2_000.0)
_NEARBY_LAT = st.floats(33.95, 34.15)
_NEARBY_LON = st.floats(-118.35, -118.15)


@given(
    rows=st.lists(
        st.fixed_dictionaries(
            {
                "bike_id": st.text(min_size=1, max_size=6),
                "lat": _NEARBY_LAT | st.none(),
                "lon": _NEARBY_LON | st.none(),
            }
        ),
        max_size=8,
    )
)
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_gbfs_vehicles_zone_filter_law(rows: list[dict[str, object]]) -> None:
    """get_vehicles(zone) keeps exactly the coord-having rows inside the
    zone; nothing else.
    """
    handle = GbfsFeedHandle.__new__(GbfsFeedHandle)
    handle._doc_cache = {
        "free_bike_status": (float("inf"), float("inf"), {"data": {"bikes": rows}}),
    }
    handle._endpoints = {"free_bike_status": "x"}
    unfiltered = asyncio.run(handle.get_vehicles(None))
    filtered = asyncio.run(handle.get_vehicles(_GBFS_ZONE))
    assert filtered == [
        v for v in unfiltered if in_circle(_GBFS_ZONE, v.latitude, v.longitude)
    ]


@given(
    rows=st.lists(
        st.fixed_dictionaries(
            {
                "station_id": st.text(min_size=1, max_size=6),
                "lat": _NEARBY_LAT | st.none(),
                "lon": _NEARBY_LON | st.none(),
            }
        ),
        max_size=8,
        unique_by=lambda row: row["station_id"],
    )
)
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_gbfs_stations_zone_filter_law(rows: list[dict[str, object]]) -> None:
    """get_stations(zone) keeps exactly the coord-having rows inside the
    zone; nothing else.
    """
    handle = GbfsFeedHandle.__new__(GbfsFeedHandle)
    handle._doc_cache = {
        "station_information": (
            float("inf"),
            float("inf"),
            {"data": {"stations": rows}},
        ),
        "station_status": (float("inf"), float("inf"), {"data": {"stations": []}}),
    }
    handle._endpoints = {"station_information": "x", "station_status": "x"}
    unfiltered = asyncio.run(handle.get_stations(None))
    filtered = asyncio.run(handle.get_stations(_GBFS_ZONE))
    assert filtered == [
        s
        for s in unfiltered
        if s.latitude is not None
        and s.longitude is not None
        and in_circle(_GBFS_ZONE, s.latitude, s.longitude)
    ]


@given(
    lat1=st.floats(-85, 85),
    lon1=st.floats(-179, 179),
    lat2=st.floats(-85, 85),
    lon2=st.floats(-179, 179),
)
def test_haversine_metric_laws(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> None:
    """Non-negativity, symmetry, identity-of-indiscernibles (same point),
    and the trivial upper bound (half the Earth's circumference).
    """
    d_ab = haversine_m(lat1, lon1, lat2, lon2)
    assert d_ab >= 0
    assert abs(d_ab - haversine_m(lat2, lon2, lat1, lon1)) < 1e-6
    assert haversine_m(lat1, lon1, lat1, lon1) < 1e-6
    assert d_ab <= math.pi * 6_371_000.0 + 1.0


@given(
    entries=st.lists(
        st.tuples(
            st.text(min_size=1, max_size=10), st.sampled_from(["de", "fr", "es"])
        ),
        min_size=1,
        max_size=4,
    ),
    en_text=st.text(min_size=1, max_size=10),
    include_en=st.booleans(),
)
def test_first_translation_prefers_en_else_first(
    entries: list[tuple[str, str]], en_text: str, include_en: bool
) -> None:
    """Mirrors test_localized_prefers_en_else_first for the protobuf-side
    translation picker: 'en' wins wherever it sits; otherwise first wins.
    Non-'en' languages are drawn from a fixed pool so a coincidental 'en'
    never sneaks in and makes the oracle wrong.
    """
    translated = gtfs_realtime_pb2.TranslatedString()
    all_entries = list(entries)
    if include_en:
        all_entries.insert(len(all_entries) // 2, (en_text, "en"))
    for text, language in all_entries:
        entry = translated.translation.add()
        entry.text = text
        entry.language = language
    result = _first_translation(translated)
    assert result == (en_text if include_en else entries[0][0])


# --- frequencies.txt materialization properties -----------------------------

# One UTC service day (2026-07-30) so the oracle is a plain arithmetic
# progression from a single anchor -- no adjacent-day instances to model.
_FREQ_ANCHOR = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)
_FREQ_TEMPLATE_BASE = 28800  # templates anchored at 08:00:00 (arbitrary)
_FREQ_LOOKAHEAD = timedelta(hours=48)


def _format_gtfs_time(secs: int) -> str:
    hours, rem = divmod(secs, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _frequency_index(
    template: list[tuple[int, int]], freq_rows: list[tuple[int, int, int, str]]
) -> StaticIndex:
    """One frequency trip F1 over the single UTC service day 2026-07-30.

    ``template`` holds per-stop (arrival, departure) offsets from the
    template's first arrival; ``freq_rows`` are (start, end, headway,
    exact_times) with start/end in absolute seconds.
    """
    stop_rows = "".join(f"S{i},Stop {i},0,0\n" for i in range(len(template)))
    stop_time_rows = "".join(
        f"F1,{_format_gtfs_time(_FREQ_TEMPLATE_BASE + arr)},"
        f"{_format_gtfs_time(_FREQ_TEMPLATE_BASE + dep)},S{i},{i + 1}\n"
        for i, (arr, dep) in enumerate(template)
    )
    frequency_rows = "".join(
        f"F1,{_format_gtfs_time(start)},{_format_gtfs_time(end)},{headway},{exact}\n"
        for start, end, headway, exact in freq_rows
    )
    files = {
        "agency.txt": (
            "agency_id,agency_name,agency_url,agency_timezone\nA1,T,https://e.com,UTC\n"
        ),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n" + stop_rows,
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\nR1,ONE,F1,H\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + stop_time_rows
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\nONE,1,1,1,1,1,1,1,20260730,20260730\n"
        ),
        "frequencies.txt": (
            "trip_id,start_time,end_time,headway_secs,exact_times\n" + frequency_rows
        ),
    }
    return _build_index_from_files(files)


def _expected_rep_starts(freq_rows: list[tuple[int, int, int, str]]) -> set[int]:
    """Independent restatement of the GTFS rule: start + n*headway, n while
    STRICTLY under end_time, deduplicated across overlapping rows.
    """
    starts: set[int] = set()
    for start, end, headway, _ in freq_rows:
        rep = start
        while rep < end:
            starts.add(rep)
            rep += headway
    return starts


@st.composite
def _frequency_template_strategy(draw: st.DrawFn) -> list[tuple[int, int]]:
    """Per-stop (arrival, departure) offsets: non-decreasing, first arrival 0."""
    template: list[tuple[int, int]] = []
    current = 0
    for i in range(draw(st.integers(1, 4))):
        if i:
            current += draw(st.integers(30, 600))
        dwell = draw(st.integers(0, 60))
        template.append((current, current + dwell))
        current += dwell
    return template


# end = start + span; span may be non-positive, producing a degenerate
# [start, end) window that must contribute zero repetitions.
_FREQ_ROW_STRATEGY = st.builds(
    lambda start, span, headway, exact: (start, max(0, start + span), headway, exact),
    st.integers(0, 26 * 3600),
    st.integers(-600, 5400),
    st.integers(60, 1800),
    st.sampled_from(["", "0", "1"]),
)


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    template=_frequency_template_strategy(),
    freq_rows=st.lists(_FREQ_ROW_STRATEGY, min_size=1, max_size=3),
)
def test_frequency_departures_match_progression_oracle(
    template: list[tuple[int, int]], freq_rows: list[tuple[int, int, int, str]]
) -> None:
    """Materialized first-stop departures are EXACTLY the union of each row's
    arithmetic progression within [start, end), shifted by the first stop's
    dwell -- one row per repetition, so synthetic ids never duplicate. The
    oracle restates the GTFS rule in pure Python arithmetic, independent of
    the parse/SQL/service-day path under test.
    """
    index = _frequency_index(template, freq_rows)
    try:
        departures = index.upcoming_departures(
            ["S0"], None, _FREQ_ANCHOR, _FREQ_LOOKAHEAD, 100_000
        )
        dwell0 = template[0][1] - template[0][0]
        expected = {
            (f"F1#{s}", _FREQ_ANCHOR + timedelta(seconds=s + dwell0))
            for s in _expected_rep_starts(freq_rows)
        }
        assert {(d.trip_id, d.departure) for d in departures} == expected
        assert len(departures) == len(expected)  # no duplicated repetitions
    finally:
        index.close()


@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    template=_frequency_template_strategy(),
    freq_rows=st.lists(_FREQ_ROW_STRATEGY, min_size=1, max_size=2),
)
def test_frequency_offsets_preserved_on_every_repetition(
    template: list[tuple[int, int]], freq_rows: list[tuple[int, int, int, str]]
) -> None:
    """EVERY stop of EVERY repetition keeps the template's elapsed offset
    from the first-stop anchor: arrival/departure at stop i equal
    repetition_start + template offset, for all repetitions.
    """
    index = _frequency_index(template, freq_rows)
    try:
        stop_ids = [f"S{i}" for i in range(len(template))]
        departures = index.upcoming_departures(
            stop_ids, None, _FREQ_ANCHOR, _FREQ_LOOKAHEAD, 100_000
        )
        by_key = {(d.trip_id, d.stop_id): d for d in departures}
        assert len(by_key) == len(departures)
        for start in _expected_rep_starts(freq_rows):
            for i, (arr, dep) in enumerate(template):
                row = by_key[(f"F1#{start}", f"S{i}")]
                assert row.arrival == _FREQ_ANCHOR + timedelta(seconds=start + arr)
                assert row.departure == _FREQ_ANCHOR + timedelta(seconds=start + dep)
    finally:
        index.close()


@settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    template=_frequency_template_strategy(),
    rows=st.lists(
        st.tuples(
            st.integers(0, 24 * 3600), st.integers(1, 5400), st.integers(60, 1800)
        ),
        min_size=1,
        max_size=2,
    ),
)
def test_frequency_exact_times_values_materialize_identically(
    template: list[tuple[int, int]], rows: list[tuple[int, int, int]]
) -> None:
    """Pin the documented equivalence: exact_times=0 (idealized headway
    service) and exact_times=1 (exact schedule) materialize identical
    repetitions -- the column changes nothing downstream.
    """
    results = []
    for exact in ("0", "1"):
        freq_rows = [
            (start, start + span, headway, exact) for start, span, headway in rows
        ]
        index = _frequency_index(template, freq_rows)
        try:
            results.append(
                index.upcoming_departures(
                    ["S0"], None, _FREQ_ANCHOR, _FREQ_LOOKAHEAD, 100_000
                )
            )
        finally:
            index.close()
    assert results[0] == results[1]


@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    template=_frequency_template_strategy(),
    start=st.integers(0, 24 * 3600),
    spans=st.tuples(st.integers(1, 3600), st.integers(1, 3600)),
    headways=st.tuples(st.integers(60, 900), st.integers(60, 900)),
)
def test_frequency_overlapping_rows_dedupe_shared_repetitions(
    template: list[tuple[int, int]],
    start: int,
    spans: tuple[int, int],
    headways: tuple[int, int],
) -> None:
    """Two rows sharing a start (guaranteed overlap: both progressions begin
    at ``start``) must yield UNIQUE synthetic ids covering the union of both
    progressions -- never a duplicated repetition row.
    """
    freq_rows: list[tuple[int, int, int, str]] = [
        (start, start + spans[0], headways[0], ""),
        (start, start + spans[1], headways[1], ""),
    ]
    index = _frequency_index(template, freq_rows)
    try:
        departures = index.upcoming_departures(
            ["S0"], None, _FREQ_ANCHOR, _FREQ_LOOKAHEAD, 100_000
        )
        trip_ids = [d.trip_id for d in departures]
        assert len(trip_ids) == len(set(trip_ids))
        assert set(trip_ids) == {f"F1#{s}" for s in _expected_rep_starts(freq_rows)}
    finally:
        index.close()


@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    template=_frequency_template_strategy(),
    start=st.integers(85_800, 86_399),  # 23:50:00 .. 23:59:59
    span=st.integers(1200, 10_800),
    headway=st.integers(60, 600),
)
def test_frequency_repetitions_cross_midnight_correctly(
    template: list[tuple[int, int]], start: int, span: int, headway: int
) -> None:
    """Repetitions whose start crosses 24:00:00 stay on the GENERATING
    service day's timeline (anchor + seconds, landing on the next clock
    day), match the progression oracle, and the board stays sorted. The
    parameter ranges guarantee at least one repetition at or past 24:00:00,
    so the crossing is exercised on every example (asserted, not assumed).
    """
    freq_rows: list[tuple[int, int, int, str]] = [(start, start + span, headway, "")]
    index = _frequency_index(template, freq_rows)
    try:
        departures = index.upcoming_departures(
            ["S0"], None, _FREQ_ANCHOR, _FREQ_LOOKAHEAD, 100_000
        )
        dwell0 = template[0][1] - template[0][0]
        expected = {
            (f"F1#{s}", _FREQ_ANCHOR + timedelta(seconds=s + dwell0))
            for s in _expected_rep_starts(freq_rows)
        }
        assert {(d.trip_id, d.departure) for d in departures} == expected
        instants = [d.departure for d in departures]
        assert instants == sorted(instants)
        assert any(t >= _FREQ_ANCHOR + timedelta(days=1) for t in instants)
    finally:
        index.close()


def _run_frequencies_rt_scenario(
    msg: gtfs_realtime_pb2.FeedMessage,
) -> list[StopArrival]:
    """Serve the frequencies fixture zip plus one scripted RT message."""

    async def scenario() -> list[StopArrival]:
        api = MockApi()
        await api.start()
        try:
            base = api.url()
            api.post("/v1/tokens", payload=TOKEN_RESPONSE)
            api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
            api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
            api.get(
                "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds",
                payload=[with_base(GTFS_RT_FEED, base)],
            )
            api.get(
                "/hosted/mdb-100.zip",
                body=build_frequencies_gtfs_zip_bytes(),
                content_type="application/zip",
            )
            api.get(
                "/rt/all",
                body=msg.SerializeToString(),
                content_type="application/octet-stream",
            )
            async with MobilityFeedsClient("t", base_url=base) as client:
                handle = await client.get_transit_feed("mdb-100")
                return await handle.get_arrivals(
                    ["S1", "S2", "S3"],
                    lookahead=timedelta(hours=2),
                    limit=10,
                    now_utc=datetime(2026, 7, 30, 12, 45, tzinfo=UTC),
                )
        finally:
            await api.stop()

    return asyncio.run(scenario())


# The frequencies fixture zip's F1 repetition starts (see fixtures.py).
_F1_REP_STARTS = (21600, 22200, 22800, 25200, 25800)


@settings(max_examples=12, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    rep=st.sampled_from(_F1_REP_STARTS),
    mode=st.sampled_from(["aligned", "misaligned", "missing"]),
    kind=st.sampled_from(["prediction", "cancellation"]),
    stop_id=st.sampled_from(["S1", "S2", "S3"]),
    delay=st.integers(-600, 1800),
    misalign=st.integers(1, 599),
)
def test_frequency_rt_start_time_matches_exactly_one_repetition(
    *, rep: int, mode: str, kind: str, stop_id: str, delay: int, misalign: int
) -> None:
    """Over generated updates against the frequencies fixture: a prediction
    or cancellation whose start_time is ALIGNED to a materialized
    repetition affects exactly that one repetition; a MISALIGNED start_time
    (off by 1..599s -- repetitions are >=600s apart, so it never lands on a
    sibling) or a MISSING start_time affects none. The window holds 15
    synthetic rows (5 F1 repetitions x 3 stops) when nothing is canceled.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "tu-gen"
    entity.trip_update.trip.trip_id = "F1"
    if mode == "aligned":
        entity.trip_update.trip.start_time = _format_gtfs_time(rep)
    elif mode == "misaligned":
        entity.trip_update.trip.start_time = _format_gtfs_time(rep + misalign)
    if kind == "cancellation":
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )
    else:
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = stop_id
        stu.departure.delay = delay
        stu.departure.time = (
            int(datetime(2026, 7, 30, 13, 0, tzinfo=UTC).timestamp()) + delay
        )
    arrivals = _run_frequencies_rt_scenario(msg)
    all_ids = {f"F1#{start}" for start in _F1_REP_STARTS}
    got_ids = {row.trip_id for row in arrivals}
    if kind == "cancellation":
        dropped = {f"F1#{rep}"} if mode == "aligned" else set()
        assert got_ids == all_ids - dropped
        assert len(arrivals) == 15 - 3 * len(dropped)
        assert all(row.realtime is False for row in arrivals)
    else:
        assert got_ids == all_ids
        assert len(arrivals) == 15
        for row in arrivals:
            expected_rt = (
                mode == "aligned"
                and row.trip_id == f"F1#{rep}"
                and row.stop_id == stop_id
            )
            assert row.realtime is expected_rt
            if expected_rt:
                assert row.delay_seconds == delay
            else:
                assert row.predicted_departure is None


@given(done=st.integers(0, 2**40), total=st.integers(0, 2**40) | st.none())
def test_build_progress_fraction_bounds(done: int, total: int | None) -> None:
    fraction = StaticBuildProgress(
        phase="index", done_bytes=done, total_bytes=total
    ).fraction
    assert fraction is None or 0.0 <= fraction <= 1.0
