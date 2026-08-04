"""Property-based tests (hypothesis) for timing and geo invariants."""

import asyncio
import csv
import io
import math
import tempfile
import zipfile
from datetime import UTC, date, datetime, time, timedelta
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
from aiomobilitydatabase.feeds.models import StaticBuildProgress, StopArrival
from aiomobilitydatabase.feeds.rt import (
    _epoch_to_utc,
    _first_translation,
    alerts_from_message,
    fetch_feed_message,
    trip_updates_from_message,
    vehicles_from_message,
)
from aiomobilitydatabase.feeds.static_index import StaticIndex, parse_gtfs_time
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.fixtures import (
    _FILES,
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
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


def _maybe_fill_trip_update_entity(
    entity: gtfs_realtime_pb2.FeedEntity, data: st.DataObject
) -> set[tuple[str, str]]:
    """Fill a trip_update entity; return the (trip_id, stop_id) pairs it
    could contribute as predictions (empty when canceled or added, since
    those entities never populate ``updates.predictions``).
    """
    trip_id = data.draw(st.text(max_size=6))
    entity.trip_update.trip.trip_id = trip_id
    relationship = data.draw(st.sampled_from([0, 1, 2, 3, 5]))
    entity.trip_update.trip.schedule_relationship = relationship
    is_prediction_eligible = relationship not in (
        gtfs_realtime_pb2.TripDescriptor.CANCELED,
        gtfs_realtime_pb2.TripDescriptor.ADDED,
    )
    possible_keys: set[tuple[str, str]] = set()
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
            possible_keys.add((trip_id, stop_id))
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
    possible_prediction_keys: set[tuple[str, str]] = set()
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
        {trip_id for trip_id, _ in updates.predictions}
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


@given(done=st.integers(0, 2**40), total=st.integers(0, 2**40) | st.none())
def test_build_progress_fraction_bounds(done: int, total: int | None) -> None:
    fraction = StaticBuildProgress(
        phase="index", done_bytes=done, total_bytes=total
    ).fraction
    assert fraction is None or 0.0 <= fraction <= 1.0
