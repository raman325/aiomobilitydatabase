"""Property-based tests (hypothesis) for timing and geo invariants."""

import asyncio
import csv
import io
import math
import tempfile
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from enum import IntEnum
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import pytest
from google.transit import gtfs_realtime_pb2
from hypothesis import HealthCheck, event, given, settings
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
    _endpoints_from_discovery,
    _localized,
    _version_key,
)
from aiomobilitydatabase.feeds.geo import Circle, haversine_m, in_circle
from aiomobilitydatabase.feeds.models import (
    AlertCause,
    AlertEffect,
    AlertSeverity,
    ArrivalsQuery,
    BikesAllowed,
    CongestionLevel,
    FeedInfo,
    OccupancyStatus,
    PickupDropOffType,
    StaticBuildProgress,
    Stop,
    StopArrival,
    StopLocationType,
    UpcomingTrip,
    VehicleStopStatus,
    WheelchairAccess,
)
from aiomobilitydatabase.feeds.rt import (
    _epoch_to_utc,
    _first_translation,
    _trip_start_date,
    alerts_from_message,
    fetch_feed_message,
    resolve_trip_predictions,
    trip_updates_from_message,
    vehicles_from_message,
)
from aiomobilitydatabase.feeds.static_index import (
    ScheduledTrip,
    StaticIndex,
    parse_gtfs_time,
)
from aiomobilitydatabase.feeds.transit import TransitFeedHandle, group_stations

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


# DST-transition days (and their neighbors) for every DST-observing zone in
# TIMEZONES, mixed into query_date at a meaningful rate rather than the ~1%
# a uniform two-year date range would produce: US spring-forward/fall-back,
# EU changeovers, and the southern-hemisphere Australian pair, 2026 + 2027.
_DST_TRANSITION_DATES = sorted(
    {
        transition + timedelta(days=offset)
        for transition in (
            date(2026, 3, 8),  # US spring forward (America/Los_Angeles + New_York)
            date(2026, 11, 1),  # US fall back
            date(2027, 3, 14),
            date(2027, 11, 7),
            date(2026, 3, 29),  # Europe/Berlin spring forward
            date(2026, 10, 25),  # Europe/Berlin fall back
            date(2027, 3, 28),
            date(2027, 10, 31),
            date(2026, 4, 5),  # Australia/Sydney end of DST
            date(2026, 10, 4),  # Australia/Sydney start of DST
            date(2027, 4, 4),
            date(2027, 10, 3),
        )
        for offset in (-1, 0, 1)
    }
)

_QUERY_DATES = st.one_of(
    st.dates(min_value=date(2026, 2, 1), max_value=date(2027, 11, 30)),
    st.sampled_from(_DST_TRANSITION_DATES),
)


def _boundary_dep_secs(target: datetime, tz: ZoneInfo, query_date: date) -> int:
    """A dep_secs placing one service day's departure EXACTLY on ``target``.

    Service-day anchors are ~24h apart, so some day within a few days of
    the query date puts ``target`` at a representable [0, 30h) offset.
    """
    for offset in range(-2, 7):
        service_date = query_date + timedelta(days=offset)
        secs = (target - _oracle_anchor_utc(service_date, tz)).total_seconds()
        if 0 <= secs < 30 * 3600 and secs == int(secs):
            return int(secs)
    raise AssertionError(f"no representable boundary dep_secs for {target}")


@settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(
    tz_name=st.sampled_from(TIMEZONES),
    query_date=_QUERY_DATES,
    dep_secs=st.integers(min_value=0, max_value=30 * 3600 - 1),
    lookahead_hours=st.integers(min_value=1, max_value=72),
    # Pin the departure to EXACTLY `now` or EXACTLY `now + lookahead` on a
    # drawn fraction of examples: uniform dep_secs draws essentially never
    # sample the window boundary, so the >= / <= comparisons were only
    # tested strictly inside the window.
    pin=st.sampled_from(["none", "lower", "upper"]),
    limit=st.integers(min_value=1, max_value=3),
)
def test_departures_match_elapsed_seconds_oracle(
    *,
    tz_name: str,
    query_date: date,
    dep_secs: int,
    lookahead_hours: int,
    pin: str,
    limit: int,
) -> None:
    tz = ZoneInfo(tz_name)
    now = datetime.combine(query_date, time(3, 0), tzinfo=UTC)
    lookahead = timedelta(hours=lookahead_hours)
    if pin == "lower":
        dep_secs = _boundary_dep_secs(now, tz, query_date)
    elif pin == "upper":
        dep_secs = _boundary_dep_secs(now + lookahead, tz, query_date)
    index = _single_trip_index(tz_name, dep_secs)
    try:
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
        # The window boundary is INCLUSIVE at both ends: the pinned instant
        # must itself be returned (non-vacuous by construction).
        if pin == "lower":
            assert now in got
        elif pin == "upper":
            assert now + lookahead in got
        # Per-stop limit law: a small limit keeps exactly the NEAREST-N of
        # the oracle set, in order -- never the farthest N.
        limited = index.upcoming_departures(["S1"], None, now, lookahead, limit)
        assert [dep.departure for dep in limited] == sorted(expected)[:limit]
    finally:
        index.close()


@given(
    hours=st.integers(min_value=0, max_value=47),
    minutes=st.integers(min_value=0, max_value=59),
    seconds=st.integers(min_value=0, max_value=59),
    pad_hours=st.booleans(),
)
def test_parse_gtfs_time_round_trip(
    hours: int, minutes: int, seconds: int, pad_hours: bool
) -> None:
    # Single-digit UNPADDED hours ("8:00:00") are valid GTFS the spec calls
    # out explicitly; the suite previously only ever generated "08:00:00".
    hour_cell = f"{hours:02d}" if pad_hours else str(hours)
    value = f"{hour_cell}:{minutes:02d}:{seconds:02d}"
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
    # A date may be drawn as BOTH added and removed -- a real producer error
    # GTFS leaves undefined. The oracle below encodes the library's
    # documented resolution (two passes, removed wins) instead of filtering
    # the collision out of the generated data.
    exceptions = [(d, 1) for d in added] + [(d, 2) for d in removed]
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
        # Naive oracle, restated from the GTFS rules plus the library's
        # documented removed-wins tiebreak for a date carrying both
        # exception types (static_index.active_service_ids's two passes).
        in_range = CAL_START <= probe <= CAL_END
        base_active = in_range and weekdays[probe.weekday()]
        if probe in set(removed):
            oracle_active = False
        elif probe in set(added):
            oracle_active = True
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


# Localized entries with an OPTIONAL "text" key: real GBFS documents ship
# entries missing text, and the selected entry must then yield None -- never
# the literal string "None" (the pre-0.3.0 str() coercion bug).
def _localized_entry(language: st.SearchStrategy[str]) -> st.SearchStrategy[dict]:
    return st.fixed_dictionaries(
        {"language": language},
        optional={"text": st.text(min_size=1, max_size=20)},
    )


@given(
    entries=st.lists(
        _localized_entry(st.sampled_from(["de", "fr", "es"])),
        min_size=1,
        max_size=4,
    ),
    en_entry=_localized_entry(st.just("en")),
    include_en=st.booleans(),
)
def test_localized_prefers_en_else_first(
    entries: list[dict], en_entry: dict, include_en: bool
) -> None:
    payload = list(entries)
    if include_en:
        payload.insert(len(payload) // 2, en_entry)
    selected = en_entry if include_en else entries[0]
    # dict.get: a text-less selected entry maps to None, not "None".
    assert _localized(payload) == selected.get("text")


def test_localized_entry_without_text_is_none() -> None:
    """Deterministic regression for the "None"-string bug: an entry lacking
    "text" (or carrying an explicit null) yields None on BOTH the
    preferred-language branch and the first-entry fallback.
    """
    assert _localized([{"language": "en"}]) is None  # preferred-language branch
    assert _localized([{"language": "de"}]) is None  # first-entry fallback
    assert _localized([{"language": "en", "text": None}]) is None
    assert _localized([{"language": "de"}, {"language": "en", "text": "x"}]) == "x"


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
    """Merged stations: ids come only from information; never raises; every
    shared id carries EXACTLY the drawn status-row values -- including the
    2.x ``num_bikes_available`` vs 3.x ``num_vehicles_available`` fallback
    (a present-but-null num_bikes_available key must NOT fall through).
    """
    info_rows = [
        {
            "station_id": sid,
            "name": data.draw(st.text(max_size=10) | st.none()),
            "lat": data.draw(st.floats(-85, 85) | st.none()),
            "lon": data.draw(st.floats(-179, 179) | st.none()),
        }
        for sid in info_ids
    ]
    status_rows: list[dict[str, object]] = []
    expected_status: dict[str, tuple[int | None, bool | None]] = {}
    for sid in status_ids:
        row: dict[str, object] = {"station_id": sid}
        bikes_keys = data.draw(st.sampled_from(["bikes", "vehicles", "both", "none"]))
        bikes_value = data.draw(st.integers(0, 50) | st.none())
        vehicles_value = data.draw(st.integers(0, 50) | st.none())
        if bikes_keys in ("bikes", "both"):
            row["num_bikes_available"] = bikes_value
        if bikes_keys in ("vehicles", "both"):
            row["num_vehicles_available"] = vehicles_value
        renting = data.draw(st.sampled_from([0, 1, True, False]) | st.none())
        row["is_renting"] = renting
        status_rows.append(row)
        # Oracle from the drawn values: dict.get semantics mean a PRESENT
        # num_bikes_available key wins even when its value is null; the
        # num_vehicles_available fallback applies only when the 2.x key is
        # absent entirely. is_renting: 0/1/bool coerce, null stays None.
        expected_bikes = (
            bikes_value
            if bikes_keys in ("bikes", "both")
            else vehicles_value
            if bikes_keys == "vehicles"
            else None
        )
        expected_status[sid] = (
            expected_bikes,
            None if renting is None else bool(renting),
        )
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
    by_id = {s.id: s for s in stations}
    for sid in info_ids:
        station = by_id[str(sid)]
        if sid in expected_status:
            expected_bikes, expected_renting = expected_status[sid]
            assert station.bikes_available == expected_bikes
            assert station.is_renting is expected_renting
        else:
            # No status row at all: everything status-derived is unknown.
            assert station.bikes_available is None
            assert station.is_renting is None


def _maybe_fill_vehicle_entity(
    entity: gtfs_realtime_pb2.FeedEntity, data: st.DataObject
) -> dict[str, object] | None:
    """Fill a vehicle entity; return the expected descriptive status
    surface when a position was set (the entity surfaces), else None.

    Draws exercise the whole VehiclePosition descriptive surface:
    current_status across EVERY protobuf value and absent, congestion
    across every value and absent, stop_id / current_stop_sequence
    presence (the referent gate for the IN_TRANSIT_TO default), and
    license_plate. The expected values restate the documented rules from
    the drawn values: explicit status verbatim, the default only with a
    stop referent, None otherwise; congestion only when set.
    """
    has_position = data.draw(st.booleans())
    if has_position:
        entity.vehicle.position.latitude = data.draw(st.floats(-90, 90))
        entity.vehicle.position.longitude = data.draw(st.floats(-180, 180))
    if data.draw(st.booleans()):
        entity.vehicle.trip.trip_id = data.draw(st.text(max_size=6))
    if data.draw(st.booleans()):
        entity.vehicle.vehicle.id = data.draw(st.text(max_size=6))
    license_plate = data.draw(st.none() | st.text(min_size=1, max_size=8))
    if license_plate is not None:
        entity.vehicle.vehicle.license_plate = license_plate
    stop_id = data.draw(st.none() | st.text(min_size=1, max_size=6))
    if stop_id is not None:
        entity.vehicle.stop_id = stop_id
    stop_sequence = data.draw(st.none() | st.integers(0, 50))
    if stop_sequence is not None:
        entity.vehicle.current_stop_sequence = stop_sequence
    status_value = data.draw(
        st.none()
        | st.sampled_from(
            list(gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.values())
        )
    )
    if status_value is not None:
        entity.vehicle.current_status = status_value
    congestion_value = data.draw(
        st.none()
        | st.sampled_from(
            list(gtfs_realtime_pb2.VehiclePosition.CongestionLevel.values())
        )
    )
    if congestion_value is not None:
        entity.vehicle.congestion_level = congestion_value
    if not has_position:
        return None
    if status_value is not None:
        expected_status: VehicleStopStatus | None = VehicleStopStatus(
            gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(status_value)
        )
    elif stop_id is not None or stop_sequence is not None:
        expected_status = VehicleStopStatus.IN_TRANSIT_TO
    else:
        expected_status = None
    return {
        "current_status": expected_status,
        "congestion_level": (
            CongestionLevel(
                gtfs_realtime_pb2.VehiclePosition.CongestionLevel.Name(congestion_value)
            )
            if congestion_value is not None
            else None
        ),
        "stop_id": stop_id,
        "current_stop_sequence": stop_sequence,
        "license_plate": license_plate,
    }


# Sampled TripDescriptor.start_time values with their expected parsed
# start_secs component: absent and garbage both key as None.
_START_TIME_SAMPLES: dict[str, int | None] = {
    "": None,
    "06:10:00": 22200,
    "25:00:00": 90000,
    "not-a-time": None,
}

# Sampled TripDescriptor.start_date values with their expected parsed
# start_date component: absent, wrong-length, non-digit, and
# calendar-invalid cells all key as None (garbage behaves as absent).
_START_DATE_SAMPLES: dict[str, date | None] = {
    "": None,
    "20260730": date(2026, 7, 30),
    "20261332": None,
    "2026073": None,
    "garbage!": None,
    "00000101": None,
}


def _maybe_fill_trip_update_entity(
    entity: gtfs_realtime_pb2.FeedEntity, data: st.DataObject
) -> tuple[
    set[tuple[str, date | None, int | None]],
    set[tuple[str, date | None, int | None]],
]:
    """Fill a trip_update entity; return ``(eligible_keys, canceled_keys)``:
    the (trip_id, start_date, start_secs) key it contributes as a trip
    entry, and the key it contributes as a cancellation (ADDED entities
    contribute neither). Tracking BOTH sets lets the property assert the
    parsed trips index EXACTLY (a subset check would pass on an empty
    parse). Draws exercise the full StopTimeUpdate surface: per-stop
    schedule_relationship (all valid values), stop_id and/or stop_sequence
    presence, delays, explicit times, the trip-level TripUpdate.delay, and
    start_date/start_time cells across valid, invalid, and absent forms.
    """
    trip_id = data.draw(st.text(max_size=6))
    entity.trip_update.trip.trip_id = trip_id
    start_time = data.draw(st.sampled_from(sorted(_START_TIME_SAMPLES)))
    if start_time:
        entity.trip_update.trip.start_time = start_time
    start_secs = _START_TIME_SAMPLES[start_time]
    start_date_cell = data.draw(st.sampled_from(sorted(_START_DATE_SAMPLES)))
    if start_date_cell:
        entity.trip_update.trip.start_date = start_date_cell
    start_date = _START_DATE_SAMPLES[start_date_cell]
    relationship = data.draw(st.sampled_from([0, 1, 2, 3, 5]))
    entity.trip_update.trip.schedule_relationship = relationship
    if data.draw(st.booleans()):
        entity.trip_update.delay = data.draw(st.integers(-3600, 3600))
    for _ in range(data.draw(st.integers(0, 2))):
        stu = entity.trip_update.stop_time_update.add()
        stu.schedule_relationship = data.draw(st.sampled_from([0, 1, 2, 3]))
        if data.draw(st.booleans()):
            stu.stop_id = data.draw(st.text(max_size=6))
        if data.draw(st.booleans()):
            stu.stop_sequence = data.draw(st.integers(0, 50))
        if data.draw(st.booleans()):
            stu.arrival.time = data.draw(st.integers(0, 2_000_000_000))
        if data.draw(st.booleans()):
            stu.departure.delay = data.draw(st.integers(-3600, 3600))
    key = (trip_id, start_date, start_secs)
    if relationship == gtfs_realtime_pb2.TripDescriptor.CANCELED:
        return set(), {key}
    if relationship == gtfs_realtime_pb2.TripDescriptor.ADDED:
        return set(), set()
    return {key}, set()


def _maybe_fill_alert_entity(
    entity: gtfs_realtime_pb2.FeedEntity, data: st.DataObject
) -> list[str]:
    """Fill an alert entity; return the EXACT trip_ids the parsed alert
    must carry (sorted, deduplicated, empty trip ids excluded)."""
    # Force the alert submessage to exist even when every draw below
    # declines, so the entity ALWAYS surfaces and the expected list zips
    # 1:1 with the parsed alerts.
    entity.alert.SetInParent()
    if data.draw(st.booleans()):
        translation = entity.alert.header_text.translation.add()
        translation.text = data.draw(st.text(max_size=10))
    if data.draw(st.booleans()):
        informed = entity.alert.informed_entity.add()
        informed.route_id = data.draw(st.text(max_size=6))
    trip_ids: set[str] = set()
    for _ in range(data.draw(st.integers(0, 2))):
        informed_trip = entity.alert.informed_entity.add()
        trip_id = data.draw(st.text(max_size=6))
        informed_trip.trip.trip_id = trip_id
        if trip_id:
            trip_ids.add(trip_id)
    if data.draw(st.booleans()):
        entity.alert.cause = data.draw(
            st.sampled_from(list(gtfs_realtime_pb2.Alert.Cause.values()))
        )
    if data.draw(st.booleans()):
        entity.alert.effect = data.draw(
            st.sampled_from(list(gtfs_realtime_pb2.Alert.Effect.values()))
        )
    if data.draw(st.booleans()):
        entity.alert.severity_level = data.draw(
            st.sampled_from(list(gtfs_realtime_pb2.Alert.SeverityLevel.values()))
        )
    return sorted(trip_ids)


@given(data=st.data())
@settings(max_examples=100, deadline=None)
def test_rt_parsers_are_total_over_field_presence(data: st.DataObject) -> None:
    """Random field-presence combinations must never crash any RT parser,
    trip entries only come from eligible entities, drawn enum values map to
    typed members (or None), and resolving any parsed entry against an
    arbitrary stop order never raises and stays inside that order.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    fillers = {
        "vehicle": _maybe_fill_vehicle_entity,
        "trip_update": _maybe_fill_trip_update_entity,
        "alert": _maybe_fill_alert_entity,
        "empty": None,
    }
    expected_vehicles: list[dict[str, object]] = []
    expected_alert_trip_ids: list[list[str]] = []
    eligible_keys: set[tuple[str, date | None, int | None]] = set()
    canceled_keys: set[tuple[str, date | None, int | None]] = set()
    for i in range(data.draw(st.integers(0, 4))):
        entity = msg.entity.add()
        entity.id = f"e{i}"
        kind = data.draw(st.sampled_from(list(fillers)))
        filler = fillers[kind]
        if filler is None:
            continue
        result = filler(entity, data)
        if kind == "vehicle":
            if result is not None:
                expected_vehicles.append(result)  # type: ignore[arg-type]
        elif kind == "trip_update":
            entity_eligible, entity_canceled = result  # type: ignore[misc]
            eligible_keys |= entity_eligible
            canceled_keys |= entity_canceled
        elif kind == "alert":
            expected_alert_trip_ids.append(result)  # type: ignore[arg-type]
    vehicles = vehicles_from_message(msg, route_names={}, trip_routes={})
    assert len(vehicles) == len(expected_vehicles)
    for vehicle, expected in zip(vehicles, expected_vehicles, strict=True):
        assert vehicle.latitude is not None and vehicle.longitude is not None
        assert vehicle.occupancy_status is None or isinstance(
            vehicle.occupancy_status, OccupancyStatus
        )
        # The descriptive status surface maps EXACTLY per the drawn spec:
        # explicit status verbatim, the IN_TRANSIT_TO default only behind
        # a stop referent, and raw pass-through for the rest.
        assert vehicle.current_status is expected["current_status"]
        assert vehicle.congestion_level is expected["congestion_level"]
        assert vehicle.stop_id == expected["stop_id"]
        assert vehicle.current_stop_sequence == expected["current_stop_sequence"]
        assert vehicle.license_plate == expected["license_plate"]
    updates = trip_updates_from_message(msg)
    assert updates.canceled_trips.isdisjoint(set(updates.trips))
    # EXACT key-set equalities (a `<=` check passed even on an empty
    # parse): every eligible entity's key survives unless canceled, and
    # the cancellation set is exactly the drawn CANCELED keys.
    assert updates.canceled_trips == canceled_keys
    assert set(updates.trips) == eligible_keys - canceled_keys
    # Resolution totality: any entry against any stop order never raises,
    # and its outcomes never leave that order.
    stop_calls = [
        (seq, data.draw(st.sampled_from(["A", "B", "C"])))
        for seq in sorted(data.draw(st.sets(st.integers(0, 50), max_size=4)))
    ]
    known_seqs = {seq for seq, _ in stop_calls}
    for entry in updates.trips.values():
        resolved = resolve_trip_predictions(entry, stop_calls)
        assert set(resolved.predictions) <= known_seqs
        assert resolved.skipped <= known_seqs
        assert resolved.skipped.isdisjoint(resolved.predictions)
    alerts = alerts_from_message(msg)  # must simply not raise
    for alert, expected_trip_ids in zip(alerts, expected_alert_trip_ids, strict=True):
        assert alert.cause is None or isinstance(alert.cause, AlertCause)
        assert alert.effect is None or isinstance(alert.effect, AlertEffect)
        assert alert.severity is None or isinstance(alert.severity, AlertSeverity)
        # Informed-entity trip descriptors map to the EXACT trip_ids list.
        assert alert.trip_ids == expected_trip_ids


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

    One ANCHOR exists amid the mess: the first trip keeps its real service
    id, its first calendar row a non-degenerate end date, and its first two
    stop_times rows real trip/stop ids -- so query properties can bias
    toward data that CAN produce rows (their non-vacuity guards depend on
    it) while everything around the anchor stays adversarial.
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
        # Anchor: the first trip's service reference is always real.
        f"{maybe_fake(route_ids)},"
        f"{service_ids[0] if t_idx == 0 else maybe_fake(service_ids)},{tid},"
        f"{draw(st.sampled_from(['Downtown', '終点🚌', '']))}"
        for t_idx, tid in enumerate(trip_ids)
    ]
    times = ["00:00:00", "08:00:00", "24:00:00", "47:59:59", ""]
    stop_times_rows = []
    for t_idx, tid in enumerate(trip_ids):
        n_rows = draw(st.integers(2, 3)) if t_idx == 0 else draw(st.integers(1, 3))
        for seq in range(n_rows):
            # Anchor: the first trip's first two calls keep real ids.
            anchored = t_idx == 0 and seq < 2
            row_tid = tid if anchored else maybe_fake([tid])
            row_stop = (
                stop_ids[seq % len(stop_ids)] if anchored else maybe_fake(stop_ids)
            )
            stop_times_rows.append(
                f"{row_tid},{draw(st.sampled_from(times))},"
                f"{draw(st.sampled_from(times))},{row_stop},{seq}"
            )

    def cal_end(s_idx: int) -> str:
        # Anchor: the first service keeps a sane end date; later rows may
        # end before they start.
        return (
            "20271231"
            if s_idx == 0
            else draw(st.sampled_from(["20271231", "20250101"]))
        )

    cal_rows = [
        f"{sid},1,1,1,1,1,{draw(st.sampled_from(['0', '1']))},1,"
        f"{draw(st.sampled_from(['20260101', '20270101']))},{cal_end(s_idx)}"
        for s_idx, sid in enumerate(service_ids[:-1] or service_ids)
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


# Non-vacuity accounting for the two generated-feed query properties: the
# review found upcoming_trips non-empty on ~1.5% of draws (3/206) and the
# determinism property on ~17% -- low enough that most assertions ran on
# empty results. The biased strategies below must keep the non-empty rate
# healthy, and the *_nonvacuity_rate guard tests (which pytest runs AFTER
# their property, in definition order) fail the suite if it collapses.
# Non-vacuity tallies for the two generated-feed query properties below.
#
# These stay module-level because the properties that write to them are
# plain @given functions, and the fragility worth fixing is not WHERE the
# counts live -- a pytest stash is global mutable state too, keyed on the
# config instead of the module, with identical ordering semantics -- but
# that the guard assertions used to depend on running AFTER the property.
# Each guard now seeds its own measurement when it finds an empty tally
# (see _seeded_tally), which makes it correct when selected alone with -k,
# under a random-order plugin, and on an xdist worker that received the
# guard but not the property.
#
# Honest-measurement caveat: a count is bumped once per example that
# reached the assertions, including Hypothesis's reuse-phase replays of
# database entries, so "examples" can exceed max_examples. Both counters
# are bumped together, so the RATE the guards assert is unaffected; only
# the absolute floor is (upward, i.e. conservatively).
_DEPARTURES_VACUITY = {"examples": 0, "nonempty": 0}
_TRIPS_VACUITY = {"examples": 0, "nonempty": 0}


def _seeded_tally(
    tally: dict[str, int], property_test: Callable[[], None]
) -> dict[str, int]:
    """Return ``tally``, first running ``property_test`` if it is empty.

    An empty tally means the property has not run in this process yet, so
    the guard would otherwise assert on nothing.
    """
    if not tally["examples"]:
        property_test()
    return tally


# The generator's calendar rows start service in 2026 OR 2027; probing one
# Thursday in each year finds in-window schedule data for most feeds that
# have any at all (2026-07-30 and 2027-07-29 are both Thursdays, and the
# generator's weekday bits are all-on except Saturday).
_GEN_PROBE_NOWS = (
    datetime(2026, 7, 30, 12, 0, tzinfo=UTC),
    datetime(2027, 7, 29, 12, 0, tzinfo=UTC),
)


@given(zip_bytes=_random_gtfs_zip(), data=st.data())
@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_upcoming_departures_deterministic(
    zip_bytes: bytes, data: st.DataObject
) -> None:
    """Identical queries must return identical row sequences (HA sensors must
    not flap between tied departures), and a drawn route_ids subset returns
    exactly the oracle-side filter of the unfiltered rows.
    """
    try:
        index = _index_from_zip_bytes(zip_bytes)
    except FeedParseError:
        return  # acceptable outcome for genuinely unbuildable feeds
    try:
        stops = [s.id for s in index.stops()]
        if not stops:
            return
        now = _GEN_PROBE_NOWS[0]
        lookahead = timedelta(hours=30)
        # Bias toward a query date and stops that actually have departures
        # so the invariants are exercised on non-empty results, while still
        # drawing arbitrary (often-empty) stops on a fraction of examples.
        pool = stops
        if data.draw(st.integers(0, 9)) < 8:
            for probe_now in _GEN_PROBE_NOWS:
                served = sorted(
                    {
                        dep.stop_id
                        for dep in index.upcoming_departures(
                            stops, None, probe_now, lookahead, 10_000
                        )
                    }
                )
                if served:
                    pool, now = served, probe_now
                    break
        queried = data.draw(
            st.lists(st.sampled_from(pool), min_size=1, max_size=3, unique=True)
        )
        first = index.upcoming_departures(queried, None, now, lookahead, 5)
        second = index.upcoming_departures(queried, None, now, lookahead, 5)
        assert first == second
        assert first == sorted(
            first, key=lambda dep: (dep.departure, dep.trip_id, dep.stop_id)
        )
        _DEPARTURES_VACUITY["examples"] += 1
        if first:
            _DEPARTURES_VACUITY["nonempty"] += 1
            event("upcoming_departures non-empty")
        # route_ids filter law (oracle-side filter over an unlimited query,
        # so the per-stop limit cannot mask a filter bug).
        unfiltered = index.upcoming_departures(queried, None, now, lookahead, 10_000)
        route_pool = sorted({dep.route_id for dep in unfiltered})
        if route_pool:
            subset = data.draw(
                st.lists(
                    st.sampled_from(route_pool), min_size=1, max_size=2, unique=True
                )
            )
            filtered = index.upcoming_departures(
                queried, subset, now, lookahead, 10_000
            )
            assert filtered == [
                dep for dep in unfiltered if dep.route_id in set(subset)
            ]
    finally:
        index.close()


def test_upcoming_departures_nonvacuity_rate() -> None:
    """Guard for the property above: fail if its non-empty rate collapses."""
    tally = _seeded_tally(_DEPARTURES_VACUITY, test_upcoming_departures_deterministic)
    examples = tally["examples"]
    assert examples > 0, "no example reached the assertions even after seeding"
    assert tally["nonempty"] >= max(3, examples // 5)


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
        now = _GEN_PROBE_NOWS[0]
        lookahead = timedelta(hours=30)
        # Bias the pair toward two stops SHARING a trip: pick an in-window
        # DEPARTURE row (its stop is a known-good origin with a non-null,
        # in-window departure) and a LATER call of the same trip as the
        # destination, probing both generator calendar years. A fraction of
        # examples still draws arbitrary (usually unrelated) stops.
        origin: str | None = None
        destination: str | None = None
        if data.draw(st.integers(0, 9)) < 8:
            for probe_now in _GEN_PROBE_NOWS:
                deps = index.upcoming_departures(
                    stops, None, probe_now, lookahead, 10_000
                )
                if not deps:
                    continue
                calls_by_trip = index.trip_stop_calls(
                    sorted({dep.trip_id for dep in deps})
                )
                candidates = [
                    (dep.stop_id, later)
                    for dep in deps
                    if (
                        later := [
                            call
                            for call in calls_by_trip.get(dep.trip_id, [])
                            if call[0] > dep.stop_sequence
                        ]
                    )
                ]
                if not candidates:
                    continue
                origin, later_calls = data.draw(st.sampled_from(candidates))
                destination = data.draw(st.sampled_from(later_calls))[1]
                now = probe_now
                break
        if origin is None or destination is None:
            origin = data.draw(st.sampled_from(stops))
            destination = data.draw(st.sampled_from(stops))
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
        _TRIPS_VACUITY["examples"] += 1
        if first:
            _TRIPS_VACUITY["nonempty"] += 1
            event("upcoming_trips non-empty")
    finally:
        index.close()


def test_upcoming_trips_nonvacuity_rate() -> None:
    """Guard for the property above: fail if its non-empty rate collapses."""
    tally = _seeded_tally(_TRIPS_VACUITY, test_upcoming_trips_invariants)
    examples = tally["examples"]
    assert examples > 0, "no example reached the assertions even after seeding"
    assert tally["nonempty"] >= max(3, examples // 5)


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
        "stop_desc": _TEXT_CELL,
        "stop_url": _TEXT_CELL,
        "zone_id": _TEXT_CELL,
        "stop_timezone": _TEXT_CELL,
        "route_desc": _TEXT_CELL,
        "route_sort_order": _INT_CELL,
        "trip_short_name": _TEXT_CELL,
        "block_id": _TEXT_CELL,
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
            "wheelchair_boarding,stop_desc,stop_url,zone_id,stop_timezone\n"
            f"S1,A,0,0,{cells['stop_code']},{cells['platform_code']},"
            f"{cells['wheelchair_boarding']},{cells['stop_desc']},"
            f"{cells['stop_url']},{cells['zone_id']},{cells['stop_timezone']}\n"
            "S2,B,0,0,,,,,,,\n"
        ),
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type,agency_id,"
            "route_color,route_text_color,route_url,route_desc,"
            "route_sort_order\n"
            f"R1,1,Line,3,{cells['agency_id']},{cells['route_color']},"
            f"{cells['route_text_color']},{cells['route_url']},"
            f"{cells['route_desc']},{cells['route_sort_order']}\n"
        ),
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,wheelchair_accessible,"
            "bikes_allowed,direction_id,trip_short_name,block_id\n"
            f"R1,ONE,T1,H,{cells['wheelchair_accessible']},"
            f"{cells['bikes_allowed']},{cells['direction_id']},"
            f"{cells['trip_short_name']},{cells['block_id']}\n"
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
        assert stop.description == _expected_text(cells["stop_desc"])
        assert stop.url == _expected_text(cells["stop_url"])
        assert stop.zone_id == _expected_text(cells["zone_id"])
        assert stop.timezone == _expected_text(cells["stop_timezone"])
        (route,) = index.routes()
        assert route.agency_id == _expected_text(cells["agency_id"])
        assert route.color == _expected_text(cells["route_color"])
        assert route.text_color == _expected_text(cells["route_text_color"])
        assert route.url == _expected_text(cells["route_url"])
        assert route.description == _expected_text(cells["route_desc"])
        assert route.sort_order == _expected_lenient_int(cells["route_sort_order"])
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
        assert trip.trip_short_name == _expected_text(cells["trip_short_name"])
        assert trip.block_id == _expected_text(cells["block_id"])
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


# Producer date cells for feed_info: every valid calendar date plus the
# malformed shapes real feeds ship (wrong length, non-digits,
# calendar-invalid months/days, year zero, whitespace, blank).
_DATE_CELL = st.one_of(
    st.dates().map(lambda d: f"{d.year:04d}{d.month:02d}{d.day:02d}"),
    st.sampled_from(
        [
            "",
            " ",
            "garbage!",
            "2026073",
            "202607301",
            "20261332",
            "00000000",
            "20260230",
        ]
    ),
)


def _expected_lenient_date(cell: str) -> date | None:
    """Oracle for lenient YYYYMMDD cells: exactly 8 ASCII digits forming a
    real calendar date -> that date; anything else -> None."""
    cell = cell.strip()
    if len(cell) != 8 or not (cell.isascii() and cell.isdigit()):
        return None
    try:
        return date(int(cell[:4]), int(cell[4:6]), int(cell[6:8]))
    except ValueError:
        return None


_FEED_INFO_CELLS = st.fixed_dictionaries(
    {
        "publisher_name": _TEXT_CELL,
        "publisher_url": _TEXT_CELL,
        "lang": _TEXT_CELL,
        "version": _TEXT_CELL,
        "start": _DATE_CELL,
        "end": _DATE_CELL,
    }
)


@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(cells=_FEED_INFO_CELLS)
def test_feed_info_cells_parse_total_and_map_per_rules(cells: dict[str, str]) -> None:
    """Arbitrary producer cells across EVERY feed_info column at once: the
    build never raises, text columns pass through verbatim (empty -> None),
    and the date columns map per the lenient-date oracle (valid YYYYMMDD ->
    the exact date, every malformed shape -> None).
    """
    files = {
        "agency.txt": _UTC_AGENCY,
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,A,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\nR1,ONE,T1,H\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,08:00:00,08:00:00,S1,1\n"
        ),
        "calendar.txt": _ONE_DAY_CALENDAR,
        "feed_info.txt": (
            "feed_publisher_name,feed_publisher_url,feed_lang,feed_version,"
            "feed_start_date,feed_end_date\n"
            f"{cells['publisher_name']},{cells['publisher_url']},"
            f"{cells['lang']},{cells['version']},{cells['start']},{cells['end']}\n"
        ),
    }
    index = _build_index_from_files(files)  # totality: must never raise
    try:
        assert index.feed_info() == FeedInfo(
            publisher_name=_expected_text(cells["publisher_name"]),
            publisher_url=_expected_text(cells["publisher_url"]),
            lang=_expected_text(cells["lang"]),
            version=_expected_text(cells["version"]),
            start_date=_expected_lenient_date(cells["start"]),
            end_date=_expected_lenient_date(cells["end"]),
        )
    finally:
        index.close()


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    trip_cells=st.fixed_dictionaries(
        {
            "wheelchair": _INT_CELL,
            "bikes": _INT_CELL,
            "direction": _INT_CELL,
            "short_name": _TEXT_CELL,
            "block": _TEXT_CELL,
        }
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
            "bikes_allowed,direction_id,trip_short_name,block_id\n"
            f"R1,ONE,F1,H,{trip_cells['wheelchair']},{trip_cells['bikes']},"
            f"{trip_cells['direction']},{trip_cells['short_name']},"
            f"{trip_cells['block']}\n"
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
                assert dep.trip_short_name == _expected_text(trip_cells["short_name"])
                assert dep.block_id == _expected_text(trip_cells["block"])
        trip_rows = index.upcoming_trips("S0", "S1", now, timedelta(hours=30), 1000)
        assert len(trip_rows) == reps
        for trip in trip_rows:
            assert trip.direction_id == _expected_lenient_int(trip_cells["direction"])
            assert trip.trip_short_name == _expected_text(trip_cells["short_name"])
            assert trip.block_id == _expected_text(trip_cells["block"])
    finally:
        index.close()


@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    first_offset=st.integers(min_value=0, max_value=82_800),
    mid_gaps=st.lists(
        st.integers(min_value=2, max_value=86_000), min_size=1, max_size=4
    ),
    spill_offset=st.integers(min_value=86_400, max_value=107_999),
    tie_at_max=st.booleans(),
)
def test_exactly_one_first_and_one_last_per_service_day_pair(
    first_offset: int, mid_gaps: list[int], spill_offset: int, tie_at_max: bool
) -> None:
    """Over generated TWO-service-day schedules -- duplicate departure times
    allowed, always including one guaranteed >24:00:00 spillover trip, and
    on drawn examples a SECOND trip TYING the spillover at the day's
    maximum departure -- every service day flags EXACTLY one is_first and
    one is_last row for the pair, the flagged rows ARE the min/max of the
    (departure, trip_id) total order over the WHOLE service day (the tie at
    the last slot pins the DESC trip_id tiebreak), and the spillover row
    (which runs on the next clock day, interleaved with that day's
    departures) is flagged as its OWN day's last, never the next day's.

    A second query then sits `now` MID-DAY with a sub-day window: the
    day's true first departure precedes the window by construction, so the
    flags on the returned rows can only be right if the extremes are
    computed over the whole service day, never the query window.
    """
    # A guaranteed strictly-later-than-first mid-day trip on every example
    # (so the mid-day window below always returns a row that is NOT the
    # day's first); duplicates among mids are allowed and welcome.
    mid_offsets = [min(first_offset + gap, 86_399) for gap in mid_gaps]
    dep_offsets = [first_offset, *mid_offsets, spill_offset]
    if tie_at_max:
        dep_offsets.append(spill_offset)  # departure tie at the day's LAST slot
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
        day_one = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)  # day 1's UTC anchor
        # 55h reaches 31h of elapsed seconds even on day 2, so BOTH service
        # days are fully inside the window and nothing is clipped.
        trips = index.upcoming_trips("S1", "S2", day_one, timedelta(hours=55), 1000)
        assert len(trips) == 2 * len(dep_offsets)
        offset_of = {f"T{i}": secs for i, secs in enumerate(dep_offsets)}
        by_day: dict[datetime, list[ScheduledTrip]] = {}
        for trip in trips:
            anchor = trip.departure - timedelta(seconds=offset_of[trip.trip_id])
            by_day.setdefault(anchor, []).append(trip)
        assert set(by_day) == {
            day_one,
            datetime(2026, 7, 31, 0, 0, tzinfo=UTC),
        }
        ordered = sorted((secs, trip_id) for trip_id, secs in offset_of.items())
        first_key, last_key = ordered[0], ordered[-1]
        # first/mid offsets stay below 24:00:00, so the spillover slot is the
        # maximum departure; when a second trip ties it, the DESC trip_id
        # tiebreak makes the higher trip_id the day's unique last.
        assert last_key == (spill_offset, f"T{len(dep_offsets) - 1}")
        assert first_key == (first_offset, "T0")
        for day_trips in by_day.values():
            assert len(day_trips) == len(dep_offsets)
            assert sum(trip.is_first for trip in day_trips) == 1
            assert sum(trip.is_last for trip in day_trips) == 1
            for trip in day_trips:
                key = (offset_of[trip.trip_id], trip.trip_id)
                assert trip.is_first == (key == first_key)
                assert trip.is_last == (key == last_key)
        # Mid-day query: `now` sits just past the day's first departure with
        # a sub-day (23:59:59) window. T1's departure is inside the window
        # by construction, the day's first is NOT -- so any row flagged
        # is_first here would prove the extremes were window-restricted.
        now_mid = day_one + timedelta(seconds=first_offset + 1)
        windowed = index.upcoming_trips(
            "S1", "S2", now_mid, timedelta(seconds=86_399), 1000
        )
        assert windowed  # the guaranteed mid trip is always inside
        for trip in windowed:
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
                # limit=10 keeps every scheduled AND generated ADDED row
                # inside the query's cap (at most 2 scheduled + 3 added at S1
                # plus S2's single scheduled row), so the exactly-once ADDED
                # assertions below can never be masked by truncation; the
                # limit law itself is pinned by the elapsed-seconds oracle
                # property and the end-to-end totality property.
                [arrivals] = await handle.get_arrivals(
                    [ArrivalsQuery(["S1", "S2"], limit=10)],
                    lookahead=timedelta(hours=1),
                    now_utc=datetime(2026, 7, 30, 14, 45, tzinfo=UTC),
                )
                return arrivals
        finally:
            await api.stop()

    return asyncio.run(scenario())


@given(data=st.data())
@settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_arrivals_merge_invariants(data: st.DataObject) -> None:
    """Over random cancellation/prediction/added sets: canceled trips absent,
    realtime <=> prediction existed, rows sorted, row count <= limit, and
    every NON-canceled generated ADDED trip surfaces EXACTLY once, at its
    announced stop, carrying its drawn epoch as the predicted departure
    (deleting the ADDED-row merge loop entirely once survived the suite).

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
    # Every surviving generated ADDED trip merges in exactly once, at its
    # announced stop, with its drawn epoch as the explicit prediction.
    for i, trip_id in enumerate(added_ids):
        rows = [row for row in arrivals if row.trip_id == trip_id]
        if trip_id in canceled:
            assert rows == []
            continue
        (row,) = rows
        assert row.stop_id == "S1"
        assert row.realtime is True
        assert row.scheduled_departure is None
        assert row.predicted_departure == datetime.fromtimestamp(
            base_epoch + 60 * (i + 1), tz=UTC
        )
    keys = [
        ((a.predicted_departure or a.scheduled_departure), a.trip_id or "", a.stop_id)
        for a in arrivals
    ]
    assert keys == sorted(keys)
    per_stop: dict[str, int] = {}
    for row in arrivals:
        per_stop[row.stop_id] = per_stop.get(row.stop_id, 0) + 1
    assert all(count <= 10 for count in per_stop.values())


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


_EARTH_HALF_CIRCUMFERENCE_M = math.pi * 6_371_000.0
# Metres per degree of latitude along a meridian, for the calibration law
# below. Written from first principles (R * pi / 180), NOT imported from
# geo.py, so a wrong radius there cannot cancel out of the oracle.
_M_PER_DEG_LAT = 6_371_000.0 * math.pi / 180.0


@given(
    # Full domain, including the poles, where cos(phi) -> 0. The previous
    # +-85/+-179 clipping excluded them and nothing else covered them.
    lat1=st.floats(-90, 90),
    lon1=st.floats(-180, 180),
    lat2=st.floats(-90, 90),
    lon2=st.floats(-180, 180),
    third=st.tuples(st.floats(-90, 90), st.floats(-180, 180)),
)
def test_haversine_metric_laws(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    third: tuple[float, float],
) -> None:
    """Non-negativity, symmetry, identity-of-indiscernibles (same point),
    the trivial upper bound (half the Earth's circumference), and the
    triangle inequality via a third drawn point.

    The triangle inequality does NOT catch a lat/lon argument swap:
    measured over 2000 examples the swapped implementation produces zero
    violations, because swapping the coordinates is a bijection of the
    sphere and the composition is still a metric. The calibration law
    below is what catches that.
    """
    lat3, lon3 = third
    d_ab = haversine_m(lat1, lon1, lat2, lon2)
    d_bc = haversine_m(lat2, lon2, lat3, lon3)
    d_ac = haversine_m(lat1, lon1, lat3, lon3)
    assert d_ab >= 0
    assert abs(d_ab - haversine_m(lat2, lon2, lat1, lon1)) < 1e-6
    assert haversine_m(lat1, lon1, lat1, lon1) < 1e-6
    assert d_ab <= _EARTH_HALF_CIRCUMFERENCE_M + 1.0
    # Epsilon in metres: these distances reach ~2e7, so float rounding in
    # sqrt/asin is worth more slack than the 1e-6 used for the exact laws.
    assert d_ac <= d_ab + d_bc + 1e-3


@given(lat1=st.floats(-90, 90), lat2=st.floats(-90, 90), lon=st.floats(-180, 180))
def test_haversine_along_a_meridian_equals_arc_length(
    lat1: float, lat2: float, lon: float
) -> None:
    """Calibration, not just structure: two points on the SAME meridian are
    exactly |dlat| degrees of arc apart, so the distance must equal
    |dlat| * R * pi / 180 whatever longitude they share.

    This is the law the metric laws cannot express. All of them -- including
    the triangle inequality -- survive a lat/lon argument swap and a wrong
    Earth radius; this one fails on both (verified over a 150-point probe
    grid: 58 violations under the swap, 145 under a halved radius).
    """
    distance = haversine_m(lat1, lon, lat2, lon)
    expected = abs(lat2 - lat1) * _M_PER_DEG_LAT
    assert math.isclose(distance, expected, rel_tol=1e-9, abs_tol=1e-6)


@given(
    # Languages INCLUDING "en", with the oracle computed from the drawn list
    # rather than from a separately injected entry. That lets one strategy
    # cover every branch at once: the empty list (-> None, the branch the
    # previous version could not reach because min_size was 1), the
    # first-entry fallback, "en" anywhere, and -- undrawn before -- a list
    # with MORE THAN ONE "en" entry, where the first one must win.
    entries=st.lists(
        st.tuples(
            st.text(max_size=10), st.sampled_from(["de", "fr", "es", "en", "en"])
        ),
        max_size=5,
    )
)
def test_first_translation_prefers_en_else_first_else_none(
    entries: list[tuple[str, str]],
) -> None:
    """Mirrors test_localized_prefers_en_else_first for the protobuf-side
    translation picker: the first 'en' wins wherever it sits; otherwise the
    first entry wins; and an empty translation list yields None.
    """
    translated = gtfs_realtime_pb2.TranslatedString()
    for text, language in entries:
        entry = translated.translation.add()
        entry.text = text
        entry.language = language
    expected = next(
        (text for text, language in entries if language == "en"),
        entries[0][0] if entries else None,
    )
    assert _first_translation(translated) == expected


def test_first_translation_of_empty_is_none() -> None:
    """Deterministic companion: the property above draws the empty list on
    only about 1 example in 200 (measured), too rare to rely on for the
    branch that distinguishes None from "".
    """
    assert _first_translation(gtfs_realtime_pb2.TranslatedString()) is None


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
# [start, end) window that must contribute zero repetitions. On drawn
# examples span is an EXACT MULTIPLE of the headway, so a repetition
# landing precisely on end_time (which must NOT run: the bound is strict)
# is generated deliberately rather than only via zero-bias luck.
@st.composite
def _freq_row(draw: st.DrawFn) -> tuple[int, int, int, str]:
    start = draw(st.integers(0, 26 * 3600))
    headway = draw(st.integers(60, 1800))
    exact = draw(st.sampled_from(["", "0", "1"]))
    if draw(st.booleans()):
        span = headway * draw(st.integers(0, 4))  # end lands ON a repetition
    else:
        span = draw(st.integers(-600, 5400))
    return (start, max(0, start + span), headway, exact)


_FREQ_ROW_STRATEGY = _freq_row()


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


@pytest.mark.parametrize(
    ("template", "rows"),
    [
        pytest.param(
            [(0, 0), (300, 330), (900, 900)],
            [(21_600, 3_600, 600), (43_200, 1_800, 900)],
            id="two-spans-three-stops",
        )
    ],
)
def test_frequency_exact_times_values_materialize_identically(
    template: list[tuple[int, int]], rows: list[tuple[int, int, int]]
) -> None:
    """Regression guard for the documented equivalence: exact_times=0
    (idealized headway service) and exact_times=1 (exact schedule)
    materialize identical repetitions.

    Deliberately ONE fixed example rather than a @given property. The
    column is read by no production code path -- _parse_frequency_spans
    destructures only trip_id/start_time/end_time/headway_secs, and
    static_index's docstring says the column is ignored for both values --
    so the assertion cannot fail for any drawn input, and drawn variety
    bought nothing but two extra full index builds per example. It stays
    as a guard that would fail if someone later started branching on the
    column without saying so.
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
    # Not vacuous: the fixed feed really materializes repetitions.
    assert results[0]


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
                # One query per stop: the 10-row limit is per query, and the
                # property counts all 15 rows the window holds across the
                # three stops.
                per_stop = await handle.get_arrivals(
                    [
                        ArrivalsQuery([stop_id], limit=10)
                        for stop_id in ("S1", "S2", "S3")
                    ],
                    lookahead=timedelta(hours=2),
                    now_utc=datetime(2026, 7, 30, 12, 45, tzinfo=UTC),
                )
                return [row for rows in per_stop for row in rows]
        finally:
            await api.stop()

    return asyncio.run(scenario())


# The frequencies fixture zip's F1 repetition starts (see fixtures.py).
_F1_REP_STARTS = (21600, 22200, 22800, 25200, 25800)


# The frequencies fixture's F1 template offsets from a repetition start
# (see fixtures.py): S1 arr=dep at +0, S2 +600/+630, S3 +1200. The service
# day anchor for 2026-07-30 in America/Los_Angeles (PDT) is 07:00 UTC.
_F1_STOP_POS = {"S1": 0, "S2": 1, "S3": 2}
_F1_ARR_OFFSETS = {"S1": 0, "S2": 600, "S3": 1200}
_F1_DEP_OFFSETS = {"S1": 0, "S2": 630, "S3": 1200}
_F1_DAY_ANCHOR = datetime(2026, 7, 30, 7, 0, tzinfo=UTC)


# 40+ examples: the drawn space is ~90 discrete combos (5 reps x 3 modes x
# 2 kinds x 3 stops), so 12 examples left most of it unvisited.
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
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
    repetition affects exactly that one repetition -- the STU stop plus
    propagation to that repetition's SUBSEQUENT stops, never a sibling
    repetition and never a stop before the STU; a MISALIGNED start_time
    (off by 1..599s -- repetitions are >=600s apart, so it never lands on
    a sibling) or a MISSING start_time affects none. The window holds 15
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
    explicit_departure_epoch = (
        int(datetime(2026, 7, 30, 13, 0, tzinfo=UTC).timestamp()) + delay
    )
    if kind == "cancellation":
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )
    else:
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = stop_id
        stu.departure.delay = delay
        stu.departure.time = explicit_departure_epoch
    arrivals = _run_frequencies_rt_scenario(msg)
    all_ids = {f"F1#{start}" for start in _F1_REP_STARTS}
    got_ids = {row.trip_id for row in arrivals}
    if kind == "cancellation":
        dropped = {f"F1#{rep}"} if mode == "aligned" else set()
        assert got_ids == all_ids - dropped
        assert len(arrivals) == 15 - 3 * len(dropped)
        assert all(row.realtime is False for row in arrivals)
        return
    assert got_ids == all_ids
    assert len(arrivals) == 15
    for row in arrivals:
        assert row.stop_id is not None
        # Realtime iff this row is the matched repetition's STU stop or a
        # LATER stop of that same repetition (propagation): siblings and
        # earlier stops never gain predictions.
        expected_rt = (
            mode == "aligned"
            and row.trip_id == f"F1#{rep}"
            and _F1_STOP_POS[row.stop_id] >= _F1_STOP_POS[stop_id]
        )
        assert row.realtime is expected_rt
        if not expected_rt:
            assert row.predicted_departure is None
            assert row.delay_seconds is None
            continue
        assert row.delay_seconds == delay
        if row.stop_id == stop_id:
            # The STU's own stop keeps its explicit epoch prediction.
            assert row.predicted_departure == datetime.fromtimestamp(
                explicit_departure_epoch, tz=UTC
            )
        else:
            # Propagated stops predict scheduled + delay on this
            # repetition's own timeline.
            assert row.predicted_departure == _F1_DAY_ANCHOR + timedelta(
                seconds=rep + _F1_DEP_OFFSETS[row.stop_id] + delay
            )
            assert row.predicted_arrival == _F1_DAY_ANCHOR + timedelta(
                seconds=rep + _F1_ARR_OFFSETS[row.stop_id] + delay
            )


# --- delay-propagation properties (spec-correct GTFS-RT overlay) ------------

# One service day so the oracle timeline is anchor + elapsed seconds. The
# catalog fixture's latest_dataset.agency_timezone (America/Los_Angeles)
# overrides the generated agency.txt, so the 2026-07-30 service day starts
# at local midnight PDT = 07:00 UTC.
_PROP_ANCHOR = datetime(2026, 7, 30, 7, 0, tzinfo=UTC)
_PROP_NOW = datetime(2026, 7, 30, 14, 0, tzinfo=UTC)  # 07:00 PDT
_PROP_BASE = 28800  # TP's first-stop arrival: 08:00:00
_PROP_STEP = 600
_PROP_DWELL = 60
_PROP_OTHER_SHIFT = 30  # OTHER trip runs 30s behind TP at every stop


def _prop_arr_secs(i: int) -> int:
    return _PROP_BASE + _PROP_STEP * i


def _case_call_stops(n_stops: int, loop: tuple[int, int] | None) -> list[str]:
    """The trip's ordered stop ids: position ``dup_at`` REVISITS ``dup_from``'s
    stop when ``loop`` is set, making the trip a loop (duplicated stop id).
    """
    call_stops = [f"S{i}" for i in range(n_stops)]
    if loop is not None:
        dup_from, dup_at = loop
        call_stops[dup_at] = f"S{dup_from}"
    return call_stops


def _propagation_zip(
    n_stops: int, seq_gap: int, loop: tuple[int, int] | None = None
) -> bytes:
    """One-day UTC feed: trip TP plus an RT-untouched OTHER trip over the
    same stops, with stop_sequence values spaced by ``seq_gap`` (gaps prove
    sequence ADDRESSING, not list indexing). ``loop`` makes both trips
    revisit an earlier stop (see :func:`_case_call_stops`). OTHER runs on
    its own route R2 so route_ids filtering has two distinguishable rows.
    """
    call_stops = _case_call_stops(n_stops, loop)
    stop_rows = "".join(f"S{i},Stop {i},0,0\n" for i in range(n_stops))
    stop_time_rows = ""
    for trip_id, shift in (("TP", 0), ("OTHER", _PROP_OTHER_SHIFT)):
        for i in range(n_stops):
            arr = _format_gtfs_time(_prop_arr_secs(i) + shift)
            dep = _format_gtfs_time(_prop_arr_secs(i) + shift + _PROP_DWELL)
            sequence = seq_gap * (i + 1)
            stop_time_rows += f"{trip_id},{arr},{dep},{call_stops[i]},{sequence}\n"
    files = {
        "agency.txt": _UTC_AGENCY,
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n" + stop_rows,
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\n"
            "R1,1,Line,3\nR2,2,Other Line,3\n"
        ),
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign\nR1,ONE,TP,H\nR2,ONE,OTHER,H\n"
        ),
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + stop_time_rows
        ),
        "calendar.txt": _ONE_DAY_CALENDAR,
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _run_propagation_scenario(
    zip_bytes: bytes,
    msg: gtfs_realtime_pb2.FeedMessage,
    stop_ids: list[str],
    pairs: list[tuple[str, str]],
    *,
    lookahead: timedelta = timedelta(hours=6),
    now: datetime = _PROP_NOW,
) -> tuple[list[StopArrival], dict[tuple[str, str], list[UpcomingTrip]]]:
    """Serve a generated zip + one RT message; run arrivals and pair queries."""

    async def scenario() -> tuple[
        list[StopArrival], dict[tuple[str, str], list[UpcomingTrip]]
    ]:
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
                "/hosted/mdb-100.zip", body=zip_bytes, content_type="application/zip"
            )
            body = msg.SerializeToString()
            for _ in range(1 + len(pairs)):  # one RT fetch per merging call
                api.get("/rt/all", body=body, content_type="application/octet-stream")
            async with MobilityFeedsClient("t", base_url=base) as client:
                handle = await client.get_transit_feed("mdb-100")
                [arrivals] = await handle.get_arrivals(
                    [ArrivalsQuery(stop_ids, limit=150)],
                    lookahead=lookahead,
                    now_utc=now,
                )
                trips = {}
                for origin, destination in pairs:
                    trips[(origin, destination)] = await handle.upcoming_trips(
                        origin,
                        destination,
                        lookahead=lookahead,
                        now_utc=now,
                    )
                return arrivals, trips
        finally:
            await api.stop()

    return asyncio.run(scenario())


def _draw_stu_spec(
    draw: st.DrawFn, i: int, call_stops: list[str], *, forced: bool
) -> dict[str, object] | None:
    """Draw one StopTimeUpdate spec for call position ``i`` (None = no STU).

    Kinds: a delay STU (departure-side, arrival-side, or BOTH with a
    different arrival value that must lose to the departure), an
    explicit-time STU (departure epoch, arrival epoch, or both), both
    combined, an empty SCHEDULED STU, a SKIPPED marker, or a NO_DATA
    marker. Addressing: stop_id, stop_sequence, both (consistent), or
    CONFLICTING both -- stop_sequence names call ``i`` while stop_id names
    a different call, and stop_sequence must win.
    """
    kinds = ["delay", "explicit", "delay_explicit", "empty", "skipped", "no_data"]
    if not forced:
        kinds = ["none", "none", *kinds]
    kind = draw(st.sampled_from(kinds))
    if kind == "none":
        return None
    spec: dict[str, object] = {"kind": kind}
    if kind in ("delay", "delay_explicit"):
        side = draw(st.sampled_from(["departure", "arrival", "both"]))
        spec["delay_side"] = side
        delay = draw(st.integers(-300, 900))
        spec["delay"] = delay
        if side == "both":
            # The departure delay is the "last known delay" and must win;
            # the arrival carries a DIFFERENT value so a preference flip
            # is observable, never masked by an equal draw.
            spec["shadow_delay"] = delay + draw(st.sampled_from([-97, 61, 293]))
    if kind in ("explicit", "delay_explicit"):
        ends = draw(st.sampled_from(["departure", "arrival", "both"]))
        if ends in ("departure", "both"):
            spec["dep_time"] = _PROP_ANCHOR + timedelta(
                seconds=_prop_arr_secs(i) + _PROP_DWELL + draw(st.integers(-120, 1200))
            )
        if ends in ("arrival", "both"):
            spec["arr_time"] = _PROP_ANCHOR + timedelta(
                seconds=_prop_arr_secs(i) + draw(st.integers(-120, 1200))
            )
    addressing_options = ["stop_id", "stop_sequence", "both"]
    conflict_pool = [
        k for k in range(len(call_stops)) if call_stops[k] != call_stops[i]
    ]
    if conflict_pool:
        addressing_options.append("conflict")
    addressing = draw(st.sampled_from(addressing_options))
    spec["addressing"] = addressing
    if addressing == "conflict":
        spec["conflict_index"] = draw(st.sampled_from(conflict_pool))
    return spec


@st.composite
def _propagation_case(draw: st.DrawFn) -> dict[str, object]:
    """A static trip (3-8 stops) plus a drawn StopTimeUpdate set.

    Per stop: no STU or a :func:`_draw_stu_spec` draw. Optional loop trip
    (a later call revisits an earlier stop id, so a bare stop_id STU must
    resolve to the FIRST visit), optional trip-level delay, optional
    unplaceable "ghost" STU, optional DUPLICATE trailing STU for one call
    (the last one in feed order must win), drawn feed order.
    """
    n_stops = draw(st.integers(3, 8))
    loop: tuple[int, int] | None = None
    if draw(st.booleans()):
        dup_at = draw(st.integers(2, n_stops - 1))
        dup_from = draw(st.integers(0, dup_at - 1))
        loop = (dup_from, dup_at)
    call_stops = _case_call_stops(n_stops, loop)
    specs: dict[int, dict[str, object]] = {}
    for i in range(n_stops):
        spec = _draw_stu_spec(draw, i, call_stops, forced=False)
        if spec is not None:
            specs[i] = spec
    extra: tuple[int, dict[str, object]] | None = None
    if draw(st.booleans()):
        target = draw(st.integers(0, n_stops - 1))
        extra_spec = _draw_stu_spec(draw, target, call_stops, forced=True)
        assert extra_spec is not None
        extra = (target, extra_spec)
    return {
        "n_stops": n_stops,
        "seq_gap": draw(st.sampled_from([1, 10])),
        "loop": loop,
        "trip_delay": draw(st.none() | st.integers(-300, 900)),
        "specs": specs,
        "extra": extra,
        "ghost": draw(st.booleans()),
        "reverse_order": draw(st.booleans()),
    }


def _case_feed_order(case: dict[str, object]) -> list[int]:
    """The order the message writer emits the per-call STUs in."""
    specs: dict[int, dict[str, object]] = case["specs"]  # type: ignore[assignment]
    order = sorted(specs)
    if case["reverse_order"]:
        order.reverse()  # feed order must not matter for PLACEMENT
    return order


def _write_stu(
    entity: gtfs_realtime_pb2.FeedEntity,
    i: int,
    spec: dict[str, object],
    seq_gap: int,
    call_stops: list[str],
) -> None:
    """Encode one drawn STU spec for call position ``i``."""
    stu = entity.trip_update.stop_time_update.add()
    addressing = spec["addressing"]
    if addressing in ("stop_id", "both"):
        stu.stop_id = call_stops[i]
    if addressing == "conflict":
        # CONFLICTING addressing: stop_sequence names call i while stop_id
        # names a different call -- stop_sequence must win.
        stu.stop_id = call_stops[spec["conflict_index"]]  # type: ignore[index]
    if addressing in ("stop_sequence", "both", "conflict"):
        stu.stop_sequence = seq_gap * (i + 1)
    kind = spec["kind"]
    if kind == "skipped":
        stu.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
        return
    if kind == "no_data":
        stu.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.NO_DATA
        return
    if "delay" in spec:
        side = spec["delay_side"]
        if side in ("departure", "both"):
            stu.departure.delay = spec["delay"]  # type: ignore[assignment]
        elif side == "arrival":
            stu.arrival.delay = spec["delay"]  # type: ignore[assignment]
        if side == "both":
            stu.arrival.delay = spec["shadow_delay"]  # type: ignore[assignment]
    if "dep_time" in spec:
        stu.departure.time = int(spec["dep_time"].timestamp())  # type: ignore[union-attr]
    if "arr_time" in spec:
        stu.arrival.time = int(spec["arr_time"].timestamp())  # type: ignore[union-attr]


def _propagation_message(case: dict[str, object]) -> gtfs_realtime_pb2.FeedMessage:
    """Encode a drawn case as a TripUpdates FeedMessage for trip TP."""
    specs: dict[int, dict[str, object]] = case["specs"]  # type: ignore[assignment]
    seq_gap: int = case["seq_gap"]  # type: ignore[assignment]
    call_stops = _case_call_stops(case["n_stops"], case["loop"])  # type: ignore[arg-type]
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "tu-prop"
    entity.trip_update.trip.trip_id = "TP"
    if case["trip_delay"] is not None:
        entity.trip_update.delay = case["trip_delay"]  # type: ignore[assignment]
    for i in _case_feed_order(case):
        _write_stu(entity, i, specs[i], seq_gap, call_stops)
    if case["extra"] is not None:
        # A DUPLICATE STU for one call, deliberately emitted LAST so it is
        # the one that must win its placement slot (last-wins, rt.py).
        target, extra_spec = case["extra"]  # type: ignore[misc]
        _write_stu(entity, target, extra_spec, seq_gap, call_stops)
    if case["ghost"]:
        ghost = entity.trip_update.stop_time_update.add()
        ghost.stop_id = "GHOST"  # matches no static call: must change nothing
        ghost.departure.delay = 999
    return msg


def _placed_index(i: int, spec: dict[str, object], call_stops: list[str]) -> int:
    """Which call position an STU emitted for position ``i`` lands on.

    Bare stop_id addressing resolves to the FIRST call with that stop id
    (loop trips revisit); every sequence-bearing form addresses ``i``
    itself (stop_sequence wins over a conflicting stop_id).
    """
    if spec["addressing"] == "stop_id":
        return call_stops.index(call_stops[i])
    return i


def _propagation_oracle(
    case: dict[str, object],
) -> dict[int, object]:
    """Independent piecewise restatement of the spec, from the DRAWN values.

    Per stop index: ``"skipped"``, ``None`` (schedule-only), or a dict with
    the effective delay (may be None for an explicit-only stop) and the
    explicit departure/arrival epochs (None where the prediction is purely
    scheduled+delay). Placement first: each emitted STU lands per
    :func:`_placed_index` in feed order, the LAST one landing on a call
    wins it. Then the walk: last STU delay at-or-before the stop
    propagates, NO_DATA cuts it, explicit times override at their own
    stop, the trip-level delay covers stops no STU information reaches,
    and everything else is schedule-only.
    """
    specs: dict[int, dict[str, object]] = case["specs"]  # type: ignore[assignment]
    call_stops = _case_call_stops(case["n_stops"], case["loop"])  # type: ignore[arg-type]
    placed: dict[int, dict[str, object]] = {}
    for i in _case_feed_order(case):
        spec = specs[i]
        placed[_placed_index(i, spec, call_stops)] = spec
    if case["extra"] is not None:
        target, extra_spec = case["extra"]  # type: ignore[misc]
        placed[_placed_index(target, extra_spec, call_stops)] = extra_spec
    trip_delay = case["trip_delay"]
    outcomes: dict[int, object] = {}
    covered = False
    current: object = None
    for i in range(case["n_stops"]):  # type: ignore[arg-type]
        spec_at = placed.get(i)
        if spec_at is None:
            fallback = current if covered else trip_delay
            outcomes[i] = (
                None
                if fallback is None
                else {"delay": fallback, "dep_time": None, "arr_time": None}
            )
            continue
        kind = spec_at["kind"]
        if kind == "skipped":
            outcomes[i] = "skipped"
            continue
        if kind == "no_data":
            covered, current = True, None
            outcomes[i] = None
            continue
        own_delay = spec_at.get("delay")
        if own_delay is not None:
            covered, current = True, own_delay
        effective = (
            own_delay if own_delay is not None else (current if covered else trip_delay)
        )
        dep_time = spec_at.get("dep_time")
        arr_time = spec_at.get("arr_time")
        if effective is None and dep_time is None and arr_time is None:
            outcomes[i] = None  # an empty STU carries no realtime content
        else:
            outcomes[i] = {
                "delay": effective,
                "dep_time": dep_time,
                "arr_time": arr_time,
            }
    return outcomes


def _assert_prediction_matches_outcome(
    row: StopArrival,
    outcome: dict[str, object],
    sched_arr: datetime,
    sched_dep: datetime,
) -> None:
    """One merged row against one oracle outcome dict (explicit epochs win,
    else scheduled+delay, else no prediction at that end)."""
    delay = outcome["delay"]
    assert row.realtime is True
    assert row.delay_seconds == delay
    if outcome["dep_time"] is not None:
        assert row.predicted_departure == outcome["dep_time"]
    elif delay is not None:
        assert row.predicted_departure == sched_dep + timedelta(seconds=delay)  # type: ignore[arg-type]
    else:
        assert row.predicted_departure is None
    if outcome["arr_time"] is not None:
        assert row.predicted_arrival == outcome["arr_time"]
    elif delay is not None:
        assert row.predicted_arrival == sched_arr + timedelta(seconds=delay)  # type: ignore[arg-type]
    else:
        assert row.predicted_arrival is None


@settings(max_examples=12, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(case=_propagation_case())
def test_propagation_matches_piecewise_oracle(case: dict[str, object]) -> None:
    """THE load-bearing propagation property: for EVERY stop of a generated
    trip under a drawn StopTimeUpdate set, the merged arrival row equals
    the piecewise oracle computed independently from the drawn values --
    and the RT-untouched sibling trip stays schedule-only throughout.
    Rows are keyed by scheduled departure, not stop_id: loop trips visit a
    stop twice, and both visits must resolve independently.
    """
    n_stops: int = case["n_stops"]  # type: ignore[assignment]
    call_stops = _case_call_stops(n_stops, case["loop"])  # type: ignore[arg-type]
    stop_ids = [f"S{i}" for i in range(n_stops)]
    arrivals, _ = _run_propagation_scenario(
        _propagation_zip(n_stops, case["seq_gap"], case["loop"]),  # type: ignore[arg-type]
        _propagation_message(case),
        stop_ids,
        [],
    )
    outcomes = _propagation_oracle(case)
    by_key = {(a.trip_id, a.scheduled_departure): a for a in arrivals}
    assert len(by_key) == len(arrivals)
    for i in range(n_stops):
        sched_arr = _PROP_ANCHOR + timedelta(seconds=_prop_arr_secs(i))
        sched_dep = sched_arr + timedelta(seconds=_PROP_DWELL)
        # The RT-untouched sibling: always present, always schedule-only.
        other = by_key[("OTHER", sched_dep + timedelta(seconds=_PROP_OTHER_SHIFT))]
        assert other.stop_id == call_stops[i]
        assert other.realtime is False
        assert other.predicted_departure is None
        assert other.delay_seconds is None
        outcome = outcomes[i]
        if outcome == "skipped":
            assert ("TP", sched_dep) not in by_key
            continue
        row = by_key[("TP", sched_dep)]
        assert row.stop_id == call_stops[i]
        assert row.scheduled_departure == sched_dep
        if outcome is None:
            assert row.realtime is False
            assert row.predicted_arrival is None
            assert row.predicted_departure is None
            assert row.delay_seconds is None
            continue
        assert isinstance(outcome, dict)
        _assert_prediction_matches_outcome(row, outcome, sched_arr, sched_dep)


# The PURE volume property: same drawn cases and the same oracle, but run
# straight through trip_updates_from_message + resolve_trip_predictions with
# no server, zip build, or SQLite in the loop -- so the example budget can
# be two orders of magnitude higher than the merged end-to-end property's.
@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(case=_propagation_case())
def test_propagation_pure_oracle_matches_resolver(case: dict[str, object]) -> None:
    n_stops: int = case["n_stops"]  # type: ignore[assignment]
    seq_gap: int = case["seq_gap"]  # type: ignore[assignment]
    call_stops = _case_call_stops(n_stops, case["loop"])  # type: ignore[arg-type]
    updates = trip_updates_from_message(_propagation_message(case))
    entry = updates.trips[("TP", None, None)]
    stop_calls = [(seq_gap * (i + 1), call_stops[i]) for i in range(n_stops)]
    resolved = resolve_trip_predictions(entry, stop_calls)
    outcomes = _propagation_oracle(case)
    assert resolved.skipped == {
        seq_gap * (i + 1) for i, outcome in outcomes.items() if outcome == "skipped"
    }
    for i in range(n_stops):
        seq = seq_gap * (i + 1)
        outcome = outcomes[i]
        if outcome == "skipped" or outcome is None:
            assert seq not in resolved.predictions
            continue
        assert isinstance(outcome, dict)
        prediction = resolved.predictions[seq]
        assert prediction.delay_seconds == outcome["delay"]
        assert prediction.departure == outcome["dep_time"]
        assert prediction.arrival == outcome["arr_time"]


@settings(max_examples=12, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    n_stops=st.integers(3, 5),
    data=st.data(),
)
def test_skipped_totality_over_arrivals_and_trips(
    n_stops: int, data: st.DataObject
) -> None:
    """SKIPPED totality: any drawn subset of skipped stops (optionally
    alongside a real delay) removes exactly those stops from arrivals and
    kills exactly the origin->destination rows whose boarding OR alighting
    stop is skipped -- every other row, including the whole RT-untouched
    sibling trip, is unaffected.
    """
    skipped = data.draw(st.sets(st.integers(0, n_stops - 1), max_size=n_stops))
    with_delay = data.draw(st.booleans()) and 0 not in skipped
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "tu-skip"
    entity.trip_update.trip.trip_id = "TP"
    if with_delay:
        first = entity.trip_update.stop_time_update.add()
        first.stop_id = "S0"
        first.departure.delay = 240
    for i in sorted(skipped):
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = f"S{i}"
        stu.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    stop_ids = [f"S{i}" for i in range(n_stops)]
    pairs = [(f"S{i}", f"S{j}") for i in range(n_stops) for j in range(i + 1, n_stops)]
    arrivals, trips_by_pair = _run_propagation_scenario(
        _propagation_zip(n_stops, 1), msg, stop_ids, pairs
    )
    tp_stops = {a.stop_id for a in arrivals if a.trip_id == "TP"}
    assert tp_stops == {f"S{i}" for i in range(n_stops) if i not in skipped}
    other_stops = {a.stop_id for a in arrivals if a.trip_id == "OTHER"}
    assert other_stops == set(stop_ids)  # sibling trip fully unaffected
    for (origin, destination), rows in trips_by_pair.items():
        i, j = int(origin[1:]), int(destination[1:])
        got = {row.trip_id for row in rows}
        expected = {"OTHER"}  # never touched by RT: every pair keeps its row
        if i not in skipped and j not in skipped:
            expected.add("TP")
        assert got == expected
        for row in rows:
            if row.trip_id == "OTHER":
                assert row.realtime is False
            elif with_delay:
                # The S0 delay propagates over the whole trip, so every
                # surviving TP journey row is realtime with delay 240.
                assert row.realtime is True
                assert row.delay_seconds == 240


# --- TripDescriptor.start_date service-day matching properties ---------------

# Garbage start_date cells: wrong lengths, non-digits, calendar-invalid
# dates, year zero -- all must parse to None and behave as absent.
_GARBAGE_START_DATES = ["20261332", "2026073", "202607301", "garbage!", "00000000"]


@given(raw=st.text(max_size=12))
def test_trip_start_date_parse_is_total(raw: str) -> None:
    """Any producer string in start_date parses to a date or None -- never
    raises (totality over the full unicode input space).
    """
    trip = gtfs_realtime_pb2.TripDescriptor()
    trip.start_date = raw
    result = _trip_start_date(trip)
    assert result is None or isinstance(result, date)


@given(day=st.dates())
def test_trip_start_date_round_trips_valid_dates(day: date) -> None:
    """Every valid YYYYMMDD cell parses back to exactly its date."""
    trip = gtfs_realtime_pb2.TripDescriptor()
    trip.start_date = f"{day.year:04d}{day.month:02d}{day.day:02d}"
    assert _trip_start_date(trip) == day


# Two-service-day schedule for the disambiguation oracle: plain trip TP and
# frequency trip FQ (repetitions FQ#32400 / FQ#33000), each running on BOTH
# 2026-07-30 and 2026-07-31 (America/Los_Angeles via the catalog fixture, so
# anchors are 07:00 UTC). Any two same-identity instances are exactly 24h
# apart -- the collision start_date exists to resolve.
_TWO_DAY_ANCHOR_A = datetime(2026, 7, 30, 7, 0, tzinfo=UTC)
_TWO_DAY_ANCHOR_B = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
_TWO_DAY_A = date(2026, 7, 30)
_TWO_DAY_B = date(2026, 7, 31)
_TWO_DAY_NOW = datetime(2026, 7, 30, 14, 0, tzinfo=UTC)
_TWO_DAY_CONCRETE_IDS = ("TP", "FQ#32400", "FQ#33000")
# Per-stop departure offsets from the service-day anchor, per concrete trip.
_TWO_DAY_DEP_OFFSETS = {
    "TP": {"S0": 28810, "S1": 29410},
    "FQ#32400": {"S0": 32410, "S1": 33010},
    "FQ#33000": {"S0": 33010, "S1": 33610},
}


def _two_instance_zip() -> bytes:
    files = {
        "agency.txt": _UTC_AGENCY,  # catalog fixture overrides tz to LA
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS0,A,0,0\nS1,B,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign\nR1,TWO,TP,H\nR1,TWO,FQ,H\n"
        ),
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "TP,08:00:00,08:00:10,S0,1\n"
            "TP,08:10:00,08:10:10,S1,2\n"
            "FQ,09:00:00,09:00:10,S0,1\n"
            "FQ,09:10:00,09:10:10,S1,2\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\nTWO,1,1,1,1,1,1,1,20260730,20260731\n"
        ),
        # Repetitions 09:00:00 (32400) and 09:10:00 (33000); 09:20:00 lands
        # on end_time and does not run.
        "frequencies.txt": (
            "trip_id,start_time,end_time,headway_secs\nFQ,09:00:00,09:20:00,600\n"
        ),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _two_instance_message(
    *,
    variant: str,
    kind: str,
    start_date_cell: str,
    rep: int,
    delay: int,
) -> gtfs_realtime_pb2.FeedMessage:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "tu-two-day"
    trip = entity.trip_update.trip
    trip.trip_id = "TP" if variant == "plain" else "FQ"
    if variant == "frequency":
        trip.start_time = _format_gtfs_time(rep)
    if start_date_cell:
        trip.start_date = start_date_cell
    if kind == "cancellation":
        trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    else:
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S0"
        stu.departure.delay = delay
    return msg


def _instance_day(scheduled_departure: datetime) -> date:
    return _TWO_DAY_A if scheduled_departure < _TWO_DAY_ANCHOR_B else _TWO_DAY_B


# 40+ examples: 2 variants x 2 kinds x 4 date modes x 2 reps = 32 core
# combos (before delay/lookahead variation); 12 examples undersampled it.
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    variant=st.sampled_from(["plain", "frequency"]),
    kind=st.sampled_from(["prediction", "cancellation"]),
    date_mode=st.sampled_from(["day_a", "day_b", "absent", "garbage"]),
    rep=st.sampled_from([32400, 33000]),
    delay=st.integers(-600, 1800),
    lookahead_hours=st.integers(27, 48),
    garbage=st.sampled_from(_GARBAGE_START_DATES),
)
def test_two_instance_start_date_disambiguation_oracle(
    *,
    variant: str,
    kind: str,
    date_mode: str,
    rep: int,
    delay: int,
    lookahead_hours: int,
    garbage: str,
) -> None:
    """THE start_date property: with BOTH daily instances of every identity
    in a >=27h window, a drawn prediction or cancellation affects exactly
    one instance and never its 24h-apart sibling. A date naming day A or
    day B affects that day's instance; an absent date affects the
    currently-active (earliest in-window) instance -- day A; a garbage
    date parses to None and behaves exactly as absent. Every other trip
    instance -- the sibling day AND the sibling repetition/plain trip --
    stays schedule-only, in both the arrivals merge and the
    origin->destination overlay.
    """
    start_date_cell = {
        "day_a": "20260730",
        "day_b": "20260731",
        "absent": "",
        "garbage": garbage,
    }[date_mode]
    target_id = "TP" if variant == "plain" else f"FQ#{rep}"
    affected_day = _TWO_DAY_B if date_mode == "day_b" else _TWO_DAY_A
    msg = _two_instance_message(
        variant=variant,
        kind=kind,
        start_date_cell=start_date_cell,
        rep=rep,
        delay=delay,
    )
    arrivals, trips_by_pair = _run_propagation_scenario(
        _two_instance_zip(),
        msg,
        ["S0", "S1"],
        [("S0", "S1")],
        lookahead=timedelta(hours=lookahead_hours),
        now=_TWO_DAY_NOW,
    )
    all_rows = {
        (trip_id, day, stop)
        for trip_id in _TWO_DAY_CONCRETE_IDS
        for day in (_TWO_DAY_A, _TWO_DAY_B)
        for stop in ("S0", "S1")
    }
    got_rows = {
        (a.trip_id, _instance_day(a.scheduled_departure), a.stop_id)
        for a in arrivals
        if a.scheduled_departure is not None and a.trip_id is not None
    }
    assert len(got_rows) == len(arrivals)
    if kind == "cancellation":
        # Exactly the affected instance's rows disappear; nothing else is
        # touched (all survivors schedule-only).
        assert got_rows == all_rows - {
            (target_id, affected_day, stop) for stop in ("S0", "S1")
        }
        assert all(a.realtime is False for a in arrivals)
    else:
        assert got_rows == all_rows
        anchor = {
            _TWO_DAY_A: _TWO_DAY_ANCHOR_A,
            _TWO_DAY_B: _TWO_DAY_ANCHOR_B,
        }
        for row in arrivals:
            assert row.trip_id is not None
            assert row.scheduled_departure is not None
            inst_day = _instance_day(row.scheduled_departure)
            is_target = row.trip_id == target_id and inst_day == affected_day
            assert row.realtime is is_target
            if is_target:
                # S0 carries the STU's own delay; S1 the propagated one.
                assert row.delay_seconds == delay
                offset = _TWO_DAY_DEP_OFFSETS[row.trip_id][row.stop_id]
                assert row.predicted_departure == anchor[inst_day] + timedelta(
                    seconds=offset + delay
                )
            else:
                assert row.predicted_departure is None
                assert row.delay_seconds is None
    # The origin->destination overlay resolves instances identically.
    journeys = trips_by_pair[("S0", "S1")]
    got_journeys = {(t.trip_id, _instance_day(t.scheduled_departure)) for t in journeys}
    all_journeys = {
        (trip_id, day)
        for trip_id in _TWO_DAY_CONCRETE_IDS
        for day in (_TWO_DAY_A, _TWO_DAY_B)
    }
    if kind == "cancellation":
        assert got_journeys == all_journeys - {(target_id, affected_day)}
        assert all(t.realtime is False for t in journeys)
    else:
        assert got_journeys == all_journeys
        for journey in journeys:
            inst_day = _instance_day(journey.scheduled_departure)
            is_target = journey.trip_id == target_id and inst_day == affected_day
            assert journey.realtime is is_target
            if is_target:
                assert journey.delay_seconds == delay


def _short_window_message(
    kind: str, start_date_cell: str, delay: int
) -> gtfs_realtime_pb2.FeedMessage:
    """One TP update against the one-day propagation zip, optionally dated."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "tu-short"
    entity.trip_update.trip.trip_id = "TP"
    if start_date_cell:
        entity.trip_update.trip.start_date = start_date_cell
    if kind == "cancellation":
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )
    else:
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S0"
        stu.departure.delay = delay
    return msg


@settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    kind=st.sampled_from(["prediction", "cancellation"]),
    absent_form=st.sampled_from(["absent", "garbage"]),
    garbage=st.sampled_from(_GARBAGE_START_DATES),
    delay=st.integers(-600, 1800),
)
def test_short_window_start_date_invariance(
    kind: str, absent_form: str, garbage: str, delay: int
) -> None:
    """With a <24h window (single instance per identity, the pre-start_date
    regime), a matching date (2026-07-30, the propagation fixture's only
    service day) and an absent date -- or a garbage one, which parses to
    None -- behave IDENTICALLY, pinning that single-instance behavior did
    not change; a different day's date attaches nowhere: no prediction, no
    cancellation.
    """
    zip_bytes = _propagation_zip(3, 1)
    stop_ids = ["S0", "S1", "S2"]
    absent_cell = "" if absent_form == "absent" else garbage
    matching, matching_trips = _run_propagation_scenario(
        zip_bytes,
        _short_window_message(kind, "20260730", delay),
        stop_ids,
        [("S0", "S2")],
    )
    absent, absent_trips = _run_propagation_scenario(
        zip_bytes,
        _short_window_message(kind, absent_cell, delay),
        stop_ids,
        [("S0", "S2")],
    )
    assert matching == absent
    assert matching_trips == absent_trips
    # Guard against vacuous equality: the matching-date run really attached.
    tp_rows = [a for a in matching if a.trip_id == "TP"]
    if kind == "cancellation":
        assert tp_rows == []
    else:
        assert tp_rows and all(a.realtime is True for a in tp_rows)
    wrong_day, wrong_day_trips = _run_propagation_scenario(
        zip_bytes,
        _short_window_message(kind, "20260731", delay),
        stop_ids,
        [("S0", "S2")],
    )
    assert {(a.trip_id, a.stop_id) for a in wrong_day} == {
        (trip_id, stop) for trip_id in ("TP", "OTHER") for stop in stop_ids
    }
    assert all(a.realtime is False for a in wrong_day)
    assert {t.trip_id for t in wrong_day_trips[("S0", "S2")]} == {"TP", "OTHER"}
    assert all(t.realtime is False for t in wrong_day_trips[("S0", "S2")])


# done_bytes is drawn non-negative deliberately. Both producers in
# transit.py accumulate len(chunk) / a row counter from 0, so a negative
# value is unreachable; the dataclass accepts one and `fraction` would
# return a negative float, but asserting a clamp the code does not perform
# would be asserting a wish. The ceiling is well past 2**53: this code path
# divides two ints into a float, the documented precision-loss hot spot.
_PROGRESS_BYTES = st.integers(0, 2**70)


@given(done=_PROGRESS_BYTES, total=_PROGRESS_BYTES | st.none())
def test_build_progress_fraction_laws(done: int, total: int | None) -> None:
    """``fraction`` is None if and only if ``total_bytes`` is falsy, and
    otherwise lands in [0, 1].

    The iff is the real claim: total_bytes == 0 yields None rather than
    0.0, a deliberate-looking choice ("unknown total", same as missing
    Content-Length) that nothing asserted before. The lower bound alone is
    vacuous by construction over this domain.
    """
    fraction = StaticBuildProgress(
        phase="index", done_bytes=done, total_bytes=total
    ).fraction
    assert (fraction is None) is (not total)
    assert fraction is None or 0.0 <= fraction <= 1.0


@given(total=st.integers(1, 2**70), dones=st.lists(_PROGRESS_BYTES, min_size=2))
def test_build_progress_fraction_is_monotonic_in_done_bytes(
    total: int, dones: list[int]
) -> None:
    """More bytes done never means less progress reported, including past
    the clamp where done_bytes exceeds total_bytes."""
    fractions = [
        StaticBuildProgress(
            phase="download", done_bytes=done, total_bytes=total
        ).fraction
        for done in sorted(dones)
    ]
    assert fractions == sorted(fractions)


def test_build_progress_fraction_saturates_beyond_float_precision() -> None:
    """Pinned, not a bug to fix: int/int -> float, so a total above 2**53
    reports 1.0 while bytes remain. Unreachable at real dataset sizes
    (2**53 bytes is 9 PB), which is why this is documented rather than
    fixed with integer arithmetic.
    """
    total = 2**60
    assert (
        StaticBuildProgress(
            phase="download", done_bytes=total - 1, total_bytes=total
        ).fraction
        == 1.0
    )


# --- station grouping properties (transit.group_stations) --------------------

# Names drawn from a small pool with deliberate casefold collisions (plus
# None) so parentless same-name grouping is exercised on most examples.
_GROUP_NAMES = (
    st.sampled_from(
        ["Metro Center", "metro center", "METRO CENTER", "Union Sq", "union sq"]
    )
    | st.none()
)


def _mk_stop(
    stop_id: str,
    name: str | None = None,
    parent: str | None = None,
    location_type: StopLocationType | None = None,
) -> Stop:
    return Stop(
        id=stop_id,
        name=name,
        latitude=None,
        longitude=None,
        parent_station=parent,
        location_type=location_type,
        stop_code=None,
        platform_code=None,
        wheelchair_boarding=None,
        description=None,
        url=None,
        zone_id=None,
        timezone=None,
    )


@given(data=st.data())
@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_group_stations_partition_oracle(data: st.DataObject) -> None:
    """Over drawn hierarchies -- parent links (including dangling ones),
    every location type, casefold-colliding names -- every boarding stop
    lands in EXACTLY one group, entrances/nodes/boarding areas and station
    records themselves in none, the parent station's name wins over member
    names, and the output is name-sorted. Oracle restated from the drawn
    values: group key = parent station id, else casefolded (name or id).
    """
    stations = [
        _mk_stop(
            f"ST{k}",
            name=data.draw(_GROUP_NAMES),
            location_type=StopLocationType.STATION,
        )
        for k in range(data.draw(st.integers(0, 3)))
    ]
    parent_pool = [station.id for station in stations] + ["MISSING"]
    boarding = [
        _mk_stop(
            f"B{k}",
            name=data.draw(_GROUP_NAMES),
            parent=data.draw(st.none() | st.sampled_from(parent_pool)),
            location_type=data.draw(st.sampled_from([None, StopLocationType.STOP])),
        )
        for k in range(data.draw(st.integers(0, 6)))
    ]
    never_grouped = [
        _mk_stop(
            f"X{k}",
            name=data.draw(_GROUP_NAMES),
            parent=data.draw(st.none() | st.sampled_from(parent_pool)),
            location_type=data.draw(
                st.sampled_from(
                    [
                        StopLocationType.ENTRANCE_EXIT,
                        StopLocationType.GENERIC_NODE,
                        StopLocationType.BOARDING_AREA,
                    ]
                )
            ),
        )
        for k in range(data.draw(st.integers(0, 3)))
    ]
    ordered = data.draw(st.permutations(stations + boarding + never_grouped))
    groups = group_stations(ordered)
    # Independent oracle from the drawn stops, in the same input order.
    station_names = {station.id: station.name for station in stations}
    expected: dict[str, tuple[str, list[str]]] = {}
    for stop in ordered:
        if stop.location_type not in (None, StopLocationType.STOP):
            continue
        if stop.parent_station:
            key = stop.parent_station
            name = station_names.get(stop.parent_station) or stop.name or key
        else:
            name = stop.name or stop.id
            key = name.casefold()
        if key in expected:
            expected[key][1].append(stop.id)
        else:
            expected[key] = (name, [stop.id])
    assert {g.id: (g.name, list(g.stop_ids)) for g in groups} == expected
    assert [g.name for g in groups] == sorted(g.name for g in groups)
    # Partition: every boarding stop in exactly one group; nothing else in any.
    member_ids = [stop_id for g in groups for stop_id in g.stop_ids]
    assert sorted(member_ids) == sorted(stop.id for stop in boarding)
    assert len(member_ids) == len(set(member_ids))


# --- multi-RT-feed aggregation properties (direct-URL, two TU sources) -------

# Direct-URL handles read the timezone from agency.txt (UTC here), so the
# 2026-07-30 service day anchors at 00:00 UTC and the TP/OTHER trips run
# 08:00-09:1x UTC.
_MULTI_RT_NOW = datetime(2026, 7, 30, 7, 0, tzinfo=UTC)


def _multi_rt_message(kind: str, delay: int) -> gtfs_realtime_pb2.FeedMessage:
    """One TU source's message about TP: a delay, a cancellation, or nothing."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    if kind == "absent":
        return msg
    entity = msg.entity.add()
    entity.id = "tu"
    entity.trip_update.trip.trip_id = "TP"
    if kind == "cancel":
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )
    else:
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S0"
        stu.departure.delay = delay
    return msg


def _run_multi_rt_scenario(
    msg_one: gtfs_realtime_pb2.FeedMessage, msg_two: gtfs_realtime_pb2.FeedMessage
) -> list[StopArrival]:
    """Two scripted TU sources behind one DIRECT-URL handle."""

    async def scenario() -> list[StopArrival]:
        api = MockApi()
        await api.start()
        try:
            api.head("/static.zip")  # no validators: the sha256 path is fine
            api.get(
                "/static.zip",
                body=_propagation_zip(3, 1),
                content_type="application/zip",
            )
            api.get(
                "/rt/one",
                body=msg_one.SerializeToString(),
                content_type="application/octet-stream",
            )
            api.get(
                "/rt/two",
                body=msg_two.SerializeToString(),
                content_type="application/octet-stream",
            )
            async with MobilityFeedsClient() as client:
                handle = await client.get_transit_feed_from_urls(
                    api.url("/static.zip"),
                    trip_updates_urls=[api.url("/rt/one"), api.url("/rt/two")],
                )
                [arrivals] = await handle.get_arrivals(
                    [ArrivalsQuery(["S0", "S1", "S2"], limit=150)],
                    lookahead=timedelta(hours=6),
                    now_utc=_MULTI_RT_NOW,
                )
                return arrivals
        finally:
            await api.stop()

    return asyncio.run(scenario())


@settings(max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    kind_one=st.sampled_from(["delay", "cancel", "absent"]),
    kind_two=st.sampled_from(["delay", "cancel", "absent"]),
    delay_one=st.integers(-300, 900),
    delay_gap=st.integers(1, 500),
)
def test_multi_rt_aggregation_last_feed_wins_and_cancellation_suppresses(
    kind_one: str, kind_two: str, delay_one: int, delay_gap: int
) -> None:
    """Two TU-capable sources sharing the TP identity: when both carry an
    entry, the LAST feed's delay wins wholesale (values drawn distinct so a
    flip is observable); a cancellation in EITHER source suppresses the
    rows end-to-end -- including the delay+cancel mix where the identity
    sits in both ``trips`` and ``canceled_trips`` after aggregation -- and
    the RT-untouched sibling trip is never affected.
    """
    delay_two = delay_one + delay_gap  # distinct by construction
    arrivals = _run_multi_rt_scenario(
        _multi_rt_message(kind_one, delay_one),
        _multi_rt_message(kind_two, delay_two),
    )
    other_rows = [row for row in arrivals if row.trip_id == "OTHER"]
    assert {row.stop_id for row in other_rows} == {"S0", "S1", "S2"}
    assert all(row.realtime is False for row in other_rows)
    tp_rows = [row for row in arrivals if row.trip_id == "TP"]
    if "cancel" in (kind_one, kind_two):
        assert tp_rows == []  # cancellation wins over any sibling entry
        return
    assert {row.stop_id for row in tp_rows} == {"S0", "S1", "S2"}
    if kind_one == kind_two == "absent":
        assert all(row.realtime is False for row in tp_rows)
        return
    # Exactly one or both feeds sent a delay entry: the LAST feed that sent
    # one wins wholesale (the S0 delay propagates over the whole trip).
    expected_delay = delay_two if kind_two == "delay" else delay_one
    for row in tp_rows:
        assert row.realtime is True
        assert row.delay_seconds == expected_delay


# --- get_arrivals route_ids filter properties --------------------------------


def _route_filter_added_message(
    added_route: str | None,
) -> gtfs_realtime_pb2.FeedMessage:
    """A TP delay plus one ADDED trip at S0 announcing ``added_route``."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "tu-tp"
    entity.trip_update.trip.trip_id = "TP"
    stu = entity.trip_update.stop_time_update.add()
    stu.stop_id = "S0"
    stu.departure.delay = 120
    added = msg.entity.add()
    added.id = "tu-added"
    added.trip_update.trip.trip_id = "GEN-ADDED-R"
    if added_route is not None:
        added.trip_update.trip.route_id = added_route
    added.trip_update.trip.schedule_relationship = (
        gtfs_realtime_pb2.TripDescriptor.ADDED
    )
    added_stu = added.trip_update.stop_time_update.add()
    added_stu.stop_id = "S0"
    added_stu.departure.time = int(
        datetime(2026, 7, 30, 15, 10, tzinfo=UTC).timestamp()
    )
    return msg


def _run_route_filter_scenario(
    msg: gtfs_realtime_pb2.FeedMessage, route_ids: list[str]
) -> tuple[list[StopArrival], list[StopArrival]]:
    """The catalog propagation scenario, queried unfiltered THEN filtered."""

    async def scenario() -> tuple[list[StopArrival], list[StopArrival]]:
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
                body=_propagation_zip(3, 1),
                content_type="application/zip",
            )
            body = msg.SerializeToString()
            for _ in range(2):  # one RT fetch per get_arrivals call
                api.get("/rt/all", body=body, content_type="application/octet-stream")
            async with MobilityFeedsClient("t", base_url=base) as client:
                handle = await client.get_transit_feed("mdb-100")
                [unfiltered] = await handle.get_arrivals(
                    [ArrivalsQuery(["S0", "S1", "S2"], limit=150)],
                    lookahead=timedelta(hours=6),
                    now_utc=_PROP_NOW,
                )
                [filtered] = await handle.get_arrivals(
                    [ArrivalsQuery(["S0", "S1", "S2"], route_ids, limit=150)],
                    lookahead=timedelta(hours=6),
                    now_utc=_PROP_NOW,
                )
                return unfiltered, filtered
        finally:
            await api.stop()

    return asyncio.run(scenario())


@settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    route_ids=st.sampled_from([["R1"], ["R2"], ["R1", "R2"], ["NOPE"]]),
    added_route=st.sampled_from(["R1", "R2", None]),
)
def test_arrivals_route_filter_matches_oracle(
    route_ids: list[str], added_route: str | None
) -> None:
    """An ArrivalsQuery's route_ids returns EXACTLY the oracle-side filter of
    the unfiltered merge: scheduled rows by their trip's route (TP on R1,
    OTHER on R2), RT-ADDED rows by their announced route -- and an added
    row announcing NO route never passes any filter.
    """
    unfiltered, filtered = _run_route_filter_scenario(
        _route_filter_added_message(added_route), route_ids
    )
    # Sanity: the unfiltered merge carries both trips and the added row.
    assert {row.trip_id for row in unfiltered} == {"TP", "OTHER", "GEN-ADDED-R"}
    allowed = set(route_ids)
    assert filtered == [
        row
        for row in unfiltered
        if row.route_id is not None and row.route_id in allowed
    ]


# --- GBFS discovery-document and endpoint-preference properties --------------

_DISCOVERY_FEED_ENTRY: st.SearchStrategy[object] = st.one_of(
    st.fixed_dictionaries(
        {
            "name": st.sampled_from(
                ["system_information", "station_status", "free_bike_status", ""]
            ),
            "url": st.sampled_from(["https://e.com/a", "https://e.com/b", ""]),
        }
    ),
    st.fixed_dictionaries(
        {"name": st.sampled_from(["system_information", "vehicle_status"])}
    ),  # url missing entirely
    st.just("not-a-feed-object"),
    st.just(None),
)


def _draw_language_blocks(data: st.DataObject) -> dict[str, object]:
    """Drawn 2.x ``data`` payload: language -> feeds block / junk."""
    languages = data.draw(
        st.lists(
            st.sampled_from(["en", "fr", "de"]), min_size=1, max_size=3, unique=True
        )
    )
    blocks: dict[str, object] = {}
    for language in languages:
        block_kind = data.draw(
            st.sampled_from(["feeds", "no_feeds", "bad_feeds", "garbage"])
        )
        if block_kind == "feeds":
            blocks[language] = {
                "feeds": data.draw(st.lists(_DISCOVERY_FEED_ENTRY, max_size=4))
            }
        elif block_kind == "no_feeds":
            blocks[language] = {}
        elif block_kind == "bad_feeds":
            blocks[language] = {"feeds": "not-a-list"}
        else:
            blocks[language] = "garbage-block"
    return blocks


def _expected_language_feeds(blocks: dict[str, object]) -> list[object]:
    """Oracle language preference: "en" when it carries a feeds LIST, else
    the first block (insertion order) that does, else nothing."""
    en_block = blocks.get("en")
    if isinstance(en_block, dict) and isinstance(en_block.get("feeds"), list):
        return list(en_block["feeds"])
    for block in blocks.values():
        if isinstance(block, dict) and isinstance(block.get("feeds"), list):
            return list(block["feeds"])
    return []


@given(data=st.data())
@settings(max_examples=100, deadline=None)
def test_endpoints_from_discovery_layouts_oracle(data: st.DataObject) -> None:
    """Generated 3.x direct (``data.feeds``) and 2.x language-keyed
    (``data.<lang>.feeds``) discovery layouts -- with invalid entries,
    blank names/urls, garbage language blocks, and feeds-less blocks --
    resolve to exactly the oracle's name->url table (preferred language
    first, else the first block carrying a feeds list), and FeedParseError
    is raised IFF no usable feeds remain.
    """
    layout = data.draw(st.sampled_from(["3.x", "2.x"]))
    if layout == "3.x":
        chosen = data.draw(st.lists(_DISCOVERY_FEED_ENTRY, max_size=4))
        document: dict[str, object] = {"data": {"feeds": chosen}}
    else:
        blocks = _draw_language_blocks(data)
        document = {"data": blocks}
        chosen = _expected_language_feeds(blocks)
    expected = {
        str(feed["name"]): str(feed["url"])
        for feed in chosen
        if isinstance(feed, dict) and feed.get("name") and feed.get("url")
    }
    if expected:
        assert _endpoints_from_discovery(document) == expected
    else:
        with pytest.raises(FeedParseError):
            _endpoints_from_discovery(document)


def _vehicle_handle(endpoints: dict[str, dict[str, object]]) -> GbfsFeedHandle:
    """A detached handle whose named endpoints serve pre-cached documents."""
    handle = GbfsFeedHandle.__new__(GbfsFeedHandle)
    handle._doc_cache = {
        name: (float("inf"), float("inf"), {"data": payload})
        for name, payload in endpoints.items()
    }
    handle._endpoints = dict.fromkeys(endpoints, "x")
    return handle


@given(
    rows=st.lists(
        st.fixed_dictionaries(
            {
                "id": st.text(min_size=1, max_size=6),
                "lat": _NEARBY_LAT | st.none(),
                "lon": _NEARBY_LON | st.none(),
                "is_reserved": st.booleans() | st.none(),
                "is_disabled": st.booleans() | st.none(),
                "vehicle_type_id": st.text(min_size=1, max_size=4) | st.none(),
                "current_range_meters": st.floats(0, 50_000) | st.none(),
            }
        ),
        max_size=6,
    )
)
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_gbfs_vehicle_status_and_free_bike_status_equivalent(
    rows: list[dict[str, object]],
) -> None:
    """Metamorphic 3.x/2.x path law: IDENTICAL drawn rows served as
    ``vehicle_status`` (3.x: ``vehicles``/``vehicle_id``) vs
    ``free_bike_status`` (2.x: ``bikes``/``bike_id``) parse to identical
    vehicles, and a system publishing BOTH endpoints reads 3.x -- proven
    with a decoy 2.x document that must NOT surface.
    """
    v_rows = [
        {**{k: v for k, v in row.items() if k != "id"}, "vehicle_id": row["id"]}
        for row in rows
    ]
    b_rows = [
        {**{k: v for k, v in row.items() if k != "id"}, "bike_id": row["id"]}
        for row in rows
    ]
    via_30 = asyncio.run(
        _vehicle_handle({"vehicle_status": {"vehicles": v_rows}}).get_vehicles()
    )
    via_23 = asyncio.run(
        _vehicle_handle({"free_bike_status": {"bikes": b_rows}}).get_vehicles()
    )
    assert via_30 == via_23
    decoy = [{"bike_id": "DECOY", "lat": 34.05, "lon": -118.25}]
    via_both = asyncio.run(
        _vehicle_handle(
            {
                "vehicle_status": {"vehicles": v_rows},
                "free_bike_status": {"bikes": decoy},
            }
        ).get_vehicles()
    )
    assert via_both == via_30
    assert all(vehicle.id != "DECOY" for vehicle in via_both)
