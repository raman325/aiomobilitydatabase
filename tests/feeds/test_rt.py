"""Tests for GTFS-RT fetching (auth, errors) and protobuf parsing."""

from datetime import UTC, date, datetime

import aiohttp
import pytest
from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds.exceptions import (
    FeedParseError,
    SourceAuthenticationError,
    SourceConnectionError,
)
from aiomobilitydatabase.feeds.models import (
    AlertCause,
    AlertEffect,
    CongestionLevel,
    OccupancyStatus,
    Route,
    VehicleStopStatus,
    WheelchairAccess,
)
from aiomobilitydatabase.feeds.rt import (
    TripStopUpdate,
    TripUpdateEntry,
    _pb_enum_name,
    _vocab_or_none,
    alerts_from_message,
    fetch_feed_message,
    resolve_trip_predictions,
    trip_updates_from_message,
    vehicles_from_message,
)

from tests.feeds.fixtures import (
    ALERTS,
    ALERTS_TRIP_SCOPED,
    TRIP_UPDATES_BASELINE,
    VEHICLE_POSITIONS,
    VEHICLE_POSITIONS_STATUS,
)
from tests.mock_server import MockApi

PB = "application/octet-stream"


def _route(route_id: str, short_name: str, long_name: str) -> Route:
    return Route(
        id=route_id,
        short_name=short_name,
        long_name=long_name,
        type=3,
        agency_id=None,
        color=None,
        text_color=None,
        url=None,
        description=None,
        sort_order=None,
    )


async def _fetch(
    mock_api: MockApi, path: str = "/rt", **kwargs: object
) -> gtfs_realtime_pb2.FeedMessage:
    async with aiohttp.ClientSession() as session:
        message, _ = await fetch_feed_message(session, mock_api.url(path), **kwargs)  # type: ignore[arg-type]
        assert message is not None
        return message


async def test_fetch_plain(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    msg = await _fetch(mock_api)
    assert len(msg.entity) == 2


async def test_fetch_header_auth(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    await _fetch(mock_api, auth_type=2, api_key_name="X-Api-Key", api_key="secret123")
    assert mock_api.requests[-1].headers.get("X-Api-Key") == "secret123"


async def test_fetch_query_param_auth(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    await _fetch(mock_api, auth_type=1, api_key_name="api_key", api_key="secret123")
    assert mock_api.requests[-1].query.get("api_key") == "secret123"


async def test_fetch_401_maps_to_source_auth_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", status=401, body=b"denied", content_type="text/plain")
    with pytest.raises(SourceAuthenticationError):
        await _fetch(mock_api)


async def test_fetch_500_maps_to_source_connection_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", status=500, body=b"oops", content_type="text/plain")
    with pytest.raises(SourceConnectionError) as excinfo:
        await _fetch(mock_api)
    assert excinfo.value.status == 500


async def test_fetch_unreachable_maps_to_source_connection_error() -> None:
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError):
            await fetch_feed_message(session, "http://127.0.0.1:1/rt")


async def test_fetch_garbage_maps_to_feed_parse_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=b"\x00\x01 not protobuf \xff" * 10, content_type=PB)
    with pytest.raises(FeedParseError):
        await _fetch(mock_api)


async def test_fetch_rejects_non_http_scheme() -> None:
    """Task 15R-b item 8: a producer_url with a non-http(s) scheme (data
    from the catalog, not a caller parameter) is rejected explicitly and
    BEFORE any network attempt, naming the scheme in the message. Pre-fix,
    this still raised SourceConnectionError (aiohttp itself rejects
    file:// with a ClientError subclass the existing except clause already
    caught) but with a generic message that never mentioned "scheme" --
    incidental, not deliberate, and not guaranteed for every non-http
    scheme aiohttp might handle differently in some other version/config.
    """
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError, match="scheme"):
            await fetch_feed_message(session, "file:///etc/passwd")


def test_vehicles_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(VEHICLE_POSITIONS)
    vehicles = vehicles_from_message(
        msg,
        routes_by_id={
            "R1": _route("R1", "10", "Main Line"),
            "R2": _route("R2", "20", "Night Owl"),
        },
        stops_by_id={},
        trip_routes={"T3": "R2"},
    )
    assert len(vehicles) == 2
    v1 = next(v for v in vehicles if v.vehicle_id == "V1")
    assert v1.route_id == "R1"
    assert v1.route is not None
    assert v1.route.display_name == "10 Main Line"
    # Typed vocabulary: the StrEnum member IS the old raw string, so both
    # identity and legacy string comparisons hold.
    assert v1.occupancy_status is OccupancyStatus.MANY_SEATS_AVAILABLE
    assert v1.occupancy_status == "MANY_SEATS_AVAILABLE"
    assert v1.timestamp == datetime.fromtimestamp(1_785_500_000, tz=UTC)
    # The baseline fixture sets no status surface at all: no stop referent
    # means no synthesized IN_TRANSIT_TO default -- everything is None.
    assert v1.current_status is None
    assert v1.congestion_level is None
    assert v1.stop_id is None
    assert v1.current_stop_sequence is None
    assert v1.license_plate is None
    v2 = next(v for v in vehicles if v.vehicle_id == "V2")
    assert v2.route_id == "R2"  # resolved via trip_routes fallback
    assert v2.route is not None
    assert v2.route.display_name == "20 Night Owl"


def test_vehicles_from_message_status_surface() -> None:
    """The descriptive status surface: explicit current_status verbatim,
    the spec default IN_TRANSIT_TO synthesized ONLY when a stop referent
    (current_stop_sequence or stop_id) is present, None with neither, and
    congestion/stop referent/license plate passed through raw.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(VEHICLE_POSITIONS_STATUS)
    vehicles = vehicles_from_message(
        msg, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    v7, v8, v9 = (
        next(v for v in vehicles if v.vehicle_id == vid) for vid in ("V7", "V8", "V9")
    )
    assert v7.current_status is VehicleStopStatus.STOPPED_AT
    assert v7.current_status == "STOPPED_AT"
    assert v7.congestion_level is CongestionLevel.SEVERE_CONGESTION
    assert v7.stop_id == "S2"
    assert v7.current_stop_sequence == 2
    assert v7.license_plate == "8ABC123"
    # V8: referent present (current_stop_sequence), status unset -> the
    # protobuf default IN_TRANSIT_TO is real information and surfaces.
    assert v8.current_status is VehicleStopStatus.IN_TRANSIT_TO
    assert v8.current_stop_sequence == 1
    assert v8.stop_id is None
    assert v8.congestion_level is None
    assert v8.license_plate is None
    # V9: no status, no referent -> None (nothing to be in transit to).
    assert v9.current_status is None
    assert v9.stop_id is None
    assert v9.current_stop_sequence is None


def test_vehicles_stop_id_referent_alone_surfaces_default() -> None:
    """A bare stop_id (no current_stop_sequence) is also a stop referent:
    an unset current_status surfaces the IN_TRANSIT_TO default."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "vp-ref"
    entity.vehicle.position.latitude = 1.0
    entity.vehicle.position.longitude = 2.0
    entity.vehicle.stop_id = "S9"
    (vehicle,) = vehicles_from_message(
        msg, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert vehicle.stop_id == "S9"
    assert vehicle.current_status is VehicleStopStatus.IN_TRANSIT_TO


def test_trip_updates_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(TRIP_UPDATES_BASELINE)
    updates = trip_updates_from_message(msg)
    assert updates.canceled_trips == {("T2", None, None)}
    entry = updates.trips[("T1", None, None)]
    assert entry.vehicle_id == "V1"
    assert entry.delay_seconds is None  # no trip-level TripUpdate.delay set
    (stu,) = entry.stop_updates
    assert stu.stop_id == "S1"
    assert stu.stop_sequence is None  # producer sent stop_id addressing only
    assert stu.delay_seconds == 300
    assert stu.arrival == datetime.fromtimestamp(1_785_500_300, tz=UTC)
    assert stu.departure == datetime.fromtimestamp(1_785_500_330, tz=UTC)
    added = updates.added[0]
    assert added.trip_id == "ADDED-9"
    assert added.route_id == "R1"
    assert added.stop_id == "S2"
    assert added.departure == datetime.fromtimestamp(1_785_500_630, tz=UTC)
    assert ("ADDED-9", None, None) not in updates.trips  # ADDED rows never form entries


def _build_conflicting_trip_update(
    *, canceled_first: bool
) -> gtfs_realtime_pb2.FeedMessage:
    """A CANCELED entity plus a stale prediction and a stale ADDED stop time,
    all for the same trip_id, in the given order.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"

    def add_canceled(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "cancel"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )

    def add_prediction(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "stale-update"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.arrival.time = 1_785_500_000

    def add_stale_added(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "stale-added"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ADDED
        )
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S2"
        stu.arrival.time = 1_785_500_100

    if canceled_first:
        add_canceled(msg)
        add_prediction(msg)
        add_stale_added(msg)
    else:
        add_prediction(msg)
        add_stale_added(msg)
        add_canceled(msg)
    return msg


@pytest.mark.parametrize("canceled_first", [True, False])
def test_trip_updates_cancellation_wins_regardless_of_entity_order(
    canceled_first: bool,
) -> None:
    """A CANCELED trip_update plus a stale prediction and a stale ADDED stop
    time for the same trip_id can arrive in either entity order; cancellation
    must always win so the parsed result never claims both statuses for one
    trip.
    """
    msg = _build_conflicting_trip_update(canceled_first=canceled_first)
    updates = trip_updates_from_message(msg)
    assert updates.canceled_trips == {("STALE-TRIP", None, None)}
    assert ("STALE-TRIP", None, None) not in updates.trips
    assert all(added.trip_id != "STALE-TRIP" for added in updates.added)


def test_trip_updates_start_time_keys() -> None:
    """TripDescriptor.start_time becomes the start_secs key component:
    parsed for trip entries AND cancellations (>24:00:00 supported), while
    a garbage start_time degrades to None instead of failing the message.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    cases = [("F1", "06:10:00"), ("F2", "25:00:00"), ("F3", "not-a-time")]
    for i, (trip_id, start_time) in enumerate(cases):
        entity = msg.entity.add()
        entity.id = f"tu-{i}"
        entity.trip_update.trip.trip_id = trip_id
        entity.trip_update.trip.start_time = start_time
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.departure.time = 1_785_500_000
    cancel = msg.entity.add()
    cancel.id = "tu-cancel"
    cancel.trip_update.trip.trip_id = "F1"
    cancel.trip_update.trip.start_time = "06:20:00"
    cancel.trip_update.trip.schedule_relationship = (
        gtfs_realtime_pb2.TripDescriptor.CANCELED
    )
    updates = trip_updates_from_message(msg)
    assert set(updates.trips) == {
        ("F1", None, 22200),
        ("F2", None, 90000),
        ("F3", None, None),
    }
    assert updates.canceled_trips == {("F1", None, 22800)}


def test_trip_updates_start_date_keys() -> None:
    """TripDescriptor.start_date becomes the start_date key component:
    parsed for trip entries AND cancellations, while garbage (bad length,
    non-digits, calendar-invalid dates) degrades to None -- behaving
    exactly like an absent date -- instead of failing the message.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    cases = [
        ("D1", "20260730"),  # valid
        ("D2", "20261332"),  # calendar-invalid month/day
        ("D3", "2026073"),  # wrong length
        ("D4", "2026073a"),  # non-digit
        ("D5", ""),  # absent
    ]
    for i, (trip_id, start_date) in enumerate(cases):
        entity = msg.entity.add()
        entity.id = f"tu-{i}"
        entity.trip_update.trip.trip_id = trip_id
        if start_date:
            entity.trip_update.trip.start_date = start_date
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.departure.time = 1_785_500_000
    cancel = msg.entity.add()
    cancel.id = "tu-cancel"
    cancel.trip_update.trip.trip_id = "D1"
    cancel.trip_update.trip.start_date = "20260731"
    cancel.trip_update.trip.start_time = "06:10:00"
    cancel.trip_update.trip.schedule_relationship = (
        gtfs_realtime_pb2.TripDescriptor.CANCELED
    )
    updates = trip_updates_from_message(msg)
    assert set(updates.trips) == {
        ("D1", date(2026, 7, 30), None),
        ("D2", None, None),
        ("D3", None, None),
        ("D4", None, None),
        ("D5", None, None),
    }
    assert updates.canceled_trips == {("D1", date(2026, 7, 31), 22200)}


def test_alerts_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(ALERTS)
    alerts = alerts_from_message(msg)
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.id == "alert-1"
    assert alert.header == "Detour on Main"
    assert alert.description == "Use Second Ave"
    # Typed vocabulary: StrEnum members ARE the old raw strings.
    assert alert.cause is AlertCause.CONSTRUCTION
    assert alert.cause == "CONSTRUCTION"
    assert alert.effect is AlertEffect.DETOUR
    assert alert.severity is None  # fixture sets no severity_level
    assert alert.route_ids == ["R1"]
    assert alert.stop_ids == ["S1"]
    assert alert.trip_ids == []  # fixture informs a route and a stop only
    assert alert.active_periods == [
        (datetime.fromtimestamp(1_785_400_000, tz=UTC), None)
    ]


def test_alerts_trip_scoped_not_agency_wide() -> None:
    """The semantic fix: an alert whose informed entities carry ONLY trip
    descriptors populates trip_ids -- so it is scoped (to those trips),
    not mistaken for an unscoped agency-wide alert. Unscoped now means
    route_ids, stop_ids, AND trip_ids all empty.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(ALERTS_TRIP_SCOPED)
    (alert,) = alerts_from_message(msg)
    assert alert.header == "T1 and T3 running late"
    assert alert.route_ids == []
    assert alert.stop_ids == []
    assert alert.trip_ids == ["T1", "T3"]  # sorted, deduplicated
    assert alert.effect is AlertEffect.SIGNIFICANT_DELAYS


def test_pb_enum_name_and_vocab_leniency() -> None:
    """Both conversion layers degrade unknown values to None, never raise:
    an int the bindings don't know yields no name, and a name the model
    enum doesn't know (newer bindings than this library) yields no member.
    """
    assert _pb_enum_name(gtfs_realtime_pb2.Alert.Cause, 10) == "CONSTRUCTION"
    assert _pb_enum_name(gtfs_realtime_pb2.Alert.Cause, 999) is None
    assert _vocab_or_none(AlertCause, "CONSTRUCTION") is AlertCause.CONSTRUCTION
    assert _vocab_or_none(AlertCause, "FUTURE_SPEC_VALUE") is None
    assert _vocab_or_none(AlertCause, None) is None
    # The new VehiclePosition vocabularies ride the same two-layer leniency.
    assert (
        _pb_enum_name(gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus, 999) is None
    )
    assert _pb_enum_name(gtfs_realtime_pb2.VehiclePosition.CongestionLevel, 999) is None
    assert _vocab_or_none(VehicleStopStatus, "FUTURE_STATUS") is None
    assert _vocab_or_none(CongestionLevel, "FUTURE_LEVEL") is None


def test_trip_updates_delay_field_and_departure_preference() -> None:
    """TripUpdate.delay is captured only when present, and an STU carrying
    BOTH delays keeps the departure one (the later event at the stop is the
    'last known delay' that propagates).
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "tu-delays"
    entity.trip_update.trip.trip_id = "T1"
    entity.trip_update.delay = -60
    stu = entity.trip_update.stop_time_update.add()
    stu.stop_sequence = 2
    stu.arrival.delay = 100
    stu.departure.delay = 200
    updates = trip_updates_from_message(msg)
    entry = updates.trips[("T1", None, None)]
    assert entry.delay_seconds == -60
    (parsed,) = entry.stop_updates
    assert parsed.stop_sequence == 2
    assert parsed.stop_id is None
    assert parsed.delay_seconds == 200


# --- wire-level out-of-range enum totality ----------------------------------


def _varint(value: int) -> bytes:
    out = b""
    while True:
        low, value = value & 0x7F, value >> 7
        out += bytes([low | 0x80 if value else low])
        if not value:
            return out


def _tagged(field: int, wire_type: int, payload: bytes) -> bytes:
    return _varint((field << 3) | wire_type) + payload


def _length_delimited(field: int, data: bytes) -> bytes:
    return _tagged(field, 2, _varint(len(data)) + data)


def test_out_of_range_enums_on_the_wire_never_raise() -> None:
    """A producer (or newer spec) can put enum ints this bindings version
    doesn't know on the wire. Proto2 parks them in unknown fields, so the
    parser sees the default (SCHEDULED) and the message flows through as a
    normal entry -- never an exception. The bytes are hand-encoded because
    the Python protobuf API refuses to ASSIGN out-of-range values.
    """
    # TripDescriptor: trip_id=1, schedule_relationship=4 (varint 99).
    trip = _length_delimited(1, b"T9") + _tagged(4, 0, _varint(99))
    # StopTimeUpdate: stop_sequence=1, stop_id=4, schedule_relationship=5.
    stu = (
        _tagged(1, 0, _varint(1))
        + _length_delimited(4, b"S1")
        + _tagged(5, 0, _varint(99))
    )
    # TripUpdate: trip=1, stop_time_update=2, delay=5.
    trip_update = (
        _length_delimited(1, trip)
        + _length_delimited(2, stu)
        + _tagged(5, 0, _varint(7))
    )
    entity = _length_delimited(1, b"e1") + _length_delimited(3, trip_update)
    header = _length_delimited(1, b"2.0")  # FeedHeader.gtfs_realtime_version
    raw = _length_delimited(1, header) + _length_delimited(2, entity)
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)
    updates = trip_updates_from_message(msg)
    entry = updates.trips[("T9", None, None)]
    assert entry.delay_seconds == 7
    (parsed,) = entry.stop_updates
    # Both unknown relationships degraded to the SCHEDULED default.
    assert parsed.relationship == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED
    assert parsed.stop_sequence == 1
    assert updates.canceled_trips == set()
    assert updates.added == []


# --- resolve_trip_predictions: the propagation state machine ----------------

_CALLS = [(1, "A"), (2, "B"), (3, "C")]
_LOOP_CALLS = [(1, "A"), (2, "B"), (3, "A"), (4, "C")]
_SKIPPED = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
_NO_DATA = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.NO_DATA


def _stu(
    *,
    stop_id: str | None = None,
    stop_sequence: int | None = None,
    relationship: int = 0,
    arrival: datetime | None = None,
    departure: datetime | None = None,
    delay: int | None = None,
) -> TripStopUpdate:
    return TripStopUpdate(
        stop_id=stop_id,
        stop_sequence=stop_sequence,
        relationship=relationship,
        arrival=arrival,
        departure=departure,
        delay_seconds=delay,
    )


def _entry(
    *stop_updates: TripStopUpdate,
    trip_delay: int | None = None,
    vehicle_id: str | None = None,
) -> TripUpdateEntry:
    return TripUpdateEntry(
        stop_updates=stop_updates, delay_seconds=trip_delay, vehicle_id=vehicle_id
    )


def test_resolve_one_stu_propagates_to_tail() -> None:
    entry = _entry(_stu(stop_id="A", delay=300), vehicle_id="V1")
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.skipped == frozenset()
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 300,
        2: 300,
        3: 300,
    }
    # Propagated stops carry no explicit times (consumers compute
    # scheduled+delay) but DO carry the trip's vehicle.
    assert resolved.predictions[2].arrival is None
    assert resolved.predictions[2].departure is None
    assert resolved.predictions[2].vehicle_id == "V1"


def test_resolve_zero_delay_propagates() -> None:
    """delay=0 (on time) is realtime information, not falsy-absent."""
    entry = _entry(_stu(stop_id="A", delay=0))
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 0,
        2: 0,
        3: 0,
    }


def test_resolve_later_stu_overrides_propagation() -> None:
    entry = _entry(_stu(stop_id="A", delay=300), _stu(stop_id="C", delay=60))
    resolved = resolve_trip_predictions(entry, [*_CALLS, (4, "D")])
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 300,
        2: 300,
        3: 60,
        4: 60,
    }


def test_resolve_no_data_cuts_propagation_and_trip_delay() -> None:
    """NO_DATA yields no prediction at its stop and schedule-only stops
    after it -- the trip-level fallback does NOT resume inside a NO_DATA
    region (it is StopTimeUpdate-derived coverage).
    """
    entry = _entry(
        _stu(stop_id="A", delay=300),
        _stu(stop_id="B", relationship=_NO_DATA),
        trip_delay=120,
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert set(resolved.predictions) == {1}
    assert resolved.predictions[1].delay_seconds == 300


def test_resolve_stu_after_no_data_resumes() -> None:
    entry = _entry(
        _stu(stop_id="A", relationship=_NO_DATA),
        _stu(stop_id="B", delay=45),
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        2: 45,
        3: 45,
    }


def test_resolve_trip_delay_fallback_only_where_uncovered() -> None:
    """The trip-level delay covers stops BEFORE the first STU (propagation
    only flows forward) and trips with no STUs at all; STU-derived state
    wins from the first delay-bearing STU onward.
    """
    entry = _entry(_stu(stop_id="B", delay=300), trip_delay=120)
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 120,
        2: 300,
        3: 300,
    }
    bare = resolve_trip_predictions(_entry(trip_delay=90), _CALLS)
    assert {seq: p.delay_seconds for seq, p in bare.predictions.items()} == {
        1: 90,
        2: 90,
        3: 90,
    }


def test_resolve_explicit_times_win_and_delay_less_stu_keeps_state() -> None:
    when = datetime(2026, 7, 30, 15, 30, tzinfo=UTC)
    entry = _entry(
        _stu(stop_id="A", delay=300),
        _stu(stop_id="B", departure=when),
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    # B keeps its explicit departure AND inherits the last-known delay;
    # C still sees 300 (a delay-less STU never changes the propagation state).
    assert resolved.predictions[2].departure == when
    assert resolved.predictions[2].delay_seconds == 300
    assert resolved.predictions[3].delay_seconds == 300
    assert resolved.predictions[3].departure is None


def test_resolve_skipped_marks_stop_and_never_alters_propagation() -> None:
    entry = _entry(
        _stu(stop_id="A", delay=300),
        # Any delay a SKIPPED STU carries is ignored (spec discourages it).
        _stu(stop_id="B", relationship=_SKIPPED, delay=999),
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.skipped == frozenset({2})
    assert set(resolved.predictions) == {1, 3}
    assert resolved.predictions[3].delay_seconds == 300


def test_resolve_empty_stu_has_no_content_unless_covered() -> None:
    """A SCHEDULED STU with no times and no delay contributes nothing on
    its own, but inside a propagation region it surfaces the inherited
    last-known delay at its stop.
    """
    alone = resolve_trip_predictions(_entry(_stu(stop_id="B")), _CALLS)
    assert alone.predictions == {}
    covered = resolve_trip_predictions(
        _entry(_stu(stop_id="A", delay=300), _stu(stop_id="B")), _CALLS
    )
    assert covered.predictions[2].delay_seconds == 300


def test_resolve_stop_id_matches_first_call_on_loop_trips() -> None:
    entry = _entry(_stu(stop_id="A", delay=100))
    resolved = resolve_trip_predictions(entry, _LOOP_CALLS)
    # Bare stop_id addresses the FIRST visit to A; propagation then covers
    # every later call, including the second visit.
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 100,
        2: 100,
        3: 100,
        4: 100,
    }


def test_resolve_stop_sequence_wins_and_addresses_later_loop_visit() -> None:
    entry = _entry(_stu(stop_id="A", stop_sequence=3, delay=200))
    resolved = resolve_trip_predictions(entry, _LOOP_CALLS)
    # stop_sequence=3 addresses the SECOND visit to A even though stop_id
    # "A" alone would have matched the first: stops before it stay
    # schedule-only, the addressed visit and the tail get the delay.
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        3: 200,
        4: 200,
    }


def test_resolve_unplaceable_stus_are_ignored() -> None:
    entry = _entry(
        _stu(stop_id="GHOST", delay=300),
        _stu(stop_sequence=99, delay=300),
        _stu(delay=300),  # neither field
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.predictions == {}
    assert resolved.skipped == frozenset()


def test_resolve_unrecognized_relationship_behaves_as_scheduled() -> None:
    """UNSCHEDULED and out-of-vocabulary relationship values resolve as
    SCHEDULED (TripStopUpdate's documented contract): the delay predicts
    its own stop and propagates onward exactly as a SCHEDULED STU would.
    """
    for relationship in (gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.UNSCHEDULED, 99):
        entry = _entry(_stu(stop_id="A", relationship=relationship, delay=120))
        resolved = resolve_trip_predictions(entry, _CALLS)
        assert resolved.skipped == frozenset()
        assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
            1: 120,
            2: 120,
            3: 120,
        }


def _rich_vehicle_message() -> gtfs_realtime_pb2.FeedMessage:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "v1"
    vehicle = entity.vehicle
    vehicle.vehicle.id = "V1"
    vehicle.vehicle.wheelchair_accessible = (
        gtfs_realtime_pb2.VehicleDescriptor.WHEELCHAIR_ACCESSIBLE
    )
    vehicle.position.latitude = 34.0
    vehicle.position.longitude = -118.0
    vehicle.position.odometer = 123456.75
    vehicle.occupancy_percentage = 55
    for index, (label, percent) in enumerate((("A", 10), ("B", -1))):
        carriage = vehicle.multi_carriage_details.add()
        carriage.id = f"c{index}"
        carriage.label = label
        carriage.occupancy_percentage = percent
        carriage.carriage_sequence = index + 1
    return message


def test_vehicle_rich_fields_are_surfaced() -> None:
    (vehicle,) = vehicles_from_message(
        _rich_vehicle_message(), routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert vehicle.odometer == 123456.75
    assert vehicle.occupancy_percentage == 55
    # RT numbers WheelchairAccessible 0-3 and the static cell 0-2, so this
    # must map by name; mapping by value would make ACCESSIBLE (2) read as
    # the static NOT_POSSIBLE (2).
    assert vehicle.wheelchair_accessible is WheelchairAccess.POSSIBLE
    assert [c.label for c in vehicle.carriages] == ["A", "B"]
    assert vehicle.carriages[0].occupancy_percentage == 10
    # -1 is the proto's "no data" sentinel, not a negative percentage.
    assert vehicle.carriages[1].occupancy_percentage is None
    assert [c.carriage_sequence for c in vehicle.carriages] == [1, 2]


def test_vehicle_unset_rich_fields_are_none() -> None:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "v2"
    entity.vehicle.position.latitude = 1.0
    entity.vehicle.position.longitude = 2.0
    (vehicle,) = vehicles_from_message(
        message, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert vehicle.odometer is None
    assert vehicle.occupancy_percentage is None
    # NO_VALUE is the proto default: the producer said nothing at all, which
    # is not the same as an explicit UNKNOWN.
    assert vehicle.wheelchair_accessible is None
    assert vehicle.carriages == []


def test_alert_text_detail_and_images_are_surfaced() -> None:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "a1"
    alert = entity.alert
    alert.header_text.translation.add().text = "Detour"
    alert.tts_header_text.translation.add().text = "Dee tour"
    alert.description_text.translation.add().text = "Via 5th"
    alert.tts_description_text.translation.add().text = "Via fifth"
    alert.cause_detail.translation.add().text = "Water main"
    alert.effect_detail.translation.add().text = "Stops skipped"
    alert.image_alternative_text.translation.add().text = "Map of the detour"
    first = alert.image.localized_image.add()
    first.url = "https://example.com/detour.png"
    first.media_type = "image/png"
    first.language = "en"
    # A localized variant with no url is not an image.
    alert.image.localized_image.add().media_type = "image/png"

    [parsed] = alerts_from_message(message)
    assert parsed.tts_header == "Dee tour"
    assert parsed.tts_description == "Via fifth"
    assert parsed.cause_detail == "Water main"
    assert parsed.effect_detail == "Stops skipped"
    assert parsed.image_alternative_text == "Map of the detour"
    assert [(i.url, i.media_type, i.language) for i in parsed.images] == [
        ("https://example.com/detour.png", "image/png", "en")
    ]
