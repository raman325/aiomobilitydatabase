"""GTFS-RT fetching and protobuf parsing into typed models."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from http import HTTPStatus
from typing import Any

import aiohttp
from google.transit import gtfs_realtime_pb2

from ._freshness import (
    Response,
    Validators,
    first_hop_fields,
    header_fields,
    request_fields,
    sent_fields,
)
from ._freshness import (
    hops as _hops,
)
from ._redirects import get_withholding
from ._urls import _require_http_url
from .exceptions import FeedParseError, SourceAuthenticationError, SourceConnectionError
from .models import (
    AlertCause,
    AlertEffect,
    AlertImage,
    AlertSeverity,
    CarriageDetail,
    CongestionLevel,
    OccupancyStatus,
    Route,
    ServiceAlert,
    Stop,
    VehiclePosition,
    VehicleStopStatus,
    WheelchairAccess,
)
from .static_index import parse_gtfs_time

# Only these carry the requested representation (RFC 9110 sections 15.3.1, 15.3.4).
_FEED_STATUSES = frozenset({HTTPStatus.OK, HTTPStatus.NON_AUTHORITATIVE_INFORMATION})
_AUTH_STATUSES = (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN)

_AUTH_TYPE_QUERY_PARAM = 1
_AUTH_TYPE_HEADER = 2

# The RT identity a TripDescriptor addresses: (trip_id, start_date,
# start_secs). start_date pins ONE service day's instance of the trip
# (adjacent daily instances are exactly 24h apart); start_secs pins ONE
# repetition of a frequency-based trip. Either component is None when the
# producer omitted it or sent garbage (parsing is lenient, never raising).
TripUpdateKey = tuple[str, date | None, int | None]

_START_DATE_LENGTH = 8  # YYYYMMDD


def _epoch_to_utc(value: int) -> datetime | None:
    """Convert a GTFS-RT epoch (uint64) to a UTC datetime.

    Producer timestamps are unchecked uint64 values that can vastly exceed
    what ``datetime.fromtimestamp`` can represent; a garbage value becomes
    "no timestamp" rather than crashing.
    """
    if not value:
        return None
    try:
        return datetime.fromtimestamp(value, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _pb_enum_name(enum_type: Any, value: int) -> str | None:
    """Protobuf enum int -> name; None for values this bindings version lacks.

    ``enum_type`` is a protobuf EnumTypeWrapper class (``Any`` because the
    protobuf runtime ships no inline types and only the ``Name`` surface is
    needed). Proto2 shields parsers from out-of-range wire values (they
    land in unknown fields, leaving the field unset), so in practice
    ``value`` is always known -- but a bindings/library version skew could
    still surface one, and RT payloads must degrade rather than raise.
    """
    try:
        return str(enum_type.Name(value))
    except ValueError:
        return None


def _vocab_or_none[StrEnumT: StrEnum](
    enum_cls: type[StrEnumT], name: str | None
) -> StrEnumT | None:
    """Model-boundary StrEnum conversion: unknown/future names become None.

    Mirrors the static side's ``_enum_or_none``: protobuf vocabularies are
    closed per bindings version, but the spec adds members over time, so a
    name the model enum doesn't know (newer bindings than this library)
    must degrade to None, never raise.
    """
    if name is None:
        return None
    try:
        return enum_cls(name)
    except ValueError:
        return None


def _feed_request(
    auth_type: int | None,
    api_key_name: str | None,
    api_key: str | None,
    headers: Mapping[str, str] | None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Return the query parameters and headers of a GTFS-RT request.

    See :func:`fetch_feed_message` for how auth and custom headers combine.
    """
    params: dict[str, str] = {}
    req_headers: dict[str, str] = dict(headers) if headers else {}
    if api_key is not None and api_key_name:
        if auth_type == _AUTH_TYPE_QUERY_PARAM:
            params[api_key_name] = api_key
        elif auth_type == _AUTH_TYPE_HEADER:
            req_headers[api_key_name] = api_key
    return params, req_headers


def feed_request_fields(
    session: aiohttp.ClientSession,
    url: str,
    *,
    auth_type: int | None = None,
    api_key_name: str | None = None,
    api_key: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> dict[str, tuple[str, ...]]:
    """Return the controlled header fields :func:`fetch_feed_message` sends now.

    Unconditional: what a stored response's Vary is matched against (see
    :func:`~._freshness.request_fields`).
    """
    _params, req_headers = _feed_request(auth_type, api_key_name, api_key, headers)
    return request_fields(session, url, req_headers)


async def fetch_feed_message(
    session: aiohttp.ClientSession,
    url: str,
    *,
    auth_type: int | None = None,
    api_key_name: str | None = None,
    api_key: str | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float = 30.0,
    validators: Validators | None = None,
) -> Response[gtfs_realtime_pb2.FeedMessage]:
    """GET a GTFS-RT producer URL and parse the protobuf FeedMessage.

    ``auth_type`` follows the catalog's ``source_info.authentication_type``:
    1 = query parameter named ``api_key_name``; 2 = header named
    ``api_key_name``. ``headers`` (a direct-URL handle's custom headers)
    are merged in BEFORE auth handling, so under header auth an explicit
    ``api_key`` wins over a same-named custom header (case-insensitively:
    the producer sees one header, under ``api_key_name``'s own casing,
    carrying the explicit key) rather than being silently shadowed. Under
    query auth the key never touches the headers, so a same-named custom
    header is a different namespace and passes through untouched. Parsing
    runs in a thread (CPU-bound for large feeds).

    With ``validators`` the request is conditional, and a ``304`` comes
    back as a :class:`Response` with no value. The response's header
    fields and request/receipt instants ride along: the freshness cache
    derives freshness and validators from them (RFC 9111).
    """
    _require_http_url(url, "GTFS-RT producer URL")
    params, req_headers = _feed_request(auth_type, api_key_name, api_key, headers)
    # Before the validators: they are the cache's, not the request's own.
    controlled = request_fields(session, url, req_headers)
    if validators is not None:
        req_headers.update(validators.request_headers())
    requested_at = datetime.now(UTC)
    secret = api_key_name if auth_type == _AUTH_TYPE_HEADER and api_key else None
    try:
        async with get_withholding(
            session,
            url,
            params=params,
            headers=req_headers,
            total=timeout_seconds,
            secret=secret,
        ) as (resp, history):
            received_at = datetime.now(UTC)
            if resp.status in _AUTH_STATUSES:
                raise SourceAuthenticationError(
                    f"Producer rejected credentials ({resp.status}): {url}"
                )
            fields = header_fields(resp.headers)
            # 304 is the producer confirming the bytes are unchanged, so
            # reusing the previous parse is not a staleness risk.
            if resp.status == HTTPStatus.NOT_MODIFIED:
                if validators is None:
                    # Unsolicited: there is no previous parse to reuse. A
                    # misbehaving proxy must raise the library's own error.
                    raise SourceConnectionError(
                        f"Producer answered 304 to an unconditional request: {url}",
                        status=int(HTTPStatus.NOT_MODIFIED),
                    )
                return Response(
                    value=None,
                    fields=fields,
                    requested_at=requested_at,
                    received_at=received_at,
                    request_fields=sent_fields(controlled, resp, history),
                    hops=_hops(resp, history),
                    url=str(resp.url),
                    first_hop_request_fields=first_hop_fields(
                        controlled, resp, history
                    ),
                )
            if resp.status >= HTTPStatus.BAD_REQUEST:
                raise SourceConnectionError(
                    f"Producer error {resp.status}: {url}", status=resp.status
                )
            if resp.status not in _FEED_STATUSES:
                # A 204, a 300, a 3xx left unfollowed: no feed to parse (an
                # empty body parses as an empty FeedMessage).
                raise SourceConnectionError(
                    f"Producer answered {resp.status} without a feed: {url}",
                    status=resp.status,
                )
            raw = await resp.read()
            sent = sent_fields(controlled, resp, history)
            hops = _hops(resp, history)
            answered = str(resp.url)
            first = first_hop_fields(controlled, resp, history)
    except (TimeoutError, aiohttp.ClientError) as err:
        raise SourceConnectionError(f"Error fetching {url}: {err}") from err
    message = gtfs_realtime_pb2.FeedMessage()
    try:
        await asyncio.to_thread(message.ParseFromString, raw)
    except Exception as err:  # DecodeError subclasses Exception, not a shared base
        raise FeedParseError(f"Undecodable GTFS-RT protobuf from {url}") from err
    return Response(
        value=message,
        fields=fields,
        requested_at=requested_at,
        received_at=received_at,
        request_fields=sent,
        hops=hops,
        url=answered,
        first_hop_request_fields=first,
    )


# GTFS-RT's WheelchairAccessible is NOT the static wheelchair_accessible
# vocabulary: it numbers NO_VALUE/UNKNOWN/ACCESSIBLE/INACCESSIBLE 0-3, while
# the static cell numbers no-info/possible/not-possible 0-2. The concept is
# the same, so it maps onto the same model enum -- but by name, never by
# value, since the ints mean different things.
_RT_WHEELCHAIR = {
    "WHEELCHAIR_ACCESSIBLE": WheelchairAccess.POSSIBLE,
    "WHEELCHAIR_INACCESSIBLE": WheelchairAccess.NOT_POSSIBLE,
    "UNKNOWN": WheelchairAccess.UNKNOWN,
}


def _rt_wheelchair(
    descriptor: gtfs_realtime_pb2.VehicleDescriptor,
) -> WheelchairAccess | None:
    """Vehicle accessibility from a VehicleDescriptor, or None when unstated.

    NO_VALUE is the proto's "field not populated" default and means the
    producer said nothing, which is distinct from an explicit UNKNOWN.
    """
    if not descriptor.HasField("wheelchair_accessible"):
        return None
    name = _pb_enum_name(
        gtfs_realtime_pb2.VehicleDescriptor.WheelchairAccessible,
        descriptor.wheelchair_accessible,
    )
    return _RT_WHEELCHAIR.get(name) if name else None


@dataclass(frozen=True)
class ReplacementCall:
    """One stop a modification routes a trip through instead.

    ``travel_time_to_stop`` is seconds from the REFERENCE stop's arrival --
    the call before the replaced span, or the trip's first call when the
    span starts there. The spec requires it to increase monotonically and
    allows it to be negative only in that first-call case.
    """

    stop_id: str
    travel_time_to_stop: int | None


@dataclass(frozen=True)
class Modification:
    """One replaced span of a trip's calls."""

    start_stop_sequence: int | None
    start_stop_id: str | None
    end_stop_sequence: int | None
    end_stop_id: str | None
    propagated_delay_seconds: int
    replacements: list[ReplacementCall]


@dataclass(frozen=True)
class TripModifications:
    """Modifications applying to a set of trip instances."""

    trip_ids: list[str]
    service_dates: frozenset[date]  # never empty: the spec requires them
    start_secs: frozenset[int]
    modifications: list[Modification]


def _stop_from_rt(stop: Any) -> Stop:
    """Return a GTFS-RT Stop entity as the model a static stop uses.

    A replacement stop may be one the static feed has never heard of, so
    the RT entity is the only definition of it that exists.
    """
    return Stop(
        id=stop.stop_id,
        name=stop.stop_name.translation[0].text if stop.stop_name.translation else None,
        latitude=stop.stop_lat if stop.HasField("stop_lat") else None,
        longitude=stop.stop_lon if stop.HasField("stop_lon") else None,
        parent_station=stop.parent_station or None,
        location_type=None,
        stop_code=stop.stop_code or None,
        platform_code=stop.platform_code or None,
        wheelchair_boarding=None,
        description=stop.stop_desc.translation[0].text
        if stop.stop_desc.translation
        else None,
        url=stop.stop_url.translation[0].text if stop.stop_url.translation else None,
        zone_id=stop.zone_id or None,
        timezone=stop.stop_timezone or None,
    )


def stops_from_message(message: gtfs_realtime_pb2.FeedMessage) -> dict[str, Stop]:
    """RT-added stops, keyed by id; rows without an id are not stops."""
    return {
        entity.stop.stop_id: _stop_from_rt(entity.stop)
        for entity in message.entity
        if entity.HasField("stop") and entity.stop.stop_id
    }


def trip_modifications_from_message(
    message: gtfs_realtime_pb2.FeedMessage,
) -> list[TripModifications]:
    """Trip modifications, one record per selected_trips group.

    service_dates parse leniently: a malformed date degrades to "not this
    day" rather than failing the message, like every other RT descriptor.
    The spec requires service_dates, so an entity naming no usable date
    modifies no run at all -- it is never read as "every day".
    """
    found: list[TripModifications] = []
    for entity in message.entity:
        if not entity.HasField("trip_modifications"):
            continue
        modifications = entity.trip_modifications
        parsed = [
            Modification(
                start_stop_sequence=(
                    modification.start_stop_selector.stop_sequence
                    if modification.start_stop_selector.HasField("stop_sequence")
                    else None
                ),
                start_stop_id=modification.start_stop_selector.stop_id or None,
                end_stop_sequence=(
                    modification.end_stop_selector.stop_sequence
                    if modification.end_stop_selector.HasField("stop_sequence")
                    else None
                ),
                end_stop_id=modification.end_stop_selector.stop_id or None,
                propagated_delay_seconds=modification.propagated_modification_delay,
                replacements=[
                    ReplacementCall(
                        stop_id=replacement.stop_id,
                        travel_time_to_stop=(
                            replacement.travel_time_to_stop
                            if replacement.HasField("travel_time_to_stop")
                            else None
                        ),
                    )
                    for replacement in modification.replacement_stops
                    if replacement.stop_id
                ],
            )
            for modification in modifications.modifications
        ]
        if not parsed:
            continue
        dates = frozenset(
            parsed_date
            for raw in modifications.service_dates
            if (parsed_date := _lenient_service_date(raw)) is not None
        )
        if not dates:
            continue
        starts = frozenset(
            secs
            for raw in modifications.start_times
            if (secs := _lenient_start_secs(raw)) is not None
        )
        for selected in modifications.selected_trips:
            if trip_ids := list(selected.trip_ids):
                found.append(
                    TripModifications(
                        trip_ids=trip_ids,
                        service_dates=dates,
                        start_secs=starts,
                        modifications=parsed,
                    )
                )
    return found


def _lenient_service_date(raw: str) -> date | None:
    """YYYYMMDD to a date; anything else is "not a day this applies to"."""
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return None


def _lenient_start_secs(raw: str) -> int | None:
    """Return a start_time in seconds; hours may exceed 24, garbage degrades."""
    try:
        return parse_gtfs_time(raw)
    except FeedParseError:
        return None


def _occupancy_percentage(holder: Any) -> int | None:
    """Read an occupancy_percentage, treating the proto's -1 as unset.

    The field defaults to -1 ("no data"), so surfacing it verbatim would
    hand consumers a negative percentage.
    """
    if not holder.HasField("occupancy_percentage"):
        return None
    value = holder.occupancy_percentage
    return None if value < 0 else int(value)


def _carriages(
    vehicle: gtfs_realtime_pb2.VehiclePosition,
) -> tuple[CarriageDetail, ...]:
    """Per-carriage occupancy for a multi-carriage vehicle."""
    return tuple(
        CarriageDetail(
            id=carriage.id or None,
            label=carriage.label or None,
            occupancy_status=_vocab_or_none(
                OccupancyStatus,
                _pb_enum_name(
                    gtfs_realtime_pb2.VehiclePosition.OccupancyStatus,
                    carriage.occupancy_status,
                ),
            )
            if carriage.HasField("occupancy_status")
            else None,
            occupancy_percentage=_occupancy_percentage(carriage),
            carriage_sequence=carriage.carriage_sequence
            if carriage.HasField("carriage_sequence")
            else None,
        )
        for carriage in vehicle.multi_carriage_details
    )


def _alert_images(
    image: gtfs_realtime_pb2.TranslatedImage,
) -> tuple[AlertImage, ...]:
    """Every localized variant of an alert image that names a url."""
    return tuple(
        AlertImage(
            url=localized.url,
            media_type=localized.media_type or None,
            language=localized.language or None,
        )
        for localized in image.localized_image
        if localized.url
    )


def vehicles_from_message(
    message: gtfs_realtime_pb2.FeedMessage,
    *,
    routes_by_id: Mapping[str, Route],
    stops_by_id: Mapping[str, Stop],
    trip_routes: Mapping[str, str],
) -> list[VehiclePosition]:
    """Extract vehicle positions, resolving route via trip when unset."""
    vehicles: list[VehiclePosition] = []
    for entity in message.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle = entity.vehicle
        if not vehicle.HasField("position"):
            continue
        route_id = (
            vehicle.trip.route_id or trip_routes.get(vehicle.trip.trip_id) or None
        )
        occupancy = (
            _vocab_or_none(
                OccupancyStatus,
                _pb_enum_name(
                    gtfs_realtime_pb2.VehiclePosition.OccupancyStatus,
                    vehicle.occupancy_status,
                ),
            )
            if vehicle.HasField("occupancy_status")
            else None
        )
        # current_status is only meaningful with respect to a current stop
        # (the proto documents it as ignored without one), but its proto2
        # default (IN_TRANSIT_TO) is real information once a stop referent
        # exists. So: an explicitly SET status always surfaces verbatim;
        # the implicit default surfaces only when current_stop_sequence or
        # stop_id identifies the stop it refers to; with neither, an unset
        # status is None -- there is no stop to be "in transit to".
        has_stop_referent = vehicle.HasField(
            "current_stop_sequence"
        ) or vehicle.HasField("stop_id")
        current_status = (
            _vocab_or_none(
                VehicleStopStatus,
                _pb_enum_name(
                    gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus,
                    vehicle.current_status,
                ),
            )
            if vehicle.HasField("current_status") or has_stop_referent
            else None
        )
        congestion = (
            _vocab_or_none(
                CongestionLevel,
                _pb_enum_name(
                    gtfs_realtime_pb2.VehiclePosition.CongestionLevel,
                    vehicle.congestion_level,
                ),
            )
            if vehicle.HasField("congestion_level")
            else None
        )
        vehicles.append(
            VehiclePosition(
                vehicle_id=vehicle.vehicle.id or None,
                label=vehicle.vehicle.label or None,
                latitude=vehicle.position.latitude,
                longitude=vehicle.position.longitude,
                bearing=vehicle.position.bearing
                if vehicle.position.HasField("bearing")
                else None,
                speed=vehicle.position.speed
                if vehicle.position.HasField("speed")
                else None,
                route_id=route_id,
                route=routes_by_id.get(route_id) if route_id else None,
                trip_id=vehicle.trip.trip_id or None,
                trip_start_date=_trip_start_date(vehicle.trip),
                trip_start_secs=_trip_start_secs(vehicle.trip),
                occupancy_status=occupancy,
                occupancy_percentage=_occupancy_percentage(vehicle),
                carriages=_carriages(vehicle),
                wheelchair_accessible=_rt_wheelchair(vehicle.vehicle),
                odometer=vehicle.position.odometer
                if vehicle.position.HasField("odometer")
                else None,
                timestamp=_epoch_to_utc(vehicle.timestamp),
                current_status=current_status,
                congestion_level=congestion,
                stop_id=vehicle.stop_id or None,
                stop=stops_by_id.get(vehicle.stop_id) if vehicle.stop_id else None,
                current_stop_sequence=(
                    vehicle.current_stop_sequence
                    if vehicle.HasField("current_stop_sequence")
                    else None
                ),
                license_plate=vehicle.vehicle.license_plate or None,
            )
        )
    return vehicles


@dataclass(frozen=True)
class StopPrediction:
    """Resolved RT outcome for one (trip, stop) call.

    ``arrival``/``departure`` are EXPLICIT epoch predictions from this
    stop's own StopTimeUpdate (absent for propagated-only stops);
    ``delay_seconds`` is the effective delay at the stop -- its own STU's
    delay, else the propagated last-known delay, else the trip-level
    fallback. Consumers compute schedule-relative predicted times as
    ``scheduled + delay`` wherever an explicit time is absent.
    """

    arrival: datetime | None
    departure: datetime | None
    delay_seconds: int | None
    vehicle_id: str | None


@dataclass(frozen=True)
class AddedStopTime:
    """A stop event on an RT-added trip absent from the schedule."""

    trip_id: str
    route_id: str | None
    stop_id: str
    arrival: datetime | None
    departure: datetime | None
    vehicle_id: str | None
    start_date: date | None = None


@dataclass(frozen=True)
class TripStopUpdate:
    """One StopTimeUpdate, positioned by stop_sequence and/or stop_id.

    ``stop_sequence`` and ``stop_id`` carry exactly what the producer sent
    (either, or both); resolution against the static stop order prefers
    ``stop_sequence`` because ``stop_id`` is ambiguous on loop trips.
    ``relationship`` is the raw per-stop schedule_relationship int
    (SCHEDULED/SKIPPED/NO_DATA; anything else -- e.g. UNSCHEDULED or a
    future value -- is treated as SCHEDULED, today's behavior for every
    unrecognized relationship). ``delay_seconds`` prefers the departure
    delay over the arrival delay: the departure is the later event at a
    stop, so it is the "last known delay" the spec says propagates onward.
    """

    stop_id: str | None
    stop_sequence: int | None
    relationship: int
    arrival: datetime | None
    departure: datetime | None
    delay_seconds: int | None


@dataclass(frozen=True)
class TripUpdateEntry:
    """All StopTimeUpdate-level data for one :data:`TripUpdateKey` identity.

    ``stop_updates`` keeps feed order (resolution re-orders by static stop
    position anyway); ``delay_seconds`` is the trip-level
    ``TripUpdate.delay`` fallback, applied only where no
    StopTimeUpdate-derived information covers a stop (spec: "should only
    be used if a prediction ... is not provided").
    """

    stop_updates: tuple[TripStopUpdate, ...]
    delay_seconds: int | None
    vehicle_id: str | None


@dataclass(frozen=True)
class TripPredictions:
    """Per-stop resolved outcomes for one trip against its static stop order.

    ``predictions`` is keyed by static ``stop_sequence``; ``skipped`` holds
    the stop_sequences the vehicle will not serve (their arrivals and any
    origin/destination journey rows touching them must be suppressed).
    """

    predictions: Mapping[int, StopPrediction]
    skipped: frozenset[int]


@dataclass
class TripUpdates:
    """Parsed index of a TripUpdates feed.

    Trip entries are keyed by :data:`TripUpdateKey` --
    ``(trip_id, start_date, start_secs)``, also the cancellation key --
    where ``start_date`` is the parsed ``TripDescriptor.start_date``
    (None when absent or unparseable) and ``start_secs`` is the parsed
    ``TripDescriptor.start_time`` (likewise None). ``start_date`` is how
    GTFS-RT addresses ONE service day's instance of a trip (an update
    posted today for tomorrow's instance must not touch today's);
    ``start_time`` is how it addresses ONE repetition of a
    frequency-based trip. The consumer-side merge matches both against
    the static index's per-service-day rows and materialized
    repetitions; propagation therefore happens within one matched
    instance only.

    A producer sending several TripUpdate entities for one identity is
    out of spec ("at most one trip_update per actual trip"); the last
    entity wins wholesale rather than attempting a field-level merge.
    """

    trips: dict[TripUpdateKey, TripUpdateEntry] = field(default_factory=dict)
    canceled_trips: set[TripUpdateKey] = field(default_factory=set)
    added: list[AddedStopTime] = field(default_factory=list)
    # When the producer built this snapshot (header.timestamp), the instant
    # "the current service day" is judged at. None when absent or zero.
    feed_time: datetime | None = None


def resolve_trip_predictions(
    entry: TripUpdateEntry, stop_calls: Sequence[tuple[int, str]]
) -> TripPredictions:
    """Resolve one trip's StopTimeUpdates against its static stop order.

    ``stop_calls`` is the trip's ordered ``(stop_sequence, stop_id)`` calls
    from the static schedule (one materialized repetition's calls for
    frequency trips). Per the GTFS-RT spec, walking the calls in order:

    - A SKIPPED StopTimeUpdate marks its stop skipped and nothing else --
      it never alters propagation, and any times/delays it carries are
      ignored (the spec discourages them).
    - A NO_DATA StopTimeUpdate yields no prediction at its stop AND cuts
      propagation: subsequent stops are schedule-only (the trip-level
      delay fallback does NOT resume there -- "no data" IS
      StopTimeUpdate-derived coverage) until a later STU with a delay.
    - A SCHEDULED StopTimeUpdate's delay becomes the propagated last-known
      delay for subsequent stops until newer information; an STU carrying
      only explicit times (no delay) predicts its own stop but leaves the
      propagation state untouched.
    - A stop with no STU at-or-before it falls back to the trip-level
      delay (when present), else stays schedule-only.

    STUs that cannot be placed on the static order (unknown stop_sequence,
    unknown stop_id, or neither field) are ignored. ``stop_sequence`` wins
    when both fields are present; a bare ``stop_id`` matches the FIRST
    call with that id (loop trips need stop_sequence to address later
    visits). Several STUs placing on one call: the last one wins.
    """
    sequence_pos = {seq: pos for pos, (seq, _) in enumerate(stop_calls)}
    stop_id_pos: dict[str, int] = {}
    for pos, (_, stop_id) in enumerate(stop_calls):
        stop_id_pos.setdefault(stop_id, pos)
    placed: dict[int, TripStopUpdate] = {}
    for stu in entry.stop_updates:
        placed_pos = (
            sequence_pos.get(stu.stop_sequence)
            if stu.stop_sequence is not None
            else stop_id_pos.get(stu.stop_id)
            if stu.stop_id is not None
            else None
        )
        if placed_pos is not None:
            placed[placed_pos] = stu
    skipped_rel = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    no_data_rel = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.NO_DATA
    predictions: dict[int, StopPrediction] = {}
    skipped: set[int] = set()
    covered = False  # has any delay-bearing or NO_DATA STU been passed?
    current: int | None = None  # propagated delay; None while covered = no data
    for pos, (seq, _) in enumerate(stop_calls):
        own = placed.get(pos)
        if own is not None and own.relationship == skipped_rel:
            skipped.add(seq)
            continue
        if own is not None and own.relationship == no_data_rel:
            covered, current = True, None
            continue
        if own is not None:
            if own.delay_seconds is not None:
                covered, current = True, own.delay_seconds
            effective = own.delay_seconds
            if effective is None:
                effective = current if covered else entry.delay_seconds
            if own.arrival is None and own.departure is None and effective is None:
                # An STU with no times and no applicable delay carries no
                # realtime content for this stop.
                continue
            predictions[seq] = StopPrediction(
                arrival=own.arrival,
                departure=own.departure,
                delay_seconds=effective,
                vehicle_id=entry.vehicle_id,
            )
            continue
        delay = current if covered else entry.delay_seconds
        if delay is not None:
            predictions[seq] = StopPrediction(
                arrival=None,
                departure=None,
                delay_seconds=delay,
                vehicle_id=entry.vehicle_id,
            )
    return TripPredictions(predictions=predictions, skipped=frozenset(skipped))


def _trip_start_secs(trip: gtfs_realtime_pb2.TripDescriptor) -> int | None:
    """Parse TripDescriptor.start_time (hours may exceed 24) to seconds.

    Absent or unparseable start_time becomes None: RT payloads are
    best-effort, so one producer's garbage start_time must degrade to "no
    repetition addressed" rather than failing the whole message (the same
    leniency ``_epoch_to_utc`` applies to garbage timestamps).
    """
    try:
        return parse_gtfs_time(trip.start_time)
    except FeedParseError:
        return None


def _trip_start_date(trip: gtfs_realtime_pb2.TripDescriptor) -> date | None:
    """Parse TripDescriptor.start_date (``YYYYMMDD``) to a date.

    Absent or garbage start_date becomes None -- the same leniency
    ``_trip_start_secs`` applies to start_time: one producer's malformed
    date must degrade to "no service day addressed" (behaving exactly
    like an absent date downstream) rather than failing the whole
    message. ASCII digits only; calendar-invalid dates (month 13, day 32,
    year 0) are garbage too.
    """
    raw = trip.start_date
    if len(raw) != _START_DATE_LENGTH or not (raw.isascii() and raw.isdigit()):
        return None
    try:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    except ValueError:
        return None


def trip_updates_from_message(message: gtfs_realtime_pb2.FeedMessage) -> TripUpdates:
    """Index TripUpdate entities by their (trip_id, start_date, start_secs) key."""
    updates = TripUpdates()
    if message.header.HasField("timestamp") and message.header.timestamp:
        updates.feed_time = _epoch_to_utc(message.header.timestamp)
    canceled = gtfs_realtime_pb2.TripDescriptor.CANCELED
    added = gtfs_realtime_pb2.TripDescriptor.ADDED
    for entity in message.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id
        start_date = _trip_start_date(trip_update.trip)
        start_secs = _trip_start_secs(trip_update.trip)
        vehicle_id = trip_update.vehicle.id or None
        if trip_update.trip.schedule_relationship == canceled:
            updates.canceled_trips.add((trip_id, start_date, start_secs))
            continue
        is_added = trip_update.trip.schedule_relationship == added
        stop_updates: list[TripStopUpdate] = []
        for stu in trip_update.stop_time_update:
            arrival = (
                _epoch_to_utc(stu.arrival.time) if stu.HasField("arrival") else None
            )
            departure = (
                _epoch_to_utc(stu.departure.time) if stu.HasField("departure") else None
            )
            # Departure delay preferred over arrival delay: the departure
            # is the later event at the stop, so it is the "last known
            # delay" the spec says propagates to subsequent stops.
            delay = (
                stu.departure.delay
                if stu.HasField("departure") and stu.departure.HasField("delay")
                else (
                    stu.arrival.delay
                    if stu.HasField("arrival") and stu.arrival.HasField("delay")
                    else None
                )
            )
            if is_added:
                updates.added.append(
                    AddedStopTime(
                        trip_id=trip_id,
                        route_id=trip_update.trip.route_id or None,
                        stop_id=stu.stop_id,
                        arrival=arrival,
                        departure=departure,
                        vehicle_id=vehicle_id,
                        start_date=start_date,
                    )
                )
            else:
                stop_updates.append(
                    TripStopUpdate(
                        stop_id=stu.stop_id or None,
                        stop_sequence=(
                            stu.stop_sequence if stu.HasField("stop_sequence") else None
                        ),
                        relationship=stu.schedule_relationship,
                        arrival=arrival,
                        departure=departure,
                        delay_seconds=delay,
                    )
                )
        if not is_added:
            # An entry is recorded even with zero StopTimeUpdates: a bare
            # TripUpdate.delay with no STUs is a valid trip-wide fallback.
            updates.trips[(trip_id, start_date, start_secs)] = TripUpdateEntry(
                stop_updates=tuple(stop_updates),
                delay_seconds=(
                    trip_update.delay if trip_update.HasField("delay") else None
                ),
                vehicle_id=vehicle_id,
            )
    # Cancellation wins regardless of entity order: a producer may send a
    # CANCELED trip_update alongside stale predictions/added-stop-times for
    # the same trip in either order within one message. The parsed result
    # must be self-consistent rather than relying on consumers checking
    # canceled_trips first. Trip entries are matched on the full
    # (trip_id, start_date, start_secs) identity; ADDED stop times are
    # matched on bare trip_id -- an added trip is identified by the id the
    # producer minted for it, and dropping its rows on ANY cancellation of
    # that id is the conservative "removed wins" choice (never show a trip
    # that might not run).
    if updates.canceled_trips:
        updates.trips = {
            key: entry
            for key, entry in updates.trips.items()
            if key not in updates.canceled_trips
        }
        canceled_ids = {trip_id for trip_id, _, _ in updates.canceled_trips}
        updates.added = [
            stop_time
            for stop_time in updates.added
            if stop_time.trip_id not in canceled_ids
        ]
    return updates


def _first_translation(translated: object) -> str | None:
    translations = getattr(translated, "translation", None)
    if not translations:
        return None
    for candidate in translations:
        if candidate.language == "en":
            return str(candidate.text)
    return str(translations[0].text)


def alerts_from_message(message: gtfs_realtime_pb2.FeedMessage) -> list[ServiceAlert]:
    """Extract service alerts with best-effort English text.

    Scoping: EVERY selector an informed_entity can carry is collected --
    agency_id, route_id, route_type, direction_id, stop_id and the trip
    descriptor's trip_id -- so an alert is unscoped (feed-wide) only when
    all of them are empty. Collecting a subset would make an alert scoped
    by an uncollected selector (an agency, or a whole mode) read as
    applying everywhere (see :class:`~.models.ServiceAlert`).
    """
    alerts: list[ServiceAlert] = []
    for entity in message.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert
        agency_ids = sorted(
            {ie.agency_id for ie in alert.informed_entity if ie.agency_id}
        )
        route_ids = sorted({ie.route_id for ie in alert.informed_entity if ie.route_id})
        # route_type/direction_id have 0 as a meaningful value (tram,
        # and the feed's first direction), so presence is HasField, not
        # truthiness.
        route_types = sorted(
            {ie.route_type for ie in alert.informed_entity if ie.HasField("route_type")}
        )
        direction_ids = sorted(
            {
                ie.direction_id
                for ie in alert.informed_entity
                if ie.HasField("direction_id")
            }
        )
        stop_ids = sorted({ie.stop_id for ie in alert.informed_entity if ie.stop_id})
        trip_ids = sorted(
            {ie.trip.trip_id for ie in alert.informed_entity if ie.trip.trip_id}
        )
        alerts.append(
            ServiceAlert(
                id=entity.id,
                header=_first_translation(alert.header_text),
                description=_first_translation(alert.description_text),
                tts_header=_first_translation(alert.tts_header_text),
                tts_description=_first_translation(alert.tts_description_text),
                cause=_vocab_or_none(
                    AlertCause,
                    _pb_enum_name(gtfs_realtime_pb2.Alert.Cause, alert.cause),
                )
                if alert.HasField("cause")
                else None,
                cause_detail=_first_translation(alert.cause_detail),
                effect=_vocab_or_none(
                    AlertEffect,
                    _pb_enum_name(gtfs_realtime_pb2.Alert.Effect, alert.effect),
                )
                if alert.HasField("effect")
                else None,
                effect_detail=_first_translation(alert.effect_detail),
                severity=(
                    _vocab_or_none(
                        AlertSeverity,
                        _pb_enum_name(
                            gtfs_realtime_pb2.Alert.SeverityLevel,
                            alert.severity_level,
                        ),
                    )
                    if alert.HasField("severity_level")
                    else None
                ),
                images=_alert_images(alert.image),
                image_alternative_text=_first_translation(alert.image_alternative_text),
                agency_ids=tuple(agency_ids),
                route_ids=tuple(route_ids),
                route_types=tuple(route_types),
                direction_ids=tuple(direction_ids),
                stop_ids=tuple(stop_ids),
                trip_ids=tuple(trip_ids),
                active_periods=tuple(
                    (
                        _epoch_to_utc(period.start)
                        if period.HasField("start")
                        else None,
                        _epoch_to_utc(period.end) if period.HasField("end") else None,
                    )
                    for period in alert.active_period
                ),
                url=_first_translation(alert.url),
            )
        )
    return alerts
