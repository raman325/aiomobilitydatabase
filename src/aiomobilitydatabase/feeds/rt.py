"""GTFS-RT fetching and protobuf parsing into typed models."""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from http import HTTPStatus
from typing import Any

import aiohttp
from google.transit import gtfs_realtime_pb2

from ._freshness import Response, Validators, header_fields, request_fields
from ._freshness import hops as _hops
from ._language import is_preferred_language
from ._redirects import get_withholding
from ._urls import _require_http_url
from .exceptions import FeedParseError, SourceAuthenticationError, SourceConnectionError
from .models import (
    AlertCause,
    AlertEffect,
    AlertImage,
    AlertSeverity,
    BikesAllowed,
    CarriageDetail,
    CongestionLevel,
    EntitySelector,
    OccupancyStatus,
    PickupDropOffType,
    Route,
    ServiceAlert,
    Stop,
    StopLocationType,
    TripSelector,
    VehiclePosition,
    VehicleStopStatus,
    WheelchairAccess,
)
from .static_index import TripCall, parse_gtfs_time

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
) -> dict[str, tuple[str, ...]] | None:
    """Return the header fields :func:`fetch_feed_message` would send now.

    Unconditional: what a stored response's Vary is matched against (see
    :func:`~._freshness.request_fields`; None when they can't be known).
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
            conditional=None if validators is None else validators.request_headers(),
            conditional_url=None if validators is None else validators.url,
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
                    hops=_hops(resp, history),
                    url=str(resp.url),
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
            hops = _hops(resp, history)
            answered = str(resp.url)
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
        hops=hops,
        url=answered,
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
    # The start_times that parse; None where the entity lists none (every
    # run of its trips), empty where none of those it lists parses (none).
    start_secs: frozenset[int] | None
    modifications: list[Modification]


_RT_BOARDING = {
    "UNKNOWN": WheelchairAccess.UNKNOWN,
    "AVAILABLE": WheelchairAccess.POSSIBLE,
    "NOT_AVAILABLE": WheelchairAccess.NOT_POSSIBLE,
}


def _stop_from_rt(stop: Any) -> Stop:
    """Return a GTFS-RT Stop entity as the model a static stop uses.

    A replacement stop may be one the static feed has never heard of, so
    the RT entity is the only definition of it that exists. GTFS-RT's Stop
    has no location_type, so it is a stop -- GTFS's reading of an empty
    one. Its own cells only: parent-station inheritance needs the static
    stops, and the handle applies it. Its TranslatedString fields read as
    every alert text does (:func:`_first_translation`).
    """
    return Stop(
        id=stop.stop_id,
        name=_first_translation(stop.stop_name),
        latitude=stop.stop_lat if stop.HasField("stop_lat") else None,
        longitude=stop.stop_lon if stop.HasField("stop_lon") else None,
        parent_station=stop.parent_station or None,
        location_type=StopLocationType.STOP,
        stop_code=_first_translation(stop.stop_code),
        platform_code=_first_translation(stop.platform_code),
        # Unset reads as the proto default, UNKNOWN: GTFS's "0 or empty".
        wheelchair_boarding=_RT_BOARDING.get(
            _pb_enum_name(
                gtfs_realtime_pb2.Stop.WheelchairBoarding, stop.wheelchair_boarding
            )
            or ""
        ),
        description=_first_translation(stop.stop_desc),
        url=_first_translation(stop.stop_url),
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
            # A StopSelector's fields "cannot be both empty": an end selector
            # sent empty names no end, and the modification no span.
            if not modification.HasField("end_stop_selector")
            or modification.end_stop_selector.HasField("stop_sequence")
            or modification.end_stop_selector.stop_id
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
        # A restriction that does not parse restricts to nothing: it is
        # never widened to every run.
        starts = (
            frozenset(
                secs
                for raw in modifications.start_times
                if (secs := _lenient_start_secs(raw)) is not None
            )
            if modifications.start_times
            else None
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
    """YYYYMMDD to a date; anything else is "not a day this applies to".

    Exactly eight ASCII digits naming a real day: ``strptime`` would read
    ``2026111`` as 2026-11-01 and apply a detour to the wrong day.
    """
    if len(raw) != _START_DATE_LENGTH or not (raw.isascii() and raw.isdigit()):
        return None
    try:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
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
    """Per-carriage occupancy for a multi-carriage vehicle, front first.

    "The first occurrence represents the first carriage of the vehicle,
    given the current direction of travel", and the carriages must be
    numbered 1, 2, ... in that order: "If the second carriage in the
    direction of travel has a value of 3, consumers will discard data for
    all carriages". So unless the i-th has carriage_sequence i, there are
    none. An unset occupancy_status is the proto's declared default,
    NO_DATA_AVAILABLE.
    """
    carriages = vehicle.multi_carriage_details
    if [
        carriage.carriage_sequence if carriage.HasField("carriage_sequence") else None
        for carriage in carriages
    ] != list(range(1, len(carriages) + 1)):
        return ()
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
            ),
            occupancy_percentage=_occupancy_percentage(carriage),
            carriage_sequence=carriage.carriage_sequence,
        )
        for carriage in carriages
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
    """Extract vehicle positions, resolving route via trip when unset.

    A vehicle without a timestamp takes the message's: "consumers must use
    the message timestamp".
    """
    vehicles: list[VehiclePosition] = []
    sent = feed_timestamp(message)
    for entity in message.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle = entity.vehicle
        # Position is optional: a vehicle may say only which trip and stop.
        positioned = vehicle.HasField("position")
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
        # current_status is "Ignored if current_stop_sequence is missing";
        # with one, "If current_status is missing IN_TRANSIT_TO is assumed"
        # (the proto2 default an unset field reads as).
        current_status = (
            _vocab_or_none(
                VehicleStopStatus,
                _pb_enum_name(
                    gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus,
                    vehicle.current_status,
                ),
            )
            if vehicle.HasField("current_stop_sequence")
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
                latitude=vehicle.position.latitude if positioned else None,
                longitude=vehicle.position.longitude if positioned else None,
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
                # Unset, 0 and unrepresentable all read as no timestamp.
                timestamp=_epoch_to_utc(vehicle.timestamp) or sent,
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
    """Resolved RT outcome for one call of one trip instance.

    ``arrival``/``departure`` are the predicted instants (see
    :func:`resolve_trip_predictions`): an event's own time, else the
    scheduled time moved by its own or the carried delay; None for an event
    realtime says nothing about. ``delay_seconds`` is the delay at the
    stop: the one its own update states (the departure's, else the
    arrival's; an event's time against the schedule, else its delay), else
    for a stop with no update of its own, the carried or trip-level delay;
    None for a stop whose own update states none. (So with a 60 s arrival
    delay and a
    120 s departure delay it is 120; ``arrival`` is still the scheduled
    arrival plus 60.)
    """

    arrival: datetime | None
    departure: datetime | None
    delay_seconds: int | None
    vehicle_id: str | None


@dataclass(frozen=True)
class RealtimeCall:
    """One call of a realtime-only trip: its stop and times.

    ``scheduled_arrival``/``scheduled_departure`` are the events'
    ``scheduled_time`` (the new or replacement trip's own schedule, which a
    NO_DATA stop still gives; a lone one is both, as in stop_times), or a
    DUPLICATED copy's (its original's, moved, unless its updates state
    times of their own);
    ``arrival``/``departure`` the predictions: for NEW, ADDED and
    REPLACEMENT calls an event's own time, else its scheduled time moved by
    its own delay; for a copy, as a scheduled trip's resolve (carried and
    trip-level delays included) unless its updates state its times, when
    each call is as a NEW trip's. ``delay_seconds`` is the delay at the call
    (the departure's, else the arrival's). At least one time is set.
    """

    stop_id: str
    scheduled_arrival: datetime | None
    scheduled_departure: datetime | None
    arrival: datetime | None
    departure: datetime | None
    delay_seconds: int | None
    # A DUPLICATED copy's stop_time descriptors, its original's; unknown
    # (None) for NEW, ADDED and REPLACEMENT calls.
    pickup_type: PickupDropOffType | None = None
    drop_off_type: PickupDropOffType | None = None
    timepoint_exact: bool | None = None
    stop_headsign: str | None = None


@dataclass(frozen=True)
class RealtimeTrip:
    """A trip whose whole journey realtime states, absent from the schedule.

    A DUPLICATED copy is built the same way from its original (see
    ``transit``): its own trip_id, the original's moved calls, and the
    original's descriptors. So is a run of an exact_times=0 frequency trip,
    which has no schedule of its own: the trip's calls moved to the run's
    start, ``replaces`` naming the repetition whose headway slot it takes,
    and ``run`` its own (trip_id, start_date, start_secs).

    A NEW trip ("an extra trip unrelated to any existing trips"; ADDED, its
    deprecated predecessor, is read the same way) or a REPLACEMENT run,
    for which "the original schedule from the GTFS static isn't used":
    ``replaces`` is then the key of the scheduled run it replaces -- it is
    that run, on a new schedule. ``calls`` are its StopTimeUpdates in feed
    order (updates "must be sorted by stop_sequence"), less those it
    skips and those naming no stop or no time. ``route_id``,
    ``direction_id`` and ``headsign`` are what the update says
    (``TripProperties.trip_headsign`` for the headsign); a replacement,
    once matched, takes the replaced run's ``trip_id`` (a frequency
    repetition's own; a headway-based run's, its own start's), route and
    direction, and its headsign where the
    update gives none.
    """

    trip_id: str
    route_id: str | None
    direction_id: int | None
    headsign: str | None
    start_date: date | None
    vehicle_id: str | None
    calls: tuple[RealtimeCall, ...]
    replaces: TripUpdateKey | None = None
    # The key realtime names the run by, where it differs from ``replaces``
    # (an exact_times=0 run's own start): its vehicle names it so.
    run: TripUpdateKey | None = None
    # A copy's or a schedule run's trip descriptors (a short name also from
    # TripProperties, for any extra trip); unknown (None) otherwise. The
    # accessibility the update's VehicleDescriptor states overrides them.
    wheelchair_accessible: WheelchairAccess | None = None
    bikes_allowed: BikesAllowed | None = None
    trip_short_name: str | None = None
    # A run the calendar runs (a detoured run, an exact_times=0 run): its
    # trip's service; None for any other realtime-only trip.
    service_id: str | None = None
    # A detoured scheduled run's block: it is still that run, in its
    # vehicle's sequence of trips. No other realtime-only trip has one.
    block_id: str | None = None


@dataclass(frozen=True)
class TripStopUpdate:
    """One StopTimeUpdate, positioned by stop_sequence and/or stop_id.

    ``stop_sequence`` and ``stop_id`` carry exactly what the producer sent
    (either, or both). ``relationship`` is the raw per-stop
    schedule_relationship int (SCHEDULED/SKIPPED/NO_DATA; anything else --
    e.g. UNSCHEDULED or a future value -- is treated as SCHEDULED).
    ``arrival``/``departure`` are the events' explicit times and
    ``arrival_delay``/``departure_delay`` their delays, each event its own:
    a StopTimeEvent is "timing information for a single predicted event".
    """

    stop_id: str | None
    stop_sequence: int | None
    relationship: int
    arrival: datetime | None
    departure: datetime | None
    arrival_delay: int | None = None
    departure_delay: int | None = None
    # StopTimeProperties.assigned_stop_id: a real-time platform assignment,
    # which a stop_id sent alongside "must match".
    assigned_stop_id: str | None = None
    # StopTimeEvent.scheduled_time: a NEW, REPLACEMENT or DUPLICATED trip's
    # own schedule.
    scheduled_arrival: datetime | None = None
    scheduled_departure: datetime | None = None


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
    # TripUpdate.vehicle.wheelchair_accessible, None for NO_VALUE: it
    # "will overwrite the value from the GTFS".
    wheelchair_accessible: WheelchairAccess | None = None


@dataclass(frozen=True)
class TripPredictions:
    """Per-stop resolved outcomes for one trip against its static stop order.

    ``predictions`` is keyed by static ``stop_sequence``; ``skipped`` holds
    the stop_sequences the vehicle will not serve (their arrivals and any
    origin/destination journey rows touching them must be suppressed).
    """

    predictions: Mapping[int, StopPrediction]
    skipped: frozenset[int]
    # The trip's accessibility as its update states it (see
    # :attr:`TripUpdateEntry.wheelchair_accessible`).
    wheelchair_accessible: WheelchairAccess | None = None


@dataclass
class TripUpdates:
    """Indexed TripUpdates: what realtime says about each trip instance.

    Keyed by :data:`TripUpdateKey` -- ``(trip_id, start_date,
    start_secs)``. ``start_date`` is how GTFS-RT addresses ONE service
    day's instance of a trip; ``start_time`` ONE repetition of a
    frequency-based trip. As parsed from a message
    (:func:`trip_updates_from_message`) keys are the descriptors as sent;
    once matched against the schedule (see ``transit``) every key in
    ``trips`` names one dated run, so propagation happens within one
    matched instance. A removal naming no scheduled run keeps its key as
    sent: it matches no run but still removes an extra trip of that id.

    ``trips`` holds the predictions for runs that operate;
    ``canceled_trips`` the runs that were scheduled but do not operate
    (shown as cancelled); ``deleted_trips`` runs removed outright, which
    "must not be shown to users"; ``realtime_trips`` the realtime-only
    trips (NEW, ADDED, REPLACEMENT; once matched, runs of exact_times=0
    frequency trips too), a replacement also naming the
    scheduled run it replaces (see :attr:`replaced_trips`); ``duplicates``
    the DUPLICATED records, whose copies ``transit`` builds from the
    schedule and adds to ``realtime_trips``.
    """

    trips: dict[TripUpdateKey, TripUpdateEntry] = field(default_factory=dict)
    canceled_trips: set[TripUpdateKey] = field(default_factory=set)
    deleted_trips: set[TripUpdateKey] = field(default_factory=set)
    realtime_trips: list[RealtimeTrip] = field(default_factory=list)
    # DUPLICATED records, one per copy trip_id: the copy's calls come from
    # the schedule (see ``transit``), so they are built there.
    duplicates: list[TripUpdateRecord] = field(default_factory=list)

    @property
    def replaced_trips(self) -> set[TripUpdateKey]:
        """The scheduled runs a REPLACEMENT stands in for, not shown."""
        return {
            trip.replaces for trip in self.realtime_trips if trip.replaces is not None
        }


@dataclass(frozen=True)
class TripUpdateRecord:
    """One TripUpdate entity as its TripDescriptor names it, before matching.

    ``trip_id`` is ``""`` when the descriptor omits it: such a record names
    its trip only by ``route_id``, ``direction_id``, ``start_date`` and
    ``start_secs`` together (GTFS-RT's alternative trip matching), which
    only the schedule can resolve. ``relationship`` is the raw
    ``TripDescriptor.schedule_relationship``.
    """

    trip_id: str
    start_date: date | None
    start_secs: int | None
    route_id: str | None
    direction_id: int | None
    relationship: int
    entry: TripUpdateEntry
    # TripProperties.trip_headsign: "the headsign for this trip when it
    # differs from the original".
    trip_headsign: str | None = None
    # TripProperties.trip_id/start_date/start_time: a DUPLICATED trip's
    # copy -- its own id, and the service day and start it runs at.
    copy_trip_id: str | None = None
    copy_start_date: date | None = None
    copy_start_secs: int | None = None
    # TripProperties.trip_short_name: "the name for this trip when it
    # differs from the original" -- a copy's own train number, say.
    trip_short_name: str | None = None

    @property
    def key(self) -> TripUpdateKey:
        """The (trip_id, start_date, start_secs) identity this record names."""
        return (self.trip_id, self.start_date, self.start_secs)


def _placed(
    entry: TripUpdateEntry, calls: Sequence[TripCall]
) -> dict[int, TripStopUpdate]:
    """Place each StopTimeUpdate on a call position; unplaceable ones drop.

    A ``stop_sequence`` names its call, and a ``stop_id`` sent with it must
    agree -- with the scheduled stop, or with the update's
    ``assigned_stop_id``, which a stop_id "must match" when both are sent;
    otherwise the two halves name different calls and so none. A bare
    ``stop_id`` names its call only on a trip visiting that stop once:
    "stop_sequence is required for trips that visit the same stop_id more
    than once (e.g., a loop) to disambiguate", and an update without it is
    not guessed onto a visit. Several updates on one call: the last wins.
    """
    sequence_pos = {call.stop_sequence: pos for pos, call in enumerate(calls)}
    visits: dict[str, list[int]] = {}
    for index, call in enumerate(calls):
        visits.setdefault(call.stop_id, []).append(index)
    placed: dict[int, TripStopUpdate] = {}
    for stu in entry.stop_updates:
        pos: int | None = None
        if stu.stop_sequence is not None:
            pos = sequence_pos.get(stu.stop_sequence)
            if pos is not None and stu.stop_id not in (
                None,
                calls[pos].stop_id,
                stu.assigned_stop_id,
            ):
                pos = None
        elif stu.stop_id is not None:
            at = visits.get(stu.stop_id, [])
            if len(at) == 1:
                pos = at[0]
        if pos is not None:
            placed[pos] = stu
    return placed


def _moved(when: datetime | None, delay: int | None) -> datetime | None:
    """Return ``when`` shifted by ``delay``; None without either, or past datetime."""
    if when is None or delay is None:
        return None
    try:
        return when + timedelta(seconds=delay)
    except OverflowError:
        return None


def _event_delay(
    explicit: datetime | None, delay: int | None, scheduled: datetime | None
) -> int | None:
    """Return the delay one event states: its time against the schedule, else its delay.

    "If both time and delay are specified, time will take precedence", and
    "if only time is provided in the update, a delay computed by comparing
    the time against the GTFS schedule time" is what propagates -- so an
    event's delay always agrees with the instant it shows.
    """
    if explicit is not None and scheduled is not None:
        return round((explicit - scheduled).total_seconds())
    return delay


def resolve_trip_predictions(
    entry: TripUpdateEntry, calls: Sequence[TripCall]
) -> TripPredictions:
    """Resolve one trip instance's StopTimeUpdates against its calls.

    ``calls`` is the instance's complete ordered call list with its
    scheduled instants (one materialized repetition's for frequency
    trips). Only what the spec states is predicted, walking the calls in
    order:

    - An event (arrival or departure) an update gives is predicted from its
      time, else its delay ("If both time and delay are specified, time
      will take precedence"); an event it does not give is not predicted
      from it.
    - The delay an update states -- its departure's, else its arrival's;
      an event's time against the schedule, else its delay -- "is
      propagated to all subsequent stops" until newer information. Every
      SCHEDULED update replaces the delay carried from earlier stops ("updates
      with schedule relationships of SCHEDULED ... or NO_DATA will" stop
      propagation): once any update has stated a delay or NO_DATA, one
      stating none leaves later stops without one.
    - A SKIPPED update marks its stop skipped and leaves propagation alone.
      A NO_DATA update predicts nothing and cuts propagation: later stops
      are schedule-only until an update states a delay.
    - The trip-level delay is the carried delay until the first update that
      states a delay or is NO_DATA: it "is only propagated until the next
      stop along the trip with a StopTimeUpdate delay value specified".
    - A stop no update reaches takes the carried delay at both events.

    Placement is :func:`_placed`.
    """
    placed = _placed(entry, calls)
    skipped_rel = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    no_data_rel = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.NO_DATA
    predictions: dict[int, StopPrediction] = {}
    skipped: set[int] = set()
    covered = False  # has a stated delay or NO_DATA ended the trip-level delay?
    current: int | None = None  # the stop-level delay carried on, once covered
    for pos, call in enumerate(calls):
        own = placed.get(pos)
        carried = current if covered else entry.delay_seconds
        if own is not None and own.relationship == skipped_rel:
            skipped.add(call.stop_sequence)
            continue
        if own is not None and own.relationship == no_data_rel:
            covered, current = True, None
            continue
        if own is None:
            arrival = _moved(call.arrival, carried)
            departure = _moved(call.departure, carried)
            delay = carried
        else:
            arrival_delay = _event_delay(own.arrival, own.arrival_delay, call.arrival)
            departure_delay = _event_delay(
                own.departure, own.departure_delay, call.departure
            )
            stated = departure_delay if departure_delay is not None else arrival_delay
            arrival = own.arrival or _moved(call.arrival, own.arrival_delay)
            departure = own.departure or _moved(call.departure, own.departure_delay)
            delay = stated
            if stated is not None or covered:
                covered, current = True, stated
        if arrival is None and departure is None and delay is None:
            continue
        predictions[call.stop_sequence] = StopPrediction(
            arrival=arrival,
            departure=departure,
            delay_seconds=delay,
            vehicle_id=entry.vehicle_id,
        )
    return TripPredictions(
        predictions=predictions,
        skipped=frozenset(skipped),
        wheelchair_accessible=entry.wheelchair_accessible,
    )


# A TripDescriptor, or TripProperties naming a duplicate's start alike.
_Started = (
    gtfs_realtime_pb2.TripDescriptor | gtfs_realtime_pb2.TripUpdate.TripProperties
)


def _trip_start_secs(trip: _Started) -> int | None:
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


def _trip_start_date(trip: _Started) -> date | None:
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


def feed_timestamp(message: gtfs_realtime_pb2.FeedMessage) -> datetime | None:
    """When the producer built this message (header.timestamp), if it says.

    The instant its date-less trip references are judged at: the spec's
    timestamp "identifies the moment when the content of this feed has
    been created". None when absent, zero or unrepresentable.
    """
    if message.header.HasField("timestamp") and message.header.timestamp:
        return _epoch_to_utc(message.header.timestamp)
    return None


def _stop_update(stu: gtfs_realtime_pb2.TripUpdate.StopTimeUpdate) -> TripStopUpdate:
    """Parse one StopTimeUpdate, each event its own time and delay."""
    arrival = stu.arrival if stu.HasField("arrival") else None
    departure = stu.departure if stu.HasField("departure") else None
    return TripStopUpdate(
        stop_id=stu.stop_id or None,
        stop_sequence=stu.stop_sequence if stu.HasField("stop_sequence") else None,
        relationship=stu.schedule_relationship,
        arrival=_epoch_to_utc(arrival.time) if arrival is not None else None,
        departure=_epoch_to_utc(departure.time) if departure is not None else None,
        arrival_delay=(
            arrival.delay if arrival is not None and arrival.HasField("delay") else None
        ),
        departure_delay=(
            departure.delay
            if departure is not None and departure.HasField("delay")
            else None
        ),
        assigned_stop_id=(
            stu.stop_time_properties.assigned_stop_id or None
            if stu.HasField("stop_time_properties")
            else None
        ),
        scheduled_arrival=(
            _epoch_to_utc(arrival.scheduled_time)
            if arrival is not None and arrival.HasField("scheduled_time")
            else None
        ),
        scheduled_departure=(
            _epoch_to_utc(departure.scheduled_time)
            if departure is not None and departure.HasField("scheduled_time")
            else None
        ),
    )


def trip_update_records(
    message: gtfs_realtime_pb2.FeedMessage,
) -> list[TripUpdateRecord]:
    """Parse every TripUpdate entity of a message, in feed order, unmatched."""
    records: list[TripUpdateRecord] = []
    for entity in message.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        trip = trip_update.trip
        properties = trip_update.trip_properties
        records.append(
            TripUpdateRecord(
                trip_id=trip.trip_id,
                start_date=_trip_start_date(trip),
                start_secs=_trip_start_secs(trip),
                route_id=trip.route_id or None,
                direction_id=(
                    trip.direction_id if trip.HasField("direction_id") else None
                ),
                relationship=trip.schedule_relationship,
                trip_headsign=properties.trip_headsign or None,
                copy_trip_id=properties.trip_id or None,
                copy_start_date=_trip_start_date(properties),
                copy_start_secs=_trip_start_secs(properties),
                trip_short_name=properties.trip_short_name or None,
                entry=TripUpdateEntry(
                    stop_updates=tuple(
                        _stop_update(stu) for stu in trip_update.stop_time_update
                    ),
                    delay_seconds=(
                        trip_update.delay if trip_update.HasField("delay") else None
                    ),
                    vehicle_id=trip_update.vehicle.id or None,
                    wheelchair_accessible=_rt_wheelchair(trip_update.vehicle),
                ),
            )
        )
    return records


def _realtime_call(update: TripStopUpdate) -> RealtimeCall | None:
    """Return the call one StopTimeUpdate of a new or replacement trip states.

    None for a call the vehicle skips, or one naming no stop or no time. A
    NO_DATA call has its scheduled times only: "real-time prediction
    information is not available yet".
    """
    relationships = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
    if update.relationship == relationships.SKIPPED or not update.stop_id:
        return None
    # scheduled_time is defined as stop_times' times are, and there a lone
    # arrival or departure is both.
    scheduled_arrival = update.scheduled_arrival or update.scheduled_departure
    scheduled_departure = update.scheduled_departure or update.scheduled_arrival
    predicted = update.relationship != relationships.NO_DATA
    arrival = (
        update.arrival or _moved(scheduled_arrival, update.arrival_delay)
        if predicted
        else None
    )
    departure = (
        update.departure or _moved(scheduled_departure, update.departure_delay)
        if predicted
        else None
    )
    if all(
        when is None
        for when in (scheduled_arrival, scheduled_departure, arrival, departure)
    ):
        return None
    delays = [
        round((when - scheduled).total_seconds())
        for when, scheduled in (
            (departure, scheduled_departure),
            (arrival, scheduled_arrival),
        )
        if when is not None and scheduled is not None
    ]
    return RealtimeCall(
        stop_id=update.stop_id,
        scheduled_arrival=scheduled_arrival,
        scheduled_departure=scheduled_departure,
        arrival=arrival,
        departure=departure,
        delay_seconds=delays[0] if delays else None,
    )


def realtime_calls(entry: TripUpdateEntry) -> tuple[RealtimeCall, ...]:
    """Return the calls a trip's StopTimeUpdates state in full, in order."""
    return tuple(
        call for update in entry.stop_updates if (call := _realtime_call(update))
    )


def stated_copy_calls(
    entry: TripUpdateEntry, calls: Sequence[TripCall]
) -> tuple[RealtimeCall, ...]:
    """Return the calls a DUPLICATED copy's updates state, on its original's stops.

    A copy whose updates state ``scheduled_time`` states its own run and
    dwell times, but it is still "the same as an existing scheduled trip
    except for service start date and time": its updates are placed on the
    original's calls as a scheduled trip's are (by stop_sequence, or a stop
    visited once), and each stated call is that stop -- its id and
    descriptors -- at the times the update gives, as a NEW trip's call is.
    Only the stops its updates name are shown, in the trip's order.
    """
    stated: list[RealtimeCall] = []
    for pos, update in sorted(_placed(entry, calls).items()):
        call = calls[pos]
        realtime = _realtime_call(replace(update, stop_id=call.stop_id))
        if realtime is not None:
            stated.append(
                replace(
                    realtime,
                    pickup_type=call.pickup_type,
                    drop_off_type=call.drop_off_type,
                    timepoint_exact=call.timepoint_exact,
                    stop_headsign=call.stop_headsign,
                )
            )
    return tuple(stated)


def _realtime_trip(record: TripUpdateRecord, *, replaces: bool) -> RealtimeTrip:
    """Build the trip a NEW, ADDED or REPLACEMENT record states in full."""
    return RealtimeTrip(
        trip_id=record.trip_id,
        route_id=record.route_id,
        direction_id=record.direction_id,
        headsign=record.trip_headsign,
        start_date=record.start_date,
        vehicle_id=record.entry.vehicle_id,
        calls=realtime_calls(record.entry),
        replaces=record.key if replaces else None,
        wheelchair_accessible=record.entry.wheelchair_accessible,
        trip_short_name=record.trip_short_name,
    )


def _extra_trips(
    records: Sequence[TripUpdateRecord], removed_ids: Collection[str]
) -> tuple[list[RealtimeTrip], list[TripUpdateRecord]]:
    """Return the NEW/ADDED trips and DUPLICATED copies records add.

    An extra trip -- NEW, ADDED, or a copy under its own
    ``TripProperties.trip_id`` -- is that id, the one the producer minted,
    as removals read it too: the same trip from two feeds (or one url
    listed twice) is one trip, the last sent winning whatever its kind, and
    any removal naming the id drops it. One with no id stands alone. A
    DUPLICATED record lacking any of TripProperties' trip_id, start_date
    and start_time ("required if schedule_relationship is DUPLICATED")
    names no copy. An ADDED naming a DUPLICATED's original or copy is the
    pre-migration form of that copy and is ignored, as the migration guide
    tells consumers to.
    """
    descriptor = gtfs_realtime_pb2.TripDescriptor
    # The ADDED-to-DUPLICATED migration guide: consumers ignore an ADDED
    # whose trip_id is a DUPLICATED's trip_id (the original) or its
    # TripProperties.trip_id (the copy) -- a producer may send both -- as
    # long as that DUPLICATED names a copy at all.
    migrated = {
        trip_id
        for record in records
        if record.relationship == descriptor.DUPLICATED
        and record.copy_trip_id
        and record.copy_start_date is not None
        and record.copy_start_secs is not None
        for trip_id in (record.trip_id, record.copy_trip_id)
        if trip_id
    } | {
        # "consumers ignore any ADDED trips that have the same trip_id as a
        # NEW trip trip_id" -- the same guide.
        record.trip_id
        for record in records
        if record.relationship == descriptor.NEW and record.trip_id
    }
    by_id: dict[str, RealtimeTrip | TripUpdateRecord] = {}
    anonymous: list[RealtimeTrip] = []
    for record in records:
        if record.relationship == descriptor.ADDED and record.trip_id in migrated:
            continue
        if record.relationship in (descriptor.NEW, descriptor.ADDED):
            trip = _realtime_trip(record, replaces=False)
            if record.trip_id:
                by_id[record.trip_id] = trip
            else:
                anonymous.append(trip)
        elif (
            record.relationship == descriptor.DUPLICATED
            and record.trip_id
            and record.copy_trip_id
            and record.copy_start_date is not None
            and record.copy_start_secs is not None
        ):
            by_id[record.copy_trip_id] = record
    trips = [
        extra
        for trip_id, extra in by_id.items()
        if trip_id not in removed_ids and isinstance(extra, RealtimeTrip)
    ]
    copies = [
        extra
        for trip_id, extra in by_id.items()
        if trip_id not in removed_ids and isinstance(extra, TripUpdateRecord)
    ]
    return [*trips, *anonymous], copies


def indexed_trip_updates(records: Iterable[TripUpdateRecord]) -> TripUpdates:
    """Index records by the identity each names; a later record wins.

    A record without a trip_id names no scheduled trip here (only the
    schedule can resolve its route, direction and start; see ``transit``);
    a NEW or ADDED one's trip is still kept, as sent. DELETED and CANCELED
    win regardless of order: a producer may send one alongside stale
    predictions for the same trip in either order, and the result must be
    self-consistent rather than relying on consumers checking the removal
    sets first. DELETED wins over CANCELED -- a run removed outright is not
    shown at all. Predictions and replacements match on the full
    (trip_id, start_date, start_secs) identity, and of the two for one run
    the later wins; NEW and ADDED trips on bare trip_id (one without a
    trip_id stands alone), and DUPLICATED copies on their own
    ``TripProperties.trip_id`` -- an extra trip is identified by the id the
    producer minted for it, and dropping it on ANY removal of that id is
    the conservative "removed wins" choice (never show a trip that might
    not run). A DUPLICATED record lacking any of TripProperties' trip_id,
    start_date and start_time names no copy. A producer sending several updates
    for one identity is out of spec ("at most one trip_update per actual
    trip"); the last wins wholesale rather than attempting a field-level
    merge. Any other relationship -- SCHEDULED, UNSCHEDULED, or one this
    library does not model yet -- is read as predictions for the trip it
    names.
    """
    descriptor = gtfs_realtime_pb2.TripDescriptor
    updates = TripUpdates()
    records = list(records)
    extras = (descriptor.NEW, descriptor.ADDED, descriptor.DUPLICATED)
    # Per run, its predictions or its replacement, whichever came last.
    runs: dict[TripUpdateKey, TripUpdateEntry | RealtimeTrip] = {}
    for record in records:
        key = record.key
        if record.relationship in extras or not record.trip_id:
            continue
        elif record.relationship == descriptor.CANCELED:
            updates.canceled_trips.add(key)
        elif record.relationship == descriptor.DELETED:
            updates.deleted_trips.add(key)
        elif record.relationship == descriptor.REPLACEMENT:
            runs[key] = _realtime_trip(record, replaces=True)
        else:
            # An entry is recorded even with zero StopTimeUpdates: a bare
            # TripUpdate.delay with no STUs is a valid trip-wide fallback.
            runs[key] = record.entry
    updates.canceled_trips -= updates.deleted_trips
    removed = updates.canceled_trips | updates.deleted_trips
    removed_ids = {trip_id for trip_id, _, _ in removed}
    for key, run in runs.items():
        if key in removed:
            continue
        if isinstance(run, RealtimeTrip):
            updates.realtime_trips.append(run)
        else:
            updates.trips[key] = run
    extra, updates.duplicates = _extra_trips(records, removed_ids)
    updates.realtime_trips.extend(extra)
    return updates


def trip_updates_from_message(message: gtfs_realtime_pb2.FeedMessage) -> TripUpdates:
    """Index a message's TripUpdates by their descriptors as sent.

    No schedule matching happens here: date-less, trip_id-less and
    start-time-naming references stay as sent (see ``transit`` for the
    matched form).
    """
    return indexed_trip_updates(trip_update_records(message))


def _first_translation(translated: object) -> str | None:
    """Return a TranslatedString's text in the language the spec picks.

    The spec: the UI language's translation, else the default UI
    language's (English), else "If some translation has an unspecified
    language code, that translation is picked". The library has no UI
    language of its own, so its preference, English, stands for both, as
    for GBFS (:func:`~._language.is_preferred_language`: "en-US" is
    English). A translation without text is no translation. Where none
    of those exist, the first translation with text: some text over none,
    as for GBFS. None when there is none.
    """
    usable = [
        candidate
        for candidate in getattr(translated, "translation", ())
        if candidate.text
    ]
    for candidate in usable:
        if is_preferred_language(candidate.language):
            return str(candidate.text)
    for candidate in usable:
        if not candidate.language:
            return str(candidate.text)
    return str(usable[0].text) if usable else None


_NOT_YET = datetime.max.replace(tzinfo=UTC) - timedelta(days=2)


def _period_start(period: gtfs_realtime_pb2.TimeRange) -> datetime | None:
    """Return a TimeRange's start: None (minus infinity) only when absent.

    A present value is an instant, 0 the epoch; one past what a datetime
    holds has not started yet, so it reads as an instant later than any
    other (with a margin, so that it still converts to any timezone).
    """
    if not period.HasField("start"):
        return None
    try:
        return min(datetime.fromtimestamp(period.start, tz=UTC), _NOT_YET)
    except (OverflowError, OSError, ValueError):
        return _NOT_YET


def _period_end(period: gtfs_realtime_pb2.TimeRange) -> datetime | None:
    """Return a TimeRange's end: None (plus infinity) when absent or past datetime.

    A present value is an instant, 0 the epoch; one within days of what a
    datetime holds, or past it, never comes, as plus infinity.
    """
    if not period.HasField("end"):
        return None
    try:
        end = datetime.fromtimestamp(period.end, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None
    return None if end >= _NOT_YET else end


def _trip_selector(trip: gtfs_realtime_pb2.TripDescriptor) -> TripSelector | None:
    """Return the trip an EntitySelector names, or None where it cannot say.

    A start date or time that is present but does not parse names a run
    nobody can tell. A trip without trip_id names one only with all of
    route_id, direction_id, start_date and start_time ("must all be
    provided"): with fewer it is not one run, and its route_id "cannot be
    used ... to specify a route-wide alert". Either way the selector names
    nothing readable.
    """
    start_date = _trip_start_date(trip)
    start_secs = _trip_start_secs(trip)
    if (trip.start_date and start_date is None) or (
        trip.start_time and start_secs is None
    ):
        return None
    if not trip.trip_id and (
        not trip.route_id
        or not trip.HasField("direction_id")
        or start_date is None
        or start_secs is None
    ):
        return None
    return TripSelector(
        trip_id=trip.trip_id or None,
        route_id=trip.route_id or None,
        direction_id=trip.direction_id if trip.HasField("direction_id") else None,
        start_date=start_date,
        start_secs=start_secs,
    )


def _entity_selectors(alert: gtfs_realtime_pb2.Alert) -> tuple[EntitySelector, ...]:
    """Every informed_entity the alert names readably, in feed order.

    route_type and direction_id have 0 as a meaningful value (tram, and
    the feed's first direction), so presence is HasField. "If direction_id
    is provided, route_id must also be provided": a direction alone is
    every route's, so such a selector is left out.
    """
    selectors: list[EntitySelector] = []
    for entity in alert.informed_entity:
        if entity.HasField("direction_id") and not entity.route_id:
            continue
        trip = None
        if entity.HasField("trip"):
            trip = _trip_selector(entity.trip)
            if trip is None:
                continue
        selectors.append(
            EntitySelector(
                agency_id=entity.agency_id or None,
                route_id=entity.route_id or None,
                route_type=entity.route_type if entity.HasField("route_type") else None,
                direction_id=entity.direction_id
                if entity.HasField("direction_id")
                else None,
                trip=trip,
                stop_id=entity.stop_id or None,
            )
        )
    return tuple(selectors)


def alerts_from_message(message: gtfs_realtime_pb2.FeedMessage) -> list[ServiceAlert]:
    """Extract service alerts, each selector kept whole (see ServiceAlert)."""
    alerts: list[ServiceAlert] = []
    for entity in message.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert
        alerts.append(
            ServiceAlert(
                id=entity.id,
                header=_first_translation(alert.header_text),
                description=_first_translation(alert.description_text),
                tts_header=_first_translation(alert.tts_header_text),
                tts_description=_first_translation(alert.tts_description_text),
                # Unset reads as the declared default (proto2's getter):
                # UNKNOWN_CAUSE, UNKNOWN_EFFECT, UNKNOWN_SEVERITY.
                cause=_vocab_or_none(
                    AlertCause,
                    _pb_enum_name(gtfs_realtime_pb2.Alert.Cause, alert.cause),
                ),
                cause_detail=_first_translation(alert.cause_detail),
                effect=_vocab_or_none(
                    AlertEffect,
                    _pb_enum_name(gtfs_realtime_pb2.Alert.Effect, alert.effect),
                ),
                effect_detail=_first_translation(alert.effect_detail),
                severity=_vocab_or_none(
                    AlertSeverity,
                    _pb_enum_name(
                        gtfs_realtime_pb2.Alert.SeverityLevel, alert.severity_level
                    ),
                ),
                images=_alert_images(alert.image),
                image_alternative_text=_first_translation(alert.image_alternative_text),
                informed_entities=_entity_selectors(alert),
                active_periods=tuple(
                    (_period_start(period), _period_end(period))
                    for period in alert.active_period
                ),
                url=_first_translation(alert.url),
            )
        )
    return alerts
