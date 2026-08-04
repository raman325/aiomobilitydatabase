"""GTFS-RT fetching and protobuf parsing into typed models."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http import HTTPStatus
from urllib.parse import urlsplit

import aiohttp
from google.transit import gtfs_realtime_pb2

from .exceptions import FeedParseError, SourceAuthenticationError, SourceConnectionError
from .models import ServiceAlert, VehiclePosition
from .static_index import parse_gtfs_time

_AUTH_STATUSES = (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN)

_AUTH_TYPE_QUERY_PARAM = 1
_AUTH_TYPE_HEADER = 2

_ALLOWED_URL_SCHEMES = frozenset({"http", "https"})


def _require_http_url(url: str, context: str) -> None:
    """Reject a data-origin URL whose scheme isn't http/https before fetch.

    Producer URLs, hosted dataset URLs, and GBFS endpoint URLs all
    originate from data (catalog payloads, GBFS documents an agency
    controls) rather than from a caller-supplied parameter, so a malicious
    or corrupt source could point at a local resource (``file://``) or
    another unintended scheme. This is a proportionate library-level guard,
    not full SSRF prevention -- producer hosts are legitimately arbitrary,
    so host/IP validation is out of scope; a stricter policy belongs in the
    consuming application (e.g. the HA integration) if needed.
    """
    scheme = urlsplit(url).scheme.lower()
    if scheme not in _ALLOWED_URL_SCHEMES:
        raise SourceConnectionError(
            f"Refusing to fetch {context}: unsupported URL scheme {scheme!r} ({url})"
        )


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


async def fetch_feed_message(
    session: aiohttp.ClientSession,
    url: str,
    *,
    auth_type: int | None = None,
    api_key_name: str | None = None,
    api_key: str | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float = 30.0,
) -> gtfs_realtime_pb2.FeedMessage:
    """GET a GTFS-RT producer URL and parse the protobuf FeedMessage.

    ``auth_type`` follows the catalog's ``source_info.authentication_type``:
    1 = query parameter named ``api_key_name``; 2 = header named
    ``api_key_name``. ``headers`` (a direct-URL handle's custom headers)
    are merged in BEFORE auth handling, so an explicit ``api_key`` always
    wins over a same-named custom header rather than being silently
    shadowed. Parsing runs in a thread (CPU-bound for large feeds).
    """
    _require_http_url(url, "GTFS-RT producer URL")
    params: dict[str, str] = {}
    req_headers: dict[str, str] = dict(headers) if headers else {}
    if api_key is not None and api_key_name:
        if auth_type == _AUTH_TYPE_QUERY_PARAM:
            params[api_key_name] = api_key
        elif auth_type == _AUTH_TYPE_HEADER:
            req_headers[api_key_name] = api_key
    try:
        async with session.get(
            url,
            params=params or None,
            headers=req_headers or None,
            timeout=aiohttp.ClientTimeout(total=timeout_seconds),
        ) as resp:
            if resp.status in _AUTH_STATUSES:
                raise SourceAuthenticationError(
                    f"Producer rejected credentials ({resp.status}): {url}"
                )
            if resp.status >= HTTPStatus.BAD_REQUEST:
                raise SourceConnectionError(
                    f"Producer error {resp.status}: {url}", status=resp.status
                )
            raw = await resp.read()
    except (TimeoutError, aiohttp.ClientError) as err:
        raise SourceConnectionError(f"Error fetching {url}: {err}") from err
    message = gtfs_realtime_pb2.FeedMessage()
    try:
        await asyncio.to_thread(message.ParseFromString, raw)
    except Exception as err:  # DecodeError subclasses Exception, not a shared base
        raise FeedParseError(f"Undecodable GTFS-RT protobuf from {url}") from err
    return message


def vehicles_from_message(
    message: gtfs_realtime_pb2.FeedMessage,
    *,
    route_names: dict[str, str],
    trip_routes: dict[str, str],
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
            gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(
                vehicle.occupancy_status
            )
            if vehicle.HasField("occupancy_status")
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
                route_name=route_names.get(route_id) if route_id else None,
                trip_id=vehicle.trip.trip_id or None,
                occupancy_status=occupancy,
                timestamp=_epoch_to_utc(vehicle.timestamp),
            )
        )
    return vehicles


@dataclass(frozen=True)
class StopPrediction:
    """RT prediction for one (trip, stop)."""

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


@dataclass
class TripUpdates:
    """Parsed index of a TripUpdates feed.

    Predictions are keyed by ``(trip_id, start_secs, stop_id)`` and
    cancellations by ``(trip_id, start_secs)``, where ``start_secs`` is the
    parsed ``TripDescriptor.start_time`` (None when absent or unparseable).
    ``start_time`` is how GTFS-RT addresses ONE repetition of a
    frequency-based trip, and the consumer-side merge matches it against
    the static index's materialized repetitions.
    ``TripDescriptor.start_date`` is deliberately NOT consulted in this
    pass: a start_time repeats daily, but the arrivals lookahead window
    plus the absolute prediction timestamps make cross-service-day
    collisions marginal -- start_date disambiguation is a documented
    refinement, not a correctness prerequisite here.
    """

    predictions: dict[tuple[str, int | None, str], StopPrediction] = field(
        default_factory=dict
    )
    canceled_trips: set[tuple[str, int | None]] = field(default_factory=set)
    added: list[AddedStopTime] = field(default_factory=list)


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


def trip_updates_from_message(message: gtfs_realtime_pb2.FeedMessage) -> TripUpdates:
    """Index TripUpdate entities by (trip_id, start_secs, stop_id)."""
    updates = TripUpdates()
    canceled = gtfs_realtime_pb2.TripDescriptor.CANCELED
    added = gtfs_realtime_pb2.TripDescriptor.ADDED
    for entity in message.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id
        start_secs = _trip_start_secs(trip_update.trip)
        vehicle_id = trip_update.vehicle.id or None
        if trip_update.trip.schedule_relationship == canceled:
            updates.canceled_trips.add((trip_id, start_secs))
            continue
        is_added = trip_update.trip.schedule_relationship == added
        for stu in trip_update.stop_time_update:
            arrival = (
                _epoch_to_utc(stu.arrival.time) if stu.HasField("arrival") else None
            )
            departure = (
                _epoch_to_utc(stu.departure.time) if stu.HasField("departure") else None
            )
            delay = (
                stu.arrival.delay
                if stu.HasField("arrival") and stu.arrival.HasField("delay")
                else (
                    stu.departure.delay
                    if stu.HasField("departure") and stu.departure.HasField("delay")
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
                    )
                )
            else:
                updates.predictions[(trip_id, start_secs, stu.stop_id)] = (
                    StopPrediction(
                        arrival=arrival,
                        departure=departure,
                        delay_seconds=delay,
                        vehicle_id=vehicle_id,
                    )
                )
    # Cancellation wins regardless of entity order: a producer may send a
    # CANCELED trip_update alongside stale predictions/added-stop-times for
    # the same trip in either order within one message. The parsed result
    # must be self-consistent rather than relying on consumers checking
    # canceled_trips first. Predictions are matched on the full
    # (trip_id, start_secs) identity; ADDED stop times are matched on bare
    # trip_id -- an added trip is identified by the id the producer minted
    # for it, and dropping its rows on ANY cancellation of that id is the
    # conservative "removed wins" choice (never show a trip that might not
    # run).
    if updates.canceled_trips:
        updates.predictions = {
            key: prediction
            for key, prediction in updates.predictions.items()
            if (key[0], key[1]) not in updates.canceled_trips
        }
        canceled_ids = {trip_id for trip_id, _ in updates.canceled_trips}
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
    """Extract service alerts with best-effort English text."""
    alerts: list[ServiceAlert] = []
    for entity in message.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert
        route_ids = sorted({ie.route_id for ie in alert.informed_entity if ie.route_id})
        stop_ids = sorted({ie.stop_id for ie in alert.informed_entity if ie.stop_id})
        alerts.append(
            ServiceAlert(
                id=entity.id,
                header=_first_translation(alert.header_text),
                description=_first_translation(alert.description_text),
                cause=gtfs_realtime_pb2.Alert.Cause.Name(alert.cause)
                if alert.HasField("cause")
                else None,
                effect=gtfs_realtime_pb2.Alert.Effect.Name(alert.effect)
                if alert.HasField("effect")
                else None,
                severity=(
                    gtfs_realtime_pb2.Alert.SeverityLevel.Name(alert.severity_level)
                    if alert.HasField("severity_level")
                    else None
                ),
                route_ids=route_ids,
                stop_ids=stop_ids,
                active_periods=[
                    (
                        _epoch_to_utc(period.start)
                        if period.HasField("start")
                        else None,
                        _epoch_to_utc(period.end) if period.HasField("end") else None,
                    )
                    for period in alert.active_period
                ],
                url=_first_translation(alert.url),
            )
        )
    return alerts
