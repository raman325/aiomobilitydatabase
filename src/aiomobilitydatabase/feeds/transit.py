"""TransitFeedHandle: schedule + realtime snapshots for one transit feed."""

from __future__ import annotations

import asyncio
import hashlib
import shutil
import tempfile
from collections.abc import (
    AsyncIterator,
    Callable,
    Collection,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
from google.transit import gtfs_realtime_pb2

from ..models import (
    DataType,
    EntityType,
    GtfsFeed,
    GtfsRtFeed,
    LatestDataset,
    SourceInfo,
)
from ._urls import (
    _require_http_url,  # deliberate friend access: shared data-origin URL guard
)
from .const import STATIC_DB_FILENAME
from .exceptions import (
    FeedParseError,
    SourceConnectionError,
    StaticDataUnavailableError,
)
from .geo import Circle, in_circle
from .models import (
    Agency,
    ArrivalsQuery,
    FeedInfo,
    Route,
    ServiceAlert,
    StaticBuildProgress,
    StationGroup,
    Stop,
    StopArrival,
    StopLocationType,
    TripQuery,
    UpcomingTrip,
    VehiclePosition,
)
from .rt import (
    FeedValidators,
    Modification,
    TripModifications,
    TripPredictions,
    TripUpdateKey,
    TripUpdates,
    alerts_from_message,
    fetch_feed_message,
    resolve_trip_predictions,
    stops_from_message,
    trip_modifications_from_message,
    trip_updates_from_message,
    vehicles_from_message,
)
from .static_index import (
    ScheduledDeparture,
    ScheduledTrip,
    StaticIndex,
    TripCall,
    TripInstanceCalls,
)

if TYPE_CHECKING:
    from .client import MobilityFeedsClient

_PROGRESS_CHUNK_BYTES = 262_144
_CACHE_KEY_HASH_CHARS = 16

# Direct-URL RT feeds advertise every entity type: capabilities are unknown
# without a catalog record, so every fetch tries all parsers and absent
# types simply yield nothing.


def _direct_cache_key(static_url: str) -> str:
    """Cache-directory name for a direct static URL: ``url-<sha256[:16]>``.

    A hash rather than the URL itself: URLs contain path separators and
    other filesystem-hostile characters, while a fixed-length hex prefix
    can never carry traversal components -- so purge_cache's (and
    _ensure_index_from_source's) path-containment guard keeps working
    unchanged. This key is what :attr:`TransitFeedHandle.static_feed_id`
    returns for direct handles, so consumers can purge with it.
    """
    digest = hashlib.sha256(static_url.encode()).hexdigest()
    return f"url-{digest[:_CACHE_KEY_HASH_CHARS]}"


def _sha256_of_file(path: Path) -> str:
    """Hash a downloaded zip (synchronous: call via ``asyncio.to_thread``)."""
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        while chunk := fp.read(1 << 16):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class _StaticSource:
    """Where a static GTFS zip comes from and how its dataset is identified.

    One shape for both acquisition paths so the download+build core never
    forks: the catalog path fills ``dataset_id`` from latest-dataset
    metadata, while the direct-URL path fills it from HTTP validators
    (``etag:``/``lastmod:`` prefixed) -- or leaves it None, meaning
    "identify by sha256 of the downloaded bytes" (which requires the
    download to happen before the cache can be consulted).
    """

    url: str
    cache_key: str
    dataset_id: str | None
    timezone_name: str | None
    headers: Mapping[str, str] | None


@dataclass(frozen=True)
class _DirectUrls:
    """A direct handle's origin, kept for refresh_static re-probing."""

    static_url: str
    headers: Mapping[str, str] | None


def _predicted_time(
    explicit: datetime | None, scheduled: datetime | None, delay_seconds: int | None
) -> datetime | None:
    """One end's predicted instant: explicit epoch wins over scheduled+delay.

    An explicit ``arrival.time``/``departure.time`` from the stop's own
    StopTimeUpdate takes precedence exactly as before this seam existed;
    otherwise a known (possibly propagated or trip-level) delay shifts the
    scheduled instant. No prediction when neither applies (e.g. a delay
    with no scheduled time at that end).
    """
    if explicit is not None:
        return explicit
    if scheduled is not None and delay_seconds is not None:
        return scheduled + timedelta(seconds=delay_seconds)
    return None


# A scheduled row's date-less RT identity: (source_trip_id, start_secs) --
# (trip_id, None) for plain trips, (template id, repetition start) for
# frequency repetitions. Adjacent service days repeat the same identity
# exactly 24h apart; the service day disambiguates between them.
_RtIdentity = tuple[str, int | None]

# One scheduled trip instance in a query result: the CONCRETE trip id
# (synthetic repetition ids included) plus its service day. The same
# concrete trip id recurs across service days, so the id alone cannot key
# resolved predictions in windows spanning 24h or more.
_TripInstance = tuple[str, date]


def _rt_key_for_row(
    identity: _RtIdentity,
    service_date: date,
    keys: Collection[TripUpdateKey],
    current_dates: Mapping[_RtIdentity, date],
) -> TripUpdateKey | None:
    """Resolve which RT key in ``keys`` (if any) addresses one scheduled row.

    A key WITH a start_date matches only the row whose service day equals
    it -- AND-ed with the start_time rules, which are unchanged: frequency
    rows need an aligned start_secs, plain rows need start_secs None. A
    key WITHOUT a start_date follows the spec's "assume the trip is
    running on the current service day": it matches only the identity's
    CURRENT run, ``current_dates[identity]`` (see
    :meth:`StaticIndex.current_service_dates`). That is decided from the
    whole trip and the query's ``start``, never from which runs happen to
    fall in the window -- so a long or open-ended window can't move a
    date-less update onto tomorrow's run once today's has finished. An
    identity with no current run takes no date-less update at all. When a
    dated and a date-less key both exist for one row, the dated key wins
    (it is strictly more specific). A garbage start_date parses to None
    (see ``rt._trip_start_date``), so it behaves exactly like an absent one.
    """
    source_trip_id, start_secs = identity
    dated: TripUpdateKey = (source_trip_id, service_date, start_secs)
    if dated in keys:
        return dated
    dateless: TripUpdateKey = (source_trip_id, None, start_secs)
    if dateless in keys and current_dates.get(identity) == service_date:
        return dateless
    return None


def _effective_departure(arrival: StopArrival, fallback: datetime) -> datetime:
    """Effective departure instant for ordering and the past-row drop.

    Predicted departure if any, else scheduled, else the predicted arrival
    (an RT-added terminal call announces only an arrival), else
    ``fallback`` for a row with no time at all.
    """
    return (
        arrival.predicted_departure
        or arrival.scheduled_departure
        or arrival.predicted_arrival
        or fallback
    )


@dataclass(frozen=True)
class _Vehicles:
    """Live vehicles indexed for matching onto scheduled runs.

    ``by_instance`` is keyed by (trip id, start seconds, service date) --
    the trip, the repetition of a frequency-based trip, and the run's
    service day, as a VehiclePosition's TripDescriptor names them (either
    of the last two may be absent). ``dateless_trip_ids`` are trips some
    vehicle names WITHOUT a start date: those vehicles can only match the
    trip's current run, which the caller resolves.
    """

    by_vehicle_id: Mapping[str, VehiclePosition]
    by_instance: Mapping[tuple[str, int | None, date | None], VehiclePosition | None]
    dateless_trip_ids: frozenset[str]


def _vehicle_index(vehicles: Sequence[VehiclePosition]) -> _Vehicles:
    """Index live vehicles for matching, by vehicle id and by run.

    An instance entry holds None where two vehicles claim the same run:
    that is a genuinely ambiguous feed, not a resolvable one, and guessing
    would put the wrong vehicle on the map.
    """
    by_vehicle_id: dict[str, VehiclePosition] = {}
    by_instance: dict[tuple[str, int | None, date | None], VehiclePosition | None] = {}
    dateless: set[str] = set()
    for vehicle in vehicles:
        if vehicle.vehicle_id:
            by_vehicle_id[vehicle.vehicle_id] = vehicle
        if vehicle.trip_id:
            key = (vehicle.trip_id, vehicle.trip_start_secs, vehicle.trip_start_date)
            # A second claimant poisons the entry rather than overwriting it.
            by_instance[key] = None if key in by_instance else vehicle
            if vehicle.trip_start_date is None:
                dateless.add(vehicle.trip_id)
    return _Vehicles(by_vehicle_id, by_instance, frozenset(dateless))


def _match_vehicle(
    vehicle_id: str | None,
    plain_trip_id: str | None,
    start_secs: int | None,
    *,
    service_date: date | None,
    current_dates: Mapping[_RtIdentity, date],
    vehicles: _Vehicles,
    scheduled: bool = True,
) -> VehiclePosition | None:
    """Return the vehicle serving a row, or None if it is not pinnable.

    A vehicle id from the row's own realtime prediction wins: that
    prediction was already matched to this exact run. Otherwise the row's
    run is looked up by trip: a vehicle naming this service day matches;
    one naming no day matches only the trip's CURRENT run (``service_date``
    equal to ``current_dates`` for the identity), so today's vehicle never
    lands on tomorrow's row.

    ``plain_trip_id`` must be the PRODUCER-facing trip id (a repetition's
    template id, not its synthetic ``{trip_id}#{start_secs}`` id). A
    repetition is addressed by (template id, its start); a vehicle that
    names no start_time has not said which repetition it means, so it
    serves whichever repetition of the current run asks. (This is more
    lenient than TripUpdates, where an update without start_time matches
    no repetition: a position is display-only, a prediction is not.)
    ``scheduled=False`` is an RT-added row: it matches a vehicle naming
    its ``service_date`` (the added trip's own start_date, possibly None)
    or naming no day at all.
    """
    if vehicle_id and (vehicle := vehicles.by_vehicle_id.get(vehicle_id)) is not None:
        return vehicle
    if not plain_trip_id:
        return None
    # An RT-added row has no schedule to judge "current" against: a vehicle
    # naming no day may ride it, as may one naming the row's own day.
    is_current = (
        not scheduled or current_dates.get((plain_trip_id, start_secs)) == service_date
    )
    for secs in (start_secs, None) if start_secs is not None else (None,):
        if service_date is not None and (
            dated := vehicles.by_instance.get((plain_trip_id, secs, service_date))
        ):
            return dated
        if is_current and (
            dateless := vehicles.by_instance.get((plain_trip_id, secs, None))
        ):
            return dateless
    return None


def _selector_index(
    calls: Sequence[TripCall], stop_sequence: int | None, stop_id: str | None
) -> int | None:
    """Resolve a StopSelector to an index into a trip's calls.

    The spec says both fields must match the GTFS feed, so when a selector
    carries both they must agree -- a selector naming stop_sequence 5 and
    stop_id "X" where call 5 is stop "Y" describes no call of this trip,
    and guessing which half the producer meant would silently detour the
    wrong span.
    """
    for index, call in enumerate(calls):
        if stop_sequence is not None and call.stop_sequence != stop_sequence:
            continue
        if stop_id is not None and call.stop_id != stop_id:
            continue
        if stop_sequence is None and stop_id is None:
            return None
        return index
    return None


def _apply_modification(
    calls: list[TripCall],
    modification: Modification,
    rt_stops: Mapping[str, Stop],
    static_stops: Mapping[str, Stop],
) -> list[TripCall] | None:
    """Replace a span of calls, returning the new sequence.

    Returns None when the modification does not describe this trip -- an
    unresolvable selector, or an end before its start. Dropping it is the
    conservative read: applying half a detour would be worse than applying
    none of it.
    """
    start = _selector_index(
        calls, modification.start_stop_sequence, modification.start_stop_id
    )
    end = _selector_index(
        calls, modification.end_stop_sequence, modification.end_stop_id
    )
    if start is None or end is None or end < start:
        return None
    # The reference is the call BEFORE the span -- or the span's own first
    # call when the span starts the trip, which is the only case the spec
    # allows a negative travel_time_to_stop for.
    reference = calls[start - 1] if start > 0 else calls[start]
    anchor_time = reference.arrival or reference.departure
    replacements: list[TripCall] = []
    for offset, replacement in enumerate(modification.replacements):
        if (
            replacement.stop_id not in rt_stops
            and replacement.stop_id not in static_stops
        ):
            # A replacement naming a stop nothing defines cannot be placed
            # on a map or named in a board.
            continue
        if replacement.travel_time_to_stop is None or anchor_time is None:
            # Untimed: no travel time, or an untimed reference call to
            # measure it from. Placing it anyway would invent a time.
            continue
        when = anchor_time + timedelta(seconds=replacement.travel_time_to_stop)
        replacements.append(
            TripCall(
                # Synthetic sequence numbers keep the span ordered between
                # its neighbours without colliding with real ones.
                stop_sequence=reference.stop_sequence * 1000 + offset + 1,
                stop_id=replacement.stop_id,
                arrival=when,
                departure=when,
                pickup_type=None,
                drop_off_type=None,
                timepoint_exact=None,
                stop_headsign=None,
            )
        )
    tail = calls[end + 1 :]
    delay = timedelta(seconds=modification.propagated_delay_seconds)
    shifted = [
        TripCall(
            stop_sequence=call.stop_sequence,
            stop_id=call.stop_id,
            arrival=call.arrival + delay if call.arrival else None,
            departure=call.departure + delay if call.departure else None,
            pickup_type=call.pickup_type,
            drop_off_type=call.drop_off_type,
            timepoint_exact=call.timepoint_exact,
            stop_headsign=call.stop_headsign,
        )
        for call in tail
    ]
    return calls[:start] + replacements + shifted


def _modified_calls(
    instance: TripInstanceCalls,
    modifications: Sequence[Modification],
    rt_stops: Mapping[str, Stop],
    static_stops: Mapping[str, Stop],
) -> list[TripCall]:
    """Apply every modification of one entity to one trip instance.

    Applied latest-span-first so an earlier modification's indices are not
    invalidated by a later one rewriting the tail.
    """
    calls = list(instance.calls)
    ordered = sorted(
        modifications,
        key=lambda m: (
            _selector_index(calls, m.start_stop_sequence, m.start_stop_id) or 0
        ),
        reverse=True,
    )
    for modification in ordered:
        if (
            applied := _apply_modification(calls, modification, rt_stops, static_stops)
        ) is not None:
            calls = applied
    return calls


def _detoured_rows(
    template: TripInstanceCalls,
    calls: Sequence[TripCall],
    shift: timedelta,
    *,
    wanted_stops: Collection[str],
    end: datetime | None,
    rt_stops: Mapping[str, Stop],
    stops_by_id: Mapping[str, Stop],
    route: Route | None,
) -> list[StopArrival]:
    """One detoured run's rows at the queried stops.

    ``calls`` were rebuilt on ``template``'s day; ``shift`` moves them to
    the run's own day (GTFS times are offsets from the day's start, and a
    detour only adds offsets, so this is exact). Untimed calls are never
    rows, as in the schedule itself; nor are calls past ``end`` or past the
    end of datetime.
    """
    rows: list[StopArrival] = []
    for call in calls:
        if call.stop_id not in wanted_stops or call.departure is None:
            continue
        try:
            departure = call.departure + shift
            arrival = call.arrival + shift if call.arrival else None
        except OverflowError:
            continue  # past the end of datetime: no instant to show
        if end is not None and departure > end:
            continue
        rows.append(
            StopArrival(
                stop_id=call.stop_id,
                # An RT-added stop is the ONLY definition of itself.
                stop=rt_stops.get(call.stop_id) or stops_by_id.get(call.stop_id),
                route_id=template.route_id,
                route=route,
                trip_id=template.trip_id,
                service_id=template.service_id,
                headsign=template.headsign,
                scheduled_arrival=arrival,
                scheduled_departure=departure,
                predicted_arrival=None,
                predicted_departure=None,
                delay_seconds=None,
                realtime=True,
                vehicle_id=None,
                vehicle=None,
                wheelchair_accessible=template.wheelchair_accessible,
                bikes_allowed=template.bikes_allowed,
                direction_id=template.direction_id,
                pickup_type=call.pickup_type,
                drop_off_type=call.drop_off_type,
                timepoint_exact=call.timepoint_exact,
                stop_headsign=call.stop_headsign,
                trip_short_name=template.trip_short_name,
                block_id=template.block_id,
            )
        )
    return rows


def _effective_trip_departure(trip: UpcomingTrip) -> datetime:
    """Predicted origin departure if any, else scheduled."""
    return trip.predicted_departure or trip.scheduled_departure


# Open-ended queries read the schedule over a growing horizon: the first
# span, then doubling. Two hours covers a daytime board in one pass.
_INITIAL_HORIZON = timedelta(hours=2)


def _realtime_now(updates: TripUpdates, start: datetime) -> datetime:
    """Return the instant realtime describes: the feed's time, else the present.

    "The current service day" of a date-less update is judged at the
    producer's header timestamp, not at the query's ``start`` -- a board
    for tomorrow morning must not move today's cancellation onto
    tomorrow's run. A feed without a timestamp (the spec requires one)
    falls back to now, but never later than ``start``: realtime describes
    the present, and a query about the past is answered as of its start.
    """
    if updates.feed_time is not None:
        return updates.feed_time
    return min(start, datetime.now(UTC))


def _utc_window(
    start: datetime | None, end: datetime | None
) -> tuple[datetime, datetime | None]:
    """Return the query window in UTC (``start`` defaulting to now).

    Python adds and compares two aware datetimes sharing a tzinfo by wall
    clock, ignoring their offsets: a ``start`` given in the feed's own
    zone would shift by a DST jump against the service-day starts. In UTC
    it never does. A naive datetime names no instant at all.
    """
    start = start or datetime.now(UTC)
    for name, value in (("start", start), ("end", end)):
        if value is not None and value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware, got {value!r}")
    return start.astimezone(UTC), end.astimezone(UTC) if end is not None else None


def _require_bound(end: datetime | None, limits: Sequence[int | None]) -> None:
    """Reject an open-ended query with no limit: it would scan the whole feed."""
    if end is None and any(limit is None for limit in limits):
        raise ValueError("an open-ended query (end=None) needs a limit")


def _horizons(
    start: datetime, end: datetime | None, ceiling: datetime | None
) -> Iterator[tuple[datetime, bool]]:
    """Yield ``(horizon_end, is_final)`` for one dated query.

    Horizons grow from ``start + _INITIAL_HORIZON`` by doubling up to the
    last instant worth reading: ``end`` when given, but never past
    ``ceiling`` -- the latest instant any queried departure can occur
    (None: none can) -- and never before ``start``. Bounded and open-ended
    queries scan the same way, so a limited board stops reading as soon as
    it settles. Spans are compared before they are added, so a ceiling
    near the end of ``datetime`` never overflows.
    """
    bound = ceiling if ceiling is not None else start
    if end is not None:
        bound = min(end, bound)
    last = max(bound, start)
    remaining = last - start
    span = _INITIAL_HORIZON
    while span < remaining:
        yield start + span, False
        span *= 2
    yield last, True


def _filters(
    query: ArrivalsQuery,
) -> tuple[list[str], list[str] | None, list[str] | None]:
    """One query's stops and filters, as the index's ceiling query takes them."""
    return (
        sorted(set(query.stop_ids)),
        sorted(set(query.route_ids)) if query.route_ids else None,
        sorted(set(query.headsigns)) if query.headsigns else None,
    )


def _past_ceiling(ceiling: datetime | None, horizon: datetime) -> bool:
    """Whether a board can no longer change: no departure is left to read.

    Every scheduled departure a query could show is at or before its
    ceiling, so once the horizon reaches it (or there is no ceiling:
    nothing can ever match) the board is complete.
    """
    return ceiling is None or horizon >= ceiling


def _pushdown(
    queries: Sequence[ArrivalsQuery],
) -> tuple[list[str], list[str] | None, list[str] | None]:
    """Collect a batch's stops and the route/headsign filters SQL can apply.

    A filter is pushed into the schedule read only when EVERY query has
    one (their union then covers every row any query could keep); one
    unfiltered query needs all rows. Each query still applies its own
    filters afterwards.
    """
    stops = sorted({stop_id for query in queries for stop_id in query.stop_ids})
    routes = (
        sorted({route for query in queries for route in query.route_ids or ()})
        if queries and all(query.route_ids for query in queries)
        else None
    )
    headsigns = (
        sorted({sign for query in queries for sign in query.headsigns or ()})
        if queries and all(query.headsigns for query in queries)
        else None
    )
    return stops, routes, headsigns


# A scheduled row's identity: one stop_times row of one run. Rows read up
# front and rows read by the scan are the same row only when all of it
# matches -- an (invalid) feed repeating a stop_sequence within a trip
# keeps both rows, whichever path reads them.
_RowKey = tuple[str, date, int, str, datetime]


def _departure_key(dep: ScheduledDeparture) -> _RowKey:
    return (
        dep.trip_id,
        dep.service_date,
        dep.stop_sequence,
        dep.stop_id,
        dep.departure,
    )


def _journey_key(trip: ScheduledTrip) -> _RowKey:
    return (
        trip.trip_id,
        trip.service_date,
        trip.origin_stop_sequence,
        trip.destination_stop_id,
        trip.departure,
    )


def _board_settles(
    times: Sequence[datetime], limit: int | None, horizon: datetime
) -> bool:
    """Whether one board read over ``[start, horizon]`` is final.

    ``times`` are the board's effective departures, sorted and cut to
    ``limit`` (``None`` = unlimited). Every row realtime can move was read
    in full before the scan began (see :meth:`TransitFeedHandle.get_arrivals`),
    so a wider horizon only adds rows realtime leaves at their scheduled
    time, after ``horizon``. Such a row changes the board only if the
    board has room for it (an unlimited board always has, so only its
    ceiling ends it) or the board's last row is itself after ``horizon``.
    """
    if limit is None or len(times) < limit:
        return False
    return not times or times[-1] <= horizon


@dataclass
class _PredictionMemo:
    """One call's resolved predictions, kept across its horizon passes.

    ``resolved`` is keyed by (concrete trip id, TripUpdate key): resolution
    depends on nothing else. ``stop_calls`` holds the static stop orders it
    was resolved against.
    """

    stop_calls: dict[str, list[tuple[int, str]]] = field(default_factory=dict)
    resolved: dict[tuple[str, TripUpdateKey], TripPredictions] = field(
        default_factory=dict
    )


def _scheduled_row(
    dep: ScheduledDeparture,
    stops_by_id: Mapping[str, Stop],
    routes_by_id: Mapping[str, Route],
) -> StopArrival:
    """Build a schedule-only :class:`StopArrival`: no prediction, no vehicle."""
    return StopArrival(
        stop_id=dep.stop_id,
        stop=stops_by_id.get(dep.stop_id),
        route_id=dep.route_id,
        route=routes_by_id.get(dep.route_id),
        trip_id=dep.trip_id,
        service_id=dep.service_id,
        headsign=dep.headsign,
        scheduled_arrival=dep.arrival,
        scheduled_departure=dep.departure,
        predicted_arrival=None,
        predicted_departure=None,
        delay_seconds=None,
        realtime=False,
        vehicle_id=None,
        vehicle=None,
        wheelchair_accessible=dep.wheelchair_accessible,
        bikes_allowed=dep.bikes_allowed,
        direction_id=dep.direction_id,
        pickup_type=dep.pickup_type,
        drop_off_type=dep.drop_off_type,
        timepoint_exact=dep.timepoint_exact,
        stop_headsign=dep.stop_headsign,
        trip_short_name=dep.trip_short_name,
        block_id=dep.block_id,
    )


def _select_arrivals(
    arrivals: Sequence[StopArrival], query: ArrivalsQuery
) -> list[StopArrival]:
    """Apply one query's stop, route, and headsign filters, then its limit."""
    stop_ids = set(query.stop_ids)
    route_ids = set(query.route_ids) if query.route_ids else None
    headsigns = set(query.headsigns) if query.headsigns else None
    selected = [
        arrival
        for arrival in arrivals
        if arrival.stop_id in stop_ids
        and (route_ids is None or arrival.route_id in route_ids)
        and (headsigns is None or arrival.headsign in headsigns)
    ]
    return selected[: query.limit]


def group_stations(stops: Sequence[Stop]) -> list[StationGroup]:
    """Group boarding stops into logical stations, sorted by name.

    GTFS models stations as hierarchies: platforms (location_type 0, or
    unset) may link to a parent station (1), while entrances (2) and
    pathway nodes (3+) are never boarding stops and are dropped. Boarding
    stops sharing a parent station — or, without one, an identical name —
    are grouped so consumers can offer "Metro Center" instead of every
    platform and entrance.
    """
    stations = {
        stop.id: stop
        for stop in stops
        if stop.location_type is StopLocationType.STATION
    }
    names: dict[str, str] = {}
    members: dict[str, list[str]] = {}
    for stop in stops:
        if stop.location_type not in (None, StopLocationType.STOP):
            continue
        if stop.parent_station:
            key = stop.parent_station
            station = stations.get(stop.parent_station)
            name = (station.name if station else None) or stop.name or key
        else:
            name = stop.name or stop.id
            key = name.casefold()
        names.setdefault(key, name)
        members.setdefault(key, []).append(stop.id)
    return sorted(
        (
            StationGroup(id=key, name=names[key], stop_ids=tuple(stop_ids))
            for key, stop_ids in members.items()
        ),
        key=lambda group: group.name,
    )


class _IndexGuard:
    """Readers-writer gate around the live :class:`StaticIndex`.

    Queries enter as readers and may overlap freely. The index swap in
    :meth:`TransitFeedHandle.refresh_static` enters as the writer: it
    parks new readers, waits for in-flight ones to drain, swaps and closes
    the old index, then releases. Only the swap itself blocks; the
    download and build that precede it run outside the guard. Readers cover
    index reads only, never the realtime HTTP fetch, so a swap waits on
    SQLite work, not on a producer's network latency. Writers never
    overlap because the only one runs under the refresh lock.

    Built on events rather than a condition so that no teardown path
    awaits: a cancelled reader or writer always restores the gate.
    """

    def __init__(self) -> None:
        self._readers = 0
        self._workers: set[asyncio.Future[Any]] = set()
        self._open = asyncio.Event()
        self._open.set()
        self._drained = asyncio.Event()
        self._drained.set()

    def _release_if_idle(self) -> None:
        if not self._readers and not self._workers:
            self._drained.set()

    @asynccontextmanager
    async def reader(self) -> AsyncIterator[None]:
        while not self._open.is_set():
            await self._open.wait()
        self._readers += 1
        self._drained.clear()
        try:
            yield
        finally:
            self._readers -= 1
            self._release_if_idle()

    def track(self, worker: asyncio.Future[Any]) -> None:
        """Hold the gate until an offloaded index read's thread finishes.

        Cancelling an ``asyncio.to_thread`` await abandons the future but
        cannot stop the worker, so the reader count alone would let a swap
        close the connection out from under a thread still reading it.
        """
        self._workers.add(worker)
        self._drained.clear()
        worker.add_done_callback(self._untrack)

    def _untrack(self, worker: asyncio.Future[Any]) -> None:
        self._workers.discard(worker)
        # An abandoned read's exception has no awaiter left to receive it.
        if not worker.cancelled():
            worker.exception()
        self._release_if_idle()

    @asynccontextmanager
    async def writer(self) -> AsyncIterator[None]:
        self._open.clear()
        try:
            await self._drained.wait()
            yield
        finally:
            self._open.set()


class TransitFeedHandle:
    """Snapshot access to one transit feed (GTFS + its GTFS-RT siblings).

    Create via :meth:`MobilityFeedsClient.get_transit_feed` — accepts a GTFS
    or GTFS-RT feed ID and resolves the sibling relationship through the
    catalog — or via :meth:`MobilityFeedsClient.get_transit_feed_from_urls`
    for user-supplied URLs with no catalog involved. Construction
    downloads/opens the static index.
    """

    def __init__(
        self,
        client: MobilityFeedsClient,
        static_feed: GtfsFeed | None,
        rt_feeds: list[GtfsRtFeed],
        index: StaticIndex,
        api_key: str | None,
        *,
        direct: _DirectUrls | None = None,
    ) -> None:
        """Construct via ``MobilityFeedsClient.get_transit_feed()`` instead."""
        self._client = client
        self._static_feed = static_feed
        self._direct = direct
        if direct is not None:
            self._feed_key = _direct_cache_key(direct.static_url)
            self._headers: Mapping[str, str] | None = direct.headers
        else:
            # Catalog-resolved feeds always carry IDs (the API keys on them).
            assert static_feed is not None and static_feed.id is not None
            self._feed_key = static_feed.id
            self._headers = None
        self._rt_feeds = tuple(rt_feeds)
        self._index = index
        self._api_key = api_key
        self._guard = _IndexGuard()
        self._refresh_lock = asyncio.Lock()
        # Per-url HTTP validators plus the parse they belong to. Only ever
        # reused on a 304, i.e. when the producer itself states the bytes
        # are unchanged -- this is revalidation, not a TTL cache.
        self._rt_validators: dict[str, tuple[FeedValidators, Any]] = {}
        self._stops: tuple[Stop, ...] = tuple(index.stops())
        self._routes: tuple[Route, ...] = tuple(index.routes())
        self._agencies: tuple[Agency, ...] = tuple(index.agencies())
        # None when the dataset ships no feed_info.txt (an optional file).
        self._feed_info: FeedInfo | None = index.feed_info()

    async def _index_read[T](
        self, fn: Callable[..., T], *args: object, **kwargs: object
    ) -> T:
        """Run one index read on a worker thread, tracked by the guard.

        Shielded so a cancelled caller returns at once while the guard keeps
        waiting for the thread; see :meth:`_IndexGuard.track`.
        """
        worker = asyncio.ensure_future(asyncio.to_thread(fn, *args, **kwargs))
        self._guard.track(worker)
        return await asyncio.shield(worker)

    @property
    def static_feed_id(self) -> str:
        """Catalog ID of the static feed, or the url-derived cache key.

        Direct-URL handles have no catalog identity, so they return the
        same ``url-<sha256(static_url)[:16]>`` key that names their cache
        directory — always safe to pass to
        :meth:`MobilityFeedsClient.purge_cache`.
        """
        return self._feed_key

    @property
    def static_dataset(self) -> LatestDataset | None:
        """Catalog metadata for the indexed dataset (None for direct handles).

        Includes downloaded_at, service date range, and hashes — for
        consumer diagnostics entities. Direct-URL handles have no catalog
        dataset record, so consumers must treat None as "no metadata
        available", not as "no data".
        """
        return None if self._static_feed is None else self._static_feed.latest_dataset

    @property
    def rt_feeds(self) -> tuple[GtfsRtFeed, ...]:
        """GTFS-RT sources this handle fetches (catalog records or synthesized)."""
        return self._rt_feeds

    @property
    def stops(self) -> tuple[Stop, ...]:
        """Every stop in the indexed dataset; replaced atomically on refresh."""
        return self._stops

    @property
    def routes(self) -> tuple[Route, ...]:
        """Every route in the indexed dataset; replaced atomically on refresh."""
        return self._routes

    @property
    def agencies(self) -> tuple[Agency, ...]:
        """Every agency in the indexed dataset; replaced atomically on refresh."""
        return self._agencies

    @property
    def feed_info(self) -> FeedInfo | None:
        """The dataset's feed_info.txt record, or None when it ships none."""
        return self._feed_info

    @classmethod
    async def create(
        cls,
        client: MobilityFeedsClient,
        feed_id: str,
        api_key: str | None,
        on_progress: Callable[[StaticBuildProgress], None] | None = None,
    ) -> TransitFeedHandle:
        """Resolve siblings via the catalog and ensure the static index."""
        catalog = client.catalog
        feed = await catalog.get_feed(feed_id)
        if feed.data_type is DataType.GTFS:
            static_feed = await catalog.get_gtfs_feed(feed_id)
            rt_feeds = await catalog.get_gtfs_feed_gtfs_rt_feeds(feed_id)
        elif feed.data_type is DataType.GTFS_RT:
            rt_self = await catalog.get_gtfs_rt_feed(feed_id)
            references = rt_self.feed_references or ()
            if not references:
                raise StaticDataUnavailableError(
                    f"GTFS-RT feed {feed_id} has no feed_references to a static feed"
                )
            static_feed = await catalog.get_gtfs_feed(references[0])
            rt_feeds = await catalog.get_gtfs_feed_gtfs_rt_feeds(references[0])
            if rt_self.id not in {rt.id for rt in rt_feeds}:
                rt_feeds.append(rt_self)
        else:
            raise ValueError(f"Feed {feed_id} is {feed.data_type}; use get_gbfs_feed()")
        index = await cls._ensure_index(client, static_feed, on_progress)
        return cls(client, static_feed, rt_feeds, index, api_key)

    @classmethod
    async def create_from_urls(
        cls,
        client: MobilityFeedsClient,
        static_url: str,
        *,
        trip_updates_urls: Sequence[str] | None = None,
        vehicle_positions_urls: Sequence[str] | None = None,
        service_alerts_urls: Sequence[str] | None = None,
        headers: Mapping[str, str] | None = None,
        on_progress: Callable[[StaticBuildProgress], None] | None = None,
    ) -> TransitFeedHandle:
        """Build a handle from user-supplied URLs, bypassing the catalog.

        RT URLs are declared per layer so each operation fetches only the
        sources that can serve it (and so consumers can see real
        trip-updates capability instead of an assumption). A URL listed
        under several layers — a combined feed — is deduplicated into ONE
        synthesized :class:`GtfsRtFeed` carrying the union of its declared
        entity types, so it is fetched once per operation, never twice.
        Each synthesized feed's id IS the url, with
        ``SourceInfo(producer_url=url, authentication_type=0)`` — the
        shared RT fetch/merge machinery runs unchanged. ``headers`` apply
        to the static download and every RT fetch made through this
        handle.
        """
        _require_http_url(static_url, "static GTFS dataset URL")
        url_types: dict[str, list[EntityType]] = {}
        for urls, entity_type in (
            (trip_updates_urls, EntityType.TRIP_UPDATES),
            (vehicle_positions_urls, EntityType.VEHICLE_POSITIONS),
            (service_alerts_urls, EntityType.SERVICE_ALERTS),
        ):
            for rt_url in urls or []:
                _require_http_url(rt_url, "GTFS-RT producer URL")
                types = url_types.setdefault(rt_url, [])
                if entity_type not in types:
                    types.append(entity_type)
        rt_feeds = [
            GtfsRtFeed(
                id=rt_url,
                data_type=DataType.GTFS_RT,
                entity_types=tuple(types),
                source_info=SourceInfo(producer_url=rt_url, authentication_type=0),
            )
            for rt_url, types in url_types.items()
        ]
        direct = _DirectUrls(static_url=static_url, headers=headers)
        source = _StaticSource(
            url=static_url,
            cache_key=_direct_cache_key(static_url),
            dataset_id=await cls._probe_direct_dataset_id(client, direct),
            timezone_name=None,  # always sourced from agency.txt at build
            headers=headers,
        )
        index = await cls._ensure_index_from_source(client, source, on_progress)
        return cls(client, None, rt_feeds, index, None, direct=direct)

    @staticmethod
    async def _probe_direct_dataset_id(
        client: MobilityFeedsClient, direct: _DirectUrls
    ) -> str | None:
        """HEAD the static URL and derive a dataset id from HTTP validators.

        Catalog datasets carry a stable dataset id for cache comparisons; a
        bare URL doesn't, so ETag (preferred: content-derived) or
        Last-Modified stand in, cheaply checked without downloading.
        Returns None when the server offers neither validator or rejects
        HEAD outright (e.g. 405) — callers then fall back to hashing the
        downloaded bytes. Connection failures raise instead of silently
        degrading to the hash path: the GET would fail identically, so
        failing here is both earlier and cheaper.
        """
        session = client._get_session()  # deliberate friend access
        try:
            async with session.head(
                direct.static_url,
                headers=dict(direct.headers) if direct.headers else None,
                allow_redirects=True,  # match GET semantics: probe the final URL
                timeout=aiohttp.ClientTimeout(total=client.timeout_seconds),
            ) as resp:
                if resp.status < HTTPStatus.BAD_REQUEST:
                    if etag := resp.headers.get("ETag"):
                        return f"etag:{etag}"
                    if last_modified := resp.headers.get("Last-Modified"):
                        return f"lastmod:{last_modified}"
        except (TimeoutError, aiohttp.ClientError) as err:
            raise SourceConnectionError(
                f"Error probing {direct.static_url}: {err}"
            ) from err
        return None

    @classmethod
    async def _ensure_index(
        cls,
        client: MobilityFeedsClient,
        static_feed: GtfsFeed,
        on_progress: Callable[[StaticBuildProgress], None] | None = None,
    ) -> StaticIndex:
        """Catalog strategy: translate latest-dataset metadata to a source."""
        dataset = static_feed.latest_dataset
        if dataset is None or not dataset.id or not dataset.hosted_url:
            raise StaticDataUnavailableError(
                f"Feed {static_feed.id} has no hosted static dataset"
            )
        _require_http_url(dataset.hosted_url, "hosted GTFS dataset URL")
        source = _StaticSource(
            url=dataset.hosted_url,
            cache_key=str(static_feed.id),
            dataset_id=dataset.id,
            timezone_name=dataset.agency_timezone,
            headers=None,
        )
        return await cls._ensure_index_from_source(client, source, on_progress)

    @classmethod
    async def _ensure_index_from_source(
        cls,
        client: MobilityFeedsClient,
        source: _StaticSource,
        on_progress: Callable[[StaticBuildProgress], None] | None = None,
    ) -> StaticIndex:
        """Shared download+build core behind both acquisition strategies.

        With a known ``dataset_id`` the cache is consulted before any
        network I/O; a None id (direct URL without HTTP validators) forces
        the download first, then retries the cache with the computed
        ``sha256:`` id — an unchanged validator-less dataset still avoids
        an index rebuild, just not the transfer.
        """
        db_path: Path | None = None
        if client.cache_dir is not None:
            feed_dir = client.cache_dir / source.cache_key
            # The catalog cache_key is catalog DATA (the JSON response
            # body's feed id), not a caller-supplied parameter -- same
            # traversal class as purge_cache's feed_id, so it gets the
            # identical resolve()+is_relative_to() containment check, but
            # raises FeedParseError (a data problem) rather than ValueError
            # (a caller problem). Must run BEFORE mkdir: mkdir(parents=True)
            # would otherwise silently create the directory outside
            # cache_dir first. (Direct-URL keys are generated fixed-length
            # hashes and can never escape, but they flow through the same
            # guard anyway.)
            if not feed_dir.resolve().is_relative_to(client.cache_dir.resolve()):
                raise FeedParseError(
                    f"Feed id {source.cache_key!r} escapes the cache directory"
                )
            await asyncio.to_thread(feed_dir.mkdir, parents=True, exist_ok=True)
            db_path = feed_dir / STATIC_DB_FILENAME
            if source.dataset_id is not None:
                cached = await asyncio.to_thread(
                    StaticIndex.open_cached, db_path, source.dataset_id
                )
                if cached is not None:
                    return cached
        tmp_dir = await asyncio.to_thread(tempfile.mkdtemp)
        try:
            zip_path = Path(tmp_dir) / "dataset.zip"
            await cls._download_zip(client, source, zip_path, on_progress)
            dataset_id = source.dataset_id
            if dataset_id is None:
                sha = await asyncio.to_thread(_sha256_of_file, zip_path)
                dataset_id = f"sha256:{sha}"
                if db_path is not None:
                    cached = await asyncio.to_thread(
                        StaticIndex.open_cached, db_path, dataset_id
                    )
                    if cached is not None:
                        return cached
            return await cls._build_index(
                zip_path, db_path, dataset_id, source.timezone_name, on_progress
            )
        finally:
            await asyncio.to_thread(shutil.rmtree, tmp_dir, ignore_errors=True)

    @staticmethod
    async def _download_zip(
        client: MobilityFeedsClient,
        source: _StaticSource,
        zip_path: Path,
        on_progress: Callable[[StaticBuildProgress], None] | None,
    ) -> None:
        """Stream the dataset zip to disk, emitting download progress."""
        session = client._get_session()  # deliberate friend access
        try:
            async with session.get(
                source.url,
                headers=dict(source.headers) if source.headers else None,
                timeout=aiohttp.ClientTimeout(
                    total=None, sock_read=client.timeout_seconds
                ),
            ) as resp:
                if resp.status >= HTTPStatus.BAD_REQUEST:
                    raise SourceConnectionError(
                        f"Dataset fetch failed ({resp.status}) for {source.url}",
                        status=resp.status,
                    )
                total_bytes = resp.content_length
                done_bytes = 0
                last_emitted = 0
                first_chunk = True
                # File writes stay off the event loop: consumers (for
                # example Home Assistant) run this on their loop and a
                # large dataset means thousands of 64 KiB writes.
                fp = await asyncio.to_thread(zip_path.open, "wb")
                try:
                    async for chunk in resp.content.iter_chunked(1 << 16):
                        await asyncio.to_thread(fp.write, chunk)
                        done_bytes += len(chunk)
                        if on_progress is not None and (
                            first_chunk
                            or done_bytes - last_emitted >= _PROGRESS_CHUNK_BYTES
                        ):
                            on_progress(
                                StaticBuildProgress(
                                    phase="download",
                                    done_bytes=done_bytes,
                                    total_bytes=total_bytes,
                                )
                            )
                            last_emitted = done_bytes
                            first_chunk = False
                finally:
                    await asyncio.to_thread(fp.close)
                if on_progress is not None:
                    on_progress(
                        StaticBuildProgress(
                            phase="download",
                            done_bytes=done_bytes,
                            total_bytes=total_bytes,
                        )
                    )
        except (TimeoutError, aiohttp.ClientError) as err:
            raise SourceConnectionError(f"Error downloading dataset: {err}") from err

    @staticmethod
    async def _build_index(
        zip_path: Path,
        db_path: Path | None,
        dataset_id: str,
        timezone_name: str | None,
        on_progress: Callable[[StaticBuildProgress], None] | None,
    ) -> StaticIndex:
        """Build the SQLite index in a worker thread, marshaling progress."""
        build_progress: Callable[[int, int | None], None] | None = None
        if on_progress is not None:
            loop = asyncio.get_running_loop()

            def build_progress(done: int, total: int | None) -> None:
                loop.call_soon_threadsafe(
                    on_progress,
                    StaticBuildProgress(
                        phase="index", done_bytes=done, total_bytes=total
                    ),
                )

        return await asyncio.to_thread(
            StaticIndex.build,
            zip_path,
            str(db_path) if db_path is not None else ":memory:",
            dataset_id,
            timezone_name,
            build_progress,
        )

    def _rt_feeds_for(self, entity_type: EntityType) -> list[GtfsRtFeed]:
        return [
            feed
            for feed in self.rt_feeds
            if feed.entity_types and entity_type in feed.entity_types
        ]

    async def _fetch_entity_messages(
        self,
        entity_type: EntityType,
        memo: dict[str, gtfs_realtime_pb2.FeedMessage] | None = None,
    ) -> list[gtfs_realtime_pb2.FeedMessage]:
        """Fetch and parse every feed declaring ``entity_type``.

        One GTFS-RT FeedMessage can carry TripUpdates, VehiclePositions and
        Alerts together, and a feed declares every type it serves -- so a
        single producer_url is selected once per declared type. ``memo``
        (a per-call dict keyed by url) makes a call that needs two entity
        types off one bundled feed fetch it once; every consumer already
        filters the parsed message for its own field. Across calls the url
        is revalidated with its stored ETag/Last-Modified, so an unchanged
        feed costs a 304 rather than a re-download and re-parse.
        """
        session = self._client._get_session()
        messages = []
        for feed in self._rt_feeds_for(entity_type):
            source = feed.source_info
            if source is None or not source.producer_url:
                continue
            url = source.producer_url
            # A memo hit means THIS call already downloaded this url, so
            # even a conditional request would spend a round trip to learn
            # nothing.
            if memo is not None and (memoed := memo.get(url)) is not None:
                messages.append(memoed)
                continue
            known = self._rt_validators.get(url)
            message, fresh = await fetch_feed_message(
                session,
                url,
                auth_type=source.authentication_type,
                api_key_name=source.api_key_parameter_name,
                api_key=self._api_key,
                headers=self._headers,
                timeout_seconds=self._client.timeout_seconds,
                validators=known[0] if known else None,
            )
            if message is None:
                # 304: the producer confirmed our copy is current. It still
                # goes in the memo -- a second entity type off this same
                # url must not re-ask within the one call.
                if known is None:
                    # Unsolicited: nothing was revalidated, so there is no
                    # previous parse to reuse. A misbehaving proxy must
                    # raise the library's own error, not AssertionError --
                    # which `python -O` would strip into a TypeError.
                    raise SourceConnectionError(
                        f"Producer answered 304 to an unconditional request: {url}",
                        status=int(HTTPStatus.NOT_MODIFIED),
                    )
                message = known[1]
            elif fresh is not None:
                self._rt_validators[url] = (fresh, message)
            else:
                # A producer that stops offering validators must not leave a
                # stale entry behind to be revalidated against forever.
                self._rt_validators.pop(url, None)
            if memo is not None:
                memo[url] = message
            messages.append(message)
        return messages

    async def _aggregated_trip_updates(
        self, memo: dict[str, gtfs_realtime_pb2.FeedMessage] | None = None
    ) -> TripUpdates:
        """Merge TripUpdates across every TU-capable sibling RT source.

        Deliberate tiebreak: when multiple TU-capable sibling feeds report
        the same (trip_id, start_date, start_secs) identity, the last feed
        in catalog order wins wholesale (no freshness reconciliation in
        v1).
        """
        aggregate = TripUpdates()
        for message in await self._fetch_entity_messages(EntityType.TRIP_UPDATES, memo):
            updates = trip_updates_from_message(message)
            aggregate.trips.update(updates.trips)
            aggregate.canceled_trips |= updates.canceled_trips
            aggregate.added.extend(updates.added)
            if updates.feed_time is not None and (
                aggregate.feed_time is None or updates.feed_time > aggregate.feed_time
            ):
                aggregate.feed_time = updates.feed_time
        return aggregate

    async def _resolve_predictions(
        self,
        updates: TripUpdates,
        rt_keys: Mapping[_TripInstance, TripUpdateKey],
        memo: _PredictionMemo,
    ) -> dict[_TripInstance, TripPredictions]:
        """Resolve STU propagation for every matched scheduled trip instance.

        ``rt_keys`` maps each matched instance -- CONCRETE trip id
        (synthetic repetition ids included) plus service day -- to the
        ``updates.trips`` key that addresses it, as resolved per row by
        :func:`_rt_key_for_row` (callers pass matched instances only).
        Instances are the unit here, not bare trip ids, because a window
        of 24h or more holds several service days' rows for one concrete
        trip id and a dated update must reach exactly one of them. Each
        resolves via :func:`resolve_trip_predictions` against its trip's
        static stop order, which is service-day independent -- so a
        resolution depends only on (concrete trip, key), and ``memo``
        keeps it for the rest of the call's horizon passes. Every matched
        trip is guaranteed a stop-calls entry: scheduled rows only exist
        because the trip has stop_times rows.
        """
        missing = sorted(
            {
                trip_id
                for (trip_id, _), key in rt_keys.items()
                if (trip_id, key) not in memo.resolved
            }
            - memo.stop_calls.keys()
        )
        if missing:
            memo.stop_calls.update(
                await self._index_read(self._index.trip_stop_calls, missing)
            )
        resolved: dict[_TripInstance, TripPredictions] = {}
        for (trip_id, service_date), key in rt_keys.items():
            found = memo.resolved.get((trip_id, key))
            if found is None:
                found = memo.resolved[(trip_id, key)] = resolve_trip_predictions(
                    updates.trips[key], memo.stop_calls[trip_id]
                )
            resolved[(trip_id, service_date)] = found
        return resolved

    async def get_arrivals(
        self,
        queries: Sequence[ArrivalsQuery],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        grace: timedelta = timedelta(hours=1),
        with_vehicles: bool = False,
    ) -> list[list[StopArrival]]:
        """Upcoming arrivals for each query: schedule merged with RT.

        One TripUpdates fetch, one scheduled read over the union of every
        query's stops, one merge; then each query's route and headsign
        filters and ``limit`` apply to the merged rows, so a filter can
        never see an empty board because other routes crowded out the
        limit. ``limit`` caps each query's merged result as a whole, not
        per stop. RT-added rows carry no headsign and so never pass a
        headsign filter; a row that announces neither arrival nor
        departure keeps every time field ``None`` and sorts at ``start``.
        Returns one list per query, in order.

        The window opens at ``start`` (default: now) and closes at ``end``
        for SCHEDULED departures. A delayed trip scheduled inside the window
        can be predicted after ``end``, and RT-added rows have no schedule
        at all; both are kept, as before. With ``end=None`` the window is
        open-ended: each board fills to its ``limit`` however far ahead the
        departures are -- overnight, over a weekend, past a holiday.

        Realtime is fetched once. The runs it can move -- those a
        TripUpdate addresses, and those a TripModifications entity rebuilds
        on the ``service_dates`` the spec requires it to name -- are then
        read whole, with the window applied to their scheduled times. Every
        other row sorts at its scheduled time, so the rest of the window is
        read in growing slices (2h, then doubling), each pass reading only
        the new slice, only for boards still filling, with route/headsign
        filters applied in SQL, and never past the last instant any
        scheduled departure matching that query can occur (none: one
        pass). A limited board is final once it is full with its last row
        inside the slice read so far: nothing unread can sort ahead of it.
        The answer is exactly that of reading the whole window at once.
        Open-ended queries need a limit: ``end=None`` with any
        ``limit=None`` query raises ``ValueError``. ``end < start`` returns
        empty boards.

        ``grace`` reaches that far BEFORE ``start`` when collecting
        scheduled candidates, so a trip whose scheduled time has passed
        still picks up its realtime prediction. After the merge, rows whose
        effective departure (predicted if present, else scheduled) is before
        ``start`` are dropped: a schedule-only row past its time is gone, a
        delayed one stays until its prediction passes.

        Delay propagation (GTFS-RT spec): a StopTimeUpdate's delay applies
        to its own stop AND propagates to every subsequent stop of the
        trip until the next StopTimeUpdate provides newer information; a
        NO_DATA StopTimeUpdate cuts propagation (subsequent stops are
        schedule-only until a later update resumes); a SKIPPED
        StopTimeUpdate suppresses its stop's row entirely (the vehicle
        will not serve it); the trip-level ``TripUpdate.delay`` applies
        only where no StopTimeUpdate-derived information covers a stop.
        ``realtime`` is True for propagated-only and trip-delay-only stops
        too — a propagated delay IS realtime information — with
        ``predicted_*`` computed as scheduled + delay wherever the stop's
        own update supplies no explicit epoch time (explicit times always
        win). RT-ADDED trips get no propagation: with no static schedule
        to propagate over, only their explicit StopTimeUpdates surface.

        Frequency-based repetitions (synthetic ``{trip_id}#{start_secs}``
        ids materialized from frequencies.txt) match RT via
        ``TripDescriptor.start_time``: a prediction or cancellation whose
        (trip_id, start_time) equals a repetition's (template id, start)
        applies to exactly that repetition. The matching is deliberately
        strict in both directions: a prediction/cancellation WITHOUT
        start_time never applies to repetitions (which repetition was meant
        is unknowable, and guessing — or applying it to all of them — would
        be wrong more often than not), a start_time matching no
        materialized repetition applies to nothing, and plain
        (non-frequency) rows only match predictions WITHOUT start_time
        (a producer redundantly sending start_time for a regular trip does
        not match).

        ``TripDescriptor.start_date`` picks the SERVICE DAY instance the
        start_time rules then apply within (see :func:`_rt_key_for_row`):
        a dated prediction or cancellation affects only the instance whose
        service day it names — an early-posted "trip X canceled tomorrow"
        never cancels today's departure — while a date-less one affects
        only the trip's current run, judged at the feed's own timestamp
        (see :meth:`StaticIndex.current_service_dates`), so a board for
        tomorrow morning never moves today's update onto tomorrow's run.

        ``with_vehicles`` attaches each row's live
        :class:`VehiclePosition` when one can be pinned down -- off by
        default because vehicle positions are a SEPARATE network fetch,
        and a board that shows no vehicles should not pay for one. A
        producer serving both entity types off one url is fetched once
        either way, so there the flag costs no extra request.
        """
        _require_bound(end, [query.limit for query in queries])
        start, end = _utc_window(start, end)
        all_stop_ids = sorted(
            {stop_id for query in queries for stop_id in query.stop_ids}
        )
        if not all_stop_ids or (end is not None and end < start):
            return [[] for _ in queries]
        # One memo per call: a bundled feed serving both TripUpdates and
        # VehiclePositions is downloaded and parsed once, not once per type.
        rt_memo: dict[str, gtfs_realtime_pb2.FeedMessage] = {}
        updates = await self._aggregated_trip_updates(rt_memo)
        vehicle_messages = (
            await self._fetch_entity_messages(EntityType.VEHICLE_POSITIONS, rt_memo)
            if with_vehicles
            else []
        )
        # Sourced from the TU-declaring feeds, NOT from whatever the memo
        # happens to hold: reading the memo made a vp-declaring feed that
        # also bundles trip_modifications contribute detours only when
        # with_vehicles was set, so a display flag changed the schedule.
        # These are all memo hits, so this costs no request.
        tu_messages = await self._fetch_entity_messages(
            EntityType.TRIP_UPDATES, rt_memo
        )
        rt_stops: dict[str, Stop] = {}
        modifications: list[TripModifications] = []
        for message in tu_messages:
            rt_stops.update(stops_from_message(message))
            modifications.extend(trip_modifications_from_message(message))
        async with self._guard.reader():
            stops_by_id = await self._index_read(self._index.stops_by_id)
            routes_by_id = await self._index_read(self._index.routes_by_id)
            vehicles = _vehicle_index(
                await self._vehicles_from(vehicle_messages, stops_by_id, routes_by_id)
            )
            current_at = _realtime_now(updates, start) - grace
            # Realtime moves exactly the runs it names: those a TripUpdate
            # addresses and those a detour rebuilds. Both are read whole,
            # here, with the window applied to their scheduled times -- so
            # every row realtime can move is present from the first pass,
            # and the scan below only adds rows that sort at their
            # scheduled time.
            batch_stops, batch_routes, batch_headsigns = _pushdown(queries)
            addressed = [
                dep
                for dep in await self._index_read(
                    self._index.instance_departures,
                    await self._addressed_runs(updates, current_at),
                    batch_stops,
                    batch_routes,
                    batch_headsigns,
                )
                if dep.departure >= start - grace
                and (end is None or dep.departure <= end)
            ]
            modified_rows, modified_instances = await self._apply_trip_modifications(
                modifications,
                set(all_stop_ids),
                end=end,
                rt_stops=rt_stops,
                stops_by_id=stops_by_id,
                routes_by_id=routes_by_id,
            )
            # Per query, the last instant a matching scheduled departure can
            # occur (None: never) bounds how far the scan reads.
            ceilings = [
                await self._index_read(self._index.departures_ceiling, *_filters(q))
                for q in queries
            ]
            ceiling = max((c for c in ceilings if c is not None), default=None)
            memo = _PredictionMemo()
            boards: list[list[StopArrival] | None] = [None] * len(queries)
            scheduled: dict[_RowKey, ScheduledDeparture] = {
                _departure_key(dep): dep for dep in addressed
            }
            read_from = start - grace
            for horizon, final in _horizons(start, end, ceiling):
                pending = [i for i, board in enumerate(boards) if board is None]
                # Read only the new slice, only for boards still filling.
                stops, routes, headsigns = _pushdown([queries[i] for i in pending])
                if horizon >= read_from:
                    for dep in await self._index_read(
                        self._index.upcoming_departures,
                        stops,
                        routes,
                        read_from,
                        horizon - read_from,
                        None,
                        headsigns=headsigns,
                    ):
                        scheduled.setdefault(_departure_key(dep), dep)
                read_from = horizon
                arrivals = await self._merged_arrivals(
                    list(scheduled.values()),
                    all_stop_ids,
                    start=start,
                    current_at=current_at,
                    updates=updates,
                    modified_rows=modified_rows,
                    modified_instances=modified_instances,
                    stops_by_id=stops_by_id,
                    routes_by_id=routes_by_id,
                    vehicles=vehicles,
                    memo=memo,
                )
                for i in pending:
                    board = _select_arrivals(arrivals, queries[i])
                    if (
                        final
                        or _past_ceiling(ceilings[i], horizon)
                        or _board_settles(
                            [_effective_departure(row, start) for row in board],
                            queries[i].limit,
                            horizon,
                        )
                    ):
                        boards[i] = board
                if all(board is not None for board in boards):
                    break
            return [board or [] for board in boards]

    async def _addressed_runs(
        self, updates: TripUpdates, current_at: datetime
    ) -> list[_TripInstance]:
        """Return the runs TripUpdates address, resolved as the merge does.

        Dated keys name their day; date-less ones the current run as of
        ``current_at``. These are the only scheduled runs realtime can move
        (detours aside), so they are read whole before any scan. Caller
        holds the reader guard.
        """
        return sorted(
            {
                instance
                for instance, _key in await self._index_read(
                    self._index.rt_instances, list(updates.trips), current_at
                )
            }
        )

    async def _current_runs(
        self,
        rows: Sequence[tuple[str, str, int | None]],
        updates: TripUpdates,
        vehicles: _Vehicles,
        at: datetime,
    ) -> dict[_RtIdentity, date]:
        """Current-run service days for the identities that need one.

        ``rows`` are (concrete trip id, source trip id, start secs). Only
        identities some date-less TripUpdate, cancellation or vehicle could
        address are looked up (see :meth:`StaticIndex.current_service_dates`);
        caller holds the reader guard.
        """
        dateless_keys = {
            (key[0], key[2])
            for key in (*updates.trips, *updates.canceled_trips)
            if key[1] is None
        }
        wanted = {
            trip_id: (source, start)
            for trip_id, source, start in rows
            if (source, start) in dateless_keys or source in vehicles.dateless_trip_ids
        }
        if not wanted:
            return {}
        dates = await self._index_read(
            self._index.current_service_dates, sorted(wanted), at
        )
        return {
            wanted[trip_id]: service_date for trip_id, service_date in dates.items()
        }

    async def _merged_arrivals(
        self,
        scheduled: list[ScheduledDeparture],
        all_stop_ids: list[str],
        *,
        start: datetime,
        current_at: datetime,
        updates: TripUpdates,
        modified_rows: Sequence[StopArrival],
        modified_instances: Collection[tuple[str, date]],
        stops_by_id: Mapping[str, Stop],
        routes_by_id: Mapping[str, Route],
        vehicles: _Vehicles,
        memo: _PredictionMemo,
    ) -> list[StopArrival]:
        """Merge one horizon's board over ``all_stop_ids``, sorted.

        ``scheduled`` is every scheduled row read so far. Adds the detoured
        runs' rebuilt rows (``modified_rows``, replacing those runs'
        schedule), the realtime overlay and RT-added rows; rows whose
        effective departure is before ``start`` are dropped. Caller holds
        the reader guard and applies per-query filters and limits. Runs
        once per horizon of :meth:`get_arrivals`, with RT already fetched.
        """
        canceled = updates.canceled_trips
        added_rows = updates.added
        # The unmodified schedule for these instances describes a route the
        # vehicle is no longer taking. Keyed by INSTANCE, not by trip: a
        # detour dated yesterday must not delete today's run of the trip.
        scheduled = [
            dep
            for dep in scheduled
            if (dep.trip_id, dep.service_date) not in modified_instances
        ]
        # Per-row RT matching: each scheduled row's (identity, service day)
        # resolves to at most one TripUpdates key via _rt_key_for_row —
        # dated keys hit exactly their service day's run, date-less keys
        # only the trip's current run.
        current_dates = await self._current_runs(
            [(dep.trip_id, dep.source_trip_id, dep.start_secs) for dep in scheduled],
            updates,
            vehicles,
            current_at,
        )
        resolved = await self._resolve_predictions(
            updates,
            {
                (dep.trip_id, dep.service_date): key
                for dep in scheduled
                if (
                    key := _rt_key_for_row(
                        (dep.source_trip_id, dep.start_secs),
                        dep.service_date,
                        updates.trips,
                        current_dates,
                    )
                )
                is not None
            },
            memo,
        )

        arrivals: list[StopArrival] = []
        for dep in scheduled:
            if (
                _rt_key_for_row(
                    (dep.source_trip_id, dep.start_secs),
                    dep.service_date,
                    canceled,
                    current_dates,
                )
                is not None
            ):
                continue
            trip_rt = resolved.get((dep.trip_id, dep.service_date))
            prediction = None
            if trip_rt is not None:
                if dep.stop_sequence in trip_rt.skipped:
                    continue  # SKIPPED: the vehicle will not serve this stop
                prediction = trip_rt.predictions.get(dep.stop_sequence)
            arrivals.append(
                StopArrival(
                    stop_id=dep.stop_id,
                    stop=stops_by_id.get(dep.stop_id),
                    route_id=dep.route_id,
                    route=routes_by_id.get(dep.route_id),
                    trip_id=dep.trip_id,
                    service_id=dep.service_id,
                    headsign=dep.headsign,
                    scheduled_arrival=dep.arrival,
                    scheduled_departure=dep.departure,
                    predicted_arrival=(
                        _predicted_time(
                            prediction.arrival,
                            dep.arrival,
                            prediction.delay_seconds,
                        )
                        if prediction
                        else None
                    ),
                    predicted_departure=(
                        _predicted_time(
                            prediction.departure,
                            dep.departure,
                            prediction.delay_seconds,
                        )
                        if prediction
                        else None
                    ),
                    delay_seconds=prediction.delay_seconds if prediction else None,
                    realtime=prediction is not None,
                    vehicle_id=prediction.vehicle_id if prediction else None,
                    vehicle=_match_vehicle(
                        prediction.vehicle_id if prediction else None,
                        dep.source_trip_id,
                        dep.start_secs,
                        service_date=dep.service_date,
                        current_dates=current_dates,
                        vehicles=vehicles,
                    ),
                    wheelchair_accessible=dep.wheelchair_accessible,
                    bikes_allowed=dep.bikes_allowed,
                    direction_id=dep.direction_id,
                    pickup_type=dep.pickup_type,
                    drop_off_type=dep.drop_off_type,
                    timepoint_exact=dep.timepoint_exact,
                    stop_headsign=dep.stop_headsign,
                    trip_short_name=dep.trip_short_name,
                    block_id=dep.block_id,
                )
            )
        wanted_stops = set(all_stop_ids)
        for row in added_rows:
            if row.stop_id not in wanted_stops:
                continue
            arrivals.append(
                StopArrival(
                    stop_id=row.stop_id,
                    stop=stops_by_id.get(row.stop_id),
                    route_id=row.route_id,
                    route=routes_by_id.get(row.route_id) if row.route_id else None,
                    trip_id=row.trip_id,
                    service_id=None,
                    vehicle=_match_vehicle(
                        row.vehicle_id,
                        row.trip_id,
                        None,
                        service_date=row.start_date,
                        current_dates={},
                        vehicles=vehicles,
                        scheduled=False,
                    ),
                    headsign=None,
                    scheduled_arrival=None,
                    scheduled_departure=None,
                    predicted_arrival=row.arrival,
                    predicted_departure=row.departure,
                    delay_seconds=None,
                    realtime=True,
                    vehicle_id=row.vehicle_id,
                    # RT-added trips have no static schedule row, so every
                    # descriptive field is unknown — including timepoint,
                    # whose absent-means-exact default only applies to rows
                    # that exist in stop_times.
                    wheelchair_accessible=None,
                    bikes_allowed=None,
                    direction_id=None,
                    pickup_type=None,
                    drop_off_type=None,
                    timepoint_exact=None,
                    stop_headsign=None,
                    trip_short_name=None,
                    block_id=None,
                )
            )
        arrivals.extend(modified_rows)
        # The grace window admitted scheduled rows before `now` so their
        # predictions could attach; now only rows still ahead survive.
        arrivals = [
            arrival
            for arrival in arrivals
            if _effective_departure(arrival, start) >= start
        ]
        # Total sort key: effective time alone ties frequently, so trip_id
        # and stop_id break ties -- and the scheduled times after them, since
        # the same trip can reach the same stop at the same effective
        # instant on two service days (tomorrow's run predicted early).
        # Without them the order would depend on the order rows were read.
        arrivals.sort(
            key=lambda a: (
                _effective_departure(a, start),
                a.trip_id or "",
                a.stop_id,
                a.scheduled_departure or start,
                a.scheduled_arrival or start,
            )
        )
        return arrivals

    async def get_upcoming_trips(
        self,
        queries: Sequence[TripQuery],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        grace: timedelta = timedelta(hours=1),
        with_vehicles: bool = False,
    ) -> list[list[UpcomingTrip]]:
        """Upcoming departures from the origin on trips serving the destination.

        The parity query for Home Assistant's legacy ``gtfs`` integration:
        its sensor tracks "the next vehicle leaving stop A that will reach
        stop B", not merely the next departure at A. Wrong-direction trips
        are excluded — a return trip serves both stops too, but in reverse
        order. Each row also carries the legacy sensor's descriptive
        surface: trip wheelchair/bikes/direction flags, both ends'
        stop_time descriptors, and ``is_first``/``is_last`` — whether the
        departure is the first/last OF ITS SERVICE DAY for this stop pair
        (see :class:`~.models.UpcomingTrip`).

        Takes a batch of :class:`TripQuery` and returns one list per
        query, in order, from ONE realtime fetch -- several sensors polling
        different stop pairs cost one TripUpdates download, not one each.
        ``start``/``end``/``grace`` follow :meth:`get_arrivals` exactly,
        including the open-ended default: with ``end=None`` each query's
        result fills to its ``limit`` however far ahead the next journeys
        are, and ``end=None`` with a ``limit=None`` query raises
        ``ValueError``. Each query's ``limit`` caps its merged result,
        nearest effective origin departure first. RT cancellations — and
        SKIPPED stops: a skipped origin or skipped destination kills the
        row (the rider cannot board or alight there), while a skipped
        intermediate stop changes nothing — are removed before the limit
        applies. RT-added trips (schedule_relationship ADDED) are never
        included: an added trip's full stop sequence is unknown, so
        whether it serves the destination after the origin cannot be
        determined.

        Delay propagation follows :meth:`get_arrivals` exactly: each end's
        prediction comes from its own StopTimeUpdate, a propagated
        last-known delay, or the trip-level fallback — so one early-stop
        update predicts both ends. ``realtime`` is True whenever either
        end carries any RT-derived prediction, propagated ones included;
        ``delay_seconds`` reports the origin's effective delay.

        Frequency-based repetitions (synthetic ``{trip_id}#{start_secs}``
        ids) match RT via ``TripDescriptor.start_time`` exactly as in
        :meth:`get_arrivals`: aligned start_time applies to that one
        repetition; missing or unmatched start_time applies to no
        repetition, and plain trips only match start_time-less updates.
        ``TripDescriptor.start_date`` service-day matching also follows
        :meth:`get_arrivals` exactly: a dated update or cancellation
        affects only its named service day's instance, a date-less one
        only the trip's current run as of the feed's timestamp (see
        :func:`_rt_key_for_row`).

        ``with_vehicles`` attaches the live :class:`VehiclePosition` running
        each journey, exactly as :meth:`get_arrivals` does; ``vehicle_id``
        is filled from the realtime prediction either way.
        """
        _require_bound(end, [query.limit for query in queries])
        start, end = _utc_window(start, end)
        if not queries or (end is not None and end < start):
            return [[] for _ in queries]
        rt_memo: dict[str, gtfs_realtime_pb2.FeedMessage] = {}
        updates = await self._aggregated_trip_updates(rt_memo)
        vehicle_messages = (
            await self._fetch_entity_messages(EntityType.VEHICLE_POSITIONS, rt_memo)
            if with_vehicles
            else []
        )
        async with self._guard.reader():
            routes_by_id = await self._index_read(self._index.routes_by_id)
            stops_by_id = await self._index_read(self._index.stops_by_id)
            vehicles = _vehicle_index(
                await self._vehicles_from(vehicle_messages, stops_by_id, routes_by_id)
            )
            current_at = _realtime_now(updates, start) - grace
            # As in get_arrivals: the runs realtime addresses are read whole
            # up front, so the scan only adds rows at their scheduled time.
            addressed = await self._addressed_runs(updates, current_at)
            memo = _PredictionMemo()
            results: list[list[UpcomingTrip]] = []
            for query in queries:
                rows: list[UpcomingTrip] = []
                # The last origin departure any journey between the pair can
                # have; None means no journey can ever occur (RT-added trips
                # are never journeys: their stop order is unknown).
                ceiling = await self._index_read(
                    self._index.journeys_ceiling,
                    query.origin_stop_id,
                    query.destination_stop_id,
                )
                if ceiling is None:
                    results.append([])
                    continue
                scheduled: dict[_RowKey, ScheduledTrip] = {
                    _journey_key(trip): trip
                    for trip in await self._index_read(
                        self._index.instance_trips,
                        addressed,
                        query.origin_stop_id,
                        query.destination_stop_id,
                    )
                    if trip.departure >= start - grace
                    and (end is None or trip.departure <= end)
                }
                read_from = start - grace
                for horizon, final in _horizons(start, end, ceiling):
                    # Read only the new slice of origin departures.
                    if horizon >= read_from:
                        for trip in await self._index_read(
                            self._index.upcoming_trips,
                            query.origin_stop_id,
                            query.destination_stop_id,
                            read_from,
                            horizon - read_from,
                            None,
                        ):
                            scheduled.setdefault(_journey_key(trip), trip)
                    read_from = horizon
                    rows = (
                        await self._merged_trips(
                            list(scheduled.values()),
                            start=start,
                            current_at=current_at,
                            updates=updates,
                            stops_by_id=stops_by_id,
                            routes_by_id=routes_by_id,
                            vehicles=vehicles,
                            memo=memo,
                        )
                    )[: query.limit]
                    if final or _board_settles(
                        [_effective_trip_departure(row) for row in rows],
                        query.limit,
                        horizon,
                    ):
                        break
                results.append(rows)
            return results

    async def _merged_trips(
        self,
        scheduled: list[ScheduledTrip],
        *,
        start: datetime,
        current_at: datetime,
        updates: TripUpdates,
        stops_by_id: Mapping[str, Stop],
        routes_by_id: Mapping[str, Route],
        vehicles: _Vehicles,
        memo: _PredictionMemo,
    ) -> list[UpcomingTrip]:
        """Merge one horizon's journeys for one stop pair, sorted, unlimited.

        ``scheduled`` is every scheduled journey read so far, with its
        origin departure in ``[start - grace, end]``. Adds the realtime
        overlay, removes cancellations and skips, and drops rows whose
        effective origin departure is before ``start``. Caller holds the
        reader guard and applies the query's limit.
        """
        # Same per-run RT matching as get_arrivals (_rt_key_for_row): dated
        # keys hit their service day's run, date-less keys only the current.
        current_dates = await self._current_runs(
            [
                (trip.trip_id, trip.source_trip_id, trip.start_secs)
                for trip in scheduled
            ],
            updates,
            vehicles,
            current_at,
        )
        resolved = await self._resolve_predictions(
            updates,
            {
                (trip.trip_id, trip.service_date): key
                for trip in scheduled
                if (
                    key := _rt_key_for_row(
                        (trip.source_trip_id, trip.start_secs),
                        trip.service_date,
                        updates.trips,
                        current_dates,
                    )
                )
                is not None
            },
            memo,
        )
        trips: list[UpcomingTrip] = []
        for trip in scheduled:
            if (
                _rt_key_for_row(
                    (trip.source_trip_id, trip.start_secs),
                    trip.service_date,
                    updates.canceled_trips,
                    current_dates,
                )
                is not None
            ):
                continue
            trip_rt = resolved.get((trip.trip_id, trip.service_date))
            origin_pred = dest_pred = None
            if trip_rt is not None:
                if (
                    trip.origin_stop_sequence in trip_rt.skipped
                    or trip.destination_stop_sequence in trip_rt.skipped
                ):
                    continue  # SKIPPED boarding or alighting kills the journey
                origin_pred = trip_rt.predictions.get(trip.origin_stop_sequence)
                dest_pred = trip_rt.predictions.get(trip.destination_stop_sequence)
            vehicle_id = next(
                (
                    prediction.vehicle_id
                    for prediction in (origin_pred, dest_pred)
                    if prediction is not None and prediction.vehicle_id
                ),
                None,
            )
            trips.append(
                UpcomingTrip(
                    trip_id=trip.trip_id,
                    route_id=trip.route_id,
                    service_id=trip.service_id,
                    route=routes_by_id.get(trip.route_id),
                    headsign=trip.headsign,
                    origin_stop_id=trip.origin_stop_id,
                    origin_stop=stops_by_id.get(trip.origin_stop_id),
                    destination_stop_id=trip.destination_stop_id,
                    destination_stop=stops_by_id.get(trip.destination_stop_id),
                    scheduled_departure=trip.departure,
                    predicted_departure=(
                        _predicted_time(
                            origin_pred.departure,
                            trip.departure,
                            origin_pred.delay_seconds,
                        )
                        if origin_pred
                        else None
                    ),
                    scheduled_arrival=trip.arrival,
                    predicted_arrival=(
                        _predicted_time(
                            dest_pred.arrival, trip.arrival, dest_pred.delay_seconds
                        )
                        if dest_pred
                        else None
                    ),
                    delay_seconds=(origin_pred.delay_seconds if origin_pred else None),
                    realtime=origin_pred is not None or dest_pred is not None,
                    vehicle_id=vehicle_id,
                    vehicle=_match_vehicle(
                        vehicle_id,
                        trip.source_trip_id,
                        trip.start_secs,
                        service_date=trip.service_date,
                        current_dates=current_dates,
                        vehicles=vehicles,
                    ),
                    wheelchair_accessible=trip.wheelchair_accessible,
                    bikes_allowed=trip.bikes_allowed,
                    direction_id=trip.direction_id,
                    origin_pickup_type=trip.origin_pickup_type,
                    origin_drop_off_type=trip.origin_drop_off_type,
                    origin_timepoint_exact=trip.origin_timepoint_exact,
                    origin_stop_headsign=trip.origin_stop_headsign,
                    destination_pickup_type=trip.destination_pickup_type,
                    destination_drop_off_type=trip.destination_drop_off_type,
                    destination_timepoint_exact=trip.destination_timepoint_exact,
                    destination_stop_headsign=trip.destination_stop_headsign,
                    is_first=trip.is_first,
                    is_last=trip.is_last,
                    trip_short_name=trip.trip_short_name,
                    block_id=trip.block_id,
                )
            )
        trips = [row for row in trips if _effective_trip_departure(row) >= start]
        # Total sort key, matching get_arrivals: origin predictions can
        # reorder rows relative to the scheduled ordering, and trip_id and
        # the scheduled times break effective-departure ties (the same trip
        # on two service days included) deterministically.
        trips.sort(
            key=lambda row: (
                _effective_trip_departure(row),
                row.trip_id,
                row.scheduled_departure,
                row.scheduled_arrival,
            )
        )
        return trips

    async def get_vehicles(self, zone: Circle | None = None) -> list[VehiclePosition]:
        """Live vehicle positions across the feed's VP-capable RT sources.

        Vehicle (and alert) trip references keep the producer's PLAIN trip
        ids — a display-only association resolved against the retained
        original trip rows, with no per-repetition matching for
        frequency-based trips.

        With ``zone``, only vehicles inside the circle are returned, as in
        :meth:`GbfsFeedHandle.get_vehicles`.
        """
        messages = await self._fetch_entity_messages(EntityType.VEHICLE_POSITIONS)
        async with self._guard.reader():
            routes_by_id = await self._index_read(self._index.routes_by_id)
            stops_by_id = await self._index_read(self._index.stops_by_id)
            vehicles = await self._vehicles_from(messages, stops_by_id, routes_by_id)
        if zone is None:
            return vehicles
        return [
            vehicle
            for vehicle in vehicles
            if in_circle(zone, vehicle.latitude, vehicle.longitude)
        ]

    async def _vehicles_from(
        self,
        messages: Sequence[gtfs_realtime_pb2.FeedMessage],
        stops_by_id: Mapping[str, Stop],
        routes_by_id: Mapping[str, Route],
    ) -> list[VehiclePosition]:
        """Parse VehiclePositions, resolving each trip's route from the index.

        Caller holds the reader guard. Only trips the messages name are
        looked up, so an empty message list costs no index read.
        """
        if not messages:
            return []
        trip_ids = sorted(
            {
                entity.vehicle.trip.trip_id
                for message in messages
                for entity in message.entity
                if entity.HasField("vehicle") and entity.vehicle.trip.trip_id
            }
        )
        trip_routes = await self._index_read(self._index.routes_for_trips, trip_ids)
        vehicles: list[VehiclePosition] = []
        for message in messages:
            vehicles.extend(
                vehicles_from_message(
                    message,
                    routes_by_id=routes_by_id,
                    stops_by_id=stops_by_id,
                    trip_routes=trip_routes,
                )
            )
        return vehicles

    async def _apply_trip_modifications(
        self,
        modifications: Sequence[TripModifications],
        wanted_stops: set[str],
        *,
        end: datetime | None,
        rt_stops: Mapping[str, Stop],
        stops_by_id: Mapping[str, Stop],
        routes_by_id: Mapping[str, Route],
    ) -> tuple[list[StopArrival], set[tuple[str, date]]]:
        """Rebuild every detoured run, whole, around its detours.

        Returns the rows the modified runs now produce at the queried stops
        (scheduled no later than ``end``, if given), and the (trip id,
        service date) INSTANCES whose original schedule must be discarded.
        The runs are exactly the ones the entities name: their trips (a
        frequency template covers its repetitions, narrowed by
        ``start_times``) on their required ``service_dates``, where the
        trip's service runs that day -- so they are read whole, once, and
        no horizon can cut a rebuilt row off. Caller must already hold the
        reader guard.

        A detour can route a trip through a stop its static schedule never
        served, so this cannot filter by stop before applying: the whole
        call sequence is rebuilt, then the queried stops are selected out
        of the result.
        """
        if not modifications:
            return [], set()
        # Only trips that can put a row at a queried stop matter: those
        # calling at one, or whose entity names one as a replacement. Any
        # other detoured run produces no row here, and dropping its
        # schedule is invisible to this query.
        source_ids = sorted({tid for mod in modifications for tid in mod.trip_ids})
        relevant = await self._index_read(
            self._index.trips_serving, source_ids, sorted(wanted_stops)
        )
        relevant |= {
            source_id
            for mod in modifications
            if any(
                replacement.stop_id in wanted_stops
                for modification in mod.modifications
                for replacement in modification.replacements
            )
            for source_id in mod.trip_ids
        }
        concrete = await self._index_read(
            self._index.concrete_trip_ids, sorted(relevant)
        )
        days = await self._index_read(
            self._index.run_days,
            sorted(
                {
                    (trip_id, service_date)
                    for mod in modifications
                    for source_id in mod.trip_ids
                    for trip_id in concrete.get(source_id, ())
                    for service_date in mod.service_dates
                }
            ),
        )
        # One template run per trip: its calls are offsets from its day's
        # start, and a detour only adds offsets to them, so each (trip,
        # modifications) rebuild is computed once and shifted to every day.
        first_day: dict[str, date] = {}
        for trip_id, service_date, _ in days:
            first_day.setdefault(trip_id, service_date)
        templates = {
            instance.trip_id: instance
            for instance in await self._index_read(
                self._index.instance_trip_calls, sorted(first_day.items())
            )
        }
        flat = [
            (mod, modification)
            for mod in modifications
            for modification in mod.modifications
        ]
        # Per producer trip, the modifications naming it (in feed order), so
        # applicability costs a run's own modifications, not all of them.
        by_source: dict[str, list[int]] = {}
        for n, (mod, _) in enumerate(flat):
            for source_id in dict.fromkeys(mod.trip_ids):
                by_source.setdefault(source_id, []).append(n)
        rebuilt: dict[tuple[str, tuple[int, ...]], list[TripCall] | None] = {}
        rows: list[StopArrival] = []
        touched: set[tuple[str, date]] = set()
        for trip_id, service_date, day_start in days:
            template = templates.get(trip_id)
            if template is None:
                # No calls (a frequency template's own trips row): no run
                # a detour could rebuild.
                continue
            applicable = tuple(
                n
                for n in by_source.get(template.source_trip_id, ())
                if service_date in (mod := flat[n][0]).service_dates
                # An entity listing start_times addresses specific
                # repetitions; one listing none addresses every instance.
                and (not mod.start_secs or template.start_secs in mod.start_secs)
            )
            if not applicable:
                continue
            key = (trip_id, applicable)
            if key not in rebuilt:
                modified = _modified_calls(
                    template, [flat[n][1] for n in applicable], rt_stops, stops_by_id
                )
                rebuilt[key] = None if modified == template.calls else modified
            calls = rebuilt[key]
            if calls is None:
                continue
            touched.add((trip_id, service_date))
            rows.extend(
                _detoured_rows(
                    template,
                    calls,
                    day_start - template.day_start_utc,
                    wanted_stops=wanted_stops,
                    end=end,
                    rt_stops=rt_stops,
                    stops_by_id=stops_by_id,
                    route=routes_by_id.get(template.route_id),
                )
            )
        return rows, touched

    async def get_scheduled_departures(
        self,
        queries: Sequence[ArrivalsQuery],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[list[StopArrival]]:
        """Scheduled departures for each query, schedule only -- no network.

        The schedule-only twin of :meth:`get_arrivals`: same
        :class:`ArrivalsQuery` batch, same ``start``/``end``/``limit``
        rules (open-ended by default, ``ValueError`` for ``end=None`` with
        a ``limit=None`` query, ``end < start`` gives empty boards), but no
        realtime overlay and no ``grace``. It never makes a network request,
        so a calendar view or a config flow keeps working when a producer's
        realtime endpoint is down or its API key has lapsed. For "every
        departure in a date range", pass ``end`` and ``limit=None``.

        Rows come back ``realtime=False`` with no predictions and no
        vehicle, sorted by scheduled departure. Without predictions the
        schedule order is final, so an open-ended board settles as soon as
        it is full inside the slice read so far.
        """
        _require_bound(end, [query.limit for query in queries])
        start, end = _utc_window(start, end)
        all_stop_ids = sorted(
            {stop_id for query in queries for stop_id in query.stop_ids}
        )
        if not all_stop_ids or (end is not None and end < start):
            return [[] for _ in queries]
        async with self._guard.reader():
            stops_by_id = await self._index_read(self._index.stops_by_id)
            routes_by_id = await self._index_read(self._index.routes_by_id)
            ceilings = [
                await self._index_read(self._index.departures_ceiling, *_filters(q))
                for q in queries
            ]
            ceiling = max((c for c in ceilings if c is not None), default=None)
            boards: list[list[StopArrival] | None] = [None] * len(queries)
            seen: set[_RowKey] = set()
            rows: list[StopArrival] = []
            read_from = start
            for horizon, final in _horizons(start, end, ceiling):
                pending = [i for i, board in enumerate(boards) if board is None]
                stops, routes, headsigns = _pushdown([queries[i] for i in pending])
                for dep in await self._index_read(
                    self._index.upcoming_departures,
                    stops,
                    routes,
                    read_from,
                    horizon - read_from,
                    None,
                    headsigns=headsigns,
                ):
                    key = _departure_key(dep)
                    if key not in seen:
                        seen.add(key)
                        rows.append(_scheduled_row(dep, stops_by_id, routes_by_id))
                read_from = horizon
                # The index's own total order, so slice boundaries never
                # change which rows a limit keeps.
                rows.sort(
                    key=lambda row: (
                        row.scheduled_departure or start,
                        row.trip_id or "",
                        row.stop_id,
                    )
                )
                for i in pending:
                    board = _select_arrivals(rows, queries[i])
                    if (
                        final
                        or _past_ceiling(ceilings[i], horizon)
                        or _board_settles(
                            [_effective_departure(row, start) for row in board],
                            queries[i].limit,
                            horizon,
                        )
                    ):
                        boards[i] = board
                if all(board is not None for board in boards):
                    break
            return [board or [] for board in boards]

    async def get_services_on(self, service_date: date) -> set[str]:
        """Service ids running on a GTFS service date.

        Pairs with ``StopArrival.service_id``/``UpcomingTrip.service_id``:
        those say which calendar a departure belongs to, this says whether
        that calendar runs on a given day -- so "does this trip run next
        Tuesday" is answerable without a second arrivals query.

        calendar_dates exceptions are applied over calendar.txt, with
        removals winning when a producer lists both types for one date.
        The date is a GTFS SERVICE date, which for a past-midnight
        departure is the day the service STARTED, not the clock day it
        lands on.
        """
        async with self._guard.reader():
            return await self._index_read(self._index.active_service_ids, service_date)

    async def get_alerts(self) -> list[ServiceAlert]:
        """Service alerts across the feed's SA-capable RT sources.

        Alert scoping contract: each alert carries EVERY selector an
        informed entity named -- agency ids, route ids, route types,
        direction ids, stop ids, and trip ids. An alert is unscoped
        ("applies everywhere") ONLY when all six are empty, so an alert
        naming just an agency, or just a mode, is scoped to that rather
        than feed-wide. Alert trip references keep the producer's
        PLAIN trip ids (like vehicles): a display-only association with
        no per-repetition matching for frequency-based trips.
        """
        messages = await self._fetch_entity_messages(EntityType.SERVICE_ALERTS)
        alerts: list[ServiceAlert] = []
        for message in messages:
            alerts.extend(alerts_from_message(message))
        return alerts

    async def refresh_static(self) -> bool:
        """Re-check the dataset's identity; rebuild the index only on change.

        Catalog handles re-fetch the feed and compare dataset IDs; direct
        handles re-probe the URL's HTTP validators (falling back to
        re-downloading and hashing when the server offers none). Returns
        True if the index was rebuilt. Stale-while-revalidate: the old
        index keeps serving until the new one is ready, then swaps.

        Safe to call concurrently with queries and with itself. Queries
        keep running against the old index while a new dataset downloads
        and builds; the swap waits for in-flight queries to finish and
        parks new ones only for the swap-and-close itself. Overlapping
        refresh calls run back to back; the second re-checks the catalog
        and finds nothing new.
        """
        async with self._refresh_lock:
            if self._direct is not None:
                return await self._refresh_static_direct(self._direct)
            return await self._refresh_static_catalog()

    async def _refresh_static_catalog(self) -> bool:
        """Catalog strategy: compare the latest dataset ID against ours."""
        fresh = await self._client.catalog.get_gtfs_feed(self.static_feed_id)
        dataset = fresh.latest_dataset
        if dataset is None or not dataset.id or dataset.id == self._index.dataset_id:
            return False
        new_index = await self._ensure_index(self._client, fresh)
        await self._swap_index(new_index, fresh)
        return True

    async def _refresh_static_direct(self, direct: _DirectUrls) -> bool:
        """Direct strategy: HTTP validators stand in for the catalog compare.

        A HEAD-derived id (``etag:``/``lastmod:``) that matches the current
        index short-circuits with no download. A validator-less server
        forces a re-download so the bytes can be hashed — the resulting
        ``sha256:`` id may then match the current index after all, in which
        case the freshly acquired candidate is discarded and False is
        returned (the transfer was the unavoidable cost of identification).
        """
        probed = await self._probe_direct_dataset_id(self._client, direct)
        if probed is not None and probed == self._index.dataset_id:
            return False
        source = _StaticSource(
            url=direct.static_url,
            cache_key=self._feed_key,
            dataset_id=probed,
            timezone_name=None,
            headers=direct.headers,
        )
        new_index = await self._ensure_index_from_source(self._client, source)
        if new_index.dataset_id == self._index.dataset_id:
            await asyncio.to_thread(new_index.close)
            return False
        await self._swap_index(new_index)
        return True

    async def _swap_index(
        self, new_index: StaticIndex, static_feed: GtfsFeed | None = None
    ) -> None:
        """Publish the new index atomically, then close the old one.

        The new index's own reads need no exclusion, and the publish is a
        single assignment so no caller can observe mixed-dataset
        attributes; the old index closes with no reader inside. The catalog
        record travels with the index so a failure after this point can
        never leave ``static_dataset`` describing a dataset that is no
        longer the one being served.
        """
        stops = tuple(await asyncio.to_thread(new_index.stops))
        routes = tuple(await asyncio.to_thread(new_index.routes))
        agencies = tuple(await asyncio.to_thread(new_index.agencies))
        feed_info = await asyncio.to_thread(new_index.feed_info)
        async with self._guard.writer():
            old_index = self._index
            if static_feed is not None:
                self._static_feed = static_feed
            self._index, self._stops, self._routes, self._agencies, self._feed_info = (
                new_index,
                stops,
                routes,
                agencies,
                feed_info,
            )
            await asyncio.to_thread(old_index.close)

    def stops_in(self, zone: Circle) -> list[Stop]:
        """Return stops within a circular zone (config-flow stop picker)."""
        return [
            stop
            for stop in self.stops
            if stop.latitude is not None
            and stop.longitude is not None
            and in_circle(zone, stop.latitude, stop.longitude)
        ]

    def stations_in(self, zone: Circle) -> list[StationGroup]:
        """Boarding stops within a zone, grouped into logical stations.

        The presentation-ready companion to :meth:`stops_in`: station
        hierarchies collapse to one entry per station and entrances or
        pathway nodes never appear.
        """
        return group_stations(self.stops_in(zone))

    async def get_routes_serving(self, stop_id: str) -> list[Route]:
        """Routes with scheduled service at the stop (route-filter picker)."""
        async with self._guard.reader():
            return await self._index_read(self._index.routes_serving, stop_id)

    async def get_headsigns_serving(
        self, stop_id: str, route_id: str | None = None
    ) -> list[str]:
        """Distinct headsigns at the stop (direction-filter picker options)."""
        async with self._guard.reader():
            return await self._index_read(
                self._index.headsigns_serving, stop_id, route_id
            )

    def close(self) -> None:
        """Release the SQLite connection.

        Not safe with queries in flight; stop polling before closing.
        """
        self._index.close()
