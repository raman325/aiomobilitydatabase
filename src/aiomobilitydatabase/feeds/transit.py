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
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import asynccontextmanager
from dataclasses import dataclass
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
    _require_http_url,  # deliberate friend access: shared data-origin URL guard
    alerts_from_message,
    fetch_feed_message,
    resolve_trip_predictions,
    stops_from_message,
    trip_modifications_from_message,
    trip_updates_from_message,
    vehicles_from_message,
)
from .static_index import StaticIndex, TripCall, TripInstanceCalls

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


def _current_service_dates(
    rows: Iterable[tuple[str, int | None, date]],
) -> dict[_RtIdentity, date]:
    """Earliest in-window service day per RT identity.

    Defines the "currently active" instance a date-less update addresses
    when several service days' instances of one identity share the query
    window (see :func:`_rt_key_for_row`).
    """
    current: dict[_RtIdentity, date] = {}
    for source_trip_id, start_secs, service_date in rows:
        identity = (source_trip_id, start_secs)
        held = current.get(identity)
        if held is None or service_date < held:
            current[identity] = service_date
    return current


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
    running on the day it is currently active on": it matches only the
    CURRENT instance of its identity, defined precisely as the earliest
    in-window service day for that identity (``current_dates`` holds that
    minimum over the scheduled rows; ``identity`` always appears in it
    because the queried row is itself in the window). With exactly one
    instance in the window -- every sub-24h lookahead -- this is the
    identity's only row, so date-less behavior is unchanged from before
    start_date existed. When a dated and a date-less key both exist for
    one row, the dated key wins (it is strictly more specific); the
    date-less key never falls through to a sibling day's instance. A
    garbage start_date parses to None (see ``rt._trip_start_date``), so
    it behaves exactly like an absent one.
    """
    source_trip_id, start_secs = identity
    dated: TripUpdateKey = (source_trip_id, service_date, start_secs)
    if dated in keys:
        return dated
    dateless: TripUpdateKey = (source_trip_id, None, start_secs)
    if dateless in keys and current_dates[identity] == service_date:
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


def _vehicle_index(
    vehicles: Sequence[VehiclePosition],
) -> tuple[
    dict[str, VehiclePosition],
    dict[tuple[str, int | None], VehiclePosition | None],
]:
    """Index live vehicles for arrival matching, by vehicle id and instance.

    The second map is keyed by (trip id, start seconds) -- the pair a
    TripDescriptor uses to address ONE repetition of a frequency-based
    trip -- so concurrent repetitions of a template trip no longer
    collide. A vehicle without start_time is filed under
    ``(trip_id, None)``, which is exactly how a plain trip is addressed.

    An entry still holds None where two vehicles claim the same instance:
    that is a genuinely ambiguous feed, not a resolvable one, and guessing
    would put the wrong vehicle on the map.
    """
    by_vehicle_id: dict[str, VehiclePosition] = {}
    by_instance: dict[tuple[str, int | None], VehiclePosition | None] = {}
    for vehicle in vehicles:
        if vehicle.vehicle_id:
            by_vehicle_id[vehicle.vehicle_id] = vehicle
        if vehicle.trip_id:
            key = (vehicle.trip_id, vehicle.trip_start_secs)
            # A second claimant poisons the entry rather than overwriting it.
            by_instance[key] = None if key in by_instance else vehicle
    return by_vehicle_id, by_instance


def _match_vehicle(
    vehicle_id: str | None,
    plain_trip_id: str | None,
    start_secs: int | None,
    by_vehicle_id: Mapping[str, VehiclePosition],
    by_instance: Mapping[tuple[str, int | None], VehiclePosition | None],
) -> VehiclePosition | None:
    """Return the vehicle serving an arrival, or None if it is not pinnable.

    ``plain_trip_id`` must be the PRODUCER-facing trip id (a repetition's
    template id, not its synthetic ``{trip_id}#{start_secs}`` id): vehicle
    trip references keep plain ids, so matching on the synthetic id would
    never hit for frequency-based service.

    A repetition is addressed by (template id, its start); the exact
    instance is tried first. A producer that sends no start_time has not
    said which repetition it means, so its position falls back to the
    date-less key and serves whichever repetition asks -- the same
    leniency the TripUpdates path applies, and better than attaching
    nothing at all.
    """
    if vehicle_id and (vehicle := by_vehicle_id.get(vehicle_id)) is not None:
        return vehicle
    if not plain_trip_id:
        return None
    if start_secs is not None and (
        exact := by_instance.get((plain_trip_id, start_secs))
    ):
        return exact
    return by_instance.get((plain_trip_id, None))


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
        if replacement.travel_time_to_stop is None:
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
            departure=call.departure + delay,
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


def _effective_trip_departure(trip: UpcomingTrip) -> datetime:
    """Predicted origin departure if any, else scheduled."""
    return trip.predicted_departure or trip.scheduled_departure


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


def group_stations(stops: list[Stop]) -> list[StationGroup]:
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
        self.rt_feeds = rt_feeds
        self._index = index
        self._api_key = api_key
        self._guard = _IndexGuard()
        self._refresh_lock = asyncio.Lock()
        # Per-url HTTP validators plus the parse they belong to. Only ever
        # reused on a 304, i.e. when the producer itself states the bytes
        # are unchanged -- this is revalidation, not a TTL cache.
        self._rt_validators: dict[str, tuple[FeedValidators, Any]] = {}
        self.stops: list[Stop] = index.stops()
        self.routes: list[Route] = index.routes()
        self.agencies: list[Agency] = index.agencies()
        # None when the dataset ships no feed_info.txt (an optional file).
        self.feed_info: FeedInfo | None = index.feed_info()

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
            references = rt_self.feed_references or []
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
                entity_types=types,
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
                assert known is not None
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
        return aggregate

    async def _resolve_predictions(
        self, updates: TripUpdates, rt_keys: Mapping[_TripInstance, TripUpdateKey]
    ) -> dict[_TripInstance, TripPredictions]:
        """Resolve STU propagation for every matched scheduled trip instance.

        ``rt_keys`` maps each matched instance -- CONCRETE trip id
        (synthetic repetition ids included) plus service day -- to the
        ``updates.trips`` key that addresses it, as resolved per row by
        :func:`_rt_key_for_row` (callers pass matched instances only).
        Instances are the unit here, not bare trip ids, because a window
        of 24h or more holds several service days' rows for one concrete
        trip id and a dated update must reach exactly one of them. The
        static stop order is fetched once per concrete trip id (it is
        service-day independent) and each instance resolves via
        :func:`resolve_trip_predictions`, so propagation happens against
        the exact instance the update addressed. Every matched trip is
        guaranteed a stop-calls entry: scheduled rows only exist because
        the trip has stop_times rows.
        """
        if not rt_keys:
            return {}
        calls = await self._index_read(
            self._index.trip_stop_calls, sorted({trip_id for trip_id, _ in rt_keys})
        )
        return {
            (trip_id, service_date): resolve_trip_predictions(
                updates.trips[key], calls[trip_id]
            )
            for (trip_id, service_date), key in rt_keys.items()
        }

    async def get_arrivals(
        self,
        queries: Sequence[ArrivalsQuery],
        *,
        lookahead: timedelta = timedelta(hours=2),
        grace: timedelta = timedelta(hours=1),
        now_utc: datetime | None = None,
        with_vehicles: bool = False,
    ) -> list[list[StopArrival]]:
        """Upcoming arrivals for each query: schedule merged with RT.

        One scheduled query over the union of every query's stops, one
        TripUpdates fetch, one merge; then each query's route and headsign
        filters and ``limit`` apply to the merged rows, so a filter can
        never see an empty board because other routes crowded out the
        limit. ``limit`` caps each query's merged result as a whole, not
        per stop. RT-added rows carry no headsign and so never pass a
        headsign filter; a row that announces neither arrival nor
        departure keeps every time field ``None`` and sorts at ``now``.
        Returns one list per query, in order.

        ``grace`` reaches that far BEFORE ``now`` when collecting scheduled
        candidates, so a trip whose scheduled time has passed still picks
        up its realtime prediction. After the merge, rows whose effective
        departure (predicted if present, else scheduled) is before ``now``
        are dropped: a schedule-only row past its time is gone, a delayed
        one stays until its prediction passes.

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
        only the currently-active (earliest in-window) instance, which
        with a sub-24h lookahead is the only instance and preserves the
        pre-start_date behavior exactly.

        ``with_vehicles`` attaches each row's live
        :class:`VehiclePosition` when one can be pinned down -- off by
        default because vehicle positions are a SEPARATE network fetch,
        and a board that shows no vehicles should not pay for one. A
        producer serving both entity types off one url is fetched once
        either way, so there the flag costs no extra request.
        ``now_utc`` exists for deterministic testing; omit it in production.
        """
        now = now_utc or datetime.now(UTC)
        all_stop_ids = sorted(
            {stop_id for query in queries for stop_id in query.stop_ids}
        )
        if not all_stop_ids:
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
        # TripModifications ride the TripUpdates feed, so the memo means
        # they cost nothing extra; so do the Stop entities their
        # replacement stops may be the only definition of.
        tu_messages = list(rt_memo.values())
        rt_stops: dict[str, Stop] = {}
        modifications: list[TripModifications] = []
        for message in tu_messages:
            rt_stops.update(stops_from_message(message))
            modifications.extend(trip_modifications_from_message(message))
        canceled = updates.canceled_trips
        added_rows = updates.added
        async with self._guard.reader():
            scheduled = await self._index_read(
                self._index.upcoming_departures,
                all_stop_ids,
                None,
                now,
                lookahead,
                None,
                grace=grace,
            )
            stops_by_id = await self._index_read(self._index.stops_by_id)
            routes_by_id = await self._index_read(self._index.routes_by_id)
            modified_rows: list[StopArrival] = []
            modified_trip_ids: set[str] = set()
            if modifications:
                (
                    modified_rows,
                    modified_trip_ids,
                ) = await self._apply_trip_modifications(
                    modifications,
                    set(all_stop_ids),
                    now=now,
                    lookahead=lookahead,
                    grace=grace,
                    rt_stops=rt_stops,
                    stops_by_id=stops_by_id,
                    routes_by_id=routes_by_id,
                )
                # The unmodified schedule for these instances describes a
                # route the vehicle is no longer taking.
                scheduled = [
                    dep for dep in scheduled if dep.trip_id not in modified_trip_ids
                ]
            vehicles: list[VehiclePosition] = []
            if vehicle_messages:
                vp_trip_ids = sorted(
                    {
                        entity.vehicle.trip.trip_id
                        for message in vehicle_messages
                        for entity in message.entity
                        if entity.HasField("vehicle") and entity.vehicle.trip.trip_id
                    }
                )
                vp_trip_routes = await self._index_read(
                    self._index.routes_for_trips, vp_trip_ids
                )
                for message in vehicle_messages:
                    vehicles.extend(
                        vehicles_from_message(
                            message,
                            routes_by_id=routes_by_id,
                            stops_by_id=stops_by_id,
                            trip_routes=vp_trip_routes,
                        )
                    )
            by_vehicle_id, by_instance = _vehicle_index(vehicles)
            # Per-row RT matching: each scheduled row's (identity, service day)
            # resolves to at most one TripUpdates key via _rt_key_for_row —
            # dated keys hit exactly their service day's instance, date-less
            # keys only the earliest in-window one.
            current_dates = _current_service_dates(
                (dep.source_trip_id, dep.start_secs, dep.service_date)
                for dep in scheduled
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
                            by_vehicle_id,
                            by_instance,
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
                            by_vehicle_id,
                            by_instance,
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
                if _effective_departure(arrival, now) >= now
            ]
            # Total sort key, matching upcoming_departures: effective time alone
            # ties frequently, so trip_id/stop_id break ties deterministically.
            arrivals.sort(
                key=lambda a: (_effective_departure(a, now), a.trip_id or "", a.stop_id)
            )
            return [_select_arrivals(arrivals, query) for query in queries]

    async def upcoming_trips(
        self,
        origin_stop_id: str,
        destination_stop_id: str,
        *,
        lookahead: timedelta = timedelta(hours=2),
        grace: timedelta = timedelta(hours=1),
        limit: int = 10,
        now_utc: datetime | None = None,
    ) -> list[UpcomingTrip]:
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

        ``grace`` reaches that far before ``now`` for scheduled candidates
        and rows whose effective origin departure is before ``now`` are
        dropped after the realtime merge, exactly as in
        :meth:`get_arrivals`. ``limit`` then caps the merged result,
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
        only the earliest in-window instance (see :func:`_rt_key_for_row`).

        ``now_utc`` exists for deterministic testing; omit it in production.
        """
        now = now_utc or datetime.now(UTC)
        updates = await self._aggregated_trip_updates()
        async with self._guard.reader():
            scheduled = await self._index_read(
                self._index.upcoming_trips,
                origin_stop_id,
                destination_stop_id,
                now,
                lookahead,
                None,
                grace=grace,
            )
            routes_by_id = await self._index_read(self._index.routes_by_id)
            stops_by_id = await self._index_read(self._index.stops_by_id)
            # Same per-instance RT matching as get_arrivals (_rt_key_for_row):
            # dated keys hit their service day's row, date-less keys only the
            # earliest in-window instance of the identity.
            current_dates = _current_service_dates(
                (trip.source_trip_id, trip.start_secs, trip.service_date)
                for trip in scheduled
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
                trips.append(
                    UpcomingTrip(
                        trip_id=trip.trip_id,
                        route_id=trip.route_id,
                        service_id=trip.service_id,
                        route=routes_by_id.get(trip.route_id),
                        headsign=trip.headsign,
                        origin_stop_id=origin_stop_id,
                        origin_stop=stops_by_id.get(origin_stop_id),
                        destination_stop_id=destination_stop_id,
                        destination_stop=stops_by_id.get(destination_stop_id),
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
                        delay_seconds=(
                            origin_pred.delay_seconds if origin_pred else None
                        ),
                        realtime=origin_pred is not None or dest_pred is not None,
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
            trips = [row for row in trips if _effective_trip_departure(row) >= now]
            # Total sort key, matching get_arrivals: origin predictions can
            # reorder rows relative to the scheduled ordering, and trip_id /
            # scheduled_arrival break effective-departure ties deterministically.
            trips.sort(
                key=lambda row: (
                    _effective_trip_departure(row),
                    row.trip_id,
                    row.scheduled_arrival,
                )
            )
            return trips[:limit]

    async def get_vehicles(self) -> list[VehiclePosition]:
        """Live vehicle positions across the feed's VP-capable RT sources.

        Vehicle (and alert) trip references keep the producer's PLAIN trip
        ids — a display-only association resolved against the retained
        original trip rows, with no per-repetition matching for
        frequency-based trips.
        """
        messages = await self._fetch_entity_messages(EntityType.VEHICLE_POSITIONS)
        trip_ids = sorted(
            {
                entity.vehicle.trip.trip_id
                for message in messages
                for entity in message.entity
                if entity.HasField("vehicle") and entity.vehicle.trip.trip_id
            }
        )
        async with self._guard.reader():
            trip_routes = await self._index_read(self._index.routes_for_trips, trip_ids)
            routes_by_id = await self._index_read(self._index.routes_by_id)
            stops_by_id = await self._index_read(self._index.stops_by_id)
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
        now: datetime,
        lookahead: timedelta,
        grace: timedelta,
        rt_stops: Mapping[str, Stop],
        stops_by_id: Mapping[str, Stop],
        routes_by_id: Mapping[str, Route],
    ) -> tuple[list[StopArrival], set[str]]:
        """Rebuild the affected trip instances around their detours.

        Returns the rows the modified trips now produce at the queried
        stops, and the concrete trip ids whose ORIGINAL schedule must be
        discarded. Caller must already hold the reader guard.

        A detour can route a trip through a stop its static schedule never
        served, so this cannot filter by stop before applying: the whole
        call sequence is rebuilt, then the queried stops are selected out
        of the result.
        """
        source_ids = sorted({tid for mod in modifications for tid in mod.trip_ids})
        concrete = await self._index_read(self._index.concrete_trip_ids, source_ids)
        wanted_concrete = sorted({c for ids in concrete.values() for c in ids})
        instances = await self._index_read(
            self._index.trip_instance_calls, wanted_concrete, now, lookahead, grace
        )
        rows: list[StopArrival] = []
        touched: set[str] = set()
        for instance in instances:
            applicable = [
                modification
                for mod in modifications
                if instance.source_trip_id in mod.trip_ids
                and (
                    not mod.service_dates or instance.service_date in mod.service_dates
                )
                # An entity listing start_times addresses specific
                # repetitions; one listing none addresses every instance.
                and (not mod.start_secs or instance.start_secs in mod.start_secs)
                for modification in mod.modifications
            ]
            if not applicable:
                continue
            calls = _modified_calls(instance, applicable, rt_stops, stops_by_id)
            if calls == instance.calls:
                continue
            touched.add(instance.trip_id)
            route = routes_by_id.get(instance.route_id)
            for call in calls:
                if call.stop_id not in wanted_stops:
                    continue
                rows.append(
                    StopArrival(
                        stop_id=call.stop_id,
                        # An RT-added stop is the ONLY definition of itself.
                        stop=rt_stops.get(call.stop_id)
                        or stops_by_id.get(call.stop_id),
                        route_id=instance.route_id,
                        route=route,
                        trip_id=instance.trip_id,
                        service_id=instance.service_id,
                        headsign=instance.headsign,
                        scheduled_arrival=call.arrival,
                        scheduled_departure=call.departure,
                        predicted_arrival=None,
                        predicted_departure=None,
                        delay_seconds=None,
                        realtime=True,
                        vehicle_id=None,
                        vehicle=None,
                        wheelchair_accessible=instance.wheelchair_accessible,
                        bikes_allowed=instance.bikes_allowed,
                        direction_id=instance.direction_id,
                        pickup_type=call.pickup_type,
                        drop_off_type=call.drop_off_type,
                        timepoint_exact=call.timepoint_exact,
                        stop_headsign=call.stop_headsign,
                        trip_short_name=instance.trip_short_name,
                        block_id=instance.block_id,
                    )
                )
        return rows, touched

    async def scheduled_departures(
        self,
        stop_ids: Sequence[str],
        *,
        start: datetime,
        end: datetime,
        route_ids: Sequence[str] | None = None,
        headsigns: Sequence[str] | None = None,
    ) -> list[StopArrival]:
        """Every scheduled departure in an absolute window, schedule only.

        The shape a calendar view needs, and deliberately not
        :meth:`get_arrivals` with a long lookahead: no realtime overlay
        (predictions are meaningless days out and a producer only
        publishes them for the current service day), no ``grace`` window,
        and no per-query limit -- a calendar wants every departure in the
        range, not the next few.

        Rows come back ``realtime=False`` with no predictions and no
        vehicle, sorted by departure. ``end`` is exclusive of nothing in
        particular: it is simply the window's upper bound, and a window
        whose end precedes its start returns [].
        """
        if end <= start or not stop_ids:
            return []
        async with self._guard.reader():
            scheduled = await self._index_read(
                self._index.upcoming_departures,
                sorted(set(stop_ids)),
                sorted(set(route_ids)) if route_ids else None,
                start,
                end - start,
                None,
                grace=timedelta(0),
            )
            stops_by_id = await self._index_read(self._index.stops_by_id)
            routes_by_id = await self._index_read(self._index.routes_by_id)
        wanted = set(headsigns) if headsigns else None
        rows = [
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
            for dep in scheduled
            if wanted is None or dep.headsign in wanted
        ]
        rows.sort(
            key=lambda row: (
                row.scheduled_departure or start,
                row.trip_id or "",
                row.stop_id,
            )
        )
        return rows

    async def services_on(self, service_date: date) -> set[str]:
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

        Alert scoping contract: each alert carries the route ids, stop
        ids, AND informed-entity trip ids it names. An alert is unscoped
        ("applies everywhere") ONLY when ``route_ids``, ``stop_ids``, and
        ``trip_ids`` are all empty -- a trip-scoped alert (informed
        entities carrying only trip descriptors) is scoped to those
        trips, not agency-wide. Alert trip references keep the producer's
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
        stops = await asyncio.to_thread(new_index.stops)
        routes = await asyncio.to_thread(new_index.routes)
        agencies = await asyncio.to_thread(new_index.agencies)
        feed_info = await asyncio.to_thread(new_index.feed_info)
        async with self._guard.writer():
            old_index = self._index
            if static_feed is not None:
                self._static_feed = static_feed
            self._index, self.stops, self.routes, self.agencies, self.feed_info = (
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

    async def routes_serving(self, stop_id: str) -> list[Route]:
        """Routes with scheduled service at the stop (route-filter picker)."""
        async with self._guard.reader():
            return await self._index_read(self._index.routes_serving, stop_id)

    async def headsigns_serving(
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
