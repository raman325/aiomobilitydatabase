"""TransitFeedHandle: schedule + realtime snapshots for one transit feed."""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
from google.transit import gtfs_realtime_pb2

from ..models import (
    DataType,
    EntityType,
    GtfsFeed,
    GtfsRtFeed,
    LatestDataset,
)
from .const import STATIC_DB_FILENAME
from .exceptions import SourceConnectionError, StaticDataUnavailableError
from .geo import Circle, in_circle
from .models import (
    Route,
    ServiceAlert,
    StaticBuildProgress,
    Stop,
    StopArrival,
    VehiclePosition,
)
from .rt import (
    AddedStopTime,
    StopPrediction,
    alerts_from_message,
    fetch_feed_message,
    trip_updates_from_message,
    vehicles_from_message,
)
from .static_index import StaticIndex

if TYPE_CHECKING:
    from .client import MobilityFeedsClient

_PROGRESS_CHUNK_BYTES = 262_144


class TransitFeedHandle:
    """Snapshot access to one transit feed (GTFS + its GTFS-RT siblings).

    Create via :meth:`MobilityFeedsClient.get_transit_feed` — accepts a GTFS
    or GTFS-RT feed ID and resolves the sibling relationship through the
    catalog. Construction downloads/opens the static index.
    """

    def __init__(
        self,
        client: MobilityFeedsClient,
        static_feed: GtfsFeed,
        rt_feeds: list[GtfsRtFeed],
        index: StaticIndex,
        api_key: str | None,
    ) -> None:
        """Construct via ``MobilityFeedsClient.get_transit_feed()`` instead."""
        self._client = client
        self._static_feed = static_feed
        self.rt_feeds = rt_feeds
        self._index = index
        self._api_key = api_key
        self.stops: list[Stop] = index.stops()
        self.routes: list[Route] = index.routes()

    @property
    def static_feed_id(self) -> str:
        """Catalog ID of the resolved static GTFS feed."""
        assert self._static_feed.id is not None  # resolved feeds always have IDs
        return self._static_feed.id

    @property
    def static_dataset(self) -> LatestDataset | None:
        """Catalog metadata for the indexed dataset.

        Includes downloaded_at, service date range, and hashes — for
        consumer diagnostics entities.
        """
        return self._static_feed.latest_dataset

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

    @staticmethod
    async def _ensure_index(
        client: MobilityFeedsClient,
        static_feed: GtfsFeed,
        on_progress: Callable[[StaticBuildProgress], None] | None = None,
    ) -> StaticIndex:
        dataset = static_feed.latest_dataset
        if dataset is None or not dataset.id or not dataset.hosted_url:
            raise StaticDataUnavailableError(
                f"Feed {static_feed.id} has no hosted static dataset"
            )
        db_path: Path | None = None
        if client.cache_dir is not None:
            feed_dir = client.cache_dir / str(static_feed.id)
            feed_dir.mkdir(parents=True, exist_ok=True)
            db_path = feed_dir / STATIC_DB_FILENAME
            cached = await asyncio.to_thread(
                StaticIndex.open_cached, db_path, dataset.id
            )
            if cached is not None:
                return cached
        session = client._get_session()  # deliberate friend access
        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = Path(tmp_dir) / "dataset.zip"
            try:
                async with session.get(
                    dataset.hosted_url,
                    timeout=aiohttp.ClientTimeout(
                        total=None, sock_read=client.timeout_seconds
                    ),
                ) as resp:
                    if resp.status >= HTTPStatus.BAD_REQUEST:
                        raise SourceConnectionError(
                            f"Hosted dataset fetch failed ({resp.status})",
                            status=resp.status,
                        )
                    total_bytes = resp.content_length
                    done_bytes = 0
                    last_emitted = 0
                    first_chunk = True
                    with zip_path.open("wb") as fp:
                        async for chunk in resp.content.iter_chunked(1 << 16):
                            fp.write(chunk)
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
                    if on_progress is not None:
                        on_progress(
                            StaticBuildProgress(
                                phase="download",
                                done_bytes=done_bytes,
                                total_bytes=total_bytes,
                            )
                        )
            except (TimeoutError, aiohttp.ClientError) as err:
                raise SourceConnectionError(
                    f"Error downloading dataset: {err}"
                ) from err
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
                dataset.id,
                dataset.agency_timezone,
                build_progress,
            )

    def _rt_feeds_for(self, entity_type: EntityType) -> list[GtfsRtFeed]:
        return [
            feed
            for feed in self.rt_feeds
            if feed.entity_types and entity_type in feed.entity_types
        ]

    async def _fetch_entity_messages(
        self, entity_type: EntityType
    ) -> list[gtfs_realtime_pb2.FeedMessage]:
        session = self._client._get_session()
        messages = []
        for feed in self._rt_feeds_for(entity_type):
            source = feed.source_info
            if source is None or not source.producer_url:
                continue
            messages.append(
                await fetch_feed_message(
                    session,
                    source.producer_url,
                    auth_type=source.authentication_type,
                    api_key_name=source.api_key_parameter_name,
                    api_key=self._api_key,
                    timeout_seconds=self._client.timeout_seconds,
                )
            )
        return messages

    async def get_arrivals(
        self,
        stop_ids: list[str],
        route_ids: list[str] | None = None,
        *,
        lookahead: timedelta = timedelta(hours=2),
        limit: int = 10,
        now_utc: datetime | None = None,
    ) -> list[StopArrival]:
        """Upcoming arrivals at the given stops: schedule merged with RT.

        ``limit`` caps the MERGED result (scheduled + RT-added) to at most
        ``limit`` rows per stop, nearest-departure-first — RT-added rows are
        not exempt.

        ``now_utc`` exists for deterministic testing; omit it in production.
        """
        now = now_utc or datetime.now(UTC)
        scheduled = await asyncio.to_thread(
            self._index.upcoming_departures, stop_ids, route_ids, now, lookahead, limit
        )
        stop_names = await asyncio.to_thread(self._index.stop_names)
        route_names = await asyncio.to_thread(self._index.route_display_names)
        predictions: dict[tuple[str, str], StopPrediction] = {}
        canceled: set[str] = set()
        added_rows: list[AddedStopTime] = []
        for message in await self._fetch_entity_messages(EntityType.TRIP_UPDATES):
            updates = trip_updates_from_message(message)
            # Deliberate tiebreak: when multiple TU-capable sibling feeds
            # report the same (trip, stop), the last feed in catalog order
            # wins (no freshness reconciliation in v1).
            predictions.update(updates.predictions)
            canceled |= updates.canceled_trips
            added_rows.extend(updates.added)

        arrivals: list[StopArrival] = []
        for dep in scheduled:
            if dep.trip_id in canceled:
                continue
            prediction = predictions.get((dep.trip_id, dep.stop_id))
            arrivals.append(
                StopArrival(
                    stop_id=dep.stop_id,
                    stop_name=stop_names.get(dep.stop_id),
                    route_id=dep.route_id,
                    route_name=route_names.get(dep.route_id),
                    trip_id=dep.trip_id,
                    headsign=dep.headsign,
                    scheduled_arrival=dep.arrival,
                    scheduled_departure=dep.departure,
                    predicted_arrival=prediction.arrival if prediction else None,
                    predicted_departure=prediction.departure if prediction else None,
                    delay_seconds=prediction.delay_seconds if prediction else None,
                    realtime=prediction is not None,
                    vehicle_id=prediction.vehicle_id if prediction else None,
                )
            )
        wanted_stops = set(stop_ids)
        for row in added_rows:
            if row.stop_id not in wanted_stops:
                continue
            if route_ids and row.route_id not in route_ids:
                continue
            arrivals.append(
                StopArrival(
                    stop_id=row.stop_id,
                    stop_name=stop_names.get(row.stop_id),
                    route_id=row.route_id,
                    route_name=route_names.get(row.route_id) if row.route_id else None,
                    trip_id=row.trip_id,
                    headsign=None,
                    scheduled_arrival=None,
                    scheduled_departure=None,
                    predicted_arrival=row.arrival,
                    predicted_departure=row.departure,
                    delay_seconds=None,
                    realtime=True,
                    vehicle_id=row.vehicle_id,
                )
            )
        # Total sort key, matching upcoming_departures: effective time alone
        # ties frequently, so trip_id/stop_id break ties deterministically.
        arrivals.sort(
            key=lambda a: (
                a.predicted_departure or a.scheduled_departure or now,
                a.trip_id or "",
                a.stop_id,
            )
        )
        # Scheduled rows already respect `limit` per stop via
        # upcoming_departures's per_stop_limit, but RT-added rows don't go
        # through that query — cap the merged (scheduled + added) result per
        # stop here too, same nearest-first truncation pattern.
        limited: list[StopArrival] = []
        per_stop_counts: dict[str, int] = {}
        for arrival in arrivals:
            count = per_stop_counts.get(arrival.stop_id, 0)
            if count < limit:
                limited.append(arrival)
                per_stop_counts[arrival.stop_id] = count + 1
        return limited

    async def get_vehicles(self) -> list[VehiclePosition]:
        """Live vehicle positions across the feed's VP-capable RT sources."""
        messages = await self._fetch_entity_messages(EntityType.VEHICLE_POSITIONS)
        trip_ids = sorted(
            {
                entity.vehicle.trip.trip_id
                for message in messages
                for entity in message.entity
                if entity.HasField("vehicle") and entity.vehicle.trip.trip_id
            }
        )
        trip_routes = await asyncio.to_thread(self._index.routes_for_trips, trip_ids)
        route_names = await asyncio.to_thread(self._index.route_display_names)
        vehicles: list[VehiclePosition] = []
        for message in messages:
            vehicles.extend(
                vehicles_from_message(
                    message, route_names=route_names, trip_routes=trip_routes
                )
            )
        return vehicles

    async def get_alerts(self) -> list[ServiceAlert]:
        """Service alerts across the feed's SA-capable RT sources."""
        messages = await self._fetch_entity_messages(EntityType.SERVICE_ALERTS)
        alerts: list[ServiceAlert] = []
        for message in messages:
            alerts.extend(alerts_from_message(message))
        return alerts

    async def refresh_static(self) -> bool:
        """Re-check the catalog; rebuild the index only on a new dataset.

        Returns True if the index was rebuilt. Stale-while-revalidate: the
        old index keeps serving until the new one is ready, then swaps.
        """
        fresh = await self._client.catalog.get_gtfs_feed(self.static_feed_id)
        dataset = fresh.latest_dataset
        if dataset is None or not dataset.id or dataset.id == self._index.dataset_id:
            return False
        new_index = await self._ensure_index(self._client, fresh)
        old_index, self._index = self._index, new_index
        self._static_feed = fresh
        self.stops = await asyncio.to_thread(new_index.stops)
        self.routes = await asyncio.to_thread(new_index.routes)
        await asyncio.to_thread(old_index.close)
        return True

    def stops_in(self, zone: Circle) -> list[Stop]:
        """Return stops within a circular zone (config-flow stop picker)."""
        return [
            stop
            for stop in self.stops
            if stop.latitude is not None
            and stop.longitude is not None
            and in_circle(zone, stop.latitude, stop.longitude)
        ]

    async def routes_serving(self, stop_id: str) -> list[Route]:
        """Routes with scheduled service at the stop (route-filter picker)."""
        return await asyncio.to_thread(self._index.routes_serving, stop_id)

    async def headsigns_serving(
        self, stop_id: str, route_id: str | None = None
    ) -> list[str]:
        """Distinct headsigns at the stop (direction-filter picker options)."""
        return await asyncio.to_thread(self._index.headsigns_serving, stop_id, route_id)

    def close(self) -> None:
        """Release the SQLite connection."""
        self._index.close()
