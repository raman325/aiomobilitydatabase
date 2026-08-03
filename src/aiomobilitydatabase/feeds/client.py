"""Facade client that hands out per-feed handles."""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Callable, Mapping
from pathlib import Path
from types import TracebackType
from typing import Self

import aiohttp

from ..client import MobilityDatabaseClient
from ..exceptions import MobilityDatabaseError
from .const import DEFAULT_TIMEOUT_SECONDS
from .gbfs import GbfsFeedHandle
from .models import StaticBuildProgress
from .transit import TransitFeedHandle


class MobilityFeedsClient:
    """Entry point: wraps the catalog client and creates feed handles.

    If ``session`` is not provided, one is lazily created and owned (closed
    by :meth:`close`); an injected session is never closed. The inner
    catalog client always receives the shared session, so all HTTP —
    catalog, GTFS-RT, GBFS, zip downloads — flows through one pool.

    ``refresh_token`` is only needed for catalog operations (feed-ID
    resolution, search, :attr:`catalog`); the direct-URL methods
    (:meth:`get_transit_feed_from_urls`, :meth:`get_gbfs_feed_from_url`)
    never touch the catalog and work on a tokenless client.
    """

    def __init__(
        self,
        refresh_token: str | None = None,
        session: aiohttp.ClientSession | None = None,
        *,
        cache_dir: Path | str | None = None,
        base_url: str | None = None,
        request_timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize the client. Performs no I/O."""
        self._refresh_token = refresh_token
        self._session = session
        self._owns_session = session is None
        self._base_url = base_url
        self._timeout = request_timeout
        self._catalog: MobilityDatabaseClient | None = None
        self.cache_dir = Path(cache_dir) if cache_dir is not None else None

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Exit the async context manager, closing owned resources."""
        await self.close()

    async def close(self) -> None:
        """Close owned resources. Idempotent; injected sessions untouched."""
        if self._catalog is not None:
            await self._catalog.close()
            self._catalog = None
        if (
            self._owns_session
            and self._session is not None
            and not self._session.closed
        ):
            await self._session.close()
            self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Return the shared session, lazily creating an owned one."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    @property
    def catalog(self) -> MobilityDatabaseClient:
        """The underlying Mobility Database catalog client (shared session).

        Raises :class:`MobilityDatabaseError` when the client was
        constructed without a refresh token: only catalog operations need
        one, so the failure is raised here (at first catalog use) rather
        than at construction, keeping tokenless direct-URL usage valid.
        """
        if self._refresh_token is None:
            raise MobilityDatabaseError(
                "A refresh token is required for catalog operations"
            )
        if self._catalog is None:
            if self._base_url is not None:
                self._catalog = MobilityDatabaseClient(
                    self._refresh_token,
                    self._get_session(),
                    base_url=self._base_url,
                    request_timeout=self._timeout,
                )
            else:
                self._catalog = MobilityDatabaseClient(
                    self._refresh_token,
                    self._get_session(),
                    request_timeout=self._timeout,
                )
        return self._catalog

    @property
    def timeout_seconds(self) -> float:
        """Request timeout shared by handle fetches."""
        return self._timeout

    async def purge_cache(self, feed_id: str | None = None) -> None:
        """Delete cached static data for one feed, or all feeds when None.

        Consumers call this on config-entry removal (and before re-pointing
        storage at a different feed). ``feed_id`` is either a catalog feed
        ID or a direct handle's url-derived ``url-<sha256(url)[:16]>`` key
        — exactly what :attr:`TransitFeedHandle.static_feed_id` returns in
        both cases. No-op without a cache_dir. Raises ``ValueError`` if
        ``feed_id`` would resolve outside the cache directory (e.g. an
        absolute path or one containing ``..`` components). Missing
        targets are silently skipped; other filesystem errors (e.g.
        permissions) propagate to the caller.
        """
        if self.cache_dir is None:
            return
        if feed_id is not None:
            target = self.cache_dir / feed_id
            if not target.resolve().is_relative_to(self.cache_dir.resolve()):
                raise ValueError(f"feed_id {feed_id!r} escapes the cache directory")
            targets = [target]
        else:
            targets = [path for path in self.cache_dir.iterdir() if path.is_dir()]
        for target in targets:
            if not target.exists():
                continue
            await asyncio.to_thread(shutil.rmtree, target)

    async def get_transit_feed(
        self,
        feed_id: str,
        api_key: str | None = None,
        on_progress: Callable[[StaticBuildProgress], None] | None = None,
    ) -> TransitFeedHandle:
        """Resolve a GTFS or GTFS-RT feed ID into a TransitFeedHandle.

        ``on_progress``, if given, must not raise: it is called inline and
        is NOT wrapped in a try/except, so a raising callback aborts this
        call -- its exception propagates out of ``get_transit_feed`` rather
        than being silently swallowed (fail-fast, not best-effort). This
        holds cleanly during the download phase, where each call happens
        directly on this coroutine's task. During the index-build phase
        (which runs in a worker thread), progress events are marshaled back
        via ``loop.call_soon_threadsafe`` -- a raising callback there still
        surfaces (as an unhandled exception in the event loop, reported via
        the loop's exception handler) but asynchronously, after the
        callback's own scheduling, so it cannot abort a build already in
        progress.
        """
        return await TransitFeedHandle.create(self, feed_id, api_key, on_progress)

    async def get_transit_feed_from_urls(
        self,
        static_url: str,
        rt_urls: list[str] | None = None,
        *,
        headers: Mapping[str, str] | None = None,
        on_progress: Callable[[StaticBuildProgress], None] | None = None,
    ) -> TransitFeedHandle:
        """Build a TransitFeedHandle from user-supplied URLs (no catalog).

        Returns the same handle type as :meth:`get_transit_feed`, so
        consumers built against catalog handles work unchanged. Each
        ``rt_urls`` entry is synthesized into an RT source that advertises
        every entity type (capabilities are unknown without a catalog
        record; parsers yield nothing for types a producer doesn't
        publish). ``headers`` apply to the static download and every RT
        fetch for the handle.

        Dataset identity without a catalog: the static URL is HEAD-probed
        for an ETag (preferred) or Last-Modified validator; servers
        offering neither fall back to a sha256 of the downloaded bytes.
        The on-disk cache key is ``url-<sha256(static_url)[:16]>`` (also
        exposed as the handle's ``static_feed_id``, accepted by
        :meth:`purge_cache`). ``on_progress`` follows the same contract as
        :meth:`get_transit_feed`.
        """
        return await TransitFeedHandle.create_from_urls(
            self, static_url, rt_urls, headers, on_progress
        )

    async def get_gbfs_feed(self, feed_id: str) -> GbfsFeedHandle:
        """Resolve a GBFS feed ID into a GbfsFeedHandle."""
        return await GbfsFeedHandle.create(self, feed_id)

    async def get_gbfs_feed_from_url(
        self,
        discovery_url: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> GbfsFeedHandle:
        """Build a GbfsFeedHandle from a GBFS auto-discovery URL (no catalog).

        ``discovery_url`` is the system's standard ``gbfs.json`` entry
        point; its published feed list replaces catalog endpoint
        resolution. Returns the same handle type as :meth:`get_gbfs_feed`.
        ``headers`` apply to the discovery fetch and every document fetch
        for the handle.
        """
        return await GbfsFeedHandle.create_from_url(self, discovery_url, headers)
