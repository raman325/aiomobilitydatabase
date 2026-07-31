"""Facade client that hands out per-feed handles."""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Self

import aiohttp

from ..client import MobilityDatabaseClient
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
    """

    def __init__(
        self,
        refresh_token: str,
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
        """The underlying Mobility Database catalog client (shared session)."""
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
        storage at a different feed). No-op without a cache_dir. Raises
        ``ValueError`` if ``feed_id`` would resolve outside the cache
        directory (e.g. an absolute path or one containing ``..``
        components). Missing targets are silently skipped; other
        filesystem errors (e.g. permissions) propagate to the caller.
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
        """Resolve a GTFS or GTFS-RT feed ID into a TransitFeedHandle."""
        return await TransitFeedHandle.create(self, feed_id, api_key, on_progress)

    async def get_gbfs_feed(self, feed_id: str) -> GbfsFeedHandle:
        """Resolve a GBFS feed ID into a GbfsFeedHandle."""
        return await GbfsFeedHandle.create(self, feed_id)
