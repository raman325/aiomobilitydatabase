"""Token-free catalog lookups from the Mobility Database's public export."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Sequence
from datetime import UTC, datetime
from http import HTTPStatus
from types import TracebackType
from typing import Self

import aiohttp
from yarl import URL

from ._bounds import Box, matches
from ._catalog_csv import CatalogFeed, CatalogFormatError, CatalogRows, read_catalog
from ._freshness import FreshnessCache, Response, Validators, request_fields
from ._http import URL_ERRORS
from ._transport import exchange
from .client import (
    _is_loopback,
    _require_catalog_session,
    _text,
    _timeout_seconds,
    _typed,
)
from .const import DEFAULT_TIMEOUT_SECONDS, PUBLIC_CATALOG_URL
from .exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseNotFoundError,
)
from .models import (
    BoundingFilterMethod,
    EntityType,
    Feed,
    GbfsFeed,
    GtfsFeed,
    GtfsRtFeed,
)


def _require_export_url(url: object) -> str:
    """Return ``url`` if the export can be fetched from it; else raise.

    An http(s) URL with a host, no credentials and no fragment. Plain http
    only on loopback: the export decides which feed URLs a caller fetches.
    """
    if not isinstance(url, str):
        raise TypeError("url must be a str")
    try:
        parsed = URL(url)
        host = parsed.host
    except URL_ERRORS:
        parsed = None
    if parsed is None:  # outside the except: the error would ride along
        raise ValueError("url is no URL")
    if parsed.scheme not in ("http", "https") or not host:
        raise ValueError("url must be an http(s) URL with a host")
    if parsed.user is not None or parsed.raw_password is not None:
        raise ValueError("url must not hold credentials")
    if parsed.raw_fragment or "#" in url:
        raise ValueError("url must not hold a fragment")
    if parsed.scheme == "http" and not _is_loopback(host):
        raise ValueError("url must be https")
    return url


class MobilityDatabasePublicCatalog:
    """The Mobility Database catalog's public export, without an account.

    Reads ``feeds_v2.csv`` (MobilityData regenerates it daily) and answers
    with the catalog API's own models and filter semantics. A field holds
    the value the API returns for the same feed, or None where the export
    doesn't carry it, with these differences:

    - ``locations`` is always None, and there are no location filters: the
      export's location is usually the one extracted from the feed's stops,
      not the registered locations the API returns and filters on.
    - ``seasonal`` is None (unknown): the export doesn't carry it.
    - ``get_gbfs_feeds`` filters on the system's bounding box (the API has
      no such filter) and has no ``system_id`` or ``version`` filter.
    - Results are ordered by provider, then id, by code point (the API
      sorts by its database's collation); ``entity_types`` and
      ``feed_references`` are in the export's order.
    - An empty cell is None: the export writes an empty string and a
      missing value alike, so where the database holds ``""`` the API
      returns ``""``.
    - The bounding filters test the feed's bounding box, the one the API
      returns. The API's GTFS filter instead matches a feed when any of its
      datasets' boxes (past ones too) passes, so a feed whose coverage moved
      can differ. A box of zero width or height (a single-stop feed, or a
      query range whose ends are equal) is the point or segment it covers;
      the API's answer for one depends on its GEOS version (see
      :mod:`._bounds`), so it may differ.

    The export is cached as HTTP allows (RFC 9111: ``max-age``, revalidated
    with its ETag), shared by concurrent callers, and parsed off the event
    loop. Session handling follows :class:`MobilityDatabaseClient`.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession | None = None,
        *,
        url: str = PUBLIC_CATALOG_URL,
        request_timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize. Performs no I/O."""
        self._url = _require_export_url(url)
        seconds = _timeout_seconds(request_timeout)
        if session is not None:
            _require_catalog_session(session)
        self._session = session
        self._owns_session = session is None
        self._timeout = aiohttp.ClientTimeout(total=seconds)
        self._cache: FreshnessCache[CatalogRows] = FreshnessCache()

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
        """Close the session if this catalog owns it. Idempotent."""
        if (
            self._owns_session
            and self._session is not None
            and not self._session.closed
        ):
            await self._session.close()
            self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _rows(self) -> CatalogRows:
        """Return the export, from the cache while fresh."""
        return await self._cache.get(
            self._url,
            self._fetch,
            request_fields=request_fields(self._get_session(), self._url, {}),
        )

    async def _fetch(self, *, validators: Validators | None) -> Response[CatalogRows]:
        """GET the export; conditional when ``validators`` is given."""
        requested_at = datetime.now(UTC)
        answer = await exchange(
            self._get_session(),
            "GET",
            self._url,
            what="Public catalog request",
            client_timeout=self._timeout,
            headers={} if validators is None else validators.request_headers(),
        )
        received_at = datetime.now(UTC)
        if answer.status == HTTPStatus.NOT_MODIFIED and validators is not None:
            return Response(
                value=None,
                fields=answer.fields,
                requested_at=requested_at,
                received_at=received_at,
                url=self._url,
            )
        if answer.status not in (
            HTTPStatus.OK,
            HTTPStatus.NON_AUTHORITATIVE_INFORMATION,
        ):
            raise MobilityDatabaseApiError(answer.status, _text(answer))
        if answer.body is None:
            raise MobilityDatabaseConnectionError(
                "Public catalog's body couldn't be read"
            )
        try:
            rows = await asyncio.to_thread(read_catalog, answer.body)
        except CatalogFormatError as err:
            problem = str(err)
        else:
            return Response(
                value=rows,
                fields=answer.fields,
                requested_at=requested_at,
                received_at=received_at,
                url=self._url,
            )
        raise MobilityDatabaseConnectionError(f"Public catalog unreadable: {problem}")

    @_typed
    async def get_gtfs_feed(self, feed_id: str) -> GtfsFeed:
        """Get a single GTFS feed by ID."""
        return _find(await self._rows(), feed_id, GtfsFeed)

    @_typed
    async def get_gtfs_rt_feed(self, feed_id: str) -> GtfsRtFeed:
        """Get a single GTFS Realtime feed by ID."""
        return _find(await self._rows(), feed_id, GtfsRtFeed)

    @_typed
    async def get_gbfs_feed(self, feed_id: str) -> GbfsFeed:
        """Get a single GBFS feed by ID."""
        return _find(await self._rows(), feed_id, GbfsFeed)

    @_typed
    async def get_gtfs_feeds(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        provider: str | None = None,
        producer_url: str | None = None,
        dataset_latitudes: tuple[float, float] | None = None,
        dataset_longitudes: tuple[float, float] | None = None,
        bounding_filter_method: BoundingFilterMethod | None = None,
        is_official: bool | None = None,
    ) -> list[GtfsFeed]:
        """Get GTFS schedule feeds.

        Filters as :meth:`MobilityDatabaseClient.get_gtfs_feeds`.
        """
        query = _Query.of(
            limit=limit,
            offset=offset,
            provider=provider,
            producer_url=producer_url,
            latitudes=dataset_latitudes,
            longitudes=dataset_longitudes,
            method=bounding_filter_method,
            is_official=is_official,
        )
        return query.select((await self._rows()).gtfs)

    @_typed
    async def get_gtfs_rt_feeds(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        provider: str | None = None,
        producer_url: str | None = None,
        entity_types: list[EntityType] | None = None,
        is_official: bool | None = None,
    ) -> list[GtfsRtFeed]:
        """Get GTFS Realtime feeds.

        Filters as :meth:`MobilityDatabaseClient.get_gtfs_rt_feeds`.
        """
        query = _Query.of(
            limit=limit,
            offset=offset,
            provider=provider,
            producer_url=producer_url,
            entity_types=entity_types,
            is_official=is_official,
        )
        return query.select((await self._rows()).gtfs_rt)

    @_typed
    async def get_gbfs_feeds(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        provider: str | None = None,
        producer_url: str | None = None,
        dataset_latitudes: tuple[float, float] | None = None,
        dataset_longitudes: tuple[float, float] | None = None,
        bounding_filter_method: BoundingFilterMethod | None = None,
    ) -> list[GbfsFeed]:
        """Get GBFS feeds; the bounding filters apply to the system's box."""
        query = _Query.of(
            limit=limit,
            offset=offset,
            provider=provider,
            producer_url=producer_url,
            latitudes=dataset_latitudes,
            longitudes=dataset_longitudes,
            method=bounding_filter_method,
        )
        return query.select((await self._rows()).gbfs)


def _find[F: CatalogFeed](rows: CatalogRows, feed_id: str, kind: type[F]) -> F:
    """Return the feed with this id and type, or raise not found."""
    feed = rows.by_id.get(feed_id)
    # The three feed models are siblings: an instance is of exactly one.
    if not isinstance(feed, kind):
        raise MobilityDatabaseNotFoundError("Not found")
    return feed


def _needle(value: str | None) -> str | None:
    """Return a string filter as the API normalizes it: trimmed; empty is none."""
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed.lower() if trimmed else None


class _Query:
    """One list call's filters, checked before any request."""

    def __init__(
        self,
        *,
        start: int,
        stop: int | None,
        provider: str | None,
        producer_url: str | None,
        box: Box | None,
        method: BoundingFilterMethod,
        is_official: bool | None,
        entity_types: frozenset[EntityType] | None,
    ) -> None:
        self.start = start
        self.stop = stop
        self.provider = provider
        self.producer_url = producer_url
        self.box = box
        self.method = method
        self.is_official = is_official
        self.entity_types = entity_types

    @classmethod
    def of(
        cls,
        *,
        limit: int | None,
        offset: int | None,
        provider: str | None,
        producer_url: str | None,
        latitudes: tuple[float, float] | None = None,
        longitudes: tuple[float, float] | None = None,
        method: BoundingFilterMethod | None = None,
        is_official: bool | None = None,
        entity_types: Sequence[EntityType] | None = None,
    ) -> _Query:
        """Check a list call's filters and normalize them as the API does."""
        if limit is not None and limit < 0:
            raise ValueError("limit must not be negative")
        if offset is not None and offset < 0:
            raise ValueError("offset must not be negative")
        start = offset or 0
        box = None
        # As the API: the box applies only with both ranges given.
        if latitudes is not None and longitudes is not None:
            if not all(math.isfinite(value) for value in (*latitudes, *longitudes)):
                raise ValueError("a query parameter is no finite number")
            (lat_a, lat_b), (lon_a, lon_b) = latitudes, longitudes
            # PostGIS builds the same rectangle from either order.
            box = Box(
                min(lat_a, lat_b),
                max(lat_a, lat_b),
                min(lon_a, lon_b),
                max(lon_a, lon_b),
            )
        return cls(
            start=start,
            stop=None if limit is None else start + limit,
            provider=_needle(provider),
            producer_url=_needle(producer_url),
            box=box,
            method=method or BoundingFilterMethod.COMPLETELY_ENCLOSED,
            is_official=is_official,
            entity_types=frozenset(entity_types) if entity_types else None,
        )

    def _keeps(self, feed: CatalogFeed) -> bool:
        if (
            self.provider is not None
            and self.provider not in (feed.provider or "").lower()
        ):
            return False
        if self.producer_url is not None:
            url = feed.source_info.producer_url if feed.source_info else None
            if self.producer_url not in (url or "").lower():
                return False
        # The API's is_official=False keeps False and unknown.
        if (
            self.is_official is not None
            and isinstance(feed, Feed)
            and (feed.official is True) is not self.is_official
        ):
            return False
        if (
            self.entity_types is not None
            and isinstance(feed, GtfsRtFeed)
            and not self.entity_types.intersection(feed.entity_types or ())
        ):
            return False
        return (
            self.box is None
            or not isinstance(feed, GtfsFeed | GbfsFeed)
            or self._inside(feed, self.box)
        )

    def _inside(self, feed: GtfsFeed | GbfsFeed, box: Box) -> bool:
        """Whether the feed's bounding box passes the bounding filter."""
        bounds = feed.bounding_box
        if (
            bounds is None
            or bounds.minimum_latitude is None
            or bounds.maximum_latitude is None
            or bounds.minimum_longitude is None
            or bounds.maximum_longitude is None
        ):
            return False
        feed_box = Box(
            bounds.minimum_latitude,
            bounds.maximum_latitude,
            bounds.minimum_longitude,
            bounds.maximum_longitude,
        )
        return matches(self.method, feed_box, box)

    def select[F: CatalogFeed](self, feeds: Sequence[F]) -> list[F]:
        """Return the feeds that pass, then apply ``offset`` and ``limit``."""
        return [feed for feed in feeds if self._keeps(feed)][self.start : self.stop]
