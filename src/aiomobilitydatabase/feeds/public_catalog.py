"""Token-free catalog lookups from the Mobility Database's public export."""

from __future__ import annotations

import asyncio
import ipaddress
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from types import TracebackType
from typing import Self

import aiohttp
from yarl import URL

from .._http import REQUEST_OPTIONS, URL_ERRORS, require_text
from .._transport import Answer

# Deliberate friend access: the argument checks are shared with the API
# client so both answer a bad argument alike.
from ..client import (
    _require_catalog_session,
    _require_id,
    _text,
    _timeout_seconds,
    _typed,
)
from ..exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseNotFoundError,
    MobilityDatabaseRateLimitError,
)
from ..models import (
    BoundingFilterMethod,
    EntityType,
    Feed,
    GbfsFeed,
    GtfsFeed,
    GtfsRtFeed,
)
from ._bounds import Box, matches
from ._catalog_csv import CatalogFeed, CatalogFormatError, CatalogRows, read_catalog
from ._freshness import (
    FreshnessCache,
    Response,
    Validators,
    header_fields,
    request_fields,
)
from .const import DEFAULT_TIMEOUT_SECONDS, PUBLIC_CATALOG_URL

# The API's paging bounds (mobility-feed-api, feeds_api.py: ``limit`` is
# ``Query(N, ge=0, le=N)``, ``offset`` ``ge=0``): each endpoint's default
# limit is also its largest.
_GTFS_FEEDS_MAX_LIMIT = 2500
_GTFS_RT_FEEDS_MAX_LIMIT = 1000
_GBFS_FEEDS_MAX_LIMIT = 500


def _require_export_url(url: object) -> str:
    """Return ``url`` if the export can be fetched from it; else raise.

    An http(s) URL with a host, no credentials and no fragment. Plain http
    only on loopback: the export decides which feed URLs a caller fetches.
    """
    if not isinstance(url, str):
        raise TypeError("url must be a str")
    host: str | None = None
    try:
        parsed = URL(url)
        host = parsed.host
    except URL_ERRORS:
        parsed = None
    if parsed is None:  # outside the except: the error would quote the URL
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


def _is_loopback(host: str) -> bool:
    """Whether a host names this machine (where plain http stays local)."""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


async def _exchange(
    session: aiohttp.ClientSession,
    url: str,
    *,
    client_timeout: aiohttp.ClientTimeout,
    headers: Mapping[str, str],
) -> tuple[Answer, dict[str, tuple[str, ...]]]:
    """GET the export whole, never following redirects; also its header fields.

    The catalog client's :func:`~aiomobilitydatabase._transport.exchange`,
    plus the header fields a cache keeps (R2 fetches as the feeds do).
    """
    if session.closed:
        raise RuntimeError("the session is closed")  # the caller's, not the API's
    try:
        async with session.request(
            "GET",
            url,
            timeout=client_timeout,
            headers=headers,
            **REQUEST_OPTIONS,
        ) as resp:
            body: bytes | None
            try:
                body = await resp.read()
            except aiohttp.ClientPayloadError:
                body = None
            header = resp.headers.get("Content-Type", "")
            media_type = header.split(";", 1)[0].strip(" \t").lower()
            return Answer(resp.status, media_type, body), header_fields(resp.headers)
    except Exception as err:  # the boundary: see _transport's docstring
        failure = type(err).__name__
    # Raised outside the except, so the original isn't even the context.
    raise MobilityDatabaseConnectionError(f"Public catalog request failed ({failure})")


class MobilityDatabasePublicCatalog:
    r"""The Mobility Database catalog's public export, without an account.

    Reads ``feeds_v2.csv`` (MobilityData regenerates it daily) and answers
    with the catalog API's own models and filter semantics. A field holds
    the value the API returns for the same feed, or None where the export
    doesn't carry it, with these differences:

    - ``locations`` is always None, and there are no location filters: the
      export's location is usually the one extracted from the feed's stops,
      not the registered locations the API returns and filters on.
    - ``seasonal`` isn't in the export: it holds the model's default
      (False), which says nothing about the feed.
    - ``get_gbfs_feeds`` filters on the system's bounding box (the API has
      no such filter) and has no ``system_id`` or ``version`` filter.
    - Results are ordered by provider, then id, by code point (the API's
      GTFS list sorts by its database's collation; its GBFS list sets no
      order); ``entity_types`` and ``feed_references`` are in the export's
      order.
    - The export holds published feeds only: the API's GBFS list doesn't
      filter on publication, and its get-by-id endpoints may return an
      unpublished feed to some accounts.
    - The API's GBFS list applies ``limit`` to its rows joined with each
      system's versions and endpoints, so its pages can hold fewer feeds
      than ``limit``; here a page holds ``limit`` feeds.
    - An export without any GBFS rows (MobilityData's carries none until
      its GBFS export, mobility-feed-api #1888, is deployed) can't say
      whether a system exists: ``get_gbfs_feeds`` and ``get_gbfs_feed`` raise
      :class:`MobilityDatabaseConnectionError` rather than answer ``[]`` or
      not found. With GBFS rows present, no match is ``[]`` as in the API.
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
    - A ``limit`` past the endpoint's maximum (also its default: 2500 GTFS,
      1000 GTFS Realtime, 500 GBFS feeds) or below 0, or a negative
      ``offset``, raises ValueError before any request, where the API
      answers 422 (a :class:`MobilityDatabaseApiError`).
    - ``provider`` and ``producer_url`` match as the API's ``ILIKE`` does
      (see :func:`_ilike`), but compare case by Python's ``str.lower``,
      where Postgres uses its locale's ``lower()``. A value with ``%`` that
      ends in a lone backslash (``e%\``) matches a literal backslash here,
      where the API answers an error.
    - The export's own status maps as the client's answers do, but for a
      404 and a 401: a missing export (404) or a 401 is a
      :class:`MobilityDatabaseApiError` with that status (the client maps
      a 401 to :class:`MobilityDatabaseAuthenticationError`, and
      :class:`MobilityDatabaseNotFoundError` means no such feed). A 429 is a
      :class:`MobilityDatabaseRateLimitError`.

    As the API, ``get_gtfs_rt_feeds`` leaves out a feed without entity
    types (its query inner-joins them; a type this library doesn't know
    still counts), which ``get_gtfs_rt_feed`` returns; and
    ``get_gbfs_feeds(limit=0)`` returns every system (the API applies a
    GBFS limit only when it is truthy), where the GTFS lists return none.

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
        """Initialize. Performs no I/O.

        Raises ``TypeError`` for a ``url`` that isn't a ``str`` or a
        ``request_timeout`` that isn't a number, and ``ValueError`` for a
        ``url`` the export can't be fetched from (see
        :func:`_require_export_url`), a ``request_timeout`` that isn't a
        finite positive number, or an injected session whose defaults would
        change what the catalog sends: an ``auth``, a ``base_url``, a
        credential default or a default header HTTP can't carry.
        """
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
        """Close the underlying session if this catalog owns it. Idempotent."""
        if (
            self._owns_session
            and self._session is not None
            and not self._session.closed
        ):
            await self._session.close()
            self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Return the session, lazily creating an owned one if needed."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _rows(self) -> CatalogRows:
        """Return the export, from the cache while fresh."""
        # No session yet: _fetch makes a plain one, whose request carries no
        # controlled fields; a cache hit then needs no session at all.
        session = self._session
        fields = {} if session is None else request_fields(session, self._url, {})
        return await self._cache.get(self._url, self._fetch, request_fields=fields)

    async def _fetch(self, *, validators: Validators | None) -> Response[CatalogRows]:
        """GET the export; conditional when ``validators`` is given."""
        requested_at = datetime.now(UTC)
        answer, fields = await _exchange(
            self._get_session(),
            self._url,
            client_timeout=self._timeout,
            headers={} if validators is None else validators.request_headers(),
        )
        received_at = datetime.now(UTC)
        if answer.status == HTTPStatus.NOT_MODIFIED and validators is not None:
            return Response(
                value=None,
                fields=fields,
                requested_at=requested_at,
                received_at=received_at,
                url=self._url,
            )
        if answer.status == HTTPStatus.TOO_MANY_REQUESTS:
            raise MobilityDatabaseRateLimitError("Public catalog rate limit exceeded")
        # A 404 stays an API error: NotFound means no such feed.
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
                fields=fields,
                requested_at=requested_at,
                received_at=received_at,
                url=self._url,
            )
        raise MobilityDatabaseConnectionError(f"Public catalog unreadable: {problem}")

    @_typed
    async def get_gtfs_feed(self, feed_id: str) -> GtfsFeed:
        """Get a single GTFS feed by ID."""
        key = _require_id(feed_id)  # as the client's, before any request
        return _find(await self._rows(), key, GtfsFeed)

    @_typed
    async def get_gtfs_rt_feed(self, feed_id: str) -> GtfsRtFeed:
        """Get a single GTFS Realtime feed by ID."""
        key = _require_id(feed_id)  # as the client's, before any request
        return _find(await self._rows(), key, GtfsRtFeed)

    @_typed
    async def get_gbfs_feed(self, feed_id: str) -> GbfsFeed:
        """Get a single GBFS feed by ID."""
        key = _require_id(feed_id)  # as the client's, before any request
        rows = await self._rows()
        _gbfs(rows)
        return _find(rows, key, GbfsFeed)

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
            max_limit=_GTFS_FEEDS_MAX_LIMIT,
            provider=provider,
            producer_url=producer_url,
            latitudes=dataset_latitudes,
            longitudes=dataset_longitudes,
            method=bounding_filter_method,
            is_official=is_official,
        )
        rows = await self._rows()
        return query.select(rows.gtfs, rows.boxes)

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
            max_limit=_GTFS_RT_FEEDS_MAX_LIMIT,
            provider=provider,
            producer_url=producer_url,
            entity_types=entity_types,
            is_official=is_official,
        )
        # The API's list inner-joins the entity types: a feed without any
        # isn't listed, though get_gtfs_rt_feed returns it.
        rows = await self._rows()
        return query.select(rows.gtfs_rt_listed, rows.boxes)

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
            max_limit=_GBFS_FEEDS_MAX_LIMIT,
            provider=provider,
            producer_url=producer_url,
            latitudes=dataset_latitudes,
            longitudes=dataset_longitudes,
            method=bounding_filter_method,
            zero_is_no_limit=True,
        )
        rows = await self._rows()
        return query.select(_gbfs(rows), rows.boxes)


def _find[F: CatalogFeed](rows: CatalogRows, feed_id: str, kind: type[F]) -> F:
    """Return the feed with this id and type, or raise not found."""
    feed = rows.by_id.get(feed_id)
    # The three feed models are siblings: an instance is of exactly one.
    if not isinstance(feed, kind):
        raise MobilityDatabaseNotFoundError("Not found")
    return feed


def _gbfs(rows: CatalogRows) -> tuple[GbfsFeed, ...]:
    """Return the export's GBFS feeds; raise when it carries none at all.

    An export without a GBFS section (one written before mobility-feed-api
    exported GBFS) can't tell whether a system exists: neither an empty
    list nor NotFound would be true.
    """
    if not rows.gbfs:
        raise MobilityDatabaseConnectionError(
            "Public catalog export carries no GBFS feeds"
        )
    return rows.gbfs


def _needle(value: str | None) -> re.Pattern[str] | None:
    """Return a string filter as the API normalizes it: trimmed; empty is none."""
    if value is None:
        return None
    # As the client's encoder: plain str text, else ValueError.
    trimmed = require_text(str.__str__(value), "a query parameter").strip()
    return _ilike(trimmed) if trimmed else None


def _ilike(value: str) -> re.Pattern[str]:
    """Return the pattern the API's ``ILIKE`` filter matches, lower-cased.

    fastapi-filter's ``__ilike`` wraps a value without ``%`` in ``%...%``
    and takes one with ``%`` as given, anchored. Postgres then reads ``%``
    as any run of characters, ``_`` as any one, and a backslash as escaping
    the next character; a trailing lone backslash, an error there, is a
    literal backslash here. The pattern matches a whole lower-cased string
    (``fullmatch``).

    Not ``%`` as ``.*``: backtracking through those takes time growing as
    the value's length to the power of their count (seconds to minutes for
    a short filter over the export). The ``%`` split the value into
    fixed-width segments (literals and ``_``); the first must start the
    string and the last end it, and each one between is taken at its first
    occurrence after the previous -- an atomic lazy group, never revisited.
    The first occurrence is always right: it leaves the most string for the
    rest. Linear in the value.
    """
    if "%" not in value:
        value = f"%{value}%"
    segments: list[list[str]] = [[]]
    characters = iter(value.lower())
    for character in characters:
        if character == "\\":
            segments[-1].append(re.escape(next(characters, "\\")))
        elif character == "%":
            segments.append([])
        elif character == "_":
            segments[-1].append(".")
        else:
            segments[-1].append(re.escape(character))
    texts = ["".join(segment) for segment in segments]
    if len(texts) == 1:  # every % escaped (a\%): the whole string
        return re.compile(texts[0], re.DOTALL)
    first, *middle, last = texts
    between = "".join(f"(?>.*?{segment})" for segment in middle)
    return re.compile(f"{first}{between}.*{last}", re.DOTALL)


def _like(pattern: re.Pattern[str] | None, value: str | None) -> bool:
    """Whether a value passes a string filter (none: every value does).

    NULL never matches ``ILIKE``: a feed without the value fails any filter.
    """
    if pattern is None:
        return True
    return value is not None and pattern.fullmatch(value.lower()) is not None


@dataclass(frozen=True, slots=True, kw_only=True)
class _Query:
    """One list call's filters, checked before any request; see :meth:`of`."""

    start: int
    stop: int | None
    provider: re.Pattern[str] | None
    producer_url: re.Pattern[str] | None
    box: Box | None
    method: BoundingFilterMethod
    is_official: bool | None
    entity_types: frozenset[EntityType] | None

    @classmethod
    def of(
        cls,
        *,
        limit: int | None,
        offset: int | None,
        max_limit: int,
        provider: str | None,
        producer_url: str | None,
        latitudes: tuple[float, float] | None = None,
        longitudes: tuple[float, float] | None = None,
        method: BoundingFilterMethod | None = None,
        is_official: bool | None = None,
        entity_types: Sequence[EntityType] | None = None,
        zero_is_no_limit: bool = False,
    ) -> _Query:
        """Check a list call's filters and normalize them as the API does.

        ``max_limit`` is the endpoint's largest ``limit``, and its default.
        ``zero_is_no_limit``: a ``limit`` of 0 returns every feed (the API's
        GBFS list applies a limit only when it is truthy), where otherwise
        it returns none (SQL's ``LIMIT 0``).
        """
        if limit is None:
            limit = max_limit
        if not 0 <= limit <= max_limit:
            raise ValueError(f"limit must be from 0 to {max_limit}")
        if offset is not None and offset < 0:
            raise ValueError("offset must not be negative")
        start = offset or 0
        # As the client, which encodes every given range: one alone is
        # checked too, though the API ignores it.
        for given in (latitudes, longitudes):
            if given is not None and not _finite(given):
                raise ValueError("a query parameter is no finite number")
        box = None
        # As the API: the box applies only with both ranges given.
        if latitudes is not None and longitudes is not None:
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
            stop=None if zero_is_no_limit and limit == 0 else start + limit,
            provider=_needle(provider),
            producer_url=_needle(producer_url),
            box=box,
            method=method or BoundingFilterMethod.COMPLETELY_ENCLOSED,
            is_official=is_official,
            entity_types=frozenset(entity_types) if entity_types else None,
        )

    def _keeps(self, feed: CatalogFeed, boxes: Mapping[str, Box]) -> bool:
        source = feed.source_info
        if not (
            _like(self.provider, feed.provider)
            and _like(self.producer_url, source.producer_url if source else None)
        ):
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
        box = self.box
        # A GTFS Realtime feed has no box: the API has no such filter for it.
        if box is None or isinstance(feed, GtfsRtFeed):
            return True
        # Every export feed has an id; the model allows None.
        feed_box = None if feed.id is None else boxes.get(feed.id)
        return feed_box is not None and matches(self.method, feed_box, box)

    def select[F: CatalogFeed](
        self, feeds: Sequence[F], boxes: Mapping[str, Box]
    ) -> list[F]:
        """Return the feeds that pass, then apply ``offset`` and ``limit``.

        ``boxes`` are the feeds' bounding boxes by id (see
        :class:`CatalogRows`).
        """
        kept = [feed for feed in feeds if self._keeps(feed, boxes)]
        return kept[self.start : self.stop]


def _finite(values: tuple[float, float]) -> bool:
    """Whether both values are finite; an int past float's range is not."""
    try:
        return all(math.isfinite(value) for value in values)
    except OverflowError:
        return False
