"""Token-free feed lookups from the Mobility Database's public catalog export."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from itertools import islice
from typing import TYPE_CHECKING

from .._http import require_text

# Deliberate friend access: the argument checks are shared with the API
# client so both answer a bad argument alike.
from ..client import _require_id, _typed
from ..exceptions import MobilityDatabaseNotFoundError
from ..models import BoundingFilterMethod, EntityType
from ._bounds import Box, matches
from ._catalog_csv import CatalogFormatError, CatalogRows, PublicFeed, read_catalog
from ._freshness import (
    FreshnessCache,
    Response,
    Validators,
    header_fields,
    request_fields,
)
from ._freshness import hops as _hops
from ._redirects import get_withholding, network
from ._urls import (  # deliberate friend access: shared URL guard
    _require_http_url,
    redact,
)
from .exceptions import FeedParseError, SourceConnectionError
from .models import PublicGbfsFeed, PublicGtfsFeed, PublicGtfsRtFeed

if TYPE_CHECKING:
    from .client import MobilityFeedsClient

# The API's paging bounds (mobility-feed-api, feeds_api.py: ``limit`` is
# ``Query(N, ge=0, le=N)``, ``offset`` ``ge=0``): each endpoint's default
# limit is also its largest.
_GTFS_FEEDS_MAX_LIMIT = 2500
_GTFS_RT_FEEDS_MAX_LIMIT = 1000
_GBFS_FEEDS_MAX_LIMIT = 500
_CONTEXT = "public catalog URL"


class PublicCatalog:
    r"""The Mobility Database catalog's public export, without an account.

    Obtain it as :attr:`MobilityFeedsClient.public_catalog`. It reads
    ``feeds_v2.csv`` (MobilityData regenerates it daily) into
    :class:`~.models.PublicGtfsFeed`, :class:`~.models.PublicGtfsRtFeed`
    and :class:`~.models.PublicGbfsFeed` -- the export's columns that mean
    what the catalog API's same-named fields mean -- and filters as the API
    client's same-named methods do, with these differences:

    - No location filters, and ``get_gbfs_feeds`` filters on the system's
      bounding box (the API has no such filter) with no ``system_id`` or
      ``version`` filter.
    - Results are ordered by provider, then id, by code point (the API's
      GTFS list sorts by its database's collation; its GBFS list sets no
      order), so a ``limit``/``offset`` page can hold other feeds.
    - The bounding filters test the feed's bounding box; the API's GTFS
      filter matches a feed when any of its datasets' boxes (past ones too)
      passes. A box of zero width or height (a single-stop feed, or a query
      range whose ends are equal) may differ too: the API's answer depends
      on its GEOS version (see :mod:`._bounds`).
    - The export holds published feeds only: the API's GBFS list doesn't
      filter on publication, and its get-by-id endpoints may return an
      unpublished feed to some accounts. The API's GBFS list applies
      ``limit`` to its rows joined with each system's versions and
      endpoints, so its pages can hold fewer feeds; here a page holds
      ``limit`` feeds.
    - ``provider`` and ``producer_url`` match as the API's ``ILIKE`` (see
      :func:`_ilike`), comparing case by Python's ``str.lower`` where
      Postgres uses its locale's ``lower()``; a value with ``%`` ending in
      a lone backslash (``e%\``) matches a literal backslash here, where the
      API answers an error.
    - A ``limit`` past the endpoint's maximum (also its default: 2500 GTFS,
      1000 GTFS Realtime, 500 GBFS feeds) or below 0, or a negative
      ``offset``, raises ValueError before any request, where the API
      answers 422.

    As the API, ``get_gtfs_rt_feeds`` leaves out a feed without entity
    types (its query inner-joins them; a type this library doesn't know
    still counts), which ``get_gtfs_rt_feed`` returns; and
    ``get_gbfs_feeds(limit=0)`` returns every system (the API applies a
    GBFS limit only when it is truthy), where the GTFS lists return none.

    The export is fetched as the feeds layer fetches a GBFS document:
    redirects followed, cached as HTTP allows (RFC 9111), shared by
    concurrent callers, and parsed off the event loop. Errors are the feeds
    layer's: :class:`SourceConnectionError` when it can't be fetched (with
    the ``status`` of an error answer), :class:`FeedParseError` when it is
    no export -- or carries no GBFS feeds at all, for a GBFS lookup, which
    then can't say whether a system exists (MobilityData's carries none
    until mobility-feed-api #1888 is deployed). An id no feed of the asked
    type has is :class:`MobilityDatabaseNotFoundError`, as in the API.
    """

    def __init__(self, client: MobilityFeedsClient, url: str) -> None:
        """Read the export at ``url`` on ``client``'s session. No I/O."""
        self._client = client
        self._url = url
        self._cache: FreshnessCache[CatalogRows] = FreshnessCache()

    async def _rows(self) -> CatalogRows:
        """Return the export, from the cache while fresh."""
        _require_http_url(self._url, _CONTEXT)  # before request_fields parses it
        return await self._cache.get(
            self._url,
            self._fetch,
            request_fields=request_fields(
                self._client._get_session(),  # deliberate friend access
                self._url,
                {},
            ),
        )

    async def _fetch(self, *, validators: Validators | None) -> Response[CatalogRows]:
        """GET the export; conditional when ``validators`` is given.

        As :meth:`GbfsFeedHandle._fetch_json_document`: a 304 comes back
        with no value, and to an unconditional request raises.
        """
        url = self._url
        _require_http_url(url, _CONTEXT)
        session = self._client._get_session()  # deliberate friend access
        requested_at = datetime.now(UTC)
        shown = redact(url)
        async with get_withholding(
            session,
            url,
            params={},
            headers={},
            total=self._client.timeout_seconds,
            secret=None,
            conditional=None if validators is None else validators.request_headers(),
            conditional_url=None if validators is None else validators.url,
        ) as (resp, history):
            received_at = datetime.now(UTC)
            fields = header_fields(resp.headers)
            if resp.status == HTTPStatus.NOT_MODIFIED:
                if validators is None:
                    raise SourceConnectionError(
                        "Public catalog answered 304 to an unconditional request",
                        status=int(HTTPStatus.NOT_MODIFIED),
                        url=shown,
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
                    f"Public catalog error {resp.status}", status=resp.status, url=shown
                )
            if resp.status not in (
                HTTPStatus.OK,
                HTTPStatus.NON_AUTHORITATIVE_INFORMATION,
            ):
                raise SourceConnectionError(
                    f"Public catalog answered {resp.status} without an export",
                    status=resp.status,
                    url=shown,
                )
            raw = await network(resp.read(), url)
            hops = _hops(resp, history)
            answered = str(resp.url)
        try:
            rows: CatalogRows | None = await asyncio.to_thread(read_catalog, raw)
        except CatalogFormatError as err:
            problem = str(err)
            rows = None
        if rows is None:
            # Outside the except: nothing of the parse rides along.
            raise FeedParseError(f"Public catalog unreadable: {problem}", url=shown)
        return Response(
            value=rows,
            fields=fields,
            requested_at=requested_at,
            received_at=received_at,
            hops=hops,
            url=answered,
        )

    @_typed
    async def get_gtfs_feed(self, feed_id: str) -> PublicGtfsFeed:
        """Get a single GTFS feed by ID."""
        key = _require_id(feed_id)  # as the client's, before any request
        return _find(await self._rows(), key, PublicGtfsFeed)

    @_typed
    async def get_gtfs_rt_feed(self, feed_id: str) -> PublicGtfsRtFeed:
        """Get a single GTFS Realtime feed by ID."""
        key = _require_id(feed_id)  # as the client's, before any request
        return _find(await self._rows(), key, PublicGtfsRtFeed)

    @_typed
    async def get_gbfs_feed(self, feed_id: str) -> PublicGbfsFeed:
        """Get a single GBFS feed by ID."""
        key = _require_id(feed_id)  # as the client's, before any request
        rows = await self._rows()
        _gbfs(rows, self._url)
        return _find(rows, key, PublicGbfsFeed)

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
    ) -> list[PublicGtfsFeed]:
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
    ) -> list[PublicGtfsRtFeed]:
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
    ) -> list[PublicGbfsFeed]:
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
        return query.select(_gbfs(rows, self._url), rows.boxes)


def _find[F: PublicFeed](rows: CatalogRows, feed_id: str, kind: type[F]) -> F:
    """Return the feed with this id and type, or raise not found."""
    feed = rows.by_id.get(feed_id)
    # The three feed models are siblings: an instance is of exactly one.
    if not isinstance(feed, kind):
        raise MobilityDatabaseNotFoundError("Not found")
    return feed


def _gbfs(rows: CatalogRows, url: str) -> tuple[PublicGbfsFeed, ...]:
    """Return the export's GBFS feeds; raise when it carries none at all.

    An export without a GBFS section (one written before mobility-feed-api
    exported GBFS) can't tell whether a system exists: neither an empty
    list nor NotFound would be true.
    """
    if not rows.gbfs:
        raise FeedParseError(
            "Public catalog export carries no GBFS feeds", url=redact(url)
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

    def _keeps(self, feed: PublicFeed, boxes: Mapping[str, Box]) -> bool:
        source = feed.source_info
        if not (
            _like(self.provider, feed.provider)
            and _like(self.producer_url, source.producer_url if source else None)
        ):
            return False
        # The API's is_official=False keeps False and unknown.
        if (
            self.is_official is not None
            and not isinstance(feed, PublicGbfsFeed)
            and (feed.official is True) is not self.is_official
        ):
            return False
        if (
            self.entity_types is not None
            and isinstance(feed, PublicGtfsRtFeed)
            and not self.entity_types.intersection(feed.entity_types)
        ):
            return False
        box = self.box
        # A GTFS Realtime feed has no box: the API has no such filter for it.
        if box is None or isinstance(feed, PublicGtfsRtFeed):
            return True
        feed_box = boxes.get(feed.id)
        return feed_box is not None and matches(self.method, feed_box, box)

    def select[F: PublicFeed](
        self, feeds: Sequence[F], boxes: Mapping[str, Box]
    ) -> list[F]:
        """Return the feeds that pass, then apply ``offset`` and ``limit``.

        ``boxes`` are the feeds' bounding boxes by id (see
        :class:`CatalogRows`).
        """
        kept = (feed for feed in feeds if self._keeps(feed, boxes))
        return list(islice(kept, self.start, self.stop))


def _finite(values: tuple[float, float]) -> bool:
    """Whether both values are finite; an int past float's range is not."""
    try:
        return all(math.isfinite(value) for value in values)
    except OverflowError:
        return False
