"""Token-free feed lookups from the Mobility Database's public catalog export."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial
from itertools import islice
from typing import TYPE_CHECKING

from .._http import require_text

# Deliberate friend access: the argument checks are shared with the API
# client so both answer a bad argument alike.
from ..client import _require_id, _typed
from ..exceptions import MobilityDatabaseNotFoundError
from ..models import BoundingBox, BoundingFilterMethod, EntityType
from ._bounds import Box, matches
from ._catalog_csv import (
    CatalogFormatError,
    CatalogRows,
    PublicFeed,
    box_of,
    read_catalog,
)
from ._documents import cached_document, fetch_document
from ._freshness import FreshnessCache
from ._urls import redact
from .exceptions import FeedParseError
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
      GTFS list sorts by its database's collation; its GTFS Realtime and
      GBFS lists set no order), so a ``limit``/``offset`` page can hold
      other feeds. An RT feed's ``entity_types`` and ``feed_references``
      are in the export's order (see :class:`~.models.PublicGtfsRtFeed`).
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
      API answers an error. As the API, the GTFS Realtime list strips them
      (empty: no filter), the GTFS and GBFS lists take them as given (``""``
      matches every value but a missing one). A value the export leaves
      empty is missing here, where the database may hold ``""``.
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
        self._closed = False

    def _close(self) -> None:
        """Refuse every later lookup: the client closed (see its close())."""
        self._closed = True

    async def _rows(self) -> CatalogRows:
        """Return the export, from the cache while fresh."""
        # Before anything asks the client for a session, which would open a
        # new one nobody closes.
        if self._closed:
            raise RuntimeError("the client is closed")
        return await cached_document(
            self._cache,
            self._client,
            self._url,
            headers={},
            context=_CONTEXT,
            fetch=partial(
                fetch_document,
                self._client,
                self._url,
                None,
                _CONTEXT,
                label="Public catalog",
                missing="an export",
                parse=_read,
            ),
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
            trimmed=False,
        )
        rows = await self._rows()
        return query.select(rows.gtfs)

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
            trimmed=True,
        )
        # The API's list inner-joins the entity types: a feed without any
        # isn't listed, though get_gtfs_rt_feed returns it.
        rows = await self._rows()
        return query.select(rows.gtfs_rt_listed)

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
            trimmed=False,
        )
        rows = await self._rows()
        return query.select(_gbfs(rows, self._url))


async def _read(raw: bytes, shown: str | None) -> CatalogRows:
    """Read the export off the event loop; FeedParseError if it is none."""
    try:
        rows: CatalogRows | None = await asyncio.to_thread(read_catalog, raw)
    except CatalogFormatError as err:
        problem = str(err)
        rows = None
    if rows is None:
        # Outside the except: nothing of the parse rides along.
        raise FeedParseError(f"Public catalog unreadable: {problem}", url=shown)
    return rows


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


def _needle(value: str | None, *, trimmed: bool) -> re.Pattern[str] | None:
    """Return a string filter as the endpoint passes it to ``ILIKE``.

    ``trimmed``: stripped, and empty is no filter (the API's GTFS Realtime
    filter normalizes so). Otherwise as given: ``""`` is ``ILIKE '%%'``,
    which every value but a missing one matches, and spaces count.
    """
    if value is None:
        return None
    # As the client's encoder: plain str text, else ValueError.
    text = require_text(str.__str__(value), "a query parameter")
    if trimmed:
        text = text.strip()
        if not text:
            return None
    return _ilike(text)


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
        trimmed: bool,
    ) -> _Query:
        """Check a list call's filters and normalize them as the API does.

        ``max_limit`` is the endpoint's largest ``limit``, and its default.
        ``trimmed``: the endpoint strips its text filters (see
        :func:`_needle`).
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
            provider=_needle(provider, trimmed=trimmed),
            producer_url=_needle(producer_url, trimmed=trimmed),
            box=box,
            method=method or BoundingFilterMethod.COMPLETELY_ENCLOSED,
            is_official=is_official,
            entity_types=frozenset(entity_types) if entity_types else None,
        )

    def _keeps(self, feed: PublicFeed) -> bool:
        if not (
            _like(self.provider, feed.provider)
            and _like(self.producer_url, feed.source_info.producer_url)
        ):
            return False
        match feed:
            case PublicGtfsFeed():
                return self._official(feed.official) and self._within(feed.bounding_box)
            case PublicGtfsRtFeed():
                return self._official(feed.official) and self._typed_as(
                    feed.entity_types
                )
            case PublicGbfsFeed():
                return self._within(feed.bounding_box)

    def _official(self, official: bool | None) -> bool:
        """Whether the official flag passes; as the API, False keeps unknown too."""
        return self.is_official is None or (official is True) is self.is_official

    def _typed_as(self, entity_types: tuple[EntityType, ...]) -> bool:
        """Whether the feed has any of the asked entity types."""
        return self.entity_types is None or not self.entity_types.isdisjoint(
            entity_types
        )

    def _within(self, bounding_box: BoundingBox | None) -> bool:
        """Whether the box passes the bounding filter; no box passes none."""
        if self.box is None:
            return True
        return bounding_box is not None and matches(
            self.method, box_of(bounding_box), self.box
        )

    def select[F: PublicFeed](self, feeds: Sequence[F]) -> list[F]:
        """Return the feeds that pass, then apply ``offset`` and ``limit``."""
        kept = (feed for feed in feeds if self._keeps(feed))
        return list(islice(kept, self.start, self.stop))


def _finite(values: tuple[float, float]) -> bool:
    """Whether both values are finite; an int past float's range is not."""
    try:
        return all(math.isfinite(value) for value in values)
    except OverflowError:
        return False
