"""Read the Mobility Database's public catalog export (``feeds_v2.csv``).

The columns are mobility-feed-api's ``export_csv`` (documented in its
``docs/SpreadsheetSchemaV2.md``). Each row becomes the model the catalog
API returns for that feed, holding the values the export carries from the
same database columns and nothing else (see
:class:`~aiomobilitydatabase.feeds.public_catalog.MobilityDatabasePublicCatalog`).
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

from ..models import (
    BoundingBox,
    DataType,
    EntityType,
    FeedStatus,
    GbfsFeed,
    GtfsFeed,
    GtfsRtFeed,
    Redirect,
    SourceInfo,
)
from ._bounds import Box
from ._numbers import ascii_number
from ._vocab import vocab_or_none

type CatalogFeed = GtfsFeed | GtfsRtFeed | GbfsFeed

_REQUIRED = ("id", "data_type")
# The database's authentication_type (an enum, "0" to "2") as int() reads it: ASCII
# digits only (int() also takes other scripts' digits, a sign, spaces and
# "1_0"), and at most nine, well inside int()'s 4300-digit limit, past which
# it raises ValueError.
_DIGITS = re.compile(r"[0-9]{1,9}")
# A date-time as the export writes one (Python's str(datetime), an offset
# optional), or with "T" and "Z". fromisoformat would also take a bare date
# (midnight), the basic and week formats, 24:00 (the next day), and more
# than six fraction digits (truncated). Narrower than _payload._DATE_TIME,
# which reads the API's JSON: the export is written by str(datetime), never
# with a lowercase "t" or more than six fraction digits.
_DATE_TIME = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[T ](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])?"
)
_BOX = (
    "location.bounding_box.minimum_latitude",
    "location.bounding_box.maximum_latitude",
    "location.bounding_box.minimum_longitude",
    "location.bounding_box.maximum_longitude",
)
_BOOLEANS = {"True": True, "False": False}

type _Row = Mapping[str | None, str | list[str] | None]


class CatalogFormatError(ValueError):
    """The body is no catalog export: not UTF-8, not CSV, or no id/data_type."""


@dataclass(frozen=True)
class CatalogRows:
    """One export, read: each type's feeds in (provider, id) order, and by id.

    ``gtfs_rt_listed`` are the GTFS Realtime feeds the API's list returns:
    those with an entity type (its query inner-joins them), one this
    library doesn't know included. ``boxes`` holds each GTFS and GBFS
    feed's bounding box, by id, for the bounding filters.
    """

    gtfs: tuple[GtfsFeed, ...]
    gtfs_rt: tuple[GtfsRtFeed, ...]
    gbfs: tuple[GbfsFeed, ...]
    by_id: Mapping[str, CatalogFeed]
    gtfs_rt_listed: tuple[GtfsRtFeed, ...]
    boxes: Mapping[str, Box]


def read_catalog(body: bytes) -> CatalogRows:
    """Read an export's bytes; :class:`CatalogFormatError` if it is none."""
    try:
        text: str | None = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        # Raised below, outside the except: the error's .object holds the
        # whole body, and would ride along as the new error's __context__.
        text = None
    if text is None:
        raise CatalogFormatError("not UTF-8")
    feeds: dict[str, CatalogFeed] = {}
    boxes: dict[str, Box] = {}
    listed: set[str] = set()
    # strict: a stray or unclosed quote is an error, where the default reads
    # on -- joining text after a closing quote onto the cell, or swallowing
    # every later row into one unclosed cell.
    reader = csv.DictReader(io.StringIO(text, newline=""), restval="", strict=True)
    failure: str | None = None
    try:
        columns = reader.fieldnames or ()
        if all(name in columns for name in _REQUIRED):
            for row in reader:
                feed_id = _cell(row, "id")
                if feed_id is None or feed_id in feeds:
                    continue
                box = _box(row)
                feed = _feed(feed_id, row, box)
                if feed is None:
                    continue
                feeds[feed_id] = feed
                if box is not None and isinstance(feed, GtfsFeed | GbfsFeed):
                    boxes[feed_id] = box
                if isinstance(feed, GtfsRtFeed) and _split(row, "entity_type"):
                    listed.add(feed_id)
        else:
            failure = "no id or data_type column"
    except csv.Error:
        failure = "not CSV"
    if failure is not None:
        # Outside the except, as everywhere in this library: nothing of the
        # csv.Error is chained onto the new one.
        raise CatalogFormatError(failure)
    ordered = sorted(feeds.values(), key=_order)
    return CatalogRows(
        gtfs=tuple(feed for feed in ordered if isinstance(feed, GtfsFeed)),
        gtfs_rt=tuple(feed for feed in ordered if isinstance(feed, GtfsRtFeed)),
        gbfs=tuple(feed for feed in ordered if isinstance(feed, GbfsFeed)),
        by_id=MappingProxyType(feeds),
        gtfs_rt_listed=tuple(
            feed
            for feed in ordered
            if isinstance(feed, GtfsRtFeed) and feed.id in listed
        ),
        boxes=MappingProxyType(boxes),
    )


def _order(feed: CatalogFeed) -> tuple[bool, str, str]:
    """Order by provider, then id, each by code point; no provider last.

    Not the API's order: it sorts by its database's en_US.UTF8 collation,
    which sets case, punctuation and accents apart differently, so the
    order -- and with it which feeds a limit/offset page holds -- can
    differ. An empty provider is None here (sorted last), where Postgres
    would sort its "" first.
    """
    return (feed.provider is None, feed.provider or "", feed.id or "")


def _cell(row: _Row, name: str) -> str | None:
    """Return a cell's text; None when empty (or not a column of this export)."""
    value = row.get(name)
    return value if isinstance(value, str) and value else None


def _feed(feed_id: str, row: _Row, box: Box | None) -> CatalogFeed | None:
    """Build the row's model, by data type; None for a type this library lacks."""
    data_type = _cell(row, "data_type")
    if data_type == DataType.GTFS:
        return GtfsFeed(
            id=feed_id,
            data_type=DataType.GTFS,
            provider=_cell(row, "provider"),
            feed_contact_email=_cell(row, "feed_contact_email"),
            source_info=_source_info(row),
            redirects=_redirects(row),
            status=_status(row),
            official=_BOOLEANS.get(_cell(row, "is_official") or ""),
            # seasonal: not in the export; the model's default stands (R2: own models)
            feed_name=_cell(row, "name"),
            note=_cell(row, "note"),
            bounding_box=_bounding_box(box),
        )
    if data_type == DataType.GTFS_RT:
        return GtfsRtFeed(
            id=feed_id,
            data_type=DataType.GTFS_RT,
            provider=_cell(row, "provider"),
            feed_contact_email=_cell(row, "feed_contact_email"),
            source_info=_source_info(row),
            redirects=_redirects(row),
            status=_status(row),
            official=_BOOLEANS.get(_cell(row, "is_official") or ""),
            # seasonal: not in the export; the model's default stands (R2: own models)
            feed_name=_cell(row, "name"),
            note=_cell(row, "note"),
            entity_types=_entity_types(row),
            feed_references=_split(row, "static_reference"),
        )
    if data_type == DataType.GBFS:
        return GbfsFeed(
            id=feed_id,
            data_type=DataType.GBFS,
            provider=_cell(row, "provider"),
            feed_contact_email=_cell(row, "feed_contact_email"),
            source_info=_source_info(row),
            redirects=_redirects(row),
            bounding_box=_bounding_box(box),
            bounding_box_generated_at=_naive_utc(
                _cell(row, "location.bounding_box.extracted_on")
            ),
        )
    return None


def _source_info(row: _Row) -> SourceInfo:
    """Read the ``urls.*`` columns, as the API's always-present source_info."""
    auth = _cell(row, "urls.authentication_type")
    return SourceInfo(
        producer_url=_cell(row, "urls.direct_download"),
        authentication_type=int(auth) if auth and _DIGITS.fullmatch(auth) else None,
        authentication_info_url=_cell(row, "urls.authentication_info"),
        api_key_parameter_name=_cell(row, "urls.api_key_parameter_name"),
        license_url=_cell(row, "urls.license"),
    )


def _status(row: _Row) -> FeedStatus | None:
    """Read the status; one this library doesn't know is None."""
    return vocab_or_none(FeedStatus, _cell(row, "status"))


def _split(row: _Row, name: str) -> tuple[str, ...]:
    """Split a ``|``-separated column; empty is ``()``, as the API returns ``[]``."""
    return tuple(part for part in (_cell(row, name) or "").split("|") if part)


def _entity_types(row: _Row) -> tuple[EntityType, ...]:
    """Read the entity types, leaving out any this library doesn't know."""
    types = (vocab_or_none(EntityType, name) for name in _split(row, "entity_type"))
    return tuple(entity for entity in types if entity is not None)


def _redirects(row: _Row) -> tuple[Redirect, ...]:
    """Pair redirect ids and comments by position.

    The export joins each with ``|``; a comment holding ``|`` itself can't
    be paired, and then no comment is.
    """
    targets = _split(row, "redirect.id")
    comments = (_cell(row, "redirect.comment") or "").split("|")
    if len(comments) != len(targets):
        comments = [""] * len(targets)
    return tuple(
        Redirect(target_id=target, comment=comment or None)
        for target, comment in zip(targets, comments, strict=True)
    )


def _number(row: _Row, name: str) -> float | None:
    """Read a finite ASCII number (see :func:`._numbers.ascii_number`)."""
    cell = _cell(row, name)
    return None if cell is None else ascii_number(cell)


def _box(row: _Row) -> Box | None:
    """All four bounds, finite, each minimum at most its maximum; else None."""
    min_lat, max_lat, min_lon, max_lon = (_number(row, name) for name in _BOX)
    if min_lat is None or max_lat is None or min_lon is None or max_lon is None:
        return None
    if min_lat > max_lat or min_lon > max_lon:
        return None
    return Box(min_lat, max_lat, min_lon, max_lon)


def _bounding_box(box: Box | None) -> BoundingBox | None:
    """Return the model's bounding box for a row's box."""
    if box is None:
        return None
    return BoundingBox(
        minimum_latitude=box.minimum_latitude,
        maximum_latitude=box.maximum_latitude,
        minimum_longitude=box.minimum_longitude,
        maximum_longitude=box.maximum_longitude,
    )


def _naive_utc(value: str | None) -> datetime | None:
    """Read a date-time as the API returns this column: naive, in UTC.

    The database column has no zone; the export marks it ``+00:00``.
    Anything but a full date-time (see ``_DATE_TIME``) is None.
    """
    if value is None or not _DATE_TIME.fullmatch(value):
        return None
    try:
        moment = datetime.fromisoformat(value)
        if moment.tzinfo is not None:
            moment = moment.astimezone(UTC).replace(tzinfo=None)
    except (ValueError, OverflowError):
        return None
    return moment
