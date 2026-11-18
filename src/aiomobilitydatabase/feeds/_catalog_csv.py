"""Read the Mobility Database's public catalog export (``feeds_v2.csv``).

The columns are mobility-feed-api's ``export_csv`` (documented in its
``docs/SpreadsheetSchemaV2.md``). Each row becomes a
:class:`~.models.PublicGtfsFeed`, :class:`~.models.PublicGtfsRtFeed` or
:class:`~.models.PublicGbfsFeed`: the export's columns that mean what the
catalog API's same-named fields mean, and nothing else.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import chain
from types import MappingProxyType

from ..models import (
    BoundingBox,
    DataType,
    EntityType,
    FeedStatus,
    Redirect,
    SourceInfo,
)
from ._bounds import Box
from ._numbers import ascii_number
from ._vocab import vocab_or_none
from .models import PublicGbfsFeed, PublicGtfsFeed, PublicGtfsRtFeed

type PublicFeed = PublicGtfsFeed | PublicGtfsRtFeed | PublicGbfsFeed

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

# A record's cells by column name; a column the record is short of is absent.
type _Row = Mapping[str, str]


class CatalogFormatError(ValueError):
    """The body is no catalog export.

    Not UTF-8, no header with an id and a data_type column, or a record
    whose quoting is broken (see :func:`_records`).
    """


@dataclass(frozen=True)
class CatalogRows:
    """One export, read: each type's feeds in (provider, id) order, and by id.

    ``gtfs_rt_listed`` are the GTFS Realtime feeds the API's list returns:
    those with an entity type (its query inner-joins them), one this
    library doesn't know included. ``boxes`` holds each GTFS and GBFS
    feed's bounding box, by id, for the bounding filters.
    """

    gtfs: tuple[PublicGtfsFeed, ...]
    gbfs: tuple[PublicGbfsFeed, ...]
    by_id: Mapping[str, PublicFeed]
    gtfs_rt_listed: tuple[PublicGtfsRtFeed, ...]
    boxes: Mapping[str, Box]


def read_catalog(body: bytes) -> CatalogRows:
    """Read an export's bytes; :class:`CatalogFormatError` if it is none.

    A record the csv module can't read for its size is skipped (see
    :func:`_records`). The first record with an id wins, whatever its data
    type: a later one with that id is ignored, as is one of a type this
    library doesn't know.
    """
    try:
        text: str | None = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        # Raised below, outside the except: the error's .object holds the
        # whole body, and would ride along as the new error's __context__.
        text = None
    if text is None:
        raise CatalogFormatError("not UTF-8")
    records = _records(text)
    columns = next(records, None)
    if columns is None or not all(name in columns for name in _REQUIRED):
        raise CatalogFormatError("no id or data_type column")
    feeds: dict[str, PublicFeed] = {}
    seen: set[str] = set()
    boxes: dict[str, Box] = {}
    listed: set[str] = set()
    for record in records:
        if record is None:
            continue  # past the csv module's field size limit
        # As csv.DictReader: by column, the first of a repeated name losing;
        # cells past the header ignored.
        row = dict(zip(columns, record, strict=False))
        feed_id = _cell(row, "id")
        if feed_id is None or feed_id in seen:
            continue
        seen.add(feed_id)
        box = _box(row)
        feed = _feed(feed_id, row, box)
        if feed is None:
            continue
        feeds[feed_id] = feed
        if box is not None and not isinstance(feed, PublicGtfsRtFeed):
            boxes[feed_id] = box
        if isinstance(feed, PublicGtfsRtFeed) and _split(row, "entity_type"):
            listed.add(feed_id)
    ordered = sorted(feeds.values(), key=_order)
    return CatalogRows(
        gtfs=tuple(feed for feed in ordered if isinstance(feed, PublicGtfsFeed)),
        gbfs=tuple(feed for feed in ordered if isinstance(feed, PublicGbfsFeed)),
        by_id=MappingProxyType(feeds),
        gtfs_rt_listed=tuple(
            feed
            for feed in ordered
            if isinstance(feed, PublicGtfsRtFeed) and feed.id in listed
        ),
        boxes=MappingProxyType(boxes),
    )


class _Lines:
    """A text's lines, as the csv module reads them, keeping the record's.

    ``record`` holds the lines handed out since it was last cleared: those
    of the record being read.
    """

    def __init__(self, text: str) -> None:
        # newline="": split at "\r\n", "\r" and "\n", each kept, as the csv
        # module needs.
        self._lines = iter(io.StringIO(text, newline=""))
        self.record: list[str] = []

    def __iter__(self) -> Iterator[str]:
        return self

    def __next__(self) -> str:
        line = next(self._lines)
        self.record.append(line)
        return line


def _records(text: str) -> Iterator[list[str] | None]:
    """Yield each record's cells; None for one past the field size limit.

    The csv module's strict reader: a stray or unclosed quote is an error,
    where the default reads on -- joining text after a closing quote onto
    the cell, or swallowing every later record into one unclosed cell.
    After an error the reader resumes at the next line, which may still be
    inside the record (a quoted cell running over several lines), so the
    record is read past here (see :func:`_record_ends`).

    An error can mean a cell past ``csv.field_size_limit()`` (a
    process-wide setting, left alone here): the record is well formed, only
    too large, and is skipped. Or the quoting is broken, and then where the
    record was meant to end can't be known -- a stray quote after a closing
    one and an unclosed quote whose cell runs on to a later quote read
    alike, the second having swallowed the records between -- so it raises
    :class:`CatalogFormatError`.
    """
    lines = _Lines(text)
    reader = csv.reader(lines, strict=True)
    while True:
        lines.record.clear()
        try:
            record: list[str] | None = next(reader)
        except StopIteration:
            return
        except csv.Error:
            record = None
        if record is None:
            # Outside the except, as everywhere in this library: nothing of
            # the csv.Error (which can quote the body) is chained on.
            if not _record_ends(chain(tuple(lines.record), lines)):
                raise CatalogFormatError("a record's quoting is broken")
        elif not record:
            continue  # a blank line, as csv.DictReader skips it
        yield record


def _record_ends(lines: Iterator[str]) -> bool:
    """Read one record's lines, from its first; whether its quoting holds.

    True at the record's end (a line break outside quotes); False where
    the csv module's strict reader finds the quoting broken: anything but a
    delimiter or line break after a closing quote, or the text ending in a
    quoted cell. Its rules for the default dialect: a quote opens a quoted
    cell only at a cell's start (elsewhere it is text), and inside one a
    doubled quote is a quote. Lines past the record's end are left unread.
    """
    quoted = False
    for line in lines:
        # The text before the line break (newline="" lines end in one at most).
        end = len(line.rstrip("\r\n"))
        at = 0
        while True:
            if quoted:
                close = line.find('"', at)
                if close == -1:
                    break  # the quoted cell runs on into the next line
                if line.startswith('"', close + 1):
                    at = close + 2  # a doubled quote
                    continue
                quoted = False
                at = close + 1
                if at >= end:
                    return True
                if line[at] != ",":
                    return False
                at += 1
            elif at >= end:
                return True
            elif line[at] == '"':
                quoted = True
                at += 1
            else:
                comma = line.find(",", at, end)
                if comma == -1:
                    return True
                at = comma + 1
    return False


def _order(feed: PublicFeed) -> tuple[bool, str, str]:
    """Order by provider, then id, each by code point; no provider last.

    Not the API's order: it sorts by its database's en_US.UTF8 collation,
    which sets case, punctuation and accents apart differently, so the
    order -- and with it which feeds a limit/offset page holds -- can
    differ. An empty provider is None here (sorted last), where Postgres
    would sort its "" first.
    """
    return (feed.provider is None, feed.provider or "", feed.id)


def _cell(row: _Row, name: str) -> str | None:
    """Return a cell's text; None when empty (or not a column of this export)."""
    return row.get(name) or None


def _feed(feed_id: str, row: _Row, box: Box | None) -> PublicFeed | None:
    """Build the row's model, by data type; None for a type this library lacks."""
    data_type = _cell(row, "data_type")
    if data_type == DataType.GTFS:
        return PublicGtfsFeed(
            id=feed_id,
            provider=_cell(row, "provider"),
            feed_name=_cell(row, "name"),
            note=_cell(row, "note"),
            feed_contact_email=_cell(row, "feed_contact_email"),
            status=_status(row),
            official=_BOOLEANS.get(_cell(row, "is_official") or ""),
            source_info=_source_info(row),
            redirects=_redirects(row),
            bounding_box=_bounding_box(box),
        )
    if data_type == DataType.GTFS_RT:
        return PublicGtfsRtFeed(
            id=feed_id,
            provider=_cell(row, "provider"),
            feed_name=_cell(row, "name"),
            note=_cell(row, "note"),
            feed_contact_email=_cell(row, "feed_contact_email"),
            status=_status(row),
            official=_BOOLEANS.get(_cell(row, "is_official") or ""),
            source_info=_source_info(row),
            redirects=_redirects(row),
            entity_types=_entity_types(row),
            feed_references=_split(row, "static_reference"),
        )
    if data_type == DataType.GBFS:
        return PublicGbfsFeed(
            id=feed_id,
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
