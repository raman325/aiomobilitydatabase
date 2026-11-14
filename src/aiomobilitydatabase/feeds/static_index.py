"""Synchronous SQLite index over a static GTFS dataset.

Everything in this module is synchronous by design: callers in async code
MUST invoke build/open/query methods through ``asyncio.to_thread`` so the
event loop is never blocked (a hard Home Assistant requirement).
"""

from __future__ import annotations

import csv
import io
import os
import sqlite3
import tempfile
import zipfile
import zlib
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from enum import IntEnum
from pathlib import Path
from typing import IO, Any, NamedTuple
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .exceptions import FeedParseError
from .models import (
    Agency,
    BikesAllowed,
    FeedInfo,
    PickupDropOffType,
    Route,
    Stop,
    StopLocationType,
    WheelchairAccess,
)

# v3: descriptive surface sweep (stop desc/url/zone/timezone, route
# desc/sort_order, trip short_name/block_id, feed_info table).
# v4: agencies.email, feed_info.contact_email/contact_url.
# v5: calendar and calendar_dates dates stored canonical (YYYYMMDD, or NULL
#     when unparseable) -- same columns, different contents, so old caches
#     must not be reused.
# BUMP THIS whenever _SCHEMA changes. A cache stamped with an older
# version is discarded and rebuilt; leaving the number alone makes a
# stale cache pass validation and then fail on the first query for a
# column it does not have. test_schema_fingerprint_matches_version
# fails if the two drift apart.
SCHEMA_VERSION = 9
_BATCH_SIZE = 5000
_SECONDS_OR_MINUTES_PER_UNIT = 60

# Descriptive vocabulary columns are stored as the raw parsed ints, with a
# blank or absent cell resolved to the value GTFS gives "empty" (see
# _defaulted_int) and NULL for unparseable values; the closed-vocabulary
# enums live at the Python model boundary only, where out-of-vocabulary ints
# turn into None.
_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE agencies (
    id TEXT, name TEXT, url TEXT, timezone TEXT,
    lang TEXT, phone TEXT, fare_url TEXT, email TEXT
);
CREATE TABLE stops (
    id TEXT PRIMARY KEY, name TEXT, lat REAL, lon REAL,
    parent_station TEXT, location_type INTEGER,
    stop_code TEXT, platform_code TEXT, wheelchair_boarding INTEGER,
    description TEXT, url TEXT, zone_id TEXT, timezone TEXT
);
CREATE TABLE routes (
    id TEXT PRIMARY KEY, short_name TEXT, long_name TEXT, type INTEGER,
    agency_id TEXT, color TEXT, text_color TEXT, url TEXT,
    description TEXT, sort_order INTEGER
);
CREATE TABLE trips (
    id TEXT PRIMARY KEY, route_id TEXT NOT NULL,
    service_id TEXT NOT NULL, headsign TEXT,
    -- RT-matching identity: the GTFS trip id as a producer would reference
    -- it, plus the repetition start for frequency-materialized trips.
    -- Plain trips carry (their own id, NULL); synthetic repetition trips
    -- carry (template trip id, repetition start seconds). Real columns
    -- rather than string-parsing the synthetic "#"-suffixed id, which
    -- would be ambiguous if a real trip id contained "#".
    source_trip_id TEXT NOT NULL, start_secs INTEGER,
    wheelchair_accessible INTEGER, bikes_allowed INTEGER, direction_id INTEGER,
    short_name TEXT, block_id TEXT,
    -- The trip's first departure (see StaticIndex._store_first_departures),
    -- filled once every stop_time is loaded; NULL with no timed call.
    first_secs INTEGER,
    -- An exact_times=0 repetition's headway: the slot of starts
    -- [start_secs, start_secs + slot_secs) a realtime run of the trip may
    -- take in its place. NULL for every trip that keeps exact times.
    slot_secs INTEGER
);
-- GTFS-RT's alternative trip matching names a trip by route, direction and
-- first departure.
CREATE INDEX ix_trips_route_start ON trips (route_id, direction_id, first_secs);
-- Realtime names trips by their GTFS id and repetition start.
CREATE INDEX ix_trips_source ON trips (source_trip_id, start_secs);
-- Single-record feed metadata (feed_info.txt); dates stay raw YYYYMMDD
-- text cells, parsed leniently at the model boundary like every other
-- descriptive value.
CREATE TABLE feed_info (
    publisher_name TEXT, publisher_url TEXT, lang TEXT, version TEXT,
    start_date TEXT, end_date TEXT, contact_email TEXT, contact_url TEXT
);
CREATE TABLE stop_times (
    trip_id TEXT NOT NULL, stop_id TEXT NOT NULL,
    arrival_secs INTEGER, departure_secs INTEGER, stop_sequence INTEGER NOT NULL,
    pickup_type INTEGER, drop_off_type INTEGER, timepoint INTEGER,
    stop_headsign TEXT
);
CREATE INDEX ix_stop_times_stop_departure ON stop_times (stop_id, departure_secs);
-- Per-trip ordered stop calls: RT delay propagation resolves StopTimeUpdates
-- against a trip's full static stop order at query time, and the frequency
-- loader reads whole templates by trip during build.
CREATE INDEX ix_stop_times_trip_sequence ON stop_times (trip_id, stop_sequence);
CREATE TABLE calendar (
    service_id TEXT PRIMARY KEY,
    monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER,
    friday INTEGER, saturday INTEGER, sunday INTEGER,
    start_date TEXT, end_date TEXT
);
CREATE TABLE calendar_dates (
    service_id TEXT NOT NULL, date TEXT NOT NULL, exception_type INTEGER NOT NULL
);
CREATE INDEX ix_calendar_dates_date ON calendar_dates (date);
"""

_EXCEPTION_SERVICE_ADDED = 1
# Open-ended scans never reach past this instant: a calendar running to year
# 9999 ("forever") would otherwise overflow date arithmetic near datetime.max.
_LATEST_INSTANT = datetime(9999, 12, 1, tzinfo=UTC)
# Nor start before this one: two days past datetime.min, so every local date
# and service-day start a query derives from it stays representable.
_EARLIEST_INSTANT = datetime(1, 1, 3, tzinfo=UTC)
# Departures further past their service day than this are placed row by row,
# not by scanning every day back (see StaticIndex._reach_profile). Any value
# is correct -- it only moves work between the day scan and the placement;
# four days keeps real night and multi-day services in the scan.
_LONG_REACH = 4 * 86_400
_EXCEPTION_SERVICE_REMOVED = 2
# GTFS-RT requires start_date for a trip lasting longer than this.
_ONE_DAY = 86_400
# Ids bound per statement: under SQLite's lowest default variable limit (999).
_ID_SLICE = 500

# A journey needs a time at both ends, a pickup at the origin and a drop-off
# at the destination: pickup_type/drop_off_type 1 is "No pickup available" /
# "No drop off available". (2 and 3 -- arrange by phone, coordinate with the
# driver -- are still a pickup.) An unparseable cell (NULL) is no evidence
# either way and does not exclude the call.
_USABLE_PAIR_SQL = (
    "AND o.departure_secs IS NOT NULL AND d.arrival_secs IS NOT NULL "
    "AND o.pickup_type IS NOT 1 AND d.drop_off_type IS NOT 1"
)

# The headsign shown at a stop: stop_times.stop_headsign "overrides the
# default trips.trip_headsign when the headsign changes between stops".
_HEADSIGN_SQL = "COALESCE(st.stop_headsign, t.headsign)"

_REQUIRED_FILES = frozenset({"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"})

_WEEKDAY_COLUMNS = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)


def _ascii_digits(value: str) -> bool:
    """Report whether a cell component is ASCII digits and nothing else.

    ``int()`` also accepts any unicode decimal ("\u0660\u0668"), a sign prefix
    ("+8") and PEP 515 underscore grouping ("1_0"), each of which turns a
    corrupt GTFS cell into a plausible-looking number instead of a
    detected problem. Every GTFS numeric field is spelled in ASCII digits.
    """
    return bool(value) and value.isascii() and value.isdigit()


# SQLite's INTEGER is a signed 64-bit value, so a Python int above this
# raises OverflowError at INSERT -- thousands of rows away from the cell that
# caused it, and outside every parse guard. Python ints are unbounded, so the
# ceiling has to be stated here rather than discovered at the storage layer.
_SQLITE_INT_MAX = 2**63 - 1


def _ascii_int(value: str) -> int | None:
    """Convert an ASCII-digit cell to a STORABLE int, or None if it is not one.

    Folds the predicate, the conversion and the range check together
    because an all-ASCII-digit cell can still fail to become a usable
    number three different ways: it is not digits, CPython caps
    int-string conversion at 4300 digits, or the value exceeds what
    SQLite can store. All three are the same "not a usable number"
    answer; every caller then decides whether that answer raises or
    degrades.
    """
    if not _ascii_digits(value):
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return None if parsed > _SQLITE_INT_MAX else parsed


def _strict_int(value: str, field: str) -> int:
    """Parse a STRUCTURAL integer cell: anything unparseable raises.

    The counterpart to ``_lenient_int`` for the columns the index
    navigates by -- stop_sequence orders the stop calls realtime delay
    propagation walks positionally, headway_secs sets how many
    repetitions of a template trip exist, exception_type decides whether
    a service day is added or removed. Same ASCII-digit rule as
    ``parse_gtfs_time``, and the same reasoning: a corrupt structural
    value that silently parses is worse than a failed build.
    """
    if (parsed := _ascii_int(value.strip())) is None:
        raise FeedParseError(f"Invalid GTFS {field}: {value!r}")
    return parsed


_GTFS_TIME_COMPONENTS = 3


def parse_gtfs_time(value: str) -> int | None:
    """Parse an ``HH:MM:SS`` GTFS time (hours may exceed 23) to seconds.

    STRUCTURAL field: a blank cell means "no time here" (None), but
    anything else unparseable raises ``FeedParseError`` rather than
    degrading -- a wrong departure time is worse than a failed build.

    Accepted: exactly three colon-separated ASCII-digit components, any
    zero-padding or none ("8:0:0"), hours past 23 (service days run past
    midnight), minutes and seconds in [0, 60), and whitespace around the
    whole cell (real exporters emit it). Everything else -- unicode
    digits, sign prefixes, underscore grouping, fractional seconds, wrong
    arity, intra-component whitespace -- is rejected.

    Hours are bounded only by what the resulting seconds can be stored
    as: the components are storable individually long before their sum
    is, so an 18-digit hour converts and then overflows at INSERT. The
    total is checked here instead.
    """
    value = value.strip()
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != _GTFS_TIME_COMPONENTS:
        raise FeedParseError(f"Invalid GTFS time: {value!r}")
    components: list[int] = []
    for part in parts:
        if (parsed := _ascii_int(part)) is None:
            raise FeedParseError(f"Invalid GTFS time: {value!r}")
        components.append(parsed)
    hours, minutes, seconds = components
    if (
        minutes >= _SECONDS_OR_MINUTES_PER_UNIT
        or seconds >= _SECONDS_OR_MINUTES_PER_UNIT
    ):
        raise FeedParseError(f"Invalid GTFS time: {value!r}")
    total = hours * 3600 + minutes * 60 + seconds
    if total > _SQLITE_INT_MAX:
        raise FeedParseError(f"GTFS time out of storable range: {value!r}")
    return total


def _lenient_int(value: str | None) -> int | None:
    """Parse a DESCRIPTIVE integer column leniently: blank/garbage -> None.

    Descriptive metadata (wheelchair flags, pickup types, direction ids)
    must never fail a build the way structural fields (stop_sequence,
    times) do -- a producer's typo in an accessibility column should not
    take the whole schedule down. In-range values are stored as-is, even
    outside the closed vocabulary; the model boundary maps those to None.

    Same ASCII-digit rule as ``parse_gtfs_time`` and ``_lenient_date``
    (every column read through here is a GTFS non-negative integer, so a
    sign prefix is garbage too), but lenient in KIND: garbage yields None
    instead of raising. Whitespace around the cell is tolerated.
    """
    if value is None:
        return None
    return _ascii_int(value.strip())


_GTFS_DATE_LENGTH = 8  # YYYYMMDD


def _lenient_date(value: str | None) -> date | None:
    """Parse a DESCRIPTIVE ``YYYYMMDD`` date cell leniently: garbage -> None.

    Same contract as ``_lenient_int``: absent, blank, wrong-length,
    non-digit, or calendar-invalid (month 13, day 32) cells become None
    rather than failing the build. ASCII digits only, like every other
    scalar helper here.
    """
    if value is None:
        return None
    value = value.strip()
    if len(value) != _GTFS_DATE_LENGTH or not _ascii_digits(value):
        return None
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    except ValueError:
        return None


def _gtfs_date(day: date) -> str:
    """Format a date as GTFS ``YYYYMMDD``, the year always four digits.

    Never ``strftime("%Y")``: the C library decides its width, and glibc
    writes year 1 as ``1`` (macOS as ``0001``), so stored and looked-up
    dates would disagree on one platform only.
    """
    return f"{day.year:04d}{day.month:02d}{day.day:02d}"


def _canonical_date(value: str | None) -> str | None:
    """Store a service date as canonical ``YYYYMMDD`` text, or None.

    Service activity compares these cells as text, and the scan ceiling
    takes their maximum; both must read the same dates. Canonicalizing at
    load time makes that true: a cell :func:`_lenient_date` rejects (wrong
    length, dashes, month 13) is stored NULL and is never active, instead
    of comparing by accident of its characters.
    """
    parsed = _lenient_date(value)
    return _gtfs_date(parsed) if parsed is not None else None


def _enum_or_none[IntEnumT: IntEnum](
    enum_cls: type[IntEnumT], value: int | None
) -> IntEnumT | None:
    """Model-boundary enum conversion: out-of-vocabulary ints become None."""
    if value is None:
        return None
    try:
        return enum_cls(value)
    except ValueError:
        return None


def _stored_timepoint(value: str | None) -> int | None:
    """Resolve stop_times.timepoint at load time: GTFS's default is EXACT.

    An absent column or blank value means the scheduled times ARE exact
    (stored 1); ``0``/``1`` are stored as-is; anything else (garbage text,
    out-of-vocabulary ints) stores NULL, surfacing as None at the model
    boundary. Resolved here rather than at query time because a NULL in
    the column must mean "unknown", not "absent" -- the absent-means-exact
    default would otherwise be indistinguishable from a bad value.
    """
    if value is None or not value.strip():
        return 1
    parsed = _lenient_int(value)
    return parsed if parsed in (0, 1) else None


def _defaulted_int(value: str | None, default: int) -> int | None:
    """Parse a descriptive integer whose blank value GTFS defines.

    GTFS gives "empty" a meaning for several vocabulary columns --
    ``pickup_type``/``drop_off_type`` "0 or empty" is a regular stop,
    ``location_type`` "0 (or empty)" a stop or platform, the wheelchair and
    bikes columns "0 or empty" no information -- so an absent column or a
    blank cell is ``default``, not unknown. Anything else parses as
    :func:`_lenient_int` does: garbage stays NULL, surfacing as None.
    """
    if value is None or not value.strip():
        return default
    return _lenient_int(value)


def _defaulted_text(value: str | None, default: str) -> str:
    """Return a text cell, or GTFS's default when omitted or empty (colours)."""
    return value if value is not None and value.strip() else default


# What a member of the zip can raise when opened: an encrypted entry, an
# unsupported compression method, a corrupt local header.
_OPEN_ERRORS = (RuntimeError, NotImplementedError, zipfile.BadZipFile)
# ... and while it is read and parsed: bad bytes (not UTF-8), a failed CRC
# or truncated stream, a field past csv's size limit, a ragged or missing
# column the loaders index.
_READ_ERRORS = (
    ValueError,
    KeyError,
    AttributeError,
    TypeError,
    csv.Error,
    zipfile.BadZipFile,
    zlib.error,
    EOFError,
)


@contextmanager
def _open_csv(
    archive: zipfile.ZipFile, filename: str
) -> Iterator[tuple[IO[bytes], csv.DictReader[str]]]:
    """Open one dataset file as CSV; anything wrong with it is FeedParseError."""
    try:
        fp = archive.open(filename)
    except _OPEN_ERRORS as err:
        raise FeedParseError(f"Unreadable {filename}: {err}") from err
    with fp:
        try:
            yield fp, csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
        except _READ_ERRORS as err:
            raise FeedParseError(f"Malformed {filename}: {err}") from err


type _StopRow = tuple[Any, ...]


def _ancestor(
    by_id: Mapping[str, _StopRow],
    row: _StopRow,
    climb: Callable[[_StopRow], bool],
) -> _StopRow:
    """Walk ``parent_station`` up from ``row`` while ``climb(row)`` holds.

    Stops at the first row with no (known) parent, or before revisiting a
    stop: a ``parent_station`` cycle ends the chain instead of looping.
    """
    seen = {row[0]}
    while climb(row) and (parent := by_id.get(row[4])) is not None:
        if parent[0] in seen:
            break
        seen.add(parent[0])
        row = parent
    return row


def _resolved_boarding(by_id: Mapping[str, _StopRow], row: _StopRow) -> int | None:
    """Resolve a stop's wheelchair_boarding through GTFS's parent inheritance.

    0 or empty (stored 0) "will inherit ... from the parent station, if
    specified in the parent": the nearest ancestor saying 1 or 2 decides;
    none doing so (a 0, an unusable cell, a cycle) leaves 0.
    """
    own: int | None = row[8]
    if own != 0:
        return own
    source: int | None = _ancestor(by_id, row, lambda r: r[8] == 0)[8]
    return source if source in (1, 2) else 0


def _valid_zone(name: str) -> bool:
    """Whether ``name`` is an IANA zone this system can load."""
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, TypeError, ValueError, OSError):
        # OSError: with the tzdata package, a directory name ("America")
        # or an over-long one fails as a file error, not as not-found.
        return False
    return True


def repetition_trip_id(trip_id: str, start_secs: int) -> str:
    """Return the synthetic trip id of one run of a frequency trip.

    A frequencies.txt repetition is ``{trip_id}#{start_secs}`` (e.g.
    ``CITY1#21600``), as is a realtime run of an exact_times=0 trip at its
    own start.
    """
    return f"{trip_id}#{start_secs}"


def _later(when: datetime | None, shift: timedelta) -> datetime | None:
    """Return ``when`` moved by ``shift``; None without it, or past datetime."""
    if when is None:
        return None
    try:
        return when + shift
    except OverflowError:
        return None


def _shifted(secs: int | None, shift: int) -> int | None:
    """Move a template time onto a repetition; None before its day starts."""
    if secs is None or secs + shift < 0:
        return None
    return secs + shift


def _timepoint_exact(stored: int | None) -> bool | None:
    """Model-boundary read of the load-resolved timepoint column."""
    return None if stored is None else bool(stored)


def _reporter(
    progress: Callable[[int, int | None], None] | None,
    fp: IO[bytes],
    completed: int,
    total: int | None,
) -> Callable[[], None] | None:
    """Per-file progress closure: completed prior bytes + position in this file."""
    if progress is None:
        return None
    return lambda: progress(completed + fp.tell(), total)


@dataclass(frozen=True)
class TripCall:
    """One stop_time row of a trip, as absolute instants."""

    stop_sequence: int
    stop_id: str
    arrival: datetime | None
    # None for an untimed call (GTFS allows untimed intermediate stops).
    departure: datetime | None
    pickup_type: PickupDropOffType | None
    drop_off_type: PickupDropOffType | None
    timepoint_exact: bool | None
    stop_headsign: str | None
    # A TripModifications replacement stop, not one of the trip's own: its
    # stop_sequence is synthetic, and no StopTimeUpdate addresses it.
    replacement: bool = False


@dataclass(frozen=True)
class TripInstanceCalls:
    """One trip instance's COMPLETE ordered call sequence.

    ``upcoming_departures`` returns only the calls at queried stops, which
    is all a board needs -- but a TripModifications detour replaces a SPAN
    of a trip's calls, so applying one requires the whole sequence (the
    stop before the span is the reference point its replacement times are
    measured from).
    """

    trip_id: str
    source_trip_id: str
    start_secs: int | None
    service_date: date
    day_start_utc: datetime
    route_id: str
    service_id: str
    headsign: str | None
    wheelchair_accessible: WheelchairAccess | None
    bikes_allowed: BikesAllowed | None
    direction_id: int | None
    trip_short_name: str | None
    block_id: str | None
    calls: list[TripCall]


class SlotRun(NamedTuple):
    """A realtime run of an exact_times=0 trip, and the repetition it takes.

    ``run`` is the trip at the run's own start: its slot repetition's calls
    moved there, under the run's own synthetic id. ``slot_start`` is that
    repetition's start, the scheduled run it stands in for.
    """

    slot_start: int
    run: TripInstanceCalls


@dataclass(frozen=True)
class ScheduledDeparture:
    """A scheduled stop event resolved to tz-aware datetimes.

    ``source_trip_id``/``start_secs`` form the RT-matching identity: for a
    plain trip they are ``(trip_id, None)``; for a frequency-materialized
    repetition (synthetic ``{trip_id}#{start_secs}`` id) they are the
    template trip id and the repetition start in GTFS seconds -- the pair a
    GTFS-RT ``TripDescriptor`` (trip_id + start_time) addresses.
    ``stop_sequence`` positions this call within its trip so RT delay
    propagation (resolved per stop_sequence) can address it unambiguously,
    including on loop trips where stop_id repeats.
    ``service_date`` is the GTFS service day this row runs on -- the date
    a GTFS-RT ``TripDescriptor.start_date`` addresses. For a >24:00:00
    spillover row it is the GENERATING service day, not the clock day the
    departure lands on.

    The descriptive tail mirrors :class:`~.models.StopArrival`:
    trip-level wheelchair/bikes flags and short-name/block identifiers
    plus this stop_time row's pickup/drop-off/timepoint/headsign
    descriptors.
    """

    trip_id: str
    route_id: str
    service_id: str
    headsign: str | None
    stop_id: str
    stop_sequence: int
    arrival: datetime | None
    departure: datetime
    source_trip_id: str
    start_secs: int | None
    service_date: date
    wheelchair_accessible: WheelchairAccess | None
    bikes_allowed: BikesAllowed | None
    direction_id: int | None
    pickup_type: PickupDropOffType | None
    drop_off_type: PickupDropOffType | None
    timepoint_exact: bool | None
    stop_headsign: str | None
    trip_short_name: str | None
    block_id: str | None


@dataclass(frozen=True)
class ScheduledTrip:
    """A scheduled origin-to-destination journey resolved to tz-aware datetimes.

    ``departure`` is at the origin stop and ``arrival`` at the destination;
    both are non-optional because the producing query's WHERE clauses
    require the underlying GTFS times to be present.
    ``source_trip_id``/``start_secs`` are the RT-matching identity, exactly
    as on :class:`ScheduledDeparture`; ``service_date`` is the journey's
    service day (an origin->destination row is one trip, so one service
    day -- the ORIGIN's, which for >24:00:00 rows is the generating
    service day, not the clock day);
    ``origin_stop_sequence``/``destination_stop_sequence`` position the two
    calls for RT delay propagation (the destination sequence belongs to the
    earliest-arrival destination call the MIN aggregate selected).

    The descriptive tail mirrors :class:`~.models.UpcomingTrip`: trip-level
    wheelchair/bikes/direction, per-end stop_time descriptors, and the
    ``is_first``/``is_last`` service-day flags (see ``upcoming_trips``).
    """

    trip_id: str
    route_id: str
    service_id: str
    headsign: str | None
    origin_stop_id: str
    destination_stop_id: str
    departure: datetime
    arrival: datetime
    source_trip_id: str
    start_secs: int | None
    service_date: date
    origin_stop_sequence: int
    destination_stop_sequence: int
    wheelchair_accessible: WheelchairAccess | None
    bikes_allowed: BikesAllowed | None
    direction_id: int | None
    origin_pickup_type: PickupDropOffType | None
    origin_drop_off_type: PickupDropOffType | None
    origin_timepoint_exact: bool | None
    origin_stop_headsign: str | None
    destination_pickup_type: PickupDropOffType | None
    destination_drop_off_type: PickupDropOffType | None
    destination_timepoint_exact: bool | None
    destination_stop_headsign: str | None
    is_first: bool
    is_last: bool
    trip_short_name: str | None
    block_id: str | None


def _days(first: date, last: date, *, backward: bool = False) -> Iterator[date]:
    """Yield the dates from ``first`` through ``last``, lazily; or last to first."""
    for offset in range(max((last - first).days + 1, 0)):
        yield (
            last - timedelta(days=offset)
            if backward
            else first + timedelta(days=offset)
        )


class _RunQuery(NamedTuple):
    """One trip's runs to choose among: its service, span, and the instant."""

    service_id: str
    start_secs: int
    end_secs: int
    now: datetime


def _file_uri(path: str) -> str:
    """Return a ``file:`` URI for ``path`` with an empty authority.

    SQLite refuses any URI authority but ``localhost``; ``Path.as_uri()``
    gives a Windows network path (a UNC share) one. An empty
    authority before the full path (``file:////server/share/...``) is the
    form SQLite passes on unchanged.
    """
    posix = Path(path).absolute().as_posix()
    if not posix.startswith("/"):
        posix = f"/{posix}"  # a drive letter: file:///C:/...
    return "file://" + quote(posix, safe="/:")


def _scratch_path(db_target: str) -> str:
    """Create an empty scratch file beside ``db_target``; return its path.

    Unique per build: a fixed name would be left behind by a build killed
    midway (its tables then make every later build fail), and two builds of
    one target would share it.
    """
    target = Path(db_target)
    fd, path = tempfile.mkstemp(
        dir=target.parent, prefix=f"{target.name}.", suffix=".building"
    )
    os.close(fd)
    return path


class StaticIndex:
    """Read interface over the built SQLite database."""

    def __init__(
        self, conn: sqlite3.Connection, dataset_id: str, timezone_name: str
    ) -> None:
        """Wrap an open connection. Use ``build``/``open_cached`` instead."""
        self._conn = conn
        self.dataset_id = dataset_id
        self.timezone_name = timezone_name
        self._tz = ZoneInfo(timezone_name)
        self._last_dates: dict[str, date] | None = None
        self._first_dates: dict[str, date] | None = None
        self._first_date: date | None = None
        self._first_date_known = False
        self._reach: tuple[int, tuple[int, ...]] | None = None

    # -- construction ------------------------------------------------------

    @classmethod
    def build(
        cls,
        zip_path: Path,
        db_target: str,
        dataset_id: str,
        timezone_name: str | None,
        progress: Callable[[int, int | None], None] | None = None,
    ) -> StaticIndex:
        """Parse the GTFS zip into a fresh database.

        ``db_target`` is ``":memory:"`` or a filesystem path; file builds
        write to a scratch file of their own beside it
        (``<name>.<random>.building``) then atomically replace, so a reader
        of the old file is never left with a half-built database, a build
        killed midway never blocks the next one, and two builds of one
        target never write into each other's file. ``progress``,
        when given, is called with ``(done_bytes, total_bytes)`` as each
        source file is parsed (``total_bytes`` may be ``None``).

        Stop times are "in the time zone specified by
        ``agency.agency_timezone``", so agency.txt's zone wins; the
        catalog's ``timezone_name`` is used only when agency.txt names no
        valid zone (absent file, blank cell, unknown name).
        """
        try:
            archive = zipfile.ZipFile(zip_path)
        except (zipfile.BadZipFile, OSError) as err:
            raise FeedParseError(f"Unreadable GTFS zip: {err}") from err
        with archive:
            names = set(archive.namelist())
            # Before the timezone: a zip whose files sit in a subfolder has
            # no agency.txt either, and should say what is actually wrong.
            if missing := sorted(_REQUIRED_FILES - names):
                raise FeedParseError(f"GTFS zip missing required file: {missing[0]}")
            timezone_name = next(
                (
                    name
                    for name in (
                        cls._timezone_from_agency(archive, names),
                        timezone_name,
                    )
                    if name is not None and _valid_zone(name)
                ),
                None,
            )
            if timezone_name is None:
                raise FeedParseError(
                    "No valid agency timezone available (agency.txt or catalog)"
                )
            in_memory = db_target == ":memory:"
            build_path = db_target if in_memory else _scratch_path(db_target)
            conn: sqlite3.Connection | None = None
            try:
                conn = sqlite3.connect(build_path, check_same_thread=False)
                conn.executescript(_SCHEMA)
                cls._load_all(conn, archive, names, progress)
                cls._store_first_departures(conn)
                conn.executemany(
                    "INSERT INTO meta (key, value) VALUES (?, ?)",
                    [
                        ("dataset_id", dataset_id),
                        ("schema_version", str(SCHEMA_VERSION)),
                        ("timezone", timezone_name),
                    ],
                )
                # Single commit for the whole build: sqlite3's implicit-BEGIN
                # semantics already wrap everything above in one transaction,
                # so a mid-build commit would only risk leaving a partially
                # visible file on disk. Do not add one.
                conn.commit()
            except BaseException:
                if conn is not None:
                    conn.close()
                if not in_memory:
                    Path(build_path).unlink(missing_ok=True)
                raise
        if in_memory:
            return cls(conn, dataset_id, timezone_name)
        conn.close()
        try:
            Path(build_path).replace(db_target)
        except OSError:
            Path(build_path).unlink(missing_ok=True)
            raise
        # Reopened by name: a concurrent build of the same target may have
        # replaced ours since the rename. Label the index with the dataset
        # the file actually holds, so it is never served as another (the
        # caller decides what a different one means). mode=rw: a target
        # purged meanwhile is an error, not a new empty file.
        try:
            conn = sqlite3.connect(
                f"{_file_uri(db_target)}?mode=rw", uri=True, check_same_thread=False
            )
        except sqlite3.Error as err:
            raise FeedParseError(
                f"{db_target} could not be reopened after its build"
            ) from err
        try:
            meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
            if (
                not isinstance(meta.get("dataset_id"), str)
                or not isinstance(meta.get("timezone"), str)
                or meta.get("schema_version") != str(SCHEMA_VERSION)
            ):
                raise FeedParseError(f"{db_target} is not a built static index")
            return cls(conn, meta["dataset_id"], meta["timezone"])
        except (sqlite3.Error, ZoneInfoNotFoundError, ValueError, OSError) as err:
            conn.close()
            raise FeedParseError(f"{db_target} is not a built static index") from err
        except BaseException:
            conn.close()
            raise

    @classmethod
    def open_cached(cls, db_path: Path, dataset_id: str) -> StaticIndex | None:
        """Open an existing DB if it matches dataset ID and schema version.

        Total over whatever sits at ``db_path``: anything that is not a
        usable cached database is a cache miss (None). The caller rebuilds
        on None but lets exceptions propagate, so raising here would brick
        a feed instead of letting a corrupt entry self-heal. Non-regular
        files (a directory, a FIFO) are rejected before connecting --
        sqlite3.connect itself raises on those, outside any query guard.
        Every failure path closes the connection it opened.
        """
        if not Path(db_path).is_file():
            return None
        try:
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
        except (sqlite3.Error, OSError):
            return None
        try:
            meta = dict(conn.execute("SELECT key, value FROM meta"))
        except (sqlite3.Error, OSError):
            conn.close()
            return None
        if (
            meta.get("dataset_id") != dataset_id
            or meta.get("schema_version") != str(SCHEMA_VERSION)
            or "timezone" not in meta
        ):
            conn.close()
            return None
        try:
            return cls(conn, dataset_id, meta["timezone"])
        except (ZoneInfoNotFoundError, TypeError, ValueError, OSError):
            # Unusable cached timezone metadata (unknown key, empty, or a
            # non-string the column's TEXT affinity left as a blob) is a
            # cache miss like any other corruption: ZoneInfo raises in
            # __init__, one layer below the guards above.
            conn.close()
            return None

    @staticmethod
    def _timezone_from_agency(archive: zipfile.ZipFile, names: set[str]) -> str | None:
        """Return the first non-blank agency_timezone in agency.txt, if any.

        Read before the loaders run, so an unreadable file (not UTF-8, a
        ragged row) fails here as the loaders would: FeedParseError.
        """
        if "agency.txt" not in names:
            return None
        with _open_csv(archive, "agency.txt") as (_fp, reader):
            for row in reader:
                tz_name = (row.get("agency_timezone") or "").strip()
                if tz_name:
                    return tz_name
        return None

    @classmethod
    def _load_all(
        cls,
        conn: sqlite3.Connection,
        archive: zipfile.ZipFile,
        names: set[str],
        progress: Callable[[int, int | None], None] | None = None,
    ) -> None:
        files = (
            ("agency.txt", cls._load_agencies),
            ("stops.txt", cls._load_stops),
            ("routes.txt", cls._load_routes),
            ("trips.txt", cls._load_trips),
            ("stop_times.txt", cls._load_stop_times),
            # frequencies.txt MUST follow trips.txt and stop_times.txt: its
            # loader reads both tables to materialize repetitions.
            ("frequencies.txt", cls._load_frequencies),
            ("calendar.txt", cls._load_calendar),
            ("calendar_dates.txt", cls._load_calendar_dates),
            ("feed_info.txt", cls._load_feed_info),
        )
        parsed = [filename for filename, _ in files if filename in names]
        total = sum(archive.getinfo(filename).file_size for filename in parsed) or None
        completed = 0
        for filename, loader in files:
            if filename not in names:
                continue  # build() has already rejected a missing required file
            with _open_csv(archive, filename) as (fp, reader):
                loader(conn, reader, _reporter(progress, fp, completed, total))
            completed += archive.getinfo(filename).file_size
            if progress is not None:
                progress(completed, total)

    @staticmethod
    def _store_first_departures(conn: sqlite3.Connection) -> None:
        """Fill every trip's ``first_secs``: its first departure.

        GTFS-RT reads a trip's start as its first departure: the departure
        of its first TIMED call by stop_sequence (the loader already lets a
        lone arrival or departure stand for both), the earliest where that
        sequence repeats; a trip of one call starts at its arrival. Matches
        :meth:`_trip_spans`. Runs once all stop_times, frequency
        repetitions included, are loaded.
        """
        # Separate statements, not executescript: that commits first, and the
        # build must stay one transaction (see build).
        conn.execute(
            "CREATE TEMP TABLE first_departures "
            "(trip_id TEXT PRIMARY KEY, secs INTEGER)"
        )
        conn.execute(
            """
            INSERT INTO first_departures
            SELECT b.trip_id,
                   CASE WHEN b.first = b.last THEN MIN(st.arrival_secs)
                        ELSE MIN(st.departure_secs) END
            FROM (
                SELECT trip_id, MIN(stop_sequence) AS first,
                       MAX(stop_sequence) AS last
                FROM stop_times
                WHERE arrival_secs IS NOT NULL OR departure_secs IS NOT NULL
                GROUP BY trip_id
            ) AS b
            JOIN stop_times st
              ON st.trip_id = b.trip_id AND st.stop_sequence = b.first
             AND (st.arrival_secs IS NOT NULL OR st.departure_secs IS NOT NULL)
            GROUP BY b.trip_id
            """
        )
        conn.execute(
            "UPDATE trips SET first_secs = "
            "(SELECT secs FROM first_departures WHERE trip_id = trips.id)"
        )
        conn.execute("DROP TABLE first_departures")

    @staticmethod
    def _batched_insert(
        conn: sqlite3.Connection, sql: str, rows: list[tuple[object, ...]]
    ) -> None:
        if rows:
            conn.executemany(sql, rows)
            rows.clear()

    @classmethod
    def _load_agencies(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        sql = "INSERT INTO agencies VALUES (?,?,?,?,?,?,?,?)"
        for row in reader:
            rows.append(
                (
                    row.get("agency_id") or None,
                    row.get("agency_name") or None,
                    row.get("agency_url") or None,
                    row.get("agency_timezone") or None,
                    row.get("agency_lang") or None,
                    row.get("agency_phone") or None,
                    row.get("agency_fare_url") or None,
                    row.get("agency_email") or None,
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(conn, sql, rows)
                if report is not None:
                    report()
        cls._batched_insert(conn, sql, rows)
        if report is not None:
            report()

    @classmethod
    def _load_stops(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        sql = "INSERT OR REPLACE INTO stops VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
        for row in reader:
            rows.append(
                (
                    row["stop_id"],
                    row.get("stop_name") or None,
                    float(lat) if (lat := row.get("stop_lat", "").strip()) else None,
                    float(lon) if (lon := row.get("stop_lon", "").strip()) else None,
                    row.get("parent_station") or None,
                    _defaulted_int(row.get("location_type"), 0),
                    row.get("stop_code") or None,
                    row.get("platform_code") or None,
                    _defaulted_int(row.get("wheelchair_boarding"), 0),
                    row.get("stop_desc") or None,
                    row.get("stop_url") or None,
                    row.get("zone_id") or None,
                    row.get("stop_timezone") or None,
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(conn, sql, rows)
                if report is not None:
                    report()
        cls._batched_insert(conn, sql, rows)
        if report is not None:
            report()

    @classmethod
    def _load_routes(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        sql = "INSERT OR REPLACE INTO routes VALUES (?,?,?,?,?,?,?,?,?,?)"
        for row in reader:
            rows.append(
                (
                    row["route_id"],
                    row.get("route_short_name") or None,
                    row.get("route_long_name") or None,
                    _lenient_int(row.get("route_type")),
                    row.get("agency_id") or None,
                    _defaulted_text(row.get("route_color"), "FFFFFF"),
                    _defaulted_text(row.get("route_text_color"), "000000"),
                    row.get("route_url") or None,
                    row.get("route_desc") or None,
                    _lenient_int(row.get("route_sort_order")),
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(conn, sql, rows)
                if report is not None:
                    report()
        cls._batched_insert(conn, sql, rows)
        if report is not None:
            report()

    @classmethod
    def _load_trips(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        sql = "INSERT OR REPLACE INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)"
        for row in reader:
            rows.append(
                (
                    row["trip_id"],
                    row["route_id"],
                    row["service_id"],
                    row.get("trip_headsign") or None,
                    row["trip_id"],  # source_trip_id: a plain trip is its own source
                    None,  # start_secs: only frequency repetitions carry one
                    _defaulted_int(row.get("wheelchair_accessible"), 0),
                    _defaulted_int(row.get("bikes_allowed"), 0),
                    _lenient_int(row.get("direction_id")),
                    row.get("trip_short_name") or None,
                    row.get("block_id") or None,
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(conn, sql, rows)
                if report is not None:
                    report()
        cls._batched_insert(conn, sql, rows)
        if report is not None:
            report()

    @classmethod
    def _load_stop_times(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        sql = "INSERT INTO stop_times VALUES (?,?,?,?,?,?,?,?,?)"
        for row in reader:
            arrival = parse_gtfs_time(row.get("arrival_time", ""))
            departure = parse_gtfs_time(row.get("departure_time", ""))
            rows.append(
                (
                    row["trip_id"],
                    row["stop_id"],
                    # "If there are not separate times for arrival and
                    # departure at a stop, arrival_time and departure_time
                    # should be the same": a call timed by one of them alone
                    # (legal for any stop -- departure_time is required for
                    # none but timepoints) arrives and departs at that time.
                    arrival if arrival is not None else departure,
                    departure if departure is not None else arrival,
                    _strict_int(row["stop_sequence"], "stop_sequence"),
                    _defaulted_int(row.get("pickup_type"), 0),
                    _defaulted_int(row.get("drop_off_type"), 0),
                    _stored_timepoint(row.get("timepoint")),
                    row.get("stop_headsign") or None,
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(conn, sql, rows)
                if report is not None:
                    report()
        cls._batched_insert(conn, sql, rows)
        if report is not None:
            report()

    @classmethod
    def _load_frequencies(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        """Materialize frequencies.txt repetitions into concrete trips.

        frequencies.txt defines a trip's stop_times rows as a TEMPLATE plus
        repetition rules; the template alone is not a real trip. Each
        repetition becomes a full copy of the template under a synthetic
        trip id ``{trip_id}#{start_secs}`` (e.g. ``CITY1#21600``), shifted
        so the first stop's DEPARTURE lands on the repetition start --
        ``start_time`` is the "time at which the first vehicle departs from
        the first stop" -- while every other call keeps its offset from it;
        the original template rows are then deleted. A shifted time that
        would fall before its service day starts (an origin dwell on a
        repetition starting at 00:00:00, or a template timed backwards) is
        not representable and is stored blank, the call's other time
        standing in for it as at load. Repetition starts are ``start_time
        + n * headway_secs`` for n = 0.. while STRICTLY less than
        ``end_time`` (a repetition landing exactly on end_time does not
        run), and identical start seconds produced by overlapping rows for
        the same trip are deduplicated (the first row wins) so synthetic
        ids stay unique. Both exact_times values are materialized at the
        stated headway, the standard journey-planner interpretation; but
        exact_times=0 (or empty) is service that "does not follow a fixed
        schedule", so its repetitions' times are stored as approximate
        (timepoint 0) whatever the template says, and an unparseable
        exact_times leaves timepoint unknown. An exact_times=0 repetition
        also keeps its headway as ``slot_secs``: GTFS-RT names such a run by
        a start_time that "may be arbitrary", and :meth:`slot_runs` finds
        the repetition whose slot it starts in.

        A trips row is duplicated per repetition under the synthetic id
        (INSERT..SELECT from the original row, a no-op for orphan trips)
        so every downstream join on trips.id -- departure boards, trip
        queries, route/headsign pickers -- works unchanged; the ORIGINAL
        trip row is kept so RT lookups keyed by the bare template trip id
        (e.g. vehicle positions) still resolve a route. Each duplicated
        row carries ``(source_trip_id, start_secs)`` = (template trip id,
        repetition start), the identity a GTFS-RT TripDescriptor
        (trip_id + start_time) addresses, so realtime matching reads real
        columns instead of string-parsing the synthetic id (which would be
        ambiguous if a real trip id contained ``#``). Materialized times
        may exceed 24:00:00 and flow through the normal service-day
        handling. A row referencing a trip with no stop_times template is
        skipped, matching the loaders' no-referential-checks policy. A
        real trip id that happens to equal a synthetic one (it would need
        a literal ``#<secs>`` suffix) would be overwritten -- accepted as
        negligible. Malformed rows (unparseable or missing times,
        non-positive headway) raise, mirroring stop_times.
        """
        spans = cls._parse_frequency_spans(reader)
        # The INSERT..SELECT and the template copy below must carry EVERY
        # descriptive trip/stop_time column, or repetitions would silently
        # drop wheelchair/bikes/direction and pickup/drop-off/timepoint/
        # headsign metadata their template declared.
        trips_sql = (
            "INSERT OR REPLACE INTO trips "
            "(id, route_id, service_id, headsign, source_trip_id, start_secs, "
            "wheelchair_accessible, bikes_allowed, direction_id, "
            "short_name, block_id, slot_secs) "
            "SELECT ?, route_id, service_id, headsign, ?, ?, "
            "wheelchair_accessible, bikes_allowed, direction_id, "
            "short_name, block_id, ? "
            "FROM trips WHERE id = ?"
        )
        stop_times_sql = "INSERT INTO stop_times VALUES (?,?,?,?,?,?,?,?,?)"
        trip_rows: list[tuple[object, ...]] = []
        stop_time_rows: list[tuple[object, ...]] = []
        for trip_id, trip_spans in spans.items():
            template = conn.execute(
                "SELECT stop_id, arrival_secs, departure_secs, stop_sequence, "
                "pickup_type, drop_off_type, timepoint, stop_headsign "
                "FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence",
                (trip_id,),
            ).fetchall()
            if not template:
                continue  # dangling reference: nothing to repeat
            # The loader already lets a lone arrival stand in for a blank
            # departure, so this is None only for an untimed first stop.
            anchor = template[0][2]
            if anchor is None:
                raise ValueError(
                    f"frequency trip {trip_id!r} has no first-stop time "
                    "to anchor repetition offsets"
                )
            conn.execute("DELETE FROM stop_times WHERE trip_id = ?", (trip_id,))
            starts: dict[int, tuple[int | None, int]] = {}
            for start_secs, end_secs, headway_secs, exact_times in trip_spans:
                rep_start = start_secs
                while rep_start < end_secs:
                    starts.setdefault(rep_start, (exact_times, headway_secs))
                    rep_start += headway_secs
            for rep_start, (exact_times, headway_secs) in sorted(starts.items()):
                synthetic_id = repetition_trip_id(trip_id, rep_start)
                shift = rep_start - anchor
                trip_rows.append(
                    (
                        synthetic_id,
                        trip_id,
                        rep_start,
                        headway_secs if exact_times == 0 else None,
                        trip_id,
                    )
                )
                for (
                    stop_id,
                    arrival_secs,
                    departure_secs,
                    stop_sequence,
                    pickup_type,
                    drop_off_type,
                    timepoint,
                    stop_headsign,
                ) in template:
                    arrival = _shifted(arrival_secs, shift)
                    departure = _shifted(departure_secs, shift)
                    stop_time_rows.append(
                        (
                            synthetic_id,
                            stop_id,
                            arrival if arrival is not None else departure,
                            departure if departure is not None else arrival,
                            stop_sequence,
                            pickup_type,
                            drop_off_type,
                            timepoint if exact_times == 1 else exact_times,
                            stop_headsign,
                        )
                    )
                if len(stop_time_rows) >= _BATCH_SIZE:
                    cls._batched_insert(conn, trips_sql, trip_rows)
                    cls._batched_insert(conn, stop_times_sql, stop_time_rows)
                    if report is not None:
                        report()
        cls._batched_insert(conn, trips_sql, trip_rows)
        cls._batched_insert(conn, stop_times_sql, stop_time_rows)
        if report is not None:
            report()

    @staticmethod
    def _parse_frequency_spans(
        reader: csv.DictReader[str],
    ) -> dict[str, list[tuple[int, int, int, int | None]]]:
        """Parse frequencies rows into per-trip spans.

        Each span is (start, end, headway, exact_times): exact_times is 0
        for an absent or blank cell ("0 or empty"), 0/1 as written, and
        None when unparseable or outside that vocabulary. Raises (via the
        ``_load_all`` wrapping into FeedParseError) on rows missing a
        start/end time or carrying a non-positive headway.
        """
        spans: dict[str, list[tuple[int, int, int, int | None]]] = {}
        for row in reader:
            trip_id = row["trip_id"]
            start_secs = parse_gtfs_time(row.get("start_time", ""))
            end_secs = parse_gtfs_time(row.get("end_time", ""))
            if start_secs is None or end_secs is None:
                raise ValueError(
                    f"frequencies row for trip {trip_id!r} lacks a start/end time"
                )
            headway_secs = _strict_int(row["headway_secs"], "headway_secs")
            if headway_secs <= 0:
                raise ValueError(
                    f"non-positive headway_secs for trip {trip_id!r}: {headway_secs}"
                )
            exact_times = _defaulted_int(row.get("exact_times"), 0)
            spans.setdefault(trip_id, []).append(
                (
                    start_secs,
                    end_secs,
                    headway_secs,
                    exact_times if exact_times in (0, 1) else None,
                )
            )
        return spans

    @classmethod
    def _load_calendar(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        sql = "INSERT OR REPLACE INTO calendar VALUES (?,?,?,?,?,?,?,?,?,?)"
        for row in reader:
            rows.append(
                (
                    row["service_id"],
                    *(
                        _strict_int(row.get(day) or "0", day)
                        for day in _WEEKDAY_COLUMNS
                    ),
                    _canonical_date(row.get("start_date")),
                    _canonical_date(row.get("end_date")),
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(conn, sql, rows)
                if report is not None:
                    report()
        cls._batched_insert(conn, sql, rows)
        if report is not None:
            report()

    @classmethod
    def _load_calendar_dates(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        sql = "INSERT INTO calendar_dates VALUES (?,?,?)"
        for row in reader:
            day = _canonical_date(row["date"])
            if day is None:
                # An exception on a date that does not parse can never apply:
                # skip it (it would match no service day anyway).
                continue
            rows.append(
                (
                    row["service_id"],
                    day,
                    _strict_int(row["exception_type"], "exception_type"),
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(conn, sql, rows)
                if report is not None:
                    report()
        cls._batched_insert(conn, sql, rows)
        if report is not None:
            report()

    @classmethod
    def _load_feed_info(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        """Load feed_info.txt: a single-record file, so the FIRST data row wins.

        GTFS defines exactly one record; a producer shipping extras is an
        error the loader resolves by keeping row one and ignoring the rest
        (documented on :class:`~.models.FeedInfo`). Date cells stay raw
        text; the model boundary parses them leniently.
        """
        for row in reader:
            conn.execute(
                "INSERT INTO feed_info VALUES (?,?,?,?,?,?,?,?)",
                (
                    row.get("feed_publisher_name") or None,
                    row.get("feed_publisher_url") or None,
                    row.get("feed_lang") or None,
                    row.get("feed_version") or None,
                    row.get("feed_start_date") or None,
                    row.get("feed_end_date") or None,
                    row.get("feed_contact_email") or None,
                    row.get("feed_contact_url") or None,
                ),
            )
            break
        if report is not None:
            report()

    # -- queries -----------------------------------------------------------

    def stops(self) -> list[Stop]:
        """All stops (for pickers), with GTFS's parent inheritance applied.

        A stop whose ``wheelchair_boarding`` is 0 or empty "will inherit its
        wheelchair_boarding behavior from the parent station"; a stop with
        a parent "inherits the parent station's timezone instead of applying
        its own", and one without falls back to ``agency_timezone``. Both
        resolve through the parent's own resolved value, so a boarding area
        under a platform under a station reads the station's. A parent id
        naming no stop, or a ``parent_station`` cycle, ends the chain there.
        """
        rows = self._conn.execute(
            "SELECT id, name, lat, lon, parent_station, location_type, "
            "stop_code, platform_code, wheelchair_boarding, "
            "description, url, zone_id, timezone "
            "FROM stops ORDER BY name"
        ).fetchall()
        by_id = {row[0]: row for row in rows}
        return [
            Stop(
                id=row[0],
                name=row[1],
                latitude=row[2],
                longitude=row[3],
                parent_station=row[4],
                location_type=_enum_or_none(StopLocationType, row[5]),
                stop_code=row[6],
                platform_code=row[7],
                wheelchair_boarding=_enum_or_none(
                    WheelchairAccess, _resolved_boarding(by_id, row)
                ),
                description=row[9],
                url=row[10],
                zone_id=row[11],
                timezone=_ancestor(by_id, row, lambda _: True)[12]
                or self.timezone_name,
            )
            for row in rows
        ]

    @staticmethod
    def _route_from_row(row: tuple[Any, ...]) -> Route:
        """One Route from a full-width routes row (shared by both queries)."""
        return Route(
            id=row[0],
            short_name=row[1],
            long_name=row[2],
            type=row[3],
            agency_id=row[4],
            color=row[5],
            text_color=row[6],
            url=row[7],
            description=row[8],
            sort_order=row[9],
        )

    def routes(self) -> list[Route]:
        """All routes (for pickers)."""
        return [
            self._route_from_row(row)
            for row in self._conn.execute(
                "SELECT id, short_name, long_name, type, "
                "agency_id, color, text_color, url, description, sort_order "
                "FROM routes ORDER BY id"
            )
        ]

    def feed_info(self) -> FeedInfo | None:
        """Return the feed_info.txt record, or None when the file was absent.

        A single record by definition (the loader keeps only the first
        data row); date columns parse leniently at this boundary, so a
        malformed feed_start_date surfaces as None rather than an error.
        """
        row = self._conn.execute(
            "SELECT publisher_name, publisher_url, lang, version, "
            "start_date, end_date, contact_email, contact_url FROM feed_info"
        ).fetchone()
        if row is None:
            return None
        return FeedInfo(
            publisher_name=row[0],
            publisher_url=row[1],
            lang=row[2],
            version=row[3],
            start_date=_lenient_date(row[4]),
            end_date=_lenient_date(row[5]),
            contact_email=row[6],
            contact_url=row[7],
        )

    def agencies(self) -> list[Agency]:
        """All agencies, in agency.txt order (route rows reference them)."""
        return [
            Agency(
                id=row[0],
                name=row[1],
                url=row[2],
                timezone=row[3],
                lang=row[4],
                phone=row[5],
                fare_url=row[6],
                email=row[7],
            )
            for row in self._conn.execute(
                "SELECT id, name, url, timezone, lang, phone, fare_url, email "
                "FROM agencies"
            )
        ]

    def routes_by_id(self) -> dict[str, Route]:
        """Map of route_id to the full Route record.

        Supersedes building a per-attribute map off :meth:`routes`: the
        rows are materialized into Route objects either way, so handing
        back the whole record costs nothing over discarding all but one
        field.
        """
        return {route.id: route for route in self.routes()}

    def stops_by_id(self) -> dict[str, Stop]:
        """Map of stop_id to the full Stop record."""
        return {stop.id: stop for stop in self.stops()}

    def routes_for_trips(self, trip_ids: list[str]) -> dict[str, str]:
        """Map trip_id -> route_id for the given trips."""
        if not trip_ids:
            return {}
        marks = ",".join("?" * len(trip_ids))
        return dict(
            self._conn.execute(
                f"SELECT id, route_id FROM trips WHERE id IN ({marks})",
                trip_ids,
            )
        )

    def trip_summaries(
        self, trip_ids: Sequence[str]
    ) -> dict[str, tuple[str, str | None, int | None, str | None]]:
        """Map each trip to its (route_id, headsign, direction_id, short_name)."""
        found: dict[str, tuple[str, str | None, int | None, str | None]] = {}
        for offset in range(0, len(trip_ids), _ID_SLICE):
            chunk = list(trip_ids[offset : offset + _ID_SLICE])
            for trip_id, route_id, headsign, direction_id, short in self._conn.execute(
                "SELECT id, route_id, headsign, direction_id, short_name FROM trips "
                f"WHERE id IN ({','.join('?' * len(chunk))})",
                chunk,
            ):
                found[trip_id] = (route_id, headsign, direction_id, short)
        return found

    def copied_run(
        self,
        source_trip_id: str,
        start_secs: int | None,
        service_date: date,
        copy_start_secs: int,
    ) -> TripInstanceCalls | None:
        """Return a trip's calls copied onto a day, starting at ``copy_start_secs``.

        GTFS-RT DUPLICATED: "Scheduled arrival and departure times for the
        duplicated trip are calculated based on the offset between the
        original trip departure_time and this field". The original is the
        run a TripDescriptor names -- a plain trip (``start_secs`` None or
        its own first departure) or one exact_times=1 repetition -- and
        every call keeps its offset from that trip's first departure. The
        copy runs whether or not the original's service runs that day. None
        for a trip naming no run, one with no timed call, or an
        exact_times=0 repetition: such trips "cannot be duplicated".
        """
        rows = self._conn.execute(
            "SELECT id, service_id, start_secs, first_secs FROM trips "
            "WHERE source_trip_id = ? AND first_secs IS NOT NULL "
            "AND slot_secs IS NULL",
            (source_trip_id,),
        ).fetchall()
        named = [
            (trip_id, service_id, first)
            for trip_id, service_id, repetition, first in rows
            if (repetition is not None and repetition == start_secs)
            or (
                repetition is None
                and trip_id == source_trip_id
                and start_secs in (None, first)
            )
        ]
        if len(named) != 1:
            return None
        [(trip_id, service_id, first)] = named
        try:
            day_start = self._service_day_start(service_date) + timedelta(
                seconds=copy_start_secs - first
            )
        except OverflowError:
            return None
        found = self._instance_calls_on(
            service_date, day_start, [trip_id], {service_id}
        )
        # The calls are placed from the moved instant; the run's day starts
        # where the copy's service day does.
        return (
            replace(found[0], day_start_utc=self._service_day_start(service_date))
            if found
            else None
        )

    def slot_runs[K: tuple[str, date | None, int | None]](
        self, keys: Iterable[K]
    ) -> dict[K, SlotRun]:
        """Resolve realtime references to exact_times=0 trips to their runs.

        Each key is ``(trip id, start_date, start_secs)``. GTFS-RT: such a
        trip's start_time "may be arbitrary, and is initially expected to be
        the first departure of the trip", so a run is the trip at that
        start. frequencies.txt gives it no schedule of its own, only
        headways: the run takes the place of the repetition whose slot holds
        its start -- from that repetition's start for one headway, but never
        past the trip's next repetition ("New headways may start at the
        exact time the previous headway ends", and an exact_times=1 one is
        a run of its own) -- and is that repetition's calls moved to its
        start (a call materialization left untimed stays so). A key naming
        no such slot, or a day the trip's service does not run, names
        nothing here.
        """
        keys = [key for key in keys if key[1] is not None and key[2] is not None]
        sources = sorted({source for source, _day, _secs in keys})
        # Every repetition, in order: a slot ends at the next one.
        repetitions: dict[str, list[tuple[int, int | None, str]]] = {}
        # In slices: realtime can name more trips than SQLite allows bound
        # variables in one statement.
        for offset in range(0, len(sources), _ID_SLICE):
            chunk = sources[offset : offset + _ID_SLICE]
            for trip_id, source, start, width in self._conn.execute(
                "SELECT id, source_trip_id, start_secs, slot_secs FROM trips "
                f"WHERE source_trip_id IN ({','.join('?' * len(chunk))}) "
                "AND start_secs IS NOT NULL ORDER BY source_trip_id, start_secs",
                chunk,
            ):
                repetitions.setdefault(source, []).append((start, width, trip_id))
        taken: dict[K, tuple[int, str, date]] = {}
        for key in keys:
            source, day, secs = key
            assert day is not None and secs is not None
            runs_of = repetitions.get(source, [])
            for n, (start, width, trip_id) in enumerate(runs_of):
                if width is None:
                    continue  # exact times: a run of its own, no slot
                end = start + width
                if n + 1 < len(runs_of):
                    end = min(end, runs_of[n + 1][0])
                if start <= secs < end:
                    taken[key] = (start, trip_id, day)
                    break
        runs = {
            (run.trip_id, run.service_date): run
            for run in self.instance_trip_calls(
                {(trip_id, day) for _start, trip_id, day in taken.values()}
            )
        }
        found: dict[K, SlotRun] = {}
        for key, (start, trip_id, day) in taken.items():
            source, _day, secs = key
            assert secs is not None
            run = runs.get((trip_id, day))
            if run is None:
                continue  # the trip's service does not run that day
            shift = timedelta(seconds=secs - start)
            found[key] = SlotRun(
                start,
                replace(
                    run,
                    trip_id=repetition_trip_id(source, secs),
                    start_secs=secs,
                    calls=[
                        replace(
                            call,
                            arrival=_later(call.arrival, shift),
                            departure=_later(call.departure, shift),
                        )
                        for call in run.calls
                    ],
                ),
            )
        return found

    def instance_trip_calls(
        self, instances: Iterable[tuple[str, date]]
    ) -> list[TripInstanceCalls]:
        """Complete call sequences of specific (concrete trip, service day) runs.

        Keyed on CONCRETE trip ids, so a frequency repetition resolves to
        its own run. A run whose trip's service doesn't operate that day
        doesn't exist and yields nothing. Unlike ``upcoming_departures``
        this filters by trip rather than by stop and does not window the
        calls: a modification's reference stop may be anywhere on the trip.
        """
        by_day: dict[date, set[str]] = {}
        for trip_id, service_date in instances:
            by_day.setdefault(service_date, set()).add(trip_id)
        found: list[TripInstanceCalls] = []
        for service_date in sorted(by_day):
            active = self.active_service_ids(service_date)
            try:
                day_start = self._service_day_start(service_date)
            except OverflowError:
                continue  # a day whose start can't be dated has no runs
            if active:
                found.extend(
                    self._instance_calls_on(
                        service_date,
                        day_start,
                        sorted(by_day[service_date]),
                        active,
                    )
                )
        return found

    def trips_serving(
        self, source_trip_ids: Sequence[str], stop_ids: Sequence[str]
    ) -> set[str]:
        """Return the producer-facing trips (of those given) calling at a stop."""
        if not source_trip_ids or not stop_ids:
            return set()
        rows = self._conn.execute(
            "SELECT DISTINCT t.source_trip_id FROM trips t "
            "JOIN stop_times st ON st.trip_id = t.id "
            f"WHERE t.source_trip_id IN ({','.join('?' * len(source_trip_ids))}) "
            f"AND st.stop_id IN ({','.join('?' * len(stop_ids))})",
            [*source_trip_ids, *stop_ids],
        ).fetchall()
        return {source for (source,) in rows}

    def run_days(
        self, instances: Iterable[tuple[str, date]]
    ) -> list[tuple[str, date, datetime]]:
        """Return the (concrete trip, day) runs that exist, with day starts.

        A run exists when its trip's service operates that day. Cheap: no calls
        are read, so a caller can build one run's calls and shift them to
        every other day (GTFS times are offsets from the day's start).
        """
        by_day: dict[date, set[str]] = {}
        for trip_id, service_date in instances:
            by_day.setdefault(service_date, set()).add(trip_id)
        trip_ids = sorted({trip for trips in by_day.values() for trip in trips})
        if not trip_ids:
            return []
        service_of = dict(
            self._conn.execute(
                "SELECT id, service_id FROM trips "
                f"WHERE id IN ({','.join('?' * len(trip_ids))})",
                trip_ids,
            ).fetchall()
        )
        found: list[tuple[str, date, datetime]] = []
        for service_date in sorted(by_day):
            try:
                day_start = self._service_day_start(service_date)
            except OverflowError:
                continue  # a day whose start can't be dated has no runs
            active = self.active_service_ids(service_date)
            found.extend(
                (trip_id, service_date, day_start)
                for trip_id in sorted(by_day[service_date])
                if service_of.get(trip_id) in active
            )
        return found

    def _instance_calls_on(
        self,
        service_date: date,
        day_start_utc: datetime,
        trip_ids: list[str],
        active: set[str],
    ) -> list[TripInstanceCalls]:
        marks = ",".join("?" * len(trip_ids))
        instances: list[TripInstanceCalls] = []
        service_marks = ",".join("?" * len(active))
        rows = self._conn.execute(
            "SELECT t.id, t.source_trip_id, t.start_secs, t.route_id, "
            "t.service_id, t.headsign, t.wheelchair_accessible, "
            "t.bikes_allowed, t.direction_id, t.short_name, t.block_id, "
            "st.stop_sequence, st.stop_id, st.arrival_secs, "
            "st.departure_secs, st.pickup_type, st.drop_off_type, "
            "st.timepoint, st.stop_headsign "
            "FROM stop_times st JOIN trips t ON t.id = st.trip_id "
            f"WHERE t.id IN ({marks}) "
            f"AND t.service_id IN ({service_marks}) "
            "ORDER BY t.id, st.stop_sequence",
            [*trip_ids, *sorted(active)],
        ).fetchall()
        by_trip: dict[str, list[Any]] = {}
        for row in rows:
            by_trip.setdefault(row[0], []).append(row)
        for trip_id, trip_rows in by_trip.items():
            head = trip_rows[0]
            try:
                calls = [
                    TripCall(
                        stop_sequence=row[11],
                        stop_id=row[12],
                        arrival=(
                            day_start_utc + timedelta(seconds=row[13])
                            if row[13] is not None
                            else None
                        ),
                        departure=(
                            day_start_utc + timedelta(seconds=row[14])
                            if row[14] is not None
                            else None
                        ),
                        pickup_type=_enum_or_none(PickupDropOffType, row[15]),
                        drop_off_type=_enum_or_none(PickupDropOffType, row[16]),
                        timepoint_exact=_timepoint_exact(row[17]),
                        stop_headsign=row[18],
                    )
                    for row in trip_rows
                ]
            except OverflowError:
                continue  # a call past the end of datetime: no run to date
            instances.append(
                TripInstanceCalls(
                    trip_id=trip_id,
                    source_trip_id=head[1],
                    start_secs=head[2],
                    service_date=service_date,
                    day_start_utc=day_start_utc,
                    route_id=head[3],
                    service_id=head[4],
                    headsign=head[5],
                    wheelchair_accessible=_enum_or_none(WheelchairAccess, head[6]),
                    bikes_allowed=_enum_or_none(BikesAllowed, head[7]),
                    direction_id=head[8],
                    trip_short_name=head[9],
                    block_id=head[10],
                    calls=calls,
                )
            )
        return instances

    def concrete_trip_ids(self, source_trip_ids: list[str]) -> dict[str, list[str]]:
        """Map each PRODUCER-facing trip id to its concrete instances.

        A plain trip is its own only instance; a frequency template fans
        out to every materialized repetition. TripModifications name the
        producer's id, so applying one means finding the repetitions it
        covers.
        """
        if not source_trip_ids:
            return {}
        marks = ",".join("?" * len(source_trip_ids))
        found: dict[str, list[str]] = {}
        for source_trip_id, trip_id in self._conn.execute(
            "SELECT source_trip_id, id FROM trips "
            f"WHERE source_trip_id IN ({marks}) ORDER BY source_trip_id, id",
            source_trip_ids,
        ):
            found.setdefault(source_trip_id, []).append(trip_id)
        return found

    def routes_serving(self, stop_id: str) -> list[Route]:
        """Routes with at least one scheduled stop_time at the stop."""
        return [
            self._route_from_row(row)
            for row in self._conn.execute(
                "SELECT DISTINCT r.id, r.short_name, r.long_name, r.type, "
                "r.agency_id, r.color, r.text_color, r.url, "
                "r.description, r.sort_order "
                "FROM stop_times st "
                "JOIN trips t ON t.id = st.trip_id "
                "JOIN routes r ON r.id = t.route_id "
                "WHERE st.stop_id = ? ORDER BY r.id",
                (stop_id,),
            )
        ]

    def headsigns_serving(self, stop_id: str, route_id: str | None = None) -> list[str]:
        """Distinct headsigns shown at a stop, optionally narrowed to one route.

        The headsign at a stop is its stop_headsign where the stop_time row
        sets one -- it "overrides the default trips.trip_headsign" -- and the
        trip's headsign otherwise: the same value the headsign filters
        match, so picker options and filterable values always agree.
        """
        sql = (
            f"SELECT DISTINCT {_HEADSIGN_SQL} AS sign FROM stop_times st "
            "JOIN trips t ON t.id = st.trip_id "
            f"WHERE st.stop_id = ? AND {_HEADSIGN_SQL} IS NOT NULL"
        )
        params: list[str] = [stop_id]
        if route_id is not None:
            sql += " AND t.route_id = ?"
            params.append(route_id)
        sql += " ORDER BY sign"
        return [row[0] for row in self._conn.execute(sql, params)]

    def active_service_ids(self, service_date: date) -> set[str]:
        """Service IDs active on the given local service date."""
        datestr = _gtfs_date(service_date)
        weekday_col = _WEEKDAY_COLUMNS[service_date.weekday()]
        active = {
            row[0]
            for row in self._conn.execute(
                f"SELECT service_id FROM calendar WHERE {weekday_col} = 1 "
                "AND start_date <= ? AND end_date >= ?",
                (datestr, datestr),
            )
        }
        exceptions = self._conn.execute(
            "SELECT service_id, exception_type FROM calendar_dates WHERE date = ?",
            (datestr,),
        ).fetchall()
        # Two passes -- all additions, then all removals -- so a service
        # with BOTH exception types for the same date resolves the same way
        # regardless of which row the source CSV happened to list first.
        # GTFS doesn't define a tiebreak for this producer error, so
        # "removed wins" is the conservative choice (never show a trip that
        # might not run).
        for service_id, exception_type in exceptions:
            if exception_type == _EXCEPTION_SERVICE_ADDED:
                active.add(service_id)
        for service_id, exception_type in exceptions:
            if exception_type == _EXCEPTION_SERVICE_REMOVED:
                active.discard(service_id)
        return active

    def _first_service_date(self) -> date | None:
        """Return the first day any service can run; None for no services.

        From ``calendar`` rows that can run at all and ``calendar_dates``
        ADDED dates: a lower bound, like :meth:`_service_last_dates`'s upper
        one. Computed once; the index never changes.
        """
        if not self._first_date_known:
            weekday_any = " OR ".join(f"{day} = 1" for day in _WEEKDAY_COLUMNS)
            rows = self._conn.execute(
                "SELECT start_date FROM calendar "
                f"WHERE ({weekday_any}) AND start_date <= end_date "
                "UNION ALL SELECT MIN(date) FROM calendar_dates "
                "WHERE exception_type = ?",
                (_EXCEPTION_SERVICE_ADDED,),
            )
            dates = [d for (raw,) in rows if (d := _lenient_date(raw)) is not None]
            self._first_date = min(dates, default=None)
            self._first_date_known = True
        return self._first_date

    def _reach_profile(self) -> tuple[int, tuple[int, ...]]:
        """Return (normal reach, long reaches): how far departures reach past their day.

        The window scans select rows by departure time, so departures alone
        decide which days they read -- an arrival, however late, never puts
        a row in a window. Most departures fall within a few days of their
        service day; those bound the day scan's look-back. Later ones
        (multi-week trips, or garbage like 30000000:00:00) would drag every
        scan back years, so they are listed separately and placed row by
        row. Computed once.
        """
        if self._reach is None:
            normal = self._conn.execute(
                "SELECT MAX(departure_secs) FROM stop_times WHERE departure_secs < ?",
                (_LONG_REACH,),
            ).fetchone()[0]
            long = self._conn.execute(
                "SELECT DISTINCT departure_secs FROM stop_times "
                "WHERE departure_secs >= ? ORDER BY departure_secs",
                (_LONG_REACH,),
            ).fetchall()
            self._reach = (normal or 0, tuple(row[0] for row in long))
        return self._reach

    def _reaches(self, service_date: date, secs: int, at: datetime) -> bool:
        """Whether a run on ``service_date`` can still be under way at ``at``."""
        try:
            return self._service_day_start(service_date) + timedelta(seconds=secs) >= at
        except OverflowError:
            # Past the end of datetime it reaches anything; before its start
            # (a year-1 day east of UTC) no run can be dated at all.
            return service_date.year > 1

    def _first_reaching_day(
        self, lower: date, upper: date, secs: int, at: datetime
    ) -> date | None:
        """Return the first day in [lower, upper] whose runs reach ``at``.

        Day starts only increase, so the days that reach are a suffix:
        binary search, O(log days) whatever the stop times hold.
        """
        if lower > upper or not self._reaches(upper, secs, at):
            return None
        low, high = 0, (upper - lower).days
        while low < high:
            middle = (low + high) // 2
            if self._reaches(lower + timedelta(days=middle), secs, at):
                high = middle
            else:
                low = middle + 1
        return lower + timedelta(days=low)

    def _long_reach_days(
        self, window_start: datetime, window_end: datetime, before: date
    ) -> set[date]:
        """Days before ``before`` that a long-reaching stop time can put in the window.

        A row ``secs`` past its day lands in the window exactly when its
        day starts within [window_start - secs, window_end - secs]: a
        handful of days per distinct value (the window filter itself stays
        exact), never a scan of every day back.
        """
        first = self._first_service_date()
        days: set[date] = set()
        if first is None:
            return days
        for secs in self._reach_profile()[1]:
            try:
                span = timedelta(seconds=secs)
            except OverflowError:
                continue  # longer than datetime spans: lands in no window
            try:
                latest = (window_end - span).astimezone(self._tz).date()
            except OverflowError:
                continue  # its day would start before datetime: no such run
            try:
                earliest = (window_start - span).astimezone(self._tz).date()
            except OverflowError:
                earliest = first
            # A day can start the evening before its date (spring-forward:
            # 23:00 the previous local date), hours after its local midnight
            # (fall-back: 01:00, 02:00 in Troll), or at the NEXT date's
            # midnight (a skipped date, like Apia's 2011-12-30) -- hence a
            # day of margin on both sides.
            day = (
                max(first, earliest - timedelta(days=1)) if earliest > first else first
            )
            last = latest + timedelta(days=1)
            # ``day < before``, never ``before - 1``: before may be 0001-01-01.
            while day < before and day <= last:
                days.add(day)
                day += timedelta(days=1)
        return days

    def _scan_start(self, window_start: datetime, local_today: date) -> date:
        """Return the earliest service day whose runs can reach ``window_start``.

        A run on day D reaches D's start plus its latest stop time; with the
        normal reach (see :meth:`_reach_profile`) that is exact in any zone
        -- a two-hour DST jump, a skipped day -- by binary search, never
        before the feed's first service day. Long-reaching rows are placed
        separately (:meth:`_long_reach_days`). The search alone is exact --
        on a fall-back night too, where an instant's local date precedes the
        start of its own service day.
        """
        first = self._first_service_date()
        if first is None:
            return local_today
        normal = self._reach_profile()[0]
        reaching = self._first_reaching_day(first, local_today, normal, window_start)
        return reaching or local_today

    def _service_last_dates(self) -> dict[str, date]:
        """Map each service id to the last service date it can be active on.

        From ``calendar`` rows that can run at all (some weekday set, start
        not after end) and from ``calendar_dates`` ADDED dates, the later
        winning. Removals can only shorten service, so they are ignored:
        this is an upper bound. Computed once; the index never changes.
        """
        if self._last_dates is not None:
            return self._last_dates
        weekday_any = " OR ".join(f"{day} = 1" for day in _WEEKDAY_COLUMNS)
        last: dict[str, date] = {}
        rows = self._conn.execute(
            "SELECT service_id, end_date FROM calendar "
            f"WHERE ({weekday_any}) AND start_date <= end_date "
            "UNION ALL SELECT service_id, MAX(date) FROM calendar_dates "
            "WHERE exception_type = ? GROUP BY service_id",
            (_EXCEPTION_SERVICE_ADDED,),
        )
        for service_id, raw in rows:
            parsed = _lenient_date(raw)
            if parsed is not None and (
                service_id not in last or parsed > last[service_id]
            ):
                last[service_id] = parsed
        self._last_dates = last
        return last

    def _ceiling_from(self, rows: Iterable[tuple[str, int]]) -> datetime | None:
        """Latest instant across (service id, latest stop-time secs) rows."""
        last_dates = self._service_last_dates()
        best: datetime | None = None
        for service_id, max_secs in rows:
            last = last_dates.get(service_id)
            if last is None or max_secs is None:
                continue
            try:
                day_start = self._service_day_start(last)
            except OverflowError:
                if last.year == 1:
                    continue  # its day would start before datetime: no runs
                instant = _LATEST_INSTANT
            else:
                try:
                    instant = day_start + timedelta(seconds=max_secs)
                except OverflowError:
                    instant = _LATEST_INSTANT
            best = instant if best is None else max(best, instant)
        return None if best is None else min(best, _LATEST_INSTANT)

    def departures_ceiling(
        self,
        stop_ids: list[str],
        route_ids: list[str] | None = None,
        headsigns: list[str] | None = None,
    ) -> datetime | None:
        """Latest instant a departure matching these filters can fall at.

        Considers only the services that actually have a timed departure at
        ``stop_ids`` passing the route/headsign filters, each up to the last
        day it can run. None means no such departure can ever occur, so an
        open-ended query over these filters has nothing to scan for. Far
        tighter than the feed's own end: a filter matching nothing ends at
        once, and a seasonal route ends with its season.

        An upper bound, not an exact last departure: calendar removals and
        a date range that never falls on one of its weekdays are ignored.
        The one costly case is a filter matched only by a placeholder
        service running to a far year with almost no active days, which
        scans day by day to its end -- rare enough to accept.
        """
        if not stop_ids:
            return None
        sql = (
            "SELECT t.service_id, MAX(st.departure_secs) "
            "FROM stop_times st JOIN trips t ON t.id = st.trip_id "
            f"WHERE st.stop_id IN ({','.join('?' * len(stop_ids))}) "
            "AND st.departure_secs IS NOT NULL"
        )
        params: list[object] = list(stop_ids)
        if route_ids:
            sql += f" AND t.route_id IN ({','.join('?' * len(route_ids))})"
            params.extend(route_ids)
        if headsigns:
            sql += f" AND {_HEADSIGN_SQL} IN ({','.join('?' * len(headsigns))})"
            params.extend(headsigns)
        sql += " GROUP BY t.service_id"
        return self._ceiling_from(self._conn.execute(sql, params))

    def journeys_ceiling(
        self, origin_stop_id: str, destination_stop_id: str
    ) -> datetime | None:
        """Latest origin departure of any journey from origin to destination.

        The :meth:`departures_ceiling` of the stop pair, under the same
        candidate predicate as :meth:`upcoming_trips` (right direction, both
        ends timed). None means no journey between them can ever occur.
        """
        rows = self._conn.execute(
            "SELECT t.service_id, MAX(o.departure_secs) "
            "FROM stop_times o "
            "JOIN stop_times d ON d.trip_id = o.trip_id "
            "JOIN trips t ON t.id = o.trip_id "
            "WHERE o.stop_id = ? AND d.stop_id = ? "
            "AND o.stop_sequence < d.stop_sequence "
            f"{_USABLE_PAIR_SQL} "
            "GROUP BY t.service_id",
            (origin_stop_id, destination_stop_id),
        )
        return self._ceiling_from(rows)

    def rt_instances[K: tuple[str, date | None, int | None]](
        self, keys: Iterable[K]
    ) -> list[tuple[tuple[str, date], K]]:
        """Resolve matched TripUpdate keys to the concrete runs they address.

        Each key is ``(source trip id, start_date, start_secs or None)``.
        Its trip resolves the way the merge matches rows: a plain trip only
        by a key without start_secs, a frequency repetition only by its own
        start; the key addresses that day's run if the trip's service runs
        that day. A key without a date addresses nothing (matching gives
        every reference that names a run its date first). Returns
        ``((concrete trip id, service date), key)`` pairs.
        """
        keys = [key for key in keys if key[1] is not None]
        if not keys:
            return []
        sources = sorted({source for source, _day, _secs in keys})
        # Only trips with calls: a frequency template keeps its trips row
        # after its calls move to the materialized repetitions, and a run
        # with no calls is not a run any row could come from.
        trips = self._conn.execute(
            "SELECT id, source_trip_id, start_secs, service_id FROM trips t "
            f"WHERE source_trip_id IN ({','.join('?' * len(sources))}) "
            "AND EXISTS (SELECT 1 FROM stop_times st WHERE st.trip_id = t.id)",
            sources,
        ).fetchall()
        by_identity = {(source, secs): trip_id for trip_id, source, secs, _ in trips}
        service_of = {trip_id: service for trip_id, _, _, service in trips}
        active: dict[date, set[str]] = {}
        found: list[tuple[tuple[str, date], K]] = []
        for key in keys:
            source, day, secs = key
            trip_id = by_identity.get((source, secs))
            if trip_id is None or day is None:
                continue
            if day not in active:
                active[day] = self.active_service_ids(day)
            if service_of[trip_id] in active[day]:
                found.append(((trip_id, day), key))
        return found

    def alternative_trip_ids(
        self, refs: Iterable[tuple[str, int, date, int]]
    ) -> dict[tuple[str, int, date, int], str]:
        """Resolve trip_id-less TripDescriptors to the one trip each names.

        GTFS-RT's alternative trip matching: a trip that is not frequency
        based "may also be uniquely identified by" ``(route_id,
        direction_id, start_time, start_date)``, "as long as the
        combination of ids provided resolves to a unique trip".
        ``start_time`` is the trip's scheduled first departure (see
        :meth:`plain_trip_starts`) and its service must run on
        ``start_date``. A ref matching no trip, or several, names none; a
        trip with no direction_id matches no direction. Frequency
        repetitions are not candidates (the alternative applies to trips
        "which are not frequency based"), so one leaving at the same time
        does not make a plain trip ambiguous.
        """
        active: dict[date, set[str]] = {}
        found: dict[tuple[str, int, date, int], str] = {}
        for ref in sorted(set(refs)):
            route_id, direction_id, day, secs = ref
            candidates = self._conn.execute(
                "SELECT id, service_id FROM trips WHERE route_id = ? "
                "AND direction_id = ? AND first_secs = ? "
                "AND start_secs IS NULL AND source_trip_id = id",
                (route_id, direction_id, secs),
            ).fetchall()
            if day not in active:
                active[day] = self.active_service_ids(day)
            running = [
                trip_id
                for trip_id, service_id in candidates
                if service_id in active[day]
            ]
            if len(running) == 1:
                found[ref] = running[0]
        return found

    def current_service_dates(
        self, trip_ids: Sequence[str], now: datetime
    ) -> dict[str, date]:
        """Map each trip to the service day of the run a date-less update means.

        GTFS-RT's TripDescriptor: a ``trip_id`` alone identifies a trip's
        run, but ``start_date`` is required for a trip lasting more than 24
        hours, and ``start_date`` with ``start_time`` for a frequencies.txt
        trip. So a date-less key for a frequency repetition, or for a trip
        lasting more than a day from its first departure to its last
        arrival, names no run -- nor does one whose last arrival precedes
        its first departure (GTFS requires times not to decrease).

        For any other trip it means, among its runs on service days up to
        today -- the service day ``now`` falls in, which on a spring-forward
        eve is already the next date from 23:00 local (see
        :meth:`_service_day_of`) -- the run under way at ``now``, else the
        run nearest in time: to its first departure if still to come, from
        its last arrival if ended. A morning update reaches the afternoon's
        run, not yesterday's; a run running late keeps its updates past
        midnight.
        Two runs equally near -- two under way at once -- collide (the spec
        requires ``start_date`` there): no run. A later day's run is never
        meant. Nothing but the feed's own time decides it, so one snapshot
        binds each update to one run whatever a query asks. A run lasts
        from its first departure to its last arrival, over the whole trip,
        not just the stops a query asked about; a run whose day starts
        before datetime can hold is no run (as in the window scans).

        A ``now`` outside the range service days can be dated in (a
        garbage but representable feed timestamp) has no current run.
        """
        if not trip_ids or not _EARLIEST_INSTANT <= now < _LATEST_INSTANT:
            return {}
        trips = self._trip_spans(trip_ids)
        today = self._service_day_of(now)
        first_dates = self._service_first_dates()
        last_dates = self._service_last_dates()
        active_by_day: dict[date, set[str]] = {}
        current: dict[str, date] = {}
        for trip_id, service_id, repetition, start_secs, end_secs in trips:
            if start_secs is None or end_secs is None or repetition is not None:
                continue  # untimed, or a frequency repetition: start_time needed
            if end_secs - start_secs > _ONE_DAY:
                continue  # longer than a day: start_date required
            if end_secs < start_secs:
                continue  # arriving before it departs: times that name no run
            # Only the days its own service can run: a walk never crosses
            # years of a calendar the trip has no part in.
            lower, upper = first_dates.get(service_id), last_dates.get(service_id)
            if lower is None or upper is None:
                continue
            day = self._nearest_run(
                _RunQuery(service_id, start_secs, end_secs, now),
                lower,
                min(upper, today),
                active_by_day,
            )
            if day is not None:
                current[trip_id] = day
        return current

    def plain_trip_starts(self, trip_ids: Iterable[str]) -> dict[str, int]:
        """Map each plain (non-frequency) trip to its start time, in seconds.

        GTFS-RT: a non-frequency trip's ``start_time``, if given, "should
        either be omitted or be equal to the value in the GTFS feed" -- its
        first departure (see :meth:`_trip_spans`). A realtime key naming
        that value addresses the trip exactly as one naming none. Frequency
        templates (whose calls moved to their repetitions) and trips with
        no timed call are left out.
        """
        ids = sorted(set(trip_ids))
        starts: dict[str, int] = {}
        # In slices: realtime can name more trips than SQLite allows bound
        # variables in one statement.
        for offset in range(0, len(ids), _ID_SLICE):
            chunk = ids[offset : offset + _ID_SLICE]
            starts.update(
                self._conn.execute(
                    "SELECT id, first_secs FROM trips "
                    f"WHERE id IN ({','.join('?' * len(chunk))}) "
                    "AND start_secs IS NULL AND source_trip_id = id "
                    "AND first_secs IS NOT NULL",
                    chunk,
                ).fetchall()
            )
        return starts

    def _trip_spans(
        self, trip_ids: Sequence[str]
    ) -> list[tuple[str, str, int | None, int | None, int | None]]:
        """Return (trip, service, repetition start, first, last) per trip.

        A trip lasts from its first departure to its last arrival: the first
        and last TIMED calls by stop_sequence, a departure standing in for a
        missing arrival and vice versa (GTFS-RT reads a trip's start as its
        first departure). A trip of one call lasts that call's own dwell,
        arrival to departure. Repeated sequences take the earliest departure
        and the latest arrival. No timed call: first and last are None.
        """
        marks = ",".join("?" * len(trip_ids))
        trips = self._conn.execute(
            f"SELECT id, service_id, start_secs FROM trips WHERE id IN ({marks})",
            list(trip_ids),
        ).fetchall()
        calls: dict[str, list[tuple[int, int, int]]] = {}
        for trip_id, sequence, arrival, departure in self._conn.execute(
            "SELECT trip_id, stop_sequence, arrival_secs, departure_secs "
            f"FROM stop_times WHERE trip_id IN ({marks}) "
            "AND (arrival_secs IS NOT NULL OR departure_secs IS NOT NULL)",
            list(trip_ids),
        ):
            arrives = arrival if arrival is not None else departure
            departs = departure if departure is not None else arrival
            calls.setdefault(trip_id, []).append((sequence, arrives, departs))
        spans: list[tuple[str, str, int | None, int | None, int | None]] = []
        for trip_id, service_id, repetition in trips:
            rows = calls.get(trip_id)
            if not rows:
                spans.append((trip_id, service_id, repetition, None, None))
                continue
            first = min(sequence for sequence, _, _ in rows)
            last = max(sequence for sequence, _, _ in rows)
            if first == last:  # one call: its own dwell, arrival to departure
                start = min(arrives for seq, arrives, _ in rows if seq == first)
                end = max(departs for seq, _, departs in rows if seq == last)
            else:
                start = min(departs for seq, _, departs in rows if seq == first)
                end = max(arrives for seq, arrives, _ in rows if seq == last)
            spans.append((trip_id, service_id, repetition, start, end))
        return spans

    def _nearest_run(
        self,
        query: _RunQuery,
        lower: date,
        upper: date,
        active_by_day: dict[date, set[str]],
    ) -> date | None:
        """Return the day of the run nearest ``query.now`` in [lower, upper].

        Under way is distance zero; otherwise the gap to a run's first
        departure (to come) or from its last arrival (ended). Two equally
        near: None.
        """
        service_id, start_secs, end_secs, now = query

        def running(days: Iterable[date], most: int) -> list[date]:
            found: list[date] = []
            for day in days:
                active = active_by_day.get(day)
                if active is None:
                    active = active_by_day[day] = self.active_service_ids(day)
                if service_id in active:
                    found.append(day)
                    if len(found) == most:
                        break
            return found

        if lower > upper:
            return None
        # Day starts only increase, so the days whose run hasn't started by
        # ``now`` are a suffix (binary search); nothing is ever subtracted
        # from a date before it, so year 1 is safe.
        to_come = self._first_reaching_day(
            lower, upper, start_secs, now + timedelta(microseconds=1)
        )
        started: list[date] = []
        if to_come is None or to_come > lower:
            last_started = upper if to_come is None else to_come - timedelta(days=1)
            # The latest started run is the nearest of those (later runs end
            # later).
            started = running(_days(lower, last_started, backward=True), 1)
            latest = started[0] if started else None
            run = self._run_span(latest, start_secs, end_secs) if latest else None
            if latest is not None and run is not None and latest > lower:
                # An earlier run can only tie with it if it ends no earlier:
                # under way too, or ended at the same instant (two dates
                # sharing one start, a skipped date). Those days are a suffix
                # ending just before it -- never a walk across a long gap.
                before = latest - timedelta(days=1)
                tie_from = self._first_reaching_day(
                    lower, before, end_secs, min(now, run[1])
                )
                if tie_from is not None:
                    started += running(_days(tie_from, before, backward=True), 1)
        # Two runs to come can tie too: two dates sharing one start.
        coming = running(_days(to_come, upper), 2) if to_come is not None else []
        distances: list[tuple[timedelta, date]] = []
        for day in started:
            if (span := self._run_span(day, start_secs, end_secs)) is not None:
                distances.append((max(now - span[1], timedelta(0)), day))
        for day in coming:
            if (span := self._run_span(day, start_secs, end_secs)) is not None:
                distances.append((span[0] - now, day))
        distances.sort()
        if distances and (len(distances) == 1 or distances[0][0] < distances[1][0]):
            return distances[0][1]
        return None

    def _run_span(
        self, day: date, start_secs: int, end_secs: int
    ) -> tuple[datetime, datetime] | None:
        """Return a run's first and last stop instants; None if undatable."""
        try:
            day_start = self._service_day_start(day)
            return (
                day_start + timedelta(seconds=start_secs),
                day_start + timedelta(seconds=end_secs),
            )
        except OverflowError:
            return None

    def _service_first_dates(self) -> dict[str, date]:
        """Map each service id to the first service date it can be active on.

        The mirror of :meth:`_service_last_dates`: from ``calendar`` rows that
        can run at all and ``calendar_dates`` ADDED dates, the earlier
        winning -- a lower bound. Computed once; the index never changes.
        """
        if self._first_dates is not None:
            return self._first_dates
        weekday_any = " OR ".join(f"{day} = 1" for day in _WEEKDAY_COLUMNS)
        first: dict[str, date] = {}
        rows = self._conn.execute(
            "SELECT service_id, start_date FROM calendar "
            f"WHERE ({weekday_any}) AND start_date <= end_date "
            "UNION ALL SELECT service_id, MIN(date) FROM calendar_dates "
            "WHERE exception_type = ? GROUP BY service_id",
            (_EXCEPTION_SERVICE_ADDED,),
        )
        for service_id, raw in rows:
            parsed = _lenient_date(raw)
            if parsed is not None and (
                service_id not in first or parsed < first[service_id]
            ):
                first[service_id] = parsed
        self._first_dates = first
        return first

    def _service_day_of(self, at: datetime) -> date:
        """Return the service day ``at`` falls in: the latest day started by then.

        Not always ``at``'s local date: a day starts at noon minus 12
        ELAPSED hours, so a spring-forward day starts at 23:00 the evening
        before (22:00 in Troll), and a fall-back day only at 01:00 (02:00
        in Troll). Day starts never decrease, and every day starts within
        a day of its local midnight, so the answer is the local date or
        one of its neighbours. ``at`` must lie in [``_EARLIEST_INSTANT``,
        ``_LATEST_INSTANT``), where all three days' starts can be dated.
        """
        local = at.astimezone(self._tz).date()
        for day in (local + timedelta(days=1), local):
            if self._service_day_start(day) <= at:
                return day
        return local - timedelta(days=1)

    def _service_day_start(self, service_date: date) -> datetime:
        """Start instant of a GTFS service day: local noon minus 12 hours, UTC.

        GTFS measures times from "noon minus 12h" of the service day --
        "effectively midnight except for days on which daylight savings
        time changes occur". Twelve ELAPSED hours: subtracted in UTC, since
        Python's aware-datetime arithmetic in the local zone is wall-clock
        and would land on local midnight, an hour off on DST days. Raises
        OverflowError for a day whose start can't be represented.
        """
        noon = datetime.combine(service_date, time(12, 0), tzinfo=self._tz)
        return noon.astimezone(UTC) - timedelta(hours=12)

    def _service_day_windows(
        self, now_utc: datetime, lookahead: timedelta, grace: timedelta
    ) -> Iterator[tuple[date, datetime, set[str], float, float]]:
        """Yield ``(service_date, day_start_utc, active_ids, window_lo, window_hi)``.

        The window runs from ``now_utc - grace`` to ``now_utc + lookahead``.
        Shared by ``upcoming_departures`` and ``upcoming_trips`` so the
        DST-safe day-window arithmetic exists exactly once. Scans every
        service day that could contribute: from the first whose runs reach
        the window (see :meth:`_scan_start`; times past 24:00:00 are still
        upcoming), plus the few earlier days a long-reaching stop time lands
        from (:meth:`_long_reach_days`), through one day AFTER the window's
        local end date (enclosure margin for DST edge instants). Empty days
        cost one cheap indexed query; silently narrower bounds cost dropped
        departures (hypothesis-found bug, 2026-07-31: the old hardcoded
        local_today +/- 1 day tuple ignored `lookahead` entirely).
        """
        # In UTC: two aware datetimes sharing a tzinfo (a caller's instant
        # in the feed's own zone, and the local day starts) would otherwise
        # subtract and compare by wall clock, an hour off across DST.
        now = now_utc.astimezone(UTC)
        window_start = now - grace
        # Clamped so a window reaching the end of datetime (an explicit far
        # end, or a calendar running "forever") never overflows date math.
        window_end = min(now + lookahead, _LATEST_INSTANT)
        local_today = window_start.astimezone(self._tz).date()
        end_local = window_end.astimezone(self._tz).date()
        scan_start = self._scan_start(window_start, local_today)
        scan_end = end_local + timedelta(days=1)
        days = [
            scan_start + timedelta(days=offset)
            for offset in range((scan_end - scan_start).days + 1)
        ]
        # Plus the few earlier days a long-reaching stop time can land from.
        days[:0] = sorted(self._long_reach_days(window_start, window_end, scan_start))
        for service_date in days:
            active = self.active_service_ids(service_date)
            if not active:
                continue
            try:
                # Already UTC: GTFS times are elapsed seconds from it.
                day_start_utc = self._service_day_start(service_date)
            except OverflowError:
                continue  # a day whose start can't be dated has no runs
            window_lo = (window_start - day_start_utc).total_seconds()
            window_hi = (window_end - day_start_utc).total_seconds()
            if window_hi < 0:
                continue
            yield service_date, day_start_utc, active, window_lo, window_hi

    def upcoming_departures(
        self,
        stop_ids: list[str],
        route_ids: list[str] | None,
        now_utc: datetime,
        lookahead: timedelta,
        per_stop_limit: int | None = None,
        *,
        grace: timedelta = timedelta(0),
        headsigns: list[str] | None = None,
    ) -> list[ScheduledDeparture]:
        """Scheduled departures at the given stops within the query window.

        The window runs from ``now_utc - grace`` through ``now_utc +
        lookahead`` (the handle layer defaults ``grace`` to one hour);
        ``grace`` lets callers that overlay realtime
        predictions keep rows whose scheduled time has passed but whose
        vehicle may still be coming. Considers every service day whose
        runs can reach the window (see :meth:`_service_day_windows`), so
        past-midnight trips (>24:00:00 times) surface on the correct clock
        day and long lookaheads are never truncated.
        Results are sorted by departure and, when ``per_stop_limit`` is
        given, truncated to that many rows per stop.

        Frequency-based trips (frequencies.txt) surface as one row per
        materialized repetition, under synthetic ``{trip_id}#{start_secs}``
        trip ids (see ``_load_frequencies``); the bare template trip id
        never appears. ``route_ids``/``headsigns`` (the headsign at the stop)
        narrow the rows in SQL; None or empty means no narrowing.
        """
        results: list[ScheduledDeparture] = []
        for (
            service_date,
            day_start_utc,
            active,
            window_lo,
            window_hi,
        ) in self._service_day_windows(now_utc, lookahead, grace):
            results.extend(
                self._departures_on(
                    service_date,
                    day_start_utc,
                    active,
                    stop_ids=stop_ids,
                    route_ids=route_ids,
                    headsigns=headsigns,
                    window_sql="AND st.departure_secs >= ? AND st.departure_secs <= ?",
                    window_params=[window_lo, window_hi],
                )
            )
        # Total sort key: departure alone ties frequently (same-minute
        # departures across trips/stops); trip_id/stop_id break ties so
        # repeated identical queries never flap between orderings, even if
        # a future SQLite version/query-plan change stops preserving the
        # scan order this happened to be stable under (verified empirically
        # stable pre-fix across 500+ generated feeds, 2026-07-31 — but
        # stability was never guaranteed by the SQL, so fix it anyway).
        results.sort(key=lambda dep: (dep.departure, dep.trip_id, dep.stop_id))
        if per_stop_limit is None:
            return results
        limited: list[ScheduledDeparture] = []
        per_stop_counts: dict[str, int] = {}
        for dep in results:
            count = per_stop_counts.get(dep.stop_id, 0)
            if count < per_stop_limit:
                limited.append(dep)
                per_stop_counts[dep.stop_id] = count + 1
        return limited

    def _departures_on(
        self,
        service_date: date,
        day_start_utc: datetime,
        active: set[str],
        *,
        stop_ids: Sequence[str],
        route_ids: Sequence[str] | None,
        headsigns: Sequence[str] | None,
        window_sql: str,
        window_params: Sequence[object],
    ) -> list[ScheduledDeparture]:
        """One service day's timed departures at ``stop_ids`` (see callers)."""
        rows: list[ScheduledDeparture] = []
        stop_marks = ",".join("?" * len(stop_ids))
        service_marks = ",".join("?" * len(active))
        sql = (
            "SELECT st.trip_id, t.route_id, t.service_id, t.headsign, st.stop_id, "
            "st.stop_sequence, "
            "st.arrival_secs, st.departure_secs, t.source_trip_id, t.start_secs, "
            "t.wheelchair_accessible, t.bikes_allowed, t.direction_id, "
            "st.pickup_type, st.drop_off_type, st.timepoint, st.stop_headsign, "
            "t.short_name, t.block_id "
            "FROM stop_times st JOIN trips t ON t.id = st.trip_id "
            f"WHERE st.stop_id IN ({stop_marks}) "
            f"AND t.service_id IN ({service_marks}) "
            "AND st.departure_secs IS NOT NULL "
            f"{window_sql}"
        )
        params: list[object] = [*stop_ids, *sorted(active), *window_params]
        if route_ids:
            route_marks = ",".join("?" * len(route_ids))
            sql += f" AND t.route_id IN ({route_marks})"
            params.extend(route_ids)
        if headsigns:
            headsign_marks = ",".join("?" * len(headsigns))
            sql += f" AND {_HEADSIGN_SQL} IN ({headsign_marks})"
            params.extend(headsigns)
        for (
            trip_id,
            route_id,
            service_id,
            headsign,
            stop_id,
            stop_sequence,
            arr_secs,
            dep_secs,
            source_trip_id,
            start_secs,
            wheelchair,
            bikes,
            direction_id,
            pickup_type,
            drop_off_type,
            timepoint,
            stop_headsign,
            trip_short_name,
            block_id,
        ) in self._conn.execute(sql, params):
            try:
                departure = day_start_utc + timedelta(seconds=dep_secs)
                arrival = (
                    day_start_utc + timedelta(seconds=arr_secs)
                    if arr_secs is not None
                    else None
                )
            except OverflowError:
                continue  # past the end of datetime: no instant to show
            rows.append(
                ScheduledDeparture(
                    trip_id=trip_id,
                    route_id=route_id,
                    service_id=service_id,
                    headsign=headsign,
                    stop_id=stop_id,
                    stop_sequence=stop_sequence,
                    arrival=arrival,
                    departure=departure,
                    source_trip_id=source_trip_id,
                    start_secs=start_secs,
                    service_date=service_date,
                    wheelchair_accessible=_enum_or_none(WheelchairAccess, wheelchair),
                    bikes_allowed=_enum_or_none(BikesAllowed, bikes),
                    direction_id=direction_id,
                    pickup_type=_enum_or_none(PickupDropOffType, pickup_type),
                    drop_off_type=_enum_or_none(PickupDropOffType, drop_off_type),
                    timepoint_exact=_timepoint_exact(timepoint),
                    stop_headsign=stop_headsign,
                    trip_short_name=trip_short_name,
                    block_id=block_id,
                )
            )
        return rows

    def instance_departures(
        self,
        instances: Iterable[tuple[str, date]],
        stop_ids: Sequence[str],
        route_ids: Sequence[str] | None = None,
        headsigns: Sequence[str] | None = None,
    ) -> list[ScheduledDeparture]:
        """Every timed departure of specific runs at ``stop_ids``, any time.

        The runs are (concrete trip, service day) pairs, as
        :meth:`rt_instances` resolves them; a run whose service doesn't
        operate that day yields nothing. Rows are exactly the ones
        :meth:`upcoming_departures` builds for these runs, with no window.
        """
        by_day: dict[date, set[str]] = {}
        for trip_id, service_date in instances:
            by_day.setdefault(service_date, set()).add(trip_id)
        if not stop_ids:
            return []
        rows: list[ScheduledDeparture] = []
        for service_date in sorted(by_day):
            active = self.active_service_ids(service_date)
            if not active:
                continue
            try:
                day_start = self._service_day_start(service_date)
            except OverflowError:
                continue  # a day whose start can't be dated has no runs
            trip_ids = sorted(by_day[service_date])
            rows.extend(
                self._departures_on(
                    service_date,
                    day_start,
                    active,
                    stop_ids=stop_ids,
                    route_ids=route_ids,
                    headsigns=headsigns,
                    window_sql=f"AND st.trip_id IN ({','.join('?' * len(trip_ids))})",
                    window_params=trip_ids,
                )
            )
        rows.sort(key=lambda dep: (dep.departure, dep.trip_id, dep.stop_id))
        return rows

    # Shared origin->destination candidate predicate: right direction, both
    # ends timed and usable (see _USABLE_PAIR_SQL), service active. Kept as
    # one fragment so the row query and the first/last-of-day extremes
    # query can NEVER drift apart (the flags are only correct if both
    # queries agree on what a candidate is).
    _PAIR_CANDIDATES_SQL = (
        "FROM stop_times o "
        "JOIN stop_times d ON d.trip_id = o.trip_id "
        "JOIN trips t ON t.id = o.trip_id "
        "WHERE o.stop_id = ? AND d.stop_id = ? "
        "AND o.stop_sequence < d.stop_sequence "
        f"{_USABLE_PAIR_SQL} "
        "AND t.service_id IN ({service_marks})"
    )

    def _pair_day_extremes(
        self,
        origin_stop_id: str,
        destination_stop_id: str,
        active: set[str],
    ) -> tuple[tuple[str, int], tuple[str, int]]:
        """Return one service day's first/last departure identities for the pair.

        The identities are (trip_id, origin stop_sequence) pairs computed
        over the WHOLE service day, not the query window.

        Ordered by (departure_secs, trip_id, stop_sequence) — the same
        total order the row sort uses — so exactly one candidate is the
        first and exactly one is the last even when departures tie. Callers
        only invoke this for days that produced candidate rows, so both
        LIMIT-1 queries always find a row.
        """
        service_marks = ",".join("?" * len(active))
        candidates = self._PAIR_CANDIDATES_SQL.format(service_marks=service_marks)
        params = [origin_stop_id, destination_stop_id, *sorted(active)]
        extremes: list[tuple[str, int]] = []
        for direction in ("ASC", "DESC"):
            sql = (
                "SELECT o.trip_id, o.stop_sequence "
                f"{candidates} "
                f"ORDER BY o.departure_secs {direction}, o.trip_id {direction}, "
                f"o.stop_sequence {direction} LIMIT 1"
            )
            row = self._conn.execute(sql, params).fetchone()
            extremes.append((row[0], row[1]))
        return extremes[0], extremes[1]

    def _trips_on(
        self,
        service_date: date,
        day_start_utc: datetime,
        active: set[str],
        *,
        origin_stop_id: str,
        destination_stop_id: str,
        window_sql: str,
        window_params: Sequence[object],
    ) -> list[ScheduledTrip]:
        """One service day's origin->destination journeys (see callers)."""
        found: list[ScheduledTrip] = []
        service_marks = ",".join("?" * len(active))
        candidates = self._PAIR_CANDIDATES_SQL.format(service_marks=service_marks)
        # The IS NOT NULL clauses (in the shared candidate predicate)
        # guarantee ScheduledTrip's non-optional datetimes: a stop_time
        # without a departure at the origin (or an arrival at the
        # destination) can never produce a row. The bare d.* columns are
        # well-defined under GROUP BY because the query has exactly one
        # min/max aggregate: SQLite documents that they then come from
        # the row MIN(d.arrival_secs) selected, i.e. the destination
        # call actually ridden to.
        sql = (
            "SELECT o.trip_id, t.route_id, t.service_id, t.headsign, "
            "o.departure_secs, MIN(d.arrival_secs), "
            "t.source_trip_id, t.start_secs, "
            "t.wheelchair_accessible, t.bikes_allowed, t.direction_id, "
            "o.pickup_type, o.drop_off_type, o.timepoint, o.stop_headsign, "
            "d.pickup_type, d.drop_off_type, d.timepoint, d.stop_headsign, "
            "o.stop_sequence, d.stop_sequence, t.short_name, t.block_id "
            f"{candidates} "
            f"{window_sql} "
            # A feed repeating an origin stop_sequence with another time has
            # two origin calls: each is its own journey (as each is its own
            # departure row), so no read -- windowed or not -- picks between
            # them.
            "GROUP BY o.trip_id, o.stop_sequence, o.departure_secs"
        )
        params: list[object] = [
            origin_stop_id,
            destination_stop_id,
            *sorted(active),
            *window_params,
        ]
        rows = self._conn.execute(sql, params).fetchall()
        if not rows:
            return []
        first_key, last_key = self._pair_day_extremes(
            origin_stop_id, destination_stop_id, active
        )
        for (
            trip_id,
            route_id,
            service_id,
            headsign,
            dep_secs,
            arr_secs,
            source_trip_id,
            start_secs,
            wheelchair,
            bikes,
            direction_id,
            o_pickup,
            o_drop_off,
            o_timepoint,
            o_stop_headsign,
            d_pickup,
            d_drop_off,
            d_timepoint,
            d_stop_headsign,
            o_sequence,
            d_sequence,
            trip_short_name,
            block_id,
        ) in rows:
            try:
                departure = day_start_utc + timedelta(seconds=dep_secs)
                arrival = day_start_utc + timedelta(seconds=arr_secs)
            except OverflowError:
                continue  # past the end of datetime: no instant to show
            found.append(
                ScheduledTrip(
                    trip_id=trip_id,
                    route_id=route_id,
                    service_id=service_id,
                    headsign=headsign,
                    origin_stop_id=origin_stop_id,
                    destination_stop_id=destination_stop_id,
                    departure=departure,
                    arrival=arrival,
                    source_trip_id=source_trip_id,
                    start_secs=start_secs,
                    service_date=service_date,
                    origin_stop_sequence=o_sequence,
                    destination_stop_sequence=d_sequence,
                    wheelchair_accessible=_enum_or_none(WheelchairAccess, wheelchair),
                    bikes_allowed=_enum_or_none(BikesAllowed, bikes),
                    direction_id=direction_id,
                    origin_pickup_type=_enum_or_none(PickupDropOffType, o_pickup),
                    origin_drop_off_type=_enum_or_none(PickupDropOffType, o_drop_off),
                    origin_timepoint_exact=_timepoint_exact(o_timepoint),
                    origin_stop_headsign=o_stop_headsign,
                    destination_pickup_type=_enum_or_none(PickupDropOffType, d_pickup),
                    destination_drop_off_type=_enum_or_none(
                        PickupDropOffType, d_drop_off
                    ),
                    destination_timepoint_exact=_timepoint_exact(d_timepoint),
                    destination_stop_headsign=d_stop_headsign,
                    is_first=(trip_id, o_sequence) == first_key,
                    is_last=(trip_id, o_sequence) == last_key,
                    trip_short_name=trip_short_name,
                    block_id=block_id,
                )
            )
        return found

    def instance_trips(
        self,
        instances: Iterable[tuple[str, date]],
        origin_stop_id: str,
        destination_stop_id: str,
    ) -> list[ScheduledTrip]:
        """Every origin->destination journey of specific runs, any time.

        The runs are (concrete trip, service day) pairs; rows are exactly
        the ones :meth:`upcoming_trips` builds for them (first/last of day
        included), with no window.
        """
        by_day: dict[date, set[str]] = {}
        for trip_id, service_date in instances:
            by_day.setdefault(service_date, set()).add(trip_id)
        rows: list[ScheduledTrip] = []
        for service_date in sorted(by_day):
            active = self.active_service_ids(service_date)
            if not active:
                continue
            try:
                day_start = self._service_day_start(service_date)
            except OverflowError:
                continue  # a day whose start can't be dated has no runs
            trip_ids = sorted(by_day[service_date])
            rows.extend(
                self._trips_on(
                    service_date,
                    day_start,
                    active,
                    origin_stop_id=origin_stop_id,
                    destination_stop_id=destination_stop_id,
                    window_sql=f"AND o.trip_id IN ({','.join('?' * len(trip_ids))})",
                    window_params=trip_ids,
                )
            )
        rows.sort(key=lambda trip: (trip.departure, trip.trip_id, trip.arrival))
        return rows

    def upcoming_trips(
        self,
        origin_stop_id: str,
        destination_stop_id: str,
        now_utc: datetime,
        lookahead: timedelta,
        limit: int | None = None,
        *,
        grace: timedelta = timedelta(0),
    ) -> list[ScheduledTrip]:
        """Scheduled trips departing the origin that later serve the destination.

        Uses the same service-day scanning as ``upcoming_departures``; the
        window applies to the ORIGIN departure. The ``o.stop_sequence <
        d.stop_sequence`` self-join predicate is what excludes
        wrong-direction trips: a return trip serves both stops too, but its
        destination call precedes its origin call. A loop trip serving the
        destination more than once after the origin collapses to its
        earliest destination arrival (MIN) — ride until the vehicle first
        reaches the destination. The window runs from ``now_utc - grace``
        to ``now_utc + lookahead`` (the handle layer defaults ``grace`` to
        one hour). Results are sorted by origin departure
        and, when ``limit`` is given, truncated to it.

        ``is_first``/``is_last`` are computed per SERVICE DAY, over the
        whole day rather than the query window (legacy ``gtfs`` sensor
        parity): a row is first/last iff it is that day's first/last
        candidate departure for this pair, under the same candidate
        predicate as the rows themselves. Each scanned day costs at most
        two extra LIMIT-1 index lookups, and only when it produced rows.
        A >24:00:00 departure carries its own service day's flags, and
        frequency-materialized repetitions count as ordinary trips.
        """
        results: list[ScheduledTrip] = []
        for (
            service_date,
            day_start_utc,
            active,
            window_lo,
            window_hi,
        ) in self._service_day_windows(now_utc, lookahead, grace):
            results.extend(
                self._trips_on(
                    service_date,
                    day_start_utc,
                    active,
                    origin_stop_id=origin_stop_id,
                    destination_stop_id=destination_stop_id,
                    window_sql="AND o.departure_secs >= ? AND o.departure_secs <= ?",
                    window_params=[window_lo, window_hi],
                )
            )
        # Total sort key, matching upcoming_departures's rationale: arrival
        # is the third component because a degenerate loop trip can depart
        # the origin twice at the identical instant.
        results.sort(key=lambda trip: (trip.departure, trip.trip_id, trip.arrival))
        return results[:limit]

    def close(self) -> None:
        """Close the underlying connection."""
        self._conn.close()
