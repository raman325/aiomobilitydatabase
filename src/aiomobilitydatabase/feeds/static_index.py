"""Synchronous SQLite index over a static GTFS dataset.

Everything in this module is synchronous by design: callers in async code
MUST invoke build/open/query methods through ``asyncio.to_thread`` so the
event loop is never blocked (a hard Home Assistant requirement).
"""

from __future__ import annotations

import csv
import io
import sqlite3
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import IntEnum
from pathlib import Path
from typing import IO, Any
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
SCHEMA_VERSION = 3
_BATCH_SIZE = 5000
_SECONDS_OR_MINUTES_PER_UNIT = 60

# Descriptive vocabulary columns are stored as the raw parsed ints (or NULL
# for blank/unparseable values); the closed-vocabulary enums live at the
# Python model boundary only, where out-of-vocabulary ints turn into None.
_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE agencies (
    id TEXT, name TEXT, url TEXT, timezone TEXT,
    lang TEXT, phone TEXT, fare_url TEXT
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
    short_name TEXT, block_id TEXT
);
-- Single-record feed metadata (feed_info.txt); dates stay raw YYYYMMDD
-- text cells, parsed leniently at the model boundary like every other
-- descriptive value.
CREATE TABLE feed_info (
    publisher_name TEXT, publisher_url TEXT, lang TEXT, version TEXT,
    start_date TEXT, end_date TEXT
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
_EXCEPTION_SERVICE_REMOVED = 2

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
        write to ``<path>.building`` then atomically replace, so a reader
        of the old file is never left with a half-built database. ``progress``,
        when given, is called with ``(done_bytes, total_bytes)`` as each
        source file is parsed (``total_bytes`` may be ``None``).
        """
        try:
            archive = zipfile.ZipFile(zip_path)
        except (zipfile.BadZipFile, OSError) as err:
            raise FeedParseError(f"Unreadable GTFS zip: {err}") from err
        with archive:
            names = set(archive.namelist())
            if timezone_name is None:
                timezone_name = cls._timezone_from_agency(archive, names)
            if timezone_name is None:
                raise FeedParseError(
                    "No agency timezone available (catalog or agency.txt)"
                )
            in_memory = db_target == ":memory:"
            build_path = db_target if in_memory else f"{db_target}.building"
            conn = sqlite3.connect(build_path, check_same_thread=False)
            try:
                conn.executescript(_SCHEMA)
                cls._load_all(conn, archive, names, progress)
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
        return cls(
            sqlite3.connect(db_target, check_same_thread=False),
            dataset_id,
            timezone_name,
        )

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
        except (ZoneInfoNotFoundError, TypeError, ValueError):
            # Unusable cached timezone metadata (unknown key, empty, or a
            # non-string the column's TEXT affinity left as a blob) is a
            # cache miss like any other corruption: ZoneInfo raises in
            # __init__, one layer below the guards above.
            conn.close()
            return None

    @staticmethod
    def _timezone_from_agency(archive: zipfile.ZipFile, names: set[str]) -> str | None:
        if "agency.txt" not in names:
            return None
        with archive.open("agency.txt") as fp:
            reader = csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
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
        required = {"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}
        parsed = [filename for filename, _ in files if filename in names]
        total = sum(archive.getinfo(filename).file_size for filename in parsed) or None
        completed = 0
        for filename, loader in files:
            if filename not in names:
                if filename in required:
                    raise FeedParseError(f"GTFS zip missing required file: {filename}")
                continue
            with archive.open(filename) as fp:
                report = _reporter(progress, fp, completed, total)
                reader = csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
                try:
                    loader(conn, reader, report)
                except (ValueError, KeyError, AttributeError, TypeError) as err:
                    raise FeedParseError(f"Malformed {filename}: {err}") from err
            completed += archive.getinfo(filename).file_size
            if progress is not None:
                progress(completed, total)

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
        sql = "INSERT INTO agencies VALUES (?,?,?,?,?,?,?)"
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
                    _lenient_int(row.get("location_type")),
                    row.get("stop_code") or None,
                    row.get("platform_code") or None,
                    _lenient_int(row.get("wheelchair_boarding")),
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
                    row.get("route_color") or None,
                    row.get("route_text_color") or None,
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
        sql = "INSERT OR REPLACE INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        for row in reader:
            rows.append(
                (
                    row["trip_id"],
                    row["route_id"],
                    row["service_id"],
                    row.get("trip_headsign") or None,
                    row["trip_id"],  # source_trip_id: a plain trip is its own source
                    None,  # start_secs: only frequency repetitions carry one
                    _lenient_int(row.get("wheelchair_accessible")),
                    _lenient_int(row.get("bikes_allowed")),
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
            rows.append(
                (
                    row["trip_id"],
                    row["stop_id"],
                    parse_gtfs_time(row.get("arrival_time", "")),
                    parse_gtfs_time(row.get("departure_time", "")),
                    _strict_int(row["stop_sequence"], "stop_sequence"),
                    _lenient_int(row.get("pickup_type")),
                    _lenient_int(row.get("drop_off_type")),
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
        so the first stop's anchor time (its arrival, falling back to its
        departure) lands on the repetition start while every later stop
        keeps its elapsed offset from that anchor; the original template
        rows are then deleted. Repetition starts are ``start_time + n *
        headway_secs`` for n = 0.. while STRICTLY less than ``end_time``
        (a repetition landing exactly on end_time does not run), and
        identical start seconds produced by overlapping rows for the same
        trip are deduplicated so synthetic ids stay unique. This applies
        to BOTH exact_times values: exact_times=0 describes idealized
        headway service without a fixed timetable, and materializing it at
        the stated headway is the standard journey-planner interpretation,
        so the column is read for neither value and the two kinds are
        indistinguishable downstream.

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
            "short_name, block_id) "
            "SELECT ?, route_id, service_id, headsign, ?, ?, "
            "wheelchair_accessible, bikes_allowed, direction_id, "
            "short_name, block_id "
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
            first_arrival, first_departure = template[0][1], template[0][2]
            anchor = first_arrival if first_arrival is not None else first_departure
            if anchor is None:
                raise ValueError(
                    f"frequency trip {trip_id!r} has no first-stop time "
                    "to anchor repetition offsets"
                )
            conn.execute("DELETE FROM stop_times WHERE trip_id = ?", (trip_id,))
            starts: set[int] = set()
            for start_secs, end_secs, headway_secs in trip_spans:
                rep_start = start_secs
                while rep_start < end_secs:
                    starts.add(rep_start)
                    rep_start += headway_secs
            for rep_start in sorted(starts):
                synthetic_id = f"{trip_id}#{rep_start}"
                shift = rep_start - anchor
                trip_rows.append((synthetic_id, trip_id, rep_start, trip_id))
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
                    stop_time_rows.append(
                        (
                            synthetic_id,
                            stop_id,
                            arrival_secs + shift if arrival_secs is not None else None,
                            (
                                departure_secs + shift
                                if departure_secs is not None
                                else None
                            ),
                            stop_sequence,
                            pickup_type,
                            drop_off_type,
                            timepoint,
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
    ) -> dict[str, list[tuple[int, int, int]]]:
        """Parse frequencies rows into per-trip (start, end, headway) spans.

        Raises (via the ``_load_all`` wrapping into FeedParseError) on rows
        missing a start/end time or carrying a non-positive headway.
        """
        spans: dict[str, list[tuple[int, int, int]]] = {}
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
            spans.setdefault(trip_id, []).append((start_secs, end_secs, headway_secs))
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
                    row.get("start_date") or "",
                    row.get("end_date") or "",
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
            rows.append(
                (
                    row["service_id"],
                    row["date"],
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
                "INSERT INTO feed_info VALUES (?,?,?,?,?,?)",
                (
                    row.get("feed_publisher_name") or None,
                    row.get("feed_publisher_url") or None,
                    row.get("feed_lang") or None,
                    row.get("feed_version") or None,
                    row.get("feed_start_date") or None,
                    row.get("feed_end_date") or None,
                ),
            )
            break
        if report is not None:
            report()

    # -- queries -----------------------------------------------------------

    def stops(self) -> list[Stop]:
        """All stops (for pickers)."""
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
                wheelchair_boarding=_enum_or_none(WheelchairAccess, row[8]),
                description=row[9],
                url=row[10],
                zone_id=row[11],
                timezone=row[12],
            )
            for row in self._conn.execute(
                "SELECT id, name, lat, lon, parent_station, location_type, "
                "stop_code, platform_code, wheelchair_boarding, "
                "description, url, zone_id, timezone "
                "FROM stops ORDER BY name"
            )
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
            "start_date, end_date FROM feed_info"
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
            )
            for row in self._conn.execute(
                "SELECT id, name, url, timezone, lang, phone, fare_url FROM agencies"
            )
        ]

    def route_display_names(self) -> dict[str, str]:
        """Map of route_id to display name (for RT joins)."""
        return {route.id: route.display_name for route in self.routes()}

    def route_types(self) -> dict[str, int | None]:
        """Map of route_id to GTFS route_type (raw int, open vocabulary)."""
        return {route.id: route.type for route in self.routes()}

    def stop_names(self) -> dict[str, str | None]:
        """Map of stop_id to name."""
        return {
            row[0]: row[1] for row in self._conn.execute("SELECT id, name FROM stops")
        }

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

    def trip_stop_calls(self, trip_ids: list[str]) -> dict[str, list[tuple[int, str]]]:
        """Ordered ``(stop_sequence, stop_id)`` calls per trip.

        The RT delay-propagation seam: :mod:`.rt` resolves a trip's
        StopTimeUpdates against this static stop order. Keys are CONCRETE
        trip ids (synthetic repetition ids for frequency trips), so
        propagation stays within one materialized repetition. Trips with
        no stop_times rows are simply absent from the result.
        """
        if not trip_ids:
            return {}
        marks = ",".join("?" * len(trip_ids))
        calls: dict[str, list[tuple[int, str]]] = {}
        for trip_id, stop_sequence, stop_id in self._conn.execute(
            "SELECT trip_id, stop_sequence, stop_id FROM stop_times "
            f"WHERE trip_id IN ({marks}) ORDER BY trip_id, stop_sequence",
            trip_ids,
        ):
            calls.setdefault(trip_id, []).append((stop_sequence, stop_id))
        return calls

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
        """Distinct trip headsigns at a stop, optionally narrowed to one route.

        Sourced from trips.trip_headsign — the same field StopArrival.headsign
        exposes, so picker options and filterable values always agree. (GTFS's
        per-stop stop_times.stop_headsign override IS loaded and exposed as
        StopArrival.stop_headsign, but deliberately not offered as picker
        options: filters match on the trip-level headsign.)
        """
        sql = (
            "SELECT DISTINCT t.headsign FROM stop_times st "
            "JOIN trips t ON t.id = st.trip_id "
            "WHERE st.stop_id = ? AND t.headsign IS NOT NULL"
        )
        params: list[str] = [stop_id]
        if route_id is not None:
            sql += " AND t.route_id = ?"
            params.append(route_id)
        sql += " ORDER BY t.headsign"
        return [row[0] for row in self._conn.execute(sql, params)]

    def active_service_ids(self, service_date: date) -> set[str]:
        """Service IDs active on the given local service date."""
        datestr = service_date.strftime("%Y%m%d")
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

    def _service_day_start(self, service_date: date) -> datetime:
        """Start instant of a GTFS service day: local noon minus 12 hours.

        This resolves to the same instant as local midnight even on
        DST-transition days (Python's aware-datetime arithmetic is
        wall-clock, and noon is never itself ambiguous or skipped). The
        actual DST safety comes later: callers must add GTFS elapsed
        seconds to this anchor's UTC form, not to the aware local value.
        """
        noon = datetime.combine(service_date, time(12, 0), tzinfo=self._tz)
        return noon - timedelta(hours=12)

    def _service_day_windows(
        self, now_utc: datetime, lookahead: timedelta, grace: timedelta
    ) -> Iterator[tuple[date, datetime, set[str], float, float]]:
        """Yield ``(service_date, day_start_utc, active_ids, window_lo, window_hi)``.

        The window runs from ``now_utc - grace`` to ``now_utc + lookahead``.
        Shared by ``upcoming_departures`` and ``upcoming_trips`` so the
        DST-safe day-window arithmetic exists exactly once. Scans every
        service day that could contribute: one day BEFORE the window
        (>24:00:00 times still upcoming) through one day AFTER the window's
        local end date (enclosure margin for DST edge instants). Empty days
        cost one cheap indexed query; silently narrower bounds cost dropped
        departures (hypothesis-found bug, 2026-07-31: the old hardcoded
        local_today +/- 1 day tuple ignored `lookahead` entirely).
        """
        window_start = now_utc - grace
        window_end = now_utc + lookahead
        local_today = window_start.astimezone(self._tz).date()
        end_local = window_end.astimezone(self._tz).date()
        scan_start = local_today - timedelta(days=1)
        scan_end = end_local + timedelta(days=1)
        num_scan_days = (scan_end - scan_start).days + 1
        for offset in range(num_scan_days):
            service_date = scan_start + timedelta(days=offset)
            active = self.active_service_ids(service_date)
            if not active:
                continue
            day_start = self._service_day_start(service_date)
            # GTFS times are elapsed seconds from the service-day anchor, not
            # wall-clock times: add them in UTC (an absolute, offset-stable
            # timeline) rather than to the aware local `day_start`, whose
            # addition Python resolves via wall-clock semantics and would
            # drift by an hour across a DST transition.
            day_start_utc = day_start.astimezone(UTC)
            window_lo = (window_start - day_start).total_seconds()
            window_hi = (window_end - day_start).total_seconds()
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
    ) -> list[ScheduledDeparture]:
        """Scheduled departures at the given stops within the query window.

        The window runs from ``now_utc - grace`` through ``now_utc +
        lookahead`` (the handle layer defaults ``grace`` to one hour);
        ``grace`` lets callers that overlay realtime
        predictions keep rows whose scheduled time has passed but whose
        vehicle may still be coming. Considers every local service day
        from one day before the window start through one day after its
        local end date, so past-midnight trips (>24:00:00 times) surface
        on the correct clock day and long lookaheads are never truncated.
        Results are sorted by departure and, when ``per_stop_limit`` is
        given, truncated to that many rows per stop.

        Frequency-based trips (frequencies.txt) surface as one row per
        materialized repetition, under synthetic ``{trip_id}#{start_secs}``
        trip ids (see ``_load_frequencies``); the bare template trip id
        never appears.
        """
        results: list[ScheduledDeparture] = []
        for (
            service_date,
            day_start_utc,
            active,
            window_lo,
            window_hi,
        ) in self._service_day_windows(now_utc, lookahead, grace):
            stop_marks = ",".join("?" * len(stop_ids))
            service_marks = ",".join("?" * len(active))
            sql = (
                "SELECT st.trip_id, t.route_id, t.headsign, st.stop_id, "
                "st.stop_sequence, "
                "st.arrival_secs, st.departure_secs, t.source_trip_id, t.start_secs, "
                "t.wheelchair_accessible, t.bikes_allowed, "
                "st.pickup_type, st.drop_off_type, st.timepoint, st.stop_headsign, "
                "t.short_name, t.block_id "
                "FROM stop_times st JOIN trips t ON t.id = st.trip_id "
                f"WHERE st.stop_id IN ({stop_marks}) "
                f"AND t.service_id IN ({service_marks}) "
                "AND st.departure_secs >= ? AND st.departure_secs <= ?"
            )
            params: list[object] = [*stop_ids, *sorted(active), window_lo, window_hi]
            if route_ids:
                route_marks = ",".join("?" * len(route_ids))
                sql += f" AND t.route_id IN ({route_marks})"
                params.extend(route_ids)
            for (
                trip_id,
                route_id,
                headsign,
                stop_id,
                stop_sequence,
                arr_secs,
                dep_secs,
                source_trip_id,
                start_secs,
                wheelchair,
                bikes,
                pickup_type,
                drop_off_type,
                timepoint,
                stop_headsign,
                trip_short_name,
                block_id,
            ) in self._conn.execute(sql, params):
                results.append(
                    ScheduledDeparture(
                        trip_id=trip_id,
                        route_id=route_id,
                        headsign=headsign,
                        stop_id=stop_id,
                        stop_sequence=stop_sequence,
                        arrival=(
                            day_start_utc + timedelta(seconds=arr_secs)
                            if arr_secs is not None
                            else None
                        ),
                        departure=day_start_utc + timedelta(seconds=dep_secs),
                        source_trip_id=source_trip_id,
                        start_secs=start_secs,
                        service_date=service_date,
                        wheelchair_accessible=_enum_or_none(
                            WheelchairAccess, wheelchair
                        ),
                        bikes_allowed=_enum_or_none(BikesAllowed, bikes),
                        pickup_type=_enum_or_none(PickupDropOffType, pickup_type),
                        drop_off_type=_enum_or_none(PickupDropOffType, drop_off_type),
                        timepoint_exact=_timepoint_exact(timepoint),
                        stop_headsign=stop_headsign,
                        trip_short_name=trip_short_name,
                        block_id=block_id,
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

    # Shared origin->destination candidate predicate: right direction, both
    # ends timed, service active. Kept as one fragment so the row query and
    # the first/last-of-day extremes query can NEVER drift apart (the flags
    # are only correct if both queries agree on what a candidate is).
    _PAIR_CANDIDATES_SQL = (
        "FROM stop_times o "
        "JOIN stop_times d ON d.trip_id = o.trip_id "
        "JOIN trips t ON t.id = o.trip_id "
        "WHERE o.stop_id = ? AND d.stop_id = ? "
        "AND o.stop_sequence < d.stop_sequence "
        "AND o.departure_secs IS NOT NULL "
        "AND d.arrival_secs IS NOT NULL "
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
                "SELECT o.trip_id, t.route_id, t.headsign, "
                "o.departure_secs, MIN(d.arrival_secs), "
                "t.source_trip_id, t.start_secs, "
                "t.wheelchair_accessible, t.bikes_allowed, t.direction_id, "
                "o.pickup_type, o.drop_off_type, o.timepoint, o.stop_headsign, "
                "d.pickup_type, d.drop_off_type, d.timepoint, d.stop_headsign, "
                "o.stop_sequence, d.stop_sequence, t.short_name, t.block_id "
                f"{candidates} "
                "AND o.departure_secs >= ? AND o.departure_secs <= ? "
                "GROUP BY o.trip_id, o.stop_sequence"
            )
            params: list[object] = [
                origin_stop_id,
                destination_stop_id,
                *sorted(active),
                window_lo,
                window_hi,
            ]
            rows = self._conn.execute(sql, params).fetchall()
            if not rows:
                continue
            first_key, last_key = self._pair_day_extremes(
                origin_stop_id, destination_stop_id, active
            )
            for (
                trip_id,
                route_id,
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
                results.append(
                    ScheduledTrip(
                        trip_id=trip_id,
                        route_id=route_id,
                        headsign=headsign,
                        origin_stop_id=origin_stop_id,
                        destination_stop_id=destination_stop_id,
                        departure=day_start_utc + timedelta(seconds=dep_secs),
                        arrival=day_start_utc + timedelta(seconds=arr_secs),
                        source_trip_id=source_trip_id,
                        start_secs=start_secs,
                        service_date=service_date,
                        origin_stop_sequence=o_sequence,
                        destination_stop_sequence=d_sequence,
                        wheelchair_accessible=_enum_or_none(
                            WheelchairAccess, wheelchair
                        ),
                        bikes_allowed=_enum_or_none(BikesAllowed, bikes),
                        direction_id=direction_id,
                        origin_pickup_type=_enum_or_none(PickupDropOffType, o_pickup),
                        origin_drop_off_type=_enum_or_none(
                            PickupDropOffType, o_drop_off
                        ),
                        origin_timepoint_exact=_timepoint_exact(o_timepoint),
                        origin_stop_headsign=o_stop_headsign,
                        destination_pickup_type=_enum_or_none(
                            PickupDropOffType, d_pickup
                        ),
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
        # Total sort key, matching upcoming_departures's rationale: arrival
        # is the third component because a degenerate loop trip can depart
        # the origin twice at the identical instant.
        results.sort(key=lambda trip: (trip.departure, trip.trip_id, trip.arrival))
        return results[:limit]

    def close(self) -> None:
        """Close the underlying connection."""
        self._conn.close()
