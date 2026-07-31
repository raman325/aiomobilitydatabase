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
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import IO
from zoneinfo import ZoneInfo

from .exceptions import FeedParseError
from .models import Route, Stop

SCHEMA_VERSION = 1
_BATCH_SIZE = 5000
_SECONDS_OR_MINUTES_PER_UNIT = 60

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE stops (
    id TEXT PRIMARY KEY, name TEXT, lat REAL, lon REAL,
    parent_station TEXT, location_type INTEGER
);
CREATE TABLE routes (
    id TEXT PRIMARY KEY, short_name TEXT, long_name TEXT, type INTEGER
);
CREATE TABLE trips (
    id TEXT PRIMARY KEY, route_id TEXT NOT NULL,
    service_id TEXT NOT NULL, headsign TEXT
);
CREATE TABLE stop_times (
    trip_id TEXT NOT NULL, stop_id TEXT NOT NULL,
    arrival_secs INTEGER, departure_secs INTEGER, stop_sequence INTEGER NOT NULL
);
CREATE INDEX ix_stop_times_stop_departure ON stop_times (stop_id, departure_secs);
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


def parse_gtfs_time(value: str) -> int | None:
    """Parse an ``HH:MM:SS`` GTFS time (hours may exceed 23) to seconds."""
    value = value.strip()
    if not value:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in value.split(":"))
    except ValueError as err:
        raise FeedParseError(f"Invalid GTFS time: {value!r}") from err
    # int() also accepts signed and out-of-range components (e.g. "-1",
    # "08:75:00"); GTFS times are never negative and minutes/seconds are
    # bounded to [0, 60).
    if (
        hours < 0
        or not (0 <= minutes < _SECONDS_OR_MINUTES_PER_UNIT)
        or not (0 <= seconds < _SECONDS_OR_MINUTES_PER_UNIT)
    ):
        raise FeedParseError(f"Invalid GTFS time: {value!r}")
    return hours * 3600 + minutes * 60 + seconds


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
    """A scheduled stop event resolved to tz-aware datetimes."""

    trip_id: str
    route_id: str
    headsign: str | None
    stop_id: str
    arrival: datetime | None
    departure: datetime


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
        """Open an existing DB if it matches dataset ID and schema version."""
        if not Path(db_path).exists():
            return None
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        try:
            meta = dict(conn.execute("SELECT key, value FROM meta"))
        except sqlite3.DatabaseError:
            conn.close()
            return None
        if (
            meta.get("dataset_id") != dataset_id
            or meta.get("schema_version") != str(SCHEMA_VERSION)
            or "timezone" not in meta
        ):
            conn.close()
            return None
        return cls(conn, dataset_id, meta["timezone"])

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
            ("stops.txt", cls._load_stops),
            ("routes.txt", cls._load_routes),
            ("trips.txt", cls._load_trips),
            ("stop_times.txt", cls._load_stop_times),
            ("calendar.txt", cls._load_calendar),
            ("calendar_dates.txt", cls._load_calendar_dates),
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
    def _load_stops(
        cls,
        conn: sqlite3.Connection,
        reader: csv.DictReader[str],
        report: Callable[[], None] | None = None,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        for row in reader:
            rows.append(
                (
                    row["stop_id"],
                    row.get("stop_name") or None,
                    float(lat) if (lat := row.get("stop_lat", "").strip()) else None,
                    float(lon) if (lon := row.get("stop_lon", "").strip()) else None,
                    row.get("parent_station") or None,
                    int(loc) if (loc := row.get("location_type", "").strip()) else None,
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(
                    conn, "INSERT OR REPLACE INTO stops VALUES (?,?,?,?,?,?)", rows
                )
                if report is not None:
                    report()
        cls._batched_insert(
            conn, "INSERT OR REPLACE INTO stops VALUES (?,?,?,?,?,?)", rows
        )
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
        for row in reader:
            rows.append(
                (
                    row["route_id"],
                    row.get("route_short_name") or None,
                    row.get("route_long_name") or None,
                    int(rtype)
                    if (rtype := row.get("route_type", "").strip())
                    else None,
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(
                    conn, "INSERT OR REPLACE INTO routes VALUES (?,?,?,?)", rows
                )
                if report is not None:
                    report()
        cls._batched_insert(
            conn, "INSERT OR REPLACE INTO routes VALUES (?,?,?,?)", rows
        )
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
        for row in reader:
            rows.append(
                (
                    row["trip_id"],
                    row["route_id"],
                    row["service_id"],
                    row.get("trip_headsign") or None,
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(
                    conn, "INSERT OR REPLACE INTO trips VALUES (?,?,?,?)", rows
                )
                if report is not None:
                    report()
        cls._batched_insert(conn, "INSERT OR REPLACE INTO trips VALUES (?,?,?,?)", rows)
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
        for row in reader:
            rows.append(
                (
                    row["trip_id"],
                    row["stop_id"],
                    parse_gtfs_time(row.get("arrival_time", "")),
                    parse_gtfs_time(row.get("departure_time", "")),
                    int(row["stop_sequence"]),
                )
            )
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(
                    conn, "INSERT INTO stop_times VALUES (?,?,?,?,?)", rows
                )
                if report is not None:
                    report()
        cls._batched_insert(conn, "INSERT INTO stop_times VALUES (?,?,?,?,?)", rows)
        if report is not None:
            report()

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
                    *(int(row.get(day, "0") or 0) for day in _WEEKDAY_COLUMNS),
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
            rows.append((row["service_id"], row["date"], int(row["exception_type"])))
            if len(rows) >= _BATCH_SIZE:
                cls._batched_insert(conn, sql, rows)
                if report is not None:
                    report()
        cls._batched_insert(conn, sql, rows)
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
                location_type=row[5],
            )
            for row in self._conn.execute(
                "SELECT id, name, lat, lon, parent_station, location_type "
                "FROM stops ORDER BY name"
            )
        ]

    def routes(self) -> list[Route]:
        """All routes (for pickers)."""
        return [
            Route(id=row[0], short_name=row[1], long_name=row[2], type=row[3])
            for row in self._conn.execute(
                "SELECT id, short_name, long_name, type FROM routes ORDER BY id"
            )
        ]

    def route_display_names(self) -> dict[str, str]:
        """Map of route_id to display name (for RT joins)."""
        return {route.id: route.display_name for route in self.routes()}

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

    def routes_serving(self, stop_id: str) -> list[Route]:
        """Routes with at least one scheduled stop_time at the stop."""
        return [
            Route(id=row[0], short_name=row[1], long_name=row[2], type=row[3])
            for row in self._conn.execute(
                "SELECT DISTINCT r.id, r.short_name, r.long_name, r.type "
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
        per-stop stop_times.stop_headsign override is not indexed in v1.)
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
        for service_id, exception_type in self._conn.execute(
            "SELECT service_id, exception_type FROM calendar_dates WHERE date = ?",
            (datestr,),
        ):
            if exception_type == _EXCEPTION_SERVICE_ADDED:
                active.add(service_id)
            elif exception_type == _EXCEPTION_SERVICE_REMOVED:
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

    def upcoming_departures(
        self,
        stop_ids: list[str],
        route_ids: list[str] | None,
        now_utc: datetime,
        lookahead: timedelta,
        per_stop_limit: int,
    ) -> list[ScheduledDeparture]:
        """Scheduled departures at the given stops within the lookahead window.

        Considers every local service day from one day before ``now_utc``
        through one day after the window's local end date, so past-midnight
        trips (>24:00:00 times) surface on the correct clock day and long
        lookaheads are never truncated. Results are sorted by departure and
        truncated to ``per_stop_limit`` per stop.
        """
        local_today = now_utc.astimezone(self._tz).date()
        end_local = (now_utc + lookahead).astimezone(self._tz).date()
        results: list[ScheduledDeparture] = []
        # Scan every service day that could contribute: one day BEFORE the
        # window (>24:00:00 times still upcoming) through one day AFTER the
        # window's local end date (enclosure margin for DST edge instants).
        # Empty days cost one cheap indexed query; silently narrower bounds
        # cost dropped departures (hypothesis-found bug, 2026-07-31: the old
        # hardcoded local_today +/- 1 day tuple ignored `lookahead` entirely).
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
            window_lo = (now_utc - day_start).total_seconds()
            window_hi = window_lo + lookahead.total_seconds()
            if window_hi < 0:
                continue
            stop_marks = ",".join("?" * len(stop_ids))
            service_marks = ",".join("?" * len(active))
            sql = (
                "SELECT st.trip_id, t.route_id, t.headsign, st.stop_id, "
                "st.arrival_secs, st.departure_secs "
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
                arr_secs,
                dep_secs,
            ) in self._conn.execute(sql, params):
                results.append(
                    ScheduledDeparture(
                        trip_id=trip_id,
                        route_id=route_id,
                        headsign=headsign,
                        stop_id=stop_id,
                        arrival=(
                            day_start_utc + timedelta(seconds=arr_secs)
                            if arr_secs is not None
                            else None
                        ),
                        departure=day_start_utc + timedelta(seconds=dep_secs),
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
        limited: list[ScheduledDeparture] = []
        per_stop_counts: dict[str, int] = {}
        for dep in results:
            count = per_stop_counts.get(dep.stop_id, 0)
            if count < per_stop_limit:
                limited.append(dep)
                per_stop_counts[dep.stop_id] = count + 1
        return limited

    def close(self) -> None:
        """Close the underlying connection."""
        self._conn.close()
