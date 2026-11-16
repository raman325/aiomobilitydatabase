"""Tests for SQLite index build and cache-open."""

import io
import os
import sqlite3
import struct
import traceback
import zipfile
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds._numbers import exact_number
from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.models import StopLocationType
from aiomobilitydatabase.feeds.static_index import (
    SCHEMA_VERSION,
    StaticIndex,
    _coordinate,
    _lenient_date,
    _lenient_int,
    _strict_int,
    parse_gtfs_time,
)

from tests.feeds.fixtures import (
    _FILES,
    build_flex_gtfs_zip_bytes,
    build_gtfs_zip_bytes,
)

DATASET = "mdb-100-202607310000"
TZ = "America/Los_Angeles"


def _write_zip(tmp_path: Path) -> Path:
    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(build_gtfs_zip_bytes())
    return zip_path


def test_build_in_memory_and_query_static_tables(tmp_path: Path) -> None:
    index = StaticIndex.build(_write_zip(tmp_path), ":memory:", DATASET, TZ)
    stops = {stop.id: stop for stop in index.stops()}
    assert stops["S1"].name == "Main St"
    assert stops["S1"].latitude == 34.05
    assert stops["S3"].parent_station == "ST1"
    assert stops["ST1"].location_type is StopLocationType.STATION
    assert stops["ST1"].location_type == 1  # IntEnum: raw comparisons keep working
    assert stops["S1"].parent_station is None
    routes = {route.id: route for route in index.routes()}
    assert routes["R1"].display_name == "10 Main Line"
    assert index.dataset_id == DATASET
    assert index.routes_for_trips(["T1", "T3"]) == {"T1": "R1", "T3": "R2"}
    index.close()


def test_build_to_file_then_open_cached(tmp_path: Path) -> None:
    db_path = tmp_path / "static.db"
    built = StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ)
    built.close()
    assert db_path.exists()
    reopened = StaticIndex.open_cached(db_path, DATASET)
    assert reopened is not None
    assert reopened.dataset_id == DATASET
    assert {s.id for s in reopened.stops()} == {"S1", "S2", "S3", "ST1"}
    reopened.close()


def test_open_cached_rejects_dataset_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "static.db"
    StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ).close()
    assert StaticIndex.open_cached(db_path, "mdb-100-NEWER") is None
    assert StaticIndex.open_cached(tmp_path / "missing.db", DATASET) is None


def test_timezone_fallback_to_agency_txt(tmp_path: Path) -> None:
    index = StaticIndex.build(_write_zip(tmp_path), ":memory:", DATASET, None)
    assert index.timezone_name == TZ
    index.close()


def test_unreadable_zip_raises_feed_parse_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.zip"
    bad.write_bytes(b"this is not a zip")
    with pytest.raises(FeedParseError):
        StaticIndex.build(bad, ":memory:", DATASET, TZ)


def test_missing_timezone_everywhere_raises(tmp_path: Path) -> None:
    zip_path = tmp_path / "no_agency.zip"
    zip_path.write_bytes(build_gtfs_zip_bytes(omit=frozenset({"agency.txt"})))
    with pytest.raises(FeedParseError):
        StaticIndex.build(zip_path, ":memory:", DATASET, None)


def test_agency_txt_present_but_blank_timezone_raises(tmp_path: Path) -> None:
    """agency.txt is present (so the ``no agency.txt`` short-circuit doesn't
    apply) but every row's agency_timezone is blank -- the fallback scan
    must fall through its loop to "no timezone found" rather than crash.
    """
    files = dict(_FILES)
    files["agency.txt"] = (
        "agency_id,agency_name,agency_url,agency_timezone\nA1,T,https://e.com,\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    zip_path = tmp_path / "blank_tz.zip"
    zip_path.write_bytes(buf.getvalue())
    with pytest.raises(FeedParseError, match="No valid agency timezone"):
        StaticIndex.build(zip_path, ":memory:", DATASET, None)


def test_ragged_stops_row_raises_feed_parse_error(tmp_path: Path) -> None:
    """A stops.txt data row with fewer columns than the header (a ragged row,
    which real producers ship) is malformed: FeedParseError, never a bare
    error from the None csv.DictReader fills the missing cells with.
    """
    header, first_row, *rest = _FILES["stops.txt"].splitlines()
    truncated_first_row = ",".join(first_row.split(",")[:2])  # drop trailing columns
    ragged_stops = "\n".join([header, truncated_first_row, *rest]) + "\n"
    corrupted = dict(_FILES)
    corrupted["stops.txt"] = ragged_stops
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in corrupted.items():
            zf.writestr(name, content)
    zip_path = tmp_path / "ragged.zip"
    zip_path.write_bytes(buf.getvalue())
    with pytest.raises(FeedParseError):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def test_schema_version_mismatch_invalidates(tmp_path: Path) -> None:
    db_path = tmp_path / "static.db"
    built = StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ)
    built._conn.execute(
        "UPDATE meta SET value = ? WHERE key = 'schema_version'",
        (str(SCHEMA_VERSION + 1),),
    )
    built._conn.commit()
    built.close()
    assert StaticIndex.open_cached(db_path, DATASET) is None


def test_open_cached_corrupted_database_returns_none(tmp_path: Path) -> None:
    """A file that exists but isn't a valid SQLite DB (e.g. truncated by a
    prior crash) must be treated as a cache miss, not raise.
    """
    db_path = tmp_path / "static.db"
    db_path.write_bytes(b"not a sqlite database at all")
    assert StaticIndex.open_cached(db_path, DATASET) is None


def test_build_to_file_cleans_up_building_file_on_failure(tmp_path: Path) -> None:
    """A build failure partway through (missing required file) must leave
    neither the ``.building`` scratch file nor a final DB behind -- the old
    cache entry (if any) is untouched, and no half-built file lingers.
    """
    zip_path = tmp_path / "bad.zip"
    zip_path.write_bytes(build_gtfs_zip_bytes(omit=frozenset({"stops.txt"})))
    db_path = tmp_path / "static.db"
    with pytest.raises(FeedParseError):
        StaticIndex.build(zip_path, str(db_path), DATASET, TZ)
    assert not db_path.exists()
    assert not list(tmp_path.glob("*.building"))


def test_build_to_file_reraises_and_cleans_up_on_replace_failure(
    tmp_path: Path,
) -> None:
    """If the final atomic rename fails (e.g. the target is unexpectedly a
    directory), the ``.building`` scratch file must still be removed and the
    original OSError must propagate rather than being swallowed.
    """
    db_target = tmp_path / "static_as_dir"
    db_target.mkdir()  # not a plausible DB path: forces Path.replace() to fail
    with pytest.raises(OSError, match="Is a directory"):
        StaticIndex.build(_write_zip(tmp_path), str(db_target), DATASET, TZ)
    assert not list(tmp_path.glob("*.building"))


def test_a_build_killed_midway_does_not_block_the_next(tmp_path: Path) -> None:
    """A build killed midway leaves its scratch file, tables and all. The
    next build must not trip over it ("table meta already exists"): each
    build writes a scratch file of its own."""
    db_path = tmp_path / "static.db"
    for leftover in (Path(f"{db_path}.building"), tmp_path / "static.db.x.building"):
        conn = sqlite3.connect(leftover)
        conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
        conn.commit()
        conn.close()
    StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ).close()
    reopened = StaticIndex.open_cached(db_path, DATASET)
    assert reopened is not None
    reopened.close()


def test_routes_for_trips_empty_list_returns_empty_dict(tmp_path: Path) -> None:
    index = StaticIndex.build(_write_zip(tmp_path), ":memory:", DATASET, TZ)
    try:
        assert index.routes_for_trips([]) == {}
    finally:
        index.close()


def test_loaders_flush_mid_loop_past_batch_size(tmp_path: Path) -> None:
    """The fixture GTFS feed is far too small to reach the 5000-row mid-loop
    flush in every ``_load_*`` loader; manufacture oversized-but-valid CSVs
    (one file at a time is enough to prove the pattern, but we push every
    file past the threshold at once for coverage of every loader) and
    also pass a progress callback so the flush's ``report()`` call itself
    runs, not just the flush condition.
    """
    n = 5001
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n"
        + "".join(f"A{i},T,https://e.com,UTC\n" for i in range(n)),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"
        + "".join(f"S{i},Stop {i},34.0,-118.0\n" for i in range(n)),
        "routes.txt": "route_id,route_short_name,route_long_name,route_type\n"
        + "".join(f"R{i},{i},Route {i},3\n" for i in range(n)),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\n"
        + "".join(f"R0,SVC,T{i},H\n" for i in range(n)),
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        + "".join(f"T{i},08:00:00,08:00:00,S0,1\n" for i in range(n)),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
            "sunday,start_date,end_date\n"
        )
        + "".join(f"SVC{i},1,1,1,1,1,1,1,20260101,20271231\n" for i in range(n)),
        "calendar_dates.txt": "service_id,date,exception_type\n"
        + "".join("SVC,20260704,1\n" for _ in range(n)),
        # One frequency row whose 5400 one-stop repetitions (00:00-01:30
        # every second) push the materialized stop_times batch past the
        # mid-loop flush threshold inside _load_frequencies too.
        "frequencies.txt": (
            "trip_id,start_time,end_time,headway_secs\nT0,00:00:00,01:30:00,1\n"
        ),
        # feed_info.txt so its (single-row) loader's report() call runs
        # under a progress callback like every other loader's.
        "feed_info.txt": ("feed_publisher_name,feed_publisher_url\nBig Publisher,\n"),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    zip_path = tmp_path / "big.zip"
    zip_path.write_bytes(buf.getvalue())
    progress_calls: list[tuple[int, int | None]] = []
    index = StaticIndex.build(
        zip_path,
        ":memory:",
        DATASET,
        TZ,
        lambda done, total: progress_calls.append((done, total)),
    )
    try:
        assert len(index.stops()) == n
        assert len(index.routes()) == n
        assert len(index.agencies()) == n
        info = index.feed_info()
        assert info is not None and info.publisher_name == "Big Publisher"
        assert progress_calls  # report() ran at least once per flushed loader
    finally:
        index.close()


def test_unicode_digit_stop_time_fails_the_build(tmp_path: Path) -> None:
    """A whole-build check on the ASCII-digit rule: a stop_times cell spelled
    in unicode digits used to parse as a real departure (08:00:00), so the
    corrupt row entered the index unnoticed. It now fails the build.
    """
    corrupted = dict(_FILES)
    corrupted["stop_times.txt"] = _FILES["stop_times.txt"].replace(
        "08:00:00", "\u0660\u0668:00:00", 1
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in corrupted.items():
            zf.writestr(name, content)
    zip_path = tmp_path / "unicode_time.zip"
    zip_path.write_bytes(buf.getvalue())
    with pytest.raises(FeedParseError, match="Invalid GTFS time"):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("08:00:00", 28800),
        ("8:00:00", 28800),
        ("8:0:0", 28800),
        ("0008:00:00", 28800),
        # Service days run past midnight, so hours are unbounded above.
        ("27:30:00", 99000),
        ("300:00:00", 1080000),
        # A blank cell means "no time at this stop", not a parse failure.
        ("", None),
        ("   ", None),
        # Surrounding whitespace is a formatting artifact of real exporters.
        (" 08:00:00 ", 28800),
        ("\t08:00:00\n", 28800),
    ],
)
def test_parse_gtfs_time_accepts(value: str, expected: int | None) -> None:
    assert parse_gtfs_time(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        # ASCII digits only: unicode decimals would silently turn a corrupt
        # cell into a plausible-looking time.
        "\u0660\u0668:00:00",
        "08:\u0660\u0660:00",
        "08:00:\u0660\u0660",
        "\uff10\uff18:00:00",
        # int() accepts sign prefixes and underscore grouping; GTFS does not.
        "+8:00:00",
        "-1:00:00",
        "-0:00:00",
        "08:+0:00",
        "1_0:00:00",
        # Exactly three colon-separated components.
        "08:00",
        "08",
        "1:2:3:4",
        "08:00:00:",
        ":00:00",
        # No fractional seconds in GTFS.
        "08:00:00.5",
        "08.5:00:00",
        # Intra-component whitespace is not a digit.
        "08: 00:00",
        "0 8:00:00",
        # Out-of-range minutes/seconds.
        "08:60:00",
        "08:75:00",
        "08:00:60",
        "08:00:99",
        "garbage",
    ],
)
def test_parse_gtfs_time_rejects(value: str) -> None:
    with pytest.raises(FeedParseError):
        parse_gtfs_time(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("1", 1),
        ("01", 1),
        # Leniency is about not failing the build, not about the vocabulary:
        # any non-negative ASCII int is stored verbatim.
        ("12", 12),
        ("999999999999", 999999999999),
        (" 5 ", 5),
        (None, None),
        ("", None),
        ("  ", None),
        ("x", None),
        ("1.5", None),
        # Same ASCII-digit rule as parse_gtfs_time/_lenient_date, but
        # lenient in kind: garbage is None rather than an exception.
        ("\u0665", None),
        ("\uff11\uff12", None),
        ("+5", None),
        ("-5", None),
        ("5_0", None),
    ],
)
def test_lenient_int_contract(value: str | None, expected: int | None) -> None:
    assert _lenient_int(value) == expected


def _zip_from_files(
    tmp_path: Path, files: Mapping[str, str | bytes], name: str
) -> Path:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for member, content in files.items():
            zf.writestr(member, content)
    zip_path = tmp_path / name
    zip_path.write_bytes(buf.getvalue())
    return zip_path


def _with_cell(content: str, row: str, column: int, cell: str) -> str:
    """Replace one field of one data row, keeping the rest of the file."""
    lines = content.splitlines()
    fields = lines[lines.index(row)].split(",")
    fields[column] = cell
    lines[lines.index(row)] = ",".join(fields)
    return "\n".join(lines) + "\n"


# Spellings a bare int() accepts, each turning a corrupt structural cell
# into a plausible-looking number: unicode decimals, sign prefixes and PEP
# 515 underscore grouping.
_NON_ASCII_INT_CELLS = ("\u0668", "\uff11", "+1", "1_0")


@pytest.mark.parametrize("cell", _NON_ASCII_INT_CELLS)
@pytest.mark.parametrize(
    ("member", "row", "column"),
    [
        pytest.param("stop_times.txt", "T1,08:00:00,08:00:30,S1,1", 4, id="stop_seq"),
        pytest.param(
            "calendar.txt", "WKDY,1,1,1,1,1,0,0,20260101,20271231", 1, id="monday"
        ),
        pytest.param("calendar_dates.txt", "SPECIAL,20260704,1", 2, id="exception"),
    ],
)
def test_structural_int_cell_fails_the_build(
    tmp_path: Path, member: str, row: str, column: int, cell: str
) -> None:
    """Structural integer columns take the same ASCII-digit rule as the time
    columns: a corrupt cell that a bare int() would have read as a number
    fails the build instead of entering the index as plausible data.
    """
    files = dict(_FILES)
    files[member] = _with_cell(_FILES[member], row, column, cell)
    zip_path = _zip_from_files(tmp_path, files, "structural_int.zip")
    with pytest.raises(FeedParseError):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


@pytest.mark.parametrize("cell", _NON_ASCII_INT_CELLS)
def test_non_ascii_headway_fails_the_build(tmp_path: Path, cell: str) -> None:
    """headway_secs is structural too: it sets how many repetitions of the
    template trip get materialized, so a corrupt cell must not be read as a
    number by int()'s wider grammar.
    """
    files = dict(_FILES)
    files["frequencies.txt"] = (
        f"trip_id,start_time,end_time,headway_secs\nT1,08:00:00,09:00:00,{cell}\n"
    )
    zip_path = _zip_from_files(tmp_path, files, "headway.zip")
    with pytest.raises(FeedParseError):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


@pytest.mark.parametrize("cell", _NON_ASCII_INT_CELLS)
def test_non_ascii_location_type_is_none_not_a_build_failure(
    tmp_path: Path, cell: str
) -> None:
    """location_type is DESCRIPTIVE: it already degrades to None at the model
    boundary for anything outside its vocabulary, and a blank cell already
    means "a plain stop", so a corrupt cell must become None rather than
    take the whole schedule down.
    """
    files = dict(_FILES)
    files["stops.txt"] = _with_cell(
        _FILES["stops.txt"], "ST1,Depot Station,34.0705,-118.2295,,1", 5, cell
    )
    zip_path = _zip_from_files(tmp_path, files, "location_type.zip")
    index = StaticIndex.build(zip_path, ":memory:", DATASET, TZ)
    try:
        stops = {stop.id: stop for stop in index.stops()}
        assert stops["ST1"].location_type is None
    finally:
        index.close()


@pytest.mark.parametrize("cell", _NON_ASCII_INT_CELLS)
def test_non_ascii_route_type_is_not_a_build_failure(tmp_path: Path, cell: str) -> None:
    """route_type is DESCRIPTIVE: nothing in this library reads it back, so a
    corrupt cell must not fail a build that is otherwise usable.
    """
    files = dict(_FILES)
    files["routes.txt"] = _with_cell(_FILES["routes.txt"], "R1,10,Main Line,3", 3, cell)
    zip_path = _zip_from_files(tmp_path, files, "route_type.zip")
    index = StaticIndex.build(zip_path, ":memory:", DATASET, TZ)
    try:
        assert {route.id for route in index.routes()} == {"R1", "R2"}
    finally:
        index.close()


# Past CPython's default 4300-digit cap on int-string conversion, so int()
# raises ValueError even though every character is an ASCII digit.
_OVERSIZED_DIGITS = "1" * 5000


@pytest.mark.parametrize("position", [0, 1, 2])
def test_parse_gtfs_time_rejects_oversized_components(position: int) -> None:
    """A component can be all ASCII digits and still refuse to convert:
    CPython caps int-string conversion at 4300 digits. That must surface as
    the documented FeedParseError, not the raw ValueError -- callers that
    catch FeedParseError (rt.py's _trip_start_secs) would miss it.
    """
    cells = ["00", "00", "00"]
    cells[position] = _OVERSIZED_DIGITS
    with pytest.raises(FeedParseError):
        parse_gtfs_time(":".join(cells))


def test_strict_int_rejects_oversized_digits() -> None:
    """The same digit cap on a structural integer column: FeedParseError
    rather than the raw ValueError from int().
    """
    with pytest.raises(FeedParseError):
        _strict_int(_OVERSIZED_DIGITS, "stop_sequence")


def test_lenient_int_oversized_digits_is_none() -> None:
    """_lenient_int is documented to answer None for anything unparseable so
    that one bad descriptive cell cannot abort a build; a cell past the
    4300-digit conversion cap is unparseable like any other.
    """
    assert _lenient_int(_OVERSIZED_DIGITS) is None


def test_lenient_date_oversized_digits_is_none() -> None:
    """_lenient_date needs no conversion guard of its own: its length check
    rejects an oversized cell before any int() runs.
    """
    assert _lenient_date(_OVERSIZED_DIGITS) is None


def test_open_cached_directory_at_db_path_returns_none(tmp_path: Path) -> None:
    """A directory where the cached DB should be is a cache miss, not a
    crash: the caller rebuilds on None but lets exceptions propagate, so a
    raising open_cached would brick the feed permanently.
    """
    db_path = tmp_path / "static.db"
    db_path.mkdir()
    assert StaticIndex.open_cached(db_path, DATASET) is None


def test_open_cached_non_regular_file_returns_none(tmp_path: Path) -> None:
    db_path = tmp_path / "static.db"
    os.mkfifo(db_path)
    assert StaticIndex.open_cached(db_path, DATASET) is None


def test_open_cached_empty_file_returns_none(tmp_path: Path) -> None:
    db_path = tmp_path / "static.db"
    db_path.write_bytes(b"")
    assert StaticIndex.open_cached(db_path, DATASET) is None


def test_open_cached_missing_parent_returns_none(tmp_path: Path) -> None:
    assert StaticIndex.open_cached(tmp_path / "gone" / "static.db", DATASET) is None


def _open_db_fd_count(db_path: Path) -> int:
    """Number of this process's open file descriptors pointing at db_path."""
    target = str(db_path)
    count = 0
    for name in os.listdir("/proc/self/fd"):
        try:
            link = os.readlink(f"/proc/self/fd/{name}")
        except OSError:
            continue  # fd closed while scanning (e.g. listdir's own handle)
        count += link == target
    return count


@pytest.mark.parametrize(
    "bad_timezone",
    [
        pytest.param("Not/A/Zone", id="unknown-key"),
        pytest.param("", id="empty"),
        pytest.param("123", id="numeric"),
        pytest.param("/absolute", id="absolute"),
        pytest.param("../escape", id="traversal"),
        pytest.param(b"\x00\x01", id="blob"),
    ],
)
@pytest.mark.skipif(
    not Path("/proc/self/fd").is_dir(), reason="needs /proc to count open fds"
)
def test_open_cached_invalid_timezone_metadata_returns_none(
    tmp_path: Path, bad_timezone: str | bytes
) -> None:
    """A structurally valid cache carrying an unusable timezone is a cache
    miss, not a crash: ZoneInfo raises inside __init__, one layer below the
    guards, and that would propagate out of the rebuild path. The
    connection opened to read ``meta`` must not leak either.
    """
    db_path = tmp_path / "static.db"
    StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ).close()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE meta SET value = ? WHERE key = 'timezone'", (bad_timezone,)
        )
    conn.close()
    before = _open_db_fd_count(db_path)
    assert StaticIndex.open_cached(db_path, DATASET) is None
    assert _open_db_fd_count(db_path) == before


def test_open_cached_truncated_database_returns_none(tmp_path: Path) -> None:
    db_path = tmp_path / "static.db"
    StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ).close()
    intact = db_path.read_bytes()
    db_path.write_bytes(intact[: len(intact) // 3])
    assert StaticIndex.open_cached(db_path, DATASET) is None


def test_open_cached_unreadable_file_returns_none(tmp_path: Path) -> None:
    """A regular file sqlite3 cannot open is still a cache miss.

    `is_file()` screens out directories and FIFOs, so the guard around
    `sqlite3.connect` only fires for a file the process may not read -- the
    last way a corrupt cache entry could brick a feed instead of rebuilding.
    """
    db_path = tmp_path / "unreadable.db"
    StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ).close()
    db_path.chmod(0o000)
    try:
        if os.access(db_path, os.R_OK):
            pytest.skip("cannot make a file unreadable for this process (root?)")
        assert StaticIndex.open_cached(db_path, DATASET) is None
    finally:
        db_path.chmod(0o644)


def test_schema_fingerprint_matches_version(tmp_path: Path) -> None:
    """The schema's shape is pinned to SCHEMA_VERSION.

    A cache is discarded only when its stamped version differs, so adding
    a column without bumping the version makes an OLD cache pass
    validation and then raise OperationalError on the first query that
    selects the new column. No upgrade test catches that on its own,
    because tests build fresh indexes; this one fails the moment the
    schema and the version drift apart.
    """
    built = StaticIndex.build(_write_zip(tmp_path), ":memory:", DATASET, TZ)
    fingerprint = {
        table: sorted(
            row[1] for row in built._conn.execute(f"PRAGMA table_info({table})")
        )
        for (table,) in built._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
    }
    indexes = sorted(
        name
        for (name,) in built._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    )
    built.close()
    # Indexes are part of the shape too: a cache from before one was added
    # would serve every query, but slowly, until the next dataset.
    assert indexes == [
        "ix_calendar_dates_date",
        "ix_flex_stop_times_group",
        "ix_flex_stop_times_location",
        "ix_flex_stop_times_stop",
        "ix_flex_stop_times_trip",
        "ix_stop_times_stop_departure",
        "ix_stop_times_trip_sequence",
        "ix_trips_route_start",
        "ix_trips_source",
    ], "indexes changed: bump SCHEMA_VERSION and update this list"
    assert (SCHEMA_VERSION, fingerprint) == (
        13,
        {
            "agencies": [
                "email",
                "fare_url",
                "id",
                "lang",
                "name",
                "phone",
                "timezone",
                "url",
            ],
            "calendar": [
                "end_date",
                "friday",
                "monday",
                "saturday",
                "service_id",
                "start_date",
                "sunday",
                "thursday",
                "tuesday",
                "wednesday",
            ],
            "booking_rules": [
                "booking_type",
                "booking_url",
                "drop_off_message",
                "id",
                "info_url",
                "message",
                "phone_number",
                "pickup_message",
                "prior_notice_duration_max",
                "prior_notice_duration_min",
                "prior_notice_last_day",
                "prior_notice_last_secs",
                "prior_notice_service_id",
                "prior_notice_start_day",
                "prior_notice_start_secs",
                "unreadable",
            ],
            "calendar_dates": ["date", "exception_type", "service_id"],
            "feed_info": [
                "contact_email",
                "contact_url",
                "end_date",
                "lang",
                "publisher_name",
                "publisher_url",
                "start_date",
                "version",
            ],
            "flex_stop_times": [
                "drop_off_booking_rule_id",
                "drop_off_type",
                "end_window_secs",
                "location_group_id",
                "location_id",
                "pickup_booking_rule_id",
                "pickup_type",
                "start_window_secs",
                "stop_headsign",
                "stop_id",
                "stop_sequence",
                "trip_id",
            ],
            "location_group_stops": ["location_group_id", "stop_id"],
            "location_groups": ["id", "name"],
            "locations": ["description", "id", "name", "polygons"],
            "meta": ["key", "value"],
            "routes": [
                "agency_id",
                "color",
                "description",
                "id",
                "long_name",
                "short_name",
                "sort_order",
                "text_color",
                "type",
                "url",
            ],
            "stop_times": [
                "arrival_secs",
                "departure_secs",
                "drop_off_type",
                "pickup_type",
                "stop_headsign",
                "stop_id",
                "stop_sequence",
                "timepoint",
                "trip_id",
            ],
            "stops": [
                "description",
                "id",
                "lat",
                "location_type",
                "lon",
                "name",
                "parent_station",
                "platform_code",
                "stop_code",
                "timezone",
                "url",
                "wheelchair_boarding",
                "zone_id",
            ],
            "trips": [
                "bikes_allowed",
                "block_id",
                "direction_id",
                "exact_times",
                "first_secs",
                "headsign",
                "id",
                "route_id",
                "service_id",
                "short_name",
                "slot_secs",
                "source_trip_id",
                "start_secs",
                "wheelchair_accessible",
            ],
        },
    ), "schema changed: bump SCHEMA_VERSION and update this fingerprint"


def test_cache_from_the_previous_schema_is_discarded(tmp_path: Path) -> None:
    """The real upgrade path: a cache built by the PREVIOUS release.

    v3 had no agencies.email. Stamped v3, it must read as a cache miss and
    be rebuilt -- not be accepted and then raise on the first agencies()
    query.
    """
    db_path = tmp_path / "static.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        "CREATE TABLE agencies (id TEXT, name TEXT, url TEXT, timezone TEXT,"
        " lang TEXT, phone TEXT, fare_url TEXT);"
    )
    conn.executemany(
        "INSERT INTO meta VALUES (?, ?)",
        [("dataset_id", DATASET), ("schema_version", "3"), ("timezone", TZ)],
    )
    conn.commit()
    conn.close()
    assert StaticIndex.open_cached(db_path, DATASET) is None


def test_trip_queries_with_no_ids_do_not_touch_the_database(tmp_path: Path) -> None:
    """An empty id list is a question with no answer, not an empty query.

    Both of these interpolate their ids into an IN (...) clause, which an
    empty list would render as `IN ()` -- a SQLite syntax error.
    """
    built = StaticIndex.build(_write_zip(tmp_path), ":memory:", DATASET, TZ)
    assert built.concrete_trip_ids([]) == {}
    assert built.instance_trip_calls([]) == []
    built.close()


def test_a_build_replaced_by_a_concurrent_one_is_labelled_by_its_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two builds of one target can rename into place back to back. The
    build reopening the target afterwards must report the dataset the file
    really holds, never claim its own over another build's data."""
    other = tmp_path / "other.db"
    StaticIndex.build(_write_zip(tmp_path), str(other), "ds-OTHER", TZ).close()
    with sqlite3.connect(other) as conn:  # the other dataset's own zone
        conn.execute("UPDATE meta SET value = 'Europe/Paris' WHERE key = 'timezone'")
    conn.close()
    db_path = tmp_path / "static.db"
    real_replace = Path.replace

    def replace_then_lose_the_race(self: Path, target: Path | str) -> Path:
        result = real_replace(self, target)
        if Path(target) == db_path:
            # The other build renames in right after ours.
            db_path.write_bytes(other.read_bytes())
        return result

    monkeypatch.setattr(Path, "replace", replace_then_lose_the_race)
    index = StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ)
    try:
        assert (index.dataset_id, index.timezone_name) == ("ds-OTHER", "Europe/Paris")
    finally:
        index.close()


@pytest.mark.parametrize(
    "stranger",
    [
        "no keys",
        "no timezone",
        "unknown timezone",
        "older schema",
        "not a database",
        "purged",
    ],
)
def test_a_reopened_file_that_is_no_index_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stranger: str
) -> None:
    """Whatever lands on the target between the rename and the reopen, a
    file that is not one of this version's built indexes -- or none at all
    -- is refused as such, never labelled with the caller's dataset, and a
    purged target is not recreated as an empty file."""
    db_path = tmp_path / "static.db"
    real_replace = Path.replace
    meta = {
        "no keys": [],
        "no timezone": [("dataset_id", "ds"), ("schema_version", str(SCHEMA_VERSION))],
        "unknown timezone": [
            ("dataset_id", "ds"),
            ("timezone", "Mars/Olympus"),
            ("schema_version", str(SCHEMA_VERSION)),
        ],
        "older schema": [
            ("dataset_id", "ds"),
            ("timezone", TZ),
            ("schema_version", str(SCHEMA_VERSION - 1)),
        ],
    }

    def replace_with_a_stranger(self: Path, target: Path | str) -> Path:
        result = real_replace(self, target)
        if Path(target) == db_path:
            db_path.unlink()
            if stranger == "not a database":
                db_path.write_bytes(b"not a database at all, just bytes " * 100)
            elif stranger != "purged":
                with sqlite3.connect(db_path) as conn:
                    conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
                    conn.executemany("INSERT INTO meta VALUES (?, ?)", meta[stranger])
                conn.close()
        return result

    monkeypatch.setattr(Path, "replace", replace_with_a_stranger)
    opened: list[sqlite3.Connection] = []
    real_connect = sqlite3.connect

    def tracked(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        conn = real_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", tracked)
    with pytest.raises(FeedParseError):
        StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ)
    if stranger == "purged":
        assert not db_path.exists()
    for conn in opened:  # every connection the build opened is closed
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")


def test_a_build_reopens_a_target_whose_path_needs_quoting(tmp_path: Path) -> None:
    """The reopen goes through a file: URI: a path with spaces, percent
    signs and URI punctuation still names the file."""
    folder = tmp_path / "a b%20c#d?e;f&g"
    folder.mkdir()
    index = StaticIndex.build(
        _write_zip(tmp_path), str(folder / "static.db"), DATASET, TZ
    )
    try:
        assert index.dataset_id == DATASET
        assert index.stops()
    finally:
        index.close()


def test_a_scratch_file_is_removed_when_its_connection_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even a build that cannot open its scratch file leaves none behind."""
    real_connect = sqlite3.connect

    def refuse_scratch(database: str, *args: object, **kwargs: object) -> object:
        if str(database).endswith(".building"):
            raise sqlite3.OperationalError("unable to open database file")
        return real_connect(database, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(sqlite3, "connect", refuse_scratch)
    with pytest.raises(sqlite3.OperationalError):
        StaticIndex.build(
            _write_zip(tmp_path), str(tmp_path / "static.db"), DATASET, TZ
        )
    assert not list(tmp_path.glob("*.building*"))


# -- stop_lat/stop_lon: finite ASCII decimals only -------------------------------

_STOP_ROW = "ST1,Depot Station,34.0705,-118.2295,,1"


@pytest.mark.parametrize(
    "cell",
    [
        "٣٤.0705",  # Arabic-Indic digits
        chr(0xFF13) + chr(0xFF14) + ".0705",  # full-width digits
        "3_4.0705",  # PEP 515 grouping
        "nan",
        "inf",
        "-Infinity",
        "1e999",  # overflows to inf
        "34. 0705",
        "0x22",
        "abc",
    ],
)
@pytest.mark.parametrize("column", [2, 3], ids=["stop_lat", "stop_lon"])
def test_a_coordinate_float_alone_would_read_fails_the_build(
    tmp_path: Path, cell: str, column: int
) -> None:
    """GTFS writes a Float in ASCII decimal. float() also reads other
    scripts' digits, underscores, nan and inf, each turning a corrupt cell
    into a plausible -- or no -- position; they fail the build as ``abc``
    always has."""
    files = dict(_FILES)
    files["stops.txt"] = _with_cell(_FILES["stops.txt"], _STOP_ROW, column, cell)
    zip_path = _zip_from_files(tmp_path, files, "coordinate.zip")
    with pytest.raises(FeedParseError, match=r"Malformed stops\.txt"):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


_SPELLINGS = {
    "repr": repr,
    "signed": lambda value: f"{value:+}",
    "exponent": lambda value: f"{value:e}",
    "padded": lambda value: f" {value!r}\t",
    "bare-fraction": lambda value: f"{value:f}".replace("0.", ".", 1),
}


@given(value=st.floats(-180, 180), spelling=st.sampled_from(sorted(_SPELLINGS)))
def test_an_ascii_decimal_coordinate_reads_as_written(
    value: float, spelling: str
) -> None:
    text = _SPELLINGS[spelling](value)
    assert abs(float(text.strip())) <= 180  # no spelling grows the value
    assert _coordinate(text, "stop_lon", 180) == float(text.strip())


@pytest.mark.parametrize(
    ("column", "cell"),
    [
        (2, "90.000001"),
        (2, "-90.5"),
        (2, "200"),
        (3, "180.0001"),
        (3, "-181"),
        (3, "1e300"),
        # Past the limit as written, though each one's double is the limit.
        (2, "90.000000000000001"),
        (2, "-90.0000000000000000000000000001"),
        (3, "180.00000000000001"),
        (3, "-1800000000000000000000000000001e-28"),
    ],
)
def test_a_coordinate_outside_wgs84_fails_the_build(
    tmp_path: Path, column: int, cell: str
) -> None:
    """Latitude 200 names no place: an impossible coordinate is as malformed
    as an unreadable one (maintainer's choice), as an out-of-range position
    in locations.geojson already is."""
    files = dict(_FILES)
    files["stops.txt"] = _with_cell(_FILES["stops.txt"], _STOP_ROW, column, cell)
    zip_path = _zip_from_files(tmp_path, files, "range.zip")
    with pytest.raises(FeedParseError, match="outside"):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


@pytest.mark.parametrize(
    ("lat", "lon"),
    [
        ("90", "180"),
        ("-90", "-180"),
        ("+90.0", "-180.000"),
        ("9e1", "18000000000000000000000000000000e-29"),
        # Inside as written; its double is the limit, which is a place.
        ("89.99999999999999999", "-179.99999999999999999"),
    ],
)
def test_the_wgs84_edges_are_coordinates(tmp_path: Path, lat: str, lon: str) -> None:
    files = dict(_FILES)
    row = _with_cell(_FILES["stops.txt"], _STOP_ROW, 2, lat)
    files["stops.txt"] = _with_cell(row, _STOP_ROW.replace("34.0705", lat), 3, lon)
    zip_path = _zip_from_files(tmp_path, files, "edges.zip")
    index = StaticIndex.build(zip_path, ":memory:", DATASET, TZ)
    stored = next(
        r for r in index._conn.execute("SELECT * FROM stops") if r[0] == "ST1"
    )
    assert (stored[2], stored[3]) == (float(lat), float(lon))
    index.close()


_LIMIT_OFFSETS = st.integers(1, 40).map(lambda places: Decimal(10) ** -places)


@given(
    limit=st.sampled_from([90, 180]),
    side=st.sampled_from([-1, 1]),
    offset=st.one_of(_LIMIT_OFFSETS, _LIMIT_OFFSETS.map(lambda d: -d), st.just(0)),
    spelling=st.sampled_from(["plain", "exponent"]),
)
def test_a_coordinate_is_in_range_exactly_as_written(
    limit: int, side: int, offset: Decimal, spelling: str
) -> None:
    """At any distance from either limit, however small: refused iff the
    text's own value is past it -- even where its double is the limit."""
    value = side * (limit + offset)
    text = f"{value:f}" if spelling == "plain" else f"{value:E}"
    inside = abs(Fraction(text)) <= limit  # an exact oracle, not Decimal
    if inside:
        assert _coordinate(text, "stop_lat", limit) == float(text)
    else:
        with pytest.raises(ValueError, match="outside"):
            _coordinate(text, "stop_lat", limit)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("90.000000000000001", Decimal("90.000000000000001")),
        (" -.5e1\t", Decimal(-5)),
        # Exponents past what Decimal holds: infinitely far, or zero.
        ("9e9999999999999999999999", Decimal("Infinity")),
        ("-9e9999999999999999999999", Decimal("-Infinity")),
        ("9e-9999999999999999999999", Decimal(0)),
        ("0.0e9999999999999999999999", Decimal(0)),
        ("1_0", None),
        ("nan", None),
    ],
)
def test_exact_number_reads_the_value_as_written(
    text: str, expected: Decimal | None
) -> None:
    assert exact_number(text) == expected


# -- A row shorter than its header is malformed, in every file --------------------


def _short_rows() -> list[Any]:
    """Every fixture data row cut to each shorter non-empty width."""
    cases = []
    for member in sorted(_FILES):
        header, *rows = _FILES[member].splitlines()
        width = len(header.split(","))
        for index, row in enumerate(rows):
            for keep in range(1, width):
                short = ",".join(row.split(",")[:keep])
                if short:  # an empty line is no record: csv skips it
                    cases.append(
                        pytest.param(
                            member, index, short, id=f"{member}-{index}-{keep}"
                        )
                    )
    return cases


def _replace_row(member: str, index: int, row: str) -> dict[str, str]:
    header, *rows = _FILES[member].splitlines()
    files = dict(_FILES)
    files[member] = "\n".join([header, *rows[:index], row, *rows[index + 1 :]]) + "\n"
    return files


@pytest.mark.parametrize(("member", "index", "short"), _short_rows())
def test_any_short_row_fails_the_build(
    tmp_path: Path, member: str, index: int, short: str
) -> None:
    """RFC 4180: each record carries the header's field count. A short one
    is malformed in every file (the maintainer's choice over reading its
    missing trailing cells as blank) -- before, trips.txt raised a bare
    sqlite3.IntegrityError, stops.txt FeedParseError, and agency.txt loaded.
    """
    zip_path = _zip_from_files(
        tmp_path, _replace_row(member, index, short), "short.zip"
    )
    with pytest.raises(FeedParseError, match="fewer fields than the header"):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def _member_rows() -> list[Any]:
    return [
        pytest.param(member, index, id=f"{member}-{index}")
        for member in sorted(_FILES)
        for index in range(len(_FILES[member].splitlines()) - 1)
    ]


@pytest.mark.parametrize(("member", "index"), _member_rows())
def test_a_long_row_with_content_past_the_header_fails_the_build(
    tmp_path: Path, member: str, index: int
) -> None:
    """A cell past the header has no column; an unquoted comma inside a value
    is what usually puts one there, moving every later cell (the maintainer's
    choice: refused when it holds anything)."""
    row = _FILES[member].splitlines()[index + 1]
    zip_path = _zip_from_files(
        tmp_path, _replace_row(member, index, row + ",x"), "long.zip"
    )
    with pytest.raises(FeedParseError, match="more fields than the header"):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


@pytest.mark.parametrize(("member", "index"), _member_rows())
def test_blank_cells_past_the_header_are_dropped(
    tmp_path: Path, member: str, index: int
) -> None:
    """``a,b,c,,``: trailing commas carry nothing, so nothing is misplaced and
    the index is exactly the one without them."""
    row = _FILES[member].splitlines()[index + 1]
    padded = _zip_from_files(
        tmp_path, _replace_row(member, index, row + ", ,\t"), "pad.zip"
    )
    plain = _zip_from_files(tmp_path, dict(_FILES), "plain.zip")
    assert _dump(StaticIndex.build(padded, ":memory:", DATASET, TZ)) == _dump(
        StaticIndex.build(plain, ":memory:", DATASET, TZ)
    )


def test_an_unquoted_comma_never_moves_a_cell(tmp_path: Path) -> None:
    """The review's case: ``Downtown, via Main`` would have loaded
    trip_short_name ' via Main' and dropped the 10."""
    files = dict(_FILES)
    files["trips.txt"] = (
        "trip_id,route_id,service_id,trip_headsign,trip_short_name\n"
        "T1,R1,WKDY,Downtown, via Main,10\n"
    )
    zip_path = _zip_from_files(tmp_path, files, "comma.zip")
    with pytest.raises(FeedParseError, match=r"Malformed trips\.txt: line 2 has more"):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def test_a_malformed_extra_feed_info_row_fails_the_build(tmp_path: Path) -> None:
    """feed_info.txt keeps its first record, but every row is still read: a
    short second one is as malformed as anywhere else."""
    files = dict(_FILES)
    files["feed_info.txt"] = (
        "feed_publisher_name,feed_publisher_url\nPub,https://p.example\nX\n"
    )
    zip_path = _zip_from_files(tmp_path, files, "feed_info.zip")
    with pytest.raises(FeedParseError, match=r"Malformed feed_info\.txt: line 3"):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def test_a_short_trips_row_fails_the_build(tmp_path: Path) -> None:
    """The report's case: trips.txt's NOT NULL service_id met csv's None."""
    header, first, *rest = _FILES["trips.txt"].splitlines()
    files = dict(_FILES)
    files["trips.txt"] = "\n".join([header, first.split(",")[0], *rest]) + "\n"
    zip_path = _zip_from_files(tmp_path, files, "short_trip.zip")
    with pytest.raises(FeedParseError, match=r"Malformed trips\.txt: line 2 has fewer"):
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def test_a_full_row_with_blank_cells_still_loads(tmp_path: Path) -> None:
    """Only the field count matters: a row carrying every field, its optional
    ones blank, is well formed."""
    header, first, *rest = _FILES["agency.txt"].splitlines()
    width = len(header.split(","))
    cells = first.split(",") + [""] * (width - len(first.split(",")))
    files = dict(_FILES)
    files["agency.txt"] = "\n".join([header, ",".join(cells), *rest]) + "\n"
    zip_path = _zip_from_files(tmp_path, files, "blank_cells.zip")
    StaticIndex.build(zip_path, ":memory:", DATASET, TZ).close()


def _dump(index: StaticIndex) -> dict[str, list[tuple[object, ...]]]:
    """Every stored row of every table but meta: what any query can read."""
    try:
        tables = [
            name
            for (name,) in index._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name != 'meta'"
            )
        ]
        return {
            table: sorted(
                index._conn.execute(f"SELECT * FROM {table}").fetchall(), key=repr
            )
            for table in tables
        }
    finally:
        index.close()


def test_an_unreadable_zip_names_the_problem_in_the_librarys_words(
    tmp_path: Path,
) -> None:
    """zipfile's own text isn't passed on, nor the error itself."""
    bad = tmp_path / "feed.zip"
    bad.write_bytes(b"PK\x03\x04 not really a zip")
    with pytest.raises(FeedParseError) as caught:
        StaticIndex.build(bad, ":memory:", DATASET, TZ)
    assert str(caught.value) == "Unreadable GTFS zip: BadZipFile"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.parametrize(
    ("filename", "content", "reason"),
    [
        ("stops.txt", b"\xff\xfeSECRETBYTES", "UnicodeDecodeError"),
        (
            "stops.txt",
            b"stop_id,stop_name\nS1\n",
            "line 2 has fewer fields than the header",
        ),
        # Read first, for the timezone, by its own path.
        ("agency.txt", b"\xff\xfeSECRETBYTES", "UnicodeDecodeError"),
    ],
)
def test_a_malformed_file_carries_nothing_of_the_error_behind_it(
    tmp_path: Path, filename: str, content: bytes, reason: str
) -> None:
    """A codec error holds the file's bytes: the FeedParseError has nothing
    chained, and no frame on its traceback keeps the original (as a context
    manager's exit would, the conversion happening there)."""
    files: dict[str, str | bytes] = {**_FILES, filename: content}
    zip_path = _zip_from_files(tmp_path, files, "malformed.zip")
    with pytest.raises(FeedParseError) as caught:
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ)
    assert str(caught.value) == f"Malformed {filename}: {reason}"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    # Nor on the traceback: no frame below this test's (contextlib's exit,
    # say) keeps the original or the bytes as a local -- error reporters
    # capture frame locals.
    for frame, _line in traceback.walk_tb(caught.value.__traceback__):
        if frame.f_code.co_filename == __file__:
            continue
        for name, value in frame.f_locals.items():
            shown = repr(value)
            assert "SECRETBYTES" not in shown, (frame.f_code.co_name, name)
            assert not isinstance(value, UnicodeError), (frame.f_code.co_name, name)


# -- A corrupt archive is FeedParseError, whatever part of it is damaged ----------


_CENTRAL = b"PK\x01\x02"
_LOCAL = b"PK\x03\x04"
_END = b"PK\x05\x06"


def _archive(method: int = zipfile.ZIP_STORED) -> bytearray:
    """The fixture feed as zip bytes, every member compressed by ``method``."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", method) as zf:
        for member, content in _FILES.items():
            zf.writestr(member, content)
    return bytearray(buf.getvalue())


def _member_data(data: bytearray, member: str) -> int:
    """Where ``member``'s compressed bytes start."""
    start = data.index(_LOCAL)  # the first local header
    while True:
        name_length, extra_length = struct.unpack_from("<HH", data, start + 26)
        name = bytes(data[start + 30 : start + 30 + name_length])
        if name == member.encode():
            return start + 30 + name_length + extra_length
        start = data.index(_LOCAL, start + 4)


def _build_bytes(tmp_path: Path, data: bytes | bytearray) -> StaticIndex:
    zip_path = tmp_path / "corrupt.zip"
    zip_path.write_bytes(bytes(data))
    return StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def _assert_refused(
    tmp_path: Path, data: bytes | bytearray, message: str | None = None
) -> None:
    """The build fails with FeedParseError alone: nothing chained, and no
    frame on the traceback keeps another error as a local."""
    with pytest.raises(FeedParseError) as caught:
        _build_bytes(tmp_path, data).close()
    if message is not None:
        assert str(caught.value) == message
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    for frame, _line in traceback.walk_tb(caught.value.__traceback__):
        if frame.f_code.co_filename == __file__:
            continue
        for name, value in frame.f_locals.items():
            assert not isinstance(value, BaseException) or isinstance(
                value, FeedParseError
            ), (frame.f_code.co_name, name)


def test_a_name_flagged_utf8_that_is_not_is_an_unreadable_zip(
    tmp_path: Path,
) -> None:
    """ZipFile() decodes a name whose UTF-8 flag is set as UTF-8; bytes that
    are not were a bare UnicodeDecodeError."""
    data = _archive()
    central = data.index(_CENTRAL)
    flags = struct.unpack_from("<H", data, central + 8)[0]
    struct.pack_into("<H", data, central + 8, flags | 0x800)
    data[central + 46] = 0xFF  # the first byte of its name
    _assert_refused(tmp_path, data, "Unreadable GTFS zip: UnicodeDecodeError")


def test_a_central_directory_offset_before_the_file_is_an_unreadable_member(
    tmp_path: Path,
) -> None:
    """The end record placing the central directory further in than it is
    shifts every member's header to before the file's start: seeking there
    was a bare OSError (errno 22) from archive.open."""
    data = _archive()
    end = data.rindex(_END)
    offset = struct.unpack_from("<I", data, end + 16)[0]
    struct.pack_into("<I", data, end + 16, offset + 10_000)
    _assert_refused(tmp_path, data, "Unreadable agency.txt: OSError")


# Each method's stream, and where its data starts: zipfile's LZMA member
# opens with a 4-byte header and 5 bytes of properties.
_METHOD_ERRORS = [
    pytest.param(zipfile.ZIP_DEFLATED, 0, "error", id="deflate"),
    pytest.param(zipfile.ZIP_BZIP2, 0, "OSError", id="bzip2"),
    pytest.param(zipfile.ZIP_LZMA, 9, "LZMAError", id="lzma"),
]
if hasattr(zipfile, "ZIP_ZSTANDARD"):  # Python 3.14+
    _METHOD_ERRORS.append(
        pytest.param(zipfile.ZIP_ZSTANDARD, 0, "ZstdError", id="zstandard")
    )


@pytest.mark.parametrize(("method", "skip", "reason"), _METHOD_ERRORS)
def test_bad_compressed_data_is_a_malformed_member(
    tmp_path: Path, method: int, skip: int, reason: str
) -> None:
    """Each method raises its own error for data it can't decompress --
    bzip2 a bare OSError, Zstandard a ZstdError -- and each is the member's
    FeedParseError, named by type."""
    try:
        data = _archive(method)
    except RuntimeError:  # zstd: this Python was built without the module
        pytest.skip("compression method unavailable")
    start = _member_data(data, "stops.txt") + skip
    data[start : start + 8] = b"\xff" * 8  # no stream starts like this
    _assert_refused(tmp_path, data, f"Malformed stops.txt: {reason}")


_METHODS = [
    zipfile.ZIP_STORED,
    zipfile.ZIP_DEFLATED,
    zipfile.ZIP_BZIP2,
    zipfile.ZIP_LZMA,
    *([zipfile.ZIP_ZSTANDARD] if hasattr(zipfile, "ZIP_ZSTANDARD") else []),
]


@given(
    method=st.sampled_from(_METHODS),
    edits=st.lists(
        st.tuples(st.floats(0, 1, exclude_max=True), st.integers(0, 255)),
        min_size=1,
        max_size=8,
    ),
    cut=st.one_of(st.none(), st.floats(0, 1, exclude_max=True)),
)
@settings(max_examples=300, deadline=None)
def test_a_damaged_archive_builds_or_fails_as_feed_parse_error(
    tmp_path_factory: pytest.TempPathFactory,
    method: int,
    edits: list[tuple[float, int]],
    cut: float | None,
) -> None:
    """Any bytes changed anywhere, maybe cut short, under any method: the
    build succeeds or raises FeedParseError with nothing chained -- never
    zipfile's or a decompressor's own error."""
    try:
        data = _archive(method)
    except RuntimeError:  # zstd unavailable
        return
    for where, value in edits:
        data[int(where * len(data))] = value
    if cut is not None:
        data = data[: int(cut * len(data))]
    tmp_path = tmp_path_factory.mktemp("damaged")
    try:
        _build_bytes(tmp_path, data).close()
    except FeedParseError:
        refused = True
    else:
        refused = False
    if refused:  # checked again outside the except: it would chain to this one
        _assert_refused(tmp_path, data)


# -- stop_sequence MUST increase along a trip: a repeat fails the build -----------


_CALLS_HEADER = (
    "trip_id,arrival_time,departure_time,stop_id,location_id,"
    "start_pickup_drop_off_window,end_pickup_drop_off_window,stop_sequence\n"
)


@pytest.mark.parametrize(
    ("rows", "trip", "sequence"),
    [
        pytest.param(
            "T1,08:00:00,08:00:00,S1,,,,1\nT1,08:10:00,08:10:00,S2,,,,1\n",
            "'T1'",
            1,
            id="two fixed calls",
        ),
        pytest.param(
            "T1,08:00:00,08:00:00,S1,,,,1\nT1,08:10:00,08:10:00,S2,,,,01\n",
            "'T1'",
            1,
            id="one value, spelled two ways",
        ),
        pytest.param(
            "T1,08:00:00,08:00:00,S1,,,,1\nT1,,,,Z,08:05:00,08:25:00,1\n",
            "'T1'",
            1,
            id="a zone call and a fixed call",
        ),
        pytest.param(
            "T1,,,,Z,08:05:00,08:25:00,4\nT1,,,S1,,09:00:00,09:30:00,4\n",
            "'T1'",
            4,
            id="two on-demand calls",
        ),
        pytest.param(
            'T1,08:00:00,08:00:00,S1,,,,1\n"T\'1",,,S1,,,,2\n"T\'1",,,S2,,,,2\n',
            '"T\'1"',
            2,
            id="the trip id quoted as written",
        ),
    ],
)
def test_a_repeated_stop_sequence_fails_the_build(
    tmp_path: Path, rows: str, trip: str, sequence: int
) -> None:
    """GTFS: stop_sequence "values must increase along the trip" -- a repeat
    leaves the trip's call order unstated, whatever kind of call each is."""
    files = {**_FILES, "stop_times.txt": _CALLS_HEADER + rows}
    zip_path = _zip_from_files(tmp_path, files, "repeat.zip")
    _assert_refused(
        tmp_path,
        zip_path.read_bytes(),
        f"Malformed stop_times.txt: trip {trip} repeats stop_sequence {sequence}: "
        "it must increase along the trip",
    )


def test_a_trip_named_like_a_repetition_fails_the_build(tmp_path: Path) -> None:
    """A real trip whose id is a frequency repetition's (``F#21600``) would
    share that id's calls with the repetition: refused, not merged."""
    files = {
        **_FILES,
        "trips.txt": _FILES["trips.txt"] + "R1,WKDY,F,A\nR1,WKDY,F#21600,B\n",
        "stop_times.txt": _FILES["stop_times.txt"]
        + "F,06:00:00,06:00:00,S1,1\nF,06:10:00,06:10:00,S2,2\n"
        + "F#21600,09:00:00,09:00:00,S1,1\nF#21600,09:10:00,09:10:00,S2,2\n",
        "frequencies.txt": "trip_id,start_time,end_time,headway_secs\n"
        "F,06:00:00,07:00:00,1800\n",
    }
    zip_path = _zip_from_files(tmp_path, files, "collide.zip")
    _assert_refused(
        tmp_path,
        zip_path.read_bytes(),
        "Malformed frequencies.txt: trip 'F#21600' has the id of a repetition "
        "of frequency trip 'F' (starting at 21600s)",
    )


@given(
    calls=st.lists(
        st.tuples(
            st.sampled_from(["T1", "T2", "T9"]),
            st.integers(0, 6),
            st.sampled_from(["fixed", "zone", "window"]),
        ),
        min_size=1,
        max_size=10,
    )
)
@settings(max_examples=150, deadline=None)
def test_a_build_fails_exactly_when_a_trip_repeats_a_stop_sequence(
    tmp_path_factory: pytest.TempPathFactory,
    calls: list[tuple[str, int, str]],
) -> None:
    """For any calls of any kinds: the build succeeds iff no trip names one
    stop_sequence twice, and a failure names a repeated pair."""
    cells = {
        "fixed": "08:00:00,08:00:00,S1,,,",
        "zone": ",,,Z,08:00:00,09:00:00",
        "window": ",,S1,,08:00:00,09:00:00",
    }
    rows = "".join(
        f"{trip},{cells[kind]},{sequence}\n" for trip, sequence, kind in calls
    )
    files = {**_FILES, "stop_times.txt": _CALLS_HEADER + rows}
    tmp_path = tmp_path_factory.mktemp("sequences")
    zip_path = _zip_from_files(tmp_path, files, "calls.zip")
    keys = [(trip, sequence) for trip, sequence, _kind in calls]
    repeated = {key for key in keys if keys.count(key) > 1}
    try:
        StaticIndex.build(zip_path, ":memory:", DATASET, TZ).close()
    except FeedParseError as err:
        message = str(err)
    else:
        message = None
    if not repeated:
        assert message is None
    else:
        assert message is not None
        assert any(
            message == f"Malformed stop_times.txt: trip {trip!r} repeats stop_sequence "
            f"{sequence}: it must increase along the trip"
            for trip, sequence in repeated
        )


# -- A damaged cached index: a miss when opened, FeedParseError when met later ----


def damage_page(db_path: Path, name: str, fill: bytes = b"\xff") -> None:
    """Overwrite the root page of table or index ``name`` in a closed file."""
    conn = sqlite3.connect(db_path)
    try:
        [(root,)] = conn.execute(
            "SELECT rootpage FROM sqlite_master WHERE name = ?", (name,)
        ).fetchall()
        [(size,)] = conn.execute("PRAGMA page_size").fetchall()
    finally:
        conn.close()
    with db_path.open("r+b") as fp:
        fp.seek((root - 1) * size)
        fp.write(fill * size)


def _built_file(tmp_path: Path) -> Path:
    db_path = tmp_path / "static.db"
    StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ).close()
    return db_path


def _unchecked(db_path: Path) -> StaticIndex:
    """The file as an index, skipping open_cached's check: damage met later."""
    return StaticIndex(sqlite3.connect(db_path), DATASET, TZ)


_NOW_UTC = datetime(2026, 6, 10, 14, tzinfo=UTC)
_DAMAGE_MESSAGE = (
    f"The static index of dataset {DATASET!r} is damaged: SQLite reports a "
    "malformed database; purge the cache to rebuild it"
)


def _damaged_reads() -> dict[str, Callable[[StaticIndex], object]]:
    return {
        "stops": lambda index: index.stops(),
        "routes": lambda index: index.routes(),
        "trips": lambda index: index.trip_summaries(["T1"]),
        "stop_times": lambda index: index.upcoming_departures(
            ["S1"], None, _NOW_UTC, timedelta(hours=4)
        ),
        "ix_stop_times_stop_departure": lambda index: index.upcoming_departures(
            ["S1"], None, _NOW_UTC, timedelta(hours=4)
        ),
    }


@pytest.mark.parametrize("name", sorted(_damaged_reads()))
def test_a_damaged_page_is_a_cache_miss(tmp_path: Path, name: str) -> None:
    """Damage anywhere -- stops and routes the handle reads at once, a table
    or index a later query meets -- and open_cached misses, so the entry is
    rebuilt (it opened, and every open or query raised DatabaseError)."""
    db_path = _built_file(tmp_path)
    damage_page(db_path, name)
    assert StaticIndex.open_cached(db_path, DATASET) is None


@pytest.mark.parametrize("name", sorted(_damaged_reads()))
def test_damage_met_by_a_query_is_feed_parse_error(tmp_path: Path, name: str) -> None:
    """Damage the check never saw (the file changed after it was opened) is
    the index's FeedParseError, in the library's words with nothing chained
    -- not sqlite3.DatabaseError."""
    db_path = _built_file(tmp_path)
    damage_page(db_path, name)
    index = _unchecked(db_path)
    try:
        with pytest.raises(FeedParseError) as caught:
            _damaged_reads()[name](index)
    finally:
        index.close()
    assert str(caught.value) == _DAMAGE_MESSAGE
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    for frame, _line in traceback.walk_tb(caught.value.__traceback__):
        for local in frame.f_locals.values():
            assert not isinstance(local, sqlite3.Error)


def _rename_in_schema(db_path: Path) -> None:
    """Rewrite stops' schema text so it still parses but names another column."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA writable_schema = ON")
        conn.execute(
            "UPDATE sqlite_master SET sql = replace(sql, 'platform_code', "
            "'platform_kode') WHERE name = 'stops'"
        )
        conn.commit()
    finally:
        conn.close()


def _damage_header(db_path: Path) -> None:
    """Write past what this SQLite reads into the header's format fields."""
    with db_path.open("r+b") as fp:
        fp.seek(31)
        fp.write(b"\x05" * 17)


@pytest.mark.parametrize("damage", [_rename_in_schema, _damage_header])
def test_damage_sqlite_has_no_code_for(
    tmp_path: Path, damage: Callable[[Path], None]
) -> None:
    """Schema text that parses but names other columns, or a header this
    SQLite can't read, raise SQLITE_ERROR as a mistaken statement would:
    the file no longer checks out, so it is a miss when opened and the
    index's FeedParseError when met by a query."""
    db_path = _built_file(tmp_path)
    damage(db_path)
    assert StaticIndex.open_cached(db_path, DATASET) is None
    index = _unchecked(db_path)
    try:
        with pytest.raises(FeedParseError, match="is damaged"):
            index.stops()
    finally:
        index.close()


def _undecodable_name(db_path: Path) -> None:
    """Store bytes that are no UTF-8 as a stop's name: pages stay well formed."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("UPDATE stops SET name = CAST(X'FF41' AS TEXT)")
        conn.commit()
    finally:
        conn.close()


def _undecodable_schema_error(db_path: Path) -> None:
    """Damage stops' schema row so SQLite's own message quotes non-UTF-8 bytes."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA writable_schema = ON")
        conn.execute(
            "UPDATE sqlite_master SET name = CAST(X'FF' AS TEXT), "
            "sql = sql || ' nonsense' WHERE name = 'stops'"
        )
        conn.commit()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "damage",
    [
        # sqlite3's own OperationalError, with no SQLite code: quick_check
        # can't see it, so only the read meets it.
        _undecodable_name,
        # A UnicodeDecodeError: SQLite's message can't become a str.
        _undecodable_schema_error,
    ],
)
def test_text_that_no_longer_decodes_is_damage(
    tmp_path: Path, damage: Callable[[Path], None]
) -> None:
    db_path = _built_file(tmp_path)
    damage(db_path)
    index = _unchecked(db_path)
    try:
        with pytest.raises(FeedParseError) as caught:
            index.stops()
    finally:
        index.close()
    assert str(caught.value) == _DAMAGE_MESSAGE
    assert caught.value.__context__ is None


def test_a_damaged_length_that_runs_out_of_memory_is_damage(tmp_path: Path) -> None:
    """A routes index cell whose damaged length asks SQLite for more memory
    than there is (SQLITE_NOMEM, sqlite3's bare MemoryError): the file no
    longer checks out, so it is a miss when opened and the index's
    FeedParseError when met by a query."""
    db_path = _built_file(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        [(root,)] = conn.execute(
            "SELECT rootpage FROM sqlite_master WHERE name = ?",
            ("sqlite_autoindex_routes_1",),
        ).fetchall()
    finally:
        conn.close()
    with db_path.open("r+b") as fp:
        fp.seek((root - 1) * 4096 + 498)
        fp.write((b"\x88\x80" * 1796)[: 4096 - 498])
    assert StaticIndex.open_cached(db_path, DATASET) is None
    index = _unchecked(db_path)
    try:
        with pytest.raises(FeedParseError, match="is damaged"):
            index.routes()
    finally:
        index.close()


def test_an_undecodable_schema_is_a_cache_miss(tmp_path: Path) -> None:
    """Opening a file whose schema SQLite can only describe in bytes that
    are no UTF-8 raises UnicodeDecodeError at the first statement: a miss,
    not an error that would fail the feed on every open."""
    db_path = _built_file(tmp_path)
    _undecodable_schema_error(db_path)
    assert StaticIndex.open_cached(db_path, DATASET) is None


def _flex_file(tmp_path: Path) -> Path:
    zip_path = tmp_path / "flex.zip"
    zip_path.write_bytes(build_flex_gtfs_zip_bytes())
    db_path = tmp_path / "flex.db"
    StaticIndex.build(zip_path, str(db_path), DATASET, None).close()
    return db_path


_FLEX_DAY = date(2026, 6, 10)  # a Wednesday: WEEKDAYS runs
_FLEX_MORNING = datetime(2026, 6, 10, 12, tzinfo=UTC)

_STORED_DAMAGE = {
    "polygons no longer JSON": (
        "UPDATE locations SET polygons = '[[[[41.0, -87.0'",
        lambda index: index.flex_locations(),
    ),
    "polygons of another shape": (
        "UPDATE locations SET polygons = '[[[[41.0]]]]'",
        lambda index: index.flex_locations(),
    ),
    # Decodes, but no ring the build kept: a ring has four or more positions.
    # A number of the wrong storage class, in pages that stay well formed:
    # it raised TypeError where a read computed with it, or read as None.
    "stop_times seconds turned into text": (
        "UPDATE stop_times SET arrival_secs = 'x'",
        lambda index: index.upcoming_departures(
            ["S1"], None, _FLEX_MORNING, timedelta(hours=4)
        ),
    ),
    "stop_times seconds turned into bytes": (
        "UPDATE stop_times SET departure_secs = X'01'",
        lambda index: index.instance_trip_calls([("DEV1", _FLEX_DAY)]),
    ),
    "a pickup type turned into text": (
        "UPDATE stop_times SET pickup_type = 'x'",
        lambda index: index.instance_trip_calls([("DEV1", _FLEX_DAY)]),
    ),
    "a trip's first departure turned into bytes": (
        "UPDATE trips SET first_secs = X'01'",
        lambda index: index.plain_trip_starts(["DEV1"]),
    ),
    "a repetition's slot turned into text": (
        "UPDATE trips SET start_secs = 28800, slot_secs = 'x' WHERE id = 'DEV1'",
        lambda index: index.slot_runs([("DEV1", _FLEX_DAY, 28800)]),
    ),
    "flex window seconds turned into text": (
        "UPDATE flex_stop_times SET start_window_secs = 'x'",
        lambda index: index.on_demand_rows(["TOWN"], [], []),
    ),
    "a stop latitude turned into text": (
        "UPDATE stops SET lat = 'north'",
        lambda index: index.stops(),
    ),
    "a ring too short": (
        "UPDATE locations SET polygons = '[[[[41.0, -87.0], [41.0, -87.0]]]]'",
        lambda index: index.flex_locations(),
    ),
    "polygons turned into a number": (
        "UPDATE locations SET polygons = 7",
        lambda index: index.flex_locations(),
    ),
    "a calendar date no longer canonical": (
        "UPDATE calendar SET end_date = '2027-12-31'",
        lambda index: index.departures_ceiling(["H1"]),
    ),
    # A BLOB: TEXT affinity would store a number as its text.
    "a calendar date turned into bytes": (
        "UPDATE calendar SET end_date = X'3230323731323331'",
        lambda index: index.departures_ceiling(["S1"]),
    ),
    "an exception date no longer canonical": (
        "INSERT INTO calendar_dates VALUES ('WEEKDAYS', '2026061', 1)",
        lambda index: index.departures_ceiling(["H1"]),
    ),
    "a feed_info date turned into bytes": (
        "INSERT INTO feed_info (publisher_name, start_date) "
        "VALUES ('P', X'3230323630313031')",
        lambda index: index.feed_info(),
    ),
    "booking seconds turned into text": (
        "UPDATE booking_rules SET prior_notice_last_secs = '17:00'",
        lambda index: index.booking_rules(),
    ),
    "unreadable names turned into bytes": (
        "UPDATE booking_rules SET unreadable = X'05'",
        lambda index: index.booking_rules(),
    ),
}


@pytest.mark.parametrize("case", sorted(_STORED_DAMAGE))
def test_a_damaged_stored_value_is_feed_parse_error(tmp_path: Path, case: str) -> None:
    """quick_check reads structure, not cells: a value the build could not
    have stored -- in pages that stay well formed -- is the index's
    FeedParseError where a read decodes it, with nothing chained (it was a
    raw JSONDecodeError, TypeError or AttributeError)."""
    sql, read = _STORED_DAMAGE[case]
    db_path = _flex_file(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(sql)
        conn.commit()
    finally:
        conn.close()
    index = StaticIndex.open_cached(db_path, DATASET)
    assert index is not None  # well formed: the check can't see it
    try:
        with pytest.raises(FeedParseError) as caught:
            read(index)
    finally:
        index.close()
    assert str(caught.value) == _DAMAGE_MESSAGE
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_a_build_that_cannot_be_reopened_chains_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reopen after a file build failing is FeedParseError in the
    library's words, with nothing chained."""
    real_connect = sqlite3.connect

    def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        if kwargs.get("uri"):
            raise sqlite3.OperationalError("unable to open database file")
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect)
    db_path = tmp_path / "static.db"
    with pytest.raises(FeedParseError) as caught:
        StaticIndex.build(_write_zip(tmp_path), str(db_path), DATASET, TZ)
    assert str(caught.value) == f"{db_path} could not be reopened after its build"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_running_out_of_memory_reopening_a_build_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On the file a build just wrote, MemoryError is the machine's: it
    propagates rather than reading as "not a built static index"."""
    real_connect = sqlite3.connect

    def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        if kwargs.get("uri"):
            raise MemoryError
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect)
    with pytest.raises(MemoryError):
        StaticIndex.build(
            _write_zip(tmp_path), str(tmp_path / "static.db"), DATASET, TZ
        )


def test_a_mistaken_statement_on_an_intact_index_is_not_damage(
    tmp_path: Path,
) -> None:
    """SQLITE_ERROR from a file that checks out is the statement's own:
    passed on as SQLite raised it, never blamed on the index."""
    index = _unchecked(_built_file(tmp_path))
    try:
        with pytest.raises(sqlite3.OperationalError, match="no such column"):
            index._query("SELECT no_such_column FROM stops")
    finally:
        index.close()


def test_a_closed_index_still_raises_programming_error(tmp_path: Path) -> None:
    """Querying a closed index is the caller's misuse: it stays sqlite3's
    ProgrammingError, not a damaged index."""
    index = _unchecked(_built_file(tmp_path))
    index.close()
    with pytest.raises(sqlite3.ProgrammingError):
        index.stops()


@given(
    page=st.integers(1, 40),
    fill=st.binary(min_size=1, max_size=4),
    at=st.integers(0, 4095),
    length=st.integers(1, 4096),
)
# Non-UTF-8 bytes in schema text: SQLite's error text no longer decodes, so
# open_cached's meta read raised a bare UnicodeDecodeError (review, PR #84).
@example(page=21, fill=b"\x81", at=38, length=1779)
@settings(max_examples=150, deadline=None)
def test_any_damage_opens_as_a_miss_or_reads_as_feed_parse_error(
    tmp_path_factory: pytest.TempPathFactory,
    page: int,
    fill: bytes,
    at: int,
    length: int,
) -> None:
    """Bytes overwritten anywhere in any page: open_cached misses or opens
    an index; either way every query answers or raises the damaged index's
    FeedParseError (a cell the check can't judge is met by the read that
    decodes it) -- never sqlite3.DatabaseError or a decoding error."""
    tmp_path = tmp_path_factory.mktemp("damaged-db")
    db_path = _built_file(tmp_path)
    with db_path.open("r+b") as fp:
        size = 4096
        if (page - 1) * size >= db_path.stat().st_size:
            return
        fp.seek((page - 1) * size + at)
        fp.write((fill * length)[: size - at])
    opened = StaticIndex.open_cached(db_path, DATASET)
    for index in (opened, _unchecked(db_path)):
        if index is None:
            continue
        try:
            for read in _damaged_reads().values():
                try:
                    read(index)
                except FeedParseError as err:
                    assert str(err) == _DAMAGE_MESSAGE
        finally:
            index.close()
