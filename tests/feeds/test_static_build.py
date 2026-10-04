"""Tests for SQLite index build and cache-open."""

import io
import zipfile
from pathlib import Path

import pytest

from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.models import StopLocationType
from aiomobilitydatabase.feeds.static_index import (
    SCHEMA_VERSION,
    StaticIndex,
    _lenient_int,
    parse_gtfs_time,
)

from tests.feeds.fixtures import _FILES, build_gtfs_zip_bytes

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
    with pytest.raises(FeedParseError, match="No agency timezone"):
        StaticIndex.build(zip_path, ":memory:", DATASET, None)


def test_ragged_stops_row_raises_feed_parse_error(tmp_path: Path) -> None:
    """A stops.txt data row with fewer columns than the header (a ragged row,
    which real producers ship) must surface as FeedParseError, not the raw
    AttributeError from csv.DictReader's restval=None fill-in.
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
    assert not Path(f"{db_path}.building").exists()


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
    assert not Path(f"{db_target}.building").exists()


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


def _zip_from_files(tmp_path: Path, files: dict[str, str], name: str) -> Path:
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
