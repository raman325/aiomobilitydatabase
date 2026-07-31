"""Tests for SQLite index build and cache-open."""

import io
import zipfile
from pathlib import Path

import pytest

from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.static_index import SCHEMA_VERSION, StaticIndex

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
    assert stops["ST1"].location_type == 1
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
