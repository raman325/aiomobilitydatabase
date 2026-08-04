"""Tests for frequencies.txt materialization in the static index."""

import io
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.static_index import StaticIndex

from tests.feeds.fixtures import build_frequencies_gtfs_zip_bytes

DATASET = "mdb-100-202607310000"
TZ = "America/Los_Angeles"

# Thursday 2026-07-30 05:45 PDT == 12:45 UTC; that service day's anchor is
# 07:00 UTC. F1's repetitions start at 06:00/06:10/06:20 (headway row) and
# 07:00/07:10 (exact_times=1 row), all PDT.
NOW = datetime(2026, 7, 30, 12, 45, tzinfo=UTC)

# The five F1 repetitions, in departure order, with their S1 departures.
F1_EXPECTED = [
    ("F1#21600", datetime(2026, 7, 30, 13, 0, tzinfo=UTC)),
    ("F1#22200", datetime(2026, 7, 30, 13, 10, tzinfo=UTC)),
    ("F1#22800", datetime(2026, 7, 30, 13, 20, tzinfo=UTC)),
    ("F1#25200", datetime(2026, 7, 30, 14, 0, tzinfo=UTC)),
    ("F1#25800", datetime(2026, 7, 30, 14, 10, tzinfo=UTC)),
]


def _index(tmp_path: Path, extra_frequency_rows: str = "") -> StaticIndex:
    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(build_frequencies_gtfs_zip_bytes(extra_frequency_rows))
    return StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def test_repetitions_materialized_with_pinned_departures(tmp_path: Path) -> None:
    """Both frequency rows (headway-based AND exact_times=1) materialize the
    exact arithmetic-progression departures; the repetition landing exactly
    on end_time (06:30 == F1#23400) is excluded by the strict-< rule.
    """
    index = _index(tmp_path)
    departures = index.upcoming_departures(
        ["S1"], None, NOW, timedelta(hours=2), per_stop_limit=10
    )
    assert [(dep.trip_id, dep.departure) for dep in departures] == F1_EXPECTED
    first = departures[0]
    assert first.route_id == "R1"
    assert first.headsign == "Loop"
    assert first.arrival == datetime(2026, 7, 30, 13, 0, tzinfo=UTC)
    index.close()


def test_zero_repetition_window_produces_nothing(tmp_path: Path) -> None:
    """A frequency row whose end_time equals its start_time yields zero
    repetitions (strict-< semantics, degenerate window) without failing the
    build or disturbing the other rows' repetitions.
    """
    index = _index(tmp_path, extra_frequency_rows="F1,09:00:00,09:00:00,600,\n")
    departures = index.upcoming_departures(
        ["S1"], None, NOW, timedelta(hours=6), per_stop_limit=20
    )
    assert "F1#32400" not in [dep.trip_id for dep in departures]
    assert [(dep.trip_id, dep.departure) for dep in departures[:5]] == F1_EXPECTED
    index.close()


def test_offsets_preserved_for_later_stops(tmp_path: Path) -> None:
    """Every repetition preserves the template's elapsed offsets: the S2 call
    keeps its +10:00 arrival / +10:30 departure and the terminal S3 call its
    +20:00, relative to each repetition's start.
    """
    index = _index(tmp_path)
    departures = index.upcoming_departures(
        ["S2", "S3"], None, NOW, timedelta(hours=2), per_stop_limit=10
    )
    by_key = {(dep.trip_id, dep.stop_id): dep for dep in departures}
    s2 = by_key[("F1#21600", "S2")]
    assert s2.arrival == datetime(2026, 7, 30, 13, 10, tzinfo=UTC)
    assert s2.departure == datetime(2026, 7, 30, 13, 10, 30, tzinfo=UTC)
    s3 = by_key[("F1#21600", "S3")]
    assert s3.arrival == datetime(2026, 7, 30, 13, 20, tzinfo=UTC)
    assert s3.departure == datetime(2026, 7, 30, 13, 20, tzinfo=UTC)
    index.close()


def test_template_rows_replaced_not_kept(tmp_path: Path) -> None:
    """The bare template trip ids never surface: the template's own
    stop_times rows (F1 at 08:00, F2 at 23:30) are REPLACED by the
    materialized repetitions, while ordinary trips are untouched.
    """
    index = _index(tmp_path)
    departures = index.upcoming_departures(
        ["S1", "S2", "S3"], None, NOW, timedelta(hours=24), per_stop_limit=100
    )
    trip_ids = {dep.trip_id for dep in departures}
    assert "F1" not in trip_ids
    assert "F2" not in trip_ids
    assert {"T1", "T2"} <= trip_ids  # ordinary trips unaffected
    assert "F2#84600" in trip_ids  # 23:30 runs as a repetition, not a template
    index.close()


def test_is_first_last_treats_repetitions_as_ordinary_trips(tmp_path: Path) -> None:
    """Materialized repetitions are ordinary trips to the first/last flags:
    Thursday's S1->S2 candidates are the five F1 reps (06:00..07:10), T1
    (08:00:30), and the three F2 reps (23:30/24:00/24:30) -- so the day's
    06:00 F1 repetition is the first departure and nothing else in an
    early-morning window carries a flag.
    """
    index = _index(tmp_path)
    trips = index.upcoming_trips("S1", "S2", NOW, timedelta(hours=1), 10)
    assert [(t.trip_id, t.is_first, t.is_last) for t in trips] == [
        ("F1#21600", True, False),
        ("F1#22200", False, False),
        ("F1#22800", False, False),
    ]
    index.close()


def test_is_last_past_midnight_repetition_relative_to_own_day(tmp_path: Path) -> None:
    """F2's 24:30 repetition (F2#88200, running Friday 00:30 PDT) is the
    LAST S1->S2 departure of THURSDAY's service day -- the flag rides the
    service day the repetition belongs to, not the clock day it runs on.
    """
    now = datetime(2026, 7, 31, 7, 15, tzinfo=UTC)  # Friday 00:15 PDT
    index = _index(tmp_path)
    trips = index.upcoming_trips("S1", "S2", now, timedelta(hours=1), 10)
    assert [(t.trip_id, t.is_first, t.is_last) for t in trips] == [
        ("F2#88200", False, True)
    ]
    index.close()


def test_past_midnight_repetition_on_next_clock_day(tmp_path: Path) -> None:
    """F2's 24:00 and 24:30 repetitions belong to the PREVIOUS service day:
    querying Friday 00:15 PDT must surface Thursday's F2#88200 at Friday
    00:30 PDT, with a None arrival because the template's first-stop
    arrival is blank (the anchor fell back to the departure).
    """
    now = datetime(2026, 7, 31, 7, 15, tzinfo=UTC)  # Friday 00:15 PDT
    index = _index(tmp_path)
    departures = index.upcoming_departures(
        ["S1"], None, now, timedelta(hours=1), per_stop_limit=10
    )
    assert [(dep.trip_id, dep.departure) for dep in departures] == [
        ("F2#88200", datetime(2026, 7, 31, 7, 30, tzinfo=UTC))
    ]
    assert departures[0].arrival is None
    assert departures[0].route_id == "R2"
    index.close()


def test_upcoming_trips_across_materialized_repetitions(tmp_path: Path) -> None:
    """Origin→destination queries work per repetition: each synthetic F1
    trip runs S1 -> S3 with the template's elapsed span (20 minutes).
    """
    index = _index(tmp_path)
    trips = index.upcoming_trips("S1", "S3", NOW, timedelta(hours=2), 10)
    assert [trip.trip_id for trip in trips] == [trip_id for trip_id, _ in F1_EXPECTED]
    first = trips[0]
    assert first.route_id == "R1"
    assert first.headsign == "Loop"
    assert first.departure == datetime(2026, 7, 30, 13, 0, tzinfo=UTC)
    assert first.arrival == datetime(2026, 7, 30, 13, 20, tzinfo=UTC)
    index.close()


def test_dangling_frequency_row_is_skipped(tmp_path: Path) -> None:
    """A frequencies row referencing a trip with no stop_times template is
    skipped (the loaders enforce no referential integrity), leaving every
    other repetition intact.
    """
    index = _index(tmp_path, extra_frequency_rows="GHOST,06:00:00,06:30:00,600,\n")
    departures = index.upcoming_departures(
        ["S1"], None, NOW, timedelta(hours=2), per_stop_limit=10
    )
    assert [(dep.trip_id, dep.departure) for dep in departures] == F1_EXPECTED
    index.close()


@pytest.mark.parametrize(
    "bad_row",
    [
        "F1,06:00:00,06:30:00,abc,\n",  # unparseable headway
        "F1,06:00:00,06:30:00,0,\n",  # zero headway
        "F1,06:00:00,06:30:00,-60,\n",  # negative headway
        "F1,,06:30:00,600,\n",  # blank start_time
        "F1,06:00:00,,600,\n",  # blank end_time
        "F1,xx:00:00,06:30:00,600,\n",  # garbage start_time
    ],
)
def test_malformed_frequency_row_raises(tmp_path: Path, bad_row: str) -> None:
    with pytest.raises(FeedParseError):
        _index(tmp_path, extra_frequency_rows=bad_row)


def test_frequency_template_without_anchor_raises(tmp_path: Path) -> None:
    """A frequency trip whose first template stop has neither an arrival nor
    a departure time cannot anchor repetition offsets: FeedParseError.
    """
    files = {
        "agency.txt": (
            "agency_id,agency_name,agency_url,agency_timezone\nA1,T,https://e.com,UTC\n"
        ),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop,0,0\nS2,Two,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\nR1,1,Line,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\nR1,ALL,T1,H\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,,,S1,1\nT1,08:10:00,08:10:00,S2,2\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\nALL,1,1,1,1,1,1,1,20260101,20271231\n"
        ),
        "frequencies.txt": (
            "trip_id,start_time,end_time,headway_secs\nT1,06:00:00,07:00:00,600\n"
        ),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    zip_path = tmp_path / "no_anchor.zip"
    zip_path.write_bytes(buf.getvalue())
    with pytest.raises(FeedParseError, match="anchor"):
        StaticIndex.build(zip_path, ":memory:", DATASET, "UTC")
