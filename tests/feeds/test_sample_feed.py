"""Conformance tests against Google's canonical GTFS sample feed.

The static index was developed against a synthetic mini-feed; this module
proves it ingests and answers correctly on the canonical example feed the
rest of the GTFS ecosystem validates against.

Feed provenance: ``tests/feeds/data/sample_feed/*.txt`` is Google's
canonical GTFS example feed (from the original transitfeed project's
examples), obtained UNMODIFIED via the MIT-licensed pygtfs repository --
see the README.md alongside the data for the license text.

Every expectation below is hand-computed from the feed's own CSV rows:

* agency.txt: timezone America/Los_Angeles; the feed's calendar spans
  20070101-20101231, so queries pin ``now_utc`` inside June 2007 (PDT,
  UTC-7: local midnight == 07:00 UTC). 2007-06-01 is a Friday.
* calendar.txt: FULLW runs every day, WE runs Sat+Sun (both 2007-2010).
* calendar_dates.txt: FULLW is REMOVED (exception_type 2) on 2007-06-04,
  a Monday -- that day has no active service at all.
* frequencies.txt repetition starts (start + n*headway, strictly < end):
  - STBA 06:00:00-22:00:00 @1800s -> 32 reps (21600, 23400, .., 77400;
    79200 == end_time is excluded by the strict-< rule).
  - CITY1/CITY2 share five windows whose headway changes across the day:
      06:00:00-07:59:59 @1800s ->  4 reps (21600, 23400, 25200, 27000)
      08:00:00-09:59:59 @600s  -> 12 reps (28800 .. 35400)
      10:00:00-15:59:59 @1800s -> 12 reps (36000 .. 55800)
      16:00:00-18:59:59 @600s  -> 18 reps (57600 .. 67800)
      19:00:00-22:00:00 @1800s ->  6 reps (68400 .. 77400)
    = 52 reps per template, 136 synthetic trips in total.
* stop_times.txt offsets: CITY1's template anchors at STAGECOACH
  (arr 6:00:00), so its later calls ride at +5:00/+7:00 (NANAA),
  +12:00/+14:00 (NADAV), +19:00/+21:00 (DADAN), +26:00/+28:00 (EMSI).
  CITY2 anchors at EMSI's ARRIVAL 6:28:00 (not its 6:30:00 departure),
  so its terminal STAGECOACH call (arr 6:56:00, dep 6:58:00) rides at
  +28:00/+30:00 from each repetition start.
* stop_times.txt row count: 28 CSV rows; the 12 template rows (STBA 2,
  CITY1 5, CITY2 5) are replaced by 32*2 + 52*5 + 52*5 = 584 materialized
  rows, leaving 16 plain rows -> 600 rows total.
"""

import io
import zipfile
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.models import Agency
from aiomobilitydatabase.feeds.static_index import ScheduledDeparture, StaticIndex

from tests.feeds.fixtures import (
    GTFS_FEED,
    TOKEN_RESPONSE,
    build_sample_feed_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

DATASET = "mdb-100-202607310000"
TZ = "America/Los_Angeles"

ALL_STOP_IDS = [
    "AMV",
    "BEATTY_AIRPORT",
    "BULLFROG",
    "DADAN",
    "EMSI",
    "FUR_CREEK_RES",
    "NADAV",
    "NANAA",
    "STAGECOACH",
]

# Service-day starts (local midnight PDT == 07:00 UTC).
FRIDAY_START = datetime(2007, 6, 1, 7, 0, tzinfo=UTC)
SATURDAY_START = datetime(2007, 6, 2, 7, 0, tzinfo=UTC)

# Hand-derived repetition starts (seconds; see module docstring).
STBA_STARTS = list(range(21600, 79200, 1800))
CITY_STARTS = sorted(
    {
        *range(21600, 28799, 1800),
        *range(28800, 35999, 600),
        *range(36000, 57599, 1800),
        *range(57600, 68399, 600),
        *range(68400, 79200, 1800),
    }
)


def _index(tmp_path: Path) -> StaticIndex:
    zip_path = tmp_path / "sample_feed.zip"
    zip_path.write_bytes(build_sample_feed_zip_bytes())
    return StaticIndex.build(zip_path, ":memory:", DATASET, TZ)


def _full_day(index: StaticIndex, day_start_utc: datetime) -> list[ScheduledDeparture]:
    """Every departure of one service day (23h < any next-day window)."""
    return index.upcoming_departures(
        ALL_STOP_IDS, None, day_start_utc, timedelta(hours=23), per_stop_limit=200
    )


def test_ingestion_exact_counts_and_unmodeled_files_ignored(tmp_path: Path) -> None:
    """The canonical feed builds cleanly with every unmodeled file present
    in the zip (fare_attributes, fare_rules, shapes, transfers, feed_info,
    translations -- plus agency.txt's deliberate nonexistent_column), and
    the modeled entities land with exact counts.
    """
    zip_bytes = build_sample_feed_zip_bytes()
    names = set(zipfile.ZipFile(io.BytesIO(zip_bytes)).namelist())
    assert {
        "fare_attributes.txt",
        "fare_rules.txt",
        "shapes.txt",
        "transfers.txt",
        "feed_info.txt",
        "translations.txt",
    } <= names  # the graceful-ignore claim is only meaningful if they're in
    index = _index(tmp_path)
    stops = index.stops()
    assert len(stops) == 9
    assert {stop.id for stop in stops} == set(ALL_STOP_IDS)
    stagecoach = next(stop for stop in stops if stop.id == "STAGECOACH")
    assert stagecoach.name == "Stagecoach Hotel & Casino (Demo)"
    assert stagecoach.latitude == 36.915682
    assert stagecoach.longitude == -116.751677
    # stops.txt ships none of the optional descriptive columns.
    assert stagecoach.stop_code is None
    assert stagecoach.platform_code is None
    assert stagecoach.wheelchair_boarding is None
    assert stagecoach.location_type is None
    routes = index.routes()
    assert [route.id for route in routes] == [
        "AAMV",
        "AB",
        "BFC",
        "CITY",
        "EXT",
        "STBA",
    ]
    ext = next(route for route in routes if route.id == "EXT")
    assert ext.type == 103  # extended route types survive ingestion
    # routes.txt populates agency_id everywhere; its route_url/route_color/
    # route_text_color columns exist but every value is blank.
    assert all(route.agency_id == "DTA" for route in routes)
    assert all(route.color is None for route in routes)
    assert all(route.text_color is None for route in routes)
    assert all(route.url is None for route in routes)
    # agency.txt is a full single record (no lang/phone/fare_url columns);
    # the deliberate nonexistent_column is ignored.
    assert index.agencies() == [
        Agency(
            id="DTA",
            name="Demo Transit Authority",
            url="http://google.com",
            timezone="America/Los_Angeles",
            lang=None,
            phone=None,
            fare_url=None,
        )
    ]
    index.close()


def test_friday_full_service_day_exact_departure_rows(tmp_path: Path) -> None:
    """A whole-Friday scan surfaces exactly the materialized schedule:
    136 repetitions (32 STBA + 52 CITY1 + 52 CITY2) plus the four plain
    FULLW trips = 140 distinct trips over 592 (trip, stop) rows -- with
    the bare template ids gone and the weekend-only AAMV trips absent.
    """
    index = _index(tmp_path)
    rows = _full_day(index, FRIDAY_START)
    assert len(rows) == 592  # 32*2 + 52*5 + 52*5 + 4 plain trips * 2 calls
    trip_ids = {dep.trip_id for dep in rows}
    assert len(trip_ids) == 140
    plain = {trip_id for trip_id in trip_ids if "#" not in trip_id}
    assert plain == {"AB1", "AB2", "BFC1", "BFC2"}  # AAMV1-4 are WE-only
    assert Counter(dep.trip_id.split("#")[0] for dep in rows if "#" in dep.trip_id) == {
        "STBA": 32 * 2,
        "CITY1": 52 * 5,
        "CITY2": 52 * 5,
    }
    assert Counter(dep.stop_id for dep in rows) == {
        "STAGECOACH": 136,  # 32 STBA + 52 CITY1 + 52 CITY2
        "BEATTY_AIRPORT": 34,  # 32 STBA + AB1 + AB2
        "NANAA": 104,
        "NADAV": 104,
        "DADAN": 104,
        "EMSI": 104,
        "BULLFROG": 4,  # AB1, AB2, BFC1, BFC2
        "FUR_CREEK_RES": 2,  # BFC1, BFC2
    }
    # Plain trips carry the (own id, None) RT identity; pinned times: AB1
    # departs BEATTY_AIRPORT 8:00:00 local == 15:00 UTC.
    ab1 = next(dep for dep in rows if dep.trip_id == "AB1")
    assert ab1.stop_id == "BEATTY_AIRPORT"
    assert ab1.departure == datetime(2007, 6, 1, 15, 0, tzinfo=UTC)
    assert ab1.source_trip_id == "AB1"
    assert ab1.start_secs is None
    index.close()


def test_saturday_scan_covers_every_stop_time_row(tmp_path: Path) -> None:
    """Saturday activates FULLW AND WE, so one whole-day scan returns every
    one of the 600 stop_times rows (16 plain + 584 materialized) across
    144 distinct trips -- the AAMV weekend trips join Friday's 140.
    """
    index = _index(tmp_path)
    rows = _full_day(index, SATURDAY_START)
    assert len(rows) == 600
    trip_ids = {dep.trip_id for dep in rows}
    assert len(trip_ids) == 144
    assert {"AAMV1", "AAMV2", "AAMV3", "AAMV4"} <= trip_ids
    index.close()


def test_stba_repetitions_exact_starts_and_pinned_departures(tmp_path: Path) -> None:
    """STBA's single 06:00-22:00 @1800s window materializes exactly 32
    repetitions; a rep landing exactly on end_time (22:00 == STBA#79200)
    must NOT run. Pinned: first rep departs STAGECOACH 06:00 PDT (13:00
    UTC) and reaches BEATTY_AIRPORT +20:00 later; the last (21:30 PDT)
    departs 04:30 UTC the next clock day.
    """
    index = _index(tmp_path)
    rows = index.upcoming_departures(
        ["STAGECOACH", "BEATTY_AIRPORT"],
        None,
        FRIDAY_START,
        timedelta(hours=23),
        per_stop_limit=200,
    )
    stba = [dep for dep in rows if dep.trip_id.startswith("STBA#")]
    at_stagecoach = [dep for dep in stba if dep.stop_id == "STAGECOACH"]
    assert len(STBA_STARTS) == 32
    assert [dep.trip_id for dep in at_stagecoach] == [
        f"STBA#{start}" for start in STBA_STARTS
    ]
    first = at_stagecoach[0]
    assert first.departure == datetime(2007, 6, 1, 13, 0, tzinfo=UTC)
    assert first.source_trip_id == "STBA"
    assert first.start_secs == 21600
    assert first.headsign == "Shuttle"
    assert first.route_id == "STBA"
    # trips.txt has no wheelchair/bikes columns; stop_times.txt HAS
    # pickup/drop-off/headsign columns with every value blank, and no
    # timepoint column at all -- so times are exact by the GTFS default,
    # carried through materialization to this repetition.
    assert first.wheelchair_accessible is None
    assert first.bikes_allowed is None
    assert first.pickup_type is None
    assert first.drop_off_type is None
    assert first.stop_headsign is None
    assert first.timepoint_exact is True
    assert at_stagecoach[-1].trip_id == "STBA#77400"
    assert at_stagecoach[-1].departure == datetime(2007, 6, 2, 4, 30, tzinfo=UTC)
    assert "STBA#79200" not in {dep.trip_id for dep in stba}
    by_key = {(dep.trip_id, dep.stop_id): dep for dep in stba}
    airport = by_key[("STBA#21600", "BEATTY_AIRPORT")]  # template's +20:00 call
    assert airport.arrival == datetime(2007, 6, 1, 13, 20, tzinfo=UTC)
    assert airport.departure == datetime(2007, 6, 1, 13, 20, tzinfo=UTC)
    index.close()


def test_city_repetitions_across_changing_headways(tmp_path: Path) -> None:
    """CITY1 and CITY2 each materialize the same 52 repetition starts from
    five windows whose headway flips between 1800s and 600s across the
    day. Offsets ride along per repetition, and CITY2's anchor is its
    first stop's ARRIVAL (EMSI 6:28:00), so CITY2#21600's terminal
    STAGECOACH call lands at +28:00/+30:00 from the 06:00 start.
    """
    index = _index(tmp_path)
    rows = index.upcoming_departures(
        ["STAGECOACH", "NANAA", "EMSI"],
        None,
        FRIDAY_START,
        timedelta(hours=23),
        per_stop_limit=200,
    )
    assert len(CITY_STARTS) == 52
    # The last 06:00-07:59:59 @1800s start is 07:30; the 08:00-09:59:59
    # @600s window then tightens the spacing -- 07:30, 08:00, 08:10.
    assert CITY_STARTS[3:6] == [27000, 28800, 29400]
    at_stagecoach = [dep for dep in rows if dep.stop_id == "STAGECOACH"]
    assert [
        dep.trip_id for dep in at_stagecoach if dep.trip_id.startswith("CITY1#")
    ] == [f"CITY1#{start}" for start in CITY_STARTS]
    assert [
        dep.trip_id for dep in at_stagecoach if dep.trip_id.startswith("CITY2#")
    ] == [f"CITY2#{start}" for start in CITY_STARTS]
    by_key = {(dep.trip_id, dep.stop_id): dep for dep in rows}
    # CITY1#28800 (08:00 PDT start): NANAA at +5:00/+7:00, EMSI at
    # +26:00/+28:00 -- 15:05/15:07 and 15:26/15:28 UTC.
    nanaa = by_key[("CITY1#28800", "NANAA")]
    assert nanaa.arrival == datetime(2007, 6, 1, 15, 5, tzinfo=UTC)
    assert nanaa.departure == datetime(2007, 6, 1, 15, 7, tzinfo=UTC)
    emsi = by_key[("CITY1#28800", "EMSI")]
    assert emsi.arrival == datetime(2007, 6, 1, 15, 26, tzinfo=UTC)
    assert emsi.departure == datetime(2007, 6, 1, 15, 28, tzinfo=UTC)
    assert emsi.start_secs == 28800
    assert emsi.source_trip_id == "CITY1"
    # CITY2#21600 anchors on EMSI's 6:28:00 ARRIVAL: EMSI 13:00/13:02 UTC,
    # STAGECOACH 13:28/13:30 UTC.
    first_stop = by_key[("CITY2#21600", "EMSI")]
    assert first_stop.arrival == datetime(2007, 6, 1, 13, 0, tzinfo=UTC)
    assert first_stop.departure == datetime(2007, 6, 1, 13, 2, tzinfo=UTC)
    terminal = by_key[("CITY2#21600", "STAGECOACH")]
    assert terminal.arrival == datetime(2007, 6, 1, 13, 28, tzinfo=UTC)
    assert terminal.departure == datetime(2007, 6, 1, 13, 30, tzinfo=UTC)
    index.close()


def test_upcoming_trips_across_frequency_repetitions(tmp_path: Path) -> None:
    """Origin->destination answers work per repetition and per direction:
    STAGECOACH->EMSI rides CITY1 repetitions only (CITY2 serves both
    stops in reverse order and must be excluded), and vice versa. With
    now = Friday 07:45 PDT (14:45 UTC) and a 30-minute lookahead, the
    CITY1 starts in window are 28800 (08:00) and 29400 (08:10); the
    07:30 rep is past and the 08:20 rep is beyond the window.
    """
    now = datetime(2007, 6, 1, 14, 45, tzinfo=UTC)
    index = _index(tmp_path)
    outbound = index.upcoming_trips(
        "STAGECOACH", "EMSI", now, timedelta(minutes=30), 10
    )
    assert [(trip.trip_id, trip.departure, trip.arrival) for trip in outbound] == [
        (
            "CITY1#28800",
            datetime(2007, 6, 1, 15, 0, tzinfo=UTC),
            datetime(2007, 6, 1, 15, 26, tzinfo=UTC),
        ),
        (
            "CITY1#29400",
            datetime(2007, 6, 1, 15, 10, tzinfo=UTC),
            datetime(2007, 6, 1, 15, 36, tzinfo=UTC),
        ),
    ]
    assert outbound[0].route_id == "CITY"
    assert outbound[0].source_trip_id == "CITY1"
    assert outbound[0].start_secs == 28800
    # CITY1's direction_id=0 rides along into every materialized repetition;
    # neither row is the day's first (CITY1#21600) or last (CITY1#77400)
    # STAGECOACH->EMSI departure.
    assert outbound[0].direction_id == 0
    assert outbound[0].origin_timepoint_exact is True
    assert [(t.is_first, t.is_last) for t in outbound] == [
        (False, False),
        (False, False),
    ]
    # Reverse pair: CITY2 reps whose EMSI departure (+2:00 from start)
    # falls in window -- same starts; STAGECOACH arrival is +28:00.
    inbound = index.upcoming_trips("EMSI", "STAGECOACH", now, timedelta(minutes=30), 10)
    assert [(trip.trip_id, trip.departure, trip.arrival) for trip in inbound] == [
        (
            "CITY2#28800",
            datetime(2007, 6, 1, 15, 2, tzinfo=UTC),
            datetime(2007, 6, 1, 15, 28, tzinfo=UTC),
        ),
        (
            "CITY2#29400",
            datetime(2007, 6, 1, 15, 12, tzinfo=UTC),
            datetime(2007, 6, 1, 15, 38, tzinfo=UTC),
        ),
    ]
    assert inbound[0].direction_id == 1  # CITY2's direction, carried per rep
    index.close()


def test_weekend_service_gates_aamv_trips(tmp_path: Path) -> None:
    """calendar.txt weekday/weekend split: WE (AAMV trips) is active on
    Saturday alongside FULLW, and inactive on Friday -- so the same
    BEATTY_AIRPORT->AMV query answers on Saturday and is empty on Friday.
    AAMV2/AAMV4 run the reverse direction and never appear.
    """
    index = _index(tmp_path)
    assert index.active_service_ids(FRIDAY_START.date()) == {"FULLW"}
    assert index.active_service_ids(SATURDAY_START.date()) == {"FULLW", "WE"}
    saturday_0745 = datetime(2007, 6, 2, 14, 45, tzinfo=UTC)
    trips = index.upcoming_trips(
        "BEATTY_AIRPORT", "AMV", saturday_0745, timedelta(hours=6), 10
    )
    assert [(trip.trip_id, trip.departure, trip.arrival) for trip in trips] == [
        (
            "AAMV1",  # 8:00:00 -> 9:00:00 local
            datetime(2007, 6, 2, 15, 0, tzinfo=UTC),
            datetime(2007, 6, 2, 16, 0, tzinfo=UTC),
        ),
        (
            "AAMV3",  # 13:00:00 -> 14:00:00 local
            datetime(2007, 6, 2, 20, 0, tzinfo=UTC),
            datetime(2007, 6, 2, 21, 0, tzinfo=UTC),
        ),
    ]
    # AAMV1 and AAMV3 are Saturday's only BEATTY_AIRPORT->AMV candidates:
    # first/last-of-service-day flags split across them.
    assert [(t.trip_id, t.is_first, t.is_last) for t in trips] == [
        ("AAMV1", True, False),
        ("AAMV3", False, True),
    ]
    assert trips[0].direction_id == 0  # "to Amargosa Valley"
    friday_0745 = datetime(2007, 6, 1, 14, 45, tzinfo=UTC)
    assert (
        index.upcoming_trips(
            "BEATTY_AIRPORT", "AMV", friday_0745, timedelta(hours=6), 10
        )
        == []
    )
    index.close()


def test_calendar_dates_exception_silences_2007_06_04(tmp_path: Path) -> None:
    """calendar_dates.txt removes FULLW on Monday 2007-06-04 (WE never runs
    Mondays), so that day has NO active service and a departure query
    returns nothing -- while the identical query one week later answers
    with the expected STBA repetitions bracketing AB1.
    """
    index = _index(tmp_path)
    assert index.active_service_ids(datetime(2007, 6, 4, tzinfo=UTC).date()) == set()
    assert index.active_service_ids(datetime(2007, 6, 11, tzinfo=UTC).date()) == {
        "FULLW"
    }
    exception_monday = datetime(2007, 6, 4, 14, 45, tzinfo=UTC)  # 07:45 PDT
    assert (
        index.upcoming_departures(
            ["BEATTY_AIRPORT"], None, exception_monday, timedelta(hours=1), 10
        )
        == []
    )
    normal_monday = datetime(2007, 6, 11, 14, 45, tzinfo=UTC)
    departures = index.upcoming_departures(
        ["BEATTY_AIRPORT"], None, normal_monday, timedelta(hours=1), 10
    )
    # STBA reps reach BEATTY_AIRPORT at start+20:00: 07:50 and 08:20 local
    # bracket AB1's 08:00 departure.
    assert [(dep.trip_id, dep.departure) for dep in departures] == [
        ("STBA#27000", datetime(2007, 6, 11, 14, 50, tzinfo=UTC)),
        ("AB1", datetime(2007, 6, 11, 15, 0, tzinfo=UTC)),
        ("STBA#28800", datetime(2007, 6, 11, 15, 20, tzinfo=UTC)),
    ]
    index.close()


def _mock_catalog(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[])
    mock_api.get(
        "/hosted/mdb-100.zip",
        body=build_sample_feed_zip_bytes(),
        content_type="application/zip",
    )


async def test_handle_answers_from_canonical_feed(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """End-to-end through TransitFeedHandle: get_arrivals at STAGECOACH
    interleaves STBA/CITY1 repetitions with CITY2's offset terminal call
    (a frequency-repetition answer), and upcoming_trips answers the plain
    AB1 trip while excluding the reverse-direction AB2.
    """
    _mock_catalog(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    # Friday 05:45 PDT, 1h window: 06:00 starts of STBA/CITY1, their
    # 06:30 follow-ups, and CITY2#21600's terminal STAGECOACH call at
    # 06:30 (start + 30:00). CITY2#23400 reaches STAGECOACH only at
    # 07:00 -- outside the window.
    arrivals = await handle.get_arrivals(
        ["STAGECOACH"],
        lookahead=timedelta(hours=1),
        now_utc=datetime(2007, 6, 1, 12, 45, tzinfo=UTC),
    )
    assert [(a.trip_id, a.scheduled_departure) for a in arrivals] == [
        ("CITY1#21600", datetime(2007, 6, 1, 13, 0, tzinfo=UTC)),
        ("STBA#21600", datetime(2007, 6, 1, 13, 0, tzinfo=UTC)),
        ("CITY1#23400", datetime(2007, 6, 1, 13, 30, tzinfo=UTC)),
        ("CITY2#21600", datetime(2007, 6, 1, 13, 30, tzinfo=UTC)),
        ("STBA#23400", datetime(2007, 6, 1, 13, 30, tzinfo=UTC)),
    ]
    shuttle = arrivals[1]
    assert shuttle.stop_name == "Stagecoach Hotel & Casino (Demo)"
    assert shuttle.route_name == "30 Stagecoach - Airport Shuttle"
    assert shuttle.headsign == "Shuttle"
    assert shuttle.realtime is False
    assert shuttle.predicted_departure is None
    city2 = arrivals[3]
    assert city2.scheduled_arrival == datetime(2007, 6, 1, 13, 28, tzinfo=UTC)
    assert city2.headsign is None  # CITY trips publish no headsign
    # Plain-trip answer: AB1 (BEATTY_AIRPORT 8:00 -> BULLFROG 8:10 local).
    # AB2 serves both stops in reverse order and must not appear.
    trips = await handle.upcoming_trips(
        "BEATTY_AIRPORT",
        "BULLFROG",
        lookahead=timedelta(hours=1),
        now_utc=datetime(2007, 6, 1, 14, 45, tzinfo=UTC),
    )
    assert [(t.trip_id, t.scheduled_departure, t.scheduled_arrival) for t in trips] == [
        (
            "AB1",
            datetime(2007, 6, 1, 15, 0, tzinfo=UTC),
            datetime(2007, 6, 1, 15, 10, tzinfo=UTC),
        )
    ]
    assert trips[0].route_name == "10 Airport - Bullfrog"
    assert trips[0].headsign == "to Bullfrog"
    assert trips[0].realtime is False
    # AB1 is Friday's ONLY BEATTY_AIRPORT->BULLFROG candidate (AB2 runs the
    # reverse direction), so it is both the first and last of its day.
    assert trips[0].is_first is True
    assert trips[0].is_last is True
    assert trips[0].direction_id == 0
    # The handle exposes the full agency record surface, like stops/routes.
    assert [agency.id for agency in handle.agencies] == ["DTA"]
    assert handle.agencies[0].name == "Demo Transit Authority"
    handle.close()
