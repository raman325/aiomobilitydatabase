"""Tests for get_arrivals: schedule-only and schedule+RT merge."""

import io
import zipfile
from datetime import UTC, datetime, timedelta

import pytest
from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.models import ArrivalsQuery

from tests.feeds.fixtures import (
    ADDED_TRIPS_S1,
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    TRIP_UPDATES_BASELINE,
    TRIP_UPDATES_T1_CANCELED_TOMORROW,
    TRIP_UPDATES_T1_DATED_TOMORROW_DELAY,
    TRIP_UPDATES_T1_DELAYED,
    VEHICLE_POSITIONS,
    _writestr,
    build_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)  # Thursday 07:45 PDT
T1_DEPARTURE_EPOCH = int(datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC).timestamp())
PB = "application/octet-stream"

NOW_LATE = datetime(2026, 7, 30, 15, 1, 30, tzinfo=UTC)  # one minute after T1 at S1


def _mock_catalog(
    mock_api: MockApi, *, rt: bool, zip_bytes: bytes | None = None
) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    rt_feeds = [with_base(GTFS_RT_FEED, base)] if rt else []
    mock_api.get("/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=rt_feeds)
    mock_api.get(
        "/hosted/mdb-100.zip",
        body=zip_bytes if zip_bytes is not None else build_gtfs_zip_bytes(),
        content_type="application/zip",
    )


def _added_arrival_only(arrival: datetime) -> bytes:
    """TripUpdates with one RT-added trip whose only S1 call has an arrival
    and no departure, the shape of a terminal stop."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = int(NOW.timestamp())
    entity = msg.entity.add()
    entity.id = "tu-terminal"
    update = entity.trip_update
    update.trip.trip_id = "ADDED-TERMINAL"
    update.trip.route_id = "R1"
    update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.ADDED
    stu = update.stop_time_update.add()
    stu.stop_id = "S1"
    stu.arrival.time = int(arrival.timestamp())
    return msg.SerializeToString()


def _busy_stop_zip_bytes() -> bytes:
    """One stop where eleven route-A trips (08:00..08:10 local, one per
    minute) precede the first route-B trip at 08:20."""
    trips = "".join(f"RA,ALL,A{i:02d},North\n" for i in range(11)) + "RB,ALL,B1,South\n"
    stop_times = (
        "".join(f"A{i:02d},08:{i:02d}:00,08:{i:02d}:00,S1,1\n" for i in range(11))
        + "B1,08:20:00,08:20:00,S1,1\n"
    )
    files = {
        "agency.txt": (
            "agency_id,agency_name,agency_url,agency_timezone\n"
            "A1,Busy,https://e.com,America/Los_Angeles\n"
        ),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Busy Stop,0,0\n",
        "routes.txt": (
            "route_id,route_short_name,route_long_name,route_type\n"
            "RA,A,Route A,3\nRB,B,Route B,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id,trip_headsign\n" + trips,
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n" + stop_times
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\nALL,1,1,1,1,1,1,1,20260101,20271231\n"
        ),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            _writestr(zf, name, content)
    return buf.getvalue()


async def test_schedule_only_arrivals(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == ["T1", "T2"]
    first = arrivals[0]
    assert first.realtime is False
    assert first.predicted_departure is None
    assert first.scheduled_departure == datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
    assert first.stop is not None
    assert first.stop.name == "Main St"
    assert first.route is not None
    assert first.route.display_name == "10 Main Line"
    assert first.route.type == 3
    assert first.headsign == "Downtown"
    # The base fixture ships no descriptive stop_times/trips columns: every
    # descriptor is None EXCEPT timepoint_exact, whose GTFS default for an
    # absent column is "times are exact".
    assert first.wheelchair_accessible is None
    assert first.direction_id is None
    assert first.bikes_allowed is None
    assert first.pickup_type is None
    assert first.drop_off_type is None
    assert first.stop_headsign is None
    assert first.timepoint_exact is True


async def test_rt_merge_delay_cancellation_and_added(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get(
        "/rt/all",
        body=TRIP_UPDATES_T1_DELAYED,
        content_type=PB,
    )
    handle = await feeds_client.get_transit_feed("mdb-100", api_key="secret123")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    # One row per (trip, stop): a trip serving both queried stops appears twice
    # (a two-stop departure board must show both calls of the same vehicle).
    by_key = {(a.trip_id, a.stop_id): a for a in arrivals}
    # T2 canceled by RT: dropped entirely (no row at any stop).
    assert not any(trip_id == "T2" for trip_id, _ in by_key)
    # T1 delayed 300s at S1 (the TU names S1 only). The producer ALSO sent
    # explicit epoch times that differ from scheduled+delay (departure
    # epoch+330 vs 15:00:30+300 = 15:05:30): the explicit time must win.
    t1_s1 = by_key[("T1", "S1")]
    assert t1_s1.realtime is True
    assert t1_s1.delay_seconds == 300
    assert t1_s1.predicted_departure == datetime.fromtimestamp(
        T1_DEPARTURE_EPOCH + 330, tz=UTC
    )
    assert t1_s1.predicted_departure != datetime(2026, 7, 30, 15, 5, 30, tzinfo=UTC)
    assert t1_s1.scheduled_departure == datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
    assert t1_s1.vehicle_id == "V1"
    # Same trip at S2: no STU of its own, so the S1 delay PROPAGATES (GTFS-RT
    # spec: an STU's delay applies to all subsequent stops until newer
    # information). Propagated stops count as realtime -- a propagated delay
    # IS realtime information -- with predicted times = scheduled + delay.
    t1_s2 = by_key[("T1", "S2")]
    assert t1_s2.realtime is True
    assert t1_s2.delay_seconds == 300
    assert t1_s2.predicted_arrival == datetime(2026, 7, 30, 15, 15, 0, tzinfo=UTC)
    assert t1_s2.predicted_departure == datetime(2026, 7, 30, 15, 15, 30, tzinfo=UTC)
    assert t1_s2.scheduled_departure == datetime(2026, 7, 30, 15, 10, 30, tzinfo=UTC)
    assert t1_s2.vehicle_id == "V1"
    # RT-added trip at S2 with no schedule.
    added = by_key[("ADDED-9", "S2")]
    assert added.realtime is True
    assert added.scheduled_departure is None
    # An RT-added trip carries no static row, but it does name a route, so
    # the whole Route record still resolves against the index.
    assert added.route is not None
    assert added.route.display_name == "10 Main Line"
    assert added.route.type == 3
    # ... while trip-level descriptors, direction_id included, stay None.
    assert added.direction_id is None
    # An RT-added trip has no static schedule row: every descriptor is None,
    # INCLUDING timepoint_exact (the absent-means-exact default only applies
    # to stop_times rows that exist).
    assert added.wheelchair_accessible is None
    assert added.bikes_allowed is None
    assert added.pickup_type is None
    assert added.drop_off_type is None
    assert added.timepoint_exact is None
    assert added.stop_headsign is None
    # Producer auth header applied (auth_type 2, X-Api-Key).
    rt_request = next(req for req in mock_api.requests if req.path == "/rt/all")
    assert rt_request.headers.get("X-Api-Key") == "secret123"


async def test_route_filter_applies_to_added_trips(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get(
        "/rt/all",
        body=TRIP_UPDATES_T1_DELAYED,
        content_type=PB,
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2"], ["R2"])],
        lookahead=timedelta(hours=1),
        now_utc=NOW,
    )
    assert arrivals == []  # R2 has nothing scheduled in window; ADDED-9 is R1


async def test_added_trip_outside_queried_stops_excluded(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=ADDED_TRIPS_S1, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    # ADDED_TRIPS_S1's rows are all at S1; querying only S2 must filter every
    # one of them out via the stop-id check, before the route filter even
    # runs -- only S2's own scheduled row (T1) can come back.
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S2"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [a.stop_id for a in arrivals] == ["S2"]
    assert all(not (a.trip_id or "").startswith("ADDED") for a in arrivals)


async def test_limit_caps_merged_rows_per_query(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    # 3 RT-added trips at S1, departing 1/2/3 minutes from now: all strictly
    # earlier than the scheduled T1 (15:00:30, ~15.5min out) and T2
    # (15:30:30, ~45.5min out). Together with the 2 scheduled rows, S1 has
    # 5 candidate rows before the per-stop limit is applied.
    mock_api.get(
        "/rt/all",
        body=ADDED_TRIPS_S1,
        content_type=PB,
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"], limit=2)], lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == ["ADDED-A", "ADDED-B"]


# The Thursday and Friday instances of T1's S1 departure (08:00:30 PDT).
T1_S1_THURSDAY = datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
T1_S1_FRIDAY = datetime(2026, 7, 31, 15, 0, 30, tzinfo=UTC)


async def test_tomorrow_cancellation_spares_todays_instance(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """THE motivating start_date case: a cancellation posted today with
    start_date naming TOMORROW (2026-07-31) must not cancel today's
    in-window T1 departure -- pre-start_date matching keyed on bare
    (trip_id, start_secs) and would have dropped it.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_CANCELED_TOMORROW, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == ["T1", "T2"]
    assert all(a.realtime is False for a in arrivals)


async def test_tomorrow_cancellation_drops_only_tomorrows_instance(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """With a 30h window holding BOTH daily instances of T1, the dated
    cancellation removes exactly Friday's row; Thursday's T1, both T2
    instances, Thursday's T3 spillover, and Friday's T4 all survive.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_CANCELED_TOMORROW, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=30), now_utc=NOW
    )
    assert [(a.trip_id, a.scheduled_departure) for a in arrivals] == [
        ("T1", T1_S1_THURSDAY),
        ("T2", datetime(2026, 7, 30, 15, 30, 30, tzinfo=UTC)),
        ("T3", datetime(2026, 7, 31, 8, 31, tzinfo=UTC)),
        # Friday's T1 (15:00:30) is canceled; T2/T4 keep Friday rows.
        ("T2", datetime(2026, 7, 31, 15, 30, 30, tzinfo=UTC)),
        ("T4", datetime(2026, 7, 31, 16, 0, 30, tzinfo=UTC)),
    ]
    assert all(a.realtime is False for a in arrivals)


async def test_dated_prediction_attaches_to_second_instance_only(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A prediction with start_date=2026-07-31 in a 30h window attaches to
    Friday's (second) T1 instance only: Thursday's identical (trip_id,
    start_secs) row stays schedule-only.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DATED_TOMORROW_DELAY, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=30), now_utc=NOW
    )
    by_departure = {(a.trip_id, a.scheduled_departure): a for a in arrivals}
    today = by_departure[("T1", T1_S1_THURSDAY)]
    assert today.realtime is False
    assert today.predicted_departure is None
    assert today.delay_seconds is None
    tomorrow = by_departure[("T1", T1_S1_FRIDAY)]
    assert tomorrow.realtime is True
    assert tomorrow.delay_seconds == 300
    assert tomorrow.predicted_departure == T1_S1_FRIDAY + timedelta(seconds=300)
    assert all(
        a.realtime is False
        for a in arrivals
        if (a.trip_id, a.scheduled_departure) != ("T1", T1_S1_FRIDAY)
    )


async def test_delayed_trip_survives_its_scheduled_time(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A trip whose scheduled departure has passed but whose realtime
    prediction is still ahead must stay on the board."""
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=1), now_utc=NOW_LATE
    )
    assert [a.trip_id for a in arrivals] == ["T1"]  # T2 is canceled by RT
    t1 = arrivals[0]
    assert t1.realtime is True
    assert t1.scheduled_departure == datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
    assert t1.predicted_departure == datetime(2026, 7, 30, 15, 6, 0, tzinfo=UTC)


async def test_zero_grace_drops_delayed_trip_at_its_scheduled_time(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])],
        lookahead=timedelta(hours=1),
        grace=timedelta(0),
        now_utc=NOW_LATE,
    )
    assert arrivals == []


async def test_past_schedule_only_rows_are_dropped(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Without a prediction, a row inside the grace window but before now
    is gone: the vehicle left on time."""
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=1), now_utc=NOW_LATE
    )
    assert [a.trip_id for a in arrivals] == ["T2"]


async def test_route_filter_applies_before_limit(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False, zip_bytes=_busy_stop_zip_bytes())
    handle = await feeds_client.get_transit_feed("mdb-100")
    now = datetime(2026, 7, 30, 14, 55, tzinfo=UTC)  # 07:55 PDT
    route_b, unfiltered = await handle.get_arrivals(
        [ArrivalsQuery(["S1"], ["RB"]), ArrivalsQuery(["S1"])],
        lookahead=timedelta(hours=2),
        now_utc=now,
    )
    assert [a.trip_id for a in route_b] == ["B1"]
    assert len(unfiltered) == 10
    assert all(a.route_id == "RA" for a in unfiltered)


async def test_headsign_filter(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    owl, downtown = await handle.get_arrivals(
        [
            ArrivalsQuery(["S1"], headsigns=["Owl Loop"]),
            ArrivalsQuery(["S1"], headsigns=["Downtown"]),
        ],
        lookahead=timedelta(hours=24),
        now_utc=NOW,
    )
    assert [a.trip_id for a in owl] == ["T3"]
    assert [a.trip_id for a in downtown] == ["T1", "T2"]


async def test_headsign_filter_excludes_added_trips(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=ADDED_TRIPS_S1, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"], headsigns=["Downtown"])],
        lookahead=timedelta(hours=1),
        now_utc=NOW,
    )
    assert [a.trip_id for a in arrivals] == ["T1", "T2"]


async def test_batch_shares_one_realtime_fetch(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    # Exactly one scripted RT response: a second fetch would get a 404.
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    results = await handle.get_arrivals(
        [
            ArrivalsQuery(["S1"]),
            ArrivalsQuery(["S2"]),
            ArrivalsQuery(["S1", "S2"], ["R1"]),
        ],
        lookahead=timedelta(hours=1),
        now_utc=NOW,
    )
    assert len(results) == 3
    assert [a.trip_id for a in results[0]] == ["T1"]
    # At S2 the 300s delay propagated to T1 pushes it to 15:15:30, behind
    # ADDED-9's 15:11:00 departure: rows sort by effective departure.
    assert [a.trip_id for a in results[1]] == ["ADDED-9", "T1"]
    assert [(a.trip_id, a.stop_id) for a in results[2]] == [
        ("T1", "S1"),
        ("ADDED-9", "S2"),
        ("T1", "S2"),
    ]
    assert len([r for r in mock_api.requests if r.path == "/rt/all"]) == 1


async def test_empty_batch_returns_empty_lists(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await handle.get_arrivals([]) == []
    assert await handle.get_arrivals([ArrivalsQuery([])]) == [[]]


async def test_arrival_only_added_row_orders_by_its_arrival(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    # Arrives between T1 (15:00:30) and T2 (15:30:30): must sort between them,
    # not at the front as a row with "no time" would.
    mock_api.get(
        "/rt/all",
        body=_added_arrival_only(datetime(2026, 7, 30, 15, 10, tzinfo=UTC)),
        content_type=PB,
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == ["T1", "ADDED-TERMINAL", "T2"]
    assert arrivals[1].predicted_departure is None
    assert arrivals[1].predicted_arrival == datetime(2026, 7, 30, 15, 10, tzinfo=UTC)


async def test_arrival_only_added_row_is_dropped_once_past(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get(
        "/rt/all",
        body=_added_arrival_only(NOW - timedelta(minutes=5)),
        content_type=PB,
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == ["T1", "T2"]


def _bundled_message(*, extra_vehicles: tuple[tuple[str, str], ...] = ()) -> bytes:
    """One FeedMessage carrying both TripUpdates and VehiclePositions.

    The shape the canonical RT fixture declares (``entity_types`` lists vp,
    tu and sa against a single producer_url), so both entity types come off
    the same download.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(TRIP_UPDATES_BASELINE)
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.ParseFromString(VEHICLE_POSITIONS)
    message.entity.extend(vehicles.entity)
    for vehicle_id, trip_id in extra_vehicles:
        entity = message.entity.add()
        entity.id = f"extra-{vehicle_id}"
        entity.vehicle.vehicle.id = vehicle_id
        entity.vehicle.trip.trip_id = trip_id
        entity.vehicle.position.latitude = 1.0
        entity.vehicle.position.longitude = 2.0
    return message.SerializeToString()


async def test_with_vehicles_attaches_the_matching_position(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=_bundled_message(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    # 18h reaches T3's 25:30 spillover departure, which V2 claims.
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2"])],
        lookahead=timedelta(hours=18),
        now_utc=NOW,
        with_vehicles=True,
    )
    by_trip = {a.trip_id: a for a in arrivals}
    # T1: matched on the vehicle id its TripUpdate names.
    t1 = by_trip["T1"]
    assert t1.vehicle_id == "V1"
    assert t1.vehicle is not None
    assert (t1.vehicle.latitude, t1.vehicle.longitude) == (
        pytest.approx(34.055),
        pytest.approx(-118.245),
    )
    # T3: no TripUpdate at all, so no vehicle id -- matched on the plain
    # trip id instead, the fallback frequency-based service depends on.
    t3 = by_trip["T3"]
    assert t3.vehicle_id is None
    assert t3.vehicle is not None
    assert t3.vehicle.vehicle_id == "V2"
    # RT-added row that no position claims.
    assert by_trip["ADDED-9"].vehicle is None


async def test_vehicles_are_not_fetched_unless_asked(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=_bundled_message(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals([ArrivalsQuery(["S1"])], now_utc=NOW)
    # The positions are sitting in the very message that was parsed for
    # TripUpdates, and are still not surfaced: the flag gates the join too.
    assert arrivals
    assert all(arrival.vehicle is None for arrival in arrivals)


async def test_bundled_feed_is_downloaded_once_per_call(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    for _ in range(4):
        mock_api.get("/rt/all", body=_bundled_message(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    await handle.get_arrivals([ArrivalsQuery(["S1"])], now_utc=NOW, with_vehicles=True)
    # TripUpdates and VehiclePositions are different entity types off the
    # SAME producer_url: without the per-call memo this is two GETs.
    assert sum(1 for r in mock_api.requests if r.path == "/rt/all") == 1


async def test_two_vehicles_claiming_one_trip_attach_nothing(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    # V9 also claims T1, and T1's TripUpdate names no vehicle to break the
    # tie (the baseline names V1, so drop that association first).
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(_bundled_message(extra_vehicles=(("V9", "T1"),)))
    for entity in message.entity:
        if entity.HasField("trip_update") and entity.trip_update.trip.trip_id == "T1":
            entity.trip_update.ClearField("vehicle")
    mock_api.get("/rt/all", body=message.SerializeToString(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], now_utc=NOW, with_vehicles=True
    )
    t1 = next(a for a in arrivals if a.trip_id == "T1")
    assert t1.vehicle_id is None
    # Two vehicles claim T1 and nothing distinguishes them: no guess.
    assert t1.vehicle is None


async def test_revalidated_bundled_feed_still_costs_one_request_per_call(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The memo and the 304 cache have to compose.

    A 304 carries no body, so the reused parse must still land in the
    per-call memo -- otherwise the second entity type off the same url
    finds nothing memoized and spends another round trip, and the poll
    costs two requests again.
    """
    _mock_catalog(mock_api, rt=True)
    body = _bundled_message()
    mock_api.get("/rt/all", body=body, content_type=PB, headers={"ETag": '"v1"'})
    mock_api.get("/rt/all", status=304)
    mock_api.get("/rt/all", status=304)  # spare: a 2nd GET here would be the bug
    handle = await feeds_client.get_transit_feed("mdb-100")

    for _ in range(2):
        [arrivals] = await handle.get_arrivals(
            [ArrivalsQuery(["S1", "S2"])],
            lookahead=timedelta(hours=18),
            now_utc=NOW,
            with_vehicles=True,
        )
        # The revalidated parse still carries the positions.
        assert any(a.vehicle is not None for a in arrivals)

    rt_requests = [r for r in mock_api.requests if r.path == "/rt/all"]
    assert len(rt_requests) == 2, "one request per poll, not one per entity type"
    assert "If-None-Match" not in rt_requests[0].headers
    assert rt_requests[1].headers["If-None-Match"] == '"v1"'


def _detour_message(
    *,
    replacement_stop: str = "S3",
    travel_time: int = 900,
    propagated_delay: int = 0,
    service_dates: tuple[str, ...] = ("20260730",),
    define_stop: bool = False,
) -> bytes:
    """T1 detoured: its S2 call is replaced by one at ``replacement_stop``."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "mod-1"
    modifications = entity.trip_modifications
    modifications.selected_trips.add().trip_ids.append("T1")
    modifications.service_dates.extend(service_dates)
    modification = modifications.modifications.add()
    modification.start_stop_selector.stop_sequence = 2
    modification.end_stop_selector.stop_sequence = 2
    modification.propagated_modification_delay = propagated_delay
    replacement = modification.replacement_stops.add()
    replacement.stop_id = replacement_stop
    replacement.travel_time_to_stop = travel_time
    if define_stop:
        stop_entity = message.entity.add()
        stop_entity.id = "stop-new"
        stop_entity.stop.stop_id = replacement_stop
        stop_entity.stop.stop_name.translation.add().text = "Pop-up Stop"
        stop_entity.stop.stop_lat = 34.09
        stop_entity.stop.stop_lon = -118.21
    return message.SerializeToString()


async def test_detour_moves_the_trip_off_the_replaced_stop(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A Modification REPLACES a span of calls, it does not annotate them.

    Ignoring it would keep showing T1 at S2 — a departure the vehicle is
    not making — which is wrong output, not merely missing output.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=_detour_message(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2", "S3"])], now_utc=NOW
    )
    calls = {(a.trip_id, a.stop_id): a for a in arrivals}
    # The replaced call is gone...
    assert ("T1", "S2") not in calls
    # ...replaced by one at S3, 900s after the reference stop's arrival.
    # S1 is the call before the span, arriving 08:00 local = 15:00Z.
    assert ("T1", "S3") in calls
    assert calls[("T1", "S3")].scheduled_departure == datetime(
        2026, 7, 30, 15, 15, tzinfo=UTC
    )
    # Calls before the span are untouched.
    assert ("T1", "S1") in calls
    assert calls[("T1", "S1")].scheduled_departure == datetime(
        2026, 7, 30, 15, 0, 30, tzinfo=UTC
    )


async def test_detour_defines_its_own_replacement_stop(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A replacement stop may exist only as an RT Stop entity.

    The static feed has never heard of it, so the RT entity is the only
    definition of its name and position there is.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get(
        "/rt/all",
        body=_detour_message(replacement_stop="POPUP", define_stop=True),
        content_type=PB,
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2", "POPUP"])], now_utc=NOW
    )
    popup = next(a for a in arrivals if a.stop_id == "POPUP")
    assert popup.stop is not None
    assert popup.stop.name == "Pop-up Stop"
    assert (popup.stop.latitude, popup.stop.longitude) == (
        pytest.approx(34.09),
        pytest.approx(-118.21),
    )


async def test_detour_ignores_a_service_date_it_does_not_name(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get(
        "/rt/all", body=_detour_message(service_dates=("20260731",)), content_type=PB
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2", "S3"])], now_utc=NOW
    )
    calls = {(a.trip_id, a.stop_id) for a in arrivals}
    # The detour is tomorrow's; today's trip still serves S2.
    assert ("T1", "S2") in calls
    assert ("T1", "S3") not in calls


async def test_propagated_delay_shifts_stops_after_the_detour(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """propagated_modification_delay applies to calls AFTER the span.

    This is the half that makes ignoring TripModifications wrong even for
    stops nowhere near the detour: S2 is not replaced, but a trip running
    ten minutes late out of its detour reaches S2 ten minutes late.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "mod-delay"
    modifications = entity.trip_modifications
    modifications.selected_trips.add().trip_ids.append("T1")
    modifications.service_dates.append("20260730")
    modification = modifications.modifications.add()
    # Replace the FIRST call, so S2 is strictly after the span.
    modification.start_stop_selector.stop_sequence = 1
    modification.end_stop_selector.stop_sequence = 1
    modification.propagated_modification_delay = 600
    replacement = modification.replacement_stops.add()
    replacement.stop_id = "S3"
    replacement.travel_time_to_stop = 60

    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=message.SerializeToString(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2", "S3"])], now_utc=NOW
    )
    by_stop = {a.stop_id: a for a in arrivals if a.trip_id == "T1"}
    # S2 scheduled 08:10:30 local = 15:10:30Z, now +600s.
    assert by_stop["S2"].scheduled_departure == datetime(
        2026, 7, 30, 15, 20, 30, tzinfo=UTC
    )
    # The span's own first call is its own reference, so S3 sits 60s after
    # S1's original arrival (15:00Z).
    assert by_stop["S3"].scheduled_departure == datetime(2026, 7, 30, 15, 1, tzinfo=UTC)
    assert "S1" not in by_stop


async def test_detour_on_another_day_leaves_todays_board_alone(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A detour names service dates; suppression must honour them.

    trip_instance_calls yields one instance per service day and the day
    scan deliberately overshoots the window, so suppressing by trip id
    alone deletes every run of that trip in range -- the whole board for
    T1 vanished with nothing replacing it.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get(
        "/rt/all", body=_detour_message(service_dates=("20260729",)), content_type=PB
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2", "S3"])], now_utc=NOW
    )
    calls = {(a.trip_id, a.stop_id): a for a in arrivals}
    # Yesterday's detour: today's T1 runs its normal route, untouched.
    assert ("T1", "S1") in calls
    assert ("T1", "S2") in calls
    assert ("T1", "S3") not in calls
    assert calls[("T1", "S2")].scheduled_departure == datetime(
        2026, 7, 30, 15, 10, 30, tzinfo=UTC
    )


async def test_detour_rows_respect_the_lookahead_horizon(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Rebuilt rows are bounded by the caller's horizon.

    trip_instance_calls deliberately does not window the calls, because a
    modification's reference stop may sit outside the window -- so the
    rebuilt rows have to be bounded explicitly. T3 departs 25:31 of its
    service day, ~18h past a two-hour board.
    """
    _mock_catalog(mock_api, rt=True)
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "mod-t3"
    modifications = entity.trip_modifications
    modifications.selected_trips.add().trip_ids.append("T3")
    modifications.service_dates.append("20260730")
    modification = modifications.modifications.add()
    modification.start_stop_selector.stop_sequence = 1
    modification.end_stop_selector.stop_sequence = 1
    replacement = modification.replacement_stops.add()
    replacement.stop_id = "S3"
    replacement.travel_time_to_stop = 900
    mock_api.get("/rt/all", body=message.SerializeToString(), content_type=PB)

    handle = await feeds_client.get_transit_feed("mdb-100")
    horizon = NOW + timedelta(hours=2)
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2", "S3"])], lookahead=timedelta(hours=2), now_utc=NOW
    )
    assert arrivals, "the ordinary board is unaffected"
    assert all(
        row.scheduled_departure <= horizon
        for row in arrivals
        if row.scheduled_departure
    )
    assert not any(row.trip_id == "T3" for row in arrivals)


async def test_detours_do_not_depend_on_with_vehicles(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A display flag must not change which schedule you get.

    Reading modifications out of the per-call memo made them depend on
    what the memo happened to hold. The feed here declares vp but NOT tu
    while still bundling trip_modifications, so only with_vehicles=True
    put its message in the memo -- and the two boards disagreed about
    whether T1 serves S2 or S3.
    """
    boards = []
    for flag in (False, True):
        base = mock_api.url()
        rt_feed = with_base(GTFS_RT_FEED, base)
        rt_feed["entity_types"] = ["vp"]
        mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
        mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
        mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
        mock_api.get("/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[rt_feed])
        mock_api.get(
            "/hosted/mdb-100.zip",
            body=build_gtfs_zip_bytes(),
            content_type="application/zip",
        )
        for _ in range(3):
            mock_api.get("/rt/all", body=_detour_message(), content_type=PB)
        handle = await feeds_client.get_transit_feed("mdb-100")
        [arrivals] = await handle.get_arrivals(
            [ArrivalsQuery(["S1", "S2", "S3"])], now_utc=NOW, with_vehicles=flag
        )
        boards.append(sorted((a.trip_id, a.stop_id) for a in arrivals))
        handle.close()
    assert boards[0] == boards[1], "with_vehicles changed the schedule"
    # Sourced from declared TU feeds, and this one declares none, so the
    # detour applies in neither -- consistently.
    assert ("T1", "S2") in boards[0]
