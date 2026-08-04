"""Tests for get_arrivals: schedule-only and schedule+RT merge."""

from datetime import UTC, datetime, timedelta

from aiomobilitydatabase.feeds.client import MobilityFeedsClient

from tests.feeds.fixtures import (
    ADDED_TRIPS_S1,
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    TRIP_UPDATES_T1_CANCELED_TOMORROW,
    TRIP_UPDATES_T1_DATED_TOMORROW_DELAY,
    TRIP_UPDATES_T1_DELAYED,
    build_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)  # Thursday 07:45 PDT
T1_DEPARTURE_EPOCH = int(datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC).timestamp())
PB = "application/octet-stream"


def _mock_catalog(mock_api: MockApi, *, rt: bool) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    rt_feeds = [with_base(GTFS_RT_FEED, base)] if rt else []
    mock_api.get("/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=rt_feeds)
    mock_api.get(
        "/hosted/mdb-100.zip",
        body=build_gtfs_zip_bytes(),
        content_type="application/zip",
    )


async def test_schedule_only_arrivals(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    arrivals = await handle.get_arrivals(
        ["S1"], lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == ["T1", "T2"]
    first = arrivals[0]
    assert first.realtime is False
    assert first.predicted_departure is None
    assert first.scheduled_departure == datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
    assert first.stop_name == "Main St"
    assert first.route_name == "10 Main Line"
    assert first.headsign == "Downtown"
    # The base fixture ships no descriptive stop_times/trips columns: every
    # descriptor is None EXCEPT timepoint_exact, whose GTFS default for an
    # absent column is "times are exact".
    assert first.wheelchair_accessible is None
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
    arrivals = await handle.get_arrivals(
        ["S1", "S2"], lookahead=timedelta(hours=1), now_utc=NOW
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
    assert added.route_name == "10 Main Line"
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
    arrivals = await handle.get_arrivals(
        ["S1", "S2"], route_ids=["R2"], lookahead=timedelta(hours=1), now_utc=NOW
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
    arrivals = await handle.get_arrivals(
        ["S2"], lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [a.stop_id for a in arrivals] == ["S2"]
    assert all(not (a.trip_id or "").startswith("ADDED") for a in arrivals)


async def test_limit_caps_merged_rows_per_stop(
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
    arrivals = await handle.get_arrivals(
        ["S1"], lookahead=timedelta(hours=1), limit=2, now_utc=NOW
    )
    s1_arrivals = [a for a in arrivals if a.stop_id == "S1"]
    assert len(s1_arrivals) == 2
    assert [a.trip_id for a in s1_arrivals] == ["ADDED-A", "ADDED-B"]


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
    arrivals = await handle.get_arrivals(
        ["S1"], lookahead=timedelta(hours=1), now_utc=NOW
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
    arrivals = await handle.get_arrivals(
        ["S1"], lookahead=timedelta(hours=30), now_utc=NOW
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
    arrivals = await handle.get_arrivals(
        ["S1"], lookahead=timedelta(hours=30), now_utc=NOW
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
