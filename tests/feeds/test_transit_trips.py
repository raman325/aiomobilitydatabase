"""Tests for TransitFeedHandle.upcoming_trips: schedule + RT overlay."""

from datetime import UTC, date, datetime, timedelta

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.models import ArrivalsQuery

from tests.feeds.fixtures import (
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    TRIP_UPDATES_T1_BOTH_ENDS,
    TRIP_UPDATES_T1_CANCELED,
    TRIP_UPDATES_T1_DELAYED,
    TRIP_UPDATES_T1_DEST_ARRIVAL,
    TRIP_UPDATES_T1_NO_DATA_CUT,
    TRIP_UPDATES_T1_SKIP_S1,
    TRIP_UPDATES_T1_SKIP_S2,
    TRIP_UPDATES_T1_SKIP_S3,
    TRIP_UPDATES_T1_TRIP_DELAY,
    build_trip_query_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)  # Thursday 07:45 PDT
T1_DEPARTURE_EPOCH = int(datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC).timestamp())
T1_S3_ARRIVAL_EPOCH = int(datetime(2026, 7, 30, 15, 20, tzinfo=UTC).timestamp())
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
        body=build_trip_query_gtfs_zip_bytes(),
        content_type="application/zip",
    )


async def test_schedule_only_trips(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is False
    assert trip.predicted_departure is None
    assert trip.predicted_arrival is None
    assert trip.delay_seconds is None
    assert trip.scheduled_departure == datetime(2026, 7, 30, 15, 0, 30, tzinfo=UTC)
    assert trip.scheduled_arrival == datetime(2026, 7, 30, 15, 20, tzinfo=UTC)
    assert trip.origin_stop_id == "S1"
    assert trip.destination_stop_id == "S3"
    # Both ends resolve to whole Stop records, not just the ids passed in.
    assert trip.origin_stop is not None
    assert trip.origin_stop.name == "Main St"
    assert trip.destination_stop is not None
    assert trip.destination_stop.id == "S3"
    assert trip.route is not None
    assert trip.route.display_name == "10 Main Line"
    assert trip.route.type == 3
    assert trip.headsign == "Downtown"
    # Descriptor pass-through from the index: the base fixture ships no
    # descriptive columns (None everywhere, timepoint defaulting to exact),
    # and T1 is Thursday's ONLY S1->S3 candidate, so it is both the first
    # and the last departure of its service day for the pair.
    assert trip.wheelchair_accessible is None
    assert trip.bikes_allowed is None
    assert trip.direction_id is None
    assert trip.origin_pickup_type is None
    assert trip.origin_drop_off_type is None
    assert trip.origin_timepoint_exact is True
    assert trip.origin_stop_headsign is None
    assert trip.destination_pickup_type is None
    assert trip.destination_drop_off_type is None
    assert trip.destination_timepoint_exact is True
    assert trip.destination_stop_headsign is None
    assert trip.is_first is True
    assert trip.is_last is True


async def test_rt_origin_prediction_only(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100", api_key="secret123")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    # The RT feed also carries an ADDED trip (ADDED-9): added trips never
    # appear here because their full stop sequence is unknown.
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.delay_seconds == 300
    assert trip.predicted_departure == datetime.fromtimestamp(
        T1_DEPARTURE_EPOCH + 330, tz=UTC
    )
    # The TU names S1 only, but its 300s delay PROPAGATES to the
    # destination end (no explicit time there, so scheduled + delay).
    assert trip.predicted_arrival == datetime(2026, 7, 30, 15, 25, tzinfo=UTC)
    assert trip.scheduled_arrival == datetime(2026, 7, 30, 15, 20, tzinfo=UTC)


async def test_rt_destination_prediction_only(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DEST_ARRIVAL, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.predicted_arrival == datetime.fromtimestamp(
        T1_S3_ARRIVAL_EPOCH + 120, tz=UTC
    )
    assert trip.predicted_departure is None
    # delay_seconds reports the ORIGIN departure delay only, so a
    # destination-end prediction must not populate it.
    assert trip.delay_seconds is None


async def test_rt_predictions_at_both_ends(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_BOTH_ENDS, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.delay_seconds == 300
    assert trip.predicted_departure == datetime.fromtimestamp(
        T1_DEPARTURE_EPOCH + 330, tz=UTC
    )
    assert trip.predicted_arrival == datetime.fromtimestamp(
        T1_S3_ARRIVAL_EPOCH + 300, tz=UTC
    )


async def test_rt_cancellation_drops_trip(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_CANCELED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    # T1 is the only scheduled S1→S3 candidate in the window; canceling it
    # must drop the trip entirely (cancellation-wins, as in get_arrivals).
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert trips == []


async def test_skipped_origin_kills_only_that_boarding(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """SKIPPED S1: the vehicle will not serve S1, so the S1→S3 journey is
    impossible -- but boarding the same trip at S2 still works.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_SKIP_S1, content_type=PB)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_SKIP_S1, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert (
        await handle.upcoming_trips(
            "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
        )
        == []
    )
    later_boarding = await handle.upcoming_trips(
        "S2", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in later_boarding] == ["T1"]


async def test_skipped_destination_kills_only_that_alighting(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """SKIPPED S3: alighting at S3 is impossible, alighting at S2 is not."""
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_SKIP_S3, content_type=PB)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_SKIP_S3, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert (
        await handle.upcoming_trips(
            "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
        )
        == []
    )
    earlier_alighting = await handle.upcoming_trips(
        "S1", "S2", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in earlier_alighting] == ["T1"]


async def test_skipped_intermediate_changes_nothing_for_the_ends(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """SKIPPED S2 (plus a 300s delay at S1): the S1→S3 journey survives with
    the propagated delay flowing PAST the skipped stop, while S2 itself
    vanishes from arrivals and any journey alighting there dies.
    """
    _mock_catalog(mock_api, rt=True)
    for _ in range(3):  # one queued RT response per merging call below
        mock_api.get("/rt/all", body=TRIP_UPDATES_T1_SKIP_S2, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.delay_seconds == 300
    # No explicit times in the fixture: scheduled + propagated delay.
    assert trip.predicted_departure == datetime(2026, 7, 30, 15, 5, 30, tzinfo=UTC)
    assert trip.predicted_arrival == datetime(2026, 7, 30, 15, 25, tzinfo=UTC)
    assert (
        await handle.upcoming_trips(
            "S1", "S2", lookahead=timedelta(hours=1), now_utc=NOW
        )
        == []
    )
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    t1_rows = [(a.stop_id, a.realtime) for a in arrivals if a.trip_id == "T1"]
    assert t1_rows == [("S1", True)]  # the S2 call is suppressed entirely


async def test_no_data_cuts_propagation_at_and_after_its_stop(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Delay 300 at S1, NO_DATA at S2: S1 is predicted, S2 and S3 are
    schedule-only (propagation cut), so the destination end of S1→S3 has
    no prediction while the origin end keeps its delay.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_NO_DATA_CUT, content_type=PB)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_NO_DATA_CUT, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.delay_seconds == 300
    assert trip.predicted_departure == datetime(2026, 7, 30, 15, 5, 30, tzinfo=UTC)
    assert trip.predicted_arrival is None
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    by_key = {(a.trip_id, a.stop_id): a for a in arrivals}
    assert by_key[("T1", "S1")].realtime is True
    s2_row = by_key[("T1", "S2")]
    assert s2_row.realtime is False
    assert s2_row.predicted_departure is None
    assert s2_row.delay_seconds is None


async def test_trip_level_delay_fallback_covers_every_stop(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A TripUpdate carrying ONLY a trip-level delay (no STUs) predicts
    scheduled+delay at every stop of the trip -- the spec's fallback for
    producers that don't emit per-stop updates.
    """
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_TRIP_DELAY, content_type=PB)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_TRIP_DELAY, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    trip = trips[0]
    assert trip.realtime is True
    assert trip.delay_seconds == 180
    assert trip.predicted_departure == datetime(2026, 7, 30, 15, 3, 30, tzinfo=UTC)
    assert trip.predicted_arrival == datetime(2026, 7, 30, 15, 23, tzinfo=UTC)
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2"])], lookahead=timedelta(hours=1), now_utc=NOW
    )
    t1_rows = {a.stop_id: a for a in arrivals if a.trip_id == "T1"}
    assert t1_rows["S1"].delay_seconds == 180
    assert t1_rows["S2"].delay_seconds == 180
    assert t1_rows["S2"].predicted_departure == datetime(
        2026, 7, 30, 15, 13, 30, tzinfo=UTC
    )


NOW_LATE = datetime(2026, 7, 30, 15, 1, 30, tzinfo=UTC)  # one minute after T1 at S1


async def test_delayed_trip_survives_its_scheduled_origin_time(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW_LATE
    )
    assert [trip.trip_id for trip in trips] == ["T1"]
    assert trips[0].predicted_departure == datetime(2026, 7, 30, 15, 6, 0, tzinfo=UTC)


async def test_zero_grace_drops_delayed_trip_at_origin_time(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=True)
    mock_api.get("/rt/all", body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), grace=timedelta(0), now_utc=NOW_LATE
    )
    assert trips == []


async def test_past_schedule_only_trip_is_dropped(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=1), now_utc=NOW_LATE
    )
    assert trips == []


async def test_services_on_answers_future_dates(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """service_id on a row plus services_on(date) answers "does it run then".

    Neither half is useful alone: the id is opaque without a calendar
    query, and the calendar query returns opaque ids without rows naming
    them.
    """
    _mock_catalog(mock_api, rt=False)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals([ArrivalsQuery(["S1"])], now_utc=NOW)
    weekday_service = arrivals[0].service_id
    assert weekday_service == "WKDY"

    thursday = date(2026, 7, 30)
    saturday = date(2026, 8, 1)
    assert weekday_service in await handle.services_on(thursday)
    assert weekday_service not in await handle.services_on(saturday)
    # The NIGHT calendar in the fixture runs on its own days.
    assert await handle.services_on(thursday) != await handle.services_on(saturday)
