"""Handle-level tests: RT matching for frequency-based repetitions.

GTFS-RT addresses one repetition of a frequency-based trip via
TripDescriptor.start_time. The merge matches (trip_id, start_secs) against
each materialized repetition's (template id, repetition start): an aligned
start_time affects exactly that repetition, while a missing or unmatched
start_time affects no repetition at all.
"""

from datetime import UTC, datetime, timedelta

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.models import ArrivalsQuery

from tests.feeds.fixtures import (
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    TRIP_UPDATES_FREQ_BARE_CANCEL,
    TRIP_UPDATES_FREQ_CANCELED_TOMORROW,
    TRIP_UPDATES_FREQ_MATCHED,
    TRIP_UPDATES_FREQ_UNMATCHED,
    build_frequencies_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

NOW = datetime(2026, 7, 30, 12, 45, tzinfo=UTC)  # Thursday 05:45 PDT
PB = "application/octet-stream"

# The five F1 repetitions' synthetic ids (see fixtures.py); F1#22200 is the
# repetition the matched fixture predicts and F1#22800 the one it cancels.
F1_SYNTHETIC_IDS = ["F1#21600", "F1#22200", "F1#22800", "F1#25200", "F1#25800"]
F1_22200_S1_DEPARTURE = datetime(2026, 7, 30, 13, 10, tzinfo=UTC)


def _mock_catalog_with_rt(mock_api: MockApi, message_bytes: bytes) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds",
        payload=[with_base(GTFS_RT_FEED, base)],
    )
    mock_api.get(
        "/hosted/mdb-100.zip",
        body=build_frequencies_gtfs_zip_bytes(),
        content_type="application/zip",
    )
    mock_api.get("/rt/all", body=message_bytes, content_type=PB)


async def test_start_time_prediction_and_cancellation_hit_one_repetition(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The matched fixture predicts F1's 06:10:00 repetition (+120s at S1)
    and cancels its 06:20:00 repetition: exactly F1#22200 turns realtime,
    exactly F1#22800 disappears, and every sibling stays schedule-only.
    """
    _mock_catalog_with_rt(mock_api, TRIP_UPDATES_FREQ_MATCHED)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=2), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == [
        "F1#21600",
        "F1#22200",
        "F1#25200",
        "F1#25800",
    ]
    by_trip = {a.trip_id: a for a in arrivals}
    predicted = by_trip["F1#22200"]
    assert predicted.realtime is True
    assert predicted.delay_seconds == 120
    assert predicted.predicted_departure == F1_22200_S1_DEPARTURE + timedelta(
        seconds=120
    )
    assert predicted.scheduled_departure == F1_22200_S1_DEPARTURE
    assert predicted.vehicle_id == "V9"
    for trip_id in ("F1#21600", "F1#25200", "F1#25800"):
        sibling = by_trip[trip_id]
        assert sibling.realtime is False
        assert sibling.predicted_departure is None
        assert sibling.delay_seconds is None


async def test_start_time_matching_applies_to_upcoming_trips_overlay(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The origin-to-destination overlay resolves repetitions the same way:
    the predicted repetition gains an origin prediction, the canceled one
    is dropped, and siblings stay schedule-only.
    """
    _mock_catalog_with_rt(mock_api, TRIP_UPDATES_FREQ_MATCHED)
    handle = await feeds_client.get_transit_feed("mdb-100")
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=2), now_utc=NOW
    )
    assert [t.trip_id for t in trips] == [
        "F1#21600",
        "F1#22200",
        "F1#25200",
        "F1#25800",
    ]
    predicted = next(t for t in trips if t.trip_id == "F1#22200")
    assert predicted.realtime is True
    assert predicted.delay_seconds == 120
    assert predicted.predicted_departure == F1_22200_S1_DEPARTURE + timedelta(
        seconds=120
    )
    assert all(t.realtime is False for t in trips if t.trip_id != "F1#22200")


async def test_unmatched_predictions_attach_to_no_repetition(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A start_time-less prediction (repetition-ambiguous) and a start_time
    matching no materialized repetition (06:05:00) both attach nowhere:
    every repetition stays schedule-only.
    """
    _mock_catalog_with_rt(mock_api, TRIP_UPDATES_FREQ_UNMATCHED)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=2), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == F1_SYNTHETIC_IDS
    for arrival in arrivals:
        assert arrival.realtime is False
        assert arrival.predicted_departure is None
        assert arrival.delay_seconds is None


async def test_bare_cancellation_cancels_no_repetition(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Canceling the bare template trip id (no start_time) drops nothing:
    which repetition was meant is unknowable, and canceling all of them
    would be wrong.
    """
    _mock_catalog_with_rt(mock_api, TRIP_UPDATES_FREQ_BARE_CANCEL)
    # Second scripted RT response: this test drains one per RT-merging call.
    mock_api.get("/rt/all", body=TRIP_UPDATES_FREQ_BARE_CANCEL, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=2), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == F1_SYNTHETIC_IDS
    trips = await handle.upcoming_trips(
        "S1", "S3", lookahead=timedelta(hours=2), now_utc=NOW
    )
    assert [t.trip_id for t in trips] == F1_SYNTHETIC_IDS


async def test_tomorrow_repetition_cancellation_spares_todays(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The motivating start_date case, frequency variant: canceling the
    06:10:00 repetition FOR TOMORROW (start_time=06:10:00 plus
    start_date=2026-07-31) must not drop today's in-window F1#22200 row --
    the start_time alone aligns with it, but the date does not.
    """
    _mock_catalog_with_rt(mock_api, TRIP_UPDATES_FREQ_CANCELED_TOMORROW)
    handle = await feeds_client.get_transit_feed("mdb-100")
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], lookahead=timedelta(hours=2), now_utc=NOW
    )
    assert [a.trip_id for a in arrivals] == F1_SYNTHETIC_IDS
    assert all(a.realtime is False for a in arrivals)
