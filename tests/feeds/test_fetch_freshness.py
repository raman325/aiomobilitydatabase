"""Producer freshness end to end: realtime and GBFS handles over HTTP."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import SourceConnectionError
from aiomobilitydatabase.feeds.models import ArrivalsQuery

from tests.feeds.fixtures import (
    GBFS_FEED,
    GTFS_FEED,
    GTFS_RT_FEED,
    SYSTEM_INFO_23,
    TOKEN_RESPONSE,
    VEHICLE_POSITIONS,
    build_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

PB = "application/octet-stream"
NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)  # Thursday 07:45 PDT
RT = "/rt/all"


@dataclass
class _Clock:
    now: float = 1000.0

    def __call__(self) -> float:
        return self.now


def _mock_transit(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[with_base(GTFS_RT_FEED, base)]
    )
    mock_api.get(
        "/hosted/mdb-100.zip",
        body=build_gtfs_zip_bytes(),
        content_type="application/zip",
    )


def _t1_delayed(delay: int, *, timestamp: int) -> bytes:
    """A TripUpdates message delaying today's T1 by ``delay`` seconds."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = timestamp
    entity = message.entity.add()
    entity.id = "t1"
    entity.trip_update.trip.trip_id = "T1"
    entity.trip_update.trip.start_date = "20260730"
    entity.trip_update.delay = delay
    return message.SerializeToString()


def _rt_hits(mock_api: MockApi) -> list[dict[str, str]]:
    return [r.headers for r in mock_api.requests if r.path == RT]


async def _t1_delay(handle: object) -> int | None:
    [board] = await handle.get_arrivals(  # type: ignore[attr-defined]
        [ArrivalsQuery(["S1"], limit=1)], start=NOW, end=NOW + timedelta(hours=2)
    )
    assert [row.trip_id for row in board] == ["T1"]
    return board[0].delay_seconds


async def test_realtime_within_max_age_costs_no_request_then_revalidates(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    mock_api.get(
        RT,
        body=_t1_delayed(60, timestamp=100),
        content_type=PB,
        headers={"Cache-Control": "max-age=30", "ETag": '"v1"'},
    )
    mock_api.get(RT, status=304, headers={"Cache-Control": "max-age=30"})
    handle = await feeds_client.get_transit_feed("mdb-100")
    clock = _Clock()
    handle._rt_cache._clock = clock
    assert await _t1_delay(handle) == 60
    clock.now += 29
    assert await _t1_delay(handle) == 60
    assert len(_rt_hits(mock_api)) == 1  # fresh: no request at all
    clock.now += 2
    assert await _t1_delay(handle) == 60  # 304: the stored parse
    hits = _rt_hits(mock_api)
    assert len(hits) == 2
    assert hits[1].get("If-None-Match") == '"v1"'
    clock.now += 29
    assert await _t1_delay(handle) == 60
    assert len(_rt_hits(mock_api)) == 2  # the 304 renewed the lifetime


async def test_no_cache_revalidates_every_call(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    mock_api.get(
        RT,
        body=_t1_delayed(60, timestamp=100),
        content_type=PB,
        headers={"Cache-Control": "no-cache, max-age=300", "ETag": '"v1"'},
    )
    mock_api.get(RT, status=304, headers={"Cache-Control": "no-cache"})
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 60
    hits = _rt_hits(mock_api)
    assert len(hits) == 2
    assert hits[1].get("If-None-Match") == '"v1"'


async def test_a_stale_cdn_copy_never_replaces_a_newer_message(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    mock_api.get(RT, body=_t1_delayed(60, timestamp=200), content_type=PB)
    # An edge that has not caught up serves an older snapshot...
    mock_api.get(RT, body=_t1_delayed(600, timestamp=100), content_type=PB)
    # ...and the next request (the stored copy was kept but marked stale)
    # reaches a current one.
    mock_api.get(RT, body=_t1_delayed(120, timestamp=300), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 120
    assert len(_rt_hits(mock_api)) == 3


async def test_rt_min_interval_is_an_opt_in_request_budget(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    for delay in (60, 120):  # no caching headers at all
        mock_api.get(RT, body=_t1_delayed(delay, timestamp=delay), content_type=PB)
    handle = await feeds_client.get_transit_feed(
        "mdb-100", rt_min_interval=timedelta(seconds=10)
    )
    clock = _Clock()
    handle._rt_cache._clock = clock
    assert await _t1_delay(handle) == 60
    clock.now += 9
    assert await _t1_delay(handle) == 60
    assert len(_rt_hits(mock_api)) == 1
    clock.now += 1
    assert await _t1_delay(handle) == 120
    assert len(_rt_hits(mock_api)) == 2


async def test_without_freshness_every_call_asks(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    for delay in (60, 120):
        mock_api.get(RT, body=_t1_delayed(delay, timestamp=delay), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 120


async def test_concurrent_calls_share_one_download(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """An arrivals coordinator and a vehicles coordinator firing together."""
    _mock_transit(mock_api)
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(_t1_delayed(60, timestamp=100))
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.ParseFromString(VEHICLE_POSITIONS)
    message.entity.extend(vehicles.entity)
    # One response scripted; a second request would get the mock's 599.
    mock_api.get(RT, body=message.SerializeToString(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    delay, positions = await asyncio.gather(_t1_delay(handle), handle.get_vehicles())
    assert delay == 60
    assert positions
    assert len(_rt_hits(mock_api)) == 1


# -- GBFS ------------------------------------------------------------------------

INFO = "/gbfs/system_information.json"


def _mock_gbfs(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=with_base(GBFS_FEED, base))


def _info(*, ttl: int, age: int = 0, name: str = "Test Bikes") -> dict[str, object]:
    updated = int(datetime.now(UTC).timestamp()) - age
    return {
        **SYSTEM_INFO_23,
        "ttl": ttl,
        "last_updated": updated,
        "data": {**SYSTEM_INFO_23["data"], "name": name},  # type: ignore[dict-item]
    }


def _info_hits(mock_api: MockApi) -> list[dict[str, str]]:
    return [r.headers for r in mock_api.requests if r.path == INFO]


async def test_gbfs_revalidates_with_its_etag(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, payload=_info(ttl=0), headers={"ETag": '"g1"'})
    mock_api.get(INFO, status=304)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert (await handle.get_system_info()).name == "Test Bikes"
    assert (await handle.get_system_info()).name == "Test Bikes"
    hits = _info_hits(mock_api)
    assert len(hits) == 2
    assert hits[1].get("If-None-Match") == '"g1"'


async def test_gbfs_ttl_counts_from_last_updated(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Updated 50s ago with ttl 60: 10s of freshness left, not 60."""
    _mock_gbfs(mock_api)
    mock_api.get(INFO, payload=_info(ttl=60, age=50))
    mock_api.get(INFO, payload=_info(ttl=60, name="Renamed"))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    clock = _Clock()
    handle._docs._clock = clock
    await handle.get_system_info()
    clock.now += 9
    assert (await handle.get_system_info()).name == "Test Bikes"
    assert len(_info_hits(mock_api)) == 1
    clock.now += 2
    assert (await handle.get_system_info()).name == "Renamed"


async def test_gbfs_the_shorter_declared_lifetime_wins(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, payload=_info(ttl=300), headers={"Cache-Control": "max-age=0"})
    mock_api.get(INFO, payload=_info(ttl=300, name="Renamed"))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    await handle.get_system_info()
    assert (await handle.get_system_info()).name == "Renamed"


async def test_gbfs_never_goes_back_to_an_older_document(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, payload=_info(ttl=0, age=0, name="Current"))
    mock_api.get(INFO, payload=_info(ttl=0, age=600, name="Stale edge"))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert (await handle.get_system_info()).name == "Current"
    assert (await handle.get_system_info()).name == "Current"


async def test_gbfs_unsolicited_304_is_a_connection_error(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, status=304)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    with pytest.raises(SourceConnectionError) as err:
        await handle.get_system_info()
    assert err.value.status == 304
