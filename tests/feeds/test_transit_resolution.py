"""Tests for sibling-feed resolution and static index acquisition."""

import pytest

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import StaticDataUnavailableError
from aiomobilitydatabase.feeds.models import StaticBuildProgress

from tests.feeds.fixtures import (
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    build_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

ZIP_PATH = "/hosted/mdb-100.zip"


def _mock_catalog_for_gtfs(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[with_base(GTFS_RT_FEED, base)]
    )
    mock_api.get(ZIP_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip")


async def test_resolve_from_gtfs_id(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog_for_gtfs(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert handle.static_feed_id == "mdb-100"
    assert [feed.id for feed in handle.rt_feeds] == ["mdb-200"]
    assert {stop.id for stop in handle.stops} == {"S1", "S2", "S3", "ST1"}
    assert {route.id for route in handle.routes} == {"R1", "R2"}
    assert handle.static_dataset is not None
    assert handle.static_dataset.id == "mdb-100-202607310000"


async def test_resolve_from_rt_id(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-200", payload=with_base(GTFS_RT_FEED, base))
    mock_api.get("/v1/gtfs_rt_feeds/mdb-200", payload=with_base(GTFS_RT_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[with_base(GTFS_RT_FEED, base)]
    )
    mock_api.get(ZIP_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip")
    handle = await feeds_client.get_transit_feed("mdb-200")
    assert handle.static_feed_id == "mdb-100"
    assert [feed.id for feed in handle.rt_feeds] == ["mdb-200"]


async def test_rt_feed_without_references_raises(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    orphan = {**with_base(GTFS_RT_FEED, base), "feed_references": []}
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-200", payload=orphan)
    mock_api.get("/v1/gtfs_rt_feeds/mdb-200", payload=orphan)
    with pytest.raises(StaticDataUnavailableError):
        await feeds_client.get_transit_feed("mdb-200")


async def test_gtfs_feed_without_dataset_raises(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    no_dataset = {**with_base(GTFS_FEED, base), "latest_dataset": None}
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=no_dataset)
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=no_dataset)
    mock_api.get("/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[])
    with pytest.raises(StaticDataUnavailableError):
        await feeds_client.get_transit_feed("mdb-100")


async def test_cache_hit_skips_second_download(
    mock_api: MockApi, feeds_client_cached: MobilityFeedsClient
) -> None:
    _mock_catalog_for_gtfs(mock_api)
    await feeds_client_cached.get_transit_feed("mdb-100")
    # Second create: catalog again, but NO second zip download.
    base = mock_api.url()
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[with_base(GTFS_RT_FEED, base)]
    )
    await feeds_client_cached.get_transit_feed("mdb-100")
    zip_requests = [req for req in mock_api.requests if req.path == ZIP_PATH]
    assert len(zip_requests) == 1


async def test_static_build_progress_reported(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog_for_gtfs(mock_api)
    events: list[StaticBuildProgress] = []
    await feeds_client.get_transit_feed("mdb-100", on_progress=events.append)
    assert {event.phase for event in events} == {"download", "index"}
    downloads = [event for event in events if event.phase == "download"]
    assert downloads[-1].done_bytes == downloads[-1].total_bytes
    index_events = [event for event in events if event.phase == "index"]
    assert index_events[-1].fraction == 1.0
    assert all(
        event.fraction is None or 0.0 <= event.fraction <= 1.0 for event in events
    )
