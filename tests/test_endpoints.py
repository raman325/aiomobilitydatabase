"""Tests for endpoint methods: URL construction, params, and return types."""

from datetime import UTC, datetime

from aiomobilitydatabase.client import MobilityDatabaseClient
from aiomobilitydatabase.models import (
    BoundingFilterMethod,
    DataType,
    EntityType,
    FeedStatus,
    GtfsFeed,
    License,
    SortOrder,
)
from tests.fixtures import (
    AVAILABILITY_RESPONSE,
    GBFS_FEED,
    GTFS_DATASET,
    GTFS_FEED,
    GTFS_RT_FEED,
    LICENSE,
    LICENSE_WITH_RULES,
    LOCATION_SEARCH_RESPONSE,
    MATCHING_LICENSE,
    SEARCH_RESPONSE,
    TOKEN_RESPONSE,
)
from tests.mock_server import MockApi, RecordedRequest


def _mock_token(mock_api: MockApi) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)


def _last_request(mock_api: MockApi, path: str) -> RecordedRequest:
    return next(r for r in mock_api.requests if r.path == path)


async def test_get_feeds(mock_api: MockApi, client: MobilityDatabaseClient) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/feeds", payload=[GTFS_FEED])
    feeds = await client.get_feeds(limit=5, status=FeedStatus.ACTIVE, is_official=True)
    assert len(feeds) == 1
    assert feeds[0].id == "mdb-1210"
    req = _last_request(mock_api, "/v1/feeds")
    assert req.query == {"limit": "5", "status": "active", "is_official": "true"}


async def test_get_feed(mock_api: MockApi, client: MobilityDatabaseClient) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/feeds/mdb-1210", payload=GTFS_FEED)
    feed = await client.get_feed("mdb-1210")
    assert feed.provider is not None
    req = _last_request(mock_api, "/v1/feeds/mdb-1210")
    assert req.query == {}


async def test_get_gtfs_feeds_bounding_box(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gtfs_feeds", payload=[GTFS_FEED])
    feeds = await client.get_gtfs_feeds(
        dataset_latitudes=(33.5, 34.5),
        dataset_longitudes=(-118.9, -118.1),
        bounding_filter_method=BoundingFilterMethod.COMPLETELY_ENCLOSED,
    )
    assert isinstance(feeds[0], GtfsFeed)
    req = _last_request(mock_api, "/v1/gtfs_feeds")
    assert req.query == {
        "dataset_latitudes": "33.5,34.5",
        "dataset_longitudes": "-118.9,-118.1",
        "bounding_filter_method": "completely_enclosed",
    }


async def test_get_gtfs_feed(mock_api: MockApi, client: MobilityDatabaseClient) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gtfs_feeds/mdb-1210", payload=GTFS_FEED)
    feed = await client.get_gtfs_feed("mdb-1210")
    assert feed.latest_dataset is not None


async def test_get_gtfs_rt_feeds_entity_types(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gtfs_rt_feeds", payload=[GTFS_RT_FEED])
    feeds = await client.get_gtfs_rt_feeds(
        entity_types=[EntityType.VEHICLE_POSITIONS, EntityType.TRIP_UPDATES]
    )
    assert feeds[0].entity_types == [
        EntityType.VEHICLE_POSITIONS,
        EntityType.TRIP_UPDATES,
    ]
    req = _last_request(mock_api, "/v1/gtfs_rt_feeds")
    assert req.query == {"entity_types": "vp,tu"}


async def test_get_gtfs_rt_feed(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gtfs_rt_feeds/mdb-1211", payload=GTFS_RT_FEED)
    feed = await client.get_gtfs_rt_feed("mdb-1211")
    assert feed.id == "mdb-1211"


async def test_get_gbfs_feeds(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gbfs_feeds", payload=[GBFS_FEED])
    feeds = await client.get_gbfs_feeds(system_id="system-1234")
    assert feeds[0].system_id == "system-1234"
    req = _last_request(mock_api, "/v1/gbfs_feeds")
    assert req.query == {"system_id": "system-1234"}


async def test_get_gbfs_feed(mock_api: MockApi, client: MobilityDatabaseClient) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gbfs_feeds/gbfs-citibike", payload=GBFS_FEED)
    feed = await client.get_gbfs_feed("gbfs-citibike")
    assert feed.provider_url == "https://www.citybikenyc.com/"


async def test_get_gtfs_feed_datasets(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gtfs_feeds/mdb-10/datasets", payload=[GTFS_DATASET])
    datasets = await client.get_gtfs_feed_datasets(
        "mdb-10", latest=True, downloaded_after=datetime(2023, 7, 1, tzinfo=UTC)
    )
    assert datasets[0].feed_id == "mdb-10"
    req = _last_request(mock_api, "/v1/gtfs_feeds/mdb-10/datasets")
    assert req.query == {
        "latest": "true",
        "downloaded_after": "2023-07-01T00:00:00+00:00",
    }


async def test_get_gtfs_feed_gtfs_rt_feeds(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gtfs_feeds/mdb-1210/gtfs_rt_feeds", payload=[GTFS_RT_FEED])
    feeds = await client.get_gtfs_feed_gtfs_rt_feeds("mdb-1210")
    assert feeds[0].feed_references == ["mdb-1210"]


async def test_get_gtfs_feed_availability(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gtfs_feeds/mdb-123/availability", payload=AVAILABILITY_RESPONSE)
    checked_after = datetime(2026, 5, 1, tzinfo=UTC)
    checked_before = datetime(2026, 5, 31, tzinfo=UTC)
    availability = await client.get_gtfs_feed_availability(
        "mdb-123",
        checked_after=checked_after,
        checked_before=checked_before,
        sort=SortOrder.ASC,
    )
    assert availability.total == 42
    assert availability.checks[0].success is True
    req = _last_request(mock_api, "/v1/gtfs_feeds/mdb-123/availability")
    assert req.query == {
        "from": "2026-05-01T00:00:00+00:00",
        "to": "2026-05-31T00:00:00+00:00",
        "sort": "asc",
    }


async def test_get_dataset_gtfs(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/datasets/gtfs/mdb-10-202402080058", payload=GTFS_DATASET)
    dataset = await client.get_dataset_gtfs("mdb-10-202402080058")
    assert dataset.id == "mdb-10-202402080058"


async def test_search_feeds(mock_api: MockApi, client: MobilityDatabaseClient) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/search", payload=SEARCH_RESPONSE)
    results = await client.search_feeds(
        search_query="new york",
        limit=10,
        statuses=[FeedStatus.ACTIVE, FeedStatus.INACTIVE],
        data_types=[DataType.GTFS, DataType.GTFS_RT],
        is_official=True,
    )
    assert results.total == 1
    assert results.results[0].data_type is DataType.GTFS
    req = _last_request(mock_api, "/v1/search")
    assert req.query == {
        "search_query": "new york",
        "limit": "10",
        "status": "active,inactive",
        "data_type": "gtfs,gtfs_rt",
        "is_official": "true",
    }


async def test_get_locations(mock_api: MockApi, client: MobilityDatabaseClient) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/locations", payload=LOCATION_SEARCH_RESPONSE)
    results = await client.get_locations(search_query="montreal", country_code="CA")
    assert results.results[0].name == "Montréal"
    req = _last_request(mock_api, "/v1/locations")
    assert req.query == {"search_query": "montreal", "country_code": "CA"}


async def test_get_licenses(mock_api: MockApi, client: MobilityDatabaseClient) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/licenses", payload=[LICENSE])
    licenses = await client.get_licenses(limit=10)
    assert isinstance(licenses[0], License)
    req = _last_request(mock_api, "/v1/licenses")
    assert req.query == {"limit": "10"}


async def test_get_license(mock_api: MockApi, client: MobilityDatabaseClient) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/licenses/0BSD", payload=LICENSE_WITH_RULES)
    license_ = await client.get_license("0BSD")
    assert license_.license_rules is not None


async def test_get_matching_licenses(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.post("/v1/licenses:match", payload=[MATCHING_LICENSE])
    matches = await client.get_matching_licenses(
        "https://creativecommons.org/licenses/by/4.0/deed.nl"
    )
    assert matches[0].spdx_id == "CC-BY-4.0"
    req = _last_request(mock_api, "/v1/licenses:match")
    assert req.json == {
        "license_url": "https://creativecommons.org/licenses/by/4.0/deed.nl"
    }
