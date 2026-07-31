"""Tests for GBFS endpoint resolution, ttl caching, system info, stations."""

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.geo import Circle

from tests.feeds.fixtures import (
    GBFS_FEED,
    STATION_INFO_23,
    STATION_STATUS_23,
    SYSTEM_INFO_23,
    SYSTEM_INFO_30,
    TOKEN_RESPONSE,
    with_base,
)
from tests.mock_server import MockApi


def _mock_catalog(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=with_base(GBFS_FEED, base))


async def test_system_info(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/system_information.json", payload=SYSTEM_INFO_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    info = await handle.get_system_info()
    assert info.system_id == "test-bikes"
    assert info.name == "Test Bikes"


async def test_stations_merged(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    stations = {s.id: s for s in await handle.get_stations()}
    st1 = stations["st1"]
    assert st1.name == "Dock A"
    assert st1.capacity == 20
    assert st1.bikes_available == 5
    assert st1.docks_available == 15
    assert st1.is_renting is True
    assert st1.vehicle_types_available == {"bike": 4, "ebike": 1}
    st2 = stations["st2"]
    assert st2.is_renting is False
    assert st2.vehicle_types_available is None


async def test_stations_zone_filter(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    # st1 at (34.05,-118.25); st2 at (34.10,-118.20) ~7 km away.
    zoned = await handle.get_stations(
        zone=Circle(latitude=34.05, longitude=-118.25, radius_m=1000)
    )
    assert [station.id for station in zoned] == ["st1"]


async def test_ttl_micro_cache(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    await handle.get_stations()
    await handle.get_stations()  # within ttl: NO second fetch of either doc
    info_hits = [
        r for r in mock_api.requests if r.path == "/gbfs/station_information.json"
    ]
    status_hits = [
        r for r in mock_api.requests if r.path == "/gbfs/station_status.json"
    ]
    assert len(info_hits) == 1
    assert len(status_hits) == 1


async def test_gbfs_30_localized_name(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    feed_30 = with_base(GBFS_FEED, base)
    feed_30["versions"] = [
        {
            "version": "3.0",
            "source": "gbfs_versions",
            "endpoints": [
                {
                    "name": "system_information",
                    "url": f"{base}/gbfs3/system_information.json",
                },
                {"name": "vehicle_status", "url": f"{base}/gbfs3/vehicle_status.json"},
            ],
        },
        *feed_30["versions"],  # 2.3 also present: 3.0 must win
    ]
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=feed_30)
    mock_api.get("/gbfs3/system_information.json", payload=SYSTEM_INFO_30)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    info = await handle.get_system_info()
    assert info.system_id == "test-bikes-3"
    assert info.name == "Test Bikes 3"
