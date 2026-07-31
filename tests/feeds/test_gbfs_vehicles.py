"""Tests for GBFS free-floating vehicles and circular-zone filtering."""

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.geo import Circle

from tests.feeds.fixtures import (
    FREE_BIKE_STATUS_23,
    GBFS_FEED,
    TOKEN_RESPONSE,
    VEHICLE_STATUS_30,
    with_base,
)
from tests.mock_server import MockApi


async def test_vehicles_23_and_zone_filter(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=with_base(GBFS_FEED, base))
    mock_api.get("/gbfs/free_bike_status.json", payload=FREE_BIKE_STATUS_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    all_vehicles = await handle.get_vehicles()
    assert {v.id for v in all_vehicles} == {"b1", "b2"}
    zoned = await handle.get_vehicles(
        zone=Circle(latitude=34.05, longitude=-118.25, radius_m=500)
    )
    assert [v.id for v in zoned] == ["b1"]  # b2 is ~17 km away


async def test_vehicles_30_uses_vehicle_status(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    feed_30 = with_base(GBFS_FEED, base)
    feed_30["versions"] = [
        {
            "version": "3.0",
            "source": "gbfs_versions",
            "endpoints": [
                {"name": "vehicle_status", "url": f"{base}/gbfs3/vehicle_status.json"},
            ],
        }
    ]
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=feed_30)
    mock_api.get("/gbfs3/vehicle_status.json", payload=VEHICLE_STATUS_30)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    vehicles = await handle.get_vehicles()
    assert vehicles[0].id == "v1"
    assert vehicles[0].vehicle_type_id == "scooter"
    assert vehicles[0].current_range_m == 12_000.0


async def test_vehicles_absent_endpoint_returns_empty(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    stations_only = with_base(GBFS_FEED, base)
    stations_only["versions"][0]["endpoints"] = [
        {"name": "station_information", "url": f"{base}/gbfs/station_information.json"},
    ]
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=stations_only)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert await handle.get_vehicles() == []
