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
    by_id = {v.id: v for v in all_vehicles}
    # 2.x free_bike_status rental_uris pass through; absent stays None.
    assert by_id["b1"].rental_uris == {
        "android": "https://example.com/app?bike=b1&platform=android",
        "ios": "https://example.com/app?bike=b1&platform=ios",
    }
    assert by_id["b2"].rental_uris is None
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
    # 3.x vehicle_status rental_uris pass through identically to 2.x.
    assert vehicles[0].rental_uris == {
        "android": "https://example.com/app?vehicle=v1&platform=android",
        "ios": "https://example.com/app?vehicle=v1&platform=ios",
        "web": "https://example.com/vehicles/v1",
    }


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


async def test_vehicle_flags_coerce_like_station_flags(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A vehicle's is_reserved/is_disabled go through the same coercion as
    the station flags: bool("false") is True in Python, so a string flag is
    UNKNOWN (None) rather than a truthy string on a bool-typed field.
    """
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=with_base(GBFS_FEED, base))
    mock_api.get(
        "/gbfs/free_bike_status.json",
        payload={
            "ttl": 60,
            "data": {
                "bikes": [
                    {
                        "bike_id": "stringy",
                        "lat": 34.05,
                        "lon": -118.25,
                        "is_reserved": "false",
                        "is_disabled": "true",
                    },
                    {
                        "bike_id": "numeric",
                        "lat": 34.05,
                        "lon": -118.25,
                        "is_reserved": 1,
                        "is_disabled": 0,
                    },
                ]
            },
        },
    )
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    by_id = {v.id: v for v in await handle.get_vehicles()}
    assert by_id["stringy"].is_reserved is None
    assert by_id["stringy"].is_disabled is None
    assert by_id["numeric"].is_reserved is True
    assert by_id["numeric"].is_disabled is False
