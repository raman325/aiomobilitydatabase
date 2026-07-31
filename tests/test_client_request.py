"""Tests for _request() error mapping."""

import aiohttp
import pytest
from aioresponses import aioresponses

from aiomobilitydatabase.client import MobilityDatabaseClient
from aiomobilitydatabase.exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseNotFoundError,
    MobilityDatabaseRateLimitError,
)
from tests.fixtures import TOKEN_RESPONSE

BASE = "https://api.mobilitydatabase.org"


def _mock_token(mock_api: aioresponses) -> None:
    mock_api.post(f"{BASE}/v1/tokens", payload=TOKEN_RESPONSE)


async def test_404_maps_to_not_found(
    mock_api: aioresponses, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get(f"{BASE}/v1/gtfs_feeds/mdb-9999999", status=404)
    with pytest.raises(MobilityDatabaseNotFoundError):
        await client.get_gtfs_feed("mdb-9999999")


async def test_429_maps_to_rate_limit(
    mock_api: aioresponses, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get(f"{BASE}/v1/metadata", status=429)
    with pytest.raises(MobilityDatabaseRateLimitError):
        await client.get_metadata()


async def test_500_maps_to_api_error(
    mock_api: aioresponses, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get(f"{BASE}/v1/metadata", status=500, body="oops")
    with pytest.raises(MobilityDatabaseApiError) as exc_info:
        await client.get_metadata()
    assert exc_info.value.status == 500


async def test_network_error_maps_to_connection_error(
    mock_api: aioresponses, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get(f"{BASE}/v1/metadata", exception=aiohttp.ClientConnectionError("boom"))
    with pytest.raises(MobilityDatabaseConnectionError):
        await client.get_metadata()


async def test_invalid_json_maps_to_connection_error(
    mock_api: aioresponses, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get(f"{BASE}/v1/metadata", body="not json", content_type="text/html")
    with pytest.raises(MobilityDatabaseConnectionError):
        await client.get_metadata()
