"""Tests for _request() error mapping."""

import pytest

from aiomobilitydatabase.client import MobilityDatabaseClient
from aiomobilitydatabase.exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseNotFoundError,
    MobilityDatabaseRateLimitError,
)

from tests.fixtures import TOKEN_RESPONSE
from tests.mock_server import MockApi


def _mock_token(mock_api: MockApi) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)


async def test_404_maps_to_not_found(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/gtfs_feeds/mdb-9999999", status=404)
    with pytest.raises(MobilityDatabaseNotFoundError):
        await client.get_gtfs_feed("mdb-9999999")


async def test_429_maps_to_rate_limit(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/metadata", status=429)
    with pytest.raises(MobilityDatabaseRateLimitError):
        await client.get_metadata()


async def test_500_maps_to_api_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/metadata", status=500, body="oops")
    with pytest.raises(MobilityDatabaseApiError) as exc_info:
        await client.get_metadata()
    assert exc_info.value.status == 500


async def test_network_error_maps_to_connection_error() -> None:
    """A transport-level connection failure maps to ConnectionError.

    Points at a loopback port nothing is listening on (connection refused)
    rather than an injected exception, so no mock server is needed here.
    """
    async with MobilityDatabaseClient(
        "test-refresh-token", base_url="http://127.0.0.1:1"
    ) as client:
        with pytest.raises(MobilityDatabaseConnectionError):
            await client.get_metadata()


async def test_invalid_json_maps_to_connection_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    _mock_token(mock_api)
    mock_api.get("/v1/metadata", body="not json", content_type="text/html")
    with pytest.raises(MobilityDatabaseConnectionError):
        await client.get_metadata()


async def test_network_error_after_token_maps_to_connection_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """Covers _request's own transport-error except clause.

    Distinct from test_network_error_maps_to_connection_error, which fails
    during the token fetch and never reaches _request's endpoint call.
    """
    _mock_token(mock_api)
    await client._async_ensure_token()  # cache a token before breaking the URL
    client._base_url = "http://127.0.0.1:1"
    with pytest.raises(MobilityDatabaseConnectionError):
        await client.get_metadata()
