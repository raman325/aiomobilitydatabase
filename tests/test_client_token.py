"""Tests for the access-token lifecycle."""

import asyncio
from typing import Any

import pytest

from aiomobilitydatabase.client import MobilityDatabaseClient
from aiomobilitydatabase.exceptions import MobilityDatabaseAuthenticationError
from tests.fixtures import METADATA, TOKEN_RESPONSE
from tests.mock_server import MockApi

EXPIRED_TOKEN_RESPONSE: dict[str, Any] = {
    **TOKEN_RESPONSE,
    "expiration_datetime_utc": "2020-01-01T00:00:00Z",
}


async def test_lazy_token_fetch(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """No token request happens until the first API call."""
    assert client._access_token is None
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    await client.get_metadata()
    assert client._access_token == "test-access-token"


async def test_token_reused_while_valid(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """A valid token is reused; the mock server would 599 on a second POST."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    mock_api.get("/v1/metadata", payload=METADATA)
    await client.get_metadata()
    await client.get_metadata()


async def test_expired_token_refreshed(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """An expired token triggers a new token POST before the next request."""
    mock_api.post("/v1/tokens", payload=EXPIRED_TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    await client.get_metadata()
    await client.get_metadata()


async def test_single_flight_refresh(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """Concurrent first requests share one token POST."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    for _ in range(5):
        mock_api.get("/v1/metadata", payload=METADATA)
    await asyncio.gather(*(client.get_metadata() for _ in range(5)))


async def test_invalid_refresh_token_raises(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """The live API returns 500 for a bad refresh token; must raise auth error."""
    mock_api.post(
        "/v1/tokens",
        status=500,
        payload={"error": "Error generating access token."},
    )
    with pytest.raises(MobilityDatabaseAuthenticationError):
        await client.get_metadata()


async def test_unauthorized_retry_then_success(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """A 401 triggers one forced token refresh and a retry."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", status=401)
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    metadata = await client.get_metadata()
    assert metadata.version == "1.0.0"


async def test_unauthorized_twice_raises(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """A second 401 after refresh raises instead of looping."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", status=401)
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", status=401)
    with pytest.raises(MobilityDatabaseAuthenticationError):
        await client.get_metadata()
