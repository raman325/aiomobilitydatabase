"""Tests for the access-token lifecycle."""

import asyncio
from typing import Any

import pytest

from aiomobilitydatabase.client import MobilityDatabaseClient
from aiomobilitydatabase.exceptions import (
    MobilityDatabaseAuthenticationError,
    MobilityDatabaseConnectionError,
)

from tests.fixtures import METADATA, TOKEN_RESPONSE
from tests.mock_server import MockApi

EXPIRED_TOKEN_RESPONSE: dict[str, Any] = {
    **TOKEN_RESPONSE,
    "expiration_datetime_utc": "2020-01-01T00:00:00Z",
}
REFRESHED_TOKEN_RESPONSE: dict[str, Any] = {
    **TOKEN_RESPONSE,
    "access_token": "refreshed-token",
}
NAIVE_EXPIRATION_TOKEN_RESPONSE: dict[str, Any] = {
    **TOKEN_RESPONSE,
    "expiration_datetime_utc": "2030-01-01T00:00:00",
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


async def test_concurrent_401s_single_forced_refresh(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """N concurrent 401s on the same stale token trigger exactly one refresh.

    Without stale-token dedupe, every one of the 5 requests would force its
    own token POST once it acquires the lock -- only 2 are ever queued here,
    so the bug would exhaust the queue (599) and raise instead of succeeding.
    """
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    for _ in range(5):
        mock_api.get("/v1/metadata", status=401)
    mock_api.post("/v1/tokens", payload=REFRESHED_TOKEN_RESPONSE)
    for _ in range(5):
        mock_api.get("/v1/metadata", payload=METADATA)
    await asyncio.gather(*(client.get_metadata() for _ in range(5)))
    token_requests = [r for r in mock_api.requests if r.path == "/v1/tokens"]
    assert len(token_requests) == 2


async def test_token_invalid_json_maps_to_connection_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """Corrupt JSON from the token endpoint maps to ConnectionError."""
    mock_api.post(
        "/v1/tokens", status=200, body="not json", content_type="application/json"
    )
    with pytest.raises(MobilityDatabaseConnectionError):
        await client.get_metadata()


async def test_naive_expiration_coerced_to_utc(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """A token response with a naive expiration datetime is coerced to UTC."""
    mock_api.post("/v1/tokens", payload=NAIVE_EXPIRATION_TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    await client.get_metadata()
    assert client._token_expiration is not None
    assert client._token_expiration.tzinfo is not None
