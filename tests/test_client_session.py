"""Tests for client construction, session ownership, and close()."""

import math
from typing import Any

import aiohttp
import pytest
from yarl import URL

from aiomobilitydatabase.client import MobilityDatabaseClient

from tests.fixtures import METADATA, TOKEN_RESPONSE
from tests.mock_server import MockApi


async def test_owned_session_closed_on_close(mock_api: MockApi) -> None:
    base_url = str(mock_api.server.make_url("")).rstrip("/")
    client = MobilityDatabaseClient("test-refresh-token", base_url=base_url)
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    await client.get_metadata()  # forces lazy session creation
    session = client._session
    assert session is not None
    await client.close()
    assert session.closed


async def test_injected_session_not_closed(mock_api: MockApi) -> None:
    async with aiohttp.ClientSession() as session:
        client = MobilityDatabaseClient("test-refresh-token", session)
        await client.close()
        assert not session.closed


async def test_owned_session_recreated_after_close() -> None:
    client = MobilityDatabaseClient("test-refresh-token")
    first = client._get_session()
    await client.close()
    second = client._get_session()
    assert second is not first
    assert not second.closed
    await client.close()


async def test_close_idempotent_and_safe_before_use() -> None:
    client = MobilityDatabaseClient("test-refresh-token")
    await client.close()  # never used: no session exists yet
    await client.close()  # second call must not raise


async def test_context_manager_closes(mock_api: MockApi) -> None:
    base_url = str(mock_api.server.make_url("")).rstrip("/")
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    async with MobilityDatabaseClient(
        "test-refresh-token", base_url=base_url
    ) as client:
        await client.get_metadata()
        session = client._session
    assert session is not None
    assert session.closed


async def test_unscoped_session_cookies_are_refused() -> None:
    """ClientSession(cookies=...) keeps them without a domain: they would
    ride on every request, to every origin."""
    async with aiohttp.ClientSession(cookies={"sid": "SECRET"}) as session:
        with pytest.raises(ValueError, match="cookies") as caught:
            MobilityDatabaseClient("t", session)
    assert "SECRET" not in str(caught.value)


async def test_cookies_a_server_scoped_are_fine(mock_api: MockApi) -> None:
    async with aiohttp.ClientSession() as session:
        session.cookie_jar.update_cookies({"sid": "x"}, URL("https://example.com/"))
        client = MobilityDatabaseClient("t", session, base_url=mock_api.url())
        mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
        mock_api.get("/v1/metadata", payload=METADATA)
        await client.get_metadata()


async def test_a_closed_injected_session_is_the_callers_error() -> None:
    """Not a connection error: retrying won't open it."""
    session = aiohttp.ClientSession()
    await session.close()
    client = MobilityDatabaseClient("t", session, base_url="http://127.0.0.1:9")
    with pytest.raises(RuntimeError, match="closed"):
        await client.get_metadata()


@pytest.mark.parametrize("token", [None, b"t", 1])
def test_a_refresh_token_that_isnt_text_is_refused(token: Any) -> None:
    with pytest.raises(TypeError, match="refresh_token"):
        MobilityDatabaseClient(token)


@pytest.mark.parametrize("timeout", [0, -1, math.nan, math.inf, -math.inf, 10**400])
def test_a_request_timeout_that_isnt_a_finite_positive_number_is_refused(
    timeout: float,
) -> None:
    with pytest.raises(ValueError, match="request_timeout"):
        MobilityDatabaseClient("t", request_timeout=timeout)


@pytest.mark.parametrize("timeout", [True, "5", None, b"5"])
def test_a_request_timeout_that_isnt_a_number_is_a_type_error(timeout: Any) -> None:
    with pytest.raises(TypeError, match="request_timeout"):
        MobilityDatabaseClient("t", request_timeout=timeout)


@pytest.mark.parametrize("timeout", [0.5, 5, 1e9])
def test_a_positive_request_timeout_is_taken(timeout: float) -> None:
    MobilityDatabaseClient("t", request_timeout=timeout)
