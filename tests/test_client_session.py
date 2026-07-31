"""Tests for client construction, session ownership, and close()."""

import aiohttp
from aioresponses import aioresponses

from aiomobilitydatabase.client import MobilityDatabaseClient
from tests.fixtures import METADATA, TOKEN_RESPONSE


async def test_owned_session_closed_on_close(mock_api: aioresponses) -> None:
    client = MobilityDatabaseClient("test-refresh-token")
    mock_api.post("https://api.mobilitydatabase.org/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("https://api.mobilitydatabase.org/v1/metadata", payload=METADATA)
    await client.get_metadata()  # forces lazy session creation
    session = client._session
    assert session is not None
    await client.close()
    assert session.closed


async def test_injected_session_not_closed(mock_api: aioresponses) -> None:
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


async def test_context_manager_closes(mock_api: aioresponses) -> None:
    mock_api.post("https://api.mobilitydatabase.org/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("https://api.mobilitydatabase.org/v1/metadata", payload=METADATA)
    async with MobilityDatabaseClient("test-refresh-token") as client:
        await client.get_metadata()
        session = client._session
    assert session is not None
    assert session.closed
