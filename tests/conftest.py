"""Shared test fixtures."""

from collections.abc import AsyncGenerator

import pytest

from aiomobilitydatabase.client import MobilityDatabaseClient
from tests.mock_server import MockApi


@pytest.fixture
async def mock_api() -> AsyncGenerator[MockApi, None]:
    """A loopback mock API server."""
    api = MockApi()
    await api.start()
    try:
        yield api
    finally:
        await api.stop()


@pytest.fixture
async def client(mock_api: MockApi) -> AsyncGenerator[MobilityDatabaseClient, None]:
    """A client that owns its session, pointed at the loopback mock server."""
    base_url = str(mock_api.server.make_url("")).rstrip("/")
    async with MobilityDatabaseClient(
        "test-refresh-token", base_url=base_url
    ) as mdb_client:
        yield mdb_client
