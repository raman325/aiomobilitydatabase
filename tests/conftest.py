"""Shared test fixtures."""

from collections.abc import AsyncGenerator, Generator

import pytest
from aioresponses import aioresponses

from aiomobilitydatabase.client import MobilityDatabaseClient


@pytest.fixture
def mock_api() -> Generator[aioresponses, None, None]:
    """Mock all aiohttp requests."""
    with aioresponses() as mocked:
        yield mocked


@pytest.fixture
async def client() -> AsyncGenerator[MobilityDatabaseClient, None]:
    """A client that owns its session."""
    async with MobilityDatabaseClient("test-refresh-token") as mdb_client:
        yield mdb_client
