"""Feeds-specific fixtures: MobilityFeedsClient pointed at the mock server.

The ``mock_api`` fixture it depends on lives in the parent ``tests/conftest.py``
and cascades down automatically.
"""

from collections.abc import AsyncGenerator

import pytest

from aiomobilitydatabase.feeds.client import MobilityFeedsClient

from tests.mock_server import MockApi


@pytest.fixture
async def feeds_client(mock_api: MockApi) -> AsyncGenerator[MobilityFeedsClient, None]:
    """Yield a MobilityFeedsClient pointed at the mock server (no static cache)."""
    async with MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url()
    ) as feeds_client_instance:
        yield feeds_client_instance


@pytest.fixture
async def feeds_client_cached(
    mock_api: MockApi, tmp_path: object
) -> AsyncGenerator[MobilityFeedsClient, None]:
    """Yield a MobilityFeedsClient pointed at the mock server with a static cache."""
    async with MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=str(tmp_path)
    ) as feeds_client_instance:
        yield feeds_client_instance
