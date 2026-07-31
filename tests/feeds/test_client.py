"""Tests for MobilityFeedsClient construction, catalog access, and close()."""

from pathlib import Path

import aiohttp
import pytest

from aiomobilitydatabase.const import PROD_BASE_URL
from aiomobilitydatabase.feeds.client import MobilityFeedsClient

from tests.feeds.fixtures import TOKEN_RESPONSE
from tests.mock_server import MockApi


async def test_catalog_property_reaches_api(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload={"version": "1.0.0", "commit_hash": "abc"})
    metadata = await feeds_client.catalog.get_metadata()
    assert metadata.version == "1.0.0"


async def test_catalog_defaults_to_prod_base_url_when_unset() -> None:
    # No base_url passed: the inner catalog client must fall back to the
    # production API rather than requiring every caller to pass it through.
    client = MobilityFeedsClient("test-refresh-token")
    assert client.catalog._base_url == PROD_BASE_URL  # deliberate friend access
    await client.close()


async def test_owned_session_closed_and_recreated(mock_api: MockApi) -> None:
    client = MobilityFeedsClient("test-refresh-token", base_url=mock_api.url())
    first = client._get_session()
    await client.close()
    assert first.closed
    second = client._get_session()
    assert second is not first and not second.closed
    await client.close()


async def test_injected_session_never_closed(mock_api: MockApi) -> None:
    async with aiohttp.ClientSession() as session:
        client = MobilityFeedsClient(
            "test-refresh-token", session, base_url=mock_api.url()
        )
        await client.close()
        assert not session.closed


async def test_context_manager_closes(mock_api: MockApi) -> None:
    async with MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url()
    ) as client:
        session = client._get_session()
    assert session.closed


async def test_cache_dir_stored_as_path(mock_api: MockApi, tmp_path: object) -> None:
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=str(tmp_path)
    )
    assert client.cache_dir is not None
    assert client.cache_dir.name
    await client.close()


async def test_no_cache_dir(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    assert feeds_client.cache_dir is None


async def test_purge_cache(mock_api: MockApi, tmp_path: object) -> None:
    root = Path(str(tmp_path))
    for feed_id in ("mdb-100", "mdb-200"):
        (root / feed_id).mkdir()
        (root / feed_id / "static.db").write_bytes(b"x")
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    await client.purge_cache("mdb-100")
    assert not (root / "mdb-100").exists()
    assert (root / "mdb-200" / "static.db").exists()
    await client.purge_cache()
    assert not (root / "mdb-200").exists()
    await client.purge_cache("missing")  # no-op, must not raise
    await client.close()


async def test_purge_cache_without_cache_dir(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    await feeds_client.purge_cache()  # no cache_dir: silent no-op


async def test_purge_cache_rejects_escaping_feed_ids(
    mock_api: MockApi, tmp_path: object
) -> None:
    root = Path(str(tmp_path)) / "cache"
    root.mkdir()
    victim = Path(str(tmp_path)) / "victim"
    victim.mkdir()
    (victim / "data").write_bytes(b"x")
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    with pytest.raises(ValueError, match="escapes"):
        await client.purge_cache(str(victim))
    with pytest.raises(ValueError, match="escapes"):
        await client.purge_cache("../victim")
    assert (victim / "data").exists()
    await client.close()


async def test_purge_cache_rejects_symlink_escape(
    mock_api: MockApi, tmp_path: object
) -> None:
    """Task 15R-b item 4: a legitimately-named feed_id (no ``..`` or
    absolute-path tricks in the string itself) whose cache_dir ENTRY is a
    symlink pointing outside cache_dir must still be rejected -- confirming
    the existing ``resolve().is_relative_to()`` check (which follows
    symlinks before the containment comparison) already closes this, not
    just the string-based traversal case above.
    """
    root = Path(str(tmp_path)) / "cache"
    root.mkdir()
    victim = Path(str(tmp_path)) / "victim"
    victim.mkdir()
    (victim / "data").write_bytes(b"precious")
    (root / "mdb-100").symlink_to(victim, target_is_directory=True)
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    with pytest.raises(ValueError, match="escapes"):
        await client.purge_cache("mdb-100")
    assert victim.exists()
    assert (victim / "data").exists()
    await client.close()
