"""Tests for MobilityFeedsClient construction, catalog access, and close()."""

import asyncio
import shutil
import tempfile
from pathlib import Path

import aiohttp
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase import MobilityDatabasePublicCatalog
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


@pytest.mark.parametrize(
    "feed_id_template",
    ["{root}/mdb-200", "{root}/./mdb-200", "mdb-100/../mdb-200"],
)
async def test_purge_cache_rejects_ids_that_resolve_to_a_child(
    mock_api: MockApi, tmp_path: Path, feed_id_template: str
) -> None:
    """An id that reaches a feed by an absolute path or through ``..`` must
    be rejected even though it resolves to a direct child: the documented
    contract is a bare child name, and accepting these lets a caller purge
    a feed it cannot name directly.
    """
    root = tmp_path / "cache"
    for name in ("mdb-100", "mdb-200"):
        (root / name).mkdir(parents=True)
        (root / name / "static.db").write_bytes(b"x")
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    with pytest.raises(ValueError, match="escapes"):
        await client.purge_cache(feed_id_template.format(root=root))
    assert (root / "mdb-200" / "static.db").exists()
    assert (root / "mdb-100" / "static.db").exists()
    await client.close()


async def test_purge_cache_rejects_symlink_to_sibling_feed(
    mock_api: MockApi, tmp_path: Path
) -> None:
    """A symlinked cache entry pointing at a sibling feed must raise
    ValueError, not let rmtree follow the link (or fail with OSError).
    """
    root = tmp_path / "cache"
    (root / "mdb-200").mkdir(parents=True)
    (root / "mdb-200" / "static.db").write_bytes(b"x")
    (root / "evil").symlink_to(root / "mdb-200", target_is_directory=True)
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    with pytest.raises(ValueError, match="escapes"):
        await client.purge_cache("evil")
    assert (root / "mdb-200" / "static.db").exists()
    assert (root / "evil").is_symlink()
    await client.close()


@pytest.mark.parametrize("feed_id", ["", ".", "./", "nested/inner"])
async def test_purge_cache_rejects_non_child_feed_ids(
    mock_api: MockApi, tmp_path: Path, feed_id: str
) -> None:
    """A feed_id that does not name a direct child of cache_dir must raise
    rather than resolve to (and delete) the cache root itself: ``""`` and
    ``"."`` both resolve to cache_dir, which the containment check accepts
    because a path is relative to itself -- wiping every other feed's cache.
    """
    root = tmp_path / "cache"
    (root / "mdb-200").mkdir(parents=True)
    (root / "mdb-200" / "static.db").write_bytes(b"x")
    (root / "sentinel").write_bytes(b"s")
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    with pytest.raises(ValueError, match="escapes"):
        await client.purge_cache(feed_id)
    assert (root / "mdb-200" / "static.db").exists()
    assert (root / "sentinel").exists()
    await client.close()


async def test_purge_cache_is_idempotent(mock_api: MockApi, tmp_path: Path) -> None:
    root = tmp_path / "cache"
    (root / "mdb-100").mkdir(parents=True)
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    await client.purge_cache("mdb-100")
    await client.purge_cache("mdb-100")
    assert not (root / "mdb-100").exists()
    assert root.exists()
    await client.close()


@pytest.mark.parametrize("feed_id", ["/", "//", "/.", "/etc"])
async def test_purge_cache_rejects_root_like_feed_ids(
    mock_api: MockApi,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    feed_id: str,
) -> None:
    """Root-like ids must be rejected before any deletion happens. rmtree is
    stubbed to a recorder so a regression in the guard cannot delete real
    filesystem contents while this test runs.
    """
    removed: list[object] = []
    monkeypatch.setattr(shutil, "rmtree", lambda *args, **kwargs: removed.append(args))
    root = tmp_path / "cache"
    root.mkdir()
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    with pytest.raises(ValueError, match="escapes"):
        await client.purge_cache(feed_id)
    assert not removed
    await client.close()


async def test_purge_cache_before_the_cache_dir_exists(
    mock_api: MockApi, tmp_path: Path
) -> None:
    """The directory is made on the first build: purging before one is a
    no-op, as purging a feed with nothing cached is."""
    root = tmp_path / "never-built"
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    await client.purge_cache()
    await client.purge_cache("mdb-100")
    assert not root.exists()
    await client.close()


async def test_purge_cache_of_a_cache_dir_that_is_a_file_still_raises(
    mock_api: MockApi, tmp_path: Path
) -> None:
    """Only a missing directory is "nothing cached yet": a file in its place
    is a misconfiguration, and the filesystem error propagates."""
    root = tmp_path / "a-file"
    root.write_text("x")
    client = MobilityFeedsClient(
        "test-refresh-token", base_url=mock_api.url(), cache_dir=root
    )
    with pytest.raises(NotADirectoryError):
        await client.purge_cache()
    await client.close()


_ENTRY_KINDS = st.sampled_from(
    ["feed", "file", "link_to_outside_dir", "link_to_feed", "link_to_file", "dangling"]
)


@given(kinds=st.lists(_ENTRY_KINDS, max_size=6))
@settings(max_examples=60, deadline=None)
def test_purging_every_feed_removes_each_feed_directory_and_nothing_else(
    kinds: list[str],
) -> None:
    """A symlink to a directory in the cache made rmtree raise OSError,
    leaving every later feed behind: each real feed directory goes, and no
    symlink (nor what it points at) or stray file is touched."""
    with tempfile.TemporaryDirectory() as scratch:
        root, outside = Path(scratch) / "cache", Path(scratch) / "outside"
        root.mkdir()
        outside.mkdir()
        (outside / "keep").write_bytes(b"x")
        (root / "feed-anchor").mkdir()  # a feed a link can point at
        feeds = [root / "feed-anchor"]
        kept: list[Path] = []
        for number, kind in enumerate(kinds):
            entry = root / f"{number}-{kind}"
            if kind == "feed":
                entry.mkdir()
                (entry / "static.db").write_bytes(b"x")
                feeds.append(entry)
                continue
            if kind == "file":
                entry.write_bytes(b"x")
            else:
                entry.symlink_to(
                    {
                        "link_to_outside_dir": outside,
                        "link_to_feed": root / "feed-anchor",
                        "link_to_file": outside / "keep",
                        "dangling": outside / "missing",
                    }[kind]
                )
            kept.append(entry)

        async def purge() -> None:
            async with MobilityFeedsClient(cache_dir=root) as client:
                await client.purge_cache()

        asyncio.run(purge())
        assert not any(feed.exists() for feed in feeds)
        assert all(entry.is_symlink() or entry.is_file() for entry in kept)
        assert (outside / "keep").read_bytes() == b"x"


async def test_public_catalog_needs_no_token_and_shares_the_session() -> None:
    client = MobilityFeedsClient(request_timeout=7.0)
    public = client.public_catalog
    assert isinstance(public, MobilityDatabasePublicCatalog)
    assert client.public_catalog is public
    assert public._timeout.total == 7.0  # deliberate friend access
    assert public._get_session() is client._get_session()  # deliberate friend access
    await client.close()
    assert client._session is None  # owned session closed
    assert client.public_catalog is not public  # a new one after close()
    await client.close()


async def test_public_catalog_never_closes_an_injected_session() -> None:
    async with aiohttp.ClientSession() as session:
        client = MobilityFeedsClient(session=session)
        client.public_catalog  # noqa: B018 - creates it
        await client.close()
        assert not session.closed
