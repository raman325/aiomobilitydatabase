"""MobilityDatabasePublicCatalog: fetching, caching, errors, filters."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncGenerator

import aiohttp
import pytest

from aiomobilitydatabase import public_catalog
from aiomobilitydatabase._catalog_csv import CatalogRows
from aiomobilitydatabase.exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseNotFoundError,
)
from aiomobilitydatabase.public_catalog import (
    GBFS_FEEDS_MAX_LIMIT,
    GTFS_FEEDS_MAX_LIMIT,
    GTFS_RT_FEEDS_MAX_LIMIT,
    MobilityDatabasePublicCatalog,
)

from tests.mock_server import MockApi
from tests.public_catalog_data import GBFS_ROW, GTFS_ROW, GTFS_RT_ROW, export

PATH = "/feeds_v2.csv"
BODY = export(GTFS_ROW, GTFS_RT_ROW, GBFS_ROW)


def serve(
    api: MockApi, body: bytes = BODY, headers: dict[str, str] | None = None
) -> None:
    api.get(PATH, body=body, content_type="text/csv", headers=headers)


@pytest.fixture
async def public(
    mock_api: MockApi,
) -> AsyncGenerator[MobilityDatabasePublicCatalog, None]:
    async with MobilityDatabasePublicCatalog(url=mock_api.url(PATH)) as catalog:
        yield catalog


def requests_to(api: MockApi) -> int:
    return sum(1 for request in api.requests if request.path == PATH)


async def test_it_reads_the_export_without_any_token(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api)
    feeds = await public.get_gbfs_feeds()
    assert [feed.id for feed in feeds] == ["gbfs-citibike"]
    (request,) = mock_api.requests
    assert "Authorization" not in request.headers


async def test_a_fresh_export_is_reused(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, headers={"Cache-Control": "public, max-age=3600", "ETag": '"v1"'})
    await public.get_gtfs_feeds()
    await public.get_gtfs_rt_feeds()
    assert requests_to(mock_api) == 1


async def test_a_stale_export_is_revalidated_with_its_etag(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, headers={"Cache-Control": "max-age=0", "ETag": '"v1"'})
    mock_api.get(
        PATH, status=304, headers={"ETag": '"v1"', "Cache-Control": "max-age=0"}
    )
    first = await public.get_gtfs_feeds()
    second = await public.get_gtfs_feeds()
    assert second == first
    assert mock_api.requests[1].headers["If-None-Match"] == '"v1"'


async def test_concurrent_callers_share_one_fetch(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, headers={"Cache-Control": "max-age=3600"})
    results = await asyncio.gather(*(public.get_gbfs_feeds() for _ in range(5)))
    assert all(result == results[0] for result in results)
    assert requests_to(mock_api) == 1


async def test_parsing_runs_off_the_event_loop(
    mock_api: MockApi,
    public: MobilityDatabasePublicCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    threads: list[threading.Thread] = []
    real = public_catalog.read_catalog

    def spy(body: bytes) -> CatalogRows:
        threads.append(threading.current_thread())
        return real(body)

    monkeypatch.setattr(public_catalog, "read_catalog", spy)
    serve(mock_api)
    await public.get_gtfs_feeds()
    assert threads
    assert threads[0] is not threading.main_thread()


@pytest.mark.parametrize("status", [404, 500, 302])
async def test_an_error_status_raises_api_error(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog, status: int
) -> None:
    mock_api.get(PATH, status=status, body=b"nope", content_type="text/plain")
    with pytest.raises(MobilityDatabaseApiError) as caught:
        await public.get_gtfs_feeds()
    assert caught.value.status == status


async def test_an_unreadable_export_raises_connection_error(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, body=b"provider\nA\n")
    with pytest.raises(MobilityDatabaseConnectionError, match="Public catalog"):
        await public.get_gtfs_feeds()


async def test_a_network_failure_raises_connection_error() -> None:
    async with MobilityDatabasePublicCatalog(
        url="http://127.0.0.1:9/feeds_v2.csv"
    ) as catalog:
        with pytest.raises(MobilityDatabaseConnectionError):
            await catalog.get_gtfs_feeds()


async def test_getters_find_a_feed_by_id_and_type(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, headers={"Cache-Control": "max-age=3600"})
    assert (await public.get_gtfs_feed("mdb-10")).id == "mdb-10"
    assert (await public.get_gtfs_rt_feed("mdb-20")).id == "mdb-20"
    assert (await public.get_gbfs_feed("gbfs-citibike")).id == "gbfs-citibike"
    with pytest.raises(MobilityDatabaseNotFoundError):
        await public.get_gbfs_feed("mdb-10")  # another type, as the API 404s
    with pytest.raises(MobilityDatabaseNotFoundError):
        await public.get_gtfs_feed("mdb-404")


async def test_an_injected_session_is_used_and_not_closed(mock_api: MockApi) -> None:
    serve(mock_api)
    async with aiohttp.ClientSession() as session:
        async with MobilityDatabasePublicCatalog(
            session, url=mock_api.url(PATH)
        ) as catalog:
            await catalog.get_gtfs_feeds()
        assert not session.closed


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/feeds.csv",
        "http://example.com/feeds_v2.csv",  # plain http off loopback
        "https://user:pw@example.com/feeds_v2.csv",
        "https://example.com/feeds_v2.csv#x",
        "not a url",
    ],
)
def test_an_unusable_url_raises_value_error(url: str) -> None:
    with pytest.raises(ValueError):
        MobilityDatabasePublicCatalog(url=url)


def test_construction_checks_like_the_api_client() -> None:
    with pytest.raises(TypeError):
        MobilityDatabasePublicCatalog(url=7)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        MobilityDatabasePublicCatalog(request_timeout=0)


def gbfs_systems(count: int) -> bytes:
    return export(*({**GBFS_ROW, "id": f"gbfs-{n:04}"} for n in range(count)))


async def test_the_default_limit_is_the_endpoints(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, body=gbfs_systems(GBFS_FEEDS_MAX_LIMIT + 1))
    feeds = await public.get_gbfs_feeds()
    assert len(feeds) == GBFS_FEEDS_MAX_LIMIT
    assert feeds[-1].id == f"gbfs-{GBFS_FEEDS_MAX_LIMIT - 1:04}"


async def test_the_largest_limit_is_taken(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, body=gbfs_systems(GBFS_FEEDS_MAX_LIMIT + 1))
    feeds = await public.get_gbfs_feeds(limit=GBFS_FEEDS_MAX_LIMIT, offset=1)
    assert len(feeds) == GBFS_FEEDS_MAX_LIMIT
    assert feeds[0].id == "gbfs-0001"


@pytest.mark.parametrize(
    ("method", "largest"),
    [
        ("get_gtfs_feeds", GTFS_FEEDS_MAX_LIMIT),
        ("get_gtfs_rt_feeds", GTFS_RT_FEEDS_MAX_LIMIT),
        ("get_gbfs_feeds", GBFS_FEEDS_MAX_LIMIT),
    ],
)
@pytest.mark.parametrize(
    "paging",
    [{"limit": "past"}, {"limit": -1}, {"offset": -1}],
)
async def test_out_of_range_paging_raises_value_error_before_any_request(
    mock_api: MockApi,
    public: MobilityDatabasePublicCatalog,
    method: str,
    largest: int,
    paging: dict[str, object],
) -> None:
    arguments = {
        name: largest + 1 if value == "past" else value
        for name, value in paging.items()
    }
    with pytest.raises(ValueError):
        await getattr(public, method)(**arguments)
    assert mock_api.requests == []
