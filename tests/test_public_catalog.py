"""MobilityDatabasePublicCatalog: fetching, caching, errors, filters."""

from __future__ import annotations

import asyncio
import itertools
import math
import random
import re
import socket
import threading
import time
from collections.abc import AsyncGenerator

import aiohttp
import pytest

from aiomobilitydatabase import public_catalog
from aiomobilitydatabase._catalog_csv import CatalogRows
from aiomobilitydatabase.exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseNotFoundError,
    MobilityDatabaseRateLimitError,
)
from aiomobilitydatabase.public_catalog import (
    _GBFS_FEEDS_MAX_LIMIT,
    _GTFS_FEEDS_MAX_LIMIT,
    _GTFS_RT_FEEDS_MAX_LIMIT,
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
    assert second[0] is first[0]  # the stored parse, not a new one
    assert mock_api.requests[1].headers["If-None-Match"] == '"v1"'


async def test_concurrent_callers_share_one_fetch(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api)  # not reusable: only sharing the one request answers all
    results = await asyncio.gather(*(public.get_gbfs_feeds() for _ in range(5)))
    assert all(result[0] is results[0][0] for result in results)
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


def free_port() -> int:
    """Return a loopback port nothing listens on (bound, then released)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


async def test_a_network_failure_raises_connection_error() -> None:
    async with MobilityDatabasePublicCatalog(
        url=f"http://127.0.0.1:{free_port()}/feeds_v2.csv"
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
    serve(mock_api, body=gbfs_systems(_GBFS_FEEDS_MAX_LIMIT + 1))
    feeds = await public.get_gbfs_feeds()
    assert len(feeds) == _GBFS_FEEDS_MAX_LIMIT
    assert feeds[-1].id == f"gbfs-{_GBFS_FEEDS_MAX_LIMIT - 1:04}"


async def test_the_largest_limit_is_taken(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, body=gbfs_systems(_GBFS_FEEDS_MAX_LIMIT + 1))
    feeds = await public.get_gbfs_feeds(limit=_GBFS_FEEDS_MAX_LIMIT, offset=1)
    assert len(feeds) == _GBFS_FEEDS_MAX_LIMIT
    assert feeds[0].id == "gbfs-0001"


@pytest.mark.parametrize(
    ("method", "largest"),
    [
        ("get_gtfs_feeds", _GTFS_FEEDS_MAX_LIMIT),
        ("get_gtfs_rt_feeds", _GTFS_RT_FEEDS_MAX_LIMIT),
        ("get_gbfs_feeds", _GBFS_FEEDS_MAX_LIMIT),
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


@pytest.mark.parametrize("method", ["get_gtfs_feeds", "get_gbfs_feeds"])
@pytest.mark.parametrize("name", ["dataset_latitudes", "dataset_longitudes"])
@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
async def test_a_lone_non_finite_range_raises_value_error_before_any_request(
    mock_api: MockApi,
    public: MobilityDatabasePublicCatalog,
    method: str,
    name: str,
    bad: float,
) -> None:
    # The client encodes every given parameter, so one range alone is checked.
    with pytest.raises(ValueError, match="a query parameter is no finite number"):
        await getattr(public, method)(**{name: (bad, 1.0)})
    assert mock_api.requests == []


async def test_a_304_to_an_unconditional_request_raises_api_error(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    mock_api.get(PATH, status=304, headers={"ETag": '"v1"'})
    with pytest.raises(MobilityDatabaseApiError) as caught:
        await public.get_gtfs_feeds()
    assert caught.value.status == 304


async def test_rate_limiting_raises_rate_limit_error(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    mock_api.get(PATH, status=429, body=b"slow down", content_type="text/plain")
    with pytest.raises(MobilityDatabaseRateLimitError):
        await public.get_gtfs_feeds()


async def test_a_missing_export_is_an_api_error_not_a_missing_feed(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    mock_api.get(PATH, status=404, body=b"nope", content_type="text/plain")
    with pytest.raises(MobilityDatabaseApiError) as caught:
        await public.get_gtfs_feed("mdb-10")
    assert not isinstance(caught.value, MobilityDatabaseNotFoundError)
    assert caught.value.status == 404


async def test_an_error_is_not_cached(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    mock_api.get(PATH, status=500, body=b"down", content_type="text/plain")
    serve(mock_api)
    with pytest.raises(MobilityDatabaseApiError):
        await public.get_gtfs_feeds()
    assert [feed.id for feed in await public.get_gtfs_feeds()] == ["mdb-10"]
    assert requests_to(mock_api) == 2


async def test_a_cancelled_caller_leaves_the_shared_fetch_running(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api)
    leaving = asyncio.create_task(public.get_gtfs_feeds())
    staying = asyncio.create_task(public.get_gtfs_feeds())
    await asyncio.sleep(0)  # both are waiting on the one fetch now
    assert not leaving.done()
    leaving.cancel()
    assert [feed.id for feed in await staying] == ["mdb-10"]
    assert leaving.cancelled()
    assert requests_to(mock_api) == 1


async def test_a_closed_injected_session_raises_runtime_error(
    mock_api: MockApi,
) -> None:
    session = aiohttp.ClientSession()
    await session.close()
    catalog = MobilityDatabasePublicCatalog(session, url=mock_api.url(PATH))
    with pytest.raises(RuntimeError):
        await catalog.get_gtfs_feeds()
    assert mock_api.requests == []


@pytest.mark.parametrize(
    "getter", ["get_gtfs_feed", "get_gtfs_rt_feed", "get_gbfs_feed"]
)
@pytest.mark.parametrize(
    ("feed_id", "error"),
    [
        ("", ValueError),
        (".", ValueError),
        ("..", ValueError),
        ("mdb-\ud800", ValueError),  # a lone surrogate is no text
        (7, TypeError),
    ],
)
async def test_an_id_the_client_rejects_is_rejected_before_any_request(
    mock_api: MockApi,
    public: MobilityDatabasePublicCatalog,
    getter: str,
    feed_id: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        await getattr(public, getter)(feed_id)
    assert mock_api.requests == []


def gbfs_by(provider: str, feed_id: str) -> dict[str, str]:
    return {**GBFS_ROW, "id": feed_id, "provider": provider}


MATCHING = export(
    gbfs_by("Bay Area", "bay-area"),
    gbfs_by("BAYWHEELS", "baywheels"),
    gbfs_by("Metro_Bus", "metro-underscore"),
    gbfs_by("MetroXBus", "metro-x"),
    gbfs_by("50% Transit", "fifty"),
    gbfs_by("Ends\\", "backslash"),
    gbfs_by("", "no-provider"),
)


@pytest.mark.parametrize(
    ("needle", "expected"),
    [
        ("bay", {"bay-area", "baywheels"}),  # a substring, any case
        ("  BAY  ", {"bay-area", "baywheels"}),  # trimmed first
        ("b%a", {"bay-area"}),  # with a %: as given, so anchored
        ("metro_bus", {"metro-underscore", "metro-x"}),  # _: any one character
        ("metro\\_bus", {"metro-underscore"}),  # \_: a literal _
        ("50\\%%", {"fifty"}),  # \%: a literal %
        ("\\%", set()),  # anchored: only a provider that is just "%"
        ("e%\\", {"backslash"}),  # a trailing lone backslash: literal
        (
            "%",
            {
                "bay-area",
                "baywheels",
                "metro-underscore",
                "metro-x",
                "fifty",
                "backslash",
            },
        ),
        (
            "   ",
            {
                "bay-area",
                "baywheels",
                "metro-underscore",
                "metro-x",
                "fifty",
                "backslash",
                "no-provider",
            },
        ),
    ],
)
async def test_provider_matches_as_the_apis_ilike(
    mock_api: MockApi,
    public: MobilityDatabasePublicCatalog,
    needle: str,
    expected: set[str],
) -> None:
    serve(mock_api, body=MATCHING)
    feeds = await public.get_gbfs_feeds(provider=needle)
    assert {feed.id for feed in feeds} == expected


async def test_producer_url_matches_as_the_apis_ilike(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    serve(mock_api, headers={"Cache-Control": "max-age=3600"})
    feeds = await public.get_gtfs_feeds(producer_url="EXAMPLE.COM/SUB_AY")
    assert [feed.id for feed in feeds] == ["mdb-10"]
    assert await public.get_gtfs_feeds(producer_url="%example.com") == []  # anchored
    anchored = await public.get_gtfs_feeds(producer_url="https://%.zip")
    assert [feed.id for feed in anchored] == ["mdb-10"]


async def test_a_cache_hit_opens_no_session(mock_api: MockApi) -> None:
    serve(mock_api, headers={"Cache-Control": "max-age=3600"})
    catalog = MobilityDatabasePublicCatalog(url=mock_api.url(PATH))
    await catalog.get_gtfs_feeds()
    await catalog.close()
    assert [feed.id for feed in await catalog.get_gtfs_feeds()] == ["mdb-10"]
    assert catalog._session is None
    assert requests_to(mock_api) == 1


def reference_ilike(value: str) -> re.Pattern[str]:
    """The plain translation (%: .*, _: .), exponential but evidently right."""
    if "%" not in value:
        value = f"%{value}%"
    parts: list[str] = []
    characters = iter(value.lower())
    for character in characters:
        if character == "\\":
            parts.append(re.escape(next(characters, "\\")))
        else:
            parts.append({"%": ".*", "_": "."}.get(character) or re.escape(character))
    return re.compile("".join(parts), re.DOTALL)


ALPHABET = "ab%_\\\n"


def short_texts(longest: int) -> list[str]:
    return [
        "".join(letters)
        for length in range(longest + 1)
        for letters in itertools.product(ALPHABET, repeat=length)
    ]


def test_ilike_matches_the_plain_translation_on_every_short_pair() -> None:
    texts = short_texts(3)
    for needle in short_texts(4):
        fast, plain = public_catalog._ilike(needle), reference_ilike(needle)
        for text in texts:
            assert bool(fast.fullmatch(text)) == bool(plain.fullmatch(text)), (
                needle,
                text,
            )


def test_ilike_matches_the_plain_translation_on_random_pairs() -> None:
    chance = random.Random(20261118)
    for _ in range(3000):
        needle = "".join(chance.choices(ALPHABET, k=chance.randint(0, 8)))
        fast, plain = public_catalog._ilike(needle), reference_ilike(needle)
        for _ in range(10):
            text = "".join(chance.choices(ALPHABET, k=chance.randint(0, 12)))
            assert bool(fast.fullmatch(text)) == bool(plain.fullmatch(text)), (
                needle,
                text,
            )


async def test_a_wildcard_heavy_filter_runs_in_linear_time(
    mock_api: MockApi, public: MobilityDatabasePublicCatalog
) -> None:
    long_url = "https://example.com/" + "a" * 2000
    row = {**GTFS_ROW, "urls.direct_download": long_url}
    serve(mock_api, body=export(row), headers={"Cache-Control": "max-age=3600"})
    await public.get_gtfs_feeds()  # fetched and parsed outside the timing
    started = time.perf_counter()
    feeds = await public.get_gtfs_feeds(producer_url="%_%_%_%_%_%_%_%_%#")
    assert time.perf_counter() - started < 0.5
    assert feeds == []
