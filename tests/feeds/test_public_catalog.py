"""The public catalog: fetching, caching, errors, filters."""

from __future__ import annotations

import asyncio
import itertools
import math
import random
import re
import socket
import threading
import time
from collections.abc import AsyncGenerator, Awaitable, Sequence
from dataclasses import dataclass

import aiohttp
import pytest

from aiomobilitydatabase import BoundingFilterMethod, EntityType
from aiomobilitydatabase.const import DEV_BASE_URL, PROD_BASE_URL, QA_BASE_URL
from aiomobilitydatabase.exceptions import MobilityDatabaseNotFoundError
from aiomobilitydatabase.feeds import (
    FeedParseError,
    MobilityFeedsClient,
    PublicCatalog,
    SourceConnectionError,
    public_catalog,
)
from aiomobilitydatabase.feeds._catalog_csv import CatalogRows, PublicFeed
from aiomobilitydatabase.feeds.const import (
    PROD_PUBLIC_CATALOG_URL,
    QA_PUBLIC_CATALOG_URL,
)
from aiomobilitydatabase.feeds.public_catalog import (
    _GBFS_FEEDS_MAX_LIMIT,
    _GTFS_FEEDS_MAX_LIMIT,
    _GTFS_RT_FEEDS_MAX_LIMIT,
)

from tests.feeds.public_catalog_data import GBFS_ROW, GTFS_ROW, GTFS_RT_ROW, box, export
from tests.mock_server import MockApi

PATH = "/feeds_v2.csv"
BODY = export(GTFS_ROW, GTFS_RT_ROW, GBFS_ROW)


def gbfs_document(data: dict[str, object]) -> dict[str, object]:
    """A GBFS document, never fresh (ttl 0): each read is a request."""
    return {"last_updated": int(time.time()), "ttl": 0, "version": "2.3", "data": data}


@dataclass
class Clock:
    now: float = 1000.0

    def __call__(self) -> float:
        return self.now


def serve(
    api: MockApi, body: bytes = BODY, headers: dict[str, str] | None = None
) -> None:
    api.get(PATH, body=body, content_type="text/csv", headers=headers)


@pytest.fixture
async def client(mock_api: MockApi) -> AsyncGenerator[MobilityFeedsClient, None]:
    async with MobilityFeedsClient(public_catalog_url=mock_api.url(PATH)) as feeds:
        yield feeds


@pytest.fixture
async def public(client: MobilityFeedsClient) -> PublicCatalog:
    return client.public_catalog


def requests_to(api: MockApi) -> int:
    return sum(1 for request in api.requests if request.path == PATH)


async def test_it_reads_the_export_without_any_token(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api)
    feeds = await public.get_gbfs_feeds()
    assert [feed.id for feed in feeds] == ["gbfs-citibike"]
    (request,) = mock_api.requests
    assert "Authorization" not in request.headers


async def test_a_fresh_export_is_reused(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api, headers={"Cache-Control": "public, max-age=3600", "ETag": '"v1"'})
    await public.get_gtfs_feeds()
    await public.get_gtfs_rt_feeds()
    assert requests_to(mock_api) == 1


async def test_a_stale_export_is_revalidated_with_its_etag(
    mock_api: MockApi, public: PublicCatalog
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
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api)  # not reusable: only sharing the one request answers all
    results = await asyncio.gather(*(public.get_gbfs_feeds() for _ in range(5)))
    assert all(result[0] is results[0][0] for result in results)
    assert requests_to(mock_api) == 1


async def test_parsing_runs_off_the_event_loop(
    mock_api: MockApi,
    public: PublicCatalog,
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


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500, 204, 302])
async def test_a_status_without_an_export_raises_source_connection_error(
    mock_api: MockApi, public: PublicCatalog, status: int
) -> None:
    # As a GBFS document's: any error status (401/403 too), or no document.
    mock_api.get(PATH, status=status, body=b"nope", content_type="text/plain")
    with pytest.raises(SourceConnectionError) as caught:
        await public.get_gtfs_feed("mdb-10")
    assert caught.value.status == status
    assert caught.value.url == mock_api.url(PATH)


@pytest.mark.parametrize(
    "body",
    [b"provider\nA\n", b"\xff\xfe", b'id,data_type\nmdb-1,"gtfs"x\n'],
)
async def test_an_unreadable_export_raises_feed_parse_error(
    mock_api: MockApi, public: PublicCatalog, body: bytes
) -> None:
    serve(mock_api, body=body)
    with pytest.raises(FeedParseError, match="Public catalog unreadable") as caught:
        await public.get_gtfs_feeds()
    assert caught.value.url == mock_api.url(PATH)
    assert caught.value.__context__ is None


def free_port() -> int:
    """Return a loopback port nothing listens on (bound, then released)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


async def test_a_network_failure_raises_source_connection_error() -> None:
    url = f"http://127.0.0.1:{free_port()}/feeds_v2.csv"
    async with MobilityFeedsClient(public_catalog_url=url) as client:
        with pytest.raises(SourceConnectionError):
            await client.public_catalog.get_gtfs_feeds()


async def test_getters_find_a_feed_by_id_and_type(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api, headers={"Cache-Control": "max-age=3600"})
    assert (await public.get_gtfs_feed("mdb-10")).id == "mdb-10"
    assert (await public.get_gtfs_rt_feed("mdb-20")).id == "mdb-20"
    assert (await public.get_gbfs_feed("gbfs-citibike")).id == "gbfs-citibike"
    with pytest.raises(MobilityDatabaseNotFoundError):
        await public.get_gbfs_feed("mdb-10")  # another type, as the API 404s
    with pytest.raises(MobilityDatabaseNotFoundError):
        await public.get_gtfs_feed("mdb-404")


async def test_the_client_s_injected_session_is_used_and_not_closed(
    mock_api: MockApi,
) -> None:
    serve(mock_api)
    async with aiohttp.ClientSession() as session:
        client = MobilityFeedsClient(
            session=session, public_catalog_url=mock_api.url(PATH)
        )
        public = client.public_catalog
        await public.get_gtfs_feeds()
        assert client._get_session() is session  # deliberate friend access
        await client.close()
        assert not session.closed


async def test_after_close_a_new_catalog_fetches_on_a_new_session(
    mock_api: MockApi,
) -> None:
    serve(mock_api, headers={"Cache-Control": "max-age=3600"})
    serve(mock_api, headers={"Cache-Control": "max-age=3600"})
    client = MobilityFeedsClient(public_catalog_url=mock_api.url(PATH))
    first = client.public_catalog
    await first.get_gtfs_feeds()
    await client.close()
    second = client.public_catalog
    assert second is not first
    assert [feed.id for feed in await second.get_gtfs_feeds()] == ["mdb-10"]
    assert requests_to(mock_api) == 2  # the closed one's cache went with it
    await client.close()


async def test_a_catalog_kept_past_close_goes_on_as_a_gbfs_handle_does(
    mock_api: MockApi,
) -> None:
    """The feeds layer's handle convention: a handle kept past close() asks
    the client for its session again, which opens a new owned one."""
    serve(mock_api)
    serve(mock_api)
    feeds = [{"name": "system_information", "url": mock_api.url("/si.json")}]
    for _ in range(3):
        mock_api.get("/gbfs.json", payload=gbfs_document({"en": {"feeds": feeds}}))
        mock_api.get("/si.json", payload=gbfs_document({"system_id": "citi"}))
    client = MobilityFeedsClient(public_catalog_url=mock_api.url(PATH))
    handle = await client.get_gbfs_feed_from_url(mock_api.url("/gbfs.json"))
    kept = client.public_catalog
    await kept.get_gtfs_feeds()
    await handle.get_system_info()
    await client.close()
    assert client._session is None  # deliberate friend access
    assert (await handle.get_system_info()).system_id == "citi"
    assert [feed.id for feed in await kept.get_gtfs_feeds()] == ["mdb-10"]
    assert client._session is not None  # reopened, owned: close() closes it
    await client.close()
    assert client._session is None


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        (None, PROD_PUBLIC_CATALOG_URL),
        (PROD_BASE_URL, PROD_PUBLIC_CATALOG_URL),
        (f"{PROD_BASE_URL}/", PROD_PUBLIC_CATALOG_URL),
        (QA_BASE_URL, QA_PUBLIC_CATALOG_URL),
    ],
)
async def test_the_export_is_the_base_url_s_environment_s(
    base_url: str | None, expected: str
) -> None:
    async with MobilityFeedsClient(base_url=base_url) as client:
        assert client.public_catalog._url == expected  # deliberate friend access


async def test_a_given_public_catalog_url_wins(mock_api: MockApi) -> None:
    async with MobilityFeedsClient(
        base_url=QA_BASE_URL, public_catalog_url=mock_api.url(PATH)
    ) as client:
        assert client.public_catalog._url == mock_api.url(PATH)


@pytest.mark.parametrize(
    # DEV's export (dev-files.mobilitydatabase.org) answers 403: none known.
    "base_url",
    [DEV_BASE_URL, "https://catalog.example.com"],
)
async def test_another_base_url_needs_a_public_catalog_url(base_url: str) -> None:
    async with MobilityFeedsClient(base_url=base_url) as client:
        with pytest.raises(ValueError, match="public_catalog_url"):
            client.public_catalog  # noqa: B018 - raises
    async with MobilityFeedsClient(
        base_url="https://catalog.example.com",
        public_catalog_url="https://files.example.com/feeds_v2.csv",
    ) as given:
        assert given.public_catalog._url == "https://files.example.com/feeds_v2.csv"


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("ftp://example.com/feeds.csv", "unsupported URL scheme"),
        ("file:///etc/passwd", "unsupported URL scheme"),
        ("not a url", "unsupported URL scheme"),
        ("https:///feeds_v2.csv", "no host"),
        # Credentials go as Basic auth, so a user-id with a colon can't.
        ("https://us%3Aer:pw@example.com/feeds_v2.csv", "colon"),
        ("https://example.com/\ud800", "UTF-8"),
    ],
)
def test_an_unfetchable_public_catalog_url_raises_on_construction(
    url: str, reason: str
) -> None:
    with pytest.raises(
        ValueError, match=f"public_catalog_url can't be fetched.*{reason}"
    ):
        MobilityFeedsClient(public_catalog_url=url)


def test_a_public_catalog_url_that_is_no_str_raises_type_error() -> None:
    with pytest.raises(TypeError, match="public_catalog_url"):
        MobilityFeedsClient(public_catalog_url=b"https://x.org/f.csv")  # type: ignore[arg-type]


async def test_an_export_without_freshness_is_reused_for_an_hour(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    """The export is regenerated daily: RFC 9111's heuristic lifetime."""
    clock = Clock()
    public._cache._clock = clock  # deliberate friend access
    serve(mock_api)  # no Cache-Control, Expires or validators
    serve(mock_api)
    await public.get_gtfs_feeds()
    clock.now += 3599
    await public.get_gtfs_feeds()
    assert requests_to(mock_api) == 1
    clock.now += 1
    await public.get_gtfs_feeds()
    assert requests_to(mock_api) == 2


@pytest.mark.parametrize("declared", ["no-cache", "max-age=0", "no-store", "max-age=5"])
async def test_a_declared_freshness_is_never_extended(
    mock_api: MockApi, public: PublicCatalog, declared: str
) -> None:
    clock = Clock()
    public._cache._clock = clock  # deliberate friend access
    serve(mock_api, headers={"Cache-Control": declared})
    serve(mock_api)
    await public.get_gtfs_feeds()
    clock.now += 10
    await public.get_gtfs_feeds()
    assert requests_to(mock_api) == 2


async def test_a_redirect_is_followed_and_reused_only_as_its_hop_allows(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    moved = "/moved/feeds_v2.csv"
    cached = {"Cache-Control": "max-age=3600"}
    # A 302 without freshness of its own can't be reused: the next call asks
    # again, though the export it led to is fresh.
    mock_api.get(PATH, status=302, headers={"Location": mock_api.url(moved)})
    mock_api.get(moved, body=BODY, content_type="text/csv", headers=cached)
    assert [feed.id for feed in await public.get_gtfs_feeds()] == ["mdb-10"]
    mock_api.get(PATH, status=301, headers={"Location": mock_api.url(moved), **cached})
    mock_api.get(moved, body=BODY, content_type="text/csv", headers=cached)
    assert [feed.id for feed in await public.get_gtfs_feeds()] == ["mdb-10"]
    # Now both the 301 and the export are fresh: no request at all.
    assert [feed.id for feed in await public.get_gtfs_feeds()] == ["mdb-10"]
    assert [request.path for request in mock_api.requests] == [PATH, moved] * 2


def gbfs_systems(count: int) -> bytes:
    return export(*({**GBFS_ROW, "id": f"gbfs-{n:04}"} for n in range(count)))


async def test_the_default_limit_is_the_endpoints(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api, body=gbfs_systems(_GBFS_FEEDS_MAX_LIMIT + 1))
    feeds = await public.get_gbfs_feeds()
    assert len(feeds) == _GBFS_FEEDS_MAX_LIMIT
    assert feeds[-1].id == f"gbfs-{_GBFS_FEEDS_MAX_LIMIT - 1:04}"


async def test_the_largest_limit_is_taken(
    mock_api: MockApi, public: PublicCatalog
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
    public: PublicCatalog,
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
    public: PublicCatalog,
    method: str,
    name: str,
    bad: float,
) -> None:
    # The client encodes every given parameter, so one range alone is checked.
    with pytest.raises(ValueError, match="a query parameter is no finite number"):
        await getattr(public, method)(**{name: (bad, 1.0)})
    assert mock_api.requests == []


@pytest.mark.parametrize("method", ["get_gtfs_feeds", "get_gbfs_feeds"])
@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
async def test_non_finite_values_in_both_ranges_raise_value_error_before_any_request(
    mock_api: MockApi,
    public: PublicCatalog,
    method: str,
    bad: float,
) -> None:
    with pytest.raises(ValueError, match="a query parameter is no finite number"):
        await getattr(public, method)(
            dataset_latitudes=(bad, 1.0), dataset_longitudes=(0.0, 1.0)
        )
    assert mock_api.requests == []


async def test_a_304_to_an_unconditional_request_raises(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    mock_api.get(PATH, status=304, headers={"ETag": '"v1"'})
    with pytest.raises(SourceConnectionError) as caught:
        await public.get_gtfs_feeds()
    assert caught.value.status == 304


async def test_an_error_is_not_cached(mock_api: MockApi, public: PublicCatalog) -> None:
    mock_api.get(PATH, status=500, body=b"down", content_type="text/plain")
    serve(mock_api)
    with pytest.raises(SourceConnectionError):
        await public.get_gtfs_feeds()
    assert [feed.id for feed in await public.get_gtfs_feeds()] == ["mdb-10"]
    assert requests_to(mock_api) == 2


async def test_a_cancelled_caller_leaves_the_shared_fetch_running(
    mock_api: MockApi, public: PublicCatalog
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
    public: PublicCatalog,
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
        ("  BAY  ", set()),  # as given: GBFS's filter doesn't trim
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
        ("   ", set()),
        (  # %%: every provider but a missing one
            "",
            {
                "bay-area",
                "baywheels",
                "metro-underscore",
                "metro-x",
                "fifty",
                "backslash",
            },
        ),
    ],
)
async def test_provider_matches_as_the_apis_ilike(
    mock_api: MockApi,
    public: PublicCatalog,
    needle: str,
    expected: set[str],
) -> None:
    serve(mock_api, body=MATCHING)
    feeds = await public.get_gbfs_feeds(provider=needle)
    assert {feed.id for feed in feeds} == expected


TRIMMING = export(
    {**GTFS_ROW, "id": "gtfs-bay", "provider": "Bay Area"},
    {**GTFS_ROW, "id": "gtfs-none", "provider": ""},
    {**GTFS_RT_ROW, "id": "rt-bay", "provider": "Bay Area"},
    {**GTFS_RT_ROW, "id": "rt-none", "provider": ""},
    {**GBFS_ROW, "id": "gbfs-bay", "provider": "Bay Area"},
    {**GBFS_ROW, "id": "gbfs-none", "provider": ""},
)


@pytest.mark.parametrize(
    ("method", "prefix", "trims"),
    [
        ("get_gtfs_feeds", "gtfs", False),
        ("get_gtfs_rt_feeds", "rt", True),
        ("get_gbfs_feeds", "gbfs", False),
    ],
)
async def test_text_filters_are_trimmed_only_where_the_api_trims_them(
    mock_api: MockApi, public: PublicCatalog, method: str, prefix: str, trims: bool
) -> None:
    """GtfsRtFeedFilter strips a text filter and drops an empty one; GTFS's
    and GBFS's filters hand it to ILIKE as given."""
    serve(mock_api, body=TRIMMING, headers=CACHED)

    async def found(**kwargs: str) -> set[str]:
        feeds = await getattr(public, method)(**kwargs)
        return {feed.id.removeprefix(f"{prefix}-") for feed in feeds}

    if trims:  # no filter, or the trimmed value
        assert await found(provider="") == {"bay", "none"}
        assert await found(provider="  ") == {"bay", "none"}
        assert await found(provider="  Bay  ") == {"bay"}
        assert await found(producer_url=" example.com ") == {"bay", "none"}
    else:  # "" is '%%', which a missing value fails; spaces count
        assert await found(provider="") == {"bay"}
        assert await found(producer_url="") == {"bay", "none"}
        assert await found(provider="  ") == set()
        assert await found(provider="  Bay  ") == set()
        assert await found(producer_url=" example.com ") == set()


async def test_producer_url_matches_as_the_apis_ilike(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api, headers={"Cache-Control": "max-age=3600"})
    feeds = await public.get_gtfs_feeds(producer_url="EXAMPLE.COM/SUB_AY")
    assert [feed.id for feed in feeds] == ["mdb-10"]
    assert await public.get_gtfs_feeds(producer_url="%example.com") == []  # anchored
    anchored = await public.get_gtfs_feeds(producer_url="https://%.zip")
    assert [feed.id for feed in anchored] == ["mdb-10"]


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
    mock_api: MockApi, public: PublicCatalog
) -> None:
    long_url = "https://example.com/" + "a" * 2000
    row = {**GTFS_ROW, "urls.direct_download": long_url}
    serve(mock_api, body=export(row), headers={"Cache-Control": "max-age=3600"})
    await public.get_gtfs_feeds()  # fetched and parsed outside the timing
    started = time.perf_counter()
    feeds = await public.get_gtfs_feeds(producer_url="%_%_%_%_%_%_%_%_%#")
    # Milliseconds when linear. The backtracking translation (%: .*) tries
    # some 2000^9 splits here -- far past this generous bound on any machine.
    assert time.perf_counter() - started < 5
    assert feeds == []


FILTER_BODY = export(
    {
        **GTFS_ROW,
        "id": "mdb-1",
        "provider": "Alpha Transit",
        "is_official": "True",
        "urls.direct_download": "https://alpha.example.com/gtfs.zip",
        **box("0", "1", "0", "1"),
    },
    {
        **GTFS_ROW,
        "id": "mdb-2",
        "provider": "Beta Bus",
        "is_official": "False",
        "urls.direct_download": "https://beta.example.com/GTFS.zip",
        **box("0", "10", "0", "10"),
    },
    {
        **GTFS_ROW,
        "id": "mdb-3",
        "provider": "Gamma",
        "is_official": "",
        "urls.direct_download": "https://gamma.example.com/x.zip",
        **box("20", "21", "20", "21"),
    },
    {**GTFS_ROW, "id": "mdb-4", "provider": "Delta", **box("", "", "", "")},
    {**GTFS_RT_ROW, "id": "mdb-5", "entity_type": "vp"},
    {**GTFS_RT_ROW, "id": "mdb-6", "entity_type": "sa|tu"},
)


@pytest.fixture
async def filtered(mock_api: MockApi, public: PublicCatalog) -> PublicCatalog:
    serve(mock_api, body=FILTER_BODY, headers={"Cache-Control": "max-age=3600"})
    return public


async def ids(feeds: Awaitable[Sequence[PublicFeed]]) -> list[str]:
    return [feed.id for feed in await feeds]


async def test_a_trimmed_substring_provider_filters_in_provider_order(
    filtered: PublicCatalog,
) -> None:
    assert await ids(filtered.get_gtfs_feeds(provider="BETA")) == ["mdb-2"]
    assert await ids(filtered.get_gtfs_feeds(provider="ta")) == ["mdb-2", "mdb-4"]
    # Provider, then id: Alpha, Beta, Delta, Gamma.
    assert await ids(filtered.get_gtfs_feeds()) == ["mdb-1", "mdb-2", "mdb-4", "mdb-3"]


async def test_producer_url_matches_the_substring_in_any_case(
    filtered: PublicCatalog,
) -> None:
    assert await ids(filtered.get_gtfs_feeds(producer_url="gtfs.zip")) == [
        "mdb-1",
        "mdb-2",
    ]


async def test_is_official_false_keeps_false_and_unknown(
    filtered: PublicCatalog,
) -> None:
    # mdb-4 keeps GTFS_ROW's "True".
    assert await ids(filtered.get_gtfs_feeds(is_official=True)) == ["mdb-1", "mdb-4"]
    assert await ids(filtered.get_gtfs_feeds(is_official=False)) == ["mdb-2", "mdb-3"]


async def test_entity_types_match_any(filtered: PublicCatalog) -> None:
    assert await ids(
        filtered.get_gtfs_rt_feeds(entity_types=[EntityType.TRIP_UPDATES])
    ) == ["mdb-6"]
    assert await ids(
        filtered.get_gtfs_rt_feeds(
            entity_types=[EntityType.VEHICLE_POSITIONS, EntityType.SERVICE_ALERTS]
        )
    ) == ["mdb-5", "mdb-6"]


async def test_bounding_filters_default_to_completely_enclosed(
    filtered: PublicCatalog,
) -> None:
    # Reversed ranges: normalised to latitudes -1..5 and longitudes -1..5.
    near = {"dataset_latitudes": (-1.0, 5.0), "dataset_longitudes": (5.0, -1.0)}
    assert await ids(filtered.get_gtfs_feeds(**near)) == ["mdb-1"]
    assert await ids(
        filtered.get_gtfs_feeds(
            **near, bounding_filter_method=BoundingFilterMethod.PARTIALLY_ENCLOSED
        )
    ) == ["mdb-2"]
    # mdb-4 has no readable box: it matches no bounding filter, not even this.
    assert await ids(
        filtered.get_gtfs_feeds(
            **near, bounding_filter_method=BoundingFilterMethod.DISJOINT
        )
    ) == ["mdb-3"]


async def test_one_range_alone_filters_nothing(
    filtered: PublicCatalog,
) -> None:
    assert len(await filtered.get_gtfs_feeds(dataset_latitudes=(0.0, 1.0))) == 4


async def test_limit_and_offset_apply_after_filtering(
    filtered: PublicCatalog,
) -> None:
    assert await ids(filtered.get_gtfs_feeds(is_official=False, offset=1, limit=1)) == [
        "mdb-3"
    ]
    assert await ids(filtered.get_gtfs_feeds(limit=0)) == []


async def test_gbfs_filters_on_the_system_box(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api)
    home = {"dataset_latitudes": (40.75, 40.76), "dataset_longitudes": (-73.99, -73.98)}
    feeds = await public.get_gbfs_feeds(
        **home, bounding_filter_method=BoundingFilterMethod.PARTIALLY_ENCLOSED
    )
    assert [feed.id for feed in feeds] == ["gbfs-citibike"]


@pytest.mark.parametrize(
    "kwargs",
    [{"limit": True}, {"provider": 7}, {"dataset_latitudes": (1.0,)}],
)
async def test_mistyped_arguments_raise_type_error(
    mock_api: MockApi, public: PublicCatalog, kwargs: dict[str, object]
) -> None:
    with pytest.raises(TypeError):
        await public.get_gtfs_feeds(**kwargs)  # type: ignore[arg-type]
    assert mock_api.requests == []


CACHED = {"Cache-Control": "max-age=3600"}


async def test_the_rt_list_leaves_out_feeds_without_entity_types(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    untyped = {**GTFS_RT_ROW, "id": "mdb-21", "entity_type": ""}
    unknown = {**GTFS_RT_ROW, "id": "mdb-22", "entity_type": "xx"}
    serve(mock_api, body=export(GTFS_RT_ROW, untyped, unknown), headers=CACHED)
    # The API's list inner-joins the entity types; its getter doesn't.
    feeds = await public.get_gtfs_rt_feeds()
    assert [feed.id for feed in feeds] == ["mdb-20", "mdb-22"]
    assert (await public.get_gtfs_rt_feed("mdb-21")).id == "mdb-21"


async def test_a_gbfs_limit_of_0_is_no_limit(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    systems = ({**GBFS_ROW, "id": f"gbfs-{n:04}"} for n in range(501))
    serve(mock_api, body=export(GTFS_ROW, GTFS_RT_ROW, *systems), headers=CACHED)
    # The API's GBFS list applies a limit only when truthy; GTFS's take 0.
    assert len(await public.get_gbfs_feeds(limit=0)) == 501
    paged = await public.get_gbfs_feeds(limit=0, offset=1)
    assert [feed.id for feed in paged][:1] == ["gbfs-0001"]
    assert len(paged) == 500
    assert await public.get_gtfs_feeds(limit=0) == []
    assert await public.get_gtfs_rt_feeds(limit=0) == []


async def test_an_export_without_gbfs_rows_raises_feed_parse_error(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api, body=export(GTFS_ROW, GTFS_RT_ROW), headers=CACHED)
    message = "Public catalog export carries no GBFS feeds"
    with pytest.raises(FeedParseError, match=message) as listed:
        await public.get_gbfs_feeds()
    with pytest.raises(FeedParseError, match=message) as got:
        await public.get_gbfs_feed("gbfs-citibike")
    assert listed.value.url == mock_api.url(PATH)
    assert listed.value.__context__ is None
    assert got.value.__context__ is None
    assert [feed.id for feed in await public.get_gtfs_feeds()] == ["mdb-10"]


async def test_gbfs_filtered_to_nothing_is_empty(
    mock_api: MockApi, public: PublicCatalog
) -> None:
    serve(mock_api, headers=CACHED)
    assert await public.get_gbfs_feeds(provider="nobody") == []
    with pytest.raises(MobilityDatabaseNotFoundError):
        await public.get_gbfs_feed("gbfs-missing")


@pytest.mark.parametrize("method", ["get_gtfs_feeds", "get_gbfs_feeds"])
@pytest.mark.parametrize("both", [False, True])
async def test_a_coordinate_past_float_raises_value_error_before_any_request(
    mock_api: MockApi,
    public: PublicCatalog,
    method: str,
    both: bool,
) -> None:
    ranges: dict[str, tuple[float, float]] = {"dataset_latitudes": (10**400, 1.0)}
    if both:
        ranges["dataset_longitudes"] = (1.0, 2.0)
    with pytest.raises(
        ValueError, match="a query parameter is no finite number"
    ) as caught:
        await getattr(public, method)(**ranges)
    assert caught.value.__context__ is None
    assert mock_api.requests == []


@pytest.mark.parametrize(
    "method", ["get_gtfs_feeds", "get_gtfs_rt_feeds", "get_gbfs_feeds"]
)
@pytest.mark.parametrize("name", ["provider", "producer_url"])
async def test_a_text_filter_that_is_no_text_raises_before_any_request(
    mock_api: MockApi,
    public: PublicCatalog,
    method: str,
    name: str,
) -> None:
    with pytest.raises(ValueError, match="lone surrogate"):
        await getattr(public, method)(**{name: "bike\ud800"})
    assert mock_api.requests == []
