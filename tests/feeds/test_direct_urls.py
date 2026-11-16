"""Tests for direct-URL mode: transit and GBFS handles without the catalog."""

import asyncio
import base64
import io
import zipfile
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from yarl import URL

from aiomobilitydatabase import EntityType
from aiomobilitydatabase.exceptions import MobilityDatabaseError
from aiomobilitydatabase.feeds._redirects import get_withholding
from aiomobilitydatabase.feeds._urls import _require_http_url
from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import (
    FeedParseError,
    MobilityFeedsError,
    SourceAuthenticationError,
    SourceConnectionError,
)
from aiomobilitydatabase.feeds.models import ArrivalsQuery, StaticBuildProgress
from aiomobilitydatabase.feeds.rt import feed_request_fields, fetch_feed_message

from tests.feeds.fixtures import (
    _FILES,
    DISCOVERY_23,
    DISCOVERY_30,
    STATION_INFO_23,
    STATION_STATUS_23,
    SYSTEM_INFO_30,
    TRIP_UPDATES_T1_DELAYED,
    VEHICLE_POSITIONS,
    build_gtfs_zip_bytes,
    with_base,
)
from tests.feeds.test_static_build import damage_page
from tests.mock_server import MockApi

PB = "application/octet-stream"
STATIC_PATH = "/direct/gtfs.zip"
TU_PATH = "/direct/rt/trip_updates"
VP_PATH = "/direct/rt/vehicle_positions"
HDRS = {"X-Custom-Token": "abc123"}
NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)  # Thursday 07:45 PDT


def _current(discovery: dict[str, Any]) -> dict[str, Any]:
    """A discovery document updated just now: its ttl hasn't lapsed."""
    now = datetime.now(UTC)
    stamp = discovery.get("last_updated")
    discovery["last_updated"] = (
        now.isoformat() if isinstance(stamp, str) else int(now.timestamp())
    )
    return discovery


@pytest.fixture
async def direct_client() -> AsyncGenerator[MobilityFeedsClient, None]:
    """A TOKENLESS client: direct-URL mode needs no catalog credentials."""
    async with MobilityFeedsClient() as client:
        yield client


@pytest.fixture
async def direct_client_cached(
    tmp_path: Path,
) -> AsyncGenerator[MobilityFeedsClient, None]:
    """A tokenless client with a static cache directory."""
    async with MobilityFeedsClient(cache_dir=str(tmp_path)) as client:
        yield client


def _zip_with_extra_stop() -> bytes:
    """The fixture GTFS zip plus one extra stop, for changed-dataset tests."""
    files = dict(_FILES)
    files["stops.txt"] = files["stops.txt"] + "S9,Ninth St,34.07,-118.26,,0\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _requests_for(mock_api: MockApi, method: str, path: str) -> list[object]:
    return [
        req for req in mock_api.requests if req.method == method and req.path == path
    ]


# -- Transit: static acquisition and identity --------------------------------


async def test_direct_transit_happy_path_with_etag(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    events: list[StaticBuildProgress] = []
    handle = await direct_client.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH), headers=HDRS, on_progress=events.append
    )
    assert {stop.id for stop in handle.stops} == {"S1", "S2", "S3", "ST1"}
    assert {route.id for route in handle.routes} == {"R1", "R2"}
    # No catalog identity: the url-derived cache key stands in, and there is
    # no catalog dataset record at all.
    assert handle.static_feed_id.startswith("url-")
    assert handle.static_dataset is None
    assert handle.rt_feeds == ()
    # Both the HEAD probe and the zip download carried the custom headers.
    for method in ("HEAD", "GET"):
        (request,) = _requests_for(mock_api, method, STATIC_PATH)
        assert request.headers.get("X-Custom-Token") == "abc123"  # type: ignore[attr-defined]
    # Progress events fired for both phases, same contract as the catalog path.
    assert {event.phase for event in events} == {"download", "index"}


async def test_direct_transit_cache_hit_skips_download(
    mock_api: MockApi, direct_client_cached: MobilityFeedsClient
) -> None:
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH,
        body=build_gtfs_zip_bytes(),
        content_type="application/zip",
        headers={"ETag": '"v1"'},
    )
    await direct_client_cached.get_transit_feed_from_urls(mock_api.url(STATIC_PATH))
    # Second acquisition: HEAD reports the same ETag, so the cached index is
    # opened without a second zip download.
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    handle = await direct_client_cached.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH)
    )
    assert {stop.id for stop in handle.stops} == {"S1", "S2", "S3", "ST1"}
    assert len(_requests_for(mock_api, "HEAD", STATIC_PATH)) == 2
    assert len(_requests_for(mock_api, "GET", STATIC_PATH)) == 1


@pytest.mark.parametrize("damaged", ["stops", "routes", "stop_times"])
async def test_a_damaged_cached_index_is_rebuilt(
    mock_api: MockApi,
    direct_client_cached: MobilityFeedsClient,
    tmp_path: Path,
    damaged: str,
) -> None:
    """One bad page in the cached file -- in stops or routes, which the
    handle reads as it opens, or stop_times, which a query would meet --
    and the next acquisition rebuilds from the zip instead of failing."""
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH,
        body=build_gtfs_zip_bytes(),
        content_type="application/zip",
        headers={"ETag": '"v1"'},
    )
    handle = await direct_client_cached.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH)
    )
    handle.close()
    damage_page(tmp_path / handle.static_feed_id / "static.db", damaged)
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH,
        body=build_gtfs_zip_bytes(),
        content_type="application/zip",
        headers={"ETag": '"v1"'},
    )
    handle = await direct_client_cached.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH)
    )
    try:
        assert {stop.id for stop in handle.stops} == {"S1", "S2", "S3", "ST1"}
        [board] = await handle.get_scheduled_departures(
            [ArrivalsQuery(["S1"], limit=1)], start=NOW
        )
        assert board
    finally:
        handle.close()
    assert len(_requests_for(mock_api, "GET", STATIC_PATH)) == 2  # rebuilt


async def test_direct_refresh_static_etag(
    mock_api: MockApi, direct_client_cached: MobilityFeedsClient
) -> None:
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH,
        body=build_gtfs_zip_bytes(),
        content_type="application/zip",
        headers={"ETag": '"v1"'},
    )
    handle = await direct_client_cached.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH)
    )
    # Unchanged ETag: no download, no rebuild.
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    assert await handle.refresh_static() is False
    assert len(_requests_for(mock_api, "GET", STATIC_PATH)) == 1
    # Changed ETag: re-download, rebuild, and the new data becomes visible.
    mock_api.head(STATIC_PATH, headers={"ETag": '"v2"'})
    mock_api.get(
        STATIC_PATH,
        body=_zip_with_extra_stop(),
        content_type="application/zip",
        headers={"ETag": '"v2"'},
    )
    assert await handle.refresh_static() is True
    assert "S9" in {stop.id for stop in handle.stops}
    assert len(_requests_for(mock_api, "GET", STATIC_PATH)) == 2


async def test_direct_refresh_static_last_modified_is_no_identity(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    """Last-Modified is implicitly weak (RFC 9110 section 8.8.2.2): it never
    names exact bytes, so the download identifies the dataset -- an
    unchanged one by its hash, without a rebuild.
    """
    unchanged = {"Last-Modified": "Thu, 30 Jul 2026 00:00:00 GMT"}
    mock_api.head(STATIC_PATH, headers=unchanged)
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    handle = await direct_client.get_transit_feed_from_urls(mock_api.url(STATIC_PATH))
    mock_api.head(STATIC_PATH, headers=unchanged)
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    assert await handle.refresh_static() is False
    assert len(_requests_for(mock_api, "GET", STATIC_PATH)) == 2
    mock_api.head(
        STATIC_PATH, headers={"Last-Modified": "Fri, 31 Jul 2026 00:00:00 GMT"}
    )
    mock_api.get(
        STATIC_PATH, body=_zip_with_extra_stop(), content_type="application/zip"
    )
    assert await handle.refresh_static() is True
    assert "S9" in {stop.id for stop in handle.stops}


async def test_direct_refresh_static_hash_fallback(
    mock_api: MockApi, direct_client_cached: MobilityFeedsClient
) -> None:
    """No scripted HEAD at all: the mock answers 599, exercising the
    server-rejects-HEAD branch, and identity falls back to hashing the
    downloaded bytes (which therefore re-downloads on every refresh).
    """
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    handle = await direct_client_cached.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH)
    )
    # Identical bytes: downloaded again to hash, but hash matches -> False.
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    assert await handle.refresh_static() is False
    assert len(_requests_for(mock_api, "GET", STATIC_PATH)) == 2
    # Changed bytes: new hash -> rebuild, new data visible.
    mock_api.get(
        STATIC_PATH, body=_zip_with_extra_stop(), content_type="application/zip"
    )
    assert await handle.refresh_static() is True
    assert "S9" in {stop.id for stop in handle.stops}


async def test_direct_transit_head_ok_without_validators(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    # A 200 HEAD offering neither ETag nor Last-Modified also lands on the
    # hash fallback (distinct from the HEAD-rejected branch above).
    mock_api.head(STATIC_PATH)
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    handle = await direct_client.get_transit_feed_from_urls(mock_api.url(STATIC_PATH))
    assert {stop.id for stop in handle.stops} == {"S1", "S2", "S3", "ST1"}


async def test_direct_transit_probe_unreachable_raises(
    direct_client: MobilityFeedsClient,
) -> None:
    with pytest.raises(SourceConnectionError, match="Request failed"):
        await direct_client.get_transit_feed_from_urls("http://127.0.0.1:1/gtfs.zip")


async def test_direct_transit_rejects_non_http_urls(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    with pytest.raises(SourceConnectionError, match="scheme"):
        await direct_client.get_transit_feed_from_urls("file:///etc/passwd")
    # An offending RT url is rejected up front too: before any HTTP at all.
    with pytest.raises(SourceConnectionError, match="scheme"):
        await direct_client.get_transit_feed_from_urls(
            mock_api.url(STATIC_PATH), trip_updates_urls=["ftp://rt.example/feed"]
        )
    assert mock_api.requests == []


async def test_purge_cache_accepts_url_derived_key(
    mock_api: MockApi, direct_client_cached: MobilityFeedsClient, tmp_path: Path
) -> None:
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    handle = await direct_client_cached.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH)
    )
    handle.close()
    cache_entry = tmp_path / handle.static_feed_id
    assert (cache_entry / "static.db").exists()
    await direct_client_cached.purge_cache(handle.static_feed_id)
    assert not cache_entry.exists()


# -- Transit: synthesized RT sources -----------------------------------------


async def test_direct_rt_typed_layers(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    """Per-layer RT urls: get_arrivals polls ONLY the trip-updates url and
    get_vehicles ONLY the vehicle-positions url — one scripted response per
    url proves nothing polls a layer it wasn't declared for.
    """
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    mock_api.get(TU_PATH, body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    mock_api.get(VP_PATH, body=VEHICLE_POSITIONS, content_type=PB)
    handle = await direct_client.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH),
        trip_updates_urls=[mock_api.url(TU_PATH)],
        vehicle_positions_urls=[mock_api.url(VP_PATH)],
        headers=HDRS,
    )
    assert [feed.id for feed in handle.rt_feeds] == [
        mock_api.url(TU_PATH),
        mock_api.url(VP_PATH),
    ]
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1", "S2"])], start=NOW, end=NOW + timedelta(hours=1)
    )
    by_key = {(a.trip_id, a.stop_id): a for a in arrivals}
    assert by_key[("T2", "S1")].canceled is True  # canceled by RT
    t1_s1 = by_key[("T1", "S1")]
    assert t1_s1.realtime is True
    # The fixture's departure time is 330 s late (its delay field says 300):
    # "time will take precedence".
    assert t1_s1.delay_seconds == 330
    assert ("ADDED-9", "S2") in by_key  # RT-added trip came through
    vehicles = await handle.get_vehicles()
    assert {vehicle.vehicle_id for vehicle in vehicles} == {"V1", "V2"}
    # Exactly one fetch per url — arrivals never touched the VP url and
    # vice versa — and every RT fetch carried the headers.
    for path in (TU_PATH, VP_PATH):
        rt_requests = _requests_for(mock_api, "GET", path)
        assert len(rt_requests) == 1
        assert all(
            req.headers.get("X-Custom-Token") == "abc123"  # type: ignore[attr-defined]
            for req in rt_requests
        )


async def test_direct_rt_combined_url_deduplicated(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    """The same url declared for several layers — a combined feed — is
    synthesized into ONE source carrying the union of entity types, so each
    operation fetches it exactly once, never twice.
    """
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    mock_api.get(TU_PATH, body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    combined_url = mock_api.url(TU_PATH)
    handle = await direct_client.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH),
        trip_updates_urls=[combined_url],
        vehicle_positions_urls=[combined_url],
        service_alerts_urls=[combined_url],
    )
    assert len(handle.rt_feeds) == 1
    assert handle.rt_feeds[0].entity_types == (
        EntityType.TRIP_UPDATES,
        EntityType.VEHICLE_POSITIONS,
        EntityType.SERVICE_ALERTS,
    )
    [arrivals] = await handle.get_arrivals(
        [ArrivalsQuery(["S1"])], start=NOW, end=NOW + timedelta(hours=1)
    )
    assert any(arrival.realtime for arrival in arrivals)
    assert len(_requests_for(mock_api, "GET", TU_PATH)) == 1


# -- Tokenless catalog access -------------------------------------------------


async def test_tokenless_client_catalog_access_raises(
    direct_client: MobilityFeedsClient,
) -> None:
    with pytest.raises(MobilityDatabaseError, match="refresh token is required"):
        _ = direct_client.catalog
    # Catalog-backed feed resolution fails the same way.
    with pytest.raises(MobilityDatabaseError, match="refresh token is required"):
        await direct_client.get_transit_feed("mdb-100")
    with pytest.raises(MobilityDatabaseError, match="refresh token is required"):
        await direct_client.get_gbfs_feed("gbfs-300")


# -- GBFS ---------------------------------------------------------------------


async def test_direct_gbfs_23_discovery_stations_and_headers(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    mock_api.get("/gbfs/gbfs.json", payload=_current(with_base(DISCOVERY_23, base)))
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await direct_client.get_gbfs_feed_from_url(
        mock_api.url("/gbfs/gbfs.json"), headers=HDRS
    )
    stations = {station.id: station for station in await handle.get_stations()}
    assert stations["st1"].name == "Dock A"
    assert stations["st1"].bikes_available == 5
    assert stations["st2"].is_renting is False
    # Discovery fetch AND both document fetches carried the headers.
    gbfs_requests = [req for req in mock_api.requests if req.path.startswith("/gbfs/")]
    assert len(gbfs_requests) == 3
    assert all(req.headers.get("X-Custom-Token") == "abc123" for req in gbfs_requests)


async def test_direct_gbfs_30_discovery(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    mock_api.get("/gbfs3/gbfs.json", payload=_current(with_base(DISCOVERY_30, base)))
    mock_api.get("/gbfs3/system_information.json", payload=SYSTEM_INFO_30)
    handle = await direct_client.get_gbfs_feed_from_url(
        mock_api.url("/gbfs3/gbfs.json")
    )
    info = await handle.get_system_info()
    assert info.system_id == "test-bikes-3"
    assert info.name == "Test Bikes 3"


async def test_direct_gbfs_non_preferred_language_fallback(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    discovery = _current(with_base(DISCOVERY_23, base))
    discovery["data"] = {"fr": discovery["data"]["en"]}  # no "en" block at all
    mock_api.get("/gbfs/gbfs.json", payload=discovery)
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await direct_client.get_gbfs_feed_from_url(mock_api.url("/gbfs/gbfs.json"))
    assert len(await handle.get_stations()) == 2


async def test_direct_gbfs_discovery_without_feeds_raises(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    mock_api.get("/gbfs/gbfs.json", payload={"data": {}})
    with pytest.raises(FeedParseError, match="no usable feeds"):
        await direct_client.get_gbfs_feed_from_url(mock_api.url("/gbfs/gbfs.json"))


@pytest.mark.parametrize("etag", ['W/"v1"', "*", "v1", '"v1", "v2"'])
async def test_only_a_strong_etag_names_a_dataset(
    mock_api: MockApi, direct_client_cached: MobilityFeedsClient, etag: str
) -> None:
    """A weak, wildcard or malformed ETag (RFC 9110 section 8.8.3) names no
    exact bytes: each acquisition downloads, and the hash finds the cached
    index.
    """
    for _ in range(2):
        mock_api.head(STATIC_PATH, headers={"ETag": etag})
        mock_api.get(
            STATIC_PATH,
            body=build_gtfs_zip_bytes(),
            content_type="application/zip",
            headers={"ETag": etag},
        )
        handle = await direct_client_cached.get_transit_feed_from_urls(
            mock_api.url(STATIC_PATH)
        )
    assert handle._index.dataset_id.startswith("sha256:")
    assert len(_requests_for(mock_api, "GET", STATIC_PATH)) == 2


async def test_the_download_names_its_own_dataset(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    """RFC 9110 section 9.3.2: a HEAD only SHOULD match the GET. The index
    is named by the GET that delivered it, not by the HEAD before it.
    """
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH,
        body=build_gtfs_zip_bytes(),
        content_type="application/zip",
        headers={"ETag": '"v2"'},
    )
    handle = await direct_client.get_transit_feed_from_urls(mock_api.url(STATIC_PATH))
    assert handle._index.dataset_id == 'etag:"v2"'


@pytest.mark.parametrize("status", [204, 206, 300, 302])
async def test_only_a_200_or_203_is_a_dataset(
    mock_api: MockApi, direct_client: MobilityFeedsClient, status: int
) -> None:
    """RFC 9110 section 15.3: a 204 has no content, a 206 nobody asked for
    is part of one, a 3xx left unfollowed is no dataset."""
    mock_api.head(STATIC_PATH, status=405)
    mock_api.get(STATIC_PATH, status=status, body=b"x", content_type="application/zip")
    with pytest.raises(SourceConnectionError, match="without a dataset") as excinfo:
        await direct_client.get_transit_feed_from_urls(mock_api.url(STATIC_PATH))
    assert excinfo.value.status == status


_URL_TEXT = st.tuples(
    st.sampled_from(["http://", "https://", "HTTP://", "ftp://", "", "http:"]),
    st.lists(
        st.sampled_from([*"ab1:[]@/.%?#-", "99999", "::1", "xn--", "..", chr(0xE9)]),
        max_size=8,
    ).map("".join),
).map("".join)


@given(url=_URL_TEXT)
@settings(max_examples=500)
def test_the_url_guard_passes_only_urls_a_request_can_be_made_to(url: str) -> None:
    """_require_http_url either refuses with SourceConnectionError or lets
    through UTF-8 text that parses as an http(s) URL with a host (what
    yarl would request unchanged); anything else a request trips on is
    the network boundary's."""
    try:
        _require_http_url(url, "test URL")
    except SourceConnectionError:
        return
    url.encode("utf-8")
    parsed = URL(url)
    assert parsed.scheme in ("http", "https")
    assert parsed.raw_host  # RFC 9110 section 4.2.1: never an empty host


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:99999/rt.pb",
        "http://[::1/rt.pb",
        "http://[::1]:@/rt.pb",
    ],
)
async def test_an_rt_url_aiohttp_cannot_parse_is_refused_up_front(
    mock_api: MockApi, direct_client: MobilityFeedsClient, url: str
) -> None:
    with pytest.raises(SourceConnectionError, match="doesn't parse"):
        await direct_client.get_transit_feed_from_urls(
            mock_api.url(STATIC_PATH), trip_updates_urls=[url]
        )
    with pytest.raises(SourceConnectionError, match="doesn't parse"):
        await direct_client.get_transit_feed_from_urls(url)
    assert mock_api.requests == []


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:99999/rt.pb",
        "http://[::1/rt.pb",
        "http://[::1]:@/rt.pb",
    ],
)
async def test_predicting_an_rt_request_refuses_an_unparseable_url(url: str) -> None:
    """A catalog feed's producer_url never passed the up-front check: the
    cache's request prediction must refuse it, not raise yarl's ValueError."""
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError, match="doesn't parse"):
            feed_request_fields(session, url)


# -- A redirect to a URL no request can be made to ------------------------------

_BAD_LOCATIONS = [
    "http://[::1]:@/x",  # yarl raises IndexError
    "http://a..b/x",  # the resolver raises UnicodeError
    "http://[::1/x",
    "http://127.0.0.1:99999/x",
    # No host (RFC 9110 section 4.2.1): from an http:// page these keep
    # their scheme and resolve to no host at all.
    "https:",
    "https://",
    "https:///x",
    "https:x",
    # The page's own scheme too: a Location with a scheme is used as it is
    # (RFC 3986 section 5.2.2), never given the base's host.
    "http:",
    "http://",
    "http:///x",
    "HTTP:///x",
    "http:x",
    "http://a%3Ab@127.0.0.1:9/x",  # RFC 7617: no colon in a Basic user-id
    "http://a%0Ab:p@127.0.0.1:9/x",  # RFC 7617: no control character either
    "http://a:p%00x@127.0.0.1:9/x",
    "http://%E2%82%AC:p@127.0.0.1:9/x",  # Basic auth sends Latin-1 only
    "http://u:%E2%82%AC@127.0.0.1:9/x",
]


@pytest.mark.parametrize("location", ["http:///x", "http:x", "HTTP:///x", "http:"])
@pytest.mark.parametrize("api_key", [None, "secret"])
async def test_a_location_with_a_scheme_and_no_host_never_borrows_the_base_host(
    mock_api: MockApi, location: str, api_key: str | None
) -> None:
    """RFC 3986 section 5.2.2: a reference with a scheme is the target as
    written. Joined onto the base, ``http:///x`` became the producer's own
    ``/x`` -- requested, the secret header with it; it names no host."""
    mock_api.get("/rt.pb", status=302, headers={"Location": location})
    mock_api.get("/x", body=b"", content_type=PB)
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError, match="invalid URL"):
            await fetch_feed_message(
                session,
                mock_api.url("/rt.pb"),
                auth_type=2 if api_key else None,
                api_key_name="X-Key" if api_key else None,
                api_key=api_key,
            )
    assert [r.path for r in mock_api.requests] == ["/rt.pb"]


@pytest.mark.parametrize("location", _BAD_LOCATIONS)
async def test_a_gbfs_redirect_to_an_unusable_url_is_a_connection_error(
    mock_api: MockApi, direct_client: MobilityFeedsClient, location: str
) -> None:
    """Where a redirect leads is as producer-controlled as the first URL:
    aiohttp following it must not let a bare exception out."""
    mock_api.get("/gbfs/gbfs.json", status=302, headers={"Location": location})
    with pytest.raises(SourceConnectionError):
        await direct_client.get_gbfs_feed_from_url(mock_api.url("/gbfs/gbfs.json"))


@pytest.mark.parametrize("location", _BAD_LOCATIONS)
async def test_a_revalidation_redirect_to_an_unusable_url_is_a_connection_error(
    mock_api: MockApi, direct_client: MobilityFeedsClient, location: str
) -> None:
    """A conditional request follows redirects by hand (to withhold its
    validators): the Location gets the URL guard's checks there too."""
    feeds = [{"name": "system_information", "url": mock_api.url("/gbfs/si.json")}]
    discovery = {"ttl": 3600, "last_updated": 0, "data": {"feeds": feeds}}
    mock_api.get(
        "/gbfs/gbfs.json", payload=discovery, headers={"Cache-Control": "max-age=3600"}
    )
    info = {"ttl": 0, "last_updated": 0, "data": {"system_id": "s", "name": "Bikes"}}
    mock_api.get("/gbfs/si.json", payload=info, headers={"ETag": '"v1"'})
    mock_api.get("/gbfs/si.json", status=302, headers={"Location": location})
    handle = await direct_client.get_gbfs_feed_from_url(mock_api.url("/gbfs/gbfs.json"))
    assert (await handle.get_system_info()).name == "Bikes"
    with pytest.raises(SourceConnectionError):
        await handle.get_system_info()
    revalidation = [r for r in mock_api.requests if r.path == "/gbfs/si.json"][-1]
    assert revalidation.headers.get("If-None-Match") == '"v1"'


@pytest.mark.parametrize("location", _BAD_LOCATIONS)
@pytest.mark.parametrize(
    "api_key", [None, "secret"], ids=["aiohttp-follows", "by-hand"]
)
async def test_an_rt_redirect_to_an_unusable_url_is_a_connection_error(
    mock_api: MockApi, location: str, api_key: str | None
) -> None:
    """A header API key makes the library follow redirects by hand; without
    one aiohttp does. Neither lets a bare exception out."""
    mock_api.get("/rt.pb", status=302, headers={"Location": location})
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError):
            await fetch_feed_message(
                session,
                mock_api.url("/rt.pb"),
                auth_type=2 if api_key else None,
                api_key_name="X-Key" if api_key else None,
                api_key=api_key,
            )


@pytest.mark.parametrize("location", _BAD_LOCATIONS)
async def test_a_static_download_redirect_to_an_unusable_url_is_a_connection_error(
    mock_api: MockApi, direct_client: MobilityFeedsClient, location: str
) -> None:
    mock_api.head(STATIC_PATH, status=405)
    mock_api.get(STATIC_PATH, status=302, headers={"Location": location})
    with pytest.raises(SourceConnectionError):
        await direct_client.get_transit_feed_from_urls(mock_api.url(STATIC_PATH))


@pytest.mark.parametrize("location", _BAD_LOCATIONS)
async def test_a_static_probe_redirect_to_an_unusable_url_is_a_connection_error(
    mock_api: MockApi, direct_client: MobilityFeedsClient, location: str
) -> None:
    mock_api.head(STATIC_PATH, status=302, headers={"Location": location})
    with pytest.raises(SourceConnectionError):
        await direct_client.get_transit_feed_from_urls(mock_api.url(STATIC_PATH))


async def test_a_punycode_label_that_wont_decode_is_still_fetched_by_hand(
    mock_api: MockApi,
) -> None:
    """``xn--`` is no IDNA name, yet aiohttp requests it (and fails DNS).
    The hand-followed path compares origins by the ASCII host, as RFC 6454
    does, rather than decoding it -- which raised a bare UnicodeError."""
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError):
            await fetch_feed_message(
                session,
                "http://xn--.invalid/rt.pb",
                auth_type=2,
                api_key_name="X-Key",
                api_key="secret",
                timeout_seconds=5,
            )


@pytest.mark.parametrize(
    "url", ["http:", "https://", "http:x", "http:/x", "http:?q", "https:///x"]
)
def test_a_url_with_no_host_is_refused(url: str) -> None:
    """RFC 9110 section 4.2.1: a recipient MUST reject an http(s) URI with
    an empty host as invalid."""
    with pytest.raises(SourceConnectionError, match="names no host"):
        _require_http_url(url, "test URL")


@pytest.mark.parametrize("url", ["https://", "http:x", "http:/x"])
async def test_an_rt_url_with_no_host_is_refused_with_a_header_key(url: str) -> None:
    """With a header key the library follows redirects by hand, whose URL
    arithmetic raised a bare ValueError on a host-less URL."""
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError, match="names no host"):
            await fetch_feed_message(
                session, url, auth_type=2, api_key_name="X-Key", api_key="k"
            )


class _OfflineResolver(aiohttp.abc.AbstractResolver):
    """Encode the host as getaddrinfo does, then fail: no network in tests."""

    async def resolve(self, host: str, port: int = 0, family: int = 0) -> list[Any]:
        host.encode("idna")
        raise OSError(f"offline: {host}")

    async def close(self) -> None:
        pass


_LOCATION_TEXT = st.tuples(
    st.sampled_from(
        ["http://", "https://", "http:", "https:", "//", "/", "", "ftp://"]
    ),
    st.lists(
        st.sampled_from(
            [
                *"ab1:[]@/.%?#-",
                "::1",
                "xn--",
                "..",
                "99999",
                "%3A",
                "%3a@",
                chr(0xE9),
                chr(0x3002),
            ]
        ),
        max_size=8,
    ).map("".join),
).map("".join)


@given(location=_LOCATION_TEXT, by_hand=st.booleans())
@settings(max_examples=300, deadline=None)
def test_any_redirect_ends_in_a_library_exception(location: str, by_hand: bool) -> None:
    """Whatever Location a producer answers with, followed by aiohttp or by
    hand (a header key), only SourceConnectionError comes out (the mock
    serves no feed, so no fetch succeeds)."""

    async def scenario() -> None:
        api = MockApi()
        await api.start()
        try:
            for _ in range(12):  # enough for a Location that loops back
                api.get("/rt.pb", status=302, headers={"Location": location})
            connector = aiohttp.TCPConnector(resolver=_OfflineResolver())
            async with aiohttp.ClientSession(connector=connector) as session:
                with pytest.raises(SourceConnectionError):
                    await fetch_feed_message(
                        session,
                        api.url("/rt.pb"),
                        auth_type=2 if by_hand else None,
                        api_key_name="X-Key" if by_hand else None,
                        api_key="k" if by_hand else None,
                        timeout_seconds=5,
                    )
        finally:
            await api.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "name",
    ["X-Key\r\nX-Evil", "X Key", "X-Key:", "X:Key", "", "Kl\u00e9", "X-K\ud800ey"],
)
async def test_an_api_key_header_name_that_is_no_field_name_is_refused(
    name: str,
) -> None:
    """api_key_parameter_name is catalog data, and a bad one doesn't always
    raise: compiled aiohttp dropped a lone surrogate (sending X-Key) and sent
    X:Key as written (read as field X). Refused before the request."""
    async with aiohttp.ClientSession() as session:
        if not name:  # an empty name sends no key at all
            return
        with pytest.raises(SourceConnectionError, match="no header field name"):
            await fetch_feed_message(
                session,
                "http://127.0.0.1:1/rt.pb",
                auth_type=2,
                api_key_name=name,
                api_key="k",
            )


@pytest.mark.parametrize(
    "name",
    [
        "Host",
        "content-length",
        "Transfer-Encoding",
        "If-None-Match",
        "Range",
        "Proxy-Authorization",
        "Connection",
        "Expect",
    ],
)
async def test_an_api_key_header_name_that_changes_the_request_is_refused(
    name: str,
) -> None:
    """A key under Host routes the request elsewhere, under Content-Length
    or Transfer-Encoding reframes it, under a validator or Range changes what
    is answered, under Proxy-Authorization goes to a proxy: catalog data
    doesn't get to choose those. Nothing is sent."""
    sent: list[bytes] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        sent.append(await reader.read(1))
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with aiohttp.ClientSession() as session:
            with pytest.raises(
                SourceConnectionError, match="changes the request"
            ) as caught:
                await fetch_feed_message(
                    session,
                    f"http://127.0.0.1:{port}/rt.pb",
                    auth_type=2,
                    api_key_name=name,
                    api_key="KeySecret",
                )
    finally:
        server.close()
    assert sent == []
    assert name not in str(caught.value)
    assert "KeySecret" not in str(caught.value)


async def test_an_api_key_under_authorization_is_sent(mock_api: MockApi) -> None:
    """Producers do put keys in Authorization: it stays allowed."""
    mock_api.get("/rt.pb", body=b"", auth="Key1")
    async with aiohttp.ClientSession() as session:
        await fetch_feed_message(
            session,
            mock_api.url("/rt.pb"),
            auth_type=2,
            api_key_name="Authorization",
            api_key="Bearer Key1",
        )


_COLON_LOGINS = ["http://a%3Ab@127.0.0.1:9/feed", "http://%3A@127.0.0.1:9/feed"]


@pytest.mark.parametrize("url", _COLON_LOGINS)
async def test_a_colon_login_is_refused_before_the_request_at_every_entry_point(
    direct_client: MobilityFeedsClient, url: str
) -> None:
    """RFC 7617 section 2: a user-id ends at its first colon, so none can
    hold one. The caller's own URL is a ValueError up front; a URL from
    data is the guard's SourceConnectionError -- neither reaches aiohttp,
    which failed mid-request."""
    with pytest.raises(ValueError, match="colon"):
        await direct_client.get_gbfs_feed_from_url(url)
    with pytest.raises(ValueError, match="colon"):
        await direct_client.get_transit_feed_from_urls(url)
    async with aiohttp.ClientSession() as session:
        for by_hand in (False, True):
            with pytest.raises(SourceConnectionError, match="colon"):
                await fetch_feed_message(
                    session,
                    url,
                    auth_type=2 if by_hand else None,
                    api_key_name="X-Key" if by_hand else None,
                    api_key="k" if by_hand else None,
                )


def test_a_password_may_hold_a_colon() -> None:
    """Only the user-id can't: RFC 7617 splits at its first colon."""
    _require_http_url("http://user:pa%3Ass@127.0.0.1:9/feed", "test URL")


@pytest.mark.parametrize(
    "headers",
    [
        {"X-Key\r\nX-Evil": "1"},
        {"X Key": "1"},
        {"X-Key": "a\r\nX-Evil: 1"},
        {"X-Key": "a\x00b"},
        {"X-Key": "a\x7f"},
        {"X-Key": "sec\ud800ret"},  # aiohttp dropped it: "secret" was sent
        {"X-Key": "a\udc80"},
    ],
)
async def test_a_header_http_cant_carry_is_the_callers_mistake(
    mock_api: MockApi, direct_client: MobilityFeedsClient, headers: dict[str, str]
) -> None:
    """Refused up front as ValueError -- so a ValueError mid-request is
    always the producer's (the network boundary's SourceConnectionError)."""
    with pytest.raises(ValueError, match=r"visible ASCII|field name"):
        await direct_client.get_transit_feed_from_urls(
            mock_api.url(STATIC_PATH), headers=headers
        )
    with pytest.raises(ValueError, match=r"visible ASCII|field name"):
        await direct_client.get_gbfs_feed_from_url(
            mock_api.url("/gbfs/gbfs.json"), headers=headers
        )
    assert mock_api.requests == []


async def test_an_api_key_http_cant_carry_is_the_callers_mistake(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    for api_key in ("k\r\nX-Evil: 1", "k\ud800ey", "k\udcffey"):
        with pytest.raises(ValueError, match="visible ASCII"):
            await direct_client.get_transit_feed("mdb-1", api_key=api_key)
    assert mock_api.requests == []


async def test_a_tab_or_visible_ascii_in_a_header_value_is_fine(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    feeds = [{"name": "system_information", "url": mock_api.url("/gbfs/si.json")}]
    mock_api.get(
        "/gbfs/gbfs.json",
        payload={"ttl": 0, "last_updated": 0, "data": {"feeds": feeds}},
    )
    handle = await direct_client.get_gbfs_feed_from_url(
        mock_api.url("/gbfs/gbfs.json"), headers={"X-Note": "a\tb c ~!"}
    )
    assert handle is not None
    assert mock_api.requests[0].headers["X-Note"].startswith("a\tb c ~!")
    # The allowlist is visible ASCII: other text is refused, not sent as
    # whatever bytes aiohttp would make of it.
    with pytest.raises(ValueError, match="visible ASCII"):
        await direct_client.get_gbfs_feed_from_url(
            mock_api.url("/gbfs/gbfs.json"), headers={"X-Note": "caf\u00e9"}
        )


# st.lists, not st.text: st.text never yields a lone surrogate.
@given(
    value=st.lists(
        st.characters(codec="utf-8", exclude_categories=["Cs"])
        | st.sampled_from(["\ud800", "\udcff", "\t", "\r", "\x00"]),
        max_size=12,
    ).map("".join)
)
@settings(max_examples=200, deadline=None)
def test_a_header_value_is_refused_or_reaches_the_server_as_is(value: str) -> None:
    """Either the caller's mistake, up front (ValueError), or the producer
    receives exactly that value -- less the surrounding whitespace, which
    is no part of an HTTP field value (RFC 9110 section 5.5)."""

    async def scenario() -> None:
        api = MockApi()
        await api.start()
        try:
            api.get("/gbfs/gbfs.json", status=503)
            async with MobilityFeedsClient() as client:
                try:
                    await client.get_gbfs_feed_from_url(
                        api.url("/gbfs/gbfs.json"), headers={"X-Key": value}
                    )
                except ValueError:
                    assert api.requests == []
                    return
                except SourceConnectionError:
                    pass
            # Surrounding whitespace is OWS, no part of the value (servers
            # differ in stripping it: aiohttp 3.10 keeps a trailing tab).
            received = api.requests[0].headers["X-Key"]
            assert received.strip(" \t") == value.strip(" \t")
        finally:
            await api.stop()

    asyncio.run(scenario())


# BasicAuth and session auth are deprecated in aiohttp 3.14, absent its
# replacement in 3.10 (the floor): still the case to refuse.
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize(
    ("url", "headers", "session_auth"),
    [
        ("http://u:p@127.0.0.1:9/feed", {"Authorization": "Bearer t"}, None),
        ("http://127.0.0.1:9/feed", {"authorization": "Bearer t"}, ("u", "p")),
        (":pw@", {"Authorization": "Bearer t"}, None),  # a password alone counts
    ],
)
async def test_credentials_that_cant_all_be_sent_are_the_callers_mistake(
    url: str, headers: dict[str, str] | None, session_auth: tuple[str, str] | None
) -> None:
    """aiohttp sends one Authorization field and raised ValueError
    mid-request -- which open_response would have called the source's."""
    if url == ":pw@":
        url = "http://:pw@127.0.0.1:9/feed"
    auth = aiohttp.BasicAuth(*session_auth) if session_auth else None
    async with aiohttp.ClientSession(auth=auth) as session:
        client = MobilityFeedsClient(session=session)
        # One Authorization field per request; a session auth is refused
        # outright (it would ride on every request).
        with pytest.raises(ValueError, match=r"can't go with|every request"):
            await client.get_gbfs_feed_from_url(url, headers=headers)
        with pytest.raises(ValueError, match=r"can't go with|every request"):
            await client.get_transit_feed_from_urls(url, headers=headers)


@pytest.mark.parametrize(
    "url",
    [
        "http://a%0Ab:p@127.0.0.1:9/f",
        "http://a:p%00x@127.0.0.1:9/f",
        "http://a%09:p@127.0.0.1:9/f",
    ],
)
def test_credentials_with_a_control_character_are_refused(url: str) -> None:
    """RFC 7617 section 2: neither user-id nor password holds a control
    character -- aiohttp sent them, newline and NUL included."""
    with pytest.raises(SourceConnectionError, match="control character"):
        _require_http_url(url, "test URL")


_UNSENDABLE_CREDENTIALS = [
    ("http://%E2%82%AC:pw@127.0.0.1:9/f", "outside Latin-1"),
    ("http://\u20ac:pw@127.0.0.1:9/f", "outside Latin-1"),
    ("http://u:p%E2%82%AC@127.0.0.1:9/f", "outside Latin-1"),
    ("http://:%F0%9F%9A%B2@127.0.0.1:9/f", "outside Latin-1"),  # password alone
    ("http://a%3Ab:p@127.0.0.1:9/f", "colon"),
    ("http://a%0Ab:p@127.0.0.1:9/f", "control character"),
]


@pytest.mark.parametrize(("url", "reason"), _UNSENDABLE_CREDENTIALS)
def test_credentials_basic_auth_cant_send_are_refused_by_the_guard(
    url: str, reason: str
) -> None:
    """Basic auth sends credentials as Latin-1, a user-id without a colon:
    aiohttp failed mid-request ("Request failed (UnicodeEncodeError)") on
    anything else. A producer's URL is refused before the request."""
    with pytest.raises(SourceConnectionError, match=reason) as caught:
        _require_http_url(url, "test URL")
    assert "pw" not in str(caught.value)


@pytest.mark.parametrize(("url", "reason"), _UNSENDABLE_CREDENTIALS)
async def test_caller_url_credentials_basic_auth_cant_send_are_a_value_error(
    mock_api: MockApi, url: str, reason: str
) -> None:
    """The caller's own URL is the caller's input: refused up front, a
    ValueError, before any request."""
    url = url.replace("127.0.0.1:9", URL(mock_api.url("/")).raw_authority)
    async with aiohttp.ClientSession() as session:
        client = MobilityFeedsClient(session=session)
        with pytest.raises(ValueError, match=reason) as caught:
            await client.get_gbfs_feed_from_url(url)
        assert not isinstance(caught.value, MobilityDatabaseError)
        with pytest.raises(ValueError, match=reason):
            await client.get_transit_feed_from_urls(url)
        with pytest.raises(ValueError, match=reason):
            await client.get_transit_feed_from_urls(
                mock_api.url(STATIC_PATH), service_alerts_urls=[url]
            )
    assert mock_api.requests == []


@given(
    user=st.text(max_size=8),
    password=st.text(max_size=8),
)
@settings(max_examples=300)
def test_url_credentials_are_refused_exactly_when_basic_auth_cant_send_them(
    user: str, password: str
) -> None:
    """The guard's allowlist: Latin-1, no control character, no colon in
    the user-id. Whatever it passes, Basic auth encodes."""
    url = str(URL.build(scheme="http", host="h", user=user, password=password))
    parsed = URL(url)
    sent_user, sent_password = parsed.user or "", parsed.password or ""
    sendable = ":" not in sent_user and all(
        ch >= " " and ch != "\x7f" and ord(ch) <= 0xFF
        for ch in sent_user + sent_password
    )
    try:
        _require_http_url(url, "test URL")
    except SourceConnectionError:
        assert not sendable
    else:
        assert sendable
        f"{sent_user}:{sent_password}".encode("latin-1")


async def test_a_mid_request_error_echoes_no_request_header(
    mock_api: MockApi, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An error's own text can carry every request header (aiohttp's
    pure-Python writer); the SourceConnectionError names its type only,
    and carries nothing of it along."""

    async def failing(*_args: object, **kwargs: object) -> None:
        raise UnicodeEncodeError("latin-1", f"{kwargs['headers']}", 0, 1, "no")

    async with aiohttp.ClientSession() as session:
        monkeypatch.setattr(session, "request", failing)
        with pytest.raises(SourceConnectionError) as caught:
            await fetch_feed_message(
                session, mock_api.url("/rt.pb"), headers={"X-Secret": "TOPSECRET"}
            )
    assert str(caught.value) == "Request failed (UnicodeEncodeError)"
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
async def test_a_session_auth_is_refused(mock_api: MockApi) -> None:
    """It would ride on every request, to every origin: the shared policy."""
    url = mock_api.url("/gbfs/gbfs.json").replace("http://", "http://u:p@")
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("sess", "ion")) as session:
        client = MobilityFeedsClient(session=session)
        with pytest.raises(ValueError, match="every request"):
            await client.get_gbfs_feed_from_url(url)
    assert mock_api.requests == []


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize(
    ("session_headers", "session_auth"),
    [
        ({"X-Key": "a\nb"}, None),
        ({"X-Key": "sec\ud800ret"}, None),  # compiled aiohttp sent "secret"
        ({"Bad Name": "v"}, None),
        ({"Authorization": "Bearer t"}, ("u", "p")),
        ({}, ("u", "p\u20ac")),  # BasicAuth encodes as Latin-1
    ],
)
async def test_a_session_default_aiohttp_would_refuse_is_the_callers_mistake(
    mock_api: MockApi,
    session_headers: dict[str, str],
    session_auth: tuple[str, str] | None,
) -> None:
    """A caller-injected session's defaults ride on every request: checked
    when a handle is made, like headers=, at every factory."""
    auth = aiohttp.BasicAuth(*session_auth) if session_auth else None
    async with aiohttp.ClientSession(headers=session_headers, auth=auth) as session:
        client = MobilityFeedsClient(session=session)
        for make in (
            lambda: client.get_gbfs_feed_from_url(mock_api.url("/gbfs/gbfs.json")),
            lambda: client.get_transit_feed_from_urls(mock_api.url(STATIC_PATH)),
            lambda: client.get_transit_feed("mdb-1"),
            lambda: client.get_gbfs_feed("gbfs-1"),
        ):
            with pytest.raises(ValueError):
                await make()
    assert mock_api.requests == []


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:9/a\ud800b",
        "http://u\ud800:p@127.0.0.1:9/",
        "http://127.0.0.1:9/?k=\udcff",
    ],
)
def test_a_url_that_isnt_utf_8_text_is_refused(url: str) -> None:
    """yarl drops a lone surrogate: the request would go to another URL, or
    with other credentials."""
    with pytest.raises(SourceConnectionError, match="isn't UTF-8 text"):
        _require_http_url(url, "test URL")


async def test_a_redirect_to_a_url_that_isnt_utf_8_text_is_refused(
    mock_api: MockApi,
) -> None:
    mock_api.get("/rt.pb", status=302, headers={"Location": "/a\udcffb"})
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError):
            await fetch_feed_message(
                session,
                mock_api.url("/rt.pb"),
                auth_type=2,
                api_key_name="X-Key",
                api_key="k",
            )


async def test_a_query_key_name_that_isnt_text_is_refused() -> None:
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError, match="isn't text"):
            await fetch_feed_message(
                session,
                "http://127.0.0.1:9/rt.pb",
                auth_type=1,
                api_key_name="k\ud800",
                api_key="v",
            )


# -- Secrets stay out of error text --------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "ftp://user:SECRETPW@x/?key=SECRETKEY",
        "http://user:SECRET%0APW@x/?key=SECRETKEY",
    ],
)
def test_a_refused_url_shows_no_credentials(url: str) -> None:
    """The message is the guard's reason alone; the URL, on .url, has no
    credentials, query or fragment."""
    with pytest.raises(SourceConnectionError) as caught:
        _require_http_url(url, "test URL")
    for shown in (str(caught.value), caught.value.url or ""):
        assert "SECRET" not in shown


async def test_a_redirect_loop_shows_no_query_api_key(mock_api: MockApi) -> None:
    """aiohttp's TooManyRedirects text carries the real URL, ?key= and all."""
    for _ in range(12):
        mock_api.get("/loop", status=302, headers={"Location": "/loop"})
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError) as caught:
            await fetch_feed_message(
                session,
                mock_api.url("/loop"),
                auth_type=1,
                api_key_name="key",
                api_key="SECRET123",
            )
    assert "SECRET123" not in str(caught.value)


async def test_an_unrequestable_url_shows_no_credentials(mock_api: MockApi) -> None:
    mock_api.get(
        "/rt.pb",
        status=302,
        headers={
            "Location": mock_api.url("/x").replace("http://", "http://%E2%82%AC@")
        },
    )
    url = mock_api.url("/rt.pb").replace("http://", "http://user:SECRETPW@")
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError) as caught:
            await fetch_feed_message(session, url)
    assert "SECRETPW" not in str(caught.value)


# -- The network boundary, as a property ----------------------------------------------

_FEED_SECRETS = ("HdrSecret123", "QrySecret456", "UrlPw789")


async def _raw_feed_server(response: bytes) -> tuple[asyncio.Server, str]:
    """Answer every request with ``response`` verbatim."""

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(response)
            await writer.drain()
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"


_FEED_STATUS = st.sampled_from(
    [
        b"HTTP/1.1 200 OK",
        b"HTTP/1.1 302 Found",
        b"HTTP/1.1 307 X",
        b"HTTP/1.1 304 X",
        b"HTTP/1.1 401 X",
        b"HTTP/1.1 500 X",
        b"HTTP/1.1 999 X",
        b"garbage",
        b"",
    ]
)
_FEED_HEADERS = st.sampled_from(
    [
        b"Content-Type: application/json; charset=base64",
        b"Content-Type: \x80\xc3\xa9",
        b"X-Echo: HdrSecret123\x00",
        b"Echo QrySecret456",
        b"X-Echo: UrlPw789\rx",
        b"Location: /next?key=QrySecret456",
        b"Location: http://u:UrlPw789@127.0.0.1:9/x",
        b"Location: \xff\xfe",
        b"Location: http://[::1]:@/x",
        b"Location: https:",
        b"Location: http://a..b/x",
        b"Location: //[::1/x",
        b'ETag: "v\xff1"',
        b"Content-Encoding: gzip",
        b"Content-Length: 99999",
        b"Transfer-Encoding: chunked",
        b'Cache-Control: max-age="6\\"',
        b"Expires: \xc5\xbfat, 01 Aug 2026 00:00:00 GMT",
    ]
)
_FEED_BODIES = st.sampled_from(
    [
        b"",
        b'{"data": {}}',
        b"[" * 3000,
        b'{"a":1,"a":2}',
        b"\xed\xa0\x80",
        b"\x08\x96\x01",
        b"HdrSecret123 QrySecret456 UrlPw789",
    ]
)
_FEED_RESPONSES = st.builds(
    lambda status, headers, body: (
        status + b"\r\n" + b"".join(h + b"\r\n" for h in headers) + b"\r\n" + body
    ),
    _FEED_STATUS,
    st.lists(_FEED_HEADERS, max_size=4),
    _FEED_BODIES,
)


def _require_no_secret_in_chain(
    err: BaseException | None, secrets: tuple[str, ...]
) -> None:
    """No secret in the error's message or .url, nor in any error chained to
    it, hidden from tracebacks or not -- and a library error has nothing
    chained at all, on every path (network, decode, archive)."""
    if err is not None:
        assert err.__cause__ is None, repr(err.__cause__)
        assert err.__context__ is None, repr(err.__context__)
    while err is not None:
        shown = (str(err) + " " + str(getattr(err, "url", "") or "")).lower()
        for secret in secrets:
            assert secret.lower() not in shown, (type(err).__name__, shown)
        err = err.__cause__ or err.__context__


@given(
    response=_FEED_RESPONSES,
    path=st.sampled_from(["gbfs", "rt-header", "rt-query", "static"]),
)
@settings(max_examples=300, deadline=None)
def test_any_answer_gives_a_library_result_and_no_secret_in_any_message(
    response: bytes, path: str
) -> None:
    """The feeds' boundary as a property: whatever bytes a producer sends --
    malformed, redirecting anywhere, echoing the credentials -- a fetch
    returns or raises a library error, and no secret (a header key, a query
    key, a URL password) appears in any message, chained one, or .url."""

    async def scenario() -> BaseException | None:
        server, base = await _raw_feed_server(response)
        url = base.replace("http://", "http://user:UrlPw789@") + "/feed"
        try:
            async with MobilityFeedsClient() as client:
                try:
                    if path == "gbfs":
                        await asyncio.wait_for(
                            client.get_gbfs_feed_from_url(
                                url, headers={"X-Key": "HdrSecret123"}
                            ),
                            10,
                        )
                    elif path == "static":
                        await asyncio.wait_for(
                            client.get_transit_feed_from_urls(url), 10
                        )
                    else:
                        await asyncio.wait_for(
                            fetch_feed_message(
                                client._get_session(),
                                url,
                                auth_type=2 if path == "rt-header" else 1,
                                api_key_name="X-Key",
                                api_key="HdrSecret123"
                                if path == "rt-header"
                                else "QrySecret456",
                                timeout_seconds=5,
                            ),
                            10,
                        )
                except (MobilityFeedsError, MobilityDatabaseError) as err:
                    return err
            return None
        finally:
            server.close()

    err = asyncio.run(scenario())
    _require_no_secret_in_chain(err, _FEED_SECRETS)


@pytest.mark.parametrize("by_hand_key", [False, True])
async def test_a_location_that_isnt_utf_8_is_never_followed(by_hand_key: bool) -> None:
    """A byte that isn't UTF-8 arrives as a lone surrogate; yarl would drop
    it and request /real.json, another URL than the producer named."""
    paths: list[bytes] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        request = await reader.readuntil(b"\r\n\r\n")
        paths.append(request.split(b" ")[1])
        writer.write(
            b"HTTP/1.1 302 Found\r\nLocation: /real\xff.json\r\n"
            b"Content-Length: 0\r\n\r\n"
        )
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/feed"
    try:
        async with aiohttp.ClientSession() as session:
            with pytest.raises(SourceConnectionError, match="invalid URL"):
                await fetch_feed_message(
                    session,
                    url,
                    auth_type=2 if by_hand_key else None,
                    api_key_name="X-Key" if by_hand_key else None,
                    api_key="k" if by_hand_key else None,
                )
    finally:
        server.close()
    assert paths == [b"/feed"]


# -- Two origins: what a hop may carry, and what a message may show -------------------

_A_STATUS = st.sampled_from(
    [b"200 OK", b"301 X", b"302 X", b"303 X", b"307 X", b"401 X", b"500 X"]
)
_A_HEADERS = st.lists(
    st.sampled_from(
        [
            # (No echo inside a Location: a producer putting the credential
            # into its own redirect hands it over itself.)
            b"Location: {B}/next",
            b"Location: {B}/next?k=1",
            b"Location: http://other:pw@{BHOST}/x",
            b"Location: /same",
            b"Content-Type: text/{echo}",
            b"X-Echo: {echo}",
            b'ETag: "{echo}"',
        ]
    ),
    max_size=3,
)
# A GBFS discovery document is the producer's data naming further URLs:
# one at origin B (where the caller's credentials must not go) and one at A.
_DISCOVERY = (
    b'{"last_updated": 0, "ttl": 0, "version": "2.3", "data": {"en": {"feeds": '
    b'[{"name": "system_information", "url": "%s"}]}}}'
)
_A_BODIES = st.sampled_from(
    [
        b"",
        b"{echo}",
        b'{"data": {"x": "{echo}"}}',
        b"\x08\x96\x01",
        _DISCOVERY % b"{B}/si",
        _DISCOVERY % b"{A}/si",
    ]
)


@given(
    a_answer=st.tuples(_A_STATUS, _A_HEADERS, _A_BODIES),
    path=st.sampled_from(["gbfs", "rt-header", "rt-query", "static"]),
)
@settings(max_examples=300, deadline=None)
def test_a_hop_to_another_origin_carries_no_credential_and_no_message_shows_one(  # noqa: PLR0915
    a_answer: tuple[bytes, list[bytes], bytes], path: str
) -> None:
    """Origin A answers anything -- echoing the request into headers and
    bodies, redirecting to origin B (another port) or within itself. Origin
    B records what reaches it and echoes it back. B must never receive a
    credential addressed to A (the Authorization header, the header-auth
    key, the URL's password), and no exception message, chained one or .url
    may show one -- in any case, base64 Basic form included."""
    password, header_secret, query_secret, bearer = (
        "UrlPw789",
        "HdrSecret123",
        "QrySecret456",
        "BearerSecret000",
    )
    basic = base64.b64encode(f"user:{password}".encode()).decode()
    secrets = (password, header_secret, query_secret, bearer, basic)
    b_requests: list[bytes] = []

    def echo_of(request: bytes) -> bytes:
        return request.replace(b"\r", b" ").replace(b"\n", b" ")

    async def handle_b(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            b_requests.append(request)
            body = echo_of(request)
            writer.write(
                b"HTTP/1.1 500 X\r\nX-Echo: "
                + body
                + b"\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\n\r\n"
                + body
            )
            await writer.drain()
        finally:
            writer.close()

    async def scenario() -> BaseException | None:
        server_b = await asyncio.start_server(handle_b, "127.0.0.1", 0)
        b_host = f"127.0.0.1:{server_b.sockets[0].getsockname()[1]}"

        async def handle_a(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                request = await reader.readuntil(b"\r\n\r\n")
                status, headers, body = a_answer
                fill = {
                    b"{B}": f"http://{b_host}".encode(),
                    b"{BHOST}": b_host.encode(),
                    b"{echo}": echo_of(request),
                    b"{A}": f"http://127.0.0.1:{a_port}".encode(),
                }
                lines = []
                for template in headers:
                    line = template
                    for key, value in fill.items():
                        line = line.replace(key, value)
                    lines.append(line)
                payload = body
                for key, value in fill.items():
                    payload = payload.replace(key, value)
                writer.write(
                    b"HTTP/1.1 "
                    + status
                    + b"\r\n"
                    + b"".join(h + b"\r\n" for h in lines)
                    + b"Content-Length: "
                    + str(len(payload)).encode()
                    + b"\r\n\r\n"
                    + payload
                )
                await writer.drain()
            finally:
                writer.close()

        server_a = await asyncio.start_server(handle_a, "127.0.0.1", 0)
        a_port = server_a.sockets[0].getsockname()[1]
        url = f"http://user:{password}@127.0.0.1:{a_port}/feed"
        try:
            async with MobilityFeedsClient() as client:
                try:
                    if path == "gbfs":
                        # Credentials in a header (not the URL, which
                        # aiohttp refuses beside one): they go to A only,
                        # even where A's discovery document names B.
                        handle = await asyncio.wait_for(
                            client.get_gbfs_feed_from_url(
                                url.replace(f"user:{password}@", ""),
                                headers={
                                    "X-Trace": "ok",
                                    "Authorization": f"Bearer {bearer}",
                                },
                            ),
                            10,
                        )
                        call = handle.get_system_info()
                    elif path == "static":
                        call = client.get_transit_feed_from_urls(url)
                    else:
                        # An Authorization header, not URL credentials:
                        # aiohttp refuses the two together.
                        call = fetch_feed_message(
                            client._get_session(),
                            url.replace(f"user:{password}@", ""),
                            auth_type=2 if path == "rt-header" else 1,
                            api_key_name="X-Key",
                            api_key=header_secret
                            if path == "rt-header"
                            else query_secret,
                            headers={
                                "X-Trace": "ok",
                                "Authorization": f"Bearer {bearer}",
                            },
                            timeout_seconds=5,
                        )
                    await asyncio.wait_for(call, 10)
                except (MobilityFeedsError, MobilityDatabaseError) as err:
                    return err
            return None
        finally:
            server_a.close()
            server_b.close()

    err = asyncio.run(scenario())
    for request in b_requests:
        text = request.decode("latin-1").lower()
        for secret in (password, header_secret, bearer, basic):
            assert secret.lower() not in text, request
    _require_no_secret_in_chain(err, secrets)


async def test_a_session_raising_for_status_keeps_the_status_mapping(
    mock_api: MockApi,
) -> None:
    """Every request opts out of the session's raise_for_status: a 404 stays
    a SourceConnectionError with its status, a 401 a SourceAuthenticationError."""
    mock_api.get("/gbfs/gbfs.json", status=404)
    mock_api.get("/rt.pb", status=401)
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        client = MobilityFeedsClient(session=session)
        with pytest.raises(SourceConnectionError) as caught:
            await client.get_gbfs_feed_from_url(mock_api.url("/gbfs/gbfs.json"))
        assert caught.value.status == 404
        with pytest.raises(SourceAuthenticationError) as auth:
            await fetch_feed_message(session, mock_api.url("/rt.pb"))
        assert auth.value.status == 401


@pytest.mark.parametrize(
    "url", ["file:///etc/passwd", "http://h\ud800st/feed", "http:///feed", "x"]
)
async def test_the_redirect_loop_checks_its_own_url(url: str) -> None:
    """Callers check first; the one network path checks again, so a new
    caller can't reach aiohttp with a URL yarl would change or refuse."""
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError, match="Refusing") as caught:
            async with get_withholding(
                session, url, params={}, headers={}, total=5, secret=None
            ):
                pass
    assert caught.value.__context__ is None


@pytest.mark.parametrize("where", ["other-origin", "same-origin"])
async def test_gbfs_credentials_go_only_to_the_discovery_origin(where: str) -> None:
    """The endpoint URLs are the producer's data: Authorization goes to an
    endpoint at the discovery URL's origin, never to another; other caller
    headers go to both."""
    received: dict[str, dict[str, str]] = {}

    def serve(name: str, documents: dict[str, bytes]) -> Any:
        async def handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            target = head.split(" ")[1]
            received[f"{name}{target}"] = {
                line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip()
                for line in head.split("\r\n")[1:]
                if ":" in line
            }
            body = documents[target]
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
            )
            await writer.drain()
            writer.close()

        return handle

    info = b'{"last_updated": 0, "ttl": 0, "data": {"system_id": "s"}}'
    docs_b = {"/si": info}
    server_b = await asyncio.start_server(serve("B", docs_b), "127.0.0.1", 0)
    b = f"http://127.0.0.1:{server_b.sockets[0].getsockname()[1]}"
    docs_a: dict[str, bytes] = {"/si": info}
    server_a = await asyncio.start_server(serve("A", docs_a), "127.0.0.1", 0)
    a = f"http://127.0.0.1:{server_a.sockets[0].getsockname()[1]}"
    endpoint = f"{b}/si" if where == "other-origin" else f"{a}/si"
    docs_a["/gbfs.json"] = (
        b'{"last_updated": 0, "ttl": 0, "version": "2.3", "data": {"en": '
        b'{"feeds": [{"name": "system_information", "url": "'
        + endpoint.encode()
        + b'"}]}}}'
    )
    try:
        async with MobilityFeedsClient() as client:
            handle = await client.get_gbfs_feed_from_url(
                f"{a}/gbfs.json",
                headers={"Authorization": "Bearer GbfsSecret", "X-Trace": "ok"},
            )
            await handle.get_system_info()
    finally:
        server_a.close()
        server_b.close()
    assert received["A/gbfs.json"]["authorization"] == "Bearer GbfsSecret"
    seen = received["B/si" if where == "other-origin" else "A/si"]
    assert seen["x-trace"] == "ok"
    if where == "other-origin":
        assert "authorization" not in seen
    else:
        assert seen["authorization"] == "Bearer GbfsSecret"


async def test_a_producer_url_with_credentials_beside_an_authorization_header(
    mock_api: MockApi,
) -> None:
    """A discovery document naming an endpoint with credentials, fetched with
    the caller's Authorization header: which to send isn't the library's to
    pick, so neither is, and nothing reaches the endpoint."""
    endpoint = mock_api.url("/si").replace("http://", "http://u:p@")
    mock_api.get(
        "/gbfs.json",
        payload={
            "last_updated": 0,
            "ttl": 0,
            "version": "2.3",
            "data": {
                "en": {"feeds": [{"name": "system_information", "url": endpoint}]}
            },
        },
    )
    async with MobilityFeedsClient() as client:
        handle = await client.get_gbfs_feed_from_url(
            mock_api.url("/gbfs.json"), headers={"Authorization": "Bearer Tok"}
        )
        with pytest.raises(SourceConnectionError, match="can't both be sent") as caught:
            await handle.get_system_info()
    assert caught.value.__context__ is None
    assert "/si" not in [r.path for r in mock_api.requests]
