"""Tests for direct-URL mode: transit and GBFS handles without the catalog."""

import io
import zipfile
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from aiomobilitydatabase.exceptions import MobilityDatabaseError
from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import (
    FeedParseError,
    SourceConnectionError,
)
from aiomobilitydatabase.feeds.models import StaticBuildProgress

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
from tests.mock_server import MockApi

PB = "application/octet-stream"
STATIC_PATH = "/direct/gtfs.zip"
TU_PATH = "/direct/rt/trip_updates"
VP_PATH = "/direct/rt/vehicle_positions"
HDRS = {"X-Custom-Token": "abc123"}
NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)  # Thursday 07:45 PDT


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
    assert handle.rt_feeds == []
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
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
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


async def test_direct_refresh_static_etag(
    mock_api: MockApi, direct_client_cached: MobilityFeedsClient
) -> None:
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
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
        STATIC_PATH, body=_zip_with_extra_stop(), content_type="application/zip"
    )
    assert await handle.refresh_static() is True
    assert "S9" in {stop.id for stop in handle.stops}
    assert len(_requests_for(mock_api, "GET", STATIC_PATH)) == 2


async def test_direct_refresh_static_last_modified(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    # No ETag anywhere: Last-Modified is the second-choice validator.
    mock_api.head(
        STATIC_PATH, headers={"Last-Modified": "Thu, 30 Jul 2026 00:00:00 GMT"}
    )
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    handle = await direct_client.get_transit_feed_from_urls(mock_api.url(STATIC_PATH))
    mock_api.head(
        STATIC_PATH, headers={"Last-Modified": "Thu, 30 Jul 2026 00:00:00 GMT"}
    )
    assert await handle.refresh_static() is False
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
    with pytest.raises(SourceConnectionError, match="Error probing"):
        await direct_client.get_transit_feed_from_urls("http://127.0.0.1:1/gtfs.zip")


async def test_direct_transit_rejects_non_http_urls(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    with pytest.raises(SourceConnectionError, match="scheme"):
        await direct_client.get_transit_feed_from_urls("file:///etc/passwd")
    # An offending RT url is rejected up front too: before any HTTP at all.
    with pytest.raises(SourceConnectionError, match="scheme"):
        await direct_client.get_transit_feed_from_urls(
            mock_api.url(STATIC_PATH), ["ftp://rt.example/feed"]
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


async def test_direct_rt_merge_and_vehicles(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    """Two direct RT urls: a TU producer merges into arrivals; a VP-only
    producer contributes nothing there (its protobuf simply has no
    trip_update entities) but serves get_vehicles. Both feeds advertise all
    entity types, so each snapshot method polls both urls.
    """
    mock_api.head(STATIC_PATH, headers={"ETag": '"v1"'})
    mock_api.get(
        STATIC_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip"
    )
    # get_arrivals polls both urls for TripUpdates; get_vehicles polls both
    # for VehiclePositions: two scripted responses per url.
    mock_api.get(TU_PATH, body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    mock_api.get(TU_PATH, body=TRIP_UPDATES_T1_DELAYED, content_type=PB)
    mock_api.get(VP_PATH, body=VEHICLE_POSITIONS, content_type=PB)
    mock_api.get(VP_PATH, body=VEHICLE_POSITIONS, content_type=PB)
    handle = await direct_client.get_transit_feed_from_urls(
        mock_api.url(STATIC_PATH),
        [mock_api.url(TU_PATH), mock_api.url(VP_PATH)],
        headers=HDRS,
    )
    assert [feed.id for feed in handle.rt_feeds] == [
        mock_api.url(TU_PATH),
        mock_api.url(VP_PATH),
    ]
    arrivals = await handle.get_arrivals(
        ["S1", "S2"], lookahead=timedelta(hours=1), now_utc=NOW
    )
    by_key = {(a.trip_id, a.stop_id): a for a in arrivals}
    assert not any(trip_id == "T2" for trip_id, _ in by_key)  # canceled by RT
    t1_s1 = by_key[("T1", "S1")]
    assert t1_s1.realtime is True
    assert t1_s1.delay_seconds == 300
    assert ("ADDED-9", "S2") in by_key  # RT-added trip came through
    vehicles = await handle.get_vehicles()
    assert {vehicle.vehicle_id for vehicle in vehicles} == {"V1", "V2"}
    # Every RT fetch (both urls, both snapshot methods) carried the headers.
    for path in (TU_PATH, VP_PATH):
        rt_requests = _requests_for(mock_api, "GET", path)
        assert len(rt_requests) == 2
        assert all(
            req.headers.get("X-Custom-Token") == "abc123"  # type: ignore[attr-defined]
            for req in rt_requests
        )


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
    mock_api.get("/gbfs/gbfs.json", payload=with_base(DISCOVERY_23, base))
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
    mock_api.get("/gbfs3/gbfs.json", payload=with_base(DISCOVERY_30, base))
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
    discovery = with_base(DISCOVERY_23, base)
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
