"""Producer freshness end to end: realtime and GBFS handles over HTTP."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import aiohttp
import pytest
from google.transit import gtfs_realtime_pb2

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import SourceConnectionError
from aiomobilitydatabase.feeds.models import ArrivalsQuery

from tests.feeds.fixtures import (
    GBFS_FEED,
    GTFS_FEED,
    GTFS_RT_FEED,
    SYSTEM_INFO_23,
    TOKEN_RESPONSE,
    VEHICLE_POSITIONS,
    build_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

PB = "application/octet-stream"
NOW = datetime(2026, 7, 30, 14, 45, tzinfo=UTC)  # Thursday 07:45 PDT
RT = "/rt/all"


@dataclass
class _Clock:
    now: float = 1000.0

    def __call__(self) -> float:
        return self.now


def _mock_transit(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[with_base(GTFS_RT_FEED, base)]
    )
    mock_api.get(
        "/hosted/mdb-100.zip",
        body=build_gtfs_zip_bytes(),
        content_type="application/zip",
    )


def _t1_delayed(delay: int, *, timestamp: int) -> bytes:
    """A TripUpdates message delaying today's T1 by ``delay`` seconds."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = timestamp
    entity = message.entity.add()
    entity.id = "t1"
    entity.trip_update.trip.trip_id = "T1"
    entity.trip_update.trip.start_date = "20260730"
    entity.trip_update.delay = delay
    return message.SerializeToString()


def _rt_hits(mock_api: MockApi) -> list[dict[str, str]]:
    return [r.headers for r in mock_api.requests if r.path == RT]


async def _t1_delay(handle: object) -> int | None:
    [board] = await handle.get_arrivals(  # type: ignore[attr-defined]
        [ArrivalsQuery(["S1"], limit=1)], start=NOW, end=NOW + timedelta(hours=2)
    )
    assert [row.trip_id for row in board] == ["T1"]
    return board[0].delay_seconds


async def test_realtime_within_max_age_costs_no_request_then_revalidates(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    mock_api.get(
        RT,
        body=_t1_delayed(60, timestamp=100),
        content_type=PB,
        headers={"Cache-Control": "max-age=30", "ETag": '"v1"'},
    )
    mock_api.get(
        RT, status=304, headers={"Cache-Control": "max-age=30", "ETag": '"v1"'}
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    clock = _Clock()
    handle._rt_cache._clock = clock
    assert await _t1_delay(handle) == 60
    clock.now += 29
    assert await _t1_delay(handle) == 60
    assert len(_rt_hits(mock_api)) == 1  # fresh: no request at all
    clock.now += 2
    assert await _t1_delay(handle) == 60  # 304: the stored parse
    hits = _rt_hits(mock_api)
    assert len(hits) == 2
    assert hits[1].get("If-None-Match") == '"v1"'
    clock.now += 29
    assert await _t1_delay(handle) == 60
    assert len(_rt_hits(mock_api)) == 2  # the 304 renewed the lifetime


async def test_no_cache_revalidates_every_call(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    mock_api.get(
        RT,
        body=_t1_delayed(60, timestamp=100),
        content_type=PB,
        headers={"Cache-Control": "no-cache, max-age=300", "ETag": '"v1"'},
    )
    mock_api.get(RT, status=304, headers={"Cache-Control": "no-cache", "ETag": '"v1"'})
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 60
    hits = _rt_hits(mock_api)
    assert len(hits) == 2
    assert hits[1].get("If-None-Match") == '"v1"'


async def test_a_stale_cdn_copy_never_replaces_a_newer_message(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    mock_api.get(RT, body=_t1_delayed(60, timestamp=200), content_type=PB)
    # An edge that has not caught up serves an older snapshot...
    mock_api.get(RT, body=_t1_delayed(600, timestamp=100), content_type=PB)
    # ...and the next request (the stored copy was kept but marked stale)
    # reaches a current one.
    mock_api.get(RT, body=_t1_delayed(120, timestamp=300), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 120
    assert len(_rt_hits(mock_api)) == 3


async def test_rt_min_interval_is_an_opt_in_request_budget(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    for delay in (60, 120):  # no caching headers at all
        mock_api.get(RT, body=_t1_delayed(delay, timestamp=delay), content_type=PB)
    handle = await feeds_client.get_transit_feed(
        "mdb-100", rt_min_interval=timedelta(seconds=10)
    )
    clock = _Clock()
    handle._rt_cache._clock = clock
    assert await _t1_delay(handle) == 60
    clock.now += 9
    assert await _t1_delay(handle) == 60
    assert len(_rt_hits(mock_api)) == 1
    clock.now += 1
    assert await _t1_delay(handle) == 120
    assert len(_rt_hits(mock_api)) == 2


async def test_rt_min_interval_never_extends_a_declared_lifetime(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """max-age=0 is a declaration, not an absence: the budget doesn't apply."""
    _mock_transit(mock_api)
    for delay in (60, 120):
        mock_api.get(
            RT,
            body=_t1_delayed(delay, timestamp=delay),
            content_type=PB,
            headers={"Cache-Control": "max-age=0"},
        )
    handle = await feeds_client.get_transit_feed(
        "mdb-100", rt_min_interval=timedelta(seconds=10)
    )
    clock = _Clock()
    handle._rt_cache._clock = clock
    assert await _t1_delay(handle) == 60
    clock.now += 1
    assert await _t1_delay(handle) == 120


async def test_a_304_without_cache_control_after_no_cache_is_still_no_cache(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The 304 carries only its ETag, so the stored no-cache stands
    (RFC 9111 section 4.3.4) and rt_min_interval can't stretch it.
    """
    _mock_transit(mock_api)
    mock_api.get(
        RT,
        body=_t1_delayed(60, timestamp=100),
        content_type=PB,
        headers={"Cache-Control": "no-cache", "ETag": '"v1"'},
    )
    mock_api.get(RT, status=304, headers={"ETag": '"v1"'})
    mock_api.get(RT, status=304, headers={"ETag": '"v1"'})
    handle = await feeds_client.get_transit_feed(
        "mdb-100", rt_min_interval=timedelta(seconds=30)
    )
    clock = _Clock()
    handle._rt_cache._clock = clock
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 60
    clock.now += 1
    assert await _t1_delay(handle) == 60
    hits = _rt_hits(mock_api)
    assert len(hits) == 3
    assert all(hit.get("If-None-Match") == '"v1"' for hit in hits[1:])


@pytest.mark.parametrize(
    "not_modified",
    [
        pytest.param({}, id="no-validator"),
        pytest.param({"ETag": '"v2"'}, id="another-strong-etag"),
    ],
)
async def test_a_304_that_selects_nothing_is_asked_again_unconditionally(
    mock_api: MockApi, feeds_client: MobilityFeedsClient, not_modified: dict[str, str]
) -> None:
    """RFC 9111 section 4.3.4: a 304 whose validator doesn't identify the
    stored response (none, or another representation's strong ETag) MUST
    NOT update it -- here it would have renewed v1 for ten minutes. The
    cache asks again without validators and takes the current message.
    """
    _mock_transit(mock_api)
    mock_api.get(
        RT,
        body=_t1_delayed(60, timestamp=100),
        content_type=PB,
        headers={"Cache-Control": "max-age=0", "ETag": '"v1"'},
    )
    mock_api.get(
        RT, status=304, headers={**not_modified, "Cache-Control": "max-age=600"}
    )
    mock_api.get(RT, body=_t1_delayed(120, timestamp=200), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 120
    hits = _rt_hits(mock_api)
    assert len(hits) == 3
    assert hits[1].get("If-None-Match") == '"v1"'
    assert "If-None-Match" not in hits[2]


async def test_negative_rt_min_interval_is_rejected_before_any_request(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    with pytest.raises(ValueError, match="rt_min_interval"):
        await feeds_client.get_transit_feed(
            "mdb-100", rt_min_interval=timedelta(seconds=-1)
        )
    with pytest.raises(ValueError, match="rt_min_interval"):
        await feeds_client.get_transit_feed_from_urls(
            mock_api.url("/hosted/mdb-100.zip"),
            rt_min_interval=timedelta(seconds=-1),
        )
    assert not mock_api.requests  # no catalog lookup, download or build


async def test_without_freshness_every_call_asks(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_transit(mock_api)
    for delay in (60, 120):
        mock_api.get(RT, body=_t1_delayed(delay, timestamp=delay), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 120


async def test_concurrent_calls_share_one_download(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """An arrivals coordinator and a vehicles coordinator firing together."""
    _mock_transit(mock_api)
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(_t1_delayed(60, timestamp=100))
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.ParseFromString(VEHICLE_POSITIONS)
    message.entity.extend(vehicles.entity)
    # One response scripted; a second request would get the mock's 599.
    mock_api.get(RT, body=message.SerializeToString(), content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    delay, positions = await asyncio.gather(_t1_delay(handle), handle.get_vehicles())
    assert delay == 60
    assert positions
    assert len(_rt_hits(mock_api)) == 1


# -- GBFS ------------------------------------------------------------------------

INFO = "/gbfs/system_information.json"


def _mock_gbfs(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=with_base(GBFS_FEED, base))


def _info(*, ttl: int, age: int = 0, name: str = "Test Bikes") -> dict[str, object]:
    updated = int(datetime.now(UTC).timestamp()) - age
    return {
        **SYSTEM_INFO_23,
        "ttl": ttl,
        "last_updated": updated,
        "data": {**SYSTEM_INFO_23["data"], "name": name},  # type: ignore[dict-item]
    }


def _info_hits(mock_api: MockApi) -> list[dict[str, str]]:
    return [r.headers for r in mock_api.requests if r.path == INFO]


async def test_gbfs_revalidates_with_its_etag(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, payload=_info(ttl=0), headers={"ETag": '"g1"'})
    mock_api.get(INFO, status=304, headers={"ETag": '"g1"'})
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert (await handle.get_system_info()).name == "Test Bikes"
    assert (await handle.get_system_info()).name == "Test Bikes"
    hits = _info_hits(mock_api)
    assert len(hits) == 2
    assert hits[1].get("If-None-Match") == '"g1"'


async def test_gbfs_ttl_counts_from_last_updated(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Updated 50s ago with ttl 60: 10s of freshness left, not 60."""
    _mock_gbfs(mock_api)
    mock_api.get(INFO, payload=_info(ttl=60, age=50))
    mock_api.get(INFO, payload=_info(ttl=60, name="Renamed"))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    clock = _Clock()
    handle._docs._clock = clock
    await handle.get_system_info()
    # last_updated is whole seconds, so up to ~1s of the 10 may be gone.
    clock.now += 8
    assert (await handle.get_system_info()).name == "Test Bikes"
    assert len(_info_hits(mock_api)) == 1
    clock.now += 3
    assert (await handle.get_system_info()).name == "Renamed"


async def test_gbfs_a_304_never_outlives_the_document_ttl(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The 304 carries max-age=600, but the stored document still says it
    is good for ttl 60 from a last_updated 50s ago: the renewal must not
    stretch it to ten minutes.
    """
    _mock_gbfs(mock_api)
    cacheable = {"Cache-Control": "max-age=600", "ETag": '"g1"'}
    mock_api.get(INFO, payload=_info(ttl=60, age=50), headers=cacheable)
    mock_api.get(INFO, status=304, headers=cacheable)
    mock_api.get(INFO, status=304, headers=cacheable)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    clock = _Clock()
    handle._docs._clock = clock
    await handle.get_system_info()
    clock.now += 11  # past the ttl: revalidate
    await handle.get_system_info()
    assert len(_info_hits(mock_api)) == 2
    clock.now += 15  # well within max-age=600, but past the document's ttl
    assert (await handle.get_system_info()).name == "Test Bikes"
    hits = _info_hits(mock_api)
    assert len(hits) == 3
    assert hits[2].get("If-None-Match") == '"g1"'


async def test_gbfs_a_304_without_cache_control_keeps_the_stored_max_age(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """max-age=10 under a 300s ttl: a 304 carrying only its ETag keeps 10s."""
    _mock_gbfs(mock_api)
    mock_api.get(
        INFO,
        payload=_info(ttl=300),
        headers={"Cache-Control": "max-age=10", "ETag": '"g1"'},
    )
    mock_api.get(INFO, status=304, headers={"ETag": '"g1"'})
    mock_api.get(INFO, status=304, headers={"ETag": '"g1"'})
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    clock = _Clock()
    handle._docs._clock = clock
    await handle.get_system_info()
    clock.now += 10
    await handle.get_system_info()
    clock.now += 11
    await handle.get_system_info()
    assert len(_info_hits(mock_api)) == 3


async def test_gbfs_the_shorter_declared_lifetime_wins(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, payload=_info(ttl=300), headers={"Cache-Control": "max-age=0"})
    mock_api.get(INFO, payload=_info(ttl=300, name="Renamed"))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    await handle.get_system_info()
    assert (await handle.get_system_info()).name == "Renamed"


async def test_gbfs_never_goes_back_to_an_older_document(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, payload=_info(ttl=0, age=0, name="Current"))
    mock_api.get(INFO, payload=_info(ttl=0, age=600, name="Stale edge"))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert (await handle.get_system_info()).name == "Current"
    assert (await handle.get_system_info()).name == "Current"


async def test_gbfs_unsolicited_304_is_a_connection_error(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, status=304)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    with pytest.raises(SourceConnectionError) as err:
        await handle.get_system_info()
    assert err.value.status == 304


async def test_a_cookie_set_since_changes_the_request_vary_cookie_names(
    mock_api: MockApi,
) -> None:
    """RFC 9111 section 4.1 end to end: the producer sets a cookie and
    answers ``Vary: Cookie``. The next request carries that cookie, so the
    stored copy -- drawn without it -- is not its answer; the copy drawn
    with it is, while the cookie stays.
    """
    _mock_transit(mock_api)
    headers = {"Cache-Control": "max-age=600", "Vary": "Cookie"}
    mock_api.get(
        RT,
        body=_t1_delayed(60, timestamp=100),
        content_type=PB,
        headers={**headers, "Set-Cookie": "region=west"},
    )
    mock_api.get(
        RT, body=_t1_delayed(120, timestamp=200), content_type=PB, headers=headers
    )
    # Loopback is an IP host: only an unsafe jar keeps its cookies.
    async with (
        aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as session,
        MobilityFeedsClient(
            "test-refresh-token", session, base_url=mock_api.url()
        ) as client,
    ):
        handle = await client.get_transit_feed("mdb-100")
        assert await _t1_delay(handle) == 60
        assert await _t1_delay(handle) == 120  # asked: now with the cookie
        assert await _t1_delay(handle) == 120  # the same request: fresh
    hits = _rt_hits(mock_api)
    assert len(hits) == 2
    assert "Cookie" not in hits[0]
    assert hits[1]["Cookie"] == "region=west"


async def test_gbfs_a_changed_session_header_that_vary_names_asks_again(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A session's default headers can change between calls; a stored
    document answers only the header values its Vary names.
    """
    _mock_gbfs(mock_api)
    vary = {"Cache-Control": "max-age=600", "Vary": "X-Region"}
    mock_api.get(INFO, payload=_info(ttl=600, name="East Bikes"), headers=vary)
    mock_api.get(INFO, payload=_info(ttl=600, name="West Bikes"), headers=vary)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    session = feeds_client._get_session()
    session.headers["X-Region"] = "east"
    assert (await handle.get_system_info()).name == "East Bikes"
    assert (await handle.get_system_info()).name == "East Bikes"
    session.headers["X-Region"] = "west"
    assert (await handle.get_system_info()).name == "West Bikes"
    assert [hit.get("X-Region") for hit in _info_hits(mock_api)] == ["east", "west"]


async def test_a_redirected_response_with_vary_is_never_reused_unasked(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The final hop of a redirect carried headers no later request can be
    checked against in advance: under a Vary, each call asks.
    """
    _mock_transit(mock_api)
    moved = mock_api.url("/rt/moved")
    vary = {"Cache-Control": "max-age=600", "Vary": "Accept"}
    for timestamp in (100, 200):
        mock_api.get(RT, status=302, headers={"Location": moved})
        mock_api.get(
            "/rt/moved",
            body=_t1_delayed(timestamp - 40, timestamp=timestamp),
            content_type=PB,
            headers=vary,
        )
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 160
    assert len([r for r in mock_api.requests if r.path == "/rt/moved"]) == 2


async def _unsafe_jar_client(mock_api: MockApi, **session: object):  # type: ignore[no-untyped-def]
    return aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True), **session)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("session_headers", "cookies", "sent"),
    [
        # Set b, then a: aiohttp sends them sorted, as the cache compares.
        ({}, ["b=1", "a=2"], ["", "b=1", "a=2; b=1"]),
        # An explicit cookie the jar replaces by name.
        ({"Cookie": "a=1"}, ["a=2"], ["a=1", "a=2"]),
        # An explicit cookie merged, sorted, with the jar's.
        ({"Cookie": "z=1"}, ["a=2"], ["z=1", "a=2; z=1"]),
    ],
)
async def test_vary_cookie_matches_the_cookie_aiohttp_sends(
    mock_api: MockApi,
    session_headers: dict[str, str],
    cookies: list[str | None],
    sent: list[str],
) -> None:
    """Under Vary: Cookie the cache asks exactly when the Cookie it would
    send changed: once per new cookie, then a fresh answer -- however
    aiohttp orders, merges or replaces cookies on the wire.
    """
    _mock_transit(mock_api)
    vary = {"Cache-Control": "max-age=600", "Vary": "Cookie"}
    for n, cookie in enumerate([*cookies, None]):
        headers = {**vary, **({"Set-Cookie": cookie} if cookie else {})}
        mock_api.get(
            RT,
            body=_t1_delayed(60 + n, timestamp=100 + n),
            content_type=PB,
            headers=headers,
        )
    async with (
        await _unsafe_jar_client(mock_api, headers=session_headers) as session,
        MobilityFeedsClient(
            "test-refresh-token", session, base_url=mock_api.url()
        ) as client,
    ):
        handle = await client.get_transit_feed("mdb-100")
        for _ in range(len(sent) + 2):
            await _t1_delay(handle)
    hits = _rt_hits(mock_api)
    assert [hit.get("Cookie", "") for hit in hits] == sent


async def test_a_redirect_that_may_not_be_stored_is_asked_every_time(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """RFC 9111 section 4: the original URL is the key. Its redirect says
    no-store (or nothing at all), so the target's max-age can't skip it.
    """
    _mock_transit(mock_api)
    moved = mock_api.url("/rt/moved")
    for hop in ({"Cache-Control": "no-store"}, {}):
        mock_api.get(RT, status=302, headers={"Location": moved, **hop})
        mock_api.get(
            "/rt/moved",
            body=_t1_delayed(60, timestamp=100),
            content_type=PB,
            headers={"Cache-Control": "max-age=600"},
        )
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 60
    assert len(_rt_hits(mock_api)) == 2


async def test_a_redirect_targets_304_never_revalidates_the_original(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """/rt/all answered with ETag "1"; then it redirects to /rt/moved, whose
    own "1" matches the forwarded If-None-Match. That 304 is about another
    resource: the cache asks again, unconditionally, and gets /rt/moved's
    feed.
    """
    _mock_transit(mock_api)
    moved = mock_api.url("/rt/moved")
    tag = {"ETag": '"1"', "Cache-Control": "no-cache"}
    mock_api.get(RT, body=_t1_delayed(60, timestamp=100), content_type=PB, headers=tag)
    for status in (304, 200):
        mock_api.get(RT, status=301, headers={"Location": moved})
        mock_api.get(
            "/rt/moved",
            status=status,
            body=_t1_delayed(120, timestamp=200) if status == 200 else None,
            content_type=PB,
            headers=tag,
        )
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 120
    moved_hits = [r.headers for r in mock_api.requests if r.path == "/rt/moved"]
    assert [hit.get("If-None-Match") for hit in moved_hits] == ['"1"', None]


async def test_a_redirect_varying_by_cookie_is_asked_again(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A cacheable 302 varying by Cookie: the next call can't be shown to
    carry the same cookies through the redirect, so it asks."""
    _mock_transit(mock_api)
    for timestamp, target in ((100, "/rt/east"), (200, "/rt/west")):
        mock_api.get(
            RT,
            status=302,
            headers={
                "Location": mock_api.url(target),
                "Cache-Control": "max-age=600",
                "Vary": "Cookie",
            },
        )
        mock_api.get(
            target,
            body=_t1_delayed(timestamp - 40, timestamp=timestamp),
            content_type=PB,
            headers={"Cache-Control": "max-age=600"},
        )
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 160


async def test_rt_min_interval_never_reuses_a_302_without_freshness(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """A 302 (or 303, 307) may not be reused without explicit freshness,
    not even heuristically -- so the opt-in budget can't reuse it either."""
    _mock_transit(mock_api)
    moved = mock_api.url("/rt/moved")
    for delay in (60, 120):
        mock_api.get(RT, status=302, headers={"Location": moved})
        mock_api.get(
            "/rt/moved", body=_t1_delayed(delay, timestamp=delay), content_type=PB
        )
    handle = await feeds_client.get_transit_feed(
        "mdb-100", rt_min_interval=timedelta(seconds=30)
    )
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 120
    assert len(_rt_hits(mock_api)) == 2


@pytest.mark.parametrize("status", [204, 300])
async def test_a_status_without_a_feed_is_a_connection_error(
    mock_api: MockApi, feeds_client: MobilityFeedsClient, status: int
) -> None:
    """Only 200 and 203 carry the feed: a 204's empty body is not an empty
    FeedMessage to store, and a 300 isn't a feed either."""
    _mock_transit(mock_api)
    mock_api.get(RT, status=status, body=b"", content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    with pytest.raises(SourceConnectionError, match=f"answered {status}"):
        await _t1_delay(handle)


async def test_gbfs_a_status_without_a_document_is_a_connection_error(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_gbfs(mock_api)
    mock_api.get(INFO, status=204)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    with pytest.raises(SourceConnectionError, match="answered 204"):
        await handle.get_system_info()


async def test_a_redirected_feed_varying_by_accept_encoding_is_reused(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """http->https style: a cacheable 301, then a feed fresh for 60s that
    varies by Accept-Encoding (as nginx and Cloudflare send). aiohttp sends
    the same Accept-Encoding on every request, so the second call is fresh.
    """
    _mock_transit(mock_api)
    mock_api.get(
        RT,
        status=301,
        headers={
            "Location": mock_api.url("/rt/moved"),
            "Cache-Control": "max-age=3600",
        },
    )
    mock_api.get(
        "/rt/moved",
        body=_t1_delayed(60, timestamp=100),
        content_type=PB,
        headers={"Cache-Control": "max-age=60", "Vary": "Accept-Encoding"},
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert await _t1_delay(handle) == 60
    assert await _t1_delay(handle) == 60
    assert len(_rt_hits(mock_api)) == 1


async def test_gbfs_a_redirected_document_records_the_url_that_answered(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """The stored document's URL is the redirect's target: a later 304 from
    any other resource can't revalidate it."""
    _mock_gbfs(mock_api)
    moved = mock_api.url("/gbfs/moved.json")
    mock_api.get(INFO, status=301, headers={"Location": moved})
    mock_api.get("/gbfs/moved.json", payload=_info(ttl=0), headers={"ETag": '"g1"'})
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert (await handle.get_system_info()).name == "Test Bikes"
    [entry] = handle._docs._entries.values()
    assert entry.url == moved
    assert entry.redirected
