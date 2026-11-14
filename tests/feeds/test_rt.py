"""Tests for GTFS-RT fetching (auth, errors) and protobuf parsing."""

import asyncio
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from functools import partial

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from google.transit import gtfs_realtime_pb2
from yarl import URL

from aiomobilitydatabase.feeds import _redirects as redirects_module
from aiomobilitydatabase.feeds._freshness import FreshnessCache
from aiomobilitydatabase.feeds._redirects import _origin
from aiomobilitydatabase.feeds.exceptions import (
    FeedParseError,
    SourceAuthenticationError,
    SourceConnectionError,
)
from aiomobilitydatabase.feeds.models import (
    AlertCause,
    AlertEffect,
    CongestionLevel,
    OccupancyStatus,
    Route,
    VehicleStopStatus,
    WheelchairAccess,
)
from aiomobilitydatabase.feeds.rt import (
    TripStopUpdate,
    TripUpdateEntry,
    _lenient_service_date,
    _lenient_start_secs,
    _pb_enum_name,
    _vocab_or_none,
    alerts_from_message,
    fetch_feed_message,
    resolve_trip_predictions,
    trip_modifications_from_message,
    trip_updates_from_message,
    vehicles_from_message,
)
from aiomobilitydatabase.feeds.static_index import TripCall

from tests.feeds.fixtures import (
    ALERTS,
    ALERTS_TRIP_SCOPED,
    TRIP_UPDATES_BASELINE,
    VEHICLE_POSITIONS,
    VEHICLE_POSITIONS_STATUS,
)
from tests.mock_server import MockApi

PB = "application/octet-stream"


def _route(route_id: str, short_name: str, long_name: str) -> Route:
    return Route(
        id=route_id,
        short_name=short_name,
        long_name=long_name,
        type=3,
        agency_id=None,
        color=None,
        text_color=None,
        url=None,
        description=None,
        sort_order=None,
    )


async def _fetch(
    mock_api: MockApi, path: str = "/rt", **kwargs: object
) -> gtfs_realtime_pb2.FeedMessage:
    async with aiohttp.ClientSession() as session:
        message = (
            await fetch_feed_message(session, mock_api.url(path), **kwargs)
        ).value  # type: ignore[arg-type]
        assert message is not None
        return message


async def test_fetch_plain(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    msg = await _fetch(mock_api)
    assert len(msg.entity) == 2


async def test_fetch_header_auth(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    await _fetch(mock_api, auth_type=2, api_key_name="X-Api-Key", api_key="secret123")
    assert mock_api.requests[-1].headers.get("X-Api-Key") == "secret123"


async def test_fetch_query_param_auth(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB)
    await _fetch(mock_api, auth_type=1, api_key_name="api_key", api_key="secret123")
    assert mock_api.requests[-1].query.get("api_key") == "secret123"


async def test_fetch_401_maps_to_source_auth_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", status=401, body=b"denied", content_type="text/plain")
    with pytest.raises(SourceAuthenticationError):
        await _fetch(mock_api)


async def test_fetch_500_maps_to_source_connection_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", status=500, body=b"oops", content_type="text/plain")
    with pytest.raises(SourceConnectionError) as excinfo:
        await _fetch(mock_api)
    assert excinfo.value.status == 500


async def test_fetch_unreachable_maps_to_source_connection_error() -> None:
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError):
            await fetch_feed_message(session, "http://127.0.0.1:1/rt")


async def test_fetch_garbage_maps_to_feed_parse_error(mock_api: MockApi) -> None:
    mock_api.get("/rt", body=b"\x00\x01 not protobuf \xff" * 10, content_type=PB)
    with pytest.raises(FeedParseError):
        await _fetch(mock_api)


async def test_fetch_rejects_non_http_scheme() -> None:
    """Task 15R-b item 8: a producer_url with a non-http(s) scheme (data
    from the catalog, not a caller parameter) is rejected explicitly and
    BEFORE any network attempt, naming the scheme in the message. Pre-fix,
    this still raised SourceConnectionError (aiohttp itself rejects
    file:// with a ClientError subclass the existing except clause already
    caught) but with a generic message that never mentioned "scheme" --
    incidental, not deliberate, and not guaranteed for every non-http
    scheme aiohttp might handle differently in some other version/config.
    """
    async with aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError, match="scheme"):
            await fetch_feed_message(session, "file:///etc/passwd")


def _elsewhere(url: str) -> str:
    """The same mock server under another origin (another host name)."""
    assert "127.0.0.1" in url
    return url.replace("127.0.0.1", "localhost")


async def test_header_auth_key_never_follows_a_redirect_to_another_origin(
    mock_api: MockApi,
) -> None:
    """RFC 9110 section 15.4: the producer key is withheld from the first
    hop to another origin on, even when a later hop comes back; a
    same-origin hop keeps it. Authorization goes too, as aiohttp drops it.
    """
    mock_api.get("/rt", status=302, headers={"Location": "/rt/same"})
    mock_api.get(
        "/rt/same",
        status=307,
        headers={"Location": _elsewhere(mock_api.url("/rt/away"))},
    )
    mock_api.get("/rt/away", status=301, headers={"Location": mock_api.url("/rt/back")})
    mock_api.get("/rt/back", body=VEHICLE_POSITIONS, content_type=PB)
    async with aiohttp.ClientSession() as session:
        response = await fetch_feed_message(
            session,
            mock_api.url("/rt"),
            auth_type=2,
            api_key_name="X-Api-Key",
            api_key="secret123",
            headers={"Authorization": "Bearer user", "X-Other": "kept"},
        )
    assert response.value is not None and len(response.value.entity) == 2
    assert [hop.status for hop in response.hops] == [302, 307, 301]
    assert response.url == mock_api.url("/rt/back")
    seen = {r.path: r.headers for r in mock_api.requests}
    for path in ("/rt", "/rt/same"):
        assert seen[path]["X-Api-Key"] == "secret123"
        assert seen[path]["Authorization"] == "Bearer user"
    for path in ("/rt/away", "/rt/back"):
        names = {name.lower() for name in seen[path]}
        assert not names & {"x-api-key", "authorization"}
    assert all(r.headers["X-Other"] == "kept" for r in mock_api.requests)


async def test_header_auth_key_is_withheld_whatever_its_case(
    mock_api: MockApi,
) -> None:
    mock_api.get(
        "/rt", status=302, headers={"Location": _elsewhere(mock_api.url("/rt/x"))}
    )
    mock_api.get("/rt/x", body=VEHICLE_POSITIONS, content_type=PB)
    async with aiohttp.ClientSession() as session:
        await fetch_feed_message(
            session,
            mock_api.url("/rt"),
            auth_type=2,
            api_key_name="x-api-key",
            api_key="secret123",
            headers={"X-API-KEY": "custom"},
        )
    assert mock_api.requests[0].headers["x-api-key"] == "secret123"
    assert not any(k.lower() == "x-api-key" for k in mock_api.requests[1].headers)


@pytest.mark.parametrize("auth_type", [None, 2])
async def test_url_credentials_follow_hops_within_their_origin_only(
    mock_api: MockApi, auth_type: int | None
) -> None:
    """As on aiohttp's own redirects: kept within the origin, never sent
    past the first hop away -- not even back at their origin -- and a
    Location's own credentials apply to its origin.
    """
    mock_api.get("/rt", status=302, headers={"Location": "/rt/same"})
    away = _elsewhere(mock_api.url("/rt/away"))
    mock_api.get("/rt/same", status=302, headers={"Location": away})
    mock_api.get("/rt/away", status=302, headers={"Location": mock_api.url("/rt/back")})
    back = URL(mock_api.url("/rt/next")).with_user("v").with_password("w")
    mock_api.get("/rt/back", status=302, headers={"Location": str(back)})
    mock_api.get("/rt/next", body=VEHICLE_POSITIONS, content_type=PB)
    url = URL(mock_api.url("/rt")).with_user("u").with_password("p")
    async with aiohttp.ClientSession() as session:
        await fetch_feed_message(
            session, str(url), auth_type=auth_type, api_key_name="K", api_key="k"
        )
    sent = [(r.path, r.headers.get("Authorization")) for r in mock_api.requests]
    assert sent == [
        ("/rt", "Basic dTpw"),
        ("/rt/same", "Basic dTpw"),
        ("/rt/away", None),
        ("/rt/back", None),
        ("/rt/next", "Basic djp3"),
    ]


@pytest.mark.parametrize("timeout_seconds", [0, -1, None])
@pytest.mark.parametrize("auth_type", [None, 2])
async def test_a_timeout_of_none_or_less_is_no_bound_with_or_without_header_auth(
    mock_api: MockApi, auth_type: int | None, timeout_seconds: float | None
) -> None:
    """As aiohttp's own ``total``: None, zero or less sets no bound."""
    mock_api.get("/rt", status=302, headers={"Location": "/rt/x"})
    mock_api.get("/rt/x", body=VEHICLE_POSITIONS, content_type=PB)
    msg = await _fetch(
        mock_api,
        auth_type=auth_type,
        api_key_name="K",
        api_key="v",
        timeout_seconds=timeout_seconds,
    )
    assert len(msg.entity) == 2


@pytest.mark.parametrize(
    "credential",
    ["Authorization", "cookie", "Proxy-Authorization", "x-api-key", "auth"],
)
# aiohttp 3.14 deprecates session ``auth``; older floors still have it.
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
async def test_session_credentials_fail_a_header_auth_hop_to_another_origin(
    mock_api: MockApi, credential: str
) -> None:
    """A request can't shed its session's default credentials, and
    following a redirect is optional: such a hop is never sent.
    """
    mock_api.get(
        "/rt", status=302, headers={"Location": _elsewhere(mock_api.url("/rt/x"))}
    )
    options: dict[str, object] = (
        {"auth": aiohttp.BasicAuth("user", "pass")}
        if credential == "auth"
        else {"headers": {credential: "secret"}}
    )
    async with aiohttp.ClientSession(**options) as session:  # type: ignore[arg-type]
        with pytest.raises(SourceConnectionError, match="credentials"):
            await fetch_feed_message(
                session,
                mock_api.url("/rt"),
                auth_type=2,
                api_key_name="X-Api-Key",
                api_key="secret123",
            )
    assert [r.path for r in mock_api.requests] == ["/rt"]


async def test_session_credentials_follow_a_header_auth_hop_within_the_origin(
    mock_api: MockApi,
) -> None:
    mock_api.get("/rt", status=308, headers={"Location": "/rt/x"})
    mock_api.get("/rt/x", body=VEHICLE_POSITIONS, content_type=PB)
    async with aiohttp.ClientSession(headers={"Authorization": "Bearer s"}) as session:
        await fetch_feed_message(
            session,
            mock_api.url("/rt"),
            auth_type=2,
            api_key_name="X-Api-Key",
            api_key="secret123",
        )
    assert mock_api.requests[1].headers["X-Api-Key"] == "secret123"
    assert mock_api.requests[1].headers["Authorization"] == "Bearer s"


@pytest.mark.parametrize("auth_type", [None, 2])
async def test_the_tenth_redirect_fails_with_or_without_header_auth(
    mock_api: MockApi, auth_type: int | None
) -> None:
    """Followed by hand or by aiohttp, a chain stops at the same hop."""
    for hop in range(10):
        mock_api.get(f"/rt/{hop}", status=302, headers={"Location": f"/rt/{hop + 1}"})
    mock_api.get("/rt/10", body=VEHICLE_POSITIONS, content_type=PB)
    with pytest.raises(SourceConnectionError):
        await _fetch(
            mock_api, "/rt/0", auth_type=auth_type, api_key_name="K", api_key="v"
        )
    assert len(mock_api.requests) == 10


@pytest.mark.parametrize("auth_type", [None, 2])
async def test_nine_redirects_are_followed_with_or_without_header_auth(
    mock_api: MockApi, auth_type: int | None
) -> None:
    for hop in range(9):
        mock_api.get(f"/rt/{hop}", status=302, headers={"Location": f"/rt/{hop + 1}"})
    mock_api.get("/rt/9", body=VEHICLE_POSITIONS, content_type=PB)
    msg = await _fetch(
        mock_api, "/rt/0", auth_type=auth_type, api_key_name="K", api_key="v"
    )
    assert len(msg.entity) == 2


@pytest.mark.parametrize("location", ["ftp://127.0.0.1/rt", "http://[::1/rt"])
async def test_a_header_auth_redirect_to_a_non_http_url_fails(
    mock_api: MockApi, location: str
) -> None:
    mock_api.get("/rt", status=302, headers={"Location": location})
    with pytest.raises(SourceConnectionError, match="redirected"):
        await _fetch(mock_api, auth_type=2, api_key_name="K", api_key="v")
    assert len(mock_api.requests) == 1


async def test_a_header_auth_redirect_without_location_is_the_answer(
    mock_api: MockApi,
) -> None:
    """As with aiohttp: a 3xx with no Location is the response itself."""
    mock_api.get("/rt", status=302)
    with pytest.raises(SourceConnectionError, match="without a feed") as excinfo:
        await _fetch(mock_api, auth_type=2, api_key_name="K", api_key="v")
    assert excinfo.value.status == 302


@pytest.mark.parametrize(
    ("first", "second", "same"),
    [
        ("http://a.example/x", "http://a.example:80/y", True),
        ("https://A.example/", "https://a.example:443/", True),
        ("http://a.example/", "https://a.example/", False),
        ("http://a.example/", "http://a.example:8080/", False),
        ("https://a.example/", "https://b.a.example/", False),
    ],
)
def test_origins_compare_as_rfc_6454_says(first: str, second: str, same: bool) -> None:
    """An explicit default port is the same origin: the key isn't dropped."""
    assert (_origin(URL(first)) == _origin(URL(second))) is same


@pytest.mark.parametrize("auth_type", [None, 2])
async def test_the_timeout_bounds_the_whole_redirect_chain(
    auth_type: int | None,
) -> None:
    """aiohttp's ``total`` covers every hop: so does the hand-made chain."""

    async def hop(request: web.Request) -> web.Response:
        await asyncio.sleep(0.4)
        step = int(request.match_info["step"])
        if step < 4:
            return web.Response(status=302, headers={"Location": f"/hop/{step + 1}"})
        return web.Response(body=VEHICLE_POSITIONS, content_type=PB)

    app = web.Application()
    app.router.add_get("/hop/{step}", hop)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        with pytest.raises(SourceConnectionError) as excinfo:
            await fetch_feed_message(
                session,
                str(server.make_url("/hop/0")),
                auth_type=auth_type,
                api_key_name="K",
                api_key="v",
                timeout_seconds=1.0,
            )
    assert isinstance(excinfo.value.__cause__, TimeoutError)


async def test_a_deadline_spent_between_hops_sends_no_further_request(
    mock_api: MockApi, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Once the deadline has passed, the next hop is never asked."""

    readings = iter([0.0, 0.0, 31.0])  # the deadline passes after the first hop
    monkeypatch.setattr(redirects_module, "monotonic", lambda: next(readings))
    mock_api.get("/rt", status=302, headers={"Location": "/rt/x"})
    with pytest.raises(SourceConnectionError):
        await _fetch(mock_api, auth_type=2, api_key_name="K", api_key="v")
    assert [r.path for r in mock_api.requests] == ["/rt"]


@pytest.mark.parametrize("auth_type", [None, 2])
async def test_a_redirect_loop_fails_with_or_without_header_auth(
    mock_api: MockApi, auth_type: int | None
) -> None:
    """RFC 9110 section 15.4: a client SHOULD detect and intervene in
    cyclical redirections -- the redirect limit ends the loop."""
    for _ in range(5):
        mock_api.get("/rt/a", status=302, headers={"Location": "/rt/b"})
        mock_api.get("/rt/b", status=302, headers={"Location": "/rt/a"})
    with pytest.raises(SourceConnectionError):
        await _fetch(
            mock_api, "/rt/a", auth_type=auth_type, api_key_name="K", api_key="v"
        )
    assert len(mock_api.requests) == 10


async def test_an_unsolicited_304_is_a_connection_error(mock_api: MockApi) -> None:
    """A 304 answers only a conditional request; there is no stored parse."""
    mock_api.get("/rt", status=304)
    with pytest.raises(SourceConnectionError, match="unconditional") as excinfo:
        await _fetch(mock_api)
    assert excinfo.value.status == 304


async def test_a_query_auth_feed_is_revalidated_conditionally(
    mock_api: MockApi,
) -> None:
    """The validators belong to the URL with its query (the key included):
    the request asking for it carries them, so an unchanged feed is a 304."""
    url = mock_api.url("/rt")
    fresh = {"ETag": '"v1"', "Cache-Control": "no-cache"}
    mock_api.get("/rt", body=VEHICLE_POSITIONS, content_type=PB, headers=fresh)
    mock_api.get("/rt", status=304, headers=fresh)
    cache: FreshnessCache[gtfs_realtime_pb2.FeedMessage] = FreshnessCache()
    async with aiohttp.ClientSession() as session:
        fetch = partial(
            fetch_feed_message, session, url, auth_type=1, api_key_name="k", api_key="v"
        )
        first = await cache.get(url, fetch)
        assert await cache.get(url, fetch) is first
    assert [r.headers.get("If-None-Match") for r in mock_api.requests] == [
        None,
        '"v1"',
    ]
    assert all(r.query["k"] == "v" for r in mock_api.requests)


async def test_a_credentialed_session_revalidates_across_origins_unconditionally(
    mock_api: MockApi,
) -> None:
    """Without a header key, a redirect away that a hand-made request can't
    strip the session's credentials from is asked again unconditionally:
    aiohttp follows it and drops them itself -- never a failure."""
    url = mock_api.url("/rt")
    away = _elsewhere(mock_api.url("/rt/away"))
    for _ in range(3):
        mock_api.get("/rt", status=302, headers={"Location": away})
    fresh = {"ETag": '"v1"', "Cache-Control": "no-cache"}
    for _ in range(2):
        mock_api.get("/rt/away", body=VEHICLE_POSITIONS, content_type=PB, headers=fresh)
    cache: FreshnessCache[gtfs_realtime_pb2.FeedMessage] = FreshnessCache()
    async with aiohttp.ClientSession(headers={"Authorization": "Bearer s"}) as session:
        fetch = partial(fetch_feed_message, session, url)
        await cache.get(url, fetch)
        assert len((await cache.get(url, fetch)).entity) == 2
    away_hits = [r.headers for r in mock_api.requests if r.path == "/rt/away"]
    assert len(away_hits) == 2
    assert not any("Authorization" in hit for hit in away_hits)
    assert not any("If-None-Match" in hit for hit in away_hits)


def test_vehicles_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(VEHICLE_POSITIONS)
    vehicles = vehicles_from_message(
        msg,
        routes_by_id={
            "R1": _route("R1", "10", "Main Line"),
            "R2": _route("R2", "20", "Night Owl"),
        },
        stops_by_id={},
        trip_routes={"T3": "R2"},
    )
    assert len(vehicles) == 2
    v1 = next(v for v in vehicles if v.vehicle_id == "V1")
    assert v1.route_id == "R1"
    assert v1.route is not None
    assert v1.route.display_name == "10 Main Line"
    # Typed vocabulary: the StrEnum member IS the old raw string, so both
    # identity and legacy string comparisons hold.
    assert v1.occupancy_status is OccupancyStatus.MANY_SEATS_AVAILABLE
    assert v1.occupancy_status == "MANY_SEATS_AVAILABLE"
    assert v1.timestamp == datetime.fromtimestamp(1_785_500_000, tz=UTC)
    # The baseline fixture sets no status surface at all: no stop referent
    # means no synthesized IN_TRANSIT_TO default -- everything is None.
    assert v1.current_status is None
    assert v1.congestion_level is None
    assert v1.stop_id is None
    assert v1.current_stop_sequence is None
    assert v1.license_plate is None
    v2 = next(v for v in vehicles if v.vehicle_id == "V2")
    assert v2.route_id == "R2"  # resolved via trip_routes fallback
    assert v2.route is not None
    assert v2.route.display_name == "20 Night Owl"


def test_vehicles_from_message_status_surface() -> None:
    """The descriptive status surface: with a current_stop_sequence, an
    explicit current_status verbatim and an unset one IN_TRANSIT_TO; None
    without one; congestion/stop referent/license plate passed through raw.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(VEHICLE_POSITIONS_STATUS)
    vehicles = vehicles_from_message(
        msg, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    v7, v8, v9 = (
        next(v for v in vehicles if v.vehicle_id == vid) for vid in ("V7", "V8", "V9")
    )
    assert v7.current_status is VehicleStopStatus.STOPPED_AT
    assert v7.current_status == "STOPPED_AT"
    assert v7.congestion_level is CongestionLevel.SEVERE_CONGESTION
    assert v7.stop_id == "S2"
    assert v7.current_stop_sequence == 2
    assert v7.license_plate == "8ABC123"
    # V8: referent present (current_stop_sequence), status unset -> the
    # protobuf default IN_TRANSIT_TO is real information and surfaces.
    assert v8.current_status is VehicleStopStatus.IN_TRANSIT_TO
    assert v8.current_stop_sequence == 1
    assert v8.stop_id is None
    assert v8.congestion_level is None
    assert v8.license_plate is None
    # V9: no status, no current_stop_sequence -> None.
    assert v9.current_status is None
    assert v9.stop_id is None
    assert v9.current_stop_sequence is None


def test_vehicles_status_needs_current_stop_sequence() -> None:
    """current_status is "Ignored if current_stop_sequence is missing", so a
    stop_id alone gives none, even a STOPPED_AT the producer sets."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "vp-ref"
    entity.vehicle.position.latitude = 1.0
    entity.vehicle.position.longitude = 2.0
    entity.vehicle.stop_id = "S9"
    entity.vehicle.current_status = (
        gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.STOPPED_AT
    )
    (vehicle,) = vehicles_from_message(
        msg, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert vehicle.stop_id == "S9"
    assert vehicle.current_status is None


def test_trip_updates_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(TRIP_UPDATES_BASELINE)
    updates = trip_updates_from_message(msg)
    assert updates.canceled_trips == {("T2", None, None)}
    entry = updates.trips[("T1", None, None)]
    assert entry.vehicle_id == "V1"
    assert entry.delay_seconds is None  # no trip-level TripUpdate.delay set
    (stu,) = entry.stop_updates
    assert stu.stop_id == "S1"
    assert stu.stop_sequence is None  # producer sent stop_id addressing only
    assert (stu.arrival_delay, stu.departure_delay) == (300, 300)
    assert stu.arrival == datetime.fromtimestamp(1_785_500_300, tz=UTC)
    assert stu.departure == datetime.fromtimestamp(1_785_500_330, tz=UTC)
    [added] = updates.realtime_trips
    assert added.trip_id == "ADDED-9"
    assert added.route_id == "R1"
    assert added.replaces is None
    [call] = added.calls
    assert call.stop_id == "S2"
    assert call.departure == datetime.fromtimestamp(1_785_500_630, tz=UTC)
    assert ("ADDED-9", None, None) not in updates.trips  # ADDED rows never form entries


def _build_conflicting_trip_update(
    *, canceled_first: bool
) -> gtfs_realtime_pb2.FeedMessage:
    """A CANCELED entity plus a stale prediction and a stale ADDED stop time,
    all for the same trip_id, in the given order.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"

    def add_canceled(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "cancel"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )

    def add_prediction(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "stale-update"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.arrival.time = 1_785_500_000

    def add_stale_added(m: gtfs_realtime_pb2.FeedMessage) -> None:
        entity = m.entity.add()
        entity.id = "stale-added"
        entity.trip_update.trip.trip_id = "STALE-TRIP"
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ADDED
        )
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S2"
        stu.arrival.time = 1_785_500_100

    if canceled_first:
        add_canceled(msg)
        add_prediction(msg)
        add_stale_added(msg)
    else:
        add_prediction(msg)
        add_stale_added(msg)
        add_canceled(msg)
    return msg


@pytest.mark.parametrize("canceled_first", [True, False])
def test_trip_updates_cancellation_wins_regardless_of_entity_order(
    canceled_first: bool,
) -> None:
    """A CANCELED trip_update plus a stale prediction and a stale ADDED stop
    time for the same trip_id can arrive in either entity order; cancellation
    must always win so the parsed result never claims both statuses for one
    trip.
    """
    msg = _build_conflicting_trip_update(canceled_first=canceled_first)
    updates = trip_updates_from_message(msg)
    assert updates.canceled_trips == {("STALE-TRIP", None, None)}
    assert ("STALE-TRIP", None, None) not in updates.trips
    assert all(trip.trip_id != "STALE-TRIP" for trip in updates.realtime_trips)


def test_trip_updates_start_time_keys() -> None:
    """TripDescriptor.start_time becomes the start_secs key component:
    parsed for trip entries AND cancellations (>24:00:00 supported), while
    a garbage start_time degrades to None instead of failing the message.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    cases = [("F1", "06:10:00"), ("F2", "25:00:00"), ("F3", "not-a-time")]
    for i, (trip_id, start_time) in enumerate(cases):
        entity = msg.entity.add()
        entity.id = f"tu-{i}"
        entity.trip_update.trip.trip_id = trip_id
        entity.trip_update.trip.start_time = start_time
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.departure.time = 1_785_500_000
    cancel = msg.entity.add()
    cancel.id = "tu-cancel"
    cancel.trip_update.trip.trip_id = "F1"
    cancel.trip_update.trip.start_time = "06:20:00"
    cancel.trip_update.trip.schedule_relationship = (
        gtfs_realtime_pb2.TripDescriptor.CANCELED
    )
    updates = trip_updates_from_message(msg)
    assert set(updates.trips) == {
        ("F1", None, 22200),
        ("F2", None, 90000),
        ("F3", None, None),
    }
    assert updates.canceled_trips == {("F1", None, 22800)}


def test_trip_updates_start_date_keys() -> None:
    """TripDescriptor.start_date becomes the start_date key component:
    parsed for trip entries AND cancellations, while garbage (bad length,
    non-digits, calendar-invalid dates) degrades to None -- behaving
    exactly like an absent date -- instead of failing the message.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    cases = [
        ("D1", "20260730"),  # valid
        ("D2", "20261332"),  # calendar-invalid month/day
        ("D3", "2026073"),  # wrong length
        ("D4", "2026073a"),  # non-digit
        ("D5", ""),  # absent
    ]
    for i, (trip_id, start_date) in enumerate(cases):
        entity = msg.entity.add()
        entity.id = f"tu-{i}"
        entity.trip_update.trip.trip_id = trip_id
        if start_date:
            entity.trip_update.trip.start_date = start_date
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = "S1"
        stu.departure.time = 1_785_500_000
    cancel = msg.entity.add()
    cancel.id = "tu-cancel"
    cancel.trip_update.trip.trip_id = "D1"
    cancel.trip_update.trip.start_date = "20260731"
    cancel.trip_update.trip.start_time = "06:10:00"
    cancel.trip_update.trip.schedule_relationship = (
        gtfs_realtime_pb2.TripDescriptor.CANCELED
    )
    updates = trip_updates_from_message(msg)
    assert set(updates.trips) == {
        ("D1", date(2026, 7, 30), None),
        ("D2", None, None),
        ("D3", None, None),
        ("D4", None, None),
        ("D5", None, None),
    }
    assert updates.canceled_trips == {("D1", date(2026, 7, 31), 22200)}


def test_alerts_from_message() -> None:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(ALERTS)
    alerts = alerts_from_message(msg)
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.id == "alert-1"
    assert alert.header == "Detour on Main"
    assert alert.description == "Use Second Ave"
    # Typed vocabulary: StrEnum members ARE the old raw strings.
    assert alert.cause is AlertCause.CONSTRUCTION
    assert alert.cause == "CONSTRUCTION"
    assert alert.effect is AlertEffect.DETOUR
    assert alert.severity is None  # fixture sets no severity_level
    assert alert.route_ids == ("R1",)
    assert alert.stop_ids == ("S1",)
    assert alert.trip_ids == ()  # fixture informs a route and a stop only
    assert alert.active_periods == (
        (datetime.fromtimestamp(1_785_400_000, tz=UTC), None),
    )


def test_alerts_trip_scoped_not_agency_wide() -> None:
    """The semantic fix: an alert whose informed entities carry ONLY trip
    descriptors populates trip_ids -- so it is scoped (to those trips),
    not mistaken for an unscoped agency-wide alert. Unscoped now means
    route_ids, stop_ids, AND trip_ids all empty.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(ALERTS_TRIP_SCOPED)
    (alert,) = alerts_from_message(msg)
    assert alert.header == "T1 and T3 running late"
    assert alert.route_ids == ()
    assert alert.stop_ids == ()
    assert alert.trip_ids == ("T1", "T3")  # sorted, deduplicated
    assert alert.effect is AlertEffect.SIGNIFICANT_DELAYS


def test_pb_enum_name_and_vocab_leniency() -> None:
    """Both conversion layers degrade unknown values to None, never raise:
    an int the bindings don't know yields no name, and a name the model
    enum doesn't know (newer bindings than this library) yields no member.
    """
    assert _pb_enum_name(gtfs_realtime_pb2.Alert.Cause, 10) == "CONSTRUCTION"
    assert _pb_enum_name(gtfs_realtime_pb2.Alert.Cause, 999) is None
    assert _vocab_or_none(AlertCause, "CONSTRUCTION") is AlertCause.CONSTRUCTION
    assert _vocab_or_none(AlertCause, "FUTURE_SPEC_VALUE") is None
    assert _vocab_or_none(AlertCause, None) is None
    # The new VehiclePosition vocabularies ride the same two-layer leniency.
    assert (
        _pb_enum_name(gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus, 999) is None
    )
    assert _pb_enum_name(gtfs_realtime_pb2.VehiclePosition.CongestionLevel, 999) is None
    assert _vocab_or_none(VehicleStopStatus, "FUTURE_STATUS") is None
    assert _vocab_or_none(CongestionLevel, "FUTURE_LEVEL") is None


def test_trip_updates_delay_field_and_per_event_delays() -> None:
    """TripUpdate.delay is captured only when present, and an STU carrying
    both delays keeps each: a StopTimeEvent is "timing information for a
    single predicted event".
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    entity = msg.entity.add()
    entity.id = "tu-delays"
    entity.trip_update.trip.trip_id = "T1"
    entity.trip_update.delay = -60
    stu = entity.trip_update.stop_time_update.add()
    stu.stop_sequence = 2
    stu.arrival.delay = 100
    stu.departure.delay = 200
    updates = trip_updates_from_message(msg)
    entry = updates.trips[("T1", None, None)]
    assert entry.delay_seconds == -60
    (parsed,) = entry.stop_updates
    assert parsed.stop_sequence == 2
    assert parsed.stop_id is None
    assert (parsed.arrival_delay, parsed.departure_delay) == (100, 200)


# --- wire-level out-of-range enum totality ----------------------------------


def _varint(value: int) -> bytes:
    out = b""
    while True:
        low, value = value & 0x7F, value >> 7
        out += bytes([low | 0x80 if value else low])
        if not value:
            return out


def _tagged(field: int, wire_type: int, payload: bytes) -> bytes:
    return _varint((field << 3) | wire_type) + payload


def _length_delimited(field: int, data: bytes) -> bytes:
    return _tagged(field, 2, _varint(len(data)) + data)


def test_out_of_range_enums_on_the_wire_never_raise() -> None:
    """A producer (or newer spec) can put enum ints this bindings version
    doesn't know on the wire. Proto2 parks them in unknown fields, so the
    parser sees the default (SCHEDULED) and the message flows through as a
    normal entry -- never an exception. The bytes are hand-encoded because
    the Python protobuf API refuses to ASSIGN out-of-range values.
    """
    # TripDescriptor: trip_id=1, schedule_relationship=4 (varint 99).
    trip = _length_delimited(1, b"T9") + _tagged(4, 0, _varint(99))
    # StopTimeUpdate: stop_sequence=1, stop_id=4, schedule_relationship=5.
    stu = (
        _tagged(1, 0, _varint(1))
        + _length_delimited(4, b"S1")
        + _tagged(5, 0, _varint(99))
    )
    # TripUpdate: trip=1, stop_time_update=2, delay=5.
    trip_update = (
        _length_delimited(1, trip)
        + _length_delimited(2, stu)
        + _tagged(5, 0, _varint(7))
    )
    entity = _length_delimited(1, b"e1") + _length_delimited(3, trip_update)
    header = _length_delimited(1, b"2.0")  # FeedHeader.gtfs_realtime_version
    raw = _length_delimited(1, header) + _length_delimited(2, entity)
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)
    updates = trip_updates_from_message(msg)
    entry = updates.trips[("T9", None, None)]
    assert entry.delay_seconds == 7
    (parsed,) = entry.stop_updates
    # Both unknown relationships degraded to the SCHEDULED default.
    assert parsed.relationship == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED
    assert parsed.stop_sequence == 1
    assert updates.canceled_trips == set()
    assert updates.realtime_trips == []


# --- resolve_trip_predictions: the propagation state machine ----------------

_BASE = datetime(2026, 7, 30, 15, 0, tzinfo=UTC)


def _calls(*stop_ids: str) -> list[TripCall]:
    """A run calling at ``stop_ids`` ten minutes apart from 15:00 UTC."""
    return [
        TripCall(
            stop_sequence=i + 1,
            stop_id=stop_id,
            arrival=_BASE + timedelta(minutes=10 * i),
            departure=_BASE + timedelta(minutes=10 * i),
            pickup_type=None,
            drop_off_type=None,
            timepoint_exact=None,
            stop_headsign=None,
        )
        for i, stop_id in enumerate(stop_ids)
    ]


def _at(minute: int, delay: int = 0) -> datetime:
    return _BASE + timedelta(minutes=minute, seconds=delay)


_CALLS = _calls("A", "B", "C")
_LOOP_CALLS = _calls("A", "B", "A", "C")
_SKIPPED = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
_NO_DATA = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.NO_DATA


def _stu(
    *,
    stop_id: str | None = None,
    stop_sequence: int | None = None,
    relationship: int = 0,
    arrival: datetime | None = None,
    departure: datetime | None = None,
    delay: int | None = None,
    arrival_delay: int | None = None,
) -> TripStopUpdate:
    """An update; ``delay`` is the departure's."""
    return TripStopUpdate(
        stop_id=stop_id,
        stop_sequence=stop_sequence,
        relationship=relationship,
        arrival=arrival,
        departure=departure,
        arrival_delay=arrival_delay,
        departure_delay=delay,
    )


def _entry(
    *stop_updates: TripStopUpdate,
    trip_delay: int | None = None,
    vehicle_id: str | None = None,
) -> TripUpdateEntry:
    return TripUpdateEntry(
        stop_updates=stop_updates, delay_seconds=trip_delay, vehicle_id=vehicle_id
    )


def test_resolve_one_stu_propagates_to_tail() -> None:
    entry = _entry(_stu(stop_id="A", delay=300), vehicle_id="V1")
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.skipped == frozenset()
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 300,
        2: 300,
        3: 300,
    }
    # Propagated stops carry their scheduled times moved by the delay, and
    # the trip's vehicle.
    assert resolved.predictions[2].arrival == _at(10, 300)
    assert resolved.predictions[2].departure == _at(10, 300)
    assert resolved.predictions[2].vehicle_id == "V1"


def test_resolve_zero_delay_propagates() -> None:
    """delay=0 (on time) is realtime information, not falsy-absent."""
    entry = _entry(_stu(stop_id="A", delay=0))
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 0,
        2: 0,
        3: 0,
    }


def test_resolve_later_stu_overrides_propagation() -> None:
    entry = _entry(_stu(stop_id="A", delay=300), _stu(stop_id="C", delay=60))
    resolved = resolve_trip_predictions(entry, _calls("A", "B", "C", "D"))
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 300,
        2: 300,
        3: 60,
        4: 60,
    }


def test_resolve_no_data_cuts_propagation_and_trip_delay() -> None:
    """NO_DATA yields no prediction at its stop and schedule-only stops
    after it -- the trip-level fallback does NOT resume inside a NO_DATA
    region (it is StopTimeUpdate-derived coverage).
    """
    entry = _entry(
        _stu(stop_id="A", delay=300),
        _stu(stop_id="B", relationship=_NO_DATA),
        trip_delay=120,
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert set(resolved.predictions) == {1}
    assert resolved.predictions[1].delay_seconds == 300


def test_resolve_stu_after_no_data_resumes() -> None:
    entry = _entry(
        _stu(stop_id="A", relationship=_NO_DATA),
        _stu(stop_id="B", delay=45),
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        2: 45,
        3: 45,
    }


def test_resolve_trip_delay_fallback_only_where_uncovered() -> None:
    """The trip-level delay covers stops BEFORE the first STU (propagation
    only flows forward) and trips with no STUs at all; STU-derived state
    wins from the first delay-bearing STU onward.
    """
    entry = _entry(_stu(stop_id="B", delay=300), trip_delay=120)
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 120,
        2: 300,
        3: 300,
    }
    bare = resolve_trip_predictions(_entry(trip_delay=90), _CALLS)
    assert {seq: p.delay_seconds for seq, p in bare.predictions.items()} == {
        1: 90,
        2: 90,
        3: 90,
    }


def test_resolve_time_only_update_propagates_its_computed_delay() -> None:
    """ "If only time is provided in the update, a delay computed by
    comparing the time against the GTFS schedule time" is propagated: B is
    due 15:10 and predicted 15:30, so C runs 1200 s late too -- not A's
    stale 300."""
    entry = _entry(
        _stu(stop_id="A", delay=300),
        _stu(stop_id="B", departure=_at(30)),
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.predictions[2].departure == _at(30)
    assert resolved.predictions[2].arrival is None  # B's update gives no arrival
    assert resolved.predictions[2].delay_seconds == 1200
    assert resolved.predictions[3].delay_seconds == 1200
    assert resolved.predictions[3].departure == _at(20, 1200)


def test_resolve_each_event_keeps_its_own_delay() -> None:
    """An arrival delay of 60 s and a departure delay of 120 s at one stop
    predict each event by its own; the departure's goes on."""
    entry = _entry(_stu(stop_id="A", arrival_delay=60, delay=120))
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.predictions[1].arrival == _at(0, 60)
    assert resolved.predictions[1].departure == _at(0, 120)
    assert resolved.predictions[2].arrival == _at(10, 120)


def test_resolve_an_arrival_only_update_carries_its_delay_onward() -> None:
    entry = _entry(_stu(stop_id="A", arrival_delay=90))
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.predictions[1].arrival == _at(0, 90)
    assert resolved.predictions[1].departure is None  # not given: not predicted
    assert resolved.predictions[3].arrival == _at(20, 90)


def test_resolve_predicts_only_the_events_an_update_gives() -> None:
    """A layover: A due 15:00-15:10, B 15:12. Times only: A arrives 15:05,
    B arrives 15:12. A's departure is not given, so it is not predicted --
    not worked out from its arrival as 15:15, after the bus reaches B."""
    calls = [
        replace(_CALLS[0], departure=_at(10)),
        replace(_CALLS[1], arrival=_at(12), departure=_at(12)),
    ]
    entry = _entry(
        _stu(stop_id="A", arrival=_at(5)), _stu(stop_id="B", arrival=_at(12))
    )
    resolved = resolve_trip_predictions(entry, calls)
    assert (resolved.predictions[1].arrival, resolved.predictions[1].departure) == (
        _at(5),
        None,
    )
    assert resolved.predictions[1].delay_seconds == 300
    assert resolved.predictions[2].arrival == _at(12)


def test_resolve_a_time_wins_over_a_disagreeing_delay() -> None:
    """ "If both time and delay are specified, time will take precedence":
    a departure time 10 min late with delay 0 is 10 min late, and so is the
    rest of the trip."""
    entry = _entry(_stu(stop_id="A", departure=_at(10), delay=0))
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.predictions[1].delay_seconds == 600
    assert resolved.predictions[2].departure == _at(20)


def test_resolve_a_delayless_update_does_not_end_the_trip_delay() -> None:
    """The trip-level delay "is only propagated until the next stop along
    the trip with a StopTimeUpdate delay value specified": an update for an
    untimed stop states none, so the trip's 300 s still reaches C."""
    untimed = [*_CALLS[:1], replace(_CALLS[1], arrival=None, departure=None), _CALLS[2]]
    entry = _entry(_stu(stop_id="B", arrival=_at(12)), trip_delay=300)
    resolved = resolve_trip_predictions(entry, untimed)
    assert resolved.predictions[1].delay_seconds == 300
    assert resolved.predictions[2].arrival == _at(12)
    assert resolved.predictions[3].departure == _at(20, 300)


def test_resolve_an_update_predicts_only_its_own_events_under_a_trip_delay() -> None:
    """A stop's own update decides that stop: under a trip-level 300 s, B's
    update giving only a 600 s arrival delay predicts B's arrival, and no
    departure -- neither the trip's 300 s nor anything else fills it."""
    entry = _entry(_stu(stop_id="B", arrival_delay=600), trip_delay=300)
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.predictions[1].arrival == _at(0, 300)
    b = resolved.predictions[2]
    assert (b.arrival, b.departure, b.delay_seconds) == (_at(10, 600), None, 600)
    assert resolved.predictions[3].arrival == _at(20, 600)


def test_resolve_a_time_at_an_untimed_call_keeps_its_delay() -> None:
    """With no schedule to compare a time with, the event's delay is what it
    states: B (untimed) gives a time and a 600 s delay, and C runs 600 s
    late."""
    untimed = [*_CALLS[:1], replace(_CALLS[1], arrival=None, departure=None), _CALLS[2]]
    entry = _entry(_stu(stop_id="B", departure=_at(12), delay=600))
    resolved = resolve_trip_predictions(entry, untimed)
    assert resolved.predictions[2].delay_seconds == 600
    assert resolved.predictions[3].arrival == _at(20, 600)


def test_resolve_no_data_ends_the_trip_delay() -> None:
    """NO_DATA before any stated delay ends the trip-level delay too: "no
    realtime timing information" from there on."""
    entry = _entry(_stu(stop_id="B", relationship=_NO_DATA), trip_delay=300)
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert set(resolved.predictions) == {1}


def test_resolve_skipped_marks_stop_and_never_alters_propagation() -> None:
    entry = _entry(
        _stu(stop_id="A", delay=300),
        # Any delay a SKIPPED STU carries is ignored (spec discourages it).
        _stu(stop_id="B", relationship=_SKIPPED, delay=999),
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.skipped == frozenset({2})
    assert set(resolved.predictions) == {1, 3}
    assert resolved.predictions[3].delay_seconds == 300


def test_resolve_a_bare_stop_id_on_a_revisited_stop_names_nothing() -> None:
    """ "stop_sequence is required for trips that visit the same stop_id
    more than once (e.g., a loop) to disambiguate": a bare A on a trip
    calling at A twice is not guessed onto either visit, whatever came
    before it -- a 15:31 meant for the second A would otherwise make the
    whole trip half an hour late."""
    alone = _entry(_stu(stop_id="A", arrival=_at(31)))
    assert resolve_trip_predictions(alone, _LOOP_CALLS).predictions == {}
    after_b = _entry(_stu(stop_id="B", delay=60), _stu(stop_id="A", delay=120))
    resolved = resolve_trip_predictions(after_b, _LOOP_CALLS)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        2: 60,
        3: 60,
        4: 60,
    }


def test_resolve_an_assigned_stop_agrees_with_its_sequence() -> None:
    """A stop_id "must match assigned_stop_id" when both are sent: stop 2
    reassigned to platform B2 still names stop_sequence 2, and its newer
    delay replaces A's."""
    reassigned = TripStopUpdate(
        stop_id="B2",
        stop_sequence=2,
        relationship=0,
        arrival=None,
        departure=None,
        departure_delay=600,
        assigned_stop_id="B2",
    )
    entry = _entry(_stu(stop_sequence=1, delay=60), reassigned)
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        1: 60,
        2: 600,
        3: 600,
    }


def test_resolve_a_scheduled_update_replaces_the_carried_delay() -> None:
    """ "Updates with schedule relationships of SCHEDULED ... or NO_DATA
    will" stop delay propagation. An update for an untimed stop (no
    schedule to compare its time with) states no delay, so A's 600 s does
    not reach past it; an update stating nothing at all likewise."""
    untimed = [*_CALLS[:1], replace(_CALLS[1], arrival=None, departure=None), _CALLS[2]]
    timed_at_untimed = _entry(
        _stu(stop_id="A", delay=600), _stu(stop_id="B", arrival=_at(12))
    )
    resolved = resolve_trip_predictions(timed_at_untimed, untimed)
    assert resolved.predictions[2].arrival == _at(12)
    assert resolved.predictions[2].delay_seconds is None
    assert 3 not in resolved.predictions
    empty = _entry(_stu(stop_id="A", delay=300), _stu(stop_id="B"))
    assert set(resolve_trip_predictions(empty, _CALLS).predictions) == {1}
    assert resolve_trip_predictions(_entry(_stu(stop_id="B")), _CALLS).predictions == {}


def test_resolve_the_specs_example_2() -> None:
    """trip-updates.md, Example 2, verbatim: 300 s at stop_sequence 3,
    60 s at 8, NO_DATA at 10 on a 20-stop trip -- 1,2 unknown; 3-7 300 s;
    8,9 60 s; 10-20 unknown."""
    calls = _calls(*(f"S{n}" for n in range(1, 21)))
    entry = _entry(
        _stu(stop_sequence=3, delay=300, arrival_delay=300),
        _stu(stop_sequence=8, delay=60, arrival_delay=60),
        _stu(stop_sequence=10, relationship=_NO_DATA),
    )
    resolved = resolve_trip_predictions(entry, calls)
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        **dict.fromkeys(range(3, 8), 300),
        **dict.fromkeys(range(8, 10), 60),
    }


def test_resolve_disagreeing_sequence_and_stop_name_no_call() -> None:
    """stop_sequence 1 is A; an update naming it with stop C describes no
    call of this trip and is ignored, as a StopSelector would be."""
    entry = _entry(_stu(stop_sequence=1, stop_id="C", delay=300))
    assert resolve_trip_predictions(entry, _CALLS).predictions == {}


def test_resolve_stop_sequence_wins_and_addresses_later_loop_visit() -> None:
    entry = _entry(_stu(stop_id="A", stop_sequence=3, delay=200))
    resolved = resolve_trip_predictions(entry, _LOOP_CALLS)
    # stop_sequence=3 addresses the SECOND visit to A even though stop_id
    # "A" alone would have matched the first: stops before it stay
    # schedule-only, the addressed visit and the tail get the delay.
    assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
        3: 200,
        4: 200,
    }


def test_resolve_unplaceable_stus_are_ignored() -> None:
    entry = _entry(
        _stu(stop_id="GHOST", delay=300),
        _stu(stop_sequence=99, delay=300),
        _stu(delay=300),  # neither field
    )
    resolved = resolve_trip_predictions(entry, _CALLS)
    assert resolved.predictions == {}
    assert resolved.skipped == frozenset()


def test_resolve_unrecognized_relationship_behaves_as_scheduled() -> None:
    """UNSCHEDULED and out-of-vocabulary relationship values resolve as
    SCHEDULED (TripStopUpdate's documented contract): the delay predicts
    its own stop and propagates onward exactly as a SCHEDULED STU would.
    """
    for relationship in (gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.UNSCHEDULED, 99):
        entry = _entry(_stu(stop_id="A", relationship=relationship, delay=120))
        resolved = resolve_trip_predictions(entry, _CALLS)
        assert resolved.skipped == frozenset()
        assert {seq: p.delay_seconds for seq, p in resolved.predictions.items()} == {
            1: 120,
            2: 120,
            3: 120,
        }


def _rich_vehicle_message() -> gtfs_realtime_pb2.FeedMessage:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "v1"
    vehicle = entity.vehicle
    vehicle.vehicle.id = "V1"
    vehicle.vehicle.wheelchair_accessible = (
        gtfs_realtime_pb2.VehicleDescriptor.WHEELCHAIR_ACCESSIBLE
    )
    vehicle.position.latitude = 34.0
    vehicle.position.longitude = -118.0
    vehicle.position.odometer = 123456.75
    vehicle.occupancy_percentage = 55
    for index, (label, percent) in enumerate((("A", 10), ("B", -1))):
        carriage = vehicle.multi_carriage_details.add()
        carriage.id = f"c{index}"
        carriage.label = label
        carriage.occupancy_percentage = percent
        carriage.carriage_sequence = index + 1
    return message


def test_vehicle_rich_fields_are_surfaced() -> None:
    (vehicle,) = vehicles_from_message(
        _rich_vehicle_message(), routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert vehicle.odometer == 123456.75
    assert vehicle.occupancy_percentage == 55
    # RT numbers WheelchairAccessible 0-3 and the static cell 0-2, so this
    # must map by name; mapping by value would make ACCESSIBLE (2) read as
    # the static NOT_POSSIBLE (2).
    assert vehicle.wheelchair_accessible is WheelchairAccess.POSSIBLE
    assert [c.label for c in vehicle.carriages] == ["A", "B"]
    assert vehicle.carriages[0].occupancy_percentage == 10
    # -1 is the proto's "no data" sentinel, not a negative percentage.
    assert vehicle.carriages[1].occupancy_percentage is None
    assert [c.carriage_sequence for c in vehicle.carriages] == [1, 2]


def test_vehicle_unset_rich_fields_are_none() -> None:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "v2"
    entity.vehicle.position.latitude = 1.0
    entity.vehicle.position.longitude = 2.0
    (vehicle,) = vehicles_from_message(
        message, routes_by_id={}, stops_by_id={}, trip_routes={}
    )
    assert vehicle.odometer is None
    assert vehicle.occupancy_percentage is None
    # NO_VALUE is the proto default: the producer said nothing at all, which
    # is not the same as an explicit UNKNOWN.
    assert vehicle.wheelchair_accessible is None
    assert vehicle.carriages == ()


def test_alert_text_detail_and_images_are_surfaced() -> None:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "a1"
    alert = entity.alert
    alert.header_text.translation.add().text = "Detour"
    alert.tts_header_text.translation.add().text = "Dee tour"
    alert.description_text.translation.add().text = "Via 5th"
    alert.tts_description_text.translation.add().text = "Via fifth"
    alert.cause_detail.translation.add().text = "Water main"
    alert.effect_detail.translation.add().text = "Stops skipped"
    alert.image_alternative_text.translation.add().text = "Map of the detour"
    first = alert.image.localized_image.add()
    first.url = "https://example.com/detour.png"
    first.media_type = "image/png"
    first.language = "en"
    # A localized variant with no url is not an image.
    alert.image.localized_image.add().media_type = "image/png"

    [parsed] = alerts_from_message(message)
    assert parsed.tts_header == "Dee tour"
    assert parsed.tts_description == "Via fifth"
    assert parsed.cause_detail == "Water main"
    assert parsed.effect_detail == "Stops skipped"
    assert parsed.image_alternative_text == "Map of the detour"
    assert [(i.url, i.media_type, i.language) for i in parsed.images] == [
        ("https://example.com/detour.png", "image/png", "en")
    ]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("20260730", date(2026, 7, 30), id="valid"),
        pytest.param("2026-07-30", None, id="dashed"),
        pytest.param("20261345", None, id="impossible"),
        pytest.param("", None, id="empty"),
        # strptime would read these as real days.
        pytest.param("2026111", None, id="seven-digits"),
        pytest.param("2026 111", None, id="space"),
        pytest.param(
            "\uff12\uff10\uff12\uff16\uff10\uff17\uff13\uff10", None, id="fullwidth"
        ),
    ],
)
def test_lenient_service_date(raw: str, expected: date | None) -> None:
    """A malformed service_date means "not a day this applies to"."""
    assert _lenient_service_date(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("06:10:00", 22200, id="valid"),
        pytest.param("25:30:00", 91800, id="past-midnight"),
        pytest.param("ten past six", None, id="garbage"),
        pytest.param("", None, id="empty"),
    ],
)
def test_lenient_start_secs(raw: str, expected: int | None) -> None:
    assert _lenient_start_secs(raw) == expected


def test_trip_modifications_without_modifications_is_skipped() -> None:
    """An entity selecting trips but describing no change is not a detour."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "empty-mod"
    entity.trip_modifications.selected_trips.add().trip_ids.append("T1")
    entity.trip_modifications.service_dates.append("20260730")
    assert trip_modifications_from_message(message) == []


def test_trip_modifications_without_selected_trips_is_skipped() -> None:
    """A modification naming no trip applies to nothing."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "no-trips"
    modification = entity.trip_modifications.modifications.add()
    modification.start_stop_selector.stop_sequence = 1
    modification.end_stop_selector.stop_sequence = 1
    entity.trip_modifications.selected_trips.add()  # no trip_ids
    entity.trip_modifications.service_dates.append("20260730")
    assert trip_modifications_from_message(message) == []


@pytest.mark.parametrize("dates", [(), ("not-a-date",)])
def test_trip_modifications_without_a_usable_service_date_are_skipped(
    dates: tuple[str, ...],
) -> None:
    """service_dates is required; naming none (or only garbage) names no run,
    rather than every day the trip runs.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "undated"
    modification = entity.trip_modifications.modifications.add()
    modification.start_stop_selector.stop_sequence = 1
    modification.end_stop_selector.stop_sequence = 1
    entity.trip_modifications.selected_trips.add().trip_ids.append("T1")
    entity.trip_modifications.service_dates.extend(dates)
    assert trip_modifications_from_message(message) == []
