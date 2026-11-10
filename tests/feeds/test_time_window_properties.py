"""Property tests for the start/end/limit window and open-ended scanning.

The central law: an open-ended query (``end=None``, ``limit=k``) returns
exactly the first ``k`` rows of the same query bounded at the feed's scan
ceiling. Expanding horizons are an optimization; this law says they never
change an answer. It is checked for every dated query, schedule-only and
realtime, including realtime predictions running up to the early margin.
"""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta

from google.transit import gtfs_realtime_pb2
from hypothesis import HealthCheck, event, given, reject, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import FeedParseError
from aiomobilitydatabase.feeds.models import (
    ArrivalsQuery,
    StopArrival,
    TripQuery,
    UpcomingTrip,
)
from aiomobilitydatabase.feeds.static_index import StaticIndex
from aiomobilitydatabase.feeds.transit import (
    _RT_EARLY_MARGIN,
    TransitFeedHandle,
    _DirectUrls,
    _horizon_settles,
    _horizons,
)

from tests.feeds.test_properties import _index_from_zip_bytes, _random_gtfs_zip

_SETTINGS = settings(
    max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)

# The generated calendars run 2026-01-01 .. 2027-12-31.
_STARTS = st.datetimes(
    min_value=datetime(2025, 12, 1),
    max_value=datetime(2028, 1, 15),
    timezones=st.just(UTC),
)


def _run[T](
    zip_bytes: bytes,
    body: Callable[[TransitFeedHandle, StaticIndex], Awaitable[T]],
    message: gtfs_realtime_pb2.FeedMessage | None = None,
) -> T:
    """Run ``body`` against a handle over the generated feed, no network.

    With ``message``, every realtime fetch returns it; otherwise the handle
    has no realtime sources at all.
    """

    try:
        index = _index_from_zip_bytes(zip_bytes)
    except FeedParseError:
        reject()  # a generated feed too malformed to index has no answers

    async def scenario() -> T:
        async with MobilityFeedsClient() as client:
            handle = TransitFeedHandle(
                client,
                None,
                [],
                index,
                None,
                direct=_DirectUrls(
                    static_url="https://example.com/g.zip", headers=None
                ),
            )
            if message is not None:

                async def fetch(*_args: object, **_kwargs: object) -> list[object]:
                    return [message]

                handle._fetch_entity_messages = fetch  # type: ignore[method-assign]
            try:
                return await body(handle, index)
            finally:
                handle.close()

    return asyncio.run(scenario())


def _arrival_key(row: StopArrival) -> tuple[object, ...]:
    return (
        row.trip_id,
        row.stop_id,
        row.scheduled_departure,
        row.predicted_departure,
        row.predicted_arrival,
    )


def _trip_key(row: UpcomingTrip) -> tuple[object, ...]:
    return (row.trip_id, row.scheduled_departure, row.predicted_departure)


def _anchor(index: StaticIndex) -> tuple[str, list[str]]:
    """The generated feed's anchor trip and its first stops (always real ids).

    ``_random_gtfs_zip`` guarantees the first trip's first two stop_times
    rows reference real ids, which is where non-empty answers come from.
    """
    rows = index._conn.execute(
        "SELECT trip_id, stop_id FROM stop_times ORDER BY rowid LIMIT 2"
    ).fetchall()
    return rows[0][0], [stop_id for _, stop_id in rows]


def _stop_choices(index: StaticIndex, draw_ids: Sequence[int]) -> list[str]:
    stop_ids = [*sorted({stop.id for stop in index.stops()}), "NOT-A-STOP"]
    return sorted({stop_ids[i % len(stop_ids)] for i in draw_ids})


# -- the helpers themselves ---------------------------------------------------


@given(
    start=_STARTS,
    span_hours=st.integers(min_value=-48, max_value=24 * 800),
)
def test_horizons_grow_strictly_and_end_at_the_ceiling(
    start: datetime, span_hours: int
) -> None:
    ceiling = start + timedelta(hours=span_hours)
    horizons = list(_horizons(start, None, ceiling))
    assert horizons[-1] == (ceiling, True)
    assert all(not final for _, final in horizons[:-1])
    ends = [end for end, _ in horizons]
    assert ends == sorted(set(ends)), "horizons must strictly increase"
    assert len(horizons) <= 20  # doubling from 2h covers 800 days in <20 passes


def test_horizons_with_an_end_is_one_final_pass() -> None:
    start = datetime(2026, 7, 30, tzinfo=UTC)
    end = start + timedelta(minutes=5)
    assert list(_horizons(start, end, start + timedelta(days=365))) == [(end, True)]


def test_horizon_settles_rules() -> None:
    horizon = datetime(2026, 7, 30, 12, tzinfo=UTC)
    early = horizon - timedelta(hours=2)
    late = horizon - timedelta(minutes=30)
    margin = timedelta(hours=1)
    # Full and well inside the horizon: settled.
    assert _horizon_settles([[early]], [1], horizon, margin)
    # Full, but an early-running trip from beyond the horizon could still
    # sort ahead of the last row.
    assert not _horizon_settles([[late]], [1], horizon, margin)
    assert _horizon_settles([[late]], [1], horizon, timedelta(0))
    # Room left on the board.
    assert not _horizon_settles([[early]], [2], horizon, margin)
    # Unlimited never settles before the ceiling.
    assert not _horizon_settles([[early]], [None], horizon, margin)
    # Zero limit is trivially full.
    assert _horizon_settles([[]], [0], horizon, margin)
    # Every board must settle.
    assert not _horizon_settles([[early], []], [1, 1], horizon, margin)


# -- open-ended equals bounded-at-the-ceiling ---------------------------------


@_SETTINGS
@given(
    zip_bytes=_random_gtfs_zip(),
    start=_STARTS,
    stop_picks=st.lists(st.integers(0, 10), min_size=1, max_size=3),
    limit=st.integers(0, 5),
    use_anchor=st.booleans(),
)
def test_open_ended_scheduled_departures_equal_bounded_truncated(
    zip_bytes: bytes,
    start: datetime,
    stop_picks: list[int],
    limit: int,
    use_anchor: bool,
) -> None:
    async def body(handle: TransitFeedHandle, index: StaticIndex) -> None:
        stops = _stop_choices(index, stop_picks)
        if use_anchor:
            stops = sorted({*stops, _anchor(index)[1][0]})
        [open_ended] = await handle.get_scheduled_departures(
            [ArrivalsQuery(stops, limit=limit)], start=start
        )
        ceiling = index.scan_ceiling()
        if ceiling is None:
            assert open_ended == []
            return
        [bounded] = await handle.get_scheduled_departures(
            [ArrivalsQuery(stops, limit=None)], start=start, end=ceiling
        )
        event(f"scheduled rows: {min(len(open_ended), 3)}")
        assert [_arrival_key(r) for r in open_ended] == [
            _arrival_key(r) for r in bounded[:limit]
        ]
        # Limit prefix: a larger limit only appends.
        [wider] = await handle.get_scheduled_departures(
            [ArrivalsQuery(stops, limit=limit + 2)], start=start
        )
        assert [_arrival_key(r) for r in wider[:limit]] == [
            _arrival_key(r) for r in open_ended
        ]
        # Start bound.
        assert all(
            r.scheduled_departure is not None and r.scheduled_departure >= start
            for r in open_ended
        )

    _run(zip_bytes, body)


@_SETTINGS
@given(
    zip_bytes=_random_gtfs_zip(),
    start=_STARTS,
    stop_picks=st.lists(st.integers(0, 10), min_size=1, max_size=3),
    limit=st.integers(1, 5),
    delays=st.lists(
        st.integers(-int(_RT_EARLY_MARGIN.total_seconds()), 7200), min_size=1
    ),
    use_anchor=st.booleans(),
)
def test_open_ended_arrivals_equal_bounded_truncated_with_realtime(
    *,
    zip_bytes: bytes,
    start: datetime,
    stop_picks: list[int],
    limit: int,
    delays: list[int],
    use_anchor: bool,
) -> None:
    """Holds with predictions running up to the early margin, either way."""
    trip_ids = _trip_ids(zip_bytes)  # anchor trip first
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    for i, delay in enumerate(delays):
        entity = message.entity.add()
        entity.id = f"d{i}"
        entity.trip_update.trip.trip_id = trip_ids[i % len(trip_ids)]
        entity.trip_update.delay = delay

    async def body(handle: TransitFeedHandle, index: StaticIndex) -> None:
        stops = _stop_choices(index, stop_picks)
        if use_anchor:
            stops = sorted({*stops, _anchor(index)[1][0]})
        [open_ended] = await handle.get_arrivals(
            [ArrivalsQuery(stops, limit=limit)], start=start
        )
        ceiling = index.scan_ceiling()
        if ceiling is None:
            return
        [bounded] = await handle.get_arrivals(
            [ArrivalsQuery(stops, limit=None)], start=start, end=ceiling
        )
        event(f"realtime rows: {min(len(open_ended), 3)}")
        event(f"predicted: {any(r.predicted_departure for r in open_ended)}")
        assert [_arrival_key(r) for r in open_ended] == [
            _arrival_key(r) for r in bounded[:limit]
        ]

    _run(zip_bytes, body, message)


@_SETTINGS
@given(
    zip_bytes=_random_gtfs_zip(),
    start=_STARTS,
    pair_picks=st.tuples(st.integers(0, 10), st.integers(0, 10)),
    limit=st.integers(0, 4),
    use_anchor=st.booleans(),
)
def test_open_ended_upcoming_trips_equal_bounded_truncated(
    zip_bytes: bytes,
    start: datetime,
    pair_picks: tuple[int, int],
    limit: int,
    use_anchor: bool,
) -> None:
    async def body(handle: TransitFeedHandle, index: StaticIndex) -> None:
        stop_ids = sorted({stop.id for stop in index.stops()})
        origin = stop_ids[pair_picks[0] % len(stop_ids)]
        destination = stop_ids[pair_picks[1] % len(stop_ids)]
        anchor_stops = _anchor(index)[1]
        if use_anchor and len(anchor_stops) == 2:
            origin, destination = anchor_stops
        [open_ended] = await handle.get_upcoming_trips(
            [TripQuery(origin, destination, limit=limit)], start=start
        )
        ceiling = index.scan_ceiling()
        if ceiling is None:
            assert open_ended == []
            return
        [bounded] = await handle.get_upcoming_trips(
            [TripQuery(origin, destination, limit=None)], start=start, end=ceiling
        )
        event(f"trip rows: {min(len(open_ended), 3)}")
        assert [_trip_key(r) for r in open_ended] == [
            _trip_key(r) for r in bounded[:limit]
        ]

    _run(zip_bytes, body)


@_SETTINGS
@given(
    zip_bytes=_random_gtfs_zip(),
    start=_STARTS,
    query_picks=st.lists(
        st.tuples(
            st.lists(st.integers(0, 10), min_size=1, max_size=2), st.integers(0, 4)
        ),
        min_size=1,
        max_size=3,
    ),
    hours=st.integers(1, 72),
)
def test_a_batch_equals_its_queries_run_alone(
    zip_bytes: bytes,
    start: datetime,
    query_picks: list[tuple[list[int], int]],
    hours: int,
) -> None:
    end = start + timedelta(hours=hours)

    async def body(handle: TransitFeedHandle, index: StaticIndex) -> None:
        queries = [
            ArrivalsQuery(_stop_choices(index, picks), limit=limit)
            for picks, limit in query_picks
        ]
        for method in (handle.get_arrivals, handle.get_scheduled_departures):
            batch = await method(queries, start=start, end=end)
            alone = [(await method([q], start=start, end=end))[0] for q in queries]
            assert [[_arrival_key(r) for r in b] for b in batch] == [
                [_arrival_key(r) for r in a] for a in alone
            ]
            for board in batch:
                assert all(
                    r.scheduled_departure is None
                    or start - timedelta(hours=1) <= r.scheduled_departure <= end
                    for r in board
                )

    _run(zip_bytes, body)


def _trip_ids(zip_bytes: bytes) -> list[str]:
    try:
        index = _index_from_zip_bytes(zip_bytes)
    except FeedParseError:
        reject()
    try:
        anchor_trip = _anchor(index)[0]
        rows = index._conn.execute("SELECT DISTINCT source_trip_id FROM trips")
        others = sorted(row[0] for row in rows if row[0] != anchor_trip)
        return [anchor_trip, *others]
    finally:
        index.close()
