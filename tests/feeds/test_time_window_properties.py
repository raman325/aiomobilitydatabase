"""Property tests for the start/end/limit window and open-ended scanning.

The central law: an open-ended query (``end=None``, ``limit=k``) returns
exactly the first ``k`` rows of the same query bounded at the feed's scan
ceiling. Expanding horizons are an optimization; this law says they never
change an answer. It is checked for every dated query, schedule-only and
realtime, including realtime predictions running up to the early margin.
"""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, date, datetime, time, timedelta

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

# The generated calendars run 2026-01-01 .. 2027-12-31, with stop times up
# to 47:59:59, so nothing is scheduled after this instant. Bounded queries
# ending here are the reference an open-ended query must match.
_BEYOND_SERVICE = datetime(2028, 1, 3, tzinfo=UTC)

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
    end_hours=st.one_of(st.none(), st.integers(min_value=0, max_value=24 * 800)),
)
def test_horizons_grow_strictly_and_end_at_the_last_instant_worth_reading(
    start: datetime, span_hours: int, end_hours: int | None
) -> None:
    ceiling = start + timedelta(hours=span_hours)
    end = None if end_hours is None else start + timedelta(hours=end_hours)
    horizons = list(_horizons(start, end, ceiling))
    expected_last = max(start, ceiling if end is None else min(end, ceiling))
    assert horizons[-1] == (expected_last, True)
    assert all(not final for _, final in horizons[:-1])
    ends = [horizon for horizon, _ in horizons]
    assert ends == sorted(set(ends)), "horizons must strictly increase"
    assert len(horizons) <= 20  # doubling from 2h covers 800 days in <20 passes


def test_horizons_stop_at_the_end_or_the_ceiling_whichever_is_first() -> None:
    start = datetime(2026, 7, 30, tzinfo=UTC)
    ceiling = start + timedelta(days=365)
    short = start + timedelta(minutes=5)
    assert list(_horizons(start, short, ceiling)) == [(short, True)]
    week = start + timedelta(days=7)
    horizons = list(_horizons(start, week, ceiling))
    assert horizons[-1] == (week, True)
    assert [h for h, _ in horizons[:2]] == [
        start + timedelta(hours=2),
        start + timedelta(hours=4),
    ]
    # Past the ceiling nothing is scheduled: stop there.
    far = start + timedelta(days=900)
    assert list(_horizons(start, far, ceiling))[-1] == (ceiling, True)
    # No departure can ever occur: one pass at start (RT-added rows only).
    assert list(_horizons(start, week, None)) == [(start, True)]
    assert list(_horizons(start, None, None)) == [(start, True)]


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


# Realtime update shapes: (kind, delay seconds, stop_sequence, start_date).
# Delays stay within the early-running margin on the early side, which is
# the guarantee the open-ended scan makes; dates span the generated feeds.
_RT_UPDATES = st.tuples(
    st.sampled_from(["trip_delay", "stop_delay", "cancel"]),
    st.integers(-int(_RT_EARLY_MARGIN.total_seconds()), 7200),
    st.one_of(st.none(), st.integers(0, 3)),
    st.one_of(
        st.none(),
        st.just("RUN_DAY"),  # the day of the run the example starts near
        st.dates(min_value=date(2025, 12, 28), max_value=date(2028, 1, 3)).map(
            lambda day: day.strftime("%Y%m%d")
        ),
    ),
)

# Half the realtime examples start shortly before the anchor trip departs on
# some day, so date-less updates have a current run to bind to and dated
# ones a matching day; otherwise most starts fall nowhere near a run.
_NEAR_RUN = st.one_of(
    st.none(),
    st.tuples(
        st.dates(min_value=date(2026, 1, 1), max_value=date(2027, 12, 31)),
        st.integers(0, 150),
    ),
)


def _near_run_start(
    index: StaticIndex, start: datetime, near: tuple[date, int] | None
) -> tuple[datetime, date | None]:
    """``start``, or an instant ``minutes`` before the anchor's departure."""
    if near is None:
        return start, None
    day, minutes = near
    secs = index._conn.execute(
        "SELECT departure_secs FROM stop_times ORDER BY rowid LIMIT 1"
    ).fetchone()[0]
    if secs is None:
        return start, None
    midnight = datetime.combine(day, time(), tzinfo=UTC)  # generated feeds are UTC
    return midnight + timedelta(seconds=secs, minutes=-minutes), day


def _rt_message(
    trip_ids: list[str],
    updates: list[tuple[str, int, int | None, str | None]],
    run_day: date | None = None,
) -> gtfs_realtime_pb2.FeedMessage:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    for i, (kind, delay, stop_sequence, date_choice) in enumerate(updates):
        entity = message.entity.add()
        entity.id = f"u{i}"
        trip = entity.trip_update.trip
        trip.trip_id = trip_ids[i % len(trip_ids)]
        start_date = (
            (run_day.strftime("%Y%m%d") if run_day else None)
            if date_choice == "RUN_DAY"
            else date_choice
        )
        if start_date is not None:
            trip.start_date = start_date
        if kind == "cancel":
            trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
        elif kind == "trip_delay" or stop_sequence is None:
            entity.trip_update.delay = delay
        else:
            update = entity.trip_update.stop_time_update.add()
            update.stop_sequence = stop_sequence
            update.departure.delay = delay
    return message


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
        ceiling = max(_BEYOND_SERVICE, start)
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
    updates=st.lists(_RT_UPDATES, min_size=1, max_size=6),
    use_anchor=st.booleans(),
    near=_NEAR_RUN,
    lead=st.tuples(
        st.integers(-int(_RT_EARLY_MARGIN.total_seconds()), 7200),
        st.sampled_from([None, "RUN_DAY"]),
    ),
)
def test_open_ended_arrivals_equal_bounded_truncated_with_realtime(
    *,
    zip_bytes: bytes,
    start: datetime,
    stop_picks: list[int],
    limit: int,
    updates: list[tuple[str, int, int | None, str | None]],
    use_anchor: bool,
    near: tuple[date, int] | None,
    lead: tuple[int, str | None],
) -> None:
    """Holds under realtime: delays and early running up to the margin, at
    trip or stop level, and cancellations, each dated or date-less."""
    if near is not None:
        # Starting near the anchor's run: query its stop, and lead with a
        # delay for that run (dated to its day, or date-less = current).
        use_anchor = True
        updates = [("trip_delay", lead[0], None, lead[1]), *updates]
    trip_ids = _trip_ids(zip_bytes)
    message = gtfs_realtime_pb2.FeedMessage()

    async def body(handle: TransitFeedHandle, index: StaticIndex) -> None:
        nonlocal start
        start, run_day = _near_run_start(index, start, near)
        message.CopyFrom(_rt_message(trip_ids, updates, run_day))
        stops = _stop_choices(index, stop_picks)
        if use_anchor:
            stops = sorted({*stops, _anchor(index)[1][0]})
        [open_ended] = await handle.get_arrivals(
            [ArrivalsQuery(stops, limit=limit)], start=start
        )
        ceiling = max(_BEYOND_SERVICE, start)
        [bounded] = await handle.get_arrivals(
            [ArrivalsQuery(stops, limit=None)], start=start, end=ceiling
        )
        event(f"realtime rows: {min(len(open_ended), 3)}")
        event(f"predicted: {any(r.predicted_departure for r in open_ended)}")
        event(f"bounded shorter than unlimited: {len(bounded) < len(open_ended)}")
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
        ceiling = max(_BEYOND_SERVICE, start)
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


@_SETTINGS
@given(
    zip_bytes=_random_gtfs_zip(),
    start=_STARTS,
    query_picks=st.lists(
        st.tuples(
            st.lists(st.integers(0, 10), min_size=1, max_size=2),
            st.integers(0, 4),
            st.booleans(),
        ),
        min_size=2,
        max_size=4,
    ),
    updates=st.lists(_RT_UPDATES, max_size=4),
)
def test_an_open_ended_batch_equals_its_queries_run_alone(
    zip_bytes: bytes,
    start: datetime,
    query_picks: list[tuple[list[int], int, bool]],
    updates: list[tuple[str, int, int | None, str | None]],
) -> None:
    """Batching survives queries settling at different horizons.

    Later passes read only the stops (and filters) of boards still filling
    and freeze the boards already settled; that must never change a board.
    """
    message = _rt_message(_trip_ids(zip_bytes), updates)

    async def body(handle: TransitFeedHandle, index: StaticIndex) -> None:
        anchor_stop = _anchor(index)[1][0]
        queries = [
            ArrivalsQuery(
                sorted({*_stop_choices(index, picks), anchor_stop})
                if anchored
                else _stop_choices(index, picks),
                limit=limit,
            )
            for picks, limit, anchored in query_picks
        ]
        for method in (handle.get_arrivals, handle.get_scheduled_departures):
            batch = await method(queries, start=start)
            alone = [(await method([q], start=start))[0] for q in queries]
            assert [[_arrival_key(r) for r in b] for b in batch] == [
                [_arrival_key(r) for r in a] for a in alone
            ]
        event(f"distinct board sizes: {len({len(b) for b in batch})}")

    _run(zip_bytes, body, message)


@_SETTINGS
@given(zip_bytes=_random_gtfs_zip(), at=_STARTS)
def test_current_service_dates_match_their_definition(
    zip_bytes: bytes, at: datetime
) -> None:
    """The run a date-less update addresses, against a brute-force oracle.

    Definition: the earliest service day D, no later than ``at``'s local
    date, on which the trip's service runs and whose last stop time (over
    the whole trip) is at or after ``at``. Never a later day.
    """
    try:
        index = _index_from_zip_bytes(zip_bytes)
    except FeedParseError:
        reject()
    try:
        trips = index._conn.execute(
            "SELECT t.id, t.service_id, MAX(MAX(COALESCE(st.arrival_secs, -1),"
            " COALESCE(st.departure_secs, -1))) FROM trips t"
            " JOIN stop_times st ON st.trip_id = t.id GROUP BY t.id"
        ).fetchall()
        got = index.current_service_dates([trip_id for trip_id, _, _ in trips], at)
        tz = index._tz
        local_today = at.astimezone(tz).date()
        for trip_id, service_id, end_secs in trips:
            expected = None
            if end_secs is not None and end_secs >= 0:
                for back in range(end_secs // 86_400 + 1, -1, -1):
                    day = local_today - timedelta(days=back)
                    anchor = (
                        datetime.combine(day, time(12), tzinfo=tz) - timedelta(hours=12)
                    ).astimezone(UTC)
                    if service_id in index.active_service_ids(day) and (
                        anchor + timedelta(seconds=end_secs) >= at
                    ):
                        expected = day
                        break
            assert got.get(trip_id) == expected, trip_id
            if expected is not None:
                assert expected <= local_today
        event(f"has a current run: {bool(got)}")
    finally:
        index.close()
