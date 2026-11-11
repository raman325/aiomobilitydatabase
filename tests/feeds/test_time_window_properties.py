"""Property tests for the start/end/limit window and incremental scanning.

The central laws: a dated query's answer does not depend on how it was
scanned. An open-ended query (``end=None``, ``limit=k``) returns exactly the
first ``k`` rows of the same query bounded beyond the end of service; a
bounded query with ``limit=k`` returns exactly the first ``k`` rows of the
unlimited one; a larger limit only appends; a batch equals its queries run
alone. Expanding horizons, per-query freezing, SQL pushdown and ceilings are
optimizations these laws say never change an answer.

They are checked over generated feeds in several timezones, with and
without frequency-based trips, under generated realtime: delays and
explicit times running arbitrarily early or late, cancellations, RT-added
trips, detours (to pop-up stops, with negative travel times and propagated
delays) and vehicles -- each dated or date-less, with or without a feed
timestamp. Which run a date-less update addresses is checked separately
against an oracle built from the raw feed files.
"""

import asyncio
import csv
import io
import zipfile
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2
from hypothesis import HealthCheck, event, given, reject, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds import transit as transit_module
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
    TransitFeedHandle,
    _board_settles,
    _DirectUrls,
    _effective_trip_departure,
    _horizons,
)

from tests.feeds.test_properties import _index_from_zip_bytes, _random_gtfs_zip

_SETTINGS = settings(
    max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)
_RT_SETTINGS = settings(
    max_examples=100, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)

# The generated calendars run 2026-01-01 .. 2027-12-31 with stop times up to
# 250:00:00 in UTC-11..UTC+11, detours add at most a few hours, so nothing is
# scheduled after this instant: bounded queries ending here are the
# reference an open-ended query must match.
_BEYOND_SERVICE = datetime(2028, 1, 13, tzinfo=UTC)

_STARTS = st.datetimes(
    min_value=datetime(2025, 12, 1),
    max_value=datetime(2028, 1, 15),
    timezones=st.just(UTC),
)
_HOUR = 3600
# Troll's DST jump is two hours: no fixed one-hour margin covers it.
_TIMEZONES = st.sampled_from(
    ["UTC", "America/New_York", "Australia/Sydney", "Antarctica/Troll"]
)


# -- feeds ----------------------------------------------------------------------


def _restamp(zip_bytes: bytes, timezone: str, *, frequencies: bool) -> bytes:
    """Move a generated feed into ``timezone``, maybe repeating its anchor."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as source:
        files = {name: source.read(name).decode() for name in source.namelist()}
    files["agency.txt"] = files["agency.txt"].replace(",UTC\n", f",{timezone}\n")
    if frequencies:
        anchor_trip = files["trips.txt"].splitlines()[1].split(",")[2]
        files["frequencies.txt"] = (
            "trip_id,start_time,end_time,headway_secs\n"
            f"{anchor_trip},06:00:00,09:00:00,3600\n"
        )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as target:
        for name, content in files.items():
            target.writestr(name, content)
    return buf.getvalue()


@st.composite
def _feed_variant(draw: st.DrawFn) -> bytes:
    """A generated feed in a drawn timezone, maybe with frequencies.

    New York and Sydney put DST transitions (in opposite seasons) inside
    the generated calendars; the frequency rule repeats the anchor trip so
    repetitions are matched by start_time.
    """
    return _restamp(
        draw(_random_gtfs_zip()), draw(_TIMEZONES), frequencies=draw(st.booleans())
    )


def _build(zip_bytes: bytes) -> StaticIndex:
    try:
        return _index_from_zip_bytes(zip_bytes)
    except FeedParseError:
        reject()  # a generated feed too malformed to index has no answers


def _anchor(index: StaticIndex) -> tuple[str, list[str]]:
    """The first stored call's producer trip id and the first two stops.

    ``_random_gtfs_zip`` guarantees the first trip's first two stop_times
    rows reference real ids, which is where non-empty answers come from.
    """
    rows = index._conn.execute(
        "SELECT t.source_trip_id, st.stop_id FROM stop_times st "
        "JOIN trips t ON t.id = st.trip_id ORDER BY st.rowid LIMIT 2"
    ).fetchall()
    if not rows:
        reject()
    return rows[0][0], [stop_id for _, stop_id in rows]


def _stop_choices(index: StaticIndex, draw_ids: Sequence[int]) -> list[str]:
    # POPUP: a stop only realtime detours define, served by no schedule.
    stop_ids = [*sorted({stop.id for stop in index.stops()}), "NOT-A-STOP", "POPUP"]
    return sorted({stop_ids[i % len(stop_ids)] for i in draw_ids})


async def _uncapped[T](
    handle: TransitFeedHandle, until: datetime, call: Callable[[], Awaitable[T]]
) -> T:
    """Run ``call`` as a plain read of its whole window.

    The reference a scan optimization is checked against must not share
    the optimization: every per-query ceiling is lifted to ``until`` and
    the window is read in ONE pass, not in slices, looking back two days
    past the feed's longest stop time, with no separate placement of
    long-reaching rows; the up-front read of the runs realtime addresses
    is switched off, so those rows come from the window read like any
    other; and detoured runs are rebuilt one by one on their own days, with
    no relevance filter and no shifting. A ceiling that wrongly cut rows, or an
    up-front read that added or lost any, shows up as a difference.
    """
    index = handle._index
    saved = (index.departures_ceiling, index.journeys_ceiling)
    index.departures_ceiling = lambda *_a, **_k: until  # type: ignore[method-assign]
    index.journeys_ceiling = lambda *_a, **_k: until  # type: ignore[method-assign]

    async def nothing_up_front(*_a: object) -> list[object]:
        return []

    def one_pass(
        start: datetime, end: datetime | None, ceiling: datetime | None
    ) -> Iterator[tuple[datetime, bool]]:
        # The whole window in ONE read, not in slices.
        yield list(_horizons(start, end, ceiling))[-1]

    # Every day back to the feed's longest stop time, plus two: reaches
    # every run whatever the clocks do -- no reasoning about day starts.
    longest = index._conn.execute(
        "SELECT MAX(MAX(COALESCE(arrival_secs, 0), COALESCE(departure_secs, 0))) "
        "FROM stop_times"
    ).fetchone()[0]
    look_back = timedelta(days=(longest or 0) // 86_400 + 2)

    def generous_look_back(window_start: datetime, local_today: date) -> date:
        return local_today - look_back

    def no_long_reach(*_a: object) -> set[date]:
        return set()

    async def naive_detours(
        modifications: Sequence[Any],
        wanted_stops: set[str],
        *,
        end: datetime | None,
        rt_stops: Any,
        stops_by_id: Any,
        routes_by_id: Any,
    ) -> tuple[list[StopArrival], set[tuple[str, date]]]:
        # Every named run, rebuilt on its own day: no relevance filter, no
        # template, no shifting.
        source_ids = sorted({trip for mod in modifications for trip in mod.trip_ids})
        concrete = index.concrete_trip_ids(source_ids)
        runs = {
            (trip_id, service_date)
            for mod in modifications
            for source_id in mod.trip_ids
            for trip_id in concrete.get(source_id, ())
            for service_date in mod.service_dates
        }
        rows: list[StopArrival] = []
        touched: set[tuple[str, date]] = set()
        for instance in index.instance_trip_calls(sorted(runs)):
            applicable = [
                modification
                for mod in modifications
                if instance.source_trip_id in mod.trip_ids
                and instance.service_date in mod.service_dates
                and (not mod.start_secs or instance.start_secs in mod.start_secs)
                for modification in mod.modifications
            ]
            if not applicable:
                continue
            calls = transit_module._modified_calls(
                instance, applicable, rt_stops, stops_by_id
            )
            if calls == instance.calls:
                continue
            touched.add((instance.trip_id, instance.service_date))
            rows.extend(
                transit_module._detoured_rows(
                    instance,
                    calls,
                    timedelta(0),
                    wanted_stops=wanted_stops,
                    end=None,  # the reference applies ``end`` itself
                    rt_stops=rt_stops,
                    stops_by_id=stops_by_id,
                    route=routes_by_id.get(instance.route_id),
                )
            )
        rows = [
            row
            for row in rows
            if end is None
            or (row.scheduled_departure is not None and row.scheduled_departure <= end)
        ]
        return rows, touched

    handle._addressed_runs = nothing_up_front  # type: ignore[method-assign]
    handle._apply_trip_modifications = naive_detours  # type: ignore[method-assign]
    index._scan_start = generous_look_back  # type: ignore[method-assign]
    index._long_reach_days = no_long_reach  # type: ignore[method-assign]
    transit_module._horizons = one_pass
    try:
        return await call()
    finally:
        index.departures_ceiling, index.journeys_ceiling = saved
        transit_module._horizons = _horizons
        del handle._addressed_runs
        del handle._apply_trip_modifications
        del index._scan_start
        del index._long_reach_days


def _trip_ids(index: StaticIndex) -> list[str]:
    """Producer-facing trip ids, the anchor's first."""
    anchor = _anchor(index)[0]
    rows = index._conn.execute("SELECT DISTINCT source_trip_id FROM trips")
    return [anchor, *sorted(row[0] for row in rows if row[0] != anchor)]


# -- realtime -------------------------------------------------------------------

_DATE = st.one_of(
    st.none(),
    # The run the example starts near, or the next two days' runs -- which
    # lie beyond the first horizons, where an early prediction matters.
    st.sampled_from(["RUN_DAY", "RUN_DAY+1", "RUN_DAY+2"]),
    st.dates(min_value=date(2025, 12, 28), max_value=date(2028, 1, 3)),
)
_START_TIME = st.sampled_from([None, None, "06:00:00", "07:00:00"])
_SHIFT = st.integers(-6 * _HOUR, 6 * _HOUR)
_ENTITY = st.one_of(
    st.tuples(st.just("trip_delay"), st.integers(0, 5), _SHIFT, _DATE, _START_TIME),
    st.tuples(
        st.just("stop_delay"), st.integers(0, 5), st.integers(0, 3), _SHIFT, _DATE
    ),
    st.tuples(
        st.just("stop_time"),
        st.integers(0, 5),
        st.integers(0, 3),
        # GARBAGE_TIME: an explicit time of epoch 1 -- valid, absurdly early.
        st.one_of(st.integers(-6 * _HOUR, 30 * _HOUR), st.just("GARBAGE_TIME")),
        _DATE,
    ),
    st.tuples(st.just("cancel"), st.integers(0, 5), _DATE, _START_TIME),
    st.tuples(
        st.just("added"),
        st.integers(0, 4),
        st.integers(-_HOUR, 30 * _HOUR),
        _DATE,
        st.booleans(),
    ),
    st.tuples(
        st.just("detour"),
        st.integers(0, 5),
        # One or more modifications on the trip: (selected stop sequence,
        # replacements -- possibly none, which the spec allows -- delay).
        st.lists(
            st.tuples(
                st.integers(0, 3),
                st.lists(
                    st.tuples(st.integers(0, 4), st.integers(-30 * 60, 2 * _HOUR)),
                    max_size=2,
                ),
                st.integers(-_HOUR, 2 * _HOUR),
            ),
            min_size=1,
            max_size=3,
        ),
        # One service date, or several (the spec allows a week's worth).
        st.one_of(_DATE, st.tuples(_DATE, _DATE, _DATE)),
        _START_TIME,
    ),
    st.tuples(st.just("vehicle"), st.integers(0, 5), _DATE, _START_TIME),
)
# (entities, the feed's header timestamp: mostly the query's start, as a
# live feed is; sometimes hours behind or ahead of it (a board for later,
# a stale feed), absurd (year 9999), or absent -- the spec requires one,
# so that exercises the fallback)
_REALTIME = st.tuples(
    st.lists(_ENTITY, min_size=2, max_size=8),
    st.sampled_from(["start", "start", "start", "behind", "ahead", "garbage", None]),
)

# Most realtime examples start shortly before the anchor's run on some
# day, so date-less updates have a current run and dated ones a match;
# otherwise most starts fall nowhere near a run and realtime binds nothing.
_NEAR = st.tuples(
    st.one_of(
        # Around DST changes (New York and Sydney), where a day is 23 or 25
        # hours and a time past 24:00:00 lands an hour off.
        st.sampled_from(
            [
                date(2026, 3, 7),
                date(2026, 3, 8),
                date(2026, 3, 28),
                date(2026, 3, 29),
                date(2026, 3, 9),
                date(2026, 4, 4),
                date(2026, 10, 4),
                date(2026, 10, 31),
                date(2026, 11, 1),
                date(2027, 3, 14),
            ]
        ),
        st.dates(min_value=date(2026, 1, 1), max_value=date(2027, 12, 31)),
    ),
    st.integers(0, 150),
)
# Weights are drawn explicitly: st.one_of collapses identical branches, and
# Hypothesis favours small integers and early list elements, so the cases
# that matter come first.
_NEAR_RUN = st.sampled_from([True, True, True, False]).flatmap(
    lambda near: _NEAR if near else st.none()
)

# The entity a near-run example leads with: shapes that independent
# sampling would almost never line up on the anchor's own runs.
_RUN_DAYS = st.sampled_from([None, "RUN_DAY", "RUN_DAY+1"])
_EARLY_TOMORROW = st.tuples(
    # Tomorrow's run predicted at the anchor stop to depart between start
    # and the first horizon -- hours early, in a slice not yet read, yet it
    # must lead the board. (Predicted before start, every mode drops it.)
    st.just("early_tomorrow"),
    st.integers(0, 2 * _HOUR),
)
_DETOUR = st.tuples(
    # A detour of the anchor's run through a stop no schedule serves --
    # always dated (an undated one is invalid and changes nothing), and
    # mostly today's run, after start, where the board can show it.
    # (Negative travel times come from the general detour entities.)
    st.just("detour"),
    st.integers(0, 3 * _HOUR),
    st.integers(-_HOUR, _HOUR),
    st.sampled_from(["RUN_DAY", "RUN_DAY", "RUN_DAY+1"]),
)
# Trips ignore detours, so their lead is an early or shifted run; usually
# one (a lead is what makes the early-tomorrow case reachable at all).
_TRIP_LEAD = st.sampled_from(["early", "early", "shift", "none"]).flatmap(
    lambda kind: {
        "shift": st.tuples(st.just("shift"), _SHIFT, _RUN_DAYS),
        "early": _EARLY_TOMORROW,
        "none": st.none(),
    }[kind]
)
# Two entities detouring the anchor's runs on consecutive days, differently:
# each run must get the modifications naming ITS day.
_DETOUR_PAIR = st.tuples(
    st.just("detour_pair"),
    st.integers(-_HOUR, 2 * _HOUR),
    st.integers(-_HOUR, 2 * _HOUR),
)
# Hypothesis favours early elements, so the rarest shapes lead the list.
_LEAD = st.sampled_from(
    ["detour_pair", "detour", "early", "detour_pair", "detour", "early", "shift"]
).flatmap(
    lambda kind: {
        "shift": st.tuples(st.just("shift"), _SHIFT, _RUN_DAYS),
        "early": _EARLY_TOMORROW,
        "detour": _DETOUR,
        "detour_pair": _DETOUR_PAIR,
    }[kind]
)


def _with_lead(
    index: StaticIndex,
    entities: list[tuple[Any, ...]],
    lead: tuple[Any, ...] | None,
) -> tuple[list[tuple[Any, ...]], list[str]]:
    """Prepend the lead entity; return the stops it needs queried."""
    if lead is None:
        return entities, []
    anchor_stop = _anchor(index)[1][0]
    sequence = index._conn.execute(
        "SELECT stop_sequence FROM stop_times ORDER BY rowid LIMIT 1"
    ).fetchone()[0]
    if lead[0] == "shift":
        spec: tuple[Any, ...] = ("trip_delay", 0, lead[1], lead[2], None)
        stops = [anchor_stop]
    elif lead[0] == "early_tomorrow":
        spec = ("stop_time", 0, sequence, lead[1], "RUN_DAY+1")
        stops = [anchor_stop]
    elif lead[0] == "detour_pair":
        _, first, second = lead
        pair = [
            ("detour", 0, [(sequence, [(0, 60)], delay)], day, None)
            for delay, day in ((first, "RUN_DAY"), (second, "RUN_DAY+1"))
        ]
        return [*pair, *entities], [anchor_stop, "POPUP"]
    else:
        _, travel, delay, run_day = lead
        spec = ("detour", 0, [(sequence, [(0, travel)], delay)], run_day, None)
        stops = [anchor_stop, "POPUP"]
    return [spec, *entities], stops


def _near_run_start(
    index: StaticIndex, start: datetime, near: tuple[date, int] | None
) -> tuple[datetime, date | None]:
    """``start``, or an instant ``minutes`` before the anchor departs on a day.

    The day is the first on or after the drawn one when the anchor's
    service actually runs -- a run realtime can name, rather than a day
    where every "RUN_DAY" entity would address nothing.
    """
    if near is None:
        return start, None
    day, minutes = near
    row = index._conn.execute(
        "SELECT st.departure_secs, t.service_id FROM stop_times st "
        "JOIN trips t ON t.id = st.trip_id ORDER BY st.rowid LIMIT 1"
    ).fetchone()
    secs = row[0] if row else None
    if secs is None:
        return start, None
    for offset in range(14):
        if row[1] in index.active_service_ids(day + timedelta(days=offset)):
            day += timedelta(days=offset)
            break
    anchor = index._service_day_start(day).astimezone(UTC)
    return anchor + timedelta(seconds=secs, minutes=-minutes), day


def _rt_message(  # noqa: PLR0912, PLR0915 -- a branch per generated entity kind
    index: StaticIndex,
    realtime: tuple[list[tuple[Any, ...]], str | None],
    start: datetime,
    run_day: date | None,
) -> gtfs_realtime_pb2.FeedMessage:
    """Build one FeedMessage from generated entity specs."""
    entities, stamped = realtime
    trip_ids = _trip_ids(index)
    stop_ids = sorted({stop.id for stop in index.stops()}) or ["S"]

    def trip(i: int) -> str:
        return trip_ids[i % len(trip_ids)]

    def day(choice: date | str | None) -> str | None:
        if isinstance(choice, str):
            offset = int(choice.removeprefix("RUN_DAY") or 0)
            choice = run_day + timedelta(days=offset) if run_day else None
        return choice.strftime("%Y%m%d") if isinstance(choice, date) else None

    def at(offset: int) -> int:
        return int((start + timedelta(seconds=offset)).timestamp())

    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    if stamped is not None:
        message.header.timestamp = {
            "start": int(start.timestamp()),
            "behind": int(start.timestamp()) - 3 * _HOUR,
            "ahead": int(start.timestamp()) + _HOUR,
            "garbage": 253_402_290_000,  # 9999-12-31T23:00Z: representable
        }[stamped]
    # Detours may route through POPUP, a stop only this feed defines (an
    # undefined replacement stop is unplaceable and skipped).
    popup = message.entity.add()
    popup.id = "popup-stop"
    popup.stop.stop_id = "POPUP"
    popup.stop.stop_lat = 34.0
    popup.stop.stop_lon = -118.0
    for n, spec in enumerate(entities):
        entity = message.entity.add()
        entity.id = f"e{n}"
        kind = spec[0]
        if kind == "vehicle":
            _, i, date_choice, start_time = spec
            vehicle = entity.vehicle
            vehicle.vehicle.id = f"V{n}"
            vehicle.trip.trip_id = trip(i)
            if (start_date := day(date_choice)) is not None:
                vehicle.trip.start_date = start_date
            if start_time is not None:
                vehicle.trip.start_time = start_time
            vehicle.position.latitude = 34.0
            vehicle.position.longitude = -118.0
        elif kind == "detour":
            _, i, modifications, date_choice, start_time = spec
            mods = entity.trip_modifications
            mods.selected_trips.add().trip_ids.append(trip(i))
            choices = date_choice if isinstance(date_choice, tuple) else (date_choice,)
            for choice in choices:
                if (service_date := day(choice)) is not None:
                    mods.service_dates.append(service_date)
            if start_time is not None:
                mods.start_times.append(start_time)
            for sequence, replacements, delay in modifications:
                mod = mods.modifications.add()
                mod.start_stop_selector.stop_sequence = sequence
                mod.end_stop_selector.stop_sequence = sequence
                mod.propagated_modification_delay = delay
                for stop_pick, travel in replacements:
                    stop = mod.replacement_stops.add()
                    stop.stop_id = (
                        "POPUP"
                        if stop_pick == 0
                        else stop_ids[stop_pick % len(stop_ids)]
                    )
                    stop.travel_time_to_stop = travel
        elif kind == "added":
            _, stop_pick, offset, date_choice, with_vehicle = spec
            update = entity.trip_update
            update.trip.trip_id = f"ADD{n}"
            update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.ADDED
            if (start_date := day(date_choice)) is not None:
                update.trip.start_date = start_date
            stu = update.stop_time_update.add()
            stu.stop_id = stop_ids[stop_pick % len(stop_ids)]
            stu.departure.time = at(offset)
            if with_vehicle:
                update.vehicle.id = f"AV{n}"
                position = message.entity.add()
                position.id = f"e{n}-vp"
                position.vehicle.vehicle.id = f"AV{n}"
                position.vehicle.trip.trip_id = f"ADD{n}"
                if start_date is not None:
                    position.vehicle.trip.start_date = start_date
                position.vehicle.position.latitude = 34.0
                position.vehicle.position.longitude = -118.0
        else:
            update = entity.trip_update
            update.trip.trip_id = trip(spec[1])
            start_time = None
            if kind == "trip_delay":
                _, _, delay, date_choice, start_time = spec
                update.delay = delay
            elif kind == "stop_delay":
                _, _, sequence, delay, date_choice = spec
                stu = update.stop_time_update.add()
                stu.stop_sequence = sequence
                stu.departure.delay = delay
            elif kind == "stop_time":
                _, _, sequence, offset, date_choice = spec
                stu = update.stop_time_update.add()
                stu.stop_sequence = sequence
                stu.departure.time = 1 if offset == "GARBAGE_TIME" else at(offset)
            else:  # cancel
                _, _, date_choice, start_time = spec
                update.trip.schedule_relationship = (
                    gtfs_realtime_pb2.TripDescriptor.CANCELED
                )
            if (start_date := day(date_choice)) is not None:
                update.trip.start_date = start_date
            if start_time is not None:
                update.trip.start_time = start_time
    return message


def _run[T](
    index: StaticIndex,
    body: Callable[[TransitFeedHandle], Awaitable[T]],
    message: gtfs_realtime_pb2.FeedMessage | None = None,
) -> T:
    """Run ``body`` against a handle over ``index``, with no network.

    With ``message``, every realtime fetch (TripUpdates and vehicles alike)
    returns it; otherwise the handle has no realtime sources.
    """

    async def scenario() -> T:
        async with MobilityFeedsClient() as client:
            handle = TransitFeedHandle(
                client,
                None,
                [],
                index,
                None,
                direct=_DirectUrls(static_url="https://e.com/g.zip", headers=None),
            )
            if message is not None:

                async def fetch(*_args: object, **_kwargs: object) -> list[object]:
                    return [message]

                handle._fetch_entity_messages = fetch  # type: ignore[method-assign]
            return await body(handle)

    try:
        return asyncio.run(scenario())
    finally:
        index.close()


def _arrival_key(row: StopArrival) -> tuple[object, ...]:
    return (
        row.trip_id,
        row.stop_id,
        row.scheduled_departure,
        row.predicted_departure,
        row.predicted_arrival,
        row.realtime,
        row.vehicle.vehicle_id if row.vehicle else None,
    )


def _trip_key(row: UpcomingTrip) -> tuple[object, ...]:
    return (
        row.trip_id,
        row.scheduled_departure,
        row.predicted_departure,
        row.vehicle.vehicle_id if row.vehicle else None,
    )


def _keys(boards: Sequence[Sequence[StopArrival]]) -> list[list[tuple[object, ...]]]:
    return [[_arrival_key(row) for row in board] for board in boards]


# -- the helpers themselves -----------------------------------------------------


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
    far = start + timedelta(days=900)
    assert list(_horizons(start, far, ceiling))[-1] == (ceiling, True)
    assert list(_horizons(start, week, None)) == [(start, True)]
    assert list(_horizons(start, None, None)) == [(start, True)]


def test_board_settles_rules() -> None:
    horizon = datetime(2026, 7, 30, 12, tzinfo=UTC)
    inside = horizon - timedelta(hours=2)
    # Full with its last row inside (or at) the horizon: final.
    assert _board_settles([inside], 1, horizon)
    assert _board_settles([horizon], 1, horizon)
    assert _board_settles([], 0, horizon)
    # Room left, unlimited, or a delayed last row past the horizon: an
    # unread row at its scheduled time could still sort ahead.
    assert not _board_settles([inside], 2, horizon)
    assert not _board_settles([inside], None, horizon)
    assert not _board_settles([horizon + timedelta(seconds=1)], 1, horizon)


# -- scan independence ----------------------------------------------------------


@_SETTINGS
@given(
    zip_bytes=_feed_variant(),
    start=_STARTS,
    stop_picks=st.lists(st.integers(0, 10), min_size=1, max_size=3),
    limit=st.integers(0, 5),
    use_anchor=st.sampled_from([True, True, False]),
    near=_NEAR_RUN,
    hours=st.integers(1, 96),
)
def test_scheduled_departures_do_not_depend_on_how_they_were_scanned(
    *,
    zip_bytes: bytes,
    start: datetime,
    stop_picks: list[int],
    limit: int,
    use_anchor: bool,
    near: tuple[date, int] | None,
    hours: int,
) -> None:
    index = _build(zip_bytes)
    start, _ = _near_run_start(index, start, near)
    stops = _stop_choices(index, stop_picks)
    if use_anchor:
        stops = sorted({*stops, _anchor(index)[1][0]})

    async def body(handle: TransitFeedHandle) -> None:
        async def board(limit: int | None, end: datetime | None) -> list[StopArrival]:
            [rows] = await handle.get_scheduled_departures(
                [ArrivalsQuery(stops, limit=limit)], start=start, end=end
            )
            return rows

        reference = max(_BEYOND_SERVICE, start)
        whole = await _uncapped(handle, reference, lambda: board(None, reference))
        open_ended = await board(limit, None)
        assert _keys([open_ended]) == _keys([whole[:limit]])
        assert _keys([(await board(limit + 2, None))[:limit]]) == _keys([open_ended])
        end = start + timedelta(hours=hours)
        all_rows = await _uncapped(handle, end, lambda: board(None, end))
        assert _keys([await board(None, end)]) == _keys([all_rows])
        assert _keys([await board(limit, end)]) == _keys([all_rows[:limit]])
        assert all(
            row.scheduled_departure is not None
            and start <= row.scheduled_departure <= end
            for row in all_rows
        )
        event(f"scheduled rows: {min(len(open_ended), 3)}")

    _run(index, body)


@settings(_RT_SETTINGS, max_examples=300)
@given(
    zip_bytes=_feed_variant(),
    start=_STARTS,
    stop_picks=st.lists(st.integers(0, 10), min_size=1, max_size=3),
    limit=st.integers(1, 5),
    realtime=_REALTIME,
    use_anchor=st.booleans(),
    near=_NEAR_RUN,
    lead=_LEAD,
    hours=st.integers(1, 96),
)
def test_arrivals_do_not_depend_on_how_they_were_scanned(
    *,
    zip_bytes: bytes,
    start: datetime,
    stop_picks: list[int],
    limit: int,
    realtime: tuple[list[tuple[Any, ...]], str | None],
    use_anchor: bool,
    near: tuple[date, int] | None,
    lead: tuple[Any, ...],
    hours: int,
) -> None:
    """Open-ended = whole window; bounded+limit = bounded, truncated; larger
    limits only append -- under any generated realtime, vehicles included.

    "Whole window" is read with every ceiling lifted, so it shares none of
    the scan's optimizations: a ceiling or margin that dropped a row (a
    detour past the schedule, a run predicted hours early) shows up.
    """
    index = _build(zip_bytes)
    start, run_day = _near_run_start(index, start, near)
    entities, stamped = realtime
    entities, lead_stops = _with_lead(index, entities, lead if near else None)
    message = _rt_message(index, (entities, stamped), start, run_day)
    stops = sorted({*_stop_choices(index, stop_picks), *lead_stops})
    if use_anchor:
        stops = sorted({*stops, _anchor(index)[1][0]})

    async def body(handle: TransitFeedHandle) -> None:
        async def board(limit: int | None, end: datetime | None) -> list[StopArrival]:
            [rows] = await handle.get_arrivals(
                [ArrivalsQuery(stops, limit=limit)],
                start=start,
                end=end,
                with_vehicles=True,
            )
            return rows

        reference = max(_BEYOND_SERVICE, start)
        whole = await _uncapped(handle, reference, lambda: board(None, reference))
        open_ended = await board(limit, None)
        assert _keys([open_ended]) == _keys([whole[:limit]])
        assert _keys([(await board(limit + 2, None))[:limit]]) == _keys([open_ended])
        end = start + timedelta(hours=hours)
        window = await _uncapped(handle, end, lambda: board(None, end))
        assert _keys([await board(limit, end)]) == _keys([window[:limit]])
        assert _keys([await board(None, end)]) == _keys([window])
        event(f"rows: {min(len(open_ended), 3)}")
        popups = [r for r in whole if r.stop_id == "POPUP"]
        event(f"popup rows: {min(len(popups), 2)}")
        event(
            "popup rows in the compared boards: "
            f"{min(sum(r.stop_id == 'POPUP' for r in (*whole[:limit], *window)), 2)}"
        )
        event(f"lead: {lead[0] if near else None}")
        event(f"predicted: {any(r.predicted_departure for r in open_ended)}")
        event(f"vehicle: {any(r.vehicle for r in open_ended)}")
        event(f"detour in feed: {any(e[0] == 'detour' for e in entities)}")

    _run(index, body, message)


@settings(_RT_SETTINGS, max_examples=400)
@given(
    zip_bytes=_feed_variant(),
    start=_STARTS,
    pair_picks=st.tuples(st.integers(0, 10), st.integers(0, 10)),
    limit=st.integers(0, 4),
    use_anchor=st.sampled_from([True, True, True, False]),
    realtime=_REALTIME,
    near=_NEAR_RUN,
    lead=_TRIP_LEAD,
    hours=st.integers(1, 96),
)
def test_upcoming_trips_do_not_depend_on_how_they_were_scanned(
    *,
    zip_bytes: bytes,
    start: datetime,
    pair_picks: tuple[int, int],
    limit: int,
    use_anchor: bool,
    realtime: tuple[list[tuple[Any, ...]], str | None],
    near: tuple[date, int] | None,
    lead: tuple[Any, ...] | None,
    hours: int,
) -> None:
    index = _build(zip_bytes)
    start, run_day = _near_run_start(index, start, near)
    if lead is not None and lead[0] == "early_tomorrow" and near is not None:
        # Predict tomorrow's run to leave just before today's, which the
        # start sits ``near[1]`` minutes ahead of: the early run must lead
        # a board whose first horizon already holds today's.
        lead = ("early_tomorrow", lead[1] % max(1, near[1] * 60))
    entities, _ = _with_lead(index, realtime[0], lead)
    message = _rt_message(index, (entities, realtime[1]), start, run_day)
    stop_ids = sorted({stop.id for stop in index.stops()})
    origin = stop_ids[pair_picks[0] % len(stop_ids)]
    destination = stop_ids[pair_picks[1] % len(stop_ids)]
    anchor_stops = _anchor(index)[1]
    if use_anchor and len(anchor_stops) == 2:
        origin, destination = anchor_stops

    async def body(handle: TransitFeedHandle) -> None:
        async def trips(limit: int | None, end: datetime | None) -> list[UpcomingTrip]:
            [rows] = await handle.get_upcoming_trips(
                [TripQuery(origin, destination, limit=limit)],
                start=start,
                end=end,
                with_vehicles=True,
            )
            return rows

        def keys(rows: list[UpcomingTrip]) -> list[tuple[object, ...]]:
            return [_trip_key(row) for row in rows]

        reference = max(_BEYOND_SERVICE, start)
        whole = await _uncapped(handle, reference, lambda: trips(None, reference))
        open_ended = await trips(limit, None)
        assert keys(open_ended) == keys(whole[:limit])
        end = start + timedelta(hours=hours)
        window = await _uncapped(handle, end, lambda: trips(None, end))
        assert keys(await trips(limit, end)) == keys(window[:limit])
        event(f"trip rows: {min(len(open_ended), 3)}")
        leading = whole[:1]
        event(
            "a run beyond the first horizon leads, predicted early"
            if leading
            and leading[0].scheduled_departure > start + timedelta(hours=2)
            and _effective_trip_departure(leading[0]) < leading[0].scheduled_departure
            else "no early lead"
        )

    _run(index, body, message)


@_RT_SETTINGS
@given(
    zip_bytes=_feed_variant(),
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
    realtime=_REALTIME,
    near=_NEAR_RUN,
    lead=_LEAD,
    hours=st.one_of(st.none(), st.integers(1, 72)),
)
def test_a_batch_equals_its_queries_run_alone(
    *,
    zip_bytes: bytes,
    start: datetime,
    query_picks: list[tuple[list[int], int, bool]],
    realtime: tuple[list[tuple[Any, ...]], str | None],
    near: tuple[date, int] | None,
    lead: tuple[Any, ...],
    hours: int | None,
) -> None:
    """Batching survives queries settling at different horizons.

    Later passes read only the stops and filters of boards still filling
    and freeze settled boards; detours widen every query's ceiling. None of
    that may change a board.
    """
    index = _build(zip_bytes)
    start, run_day = _near_run_start(index, start, near)
    entities, stamped = realtime
    entities, lead_stops = _with_lead(index, entities, lead if near else None)
    message = _rt_message(index, (entities, stamped), start, run_day)
    queries = [
        ArrivalsQuery(
            sorted({*_stop_choices(index, picks), *lead_stops})
            if anchored
            else _stop_choices(index, picks),
            limit=limit,
        )
        for picks, limit, anchored in query_picks
    ]
    end = None if hours is None else start + timedelta(hours=hours)

    async def body(handle: TransitFeedHandle) -> None:
        for method in (handle.get_arrivals, handle.get_scheduled_departures):
            batch = await method(queries, start=start, end=end)
            alone = [(await method([q], start=start, end=end))[0] for q in queries]
            assert _keys(batch) == _keys(alone)
        event(f"distinct board sizes: {len({len(b) for b in batch})}")

    _run(index, body, message)


# -- which run a date-less update addresses -------------------------------------


def _secs(cell: str) -> int | None:
    """An independent GTFS time parser: H:MM:SS, hours past 24 allowed."""
    parts = cell.strip().split(":")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        return None
    hours, minutes, seconds = (int(part) for part in parts)
    return hours * 3600 + minutes * 60 + seconds


# A day start on a DST change (New York, Sydney, Troll) and minutes from
# it: where "today" is not the local date of ``now`` -- a spring-forward day
# starts the evening before, a fall-back day after midnight.
_NEAR_DAY_START = st.tuples(
    st.sampled_from(
        [
            date(2026, 3, 8),
            date(2026, 3, 29),
            date(2026, 4, 5),
            date(2026, 10, 4),
            date(2026, 10, 25),
            date(2026, 11, 1),
        ]
    ),
    st.integers(-180, 180),
)


def _active_days(
    index: StaticIndex,
    service_id: str,
    first: date,
    last: date,
    active: dict[date, set[str]],
) -> list[date]:
    """Every day from ``first`` to ``last`` on which ``service_id`` runs."""
    days = []
    for offset in range((last - first).days + 1):
        day = first + timedelta(days=offset)
        if day not in active:
            active[day] = index.active_service_ids(day)
        if service_id in active[day]:
            days.append(day)
    return days


def _expected_current(
    runs: list[tuple[date, datetime, datetime]], now: datetime
) -> date | None:
    """The definition's choice among a trip's runs: the nearest to ``now``.

    A run's distance is zero while under way, else the gap to its first
    stop (still to come) or from its last (ended); two equally near -- two
    under way at once -- collide, and none is meant.
    """
    distances = sorted(
        (max(first - now, now - last, timedelta(0)), day, first > now)
        for day, first, last in runs
    )
    if not distances:
        return None
    if len(distances) > 1 and distances[0][0] == distances[1][0]:
        event("equally near runs collide: none meant")
        return None
    distance, day, to_come = distances[0]
    event(
        "meant: under way"
        if not distance
        else ("meant: still to come" if to_come else "meant: ended")
    )
    return day


@settings(_SETTINGS, max_examples=300)
@given(
    zip_bytes=_random_gtfs_zip(),
    now=_STARTS,
    near=st.one_of(_NEAR_DAY_START, st.none()),
    timezone=_TIMEZONES,
)
def test_current_service_dates_match_their_definition(
    zip_bytes: bytes,
    now: datetime,
    near: tuple[date, int] | None,
    timezone: str,
) -> None:
    """The run a date-less update addresses, from the raw files.

    Definition: a trip whose stop times span more than a day names no run
    (GTFS-RT requires start_date). Otherwise "today" is the service day
    ``now`` falls in -- the latest day whose start (local noon minus 12
    elapsed hours) is at or before ``now``, found by trying every date
    around ``now`` -- and among the trip's runs on every day from the
    feed's first service day to today on which its service runs, the one
    meant is the nearest to ``now`` (see :func:`_expected_current`).
    Computed from the zip's own CSV with an independent time parser and
    anchor, by brute force over the days; only service activity and the
    feed's first day (tested elsewhere) come from the index.
    """
    restamped = _restamp(zip_bytes, timezone, frequencies=False)
    index = _build(restamped)
    try:
        with zipfile.ZipFile(io.BytesIO(restamped)) as source:
            trips_csv = source.read("trips.txt").decode()
            stop_times_csv = source.read("stop_times.txt").decode()
        service_of = {
            row["trip_id"]: row["service_id"]
            for row in csv.DictReader(io.StringIO(trips_csv))
        }
        spans: dict[str, tuple[int, int]] = {}
        for row in csv.DictReader(io.StringIO(stop_times_csv)):
            for cell in (row["arrival_time"], row["departure_time"]):
                secs = _secs(cell)
                if secs is not None and row["trip_id"] in service_of:
                    low, high = spans.get(row["trip_id"], (secs, secs))
                    spans[row["trip_id"]] = (min(low, secs), max(high, secs))
        tz = ZoneInfo(timezone)

        def anchor(day: date) -> datetime:
            noon = datetime.combine(day, time(12), tzinfo=tz).astimezone(UTC)
            return noon - timedelta(hours=12)

        if near is not None:
            now = anchor(near[0]) + timedelta(minutes=near[1])
        got = index.current_service_dates(sorted(service_of), now)
        local = now.astimezone(tz).date()
        today = max(
            day
            for day in (local + timedelta(days=k) for k in range(-3, 4))
            if anchor(day) <= now
        )
        if today != local:
            event("today is not the local date")
        first = index._first_service_date()
        active: dict[date, set[str]] = {}
        for trip_id, service_id in service_of.items():
            expected = None
            if trip_id in spans and first is not None:
                low, high = spans[trip_id]
                if high - low > 86_400:
                    event("longer than a day: names no run")
                else:
                    days = _active_days(index, service_id, first, today, active)
                    runs = [
                        (
                            day,
                            anchor(day) + timedelta(seconds=low),
                            anchor(day) + timedelta(seconds=high),
                        )
                        for day in days
                    ]
                    expected = _expected_current(runs, now)
            assert got.get(trip_id) == expected, trip_id
        event(f"has a run meant: {bool(got)}")
    finally:
        index.close()
