"""GTFS Realtime conformance: service alerts (scope, periods, text, defaults)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest
from google.transit import gtfs_realtime_pb2
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.models import (
    AlertCause,
    AlertEffect,
    AlertSeverity,
    ServiceAlert,
)
from aiomobilitydatabase.feeds.rt import alerts_from_message

_NOW = datetime(2026, 7, 30, 12, tzinfo=UTC)


def _alert(**fields: Any) -> ServiceAlert:
    """Parse one alert: ``selectors`` (dicts of EntitySelector fields, a
    ``trip`` as a dict of TripDescriptor fields), ``periods`` ((start, end)
    epoch pairs, None for absent), and any Alert scalar."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "a"
    alert = entity.alert
    alert.SetInParent()
    for selector in fields.pop("selectors", ()):
        informed = alert.informed_entity.add()
        for name, value in selector.items():
            if name == "trip":
                informed.trip.SetInParent()
                for trip_field, trip_value in value.items():
                    setattr(informed.trip, trip_field, trip_value)
            else:
                setattr(informed, name, value)
    for start, end in fields.pop("periods", ()):
        period = alert.active_period.add()
        if start is not None:
            period.start = start
        if end is not None:
            period.end = end
    for name, value in fields.items():
        setattr(alert, name, value)
    [parsed] = alerts_from_message(message)
    return parsed


# --- D11: each selector is an AND; the alert, an OR of them ------------------


def test_a_route_and_stop_selector_applies_to_the_route_at_the_stop_only() -> None:
    """EntitySelector: "in order for an alert to apply to an entity in GTFS
    it must match all of the provided EntitySelector fields"."""
    joint = _alert(selectors=[{"route_id": "R1", "stop_id": "S1"}])
    separate = _alert(selectors=[{"route_id": "R1"}, {"stop_id": "S1"}])
    assert joint.applies_to(route_id="R1", stop_id="S1")
    assert not joint.applies_to(route_id="R1", stop_id="S2")
    assert not joint.applies_to(route_id="R1")
    assert not joint.applies_to(stop_id="S1")
    # Two selectors: either one.
    assert separate.applies_to(route_id="R1", stop_id="S2")
    assert separate.applies_to(stop_id="S1")


def test_a_trip_instance_selector_applies_to_that_run_only() -> None:
    """The trip "must resolve to a single trip instance": a start_date
    narrows the trip to one day's run."""
    alert = _alert(selectors=[{"trip": {"trip_id": "T1", "start_date": "20260730"}}])
    assert alert.applies_to(trip_id="T1", start_date=date(2026, 7, 30))
    assert not alert.applies_to(trip_id="T1", start_date=date(2026, 7, 31))
    assert not alert.applies_to(trip_id="T1")  # which day is not known
    # The trip's own route and direction are implied by its id.
    assert _alert(selectors=[{"trip": {"trip_id": "T1", "route_id": "R1"}}]).applies_to(
        trip_id="T1"
    )


def test_a_trip_id_less_selector_names_its_run_and_is_never_feed_wide() -> None:
    """Without trip_id, "route_id, direction_id, start_time, and
    schedule_relationship=SCHEDULED must all be set to identify a trip
    instance": it applies to that run, and to nothing else."""
    alert = _alert(
        selectors=[
            {
                "trip": {
                    "route_id": "R1",
                    "direction_id": 0,
                    "start_time": "08:00:00",
                    "start_date": "20260730",
                }
            }
        ]
    )
    run = {
        "route_id": "R1",
        "direction_id": 0,
        "start_secs": 8 * 3600,
        "start_date": date(2026, 7, 30),
    }
    assert alert.applies_to(**run)  # type: ignore[arg-type]
    assert not alert.applies_to(**{**run, "direction_id": 1})  # type: ignore[arg-type]
    assert not alert.applies_to(trip_id="T9")
    assert not alert.applies_to()


@pytest.mark.parametrize(
    ("selector", "entity"),
    [
        # TripDescriptor.route_id "cannot be used within an Alert
        # EntitySelector to specify a route-wide alert".
        ({"trip": {"route_id": "R1"}}, {"route_id": "R1"}),
        (
            {"trip": {"route_id": "R1", "direction_id": 0}},
            {"route_id": "R1", "direction_id": 0},
        ),
        # No start_date: the 08:00 run of which day?
        (
            {"trip": {"route_id": "R1", "direction_id": 0, "start_time": "08:00:00"}},
            {"route_id": "R1", "direction_id": 0, "start_secs": 8 * 3600},
        ),
        # An empty trip names no run, so the stop does not stand alone.
        ({"stop_id": "S1", "trip": {}}, {"stop_id": "S1"}),
        # "If direction_id is provided, route_id must also be provided."
        ({"direction_id": 0}, {"route_id": "R1", "direction_id": 0}),
    ],
)
def test_a_selector_that_names_no_single_thing_is_not_widened(
    selector: dict[str, Any], entity: dict[str, Any]
) -> None:
    """A trip without trip_id must give route_id, direction_id, start_date
    and start_time to name one run; with fewer, the selector is left out
    rather than read as the route, direction or stop it does give."""
    alert = _alert(selectors=[selector])
    assert not alert.applies_to(**entity)
    assert alert.informed_entities == ()


def test_a_selector_with_a_malformed_start_names_nothing() -> None:
    """A start date that does not parse names a run nobody can tell: that
    selector is left out (it must not widen to every day), the others
    stay."""
    alert = _alert(
        selectors=[
            {"trip": {"trip_id": "T1", "start_date": "2026-07-30"}},
            {"stop_id": "S1"},
        ]
    )
    assert not alert.applies_to(trip_id="T1", start_date=date(2026, 7, 30))
    assert alert.applies_to(stop_id="S1")
    assert alert.trip_ids == ()


@pytest.mark.parametrize("start_time", ["8:00", "  ", "08:00:60"])
def test_a_selector_with_a_malformed_start_time_names_nothing(
    start_time: str,
) -> None:
    """Likewise a start time that does not parse: the selector must not
    widen to every run of the trip."""
    alert = _alert(selectors=[{"trip": {"trip_id": "T1", "start_time": start_time}}])
    assert alert.informed_entities == ()
    assert not alert.applies_to(trip_id="T1")


def test_the_index_lists_a_trip_runs_route_and_direction() -> None:
    """A trip without trip_id names its route and direction, so the index
    lists them; one with an id implies its own and lists only the id."""
    run = {
        "route_id": "R1",
        "direction_id": 0,
        "start_time": "08:00:00",
        "start_date": "20260730",
    }
    alert = _alert(
        selectors=[{"trip": run}, {"trip": {"trip_id": "T1", "route_id": "R2"}}]
    )
    assert (alert.route_ids, alert.direction_ids, alert.trip_ids) == (
        ("R1",),
        (0,),
        ("T1",),
    )


def test_an_alert_naming_nothing_applies_to_nothing() -> None:
    alert = _alert(selectors=[{}])
    assert not alert.applies_to()
    assert not alert.applies_to(route_id="R1", stop_id="S1", trip_id="T1")


_POOL = {
    "agency_id": st.sampled_from(["A1", "A2"]),
    "route_id": st.sampled_from(["R1", "R2"]),
    "route_type": st.sampled_from([0, 3]),
    "direction_id": st.sampled_from([0, 1]),
    "stop_id": st.sampled_from(["S1", "S2"]),
}
_TRIP_POOL = {
    "trip_id": st.sampled_from(["T1", "T2"]),
    "route_id": st.sampled_from(["R1", "R2"]),
    "direction_id": st.sampled_from([0, 1]),
    "start_date": st.sampled_from(["20260730", "20260731"]),
    "start_time": st.sampled_from(["08:00:00", "25:00:00"]),
}
_ENTITY_POOL = {
    **_POOL,
    "trip_id": st.sampled_from(["T1", "T2"]),
    "start_date": st.sampled_from([date(2026, 7, 30), date(2026, 7, 31)]),
    "start_secs": st.sampled_from([8 * 3600, 25 * 3600]),
}


@st.composite
def _fields(draw: st.DrawFn, pool: dict[str, Any]) -> dict[str, Any]:
    return {
        name: draw(strategy) for name, strategy in pool.items() if draw(st.booleans())
    }


@st.composite
def _selector(draw: st.DrawFn) -> dict[str, Any]:
    selector = draw(_fields(_POOL))
    if draw(st.booleans()):
        selector["trip"] = draw(_fields(_TRIP_POOL))
    return selector


def _secs(start_time: str) -> int:
    hours, minutes, seconds = (int(part) for part in start_time.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def _oracle(selector: dict[str, Any], entity: dict[str, Any]) -> bool:
    """The spec, restated: every field the selector provides must match the
    entity's; a trip with an id is that trip, one without it is the run of
    its route and direction; its start date and time pick the run."""
    if "direction_id" in selector and "route_id" not in selector:
        return False  # "route_id must also be provided"
    required = {name: selector[name] for name in _POOL if name in selector}
    trip = selector.get("trip")
    if trip is not None:
        run = ("route_id", "direction_id", "start_date", "start_time")
        if "trip_id" not in trip and not all(name in trip for name in run):
            return False  # not one run: names nothing
        if "trip_id" in trip:
            required["trip_id"] = trip["trip_id"]
        else:
            # A conflicting top-level value can never match both.
            for name in ("route_id", "direction_id"):
                if name in trip:
                    if name in required and required[name] != trip[name]:
                        return False
                    required[name] = trip[name]
        if "start_date" in trip:
            raw = trip["start_date"]
            required["start_date"] = date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
        if "start_time" in trip:
            required["start_secs"] = _secs(trip["start_time"])
    return bool(required) and all(
        entity.get(name) == value for name, value in required.items()
    )


@settings(max_examples=300, deadline=None)
@given(
    selectors=st.lists(_selector(), max_size=3),
    entity=_fields(_ENTITY_POOL),
)
def test_applies_to_is_the_or_of_each_selectors_and(
    selectors: list[dict[str, Any]], entity: dict[str, Any]
) -> None:
    """The alert applies where any selector does, and a selector where the
    entity matches every field it provides; a value the entity lacks
    matches no field that names one."""
    alert = _alert(selectors=selectors)
    expected = any(_oracle(selector, entity) for selector in selectors)
    assert alert.applies_to(**entity) is expected


# --- D10 / D26: TimeRange -----------------------------------------------------


def test_a_period_is_active_from_its_start_up_to_its_end() -> None:
    """TimeRange: "active at time t if t is greater than or equal to the
    start time and less than the end time"."""
    start, end = int(_NOW.timestamp()), int(_NOW.timestamp()) + 3600
    alert = _alert(periods=[(start, end)])
    assert alert.is_active(datetime.fromtimestamp(start, tz=UTC))
    assert not alert.is_active(datetime.fromtimestamp(end, tz=UTC))


@pytest.mark.parametrize(
    ("period", "active"),
    [
        ((0, 0), False),  # 1970, not "always"
        ((0, None), True),  # since 1970
        ((None, 0), False),  # ended in 1970
        ((2**63, None), False),  # starts past any datetime: not yet
        ((None, 2**63), True),  # ends past any datetime: not yet
        ((None, 253402300000), True),  # ends within days of the last datetime
        ((None, None), True),
    ],
)
def test_a_present_bound_is_an_instant_and_only_an_absent_one_is_infinite(
    period: tuple[int | None, int | None], active: bool
) -> None:
    """TimeRange start: "If missing, the interval starts at minus
    infinity"; end: "If missing, the interval ends at plus infinity". A
    present 0 is the epoch."""
    assert _alert(periods=[period]).is_active(_NOW) is active


# --- D13: TranslatedString ------------------------------------------------------


@pytest.mark.parametrize(
    ("translations", "expected"),
    [
        ([("Bonjour", "fr"), ("Hello", "")], "Hello"),
        ([("Hallo", "de"), ("Hi", "en-US")], "Hi"),
        ([("Hallo", "de"), ("", "en"), ("Hi", "")], "Hi"),
        ([("Hallo", "de"), ("Bonjour", "fr")], "Hallo"),
        ([("", "en")], None),
        ([("Hallo", "de"), ("Hi", "en_US")], "Hi"),  # common, if not BCP 47
        ([("Hallo", "de"), ("Hi", "eng")], "Hallo"),  # not English's subtag
    ],
)
def test_text_is_picked_in_the_spec_order(
    translations: list[tuple[str, str]], expected: str | None
) -> None:
    """The default UI language, English (an en-US tag is English), else
    "If some translation has an unspecified language code, that
    translation is picked", else the first; empty text is no text."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "a"
    for text, language in translations:
        entity.alert.header_text.translation.add(text=text, language=language)
    entity.alert.SetInParent()
    [alert] = alerts_from_message(message)
    assert alert.header == expected


# --- D27: declared defaults -----------------------------------------------------


def test_unset_cause_effect_and_severity_are_their_declared_defaults() -> None:
    """Best practice: "If hasX() returns false, the default value for that
    field defined in the gtfs-realtime.proto value should be assumed"."""
    alert = _alert()
    assert alert.cause is AlertCause.UNKNOWN_CAUSE
    assert alert.effect is AlertEffect.UNKNOWN_EFFECT
    assert alert.severity is AlertSeverity.UNKNOWN_SEVERITY
    stated = _alert(
        cause=gtfs_realtime_pb2.Alert.STRIKE,
        effect=gtfs_realtime_pb2.Alert.DETOUR,
        severity_level=gtfs_realtime_pb2.Alert.SEVERE,
    )
    assert (stated.cause, stated.effect, stated.severity) == (
        AlertCause.STRIKE,
        AlertEffect.DETOUR,
        AlertSeverity.SEVERE,
    )
