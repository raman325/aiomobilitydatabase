"""Grouping boarding stops into logical stations for presentation."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.models import StationGroup, Stop, StopLocationType
from aiomobilitydatabase.feeds.transit import group_stations


def _stop(
    stop_id: str,
    name: str | None = None,
    parent: str | None = None,
    location_type: int | None = 0,
) -> Stop:
    """A stop; ``location_type`` None is an unparseable cell (unknown type)."""
    return Stop(
        id=stop_id,
        name=name,
        latitude=0.0,
        longitude=0.0,
        parent_station=parent,
        location_type=(
            None if location_type is None else StopLocationType(location_type)
        ),
        stop_code=None,
        platform_code=None,
        wheelchair_boarding=None,
        description=None,
        url=None,
        zone_id=None,
        timezone=None,
    )


def test_platforms_group_under_parent_station() -> None:
    """Platforms share their station's group; the station row itself and
    entrances are never boarding stops."""
    groups = group_stations(
        [
            _stop("ST1", "Metro Center", location_type=1),
            _stop("P1", "Metro Center Platform 1", parent="ST1", location_type=0),
            _stop("P2", "Metro Center Platform 2", parent="ST1", location_type=0),
            _stop("E1", "Metro Center Entrance", parent="ST1", location_type=2),
            _stop("N1", "Node", parent="ST1", location_type=3),
        ]
    )
    assert groups == [
        StationGroup(id="ST1", name="Metro Center", stop_ids=("P1", "P2"))
    ]


def test_parent_station_outside_input_falls_back_to_child_name() -> None:
    """A platform whose station row wasn't included (for example just
    outside the search circle) still groups under the parent id but is
    labeled with its own name."""
    groups = group_stations(
        [_stop("P1", "Metro Center Platform 1", parent="ST1", location_type=0)]
    )
    assert groups == [
        StationGroup(id="ST1", name="Metro Center Platform 1", stop_ids=("P1",))
    ]


def test_nameless_platform_with_absent_parent_uses_parent_id() -> None:
    groups = group_stations([_stop("P1", None, parent="ST1", location_type=0)])
    assert groups == [StationGroup(id="ST1", name="ST1", stop_ids=("P1",))]


def test_same_named_parentless_stops_stay_apart() -> None:
    """GTFS relates stops only through parent_station: two parentless stops
    sharing a name (one feed's "Main St" in two towns) are two stations."""
    groups = group_stations(
        [
            _stop("S1", "Main St"),
            _stop("S2", "Main St"),
            _stop("S3", "main st"),
        ]
    )
    assert groups == [
        StationGroup(id="S1", name="Main St", stop_ids=("S1",)),
        StationGroup(id="S2", name="Main St", stop_ids=("S2",)),
        StationGroup(id="S3", name="main st", stop_ids=("S3",)),
    ]


def test_nameless_parentless_stop_uses_its_id() -> None:
    groups = group_stations([_stop("S9")])
    assert groups == [StationGroup(id="S9", name="S9", stop_ids=("S9",))]


def test_stop_of_unknown_type_is_not_a_boarding_stop() -> None:
    assert group_stations([_stop("S1", "Odd", location_type=None)]) == []


def test_groups_sorted_by_name() -> None:
    groups = group_stations([_stop("S1", "Zebra"), _stop("S2", "Alpha")])
    assert [group.name for group in groups] == ["Alpha", "Zebra"]


def test_empty_input() -> None:
    assert group_stations([]) == []


_stops_strategy = st.builds(
    lambda seeds: [
        _stop(
            f"S{index}",
            name,
            parent=parent,
            location_type=location_type,
        )
        for index, (name, parent, location_type) in enumerate(seeds)
    ],
    st.lists(
        st.tuples(
            st.sampled_from([None, "Alpha", "alpha", "Beta", "S3"]),
            st.sampled_from([None, "", "ST1", "ST2", "S0"]),
            st.sampled_from([None, 0, 1, 2, 3, 4]),
        ),
        max_size=12,
    ),
)


@given(_stops_strategy)
def test_groups_partition_boarding_stops(stops: list[Stop]) -> None:
    """Every boarding stop lands in exactly one group exactly once, and
    non-boarding stops (stations, entrances, nodes) never appear."""
    groups = group_stations(stops)
    grouped_ids = [stop_id for group in groups for stop_id in group.stop_ids]
    boarding_ids = [stop.id for stop in stops if stop.location_type == 0]
    assert sorted(grouped_ids) == sorted(boarding_ids)
    assert len(grouped_ids) == len(set(grouped_ids))


@given(_stops_strategy)
def test_group_ids_unique_and_sorted(stops: list[Stop]) -> None:
    groups = group_stations(stops)
    assert len({group.id for group in groups}) == len(groups)
    keys = [(group.name, group.id) for group in groups]
    assert keys == sorted(keys)


@given(_stops_strategy)
def test_groups_follow_parent_station_alone(stops: list[Stop]) -> None:
    """A boarding stop's group is its parent station, or itself without one:
    names never enter into it."""
    by_id = {stop.id: stop for stop in stops}
    for group in group_stations(stops):
        for stop_id in group.stop_ids:
            assert (by_id[stop_id].parent_station or stop_id) == group.id
