"""The API's bounding filters (PostGIS predicates) on rectangles."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase._bounds import (
    Box,
    contains,
    covers,
    disjoint,
    matches,
    overlaps,
)
from aiomobilitydatabase.models import BoundingFilterMethod

COMPLETELY = BoundingFilterMethod.COMPLETELY_ENCLOSED
PARTIALLY = BoundingFilterMethod.PARTIALLY_ENCLOSED
DISJOINT = BoundingFilterMethod.DISJOINT

QUERY = Box(0.0, 10.0, 0.0, 10.0)


def test_a_feed_inside_the_query_is_completely_enclosed_not_partially() -> None:
    feed = Box(2.0, 3.0, 2.0, 3.0)
    assert matches(COMPLETELY, feed, QUERY)
    # The API's partially_enclosed is ST_Overlaps OR ST_Contains(feed, query):
    # a feed strictly inside the query box is neither.
    assert not matches(PARTIALLY, feed, QUERY)
    assert not matches(DISJOINT, feed, QUERY)


def test_a_feed_around_the_query_is_partially_enclosed() -> None:
    feed = Box(-5.0, 15.0, -5.0, 15.0)
    assert matches(PARTIALLY, feed, QUERY)
    assert not matches(COMPLETELY, feed, QUERY)


def test_a_feed_crossing_the_query_edge_is_partially_enclosed() -> None:
    feed = Box(5.0, 15.0, 5.0, 15.0)
    assert matches(PARTIALLY, feed, QUERY)
    assert not matches(COMPLETELY, feed, QUERY)
    assert not matches(DISJOINT, feed, QUERY)


def test_an_equal_box_is_both_enclosed_kinds() -> None:
    assert matches(COMPLETELY, QUERY, QUERY)
    assert matches(PARTIALLY, QUERY, QUERY)  # ST_Contains(Q, Q)


def test_touching_boxes_are_not_disjoint_and_not_partial() -> None:
    feed = Box(10.0, 20.0, 0.0, 10.0)  # shares the query's top edge
    assert not matches(DISJOINT, feed, QUERY)
    assert not matches(PARTIALLY, feed, QUERY)


def test_a_point_feed_inside_the_query() -> None:
    point = Box(5.0, 5.0, 5.0, 5.0)
    assert matches(COMPLETELY, point, QUERY)
    assert not matches(PARTIALLY, point, QUERY)
    # A point query inside a feed box: the feed contains it.
    assert matches(PARTIALLY, QUERY, point)


# The oracle: OGC predicates decided on point sets. Boxes have integer
# corners in [0, 4]; every set the predicates compare (a box, its relative
# interior, an intersection) then holds a point of the half-step grid when
# it is non-empty, so deciding on that grid is exact.
_STEPS = tuple(i / 2 for i in range(9))
_GRID = tuple((y, x) for y in _STEPS for x in _STEPS)


def _closed(box: Box) -> frozenset[tuple[float, float]]:
    return frozenset(
        (y, x)
        for y, x in _GRID
        if box.minimum_latitude <= y <= box.maximum_latitude
        and box.minimum_longitude <= x <= box.maximum_longitude
    )


def _inside(value: float, low: float, high: float) -> bool:
    return value == low if low == high else low < value < high


def _interior(box: Box) -> frozenset[tuple[float, float]]:
    return frozenset(
        (y, x)
        for y, x in _GRID
        if _inside(y, box.minimum_latitude, box.maximum_latitude)
        and _inside(x, box.minimum_longitude, box.maximum_longitude)
    )


def _dim(points: frozenset[tuple[float, float]]) -> int:
    if not points:
        return -1
    spread = len({y for y, _ in points}) > 1, len({x for _, x in points}) > 1
    return sum(spread)


def _ref_covers(outer: Box, inner: Box) -> bool:
    return _closed(inner) <= _closed(outer)


def _ref_contains(a: Box, b: Box) -> bool:
    return _ref_covers(a, b) and bool(_interior(a) & _interior(b))


def _ref_overlaps(a: Box, b: Box) -> bool:
    return (
        _dim(_closed(a)) == _dim(_closed(b))
        and bool(_interior(a) & _interior(b))
        and not _ref_covers(a, b)
        and not _ref_covers(b, a)
        and _dim(_closed(a) & _closed(b)) == _dim(_closed(a))
    )


def _ref_disjoint(a: Box, b: Box) -> bool:
    return not (_closed(a) & _closed(b))


_ends = st.tuples(st.integers(0, 4), st.integers(0, 4)).map(sorted)
_boxes = st.tuples(_ends, _ends).map(
    lambda axes: Box(
        float(axes[0][0]), float(axes[0][1]), float(axes[1][0]), float(axes[1][1])
    )
)


@settings(max_examples=2000)
@given(_boxes, _boxes)
def test_predicates_match_the_point_set_definitions(a: Box, b: Box) -> None:
    assert covers(a, b) is _ref_covers(a, b)
    assert contains(a, b) is _ref_contains(a, b)
    assert overlaps(a, b) is _ref_overlaps(a, b)
    assert disjoint(a, b) is _ref_disjoint(a, b)
    assert matches(COMPLETELY, a, b) is _ref_covers(b, a)
    assert matches(PARTIALLY, a, b) is (_ref_overlaps(a, b) or _ref_contains(a, b))
    assert matches(DISJOINT, a, b) is _ref_disjoint(a, b)
