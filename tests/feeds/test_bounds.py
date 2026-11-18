"""The API's bounding filters (PostGIS predicates) on rectangles."""

from __future__ import annotations

from functools import cache
from itertools import product

from aiomobilitydatabase.feeds._bounds import (
    Box,
    _interiors_meet,
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


def test_a_far_box_is_disjoint() -> None:
    feed = Box(20.0, 30.0, 20.0, 30.0)
    assert matches(DISJOINT, feed, QUERY)
    assert not matches(PARTIALLY, feed, QUERY)
    assert not matches(COMPLETELY, feed, QUERY)


def test_a_segment_on_the_query_edge_is_completely_enclosed_not_partially() -> None:
    segment = Box(10.0, 10.0, 2.0, 5.0)  # on the query's top edge
    assert matches(COMPLETELY, segment, QUERY)
    # A segment and an area never overlap (their dimensions differ), and a
    # segment contains no area.
    assert not matches(PARTIALLY, segment, QUERY)
    assert not matches(DISJOINT, segment, QUERY)


def test_a_point_feed_inside_the_query() -> None:
    point = Box(5.0, 5.0, 5.0, 5.0)
    assert matches(COMPLETELY, point, QUERY)
    assert not matches(PARTIALLY, point, QUERY)
    # A point query inside a feed box: the feed contains it.
    assert matches(PARTIALLY, QUERY, point)


# The oracle: the OGC predicates as DE-9IM patterns over point sets, decided
# on a quarter-step grid. Boxes have whole-number corners in [0, 3]. Each
# set the patterns test is a product of per-axis pieces (a closed or open
# interval with whole-number ends, or one whole number), or such a piece
# minus a box; when non-empty it holds a quarter-step point, so emptiness
# reads exactly. Dimension reads exactly too: an open interval between whole
# numbers holds several quarter steps, not the single midpoint a half-step
# grid would give. Subset tests are exact because a box not inside another
# has a corner outside it, and corners are grid points.
_STEPS = tuple(i / 4 for i in range(13))
_GRID = frozenset((y, x) for y in _STEPS for x in _STEPS)

_Points = frozenset[tuple[float, float]]


def _inside(value: float, low: float, high: float) -> bool:
    """Whether ``value`` is in the interval's relative interior."""
    return value == low if low == high else low < value < high


@cache
def _cells(box: Box) -> tuple[_Points, _Points, _Points]:
    """Return the box's interior, boundary and exterior on the grid."""
    closed = frozenset(
        (y, x)
        for y, x in _GRID
        if box.minimum_latitude <= y <= box.maximum_latitude
        and box.minimum_longitude <= x <= box.maximum_longitude
    )
    interior = frozenset(
        (y, x)
        for y, x in closed
        if _inside(y, box.minimum_latitude, box.maximum_latitude)
        and _inside(x, box.minimum_longitude, box.maximum_longitude)
    )
    return interior, closed - interior, _GRID - closed


def _dim(points: _Points) -> int:
    """Return the dimension of a product set on the grid; -1 when empty."""
    if not points:
        return -1
    return (len({y for y, _ in points}) > 1) + (len({x for _, x in points}) > 1)


def _ref_covers(a: Box, b: Box) -> bool:
    """``T*****FF*``, ``*T****FF*``, ``***T**FF*`` or ``****T*FF*``."""
    a_in, a_bd, a_ex = _cells(a)
    b_in, b_bd, _ = _cells(b)
    meet = bool((a_in | a_bd) & (b_in | b_bd))
    return meet and not a_ex & b_in and not a_ex & b_bd


def _ref_contains(a: Box, b: Box) -> bool:
    """``T*****FF*``."""
    a_in, _, a_ex = _cells(a)
    b_in, b_bd, _ = _cells(b)
    return bool(a_in & b_in) and not a_ex & b_in and not a_ex & b_bd


def _ref_overlaps(a: Box, b: Box) -> bool:
    """dim(I(a) ∩ I(b)) = dim(a) = dim(b), I(a) ∩ E(b) and E(a) ∩ I(b) non-empty."""
    a_in, a_bd, a_ex = _cells(a)
    b_in, b_bd, b_ex = _cells(b)
    return (
        _dim(a_in & b_in) == _dim(a_in | a_bd) == _dim(b_in | b_bd)
        and bool(a_in & b_ex)
        and bool(a_ex & b_in)
    )


def _ref_disjoint(a: Box, b: Box) -> bool:
    """``FF*FF****``."""
    a_in, a_bd, _ = _cells(a)
    b_in, b_bd, _ = _cells(b)
    return not (a_in | a_bd) & (b_in | b_bd)


# The module compares endpoints within one axis only, so its answers depend
# only on how each axis's at most four endpoints are ordered (ties
# included); the OGC predicates, being topological, keep their answers
# under any order-preserving stretch of an axis too. Four whole numbers
# realise every such order, so the boxes with corners in [0, 3] cover every
# case there is.
_ENDS = tuple((low, high) for low in range(4) for high in range(low, 4))
_BOXES = tuple(
    Box(float(lat_low), float(lat_high), float(lon_low), float(lon_high))
    for (lat_low, lat_high), (lon_low, lon_high) in product(_ENDS, _ENDS)
)


def test_predicates_match_the_de9im_definitions_for_every_endpoint_order() -> None:
    for a, b in product(_BOXES, repeat=2):
        assert covers(a, b) is _ref_covers(a, b), (a, b)
        assert contains(a, b) is _ref_contains(a, b), (a, b)
        assert overlaps(a, b) is _ref_overlaps(a, b), (a, b)
        assert disjoint(a, b) is _ref_disjoint(a, b), (a, b)
        assert _interiors_meet(a, b) is bool(_cells(a)[0] & _cells(b)[0]), (a, b)
        assert matches(COMPLETELY, a, b) is _ref_covers(b, a), (a, b)
        assert matches(PARTIALLY, a, b) is (
            _ref_overlaps(a, b) or _ref_contains(a, b)
        ), (a, b)
        assert matches(DISJOINT, a, b) is _ref_disjoint(a, b), (a, b)
