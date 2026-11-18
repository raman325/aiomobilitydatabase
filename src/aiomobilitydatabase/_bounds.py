"""The catalog API's bounding filters, on rectangles.

mobility-feed-api filters with PostGIS on polygons
(``apply_bounding_filtering``): ``completely_enclosed`` is
``ST_Covers(query, feed)``, ``partially_enclosed`` is
``ST_Overlaps(feed, query) OR ST_Contains(feed, query)``, and ``disjoint``
is ``ST_Disjoint(feed, query)``. These are the same OGC predicates for
closed axis-aligned rectangles. A degenerate rectangle (zero width or
height) is the segment or point it covers: its interior is its relative
interior (the open segment, or the point), its dimension 1 or 0.

For boxes with area these match PostGIS exactly. The API stores a
degenerate box as a flat (zero-area) POLYGON, which GEOS reports invalid;
PostGIS's answers for one vary with the GEOS version, with whether it has
prepared (cached) the query geometry yet, and with the ring's starting
vertex. No stable rule reproduces them, so here such a box is the point
set it covers, and its results may differ from the API's.
"""

from __future__ import annotations

from typing import NamedTuple

from .models import BoundingFilterMethod


class Box(NamedTuple):
    """A closed rectangle in degrees, each minimum at most its maximum."""

    minimum_latitude: float
    maximum_latitude: float
    minimum_longitude: float
    maximum_longitude: float


def _axes(box: Box) -> tuple[tuple[float, float], tuple[float, float]]:
    return (
        (box.minimum_latitude, box.maximum_latitude),
        (box.minimum_longitude, box.maximum_longitude),
    )


def _dimension(box: Box) -> int:
    """2 for an area, 1 for a segment, 0 for a point."""
    return sum(low < high for low, high in _axes(box))


def covers(outer: Box, inner: Box) -> bool:
    """``ST_Covers(outer, inner)``: no point of ``inner`` outside ``outer``."""
    return all(
        outer_low <= inner_low and inner_high <= outer_high
        for (outer_low, outer_high), (inner_low, inner_high) in zip(
            _axes(outer), _axes(inner), strict=True
        )
    )


def disjoint(a: Box, b: Box) -> bool:
    """``ST_Disjoint(a, b)``: no point in common."""
    return any(
        a_high < b_low or b_high < a_low
        for (a_low, a_high), (b_low, b_high) in zip(_axes(a), _axes(b), strict=True)
    )


def _interiors_meet_on(
    a_low: float, a_high: float, b_low: float, b_high: float
) -> bool:
    """Whether two intervals' relative interiors share a point.

    A degenerate interval's relative interior is its one point.
    """
    if a_low == a_high and b_low == b_high:
        return a_low == b_low
    if a_low == a_high:
        return b_low < a_low < b_high
    if b_low == b_high:
        return a_low < b_low < a_high
    return a_low < b_high and b_low < a_high


def _interiors_meet(a: Box, b: Box) -> bool:
    """Whether the interiors share a point: axis by axis, as they are products."""
    return all(
        _interiors_meet_on(a_low, a_high, b_low, b_high)
        for (a_low, a_high), (b_low, b_high) in zip(_axes(a), _axes(b), strict=True)
    )


def _intersection(a: Box, b: Box) -> Box:
    """Return the common rectangle of two boxes whose interiors meet."""
    return Box(
        max(a.minimum_latitude, b.minimum_latitude),
        min(a.maximum_latitude, b.maximum_latitude),
        max(a.minimum_longitude, b.minimum_longitude),
        min(a.maximum_longitude, b.maximum_longitude),
    )


def contains(a: Box, b: Box) -> bool:
    """``ST_Contains(a, b)``: ``b`` covered by ``a``, and their interiors meet."""
    return covers(a, b) and _interiors_meet(a, b)


def overlaps(a: Box, b: Box) -> bool:
    """``ST_Overlaps(a, b)``: same dimension, interiors meet, neither covers the other.

    The two must also meet in that dimension: crossing segments meet only
    in a point, so they do not overlap.
    """
    return (
        _dimension(a) == _dimension(b)
        and _interiors_meet(a, b)
        and not covers(a, b)
        and not covers(b, a)
        and _dimension(_intersection(a, b)) == _dimension(a)
    )


def matches(method: BoundingFilterMethod, feed: Box, query: Box) -> bool:
    """Whether ``feed`` passes the API's ``method`` filter for ``query``."""
    if method is BoundingFilterMethod.COMPLETELY_ENCLOSED:
        return covers(query, feed)
    if method is BoundingFilterMethod.PARTIALLY_ENCLOSED:
        return overlaps(feed, query) or contains(feed, query)
    return disjoint(feed, query)
