"""Zone geometry: circles (bbox prefilter + great-circle distance), polygons."""

from __future__ import annotations

import itertools
import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from fractions import Fraction

_EARTH_RADIUS_M = 6_371_000.0
# Derived from _EARTH_RADIUS_M so it stays consistent with great_circle_m forever.
_M_PER_DEG_LAT = _EARTH_RADIUS_M * math.pi / 180.0
# The bbox prefilter must strictly enclose the circle. A slightly generous box
# only costs a few extra distance calls; a tight box wrongly rejects boundary
# points (e.g. the cos(lat) linearization under-sizes the box at high latitude).
_BBOX_SAFETY = 1.01
_HALF_TURN_DEG = 180.0
_FULL_TURN_DEG = 360.0


@dataclass(frozen=True)
class Circle:
    """A circular geographic zone (matches HA zone entities)."""

    latitude: float
    longitude: float
    radius_m: float


def great_circle_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters.

    The atan2 (Vincenty-sphere) form rather than haversine. Haversine
    computes ``asin(sqrt(a))``, and asin's derivative diverges as a -> 1,
    so near-antipodal pairs lose all precision: a and (0, 0) to
    (1e-06, 180) saturates at exactly half the circumference, reporting
    the two points 0.111 m further apart than they are and VIOLATING the
    triangle inequality. The atan2 form is conditioned well at both ends
    of the range, costs the same, and agrees with haversine to float
    precision over the short distances this library actually queries.
    """
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    sin_dlambda, cos_dlambda = math.sin(dlambda), math.cos(dlambda)
    cos_phi1, sin_phi1 = math.cos(phi1), math.sin(phi1)
    cos_phi2, sin_phi2 = math.cos(phi2), math.sin(phi2)
    numerator = math.hypot(
        cos_phi2 * sin_dlambda,
        cos_phi1 * sin_phi2 - sin_phi1 * cos_phi2 * cos_dlambda,
    )
    denominator = sin_phi1 * sin_phi2 + cos_phi1 * cos_phi2 * cos_dlambda
    return _EARTH_RADIUS_M * math.atan2(numerator, denominator)


def in_circle(zone: Circle, latitude: float, longitude: float) -> bool:
    """Return True if the point lies within the zone.

    A cheap bounding-box prefilter (four float comparisons) rejects most
    points before the trigonometric haversine runs. The box is padded by
    ``_BBOX_SAFETY`` so it strictly encloses the circle. The longitude
    comparison wraps around the antimeridian: a naive linear delta treats a
    zone near +180 and a point near -180 (or vice versa) as ~360 degrees
    apart, when the true angular separation may be a fraction of a degree.
    """
    dlat_deg = zone.radius_m * _BBOX_SAFETY / _M_PER_DEG_LAT
    if latitude < zone.latitude - dlat_deg or latitude > zone.latitude + dlat_deg:
        return False
    cos_lat = math.cos(math.radians(zone.latitude))
    dlon_deg = zone.radius_m * _BBOX_SAFETY / (_M_PER_DEG_LAT * max(cos_lat, 1e-6))
    dlon = abs(longitude - zone.longitude)
    if dlon > _HALF_TURN_DEG:
        dlon = _FULL_TURN_DEG - dlon
    if dlon > dlon_deg:
        return False
    distance = great_circle_m(zone.latitude, zone.longitude, latitude, longitude)
    return distance <= zone.radius_m


# -- Polygon validity ----------------------------------------------------------

type Point = tuple[float, float]
type _Edge = tuple[int, int, Point, Point]

# Shewchuk's ccwerrboundA, (3 + 16e)e for double precision e = 2**-53: past
# this share of the terms' magnitude the float determinant's sign is right.
_ORIENTATION_ERROR = (3 + 16 * 2.0**-53) * 2.0**-53


def _orientation(a: Point, b: Point, c: Point) -> int:
    """Return the sign of ``(b - a) x (c - a)``, exactly: 1, 0 or -1.

    Floats first; when the result is within their rounding error of zero,
    again in exact rationals, so a nearly collinear triple never reads as
    crossing (or not) by accident of rounding.
    """
    left = (b[0] - a[0]) * (c[1] - a[1])
    right = (b[1] - a[1]) * (c[0] - a[0])
    det = left - right
    if abs(det) > _ORIENTATION_ERROR * (abs(left) + abs(right)):
        return 1 if det > 0 else -1
    ax, ay = Fraction(a[0]), Fraction(a[1])
    exact = (Fraction(b[0]) - ax) * (Fraction(c[1]) - ay) - (Fraction(b[1]) - ay) * (
        Fraction(c[0]) - ax
    )
    return (exact > 0) - (exact < 0)


def _within_box(p: Point, a: Point, b: Point) -> bool:
    return min(a[0], b[0]) <= p[0] <= max(a[0], b[0]) and min(a[1], b[1]) <= p[
        1
    ] <= max(a[1], b[1])


def _touch(a: Point, b: Point, c: Point, d: Point) -> bool:
    """Return whether the closed segments ab and cd share any point."""
    o1, o2 = _orientation(a, b, c), _orientation(a, b, d)
    o3, o4 = _orientation(c, d, a), _orientation(c, d, b)
    if o1 * o2 < 0 and o3 * o4 < 0:
        return True
    return (
        (o1 == 0 and _within_box(c, a, b))
        or (o2 == 0 and _within_box(d, a, b))
        or (o3 == 0 and _within_box(a, c, d))
        or (o4 == 0 and _within_box(b, c, d))
    )


def _sign(value: float) -> int:
    return (value > 0) - (value < 0)


def _meet_only_at_a_shared_end(a: Point, b: Point, c: Point, d: Point) -> bool:
    """Return whether ab and cd meet in exactly one point, an end of both."""
    if a == c:
        p, u, v = a, b, d
    elif a == d:
        p, u, v = a, b, c
    elif b == c:
        p, u, v = b, a, d
    elif b == d:
        p, u, v = b, a, c
    else:
        return False
    if u == v:
        return False  # the same segment
    # Sharing p, they meet again only lying along one line the same way from
    # p; along one line, the coordinate signs from p are equal or opposite.
    return _orientation(p, u, v) != 0 or (
        _sign(u[0] - p[0]),
        _sign(u[1] - p[1]),
    ) == (-_sign(v[0] - p[0]), -_sign(v[1] - p[1]))


# An edge whose bounding box covers more grid cells than this is compared
# with every edge instead of being bucketed (a long straight side among
# short ones would otherwise fill a whole row of cells).
_MAX_CELLS_PER_EDGE = 16


def _candidate_pairs(edges: list[_Edge]) -> set[tuple[int, int]]:
    """Return index pairs ``(i, j)``, ``i < j``, of edges that may meet.

    Edges are bucketed on a grid with cells the size of the median edge, so
    a zone with evenly spread vertices costs about its edge count rather
    than its square; the few edges spanning many cells are paired with
    every edge. Float subtraction and division round monotonically, so a
    point both edges hold falls in a cell both cover.
    """
    xs = [x for _r, _i, a, b in edges for x in (a[0], b[0])]
    ys = [y for _r, _i, a, b in edges for y in (a[1], b[1])]
    extents = sorted(max(abs(b[0] - a[0]), abs(b[1] - a[1])) for *_, a, b in edges)
    span = max(max(xs) - min(xs), max(ys) - min(ys))
    # Never finer than about 4n cells across the zone: a few tiny edges must
    # not make every other edge cover an astronomical number of cells.
    size = max(extents[len(extents) // 2], span / (4 * len(edges))) or 1.0
    x0, y0 = min(xs), min(ys)

    def cell(value: float, origin: float) -> int:
        return int((value - origin) / size)

    buckets: defaultdict[tuple[int, int], list[int]] = defaultdict(list)
    long_edges: list[int] = []
    for index, (_r, _i, a, b) in enumerate(edges):
        columns = range(cell(min(a[0], b[0]), x0), cell(max(a[0], b[0]), x0) + 1)
        rows = range(cell(min(a[1], b[1]), y0), cell(max(a[1], b[1]), y0) + 1)
        if (columns.stop - columns.start) * (rows.stop - rows.start) > (
            _MAX_CELLS_PER_EDGE
        ):
            long_edges.append(index)
            continue
        for column in columns:
            for row in rows:
                buckets[column, row].append(index)
    pairs = {
        (first, second)
        for members in buckets.values()
        for n, first in enumerate(members)
        for second in members[n + 1 :]
    }
    pairs.update(
        (min(index, other), max(index, other))
        for index in long_edges
        for other in range(len(edges))
        if other != index
    )
    return pairs


def _position(p: Point, ring: Sequence[Point]) -> int:
    """Return 1 if ``p`` is inside ``ring`` (closed), 0 on it, -1 outside."""
    inside = False
    for a, b in itertools.pairwise(ring):
        orientation = _orientation(a, b, p)
        if orientation == 0 and _within_box(p, a, b):
            return 0
        if (a[1] > p[1]) != (b[1] > p[1]) and (orientation > 0) == (b[1] > a[1]):
            inside = not inside
    return 1 if inside else -1


def _arc_points(ring: Sequence[Point], shared: set[Point]) -> list[Point]:
    """Return one point on each arc ``ring`` splits into at ``shared`` points.

    The midpoint of the edge leaving each shared vertex (or of the first
    edge, if none is shared). With no edge crossing another ring, each arc
    lies wholly on one side of it, so these points place the whole ring.
    """
    starts = [i for i, point in enumerate(ring[:-1]) if point in shared] or [0]
    return [
        ((ring[i][0] + ring[i + 1][0]) / 2, (ring[i][1] + ring[i + 1][1]) / 2)
        for i in starts
    ]


def _box(ring: Sequence[Point]) -> tuple[float, float, float, float]:
    return (
        min(p[0] for p in ring),
        min(p[1] for p in ring),
        max(p[0] for p in ring),
        max(p[1] for p in ring),
    )


def polygon_is_valid(rings: Sequence[Sequence[Point]]) -> bool:
    """Return whether a polygon (exterior ring, then holes) is valid.

    GTFS-Flex: "Each polygon must be valid by the definition of the OpenGIS
    Simple Features Specification, section 6.1.11". Checked, on closed
    rings of planar points (consecutive repeats ignored):

    - each ring is simple: three or more distinct vertices, no edge
      touching another except a neighbour at their shared vertex, and no
      neighbour folding back along it;
    - rings do not cross: two rings meet only at vertices both have;
    - every hole lies inside the exterior and outside every other hole.

    Not checked: that the interior stays connected (a hole meeting the
    exterior at two vertices cuts it in two). Stricter than 6.1.11 in one
    case: a ring touching another along an edge's middle (a tangent there
    is valid) is refused.
    """
    cleaned: list[list[Point]] = []
    for ring in rings:
        points = [p for i, p in enumerate(ring) if i == 0 or p != ring[i - 1]]
        if len(points) < 4 or points[0] != points[-1]:  # noqa: PLR2004
            return False
        cleaned.append(points)
    edges: list[_Edge] = [
        (r, i, ring[i], ring[i + 1])
        for r, ring in enumerate(cleaned)
        for i in range(len(ring) - 1)
    ]
    for first, second in _candidate_pairs(edges):
        r1, i1, a, b = edges[first]
        r2, i2, c, d = edges[second]
        count = len(cleaned[r1]) - 1
        if r1 != r2:
            ok = not _touch(a, b, c, d) or _meet_only_at_a_shared_end(a, b, c, d)
        elif (i2 - i1) % count in (1, count - 1):
            ok = _meet_only_at_a_shared_end(a, b, c, d)
        else:
            ok = not _touch(a, b, c, d)
        if not ok:
            return False
    exterior, holes = cleaned[0], cleaned[1:]
    boxes = [_box(hole) for hole in holes]
    for h, hole in enumerate(holes):
        if any(_position(p, exterior) != 1 for p in _arc_points(hole, set(exterior))):
            return False
        for g, other in enumerate(holes):
            inner, outer = boxes[h], boxes[g]
            if g == h or not (
                outer[0] <= inner[0]
                and outer[1] <= inner[1]
                and inner[2] <= outer[2]
                and inner[3] <= outer[3]
            ):
                continue  # a hole can only lie inside one whose box holds its box
            if any(_position(p, other) == 1 for p in _arc_points(hole, set(other))):
                return False
    return True
