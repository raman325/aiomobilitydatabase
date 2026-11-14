"""Zone geometry: circles (bbox prefilter + great-circle distance), polygons."""

from __future__ import annotations

import itertools
import math
from collections import defaultdict
from collections.abc import Iterator, Sequence
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
# Points scaled to exact integers (see _exact_rings): every decision below is
# integer arithmetic, so no rounding can move a point across a line.
type _IntPoint = tuple[int, int]
type _Edge = tuple[int, int, _IntPoint, _IntPoint]


def _exact_rings(rings: Sequence[Sequence[Point]]) -> list[list[_IntPoint]]:
    """Return the rings with every coordinate scaled to an even integer.

    Each float is a fraction with a power-of-two denominator, so scaling by
    twice their common denominator is exact and keeps every orientation,
    and makes every edge midpoint an integer too.
    """
    exact = [[(Fraction(x), Fraction(y)) for x, y in ring] for ring in rings]
    scale = 2 * math.lcm(
        *(value.denominator for ring in exact for point in ring for value in point)
    )
    return [[(int(x * scale), int(y * scale)) for x, y in ring] for ring in exact]


def _orientation(a: _IntPoint, b: _IntPoint, c: _IntPoint) -> int:
    """Return the sign of ``(b - a) x (c - a)``: 1, 0 or -1."""
    det = (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
    return (det > 0) - (det < 0)


def _within_box(p: _IntPoint, a: _IntPoint, b: _IntPoint) -> bool:
    return min(a[0], b[0]) <= p[0] <= max(a[0], b[0]) and min(a[1], b[1]) <= p[
        1
    ] <= max(a[1], b[1])


def _touch(a: _IntPoint, b: _IntPoint, c: _IntPoint, d: _IntPoint) -> bool:
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


def _sign(value: int) -> int:
    return (value > 0) - (value < 0)


def _meet_only_at_a_shared_end(
    a: _IntPoint, b: _IntPoint, c: _IntPoint, d: _IntPoint
) -> bool:
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


def _cells(
    a: _IntPoint, b: _IntPoint, origin: _IntPoint, size: int
) -> Iterator[tuple[int, int]]:
    """Yield every grid cell the closed segment ab passes through.

    Column by column: the segment's y-range over each column's closed slab,
    computed exactly, gives the rows. A point of the segment lies in the
    cell of its own floor coordinates, which this always yields.
    """
    (x1, y1), (x2, y2) = sorted((a, b))
    x0, y0 = origin
    if x1 == x2:
        for row in range((min(y1, y2) - y0) // size, (max(y1, y2) - y0) // size + 1):
            yield (x1 - x0) // size, row
        return
    dx, dy = x2 - x1, y2 - y1
    for column in range((x1 - x0) // size, (x2 - x0) // size + 1):
        lo = max(x1, x0 + column * size)
        hi = min(x2, x0 + (column + 1) * size)
        # floor((y(x) - y0) / size) with y(x) = y1 + dy (x - x1) / dx, exactly
        rows = [((y1 - y0) * dx + dy * (x - x1)) // (dx * size) for x in (lo, hi)]
        for row in range(min(rows), max(rows) + 1):
            yield column, row


def _candidate_pairs(edges: list[_Edge]) -> Iterator[tuple[int, int]]:
    """Yield index pairs ``(i, j)``, ``i < j``, of edges that may meet.

    Edges are bucketed on a grid, each in just the cells its line crosses,
    and only edges sharing a cell are paired; edges that meet share the
    cell of their meeting point. A pair sharing several cells is yielded
    once per cell (checking it twice is harmless; remembering every pair
    would cost memory quadratic in the worst case).

    Cells are the size of the median edge, coarsened so the edges cover
    about 8n cells in all and so no more than about 4n fit across the zone:
    memory stays linear in the edge count. Time follows how crowded the
    busiest cells are -- about the edge count for evenly spread vertices
    with or without a few long edges, more where many edges run close
    together or converge (a fine comb, a sunburst of spikes).
    """
    xs = [x for _r, _i, a, b in edges for x in (a[0], b[0])]
    ys = [y for _r, _i, a, b in edges for y in (a[1], b[1])]
    extents = sorted(max(abs(b[0] - a[0]), abs(b[1] - a[1])) for *_, a, b in edges)
    span = max(max(xs) - min(xs), max(ys) - min(ys))
    travel = sum(abs(b[0] - a[0]) + abs(b[1] - a[1]) for *_, a, b in edges)
    size = max(
        extents[len(extents) // 2],
        span // (4 * len(edges)),
        travel // (6 * len(edges)),
        1,
    )
    origin = (min(xs), min(ys))
    buckets: defaultdict[tuple[int, int], list[int]] = defaultdict(list)
    for index, (_r, _i, a, b) in enumerate(edges):
        for cell in _cells(a, b, origin, size):
            buckets[cell].append(index)
    for members in buckets.values():
        for n, first in enumerate(members):
            for second in members[n + 1 :]:
                yield first, second


def _position(p: _IntPoint, ring: Sequence[_IntPoint]) -> int:
    """Return 1 if ``p`` is inside ``ring`` (closed), 0 on it, -1 outside."""
    inside = False
    for a, b in itertools.pairwise(ring):
        if (a[1] > p[1]) == (b[1] > p[1]) and p[1] not in (a[1], b[1]):
            continue  # the edge neither reaches p's row nor can hold p
        orientation = _orientation(a, b, p)
        if orientation == 0 and _within_box(p, a, b):
            return 0
        if (a[1] > p[1]) != (b[1] > p[1]) and (orientation > 0) == (b[1] > a[1]):
            inside = not inside
    return 1 if inside else -1


def _arc_points(ring: Sequence[_IntPoint], shared: set[_IntPoint]) -> list[_IntPoint]:
    """Return one point on each arc ``ring`` splits into at ``shared`` points.

    The midpoint of the edge leaving each shared vertex (of the first edge
    if none is shared) -- exact, every coordinate being even. With no edge
    touching the other ring except at shared vertices, an edge's interior,
    and so each arc, lies wholly on one side of it: these points place the
    whole ring.
    """
    starts = [i for i, point in enumerate(ring[:-1]) if point in shared] or [0]
    return [
        ((ring[i][0] + ring[i + 1][0]) // 2, (ring[i][1] + ring[i + 1][1]) // 2)
        for i in starts
    ]


def _box(ring: Sequence[_IntPoint]) -> tuple[int, int, int, int]:
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
    rings of planar points (consecutive repeats ignored), in exact integer
    arithmetic:

    - each ring is simple: three or more distinct vertices, no edge
      touching another except a neighbour at their shared vertex, and no
      neighbour folding back along it;
    - rings do not cross: two rings meet only at vertices both have;
    - every hole lies inside the exterior, and no two holes overlap.

    Not checked: that the interior stays connected (a hole meeting the
    exterior at two vertices cuts it in two). Stricter than 6.1.11 in one
    case: a ring touching another along an edge's middle (a tangent there
    is valid) is refused.
    """
    cleaned: list[list[_IntPoint]] = []
    for ring in _exact_rings(rings):
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
    corners = set(exterior)
    if any(
        _position(p, exterior) != 1
        for hole in holes
        for p in _arc_points(hole, corners)
    ):
        return False
    boxes = [_box(hole) for hole in holes]
    for h, hole in enumerate(holes):
        for g, other in enumerate(holes):
            mine, theirs = boxes[h], boxes[g]
            if g == h or (
                mine[2] < theirs[0]
                or theirs[2] < mine[0]
                or mine[3] < theirs[1]
                or theirs[3] < mine[1]
            ):
                continue  # disjoint boxes: no overlap
            if any(_position(p, other) == 1 for p in _arc_points(hole, set(other))):
                return False
    return True
