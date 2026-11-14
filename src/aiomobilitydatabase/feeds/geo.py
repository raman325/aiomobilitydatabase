"""Zone geometry: circles (bbox prefilter + great-circle distance), polygons."""

from __future__ import annotations

import bisect
import itertools
import math
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
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
    a: _IntPoint,
    b: _IntPoint,
    origin: _IntPoint,
    size: int,
    limit: int | None = None,
) -> Iterator[tuple[int, int]]:
    """Yield every grid cell the closed segment ab passes through.

    Column by column: the segment's y-range over each column's closed slab,
    computed exactly, gives the rows. A point of the segment lies in the
    cell of its own floor coordinates, which this always yields. With
    ``limit``, only cells with both indices in ``[0, limit)`` are walked.
    """
    (x1, y1), (x2, y2) = sorted((a, b))
    x0, y0 = origin
    low, high = (0, limit - 1) if limit is not None else (None, None)

    def clamp(first: int, last: int) -> range:
        if low is not None and high is not None:
            first, last = max(first, low), min(last, high)
        return range(first, last + 1)

    if x1 == x2:
        column = (x1 - x0) // size
        if low is not None and high is not None and not low <= column <= high:
            return
        for row in clamp((min(y1, y2) - y0) // size, (max(y1, y2) - y0) // size):
            yield column, row
        return
    dx, dy = x2 - x1, y2 - y1
    for column in clamp((x1 - x0) // size, (x2 - x0) // size):
        lo = max(x1, x0 + column * size)
        hi = min(x2, x0 + (column + 1) * size)
        # floor((y(x) - y0) / size) with y(x) = y1 + dy (x - x1) / dx, exactly
        rows = [((y1 - y0) * dx + dy * (x - x1)) // (dx * size) for x in (lo, hi)]
        for row in clamp(min(rows), max(rows)):
            yield column, row


# A cell holding more edges than this is split into a finer grid of its own
# (each side cut in _SUBDIVISION), down to unit cells: detail clustered in
# a few cells of an otherwise coarse zone stays near-linear.
_CROWDED = 64
_SUBDIVISION = 8


def _bucket_pairs(
    edges: list[_Edge],
    members: Iterable[int],
    origin: _IntPoint,
    size: int,
    limit: int | None,
    *,
    seen: set[tuple[int, int]] | None = None,
) -> Iterator[tuple[int, int]]:
    """Yield pairs of ``members`` sharing a cell of this grid (or a sub-cell).

    A crowded cell is re-bucketed on a finer grid clipped to the cell; two
    edges meeting in the cell meet in the sub-cell holding that point.
    Within one crowded cell, a pair is yielded once however many of its
    sub-cells it shares (nearly parallel edges share hundreds): ``seen``
    holds that cell's pairs only.
    """
    buckets: defaultdict[tuple[int, int], list[int]] = defaultdict(list)
    for index in members:
        _r, _i, a, b = edges[index]
        for cell in _cells(a, b, origin, size, limit):
            buckets[cell].append(index)
    for (column, row), inside in buckets.items():
        if len(inside) > _CROWDED and size > 1:
            sub = -(-size // _SUBDIVISION)
            yield from _bucket_pairs(
                edges,
                inside,
                (origin[0] + column * size, origin[1] + row * size),
                sub,
                -(-size // sub),
                seen=set() if seen is None else seen,
            )
            continue
        for n, first in enumerate(inside):
            for second in inside[n + 1 :]:
                pair = (first, second) if first < second else (second, first)
                if seen is not None:
                    if pair in seen:
                        continue
                    seen.add(pair)
                yield pair


def _candidate_pairs(edges: list[_Edge]) -> Iterator[tuple[int, int]]:
    """Yield index pairs ``(i, j)``, ``i < j``, of edges that may meet.

    Edges are bucketed on a grid, each in just the cells its line crosses,
    crowded cells split finer, and only edges sharing a cell are paired;
    edges that meet share the cell of their meeting point. A pair sharing
    several cells is yielded once per cell (checking it twice is harmless;
    remembering every pair would cost memory quadratic in the worst case).

    Cells are the size of the median edge, coarsened so the edges cover
    about 8n cells in all and so no more than about 4n fit across the zone:
    memory stays linear in the edge count. Time is about the edge count for
    evenly spread vertices, with or without a few long edges or a cluster
    of fine detail; more where many edges converge on one point (a
    sunburst of spikes), where no grid separates them.
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
    return _bucket_pairs(edges, range(len(edges)), (min(xs), min(ys)), size, None)


def _position(p: _IntPoint, ring: Sequence[_IntPoint]) -> int:
    """Return 1 if ``p`` is inside ``ring`` (closed), 0 on it, -1 outside."""
    return _positions([p], ring)[0]


def _positions(points: Sequence[_IntPoint], ring: Sequence[_IntPoint]) -> list[int]:
    """Place each point against ``ring`` (closed): 1 inside, 0 on it, -1 out.

    One pass over the ring for all the points: each edge visits only the
    points whose y lies in its own y-range (found by bisection), toggling
    their parity where a ray to +x crosses it.
    """
    order = sorted(range(len(points)), key=lambda k: points[k][1])
    ys = [points[k][1] for k in order]
    inside = [False] * len(points)
    on = [False] * len(points)
    for a, b in itertools.pairwise(ring):
        low, high = min(a[1], b[1]), max(a[1], b[1])
        for k in order[bisect.bisect_left(ys, low) : bisect.bisect_right(ys, high)]:
            p = points[k]
            orientation = _orientation(a, b, p)
            if orientation == 0 and _within_box(p, a, b):
                on[k] = True
            elif (a[1] > p[1]) != (b[1] > p[1]) and (orientation > 0) == (b[1] > a[1]):
                inside[k] = not inside[k]
    return [0 if on[k] else 1 if inside[k] else -1 for k in range(len(points))]


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


def _holes_overlap(holes: list[list[_IntPoint]]) -> bool:
    """Return whether any two holes overlap (rings meeting only at vertices).

    Swept by box: only holes whose boxes overlap are compared, each way --
    if no arc of either lies inside the other, their interiors are apart.
    """
    boxes = [_box(hole) for hole in holes]
    corners = [set(hole) for hole in holes]
    active: list[int] = []
    for h in sorted(range(len(holes)), key=lambda k: boxes[k][0]):
        active = [g for g in active if boxes[g][2] >= boxes[h][0]]
        for g in active:
            if boxes[g][3] < boxes[h][1] or boxes[h][3] < boxes[g][1]:
                continue
            for inner, outer in ((h, g), (g, h)):
                shared = corners[inner] & corners[outer]
                if 1 in _positions(_arc_points(holes[inner], shared), holes[outer]):
                    return True
        active.append(h)
    return False


def _interior_disconnected(rings: list[list[_IntPoint]]) -> bool:
    """Return whether rings meeting at shared vertices cut the interior apart.

    With rings meeting only at vertices, the interior is disconnected
    exactly when rings and the points they share form a cycle (a hole
    touching the exterior twice, or holes chained from it and back): found
    by union-find over ring-point incidences.
    """
    owners: defaultdict[_IntPoint, list[int]] = defaultdict(list)
    for r, ring in enumerate(rings):
        for point in set(ring):
            owners[point].append(r)
    parent: dict[tuple[str, object], tuple[str, object]] = {}

    def find(node: tuple[str, object]) -> tuple[str, object]:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    for point, members in owners.items():
        if len(members) < 2:  # noqa: PLR2004
            continue
        for r in members:
            first, second = find(("point", point)), find(("ring", r))
            if first == second:
                return True
            parent[first] = second
    return False


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
    - every hole lies inside the exterior, and no two holes overlap;
    - the interior is connected: rings meeting at shared vertices form no
      cycle.

    Stricter than 6.1.11 in one case: a ring touching another along an
    edge's middle (a tangent there is valid) is refused.
    """
    if not rings:
        return False
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
    arcs = [p for hole in holes for p in _arc_points(hole, corners)]
    if any(position != 1 for position in _positions(arcs, exterior)):
        return False
    return not _holes_overlap(holes) and not _interior_disconnected(cleaned)
