"""Zone geometry: circles (bbox prefilter + great-circle distance), polygons."""

from __future__ import annotations

import bisect
import itertools
import math
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from fractions import Fraction

from .models import FlexPolygon

_EARTH_RADIUS_M = 6_371_000.0
# Derived from _EARTH_RADIUS_M so it stays consistent with great_circle_m forever.
_M_PER_DEG_LAT = _EARTH_RADIUS_M * math.pi / 180.0
# The bbox prefilter must strictly enclose the circle. A slightly generous box
# only costs a few extra distance calls; a tight box wrongly rejects boundary
# points (e.g. the cos(lat) linearization under-sizes the box at high latitude).
_BBOX_SAFETY = 1.01
# ... and by a metre more, for great_circle_m's own rounding: a zero radius
# scaled by any factor is still zero, but the distance to a point a few ulps
# from the centre can round to 0.0.
_BBOX_PAD_M = 1.0
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

    That is, within ``radius_m`` of its centre by :func:`great_circle_m`. A
    cheap bounding-box prefilter rejects most points before the
    trigonometry runs; the box is padded (``_BBOX_SAFETY``, ``_BBOX_PAD_M``)
    so it strictly encloses the circle. Its latitude half-height is the radius along a
    meridian. Its longitude half-width is the spherical cap's own,
    ``asin(sin r / cos lat)`` for an angular radius ``r``: wider than the
    radius over the centre's parallel (``r / cos lat``, which leaves out
    the cap's poleward bulge) for a large or high-latitude circle, and
    every longitude once the circle reaches a pole. The longitude
    comparison wraps around the antimeridian: a naive linear delta treats a
    zone near +180 and a point near -180 (or vice versa) as ~360 degrees
    apart, when the true angular separation may be a fraction of a degree.
    """
    padded_m = zone.radius_m * _BBOX_SAFETY + _BBOX_PAD_M
    dlat_deg = padded_m / _M_PER_DEG_LAT
    if latitude < zone.latitude - dlat_deg or latitude > zone.latitude + dlat_deg:
        return False
    dlon_deg = _longitude_half_width(zone.latitude, padded_m)
    if dlon_deg is not None:
        dlon = abs(longitude - zone.longitude)
        if dlon > _HALF_TURN_DEG:
            dlon = _FULL_TURN_DEG - dlon
        if dlon > dlon_deg:
            return False
    distance = great_circle_m(zone.latitude, zone.longitude, latitude, longitude)
    return distance <= zone.radius_m


def _longitude_half_width(latitude_deg: float, radius_m: float) -> float | None:
    """Return a circle's longitude half-width in degrees.

    None when it holds a pole (and so every longitude): its angular radius
    reaches past the centre's colatitude.
    """
    angular = radius_m / _EARTH_RADIUS_M
    latitude = math.radians(latitude_deg)
    if angular >= math.pi / 2 - abs(latitude):
        return None
    ratio = math.sin(angular) / math.cos(latitude)
    return None if ratio >= 1 else math.degrees(math.asin(ratio))


# -- Circle and polygon overlap ------------------------------------------------

type _Plane = tuple[float, float]


def _segment_distance(a: _Plane, b: _Plane) -> float:
    """Return the distance from the origin to the segment ab (planar).

    Past either end it is that end's distance, each end judged from its
    own vertex (``a . d > 0`` past a, ``b . d < 0`` past b), so a far
    vertex's magnitude never swamps a near one's. Alongside the segment it
    is the cross product ``p x d`` over the length, ``p`` the nearer vertex
    (the product is the same from either): exactly zero for an origin on an
    edge along an axis, where stepping to the foot of the perpendicular
    would leave a rounding residue that a zero radius misses, and as
    precise as the nearer vertex and ``d`` allow. A dot product of exactly
    zero (or one underflowing to zero) falls through to the cross product,
    which is then exact at a vertex too.
    """
    dx, dy = b[0] - a[0], b[1] - a[1]
    if dx == 0 and dy == 0:
        return math.hypot(*a)
    if a[0] * dx + a[1] * dy > 0:
        return math.hypot(*a)
    if b[0] * dx + b[1] * dy < 0:
        return math.hypot(*b)
    p = a if a[0] * a[0] + a[1] * a[1] <= b[0] * b[0] + b[1] * b[1] else b
    return abs(p[0] * dy - p[1] * dx) / math.hypot(dx, dy)


def _edges(ring: Sequence[_Plane]) -> Iterator[tuple[_Plane, _Plane]]:
    """Yield a closed ring's edges, each with its ends in a fixed order.

    The tests below round differently with an edge's direction (the
    crossing point ``x1 + (x2 - x1) * ...`` and the nearer-end tie), which
    matters only for a centre within rounding of the boundary -- but there
    a ring's start vertex or winding would change the answer. With each
    edge judged the same way round, the answer depends on the polygon's
    edges alone, not on how its ring is written.
    """
    for a, b in itertools.pairwise(ring):
        yield (a, b) if a <= b else (b, a)


def _encloses_origin(ring: Sequence[_Plane]) -> bool:
    """Return whether the origin is inside a closed planar ring (crossings)."""
    inside = False
    for (x1, y1), (x2, y2) in _edges(ring):
        if (y1 > 0) != (y2 > 0) and x1 + (x2 - x1) * (0 - y1) / (y2 - y1) > 0:
            inside = not inside
    return inside


def polygon_overlaps_circle(polygon: FlexPolygon, zone: Circle) -> bool:
    """Return whether a GTFS-Flex polygon and a circular zone share any point.

    GeoJSON joins positions with "a straight Cartesian line" in longitude
    and latitude, so the test runs in that plane, scaled to metres around
    the circle's centre (a degree of latitude is ``_M_PER_DEG_LAT``, of
    longitude that times the cosine of the centre's latitude). They overlap
    when the centre lies inside the polygon (inside its exterior and no
    hole) or any of its edges, holes' included, comes within the radius --
    so a boundary touching the circle counts. The scaling is exact at the
    centre and drifts with distance from it: well under 1% within a few
    kilometres outside polar latitudes.

    The polygon is moved whole by multiples of 360 degrees of longitude,
    so a zone just across the antimeridian is found without bending any
    edge: the copy whose middle is nearest the circle is tested, and for a
    zone over 180 degrees wide (such as one crossing the antimeridian
    unsplit, which RFC 7946 reads as spanning the long way) its neighbours
    a turn either way too.

    Meant for circles of up to a few hundred kilometres that do not reach a
    pole; beyond that the plane bends too far from the sphere. (Unlike
    :func:`in_circle`, which measures on the sphere at any radius.)
    """
    radius = zone.radius_m
    dlat = radius * _BBOX_SAFETY / _M_PER_DEG_LAT
    latitudes = [lat for lat, _lon in polygon.exterior]
    if max(latitudes) < zone.latitude - dlat or min(latitudes) > zone.latitude + dlat:
        return False  # wholly north or south of the circle
    scale = _M_PER_DEG_LAT * math.cos(math.radians(zone.latitude))
    longitudes = [lon for _lat, lon in polygon.exterior]
    # Anchored on the zone's middle longitude, not any one vertex: within
    # half a turn wide, every other copy then lies a quarter turn or more
    # from the circle, out of its reach.
    middle = (max(longitudes) + min(longitudes)) / 2
    nearest = _FULL_TURN_DEG * round((zone.longitude - middle) / _FULL_TURN_DEG)

    def overlaps(shift: float) -> bool:
        def plane(ring: Sequence[tuple[float, float]]) -> list[_Plane]:
            return [
                (
                    (lon + shift - zone.longitude) * scale,
                    (lat - zone.latitude) * _M_PER_DEG_LAT,
                )
                for lat, lon in ring
            ]

        rings = [plane(polygon.exterior), *(plane(hole) for hole in polygon.holes)]
        if any(
            _segment_distance(a, b) <= radius for ring in rings for a, b in _edges(ring)
        ):
            return True
        return _encloses_origin(rings[0]) and not any(
            _encloses_origin(hole) for hole in rings[1:]
        )

    # A zone wider than half a turn may reach the circle from either
    # neighbouring copy too.
    turns = (
        (0.0,)
        if max(longitudes) - min(longitudes) <= _HALF_TURN_DEG
        else (0.0, -_FULL_TURN_DEG, _FULL_TURN_DEG)
    )
    return any(overlaps(nearest + turn) for turn in turns)


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
    if o1 * o2 > 0:
        return False  # c and d strictly on one side of ab's line
    o3, o4 = _orientation(c, d, a), _orientation(c, d, b)
    if o3 * o4 > 0:
        return False
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
) -> list[tuple[int, int]]:
    """Return every grid cell the closed segment ab passes through.

    Column by column: the segment's y-range over each column's closed slab,
    computed exactly, gives the rows. A point of the segment lies in the
    cell of its own floor coordinates, which this always returns. With
    ``limit``, only cells with both indices in ``[0, limit)`` are walked.
    """
    (x1, y1), (x2, y2) = (a, b) if a <= b else (b, a)
    x0, y0 = origin
    top = None if limit is None else limit - 1
    if x1 == x2:
        column = (x1 - x0) // size
        first, last = (y1 - y0) // size, (y2 - y0) // size
        if top is not None:
            if not 0 <= column <= top:
                return []
            first, last = max(first, 0), min(last, top)
        return [(column, row) for row in range(first, last + 1)]
    dx, dy = x2 - x1, y2 - y1
    # floor((y(x) - y0) / size) with y(x) = y1 + dy (x - x1) / dx, exactly
    base, unit = (y1 - y0) * dx, dx * size
    first, last = (x1 - x0) // size, (x2 - x0) // size
    if top is not None:
        first, last = max(first, 0), min(last, top)
    cells = []
    for column in range(first, last + 1):
        left = x0 + column * size
        low = (base + dy * (max(x1, left) - x1)) // unit
        high = (base + dy * (min(x2, left + size) - x1)) // unit
        if low > high:
            low, high = high, low
        if top is not None:
            low, high = max(low, 0), min(high, top)
        cells += [(column, row) for row in range(low, high + 1)]
    return cells


# A cell holding more edges than this is split into a finer grid of its own
# (each side cut in _SUBDIVISION): detail clustered in a few cells of an
# otherwise coarse zone stays near-linear. Splitting stops at unit cells,
# _MAX_DEPTH levels down, where long edges run together, or once a crowded
# cell of the top grid has spent _BUDGET steps per pair of its edges; the
# edges of a cell not split are box-swept instead (see _split_pairs).
_CROWDED = 64
_SUBDIVISION = 8
_MAX_DEPTH = 12
_BUDGET = 4

type _Cell = tuple[int, int]
# A half-open box [x0, x1) x [y0, y1): a cell clipped to its ancestors.
type _Region = tuple[int, int, int, int]


def _pair_count(count: int) -> int:
    return count * (count - 1) // 2


class _Budget:
    """Steps left to split one crowded cell of the top grid."""

    __slots__ = ("left",)

    def __init__(self, left: float) -> None:
        self.left = left

    def spend(self, steps: int) -> None:
        self.left -= steps

    @property
    def spent(self) -> bool:
        return self.left < 0


def _meeting_point(
    a: _IntPoint, b: _IntPoint, c: _IntPoint, d: _IntPoint
) -> _IntPoint | None:
    """Return the floor of the one point where ab and cd meet, if any.

    For segments not parallel, their lines' crossing ``a + (b - a) t``
    (the only point they can share); along one line, the first (lowest x,
    then y) point of both, or None if they are apart there; None for
    parallel lines apart. Floored exactly, the crossing places the pair in
    the half-open cell holding it. Whether the segments really meet is left
    to the caller.
    """
    dx1, dy1 = b[0] - a[0], b[1] - a[1]
    dx2, dy2 = d[0] - c[0], d[1] - c[1]
    den = dx1 * dy2 - dy1 * dx2
    if den:
        num = (c[0] - a[0]) * dy2 - (c[1] - a[1]) * dx2
        return a[0] + dx1 * num // den, a[1] + dy1 * num // den
    if _orientation(a, b, c) or _orientation(c, d, a):
        return None
    first, last = max(min(a, b), min(c, d)), min(max(a, b), max(c, d))
    return first if first <= last else None


def _within(point: _IntPoint, region: _Region) -> bool:
    return region[0] <= point[0] < region[2] and region[1] <= point[1] < region[3]


def _buckets(
    edges: list[_Edge],
    members: Iterable[int],
    origin: _IntPoint,
    size: int,
    limit: int | None,
) -> tuple[dict[_Cell, list[int]], dict[int, list[_Cell]]]:
    """Return the ``members`` in each cell they cross, and each one's cells."""
    buckets: defaultdict[_Cell, list[int]] = defaultdict(list)
    cells_of: dict[int, list[_Cell]] = {}
    for index in members:
        _r, _i, a, b = edges[index]
        cells_of[index] = cells = _cells(a, b, origin, size, limit)
        for cell in cells:
            buckets[cell].append(index)
    return buckets, cells_of


def _cell_of(
    point: _IntPoint, origin: _IntPoint, size: int, clip: _Region | None
) -> _Cell | None:
    """Return the cell of this grid holding ``point``, if within ``clip``."""
    if clip is not None and not _within(point, clip):
        return None
    return (point[0] - origin[0]) // size, (point[1] - origin[1]) // size


def _meets(edges: list[_Edge], first: int, second: int) -> _IntPoint | None:
    """Return two edges' meeting point (floored), or None if they do not meet."""
    _r, _i, a, b = edges[first]
    _r, _i, c, d = edges[second]
    return _meeting_point(a, b, c, d) if _touch(a, b, c, d) else None


def _leaf_pairs(
    edges: list[_Edge],
    buckets: dict[_Cell, list[int]],
    cells_of: dict[int, list[_Cell]],
    leaves: set[_Cell],
    grid: tuple[_IntPoint, int, _Region | None],
    *,
    budget: _Budget | None,
) -> Iterator[tuple[int, int]]:
    """Yield the pairs meeting in a leaf cell (one not split) of this grid.

    Each edge walks its leaf cells, marking each partner the first time it
    is met (``partner``, linear in the edges), so a pair sharing many
    cells is weighed once here.
    """
    partner: dict[int, int] = {}
    steps = 0
    for i, cells in cells_of.items():
        _r, _i, (ax, ay), (bx, by) = edges[i]
        dx, dy = bx - ax, by - ay
        for cell in cells:
            if cell not in leaves:
                continue
            inside = buckets[cell]  # ascending, as the members
            later = inside[bisect.bisect_right(inside, i) :]
            steps += len(later) + 1
            for j in later:
                if partner.get(j) == i:
                    continue
                partner[j] = i
                # _touch's first test, inline: j wholly to one side of i's line
                _r, _i, (cx, cy), (ex, ey) = edges[j]
                o1 = dx * (cy - ay) - dy * (cx - ax)
                o2 = dx * (ey - ay) - dy * (ex - ax)
                if (o1 > 0 and o2 > 0) or (o1 < 0 and o2 < 0):
                    continue
                point = _meets(edges, i, j)
                if point is not None and _cell_of(point, *grid) in leaves:
                    yield i, j
    if budget is not None:
        budget.spend(steps)


def _swept_pairs(
    edges: list[_Edge],
    boxes: list[_Region],
    members: Iterable[int],
    accept: Callable[[_IntPoint], bool],
    budget: _Budget | None = None,
) -> Iterator[tuple[int, int]]:
    """Yield the pairs of ``members`` meeting at a point ``accept`` takes.

    Swept by box, as _holes_overlap: sorted by left side, each edge is
    compared with those not yet ended to its left. Edges that meet share a
    point, so their boxes overlap. O(m log m) plus the comparisons, at most
    one per pair: near-linear for long thin edges side by side (a fine
    zigzag), quadratic only where the boxes themselves pile up.
    """
    steps = 0
    active: list[int] = []
    for k in sorted(members, key=lambda k: boxes[k][0]):
        x0, y0, _x1, y1 = boxes[k]
        _r, _i, (ax, ay), (bx, by) = edges[k]
        dx, dy = bx - ax, by - ay
        active = [g for g in active if boxes[g][2] >= x0]
        steps += len(active) + 1
        for g in active:
            if boxes[g][1] > y1 or y0 > boxes[g][3]:
                continue
            # _touch's first test, inline: g wholly to one side of k's line
            _r, _i, (cx, cy), (ex, ey) = edges[g]
            o1 = dx * (cy - ay) - dy * (cx - ax)
            o2 = dx * (ey - ay) - dy * (ex - ax)
            if (o1 > 0 and o2 > 0) or (o1 < 0 and o2 < 0):
                continue
            point = _meets(edges, g, k)
            if point is not None and accept(point):
                yield (g, k) if g < k else (k, g)
        active.append(k)
    if budget is not None:
        budget.spend(steps)


class _OutOfBudgetError(Exception):
    """A crowded cell's split ran out of budget (see _split_pairs)."""


# One level of a split in progress: its grid, its sub-cells done (meeting
# points there all yielded), and the sub-cell being split, if any.
type _Level = tuple[tuple[_IntPoint, int, _Region], set[_Cell], list[_Cell | None]]


def _done(point: _IntPoint, path: list[_Level]) -> bool:
    """Return whether the split had yielded the pairs meeting at ``point``."""
    for grid, done, current in path:
        cell = _cell_of(point, *grid)
        if cell in done:
            return True
        if cell != current[0]:
            return False
    return False


def _split_pairs(  # noqa: PLR0917
    edges: list[_Edge],
    boxes: list[_Region],
    members: list[int],
    corner: _IntPoint,
    size: int,
    clip: _Region,
    budget: _Budget,
    path: list[_Level],
) -> Iterator[tuple[int, int]]:
    """Yield the pairs of a crowded cell's ``members`` meeting in ``clip``.

    The cell (``size`` wide at ``corner``) is re-bucketed on a finer grid;
    two edges meeting in the cell meet in the sub-cell holding that point.
    A crowded sub-cell is split again, unless it is a unit cell,
    ``_MAX_DEPTH`` levels down, or one of several sub-cells each holding
    over half of this cell's edges: long, nearly parallel edges running
    together, which every finer grid would split the same way again,
    multiplying the work at each level. (One such sub-cell alone is split:
    edges converging on a point, a sunburst, separate further in.) The
    sub-cells not split are box-swept together, once.

    Every step is charged to ``budget``; once it is spent, the next split
    or sweep raises _OutOfBudgetError instead, leaving ``path`` to tell
    which sub-cells are done.
    """
    sub = -(-size // _SUBDIVISION)
    grid = (corner, sub, clip)
    buckets, cells_of = _buckets(edges, members, corner, sub, -(-size // sub))
    budget.spend(sum(len(cells) for cells in cells_of.values()))
    leaves = {cell for cell, inside in buckets.items() if len(inside) <= _CROWDED}
    current: list[_Cell | None] = [None]
    path.append((grid, set(leaves), current))
    yield from _leaf_pairs(edges, buckets, cells_of, leaves, grid, budget=budget)
    done = path[-1][1]
    dominant = sum(1 for inside in buckets.values() if 2 * len(inside) > len(members))
    swept: set[_Cell] = set()
    for (column, row), inside in buckets.items():
        x, y = corner[0] + column * sub, corner[1] + row * sub
        region = (
            max(clip[0], x),
            max(clip[1], y),
            min(clip[2], x + sub),
            min(clip[3], y + sub),
        )
        if (column, row) in leaves or region[0] >= region[2] or region[1] >= region[3]:
            continue
        if not (
            sub > 1
            and len(path) < _MAX_DEPTH
            and (dominant == 1 or 2 * len(inside) <= len(members))
        ):
            swept.add((column, row))
            continue
        if budget.spent:
            raise _OutOfBudgetError
        current[0] = column, row
        yield from _split_pairs(edges, boxes, inside, (x, y), sub, region, budget, path)
        current[0] = None
        done.add((column, row))
    if swept:
        if budget.spent:
            raise _OutOfBudgetError
        members_swept = {k for cell in swept for k in buckets[cell]}
        yield from _swept_pairs(
            edges,
            boxes,
            members_swept,
            lambda point: _cell_of(point, *grid) in swept,
            budget,
        )
    path.pop()


def _crowded_pairs(
    edges: list[_Edge], boxes: list[_Region], members: list[int], region: _Region
) -> Iterator[tuple[int, int]]:
    """Yield the pairs of a crowded top-grid cell's ``members`` meeting in it.

    Split (_split_pairs) within a budget of ``_BUDGET * m * (m - 1) / 2``
    steps for m edges -- cell visits, partners weighed, box comparisons --
    and, if that runs out, finished by one box sweep of the whole cell,
    which yields only the pairs meeting where the split had not got to.
    The step that spends the budget overruns it by one sweep at most, so
    the cell costs O(m^2) in all, whatever the coordinates' magnitude or
    precision.
    """
    budget = _Budget(_BUDGET * _pair_count(len(members)))
    path: list[_Level] = []
    size = region[2] - region[0]
    try:
        yield from _split_pairs(
            edges, boxes, members, region[:2], size, region, budget, path
        )
    except _OutOfBudgetError:
        yield from _swept_pairs(
            edges,
            boxes,
            members,
            lambda point: _within(point, region) and not _done(point, path),
        )


def _candidate_pairs(edges: list[_Edge]) -> Iterator[tuple[int, int]]:
    """Yield index pairs ``(i, j)``, ``i < j``, of the edges that meet.

    Each meeting pair exactly once, and no other. Edges are bucketed on a
    grid, each in just the cells its line crosses; a crowded cell is split
    finer or box-swept (_split_pairs). Two edges that meet share the cell
    holding their meeting point (_meeting_point, one per pair) and are
    yielded from that cell alone, so no set of pairs is kept.

    Cells are the size of the median edge, coarsened so the edges cover
    about 8n cells in all and so no more than about 4n fit across the zone:
    memory stays linear in the edge count (a split holds only the cells of
    the one cell it splits, at most _MAX_DEPTH deep). Time is about the
    edge count for evenly spread vertices, with or without a few long
    edges or a cluster of fine detail. It is more where many edges converge
    on one point (a sunburst of spikes) or run together (a bundle of
    nearly parallel edges), which no grid separates: up to quadratic --
    seconds for a few thousand such edges, minutes for tens of thousands
    -- but no worse. A crowded cell of m edges costs O(m^2)
    (_crowded_pairs), and the cells' m^2 sum to O(n^2) over the O(n) cell
    memberships.
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
    grid = (origin, size, None)
    boxes = [_box((a, b)) for *_, a, b in edges]
    buckets, cells_of = _buckets(edges, range(len(edges)), origin, size, None)
    leaves = {cell for cell, inside in buckets.items() if len(inside) <= _CROWDED}
    yield from _leaf_pairs(edges, buckets, cells_of, leaves, grid, budget=None)
    swept: set[_Cell] = set()
    for (column, row), inside in buckets.items():
        if (column, row) in leaves:
            continue
        if size == 1:
            swept.add((column, row))
            continue
        x, y = origin[0] + column * size, origin[1] + row * size
        yield from _crowded_pairs(edges, boxes, inside, (x, y, x + size, y + size))
    if swept:
        members = {k for cell in swept for k in buckets[cell]}
        yield from _swept_pairs(
            edges,
            boxes,
            members,
            lambda point: _cell_of(point, *grid) in swept,
        )


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
