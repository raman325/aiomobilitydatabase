"""Tests for circular-zone geometry."""

import itertools
import math
import time
from fractions import Fraction
from typing import Any
from unittest.mock import patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds import geo
from aiomobilitydatabase.feeds.geo import (
    _EARTH_RADIUS_M,
    Circle,
    _candidate_pairs,
    _exact_rings,
    _orientation,
    _positions,
    _touch,
    great_circle_m,
    in_circle,
    polygon_is_valid,
)

LA = (34.0522, -118.2437)


def test_great_circle_known_distance() -> None:
    # LA City Hall to a nearby point is roughly 650 m
    dist = great_circle_m(34.0537, -118.2428, 34.0562, -118.2365)
    assert 550 < dist < 700  # sanity band for this short hop


def test_in_circle_inside_and_outside() -> None:
    zone = Circle(latitude=LA[0], longitude=LA[1], radius_m=1000.0)
    assert in_circle(zone, LA[0] + 0.001, LA[1])  # ~111 m north
    assert not in_circle(zone, LA[0] + 0.1, LA[1])  # ~11 km north


def test_in_circle_bbox_prefilter_excludes_cheaply() -> None:
    zone = Circle(latitude=0.0, longitude=0.0, radius_m=1000.0)
    # Far outside the enclosing bbox: must be rejected (by prefilter or distance).
    assert not in_circle(zone, 1.0, 1.0)


def test_in_circle_edge_on_boundary() -> None:
    zone = Circle(latitude=0.0, longitude=0.0, radius_m=111_320.0)
    # 1.00056 deg north is ~111,257 m away: inside the radius, and inside the
    # narrow band where an under-sized prefilter box wrongly excludes points.
    assert in_circle(zone, 1.00056, 0.0)
    assert not in_circle(zone, 1.01, 0.0)


def test_in_circle_boundary_high_latitude() -> None:
    zone = Circle(latitude=89.0, longitude=0.0, radius_m=50_000.0)
    # Same-latitude point ~49,620 m away (true great-circle distance): inside.
    assert in_circle(zone, 89.0, 25.786)
    assert not in_circle(zone, 89.0, 27.0)


def test_near_antipodal_distance_is_not_saturated() -> None:
    """Haversine saturates at half the circumference near the antipode.

    (0, 0) and (1e-06, 180) are 1e-06 degrees of latitude short of exactly
    antipodal -- about 0.111 m. Haversine's asin(sqrt(a)) has a -> 1 there,
    where asin's derivative diverges, so it reports exactly half the
    circumference and loses that 0.111 m. The consequence is not just
    imprecision: the reported distance EXCEEDS the true one, which breaks
    the triangle inequality (pinned by the metric-law property).
    """
    half_circumference = math.pi * _EARTH_RADIUS_M
    measured = great_circle_m(0.0, 0.0, 1e-06, 180.0)
    assert measured < half_circumference
    # The shortfall is the 1e-06 degrees of latitude separating the points
    # from a true antipode.
    assert half_circumference - measured == pytest.approx(0.111, abs=1e-3)


def test_matches_haversine_at_transit_scale() -> None:
    """Switching formulas must not move any distance this library queries.

    Haversine is accurate for short separations; the atan2 form was chosen
    for the far end of the range, so the near end has to be unchanged.
    """
    for lat, lon, dlat, dlon in (
        (34.05, -118.25, 0.01, 0.0),
        (34.05, -118.25, 0.0, 0.01),
        (59.91, 10.75, 0.3, 0.4),
        (-33.87, 151.21, 0.45, -0.45),
    ):
        a = (lat + dlat) / 1  # keep the arithmetic explicit
        phi1, phi2 = math.radians(lat), math.radians(a)
        dphi, dlambda = math.radians(dlat), math.radians(dlon)
        inner = (
            math.sin(dphi / 2) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        )
        haversine = 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(inner))
        assert great_circle_m(lat, lon, lat + dlat, lon + dlon) == pytest.approx(
            haversine, abs=1e-6
        )


# -- Polygon validity (GTFS-Flex: OGC Simple Features 6.1.11) -----------------


def _ring(*points: tuple[float, float]) -> list[tuple[float, float]]:
    return [*points, points[0]]


_SQUARE = _ring((0, 0), (4, 0), (4, 4), (0, 4))


@pytest.mark.parametrize(
    ("rings", "valid"),
    [
        pytest.param([_SQUARE], True, id="square"),
        pytest.param([_SQUARE[::-1]], True, id="square, other winding"),
        pytest.param(
            [_ring((0, 0), (4, 0), (4, 0), (4, 4), (0, 4))], True, id="repeated point"
        ),
        pytest.param([_ring((0, 0), (4, 4), (4, 0), (0, 4))], False, id="bowtie"),
        pytest.param(
            [_ring((0, 0), (2, 2), (4, 0), (4, 4), (2, 2), (0, 4))],
            False,
            id="ring touching itself at a vertex",
        ),
        pytest.param(
            [_ring((0, 0), (4, 0), (6, 0), (4, 0), (4, 4), (0, 4))],
            False,
            id="spike",
        ),
        pytest.param([_ring((0, 0), (2, 0), (4, 0))], False, id="collinear"),
        pytest.param([_ring((0, 0), (4, 0))], False, id="two vertices"),
        pytest.param(
            [_SQUARE, _ring((1, 1), (1, 2), (2, 2), (2, 1))], True, id="hole inside"
        ),
        pytest.param(
            [_SQUARE, _ring((5, 5), (5, 6), (6, 6), (6, 5))], False, id="hole outside"
        ),
        pytest.param(
            [_SQUARE, _ring((3, 1), (3, 2), (5, 2), (5, 1))],
            False,
            id="hole crossing the exterior",
        ),
        pytest.param(
            [_SQUARE, _ring((0, 1), (1, 1), (1, 2), (0, 2))],
            False,
            id="hole along the exterior",
        ),
        pytest.param(
            [_SQUARE, _ring((0, 0), (1, 2), (2, 1))],
            True,
            id="hole meeting the exterior at a shared vertex",
        ),
        pytest.param(
            [_SQUARE, _ring((0, 2), (1, 3), (1, 1))],
            False,
            id="hole touching an exterior edge's middle (refused, see docstring)",
        ),
        pytest.param(
            [_SQUARE, _ring((1, 1), (4, 4), (5, 2))],
            False,
            id="hole leaving through an exterior vertex",
        ),
        pytest.param(
            [
                _SQUARE,
                _ring((1, 1), (1, 3), (2, 3), (2, 1)),
                _ring((1.5, 1.5), (1.5, 2.5), (3, 2.5), (3, 1.5)),
            ],
            False,
            id="overlapping holes",
        ),
        pytest.param(
            [
                _SQUARE,
                _ring((0.5, 0.5), (0.5, 3.5), (3.5, 3.5), (3.5, 0.5)),
                _ring((1, 1), (1, 2), (2, 2), (2, 1)),
            ],
            False,
            id="a hole inside a hole",
        ),
        pytest.param(
            [
                _SQUARE,
                _ring((1, 1), (1, 2), (2, 2), (2, 1)),
                _ring((2, 2), (2, 3), (3, 3), (3, 2)),
            ],
            True,
            id="holes meeting at a shared vertex",
        ),
        pytest.param([_SQUARE, _SQUARE], False, id="hole over the exterior"),
        pytest.param(
            [_SQUARE, _ring((0, 0), (4, 4), (3, 1))],
            False,
            id="hole meeting the exterior twice (interior cut in two)",
        ),
        pytest.param(
            [
                _SQUARE,
                _ring((0, 0), (2, 1), (1, 2)),
                _ring((2, 1), (4, 4), (3, 1)),
            ],
            False,
            id="holes chained from the exterior and back (interior cut)",
        ),
        pytest.param(
            [
                _SQUARE,
                _ring((0, 0), (2, 1), (1, 2)),
                _ring((2, 1), (3, 3), (3, 1)),
            ],
            True,
            id="holes chained from the exterior, not back",
        ),
        pytest.param([], False, id="no rings"),
        pytest.param(
            [
                _ring((-10, -10), (10, -10), (10, 10), (-10, 10)),
                _ring((1, 1), (2, 1.5), (1.5, 2)),
                _ring((1, 1), (3, 1), (3, 3), (1, 3)),
            ],
            False,
            id="a hole inside a hole listed after it, same leftmost x",
        ),
        pytest.param(
            [
                _ring((-10, -10), (10, -10), (10, 10), (-10, 10)),
                _ring((0, 0), (2, 0), (2, 2), (1, 5), (0, 2)),
                _ring((0, 0), (2, 2), (3, -1)),
            ],
            False,
            id="holes crossing at two shared vertices, boxes not nested",
        ),
        pytest.param(
            [
                _ring((1.1, 1.3), (4.1, 4.3), (4.1, -1.6999999999999995)),
                _ring(
                    (1.1, 1.3), (2.0999999999999996, 2.3), (0.10000000000000009, 3.3)
                ),
            ],
            False,
            id="hole just outside, its float midpoint just inside",
        ),
        pytest.param(
            [
                _ring(
                    (-5.752656294961473e-155, 1.3720070901607009e-155),
                    (1.9413805967715086e-155, -3.709526842205869e-155),
                    (-2.8819125348167282e-155, -5.239783377203719e-156),
                    (-5.422679501000013e-155, -4.370996783586863e-155),
                )
            ],
            False,
            id="bowtie at 1e-155 (float products underflow)",
        ),
        pytest.param(
            [_ring((1, 1), (1, 2), (2, 2), (2, 1)), _SQUARE],
            False,
            id="hole around the exterior",
        ),
    ],
)
def test_polygon_validity(rings: list[list[tuple[float, float]]], valid: bool) -> None:
    assert polygon_is_valid(rings) is valid


def _circle(count: int, radius: float = 1.0) -> list[tuple[float, float]]:
    return _ring(
        *(
            (
                radius * math.cos(2 * math.pi * k / count),
                radius * math.sin(2 * math.pi * k / count),
            )
            for k in range(count)
        )
    )


def _edges(rings: list[list[tuple[float, float]]]) -> list[tuple[Any, ...]]:
    exact = _exact_rings(rings)
    return [
        (r, i, ring[i], ring[i + 1])
        for r, ring in enumerate(exact)
        for i in range(len(ring) - 1)
    ]


def test_a_large_zone_validates_in_linear_time() -> None:
    """A 20,000-vertex ring checks in a fraction of a second, not minutes:
    the grid keeps candidate pairs near the edge count."""
    ring = _circle(20_000)
    assert sum(1 for _ in _candidate_pairs(_edges([ring]))) < 10 * 20_000
    assert polygon_is_valid([ring])


def test_coarse_holes_in_a_detailed_exterior_stay_near_linear() -> None:
    """Long hole edges among short exterior ones each sit only in the
    cells they cross, not paired with every edge."""
    holes = [
        _ring(
            (x / 10, -0.05),
            (x / 10 + 0.05, -0.05),
            (x / 10 + 0.05, 0.05),
            (x / 10, 0.05),
        )
        for x in range(-5, 5)
    ]
    rings = [_circle(20_000), *holes]
    assert sum(1 for _ in _candidate_pairs(_edges(rings))) < 10 * 20_000
    assert polygon_is_valid(rings)


_COORDINATES = st.one_of(
    st.floats(-1e3, 1e3, allow_nan=False), st.integers(-4, 4).map(float)
)
_POINTS = st.tuples(_COORDINATES, _COORDINATES)


_TINY_POINTS = st.tuples(_POINTS, st.integers(0, 320)).map(
    lambda point_scale: (
        point_scale[0][0] * 10.0 ** -point_scale[1],
        point_scale[0][1] * 10.0 ** -point_scale[1],
    )
)


def _exact_sign(
    a: tuple[float, float], b: tuple[float, float], c: tuple[float, float]
) -> int:
    ax, ay = Fraction(a[0]), Fraction(a[1])
    det = (Fraction(b[0]) - ax) * (Fraction(c[1]) - ay) - (Fraction(b[1]) - ay) * (
        Fraction(c[0]) - ax
    )
    return (det > 0) - (det < 0)


@given(
    a=st.one_of(_POINTS, _TINY_POINTS),
    b=st.one_of(_POINTS, _TINY_POINTS),
    t=st.floats(-2, 2),
    jitter=st.sampled_from([0.0, 1e-15, -1e-12, 1e-300]),
)
@settings(max_examples=300)
def test_orientation_is_exact(
    a: tuple[float, float], b: tuple[float, float], t: float, jitter: float
) -> None:
    """Scaled to integers, a triple orients as its exact rationals do: for a
    point computed onto (or a rounding error off) the line through a and b,
    and at scales where float products underflow."""
    c = (a[0] + t * (b[0] - a[0]) + jitter, a[1] + t * (b[1] - a[1]))
    [[ia, ib, ic]] = _exact_rings([[a, b, c]])
    assert _orientation(ia, ib, ic) == _exact_sign(a, b, c)


@given(
    segments=st.lists(
        st.one_of(st.tuples(_POINTS, _POINTS), st.tuples(_TINY_POINTS, _POINTS)),
        min_size=2,
        max_size=12,
    ),
    flat=st.booleans(),
)
@settings(max_examples=200)
def test_every_touching_pair_is_a_candidate(
    segments: list[tuple[tuple[float, float], tuple[float, float]]], flat: bool
) -> None:
    """The grid never loses a pair of segments that meet -- including when
    every segment lies on one vertical line (a zero-width grid)."""
    if flat:
        segments = [((0.0, a[1]), (0.0, b[1])) for a, b in segments]
    [points] = _exact_rings([[point for segment in segments for point in segment]])
    exact = list(zip(points[::2], points[1::2], strict=True))
    edges = [(0, i, a, b) for i, (a, b) in enumerate(exact)]
    candidates = set(_candidate_pairs(edges))
    for i in range(len(exact)):
        for j in range(i + 1, len(exact)):
            if _touch(*exact[i], *exact[j]):
                assert (i, j) in candidates


def _int_cross(o: tuple[int, int], a: tuple[int, int], b: tuple[int, int]) -> int:
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def _int_on(p: tuple[int, int], a: tuple[int, int], b: tuple[int, int]) -> bool:
    return (
        _int_cross(a, b, p) == 0
        and min(a[0], b[0]) <= p[0] <= max(a[0], b[0])
        and min(a[1], b[1]) <= p[1] <= max(a[1], b[1])
    )


def _int_meet(
    a: tuple[int, int], b: tuple[int, int], c: tuple[int, int], d: tuple[int, int]
) -> bool:
    d1, d2 = _int_cross(a, b, c), _int_cross(a, b, d)
    d3, d4 = _int_cross(c, d, a), _int_cross(c, d, b)
    if ((d1 > 0) != (d2 > 0) and d1 * d2 != 0) and (
        (d3 > 0) != (d4 > 0) and d3 * d4 != 0
    ):
        return True
    return _int_on(c, a, b) or _int_on(d, a, b) or _int_on(a, c, d) or _int_on(b, c, d)


def _simple_by_brute_force(points: list[tuple[int, int]]) -> bool:
    """An independent oracle on integer rings: a simple ring has three or
    more distinct vertices, neighbours meeting only at their shared vertex,
    and other edges not meeting at all."""
    ring = [p for i, p in enumerate(points) if i == 0 or p != points[i - 1]]
    if ring[-1] == ring[0]:
        ring.pop()
    n = len(ring)
    if n < 3 or len(set(ring)) < n:
        return False
    edges = [(ring[i], ring[(i + 1) % n]) for i in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            (a, b), (c, d) = edges[i], edges[j]
            if j == i + 1 or (i == 0 and j == n - 1):
                shared, u, v = (b, a, d) if j == i + 1 else (a, b, c)
                # neighbours: collinear and the same way from the shared vertex
                if (
                    _int_cross(shared, u, v) == 0
                    and (u[0] - shared[0]) * (v[0] - shared[0])
                    + (u[1] - shared[1]) * (v[1] - shared[1])
                    > 0
                ):
                    return False
            elif _int_meet(a, b, c, d):
                return False
    return True


_SMALL = st.tuples(st.integers(0, 4), st.integers(0, 4))


@given(points=st.lists(_SMALL, min_size=3, max_size=8), shift=st.integers(0, 7))
@settings(max_examples=400)
def test_a_ring_is_valid_iff_it_is_simple(
    points: list[tuple[int, int]], shift: int
) -> None:
    """On small integer rings (crowded with touches and collinear runs),
    the check equals a brute-force simplicity oracle, whatever the start
    vertex or winding."""
    if points[0] != points[-1]:
        points = [*points, points[0]]
    expected = _simple_by_brute_force(points)
    floats = [(float(x), float(y)) for x, y in points]
    body = floats[:-1]
    shift %= len(body)
    rotated = body[shift:] + body[:shift]
    for ring in ([*rotated, rotated[0]], [*rotated[::-1], rotated[-1]]):
        assert polygon_is_valid([ring]) is expected


@given(
    count=st.integers(3, 12),
    scale=st.floats(0.05, 0.5),
    turn=st.floats(0, 2 * math.pi),
)
def test_a_star_with_a_shrunk_copy_inside_is_valid_and_around_it_is_not(
    count: int, scale: float, turn: float
) -> None:
    """A regular polygon is valid; with a copy shrunk about its centre as a
    hole still valid; with a copy grown about it as the hole, not."""
    corners = [
        (
            math.cos(turn + 2 * math.pi * k / count),
            math.sin(turn + 2 * math.pi * k / count),
        )
        for k in range(count)
    ]
    exterior = _ring(*corners)
    inner = _ring(*((x * scale, y * scale) for x, y in corners))
    outer = _ring(*((x * 2, y * 2) for x, y in corners))
    assert polygon_is_valid([exterior])
    assert polygon_is_valid([exterior, inner])
    assert not polygon_is_valid([exterior, outer])


@given(a=_SMALL, b=_SMALL, c=_SMALL, d=_SMALL)
@settings(max_examples=500)
def test_touch_equals_the_integer_oracle(
    a: tuple[int, int], b: tuple[int, int], c: tuple[int, int], d: tuple[int, int]
) -> None:
    """Closed segments meet -- crossing, an end on the other, or overlapping
    -- exactly as exact integer arithmetic says, whichever order they come in."""
    expected = _int_meet(a, b, c, d)
    assert _touch(a, b, c, d) is expected
    assert _touch(c, d, a, b) is expected


def test_a_vanishingly_short_edge_does_not_shrink_the_grid() -> None:
    """Edges a subnormal long -- most of them, so the median is one -- must
    not make every other edge span an astronomical number of cells (an
    overflow, or hours of bucketing)."""
    tiny = [(k * 5e-324, 0.0) for k in range(6)]
    ring = _ring(*tiny, (1.0, 0.0), (1.0, 1.0), (0.0, 1.0))
    assert polygon_is_valid([ring])
    edges = _edges([ring])
    assert len(set(_candidate_pairs(edges))) <= len(edges) * (len(edges) - 1) // 2


@given(
    rings=st.lists(
        st.lists(st.one_of(_POINTS, _TINY_POINTS), min_size=1, max_size=5),
        min_size=1,
        max_size=3,
    )
)
@settings(max_examples=200)
def test_exact_rings_are_even_integers_in_proportion(
    rings: list[list[tuple[float, float]]],
) -> None:
    """Every coordinate scales to an even integer (so midpoints stay exact)
    by one common factor, exactly."""
    exact = _exact_rings(rings)
    pairs = [
        (value, scaled)
        for ring, scaled_ring in zip(rings, exact, strict=True)
        for point, scaled_point in zip(ring, scaled_ring, strict=True)
        for value, scaled in zip(point, scaled_point, strict=True)
    ]
    assert all(isinstance(scaled, int) and scaled % 2 == 0 for _, scaled in pairs)
    nonzero = [(value, scaled) for value, scaled in pairs if value]
    if nonzero:
        factor = Fraction(nonzero[0][1]) / Fraction(nonzero[0][0])
        assert all(
            Fraction(scaled) == Fraction(value) * factor for value, scaled in pairs
        )


def test_many_holes_are_compared_by_sweep_not_all_pairs() -> None:
    """A lattice of 2,500 holes checks in well under a second each way:
    holes are compared only with those whose boxes overlap."""
    side = 50
    holes = [
        _ring(
            (i + 0.25, j + 0.25),
            (i + 0.75, j + 0.25),
            (i + 0.75, j + 0.75),
            (i + 0.25, j + 0.75),
        )
        for i in range(side)
        for j in range(side)
    ]
    exterior = _ring((0, 0), (side, 0), (side, side), (0, side))
    started = time.perf_counter()
    assert polygon_is_valid([exterior, *holes])
    assert time.perf_counter() - started < 10


def test_fine_detail_in_a_coarse_zone_stays_near_linear() -> None:
    """A dense run of tiny edges inside one coarse cell is split finer,
    not paired all against all."""
    coarse, fine = 3000, 2000
    points = [
        (10 * math.cos(math.pi * k / coarse), 10 * math.sin(math.pi * k / coarse))
        for k in range(coarse + 1)
    ]
    points += [(-0.005, -0.001)]
    points += [
        (-0.005 + 0.01 * k / fine, -0.001 - (0.0005 if k % 2 else 0))
        for k in range(fine)
    ]
    points += [(0.005, -0.001)]
    ring = [*points, points[0]]
    assert sum(1 for _ in _candidate_pairs(_edges([ring]))) < 50 * len(ring)
    assert polygon_is_valid([ring])


@given(
    segments=st.lists(
        st.tuples(_SMALL, _SMALL).filter(lambda ab: ab[0] != ab[1]),
        min_size=2,
        max_size=30,
    )
)
@settings(max_examples=200)
def test_split_cells_never_lose_a_touching_pair(
    segments: list[tuple[tuple[int, int], tuple[int, int]]],
) -> None:
    """With every cell of more than two edges split finer, still every
    pair of segments that meet is a candidate."""
    [points] = _exact_rings([[point for segment in segments for point in segment]])
    exact = list(zip(points[::2], points[1::2], strict=True))
    edges = [(0, i, a, b) for i, (a, b) in enumerate(exact)]
    with patch.object(geo, "_CROWDED", 2):
        candidates = set(_candidate_pairs(edges))
    for i in range(len(exact)):
        for j in range(i + 1, len(exact)):
            if _touch(*exact[i], *exact[j]):
                assert (i, j) in candidates


def _position_by_brute_force(p: tuple[int, int], ring: list[tuple[int, int]]) -> int:
    inside = False
    for a, b in itertools.pairwise(ring):
        if (
            _orientation(a, b, p) == 0
            and min(a[0], b[0]) <= p[0] <= max(a[0], b[0])
            and min(a[1], b[1]) <= p[1] <= max(a[1], b[1])
        ):
            return 0
        if (a[1] > p[1]) != (b[1] > p[1]) and (_orientation(a, b, p) > 0) == (
            b[1] > a[1]
        ):
            inside = not inside
    return 1 if inside else -1


@given(
    ring=st.lists(_SMALL, min_size=3, max_size=8),
    points=st.lists(_SMALL, max_size=12),
)
@settings(max_examples=300)
def test_one_pass_places_points_as_one_at_a_time(
    ring: list[tuple[int, int]], points: list[tuple[int, int]]
) -> None:
    """The batched ray cast places every point as a per-point cast does."""
    closed = [*ring, ring[0]]
    assert _positions(points, closed) == [
        _position_by_brute_force(point, closed) for point in points
    ]
