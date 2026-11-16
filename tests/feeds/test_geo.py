"""Tests for circular-zone geometry."""

import contextlib
import decimal
import itertools
import math
import sys
import time
import tracemalloc
from fractions import Fraction
from typing import Any
from unittest.mock import patch

import pytest
from hypothesis import assume, example, given, settings
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
    polygon_overlaps_circle,
)
from aiomobilitydatabase.feeds.models import FlexPolygon

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


_WIDE = st.tuples(st.integers(0, 300), st.integers(0, 300))


@given(
    segments=st.lists(
        st.one_of(st.tuples(_SMALL, _SMALL), st.tuples(_WIDE, _WIDE)).filter(
            lambda ab: ab[0] != ab[1]
        ),
        min_size=2,
        max_size=30,
    ),
    split=st.sampled_from([2, 3, 8]),
)
@settings(max_examples=300)
def test_split_cells_never_lose_a_touching_pair(
    segments: list[tuple[tuple[int, int], tuple[int, int]]], split: int
) -> None:
    """With every cell of more than two edges split finer (in halves,
    thirds or eighths, so sub-cells overhang their parent), still every
    pair of segments that meet is a candidate."""
    [points] = _exact_rings([[point for segment in segments for point in segment]])
    exact = list(zip(points[::2], points[1::2], strict=True))
    edges = [(0, i, a, b) for i, (a, b) in enumerate(exact)]
    with patch.object(geo, "_CROWDED", 2), patch.object(geo, "_SUBDIVISION", split):
        candidates = set(_candidate_pairs(edges))
    for i in range(len(exact)):
        for j in range(i + 1, len(exact)):
            if _touch(*exact[i], *exact[j]):
                assert (i, j) in candidates


def _zigzag(edges: int, gap: float) -> list[tuple[float, float]]:
    """Teeth of height 1, ``gap`` apart, closed by a square base below."""
    points = [(k * gap, float(k % 2)) for k in range(edges - 2)]
    return _ring(*points, ((edges - 3) * gap, -1.0), (0.0, -1.0))


def _star(spikes: int, inner: float) -> list[tuple[float, float]]:
    """Spikes reaching radius 1, their valleys at radius ``inner``."""
    points: list[tuple[float, float]] = []
    for k in range(spikes):
        tip, valley = 2 * math.pi * k / spikes, 2 * math.pi * (k + 0.5) / spikes
        points += [
            (math.cos(tip), math.sin(tip)),
            (inner * math.cos(valley), inner * math.sin(valley)),
        ]
    return _ring(*points)


@pytest.mark.parametrize(
    "ring",
    [
        pytest.param(_zigzag(66, 1e-8), id="66-edge zigzag, teeth 1e-8 apart"),
        pytest.param(_zigzag(68, 1e-8), id="68-edge zigzag, teeth 1e-8 apart"),
        pytest.param(_zigzag(10_000, 1e-8), id="10,000-edge zigzag"),
        pytest.param(_star(400, 1e-3), id="400-spike star, valleys at 1e-3"),
        pytest.param(_star(400, 1e-30), id="400-spike star, valleys at 1e-30"),
        pytest.param(_star(400, 2.0**-1000), id="400-spike star, valleys at 2**-1000"),
    ],
)
def test_near_degenerate_zones_validate_in_seconds(
    ring: list[tuple[float, float]],
) -> None:
    """Long, nearly parallel edges, or edges converging on a point, are
    not split ever finer at every level of the coordinates' precision
    (minutes to hours before): a split that leaves them together is
    box-swept, and splitting any crowded cell has a budget."""
    started = time.perf_counter()
    assert polygon_is_valid([ring])
    assert time.perf_counter() - started < 10


_GRID_KNOBS = st.fixed_dictionaries(
    {
        "_CROWDED": st.sampled_from([2, 3, 64]),
        "_SUBDIVISION": st.sampled_from([2, 3, 8]),
        "_MAX_DEPTH": st.sampled_from([1, 2, 12]),
        # Fractions run out part-way through a split, on purpose.
        "_BUDGET": st.sampled_from([0, 0.001, 0.01, 0.05, 0.2, 1, 4]),
    }
)
_TINY = st.integers(0, 1074).map(lambda k: 2.0**-k)
_NEAR_DEGENERATE_RINGS = st.one_of(
    st.builds(_zigzag, st.integers(5, 80), _TINY),
    st.builds(_star, st.integers(3, 50), _TINY),
    st.builds(_circle, st.integers(3, 120), _TINY),
)


def _grid(knobs: dict[str, float]) -> contextlib.ExitStack:
    stack = contextlib.ExitStack()
    for name, value in knobs.items():
        stack.enter_context(patch.object(geo, name, value))
    return stack


def _touching_pairs_by_brute_force(
    edges: list[tuple[Any, ...]],
) -> set[tuple[int, int]]:
    return {
        (i, j)
        for i, j in itertools.combinations(range(len(edges)), 2)
        if _touch(*edges[i][2:], *edges[j][2:])
    }


@given(
    ring=_NEAR_DEGENERATE_RINGS,
    dent=st.none() | st.tuples(st.integers(0, 10**6), st.integers(0, 10**6)),
    knobs=_GRID_KNOBS,
)
@settings(max_examples=300, deadline=None)
def test_the_answer_equals_checking_every_pair(
    ring: list[tuple[float, float]],
    dent: tuple[int, int] | None,
    knobs: dict[str, float],
) -> None:
    """On zigzags, stars and polygons scaled down to the last float bit,
    one vertex optionally moved onto another (a touch), the check answers
    exactly as checking every pair of edges does -- whether cells are
    split, box-swept, cut off by depth or out of budget part-way -- and
    the candidates are exactly the pairs of edges that meet, once each."""
    if dent is not None:
        body = ring[:-1]
        body[dent[0] % len(body)] = body[dent[1] % len(body)]
        ring = _ring(*body)
    edges = _edges([ring])
    with _grid(knobs):
        candidates = list(_candidate_pairs(edges))
        answer = polygon_is_valid([ring])
    assert len(set(candidates)) == len(candidates)
    assert set(candidates) == _touching_pairs_by_brute_force(edges)
    with patch.object(
        geo,
        "_candidate_pairs",
        lambda edges: itertools.combinations(range(len(edges)), 2),
    ):
        assert answer is polygon_is_valid([ring])


@given(
    offsets=st.lists(
        st.tuples(st.integers(-3, 3), st.integers(-3, 3)), min_size=2, max_size=40
    ),
    tiny=_TINY,
    knobs=_GRID_KNOBS,
)
@settings(max_examples=300, deadline=None)
def test_nearly_parallel_long_edges_yield_exactly_the_meeting_pairs(
    offsets: list[tuple[int, int]], tiny: float, knobs: dict[str, float]
) -> None:
    """A bundle of long edges a few ``tiny`` apart at each end, crossing,
    touching or overlapping where their offsets cross or tie: the
    candidates are exactly the meeting pairs a brute-force check finds,
    once each."""
    points = [
        point
        for left, right in offsets
        for point in ((0.0, left * tiny), (1.0, 1.0 + right * tiny))
    ]
    [exact] = _exact_rings([points])
    edges = [
        (0, i, a, b)
        for i, (a, b) in enumerate(zip(exact[::2], exact[1::2], strict=True))
    ]
    with _grid(knobs):
        candidates = list(_candidate_pairs(edges))
    assert len(set(candidates)) == len(candidates)
    assert set(candidates) == _touching_pairs_by_brute_force(edges)


def _bundle(edges: int, gap: float) -> list[tuple[float, float]]:
    """Parallel diagonal edges ``gap`` apart, zigzagging up, closed below."""
    teeth = [(float(k % 2), k % 2 + k * gap) for k in range(edges - 4 + (edges % 2))]
    top = teeth[-1][1]
    return _ring(*teeth, (2.0, top), (2.0, -1.0), (-1.0, -1.0), (-1.0, 0.0))


@pytest.mark.parametrize("budget", [4, 0.01])
def test_a_bundle_yields_each_meeting_pair_once(budget: float) -> None:
    """Hundreds of long edges 1e-12 apart share every cell they cross: each
    pair that meets (here only neighbours) is yielded once and nothing
    else, even when splitting runs out of budget part-way and the cell is
    finished by a sweep."""
    ring = _bundle(600, 1e-12)
    edges = _edges([ring])
    with patch.object(geo, "_BUDGET", budget):
        candidates = list(_candidate_pairs(edges))
    assert len(candidates) == len(edges)
    assert set(candidates) == _touching_pairs_by_brute_force(edges)


def test_a_bundle_validates_in_linear_memory() -> None:
    """No set of pairs is kept: a thousand edges that all overlap one
    another's boxes (half a million box pairs) peak at a few megabytes."""
    ring = _bundle(1000, 1e-12)
    tracemalloc.start()
    try:
        assert polygon_is_valid([ring])
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 8_000_000


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


# -- Circle and polygon overlap (GTFS-Flex zones) ------------------------------

_M_PER_DEG = _EARTH_RADIUS_M * math.pi / 180.0


def _metres_to_degrees(
    lat0: float, lon0: float, ring: list[tuple[float, float]]
) -> tuple[tuple[float, float], ...]:
    """(east, north) metres around (lat0, lon0) as closed (lat, lon) ring."""
    scale = _M_PER_DEG * math.cos(math.radians(lat0))
    points = [(lat0 + north / _M_PER_DEG, lon0 + east / scale) for east, north in ring]
    return (*points, points[0])


def _square(lat0: float, lon0: float, half: float) -> FlexPolygon:
    corners = [(-half, -half), (half, -half), (half, half), (-half, half)]
    return FlexPolygon(exterior=_metres_to_degrees(lat0, lon0, corners), holes=())


def _at(lat0: float, lon0: float, east: float, north: float, radius: float) -> Circle:
    scale = _M_PER_DEG * math.cos(math.radians(lat0))
    return Circle(lat0 + north / _M_PER_DEG, lon0 + east / scale, radius)


_BASE = (41.0, -87.0)
# A U: a 400 m square with its top-middle 200 x 300 m cut out.
_U = FlexPolygon(
    exterior=_metres_to_degrees(
        *_BASE,
        [
            (-200, -200),
            (200, -200),
            (200, 200),
            (100, 200),
            (100, -100),
            (-100, -100),
            (-100, 200),
            (-200, 200),
        ],
    ),
    holes=(),
)
_HOLED = FlexPolygon(
    exterior=_square(*_BASE, 500).exterior,
    holes=(_square(*_BASE, 200).exterior,),
)


# 359 degrees wide, unsplit: a strip along the equator from 179.5 W, up a
# meridian, and a strip at 39-40 N to 179.5 E. Just east of 180 at the
# equator only the copy a turn east reaches (its west end); just west of 180
# at 39.5 N only the copy a turn west (its east end).
_Z_BAND = FlexPolygon(
    exterior=(
        (0, -179.5),
        (0, 1),
        (39, 1),
        (39, 179.5),
        (40, 179.5),
        (40, 0),
        (1, 0),
        (1, -179.5),
        (0, -179.5),
    ),
    holes=(),
)


@pytest.mark.parametrize(
    ("polygon", "zone", "overlaps"),
    [
        pytest.param(
            _square(*_BASE, 100), _at(*_BASE, 0, 0, 1), True, id="centre inside"
        ),
        pytest.param(
            _square(*_BASE, 100), _at(*_BASE, 0, 0, 0), True, id="radius 0 inside"
        ),
        pytest.param(
            _square(*_BASE, 100), _at(*_BASE, 100, 0, 0), True, id="radius 0 on an edge"
        ),
        pytest.param(
            _square(*_BASE, 100), _at(*_BASE, 150, 0, 49), False, id="49 m short"
        ),
        pytest.param(
            _square(*_BASE, 100), _at(*_BASE, 150, 0, 51), True, id="edge within 51 m"
        ),
        pytest.param(_square(*_BASE, 100), _at(*_BASE, 5000, 0, 100), False, id="far"),
        pytest.param(_U, _at(*_BASE, 0, 100, 50), False, id="in a concave notch"),
        pytest.param(_U, _at(*_BASE, 0, 100, 150), True, id="notch, reaching the U"),
        pytest.param(_HOLED, _at(*_BASE, 0, 0, 100), False, id="in a hole"),
        pytest.param(
            _HOLED, _at(*_BASE, 0, 0, 250), True, id="hole, reaching its edge"
        ),
        pytest.param(_HOLED, _at(*_BASE, 300, 0, 10), True, id="between hole and edge"),
        pytest.param(
            _square(0.0, -179.999, 50),
            Circle(0.0, 179.9995, 150.0),
            True,
            id="just across the antimeridian",
        ),
        pytest.param(
            _square(0.0, 179.9995, 20),
            Circle(0.0, -179.999, 50.0),
            False,
            id="across the antimeridian, out of reach",
        ),
        pytest.param(
            FlexPolygon(
                exterior=((0, -179), (0, 179), (1, 179), (1, -179), (0, -179)),
                holes=(),
            ),
            Circle(0.5, 179.5, 60_000.0),
            True,
            id="an unsplit band the long way round, its 179 edge in reach",
        ),
        pytest.param(
            FlexPolygon(
                exterior=((0, 179), (1, 179), (1, -179), (0, -179), (0, 179)),
                holes=(),
            ),
            Circle(0.5, 179.5, 60_000.0),
            True,
            id="the same band from another start vertex",
        ),
        pytest.param(
            FlexPolygon(
                exterior=((0, 179.5), (0, -179.5), (1, -179.5), (1, 179.5), (0, 179.5)),
                holes=(),
            ),
            Circle(0.5, -179.9, 50_000.0),
            True,
            id="an unsplit band reaching the circle from the copy a turn east",
        ),
        pytest.param(
            FlexPolygon(
                exterior=(
                    (0, -179.5),
                    (1, -179.5),
                    (1, 179.5),
                    (0, 179.5),
                    (0, -179.5),
                ),
                holes=(),
            ),
            Circle(0.5, -179.9, 50_000.0),
            True,
            id="the same band from another start vertex, east copy",
        ),
        pytest.param(
            FlexPolygon(
                exterior=((0, 0), (0, 179.9), (1, 179.9), (1, 0), (0, 0)),
                holes=(),
            ),
            Circle(0.5, -179.95, 20_000.0),
            True,
            id="a zone nearly half a turn wide, its far edge across the antimeridian",
        ),
        pytest.param(
            _Z_BAND,
            Circle(0.5, 179.9, 70_000.0),
            True,
            id="a Z band reached only by its copy a turn east",
        ),
        pytest.param(
            _Z_BAND,
            Circle(39.5, -179.9, 70_000.0),
            True,
            id="a Z band reached only by its copy a turn west",
        ),
        pytest.param(
            _square(80.0, 10.0, 100),
            _at(80.0, 10.0, 150, 0, 51),
            True,
            id="at 80 N, edge within 51 m",
        ),
        pytest.param(
            _square(80.0, 10.0, 100),
            _at(80.0, 10.0, 150, 0, 49),
            False,
            id="at 80 N, 49 m short",
        ),
    ],
)
def test_polygon_overlaps_circle(
    polygon: FlexPolygon, zone: Circle, overlaps: bool
) -> None:
    assert polygon_overlaps_circle(polygon, zone) is overlaps


_LATS = st.floats(-70, 70)
_LONS = st.floats(-179, 179)
_METRES = st.floats(-2000, 2000)


@given(
    lat0=_LATS,
    lon0=_LONS,
    corner=st.tuples(_METRES, _METRES),
    size=st.tuples(st.floats(2, 2000), st.floats(2, 2000)),
    point=st.tuples(_METRES, _METRES),
)
def test_radius_zero_on_a_rectangle_is_its_bounds_check(
    lat0: float,
    lon0: float,
    corner: tuple[float, float],
    size: tuple[float, float],
    point: tuple[float, float],
) -> None:
    """A zero-radius circle overlaps an axis-aligned rectangle exactly when
    its centre lies within the rectangle's bounds."""
    west, south = corner
    east, north = west + size[0], south + size[1]
    x, y = point
    # Clear of the boundary, where either answer is within float error.
    assume(min(abs(x - west), abs(x - east), abs(y - south), abs(y - north)) > 1e-3)
    rectangle = FlexPolygon(
        exterior=_metres_to_degrees(
            lat0, lon0, [(west, south), (east, south), (east, north), (west, north)]
        ),
        holes=(),
    )
    inside = west < x < east and south < y < north
    assert polygon_overlaps_circle(rectangle, _at(lat0, lon0, x, y, 0)) is inside


@given(
    south=st.floats(-70, 69),
    height=st.floats(1e-6, 1),
    west=st.floats(-180, 179),
    reach=st.floats(1e-9, 1),
    side=st.sampled_from(["south", "north", "west", "east"]),
    along=st.floats(0, 1),
)
def test_a_point_on_a_rectangle_edge_overlaps_at_radius_zero(  # noqa: PLR0917
    south: float,
    height: float,
    west: float,
    reach: float,
    side: str,
    along: float,
) -> None:
    """The boundary belongs to the zone: a zero-radius circle on any edge of
    an axis-aligned rectangle overlaps it, wherever on the edge -- next to
    either end too -- and however far east the edge lies (up to 180)."""
    north, east = south + height, west + reach * (180 - west)
    assume(west < east <= 180)
    rectangle = _box(south, west, north, east)
    width = east - west
    lat, lon = {
        "south": (south, west + along * width),
        "north": (north, west + along * width),
        "west": (south + along * height, west),
        "east": (south + along * height, east),
    }[side]
    assume(south <= lat <= north and west <= lon <= east)
    assert polygon_overlaps_circle(rectangle, Circle(lat, lon, 0.0))


def _box(
    south: float,
    west: float,
    north: float,
    east: float,
    holes: tuple[tuple[tuple[float, float], ...], ...] = (),
) -> FlexPolygon:
    return FlexPolygon(
        exterior=(
            (south, west),
            (south, east),
            (north, east),
            (north, west),
            (south, west),
        ),
        holes=holes,
    )


_PLANE = st.floats(-1e6, 1e6)


@given(
    a=st.tuples(_PLANE, _PLANE),
    direction=st.tuples(st.floats(-1, 1), st.floats(-1, 1)),
    length=st.floats(1e-6, 1e3),
    offset=st.floats(-1e-3, 1e-3),
)
def test_segment_distance_is_within_rounding_of_exact(
    a: tuple[float, float],
    direction: tuple[float, float],
    length: float,
    offset: float,
) -> None:
    """Against the exact distance in rationals, within a few ulps of the
    nearer vertex's magnitude -- including short edges far away with the
    origin near their line, where a cross product from the far vertex
    (``a x b``) is off by hundreds."""
    norm = math.hypot(*direction)
    assume(norm > 1e-3)
    ux, uy = direction[0] / norm, direction[1] / norm
    # Shift the edge so the origin lies near its line: a short edge far away.
    ax, ay = a[0] + offset * -uy, a[1] + offset * ux
    ax, ay = ax - (ax * ux + ay * uy) * ux, ay - (ax * ux + ay * uy) * uy
    start = (ax - ux * length / 2, ay - uy * length / 2)
    end = (ax + ux * length / 2, ay + uy * length / 2)
    exact = _exact_segment_distance(start, end)
    got = geo._segment_distance(start, end)
    scale = min(math.hypot(*start), math.hypot(*end)) + length
    assert abs(got - exact) <= 4 * sys.float_info.epsilon * scale


def _exact_segment_distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    """The distance from the origin to ab, exactly in rationals, then rounded."""
    ax, ay, bx, by = map(Fraction, (*a, *b))
    dx, dy = bx - ax, by - ay
    length = dx * dx + dy * dy
    t = 0 if length == 0 else min(1, max(0, -(ax * dx + ay * dy) / length))
    x, y = ax + t * dx, ay + t * dy
    squared = x * x + y * y
    with decimal.localcontext() as context:
        context.prec = 80
        root = (
            decimal.Decimal(squared.numerator) / decimal.Decimal(squared.denominator)
        ).sqrt()
    return float(root)


@pytest.mark.parametrize(
    ("polygon", "zone"),
    [
        pytest.param(
            _box(0.0, 0.0, 1.0, 170.0),
            Circle(0.046875, 170.0, 0.0),
            id="east edge of a 170-degree zone",
        ),
        pytest.param(
            _box(0.0, 0.0, 1.0, 1.0),
            Circle(1.0, 4.12e-263, 0.0),
            id="next to the far end of the north edge",
        ),
        pytest.param(
            _box(51.4, -0.1278, 51.6, 0.5),
            Circle(51.6, math.nextafter(-0.1278, 1), 0.0),
            id="one ulp from a London-sized zone's corner",
        ),
        pytest.param(
            _box(51.0, 0.001, 51.1, 0.011),
            Circle(51.1, math.nextafter(0.001, 1), 0.0),
            id="one ulp east of a small zone's corner",
        ),
        pytest.param(
            _box(-1.0, 0.0, 2.0, 2.0, holes=(_box(0.0, 0.5, 1.0, 1.5).exterior,)),
            Circle(0.109375, 0.5, 0.0),
            id="on a hole's west edge, where crossings say inside the hole",
        ),
        pytest.param(
            _box(0.0, 179.0, 1.0, 180.0),
            Circle(0.046875, -180.0, 0.0),
            id="on an edge at 180, from -180",
        ),
        pytest.param(
            _box(0.0, 0.0, 1.0, 1e-12),
            Circle(1.0, 5e-324, 0.0),
            id="a dot product underflowing to zero",
        ),
    ],
)
def test_a_boundary_point_overlaps_at_radius_zero(
    polygon: FlexPolygon, zone: Circle
) -> None:
    """Rounding never moves a boundary point out. Main's foot-of-the-
    perpendicular step missed the 170-degree edge, the hole's west edge and
    the antimeridian copy; an end test read off the far vertex missed the
    three next to an edge's end; a non-strict end test missed the
    underflowing dot product."""
    assert polygon_overlaps_circle(polygon, zone)


@given(
    origin=st.tuples(_LATS, _LONS),
    count=st.integers(3, 9),
    radius=st.floats(50, 2000),
    turn=st.floats(0, 2 * math.pi),
    point=st.tuples(_METRES, _METRES),
)
def test_radius_zero_on_a_convex_polygon_is_the_half_plane_test(
    origin: tuple[float, float],
    count: int,
    radius: float,
    turn: float,
    point: tuple[float, float],
) -> None:
    """For a convex polygon (counterclockwise), containment is being left of
    every edge."""
    lat0, lon0 = origin
    corners = [
        (
            radius * math.cos(turn + 2 * math.pi * k / count),
            radius * math.sin(turn + 2 * math.pi * k / count),
        )
        for k in range(count)
    ]
    sides = [(a, b) for a, b in zip(corners, [*corners[1:], corners[0]], strict=True)]
    x, y = point
    crosses = [
        (b[0] - a[0]) * (y - a[1]) - (b[1] - a[1]) * (x - a[0]) for a, b in sides
    ]
    assume(all(abs(cross) > 1e-3 for cross in crosses))
    polygon = FlexPolygon(exterior=_metres_to_degrees(lat0, lon0, corners), holes=())
    assert polygon_overlaps_circle(polygon, _at(lat0, lon0, x, y, 0)) is all(
        cross > 0 for cross in crosses
    )


@given(
    lat0=_LATS, lon0=_LONS, point=st.tuples(st.floats(-190, 190), st.floats(-190, 190))
)
def test_a_point_in_a_hole_is_never_contained(
    lat0: float, lon0: float, point: tuple[float, float]
) -> None:
    """Strictly inside a hole (clear of its edges), a zero-radius circle
    meets no part of the polygon."""
    holed = FlexPolygon(
        exterior=_square(lat0, lon0, 500).exterior,
        holes=(_square(lat0, lon0, 200).exterior,),
    )
    assert not polygon_overlaps_circle(holed, _at(lat0, lon0, *point, 0))


@given(
    lat0=_LATS,
    lon0=_LONS,
    point=st.tuples(_METRES, _METRES),
    radius=st.floats(0, 3000),
    more=st.floats(0, 3000),
)
def test_overlap_grows_with_the_radius(
    lat0: float, lon0: float, point: tuple[float, float], radius: float, more: float
) -> None:
    """Overlapping at one radius, a circle overlaps at every larger one."""
    polygon = (
        _U
        if (lat0, lon0) == _BASE
        else FlexPolygon(
            exterior=_metres_to_degrees(
                lat0,
                lon0,
                [
                    (-200, -200),
                    (200, -200),
                    (200, 200),
                    (100, 200),
                    (100, -100),
                    (-100, -100),
                    (-100, 200),
                    (-200, 200),
                ],
            ),
            holes=(_square(lat0, lon0, 50).exterior,),
        )
    )
    if polygon_overlaps_circle(polygon, _at(lat0, lon0, *point, radius)):
        assert polygon_overlaps_circle(polygon, _at(lat0, lon0, *point, radius + more))


@given(
    lat0=_LATS,
    lon0=_LONS,
    point=st.tuples(st.floats(-20_000, 20_000), st.floats(-20_000, 20_000)),
    radius=st.floats(0, 5000),
)
def test_disjoint_bounding_boxes_never_overlap(
    lat0: float, lon0: float, point: tuple[float, float], radius: float
) -> None:
    """A circle whose bounding box misses the polygon's misses the polygon."""
    polygon = _square(lat0, lon0, 300)
    x, y = point
    if abs(x) > 300 + radius * 1.01 or abs(y) > 300 + radius * 1.01:
        assert not polygon_overlaps_circle(polygon, _at(lat0, lon0, x, y, radius))


@given(
    lat0=_LATS,
    lon0=_LONS,
    bearing=st.floats(0, 2 * math.pi),
    distance=st.floats(10, 5000),
)
def test_reach_matches_great_circle_distance_within_one_percent(
    lat0: float, lon0: float, bearing: float, distance: float
) -> None:
    """A tiny zone ``distance`` away (by great circle) is reached by a radius
    1% over it and missed by one 1% under -- the planar scaling's stated
    accuracy within a few kilometres outside polar latitudes."""
    east, north = distance * math.sin(bearing), distance * math.cos(bearing)
    zone = _square(lat0, lon0, 0.5)
    centre = _at(lat0, lon0, east, north, 0)
    true = great_circle_m(centre.latitude, centre.longitude, lat0, lon0)
    reach = Circle(centre.latitude, centre.longitude, true * 1.01 + 1)
    short = Circle(centre.latitude, centre.longitude, max(0.0, true * 0.99 - 1))
    assert polygon_overlaps_circle(zone, reach)
    assert not polygon_overlaps_circle(zone, short)


@given(
    origin=st.tuples(st.floats(-80, 80), st.floats(-180, 180)),
    count=st.integers(3, 9),
    size=st.floats(50, 2000),
    shift=st.integers(0, 8),
    circle=st.tuples(_METRES, _METRES, st.floats(0, 3000)),
)
# A point within rounding of a vertex: each edge's tests rounded differently
# with its direction, so the ring's winding changed the answer.
@example(
    origin=(0.0, 0.0), count=7, size=60.0, shift=0, circle=(60.0, 1.175494351e-38, 0.0)
)
@example(
    origin=(0.0, 0.0), count=7, size=1801.0, shift=3, circle=(1801.0, 7.68e-185, 0.0)
)
def test_the_answer_ignores_the_start_vertex_and_winding(
    origin: tuple[float, float],
    count: int,
    size: float,
    shift: int,
    circle: tuple[float, float, float],
) -> None:
    """A ring names the same zone from any start vertex, either way round --
    near the antimeridian too."""
    lat0, lon0 = origin
    corners = [
        (
            size * math.cos(2 * math.pi * k / count),
            size * math.sin(2 * math.pi * k / count),
        )
        for k in range(count)
    ]
    body = list(_metres_to_degrees(lat0, lon0, corners)[:-1])
    shift %= len(body)
    zone = _at(lat0, lon0, *circle)
    answers = set()
    for ring in (body[shift:] + body[:shift], (body[shift:] + body[:shift])[::-1]):
        polygon = FlexPolygon(exterior=(*ring, ring[0]), holes=())
        answers.add(polygon_overlaps_circle(polygon, zone))
    reference = FlexPolygon(exterior=(*body, body[0]), holes=())
    answers.add(polygon_overlaps_circle(reference, zone))
    assert len(answers) == 1


def _rectangle_reach(
    south: float, north: float, west: float, east: float, zone: Circle
) -> bool:
    """Whether a longitude/latitude rectangle meets a circle, in the scaled
    plane, over every copy of it within two turns: axis-aligned there, its
    distance from the centre is a clamp per axis.

    Each copy's edge (``east + turn``) is formed first, as the library moves
    the polygon: ``zone.longitude - east - turn`` rounded -179.99999999999997
    - 180 onto -360, reading a centre 3e-14 degrees outside as on the edge.
    """
    scale = _M_PER_DEG * math.cos(math.radians(zone.latitude))
    dy = max(south - zone.latitude, 0.0, zone.latitude - north) * _M_PER_DEG
    return any(
        math.hypot(
            max((west + turn) - zone.longitude, 0.0, zone.longitude - (east + turn))
            * scale,
            dy,
        )
        <= zone.radius_m
        for turn in (-720.0, -360.0, 0.0, 360.0, 720.0)
    )


@given(
    west=st.floats(-180, 10),
    span=st.floats(170, 359),
    corner=st.integers(0, 3),
    circle=st.tuples(st.floats(-0.5, 1.5), st.floats(-180, 180), st.floats(0, 100_000)),
)
@example(west=0.0, span=180.0, corner=0, circle=(0.0, -179.99999999999997, 0.0))
def test_a_wide_zone_answers_as_its_nearest_copy_from_every_start(
    west: float, span: float, corner: int, circle: tuple[float, float, float]
) -> None:
    """Zones 170 to 359 degrees wide, around the antimeridian: from any
    start corner, the answer is whether any copy a turn or two away
    reaches the circle (computed independently, by clamping)."""
    east = west + span
    assume(east <= 180)
    corners = [(0.0, west), (0.0, east), (1.0, east), (1.0, west)]
    rotated = corners[corner:] + corners[:corner]
    polygon = FlexPolygon(exterior=(*rotated, rotated[0]), holes=())
    zone = Circle(*circle)
    assert polygon_overlaps_circle(polygon, zone) is _rectangle_reach(
        0.0, 1.0, west, east, zone
    )


# -- in_circle is great_circle_m within the radius, whatever the zone -----------


def _destination(
    lat: float, lon: float, bearing: float, distance_m: float
) -> tuple[float, float]:
    """The point ``distance_m`` from (lat, lon) along ``bearing`` (radians)
    on the same sphere great_circle_m measures."""
    delta = distance_m / geo._EARTH_RADIUS_M
    phi1, lambda1 = math.radians(lat), math.radians(lon)
    phi2 = math.asin(
        math.sin(phi1) * math.cos(delta)
        + math.cos(phi1) * math.sin(delta) * math.cos(bearing)
    )
    lambda2 = lambda1 + math.atan2(
        math.sin(bearing) * math.sin(delta) * math.cos(phi1),
        math.cos(delta) - math.sin(phi1) * math.sin(phi2),
    )
    lon2 = (math.degrees(lambda2) + 180.0) % 360.0 - 180.0
    return math.degrees(phi2), lon2


@given(
    zone_lat=st.floats(-90, 90),
    zone_lon=st.floats(-180, 180),
    radius=st.sampled_from(
        [1.0, 500.0, 50_000.0, 500_000.0, 3_000_000.0, 1.5e7, 2.1e7]
    ).flatmap(lambda scale: st.floats(0, scale)),
    bearing=st.floats(0, 2 * math.pi),
    reach=st.floats(0, 1.5),
)
@settings(max_examples=2000)
def test_in_circle_is_the_great_circle_distance_within_the_radius(
    zone_lat: float,
    zone_lon: float,
    radius: float,
    bearing: float,
    reach: float,
) -> None:
    """The prefilter only saves work: it never turns away a point the
    distance keeps. Points are drawn around the edge in every direction --
    the poleward bulge of a large or high-latitude circle included, where a
    box ``radius / cos(lat)`` wide falls short."""
    zone = Circle(zone_lat, zone_lon, radius)
    lat, lon = _destination(zone_lat, zone_lon, bearing, radius * reach)
    expected = great_circle_m(zone_lat, zone_lon, lat, lon) <= radius
    assert in_circle(zone, lat, lon) is expected


@pytest.mark.parametrize(
    ("zone", "point"),
    [
        # 3000 km around 60N reaches 65 degrees of longitude either side,
        # at 76N; a box r / cos(60N) wide stops at 54.5.
        (Circle(60.0, 0.0, 3_000_000.0), (76.0, 63.0)),
        # A circle holding the north pole holds every longitude near it.
        (Circle(85.0, 0.0, 1_000_000.0), (89.5, 179.0)),
        (Circle(-85.0, 10.0, 1_000_000.0), (-89.5, -170.0)),
    ],
)
def test_in_circle_keeps_points_past_the_centre_parallel_box(
    zone: Circle, point: tuple[float, float]
) -> None:
    assert great_circle_m(zone.latitude, zone.longitude, *point) <= zone.radius_m
    assert in_circle(zone, *point)
