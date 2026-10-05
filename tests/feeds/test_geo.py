"""Tests for circular-zone geometry."""

import math

import pytest

from aiomobilitydatabase.feeds.geo import (
    _EARTH_RADIUS_M,
    Circle,
    great_circle_m,
    in_circle,
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
