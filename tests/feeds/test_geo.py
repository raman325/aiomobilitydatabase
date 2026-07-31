"""Tests for circular-zone geometry."""

from aiomobilitydatabase.feeds.geo import Circle, haversine_m, in_circle

LA = (34.0522, -118.2437)


def test_haversine_known_distance() -> None:
    # LA City Hall to a nearby point is roughly 650 m
    dist = haversine_m(34.0537, -118.2428, 34.0562, -118.2365)
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
    # Same-latitude point ~49,620 m away (true haversine distance): inside.
    assert in_circle(zone, 89.0, 25.786)
    assert not in_circle(zone, 89.0, 27.0)
