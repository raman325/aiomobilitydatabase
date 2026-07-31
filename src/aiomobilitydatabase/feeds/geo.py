"""Circular-zone geometry: bbox prefilter + haversine."""

from __future__ import annotations

import math
from dataclasses import dataclass

_EARTH_RADIUS_M = 6_371_000.0
# Derived from _EARTH_RADIUS_M so it stays consistent with haversine_m forever.
_M_PER_DEG_LAT = _EARTH_RADIUS_M * math.pi / 180.0
# The bbox prefilter must strictly enclose the circle. A slightly generous box
# only costs a few extra haversine calls; a tight box wrongly rejects boundary
# points (e.g. the cos(lat) linearization under-sizes the box at high latitude).
_BBOX_SAFETY = 1.01


@dataclass(frozen=True)
class Circle:
    """A circular geographic zone (matches HA zone entities)."""

    latitude: float
    longitude: float
    radius_m: float


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


def in_circle(zone: Circle, latitude: float, longitude: float) -> bool:
    """Return True if the point lies within the zone.

    A cheap bounding-box prefilter (four float comparisons) rejects most
    points before the trigonometric haversine runs. The box is padded by
    ``_BBOX_SAFETY`` so it strictly encloses the circle.
    """
    dlat_deg = zone.radius_m * _BBOX_SAFETY / _M_PER_DEG_LAT
    cos_lat = math.cos(math.radians(zone.latitude))
    dlon_deg = zone.radius_m * _BBOX_SAFETY / (_M_PER_DEG_LAT * max(cos_lat, 1e-6))
    if (
        latitude < zone.latitude - dlat_deg
        or latitude > zone.latitude + dlat_deg
        or longitude < zone.longitude - dlon_deg
        or longitude > zone.longitude + dlon_deg
    ):
        return False
    distance = haversine_m(zone.latitude, zone.longitude, latitude, longitude)
    return distance <= zone.radius_m
