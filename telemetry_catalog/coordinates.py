"""Shared coordinate transforms for vehicle configuration consumers."""

from __future__ import annotations

import math
from datetime import datetime, timezone


def _julian_date(timestamp: datetime) -> float:
    ts = timestamp.astimezone(timezone.utc)
    return ts.timestamp() / 86400.0 + 2440587.5


def greenwich_sidereal_angle_rad(timestamp: datetime) -> float:
    """Approximate Greenwich sidereal angle in radians for the timestamp."""
    jd = _julian_date(timestamp)
    t = (jd - 2451545.0) / 36525.0
    gmst_deg = (
        280.46061837
        + 360.98564736629 * (jd - 2451545.0)
        + 0.000387933 * t * t
        - (t * t * t) / 38710000.0
    )
    return math.radians(gmst_deg % 360.0)


def ecef_to_eci_m(
    x_m: float,
    y_m: float,
    z_m: float,
    timestamp: datetime,
) -> tuple[float, float, float]:
    """Rotate Earth-fixed coordinates into the inertial frame."""
    theta = greenwich_sidereal_angle_rad(timestamp)
    cos_theta = math.cos(theta)
    sin_theta = math.sin(theta)
    x_eci = cos_theta * x_m - sin_theta * y_m
    y_eci = sin_theta * x_m + cos_theta * y_m
    return x_eci, y_eci, z_m


def eci_to_ecef_m(
    x_m: float,
    y_m: float,
    z_m: float,
    timestamp: datetime,
) -> tuple[float, float, float]:
    """Rotate inertial coordinates into the Earth-fixed frame."""
    theta = greenwich_sidereal_angle_rad(timestamp)
    cos_theta = math.cos(theta)
    sin_theta = math.sin(theta)
    x_ecef = cos_theta * x_m + sin_theta * y_m
    y_ecef = -sin_theta * x_m + cos_theta * y_m
    return x_ecef, y_ecef, z_m
