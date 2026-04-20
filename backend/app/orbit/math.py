"""Orbit math: LLA→ECEF, velocity, orbital parameters from position and velocity."""

from __future__ import annotations

import math
from typing import Tuple

import numpy as np

# WGS84 ellipsoid
_WGS84_A_M = 6378137.0  # semi-major axis (m)
_WGS84_E2 = 6.69437999014e-3  # first eccentricity squared

# Standard gravitational parameter and Earth radius (km)
MU_KM3_S2 = 398600.4418
EARTH_RADIUS_KM = 6378.137
EARTH_ROTATION_RAD_S = 7.2921159e-5


def lla_to_ecef_km(lat_deg: float, lon_deg: float, alt_m: float) -> Tuple[float, float, float]:
    """Convert geodetic lat/lon/alt (WGS84) to ECEF Cartesian in kilometers.

    Returns (x_km, y_km, z_km).
    """
    a = _WGS84_A_M
    e2 = _WGS84_E2
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    sin_lon = math.sin(lon)
    cos_lon = math.cos(lon)
    N = a / math.sqrt(1.0 - e2 * sin_lat * sin_lat)
    x_m = (N + alt_m) * cos_lat * cos_lon
    y_m = (N + alt_m) * cos_lat * sin_lon
    z_m = (N * (1.0 - e2) + alt_m) * sin_lat
    return x_m / 1000.0, y_m / 1000.0, z_m / 1000.0


def velocity_from_positions(
    r_prev: Tuple[float, float, float],
    r_curr: Tuple[float, float, float],
    dt_sec: float,
) -> Tuple[float, float, float]:
    """Compute velocity (km/s) from two ECEF positions (km) and time step (s)."""
    if dt_sec <= 0:
        return 0.0, 0.0, 0.0
    vx = (r_curr[0] - r_prev[0]) / dt_sec
    vy = (r_curr[1] - r_prev[1]) / dt_sec
    vz = (r_curr[2] - r_prev[2]) / dt_sec
    return (vx, vy, vz)


def inertial_velocity_from_ecef_positions(
    r_prev_ecef_km: Tuple[float, float, float],
    r_curr_ecef_km: Tuple[float, float, float],
    dt_sec: float,
) -> Tuple[float, float, float]:
    """Approximate inertial velocity from consecutive ECEF positions.

    The incoming position telemetry is ground-fixed (`gps_lla` -> ECEF), so the
    raw finite difference is in the rotating Earth frame. Add the `omega x r`
    term to recover velocity in an inertial sense for orbit estimation.
    """
    vx_ecef, vy_ecef, vz_ecef = velocity_from_positions(r_prev_ecef_km, r_curr_ecef_km, dt_sec)
    rx, ry, rz = r_curr_ecef_km
    omega_cross_r = (
        -EARTH_ROTATION_RAD_S * ry,
        EARTH_ROTATION_RAD_S * rx,
        0.0,
    )
    return (
        vx_ecef + omega_cross_r[0],
        vy_ecef + omega_cross_r[1],
        vz_ecef + omega_cross_r[2],
    )


def orbital_parameters(
    r_km: Tuple[float, float, float],
    v_km_s: Tuple[float, float, float],
) -> dict:
    """Compute orbital parameters from position (km) and velocity (km/s) in ECEF.

    Returns dict with: semi_major_axis_km, eccentricity, perigee_alt_km, apogee_alt_km,
    period_sec, speed_km_s, specific_energy_km2_s2.
    """
    r = np.array(r_km, dtype=float)
    v = np.array(v_km_s, dtype=float)
    r_norm = float(np.linalg.norm(r))
    v_norm = float(np.linalg.norm(v))
    if r_norm <= 0:
        return _insufficient_data_result(v_norm)

    # Specific orbital energy: epsilon = v^2/2 - mu/r
    specific_energy = 0.5 * v_norm * v_norm - MU_KM3_S2 / r_norm

    # Semi-major axis: a = -mu / (2 * epsilon); for unbound (epsilon >= 0) a is negative/inf
    if specific_energy >= 0:
        return _escape_result(v_norm, specific_energy)

    semi_major_axis = -MU_KM3_S2 / (2.0 * specific_energy)

    # Angular momentum vector h = r x v (km^2/s)
    h_vec = np.cross(r, v)
    h_mag = float(np.linalg.norm(h_vec))
    if h_mag <= 0:
        return _insufficient_data_result(v_norm)

    # Eccentricity from energy and angular momentum: e = sqrt(1 + 2*epsilon*h^2/mu^2)
    e_sq = 1.0 + 2.0 * specific_energy * (h_mag * h_mag) / (MU_KM3_S2 * MU_KM3_S2)
    e_sq = max(0.0, e_sq)
    eccentricity = math.sqrt(e_sq)

    # Perigee and apogee radii: r_peri = a(1-e), r_apo = a(1+e); altitudes = radius - R_earth
    r_peri = semi_major_axis * (1.0 - eccentricity)
    r_apo = semi_major_axis * (1.0 + eccentricity)
    perigee_alt_km = r_peri - EARTH_RADIUS_KM
    apogee_alt_km = r_apo - EARTH_RADIUS_KM

    # Period: T = 2*pi*sqrt(a^3/mu)
    period_sec = 2.0 * math.pi * math.sqrt((semi_major_axis ** 3) / MU_KM3_S2)

    return {
        "semi_major_axis_km": semi_major_axis,
        "eccentricity": eccentricity,
        "perigee_alt_km": perigee_alt_km,
        "apogee_alt_km": apogee_alt_km,
        "period_sec": period_sec,
        "speed_km_s": v_norm,
        "specific_energy_km2_s2": specific_energy,
    }


def _insufficient_data_result(speed_km_s: float) -> dict:
    return {
        "semi_major_axis_km": 0.0,
        "eccentricity": 0.0,
        "perigee_alt_km": 0.0,
        "apogee_alt_km": 0.0,
        "period_sec": 0.0,
        "speed_km_s": speed_km_s,
        "specific_energy_km2_s2": 0.0,
    }


def _escape_result(speed_km_s: float, specific_energy: float) -> dict:
    return {
        "semi_major_axis_km": 0.0,
        "eccentricity": 1.0,
        "perigee_alt_km": 0.0,
        "apogee_alt_km": 0.0,
        "period_sec": 0.0,
        "speed_km_s": speed_km_s,
        "specific_energy_km2_s2": specific_energy,
    }
