"""Orbit models for simulator GPS position telemetry."""

from dataclasses import dataclass
import math

EARTH_RADIUS_M = 6_378_137.0
EARTH_ROTATION_RAD_S = 2.0 * math.pi / 86_400.0


@dataclass(frozen=True)
class OrbitProfile:
    """Lightweight orbital profile used to generate deterministic LLA telemetry."""

    semi_major_axis_m: float
    eccentricity: float
    angular_rate_scale: float = 1.0
    phase_offset_rad: float = 0.0
    lat_perturbation_deg: float = 0.0
    lon_perturbation_deg: float = 0.0
    alt_perturbation_m: float = 0.0


def _profile_from_altitudes(
    *,
    perigee_alt_m: float,
    apogee_alt_m: float,
    angular_rate_scale: float = 1.0,
    phase_offset_rad: float = 0.0,
    lat_perturbation_deg: float = 0.0,
    lon_perturbation_deg: float = 0.0,
    alt_perturbation_m: float = 0.0,
) -> OrbitProfile:
    rp = EARTH_RADIUS_M + perigee_alt_m
    ra = EARTH_RADIUS_M + apogee_alt_m
    semi_major_axis_m = 0.5 * (rp + ra)
    eccentricity = (ra - rp) / (ra + rp)
    return OrbitProfile(
        semi_major_axis_m=semi_major_axis_m,
        eccentricity=eccentricity,
        angular_rate_scale=angular_rate_scale,
        phase_offset_rad=phase_offset_rad,
        lat_perturbation_deg=lat_perturbation_deg,
        lon_perturbation_deg=lon_perturbation_deg,
        alt_perturbation_m=alt_perturbation_m,
    )


ORBIT_PROFILES: dict[str, OrbitProfile] = {
    "nominal": _profile_from_altitudes(
        perigee_alt_m=400_000.0,
        apogee_alt_m=400_000.0,
        lat_perturbation_deg=0.0002,
        lon_perturbation_deg=0.00025,
        alt_perturbation_m=15.0,
    ),
    "orbit_nominal": _profile_from_altitudes(
        perigee_alt_m=400_000.0,
        apogee_alt_m=400_000.0,
        lat_perturbation_deg=0.0002,
        lon_perturbation_deg=0.00025,
        alt_perturbation_m=15.0,
    ),
    "orbit_decay": _profile_from_altitudes(
        perigee_alt_m=0.0,
        apogee_alt_m=600_000.0,
        angular_rate_scale=0.95,
    ),
    "orbit_highly_elliptical": _profile_from_altitudes(
        perigee_alt_m=150_000.0,
        apogee_alt_m=1_800_000.0,
        angular_rate_scale=1.1,
        phase_offset_rad=0.589,
    ),
    "orbit_suborbital": _profile_from_altitudes(
        perigee_alt_m=400_000.0,
        apogee_alt_m=400_000.0,
        angular_rate_scale=0.72,
    ),
    "orbit_escape": _profile_from_altitudes(
        perigee_alt_m=400_000.0,
        apogee_alt_m=400_000.0,
        angular_rate_scale=1.55,
    ),
}


def _smooth_perturbation(sim_elapsed_sec: float, amplitude: float, phase_offset: float) -> float:
    """Bounded, continuous perturbation used for nominal GPS noise."""
    if amplitude == 0.0:
        return 0.0
    fast = math.sin((2.0 * math.pi * sim_elapsed_sec / 61.0) + phase_offset)
    slow = math.sin((2.0 * math.pi * sim_elapsed_sec / 173.0) + phase_offset * 0.5)
    return amplitude * (0.65 * fast + 0.35 * slow)


def position_at_time(
    sim_elapsed_sec: float,
    *,
    period_sec: float,
    inclination_deg: float,
    alt_m: float,
    lon0_deg: float = 0.0,
    profile: str = "nominal",
) -> tuple[float, float, float]:
    """Compute lat/lon/alt (degrees, degrees, meters) for a simulator orbit profile."""
    cfg = ORBIT_PROFILES.get(
        profile,
        OrbitProfile(semi_major_axis_m=EARTH_RADIUS_M + alt_m, eccentricity=0.0),
    )
    theta = cfg.phase_offset_rad + (2.0 * math.pi * cfg.angular_rate_scale * sim_elapsed_sec / period_sec)
    i_rad = math.radians(inclination_deg)
    denom = 1.0 + cfg.eccentricity * math.cos(theta)
    radius_m = cfg.semi_major_axis_m * (1.0 - cfg.eccentricity * cfg.eccentricity) / max(
        denom,
        1e-6,
    )

    x_m = radius_m * math.cos(theta)
    y_m = radius_m * math.sin(theta) * math.cos(i_rad)
    z_m = radius_m * math.sin(theta) * math.sin(i_rad)

    lon_rad = math.atan2(y_m, x_m) + math.radians(lon0_deg) - EARTH_ROTATION_RAD_S * sim_elapsed_sec
    lat_rad = math.atan2(z_m, math.sqrt(x_m * x_m + y_m * y_m))

    lat_deg = math.degrees(lat_rad) + _smooth_perturbation(
        sim_elapsed_sec,
        cfg.lat_perturbation_deg,
        phase_offset=0.8,
    )
    lon_deg = math.degrees(lon_rad) + _smooth_perturbation(
        sim_elapsed_sec,
        cfg.lon_perturbation_deg,
        phase_offset=1.7,
    )
    alt_out_m = (radius_m - EARTH_RADIUS_M) + _smooth_perturbation(
        sim_elapsed_sec,
        cfg.alt_perturbation_m,
        phase_offset=2.2,
    )

    lon_deg = ((lon_deg + 180.0) % 360.0) - 180.0

    return (lat_deg, lon_deg, alt_out_m)
