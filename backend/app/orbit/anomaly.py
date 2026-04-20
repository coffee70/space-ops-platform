"""Orbit anomaly detection: escape, suborbital, decay, high eccentricity."""

from __future__ import annotations

from typing import Tuple


def check_anomaly(
    status_valid: bool,
    specific_energy: float,
    speed_km_s: float,
    current_alt_km: float,
    perigee_alt_km: float,
    eccentricity: float,
    orbit_type: str,
) -> Tuple[str, str]:
    """Determine orbit status and reason.

    Returns (status, reason) where status is 'VALID' or an anomaly code.
    Order of checks: escape, suborbital, decay, high-ecc for LEO.
    """
    # 1. Unbound trajectory
    if specific_energy >= 0:
        return ("ESCAPE_TRAJECTORY", "Orbital energy >= 0 (unbound trajectory)")

    # 2. Suborbital: velocity < 7 km/s while altitude < 1000 km
    if current_alt_km < 1000.0 and speed_km_s < 7.0:
        return ("SUBORBITAL", "Velocity < 7 km/s at altitude < 1000 km")

    # 3. Orbit decay: predicted perigee below 120 km
    if perigee_alt_km < 120.0:
        return ("ORBIT_DECAY", "Predicted perigee below 120 km (orbit decay)")

    # 4. Highly elliptical for expected LEO
    if orbit_type == "LEO" and eccentricity > 0.2:
        return (
            "HIGHLY_ELLIPTICAL",
            f"Eccentricity {eccentricity:.3f} > 0.2 for LEO",
        )

    return ("VALID", "")
