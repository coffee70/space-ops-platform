"""Orbit type classification by altitude band."""

from __future__ import annotations

# Altitude bands (km) - perigee/apogee/mean used to classify
LEO_ALT_MAX_KM = 2000.0
MEO_ALT_MIN_KM = 2000.0
MEO_ALT_MAX_KM = 35786.0
GEO_ALT_KM = 35786.0  # ~GEO


def classify_orbit(perigee_alt_km: float, apogee_alt_km: float) -> str:
    """Classify orbit as LEO, MEO, or GEO based on perigee/apogee altitudes (km)."""
    mean_alt = 0.5 * (perigee_alt_km + apogee_alt_km)
    if mean_alt < LEO_ALT_MAX_KM:
        return "LEO"
    if mean_alt >= GEO_ALT_KM - 500:  # allow small tolerance
        return "GEO"
    if MEO_ALT_MIN_KM <= mean_alt <= MEO_ALT_MAX_KM:
        return "MEO"
    # between 2000 and ~35786
    return "MEO"
