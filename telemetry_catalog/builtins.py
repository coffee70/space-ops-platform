"""Built-in source identities and local-stack registration specs."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_SOURCE_ID = "86a0057f-4733-4de6-af60-455cb3954f1d"
MOCK_VEHICLE_SOURCE_ID = "9a157057-347a-46c2-8626-fd3d7245b5eb"
DROGONSAT_SOURCE_ID = "27a7e3d4-bbcc-4fa1-9e14-8ebabbea1be6"
RHAEGALSAT_SOURCE_ID = "63b0c0ab-8173-44ff-918f-2616ebb449b8"

LEGACY_SOURCE_ID_ALIASES = {
    "default": DEFAULT_SOURCE_ID,
    "mock_vehicle": MOCK_VEHICLE_SOURCE_ID,
    "simulator": DROGONSAT_SOURCE_ID,
    "simulator2": RHAEGALSAT_SOURCE_ID,
}


@dataclass(frozen=True)
class BuiltInSourceSpec:
    id: str
    name: str
    description: str
    source_type: str
    vehicle_config_path: str
    base_url: str | None = None


BUILT_IN_SOURCES = (
    BuiltInSourceSpec(
        id=DEFAULT_SOURCE_ID,
        name="Aegon Relay",
        description="Baseline operator training vehicle",
        source_type="vehicle",
        vehicle_config_path="vehicles/aegon-relay.yaml",
    ),
    BuiltInSourceSpec(
        id=MOCK_VEHICLE_SOURCE_ID,
        name="Balerion Surveyor",
        description="CLI mock vehicle stream for source-aware validation",
        source_type="vehicle",
        vehicle_config_path="vehicles/balerion-surveyor.json",
    ),
    BuiltInSourceSpec(
        id=DROGONSAT_SOURCE_ID,
        name="DrogonSat",
        description="Agile tactical simulator with GPS LLA telemetry",
        source_type="simulator",
        base_url="http://simulator:8001",
        vehicle_config_path="simulators/drogonsat.yaml",
    ),
    BuiltInSourceSpec(
        id=RHAEGALSAT_SOURCE_ID,
        name="RhaegalSat",
        description="Heavy survey simulator with ECEF position telemetry",
        source_type="simulator",
        base_url="http://simulator2:8001",
        vehicle_config_path="simulators/rhaegalsat.json",
    ),
)
