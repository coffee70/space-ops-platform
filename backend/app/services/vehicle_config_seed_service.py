"""First-run seeding for persistent vehicle configuration storage."""

from __future__ import annotations

import shutil
from pathlib import Path

from telemetry_catalog.definitions import vehicle_config_root


def bundled_vehicle_config_seed_root() -> Path:
    """Return the packaged baseline vehicle configuration directory."""

    return Path(__file__).resolve().parents[2] / "resources" / "vehicle-configurations"


def ensure_vehicle_config_seeded() -> None:
    """Seed the editable vehicle config root from bundled baselines when empty."""

    target_root = vehicle_config_root()
    seed_root = bundled_vehicle_config_seed_root()
    target_root.mkdir(parents=True, exist_ok=True)

    if any(target_root.rglob("*")):
        return
    if not seed_root.is_dir():
        raise FileNotFoundError(f"Vehicle configuration seed directory not found at {seed_root}")

    shutil.copytree(seed_root, target_root, dirs_exist_ok=True)
