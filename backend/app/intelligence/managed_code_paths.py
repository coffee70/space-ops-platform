"""Canonical managed fork path prefixes (Layer 1 control-plane code roots)."""

from __future__ import annotations

# Keep in sync with control-plane `Settings.allowed_code_roots`.
MANAGED_CODE_PATH_PREFIXES: tuple[str, ...] = (
    "project/space-ops-platform",
    "project/space-ops-apps",
    "manifests/units",
)


def is_canonical_managed_code_path(path: str) -> bool:
    normalized = path.strip().lstrip("/")
    if not normalized:
        return False
    for root in MANAGED_CODE_PATH_PREFIXES:
        if normalized == root or normalized.startswith(f"{root}/"):
            return True
    return False
