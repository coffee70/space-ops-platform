"""Canonical managed fork path prefixes (Layer 1 control-plane code roots)."""

from __future__ import annotations

# Keep in sync with control-plane `Settings.allowed_code_roots`.
MANAGED_CODE_PATH_PREFIXES: tuple[str, ...] = (
    "project/space-ops-platform",
    "project/space-ops-apps",
    "manifests/units",
)

REPOSITORY_TO_MANAGED_PREFIX: dict[str, str] = {
    "space-ops-platform": "project/space-ops-platform",
    "project/space-ops-platform": "project/space-ops-platform",
    "space-ops-apps": "project/space-ops-apps",
    "project/space-ops-apps": "project/space-ops-apps",
    "manifests/units": "manifests/units",
}


def is_canonical_managed_code_path(path: str) -> bool:
    normalized = path.strip().lstrip("/")
    if not normalized:
        return False
    for root in MANAGED_CODE_PATH_PREFIXES:
        if normalized == root or normalized.startswith(f"{root}/"):
            return True
    return False


def canonicalize_managed_code_path(repository: str, path: str) -> str:
    """Resolve tool (repository, path) inputs to a single canonical path for code/file and indexing APIs."""
    repo = repository.strip()
    raw = path.strip()
    if not repo:
        raise ValueError("repository is required")
    if not raw:
        raise ValueError("path is required")
    if raw.startswith("/"):
        raise ValueError("absolute paths are not allowed for managed fork files")
    segments = [s for s in raw.split("/") if s != ""]
    if ".." in segments:
        raise ValueError("path traversal is not allowed")
    normalized = "/".join(segments)
    if not normalized:
        raise ValueError("path is required")

    if is_canonical_managed_code_path(normalized):
        return normalized

    prefix = REPOSITORY_TO_MANAGED_PREFIX.get(repo)
    if prefix is None:
        raise ValueError(f"unknown repository for managed code path: {repo!r}")

    if normalized == prefix or normalized.startswith(f"{prefix}/"):
        return normalized

    return f"{prefix}/{normalized}"
