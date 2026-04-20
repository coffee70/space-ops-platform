"""Expose repo-root simulator package under the standard PYTHONPATH=backend flow."""

from pathlib import Path

_repo_root_package = Path(__file__).resolve().parents[2] / "simulator"

if _repo_root_package.exists():
    __path__.append(str(_repo_root_package))
