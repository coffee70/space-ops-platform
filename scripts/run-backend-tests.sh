#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENVDIR="${PLATFORM_ROOT}/.venv"
PYTHON="${PYTHON:-python3}"
DEFAULT_VEHICLE_CONFIG_ROOT="${PLATFORM_ROOT}/backend/resources/vehicle-configurations"

if [[ ! -d "${DEFAULT_VEHICLE_CONFIG_ROOT}" ]]; then
  echo "error: expected Layer 2 vehicle resources at ${DEFAULT_VEHICLE_CONFIG_ROOT}" >&2
  exit 1
fi

export PYTHONPATH="${PLATFORM_ROOT}/backend:${PLATFORM_ROOT}:${PYTHONPATH:-}"
export DATABASE_URL="${DATABASE_URL:-postgresql://telemetry:telemetry@localhost:5432/telemetry_db}"
export VEHICLE_CONFIG_ROOT="${VEHICLE_CONFIG_ROOT:-${DEFAULT_VEHICLE_CONFIG_ROOT}}"

if [[ ! -d "${VENVDIR}" ]]; then
  "${PYTHON}" -m venv "${VENVDIR}"
fi

echo "==> pip install (platform backend deps)"
"${VENVDIR}/bin/pip" install -q -r "${PLATFORM_ROOT}/backend/requirements.txt" \
  --extra-index-url https://download.pytorch.org/whl/cpu

echo "==> pytest backend/tests"
exec "${VENVDIR}/bin/pytest" "${PLATFORM_ROOT}/backend/tests" "$@"
