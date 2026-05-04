#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENVDIR="${PLATFORM_ROOT}/.venv"
PYTHON="${PYTHON:-python3}"
APPS_ROOT="$(cd "${PLATFORM_ROOT}/../space-ops-apps" && pwd)"

if [[ ! -d "${APPS_ROOT}/vehicle-configurations" ]]; then
  echo "error: expected sibling checkout at ${APPS_ROOT} with vehicle-configurations/" >&2
  exit 1
fi

export PYTHONPATH="${PLATFORM_ROOT}/backend:${PLATFORM_ROOT}:${PYTHONPATH:-}"
export DATABASE_URL="${DATABASE_URL:-postgresql://telemetry:telemetry@localhost:5432/telemetry_db}"
export VEHICLE_CONFIG_ROOT="${VEHICLE_CONFIG_ROOT:-${APPS_ROOT}/vehicle-configurations}"

if [[ ! -d "${VENVDIR}" ]]; then
  "${PYTHON}" -m venv "${VENVDIR}"
fi

echo "==> pip install (platform backend deps)"
"${VENVDIR}/bin/pip" install -q -r "${PLATFORM_ROOT}/backend/requirements.txt" \
  --extra-index-url https://download.pytorch.org/whl/cpu

echo "==> pytest backend/tests"
exec "${VENVDIR}/bin/pytest" "${PLATFORM_ROOT}/backend/tests" "$@"
