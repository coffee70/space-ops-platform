#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENVDIR="${PLATFORM_ROOT}/.venv"
PYTHON="${PYTHON:-python3}"
DEFAULT_VEHICLE_CONFIG_ROOT="${PLATFORM_ROOT}/backend/resources/vehicle-configurations"
PLATFORM_TEST_WORKERS="${PLATFORM_TEST_WORKERS:-serial}"

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

if [[ "${SKIP_PIP_INSTALL:-0}" == "1" ]]; then
  echo "==> skip pip install (SKIP_PIP_INSTALL=1)"
else
  echo "==> pip install (platform backend deps)"
  "${VENVDIR}/bin/pip" install -q -r "${PLATFORM_ROOT}/backend/requirements.txt" \
    --extra-index-url https://download.pytorch.org/whl/cpu
fi

PYTEST_ARGS=(backend/tests)
case "${PLATFORM_TEST_WORKERS}" in
  ""|"serial"|"1")
    ;;
  "auto")
    PYTEST_ARGS=(-n auto "${PYTEST_ARGS[@]}")
    ;;
  *)
    if [[ "${PLATFORM_TEST_WORKERS}" =~ ^[0-9]+$ ]] && [[ "${PLATFORM_TEST_WORKERS}" -gt 1 ]]; then
      PYTEST_ARGS=(-n "${PLATFORM_TEST_WORKERS}" "${PYTEST_ARGS[@]}")
    else
      echo "error: PLATFORM_TEST_WORKERS must be auto, serial, 1, or an integer greater than 1" >&2
      exit 1
    fi
    ;;
esac

echo "==> pytest backend/tests"
cd "${PLATFORM_ROOT}"
"${VENVDIR}/bin/pytest" "${PYTEST_ARGS[@]}" "$@"

if [[ -f "${PLATFORM_ROOT}/backend/services/agent-runtime-service/package.json" ]]; then
  echo "==> npm test backend/services/agent-runtime-service"
  cd "${PLATFORM_ROOT}/backend/services/agent-runtime-service"
  npm test
fi
