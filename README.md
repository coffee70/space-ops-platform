# Space Ops Platform

Layer 2 platform APIs, data models, services, schemas, and reusable operational primitives.

Extraction baseline: `c2-infra` commit `7b4f15ace9895c440ad89a9a460566c78135c57b` (`phase1-layer-split-baseline-2026-04-20`).

## Documentation map (split checkout)

| Area | Humans | Agents / automation |
|------|--------|---------------------|
| **This repo — backend pytest, telemetry catalog** | this file | [AGENTS.md](./AGENTS.md) |
| **Layer 1 — Compose, Node/Playwright validation scripts** | [../space-ops-kernel/README.md](../space-ops-kernel/README.md) | [../space-ops-kernel/AGENTS.md](../space-ops-kernel/AGENTS.md) |
| **Layer 3 — Mission Control, Playwright workspace, simulator, adapter** | [../space-ops-apps/README.md](../space-ops-apps/README.md) | [../space-ops-apps/AGENTS.md](../space-ops-apps/AGENTS.md) |

Compose and canonical **containerized Node**/`npm ci` workflows live in **`space-ops-kernel`**; read that README whenever tests span services.

## Role

This repository owns the FastAPI backend, SQLAlchemy models, Alembic migrations, telemetry ingestion/query/source/stream APIs, realtime bus and WebSocket behavior, watchlist framework, vehicle config validation and registry APIs, orbit framework, position APIs, provider interfaces, and the `telemetry_catalog/` schema package source.

It does not own Mission Control UI code, simulator runtime behavior, SatNOGS adapter runtime behavior, concrete vehicle configuration assets, or Docker Compose orchestration.

## Contents

```text
backend/                       FastAPI app, migrations, platform tests
telemetry_catalog/             Shared telemetry schema/config package source
docs/API_TELEMETRY_CONTRACTS.md
```

The platform runtime expects concrete vehicle configs to be mounted by Layer 1 and supplied through `VEHICLE_CONFIG_ROOT`.

## Backend development

Install dependencies from this repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt --extra-index-url https://download.pytorch.org/whl/cpu
```

### Backend tests (`pytest`)

```bash
PYTHONPATH=backend:. pytest backend/tests
```

Focused example:

```bash
PYTHONPATH=backend:. pytest backend/tests/test_vehicle_config_service.py::test_specific_case -vv
```

`conftest`/fixtures configure paths such as vehicle config fixtures; Postgres-backed cases require a running database when applicable (see suite / env docs).

**Control-plane integration tests are not here** — they live under [`../space-ops-kernel/control-plane/tests`](../space-ops-kernel/control-plane/tests) and track Layer 1’s control-plane codebase.

### Agent runtime service (`backend/services/agent-runtime-service`)

**Canonical CI-style verification** (Linux Node container, reproducible native deps):

```bash
../space-ops-kernel/scripts/validate-node.sh
```

For host-only workflows, install and test **from that directory on the host** after fresh `npm ci` on matching OS/arch:

```bash
cd backend/services/agent-runtime-service
npm ci && npm run build && npm test
```

Avoid trusting a reused `node_modules` tree staged for Docker/Linux on another OS (breaks native packages such as esbuild).

### Browser / Mission Control suites

Orchestrated via Layer 1 + Layer 3: [`../space-ops-kernel/scripts/validate-playwright.sh`](../space-ops-kernel/scripts/validate-playwright.sh) and [`../space-ops-apps/tools/playwright/README.md`](../space-ops-apps/tools/playwright/README.md).

### Run the API against an existing database

```bash
export DATABASE_URL=postgresql://telemetry:telemetry@localhost:5432/telemetry_db
export VEHICLE_CONFIG_ROOT=../space-ops-apps/vehicle-configurations
cd backend
alembic -c alembic.ini upgrade head
PYTHONPATH=.:.. uvicorn app.main:app --host 0.0.0.0 --port 8000
```

For the full stack, start Compose from **`space-ops-kernel`** (see sibling README).
