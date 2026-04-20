# Space Ops Platform

Layer 2 platform APIs, data models, services, schemas, and reusable operational primitives.

Extraction baseline: `c2-infra` commit `7b4f15ace9895c440ad89a9a460566c78135c57b` (`phase1-layer-split-baseline-2026-04-20`).

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

## Backend Development

Install dependencies from this repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt --extra-index-url https://download.pytorch.org/whl/cpu
```

Run tests:

```bash
PYTHONPATH=backend:. pytest backend/tests
```

Run the API against an existing database:

```bash
export DATABASE_URL=postgresql://telemetry:telemetry@localhost:5432/telemetry_db
export VEHICLE_CONFIG_ROOT=../space-ops-apps/vehicle-configurations
cd backend
alembic -c alembic.ini upgrade head
PYTHONPATH=.:.. uvicorn app.main:app --host 0.0.0.0 --port 8000
```

For the full stack, use `space-ops-kernel`.
