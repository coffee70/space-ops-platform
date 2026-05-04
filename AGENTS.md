# Agent instructions

**Read alongside:** platform code is exercised through Layer 1 orchestration and Layer 3 apps.

| Repository | Humans | Agents / automation |
|------------|--------|---------------------|
| `space-ops-platform` | [README.md](./README.md) | this file |
| `space-ops-kernel` | [../space-ops-kernel/README.md](../space-ops-kernel/README.md) | [../space-ops-kernel/AGENTS.md](../space-ops-kernel/AGENTS.md) |
| `space-ops-apps` | [../space-ops-apps/README.md](../space-ops-apps/README.md) | [../space-ops-apps/AGENTS.md](../space-ops-apps/AGENTS.md) |

## Repo role (Layer 2)

Keep changes scoped to platform APIs, schemas, data models, migrations, realtime processing, source/stream registry behavior, watchlist primitives, vehicle config validation/registry behavior, orbit framework code, position APIs, and provider interfaces.

Do not import Mission Control UI, simulator runtime, SatNOGS adapter runtime, or app-owned vehicle configuration files as source modules. Concrete config bundles are mounted at runtime through `VEHICLE_CONFIG_ROOT`.

## How to run tests

### Backend (Python)

**Canonical** — creates or reuses **`./.venv`** (gitignored), sets `PYTHONPATH`, default `DATABASE_URL`, and **`VEHICLE_CONFIG_ROOT`** to the sibling `../space-ops-apps/vehicle-configurations`:

```bash
./scripts/run-backend-tests.sh
```

Focused runs pass pytest args: `./scripts/run-backend-tests.sh backend/tests/path_to_test.py::test_name`.

Ad‑hoc after manual `venv`/`pip`:

```bash
PYTHONPATH=backend:. pytest backend/tests
```

Layer 2 does **not** own the control-plane test suite (`space-ops-kernel/control-plane/tests`).

### Agent runtime service (Node / TypeScript)

Source: `backend/services/agent-runtime-service/`.

**Canonical:** run through Layer 1’s Linux Node container so native dependencies match CI and Dockerized workflows:

```bash
../space-ops-kernel/scripts/validate-node.sh
```

That script runs `npm ci`, `npm run build`, and `npm test` for the agent runtime inside the container (and also runs Mission Control validation).

Host-only runs are fine for tight loops **only** after `npm ci` on the same OS/architecture as the interpreter; copying `node_modules` from containers or another OS breaks `esbuild` and similar packages.

### End-to-end / browser validation

Configured in Layer 3, executed via Layer 1’s Playwright container — see `../space-ops-kernel/scripts/validate-playwright.sh` and `../space-ops-apps/tools/playwright/README.md`.
