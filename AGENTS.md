# Agent Instructions

This repository is Layer 2. Keep changes scoped to platform APIs, schemas, data models, migrations, realtime processing, source/stream registry behavior, watchlist primitives, vehicle config validation/registry behavior, orbit framework code, position APIs, and provider interfaces.

Do not import Mission Control UI, simulator runtime, SatNOGS adapter runtime, or app-owned vehicle configuration files as source modules. Concrete config bundles are mounted at runtime through `VEHICLE_CONFIG_ROOT`.
