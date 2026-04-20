# Telemetry API Contracts (source_id and backward compatibility)

This document describes the `source_id` parameter added to telemetry endpoints for multi-source operations. All `source_id` parameters default to `"default"` for backward compatibility.

## HTTP Endpoints

### GET /telemetry/overview
- **source_id** (query, optional): `"default"` — Filters watchlist channels by stream source. Uses `telemetry_current` and fallback to `telemetry_data` per source.

### GET /telemetry/anomalies
- **source_id** (query, optional): `"default"` — Same as overview.

### GET /telemetry/{name}/recent
- **source_id** (query, optional): `"default"` — Filters historical time-series by source when `telemetry_data` is source-aware.
- **limit**, **since**, **until**: Unchanged.

### GET /telemetry/{name}/summary
- **source_id** (query, optional): `"default"` — Uses stats and recent data for the given source.

### GET /telemetry/{name}/explain
- **source_id** (query, optional): `"default"` — Same as summary.

### POST /telemetry/data
- **Body**: `{ telemetry_name, data: [...], source_id?: "default" }` — `source_id` scopes ingested data when `telemetry_data` is source-aware.

### POST /telemetry/realtime/ingest
- **Body events**: each event requires `source_id`, `stream_id`, timestamp information, value, channel identity, and stream-scoped `sequence`. Historical storage keys samples by stream, channel, timestamp, and sequence.

### POST /telemetry/recompute-stats
- **source_id** (query, optional): `null` — When set, recomputes only for that source. When `telemetry_statistics` is source-aware.
- **all_sources** (query, optional): `false` — When true, recomputes per source (when source-aware).

## Ops / Timeline

### GET /ops/feed-status
- **source_id** (query, optional): `"default"` — Returns feed health for the source.

### GET /ops/events
- **source_id** (query, optional): `"default"`
- **since_minutes** (query): lookback window
- **until_minutes** (query, optional): end of window (minutes ago)
- **event_types** (query, optional): comma-separated list
- **entity_type** (query, optional)
- **channel_name** (query, optional): filter by entity_id
- **limit**, **offset**: pagination

## WebSocket (realtime)

- **subscribe_watchlist**: `{ channels, source_id?: "default" }`
- **subscribe_channel**: `{ name, source_id?: "default" }`
- **subscribe_alerts**: `{ source_id?: "default" }`

All realtime snapshots and updates are already source-scoped via `source_id` in the subscription.
