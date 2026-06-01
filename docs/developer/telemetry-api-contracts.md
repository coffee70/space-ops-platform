---
title: Telemetry API Contracts
layer: platform
audience: developer
topics:
  - telemetry-api
  - source-scoping
  - websockets
status: mvp
last_verified: 2026-06-01
---

# Telemetry API Contracts

## Purpose

This is the canonical Layer 2 telemetry API contract. Layer 3 apps consume this contract through REST and WebSocket APIs; Layer 2 owns the implementation and this document.

## Applies To

Telemetry samples, channel discovery, source registry behavior, feed status, operations timeline, realtime ingest, and WebSocket subscriptions.

## Core Concepts

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

## Procedure

Prefer source-scoped requests when showing live or historical telemetry for a selected source. Use explicit stream selections only for pinned historical or per-run views.

## Do Not Assume

Do not maintain duplicate canonical telemetry API contract docs in Layer 3. Do not infer source behavior from the frontend alone.

## Validation

Confirm endpoint status code, response shape, source filtering, and WebSocket subscription behavior through the platform gateway.

## Failure Modes

Missing or wrong `source_id` values can mix sources in watchlists, history, anomalies, and feed status views.

## Related Docs

- [Backend Service Patterns](./backend-service-patterns.md)
- [Platform HTTP Validation](../ai-engineer/platform-http-validation.md)
