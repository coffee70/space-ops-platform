---
title: Layer 2 Role
layer: platform
audience: ai-engineer
topics:
  - platform-apis
  - repo-boundaries
  - backend-services
status: mvp
last_verified: 2026-06-01
---

# Layer 2 Role

## Purpose

This doc helps the AI Engineer decide when backend work belongs in `space-ops-platform`.

## Applies To

FastAPI routes, platform services, telemetry ingestion/query, source registry behavior, intelligence services, simulator behavior, SatNOGS behavior, and vehicle configuration validation.

## Core Concepts

Layer 2 owns platform APIs, backend services, operational data models, and intelligence services.

It owns:
- FastAPI backend
- telemetry APIs
- source and stream registry APIs
- telemetry ingestion/query services
- intelligence routes
- document knowledge service
- code intelligence service
- AI Engineer tool registry/execution
- backend service implementations
- simulator service behavior
- SatNOGS adapter behavior
- vehicle config validation/registry

It does not own:
- Docker Compose orchestration
- edge proxy configuration
- frontend Mission Control implementation
- frontend application loader manifests
- managed runtime deployment lifecycle

## Procedure

Change Layer 2 when the task is about API behavior, backend services, telemetry semantics, simulator runtime behavior, source registration, or intelligence/tool backend behavior.

## Do Not Assume

Do not implement frontend UI behavior or Layer 1 deployment lifecycle changes here.

## Validation

Use backend tests for platform logic and gateway route validation for operator-facing platform APIs.

## Failure Modes

A healthy backend service may still be unavailable to the frontend if Layer 1 routing or Layer 3 route consumption is wrong.

## Related Docs

- [Backend Service Patterns](../developer/backend-service-patterns.md)
- [Telemetry API Contracts](../developer/telemetry-api-contracts.md)
