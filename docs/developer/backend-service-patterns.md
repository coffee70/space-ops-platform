---
title: Backend Service Patterns
layer: platform
audience: developer
topics:
  - backend-services
  - managed-units
  - telemetry-semantics
status: mvp
last_verified: 2026-06-01
---

# Backend Service Patterns

## Purpose

This doc explains how managed backend services should be structured and validated.

## Applies To

Layer 2 managed backend services under `backend/services/{service-name}`.

## Core Concepts

Managed backend services usually live under:

```text
backend/services/{service-name}
```

A backend service should include:
- service entrypoint
- health endpoint
- typed request/response models if applicable
- tests
- Dockerfile or runtime build configuration
- integration route or registry metadata when required

Every managed service should expose a health endpoint compatible with its unit manifest.

Services should be callable through the platform/kernel gateway path assigned by registry metadata.

## Procedure

Read existing service patterns before editing. Keep request and response shapes typed where applicable, expose the manifest health path, and validate through the gateway route the frontend or operator workflow will use.

## Do Not Assume

If a service interprets telemetry, vehicle state, subsystem behavior, or channel-specific meaning, that interpretation must come from the current vehicle configuration, telemetry dictionary, ICD, operator documentation, or other approved source documents.

Do not:
- hardcode localhost URLs in app-facing code
- bypass the platform gateway for operator workflows
- invent telemetry channel names
- invent vehicle semantics

## Validation

Run relevant backend validation for code changes and call the expected gateway route to confirm status code and response shape.

## Failure Modes

Container health can pass while the gateway route fails because registry metadata, proxy routing, or API base paths are wrong.

## Related Docs

- [Telemetry API Contracts](./telemetry-api-contracts.md)
- [Platform HTTP Validation](../ai-engineer/platform-http-validation.md)
- Related canonical doc: `space-ops-kernel/docs/platform/deployment-lifecycle.md`
