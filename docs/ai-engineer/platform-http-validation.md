---
title: Platform HTTP Validation
layer: platform
audience: ai-engineer
topics:
  - http-validation
  - gateway-routes
  - operator-readiness
status: mvp
last_verified: 2026-06-01
---

# Platform HTTP Validation

## Purpose

This doc explains safe platform HTTP validation through the platform gateway.

## Applies To

Backend service routes, telemetry routes, intelligence routes, and other operator-facing platform APIs.

## Core Concepts

Use platform HTTP validation to verify routes through the platform gateway.

Examples:

```text
/services/{service_slug}/health
/telemetry/{path}
/intelligence/{path}
```

## Procedure

Use relative platform paths. Validate through the same route the operator or frontend will use. Capture status code and response shape.

## Do Not Assume

Do not call arbitrary absolute URLs. Do not treat container health as API validation.

## Validation

The expected route responds with the expected status code and response shape.

## Failure Modes

A service may be healthy internally but fail through the edge proxy or assigned gateway route.

## Related Docs

- [AI Engineer Tool Reference](./tool-reference.md)
- [Backend Service Patterns](../developer/backend-service-patterns.md)
