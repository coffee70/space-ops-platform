---
title: Code Index Runbook
layer: platform
audience: ai-engineer
topics:
  - code-index
  - search
  - fallback
status: mvp
last_verified: 2026-06-01
---

# Code Index Runbook

## Purpose

This runbook explains what to do when code search fails or appears stale.

## Applies To

AI Engineer source discovery and code intelligence workflows.

## Core Concepts

Symptoms:
- code search returns no results
- repository is not indexed
- index status is stale
- indexed commit differs from current commit
- search results are irrelevant

## Procedure

1. Call `get_code_index_status`.
2. Confirm repository status.
3. If index is not ready, use `read_source_file` only for known paths.
4. If search results are poor, search for narrower symbols, file names, route names, or exact strings.
5. Do not invent paths.
6. If no path is known, inspect repository structure through available source tools.

## Do Not Assume

Do not treat missing search results as proof that a feature or file does not exist.

## Validation

The AI Engineer has either retrieved relevant code context or clearly stated the index limitation and used a safer fallback.

## Failure Modes

Stale index state can point to old code, while broad searches can return irrelevant files that look plausible.

## Related Docs

- [AI Engineer Tool Reference](./tool-reference.md)
