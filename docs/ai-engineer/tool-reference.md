---
title: AI Engineer Tool Reference
layer: platform
audience: ai-engineer
topics:
  - tools
  - validation
  - deployment
status: mvp
last_verified: 2026-06-01
---

# AI Engineer Tool Reference

## Purpose

This doc gives the AI Engineer a concise map of available tool categories and when to use them.

## Applies To

AI Engineer discovery, retrieval, code intelligence, telemetry, navigation, source changes, and deployment workflows.

## Core Concepts

### Discovery

- `list_available_tools`
- `list_platform_services`
- `get_platform_service`
- `list_platform_applications`
- `get_platform_application`
- `list_runtime_templates`
- `list_runtime_services`
- `list_managed_repositories`

### Knowledge retrieval

- `list_documents`
- `get_document`
- `search_documents`
- `trigger_document_reingestion`

### Code intelligence

- `get_code_index_status`
- `search_codebase`
- `read_source_file`
- `get_related_code_context`

### Telemetry

- `get_telemetry_schema`
- `query_recent_telemetry`
- `list_sources_or_adapters`

### Navigation and validation

- `navigate_to_application`
- `call_platform_http_get`
- `run_deployment_validation`
- `get_deployment_validation`

### Source changes

- `create_working_branch`
- `scaffold_service`
- `write_source_file`
- `create_commit`

### Deployment

- `resolve_preview_deploy_target`
- `deploy_service_or_application`
- `deploy_preview_change`
- `revert_preview_change`
- `get_deployment_status`
- `get_deployment_logs`
- `wait_for_deployment`

## Procedure

Search before reading source files unless the exact file path is already known. Read source files before editing. Check code index status before code search. Validate after deploy.

## Do Not Assume

Do not claim success from deploy status alone. Do not use delete tools unless explicitly authorized.

## Validation

Use route and UI validation tools to prove the expected operator-facing behavior after deployment.

## Failure Modes

Weak search results, stale indexes, or deploy-only evidence can lead to wrong edits or premature success claims.

## Related Docs

- [Code Index Runbook](./code-index-runbook.md)
- [Document Retrieval Runbook](./document-retrieval-runbook.md)
- [Platform HTTP Validation](./platform-http-validation.md)
