from __future__ import annotations

from datetime import datetime, timezone
import asyncio
import uuid

import httpx
from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.intelligence.events import raw_event
from app.intelligence.redaction import redact
from app.intelligence.tool_metadata import tool_summary
from app.intelligence.tool_permissions import build_permission_prompt, policy_for_mode
from app.intelligence.managed_code_paths import canonicalize_managed_code_path
from app.routes.handlers.tool_registry import SUPPORTED_TOOL_NAMES
from app.intelligence.schemas import ToolExecutionRequest, ToolPermissionApproveRequest, ToolPermissionDenyRequest
from app.intelligence.tool_validation import ToolInputValidationError, ToolSchemaDefinitionError, validate_tool_input
from app.intelligence.trace import extract_trace
from app.models.intelligence import ToolCall, ToolDefinition, ToolPermissionRequest
from platform_common.service_proxy import build_service_proxy_url


def _trace_identifier_text(body_value: object | None, fallback: str | None) -> str | None:
    return str(body_value) if body_value is not None else fallback


def _uuid_for_storage(value: str | None, field_name: str, *, required: bool = False) -> uuid.UUID | None:
    if not value:
        if required:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "invalid_tool_execution_trace_id",
                    "message": f"{field_name} is required and must be a valid UUID.",
                },
            )
        return None
    try:
        return uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error_code": "invalid_tool_execution_trace_id",
                "message": f"{field_name} must be a valid UUID.",
            },
        ) from exc


def _detail_from_http_response(resp: httpx.Response) -> str | dict:
    """Prefer JSON `detail` object from downstream FastAPI services for structured errors (e.g. 503 index)."""
    try:
        data = resp.json()
    except ValueError:
        return resp.text
    if isinstance(data, dict) and "detail" in data:
        return data["detail"]
    if isinstance(data, dict):
        return data
    return resp.text


def _cp_url(path: str) -> str:
    base = get_settings().control_plane_url.rstrip('/')
    return f"{base}/{path.lstrip('/')}"


async def _cp_get(path: str, params: dict | None = None) -> dict | list:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_cp_url(path), params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_detail_from_http_response(resp))
    return resp.json()


async def _cp_post(path: str, json_body: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(_cp_url(path), json=json_body)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_detail_from_http_response(resp))
    return resp.json()


async def _cp_post_with_timeout(path: str, json_body: dict, timeout: float) -> dict:
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(_cp_url(path), json=json_body)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_detail_from_http_response(resp))
    return resp.json()


async def _cp_put(path: str, json_body: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.put(_cp_url(path), json=json_body)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_detail_from_http_response(resp))
    return resp.json()


def _unit_field(unit: dict, snake: str, camel: str) -> str | None:
    value = unit.get(snake)
    if not isinstance(value, str):
        value = unit.get(camel)
    return value if isinstance(value, str) and value else None


def _normalize_changed_path(path: str) -> str:
    return path.strip().lstrip("./")


def _matches_source_path(changed_file: str, source_path: str) -> bool:
    normalized_file = _normalize_changed_path(changed_file)
    normalized_source = _normalize_changed_path(source_path).rstrip("/")
    return normalized_file == normalized_source or normalized_file.startswith(f"{normalized_source}/")


async def _resolve_preview_deploy_target(tool_input: dict) -> dict:
    changed_files = [_normalize_changed_path(path) for path in tool_input.get("changed_files", []) if isinstance(path, str) and path.strip()]
    units_response = await _cp_get("registry/units")
    units = units_response if isinstance(units_response, list) else []
    matches_by_unit: dict[str, dict] = {}
    unmatched_files: list[str] = []

    for changed_file in changed_files:
        candidates = []
        for unit in units:
            if not isinstance(unit, dict):
                continue
            source_path = _unit_field(unit, "source_path", "sourcePath")
            unit_id = _unit_field(unit, "unit_id", "unitId")
            if source_path and unit_id and _matches_source_path(changed_file, source_path):
                candidates.append((len(source_path), unit))
        if not candidates:
            unmatched_files.append(changed_file)
            continue
        _, best = max(candidates, key=lambda item: item[0])
        unit_id = _unit_field(best, "unit_id", "unitId")
        if unit_id:
            existing = matches_by_unit.setdefault(unit_id, {"unit": best, "changed_files": []})
            existing["changed_files"].append(changed_file)

    if not matches_by_unit:
        return {
            "status": "not_found",
            "confidence": "none",
            "branch": tool_input["branch"],
            "changed_files": changed_files,
            "unmatched_files": unmatched_files,
            "reason": "No changed files matched a managed runtime unit source_path.",
        }
    if len(matches_by_unit) > 1:
        return {
            "status": "ambiguous",
            "confidence": "low",
            "branch": tool_input["branch"],
            "matches": [
                {
                    "target_unit_id": unit_id,
                    "source_path": _unit_field(match["unit"], "source_path", "sourcePath"),
                    "runtime_kind": _unit_field(match["unit"], "runtime_kind", "runtimeKind"),
                    "changed_files": match["changed_files"],
                }
                for unit_id, match in sorted(matches_by_unit.items())
            ],
            "reason": "Changed files map to multiple managed runtime units. Ask for or choose a single deploy target explicitly.",
        }

    unit_id, match = next(iter(matches_by_unit.items()))
    unit = match["unit"]
    source_path = _unit_field(unit, "source_path", "sourcePath")
    runtime_kind = _unit_field(unit, "runtime_kind", "runtimeKind")
    target_application_id = tool_input.get("target_application_id")
    if not isinstance(target_application_id, str) or not target_application_id:
        target_application_id = _unit_field(unit, "application_id", "applicationId")
    if not target_application_id and runtime_kind == "frontend_shell":
        target_application_id = "telemetry"
    return {
        "status": "resolved",
        "target_unit_id": unit_id,
        "target_application_id": target_application_id,
        "runtime_kind": runtime_kind,
        "source_path": source_path,
        "confidence": "high",
        "branch": tool_input["branch"],
        "changed_files": match["changed_files"],
        "unmatched_files": unmatched_files,
        "reason": f"Changed file is under {source_path}, which maps to the managed {runtime_kind or 'runtime'} unit {unit_id}.",
    }


def _deployment_event(event_type: str, deployment: dict, *, tool_name: str = "deploy_preview_change", message: str | None = None) -> dict:
    status = str(deployment.get("status") or "unknown")
    payload = {
        "deployment_id": str(deployment.get("deployment_id") or "unknown"),
        "branch": str(deployment.get("branch") or "unknown"),
        "unit_id": str(deployment.get("unit_id") or deployment.get("target_unit_id") or "unknown"),
        "status": status,
    }
    if event_type == "deployment.requested":
        payload["tool_name"] = tool_name
    if event_type == "deployment.failed":
        payload["failure_reason"] = str(deployment.get("failure_reason") or message or "Deployment failed.")
    if event_type == "deployment.timeout":
        payload["message"] = message or "Deployment did not reach a terminal state in time."
    return raw_event(event_type=event_type, payload=payload, emitted_by="tool-execution-service", tool_call_id=None)


def _deployment_lifecycle_type(deployment: dict) -> str:
    status = deployment.get("status")
    if status == "healthy":
        return "preview.active"
    if status in {"failed", "replaced"}:
        return "deployment.failed"
    if status == "building":
        return "deployment.build_started"
    return "deployment.submitted"


async def _poll_deployment_until_terminal(deployment_id: str, *, timeout_seconds: float = 180.0, interval_seconds: float = 2.0) -> tuple[dict, list[dict]]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    events: list[dict] = []
    seen: set[tuple[str, str]] = set()
    latest: dict = {}
    while True:
        response = await _cp_get(f"deployments/{deployment_id}")
        latest = response if isinstance(response, dict) else {}
        event_type = _deployment_lifecycle_type(latest)
        key = (event_type, str(latest.get("status") or "unknown"))
        if key not in seen:
            seen.add(key)
            events.append(_deployment_event(event_type, latest))
            if event_type == "preview.active":
                events.append(_deployment_event("deployment.health_passed", latest))
        if latest.get("status") in {"healthy", "failed", "replaced"}:
            return latest, events
        if asyncio.get_running_loop().time() >= deadline:
            events.append(_deployment_event("deployment.timeout", latest, message="Preview deployment timed out before reaching a terminal state."))
            return {
                **latest,
                "status": "timeout",
                "failure_reason": "Preview deployment timed out before reaching a terminal state.",
            }, events
        await asyncio.sleep(interval_seconds)


def _deployment_next_diagnostic_tools(deployment: dict) -> list[dict]:
    deployment_id = deployment.get("deployment_id")
    if not isinstance(deployment_id, str) or not deployment_id:
        return []

    status = deployment.get("status")
    if status == "failed":
        return [{"tool_name": "get_deployment_logs", "input": {"deployment_id": deployment_id}}]
    if status == "timeout":
        return [
            {"tool_name": "get_deployment_status", "input": {"deployment_id": deployment_id}},
            {"tool_name": "wait_for_deployment", "input": {"deployment_id": deployment_id, "timeout_seconds": 120}},
        ]
    if status in {"healthy", "replaced"}:
        return []
    return [{"tool_name": "wait_for_deployment", "input": {"deployment_id": deployment_id}}]


async def _wait_for_deployment(tool_input: dict) -> dict:
    deployment_id = tool_input["deployment_id"]
    timeout_seconds = max(1, min(int(tool_input.get("timeout_seconds") or 120), 180))
    poll_interval_seconds = max(2, min(int(tool_input.get("poll_interval_seconds") or 5), 30))

    terminal_statuses = {"healthy", "failed", "replaced"}
    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + timeout_seconds
    latest: dict = {}

    while True:
        response = await _cp_get(f"deployments/{deployment_id}")
        latest = response if isinstance(response, dict) else {}
        status = latest.get("status")
        elapsed_seconds = int(loop.time() - started)

        if status in terminal_statuses:
            output = {
                **latest,
                "deployment_id": latest.get("deployment_id") or deployment_id,
                "terminal": True,
                "elapsed_seconds": elapsed_seconds,
            }
            if status == "failed":
                output["next_diagnostic_tools"] = [
                    {"tool_name": "get_deployment_logs", "input": {"deployment_id": deployment_id}},
                ]
            return output

        if loop.time() >= deadline:
            return {
                **latest,
                "deployment_id": latest.get("deployment_id") or deployment_id,
                "terminal": False,
                "elapsed_seconds": elapsed_seconds,
                "message": "Deployment did not reach a terminal state before timeout.",
                "next_diagnostic_tools": [
                    {"tool_name": "get_deployment_status", "input": {"deployment_id": deployment_id}},
                ],
            }

        await asyncio.sleep(poll_interval_seconds)


async def _deploy_preview_change(tool_input: dict, trace_payload: dict) -> dict:
    payload = {
        'branch': tool_input['branch'],
        'target_unit_id': tool_input['target_unit_id'],
        'conversation_id': trace_payload.get('conversation_id'),
        'agent_run_id': trace_payload.get('agent_run_id'),
    }
    for source_key, target_key in (
        ('commit_sha', 'commit_sha'),
        ('target_application_id', 'target_application_id'),
    ):
        if tool_input.get(source_key) is not None:
            payload[target_key] = tool_input[source_key]
    requested = raw_event(
        event_type="deployment.requested",
        payload={"tool_name": "deploy_preview_change", "branch": payload["branch"], "unit_id": payload["target_unit_id"]},
        emitted_by="tool-execution-service",
        tool_call_id=None,
    )
    try:
        submitted = await _cp_post_with_timeout('change-previews/deploy', payload, timeout=45.0)
    except httpx.TimeoutException:
        return {
            "status": "timeout",
            "deployment_id": None,
            "target_unit_id": payload["target_unit_id"],
            "target_application_id": payload.get("target_application_id"),
            "branch": payload["branch"],
            "message": "Preview deployment submission timed out before a deployment id was returned.",
            "next_diagnostic_tools": [],
            "_raw_events": [
                requested,
                raw_event(
                    event_type="deployment.timeout",
                    payload={
                        "deployment_id": "unknown",
                        "branch": payload["branch"],
                        "unit_id": payload["target_unit_id"],
                        "status": "timeout",
                        "message": "Preview deployment submission timed out before a deployment id was returned.",
                    },
                    emitted_by="tool-execution-service",
                    tool_call_id=None,
                ),
            ],
        }
    if not isinstance(submitted, dict):
        return {"deployment": submitted, "_raw_events": [requested]}
    deployment_id = submitted.get("deployment_id")
    events = [requested, _deployment_event("deployment.submitted", submitted)]
    final = submitted
    if isinstance(deployment_id, str) and submitted.get("status") not in {"healthy", "failed", "replaced"}:
        final, polled_events = await _poll_deployment_until_terminal(deployment_id)
        events.extend(polled_events)
    else:
        terminal_type = _deployment_lifecycle_type(submitted)
        if terminal_type != "deployment.submitted":
            events.append(_deployment_event(terminal_type, submitted))
            if terminal_type == "preview.active":
                events.append(_deployment_event("deployment.health_passed", submitted))
    output = {**submitted, **final}
    output["next_diagnostic_tools"] = _deployment_next_diagnostic_tools(output)
    return {**output, "_raw_events": events}


async def _runtime_get(slug: str, path: str, params: dict | None = None) -> dict | list:
    url = build_service_proxy_url(slug, path)
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
        resp = await client.get(url, params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_detail_from_http_response(resp))
    return resp.json()


async def _runtime_post(slug: str, path: str, json_body: dict | None = None) -> dict | list:
    url = build_service_proxy_url(slug, path)
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
        resp = await client.post(url, json=json_body or {})
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_detail_from_http_response(resp))
    return resp.json()


async def _execute_mapped_tool(name: str, tool_input: dict, *, db: Session, trace: dict | None = None):
    trace_payload = trace or {}
    # --- meta ---
    if name == 'list_available_tools':
        tools = (
            db.query(ToolDefinition)
            .filter(ToolDefinition.enabled.is_(True), ToolDefinition.name.in_(tuple(sorted(SUPPORTED_TOOL_NAMES))))
            .order_by(ToolDefinition.name.asc())
            .all()
        )
        return {'tools': [tool_summary(t) for t in tools]}

    # --- Layer 1 registry / templates / deployments ---
    if name == 'list_platform_services':
        return await _cp_get('registry/services')
    if name == 'get_platform_service':
        slug = tool_input.get('service_slug')
        if not slug:
            raise HTTPException(status_code=400, detail='service_slug is required')
        return await _cp_get(f'registry/services/{slug}')
    if name == 'list_platform_applications':
        return await _cp_get('registry/applications')
    if name == 'get_platform_application':
        app_id = tool_input.get('application_id')
        if not app_id:
            raise HTTPException(status_code=400, detail='application_id is required')
        return await _cp_get(f'registry/applications/{app_id}')
    if name == 'list_runtime_templates':
        raw = await _cp_get('templates')
        return raw if isinstance(raw, list) else raw
    if name == 'list_runtime_services':
        return await _cp_get('registry/units')
    if name == 'list_managed_repositories':
        return await _cp_get('code/roots')
    if name == 'resolve_preview_deploy_target':
        return await _resolve_preview_deploy_target(tool_input)
    if name == 'get_deployment_status':
        return await _cp_get(f"deployments/{tool_input['deployment_id']}")
    if name == 'get_deployment_logs':
        return await _cp_get(f"deployments/{tool_input['deployment_id']}/logs")
    if name == 'wait_for_deployment':
        return await _wait_for_deployment(tool_input)

    if name == 'create_working_branch':
        payload = {'branch': tool_input['branch'], 'from_branch': tool_input.get('from_branch') or 'main'}
        return await _cp_post('code/branches', payload)

    if name == 'scaffold_service':
        tid = tool_input['template_id']
        body = {
            'unit_id': tool_input['unit_id'],
            'display_name': tool_input['display_name'],
            'branch': tool_input.get('branch') or 'main',
        }
        if 'package_owner' in tool_input:
            body['package_owner'] = tool_input['package_owner']
        if 'source_path' in tool_input and tool_input['source_path']:
            body['source_path'] = tool_input['source_path']
        if 'discovery' in tool_input and isinstance(tool_input.get('discovery'), dict):
            body['discovery'] = tool_input['discovery']
        return await _cp_post(f'templates/{tid}/scaffold', body)

    if name == 'write_source_file':
        return await _cp_put(
            'code/file',
            {
                'branch': tool_input['branch'],
                'path': tool_input['path'],
                'content': tool_input['content'],
            },
        )

    if name == 'create_commit':
        return await _cp_post('code/commits', {'branch': tool_input['branch'], 'message': tool_input['message']})

    if name == 'deploy_service_or_application':
        dep = {'unit_id': tool_input['unit_id'], 'branch': tool_input.get('branch') or 'main'}
        if tool_input.get('commit_sha'):
            dep['commit_sha'] = tool_input['commit_sha']
        result = await _cp_post('deployments', dep)
        return result if isinstance(result, dict) else {'deployment': result}

    if name == 'deploy_preview_change':
        return await _deploy_preview_change(tool_input, trace_payload)

    if name == 'revert_preview_change':
        payload = {
            'target_unit_id': tool_input['target_unit_id'],
            'conversation_id': trace_payload.get('conversation_id'),
            'agent_run_id': trace_payload.get('agent_run_id'),
        }
        for source_key, target_key in (
            ('target_application_id', 'target_application_id'),
            ('baseline_branch', 'baseline_branch'),
            ('baseline_commit_sha', 'baseline_commit_sha'),
            ('preview_deployment_id', 'preview_deployment_id'),
        ):
            if tool_input.get(source_key) is not None:
                payload[target_key] = tool_input[source_key]
        result = await _cp_post('change-previews/revert', payload)
        return result if isinstance(result, dict) else {'deployment': result}

    if name == 'delete_managed_resources':
        mode = tool_input['mode']
        payload = {
            key: tool_input[key]
            for key in (
                'include_code',
                'include_runtime',
                'include_registry',
                'include_intelligence_records',
                'older_than_minutes',
                'unit_id',
                'deployment_id',
                'branch',
                'paths',
            )
            if key in tool_input
        }
        payload.update(
            {
                'conversation_id': trace_payload.get('conversation_id'),
                'agent_run_id': trace_payload.get('agent_run_id'),
                'request_id': trace_payload.get('request_id'),
                'tool_call_id': trace_payload.get('tool_call_id'),
            }
        )
        if mode == 'managed_unit':
            if not tool_input.get('unit_id'):
                raise HTTPException(status_code=400, detail='unit_id is required')
            return await _cp_post('internal/delete/managed-units', payload)
        if mode == 'code':
            if not tool_input.get('branch'):
                raise HTTPException(status_code=400, detail='branch is required')
            return await _cp_post('internal/delete/code', payload)
        if mode == 'stale':
            if not tool_input.get('older_than_minutes'):
                raise HTTPException(status_code=400, detail='older_than_minutes is required')
            return await _cp_post('internal/delete/stale', payload)
        raise HTTPException(status_code=400, detail='unsupported delete mode')

    # Layer 1 read file
    if name == 'read_source_file':
        return await _cp_get(
            'code/file',
            params={'branch': tool_input['branch'], 'path': tool_input['path']},
        )

    # --- Telemetry ---
    if name == 'get_telemetry_schema':
        return await _runtime_get(
            'telemetry-query-service',
            'telemetry/inventory',
            params={'source_id': tool_input['source_id']},
        )

    if name == 'query_recent_telemetry':
        ch = tool_input['name']
        lim = tool_input.get('limit') or 100
        return await _runtime_get(
            'telemetry-query-service',
            f'telemetry/{ch}/recent',
            params={'source_id': tool_input['source_id'], 'limit': lim},
        )

    if name == 'list_sources_or_adapters':
        return await _runtime_get('source-registry-service', 'telemetry/sources')

    # --- Documents ---
    if name == 'list_documents':
        return await _runtime_get('document-knowledge-service', '')
    if name == 'get_document':
        did = tool_input['document_id']
        return await _runtime_get('document-knowledge-service', did)
    if name == 'search_documents':
        return await _runtime_post(
            'document-knowledge-service',
            'search',
            {
                'query': tool_input['query'],
                'mission_id': tool_input.get('mission_id'),
                'vehicle_id': tool_input.get('vehicle_id'),
                'subsystem_id': tool_input.get('subsystem_id'),
                'limit': tool_input.get('limit') or 8,
            },
        )
    if name == 'trigger_document_reingestion':
        did = tool_input['document_id']
        return await _runtime_post('document-knowledge-service', f'{did}/reingest', {})

    # --- Code intelligence ---
    if name == 'search_codebase':
        return await _runtime_post(
            'code-intelligence-service',
            'search',
            {
                'query': tool_input['query'],
                'repository': tool_input.get('repository'),
                'branch': tool_input.get('branch') or 'main',
                'limit': tool_input.get('limit') or 6,
            },
        )
    if name == 'get_related_code_context':
        try:
            fp = canonicalize_managed_code_path(tool_input['repository'], tool_input['path'])
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={'error_code': 'invalid_managed_code_path', 'message': str(exc)},
            ) from exc
        payload = {'file_path': fp, 'branch': tool_input.get('branch') or 'main'}
        if tool_input.get('line'):
            payload['line'] = tool_input['line']
        if tool_input.get('limit'):
            payload['limit'] = tool_input['limit']
        return await _runtime_post('code-intelligence-service', 'related-context', payload)

    # --- Navigation ---
    if name == 'navigate_to_application':
        app_id = tool_input.get('application_id')
        if not app_id:
            raise HTTPException(status_code=400, detail='application_id is required')
        return {'action': 'navigate_to_application', 'application_id': app_id, 'route_path': tool_input.get('route_path') or f'/apps/{app_id}'}

    raise HTTPException(status_code=501, detail=f'tool handler not implemented: {name}')


async def execute_tool(body: ToolExecutionRequest, request: Request, db: Session = Depends(get_db)):
    trace = extract_trace(request, require_run=True, require_conversation=False)
    conversation_id = _trace_identifier_text(body.conversation_id, trace.get("conversation_id"))
    agent_run_id = _trace_identifier_text(body.agent_run_id, trace["agent_run_id"])
    request_id = _trace_identifier_text(body.request_id, trace["request_id"])
    tool_call_id = _trace_identifier_text(body.tool_call_id, trace.get("tool_call_id"))
    if not tool_call_id:
        raise HTTPException(status_code=400, detail="tool_call_id is required")
    conversation_uuid = _uuid_for_storage(conversation_id, "conversation_id")
    agent_run_uuid = _uuid_for_storage(agent_run_id, "agent_run_id", required=True)
    request_uuid = _uuid_for_storage(request_id, "request_id", required=True)
    tool_call_uuid = _uuid_for_storage(tool_call_id, "tool_call_id", required=True)
    tool = db.query(ToolDefinition).filter(ToolDefinition.name == body.tool_name).one_or_none()
    if not tool:
        raise HTTPException(status_code=404, detail='tool not found')
    if not tool.enabled:
        raise HTTPException(status_code=400, detail='tool disabled')
    mode_policy = policy_for_mode(tool, body.execution_mode)
    if mode_policy == "disabled":
        raise HTTPException(
            status_code=403,
            detail={
                "error_code": "tool_execution_mode_forbidden",
                "message": "tool not allowed in current execution mode",
                "requested_execution_mode": body.execution_mode,
                "mode_policy": mode_policy,
            },
        )
    try:
        validate_tool_input(tool.input_schema_json or {'type': 'object', 'properties': {}, 'additionalProperties': False}, body.input)
    except ToolInputValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                'error_code': 'tool_input_validation_failed',
                'message': 'tool input failed schema validation',
                'errors': exc.errors,
            },
        ) from exc
    except ToolSchemaDefinitionError as exc:
        raise HTTPException(
            status_code=500,
            detail={'error_code': 'invalid_tool_schema', 'message': str(exc)},
        ) from exc

    permission_request: ToolPermissionRequest | None = None
    if mode_policy == "requires_permission":
        if body.permission_request_id:
            permission_request = db.query(ToolPermissionRequest).filter(ToolPermissionRequest.id == body.permission_request_id).one_or_none()
            if (
                not permission_request
                or str(permission_request.tool_call_id) != tool_call_id
                or permission_request.tool_name != body.tool_name
            ):
                raise HTTPException(
                    status_code=403,
                    detail={
                        "error_code": "tool_permission_not_approved",
                        "message": "tool permission request has not been approved",
                    },
                )
            if permission_request.status == "denied":
                return {
                    "conversation_id": conversation_id,
                    "agent_run_id": agent_run_id,
                    "request_id": request_id,
                    "tool_call_id": tool_call_id,
                    "status": "permission_denied",
                    "output": permission_request.response_json
                    or {
                        "status": "permission_denied",
                        "reason": "user_denied",
                        "message": "The user denied this tool call. No action was taken.",
                    },
                    "raw_events": [
                        raw_event(
                            event_type="tool.permission_denied",
                            payload={
                                "tool_name": body.tool_name,
                                "tool_call_id": tool_call_id,
                                "permission_request_id": str(permission_request.id),
                                "reason": (permission_request.response_json or {}).get("reason") or "user_denied",
                            },
                            emitted_by="tool-execution-service",
                            tool_call_id=tool_call_id,
                        )
                    ],
                }
            if permission_request.status not in {"approved", "executing"}:
                raise HTTPException(
                    status_code=403,
                    detail={
                        "error_code": "tool_permission_not_approved",
                        "message": "tool permission request has not been approved",
                    },
                )
            permission_request.status = "executing"
            permission_request.resolved_at = datetime.now(timezone.utc)
        else:
            prompt = build_permission_prompt(body.tool_name, body.input, getattr(tool, "permission_prompt_json", {}) or {})
            permission_request = ToolPermissionRequest(
                id=uuid.uuid4(),
                conversation_id=conversation_uuid,
                agent_run_id=agent_run_uuid,
                request_id=request_uuid,
                tool_call_id=tool_call_uuid,
                tool_name=body.tool_name,
                input_json=body.input,
                redacted_input_json=redact(body.input),
                status="pending",
                prompt_json=prompt,
                mode_policy_json=getattr(tool, "mode_policy_json", {}) or {},
                execution_mode=body.execution_mode,
                created_at=datetime.now(timezone.utc),
            )
            call = ToolCall(
                conversation_id=conversation_uuid,
                agent_run_id=agent_run_uuid,
                request_id=request_uuid,
                tool_call_id=tool_call_uuid,
                tool_name=body.tool_name,
                input_json=body.input,
                redacted_input_json=redact(body.input),
                status="permission_required",
                started_at=datetime.now(timezone.utc),
            )
            db.add(call)
            db.add(permission_request)
            db.flush()
            permission_event = raw_event(
                event_type="tool.permission_required",
                payload={
                    "tool_name": tool.name,
                    "tool_call_id": tool_call_id,
                    "permission_request_id": str(permission_request.id),
                    "execution_mode": body.execution_mode,
                    "prompt": prompt,
                },
                emitted_by="tool-execution-service",
                tool_call_id=tool_call_id,
            )
            return {
                "conversation_id": conversation_id,
                "agent_run_id": agent_run_id,
                "request_id": request_id,
                "tool_call_id": tool_call_id,
                "status": "permission_required",
                "output": {
                    "permission_request_id": str(permission_request.id),
                    "tool_name": tool.name,
                    "prompt": prompt,
                },
                "raw_events": [permission_event],
            }
    elif tool.requires_confirmation and not body.confirmation_token:
        return {
            'conversation_id': conversation_id,
            'agent_run_id': agent_run_id,
            'request_id': request_id,
            'tool_call_id': tool_call_id,
            'status': 'confirmation_required',
            'output': {'error_code': 'confirmation_required', 'message': 'confirmation token required'},
            'raw_events': [],
        }

    if permission_request:
        call = db.query(ToolCall).filter(ToolCall.tool_call_id == tool_call_uuid).one_or_none()
        if call is None:
            call = ToolCall(
                conversation_id=conversation_uuid,
                agent_run_id=agent_run_uuid,
                request_id=request_uuid,
                tool_call_id=tool_call_uuid,
                tool_name=body.tool_name,
                input_json=body.input,
                redacted_input_json=redact(body.input),
                status='running',
                started_at=datetime.now(timezone.utc),
            )
            db.add(call)
        else:
            call.status = 'running'
            call.started_at = datetime.now(timezone.utc)
    else:
        call = ToolCall(
            conversation_id=conversation_uuid,
            agent_run_id=agent_run_uuid,
            request_id=request_uuid,
            tool_call_id=tool_call_uuid,
            tool_name=body.tool_name,
            input_json=body.input,
            redacted_input_json=redact(body.input),
            status='running',
            started_at=datetime.now(timezone.utc),
        )
        db.add(call)
    db.flush()
    raw_events = []
    if permission_request:
        raw_events.append(
            raw_event(
                event_type="tool.permission_approved",
                payload={
                    "tool_name": body.tool_name,
                    "tool_call_id": tool_call_id,
                    "permission_request_id": str(permission_request.id),
                },
                emitted_by="tool-execution-service",
                tool_call_id=tool_call_id,
            )
        )
    started_event = raw_event(
        event_type='tool.started',
        payload={
            'tool_name': tool.name,
            'category': tool.category,
            'read_write_classification': tool.read_write_classification,
            'input_preview': redact(body.input),
        },
        emitted_by='tool-execution-service',
        tool_call_id=tool_call_id,
    )

    try:
        output = await _execute_mapped_tool(
            body.tool_name,
            body.input,
            db=db,
            trace={
                "conversation_id": conversation_id,
                "agent_run_id": agent_run_id,
                "request_id": request_id,
                "tool_call_id": tool_call_id,
            },
        )
        mapped_raw_events = output.pop("_raw_events", []) if isinstance(output, dict) else []
    except HTTPException:
        raise
    except Exception as exc:
        call.status = 'failed'
        call.error_message = str(exc)
        call.completed_at = datetime.now(timezone.utc)
        if permission_request:
            permission_request.status = "failed"
            permission_request.response_json = {'error_code': 'tool_execution_failed', 'message': str(exc)}
            permission_request.resolved_at = datetime.now(timezone.utc)
        return {
            'conversation_id': conversation_id,
            'agent_run_id': agent_run_id,
            'request_id': request_id,
            'tool_call_id': tool_call_id,
            'status': 'failed',
            'output': {'error_code': 'tool_execution_failed', 'message': str(exc)},
            'raw_events': [
                *raw_events,
                started_event,
                raw_event(
                    event_type='tool.failed',
                    payload={'tool_name': body.tool_name, 'error_code': 'tool_execution_failed', 'message': str(exc), 'duration_ms': int((call.completed_at - call.started_at).total_seconds() * 1000)},
                    emitted_by='tool-execution-service',
                    tool_call_id=tool_call_id,
                )
            ],
        }

    call.status = 'completed'
    call.output_json = redact(output if isinstance(output, dict) else {'result': output})
    call.completed_at = datetime.now(timezone.utc)
    if permission_request:
        permission_request.status = "executed"
        permission_request.response_json = call.output_json
        permission_request.resolved_at = datetime.now(timezone.utc)
    raw_events = [
        *raw_events,
        started_event,
        *(mapped_raw_events if isinstance(mapped_raw_events, list) else []),
        raw_event(
            event_type='tool.completed',
            payload={'tool_name': body.tool_name, 'status': 'completed', 'result_preview': redact(call.output_json), 'duration_ms': int((call.completed_at - call.started_at).total_seconds() * 1000)},
            emitted_by='tool-execution-service',
            tool_call_id=tool_call_id,
        )
    ]
    if body.tool_name == 'navigate_to_application':
        raw_events.append(
            raw_event(
                event_type='navigation.requested',
                payload=output,
                emitted_by='tool-execution-service',
                tool_call_id=tool_call_id,
            )
        )
    return {'conversation_id': conversation_id,'agent_run_id': agent_run_id,'request_id': request_id,'tool_call_id': tool_call_id,'status': 'completed','output': output,'raw_events': raw_events}


def _permission_status_response(permission: ToolPermissionRequest, raw_events: list[dict] | None = None) -> dict:
    return {
        "permission_request_id": str(permission.id),
        "tool_call_id": str(permission.tool_call_id),
        "status": permission.status,
        "response_json": permission.response_json,
        "raw_events": raw_events or [],
    }


def get_tool_permission_status(permission_request_id: str, db: Session = Depends(get_db)):
    try:
        permission_uuid = uuid.UUID(permission_request_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid permission request id") from exc
    permission = db.query(ToolPermissionRequest).filter(ToolPermissionRequest.id == permission_uuid).one_or_none()
    if not permission:
        raise HTTPException(status_code=404, detail="permission request not found")
    raw_events = []
    if permission.status == "denied":
        response = permission.response_json or {}
        raw_events.append(
            raw_event(
                event_type="tool.permission_denied",
                payload={
                    "tool_name": permission.tool_name,
                    "tool_call_id": str(permission.tool_call_id),
                    "permission_request_id": str(permission.id),
                    "reason": response.get("reason") or "user_denied",
                },
                emitted_by="tool-execution-service",
                tool_call_id=str(permission.tool_call_id),
            )
        )
    return _permission_status_response(permission, raw_events)


def approve_tool_permission(permission_request_id: str, body: ToolPermissionApproveRequest = ToolPermissionApproveRequest(), db: Session = Depends(get_db)):
    try:
        permission_uuid = uuid.UUID(permission_request_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid permission request id") from exc
    permission = db.query(ToolPermissionRequest).filter(ToolPermissionRequest.id == permission_uuid).one_or_none()
    if not permission:
        raise HTTPException(status_code=404, detail="permission request not found")
    if permission.status not in {"pending", "approved"}:
        raise HTTPException(status_code=409, detail=f"permission request is {permission.status}")
    permission.status = "approved"
    permission.resolved_at = datetime.now(timezone.utc)
    event = raw_event(
        event_type="tool.permission_approved",
        payload={
            "tool_name": permission.tool_name,
            "tool_call_id": str(permission.tool_call_id),
            "permission_request_id": str(permission.id),
        },
        emitted_by="tool-execution-service",
        tool_call_id=str(permission.tool_call_id),
    )
    db.flush()
    return _permission_status_response(permission, [event])


def deny_tool_permission(permission_request_id: str, body: ToolPermissionDenyRequest = ToolPermissionDenyRequest(), db: Session = Depends(get_db)):
    try:
        permission_uuid = uuid.UUID(permission_request_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid permission request id") from exc
    permission = db.query(ToolPermissionRequest).filter(ToolPermissionRequest.id == permission_uuid).one_or_none()
    if not permission:
        raise HTTPException(status_code=404, detail="permission request not found")
    if permission.status not in {"pending", "denied"}:
        raise HTTPException(status_code=409, detail=f"permission request is {permission.status}")
    permission.status = "denied"
    permission.response_json = {
        "status": "permission_denied",
        "reason": body.reason or "user_denied",
        "message": "The user denied this tool call. No action was taken.",
    }
    permission.resolved_at = datetime.now(timezone.utc)
    event = raw_event(
        event_type="tool.permission_denied",
        payload={
            "tool_name": permission.tool_name,
            "tool_call_id": str(permission.tool_call_id),
            "permission_request_id": str(permission.id),
            "reason": body.reason or "user_denied",
        },
        emitted_by="tool-execution-service",
        tool_call_id=str(permission.tool_call_id),
    )
    db.flush()
    return _permission_status_response(permission, [event])
