from __future__ import annotations

from datetime import datetime, timezone
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


async def _cp_put(path: str, json_body: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.put(_cp_url(path), json=json_body)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_detail_from_http_response(resp))
    return resp.json()


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
        payload = {
            'unit_id': tool_input['target_unit_id'],
            'branch': tool_input['branch'],
            'conversation_id': trace_payload.get('conversation_id'),
            'agent_run_id': trace_payload.get('agent_run_id'),
            'request_id': trace_payload.get('request_id'),
            'tool_call_id': trace_payload.get('tool_call_id'),
        }
        for source_key, target_key in (
            ('commit_sha', 'commit_sha'),
            ('target_application_id', 'application_id'),
            ('changed_files', 'changed_files'),
            ('summary', 'summary'),
        ):
            if tool_input.get(source_key) is not None:
                payload[target_key] = tool_input[source_key]
        result = await _cp_post('change-previews/deploy', payload)
        return result if isinstance(result, dict) else {'deployment': result}

    if name == 'revert_preview_change':
        payload = {
            'unit_id': tool_input['target_unit_id'],
            'conversation_id': trace_payload.get('conversation_id'),
            'agent_run_id': trace_payload.get('agent_run_id'),
            'request_id': trace_payload.get('request_id'),
            'tool_call_id': trace_payload.get('tool_call_id'),
        }
        for source_key, target_key in (
            ('target_application_id', 'application_id'),
            ('baseline_branch', 'baseline_branch'),
            ('baseline_commit_sha', 'baseline_commit_sha'),
            ('preview_deployment_id', 'preview_deployment_id'),
            ('summary', 'summary'),
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

    approval_token = body.approval_token or body.confirmation_token
    permission_request: ToolPermissionRequest | None = None
    if mode_policy == "requires_permission":
        if approval_token:
            permission_request = db.query(ToolPermissionRequest).filter(ToolPermissionRequest.approval_token == approval_token).one_or_none()
            if not permission_request or str(permission_request.tool_call_id) != tool_call_id or permission_request.status not in {"approved", "executing"}:
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
                approval_token=uuid.uuid4().hex,
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
                    "approval_token": permission_request.approval_token,
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
                    "approval_token": permission_request.approval_token,
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


def approve_tool_permission(permission_request_id: str, body: ToolPermissionApproveRequest, db: Session = Depends(get_db)):
    try:
        permission_uuid = uuid.UUID(permission_request_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid permission request id") from exc
    permission = db.query(ToolPermissionRequest).filter(ToolPermissionRequest.id == permission_uuid).one_or_none()
    if not permission:
        raise HTTPException(status_code=404, detail="permission request not found")
    if permission.approval_token != body.approval_token:
        raise HTTPException(status_code=403, detail="invalid approval token")
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


def deny_tool_permission(permission_request_id: str, body: ToolPermissionDenyRequest, db: Session = Depends(get_db)):
    try:
        permission_uuid = uuid.UUID(permission_request_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid permission request id") from exc
    permission = db.query(ToolPermissionRequest).filter(ToolPermissionRequest.id == permission_uuid).one_or_none()
    if not permission:
        raise HTTPException(status_code=404, detail="permission request not found")
    if permission.approval_token != body.approval_token:
        raise HTTPException(status_code=403, detail="invalid approval token")
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
