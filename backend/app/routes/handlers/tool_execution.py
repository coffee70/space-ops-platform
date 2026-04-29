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
from app.intelligence.schemas import ToolExecutionRequest
from app.intelligence.tool_validation import ToolInputValidationError, ToolSchemaDefinitionError, validate_tool_input
from app.intelligence.trace import extract_trace
from app.models.intelligence import ToolCall, ToolDefinition

EXECUTION_MODE_RANK = {"read_only": 0, "suggest": 1, "execute": 2, "governed_execute": 3}


def _cp_url(path: str) -> str:
    base = get_settings().control_plane_url.rstrip('/')
    return f"{base}/{path.lstrip('/')}"


async def _cp_get(path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_cp_url(path), params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


async def _execute_mapped_tool(name: str, tool_input: dict):
    if name == 'list_available_tools':
        return {'message': 'Use tool registry /tools endpoint for full list.'}
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
        return await _cp_get('templates')
    if name == 'list_runtime_services':
        return await _cp_get('registry/units')
    if name == 'get_runtime_service':
        slug = tool_input.get('service_slug')
        if not slug:
            raise HTTPException(status_code=400, detail='service_slug is required')
        return await _cp_get(f'registry/services/{slug}')
    if name == 'list_managed_repositories':
        return await _cp_get('code/roots')
    if name == 'navigate_to_application':
        app_id = tool_input.get('application_id')
        if not app_id:
            raise HTTPException(status_code=400, detail='application_id is required')
        return {'action': 'navigate_to_application', 'application_id': app_id, 'route_path': tool_input.get('route_path') or f'/apps/{app_id}'}
    if name == 'open_workspace_file':
        path = tool_input.get('path')
        if not path:
            raise HTTPException(status_code=400, detail='path is required')
        return {
            'action': 'open_workspace_file',
            'application_id': 'workspace',
            'route_path': '/apps/workspace',
            'path': path,
        }
    raise HTTPException(status_code=501, detail=f'tool handler not implemented: {name}')


async def execute_tool(body: ToolExecutionRequest, request: Request, db: Session = Depends(get_db)):
    trace = extract_trace(request, require_run=True, require_conversation=False)
    conversation_id = body.conversation_id or trace.get("conversation_id")
    agent_run_id = body.agent_run_id or trace["agent_run_id"]
    request_id = body.request_id or trace["request_id"]
    tool_call_id = body.tool_call_id or trace.get("tool_call_id")
    if not tool_call_id:
        raise HTTPException(status_code=400, detail="tool_call_id is required")
    tool = db.query(ToolDefinition).filter(ToolDefinition.name == body.tool_name).one_or_none()
    if not tool:
        raise HTTPException(status_code=404, detail='tool not found')
    if not tool.enabled:
        raise HTTPException(status_code=400, detail='tool disabled')
    required_mode = getattr(tool, "required_execution_mode", "execute")
    required_rank = EXECUTION_MODE_RANK.get(required_mode, EXECUTION_MODE_RANK["execute"])
    request_rank = EXECUTION_MODE_RANK.get(body.execution_mode, EXECUTION_MODE_RANK["read_only"])
    if request_rank < required_rank:
        raise HTTPException(
            status_code=403,
            detail={
                "error_code": "tool_execution_mode_forbidden",
                "message": "tool not allowed in current execution mode",
                "required_execution_mode": required_mode,
                "requested_execution_mode": body.execution_mode,
            },
        )
    if tool.requires_confirmation and not body.confirmation_token:
        return {
            'conversation_id': conversation_id,
            'agent_run_id': agent_run_id,
            'request_id': request_id,
            'tool_call_id': tool_call_id,
            'status': 'confirmation_required',
            'output': {'error_code': 'confirmation_required', 'message': 'confirmation token required'},
            'raw_events': [],
        }
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

    call = ToolCall(
        conversation_id=uuid.UUID(conversation_id) if conversation_id else None,
        agent_run_id=uuid.UUID(agent_run_id),
        request_id=uuid.UUID(request_id),
        tool_call_id=uuid.UUID(tool_call_id),
        message_id=uuid.UUID(body.message_id) if body.message_id else None,
        tool_name=body.tool_name,
        input_json=body.input,
        redacted_input_json=redact(body.input),
        status='running',
        started_at=datetime.now(timezone.utc),
    )
    db.add(call)
    db.flush()
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
        output = await _execute_mapped_tool(body.tool_name, body.input)
        call.status = 'completed'
        call.output_json = redact(output)
        call.completed_at = datetime.now(timezone.utc)
        raw_events = [
            started_event,
            raw_event(
                event_type='tool.completed',
                payload={'tool_name': body.tool_name, 'status': 'completed', 'result_preview': redact(output), 'duration_ms': int((call.completed_at - call.started_at).total_seconds() * 1000)},
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
    except Exception as exc:
        call.status = 'failed'
        call.error_message = str(exc)
        call.completed_at = datetime.now(timezone.utc)
        return {
            'conversation_id': conversation_id,
            'agent_run_id': agent_run_id,
            'request_id': request_id,
            'tool_call_id': tool_call_id,
            'status': 'failed',
            'output': {'error_code': 'tool_execution_failed', 'message': str(exc)},
            'raw_events': [
                started_event,
                raw_event(
                    event_type='tool.failed',
                    payload={'tool_name': body.tool_name, 'error_code': 'tool_execution_failed', 'message': str(exc), 'duration_ms': int((call.completed_at - call.started_at).total_seconds() * 1000)},
                    emitted_by='tool-execution-service',
                    tool_call_id=tool_call_id,
                )
            ],
        }
