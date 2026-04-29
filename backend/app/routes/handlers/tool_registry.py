from __future__ import annotations

from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.intelligence.tooling import API_INVENTORY
from app.models.intelligence import ToolDefinition

STRICT_EMPTY_INPUT = {'type': 'object', 'properties': {}, 'additionalProperties': False}

TOOL_INPUT_SCHEMAS: dict[str, dict] = {
    'list_available_tools': STRICT_EMPTY_INPUT,
    'list_platform_services': STRICT_EMPTY_INPUT,
    'get_platform_service': {
        'type': 'object',
        'properties': {'service_slug': {'type': 'string', 'minLength': 1, 'maxLength': 128}},
        'required': ['service_slug'],
        'additionalProperties': False,
    },
    'list_platform_applications': STRICT_EMPTY_INPUT,
    'get_platform_application': {
        'type': 'object',
        'properties': {'application_id': {'type': 'string', 'minLength': 1, 'maxLength': 128}},
        'required': ['application_id'],
        'additionalProperties': False,
    },
    'list_runtime_templates': STRICT_EMPTY_INPUT,
    'list_runtime_services': STRICT_EMPTY_INPUT,
    'get_runtime_service': {
        'type': 'object',
        'properties': {'service_slug': {'type': 'string', 'minLength': 1, 'maxLength': 128}},
        'required': ['service_slug'],
        'additionalProperties': False,
    },
    'list_managed_repositories': STRICT_EMPTY_INPUT,
    'get_telemetry_schema': STRICT_EMPTY_INPUT,
    'query_recent_telemetry': {
        'type': 'object',
        'properties': {
            'name': {'type': 'string', 'minLength': 1, 'maxLength': 160},
            'limit': {'type': 'integer', 'minimum': 1, 'maximum': 500},
        },
        'required': ['name'],
        'additionalProperties': False,
    },
    'list_sources_or_adapters': STRICT_EMPTY_INPUT,
    'get_service_health': {
        'type': 'object',
        'properties': {'service_slug': {'type': 'string', 'minLength': 1, 'maxLength': 128}},
        'required': ['service_slug'],
        'additionalProperties': False,
    },
    'list_documents': {
        'type': 'object',
        'properties': {
            'mission_id': {'type': 'string', 'maxLength': 128},
            'vehicle_id': {'type': 'string', 'maxLength': 128},
            'limit': {'type': 'integer', 'minimum': 1, 'maximum': 100},
        },
        'additionalProperties': False,
    },
    'get_document': {
        'type': 'object',
        'properties': {'document_id': {'type': 'string', 'format': 'uuid'}},
        'required': ['document_id'],
        'additionalProperties': False,
    },
    'search_documents': {
        'type': 'object',
        'properties': {
            'query': {'type': 'string', 'minLength': 1, 'maxLength': 2000},
            'mission_id': {'type': 'string', 'maxLength': 128},
            'vehicle_id': {'type': 'string', 'maxLength': 128},
            'subsystem_id': {'type': 'string', 'maxLength': 128},
            'limit': {'type': 'integer', 'minimum': 1, 'maximum': 25},
        },
        'required': ['query'],
        'additionalProperties': False,
    },
    'trigger_document_reingestion': {
        'type': 'object',
        'properties': {'document_id': {'type': 'string', 'format': 'uuid'}},
        'required': ['document_id'],
        'additionalProperties': False,
    },
    'search_codebase': {
        'type': 'object',
        'properties': {
            'query': {'type': 'string', 'minLength': 1, 'maxLength': 2000},
            'repository': {'type': 'string', 'maxLength': 256},
            'branch': {'type': 'string', 'maxLength': 256},
            'limit': {'type': 'integer', 'minimum': 1, 'maximum': 50},
        },
        'required': ['query'],
        'additionalProperties': False,
    },
    'read_source_file': {
        'type': 'object',
        'properties': {
            'repository': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'path': {'type': 'string', 'minLength': 1, 'maxLength': 2000},
            'branch': {'type': 'string', 'maxLength': 256},
        },
        'required': ['repository', 'path'],
        'additionalProperties': False,
    },
    'get_related_code_context': {
        'type': 'object',
        'properties': {
            'repository': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'path': {'type': 'string', 'minLength': 1, 'maxLength': 2000},
            'branch': {'type': 'string', 'maxLength': 256},
            'line': {'type': 'integer', 'minimum': 1},
            'limit': {'type': 'integer', 'minimum': 1, 'maximum': 25},
        },
        'required': ['repository', 'path'],
        'additionalProperties': False,
    },
    'navigate_to_application': {
        'type': 'object',
        'properties': {
            'application_id': {'type': 'string', 'minLength': 1, 'maxLength': 128},
            'route_path': {'type': 'string', 'pattern': '^/apps/.*', 'maxLength': 2000},
        },
        'required': ['application_id'],
        'additionalProperties': False,
    },
    'open_workspace_file': {
        'type': 'object',
        'properties': {'path': {'type': 'string', 'minLength': 1, 'maxLength': 2000}},
        'required': ['path'],
        'additionalProperties': False,
    },
}

CONFIRMATION_REQUIRED_SCHEMA = {
    'type': 'object',
    'properties': {'confirmation_token': {'type': 'string', 'minLength': 1, 'maxLength': 128}},
    'required': ['confirmation_token'],
    'additionalProperties': False,
}


def _summary(tool: ToolDefinition) -> dict:
    return {
        'name': tool.name,
        'description': tool.description,
        'category': tool.category,
        'layer_target': tool.layer_target,
        'read_write_classification': tool.read_write_classification,
        'required_execution_mode': tool.required_execution_mode,
        'enabled': tool.enabled,
        'requires_confirmation': tool.requires_confirmation,
        'input_schema_json': tool.input_schema_json,
    }


def list_tools(include_full_metadata: bool = Query(False), enabled: bool | None = Query(None), db: Session = Depends(get_db)):
    query = db.query(ToolDefinition)
    if enabled is not None:
        query = query.filter(ToolDefinition.enabled == enabled)
    tools = query.order_by(ToolDefinition.name.asc()).all()
    if include_full_metadata:
        return [
            {
                **_summary(tool),
                'output_schema_json': tool.output_schema_json,
                'audit_policy_json': tool.audit_policy_json,
                'redaction_policy_json': tool.redaction_policy_json,
                'backing_service': tool.backing_service,
                'backing_api': tool.backing_api,
            }
            for tool in tools
        ]
    return [_summary(tool) for tool in tools]


def get_tool(tool_name: str, db: Session = Depends(get_db)):
    tool = db.query(ToolDefinition).filter(ToolDefinition.name == tool_name).one_or_none()
    if not tool:
        raise HTTPException(status_code=404, detail='tool not found')
    return {
        **_summary(tool),
        'output_schema_json': tool.output_schema_json,
        'audit_policy_json': tool.audit_policy_json,
        'redaction_policy_json': tool.redaction_policy_json,
        'backing_service': tool.backing_service,
        'backing_api': tool.backing_api,
    }


def seed_tools(db: Session = Depends(get_db)):
    seeded = 0

    def upsert(*, name: str, description: str, category: str, layer_target: str, read_write: str, execution_mode: str, enabled: bool, requires_confirmation: bool, backing_service: str | None = None, backing_api: str | None = None):
        nonlocal seeded
        existing = db.query(ToolDefinition).filter(ToolDefinition.name == name).one_or_none()
        payload = {
            'description': description,
            'category': category,
            'layer_target': layer_target,
            'read_write_classification': read_write,
            'required_execution_mode': execution_mode,
            'enabled': enabled,
            'requires_confirmation': requires_confirmation,
            'backing_service': backing_service,
            'backing_api': backing_api,
            'input_schema_json': TOOL_INPUT_SCHEMAS.get(name, STRICT_EMPTY_INPUT),
            'output_schema_json': {'type': 'object'},
            'audit_policy_json': {'log_inputs': True, 'log_outputs': True},
            'redaction_policy_json': {'redact_keys': ['authorization', 'api_key', 'token', 'cookie']},
            'show_result_in_ui': True,
            'updated_at': datetime.now(timezone.utc),
        }
        if existing:
            for key, value in payload.items():
                setattr(existing, key, value)
            return
        tool = ToolDefinition(name=name, created_at=datetime.now(timezone.utc), **payload)
        db.add(tool)
        seeded += 1

    for tool in [
        ('list_available_tools','List currently registered tools.','platform_discovery','layer2','read','read_only',True,False,None,None),
        ('list_platform_services','List platform services.','platform_discovery','layer1','read','read_only',True,False,'control-plane','GET /registry/services'),
        ('get_platform_service','Get service details.','platform_discovery','layer1','read','read_only',True,False,'control-plane','GET /registry/services/{service_slug}'),
        ('list_platform_applications','List platform applications.','platform_discovery','layer1','read','read_only',True,False,'control-plane','GET /registry/applications'),
        ('get_platform_application','Get platform application details.','platform_discovery','layer1','read','read_only',True,False,'control-plane','GET /registry/applications/{application_id}'),
        ('list_runtime_templates','List available runtime templates.','layer1_runtime','layer1','read','read_only',True,False,'control-plane','GET /templates'),
        ('list_runtime_services','List runtime services.','layer1_runtime','layer1','read','read_only',True,False,'control-plane','GET /registry/units'),
        ('get_runtime_service','Get runtime service details.','layer1_runtime','layer1','read','read_only',True,False,'control-plane','GET /registry/services/{service_slug}'),
        ('list_managed_repositories','List managed fork roots.','code_intelligence','layer1','read','read_only',True,False,'control-plane','GET /code/roots'),
        ('get_telemetry_schema','Get telemetry schema.','telemetry','layer2','read','read_only',True,False,'platform-api-gateway','GET /telemetry/schema'),
        ('query_recent_telemetry','Query recent telemetry channel values.','telemetry','layer2','read','read_only',True,False,'platform-api-gateway','GET /telemetry/{name}/recent'),
        ('list_sources_or_adapters','List telemetry sources/adapters.','telemetry','layer2','read','read_only',True,False,'platform-api-gateway','GET /telemetry/sources'),
        ('get_service_health','Get service health endpoints.','platform_discovery','layer1','read','read_only',True,False,None,None),
        ('list_documents','List uploaded documents.','documents','layer2','read','read_only',True,False,'document-knowledge-service','GET /documents'),
        ('get_document','Get document metadata.','documents','layer2','read','read_only',True,False,'document-knowledge-service','GET /documents/{document_id}'),
        ('search_documents','Search mission and vehicle documents.','documents','layer2','read','read_only',True,False,'document-knowledge-service','POST /documents/search'),
        ('trigger_document_reingestion','Trigger document re-ingestion.','documents','layer2','write','execute',True,False,'document-knowledge-service','POST /documents/{document_id}/reingest'),
        ('search_codebase','Search code intelligence index.','code_intelligence','layer2','read','read_only',True,False,'code-intelligence-service','POST /code/search'),
        ('read_source_file','Read source file from managed fork.','code_intelligence','layer2','read','read_only',True,False,'code-intelligence-service','GET /code/source-file'),
        ('get_related_code_context','Get related code context.','code_intelligence','layer2','read','read_only',True,False,'code-intelligence-service','POST /code/related-context'),
        ('navigate_to_application','Navigate UI to a platform application.','navigation','layer3','read','read_only',True,False,None,None),
        ('open_workspace_file','Open file inside workspace.','navigation','layer3','read','read_only',True,False,None,None),
    ]:
        upsert(name=tool[0],description=tool[1],category=tool[2],layer_target=tool[3],read_write=tool[4],execution_mode=tool[5],enabled=tool[6],requires_confirmation=tool[7],backing_service=tool[8],backing_api=tool[9])

    for name in ['create_working_branch','scaffold_service','scaffold_application','apply_patch','create_commit','submit_change','deploy_service_or_application','create_derived_telemetry_definition','create_monitoring_rule']:
        upsert(
            name=name,
            description=f'{name.replace("_", " ").capitalize()} (future write tool).',
            category='write_future',
            layer_target='layer1',
            read_write='write',
            execution_mode='execute',
            enabled=False,
            requires_confirmation=True,
        )
        tool = db.query(ToolDefinition).filter(ToolDefinition.name == name).one_or_none()
        if tool:
            tool.input_schema_json = CONFIRMATION_REQUIRED_SCHEMA

    return {'seeded': seeded, 'total': db.query(ToolDefinition).count(), 'inventory_sections': list(API_INVENTORY.keys())}


def patch_tool(tool_name: str, body: dict, db: Session = Depends(get_db)):
    tool = db.query(ToolDefinition).filter(ToolDefinition.name == tool_name).one_or_none()
    if not tool:
        raise HTTPException(status_code=404, detail='tool not found')
    if 'enabled' in body:
        tool.enabled = bool(body['enabled'])
    if 'requires_confirmation' in body:
        tool.requires_confirmation = bool(body['requires_confirmation'])
    if 'required_execution_mode' in body:
        tool.required_execution_mode = str(body['required_execution_mode'])
    tool.updated_at = datetime.now(timezone.utc)
    return _summary(tool)
