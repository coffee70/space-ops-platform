from __future__ import annotations

from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.intelligence.tool_metadata import tool_summary
from app.intelligence.tooling import API_INVENTORY
from app.models.intelligence import ToolDefinition

STRICT_EMPTY_INPUT = {'type': 'object', 'properties': {}, 'additionalProperties': False}

MVP_TOOL_NAMES: frozenset[str] = frozenset(
    {
        'list_available_tools',
        'list_platform_services',
        'get_platform_service',
        'list_platform_applications',
        'get_platform_application',
        'list_runtime_templates',
        'list_runtime_services',
        'list_managed_repositories',
        'get_telemetry_schema',
        'query_recent_telemetry',
        'list_sources_or_adapters',
        'list_documents',
        'get_document',
        'search_documents',
        'trigger_document_reingestion',
        'search_codebase',
        'read_source_file',
        'get_related_code_context',
        'navigate_to_application',
        'open_workspace_file',
        'create_working_branch',
        'scaffold_service',
        'write_source_file',
        'create_commit',
        'deploy_service_or_application',
    }
)

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
            'branch': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'path': {'type': 'string', 'minLength': 1, 'maxLength': 2000},
        },
        'required': ['branch', 'path'],
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
    'create_working_branch': {
        'type': 'object',
        'properties': {
            'branch': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'from_branch': {'type': 'string', 'minLength': 1, 'maxLength': 256},
        },
        'required': ['branch'],
        'additionalProperties': False,
    },
    'scaffold_service': {
        'type': 'object',
        'properties': {
            'template_id': {'type': 'string', 'minLength': 1, 'maxLength': 128},
            'unit_id': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'display_name': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'branch': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'package_owner': {'type': 'string', 'enum': ['space-ops-platform', 'space-ops-apps']},
            'source_path': {'type': 'string', 'maxLength': 512},
            'discovery': {'type': 'object'},
        },
        'required': ['template_id', 'unit_id', 'display_name'],
        'additionalProperties': False,
    },
    'write_source_file': {
        'type': 'object',
        'properties': {
            'branch': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'path': {'type': 'string', 'minLength': 1, 'maxLength': 2000},
            'content': {'type': 'string'},
        },
        'required': ['branch', 'path', 'content'],
        'additionalProperties': False,
    },
    'create_commit': {
        'type': 'object',
        'properties': {
            'branch': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'message': {'type': 'string', 'minLength': 1, 'maxLength': 4000},
        },
        'required': ['branch', 'message'],
        'additionalProperties': False,
    },
    'deploy_service_or_application': {
        'type': 'object',
        'properties': {
            'unit_id': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'branch': {'type': 'string', 'minLength': 1, 'maxLength': 256},
            'commit_sha': {'type': 'string', 'minLength': 7, 'maxLength': 64},
        },
        'required': ['unit_id'],
        'additionalProperties': False,
    },
}


def _delete_stale_tool_definitions(db: Session) -> int:
    stale = db.query(ToolDefinition).filter(ToolDefinition.name.notin_(tuple(MVP_TOOL_NAMES)))
    count_before = stale.count()
    stale.delete(synchronize_session=False)
    return count_before


def list_tools(include_full_metadata: bool = Query(False), enabled: bool | None = Query(None), db: Session = Depends(get_db)):
    query = db.query(ToolDefinition)
    if enabled is not None:
        query = query.filter(ToolDefinition.enabled == enabled)
    tools = query.order_by(ToolDefinition.name.asc()).all()
    if include_full_metadata:
        return [
            {
                **tool_summary(tool),
                'output_schema_json': tool.output_schema_json,
                'audit_policy_json': tool.audit_policy_json,
                'redaction_policy_json': tool.redaction_policy_json,
                'backing_service': tool.backing_service,
                'backing_api': tool.backing_api,
            }
            for tool in tools
        ]
    return [tool_summary(tool) for tool in tools]


def get_tool(tool_name: str, db: Session = Depends(get_db)):
    tool = db.query(ToolDefinition).filter(ToolDefinition.name == tool_name).one_or_none()
    if not tool:
        raise HTTPException(status_code=404, detail='tool not found')
    return {
        **tool_summary(tool),
        'output_schema_json': tool.output_schema_json,
        'audit_policy_json': tool.audit_policy_json,
        'redaction_policy_json': tool.redaction_policy_json,
        'backing_service': tool.backing_service,
        'backing_api': tool.backing_api,
    }


def seed_tools(db: Session = Depends(get_db)):
    seeded = 0

    def upsert(*, name: str, description: str, category: str, layer_target: str, read_write: str, execution_mode: str, backing_service: str | None = None, backing_api: str | None = None):
        nonlocal seeded
        existing = db.query(ToolDefinition).filter(ToolDefinition.name == name).one_or_none()
        payload = {
            'description': description,
            'category': category,
            'layer_target': layer_target,
            'read_write_classification': read_write,
            'required_execution_mode': execution_mode,
            'enabled': True,
            'requires_confirmation': False,
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

    read_tools = [
        ('list_available_tools', 'List currently registered MVP tools.', 'platform_discovery', 'layer2', 'read_only'),
        ('list_platform_services', 'List platform services.', 'platform_discovery', 'layer1', 'read_only'),
        ('get_platform_service', 'Get platform service details by slug.', 'platform_discovery', 'layer1', 'read_only'),
        ('list_platform_applications', 'List platform applications.', 'platform_discovery', 'layer1', 'read_only'),
        ('get_platform_application', 'Get platform application details.', 'platform_discovery', 'layer1', 'read_only'),
        ('list_runtime_templates', 'List available runtime templates.', 'layer1_runtime', 'layer1', 'read_only'),
        ('list_runtime_services', 'List runtime units (managed services/apps).', 'layer1_runtime', 'layer1', 'read_only'),
        ('list_managed_repositories', 'List managed fork repository roots.', 'code_intelligence', 'layer1', 'read_only'),
        ('get_telemetry_schema', 'Get telemetry channel schema.', 'telemetry', 'layer2', 'read_only'),
        ('query_recent_telemetry', 'Query recent values for a telemetry channel.', 'telemetry', 'layer2', 'read_only'),
        ('list_sources_or_adapters', 'List telemetry sources and adapters.', 'telemetry', 'layer2', 'read_only'),
        ('list_documents', 'List uploaded mission and vehicle documents.', 'documents', 'layer2', 'read_only'),
        ('get_document', 'Get document metadata by id.', 'documents', 'layer2', 'read_only'),
        ('search_documents', 'Search uploaded documents.', 'documents', 'layer2', 'read_only'),
        ('search_codebase', 'Search indexed code chunks.', 'code_intelligence', 'layer2', 'read_only'),
        ('read_source_file', 'Read file contents from the managed fork (Layer 1).', 'code_intelligence', 'layer1', 'read_only'),
        ('get_related_code_context', 'Related code chunks for a repository path.', 'code_intelligence', 'layer2', 'read_only'),
        ('navigate_to_application', 'Navigate Mission Control UI to a platform application.', 'navigation', 'layer3', 'read_only'),
        ('open_workspace_file', 'Open a file in the Mission Control workspace shell.', 'navigation', 'layer3', 'read_only'),
    ]

    backing_read = {
        'list_platform_services': ('control-plane', 'GET /registry/services'),
        'get_platform_service': ('control-plane', 'GET /registry/services/{service_slug}'),
        'list_platform_applications': ('control-plane', 'GET /registry/applications'),
        'get_platform_application': ('control-plane', 'GET /registry/applications/{application_id}'),
        'list_runtime_templates': ('control-plane', 'GET /templates'),
        'list_runtime_services': ('control-plane', 'GET /registry/units'),
        'list_managed_repositories': ('control-plane', 'GET /code/roots'),
        'get_telemetry_schema': ('platform-api-gateway', 'GET /telemetry/schema'),
        'query_recent_telemetry': ('platform-api-gateway', 'GET /telemetry/{name}/recent'),
        'list_sources_or_adapters': ('platform-api-gateway', 'GET /telemetry/sources'),
        'list_documents': ('document-knowledge-service', 'GET /intelligence/documents'),
        'get_document': ('document-knowledge-service', 'GET /intelligence/documents/{document_id}'),
        'search_documents': ('document-knowledge-service', 'POST /intelligence/documents/search'),
        'search_codebase': ('code-intelligence-service', 'POST /intelligence/code/search'),
        'read_source_file': ('control-plane', 'GET /code/file'),
        'get_related_code_context': ('code-intelligence-service', 'POST /intelligence/code/related-context'),
    }

    for name, description, cat, lt, ej in read_tools:
        svc, api = backing_read.get(name, (None, None))
        upsert(name=name, description=description, category=cat, layer_target=lt, read_write='read', execution_mode='read_only', backing_service=svc, backing_api=api)

    writes = [
        (
            'trigger_document_reingestion',
            'Re-run ingestion for an uploaded document.',
            'documents',
            'layer2',
            ('document-knowledge-service', 'POST /intelligence/documents/{document_id}/reingest'),
        ),
        ('create_working_branch', 'Create a working branch off an existing branch.', 'code_write', 'layer1', ('control-plane', 'POST /code/branches')),
        ('scaffold_service', 'Scaffold files from a runtime template.', 'layer1_runtime', 'layer1', ('control-plane', 'POST /templates/{template_id}/scaffold')),
        ('write_source_file', 'Overwrite a file on a managed fork branch.', 'code_write', 'layer1', ('control-plane', 'PUT /code/file')),
        ('create_commit', 'Create a commit for staged changes on a branch.', 'code_write', 'layer1', ('control-plane', 'POST /code/commits')),
        ('deploy_service_or_application', 'Build and deploy a managed unit from a branch.', 'deployment', 'layer1', ('control-plane', 'POST /deployments')),
    ]
    for name, description, cat, lt, bk in writes:
        upsert(name=name, description=description, category=cat, layer_target=lt, read_write='write', execution_mode='execute', backing_service=bk[0], backing_api=bk[1])

    stale_removed = _delete_stale_tool_definitions(db)
    db.flush()

    return {
        'seeded': seeded,
        'removed_stale_definitions': stale_removed,
        'total': db.query(ToolDefinition).count(),
        'inventory_sections': list(API_INVENTORY.keys()),
    }
