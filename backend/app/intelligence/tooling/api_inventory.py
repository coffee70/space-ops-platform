"""Source-of-truth API inventory for Phase 3 tool mapping."""

API_INVENTORY = {
    "layer1": {
        "registry": {
            "GET /registry/applications": "read_only_tool:list_platform_applications",
            "GET /registry/applications/{application_id}": "read_only_tool:get_platform_application",
            "GET /registry/services": "read_only_tool:list_platform_services",
            "GET /registry/services/{service_slug}": "read_only_tool:get_platform_service",
            "GET /registry/units": "read_only_tool:list_runtime_services",
        },
        "templates": {
            "GET /templates": "read_only_tool:list_runtime_templates",
            "POST /templates/{template_id}/scaffold": "write_tool:scaffold_service",
        },
        "code": {
            "GET /code/roots": "read_only_tool:list_managed_repositories",
            "GET /code/file": "read_only_tool:read_source_file",
            "POST /code/branches": "write_tool:create_working_branch",
            "PUT /code/file": "write_tool:write_source_file",
            "POST /code/commits": "write_tool:create_commit",
        },
        "deployments": {
            "POST /deployments": "write_tool:deploy_service_or_application",
        },
    },
    "layer2": {
        "telemetry": {
            "GET /telemetry/inventory?source_id={source_id}": "read_only_tool:get_telemetry_schema",
            "GET /telemetry/{name}/recent": "read_only_tool:query_recent_telemetry",
        },
        "sources": {
            "GET /telemetry/sources": "read_only_tool:list_sources_or_adapters",
        },
        "intelligence": {
            "GET /intelligence/documents": "read_only_tool:list_documents",
            "GET /intelligence/documents/{document_id}": "read_only_tool:get_document",
            "POST /intelligence/documents/search": "read_only_tool:search_documents",
            "POST /intelligence/documents/{document_id}/reingest": "write_tool:trigger_document_reingestion",
            "POST /intelligence/code/search": "read_only_tool:search_codebase",
            "POST /intelligence/code/related-context": "read_only_tool:get_related_code_context",
        },
    },
    "layer3": {
        "navigation": {
            "platform.openApplication": "higher_level_tool_only:navigate_to_application",
            "workspace.openFile": "higher_level_tool_only:open_workspace_file",
            "registry.listTools": "higher_level_tool_only:list_available_tools",
        }
    },
}
