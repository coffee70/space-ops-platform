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
            "GET /templates/{template_id}": "read_only_tool:get_runtime_template",
            "POST /templates/{template_id}/scaffold": "write_tool:scaffold_service_or_application",
        },
        "code": {
            "GET /code/roots": "read_only_tool:list_managed_repositories",
            "GET /code/tree": "read_only_tool:list_source_tree",
            "GET /code/file": "read_only_tool:read_source_file",
            "GET /code/branches": "read_only_tool:list_working_branches",
            "GET /code/history": "read_only_tool:get_source_history",
            "GET /code/diff": "read_only_tool:get_source_diff",
            "POST /code/branches": "write_tool:create_working_branch",
            "PUT /code/file": "write_tool:write_source_file",
            "POST /code/commits": "write_tool:create_commit",
        },
        "deployments": {
            "POST /deployments": "write_tool:deploy_service_or_application",
            "POST /code/deployment-submissions": "write_tool:deploy_service_or_application",
            "GET /deployments/{deployment_id}": "read_only_tool:get_deployment",
            "GET /deployments/{deployment_id}/logs": "read_only_tool:get_deployment_logs",
        },
    },
    "layer2": {
        "telemetry": {
            "GET /telemetry/schema": "read_only_tool:get_telemetry_schema",
            "GET /telemetry/search": "read_only_tool:search_telemetry_schema",
            "GET /telemetry/list": "read_only_tool:list_telemetry_channels",
            "GET /telemetry/{name}/recent": "read_only_tool:query_recent_telemetry",
        },
        "sources": {
            "GET /telemetry/sources": "read_only_tool:list_sources_or_adapters",
            "GET /ops/events": "read_only_tool:get_recent_ops_events",
        },
        "intelligence": {
            "POST /documents/search": "read_only_tool:search_documents",
            "POST /code/search": "read_only_tool:search_codebase",
        },
    },
    "layer3": {
        "navigation": {
            "platform.openApplication": "higher_level_tool_only:navigate_to_application",
            "workspace.openFile": "higher_level_tool_only:open_workspace_file",
        }
    },
}
