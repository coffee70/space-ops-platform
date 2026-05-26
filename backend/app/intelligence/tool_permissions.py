from __future__ import annotations

from typing import Literal

EXECUTION_MODE_RANK = {"read_only": 0, "suggest": 1, "execute": 2, "governed_execute": 3}
ToolModePolicy = Literal["disabled", "requires_permission", "enabled"]

READ_ONLY_POLICY: dict[str, ToolModePolicy] = {
    "read_only": "enabled",
    "suggest": "enabled",
    "execute": "enabled",
    "governed_execute": "enabled",
}

WRITE_POLICY: dict[str, ToolModePolicy] = {
    "read_only": "disabled",
    "suggest": "enabled",
    "execute": "enabled",
    "governed_execute": "enabled",
}

DEPLOY_REVERT_POLICY: dict[str, ToolModePolicy] = {
    "read_only": "disabled",
    "suggest": "requires_permission",
    "execute": "requires_permission",
    "governed_execute": "enabled",
}

DESTRUCTIVE_POLICY: dict[str, ToolModePolicy] = {
    "read_only": "disabled",
    "suggest": "requires_permission",
    "execute": "requires_permission",
    "governed_execute": "requires_permission",
}


def legacy_policy_for_mode(required_execution_mode: str, execution_mode: str) -> ToolModePolicy:
    required_rank = EXECUTION_MODE_RANK.get(required_execution_mode, EXECUTION_MODE_RANK["execute"])
    request_rank = EXECUTION_MODE_RANK.get(execution_mode, EXECUTION_MODE_RANK["read_only"])
    return "enabled" if request_rank >= required_rank else "disabled"


def policy_for_mode(tool: object, execution_mode: str) -> ToolModePolicy:
    mode_policy = getattr(tool, "mode_policy_json", None)
    if isinstance(mode_policy, dict):
        value = mode_policy.get(execution_mode)
        if value in {"disabled", "requires_permission", "enabled"}:
            return value
    return legacy_policy_for_mode(getattr(tool, "required_execution_mode", "execute"), execution_mode)


def build_permission_prompt(tool_name: str, tool_input: dict, prompt_overrides: dict | None = None) -> dict:
    if tool_name == "deploy_service_or_application":
        target = tool_input.get("unit_id") or "the target unit"
        branch = tool_input.get("branch") or "main"
        prompt = {
            "title": "Deploy managed service or application?",
            "description": f"The AI Engineer wants to deploy {target} from branch {branch}.",
            "primary_action": "Approve deploy",
            "secondary_action": "Cancel",
            "risk_level": "medium",
            "details": {
                "unit_id": tool_input.get("unit_id"),
                "branch": tool_input.get("branch"),
                "commit_sha": tool_input.get("commit_sha"),
            },
        }
    elif tool_name == "deploy_preview_change":
        target = tool_input.get("target_unit_id") or "the target unit"
        branch = tool_input.get("branch") or "the requested branch"
        prompt = {
            "title": "Deploy preview changes?",
            "description": f"The AI Engineer wants to deploy {target} from branch {branch}.",
            "primary_action": "Deploy changes",
            "secondary_action": "Cancel",
            "risk_level": "low",
            "details": {
                "branch": tool_input.get("branch"),
                "commit_sha": tool_input.get("commit_sha"),
                "target_unit_id": tool_input.get("target_unit_id"),
                "target_application_id": tool_input.get("target_application_id"),
                "changed_files": tool_input.get("changed_files") or [],
                "summary": tool_input.get("summary"),
            },
        }
    elif tool_name == "delete_managed_resources":
        mode = tool_input.get("mode") or "managed resources"
        target = tool_input.get("unit_id") or tool_input.get("deployment_id") or tool_input.get("branch") or "the requested resources"
        prompt = {
            "title": "Delete managed resources?",
            "description": f"The AI Engineer wants to delete {target} using {mode} cleanup.",
            "primary_action": "Approve delete",
            "secondary_action": "Cancel",
            "risk_level": "high",
            "details": {
                "mode": tool_input.get("mode"),
                "unit_id": tool_input.get("unit_id"),
                "deployment_id": tool_input.get("deployment_id"),
                "branch": tool_input.get("branch"),
                "paths": tool_input.get("paths") or [],
                "include_code": tool_input.get("include_code"),
                "include_runtime": tool_input.get("include_runtime"),
                "include_registry": tool_input.get("include_registry"),
                "include_intelligence_records": tool_input.get("include_intelligence_records"),
            },
        }
    elif tool_name == "revert_preview_change":
        target = tool_input.get("target_unit_id") or "the target unit"
        baseline = tool_input.get("baseline_branch") or "the baseline"
        prompt = {
            "title": "Revert preview to baseline?",
            "description": f"The AI Engineer wants to restore {target} to {baseline}.",
            "primary_action": "Revert to baseline",
            "secondary_action": "Cancel",
            "risk_level": "medium",
            "details": {
                "target_unit_id": tool_input.get("target_unit_id"),
                "target_application_id": tool_input.get("target_application_id"),
                "baseline_branch": tool_input.get("baseline_branch"),
                "baseline_commit_sha": tool_input.get("baseline_commit_sha"),
                "preview_deployment_id": tool_input.get("preview_deployment_id"),
                "summary": tool_input.get("summary"),
            },
        }
    else:
        prompt = {
            "title": "Approve tool execution?",
            "description": f"The AI Engineer wants to run {tool_name}.",
            "primary_action": "Approve",
            "secondary_action": "Cancel",
            "risk_level": "medium",
            "details": tool_input,
        }

    if isinstance(prompt_overrides, dict):
        return {**prompt, **prompt_overrides}
    return prompt
