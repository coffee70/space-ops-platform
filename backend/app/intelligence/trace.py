"""Trace envelope helpers for intelligence services."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import HTTPException, Request


TRACE_HEADERS = {
    "conversation_id": "x-conversation-id",
    "agent_run_id": "x-agent-run-id",
    "request_id": "x-request-id",
    "tool_call_id": "x-tool-call-id",
}


def _header(request: Request, key: str) -> str | None:
    value = request.headers.get(TRACE_HEADERS[key])
    return value.strip() if value else None


def extract_trace(request: Request, *, require_run: bool = False, require_conversation: bool = False) -> dict[str, str | None]:
    trace = {
        "conversation_id": _header(request, "conversation_id"),
        "agent_run_id": _header(request, "agent_run_id"),
        "request_id": _header(request, "request_id") or str(uuid.uuid4()),
        "tool_call_id": _header(request, "tool_call_id"),
    }
    if require_conversation and not trace["conversation_id"]:
        raise HTTPException(status_code=400, detail="missing conversation_id")
    if require_run and not trace["agent_run_id"]:
        raise HTTPException(status_code=400, detail="missing agent_run_id")
    return trace


def trace_headers(trace: dict[str, str | None]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, header in TRACE_HEADERS.items():
        value = trace.get(key)
        if value:
            headers[header] = value
    return headers


def with_trace(payload: dict[str, Any], trace: dict[str, str | None]) -> dict[str, Any]:
    merged = dict(payload)
    merged.update(trace)
    return merged
