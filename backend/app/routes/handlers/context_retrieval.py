from __future__ import annotations

import uuid

from fastapi import HTTPException, Request

from app.intelligence.clients.context_retrieval_clients import ContextRetrievalClients, get_context_clients
from app.intelligence.events import raw_event
from app.intelligence.trace import extract_trace

MAX_PACKET_CHARS = 24_000
MAX_DOC_CHUNKS = 6
MAX_CODE_CHUNKS = 6
MAX_PLATFORM_METADATA_CHARS = 6000
MAX_TOOLS = 20


def _truncate(value: str, limit: int):
    if len(value) <= limit:
        return value, False
    return value[:limit], True


def _failure_type(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        if exc.status_code == 504:
            return "timeout"
        if exc.status_code in (502, 503):
            return "unavailable"
    return "error"


def _record_failure(*, service: str, reason_prefix: str, exc: Exception, failed_sources: list[dict], truncation_reasons: list[str]) -> None:
    failure = {"service": service, "failure_type": _failure_type(exc)}
    failed_sources.append(failure)
    truncation_reasons.append(f"{reason_prefix}:{failure['failure_type']}")


def context_packet(
    body: dict,
    request: Request,
    clients: ContextRetrievalClients | None = None,
):
    clients = clients or get_context_clients()
    trace = extract_trace(request, require_run=False, require_conversation=False)
    conversation_id = body.get("conversation_id") or trace.get("conversation_id")
    agent_run_id = body.get("agent_run_id") or trace.get("agent_run_id")
    request_id = body.get("request_id") or trace.get("request_id")
    message = body.get("message", "")
    if not agent_run_id or not request_id:
        raise HTTPException(status_code=400, detail="agent_run_id and request_id are required")

    instructions = body.get("retrieval_instructions") or {}
    include_docs = bool(instructions.get("documents"))
    include_code = bool(instructions.get("code"))
    include_platform = bool(instructions.get("platform"))
    include_tools = bool(instructions.get("tools", True))

    truncation_reasons: list[str] = []
    failed_sources: list[dict] = []
    packet = {
        "mission_documents": [],
        "code_context": [],
        "platform_context": {},
        "runtime_context": {},
        "tool_context": [],
        "failed_sources": failed_sources,
    }

    if include_docs:
        try:
            packet["mission_documents"] = clients.fetch_document_context(
                query=message,
                mission_id=body.get("mission_id"),
                vehicle_id=body.get("vehicle_id"),
                limit=MAX_DOC_CHUNKS,
                trace=trace,
            )[:MAX_DOC_CHUNKS]
        except Exception as exc:  # noqa: BLE001
            _record_failure(
                service="document-knowledge-service",
                reason_prefix="document_context_failed",
                exc=exc,
                failed_sources=failed_sources,
                truncation_reasons=truncation_reasons,
            )

    if include_code:
        try:
            packet["code_context"] = clients.fetch_code_context(
                query=message,
                branch="main",
                limit=MAX_CODE_CHUNKS,
                trace=trace,
            )[:MAX_CODE_CHUNKS]
        except Exception as exc:  # noqa: BLE001
            _record_failure(
                service="code-intelligence-service",
                reason_prefix="code_context_failed",
                exc=exc,
                failed_sources=failed_sources,
                truncation_reasons=truncation_reasons,
            )

    if include_platform:
        platform_context = {"requested": True, "note": "Use registered tools for live platform metadata and runtime state."}
        payload, truncated = _truncate(str(platform_context), MAX_PLATFORM_METADATA_CHARS)
        packet["platform_context"] = {"summary": payload}
        if truncated:
            truncation_reasons.append("platform_metadata_limit")

    if include_tools:
        try:
            tools = clients.fetch_tool_registry_metadata(limit=MAX_TOOLS, trace=trace)
            packet["tool_context"] = [
                {
                    "name": str(t.get("name", "")),
                    "description": str(t.get("description", ""))[:500],
                    "category": t.get("category"),
                    "read_write_classification": t.get("read_write_classification"),
                    "required_execution_mode": t.get("required_execution_mode"),
                    "enabled": bool(t.get("enabled", False)),
                    "requires_confirmation": bool(t.get("requires_confirmation", False)),
                    "input_schema_json": str(t.get("input_schema_json", {}))[:1000],
                }
                for t in tools
            ][:MAX_TOOLS]
        except Exception as exc:  # noqa: BLE001
            _record_failure(
                service="tool-registry-service",
                reason_prefix="tool_context_failed",
                exc=exc,
                failed_sources=failed_sources,
                truncation_reasons=truncation_reasons,
            )

    serialized = str(packet)
    if len(serialized) > MAX_PACKET_CHARS:
        truncation_reasons.append("context_packet_chars")

    context_packet_id = str(uuid.uuid4())
    resolved_event = raw_event(
        event_type="context.resolved",
        payload={
            "context_packet_id": context_packet_id,
            "document_chunk_count": len(packet["mission_documents"]),
            "code_chunk_count": len(packet["code_context"]),
            "platform_metadata_bytes": len(str(packet["platform_context"])),
            "tool_definition_count": len(packet["tool_context"]),
            "truncated": bool(truncation_reasons),
            "truncation_reasons": truncation_reasons,
        },
        emitted_by="context-retrieval-service",
    )

    return {
        "conversation_id": conversation_id,
        "agent_run_id": agent_run_id,
        "request_id": request_id,
        "context_packet_id": context_packet_id,
        "document_chunk_count": len(packet["mission_documents"]),
        "code_chunk_count": len(packet["code_context"]),
        "platform_metadata_bytes": len(str(packet["platform_context"])),
        "tool_definition_count": len(packet["tool_context"]),
        "truncated": bool(truncation_reasons),
        "truncation_reasons": truncation_reasons,
        "failed_sources": failed_sources,
        "data": packet,
        "raw_events": [resolved_event],
    }
