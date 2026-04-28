from __future__ import annotations

import uuid

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.intelligence.events import emit_event
from app.models.intelligence import ToolDefinition
from app.routes.handlers import code_intelligence, document_knowledge

MAX_PACKET_CHARS = 24_000
MAX_DOC_CHUNKS = 6
MAX_CODE_CHUNKS = 6
MAX_PLATFORM_METADATA_CHARS = 6000
MAX_TOOLS = 20


def _truncate(value: str, limit: int):
    if len(value) <= limit:
        return value, False
    return value[:limit], True


def context_packet(body: dict, db: Session = Depends(get_db)):
    conversation_id = body.get("conversation_id")
    agent_run_id = body.get("agent_run_id")
    request_id = body.get("request_id")
    message = body.get("message", "")
    if not agent_run_id or not request_id:
        raise HTTPException(status_code=400, detail="agent_run_id and request_id are required")

    instructions = body.get("retrieval_instructions") or {}
    include_docs = bool(instructions.get("documents"))
    include_code = bool(instructions.get("code"))
    include_platform = bool(instructions.get("platform"))
    include_tools = bool(instructions.get("tools", True))

    truncation_reasons: list[str] = []
    packet = {
        "mission_documents": [],
        "code_context": [],
        "platform_context": {},
        "runtime_context": {},
        "tool_context": [],
    }

    if include_docs:
        packet["mission_documents"] = document_knowledge.search_documents(
            {"query": message, "mission_id": body.get("mission_id"), "vehicle_id": body.get("vehicle_id"), "limit": MAX_DOC_CHUNKS},
            db,
        )[:MAX_DOC_CHUNKS]

    if include_code:
        packet["code_context"] = code_intelligence.search_code({"query": message, "branch": "main", "limit": MAX_CODE_CHUNKS}, db)[:MAX_CODE_CHUNKS]

    if include_platform:
        platform_context = {"requested": True, "note": "Use registered tools for live platform metadata and runtime state."}
        payload, truncated = _truncate(str(platform_context), MAX_PLATFORM_METADATA_CHARS)
        packet["platform_context"] = {"summary": payload}
        if truncated:
            truncation_reasons.append("platform_metadata_limit")

    if include_tools:
        tools = db.query(ToolDefinition).order_by(ToolDefinition.name.asc()).limit(MAX_TOOLS).all()
        packet["tool_context"] = [
            {
                "name": t.name,
                "description": t.description[:500],
                "category": t.category,
                "read_write_classification": t.read_write_classification,
                "required_execution_mode": t.required_execution_mode,
                "enabled": t.enabled,
                "requires_confirmation": t.requires_confirmation,
                "input_schema_json": str(t.input_schema_json)[:1000],
            }
            for t in tools
        ]

    serialized = str(packet)
    if len(serialized) > MAX_PACKET_CHARS:
        truncation_reasons.append("context_packet_chars")

    context_packet_id = str(uuid.uuid4())
    emit_event(
        db,
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
        conversation_id=conversation_id,
        agent_run_id=agent_run_id,
        request_id=request_id,
        sequence=1,
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
        "data": packet,
    }
