"""Pydantic schemas for intelligence services."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class TraceEnvelope(BaseModel):
    conversation_id: str | None = None
    agent_run_id: str
    request_id: str
    tool_call_id: str | None = None


class ToolExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversation_id: str | None = None
    agent_run_id: str
    request_id: str
    tool_call_id: str
    tool_name: str
    input: dict[str, Any] = Field(default_factory=dict)
    confirmation_token: str | None = None
    execution_mode: Literal["read_only", "suggest", "execute", "governed_execute"] = "read_only"
    message_id: str | None = None


class ToolExecutionResponse(BaseModel):
    conversation_id: str | None = None
    agent_run_id: str
    request_id: str
    tool_call_id: str
    status: Literal["completed", "failed", "confirmation_required"]
    output: dict[str, Any] = Field(default_factory=dict)
    raw_events: list[dict[str, Any]] = Field(default_factory=list)


class ContextPacketRequest(BaseModel):
    conversation_id: str | None = None
    agent_run_id: str
    request_id: str
    message: str
    retrieval_instructions: dict[str, Any] = Field(default_factory=dict)
    mission_id: str | None = None
    vehicle_id: str | None = None
    execution_mode: str = "read_only"


class ContextPacketResponse(BaseModel):
    conversation_id: str | None = None
    agent_run_id: str
    request_id: str
    context_packet_id: str
    document_chunk_count: int
    code_chunk_count: int
    platform_metadata_bytes: int
    tool_definition_count: int
    truncated: bool
    truncation_reasons: list[str] = Field(default_factory=list)
    data: dict[str, Any] = Field(default_factory=dict)
    raw_events: list[dict[str, Any]] = Field(default_factory=list)


class ToolDefinitionSummary(BaseModel):
    name: str
    description: str
    category: str
    layer_target: str
    read_write_classification: str
    required_execution_mode: str
    enabled: bool
    requires_confirmation: bool
    input_schema_json: dict[str, Any] = Field(default_factory=dict)


class DocumentUploadResponse(BaseModel):
    document_id: str
    title: str
    ingestion_status: str


class DocumentSearchRequest(BaseModel):
    query: str
    mission_id: str | None = None
    vehicle_id: str | None = None
    subsystem_id: str | None = None
    limit: int = 6


class DocumentSearchHit(BaseModel):
    document_id: str
    title: str
    chunk_index: int
    content: str
    score: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class CodeSearchRequest(BaseModel):
    query: str
    repository: str | None = None
    branch: str = "main"
    limit: int = 6


class CodeSearchHit(BaseModel):
    repository: str
    branch: str
    commit_sha: str
    file_path: str
    symbol_name: str | None = None
    symbol_type: str | None = None
    start_line: int | None = None
    end_line: int | None = None
    content: str
    score: float = 0.0


class ConversationCreateRequest(BaseModel):
    title: str | None = None
    mission_id: str | None = None
    vehicle_id: str | None = None
    execution_mode: str = "read_only"


class ConversationMessage(BaseModel):
    role: str
    content: str
    created_at: datetime


class ChatRequest(BaseModel):
    conversation_id: str
    execution_mode: str = "read_only"
    mission_id: str | None = None
    vehicle_id: str | None = None
    messages: list[dict[str, Any]] = Field(default_factory=list)
