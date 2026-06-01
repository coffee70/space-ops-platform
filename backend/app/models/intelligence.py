"""Intelligence-layer database models."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pgvector.sqlalchemy import Vector
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Text, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Conversation(Base):
    __tablename__ = "ai_conversations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    mission_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    vehicle_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    execution_mode: Mapped[str] = mapped_column(Text, nullable=False, default="read_only")
    selected_model_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    title_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    title_model_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class ConversationMessageRecord(Base):
    __tablename__ = "ai_conversation_messages"
    __table_args__ = (Index("ix_ai_conversation_messages_conversation_created", "conversation_id", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_conversations.id", ondelete="CASCADE"), nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    request_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    agent_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class ToolDefinition(Base):
    __tablename__ = "ai_tool_definitions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    layer_target: Mapped[str] = mapped_column(Text, nullable=False)
    backing_service: Mapped[str | None] = mapped_column(Text, nullable=True)
    backing_api: Mapped[str | None] = mapped_column(Text, nullable=True)
    read_write_classification: Mapped[str] = mapped_column(Text, nullable=False)
    required_execution_mode: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    requires_confirmation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    mode_policy_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    permission_prompt_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    input_schema_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    output_schema_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    audit_policy_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    redaction_policy_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    show_result_in_ui: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class ToolCall(Base):
    __tablename__ = "ai_tool_calls"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_conversations.id", ondelete="SET NULL"), nullable=True)
    assistant_message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_conversation_messages.id", ondelete="CASCADE"), nullable=False)
    agent_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    request_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tool_call_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    tool_name: Mapped[str] = mapped_column(Text, nullable=False)
    input_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    redacted_input_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    output_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class ToolPermissionRequest(Base):
    __tablename__ = "ai_tool_permission_requests"
    __table_args__ = (
        Index("ix_ai_tool_permission_requests_agent_run_created", "agent_run_id", "created_at"),
        Index("ix_ai_tool_permission_requests_status_created", "status", "created_at"),
        Index("ix_ai_tool_permission_requests_tool_call_id", "tool_call_id", unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_conversations.id", ondelete="SET NULL"), nullable=True)
    assistant_message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_conversation_messages.id", ondelete="CASCADE"), nullable=False)
    tool_call_record_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_tool_calls.id", ondelete="CASCADE"), nullable=False)
    agent_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    request_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tool_call_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tool_name: Mapped[str] = mapped_column(Text, nullable=False)
    input_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    redacted_input_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    mode_policy_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    execution_mode: Mapped[str] = mapped_column(Text, nullable=False)
    response_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AgentEvent(Base):
    __tablename__ = "ai_agent_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_conversations.id", ondelete="SET NULL"), nullable=True)
    agent_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    request_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tool_call_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    emitted_by: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class AgentModelStepUsage(Base):
    __tablename__ = "agent_model_step_usage"
    __table_args__ = (
        Index("agent_model_step_usage_run_idx", "agent_run_id", "step_index"),
        Index("agent_model_step_usage_provider_window_idx", "provider_type", "provider_model_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    agent_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    step_index: Mapped[int] = mapped_column(Integer, nullable=False)
    step_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_type: Mapped[str] = mapped_column(Text, nullable=False)
    provider_model_id: Mapped[str] = mapped_column(Text, nullable=False)
    model_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reasoning_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cached_input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    usage_source: Mapped[str] = mapped_column(Text, nullable=False)
    is_actual: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    raw_usage_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    synced_after: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class AgentRunModelUsage(Base):
    __tablename__ = "agent_run_model_usage"
    __table_args__ = (Index("agent_run_model_usage_provider_window_idx", "provider_type", "provider_model_id", "updated_at"),)

    agent_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    provider_type: Mapped[str] = mapped_column(Text, nullable=False)
    provider_model_id: Mapped[str] = mapped_column(Text, nullable=False)
    model_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reasoning_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cached_input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    usage_source: Mapped[str] = mapped_column(Text, nullable=False)
    is_actual: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    raw_usage_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class AgentModelStepUsageEstimate(Base):
    __tablename__ = "agent_model_step_usage_estimate"
    __table_args__ = (Index("agent_model_step_usage_estimate_run_idx", "agent_run_id", "step_index", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    agent_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    step_index: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_type: Mapped[str] = mapped_column(Text, nullable=False)
    provider_model_id: Mapped[str] = mapped_column(Text, nullable=False)
    estimated_output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    estimated_total_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    estimate_source: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class Document(Base):
    __tablename__ = "ai_documents"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    document_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    mission_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    vehicle_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    subsystem_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    tags_json: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    ingestion_status: Mapped[str] = mapped_column(Text, nullable=False)
    ingestion_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class DocumentIngestionJob(Base):
    __tablename__ = "ai_document_ingestion_jobs"
    __table_args__ = (
        Index("ix_ai_document_ingestion_jobs_status_requested", "status", "requested_at"),
        Index("ix_ai_document_ingestion_jobs_document", "document_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_documents.id", ondelete="CASCADE"), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    agent_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    request_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class DocumentChunk(Base):
    __tablename__ = "ai_document_chunks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_documents.id", ondelete="CASCADE"), nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    embedding: Mapped[list | None] = mapped_column(Vector(384), nullable=True)
    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class CodeRepository(Base):
    __tablename__ = "ai_code_repositories"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    source_uri: Mapped[str] = mapped_column(Text, nullable=False)
    layer: Mapped[str] = mapped_column(Text, nullable=False)
    default_branch: Mapped[str] = mapped_column(Text, nullable=False, default="main")
    index_status: Mapped[str] = mapped_column(Text, nullable=False, default="not_indexed")
    index_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    index_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    index_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    indexed_commit_sha: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_commit_sha: Mapped[str | None] = mapped_column(Text, nullable=True)
    file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped_file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class CodeIndexJob(Base):
    __tablename__ = "ai_code_index_jobs"
    __table_args__ = (Index("ix_ai_code_index_jobs_status_requested", "status", "requested_at"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    repository_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_code_repositories.id", ondelete="CASCADE"), nullable=False)
    root: Mapped[str] = mapped_column(Text, nullable=False)
    branch: Mapped[str] = mapped_column(Text, nullable=False)
    target_commit_sha: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    requested_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped_file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_files_preview_json: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class CodeChunk(Base):
    __tablename__ = "ai_code_chunks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    repository_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ai_code_repositories.id", ondelete="CASCADE"), nullable=False)
    branch: Mapped[str] = mapped_column(Text, nullable=False)
    commit_sha: Mapped[str] = mapped_column(Text, nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str | None] = mapped_column(Text, nullable=True)
    symbol_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    symbol_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_line: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_line: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list | None] = mapped_column(Vector(384), nullable=True)
    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    indexed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
