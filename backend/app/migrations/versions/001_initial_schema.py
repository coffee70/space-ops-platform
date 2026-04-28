"""Initial baseline schema."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects.postgresql import JSONB, UUID

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"))
    op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))

    op.create_table(
        "telemetry_sources",
        sa.Column("id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("base_url", sa.Text(), nullable=True),
        sa.Column("vehicle_config_path", sa.Text(), nullable=False),
        sa.Column("monitoring_start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("history_mode", sa.Text(), nullable=False),
        sa.Column("live_state", sa.Text(), nullable=False),
        sa.Column("backfill_state", sa.Text(), nullable=False),
        sa.Column("active_backfill_target_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_backfill_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_backfill_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_backfill_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_telemetry_sources_vehicle_config_path",
        "telemetry_sources",
        ["vehicle_config_path"],
        unique=True,
    )

    op.create_table(
        "telemetry_metadata",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "source_id",
            sa.Text(),
            sa.ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("units", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("subsystem_tag", sa.Text(), nullable=True),
        sa.Column("channel_origin", sa.Text(), nullable=False),
        sa.Column("discovery_namespace", sa.Text(), nullable=True),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("red_low", sa.Numeric(20, 10), nullable=True),
        sa.Column("red_high", sa.Numeric(20, 10), nullable=True),
        sa.Column("embedding", Vector(384), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_telemetry_metadata_source_name",
        "telemetry_metadata",
        ["source_id", "name"],
        unique=True,
    )
    op.create_index(
        op.f("ix_telemetry_metadata_source_id"),
        "telemetry_metadata",
        ["source_id"],
    )
    op.create_index(
        op.f("ix_telemetry_metadata_name"),
        "telemetry_metadata",
        ["name"],
    )

    op.create_table(
        "telemetry_channel_aliases",
        sa.Column(
            "source_id",
            sa.Text(),
            sa.ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("alias_name", sa.Text(), primary_key=True, nullable=False),
        sa.Column(
            "telemetry_id",
            UUID(as_uuid=True),
            sa.ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_telemetry_channel_aliases_source_alias",
        "telemetry_channel_aliases",
        ["source_id", "alias_name"],
        unique=True,
    )
    op.create_index(
        "ix_telemetry_channel_aliases_source_telemetry",
        "telemetry_channel_aliases",
        ["source_id", "telemetry_id"],
    )

    op.create_table(
        "telemetry_data",
        sa.Column(
            "source_id",
            sa.Text(),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "telemetry_id",
            UUID(as_uuid=True),
            sa.ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("timestamp", sa.DateTime(timezone=True), primary_key=True, nullable=False),
        sa.Column("sequence", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("value", sa.Numeric(20, 10), nullable=False),
        sa.Column("packet_source", sa.Text(), nullable=True),
        sa.Column("receiver_id", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_telemetry_data_source_telemetry_timestamp",
        "telemetry_data",
        ["source_id", "telemetry_id", "timestamp", "sequence"],
    )
    op.create_index(
        "ix_telemetry_data_telemetry_timestamp_source",
        "telemetry_data",
        ["telemetry_id", "timestamp", "sequence", "source_id"],
    )
    op.execute(
        sa.text(
            """
            SELECT create_hypertable(
              'telemetry_data',
              'timestamp',
              if_not_exists => TRUE,
              migrate_data => TRUE
            )
            """
        )
    )

    op.create_table(
        "telemetry_statistics",
        sa.Column(
            "source_id",
            sa.Text(),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "telemetry_id",
            UUID(as_uuid=True),
            sa.ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("mean", sa.Numeric(20, 10), nullable=False),
        sa.Column("std_dev", sa.Numeric(20, 10), nullable=False),
        sa.Column("min_value", sa.Numeric(20, 10), nullable=False),
        sa.Column("max_value", sa.Numeric(20, 10), nullable=False),
        sa.Column("p5", sa.Numeric(20, 10), nullable=False),
        sa.Column("p50", sa.Numeric(20, 10), nullable=False),
        sa.Column("p95", sa.Numeric(20, 10), nullable=False),
        sa.Column("n_samples", sa.Integer(), nullable=False),
        sa.Column("last_computed_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "watchlist",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "source_id",
            sa.Text(),
            sa.ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("telemetry_name", sa.Text(), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(op.f("ix_watchlist_source_id"), "watchlist", ["source_id"])
    op.create_index(op.f("ix_watchlist_telemetry_name"), "watchlist", ["telemetry_name"])
    op.create_index(
        "ix_watchlist_source_telemetry_name",
        "watchlist",
        ["source_id", "telemetry_name"],
        unique=True,
    )

    op.create_table(
        "telemetry_current",
        sa.Column(
            "source_id",
            sa.Text(),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "telemetry_id",
            UUID(as_uuid=True),
            sa.ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("generation_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reception_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("value", sa.Numeric(20, 10), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("state_reason", sa.Text(), nullable=True),
        sa.Column("z_score", sa.Numeric(20, 10), nullable=True),
        sa.Column("quality", sa.Text(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=True),
        sa.Column("packet_source", sa.Text(), nullable=True),
        sa.Column("receiver_id", sa.Text(), nullable=True),
    )

    op.create_table(
        "telemetry_alerts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column(
            "telemetry_id",
            UUID(as_uuid=True),
            sa.ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("opened_reception_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_update_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("current_value_at_open", sa.Numeric(20, 10), nullable=False),
        sa.Column("acked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acked_by", sa.Text(), nullable=True),
        sa.Column("cleared_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by", sa.Text(), nullable=True),
        sa.Column("resolution_text", sa.Text(), nullable=True),
        sa.Column("resolution_code", sa.Text(), nullable=True),
    )
    op.create_index(
        op.f("ix_telemetry_alerts_source_id"),
        "telemetry_alerts",
        ["source_id"],
    )
    op.create_index(
        op.f("ix_telemetry_alerts_telemetry_id"),
        "telemetry_alerts",
        ["telemetry_id"],
    )

    op.create_table(
        "telemetry_feed_health",
        sa.Column(
            "source_id",
            sa.Text(),
            sa.ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("connected", sa.Boolean(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("last_reception_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("approx_rate_hz", sa.Numeric(20, 10), nullable=True),
        sa.Column("drop_count", sa.Integer(), nullable=False),
        sa.Column("last_transition_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "ops_events",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("stream_id", sa.Text(), nullable=True),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_id", sa.Text(), nullable=True),
        sa.Column("payload", JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ops_events_source_time", "ops_events", ["source_id", "event_time"])
    op.create_index("ix_ops_events_type_time", "ops_events", ["event_type", "event_time"])

    op.create_table(
        "telemetry_alert_notes",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "alert_id",
            UUID(as_uuid=True),
            sa.ForeignKey("telemetry_alerts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("author", sa.Text(), nullable=False),
        sa.Column("note_text", sa.Text(), nullable=False),
        sa.Column("note_type", sa.Text(), nullable=False),
    )
    op.create_index(op.f("ix_telemetry_alert_notes_alert_id"), "telemetry_alert_notes", ["alert_id"])

    op.create_table(
        "position_channel_mappings",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "source_id",
            sa.Text(),
            sa.ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("frame_type", sa.Text(), nullable=False),
        sa.Column("lat_channel_name", sa.Text(), nullable=True),
        sa.Column("lon_channel_name", sa.Text(), nullable=True),
        sa.Column("alt_channel_name", sa.Text(), nullable=True),
        sa.Column("x_channel_name", sa.Text(), nullable=True),
        sa.Column("y_channel_name", sa.Text(), nullable=True),
        sa.Column("z_channel_name", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_position_channel_mappings_source_active",
        "position_channel_mappings",
        ["source_id", "active"],
    )

    op.create_table(
        "telemetry_streams",
        sa.Column("id", sa.Text(), primary_key=True, nullable=False),
        sa.Column(
            "source_id",
            sa.Text(),
            sa.ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("packet_source", sa.Text(), nullable=True),
        sa.Column("receiver_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata", JSONB(), nullable=True),
    )
    op.create_index(op.f("ix_telemetry_streams_source_id"), "telemetry_streams", ["source_id"])

    op.create_table(
        "source_observations",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "source_id",
            sa.Text(),
            sa.ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("provider", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("station_name", sa.Text(), nullable=True),
        sa.Column("station_id", sa.Text(), nullable=True),
        sa.Column("receiver_id", sa.Text(), nullable=True),
        sa.Column("max_elevation_deg", sa.Numeric(20, 10), nullable=True),
        sa.Column("details_json", JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_source_observations_source_start",
        "source_observations",
        ["source_id", "start_time"],
    )
    op.create_index(
        "ix_source_observations_source_status_start",
        "source_observations",
        ["source_id", "status", "start_time"],
    )
    op.create_index(
        "uq_source_observations_source_external",
        "source_observations",
        ["source_id", "external_id"],
        unique=True,
        postgresql_where=sa.text("external_id IS NOT NULL"),
    )

    op.create_table(
        "ai_conversations",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("mission_id", sa.Text(), nullable=True),
        sa.Column("vehicle_id", sa.Text(), nullable=True),
        sa.Column("execution_mode", sa.Text(), nullable=False, server_default="read_only"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "ai_conversation_messages",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "conversation_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ai_conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("metadata_json", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_ai_conversation_messages_conversation_created",
        "ai_conversation_messages",
        ["conversation_id", "created_at"],
    )

    op.create_table(
        "ai_tool_definitions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("layer_target", sa.Text(), nullable=False),
        sa.Column("backing_service", sa.Text(), nullable=True),
        sa.Column("backing_api", sa.Text(), nullable=True),
        sa.Column("read_write_classification", sa.Text(), nullable=False),
        sa.Column("required_execution_mode", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("requires_confirmation", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("input_schema_json", JSONB(), nullable=False),
        sa.Column("output_schema_json", JSONB(), nullable=False),
        sa.Column("audit_policy_json", JSONB(), nullable=False),
        sa.Column("redaction_policy_json", JSONB(), nullable=False),
        sa.Column("show_result_in_ui", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ai_tool_definitions_name", "ai_tool_definitions", ["name"], unique=True)

    op.create_table(
        "ai_tool_calls",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "conversation_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ai_conversations.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("agent_run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("request_id", UUID(as_uuid=True), nullable=False),
        sa.Column("tool_call_id", UUID(as_uuid=True), nullable=False),
        sa.Column(
            "message_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ai_conversation_messages.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("tool_name", sa.Text(), nullable=False),
        sa.Column("input_json", JSONB(), nullable=False),
        sa.Column("redacted_input_json", JSONB(), nullable=False),
        sa.Column("output_json", JSONB(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index("ix_ai_tool_calls_conversation_started", "ai_tool_calls", ["conversation_id", "started_at"])
    op.create_index("ix_ai_tool_calls_agent_run_started", "ai_tool_calls", ["agent_run_id", "started_at"])
    op.create_index("ix_ai_tool_calls_tool_call_id", "ai_tool_calls", ["tool_call_id"], unique=True)

    op.create_table(
        "ai_agent_events",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "conversation_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ai_conversations.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("agent_run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("request_id", UUID(as_uuid=True), nullable=False),
        sa.Column("tool_call_id", UUID(as_uuid=True), nullable=True),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("emitted_by", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload_json", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ai_agent_events_conversation_created", "ai_agent_events", ["conversation_id", "created_at"])
    op.create_index("ix_ai_agent_events_agent_sequence", "ai_agent_events", ["agent_run_id", "sequence"], unique=True)

    op.create_table(
        "ai_documents",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("document_type", sa.Text(), nullable=False),
        sa.Column("source_uri", sa.Text(), nullable=True),
        sa.Column("mission_id", sa.Text(), nullable=True),
        sa.Column("vehicle_id", sa.Text(), nullable=True),
        sa.Column("subsystem_id", sa.Text(), nullable=True),
        sa.Column("tags_json", JSONB(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("ingestion_status", sa.Text(), nullable=False),
        sa.Column("ingestion_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ai_documents_scope", "ai_documents", ["mission_id", "vehicle_id", "subsystem_id"])
    op.create_index("ix_ai_documents_ingestion_status", "ai_documents", ["ingestion_status"])

    op.create_table(
        "ai_document_chunks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "document_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ai_documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("metadata_json", JSONB(), nullable=False),
        sa.Column("embedding", Vector(384), nullable=True),
        sa.Column("embedding_model", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ai_document_chunks_document_chunk", "ai_document_chunks", ["document_id", "chunk_index"])

    op.create_table(
        "ai_code_repositories",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("source_uri", sa.Text(), nullable=False),
        sa.Column("layer", sa.Text(), nullable=False),
        sa.Column("default_branch", sa.Text(), nullable=False, server_default="main"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "ai_code_chunks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "repository_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ai_code_repositories.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("branch", sa.Text(), nullable=False),
        sa.Column("commit_sha", sa.Text(), nullable=False),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("language", sa.Text(), nullable=True),
        sa.Column("symbol_name", sa.Text(), nullable=True),
        sa.Column("symbol_type", sa.Text(), nullable=True),
        sa.Column("start_line", sa.Integer(), nullable=True),
        sa.Column("end_line", sa.Integer(), nullable=True),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(384), nullable=True),
        sa.Column("embedding_model", sa.Text(), nullable=False),
        sa.Column("metadata_json", JSONB(), nullable=False),
        sa.Column("indexed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ai_code_chunks_repo_branch_commit_path", "ai_code_chunks", ["repository_id", "branch", "commit_sha", "file_path"])


def downgrade() -> None:
    op.drop_index("ix_ai_code_chunks_repo_branch_commit_path", table_name="ai_code_chunks")
    op.drop_table("ai_code_chunks")

    op.drop_table("ai_code_repositories")

    op.drop_index("ix_ai_document_chunks_document_chunk", table_name="ai_document_chunks")
    op.drop_table("ai_document_chunks")

    op.drop_index("ix_ai_documents_ingestion_status", table_name="ai_documents")
    op.drop_index("ix_ai_documents_scope", table_name="ai_documents")
    op.drop_table("ai_documents")

    op.drop_index("ix_ai_agent_events_agent_sequence", table_name="ai_agent_events")
    op.drop_index("ix_ai_agent_events_conversation_created", table_name="ai_agent_events")
    op.drop_table("ai_agent_events")

    op.drop_index("ix_ai_tool_calls_tool_call_id", table_name="ai_tool_calls")
    op.drop_index("ix_ai_tool_calls_agent_run_started", table_name="ai_tool_calls")
    op.drop_index("ix_ai_tool_calls_conversation_started", table_name="ai_tool_calls")
    op.drop_table("ai_tool_calls")

    op.drop_index("ix_ai_tool_definitions_name", table_name="ai_tool_definitions")
    op.drop_table("ai_tool_definitions")

    op.drop_index("ix_ai_conversation_messages_conversation_created", table_name="ai_conversation_messages")
    op.drop_table("ai_conversation_messages")

    op.drop_table("ai_conversations")

    op.drop_index(
        "uq_source_observations_source_external",
        table_name="source_observations",
        postgresql_where=sa.text("external_id IS NOT NULL"),
    )
    op.drop_index("ix_source_observations_source_status_start", table_name="source_observations")
    op.drop_index("ix_source_observations_source_start", table_name="source_observations")
    op.drop_table("source_observations")

    op.drop_index(op.f("ix_telemetry_streams_source_id"), table_name="telemetry_streams")
    op.drop_table("telemetry_streams")

    op.drop_index("ix_position_channel_mappings_source_active", table_name="position_channel_mappings")
    op.drop_table("position_channel_mappings")

    op.drop_index(op.f("ix_telemetry_alert_notes_alert_id"), table_name="telemetry_alert_notes")
    op.drop_table("telemetry_alert_notes")

    op.drop_index("ix_ops_events_type_time", table_name="ops_events")
    op.drop_index("ix_ops_events_source_time", table_name="ops_events")
    op.drop_table("ops_events")

    op.drop_index(op.f("ix_telemetry_alerts_telemetry_id"), table_name="telemetry_alerts")
    op.drop_index(op.f("ix_telemetry_alerts_source_id"), table_name="telemetry_alerts")
    op.drop_table("telemetry_alerts")

    op.drop_table("telemetry_feed_health")

    op.drop_table("telemetry_current")

    op.drop_index("ix_watchlist_source_telemetry_name", table_name="watchlist")
    op.drop_index(op.f("ix_watchlist_telemetry_name"), table_name="watchlist")
    op.drop_index(op.f("ix_watchlist_source_id"), table_name="watchlist")
    op.drop_table("watchlist")

    op.drop_table("telemetry_statistics")

    op.drop_index("ix_telemetry_data_telemetry_timestamp_source", table_name="telemetry_data")
    op.drop_index("ix_telemetry_data_source_telemetry_timestamp", table_name="telemetry_data")
    op.drop_table("telemetry_data")

    op.drop_index("ix_telemetry_channel_aliases_source_telemetry", table_name="telemetry_channel_aliases")
    op.drop_index(
        "ix_telemetry_channel_aliases_source_alias",
        table_name="telemetry_channel_aliases",
    )
    op.drop_table("telemetry_channel_aliases")

    op.drop_index(op.f("ix_telemetry_metadata_name"), table_name="telemetry_metadata")
    op.drop_index(op.f("ix_telemetry_metadata_source_id"), table_name="telemetry_metadata")
    op.drop_index("ix_telemetry_metadata_source_name", table_name="telemetry_metadata")
    op.drop_table("telemetry_metadata")

    op.drop_index("ix_telemetry_sources_vehicle_config_path", table_name="telemetry_sources")
    op.drop_table("telemetry_sources")

    op.execute(sa.text("DROP EXTENSION IF EXISTS vector"))
    op.execute(sa.text("DROP EXTENSION IF EXISTS timescaledb CASCADE"))
