"""Code repository index state and code index jobs."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ai_code_repositories",
        sa.Column("index_status", sa.Text(), nullable=False, server_default="not_indexed"),
    )
    op.add_column("ai_code_repositories", sa.Column("index_requested_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("ai_code_repositories", sa.Column("index_started_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("ai_code_repositories", sa.Column("index_completed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("ai_code_repositories", sa.Column("indexed_commit_sha", sa.Text(), nullable=True))
    op.add_column("ai_code_repositories", sa.Column("current_commit_sha", sa.Text(), nullable=True))
    op.add_column(
        "ai_code_repositories",
        sa.Column("file_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "ai_code_repositories",
        sa.Column("chunk_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "ai_code_repositories",
        sa.Column("skipped_file_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "ai_code_repositories",
        sa.Column("failed_file_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("ai_code_repositories", sa.Column("last_error", sa.Text(), nullable=True))

    op.create_table(
        "ai_code_index_jobs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "repository_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ai_code_repositories.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("root", sa.Text(), nullable=False),
        sa.Column("branch", sa.Text(), nullable=False),
        sa.Column("target_commit_sha", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("requested_by", sa.Text(), nullable=True),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("file_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("chunk_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped_file_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_file_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_files_preview_json", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index("ix_ai_code_index_jobs_status_requested", "ai_code_index_jobs", ["status", "requested_at"])
    op.create_index("ix_ai_code_index_jobs_repository", "ai_code_index_jobs", ["repository_id"])


def downgrade() -> None:
    op.drop_index("ix_ai_code_index_jobs_repository", table_name="ai_code_index_jobs")
    op.drop_index("ix_ai_code_index_jobs_status_requested", table_name="ai_code_index_jobs")
    op.drop_table("ai_code_index_jobs")

    op.drop_column("ai_code_repositories", "last_error")
    op.drop_column("ai_code_repositories", "failed_file_count")
    op.drop_column("ai_code_repositories", "skipped_file_count")
    op.drop_column("ai_code_repositories", "chunk_count")
    op.drop_column("ai_code_repositories", "file_count")
    op.drop_column("ai_code_repositories", "current_commit_sha")
    op.drop_column("ai_code_repositories", "indexed_commit_sha")
    op.drop_column("ai_code_repositories", "index_completed_at")
    op.drop_column("ai_code_repositories", "index_started_at")
    op.drop_column("ai_code_repositories", "index_requested_at")
    op.drop_column("ai_code_repositories", "index_status")
