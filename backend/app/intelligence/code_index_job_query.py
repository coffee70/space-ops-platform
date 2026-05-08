"""Shared helpers for code index job lookups (API + worker)."""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from app.models.intelligence import CodeIndexJob


def find_active_index_job(
    db: Session,
    repository_id: uuid.UUID,
    target_commit_sha: str,
) -> CodeIndexJob | None:
    """Return the oldest queued/running job for this repository and target commit, or None."""
    return (
        db.query(CodeIndexJob)
        .filter(
            CodeIndexJob.repository_id == repository_id,
            CodeIndexJob.target_commit_sha == target_commit_sha,
            CodeIndexJob.status.in_(("queued", "running")),
        )
        .order_by(CodeIndexJob.requested_at.asc())
        .first()
    )
