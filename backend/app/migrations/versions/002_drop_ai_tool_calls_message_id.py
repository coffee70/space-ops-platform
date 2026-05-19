"""Drop unused ai_tool_calls.message_id."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision: str = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("ai_tool_calls", "message_id")


def downgrade() -> None:
    op.add_column("ai_tool_calls", sa.Column("message_id", UUID(as_uuid=True), nullable=True))
    op.create_foreign_key(
        "fk_ai_tool_calls_message_id_ai_conversation_messages",
        "ai_tool_calls",
        "ai_conversation_messages",
        ["message_id"],
        ["id"],
        ondelete="SET NULL",
    )
