"""Add raw_mentions table for browsing unmatched collected data.

Revision ID: 002
Revises: 001
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "raw_mentions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("source_id", sa.String(36), sa.ForeignKey("sources.id"), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64)),
        sa.Column("source_url", sa.Text()),
        sa.Column("metadata", JSONB()),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("promoted_to_finding_id", sa.String(36), sa.ForeignKey("findings.id")),
    )
    op.create_index("ix_raw_mentions_source_id", "raw_mentions", ["source_id"])
    op.create_index("ix_raw_mentions_content_hash", "raw_mentions", ["content_hash"])
    op.create_index("ix_raw_mentions_source_collected", "raw_mentions", ["source_id", "collected_at"])
    op.create_index("ix_raw_mentions_promoted", "raw_mentions", ["promoted_to_finding_id"])


def downgrade() -> None:
    op.drop_table("raw_mentions")
