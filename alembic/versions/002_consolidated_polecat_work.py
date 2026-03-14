"""Consolidated polecat work: raw_mentions table, new source types, orphan cleanup.

Revision ID: 002
Revises: 001
Create Date: 2026-03-12
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add new enum values for source types
    op.execute("ALTER TYPE sourcetype ADD VALUE IF NOT EXISTS 'telegram_intel'")
    op.execute("ALTER TYPE sourcetype ADD VALUE IF NOT EXISTS 'discord'")
    op.execute("ALTER TYPE sourcetype ADD VALUE IF NOT EXISTS 'ransomware_aggregator'")

    # 2. Remove orphaned sources with nonexistent connector paths
    op.execute(
        sa.text(
            "DELETE FROM sources "
            "WHERE connector_class LIKE 'darkdisco.connectors.%'"
        )
    )

    # 3. Create raw_mentions table
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
