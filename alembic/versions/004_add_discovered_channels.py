"""Add discovered_channels table for auto-discovery of Telegram channels.

Revision ID: 004
Revises: 003
"""

from alembic import op
import sqlalchemy as sa

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "discovered_channels",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("url", sa.String(512), nullable=False),
        sa.Column("source_id", sa.String(36), sa.ForeignKey("sources.id"), nullable=False),
        sa.Column("source_channel", sa.String(255)),
        sa.Column("message_id", sa.Integer()),
        sa.Column(
            "status",
            sa.Enum("pending", "approved", "joined", "failed", "ignored", name="discoverystatus"),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("added_to_source_id", sa.String(36), sa.ForeignKey("sources.id")),
        sa.Column("notes", sa.Text()),
        sa.Column("discovered_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("joined_at", sa.DateTime(timezone=True)),
    )

    op.create_index("ix_discovered_channels_url", "discovered_channels", ["url"])
    op.create_index("ix_discovered_channels_status", "discovered_channels", ["status"])
    op.create_index("ix_discovered_channels_source_status", "discovered_channels", ["source_id", "status"])
    op.create_index("ix_discovered_channels_source_id", "discovered_channels", ["source_id"])


def downgrade() -> None:
    op.drop_table("discovered_channels")
    op.execute("DROP TYPE IF EXISTS discoverystatus")
