"""Add system_settings table for runtime-configurable settings.

Revision ID: 012
Revises: 011
"""

from alembic import op
import sqlalchemy as sa

revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "system_settings",
        sa.Column("key", sa.String(255), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    # Seed default download size limit (5GB)
    op.execute(
        "INSERT INTO system_settings (key, value, description) VALUES "
        "('max_download_size_bytes', '5368709120', "
        "'Maximum file download size in bytes (applies to Telegram and stealer log connectors)')"
    )


def downgrade() -> None:
    op.drop_table("system_settings")
