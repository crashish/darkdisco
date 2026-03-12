"""Add telegram_intel and discord source types.

Revision ID: 002
Revises: 001
Create Date: 2026-03-12
"""

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add new values to the sourcetype enum
    op.execute("ALTER TYPE sourcetype ADD VALUE IF NOT EXISTS 'telegram_intel'")
    op.execute("ALTER TYPE sourcetype ADD VALUE IF NOT EXISTS 'discord'")


def downgrade() -> None:
    # PostgreSQL does not support removing enum values directly.
    # A full enum recreation would be needed, which is destructive.
    pass
