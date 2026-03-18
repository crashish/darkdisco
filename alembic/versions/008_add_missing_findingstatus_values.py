"""Add confirmed and dismissed values to findingstatus enum.

Revision ID: 008
Revises: 007
Create Date: 2026-03-18
"""

from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE findingstatus ADD VALUE IF NOT EXISTS 'confirmed'")
    op.execute("ALTER TYPE findingstatus ADD VALUE IF NOT EXISTS 'dismissed'")


def downgrade() -> None:
    # PostgreSQL does not support removing values from enums.
    # A full enum replacement would be needed; skipping for safety.
    pass
