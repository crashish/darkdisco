"""Add trapline to sourcetype enum.

Revision ID: 005
Revises: 004
Create Date: 2026-03-12
"""

from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE sourcetype ADD VALUE IF NOT EXISTS 'trapline'")


def downgrade() -> None:
    # PostgreSQL does not support removing enum values; this is a no-op.
    pass
