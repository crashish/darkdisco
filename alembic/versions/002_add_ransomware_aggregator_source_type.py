"""Add ransomware_aggregator to sourcetype enum.

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
    op.execute("ALTER TYPE sourcetype ADD VALUE IF NOT EXISTS 'ransomware_aggregator'")


def downgrade() -> None:
    # PostgreSQL does not support removing values from enums.
    # The value will remain but is harmless if unused.
    pass
