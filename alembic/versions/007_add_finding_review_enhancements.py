"""Add classification column to findings and finding_audit_log table.

Revision ID: 007
Revises: 006
Create Date: 2026-03-18
"""

import sqlalchemy as sa
from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add classification column to findings
    op.add_column("findings", sa.Column("classification", sa.String(100), nullable=True))

    # Create finding_audit_log table
    op.create_table(
        "finding_audit_log",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("finding_id", sa.String(36), sa.ForeignKey("findings.id"), nullable=False),
        sa.Column("action", sa.String(50), nullable=False),
        sa.Column("username", sa.String(100), nullable=True),
        sa.Column("field", sa.String(50), nullable=True),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index(
        "ix_finding_audit_log_finding_created",
        "finding_audit_log",
        ["finding_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_table("finding_audit_log")
    op.drop_column("findings", "classification")
