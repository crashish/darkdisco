"""Add report_schedules and generated_reports tables.

Revision ID: 010
Revises: 009
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "report_schedules",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("template_id", sa.String(36), sa.ForeignKey("report_templates.id"), nullable=False),
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("cron_expression", sa.String(100), nullable=True),
        sa.Column("interval_seconds", sa.Integer(), nullable=True),
        sa.Column(
            "date_range_mode",
            sa.Enum("last_24h", "last_7d", "last_30d", "last_quarter", "custom", name="daterangemode"),
            server_default="last_7d",
            nullable=False,
        ),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "delivery_method",
            sa.Enum("s3_store", "email", "both", name="deliverymethod"),
            server_default="s3_store",
            nullable=False,
        ),
        sa.Column("recipients", JSONB, nullable=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_report_schedules_next_run", "report_schedules", ["next_run_at"])
    op.create_index("ix_report_schedules_owner", "report_schedules", ["owner_id"])

    op.create_table(
        "generated_reports",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("schedule_id", sa.String(36), sa.ForeignKey("report_schedules.id"), nullable=True),
        sa.Column("template_id", sa.String(36), sa.ForeignKey("report_templates.id"), nullable=True),
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("s3_key", sa.String(512), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=True),
        sa.Column("date_range_mode", sa.String(50), nullable=True),
        sa.Column("date_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("date_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(20), server_default="completed", nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_generated_reports_owner", "generated_reports", ["owner_id"])
    op.create_index("ix_generated_reports_schedule", "generated_reports", ["schedule_id"])
    op.create_index("ix_generated_reports_created", "generated_reports", ["created_at"])


def downgrade() -> None:
    op.drop_table("generated_reports")
    op.drop_table("report_schedules")
    op.execute("DROP TYPE IF EXISTS daterangemode")
    op.execute("DROP TYPE IF EXISTS deliverymethod")
