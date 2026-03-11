"""Initial schema.

Revision ID: 001
Revises:
Create Date: 2026-03-11
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "001"
down_revision = None
branch_labels = None
depends_on = None

# Enum types used across tables
severity_enum = postgresql.ENUM(
    "critical", "high", "medium", "low", "info",
    name="severity",
    create_type=False,
)
findingstatus_enum = postgresql.ENUM(
    "new", "reviewing", "escalated", "resolved", "false_positive",
    name="findingstatus",
    create_type=False,
)
sourcetype_enum = postgresql.ENUM(
    "paste_site", "forum", "marketplace", "telegram", "breach_db",
    "ransomware_blog", "stealer_log", "other",
    name="sourcetype",
    create_type=False,
)
watchtermtype_enum = postgresql.ENUM(
    "institution_name", "domain", "bin_range", "executive_name",
    "routing_number", "keyword", "regex",
    name="watchtermtype",
    create_type=False,
)
userrole_enum = postgresql.ENUM(
    "analyst", "admin",
    name="userrole",
    create_type=False,
)


def upgrade() -> None:
    # Create enum types first
    severity_enum.create(op.get_bind(), checkfirst=True)
    findingstatus_enum.create(op.get_bind(), checkfirst=True)
    sourcetype_enum.create(op.get_bind(), checkfirst=True)
    watchtermtype_enum.create(op.get_bind(), checkfirst=True)
    userrole_enum.create(op.get_bind(), checkfirst=True)

    # --- clients ---
    op.create_table(
        "clients",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("contract_ref", sa.String(100), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- users (before findings, which references it) ---
    op.create_table(
        "users",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("username", sa.String(100), nullable=False),
        sa.Column("hashed_password", sa.String(255), nullable=False),
        sa.Column("role", userrole_enum, nullable=False, server_default="analyst"),
        sa.Column("disabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("last_login", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_users_username", "users", ["username"], unique=True)

    # --- institutions ---
    op.create_table(
        "institutions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("client_id", sa.String(36), sa.ForeignKey("clients.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("short_name", sa.String(50), nullable=True),
        sa.Column("charter_type", sa.String(50), nullable=True),
        sa.Column("state", sa.String(2), nullable=True),
        sa.Column("primary_domain", sa.String(255), nullable=True),
        sa.Column("additional_domains", postgresql.JSONB(), nullable=True),
        sa.Column("bin_ranges", postgresql.JSONB(), nullable=True),
        sa.Column("routing_numbers", postgresql.JSONB(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_institutions_client_id", "institutions", ["client_id"])
    op.create_index("ix_institutions_client_name", "institutions", ["client_id", "name"])

    # --- sources ---
    op.create_table(
        "sources",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("source_type", sourcetype_enum, nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("connector_class", sa.String(255), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("poll_interval_seconds", sa.Integer(), nullable=False, server_default=sa.text("3600")),
        sa.Column("last_polled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("config", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_sources_name", "sources", ["name"], unique=True)

    # --- watch_terms ---
    op.create_table(
        "watch_terms",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("institution_id", sa.String(36), sa.ForeignKey("institutions.id"), nullable=False),
        sa.Column("term_type", watchtermtype_enum, nullable=False),
        sa.Column("value", sa.String(500), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("case_sensitive", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_watch_terms_institution_id", "watch_terms", ["institution_id"])
    op.create_index("ix_watch_terms_inst_type", "watch_terms", ["institution_id", "term_type"])

    # --- findings ---
    op.create_table(
        "findings",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("institution_id", sa.String(36), sa.ForeignKey("institutions.id"), nullable=False),
        sa.Column("source_id", sa.String(36), sa.ForeignKey("sources.id"), nullable=True),
        sa.Column("severity", severity_enum, nullable=False, server_default="medium"),
        sa.Column("status", findingstatus_enum, nullable=False, server_default="new"),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("raw_content", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("matched_terms", postgresql.JSONB(), nullable=True),
        sa.Column("tags", postgresql.JSONB(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column("analyst_notes", sa.Text(), nullable=True),
        sa.Column("assigned_to", sa.String(36), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("reviewed_by", sa.String(36), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("discovered_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_findings_institution_id", "findings", ["institution_id"])
    op.create_index("ix_findings_source_id", "findings", ["source_id"])
    op.create_index("ix_findings_severity", "findings", ["severity"])
    op.create_index("ix_findings_status", "findings", ["status"])
    op.create_index("ix_findings_content_hash", "findings", ["content_hash"])
    op.create_index("ix_findings_status_severity", "findings", ["status", "severity"])
    op.create_index("ix_findings_institution_status", "findings", ["institution_id", "status"])
    op.create_index("ix_findings_discovered", "findings", ["discovered_at"])

    # --- finding_attachments ---
    op.create_table(
        "finding_attachments",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("finding_id", sa.String(36), sa.ForeignKey("findings.id"), nullable=False),
        sa.Column("filename", sa.String(255), nullable=False),
        sa.Column("content_type", sa.String(100), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("sha256_hash", sa.String(64), nullable=True),
        sa.Column("s3_key", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_finding_attachments_finding_id", "finding_attachments", ["finding_id"])

    # --- alert_rules ---
    op.create_table(
        "alert_rules",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("owner_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("institution_id", sa.String(36), sa.ForeignKey("institutions.id"), nullable=True),
        sa.Column("min_severity", severity_enum, nullable=False, server_default="high"),
        sa.Column("source_types", postgresql.JSONB(), nullable=True),
        sa.Column("keyword_filter", sa.String(500), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("notify_email", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("notify_slack", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("notify_webhook_url", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- notifications ---
    op.create_table(
        "notifications",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("user_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("alert_rule_id", sa.String(36), sa.ForeignKey("alert_rules.id"), nullable=True),
        sa.Column("finding_id", sa.String(36), sa.ForeignKey("findings.id"), nullable=True),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("read", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_notifications_user_id", "notifications", ["user_id"])


def downgrade() -> None:
    op.drop_table("notifications")
    op.drop_table("alert_rules")
    op.drop_table("finding_attachments")
    op.drop_table("findings")
    op.drop_table("watch_terms")
    op.drop_table("sources")
    op.drop_table("institutions")
    op.drop_table("users")
    op.drop_table("clients")

    # Drop enum types
    userrole_enum.drop(op.get_bind(), checkfirst=True)
    watchtermtype_enum.drop(op.get_bind(), checkfirst=True)
    sourcetype_enum.drop(op.get_bind(), checkfirst=True)
    findingstatus_enum.drop(op.get_bind(), checkfirst=True)
    severity_enum.drop(op.get_bind(), checkfirst=True)
