"""Add extracted_files table for normalized archive file storage with FTS.

Revision ID: 003
Revises: 002
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TSVECTOR

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "extracted_files",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("mention_id", sa.String(36), sa.ForeignKey("raw_mentions.id"), nullable=False),
        sa.Column("filename", sa.String(1024), nullable=False),
        sa.Column("s3_key", sa.String(512)),
        sa.Column("sha256", sa.String(64)),
        sa.Column("size", sa.BigInteger()),
        sa.Column("extension", sa.String(32)),
        sa.Column("is_text", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("text_content", sa.Text()),
        sa.Column(
            "content_tsvector",
            TSVECTOR,
            sa.Computed("to_tsvector('english', COALESCE(text_content, ''))"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_index("ix_extracted_files_mention_id", "extracted_files", ["mention_id"])
    op.create_index("ix_extracted_files_sha256", "extracted_files", ["sha256"])
    op.create_index("ix_extracted_files_mention_filename", "extracted_files", ["mention_id", "filename"])
    op.create_index(
        "ix_extracted_files_content_fts",
        "extracted_files",
        ["content_tsvector"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_table("extracted_files")
