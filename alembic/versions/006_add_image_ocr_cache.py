"""Add image_ocr_cache table for deduplicating OCR processing.

Revision ID: 006
Revises: 005
Create Date: 2026-03-17
"""

import sqlalchemy as sa
from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "image_ocr_cache",
        sa.Column("sha256", sa.String(64), primary_key=True),
        sa.Column("ocr_text", sa.Text, nullable=True),
        sa.Column("confidence", sa.Float, nullable=False, server_default="0.0"),
        sa.Column("engine", sa.String(32), nullable=False, server_default="easyocr"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("image_ocr_cache")
