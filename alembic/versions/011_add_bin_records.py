"""Add bin_records table for BIN database integration.

Revision ID: 011
Revises: 010
"""

from alembic import op
import sqlalchemy as sa

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade():
    card_brand_enum = sa.Enum(
        "visa", "mastercard", "amex", "discover", "jcb", "unionpay", "diners", "maestro", "other",
        name="cardbrand",
    )
    card_type_enum = sa.Enum(
        "credit", "debit", "prepaid", "charge", "unknown",
        name="cardtype",
    )

    op.create_table(
        "bin_records",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("bin_prefix", sa.String(8), nullable=False, index=True),
        sa.Column("bin_range_start", sa.String(8), nullable=True),
        sa.Column("bin_range_end", sa.String(8), nullable=True),
        sa.Column("issuer_name", sa.String(255), nullable=True),
        sa.Column("card_brand", card_brand_enum, nullable=True),
        sa.Column("card_type", card_type_enum, nullable=True),
        sa.Column("card_level", sa.String(50), nullable=True),
        sa.Column("country_code", sa.String(3), nullable=True),
        sa.Column("country_name", sa.String(100), nullable=True),
        sa.Column("bank_url", sa.String(500), nullable=True),
        sa.Column("bank_phone", sa.String(50), nullable=True),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_index("ix_bin_records_prefix", "bin_records", ["bin_prefix"])
    op.create_index("ix_bin_records_range", "bin_records", ["bin_range_start", "bin_range_end"])
    op.create_index("ix_bin_records_issuer", "bin_records", ["issuer_name"])
    op.create_index("ix_bin_records_brand", "bin_records", ["card_brand"])


def downgrade():
    op.drop_table("bin_records")
    op.execute("DROP TYPE IF EXISTS cardbrand")
    op.execute("DROP TYPE IF EXISTS cardtype")
