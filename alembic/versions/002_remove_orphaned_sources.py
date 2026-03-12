"""Remove orphaned sources with nonexistent connector paths.

Sources pointing to ``darkdisco.connectors.*`` reference a module that was
never shipped.  The real connectors live under
``darkdisco.discovery.connectors.*``.  Delete the orphans outright — the seed
script will recreate any that are still wanted with correct paths.

Revision ID: 002
Revises: 001
Create Date: 2026-03-12
"""

from alembic import op
import sqlalchemy as sa

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Delete sources whose connector_class points to the non-existent
    # darkdisco.connectors.* module (NOT darkdisco.discovery.connectors.*).
    op.execute(
        sa.text(
            "DELETE FROM sources "
            "WHERE connector_class LIKE 'darkdisco.connectors.%'"
        )
    )


def downgrade() -> None:
    # Data migration — cannot restore deleted rows.
    pass
