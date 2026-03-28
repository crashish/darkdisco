"""Backfill download_status in raw_mentions metadata and add functional index.

Mentions with file_name in metadata but no download_status were invisible to
the download worker. This migration:
1. Adds a functional index on metadata->>'download_status' for efficient queries.
2. Backfills download_status='pending' for mentions that have file metadata
   (file_name present) but no download_status and no s3_key (not yet downloaded).

Revision ID: 013
Revises: 012
"""

from alembic import op

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Functional index for download worker queries
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_raw_mentions_download_status
        ON raw_mentions ((metadata_ ->> 'download_status'))
        WHERE metadata_ ->> 'download_status' IS NOT NULL
        """
    )

    # Backfill: mentions with file metadata but no download_status and not yet downloaded
    op.execute(
        """
        UPDATE raw_mentions
        SET metadata_ = jsonb_set(
            COALESCE(metadata_, '{}'::jsonb),
            '{download_status}',
            '"pending"'
        )
        WHERE metadata_ ->> 'file_name' IS NOT NULL
          AND metadata_ ->> 'download_status' IS NULL
          AND metadata_ ->> 's3_key' IS NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_raw_mentions_download_status")
