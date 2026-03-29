"""Backfill download_status=pending for .txt mentions stuck with null status.

66k raw_mentions reference .txt files (text/plain) but have no download_status
set — they were silently dropped by the Telegram connector's size limit without
being queued. This backfill marks them as pending so download_pending_files
picks them up.

Migration 013 attempted the same backfill but used the wrong column name
(metadata_ instead of metadata), so those records were never updated.

Revision ID: 014
Revises: 013
"""

from alembic import op

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Backfill download_status='pending' for .txt file mentions that have
    # file_size metadata but were never queued for download.
    op.execute(
        """
        UPDATE raw_mentions
        SET metadata = jsonb_set(
            COALESCE(metadata, '{}'::jsonb),
            '{download_status}',
            '"pending"'
        )
        WHERE metadata ->> 'file_name' LIKE '%.txt'
          AND metadata ->> 'download_status' IS NULL
          AND metadata ->> 'file_size' IS NOT NULL
        """
    )


def downgrade() -> None:
    # Remove download_status from .txt mentions that were backfilled.
    # Only revert records that are still 'pending' (not yet processed).
    op.execute(
        """
        UPDATE raw_mentions
        SET metadata = metadata - 'download_status'
        WHERE metadata ->> 'file_name' LIKE '%.txt'
          AND metadata ->> 'download_status' = 'pending'
        """
    )
