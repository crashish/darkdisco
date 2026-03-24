#!/bin/bash
# Automated PostgreSQL backup to S3/MinIO
# Runs pg_dump, uploads to S3, rotates backups older than BACKUP_RETENTION_DAYS
set -euo pipefail

: "${PGHOST:=postgres}"
: "${PGPORT:=5432}"
: "${PGUSER:=darkdisco}"
: "${PGDATABASE:=darkdisco}"
: "${S3_ENDPOINT:=http://minio:9000}"
: "${S3_BUCKET:=darkdisco-backups}"
: "${S3_ACCESS_KEY:=darkdisco}"
: "${S3_SECRET_KEY:=darkdisco-secret}"
: "${BACKUP_RETENTION_DAYS:=7}"

export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"

TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
BACKUP_FILE="darkdisco-${TIMESTAMP}.sql.gz"
S3_KEY="db-backups/${BACKUP_FILE}"
TMPDIR=$(mktemp -d)
LOCAL_PATH="${TMPDIR}/${BACKUP_FILE}"

cleanup() {
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "[backup] $(date -u +%FT%TZ) Starting backup → s3://${S3_BUCKET}/${S3_KEY}"

# Ensure bucket exists
aws s3 mb "s3://${S3_BUCKET}" --endpoint-url "$S3_ENDPOINT" 2>/dev/null || true

# Dump and compress
pg_dump -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    --no-owner --no-acl --clean --if-exists | gzip > "$LOCAL_PATH"

SIZE=$(du -h "$LOCAL_PATH" | cut -f1)
echo "[backup] Dump complete: ${BACKUP_FILE} (${SIZE})"

# Upload to S3
aws s3 cp "$LOCAL_PATH" "s3://${S3_BUCKET}/${S3_KEY}" \
    --endpoint-url "$S3_ENDPOINT" --quiet

echo "[backup] Uploaded to s3://${S3_BUCKET}/${S3_KEY}"

# Rotate: delete backups older than retention period
CUTOFF=$(date -u -d "${BACKUP_RETENTION_DAYS} days ago" +%Y%m%dT%H%M%SZ 2>/dev/null || \
         date -u -v-${BACKUP_RETENTION_DAYS}d +%Y%m%dT%H%M%SZ 2>/dev/null || echo "")

if [ -n "$CUTOFF" ]; then
    echo "[backup] Rotating backups older than ${BACKUP_RETENTION_DAYS} days (before ${CUTOFF})"
    aws s3 ls "s3://${S3_BUCKET}/db-backups/" --endpoint-url "$S3_ENDPOINT" 2>/dev/null | \
        awk '{print $4}' | grep '^darkdisco-' | while read -r file; do
            # Extract timestamp from filename: darkdisco-YYYYMMDDTHHMMSSz.sql.gz
            file_ts=$(echo "$file" | sed 's/darkdisco-\(.*\)\.sql\.gz/\1/')
            if [ "$file_ts" \< "$CUTOFF" ]; then
                echo "[backup] Deleting expired: ${file}"
                aws s3 rm "s3://${S3_BUCKET}/db-backups/${file}" \
                    --endpoint-url "$S3_ENDPOINT" --quiet
            fi
        done
fi

echo "[backup] $(date -u +%FT%TZ) Backup complete"
