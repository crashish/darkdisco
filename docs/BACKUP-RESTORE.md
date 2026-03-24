# Database Backup & Restore

## How It Works

The `db-backup` service runs as part of docker-compose. It:

1. Runs `pg_dump` against the PostgreSQL database
2. Compresses the dump with gzip
3. Uploads to the `darkdisco-backups` S3 bucket (MinIO locally, S3 in production)
4. Rotates backups older than `BACKUP_RETENTION_DAYS` (default: 7 days)

**Schedule:** Every hour (on the hour). An initial backup runs immediately on container start.

Backups are stored as: `s3://darkdisco-backups/db-backups/darkdisco-YYYYMMDDTHHMMSSz.sql.gz`

## Configuration

Environment variables (set in `.env` or docker-compose override):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_PASSWORD` | `darkdisco` | Database password |
| `MINIO_PASSWORD` | `darkdisco-secret` | MinIO/S3 secret key |
| `BACKUP_RETENTION_DAYS` | `7` | Days to keep backups |

### Using Real S3 Instead of MinIO

Override these environment variables for the `db-backup` service:

```yaml
# docker-compose.override.yml
services:
  db-backup:
    environment:
      S3_ENDPOINT: https://s3.amazonaws.com
      S3_ACCESS_KEY: ${AWS_ACCESS_KEY_ID}
      S3_SECRET_KEY: ${AWS_SECRET_ACCESS_KEY}
      S3_BUCKET: your-backup-bucket
```

## Manual Backup

```bash
docker compose exec db-backup /usr/local/bin/backup.sh
```

## List Backups

```bash
# Via MinIO (local)
docker compose exec db-backup aws s3 ls s3://darkdisco-backups/db-backups/ \
    --endpoint-url http://minio:9000

# Via AWS S3 (production)
aws s3 ls s3://your-backup-bucket/db-backups/
```

## Restore

### 1. Download the backup

```bash
# From MinIO (local)
docker compose exec db-backup aws s3 cp \
    s3://darkdisco-backups/db-backups/darkdisco-20260324T050000Z.sql.gz \
    /tmp/restore.sql.gz --endpoint-url http://minio:9000

# From S3 (production)
aws s3 cp s3://your-bucket/db-backups/darkdisco-20260324T050000Z.sql.gz /tmp/restore.sql.gz
```

### 2. Restore to the database

```bash
# Decompress and restore (WARNING: this drops and recreates all tables)
docker compose exec db-backup bash -c \
    'gunzip -c /tmp/restore.sql.gz | psql -h postgres -U darkdisco -d darkdisco'
```

### 3. Alternative: restore from host

```bash
# Copy backup to host first
docker compose cp db-backup:/tmp/restore.sql.gz ./restore.sql.gz

# Restore via the postgres container
gunzip -c restore.sql.gz | docker compose exec -T postgres \
    psql -U darkdisco -d darkdisco
```

## Monitoring

Check backup logs:

```bash
docker compose logs db-backup
```

The backup script logs timestamps, file sizes, and rotation actions. Look for:
- `[backup] ... Starting backup` — backup initiated
- `[backup] Dump complete: ... (SIZE)` — dump succeeded
- `[backup] Uploaded to s3://...` — upload succeeded
- `[backup] Deleting expired: ...` — old backup rotated
- `[backup] ... Backup complete` — full cycle done
