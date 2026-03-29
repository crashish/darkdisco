"""Celery pipeline worker — poll sources, match watch terms, create findings, trigger alerts.

This is the backbone that all connectors feed into. The beat schedule drives
periodic polling; each poll fans out into matching and finding creation.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import logging
from datetime import datetime, timedelta, timezone

from celery import Celery
from sqlalchemy import select
from sqlalchemy.orm import Session

from darkdisco.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Persistent per-process event loop (avoids "Event loop is closed" /
# "Future attached to a different loop" errors with httpx/asyncpg pools)
# ---------------------------------------------------------------------------

def _get_or_create_loop():
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


# ---------------------------------------------------------------------------
# Celery app
# ---------------------------------------------------------------------------

app = Celery("darkdisco")
app.conf.update(
    broker_url=settings.celery_broker_url,
    result_backend=settings.celery_result_backend,
    task_soft_time_limit=settings.celery_task_soft_time_limit,
    task_time_limit=settings.celery_task_time_limit,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Beat schedule — kick off source polling every 5 minutes
    beat_schedule={
        "schedule-source-polls": {
            "task": "darkdisco.pipeline.worker.schedule_polls",
            "schedule": 300.0,  # 5 minutes
        },
        "download-pending-files": {
            "task": "darkdisco.pipeline.worker.download_pending_files",
            "schedule": 120.0,  # 2 minutes
        },
        "backfill-ocr": {
            "task": "darkdisco.pipeline.worker.backfill_ocr",
            "schedule": 300.0,  # 5 minutes
        },
        "sync-trapline-watchlist": {
            "task": "darkdisco.pipeline.worker.sync_trapline_watchlist",
            "schedule": float(settings.trapline_sync_interval),
        },
        "process-channel-discoveries": {
            "task": "darkdisco.pipeline.worker.process_channel_discoveries",
            "schedule": 600.0,  # 10 minutes
        },
        "run-scheduled-reports": {
            "task": "darkdisco.pipeline.worker.run_scheduled_reports",
            "schedule": 60.0,  # 1 minute
        },
    },
)

# ---------------------------------------------------------------------------
# Synchronous DB helpers (Celery workers are sync)
# ---------------------------------------------------------------------------

_sync_engine = None


def _get_sync_engine():
    """Lazily create a sync SQLAlchemy engine for Celery workers."""
    global _sync_engine
    if _sync_engine is None:
        from sqlalchemy import create_engine

        # Convert asyncpg URL to psycopg2 for sync access
        sync_url = settings.database_url.replace("+asyncpg", "+psycopg2")
        _sync_engine = create_engine(sync_url, pool_pre_ping=True)
    return _sync_engine


def _get_sync_session() -> Session:
    from sqlalchemy.orm import sessionmaker

    return sessionmaker(bind=_get_sync_engine())()


# ---------------------------------------------------------------------------
# Connector loading
# ---------------------------------------------------------------------------

_CONNECTOR_MAP = {
    "paste_site": "darkdisco.discovery.connectors.paste_site:PasteSiteConnector",
    "forum": "darkdisco.discovery.connectors.forum:ForumConnector",
    "telegram": "darkdisco.discovery.connectors.telegram:TelegramConnector",
    "telegram_intel": "darkdisco.discovery.connectors.telegram:TelegramConnector",
    "discord": "darkdisco.discovery.connectors.discord:DiscordConnector",
    "breach_db": "darkdisco.discovery.connectors.breach_db:BreachDBConnector",
    "ransomware_blog": "darkdisco.discovery.connectors.ransomware_blog:RansomwareBlogConnector",
    "ransomware_aggregator": "darkdisco.discovery.connectors.ransomware_aggregator:RansomwareAggregatorConnector",
    "stealer_log": "darkdisco.discovery.connectors.stealer_log:StealerLogConnector",
    "ct_monitor": "darkdisco.discovery.connectors.ct_monitor:CTMonitorConnector",
    "urlscan": "darkdisco.discovery.connectors.urlscan:URLScanConnector",
    "phishtank": "darkdisco.discovery.connectors.phishtank:PhishTankConnector",
    "trapline": "darkdisco.discovery.connectors.trapline:TraplineConnector",
}


def _load_connector(source):
    """Instantiate the connector for a Source row."""
    # Prefer explicit connector_class on the source, fall back to type map
    class_path = source.connector_class or _CONNECTOR_MAP.get(source.source_type.value)
    if not class_path:
        raise ValueError(f"No connector for source type {source.source_type}")

    module_path, class_name = class_path.rsplit(":", 1) if ":" in class_path else class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(config=source.config or {})


def _create_session_copy(source, suffix: str) -> dict:
    """Create a copy of the primary Telegram session file with the given suffix.

    Returns a config dict with session_name pointing to the copy.
    This allows multiple Telethon clients to run concurrently without
    SQLite lock contention on the shared session file.
    """
    import shutil
    from pathlib import Path
    from darkdisco.config import settings

    config = dict(source.config or {})

    primary_session = config.get("session_name", settings.telegram_session_name)
    primary_path = Path(primary_session).expanduser()
    copy_session = str(primary_path) + f"_{suffix}"
    copy_path = Path(copy_session + ".session")
    primary_session_file = Path(str(primary_path) + ".session")

    # Refresh copy from primary if primary is newer.
    # The copy needs updated entity cache to resolve channels
    # that were joined after it was first created.
    if primary_session_file.exists():
        if not copy_path.exists():
            shutil.copy2(str(primary_session_file), str(copy_path))
            logger.info("Initialized %s session from %s", suffix, copy_path)
        elif primary_session_file.stat().st_mtime > copy_path.stat().st_mtime + 3600:
            # Primary is >1 hour newer — refresh the session copy
            shutil.copy2(str(primary_session_file), str(copy_path))
            logger.info("Refreshed %s session from primary (entity cache update)", suffix)

    config["session_name"] = copy_session
    return config


def _load_connector_with_config(source, config: dict):
    """Instantiate the connector for a Source row with overridden config."""
    class_path = source.connector_class or _CONNECTOR_MAP.get(source.source_type.value)
    if not class_path:
        raise ValueError(f"No connector for source type {source.source_type}")
    module_path, class_name = class_path.rsplit(":", 1) if ":" in class_path else class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(config=config)


def _load_connector_for_poll(source):
    """Load a Telegram connector with a per-source session file for polling.

    Each source gets its own session copy (e.g., data/darkdisco_monitor_poll_<id>)
    so multiple Telegram sources can poll concurrently without SQLite lock contention.
    """
    source_suffix = f"poll_{source.id[:8]}"
    config = _create_session_copy(source, source_suffix)
    return _load_connector_with_config(source, config)


def _load_connector_for_download(source):
    """Load a Telegram connector with a separate session file for downloads.

    Copies the primary session file to a _download variant so the download task
    doesn't contend with the polling task over SQLite locks.
    """
    config = _create_session_copy(source, "download")
    return _load_connector_with_config(source, config)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(name="darkdisco.pipeline.worker.schedule_polls")
def schedule_polls():
    """Beat-driven: find sources due for polling and dispatch per-source tasks."""
    from darkdisco.common.models import Source

    session = _get_sync_session()
    try:
        now = datetime.now(timezone.utc)
        sources = session.execute(
            select(Source).where(Source.enabled.is_(True))
        ).scalars().all()

        dispatched = 0
        for source in sources:
            # Check if source is due for polling
            if source.last_polled_at is not None:
                elapsed = (now - source.last_polled_at).total_seconds()
                if elapsed < source.poll_interval_seconds:
                    continue

            poll_source.delay(source.id)
            dispatched += 1

        logger.info("Scheduled %d source polls out of %d enabled sources", dispatched, len(sources))
        return {"dispatched": dispatched, "total_enabled": len(sources)}
    finally:
        session.close()


@app.task(name="darkdisco.pipeline.worker.poll_source", bind=True, max_retries=3,
          soft_time_limit=600, time_limit=900)
def poll_source(self, source_id: str):
    """Poll a single source for new mentions, then fan out to matching."""
    from darkdisco.common.models import Source

    session = _get_sync_session()
    try:
        source = session.get(Source, source_id)
        if source is None:
            logger.error("Source %s not found", source_id)
            return {"error": "source_not_found"}

        if not source.enabled:
            logger.info("Source %s is disabled, skipping", source.name)
            return {"skipped": True}

        since = source.last_polled_at

        # For Telegram sources, use a per-source session copy and per-source
        # lock so multiple sources can poll concurrently.
        telegram_lock = None
        if source.source_type.value in ("telegram", "telegram_intel"):
            connector = _load_connector_for_poll(source)
            import redis as _redis
            from darkdisco.config import settings as _settings
            _r = _redis.from_url(_settings.celery_broker_url)
            telegram_lock = _r.lock(
                f"darkdisco:telegram_poll_lock:{source_id}", timeout=300,
            )
            acquired = telegram_lock.acquire(blocking=True, blocking_timeout=180)
            if not acquired:
                logger.warning("Could not acquire Telegram poll lock for %s, retrying later", source.name)
                raise self.retry(countdown=30, max_retries=5)
        else:
            connector = _load_connector(source)

        try:
            # Bridge async connector to sync Celery task
            mentions = _get_or_create_loop().run_until_complete(_poll_async(connector, since))
        except Exception as exc:
            source.last_error = str(exc)[:2000]
            session.commit()
            logger.exception("Failed to poll source %s", source.name)
            if telegram_lock:
                try: telegram_lock.release()
                except Exception: pass
            raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))

        # Persist connector state (high-water marks) back to source config
        if hasattr(connector, 'config') and connector.config:
            source.config = {**(source.config or {}), **connector.config}

        # Update poll timestamp and clear errors
        source.last_polled_at = datetime.now(timezone.utc)
        source.last_error = None
        session.commit()

        logger.info("Polled source %s: %d mentions", source.name, len(mentions))

        # Extract t.me channel links from mention content for auto-discovery
        if mentions:
            _extract_channel_discoveries(session, source_id, mentions)

        if mentions:
            # Skip inline OCR/file processing — it takes 30-60min on CPU and
            # blocks the entire poll. Dispatch to matching immediately; OCR
            # runs later via backfill_ocr task on persisted extracted_files.
            serialized = [
                {
                    "source_name": m.source_name,
                    "source_url": m.source_url,
                    "title": m.title,
                    "content": m.content,
                    "author": m.author,
                    "discovered_at": m.discovered_at.isoformat() if m.discovered_at else None,
                    "metadata": {
                        k: v for k, v in m.metadata.items()
                        if k != "file_data"  # raw bytes not JSON-serializable
                    },
                }
                for m in mentions
            ]
            run_matching.delay(source_id, serialized)

        return {"source": source.name, "mentions": len(mentions)}
    finally:
        if telegram_lock:
            try: telegram_lock.release()
            except Exception: pass
        session.close()


async def _poll_async(connector, since):
    """Run the async connector poll in a fresh event loop."""
    try:
        await connector.setup()
        return await connector.poll(since=since)
    finally:
        await connector.teardown()


def _ocr_with_dedup(image_data: bytes, filename: str, image_sha256: str):
    """Run OCR with dedup — check cache first, store result after.

    Actors frequently repost the same screenshots across channels.
    By caching OCR results keyed on image SHA-256, we avoid redundant
    processing of identical images.
    """
    from darkdisco.pipeline.ocr import OCRResult, extract_text_from_image

    # Check cache
    try:
        session = _get_sync_session()
        try:
            from darkdisco.common.models import ImageOCRCache
            cached = session.get(ImageOCRCache, image_sha256)
            if cached is not None:
                logger.debug("OCR cache hit for %s (%s)", filename, image_sha256[:12])
                return OCRResult(
                    text=cached.ocr_text or "",
                    confidence=cached.confidence,
                    engine=cached.engine,
                )
        finally:
            session.close()
    except Exception:
        logger.debug("OCR cache lookup failed, proceeding with OCR", exc_info=True)

    # Cache miss — run OCR
    ocr_result = extract_text_from_image(image_data, filename)

    # Store result in cache
    if ocr_result is not None:
        try:
            session = _get_sync_session()
            try:
                from darkdisco.common.models import ImageOCRCache
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                stmt = pg_insert(ImageOCRCache).values(
                    sha256=image_sha256,
                    ocr_text=ocr_result.text,
                    confidence=ocr_result.confidence,
                    engine=ocr_result.engine,
                ).on_conflict_do_nothing(index_elements=["sha256"])
                session.execute(stmt)
                session.commit()
            finally:
                session.close()
        except Exception:
            logger.debug("Failed to cache OCR result", exc_info=True)

    return ocr_result


def _process_file_mentions(mentions: list) -> list:
    """Process file attachments in mentions: extract archives, analyze contents, run OCR.

    For each mention with file_data in metadata:
    1. If it's an archive (ZIP/RAR), extract and analyze contents
    2. If it's an image, run OCR to extract text
    3. Append extracted text to the mention content for matching
    4. Upload original + extracted files to S3
    5. Store file analysis metadata
    """
    from darkdisco.pipeline.files import (
        analyze_extracted_files,
        extract_archive,
        extract_passwords,
        is_archive,
        upload_to_s3,
    )
    from darkdisco.pipeline.ocr import (
        is_image,
        is_image_media_type,
    )

    for mention in mentions:
        file_data = mention.metadata.get("file_data")
        if not file_data or not isinstance(file_data, bytes):
            continue

        filename = mention.metadata.get("file_name") or "unknown"
        file_sha256 = hashlib.sha256(file_data).hexdigest()

        # Upload original file to S3
        s3_key = f"files/{file_sha256[:8]}/{filename}"
        if upload_to_s3(s3_key, file_data):
            mention.metadata["s3_key"] = s3_key
        mention.metadata["file_sha256"] = file_sha256

        if is_archive(filename):
            # Extract passwords from the message text
            passwords = extract_passwords(mention.content or "")
            mention.metadata["extracted_passwords"] = passwords

            try:
                extracted = extract_archive(file_data, filename, passwords)
            except Exception:
                logger.exception("Archive extraction failed for %s", filename)
                extracted = []

            if extracted:
                analysis = analyze_extracted_files(extracted)
                mention.metadata["file_analysis"] = analysis.to_dict()

                # Store per-file text content for accurate finding attribution
                per_file_texts = []
                for ef in extracted:
                    if ef.is_text and ef.content:
                        try:
                            text = ef.content.decode("utf-8", errors="replace") if isinstance(ef.content, bytes) else ef.content
                        except Exception:
                            continue
                        if text.strip():
                            per_file_texts.append({"filename": ef.filename, "content": text})
                if per_file_texts:
                    mention.metadata["extracted_file_contents"] = per_file_texts

                # Store full inventory of ALL extracted files (text + non-text)
                # so _store_extracted_files can create ExtractedFile rows for every file
                inventory = []
                for ef in extracted:
                    text_content = None
                    if ef.is_text and ef.content:
                        try:
                            text_content = ef.content.decode("utf-8", errors="replace") if isinstance(ef.content, bytes) else ef.content
                        except Exception:
                            text_content = None
                    inventory.append({
                        "filename": ef.filename,
                        "sha256": ef.sha256,
                        "size": ef.size,
                        "is_text": ef.is_text,
                        "content": text_content or "",
                    })
                mention.metadata["extracted_files_inventory"] = inventory

                # Append extracted text to mention content for matching (per-file separators)
                if per_file_texts:
                    parts = [mention.content or ""]
                    for pf in per_file_texts:
                        parts.append(f"\n\n--- Extracted file: {pf['filename']} ---\n\n{pf['content']}")
                    mention.content = "".join(parts)
                elif analysis.text_content:
                    separator = "\n\n--- Extracted from archive ---\n\n"
                    mention.content = (mention.content or "") + separator + analysis.text_content

                # Upload extracted files to S3
                for ef in extracted:
                    ef_s3_key = f"files/{file_sha256[:8]}/extracted/{ef.sha256[:8]}/{ef.filename}"
                    upload_to_s3(ef_s3_key, ef.content)

                logger.info(
                    "Extracted %d files from %s (%d text, %d credential indicators)",
                    len(extracted), filename,
                    sum(1 for f in extracted if f.is_text),
                    len(analysis.credential_indicators),
                )

                # If credentials found, boost severity hint in metadata
                if analysis.credential_indicators:
                    mention.metadata["has_credentials"] = True
                    mention.metadata["credential_count"] = len(analysis.credential_indicators)
        else:
            # Non-archive file — if it's a text file, include content
            if filename.lower().endswith((".txt", ".csv", ".log", ".json", ".sql")):
                try:
                    for enc in ("utf-8", "latin-1", "cp1251"):
                        try:
                            text = file_data.decode(enc)
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        text = file_data.decode("utf-8", errors="replace")

                    if text:
                        separator = f"\n\n--- File: {filename} ---\n\n"
                        mention.content = (mention.content or "") + separator + text[:200_000]
                except Exception:
                    pass

            # OCR processing for image files
            elif is_image(filename) or is_image_media_type(mention.metadata.get("media_type")):
                ocr_result = _ocr_with_dedup(file_data, filename, file_sha256)
                if ocr_result and ocr_result.has_text:
                    mention.metadata["ocr_text"] = ocr_result.text
                    mention.metadata["ocr_confidence"] = ocr_result.confidence
                    mention.metadata["ocr_engine"] = ocr_result.engine

                    # Append OCR text to mention content for watch term matching
                    separator = f"\n\n--- OCR text from {filename} ---\n\n"
                    mention.content = (mention.content or "") + separator + ocr_result.text

    return mentions


def _attributed_raw_content(mention, matched_terms: list[dict]) -> str:
    """Return raw_content attributed to the specific extracted file(s) that matched.

    If the mention has extracted_file_contents in metadata, find which inner file(s)
    contain the matched term values and return only those files' content with filename
    headers. Falls back to the full mention.content if no per-file data or no match
    found in individual files (e.g., match was in the original message text).
    """
    extracted_files = mention.metadata.get("extracted_file_contents")
    if not extracted_files:
        return mention.content

    # Collect the term values to search for
    term_values = [t["value"] for t in matched_terms]

    # Check if the original message (before extraction) contains any match
    # The original content is everything before the first "--- Extracted file:" separator
    original_content = (mention.content or "").split("\n\n--- Extracted file:", 1)[0]

    matching_files = []
    for ef in extracted_files:
        searchable = ef["content"].lower()
        for val in term_values:
            if val.lower() in searchable:
                matching_files.append(ef)
                break

    if not matching_files:
        # Match was in original message text or via regex — return original content
        return original_content or mention.content

    # Build attributed content: original message + only the matching file(s)
    parts = []
    if original_content.strip():
        parts.append(original_content)
    for mf in matching_files:
        parts.append(f"\n\n--- Extracted file: {mf['filename']} ---\n\n{mf['content']}")
    return "".join(parts)


def _store_extracted_files(session: Session, mention_id: str, metadata: dict) -> int:
    """Create ExtractedFile rows from mention metadata.

    Uses extracted_files_inventory (all files) if available, falling back
    to extracted_file_contents (text-only, legacy) for backwards compat.

    Returns the number of rows created.
    """
    from darkdisco.common.models import ExtractedFile

    file_sha256 = metadata.get("file_sha256", "")

    # Prefer full inventory (all files including non-text)
    inventory = metadata.get("extracted_files_inventory")
    if inventory and isinstance(inventory, list):
        count = 0
        for ef in inventory:
            filename = ef.get("filename", "")
            sha = ef.get("sha256", "")
            is_text = ef.get("is_text", False)
            content = ef.get("content", "") if is_text else ""

            ext = ""
            if "." in filename:
                ext = filename.rsplit(".", 1)[-1].lower()

            s3_key = None
            if file_sha256 and filename and sha:
                s3_key = f"files/{file_sha256[:8]}/extracted/{sha[:8]}/{filename}"
            elif file_sha256 and filename:
                s3_key = f"files/{file_sha256[:8]}/extracted/{filename}"

            row = ExtractedFile(
                mention_id=mention_id,
                filename=filename,
                s3_key=s3_key,
                sha256=sha or None,
                size=ef.get("size") or (len(content.encode("utf-8")) if content else 0),
                extension=ext or None,
                is_text=is_text,
                text_content=content if content else None,
            )
            session.add(row)
            count += 1
        return count

    # Legacy fallback: extracted_file_contents (text files only)
    extracted = metadata.get("extracted_file_contents")
    if not extracted or not isinstance(extracted, list):
        return 0

    count = 0
    for ef in extracted:
        filename = ef.get("filename", "")
        content = ef.get("content", "")
        ext = ""
        if "." in filename:
            ext = filename.rsplit(".", 1)[-1].lower()

        s3_key = None
        sha = ef.get("sha256")
        if file_sha256 and filename:
            if sha:
                s3_key = f"files/{file_sha256[:8]}/extracted/{sha[:8]}/{filename}"
            else:
                s3_key = f"files/{file_sha256[:8]}/extracted/{filename}"

        is_text = bool(content)

        row = ExtractedFile(
            mention_id=mention_id,
            filename=filename,
            s3_key=s3_key,
            sha256=sha,
            size=len(content.encode("utf-8")) if content else ef.get("size"),
            extension=ext or None,
            is_text=is_text,
            text_content=content if content else None,
        )
        session.add(row)
        count += 1

    return count


@app.task(name="darkdisco.pipeline.worker.run_matching",
          soft_time_limit=1800, time_limit=2400)
def run_matching(source_id: str, raw_mentions: list[dict]):
    """Match raw mentions against all active watch terms, enrich, filter, create findings."""
    from darkdisco.common.models import Finding, Source, WatchTerm
    from darkdisco.common.models import RawMention as RawMentionModel
    from darkdisco.discovery.connectors.base import RawMention
    from darkdisco.discovery.matcher import WatchTermIndex, recompute_highlights
    from darkdisco.enrichment import enrich_and_filter

    session = _get_sync_session()
    try:
        source = session.get(Source, source_id)
        if source is None:
            logger.error("Source %s not found for matching", source_id)
            return {"error": "source_not_found"}

        # ------------------------------------------------------------------
        # Phase 1: Persist ALL raw mentions unconditionally.
        # This must happen before watch-term checks or finding-dedup so that
        # polled data is never silently lost.
        # ------------------------------------------------------------------
        import time as _time
        phase1_start = _time.monotonic()

        # Deserialize all mentions and compute hashes up-front
        mentions_with_hashes: list[tuple[RawMention, str]] = []
        for raw in raw_mentions:
            mention = RawMention(
                source_name=raw["source_name"],
                source_url=raw.get("source_url"),
                title=raw.get("title") or "",
                content=raw.get("content") or "",
                author=raw.get("author"),
                discovered_at=datetime.fromisoformat(raw["discovered_at"]) if raw.get("discovered_at") else datetime.now(timezone.utc),
                metadata=raw.get("metadata") or {},
            )
            content_hash = hashlib.sha256(
                f"{mention.source_name}:{mention.content}".encode()
            ).hexdigest()
            mentions_with_hashes.append((mention, content_hash))

        # Batch dedup: fetch all existing raw mention hashes in one query
        all_hashes = [h for _, h in mentions_with_hashes]
        existing_mention_hashes: set[str] = set()
        BATCH_SIZE = 500
        for i in range(0, len(all_hashes), BATCH_SIZE):
            batch = all_hashes[i:i + BATCH_SIZE]
            rows = session.execute(
                select(RawMentionModel.content_hash).where(
                    RawMentionModel.content_hash.in_(batch)
                )
            ).scalars().all()
            existing_mention_hashes.update(rows)

        mention_rows: list[tuple[RawMention, str, RawMentionModel | None]] = []
        mentions_persisted = 0

        for mention, content_hash in mentions_with_hashes:
            if content_hash in existing_mention_hashes:
                mention_rows.append((mention, content_hash, None))
                continue

            db_mention = RawMentionModel(
                source_id=source_id,
                content=mention.content or "",
                content_hash=content_hash,
                source_url=mention.source_url,
                metadata_=mention.metadata,
                collected_at=mention.discovered_at,
            )
            session.add(db_mention)
            session.flush()  # assign ID before creating ExtractedFile rows

            # Create ExtractedFile rows from metadata
            ef_count = _store_extracted_files(session, db_mention.id, mention.metadata)
            if ef_count:
                logger.debug("Created %d extracted_files for mention %s", ef_count, db_mention.id)

            mentions_persisted += 1
            mention_rows.append((mention, content_hash, db_mention))

        # Commit raw mentions so they survive even if matching fails below
        session.commit()
        phase1_elapsed = _time.monotonic() - phase1_start
        logger.info(
            "Phase 1 complete: persisted %d/%d raw mentions for source %s in %.1fs",
            mentions_persisted, len(raw_mentions),
            source.name if source else source_id,
            phase1_elapsed,
        )

        # ------------------------------------------------------------------
        # Phase 2: Match against watch terms and create findings.
        # ------------------------------------------------------------------
        phase2_start = _time.monotonic()

        # Load all active watch terms and build pre-compiled index
        watch_terms = session.execute(
            select(WatchTerm).where(WatchTerm.enabled.is_(True))
        ).scalars().all()

        if not watch_terms:
            logger.warning("No active watch terms configured — matching skipped. "
                           "Add watch terms via the API to start producing findings.")
            return {
                "findings_created": 0,
                "mentions_persisted": mentions_persisted,
            }

        # Build pre-compiled index (patterns compiled once, not per-mention)
        term_index = WatchTermIndex(watch_terms)

        # Log watch term coverage for debugging
        term_types = {}
        for wt in watch_terms:
            ttype = wt.term_type.value if hasattr(wt.term_type, 'value') else str(wt.term_type)
            term_types[ttype] = term_types.get(ttype, 0) + 1
        logger.info(
            "Matching %d mentions against %d watch terms (%s)",
            len(raw_mentions), term_index.term_count,
            ", ".join(f"{k}:{v}" for k, v in sorted(term_types.items())),
        )

        # Batch dedup: fetch all existing finding hashes in one query
        existing_finding_hashes: set[str] = set()
        for i in range(0, len(all_hashes), BATCH_SIZE):
            batch = all_hashes[i:i + BATCH_SIZE]
            rows = session.execute(
                select(Finding.content_hash).where(
                    Finding.content_hash.in_(batch)
                )
            ).scalars().all()
            existing_finding_hashes.update(rows)

        findings_created = 0
        findings_suppressed = 0
        mentions_matched = 0
        mentions_processed = 0
        total_mentions = len(mention_rows)

        COMMIT_BATCH_SIZE = 50  # commit every N findings to survive timeouts
        PROGRESS_LOG_INTERVAL = 100  # log progress every N mentions

        for mention, content_hash, db_mention in mention_rows:
          try:
            mentions_processed += 1

            # Progress logging every PROGRESS_LOG_INTERVAL mentions
            if mentions_processed % PROGRESS_LOG_INTERVAL == 0:
                elapsed = _time.monotonic() - phase2_start
                rate = mentions_processed / elapsed if elapsed > 0 else 0
                logger.info(
                    "Matching progress: %d/%d mentions (%.0f%%) — "
                    "%d findings, %d suppressed — %.0f mentions/sec",
                    mentions_processed, total_mentions,
                    100 * mentions_processed / total_mentions,
                    findings_created, findings_suppressed, rate,
                )

            # Check for exact duplicate finding (from pre-fetched set)
            if content_hash in existing_finding_hashes:
                logger.debug("Duplicate finding (hash=%s), skipping", content_hash[:12])
                continue

            # Skip mentions with no searchable content — nothing to match
            if not (mention.content or mention.title):
                logger.debug(
                    "Mention has no content/title, skipping matching: source=%s hash=%s",
                    mention.source_name, content_hash[:12],
                )
                continue

            # Match against pre-compiled watch term index
            match_results = term_index.match(mention)

            if match_results:
                mentions_matched += 1
                logger.info(
                    "Mention matched: source=%s title=%s matched_institutions=%d terms=%s",
                    mention.source_name,
                    (mention.title or "")[:80],
                    len(match_results),
                    ", ".join(
                        f"{t['term_type']}:{t['value']}"
                        for r in match_results
                        for t in r.matched_terms
                    ),
                )
            else:
                logger.debug(
                    "Mention unmatched: source=%s title=%s content_len=%d",
                    mention.source_name,
                    (mention.title or "")[:80],
                    len(mention.content or ""),
                )

            for result in match_results:
                # Determine raw_content: if mention has extracted files,
                # show only the file(s) that contain the matched terms
                raw_content = _attributed_raw_content(mention, result.matched_terms)

                # Recompute highlight offsets against the actual stored raw_content
                matched_terms_with_highlights = recompute_highlights(
                    result.matched_terms, raw_content,
                ) if raw_content else result.matched_terms

                # Build candidate finding data for enrichment
                _channel = mention.metadata.get("channel_ref", "") if mention.metadata else ""
                _finding_title = mention.title or (_channel if _channel else f"Mention from {source.name}")
                candidate = {
                    "institution_id": result.institution_id,
                    "source_id": source_id,
                    "severity": result.severity_hint,
                    "title": _finding_title,
                    "summary": raw_content[:1000] if raw_content else None,
                    "raw_content": raw_content,
                    "content_hash": content_hash,
                    "source_url": mention.source_url,
                    "matched_terms": matched_terms_with_highlights,
                    "metadata": mention.metadata,
                }

                # Run enrichment pipeline (dedup scoring, FP filtering, threat intel)
                enrichment = enrich_and_filter(candidate, session)

                if not enrichment.should_create:
                    logger.info(
                        "Finding suppressed: %s",
                        enrichment.suppression_reason,
                    )
                    findings_suppressed += 1
                    continue

                # Use adjusted severity if enrichment modified it
                severity = enrichment.adjusted_severity or result.severity_hint

                # Merge enrichment metadata into finding metadata
                merged_metadata = dict(mention.metadata or {})
                # Normalize channel/sender fields for frontend display
                if "channel_ref" in merged_metadata and "channel_name" not in merged_metadata:
                    merged_metadata["channel_name"] = merged_metadata["channel_ref"]
                if mention.author and "sender_name" not in merged_metadata:
                    merged_metadata["sender_name"] = mention.author
                if enrichment.enrichment_metadata:
                    merged_metadata["enrichment"] = enrichment.enrichment_metadata

                # BIN enrichment: look up card issuer info from BIN prefixes in content
                try:
                    from darkdisco.enrichment.bin_lookup import enrich_bins
                    bin_result = enrich_bins(raw_content or "", session)
                    if bin_result.bins_found:
                        merged_metadata["bin_info"] = bin_result.to_dict()
                except Exception:
                    logger.debug("BIN enrichment failed, continuing without it", exc_info=True)

                finding = Finding(
                    institution_id=result.institution_id,
                    source_id=source_id,
                    severity=severity,
                    title=_finding_title,
                    summary=raw_content[:1000] if raw_content else None,
                    raw_content=raw_content,
                    content_hash=content_hash,
                    source_url=mention.source_url,
                    matched_terms=matched_terms_with_highlights,
                    tags=[source.source_type.value],
                    metadata_=merged_metadata,
                    discovered_at=mention.discovered_at,
                )
                session.add(finding)
                findings_created += 1

                # Link the raw mention to this finding
                if db_mention is not None:
                    db_mention.promoted_to_finding_id = finding.id

                # Periodic commit so findings survive timeouts
                if findings_created % COMMIT_BATCH_SIZE == 0:
                    session.commit()
                    logger.info("Committed batch: %d findings so far", findings_created)

          except Exception:
            logger.exception(
                "Phase 2 failed for mention: source=%s hash=%s — "
                "skipping mention, continuing with next",
                mention.source_name, content_hash[:12],
            )
            # Commit what we have so far, then continue. This preserves
            # all findings created before the failure.
            try:
                session.commit()
            except Exception:
                session.rollback()
                # Rebuild index since session was reset
                watch_terms = session.execute(
                    select(WatchTerm).where(WatchTerm.enabled.is_(True))
                ).scalars().all()
                term_index = WatchTermIndex(watch_terms)

        session.commit()

        phase2_elapsed = _time.monotonic() - phase2_start
        total_elapsed = _time.monotonic() - phase1_start
        logger.info(
            "Matching complete for source %s: %d mentions → %d matched → "
            "%d findings (%d suppressed) in %.1fs (phase1=%.1fs, phase2=%.1fs)",
            source.name if source else source_id,
            len(raw_mentions),
            mentions_matched,
            findings_created,
            findings_suppressed,
            total_elapsed,
            phase1_elapsed,
            phase2_elapsed,
        )

        # Trigger alert evaluation for new findings
        if findings_created > 0:
            evaluate_alerts.delay()

        return {
            "findings_created": findings_created,
            "findings_suppressed": findings_suppressed,
            "mentions_processed": len(raw_mentions),
            "mentions_persisted": mentions_persisted,
            "elapsed_seconds": round(total_elapsed, 1),
        }
    finally:
        session.close()


@app.task(name="darkdisco.pipeline.worker.evaluate_alerts")
def evaluate_alerts():
    """Check new findings against alert rules and create notifications.

    For each matching (finding, rule) pair:
    1. Persist an in-app Notification row
    2. Dispatch delivery to configured channels (email, Slack, webhook)
    """
    from darkdisco.common.models import AlertRule, Finding, FindingStatus, Notification
    from darkdisco.pipeline.notify import deliver_notification

    session = _get_sync_session()
    try:
        # Get new findings that haven't been alerted on yet
        new_findings = session.execute(
            select(Finding).where(Finding.status == FindingStatus.new)
        ).scalars().all()

        if not new_findings:
            return {"notifications_created": 0}

        # Get all enabled alert rules
        rules = session.execute(
            select(AlertRule).where(AlertRule.enabled.is_(True))
        ).scalars().all()

        notifications_created = 0
        deliveries: list[dict] = []
        severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}

        for finding in new_findings:
            for rule in rules:
                if not _rule_matches(rule, finding, severity_rank):
                    continue

                title = f"Alert: {finding.title}"
                message = f"[{finding.severity.value.upper()}] {finding.summary or finding.title}"

                # 1. Persist in-app notification
                notification = Notification(
                    user_id=rule.owner_id,
                    alert_rule_id=rule.id,
                    finding_id=finding.id,
                    title=title,
                    message=message,
                )
                session.add(notification)
                notifications_created += 1

                # 2. Deliver to external channels
                result = deliver_notification(
                    rule=rule,
                    title=title,
                    message=message,
                    finding_id=finding.id,
                )
                if result:
                    deliveries.append({"finding": finding.id, "rule": rule.id, **result})

        session.commit()
        logger.info(
            "Created %d notifications from %d new findings (%d external deliveries)",
            notifications_created,
            len(new_findings),
            len(deliveries),
        )
        return {"notifications_created": notifications_created, "deliveries": deliveries}
    finally:
        session.close()


def _rule_matches(rule, finding, severity_rank: dict) -> bool:
    """Check if a finding matches an alert rule's criteria."""
    # Institution filter
    if rule.institution_id and rule.institution_id != finding.institution_id:
        return False

    # Severity filter — finding must be >= rule's minimum severity
    finding_rank = severity_rank.get(finding.severity.value if hasattr(finding.severity, 'value') else finding.severity, 4)
    rule_rank = severity_rank.get(rule.min_severity.value if hasattr(rule.min_severity, 'value') else rule.min_severity, 1)
    if finding_rank > rule_rank:
        return False

    # Source type filter
    if rule.source_types and finding.source:
        source_type = finding.source.source_type.value if hasattr(finding.source.source_type, 'value') else finding.source.source_type
        if source_type not in rule.source_types:
            return False

    # Keyword filter
    if rule.keyword_filter:
        searchable = f"{finding.title} {finding.summary or ''}".lower()
        if rule.keyword_filter.lower() not in searchable:
            return False

    return True


# ---------------------------------------------------------------------------
# Channel auto-discovery
# ---------------------------------------------------------------------------


def _extract_channel_discoveries(
    session: Session, source_id: str, mentions: list,
) -> int:
    """Scan mention content for t.me links and record new DiscoveredChannel rows."""
    from darkdisco.common.models import DiscoveredChannel
    from darkdisco.discovery.connectors.telegram import extract_channel_links

    # Collect all existing known URLs for this source to avoid duplicates
    existing_urls: set[str] = set()
    rows = session.execute(
        select(DiscoveredChannel.url).where(DiscoveredChannel.source_id == source_id)
    ).scalars().all()
    existing_urls.update(r.lower() for r in rows)

    # Also collect all currently configured channels across all telegram sources
    from darkdisco.common.models import Source, SourceType

    tg_sources = session.execute(
        select(Source).where(
            Source.source_type.in_([SourceType.telegram, SourceType.telegram_intel])
        )
    ).scalars().all()
    configured_channels: set[str] = set()
    for src in tg_sources:
        cfg = src.config or {}
        for ch in cfg.get("channels", []):
            configured_channels.add(ch.lower().strip("@").rstrip("/"))

    created = 0
    for mention in mentions:
        text = mention.content or ""
        links = extract_channel_links(text)
        channel_ref = mention.metadata.get("channel_ref", "")
        message_id = mention.metadata.get("message_id")

        for link in links:
            low = link.lower()
            # Skip if already discovered or already configured
            if low in existing_urls:
                continue
            # Check if this channel is already configured (by username)
            # Extract the channel part for comparison
            channel_part = low.split("t.me/")[-1].strip("+").rstrip("/")
            if channel_part in configured_channels:
                continue

            dc = DiscoveredChannel(
                url=link,
                source_id=source_id,
                source_channel=channel_ref,
                message_id=message_id,
            )
            session.add(dc)
            existing_urls.add(low)
            created += 1

    if created:
        session.commit()
        logger.info(
            "Discovered %d new channel links from source %s",
            created, source_id,
        )

    return created


@app.task(name="darkdisco.pipeline.worker.process_channel_discoveries")
def process_channel_discoveries(batch_size: int = 5):
    """Process pending channel discoveries: auto-join and add to source config.

    Runs periodically via beat schedule. Processes pending channels automatically.
    Rate-limits joins to avoid Telegram flood bans (batch_size controls pace).
    """
    import redis as _redis
    from darkdisco.common.models import DiscoveredChannel, DiscoveryStatus, Source

    session = _get_sync_session()
    # Acquire Telegram session lock to avoid SQLite contention with poll_source
    r = _redis.from_url(settings.celery_broker_url)
    tg_lock = r.lock("darkdisco:telegram_session_lock", timeout=300)
    if not tg_lock.acquire(blocking=False):
        logger.info("Telegram session lock held, skipping channel discovery this cycle")
        session.close()
        return {"skipped": True, "reason": "session_locked"}

    try:
        pending = session.execute(
            select(DiscoveredChannel)
            .where(DiscoveredChannel.status.in_([DiscoveryStatus.pending, DiscoveryStatus.approved]))
            .order_by(DiscoveredChannel.discovered_at.asc())
            .limit(batch_size)
        ).scalars().all()

        if not pending:
            return {"processed": 0}

        joined = 0
        failed = 0
        for dc in pending:
            target_sid = dc.added_to_source_id or dc.source_id
            target_source = session.get(Source, target_sid)
            if not target_source:
                dc.status = DiscoveryStatus.failed
                dc.notes = "Target source not found"
                failed += 1
                continue

            cfg = dict(target_source.config or {})
            try:
                connector = _load_connector(target_source)
            except ValueError:
                dc.status = DiscoveryStatus.failed
                dc.notes = "No connector for target source"
                failed += 1
                continue
            try:
                success = _get_or_create_loop().run_until_complete(_join_channel_async(connector, dc.url))
            except Exception as exc:
                dc.status = DiscoveryStatus.failed
                dc.notes = f"Join error: {str(exc)[:200]}"
                failed += 1
                logger.exception("Failed to join discovered channel %s", dc.url)
                continue

            if success:
                channels: list[str] = list(cfg.get("channels", []))
                if dc.url not in channels:
                    channels.append(dc.url)
                    cfg["channels"] = channels
                    target_source.config = cfg
                dc.status = DiscoveryStatus.joined
                dc.added_to_source_id = target_sid
                dc.joined_at = datetime.now(timezone.utc)
                joined += 1
                logger.info("Auto-joined discovered channel: %s", dc.url)
            else:
                dc.status = DiscoveryStatus.failed
                dc.notes = "Join returned False"
                failed += 1

        session.commit()
        logger.info(
            "Processed channel discoveries: %d joined, %d failed",
            joined, failed,
        )
        return {"processed": len(approved), "joined": joined, "failed": failed}
    finally:
        try:
            tg_lock.release()
        except Exception:
            pass
        session.close()


async def _join_channel_async(connector, channel_ref: str) -> bool:
    """Bridge async join_channel to sync context."""
    try:
        await connector.setup()
        return await connector.join_channel(channel_ref)
    finally:
        await connector.teardown()


# ---------------------------------------------------------------------------
# Trapline watchlist sync
# ---------------------------------------------------------------------------


@app.task(name="darkdisco.pipeline.worker.download_pending_files",
          soft_time_limit=3600, time_limit=3900)
def download_pending_files(batch_size: int = 50):
    """Download large files from mentions marked as download_status=pending.

    Uses per-source Redis locks and per-source session copies so downloads
    from different Telegram sources can run concurrently, and downloads
    don't block polling (separate session files avoid SQLite contention).
    """
    import redis as _redis
    from darkdisco.config import settings
    from darkdisco.common.models import RawMention as RawMentionModel, Source

    r = _redis.from_url(settings.celery_broker_url)

    # Global download coordination lock — prevents multiple beat-triggered
    # download tasks from running simultaneously.
    coord_lock = r.lock("darkdisco:download_coordinator_lock", timeout=300)
    if not coord_lock.acquire(blocking=False):
        logger.info("Another download task is already running, skipping")
        return {"skipped": True}

    session = _get_sync_session()
    try:
        # Skip videos, webp stickers, and audio — focus on images, text, archives
        _SKIP_MIMES = ("video/", "image/webp", "audio/")
        # Prioritize documents/archives/text over images — images are high-volume
        # and lower-value; documents often contain credentials and stealer logs.
        from sqlalchemy import case, or_
        mime_priority = case(
            (RawMentionModel.metadata_["file_mime"].astext.startswith("image/"), 1),
            else_=0,
        )
        # Use or_ with NULL checks so mentions without file_mime still get downloaded
        file_mime = RawMentionModel.metadata_["file_mime"].astext
        stmt = (
            select(RawMentionModel)
            .where(
                RawMentionModel.metadata_["download_status"].astext == "pending",
                or_(file_mime.is_(None), ~file_mime.startswith("video/")),
                or_(file_mime.is_(None), file_mime != "image/webp"),
                or_(file_mime.is_(None), ~file_mime.startswith("audio/")),
            )
            .order_by(mime_priority, RawMentionModel.collected_at.desc())
            .limit(batch_size)
        )
        mentions = session.execute(stmt).scalars().all()

        if not mentions:
            return {"downloaded": 0, "pending": 0}

        logger.info("Found %d mentions with pending file downloads", len(mentions))

        by_source: dict[str, list] = {}
        for m in mentions:
            by_source.setdefault(m.source_id, []).append(m)

        downloaded = 0
        failed = 0
        deduped = 0

        # Build dedup cache once (shared across all sources)
        _dedup_cache: dict[tuple, dict] = {}
        dedup_stmt = (
            select(RawMentionModel)
            .where(RawMentionModel.metadata_["download_status"].astext == "stored")
            .where(RawMentionModel.metadata_["s3_key"].astext != "")
            .limit(5000)
        )
        for stored in session.execute(dedup_stmt).scalars():
            sm = stored.metadata_ or {}
            key = (sm.get("file_name"), str(sm.get("file_size", "")))
            if key[0] and key not in _dedup_cache:
                _dedup_cache[key] = {
                    "s3_key": sm.get("s3_key"),
                    "file_sha256": sm.get("file_sha256"),
                    "file_size": sm.get("file_size"),
                }
        logger.info("Download dedup cache: %d stored file signatures", len(_dedup_cache))

        for source_id, source_mentions in by_source.items():
            source = session.get(Source, source_id)
            if not source or source.source_type.value not in ("telegram", "telegram_intel"):
                continue

            # Per-source download lock — allows concurrent downloads from
            # different sources while preventing duplicate work on same source.
            source_lock = r.lock(
                f"darkdisco:telegram_download_lock:{source_id}", timeout=300,
            )
            if not source_lock.acquire(blocking=False):
                logger.info("Download lock held for source %s, skipping", source.name)
                continue

            try:
                connector = _load_connector_for_download(source)

                async def _do_downloads(_connector, _mentions, _dedup):
                    try:
                        await _connector.setup()
                        results = {"downloaded": 0, "failed": 0, "deduped": 0}

                        for mention in _mentions:
                            try:
                                meta = mention.metadata_ or {}
                                msg_id = meta.get("message_id")
                                if not msg_id:
                                    continue

                                # Check dedup cache before downloading
                                dedup_key = (meta.get("file_name"), str(meta.get("file_size", "")))
                                cached = _dedup.get(dedup_key)
                                if cached and cached.get("s3_key"):
                                    meta["s3_key"] = cached["s3_key"]
                                    meta["file_sha256"] = cached.get("file_sha256")
                                    meta["file_size"] = cached.get("file_size")
                                    meta["download_status"] = "stored"
                                    meta["dedup_source"] = "file_signature"
                                    mention.metadata_ = meta
                                    session.commit()
                                    results["deduped"] += 1
                                    continue

                                chat_id = meta.get("chat_id")
                                file_data = await _connector.download_media(
                                    int(msg_id),
                                    channel_id=int(chat_id) if chat_id else None,
                                )
                                if file_data:
                                    # Upload to S3
                                    import hashlib
                                    sha256 = hashlib.sha256(file_data).hexdigest()
                                    filename = meta.get("file_name") or ""
                                    if not filename:
                                        # Telegram photos have no filename — infer from MIME
                                        mime = meta.get("file_mime", "")
                                        ext = {
                                            "image/jpeg": ".jpg", "image/png": ".png",
                                            "image/gif": ".gif", "image/webp": ".webp",
                                        }.get(mime, "")
                                        filename = f"{sha256[:16]}{ext}" if ext else "unknown"
                                    s3_key = f"files/{sha256[:8]}/{filename}"

                                    from darkdisco.pipeline.files import _get_s3_client
                                    s3 = _get_s3_client()
                                    from io import BytesIO
                                    s3.upload_fileobj(BytesIO(file_data), settings.s3_bucket, s3_key)

                                    meta["s3_key"] = s3_key
                                    meta["file_sha256"] = sha256
                                    meta["file_size"] = len(file_data)
                                    meta["download_status"] = "stored"

                                    # Extract archive if applicable
                                    from darkdisco.pipeline.files import extract_archive, analyze_extracted_files
                                    fname = filename.lower()
                                    if any(fname.endswith(ext) for ext in (".zip", ".rar", ".tar", ".tar.gz", ".tar.bz2", ".7z")):
                                        passwords = meta.get("extracted_passwords", [])
                                        extracted = extract_archive(file_data, filename, passwords=passwords)
                                        if extracted:
                                            analysis = analyze_extracted_files(extracted)
                                            meta["file_analysis"] = analysis.to_dict()
                                            # Append extracted text to mention content
                                            if analysis.text_content:
                                                mention.content = (mention.content or "") + "\n\n" + analysis.text_content
                                            # Upload extracted files to S3 and create ExtractedFile rows
                                            from darkdisco.common.models import ExtractedFile as EFModel
                                            per_file_texts = []
                                            for ef in extracted:
                                                ef_key = f"files/{sha256[:8]}/extracted/{ef.sha256[:8]}/{ef.filename}"
                                                s3.upload_fileobj(BytesIO(ef.content), settings.s3_bucket, ef_key)
                                                # Create ExtractedFile row
                                                text_content = None
                                                if ef.is_text and ef.content:
                                                    try:
                                                        text_content = ef.content.decode("utf-8", errors="replace") if isinstance(ef.content, bytes) else ef.content
                                                    except Exception:
                                                        pass
                                                ext = ""
                                                if "." in ef.filename:
                                                    ext = ef.filename.rsplit(".", 1)[-1].lower()
                                                session.add(EFModel(
                                                    mention_id=mention.id,
                                                    filename=ef.filename,
                                                    s3_key=ef_key,
                                                    sha256=ef.sha256,
                                                    size=ef.size,
                                                    extension=ext or None,
                                                    is_text=ef.is_text,
                                                    text_content=text_content,
                                                ))
                                                if text_content and text_content.strip():
                                                    per_file_texts.append({"filename": ef.filename, "content": text_content})
                                            if per_file_texts:
                                                meta["extracted_file_contents"] = per_file_texts
                                            if analysis.credential_indicators:
                                                meta["has_credentials"] = True
                                                meta["credential_count"] = len(analysis.credential_indicators)

                                    mention.metadata_ = meta
                                    session.commit()
                                    results["downloaded"] += 1
                                    # Add to dedup cache for rest of batch
                                    _dedup[dedup_key] = {
                                        "s3_key": s3_key,
                                        "file_sha256": sha256,
                                        "file_size": len(file_data),
                                    }
                                    logger.info("Downloaded %s for mention %s", filename, mention.id)
                                else:
                                    meta["download_status"] = "error"
                                    meta["download_error"] = "No file data returned"
                                    mention.metadata_ = meta
                                    session.commit()
                                    results["failed"] += 1
                            except Exception:
                                logger.exception("Failed to download file for mention %s", mention.id)
                                meta = mention.metadata_ or {}
                                meta["download_status"] = "error"
                                meta["download_error"] = "Download exception"
                                mention.metadata_ = meta
                                session.commit()
                                results["failed"] += 1
                        return results
                    finally:
                        await _connector.teardown()

                results = _get_or_create_loop().run_until_complete(
                    _do_downloads(connector, source_mentions, _dedup_cache)
                )
                downloaded += results["downloaded"]
                failed += results["failed"]
                deduped += results.get("deduped", 0)
            except Exception:
                logger.exception("Failed to download files for source %s", source_id)
                failed += len(source_mentions)
            finally:
                try:
                    source_lock.release()
                except Exception:
                    pass

        return {"downloaded": downloaded, "failed": failed, "deduped": deduped}
    finally:
        try:
            coord_lock.release()
        except Exception:
            pass
        session.close()


@app.task(name="darkdisco.pipeline.worker.sync_trapline_watchlist")
def sync_trapline_watchlist():
    """Periodic: sync all active institutions to trapline's client watchlist."""
    from darkdisco.pipeline.trapline import sync_all_institutions

    session = _get_sync_session()
    try:
        return sync_all_institutions(session)
    finally:
        session.close()


@app.task(name="darkdisco.pipeline.worker.sync_institution_to_trapline")
def sync_institution_to_trapline(institution_id: str):
    """Sync a single institution to trapline after create/update."""
    from darkdisco.common.models import Institution
    from darkdisco.pipeline.trapline import sync_institution

    session = _get_sync_session()
    try:
        inst = session.get(Institution, institution_id)
        if inst is None:
            logger.error("Institution %s not found for trapline sync", institution_id)
            return {"error": "institution_not_found"}
        if not inst.active:
            logger.info("Institution %s is inactive, skipping trapline sync", inst.name)
            return {"skipped": True}
        return sync_institution(inst)
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Extracted file backfill & download
# ---------------------------------------------------------------------------


async def _download_files_async(s3_keys: list[str]) -> dict[str, bytes]:
    """Download files from S3 and return {s3_key: content_bytes}.

    Used to populate text_content on ExtractedFile rows when the content
    wasn't stored inline (e.g. backfill of older mentions).
    """
    import aioboto3

    from darkdisco.config import settings

    results: dict[str, bytes] = {}
    s3_session = aioboto3.Session()
    async with s3_session.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    ) as s3:
        for key in s3_keys:
            try:
                resp = await s3.get_object(Bucket=settings.s3_bucket, Key=key)
                data = await resp["Body"].read()
                results[key] = data
            except Exception:
                logger.warning("Failed to download S3 key: %s", key)
    return results


@app.task(name="darkdisco.pipeline.worker.backfill_extracted_files")
def backfill_extracted_files(batch_size: int = 100):
    """Backfill ExtractedFile rows from existing raw_mentions with file_analysis metadata.

    Iterates raw_mentions that have extracted_file_contents in metadata but
    no corresponding extracted_files rows yet.
    """
    from darkdisco.common.models import ExtractedFile
    from darkdisco.common.models import RawMention as RawMentionModel

    session = _get_sync_session()
    try:
        # Find mentions with metadata but no ExtractedFile rows yet
        already_backfilled = select(ExtractedFile.mention_id).distinct().scalar_subquery()
        mentions = session.execute(
            select(RawMentionModel).where(
                RawMentionModel.metadata_.isnot(None),
                RawMentionModel.id.notin_(already_backfilled),
            ).limit(batch_size)
        ).scalars().all()

        total_created = 0
        for mention in mentions:
            meta = mention.metadata_ or {}
            if not meta.get("extracted_file_contents"):
                continue
            count = _store_extracted_files(session, mention.id, meta)
            total_created += count

        session.commit()
        logger.info("Backfill: created %d extracted_files from %d mentions", total_created, len(mentions))

        # If we processed a full batch, there may be more — chain another run
        if len(mentions) >= batch_size:
            backfill_extracted_files.delay(batch_size)

        return {"created": total_created, "mentions_processed": len(mentions)}
    finally:
        session.close()


@app.task(
    name="darkdisco.pipeline.worker.extract_stored_archive",
    bind=True,
    max_retries=2,
    soft_time_limit=1800,
    time_limit=3600,
)
def extract_stored_archive(self, mention_id: str, force: bool = False):
    """Stream-extract a single stored archive from S3 for a mention.

    Downloads the archive to temp disk, extracts files, uploads extracted
    files back to S3, creates ExtractedFile rows, and updates mention metadata
    with file_analysis results.

    If force=True, deletes existing ExtractedFile rows and re-extracts.
    """
    from darkdisco.common.models import ExtractedFile
    from darkdisco.common.models import RawMention as RawMentionModel
    from darkdisco.pipeline.files import (
        analyze_extracted_files,
        extract_passwords,
        is_archive,
        stream_extract_from_s3,
        upload_to_s3,
    )

    session = _get_sync_session()
    try:
        mention = session.get(RawMentionModel, mention_id)
        if mention is None:
            logger.error("Mention %s not found for archive extraction", mention_id)
            return {"error": "mention_not_found"}

        meta = mention.metadata_ or {}
        s3_key = meta.get("s3_key")
        filename = meta.get("file_name", "archive.zip")
        file_sha256 = meta.get("file_sha256", "")

        if not s3_key:
            logger.warning("Mention %s has no s3_key in metadata", mention_id)
            return {"error": "no_s3_key"}

        if not is_archive(filename):
            logger.info("Mention %s file %s is not an archive, skipping", mention_id, filename)
            return {"skipped": True, "reason": "not_archive"}

        # Check if already extracted (has ExtractedFile rows)
        existing = session.execute(
            select(ExtractedFile.id).where(
                ExtractedFile.mention_id == mention_id
            ).limit(1)
        ).scalar_one_or_none()
        if existing and not force:
            logger.info("Mention %s already has extracted files, skipping", mention_id)
            return {"skipped": True, "reason": "already_extracted"}

        # If force re-extraction, delete old rows first
        if existing and force:
            from sqlalchemy import delete as sa_delete
            session.execute(
                sa_delete(ExtractedFile).where(
                    ExtractedFile.mention_id == mention_id
                )
            )
            logger.info("Deleted existing ExtractedFile rows for mention %s (force re-extract)", mention_id)

        # Extract passwords from the mention content
        passwords = extract_passwords(mention.content or "")
        passwords.extend(meta.get("extracted_passwords", []))

        # Stream-extract from S3 to temp disk
        try:
            extracted = stream_extract_from_s3(s3_key, filename, passwords)
        except Exception as exc:
            logger.exception("Stream extraction failed for mention %s", mention_id)
            raise self.retry(exc=exc, countdown=120 * (self.request.retries + 1))

        if not extracted:
            logger.info("No files extracted from %s for mention %s", filename, mention_id)
            meta["file_analysis"] = {"total_files": 0, "extraction_attempted": True}
            mention.metadata_ = {**meta}
            session.commit()
            return {"extracted": 0, "mention_id": mention_id}

        # Analyze extracted files
        analysis = analyze_extracted_files(extracted)
        meta["file_analysis"] = analysis.to_dict()

        # Upload extracted files to S3 and create ExtractedFile rows
        per_file_texts = []
        ef_count = 0
        for ef in extracted:
            # Upload to S3
            ef_s3_key = f"files/{file_sha256[:8]}/extracted/{ef.sha256[:8]}/{ef.filename}"
            upload_to_s3(ef_s3_key, ef.content)

            # Determine text content
            text_content = None
            if ef.is_text and ef.content:
                try:
                    text_content = ef.content.decode("utf-8", errors="replace")
                except Exception:
                    text_content = None

            # Create ExtractedFile row
            ext = ""
            if "." in ef.filename:
                ext = ef.filename.rsplit(".", 1)[-1].lower()

            row = ExtractedFile(
                mention_id=mention_id,
                filename=ef.filename,
                s3_key=ef_s3_key,
                sha256=ef.sha256,
                size=ef.size,
                extension=ext or None,
                is_text=ef.is_text,
                text_content=text_content,
            )
            session.add(row)
            ef_count += 1

            if text_content and text_content.strip():
                per_file_texts.append({"filename": ef.filename, "content": text_content})

        # Store per-file text content in metadata for matching
        if per_file_texts:
            meta["extracted_file_contents"] = per_file_texts

        if analysis.credential_indicators:
            meta["has_credentials"] = True
            meta["credential_count"] = len(analysis.credential_indicators)

        # Persist updated metadata
        mention.metadata_ = {**meta}
        session.commit()

        logger.info(
            "Stream-extracted mention %s: %d files (%d text, %d credential indicators)",
            mention_id,
            ef_count,
            sum(1 for f in extracted if f.is_text),
            len(analysis.credential_indicators),
        )

        return {
            "mention_id": mention_id,
            "extracted": ef_count,
            "text_files": sum(1 for f in extracted if f.is_text),
            "credential_indicators": len(analysis.credential_indicators),
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@app.task(name="darkdisco.pipeline.worker.backfill_stored_archives")
def backfill_stored_archives(batch_size: int = 10):
    """Find mentions with stored archives but no extraction, and dispatch extraction tasks.

    Identifies raw_mentions that have:
    - An s3_key in metadata (file uploaded to S3)
    - A file_name that is an archive
    - No file_analysis in metadata (never extracted)
    - No ExtractedFile rows

    Dispatches extract_stored_archive tasks for each, with rate limiting
    to avoid overwhelming S3 and disk.
    """
    from darkdisco.common.models import ExtractedFile
    from darkdisco.common.models import RawMention as RawMentionModel
    from darkdisco.pipeline.files import is_archive

    session = _get_sync_session()
    try:
        # Find mentions with s3_key but no ExtractedFile rows
        already_extracted = select(ExtractedFile.mention_id).distinct().scalar_subquery()
        mentions = session.execute(
            select(RawMentionModel).where(
                RawMentionModel.metadata_.isnot(None),
                RawMentionModel.id.notin_(already_extracted),
            ).order_by(RawMentionModel.collected_at.desc())
            .limit(batch_size * 5)  # Fetch extra since we filter in Python
        ).scalars().all()

        dispatched = 0
        skipped = 0
        for mention in mentions:
            meta = mention.metadata_ or {}

            # Must have an s3_key (file stored in S3)
            if not meta.get("s3_key"):
                continue

            # Must be an archive file
            filename = meta.get("file_name", "")
            if not is_archive(filename):
                continue

            # Skip if already has file_analysis (already extracted)
            if meta.get("file_analysis"):
                skipped += 1
                continue

            # Skip if already has extracted_file_contents
            if meta.get("extracted_file_contents"):
                skipped += 1
                continue

            # Dispatch extraction task with a countdown stagger
            # to avoid hammering S3 with concurrent large downloads
            extract_stored_archive.apply_async(
                args=[mention.id],
                countdown=dispatched * 30,  # 30s stagger between tasks
            )
            dispatched += 1

            if dispatched >= batch_size:
                break

        logger.info(
            "Backfill stored archives: dispatched %d extraction tasks (%d skipped)",
            dispatched,
            skipped,
        )

        # If we hit the batch limit, chain another run after all tasks complete
        if dispatched >= batch_size:
            backfill_stored_archives.apply_async(
                args=[batch_size],
                countdown=dispatched * 30 + 60,  # Wait for current batch + buffer
            )

        return {"dispatched": dispatched, "skipped": skipped}
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Historical OCR backfill
# ---------------------------------------------------------------------------


@app.task(
    name="darkdisco.pipeline.worker.backfill_ocr",
    soft_time_limit=1800,
    time_limit=3600,
)
def backfill_ocr(batch_size: int = 20, days: int = 30):
    """Backfill OCR on image attachments from the last N days.

    Finds raw_mentions with image attachments (stored in S3) that don't yet
    have ocr_text in metadata.  Downloads from S3, runs OCR (with dedup cache),
    appends OCR text to mention content, and re-runs watch term matching on
    any mentions where OCR text was added.

    Rate-limited via batch_size and chained follow-ups to avoid overloading.
    Uses a Redis lock to prevent concurrent OCR tasks from saturating all workers.
    """
    import redis as _redis
    from darkdisco.config import settings as _settings
    r = _redis.from_url(_settings.celery_broker_url)
    lock = r.lock("darkdisco:backfill_ocr_lock", timeout=1800, blocking=False)
    if not lock.acquire(blocking=False):
        logger.info("Another OCR backfill task is already running, skipping")
        return {"skipped": True}

    from datetime import timedelta

    from sqlalchemy import and_, or_

    from darkdisco.common.models import RawMention as RawMentionModel
    from darkdisco.pipeline.files import _get_s3_client
    from darkdisco.pipeline.ocr import is_image, is_image_media_type

    session = _get_sync_session()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        # Find mentions from the last N days that:
        # - have an s3_key (file stored)
        # - don't already have ocr_text in metadata
        # - have an image file or media type
        mentions = session.execute(
            select(RawMentionModel).where(
                and_(
                    RawMentionModel.collected_at >= cutoff,
                    RawMentionModel.metadata_.isnot(None),
                    RawMentionModel.metadata_["s3_key"].astext != "",
                    or_(
                        RawMentionModel.metadata_["ocr_text"].astext.is_(None),
                        ~RawMentionModel.metadata_.has_key("ocr_text"),
                    ),
                )
            )
            .order_by(RawMentionModel.collected_at.desc())
            .limit(batch_size * 3)  # Fetch extra since we filter in Python
        ).scalars().all()

        s3 = _get_s3_client()
        processed = 0
        skipped = 0
        ocr_hits = 0
        rematch_mention_ids = []

        for mention in mentions:
            meta = mention.metadata_ or {}
            filename = meta.get("file_name", "")
            media_type = meta.get("media_type", "")

            # Filter to images only
            if not (is_image(filename) or is_image_media_type(media_type)):
                continue

            # Skip if already has OCR text
            if meta.get("ocr_text"):
                skipped += 1
                continue

            s3_key = meta.get("s3_key")
            if not s3_key:
                continue

            # Download image from S3
            try:
                resp = s3.get_object(Bucket=settings.s3_bucket, Key=s3_key)
                image_data = resp["Body"].read()
            except Exception:
                logger.debug("Failed to download %s for OCR backfill", s3_key)
                skipped += 1
                continue

            # Compute hash and run OCR with dedup
            image_sha256 = hashlib.sha256(image_data).hexdigest()
            ocr_result = _ocr_with_dedup(image_data, filename, image_sha256)

            if ocr_result and ocr_result.has_text:
                meta["ocr_text"] = ocr_result.text
                meta["ocr_confidence"] = ocr_result.confidence
                meta["ocr_engine"] = ocr_result.engine

                # Append OCR text to mention content
                separator = f"\n\n--- OCR text from {filename} ---\n\n"
                mention.content = (mention.content or "") + separator + ocr_result.text
                mention.metadata_ = {**meta}
                ocr_hits += 1
                rematch_mention_ids.append((mention.id, mention.source_id))
            else:
                # Mark as attempted so we don't retry
                meta["ocr_text"] = ""
                mention.metadata_ = {**meta}

            processed += 1
            if processed >= batch_size:
                break

        session.commit()

        # Re-run watch term matching on mentions with new OCR text
        if rematch_mention_ids:
            _rematch_ocr_mentions(rematch_mention_ids)

        logger.info(
            "OCR backfill: processed=%d, ocr_hits=%d, skipped=%d",
            processed, ocr_hits, skipped,
        )

        # Chain another batch if we hit the limit
        if processed >= batch_size:
            backfill_ocr.apply_async(
                args=[batch_size, days],
                countdown=60,  # 1 min between batches
            )

        return {
            "processed": processed,
            "ocr_hits": ocr_hits,
            "skipped": skipped,
            "rematched": len(rematch_mention_ids),
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        try:
            lock.release()
        except Exception:
            pass


def _rematch_ocr_mentions(mention_source_pairs: list[tuple[str, str]]):
    """Re-run watch term matching on mentions that received OCR text via backfill.

    Groups mentions by source_id and dispatches run_matching with
    the updated content so new findings can be created from OCR text.
    """
    from darkdisco.common.models import RawMention as RawMentionModel

    session = _get_sync_session()
    try:
        by_source: dict[str, list[str]] = {}
        for mention_id, source_id in mention_source_pairs:
            by_source.setdefault(source_id, []).append(mention_id)

        for source_id, mention_ids in by_source.items():
            mentions = session.execute(
                select(RawMentionModel).where(
                    RawMentionModel.id.in_(mention_ids)
                )
            ).scalars().all()

            serialized = [
                {
                    "source_name": (m.source.name if m.source else "unknown"),
                    "source_url": m.source_url,
                    "title": "",
                    "content": m.content,
                    "author": None,
                    "discovered_at": m.collected_at.isoformat() if m.collected_at else None,
                    "metadata": m.metadata_ or {},
                }
                for m in mentions
            ]

            if serialized:
                run_matching.delay(source_id, serialized)

        logger.info(
            "Dispatched re-matching for %d mentions across %d sources",
            len(mention_source_pairs), len(by_source),
        )
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Retroactive hunt — re-match existing mentions for a specific institution
# ---------------------------------------------------------------------------


@app.task(
    name="darkdisco.pipeline.worker.retroactive_hunt",
    soft_time_limit=3600,
    time_limit=7200,
)
def retroactive_hunt(institution_name: str, batch_size: int = 200, days: int | None = None):
    """Re-run watch term matching across all existing mentions for a specific institution.

    Use this when a new institution is added and you want to discover any existing
    mentions that match its watch terms (BINs, routing numbers, name, domains).

    Args:
        institution_name: Name of the institution to hunt for (exact match).
        batch_size: Number of mentions to process per batch.
        days: Limit to mentions from the last N days. None = all time.
    """
    from darkdisco.common.models import Institution as InstitutionModel
    from darkdisco.common.models import RawMention as RawMentionModel
    from darkdisco.common.models import WatchTerm as WatchTermModel
    from darkdisco.discovery.connectors.base import RawMention
    from darkdisco.discovery.matcher import WatchTermIndex

    session = _get_sync_session()
    try:
        # Load watch terms for the target institution
        inst = session.execute(
            select(InstitutionModel).where(InstitutionModel.name == institution_name)
        ).scalars().first()

        if inst is None:
            logger.error("Institution %r not found for retroactive hunt", institution_name)
            return {"error": "institution_not_found"}

        watch_terms = session.execute(
            select(WatchTermModel).where(
                WatchTermModel.institution_id == inst.id,
                WatchTermModel.enabled.is_(True),
            )
        ).scalars().all()

        if not watch_terms:
            logger.warning("No watch terms for %s — hunt skipped", institution_name)
            return {"error": "no_watch_terms"}

        # Build pre-compiled index for fast matching
        term_index = WatchTermIndex(watch_terms)
        logger.info(
            "Retroactive hunt for %s: %d watch terms",
            institution_name, term_index.term_count,
        )

        # Query mentions in batches
        query = select(RawMentionModel).order_by(RawMentionModel.collected_at.desc())
        if days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            query = query.where(RawMentionModel.collected_at >= cutoff)

        offset = 0
        total_matched = 0
        total_scanned = 0

        while True:
            mentions = session.execute(
                query.offset(offset).limit(batch_size)
            ).scalars().all()

            if not mentions:
                break

            # Group by source for dispatching
            by_source: dict[str, list[dict]] = {}
            for m in mentions:
                total_scanned += 1
                mention = RawMention(
                    source_name=(m.source.name if m.source else "unknown"),
                    source_url=m.source_url,
                    title="",
                    content=m.content or "",
                    author=None,
                    discovered_at=m.collected_at or datetime.now(timezone.utc),
                    metadata=m.metadata_ or {},
                )

                results = term_index.match(mention)
                if results:
                    total_matched += 1
                    source_id = m.source_id
                    by_source.setdefault(source_id, []).append({
                        "source_name": mention.source_name,
                        "source_url": mention.source_url,
                        "title": mention.title,
                        "content": mention.content,
                        "author": mention.author,
                        "discovered_at": mention.discovered_at.isoformat() if mention.discovered_at else None,
                        "metadata": mention.metadata or {},
                    })

            # Dispatch matched mentions for full finding creation
            for source_id, serialized in by_source.items():
                run_matching.delay(source_id, serialized)

            offset += batch_size

            if len(mentions) < batch_size:
                break

        logger.info(
            "Retroactive hunt for %s complete: scanned=%d, matched=%d",
            institution_name, total_scanned, total_matched,
        )

        return {
            "institution": institution_name,
            "scanned": total_scanned,
            "matched": total_matched,
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Scheduled report generation
# ---------------------------------------------------------------------------


def _compute_date_range(mode: str) -> tuple[datetime, datetime]:
    """Compute date_from and date_to based on date range mode."""
    now = datetime.now(timezone.utc)
    if mode == "last_24h":
        return now - timedelta(hours=24), now
    elif mode == "last_7d":
        return now - timedelta(days=7), now
    elif mode == "last_30d":
        return now - timedelta(days=30), now
    elif mode == "last_quarter":
        return now - timedelta(days=90), now
    else:
        return now - timedelta(days=7), now


def _compute_next_run(schedule) -> datetime | None:
    """Compute next run time for a schedule based on cron or interval."""
    now = datetime.now(timezone.utc)
    if schedule.interval_seconds:
        return now + timedelta(seconds=schedule.interval_seconds)
    if schedule.cron_expression:
        try:
            from croniter import croniter
            cron = croniter(schedule.cron_expression, now)
            return cron.get_next(datetime)
        except Exception:
            logger.warning("Invalid cron expression for schedule %s: %s",
                           schedule.id, schedule.cron_expression)
            return None
    return None


@app.task(name="darkdisco.pipeline.worker.run_scheduled_reports")
def run_scheduled_reports():
    """Check for due report schedules and generate PDFs."""
    from darkdisco.common.models import GeneratedReport, ReportSchedule, ReportTemplate

    session = _get_sync_session()
    try:
        now = datetime.now(timezone.utc)
        schedules = session.execute(
            select(ReportSchedule)
            .where(
                ReportSchedule.enabled == True,  # noqa: E712
                ReportSchedule.next_run_at <= now,
            )
        ).scalars().all()

        if not schedules:
            return {"generated": 0}

        generated = 0
        for schedule in schedules:
            try:
                generate_scheduled_report.delay(schedule.id)
                generated += 1
            except Exception:
                logger.exception("Failed to dispatch report generation for schedule %s", schedule.id)

        return {"generated": generated}
    finally:
        session.close()


@app.task(name="darkdisco.pipeline.worker.generate_scheduled_report",
          bind=True, max_retries=2, soft_time_limit=600, time_limit=900)
def generate_scheduled_report(self, schedule_id: str):
    """Generate a single scheduled report: create PDF, store in S3, update schedule."""
    from uuid import uuid4 as _uuid4

    import boto3

    from darkdisco.common.models import GeneratedReport, ReportSchedule, ReportTemplate
    from darkdisco.reporting.engine import generate_pdf

    session = _get_sync_session()
    try:
        schedule = session.get(ReportSchedule, schedule_id)
        if not schedule or not schedule.enabled:
            return {"status": "skipped", "reason": "not found or disabled"}

        template = session.get(ReportTemplate, schedule.template_id)
        if not template:
            return {"status": "skipped", "reason": "template not found"}

        config = template.config or {}
        date_from, date_to = _compute_date_range(schedule.date_range_mode.value)

        # Generate PDF using async engine via event loop bridge
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
        from sqlalchemy.orm import sessionmaker as async_sessionmaker

        async_engine = create_async_engine(settings.database_url, pool_pre_ping=True)

        async def _generate():
            async with AsyncSession(async_engine) as async_session:
                pdf_bytes = await generate_pdf(
                    async_session,
                    title=config.get("title", schedule.name),
                    date_from=date_from,
                    date_to=date_to,
                    client_id=config.get("client_id"),
                    institution_id=config.get("institution_id"),
                    severities=config.get("severities"),
                    statuses=config.get("statuses"),
                    sections=config.get("sections", {}),
                    chart_options=config.get("charts", {}),
                )
            await async_engine.dispose()
            return pdf_bytes

        loop = _get_or_create_loop()
        pdf_bytes = loop.run_until_complete(_generate())

        # Store in S3
        report_id = str(_uuid4())
        s3_key = f"reports/{schedule.owner_id}/{report_id}.pdf"

        s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
        )
        s3.put_object(
            Bucket=settings.s3_bucket,
            Key=s3_key,
            Body=pdf_bytes,
            ContentType="application/pdf",
        )

        # Record in DB
        generated_report = GeneratedReport(
            id=report_id,
            schedule_id=schedule.id,
            template_id=schedule.template_id,
            owner_id=schedule.owner_id,
            title=config.get("title", schedule.name),
            s3_key=s3_key,
            file_size=len(pdf_bytes),
            date_range_mode=schedule.date_range_mode.value,
            date_from=date_from,
            date_to=date_to,
            status="completed",
        )
        session.add(generated_report)

        # Update schedule timing
        now = datetime.now(timezone.utc)
        schedule.last_run_at = now
        schedule.next_run_at = _compute_next_run(schedule)

        session.commit()

        # Send email if delivery method includes email
        if schedule.delivery_method.value in ("email", "both") and schedule.recipients:
            _send_report_email(schedule, pdf_bytes, config.get("title", schedule.name))

        logger.info("Generated scheduled report %s for schedule %s", report_id, schedule_id)
        return {"status": "completed", "report_id": report_id}

    except Exception as exc:
        session.rollback()
        # Record failure
        try:
            generated_report = GeneratedReport(
                schedule_id=schedule_id,
                owner_id=schedule.owner_id if schedule else "unknown",
                template_id=schedule.template_id if schedule else None,
                title="Failed generation",
                s3_key="",
                status="failed",
                error_message=str(exc),
            )
            session.add(generated_report)
            session.commit()
        except Exception:
            logger.exception("Failed to record report generation failure")
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))
    finally:
        session.close()


def _send_report_email(schedule, pdf_bytes: bytes, title: str):
    """Send generated report via email to recipients."""
    import smtplib
    from email.message import EmailMessage

    if not settings.smtp_host:
        logger.warning("SMTP not configured, skipping email delivery for schedule %s", schedule.id)
        return

    msg = EmailMessage()
    msg["Subject"] = f"Scheduled Report: {title}"
    msg["From"] = settings.smtp_from_address
    msg["To"] = ", ".join(schedule.recipients)
    msg.set_content(
        f"Your scheduled report '{title}' has been generated.\n\n"
        f"Date range: {schedule.date_range_mode.value}\n"
        f"Please find the PDF attached."
    )
    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=f"{title.replace(' ', '_')}.pdf",
    )

    try:
        if settings.smtp_use_tls:
            server = smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port)
        else:
            server = smtplib.SMTP(settings.smtp_host, settings.smtp_port)
            if settings.smtp_use_starttls:
                server.starttls()
        if settings.smtp_username:
            server.login(settings.smtp_username, settings.smtp_password)
        server.send_message(msg)
        server.quit()
        logger.info("Sent report email for schedule %s to %s", schedule.id, schedule.recipients)
    except Exception:
        logger.exception("Failed to send report email for schedule %s", schedule.id)
