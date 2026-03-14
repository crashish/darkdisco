"""Celery pipeline worker — poll sources, match watch terms, create findings, trigger alerts.

This is the backbone that all connectors feed into. The beat schedule drives
periodic polling; each poll fans out into matching and finding creation.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import logging
from datetime import datetime, timezone

from celery import Celery
from sqlalchemy import select
from sqlalchemy.orm import Session

from darkdisco.config import settings

logger = logging.getLogger(__name__)

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
<<<<<<< Updated upstream
        "sync-trapline-watchlist": {
            "task": "darkdisco.pipeline.worker.sync_trapline_watchlist",
            "schedule": 3600.0,  # 1 hour
=======
        "download-pending-files": {
            "task": "darkdisco.pipeline.worker.download_pending_files",
            "schedule": 600.0,  # 10 minutes
>>>>>>> Stashed changes
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


def _load_connector_for_download(source):
    """Load a Telegram connector with a separate session file for downloads.

    Copies the primary session file to a _download variant so the download task
    doesn't contend with the polling task over SQLite locks.
    """
    import shutil
    from pathlib import Path
    from darkdisco.config import settings

    config = dict(source.config or {})

    # Determine the primary session path and create a download-specific copy
    primary_session = config.get("session_name", settings.telegram_session_name)
    primary_path = Path(primary_session).expanduser()
    download_session = str(primary_path) + "_download"
    download_path = Path(download_session + ".session")
    primary_session_file = Path(str(primary_path) + ".session")

    # Copy session file ONLY if download copy doesn't exist yet.
    # Never re-copy — the download session maintains its own auth state,
    # and copying while the primary is open causes SQLite lock errors.
    if primary_session_file.exists() and not download_path.exists():
        shutil.copy2(str(primary_session_file), str(download_path))
        logger.info("Initialized download session from %s", download_path)

    config["session_name"] = download_session

    # Instantiate connector with overridden config
    class_path = source.connector_class or _CONNECTOR_MAP.get(source.source_type.value)
    if not class_path:
        raise ValueError(f"No connector for source type {source.source_type}")
    module_path, class_name = class_path.rsplit(":", 1) if ":" in class_path else class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(config=config)


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
         soft_time_limit=900, time_limit=1200)
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

        connector = _load_connector(source)
        since = source.last_polled_at

        try:
            # Bridge async connector to sync Celery task
            mentions = asyncio.run(_poll_async(connector, since))
        except Exception as exc:
            source.last_error = str(exc)[:2000]
            session.commit()
            logger.exception("Failed to poll source %s", source.name)
            raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))

        # Persist updated config (e.g. high-water marks from connectors)
        # flag_modified is needed because SQLAlchemy doesn't detect in-place JSONB mutations
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(source, "config")

        # Update poll timestamp and clear errors
        source.last_polled_at = datetime.now(timezone.utc)
        source.last_error = None
        session.commit()

        logger.info("Polled source %s: %d mentions", source.name, len(mentions))

        if mentions:
            # Process file attachments before serialization
            mentions = _process_file_mentions(mentions)

            # Persist all mentions to raw_mentions table for browsing
            _persist_raw_mentions(session, source_id, mentions)

            # Serialize mentions for the matching task
            # (file_data bytes are removed — text content extracted above)
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
        session.close()


def _persist_raw_mentions(session: Session, source_id: str, mentions: list) -> None:
    """Store all collected mentions in raw_mentions table for browsing."""
    from darkdisco.common.models import RawMention as RawMentionModel
    from uuid import uuid4

    for m in mentions:
        content_hash = hashlib.sha256(
            f"{m.source_name}:{m.content}".encode()
        ).hexdigest()

        # Skip if we already have this exact content
        existing = session.execute(
            select(RawMentionModel.id).where(
                RawMentionModel.content_hash == content_hash
            )
        ).scalar_one_or_none()
        if existing:
            continue

        # Strip file_data bytes from metadata before storing
        clean_metadata = {
            k: v for k, v in m.metadata.items()
            if k != "file_data"
        } if m.metadata else {}

        # Extract passwords from message content for archive mentions
        if m.content and clean_metadata.get("has_media"):
            from darkdisco.pipeline.files import extract_passwords
            passwords = extract_passwords(m.content)
            if passwords:
                clean_metadata["extracted_passwords"] = passwords

        row = RawMentionModel(
            id=str(uuid4()),
            source_id=source_id,
            content=m.content or "",
            content_hash=content_hash,
            source_url=m.source_url,
            metadata_=clean_metadata,
            collected_at=m.discovered_at or datetime.now(timezone.utc),
        )
        session.add(row)

    try:
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("Failed to persist raw mentions")


async def _poll_async(connector, since):
    """Run the async connector poll in a fresh event loop."""
    try:
        await connector.setup()
        return await connector.poll(since=since)
    finally:
        await connector.teardown()


def _process_file_mentions(mentions: list) -> list:
    """Process file attachments in mentions: extract archives, analyze contents.

    For each mention with file_data in metadata:
    1. If it's an archive (ZIP/RAR), extract and analyze contents
    2. Append extracted text to the mention content for matching
    3. Upload original + extracted files to S3
    4. Store file analysis metadata
    """
    from darkdisco.pipeline.files import (
        analyze_extracted_files,
        extract_archive,
        extract_passwords,
        is_archive,
        upload_to_s3,
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

    Reads extracted_file_contents and file hashes from metadata and
    persists them as normalized ExtractedFile rows linked to the mention.

    Returns the number of rows created.
    """
    from darkdisco.common.models import ExtractedFile

    extracted = metadata.get("extracted_file_contents")
    if not extracted or not isinstance(extracted, list):
        return 0

    file_sha256 = metadata.get("file_sha256", "")
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
            # Match the S3 key pattern from _process_file_mentions
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


@app.task(name="darkdisco.pipeline.worker.run_matching")
def run_matching(source_id: str, raw_mentions: list[dict]):
    """Match raw mentions against all active watch terms, enrich, filter, create findings."""
    from darkdisco.common.models import Finding, Source, WatchTerm
    from darkdisco.common.models import RawMention as RawMentionModel
    from darkdisco.discovery.connectors.base import RawMention
    from darkdisco.discovery.matcher import match_mention, recompute_highlights
    from darkdisco.enrichment import enrich_and_filter

    session = _get_sync_session()
    try:
        source = session.get(Source, source_id)
        if source is None:
            logger.error("Source %s not found for matching", source_id)
            return {"error": "source_not_found"}

        # Load all active watch terms
        watch_terms = session.execute(
            select(WatchTerm).where(WatchTerm.enabled.is_(True))
        ).scalars().all()

        if not watch_terms:
            logger.warning("No active watch terms configured — matching skipped. "
                           "Add watch terms via the API to start producing findings.")
            return {"findings_created": 0}

        # Log watch term coverage for debugging
        term_types = {}
        for wt in watch_terms:
            ttype = wt.term_type.value if hasattr(wt.term_type, 'value') else str(wt.term_type)
            term_types[ttype] = term_types.get(ttype, 0) + 1
        logger.info(
            "Matching %d mentions against %d watch terms (%s)",
            len(raw_mentions), len(watch_terms),
            ", ".join(f"{k}:{v}" for k, v in sorted(term_types.items())),
        )

        findings_created = 0
        findings_suppressed = 0
        mentions_matched = 0

        for raw in raw_mentions:
            mention = RawMention(
                source_name=raw["source_name"],
                source_url=raw.get("source_url"),
                title=raw.get("title", ""),
                content=raw.get("content", ""),
                author=raw.get("author"),
                discovered_at=datetime.fromisoformat(raw["discovered_at"]) if raw.get("discovered_at") else datetime.now(timezone.utc),
                metadata=raw.get("metadata", {}),
            )

            # Content hash for exact dedup
            content_hash = hashlib.sha256(
                f"{mention.source_name}:{mention.content}".encode()
            ).hexdigest()

            # Check for exact duplicate
            existing = session.execute(
                select(Finding.id).where(Finding.content_hash == content_hash)
            ).scalar_one_or_none()

            if existing:
                logger.debug("Duplicate finding (hash=%s), skipping", content_hash[:12])
                continue

            # Persist as a raw_mentions row for browsing and ExtractedFile linkage
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

            # Match against watch terms
            match_results = match_mention(mention, watch_terms)

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
                candidate = {
                    "institution_id": result.institution_id,
                    "source_id": source_id,
                    "severity": result.severity_hint,
                    "title": mention.title or f"Mention from {source.name}",
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
                merged_metadata = dict(mention.metadata)
                if enrichment.enrichment_metadata:
                    merged_metadata["enrichment"] = enrichment.enrichment_metadata

                finding = Finding(
                    institution_id=result.institution_id,
                    source_id=source_id,
                    severity=severity,
                    title=mention.title or f"Mention from {source.name}",
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
                db_mention.promoted_to_finding_id = finding.id

        session.commit()

        logger.info(
            "Matching complete for source %s: %d mentions → %d matched → %d findings (%d suppressed)",
            source.name if source else source_id,
            len(raw_mentions),
            mentions_matched,
            findings_created,
            findings_suppressed,
        )

        # Trigger alert evaluation for new findings
        if findings_created > 0:
            evaluate_alerts.delay()

        return {
            "findings_created": findings_created,
            "findings_suppressed": findings_suppressed,
            "mentions_processed": len(raw_mentions),
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
<<<<<<< Updated upstream
# Trapline watchlist sync
# ---------------------------------------------------------------------------


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
=======
# Async file download task — downloads large files from pending mentions
# ---------------------------------------------------------------------------

@app.task(name="darkdisco.pipeline.worker.download_pending_files",
          soft_time_limit=3600, time_limit=3900)
def download_pending_files(batch_size: int = 10):
    """Download large files from mentions marked as download_status=pending.

    Runs periodically to fetch files that were too large for inline download
    during polling. Downloads to temp disk, uploads to S3, updates mention metadata.

    Uses a Redis lock to prevent concurrent download tasks from contending
    over the Telethon SQLite session file.
    """
    import redis as _redis
    from darkdisco.config import settings
    from darkdisco.common.models import RawMention as RawMentionModel, Source

    # Acquire exclusive lock — skip if another download task is already running
    r = _redis.from_url(settings.celery_broker_url)
    lock = r.lock("darkdisco:download_files_lock", timeout=3600, blocking=False)
    if not lock.acquire(blocking=False):
        logger.info("Another download task is already running, skipping")
        return {"skipped": True}

    session = _get_sync_session()
    try:
        # Find mentions with pending downloads
        stmt = (
            select(RawMentionModel)
            .where(RawMentionModel.metadata_["download_status"].astext == "pending")
            .limit(batch_size)
        )
        mentions = session.execute(stmt).scalars().all()

        if not mentions:
            return {"downloaded": 0, "pending": 0}

        logger.info("Found %d mentions with pending file downloads", len(mentions))

        # Group by source to reuse Telegram connections
        by_source: dict[str, list] = {}
        for m in mentions:
            by_source.setdefault(m.source_id, []).append(m)

        downloaded = 0
        failed = 0

        for source_id, source_mentions in by_source.items():
            source = session.get(Source, source_id)
            if not source or source.source_type.value != "telegram":
                continue

            try:
                connector = _load_connector_for_download(source)
                results = asyncio.run(_download_files_async(
                    connector, source_mentions, session
                ))
                downloaded += results["downloaded"]
                failed += results["failed"]
            except Exception:
                logger.exception("Failed to download files for source %s", source_id)
                failed += len(source_mentions)

        return {"downloaded": downloaded, "failed": failed}
    finally:
        session.close()
        try:
            lock.release()
        except Exception:
            pass


def _match_extracted_content(mention, text_content: str, source_id: str, session):
    """Match extracted archive text against watch terms and create findings."""
    if not text_content or len(text_content) < 10:
        return

    from darkdisco.common.models import Finding, WatchTerm
    from darkdisco.discovery.connectors.base import RawMention as RawMentionDTO
    from darkdisco.discovery.matcher import match_mention
    from sqlalchemy.orm.attributes import flag_modified

    try:
        watch_terms = session.execute(
            select(WatchTerm).where(WatchTerm.enabled.is_(True))
        ).scalars().all()

        if not watch_terms:
            return

        # Create a synthetic RawMention for the matcher
        synthetic = RawMentionDTO(
            source_name="archive_extraction",
            title=f"Archive contents: {mention.metadata_.get('file_name', 'unknown')}",
            content=text_content[:200000],  # Cap for matching
            source_url=mention.source_url,
            metadata=mention.metadata_ or {},
        )

        match_results = match_mention(synthetic, watch_terms)

        if not match_results:
            return

        logger.info(
            "Archive content matched %d institution(s) for mention %s",
            len(match_results), mention.id,
        )

        for result in match_results:
            # Check for duplicate finding
            content_hash = hashlib.sha256(
                f"{source_id}:{mention.id}:{result.institution_id}".encode()
            ).hexdigest()

            existing = session.execute(
                select(Finding).where(Finding.content_hash == content_hash)
            ).scalar_one_or_none()

            if existing:
                continue

            finding = Finding(
                institution_id=result.institution_id,
                source_id=source_id,
                severity=result.severity_hint,
                title=f"Watch term match in archive: {mention.metadata_.get('file_name', 'unknown')}",
                summary=f"Matched terms: {', '.join(t['value'] for t in result.matched_terms[:5])}",
                raw_content=text_content[:10000],
                content_hash=content_hash,
                metadata_={
                    "match_source": "archive_extraction",
                    "mention_id": mention.id,
                    "matched_terms": result.matched_terms,
                    "file_name": mention.metadata_.get("file_name"),
                    "file_sha256": mention.metadata_.get("file_sha256"),
                },
            )
            session.add(finding)
            logger.info(
                "Created finding from archive content: institution=%s terms=%s",
                result.institution_id,
                ", ".join(t["value"] for t in result.matched_terms[:3]),
            )

        session.commit()

    except Exception:
        logger.exception("Watch term matching on extracted content failed for mention %s", mention.id)


async def _download_files_async(connector, mentions, session):
    """Download pending files using the Telegram connector."""
    import tempfile
    import hashlib as _hashlib
    import boto3
    from botocore.config import Config as BotoConfig
    from pathlib import Path
    from sqlalchemy.orm.attributes import flag_modified

    from darkdisco.config import settings

    await connector.setup()
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

        downloaded = 0
        failed = 0

        for mention in mentions:
            meta = mention.metadata_ or {}
            chat_id = meta.get("chat_id")
            message_id = meta.get("message_id")

            if not chat_id or not message_id:
                logger.warning("Mention %s missing chat_id/message_id for download", mention.id)
                meta["download_status"] = "error"
                meta["download_error"] = "missing chat_id or message_id"
                mention.metadata_ = meta
                flag_modified(mention, "metadata_")
                failed += 1
                continue

            tmp_path = None
            try:
                from telethon.tl.types import PeerChannel
                entity = await connector._client.get_entity(PeerChannel(int(chat_id)))
                msgs = await connector._client.get_messages(entity, ids=[int(message_id)])
                msg = msgs[0] if msgs else None

                if not msg or not msg.file:
                    meta["download_status"] = "error"
                    meta["download_error"] = "message or file not found"
                    mention.metadata_ = meta
                    flag_modified(mention, "metadata_")
                    failed += 1
                    continue

                with tempfile.NamedTemporaryFile(delete=False, dir="/tmp") as tmp:
                    tmp_path = tmp.name

                dl_path = await connector._client.download_media(msg, file=tmp_path)
                if not dl_path:
                    raise RuntimeError("download_media returned None")

                # Stream SHA256
                sha = _hashlib.sha256()
                with open(dl_path, "rb") as fh:
                    while chunk := fh.read(8 * 1024 * 1024):
                        sha.update(chunk)
                file_sha = sha.hexdigest()

                fname = meta.get("file_name") or msg.file.name or "unnamed"
                s3_key = f"files/{file_sha[:8]}/{fname}"

                s3.upload_file(dl_path, settings.s3_bucket, s3_key)

                meta["s3_key"] = s3_key
                meta["file_sha256"] = file_sha
                meta["download_status"] = "stored"
                mention.metadata_ = meta
                flag_modified(mention, "metadata_")
                session.commit()

                # Archive extraction post-download
                try:
                    from darkdisco.pipeline.files import (
                        is_archive,
                        extract_archive_from_path,
                        analyze_extracted_files,
                        upload_to_s3,
                    )

                    if is_archive(fname):
                        passwords = meta.get("extracted_passwords", [])
                        extracted = extract_archive_from_path(dl_path, fname, passwords)
                        if extracted:
                            analysis = analyze_extracted_files(extracted)
                            sha_prefix = file_sha[:8]
                            for ef in extracted:
                                if ef.is_text:
                                    ef_key = f"files/{sha_prefix}/extracted/{ef.sha256[:8]}/{ef.filename}"
                                    upload_to_s3(ef_key, ef.content)
                            meta["file_analysis"] = analysis.to_dict()

                            # Store searchable text on the mention content
                            # so the q= search parameter works on archive contents
                            if analysis.text_content:
                                # Strip null bytes — PostgreSQL text columns can't store \x00
                                search_text = analysis.text_content[:50000].replace("\x00", "")
                                mention.content = (mention.content or "") + "\n\n--- Extracted archive content ---\n" + search_text

                            mention.metadata_ = meta
                            flag_modified(mention, "metadata_")
                            session.commit()
                            logger.info(
                                "Extracted %d files from archive %s (%d text uploaded)",
                                len(extracted), fname,
                                sum(1 for ef in extracted if ef.is_text),
                            )

                            # Run watch term matching on extracted content
                            _match_extracted_content(
                                mention, analysis.text_content, mention.source_id, session
                            )
                except Exception:
                    logger.exception("Archive extraction failed for %s", fname)

                downloaded += 1
                logger.info(
                    "Downloaded file: %s (%s) -> s3://%s/%s",
                    fname,
                    f"{msg.file.size / (1024*1024):.1f}MB" if msg.file.size else "?",
                    settings.s3_bucket, s3_key,
                )

            except Exception:
                logger.exception(
                    "Failed to download file for mention %s (chat=%s msg=%s)",
                    mention.id, chat_id, message_id,
                )
                meta["download_status"] = "error"
                mention.metadata_ = meta
                flag_modified(mention, "metadata_")
                session.commit()
                failed += 1
            finally:
                if tmp_path:
                    Path(tmp_path).unlink(missing_ok=True)

            await asyncio.sleep(2)  # Rate limit between downloads

        return {"downloaded": downloaded, "failed": failed}
    finally:
        await connector.teardown()
>>>>>>> Stashed changes
