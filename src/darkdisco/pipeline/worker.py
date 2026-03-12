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


@app.task(name="darkdisco.pipeline.worker.poll_source", bind=True, max_retries=3)
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

        # Update poll timestamp and clear errors
        source.last_polled_at = datetime.now(timezone.utc)
        source.last_error = None
        session.commit()

        logger.info("Polled source %s: %d mentions", source.name, len(mentions))

        if mentions:
            # Process file attachments before serialization
            mentions = _process_file_mentions(mentions)

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

                # Append extracted text to mention content for matching
                if analysis.text_content:
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


@app.task(name="darkdisco.pipeline.worker.run_matching")
def run_matching(source_id: str, raw_mentions: list[dict]):
    """Match raw mentions against all active watch terms, enrich, filter, create findings."""
    from darkdisco.common.models import Finding, Source, WatchTerm
    from darkdisco.discovery.connectors.base import RawMention
    from darkdisco.discovery.matcher import match_mention
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
                # Build candidate finding data for enrichment
                candidate = {
                    "institution_id": result.institution_id,
                    "source_id": source_id,
                    "severity": result.severity_hint,
                    "title": mention.title or f"Mention from {source.name}",
                    "summary": mention.content[:1000] if mention.content else None,
                    "raw_content": mention.content,
                    "content_hash": content_hash,
                    "source_url": mention.source_url,
                    "matched_terms": result.matched_terms,
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
                    summary=mention.content[:1000] if mention.content else None,
                    raw_content=mention.content,
                    content_hash=content_hash,
                    source_url=mention.source_url,
                    matched_terms=result.matched_terms,
                    tags=[source.source_type.value],
                    metadata_=merged_metadata,
                    discovered_at=mention.discovered_at,
                )
                session.add(finding)
                findings_created += 1

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
