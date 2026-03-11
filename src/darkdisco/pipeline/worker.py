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
            # Serialize mentions for the matching task
            serialized = [
                {
                    "source_name": m.source_name,
                    "source_url": m.source_url,
                    "title": m.title,
                    "content": m.content,
                    "author": m.author,
                    "discovered_at": m.discovered_at.isoformat() if m.discovered_at else None,
                    "metadata": m.metadata,
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


@app.task(name="darkdisco.pipeline.worker.run_matching")
def run_matching(source_id: str, raw_mentions: list[dict]):
    """Match raw mentions against all active watch terms, create findings, trigger alerts."""
    from darkdisco.common.models import Finding, Source, WatchTerm
    from darkdisco.discovery.connectors.base import RawMention
    from darkdisco.discovery.matcher import match_mention

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
            logger.info("No active watch terms, skipping matching")
            return {"findings_created": 0}

        findings_created = 0

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

            # Content hash for dedup
            content_hash = hashlib.sha256(
                f"{mention.source_name}:{mention.content}".encode()
            ).hexdigest()

            # Check for duplicate
            existing = session.execute(
                select(Finding.id).where(Finding.content_hash == content_hash)
            ).scalar_one_or_none()

            if existing:
                logger.debug("Duplicate finding (hash=%s), skipping", content_hash[:12])
                continue

            # Match against watch terms
            match_results = match_mention(mention, watch_terms)

            for result in match_results:
                finding = Finding(
                    institution_id=result.institution_id,
                    source_id=source_id,
                    severity=result.severity_hint,
                    title=mention.title or f"Mention from {source.name}",
                    summary=mention.content[:1000] if mention.content else None,
                    raw_content=mention.content,
                    content_hash=content_hash,
                    source_url=mention.source_url,
                    matched_terms=result.matched_terms,
                    tags=[source.source_type.value],
                    metadata_=mention.metadata,
                    discovered_at=mention.discovered_at,
                )
                session.add(finding)
                findings_created += 1

        session.commit()

        logger.info(
            "Matching complete for source %s: %d mentions → %d findings",
            source.name if source else source_id,
            len(raw_mentions),
            findings_created,
        )

        # Trigger alert evaluation for new findings
        if findings_created > 0:
            evaluate_alerts.delay()

        return {"findings_created": findings_created, "mentions_processed": len(raw_mentions)}
    finally:
        session.close()


@app.task(name="darkdisco.pipeline.worker.evaluate_alerts")
def evaluate_alerts():
    """Check new findings against alert rules and create notifications."""
    from darkdisco.common.models import AlertRule, Finding, FindingStatus, Notification

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
        severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}

        for finding in new_findings:
            for rule in rules:
                if not _rule_matches(rule, finding, severity_rank):
                    continue

                notification = Notification(
                    user_id=rule.owner_id,
                    alert_rule_id=rule.id,
                    finding_id=finding.id,
                    title=f"Alert: {finding.title}",
                    message=f"[{finding.severity.value.upper()}] {finding.summary or finding.title}",
                )
                session.add(notification)
                notifications_created += 1

        session.commit()
        logger.info("Created %d notifications from %d new findings", notifications_created, len(new_findings))
        return {"notifications_created": notifications_created}
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
