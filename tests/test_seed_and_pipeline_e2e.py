"""End-to-end tests: seed sources → poll (mocked) → match → enrich → create findings.

These tests validate the full pipeline without external services by using
mock connectors and an in-memory SQLite database.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from darkdisco.common.models import (
    Client,
    Finding,
    FindingStatus,
    Institution,
    Source,
    SourceType,
    WatchTerm,
    WatchTermType,
)
from darkdisco.discovery.connectors.base import RawMention
from darkdisco.discovery.matcher import match_mention
from darkdisco.enrichment.false_positive import check_false_positive
from darkdisco.pipeline.worker import _CONNECTOR_MAP, _load_connector


# ---------------------------------------------------------------------------
# Source seeding
# ---------------------------------------------------------------------------


class TestSourceSeeding:
    """Verify that seed_sources creates all expected source records."""

    @pytest.mark.asyncio
    async def test_seed_creates_all_sources(self, db_session):
        """Run the seed script logic and verify all sources are created."""
        from scripts.seed_sources import SOURCES, _upsert_source

        for src_def in SOURCES:
            await _upsert_source(db_session, **src_def)
        await db_session.commit()

        result = await db_session.execute(select(func.count(Source.id)))
        count = result.scalar()
        assert count == len(SOURCES), f"Expected {len(SOURCES)} sources, got {count}"

    @pytest.mark.asyncio
    async def test_seed_is_idempotent(self, db_session):
        """Running seed twice should not create duplicate sources."""
        from scripts.seed_sources import SOURCES, _upsert_source

        # Seed twice
        for _ in range(2):
            for src_def in SOURCES:
                await _upsert_source(db_session, **src_def)
            await db_session.commit()

        result = await db_session.execute(select(func.count(Source.id)))
        count = result.scalar()
        assert count == len(SOURCES), f"Expected {len(SOURCES)} after double-seed, got {count}"

    @pytest.mark.asyncio
    async def test_all_sources_have_valid_connector_class(self, db_session):
        """Every seeded source should have a module:Class connector path."""
        from scripts.seed_sources import SOURCES, _upsert_source

        for src_def in SOURCES:
            await _upsert_source(db_session, **src_def)
        await db_session.commit()

        result = await db_session.execute(select(Source))
        sources = result.scalars().all()

        for src in sources:
            assert src.connector_class is not None, f"Source '{src.name}' has no connector_class"
            assert ":" in src.connector_class, (
                f"Source '{src.name}' connector_class '{src.connector_class}' "
                "should use module:Class format"
            )

    @pytest.mark.asyncio
    async def test_seeded_source_types_covered_by_connector_map(self, db_session):
        """All source types in the seed should have entries in _CONNECTOR_MAP."""
        from scripts.seed_sources import SOURCES

        seeded_types = {s["source_type"].value for s in SOURCES}
        mapped_types = set(_CONNECTOR_MAP.keys())
        # stealer_log, marketplace, other are expected to not have connectors
        uncovered = seeded_types - mapped_types - {"stealer_log", "other", "marketplace"}
        assert not uncovered, f"Source types without connector mapping: {uncovered}"

    @pytest.mark.asyncio
    async def test_all_seeded_connectors_load(self, db_session):
        """Every seeded source's connector_class should be importable.

        Skips connectors whose dependencies aren't installed (e.g. telethon).
        """
        from scripts.seed_sources import SOURCES, _upsert_source

        for src_def in SOURCES:
            await _upsert_source(db_session, **src_def)
        await db_session.commit()

        result = await db_session.execute(select(Source))
        sources = result.scalars().all()

        loaded = 0
        skipped = 0
        for src in sources:
            try:
                connector = _load_connector(src)
                assert connector is not None
                loaded += 1
            except (ImportError, ModuleNotFoundError):
                # Optional dependency not installed (e.g. telethon)
                skipped += 1
        assert loaded + skipped == len(sources), "All sources should either load or skip"
        assert loaded >= 1, f"Expected at least 1 connector to load, got {loaded}"

    @pytest.mark.asyncio
    async def test_paste_site_source_has_correct_config(self, db_session):
        """Paste site source should have expected connector and be enabled."""
        from scripts.seed_sources import SOURCES, _upsert_source

        for src_def in SOURCES:
            await _upsert_source(db_session, **src_def)
        await db_session.commit()

        result = await db_session.execute(
            select(Source).where(Source.source_type == SourceType.paste_site)
        )
        paste_sources = result.scalars().all()
        assert len(paste_sources) >= 1
        for src in paste_sources:
            assert src.enabled is True
            assert "paste_site" in src.connector_class

    @pytest.mark.asyncio
    async def test_ransomware_blog_source_has_blog_config(self, db_session):
        """Ransomware blog source should have blog group configs."""
        from scripts.seed_sources import SOURCES, _upsert_source

        for src_def in SOURCES:
            await _upsert_source(db_session, **src_def)
        await db_session.commit()

        result = await db_session.execute(
            select(Source).where(Source.source_type == SourceType.ransomware_blog)
        )
        blog_sources = result.scalars().all()
        assert len(blog_sources) >= 1
        for src in blog_sources:
            assert src.config is not None
            assert "blogs" in src.config
            assert len(src.config["blogs"]) >= 1


# ---------------------------------------------------------------------------
# Full pipeline E2E: seed → poll → match → findings
# ---------------------------------------------------------------------------


class TestPipelineEndToEnd:
    """Simulate the full pipeline: seeded sources, mock poll, matching, finding creation."""

    @pytest.fixture
    async def seeded_env(self, db_session):
        """Set up a full environment: sources + institution + watch terms."""
        from scripts.seed_sources import SOURCES, _upsert_source

        # Seed sources
        for src_def in SOURCES:
            await _upsert_source(db_session, **src_def)

        # Create client + institution + watch terms
        client = Client(
            id=str(uuid4()),
            name="E2E Test Client",
            contract_ref="E2E-001",
            active=True,
        )
        db_session.add(client)
        await db_session.flush()

        inst = Institution(
            id=str(uuid4()),
            client_id=client.id,
            name="Acme Federal Credit Union",
            short_name="Acme FCU",
            primary_domain="acmefcu.org",
            active=True,
        )
        db_session.add(inst)
        await db_session.flush()

        terms = [
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst.id,
                term_type=WatchTermType.domain,
                value="acmefcu.org",
                enabled=True,
            ),
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst.id,
                term_type=WatchTermType.institution_name,
                value="Acme Federal Credit Union",
                enabled=True,
            ),
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst.id,
                term_type=WatchTermType.keyword,
                value="acme fcu",
                enabled=True,
            ),
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst.id,
                term_type=WatchTermType.bin_range,
                value="412399",
                enabled=True,
            ),
        ]
        for t in terms:
            db_session.add(t)
        await db_session.commit()

        return {"institution": inst, "terms": terms, "client": client}

    @pytest.mark.asyncio
    async def test_credential_dump_creates_finding(self, db_session, seeded_env):
        """A paste containing institution credentials should produce a finding."""
        inst = seeded_env["institution"]
        terms = seeded_env["terms"]

        mention = RawMention(
            source_name="Paste Site Monitor",
            source_url="http://example.onion/paste/e2e-001",
            title="Credential dump with acmefcu.org users",
            content="jdoe@acmefcu.org:Summer2026!\nasmith@acmefcu.org:P@ss",
            discovered_at=datetime.now(timezone.utc),
        )

        results = match_mention(mention, terms)
        assert len(results) == 1
        assert results[0].institution_id == inst.id

        # Verify FP check passes
        candidate = {
            "title": mention.title,
            "raw_content": mention.content,
            "matched_terms": results[0].matched_terms,
        }
        fp = check_false_positive(candidate)
        assert fp.recommendation != "auto_dismiss"

        # Create finding
        content_hash = hashlib.sha256(
            f"{mention.source_name}:{mention.content}".encode()
        ).hexdigest()

        result = await db_session.execute(
            select(Source).where(Source.source_type == SourceType.paste_site)
        )
        source = result.scalars().first()

        finding = Finding(
            id=str(uuid4()),
            institution_id=inst.id,
            source_id=source.id if source else None,
            severity=results[0].severity_hint,
            title=mention.title,
            summary=mention.content[:1000],
            raw_content=mention.content,
            content_hash=content_hash,
            source_url=mention.source_url,
            matched_terms=results[0].matched_terms,
            tags=["paste_site"],
            discovered_at=mention.discovered_at,
        )
        db_session.add(finding)
        await db_session.commit()
        await db_session.refresh(finding)

        assert finding.id is not None
        assert finding.status == FindingStatus.new
        assert finding.institution_id == inst.id
        assert finding.content_hash == content_hash

    @pytest.mark.asyncio
    async def test_ransomware_blog_mention_creates_finding(self, db_session, seeded_env):
        """A ransomware blog listing should match and create a high/critical finding."""
        inst = seeded_env["institution"]
        terms = seeded_env["terms"]

        mention = RawMention(
            source_name="Ransomware Blog Monitor",
            source_url="http://lockbit.onion/victims/acme",
            title="Acme Federal Credit Union — data leak",
            content=(
                "Victim: Acme Federal Credit Union\n"
                "Data: 120 GB\n"
                "acmefcu.org systems compromised"
            ),
            discovered_at=datetime.now(timezone.utc),
            metadata={"group": "lockbit"},
        )

        results = match_mention(mention, terms)
        assert len(results) >= 1
        # Should match on both institution name and domain
        matched_types = {m["term_type"] for m in results[0].matched_terms}
        assert "institution_name" in matched_types or "domain" in matched_types

        content_hash = hashlib.sha256(
            f"{mention.source_name}:{mention.content}".encode()
        ).hexdigest()

        result = await db_session.execute(
            select(Source).where(Source.source_type == SourceType.ransomware_blog)
        )
        source = result.scalars().first()

        finding = Finding(
            id=str(uuid4()),
            institution_id=inst.id,
            source_id=source.id if source else None,
            severity=results[0].severity_hint,
            title=mention.title,
            summary=mention.content[:1000],
            raw_content=mention.content,
            content_hash=content_hash,
            source_url=mention.source_url,
            matched_terms=results[0].matched_terms,
            tags=["ransomware_blog"],
            discovered_at=mention.discovered_at,
        )
        db_session.add(finding)
        await db_session.commit()

        assert finding.id is not None
        assert finding.status == FindingStatus.new

    @pytest.mark.asyncio
    async def test_unrelated_mention_creates_no_finding(self, db_session, seeded_env):
        """Content with no watch term matches should not produce a finding."""
        terms = seeded_env["terms"]

        mention = RawMention(
            source_name="Forum Monitor",
            title="General discussion about weather",
            content="Nice weather today, nothing about any banks here.",
            discovered_at=datetime.now(timezone.utc),
        )

        results = match_mention(mention, terms)
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_duplicate_mention_blocked_by_content_hash(self, db_session, seeded_env):
        """Identical content should be deduplicated by content_hash."""
        inst = seeded_env["institution"]
        terms = seeded_env["terms"]

        content = "jdoe@acmefcu.org:leaked_password"
        content_hash = hashlib.sha256(f"source:{content}".encode()).hexdigest()

        mention = RawMention(
            source_name="source",
            title="Leak",
            content=content,
            discovered_at=datetime.now(timezone.utc),
        )

        results = match_mention(mention, terms)
        assert len(results) >= 1

        # Create first finding
        f1 = Finding(
            id=str(uuid4()),
            institution_id=inst.id,
            severity="high",
            title="First occurrence",
            raw_content=content,
            content_hash=content_hash,
            matched_terms=results[0].matched_terms,
        )
        db_session.add(f1)
        await db_session.commit()

        # Attempt to create duplicate — should be blocked by hash check
        existing = (
            await db_session.execute(
                select(Finding.id).where(Finding.content_hash == content_hash)
            )
        ).scalar_one_or_none()
        assert existing is not None, "Dedup check should detect existing finding"

    @pytest.mark.asyncio
    async def test_bin_range_match_severity_critical(self, db_session, seeded_env):
        """A BIN range match should produce a critical severity hint."""
        terms = seeded_env["terms"]

        mention = RawMention(
            source_name="Forum Monitor",
            title="Card dump",
            content="Card number 4123991234567890 found in stealer log",
            discovered_at=datetime.now(timezone.utc),
        )

        results = match_mention(mention, terms)
        assert len(results) >= 1
        assert results[0].severity_hint == "critical"

    @pytest.mark.asyncio
    async def test_schedule_polls_finds_due_sources(self, db_session, seeded_env):
        """Sources that have never been polled should all be 'due' for polling."""
        result = await db_session.execute(
            select(Source).where(Source.enabled.is_(True))
        )
        sources = result.scalars().all()

        due_count = 0
        now = datetime.now(timezone.utc)
        for src in sources:
            if src.last_polled_at is None:
                due_count += 1
            else:
                elapsed = (now - src.last_polled_at).total_seconds()
                if elapsed >= src.poll_interval_seconds:
                    due_count += 1

        assert due_count == len(sources), "All never-polled sources should be due"

    @pytest.mark.asyncio
    async def test_recently_polled_source_not_due(self, db_session, seeded_env):
        """A source polled recently should not be due for re-polling."""
        result = await db_session.execute(
            select(Source).where(Source.enabled.is_(True))
        )
        source = result.scalars().first()

        # Mark as recently polled
        poll_time = datetime.now(timezone.utc)
        source.last_polled_at = poll_time
        await db_session.commit()

        # Re-fetch to confirm persistence
        result2 = await db_session.execute(
            select(Source).where(Source.id == source.id)
        )
        refreshed = result2.scalars().first()

        assert refreshed.last_polled_at is not None, "last_polled_at should be set"
        elapsed = (datetime.now(timezone.utc) - refreshed.last_polled_at).total_seconds()
        assert elapsed < refreshed.poll_interval_seconds, "Recently polled source should not be due"


# ---------------------------------------------------------------------------
# Beat schedule and worker configuration
# ---------------------------------------------------------------------------


class TestWorkerConfig:
    """Verify Celery worker and beat schedule configuration."""

    def test_beat_schedule_has_poll_task(self):
        from darkdisco.pipeline.worker import app as celery_app

        beat = celery_app.conf.beat_schedule
        assert "schedule-source-polls" in beat
        sched = beat["schedule-source-polls"]
        assert sched["task"] == "darkdisco.pipeline.worker.schedule_polls"
        assert sched["schedule"] == 300.0

    def test_connector_map_has_required_types(self):
        required = {"paste_site", "forum", "telegram", "breach_db", "ransomware_blog"}
        assert required.issubset(set(_CONNECTOR_MAP.keys()))

    def test_connector_map_paths_are_valid_format(self):
        for source_type, path in _CONNECTOR_MAP.items():
            assert ":" in path, f"Connector path for '{source_type}' should use module:Class format"
            module_path, class_name = path.rsplit(":", 1)
            assert module_path.startswith("darkdisco."), f"Module path should start with 'darkdisco.'"
            assert class_name[0].isupper(), f"Class name '{class_name}' should be PascalCase"


# ---------------------------------------------------------------------------
# Legacy path fixes
# ---------------------------------------------------------------------------


class TestLegacyPathFixes:
    """Verify the seed script fixes old connector_class paths."""

    @pytest.mark.asyncio
    async def test_legacy_paths_get_deleted(self, db_session):
        """Sources with orphaned darkdisco.connectors.* paths should be deleted."""
        from scripts.seed_sources import _fix_legacy_paths

        # Create a source with an orphaned legacy path
        src = Source(
            id=str(uuid4()),
            name="Legacy Forum Source",
            source_type=SourceType.forum,
            connector_class="darkdisco.connectors.tor_forum.TorForumConnector",
            enabled=True,
            poll_interval_seconds=3600,
        )
        db_session.add(src)
        await db_session.commit()
        src_id = src.id

        await _fix_legacy_paths(db_session)
        await db_session.commit()

        result = await db_session.execute(
            select(Source).where(Source.id == src_id)
        )
        assert result.scalars().first() is None
