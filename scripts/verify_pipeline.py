#!/usr/bin/env python3
"""Verify the DarkDisco polling pipeline end-to-end.

Seeds initial sources, creates test institutions with watch terms, then
simulates a poll cycle through matching and finding creation — all against
a real (or test) database without requiring external services.

Usage:
    # Against the configured database (docker-compose postgres):
    python scripts/verify_pipeline.py

    # Against an in-memory SQLite DB (no docker required):
    python scripts/verify_pipeline.py --sqlite

Exit codes:
    0 — all checks passed
    1 — one or more checks failed
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from darkdisco.common.models import (
    Base,
    Client,
    Finding,
    FindingStatus,
    Institution,
    Severity,
    Source,
    SourceType,
    WatchTerm,
    WatchTermType,
)
from darkdisco.discovery.connectors.base import RawMention
from darkdisco.discovery.matcher import match_mention
from darkdisco.enrichment.false_positive import check_false_positive


def _header(msg: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {msg}")
    print(f"{'=' * 60}")


def _ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def _fail(msg: str) -> None:
    print(f"  ✗ {msg}")


async def verify(db_url: str) -> bool:
    """Run the full verification pipeline. Returns True if all checks pass."""
    engine = create_async_engine(db_url, echo=False, pool_pre_ping=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    passed = True

    # ── Step 1: Seed sources ──────────────────────────────────────────
    _header("Step 1: Seed initial sources")

    from scripts.seed_sources import SOURCES, _upsert_source

    async with session_factory() as session:
        source_ids: dict[str, str] = {}
        for src_def in SOURCES:
            src_id = await _upsert_source(session, **src_def)
            source_ids[src_def["name"]] = src_id
        await session.commit()

    async with session_factory() as session:
        result = await session.execute(select(Source).where(Source.enabled.is_(True)))
        sources = result.scalars().all()

    if len(sources) >= len(SOURCES):
        _ok(f"{len(sources)} sources seeded ({len(SOURCES)} expected)")
    else:
        _fail(f"Only {len(sources)} sources found, expected >= {len(SOURCES)}")
        passed = False

    # Verify connector_class paths are valid module:Class format
    for src in sources:
        if src.connector_class and ":" in src.connector_class:
            _ok(f"Source '{src.name}' → {src.connector_class}")
        elif src.connector_class:
            _fail(f"Source '{src.name}' has old-style connector path: {src.connector_class}")
            passed = False

    # ── Step 2: Verify connector loading ──────────────────────────────
    _header("Step 2: Verify connectors load without errors")

    from darkdisco.pipeline.worker import _load_connector

    for src in sources:
        if not src.connector_class:
            continue
        try:
            connector = _load_connector(src)
            _ok(f"Loaded {type(connector).__name__} for '{src.name}'")
        except Exception as exc:
            _fail(f"Failed to load connector for '{src.name}': {exc}")
            passed = False

    # ── Step 3: Seed a test institution + watch terms ─────────────────
    _header("Step 3: Seed test institution and watch terms")

    async with session_factory() as session:
        client = Client(
            id=str(uuid4()),
            name="Pipeline Verification Client",
            contract_ref="PV-001",
            active=True,
        )
        session.add(client)
        await session.flush()

        inst = Institution(
            id=str(uuid4()),
            client_id=client.id,
            name="Acme Federal Credit Union",
            short_name="Acme FCU",
            primary_domain="acmefcu.org",
            active=True,
        )
        session.add(inst)
        await session.flush()

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
        ]
        for t in terms:
            session.add(t)
        await session.commit()

        _ok(f"Institution '{inst.name}' created with {len(terms)} watch terms")
        inst_id = inst.id

    # ── Step 4: Simulate poll with mock mentions ──────────────────────
    _header("Step 4: Simulate polling with test mentions")

    mock_mentions = [
        RawMention(
            source_name="Paste Site Monitor",
            source_url="http://example.onion/paste/test-001",
            title="Credential dump with acmefcu.org users",
            content=(
                "Email/password dump:\n"
                "jdoe@acmefcu.org:Summer2026!\n"
                "asmith@acmefcu.org:P@ssw0rd1\n"
                "Found on darkweb paste site"
            ),
            discovered_at=datetime.now(timezone.utc),
            metadata={"paste_id": "test-001"},
        ),
        RawMention(
            source_name="Ransomware Blog Monitor",
            source_url="http://lockbit.onion/victims/acme",
            title="Acme Federal Credit Union listed on ransomware blog",
            content=(
                "Victim: Acme Federal Credit Union (acmefcu.org)\n"
                "Data: 120 GB exfiltrated\n"
                "Deadline: 7 days\n"
                "Sample: internal_audit_2025.pdf"
            ),
            discovered_at=datetime.now(timezone.utc),
            metadata={"group": "lockbit"},
        ),
        RawMention(
            source_name="Forum Monitor",
            source_url="http://forum.onion/thread/99999",
            title="General discussion about weather",
            content="Nice weather today, nothing about any banks here.",
            discovered_at=datetime.now(timezone.utc),
        ),
    ]

    _ok(f"Created {len(mock_mentions)} mock mentions (2 should match, 1 should not)")

    # ── Step 5: Run matching ──────────────────────────────────────────
    _header("Step 5: Run watch term matching")

    async with session_factory() as session:
        result = await session.execute(
            select(WatchTerm).where(WatchTerm.enabled.is_(True))
        )
        all_terms = result.scalars().all()

    matches_per_mention = []
    for mention in mock_mentions:
        results = match_mention(mention, all_terms)
        matches_per_mention.append(results)
        if results:
            _ok(f"'{mention.title[:50]}...' → {len(results)} institution(s) matched")
        else:
            _ok(f"'{mention.title[:50]}...' → no match (expected for unrelated content)")

    # Verify expected matching behavior
    if len(matches_per_mention[0]) >= 1:
        _ok("Credential dump matched (domain hit)")
    else:
        _fail("Credential dump should have matched on domain 'acmefcu.org'")
        passed = False

    if len(matches_per_mention[1]) >= 1:
        _ok("Ransomware blog post matched (institution name + domain)")
    else:
        _fail("Ransomware blog post should have matched on institution name")
        passed = False

    if len(matches_per_mention[2]) == 0:
        _ok("Unrelated mention correctly did not match")
    else:
        _fail("Unrelated mention should not have matched any watch terms")
        passed = False

    # ── Step 6: Run FP filtering ──────────────────────────────────────
    _header("Step 6: Run false-positive filtering")

    for i, (mention, matches) in enumerate(zip(mock_mentions, matches_per_mention)):
        if not matches:
            continue
        candidate = {
            "title": mention.title,
            "raw_content": mention.content,
            "matched_terms": matches[0].matched_terms,
        }
        fp_result = check_false_positive(candidate)
        _ok(
            f"FP check for mention {i + 1}: "
            f"score={fp_result.fp_score:.2f}, "
            f"recommendation={fp_result.recommendation}"
        )
        if fp_result.recommendation == "auto_dismiss":
            _fail(f"Legitimate mention {i + 1} was auto-dismissed as FP")
            passed = False

    # ── Step 7: Create findings in DB ─────────────────────────────────
    _header("Step 7: Create findings from matched mentions")

    findings_created = 0
    async with session_factory() as session:
        # Get a paste_site source to attach findings to
        result = await session.execute(
            select(Source).where(Source.source_type == SourceType.paste_site)
        )
        paste_source = result.scalars().first()

        for mention, matches in zip(mock_mentions, matches_per_mention):
            for match_result in matches:
                content_hash = hashlib.sha256(
                    f"{mention.source_name}:{mention.content}".encode()
                ).hexdigest()

                # Check dedup
                existing = (
                    await session.execute(
                        select(Finding.id).where(Finding.content_hash == content_hash)
                    )
                ).scalar_one_or_none()
                if existing:
                    _ok(f"Duplicate detected for '{mention.title[:40]}...' — skipped")
                    continue

                finding = Finding(
                    id=str(uuid4()),
                    institution_id=match_result.institution_id,
                    source_id=paste_source.id if paste_source else None,
                    severity=match_result.severity_hint,
                    status=FindingStatus.new,
                    title=mention.title,
                    summary=mention.content[:1000],
                    raw_content=mention.content,
                    content_hash=content_hash,
                    source_url=mention.source_url,
                    matched_terms=match_result.matched_terms,
                    tags=[mention.source_name.lower().replace(" ", "_")],
                    discovered_at=mention.discovered_at,
                )
                session.add(finding)
                findings_created += 1

        await session.commit()

    if findings_created >= 2:
        _ok(f"{findings_created} findings created in database")
    else:
        _fail(f"Expected >= 2 findings, got {findings_created}")
        passed = False

    # ── Step 8: Verify findings persisted ─────────────────────────────
    _header("Step 8: Verify findings persisted correctly")

    async with session_factory() as session:
        result = await session.execute(
            select(Finding).where(Finding.institution_id == inst_id)
        )
        findings = result.scalars().all()

    _ok(f"Found {len(findings)} findings for Acme FCU")

    for f in findings:
        _ok(
            f"  [{f.severity}] {f.title[:60]}... "
            f"(status={f.status}, matched_terms={len(f.matched_terms or [])})"
        )

    if len(findings) < 2:
        _fail("Expected at least 2 persisted findings")
        passed = False

    # ── Step 9: Verify beat schedule config ───────────────────────────
    _header("Step 9: Verify Celery beat schedule configuration")

    from darkdisco.pipeline.worker import app as celery_app

    beat_schedule = celery_app.conf.beat_schedule
    if "schedule-source-polls" in beat_schedule:
        sched = beat_schedule["schedule-source-polls"]
        _ok(f"Beat schedule found: task={sched['task']}, interval={sched['schedule']}s")
    else:
        _fail("Missing 'schedule-source-polls' in beat schedule")
        passed = False

    # ── Step 10: Verify connector map completeness ────────────────────
    _header("Step 10: Verify connector map covers all source types")

    from darkdisco.pipeline.worker import _CONNECTOR_MAP

    seeded_types = {src.source_type.value for src in sources}
    mapped_types = set(_CONNECTOR_MAP.keys())
    unmapped = seeded_types - mapped_types - {"stealer_log", "other", "marketplace"}
    if unmapped:
        _fail(f"Source types without connectors: {unmapped}")
        passed = False
    else:
        _ok(f"All seeded source types have connectors: {seeded_types & mapped_types}")

    # ── Summary ───────────────────────────────────────────────────────
    _header("VERIFICATION SUMMARY")
    await engine.dispose()

    if passed:
        print("\n  ✓ ALL CHECKS PASSED — Pipeline is operational\n")
        return True
    else:
        print("\n  ✗ SOME CHECKS FAILED — See above for details\n")
        return False


def main():
    parser = argparse.ArgumentParser(description="Verify DarkDisco polling pipeline")
    parser.add_argument(
        "--sqlite",
        action="store_true",
        help="Use in-memory SQLite instead of configured database",
    )
    args = parser.parse_args()

    if args.sqlite:
        db_url = "sqlite+aiosqlite:///:memory:"
    else:
        from darkdisco.config import settings
        db_url = settings.database_url

    ok = asyncio.run(verify(db_url))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
