"""Shared test fixtures — async SQLite DB, FastAPI test client, auth helpers."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TSVECTOR
from sqlalchemy import JSON, Text

# Register type compilation overrides so JSONB/ARRAY/TSVECTOR work on SQLite
from sqlalchemy.dialects import registry as _  # noqa: F401
from sqlalchemy.ext.compiler import compiles


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(type_, compiler, **kw):
    return "JSON"


@compiles(ARRAY, "sqlite")
def _compile_array_sqlite(type_, compiler, **kw):
    return "JSON"


@compiles(TSVECTOR, "sqlite")
def _compile_tsvector_sqlite(type_, compiler, **kw):
    return "TEXT"


from darkdisco.api.app import app
from darkdisco.api.auth import create_access_token, hash_password
from darkdisco.common.database import get_session
from darkdisco.common.models import (
    AlertRule,
    Base,
    Client,
    Finding,
    FindingStatus,
    Institution,
    Notification,
    Severity,
    Source,
    SourceType,
    User,
    UserRole,
    WatchTerm,
    WatchTermType,
)


# Use SQLite for tests (no Postgres required)
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def engine():
    eng = create_async_engine(TEST_DB_URL, echo=False)

    # SQLite needs special handling for nested transactions
    @event.listens_for(eng.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    # SQLite cannot handle Postgres-specific computed columns (to_tsvector).
    # Temporarily remove the content_tsvector column before creating tables.
    from darkdisco.common.models import ExtractedFile
    tsvector_col = ExtractedFile.__table__.c.get("content_tsvector")
    if tsvector_col is not None:
        ExtractedFile.__table__._columns.remove(tsvector_col)
        # Also remove the GIN index that references it
        tsvector_indexes = [
            idx for idx in list(ExtractedFile.__table__.indexes)
            if "content_tsvector" in {c.name for c in idx.columns}
        ]
        for idx in tsvector_indexes:
            ExtractedFile.__table__.indexes.discard(idx)

    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture
async def db_session(engine):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def client(engine):
    """FastAPI test client with overridden DB dependency."""
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _override_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def test_user(db_session: AsyncSession):
    """Create a test analyst user and return (user, jwt_token)."""
    user = User(
        id=str(uuid4()),
        username="testanalyst",
        hashed_password=hash_password("testpass123"),
        role=UserRole.analyst,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    token = create_access_token(user.username, user.role.value)
    return user, token


@pytest_asyncio.fixture
async def auth_headers(test_user):
    """Authorization headers for protected endpoints."""
    _, token = test_user
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture
async def sample_client(db_session: AsyncSession):
    """Create a sample client."""
    c = Client(id=str(uuid4()), name="Test Bank Corp", contract_ref="TB-2026-001", active=True)
    db_session.add(c)
    await db_session.commit()
    await db_session.refresh(c)
    return c


@pytest_asyncio.fixture
async def sample_institution(db_session: AsyncSession, sample_client: Client):
    """Create a sample institution under the sample client."""
    inst = Institution(
        id=str(uuid4()),
        client_id=sample_client.id,
        name="First National Bank",
        short_name="FNB",
        primary_domain="firstnational.com",
        bin_ranges=["412345", "412346"],
        routing_numbers=["021000021"],
        active=True,
    )
    db_session.add(inst)
    await db_session.commit()
    await db_session.refresh(inst)
    return inst


@pytest_asyncio.fixture
async def sample_source(db_session: AsyncSession):
    """Create a sample paste_site source."""
    src = Source(
        id=str(uuid4()),
        name="Test Paste Monitor",
        source_type=SourceType.paste_site,
        enabled=True,
        poll_interval_seconds=300,
    )
    db_session.add(src)
    await db_session.commit()
    await db_session.refresh(src)
    return src


@pytest_asyncio.fixture
async def sample_watch_terms(db_session: AsyncSession, sample_institution: Institution):
    """Create a set of watch terms for the sample institution."""
    terms = [
        WatchTerm(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            term_type=WatchTermType.domain,
            value="firstnational.com",
            enabled=True,
        ),
        WatchTerm(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            term_type=WatchTermType.institution_name,
            value="First National Bank",
            enabled=True,
        ),
        WatchTerm(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            term_type=WatchTermType.bin_range,
            value="412345",
            enabled=True,
        ),
        WatchTerm(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            term_type=WatchTermType.regex,
            value=r"fnb[\-_]?leak",
            enabled=True,
            case_sensitive=False,
        ),
    ]
    for t in terms:
        db_session.add(t)
    await db_session.commit()
    for t in terms:
        await db_session.refresh(t)
    return terms


@pytest_asyncio.fixture
async def sample_finding(
    db_session: AsyncSession,
    sample_institution: Institution,
    sample_source: Source,
):
    """Create a sample finding."""
    finding = Finding(
        id=str(uuid4()),
        institution_id=sample_institution.id,
        source_id=sample_source.id,
        severity=Severity.high,
        status=FindingStatus.new,
        title="Credential dump mentioning First National Bank",
        summary="Found credentials on dark web paste site referencing firstnational.com",
        raw_content="user@firstnational.com:p@ssword123\nadmin@firstnational.com:hunter2",
        content_hash="abc123def456",
        source_url="http://example.onion/paste/12345",
        matched_terms=[{"term_type": "domain", "value": "firstnational.com"}],
        tags=["paste_site"],
    )
    db_session.add(finding)
    await db_session.commit()
    await db_session.refresh(finding)
    return finding


@pytest_asyncio.fixture
async def sample_alert_rule(db_session: AsyncSession, test_user, sample_institution: Institution):
    """Create a sample alert rule."""
    user, _ = test_user
    rule = AlertRule(
        id=str(uuid4()),
        name="High severity alerts",
        owner_id=user.id,
        institution_id=sample_institution.id,
        min_severity=Severity.high,
        enabled=True,
        notify_email=False,
        notify_slack=False,
    )
    db_session.add(rule)
    await db_session.commit()
    await db_session.refresh(rule)
    return rule
