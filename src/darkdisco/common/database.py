"""Database session management."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from darkdisco.config import settings

engine = create_async_engine(settings.database_url, echo=False, pool_pre_ping=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an async DB session."""
    async with async_session() as session:
        yield session


async def get_system_setting(key: str, default: str | None = None) -> str | None:
    """Read a system setting from the database.

    Opens its own session so callers (e.g. connectors) don't need one.
    """
    from darkdisco.common.models import SystemSetting

    async with async_session() as session:
        result = await session.execute(
            select(SystemSetting.value).where(SystemSetting.key == key)
        )
        row = result.scalar_one_or_none()
        return row if row is not None else default
