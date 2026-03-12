#!/usr/bin/env python3
"""Seed/update DarkDisco source configurations with correct connector paths.

Usage:
    python scripts/seed_sources.py

Idempotent — updates existing sources, creates missing ones.
Fixes connector_class paths to match actual module locations.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from darkdisco.common.models import Base, Source, SourceType
from darkdisco.config import settings

# ---------------------------------------------------------------------------
# Source definitions with correct connector_class paths
# ---------------------------------------------------------------------------

SOURCES = [
    # ---- Telegram: Stealer log clouds (highest priority) ----
    {
        "name": "Telegram Stealer Logs",
        "source_type": SourceType.telegram,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.telegram:TelegramConnector",
        "poll_interval_seconds": 300,
        "config": {
            "channels": [
                # Stealer log clouds (verified accessible Mar 2026)
                "BHF_CLOUD",               # BHF Cloud
                "Skyl1neCloud",            # Skyline Cloud
                "PegasusCloud",            # Pegasus Cloud
                "cvv190_cloud",            # CVV190 Cloud
                "Trident_Cloud",           # Trident Cloud
                "BurnCloudLogs",           # Burn Cloud
                "darknescloud",            # Darkness Cloud
                "universecloudtxt",        # Universe Cloud / txtbases
                "realcloud0",              # RealCloud
                "Sl1ddifree",              # Sl1ddi Cloud (free logs)
                # Pending join approval:
                # "+IqEnwfj7CLU1Yjcy",    # Omega Cloud
            ],
            "last_message_ids": {},
            "history_limit": 100,
        },
    },
    # ---- Telegram: Threat intel aggregators (defensive) ----
    {
        "name": "Telegram Threat Intel",
        "source_type": SourceType.telegram,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.telegram:TelegramConnector",
        "poll_interval_seconds": 600,
        "config": {
            "channels": [
                "vxunderground",           # Malware/threat research
                "TheDarkWebInformer",      # Breach/leak/ransomware alerts
            ],
            "last_message_ids": {},
            "history_limit": 50,
        },
    },
    # ---- Forums ----
    {
        "name": "BreachForums Monitor",
        "source_type": SourceType.forum,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.forum:ForumConnector",
        "poll_interval_seconds": 1800,
        "config": {
            "forums": [
                {
                    "name": "BreachForums",
                    "base_url": "https://breachforums.bf",
                    "recent_path": "/Forum-Databases",
                    "selector_profile": "mybb",
                    "last_seen_id": "",
                },
            ],
            "max_pages": 3,
        },
    },
    {
        "name": "Exploit.in Forum Monitor",
        "source_type": SourceType.forum,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.forum:ForumConnector",
        "poll_interval_seconds": 3600,
        "config": {
            "forums": [
                {
                    "name": "Exploit.in",
                    "base_url": "https://exploit.in",
                    "recent_path": "/",
                    "selector_profile": "xenforo",
                    "last_seen_id": "",
                },
            ],
            "max_pages": 2,
        },
    },
    {
        "name": "Paste Site Monitor",
        "source_type": SourceType.paste_site,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.paste_site:PasteSiteConnector",
        "poll_interval_seconds": 600,
        "config": {},
    },
    {
        "name": "Ransomware Blog Monitor",
        "source_type": SourceType.ransomware_blog,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.ransomware_blog:RansomwareBlogConnector",
        "poll_interval_seconds": 1800,
        "config": {
            "groups": {
                "lockbit": {
                    "last_known_url": "",
                    "mirror_urls": [],
                    "parser": "generic",
                    "enabled": True,
                },
                "alphv": {
                    "last_known_url": "",
                    "mirror_urls": [],
                    "parser": "generic",
                    "enabled": True,
                },
                "clop": {
                    "last_known_url": "",
                    "mirror_urls": [],
                    "parser": "generic",
                    "enabled": True,
                },
            },
            "seen_hashes": [],
            "request_delay_seconds": 5,
            "max_pages": 3,
        },
    },
    # ---- Stealer Logs ----
    {
        "name": "Stealer Log Aggregator",
        "source_type": SourceType.stealer_log,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.stealer_log:StealerLogConnector",
        "poll_interval_seconds": 3600,
        "config": {
            "s3_prefix": "stealer-logs/incoming/",
            "archive_formats": ["zip", "tar.gz"],
            "parsers": ["redline", "raccoon", "generic"],
            "seen_hashes": [],
        },
    },
]


async def seed(db_url: str | None = None) -> None:
    url = db_url or settings.database_url
    engine = create_async_engine(url, echo=False, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        for src_def in SOURCES:
            await _upsert_source(session, **src_def)
        await session.commit()

    # Also fix any old sources with wrong connector_class paths
    async with session_factory() as session:
        await _fix_legacy_paths(session)
        await session.commit()

    await engine.dispose()
    print("\nSource seeding complete.")


async def _upsert_source(
    session: AsyncSession,
    *,
    name: str,
    source_type: SourceType,
    url: str | None,
    connector_class: str,
    poll_interval_seconds: int,
    config: dict,
) -> str:
    result = await session.execute(select(Source).where(Source.name == name))
    src = result.scalars().first()
    if src:
        # Update connector_class and config
        src.connector_class = connector_class
        src.config = config
        src.poll_interval_seconds = poll_interval_seconds
        src.enabled = True
        print(f"  [updated] {name} → {connector_class}")
        return src.id

    src = Source(
        id=str(uuid4()),
        name=name,
        source_type=source_type,
        url=url,
        connector_class=connector_class,
        enabled=True,
        poll_interval_seconds=poll_interval_seconds,
        config=config,
    )
    session.add(src)
    await session.flush()
    print(f"  [created] {name} → {connector_class}")
    return src.id


# Map old incorrect paths to correct ones
_PATH_FIXES = {
    "darkdisco.connectors.tor_forum.TorForumConnector": "darkdisco.discovery.connectors.forum:ForumConnector",
    "darkdisco.connectors.paste_site.PasteSiteConnector": "darkdisco.discovery.connectors.paste_site:PasteSiteConnector",
    "darkdisco.connectors.telegram.TelegramConnector": "darkdisco.discovery.connectors.telegram:TelegramConnector",
    "darkdisco.connectors.breach_db.DehashedConnector": "darkdisco.discovery.connectors.breach_db:BreachDBConnector",
    "darkdisco.connectors.breach_db.HIBPConnector": "darkdisco.discovery.connectors.breach_db:BreachDBConnector",
    "darkdisco.connectors.ransomware_blog.RansomwareBlogConnector": "darkdisco.discovery.connectors.ransomware_blog:RansomwareBlogConnector",
    "darkdisco.connectors.stealer_log.StealerLogConnector": "darkdisco.discovery.connectors.stealer_log:StealerLogConnector",
}


async def _fix_legacy_paths(session: AsyncSession) -> None:
    """Fix connector_class paths seeded by the old seed_institutions.py."""
    result = await session.execute(select(Source))
    for src in result.scalars().all():
        if src.connector_class in _PATH_FIXES:
            new_path = _PATH_FIXES[src.connector_class]
            if new_path:
                print(f"  [fixed] {src.name}: {src.connector_class} → {new_path}")
                src.connector_class = new_path
            else:
                print(f"  [disabled] {src.name}: no connector available, disabling")
                src.enabled = False


if __name__ == "__main__":
    asyncio.run(seed())
