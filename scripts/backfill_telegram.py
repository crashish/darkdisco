#!/usr/bin/env python3
"""One-time backfill: reset Telegram sources to re-ingest mentions lost during
the Mar 25-28 persistence outage (da-ovr).

What this does:
  1. Sets last_polled_at to Mar 24 on all Telegram sources
  2. Clears last_message_ids bookmarks (forces min_id=0, disabling time filter)
  3. Bumps history_limit to 1000 to capture the 3-day gap
  4. The next scheduled poll cycle re-ingests messages naturally
  5. Content-hash dedup prevents duplicates of already-recovered data

After the backfill poll completes, history_limit reverts to its original value
automatically (the poll writes back the connector's config, and the connector
reads history_limit from the DB each time).

Usage:
    python scripts/backfill_telegram.py              # dry-run (default)
    python scripts/backfill_telegram.py --apply       # actually reset
    python scripts/backfill_telegram.py --restore     # restore original history_limit
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from darkdisco.common.models import Source
from darkdisco.config import settings

# Reset to day before outage started
BACKFILL_TIMESTAMP = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)
BACKFILL_HISTORY_LIMIT = 1000  # ~3 days of high-volume channels


async def backfill(apply: bool = False, restore: bool = False) -> None:
    engine = create_async_engine(settings.database_url, echo=False, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as session:
        result = await session.execute(
            select(Source).where(Source.source_type.in_(["telegram", "telegram_intel"]))
        )
        sources = result.scalars().all()

        if not sources:
            print("No Telegram sources found.")
            await engine.dispose()
            return

        for source in sources:
            cfg = dict(source.config or {})
            old_polled = source.last_polled_at
            old_bookmarks = cfg.get("last_message_ids", {})
            old_limit = cfg.get("history_limit", 100)

            if restore:
                # Restore original history_limit after backfill completes
                if cfg.get("_pre_backfill_history_limit") is not None:
                    cfg["history_limit"] = cfg.pop("_pre_backfill_history_limit")
                    source.config = cfg
                    print(f"  [restored] {source.name}: history_limit → {cfg['history_limit']}")
                else:
                    print(f"  [skip] {source.name}: no backfill marker found")
                continue

            print(f"\n  Source: {source.name} (id={source.id})")
            print(f"    last_polled_at:  {old_polled} → {BACKFILL_TIMESTAMP}")
            print(f"    last_message_ids: {len(old_bookmarks)} channels → cleared")
            print(f"    history_limit:   {old_limit} → {BACKFILL_HISTORY_LIMIT}")

            if apply:
                source.last_polled_at = BACKFILL_TIMESTAMP
                cfg["last_message_ids"] = {}
                cfg["_pre_backfill_history_limit"] = old_limit
                cfg["history_limit"] = BACKFILL_HISTORY_LIMIT
                source.config = cfg
                print(f"    ✓ Applied")

        if apply or restore:
            await session.commit()
            action = "restored" if restore else "applied"
            print(f"\n✓ Changes {action} to {len(sources)} Telegram source(s).")
            print("  Next poll cycle will re-ingest from the gap period.")
            print("  Run with --restore after backfill poll completes to reset history_limit.")
        else:
            print(f"\n⚠ DRY RUN — no changes made. Pass --apply to execute.")

    await engine.dispose()


if __name__ == "__main__":
    apply = "--apply" in sys.argv
    restore = "--restore" in sys.argv
    asyncio.run(backfill(apply=apply, restore=restore))
