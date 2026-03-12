#!/usr/bin/env python3
"""Quick test: read recent messages from joined channels.

Usage:
    source .env
    PYTHONPATH=src DARKDISCO_TELEGRAM_API_ID=$TELEGRAM_API_ID \
    DARKDISCO_TELEGRAM_API_HASH=$TELEGRAM_API_HASH \
    DARKDISCO_TELEGRAM_SESSION_NAME=darkdisco_monitor \
    .venv/bin/python scripts/test_telegram_poll.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from telethon import TelegramClient
from telethon.tl.types import Message

from darkdisco.config import settings

CHANNELS = [
    "BHF_CLOUD",
    "Skyl1neCloud",
    "PegasusCloud",
    "cvv190_cloud",
    "Trident_Cloud",
    "BurnCloudLogs",
    "darknescloud",
    "universecloudtxt",
    "realcloud0",
    "Sl1ddifree",
    "vxunderground",
    "TheDarkWebInformer",
]


async def main():
    client = TelegramClient(
        str(Path(settings.telegram_session_name).expanduser()),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    await client.connect()

    if not await client.is_user_authorized():
        print("ERROR: Not authorized")
        sys.exit(1)

    me = await client.get_me()
    print(f"Authenticated as: @{me.username or me.phone}\n")

    for ch in CHANNELS:
        try:
            entity = await client.get_entity(ch)
            title = getattr(entity, "title", ch)
            count = 0
            latest_text = ""
            async for msg in client.iter_messages(entity, limit=3):
                if isinstance(msg, Message) and msg.text:
                    count += 1
                    if not latest_text:
                        latest_text = msg.text[:120].replace("\n", " ")
            print(f"  @{ch:25s} | {title:30s} | {count} msgs | {latest_text}")
        except Exception as e:
            print(f"  @{ch:25s} | ERROR: {e}")
        await asyncio.sleep(1)

    await client.disconnect()
    print("\nTest complete.")


if __name__ == "__main__":
    asyncio.run(main())
