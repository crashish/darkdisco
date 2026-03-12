#!/usr/bin/env python3
"""Follow invite links found in redirect channels to join actual data channels."""

from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    InviteHashExpiredError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Message

from darkdisco.config import settings

# Channels that appeared to be redirectors
REDIRECT_CHANNELS = [
    "BHF_CLOUD",
    "PegasusCloud",
    "cvv190_cloud",
    "Trident_Cloud",
    "BurnCloudLogs",
    "Sl1ddifree",
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

    print("Scanning redirect channels for invite links...\n")

    invite_links: set[str] = set()

    for ch in REDIRECT_CHANNELS:
        try:
            entity = await client.get_entity(ch)
            title = getattr(entity, "title", ch)
            async for msg in client.iter_messages(entity, limit=10):
                if isinstance(msg, Message) and msg.text:
                    links = re.findall(r"https?://t\.me/\+([A-Za-z0-9_-]+)", msg.text)
                    for link in links:
                        invite_links.add(link)
            print(f"  @{ch}: {title}")
        except Exception as e:
            print(f"  @{ch}: ERROR {e}")
        await asyncio.sleep(1)

    print(f"\nFound {len(invite_links)} unique invite links. Joining...\n")

    joined = 0
    for invite_hash in sorted(invite_links):
        try:
            try:
                result = await client(ImportChatInviteRequest(invite_hash))
                title = getattr(result.chats[0], "title", "?") if result.chats else "?"
                print(f"  [joined]  +{invite_hash} — {title}")
                joined += 1
            except UserAlreadyParticipantError:
                print(f"  [exists]  +{invite_hash}")
                joined += 1
            except InviteHashExpiredError:
                print(f"  [expired] +{invite_hash}")
        except FloodWaitError as e:
            print(f"\n  FLOOD WAIT: {e.seconds}s — stopping")
            break
        except Exception as e:
            print(f"  [error]   +{invite_hash} — {e}")
        await asyncio.sleep(3)

    print(f"\nDone: {joined} channels joined/confirmed")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
