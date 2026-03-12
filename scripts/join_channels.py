#!/usr/bin/env python3
"""Join all configured Telegram channels for monitoring.

Usage:
    PYTHONPATH=src python scripts/join_channels.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    InviteHashExpiredError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.messages import ImportChatInviteRequest

from darkdisco.config import settings

# All channels from seed_sources.py
CHANNELS = [
    # Stealer log clouds
    "+E9biBdpOv35iMmEy",      # SNATCH LOGS CLOUD
    "+VbZVKqzgUURlMjdi",       # Everlasting Cloud
    "BHF_CLOUD",               # BHF Cloud
    "Skyl1neCloud",            # Skyline Cloud
    "PegasusCloud",            # Pegasus Cloud
    "+IqEnwfj7CLU1Yjcy",       # Omega Cloud
    "Creditunionbanksstore",   # Credit union/bank logs
    "cvv190_cloud",            # CVV190 Cloud
    "ManticoreCloud",          # Manticore
    "Trident_Cloud",           # Trident Cloud
    "BurnCloudLogs",           # Burn Cloud
    "darknescloud",            # Darkness Cloud
    "universecloudtxt",        # Universe Cloud
    "realcloud0",              # RealCloud
    "Sl1ddifree",              # Sl1ddi Cloud
    # Threat intel
    "vxunderground",           # Malware/threat research
    "TheDarkWebInformer",      # Breach/leak/ransomware alerts
]


async def main():
    client = TelegramClient(
        str(Path(settings.telegram_session_name).expanduser()),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    await client.connect()

    if not await client.is_user_authorized():
        print("ERROR: Session not authorized. Run the interactive login first.")
        sys.exit(1)

    me = await client.get_me()
    print(f"Authenticated as: {me.first_name} (@{me.username or me.phone})\n")

    joined = 0
    failed = 0

    for ch in CHANNELS:
        try:
            if ch.startswith("+"):
                # Invite link
                invite_hash = ch[1:]  # strip leading +
                try:
                    await client(ImportChatInviteRequest(invite_hash))
                    print(f"  [joined]  {ch}")
                    joined += 1
                except UserAlreadyParticipantError:
                    print(f"  [exists]  {ch}")
                    joined += 1
                except InviteHashExpiredError:
                    print(f"  [expired] {ch}")
                    failed += 1
            else:
                # Public username
                try:
                    entity = await client.get_entity(ch)
                    title = getattr(entity, "title", ch)
                    print(f"  [ok]      @{ch} — {title}")
                    joined += 1
                except Exception as e:
                    print(f"  [failed]  @{ch} — {e}")
                    failed += 1

            # Rate limit protection
            await asyncio.sleep(2)

        except FloodWaitError as e:
            print(f"\n  FLOOD WAIT: {e.seconds}s — stopping to avoid ban")
            break
        except Exception as e:
            print(f"  [error]   {ch} — {e}")
            failed += 1

    print(f"\nDone: {joined} joined/accessible, {failed} failed")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
