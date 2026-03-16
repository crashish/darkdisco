"""Telegram connector — monitors channels via Telethon (user account, MTProto).

Uses a regular Telegram user account to passively read messages from joined
channels and groups.  Far more capable than the Bot API: can join via invite
links, read channel history, and monitor private channels.

Setup:
    1. Get api_id + api_hash from https://my.telegram.org
    2. Run `python -m darkdisco.discovery.connectors.telegram` once interactively
       to authenticate and create the session file.
    3. Set DARKDISCO_TELEGRAM_API_ID, DARKDISCO_TELEGRAM_API_HASH in .env

Config (source.config JSONB):
    channels: list[str]  — channel usernames, invite links, or numeric IDs
    last_message_ids: dict[str, int]  — per-channel high-water marks (auto-managed)
    history_limit: int  — max messages to fetch per channel per poll (default 100)
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    InviteHashExpiredError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Channel, Chat, Message, PeerChannel, User

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

HISTORY_LIMIT_DEFAULT = 100
INTER_CHANNEL_DELAY = 1.0  # seconds between channel reads
MAX_DOWNLOAD_SIZE = 4 * 1024 * 1024 * 1024  # 4GB file download limit
FILE_DOWNLOAD_MAX_AGE_DAYS = 60  # Only download files from messages within this window


class TelegramConnector(BaseConnector):
    """Monitors Telegram channels/groups via a user account (Telethon/MTProto)."""

    name = "telegram"
    source_type = "telegram"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._client: TelegramClient | None = None

    @property
    def _channels(self) -> list[str]:
        return self.config.get("channels", [])

    @property
    def _last_message_ids(self) -> dict[str, int]:
        return self.config.setdefault("last_message_ids", {})

    @property
    def _history_limit(self) -> int:
        return self.config.get("history_limit", HISTORY_LIMIT_DEFAULT)

    async def setup(self) -> None:
        if not settings.telegram_api_id or not settings.telegram_api_hash:
            raise RuntimeError(
                "DARKDISCO_TELEGRAM_API_ID and DARKDISCO_TELEGRAM_API_HASH "
                "must be configured"
            )

        # Allow per-source session name to avoid SQLite lock conflicts
        # when multiple Telegram sources poll concurrently.
        session_name = self.config.get(
            "session_name", settings.telegram_session_name
        )
        session_path = str(Path(session_name).expanduser())

        # Route Telegram traffic through Tor SOCKS proxy if configured
        proxy = None
        if settings.tor_socks_proxy and self.config.get("use_tor", False):
            proxy = _parse_socks_proxy(settings.tor_socks_proxy)

        self._client = TelegramClient(
            session_path,
            settings.telegram_api_id,
            settings.telegram_api_hash,
            proxy=proxy,
        )
        await self._client.connect()

        if not await self._client.is_user_authorized():
            raise RuntimeError(
                "Telegram session not authorized. Run the interactive login "
                "first: python -m darkdisco.discovery.connectors.telegram"
            )

    async def teardown(self) -> None:
        if self._client and self._client.is_connected():
            await self._client.disconnect()
            self._client = None

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        if self._client is None:
            await self.setup()

        # Build channel list: start from all joined channels/groups,
        # supplemented by any explicitly configured channels
        channels_to_poll = await self._get_joined_channels()
        # Add any configured channels not already in the joined list
        joined_ids = {str(c) for c in channels_to_poll}
        for ch in self._channels:
            if str(ch) not in joined_ids:
                channels_to_poll.append(str(ch))

        if not channels_to_poll:
            logger.warning("No channels found (none joined, none configured)")
            return []

        all_mentions: list[RawMention] = []

        for channel_ref in channels_to_poll:
            try:
                mentions = await self._poll_channel(channel_ref, since)
                all_mentions.extend(mentions)
                logger.info(
                    "Telegram channel %s: %d mentions",
                    channel_ref, len(mentions),
                )
            except FloodWaitError as e:
                logger.warning(
                    "Telegram flood wait %ds for %s, stopping poll",
                    e.seconds, channel_ref,
                )
                break
            except ChannelPrivateError:
                logger.warning(
                    "Cannot access channel %s (private/banned)", channel_ref
                )
            except Exception:
                logger.exception("Failed to poll channel %s", channel_ref)

            await asyncio.sleep(INTER_CHANNEL_DELAY)

        logger.info(
            "Telegram poll complete: %d total mentions from %d channels",
            len(all_mentions), len(channels_to_poll),
        )
        return all_mentions

    async def _get_joined_channels(self) -> list[str]:
        """Get all channels/groups the account is currently joined to."""
        try:
            dialogs = await self._client.get_dialogs()
            channels = []
            for d in dialogs:
                if d.is_channel or d.is_group:
                    channels.append(str(d.entity.id))
            logger.info("Found %d joined channels/groups", len(channels))
            return channels
        except Exception:
            logger.warning("Failed to enumerate joined channels, falling back to config")
            return list(self._channels)

    async def health_check(self) -> dict:
        if self._client is None:
            try:
                await self.setup()
            except Exception as exc:
                return {"healthy": False, "message": str(exc)[:200]}

        try:
            me = await self._client.get_me()
            username = me.username or me.phone
            return {
                "healthy": True,
                "message": f"Connected as @{username}",
                "channels_configured": len(self._channels),
            }
        except Exception as exc:
            return {"healthy": False, "message": str(exc)[:200]}

    async def join_channel(self, channel_ref: str) -> bool:
        """Attempt to join a channel by username or invite link.

        Call this manually or from a management script to onboard new channels.
        """
        if self._client is None:
            await self.setup()

        try:
            if _is_invite_link(channel_ref):
                invite_hash = _extract_invite_hash(channel_ref)
                await self._client(ImportChatInviteRequest(invite_hash))
            else:
                entity = await self._client.get_entity(channel_ref)
                await self._client.get_messages(entity, limit=1)
            logger.info("Joined channel: %s", channel_ref)
            return True
        except UserAlreadyParticipantError:
            logger.info("Already in channel: %s", channel_ref)
            return True
        except InviteHashExpiredError:
            logger.warning("Invite link expired: %s", channel_ref)
            return False
        except Exception:
            logger.exception("Failed to join channel: %s", channel_ref)
            return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _poll_channel(
        self, channel_ref: str, since: datetime | None,
    ) -> list[RawMention]:
        # Telethon needs PeerChannel for channel numeric IDs
        if channel_ref.lstrip("-").isdigit():
            entity = await self._client.get_entity(PeerChannel(int(channel_ref)))
        else:
            entity = await self._client.get_entity(channel_ref)
        channel_key = _channel_key(channel_ref, entity)

        # Get high-water mark for incremental reads
        min_id = self._last_message_ids.get(channel_key, 0)
        new_high_water = min_id

        mentions: list[RawMention] = []

        async for message in self._client.iter_messages(
            entity,
            limit=self._history_limit,
            min_id=min_id,
        ):
            if not isinstance(message, Message):
                continue

            # Track highest message ID seen
            if message.id > new_high_water:
                new_high_water = message.id

            # Time filter — skip for channels with no high-water mark (first poll / backfill)
            if min_id > 0 and since and message.date and message.date < since:
                continue

            # Download file attachments if present (only for recent messages)
            file_data = None
            if message.file and message.file.size:
                from datetime import timedelta
                file_age_cutoff = datetime.now(timezone.utc) - timedelta(days=FILE_DOWNLOAD_MAX_AGE_DAYS)
                msg_date = message.date.replace(tzinfo=timezone.utc) if message.date and message.date.tzinfo is None else message.date

                if msg_date and msg_date < file_age_cutoff:
                    logger.debug(
                        "Skipping file download (older than %dd): %s in %s msg#%d",
                        FILE_DOWNLOAD_MAX_AGE_DAYS,
                        message.file.name or "unnamed",
                        channel_ref, message.id,
                    )
                elif message.file.size > MAX_DOWNLOAD_SIZE:
                    logger.info(
                        "File too large to download: %s (%s) in %s msg#%d",
                        message.file.name or "unnamed",
                        f"{message.file.size / (1024*1024):.1f}MB",
                        channel_ref, message.id,
                    )
                elif message.file.size > 50 * 1024 * 1024:
                    # Large files: record metadata for async download later
                    logger.info(
                        "Large file recorded for async download: %s (%s) in %s msg#%d",
                        message.file.name or "unnamed",
                        f"{message.file.size / (1024*1024):.1f}MB",
                        channel_ref, message.id,
                    )
                    if not hasattr(message, "_dd_extra_meta"):
                        message._dd_extra_meta = {}
                    message._dd_extra_meta["download_status"] = "pending"
                else:
                    try:
                        file_data = await self._client.download_media(
                            message, file=bytes,
                        )
                    except Exception:
                        logger.debug(
                            "Failed to download file from message %d in %s",
                            message.id, channel_ref,
                        )

            mention = _message_to_mention(message, entity, channel_ref, file_data)
            if mention is not None:
                mentions.append(mention)

        # Update bookmark
        if new_high_water > min_id:
            self._last_message_ids[channel_key] = new_high_water

        return mentions


# ------------------------------------------------------------------
# Pure helpers
# ------------------------------------------------------------------


def _parse_socks_proxy(url: str) -> tuple | None:
    """Parse a SOCKS proxy URL into Telethon's proxy tuple format.

    Telethon expects: (socks_type, host, port)
    Input: socks5h://host:port or socks5://host:port
    """
    import socks  # PySocks, bundled with Telethon

    url = url.strip()
    if not url:
        return None

    # Strip scheme
    for scheme in ("socks5h://", "socks5://", "socks4://"):
        if url.startswith(scheme):
            url = url[len(scheme):]
            break

    # Parse host:port
    if ":" in url:
        host, port_str = url.rsplit(":", 1)
        port = int(port_str)
    else:
        host = url
        port = 9050

    return (socks.SOCKS5, host, port)


def _channel_key(ref: str, entity) -> str:
    """Stable key for a channel — prefer numeric ID."""
    if hasattr(entity, "id"):
        return str(entity.id)
    return ref.lower().strip("@").strip("/")


def _is_invite_link(ref: str) -> bool:
    return "+/" in ref or "joinchat/" in ref or ref.startswith("+")


def _extract_invite_hash(ref: str) -> str:
    """Extract the hash portion from a t.me invite link."""
    ref = ref.strip()
    for prefix in ("https://t.me/+", "https://t.me/joinchat/", "t.me/+", "t.me/joinchat/", "+"):
        if ref.startswith(prefix):
            return ref[len(prefix):]
    return ref


def _message_to_mention(
    msg: Message, entity, channel_ref: str, file_data: bytes | None = None,
) -> RawMention | None:
    """Convert a Telethon Message to a RawMention.

    When file_data is provided (downloaded attachment), the mention includes
    the raw bytes in metadata for downstream processing (archive extraction,
    credential scanning, etc.).
    """
    text = msg.text or ""
    if msg.message and not text:
        text = msg.message

    # Skip messages with no text AND no file (downloaded or not)
    has_file = file_data or (msg.file and msg.file.size)
    if not text and not has_file:
        return None

    # Channel/chat title
    title = ""
    if hasattr(entity, "title"):
        title = entity.title or ""

    # Author
    author = None
    if msg.sender:
        if isinstance(msg.sender, User):
            parts = [msg.sender.first_name or "", msg.sender.last_name or ""]
            author = " ".join(p for p in parts if p).strip()
            if not author and msg.sender.username:
                author = f"@{msg.sender.username}"
        elif hasattr(msg.sender, "title"):
            author = msg.sender.title

    # Timestamp
    discovered = msg.date or datetime.now(tz=timezone.utc)
    if discovered.tzinfo is None:
        discovered = discovered.replace(tzinfo=timezone.utc)

    # Source URL
    source_url = None
    if hasattr(entity, "username") and entity.username:
        source_url = f"https://t.me/{entity.username}/{msg.id}"
    elif hasattr(entity, "id"):
        source_url = f"https://t.me/c/{entity.id}/{msg.id}"

    # Metadata — prefer entity title over raw ref for readability
    display_ref = channel_ref
    if hasattr(entity, "title") and entity.title:
        display_ref = entity.title
    elif hasattr(entity, "username") and entity.username:
        display_ref = entity.username

    metadata: dict[str, Any] = {
        "message_id": msg.id,
        "chat_id": entity.id if hasattr(entity, "id") else None,
        "channel_ref": display_ref,
    }

    if msg.forward:
        if hasattr(msg.forward, "chat") and msg.forward.chat:
            metadata["forwarded_from"] = getattr(
                msg.forward.chat, "title", None
            ) or getattr(msg.forward.chat, "username", None)

    if msg.media:
        metadata["has_media"] = True
        media_type = type(msg.media).__name__
        metadata["media_type"] = media_type

    if msg.file:
        metadata["file_name"] = msg.file.name
        metadata["file_size"] = msg.file.size
        metadata["file_mime"] = msg.file.mime_type

    if file_data:
        metadata["file_data"] = file_data

    # Merge extra metadata from large file downloads
    if hasattr(msg, "_dd_extra_meta"):
        metadata.update(msg._dd_extra_meta)

    return RawMention(
        source_name=f"telegram:{title or channel_ref}",
        source_url=source_url,
        title=title,
        content=text,
        author=author,
        discovered_at=discovered,
        metadata=metadata,
    )


# ------------------------------------------------------------------
# Channel link extraction
# ------------------------------------------------------------------

# Matches t.me links including optional trailing /digits for message links.
# Group 1 = the channel/invite part, group 2 = optional /message_id suffix.
_TME_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/((?:\+[A-Za-z0-9_-]+|joinchat/[A-Za-z0-9_-]+|[A-Za-z][A-Za-z0-9_]{3,}))(/\d+)?",
)

# Paths that are Telegram features, not channels
_TME_IGNORE_PATHS = frozenset({
    "proxy", "socks", "addstickers", "addtheme",
    "share", "iv", "confirmphone", "setlanguage",
})


def extract_channel_links(text: str) -> list[str]:
    """Extract unique t.me channel/group links from message text.

    Returns normalized https://t.me/... URLs for channels and invite links.
    Skips message-specific links (t.me/channel/123) and Telegram feature paths.
    """
    if not text:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for match in _TME_LINK_RE.finditer(text):
        channel_part = match.group(1)
        msg_suffix = match.group(2)  # e.g. "/12345" for message links

        # Skip message links — we want the channel, not a specific message
        if msg_suffix:
            continue

        # Skip known non-channel paths
        first_segment = channel_part.split("/")[0].lower()
        if first_segment in _TME_IGNORE_PATHS:
            continue

        url = f"https://t.me/{channel_part}"
        low = url.lower()
        if low not in seen:
            seen.add(low)
            result.append(url)
    return result


# ------------------------------------------------------------------
# Interactive session setup — run once to authenticate
# ------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    async def _interactive_login():
        if not settings.telegram_api_id or not settings.telegram_api_hash:
            print("Error: Set DARKDISCO_TELEGRAM_API_ID and DARKDISCO_TELEGRAM_API_HASH first")
            sys.exit(1)

        session_path = str(Path(settings.telegram_session_name).expanduser())
        client = TelegramClient(session_path, settings.telegram_api_id, settings.telegram_api_hash)

        await client.start()
        me = await client.get_me()
        print(f"Authenticated as: {me.first_name} (@{me.username})")
        print(f"Session saved to: {session_path}.session")
        await client.disconnect()

    asyncio.run(_interactive_login())
