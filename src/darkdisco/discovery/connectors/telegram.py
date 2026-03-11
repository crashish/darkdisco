"""Telegram connector — monitors channels and groups for mentions.

Uses the Telegram Bot API directly via aiohttp. The bot must be added to
each target channel/group with read permissions. Messages are fetched via
getUpdates with offset tracking to avoid duplicates.

Config (source.config JSONB):
    channels: list[str]  — channel/group usernames or IDs to monitor
    last_update_id: int  — internal bookmark (auto-managed)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org"
# Telegram Bot API rate limit: ~30 requests/sec globally, 1 req/sec per chat
RATE_LIMIT_DELAY = 0.5  # seconds between getUpdates calls
MAX_UPDATES_PER_POLL = 100  # Telegram max per getUpdates call
# Retry config for transient failures
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # exponential base in seconds


class TelegramConnector(BaseConnector):
    """Monitors Telegram channels/groups for keyword matches.

    Uses Telegram Bot API to read messages from channels the bot has been
    added to. Channels are configured per source in the config JSONB field.

    Expected config:
        {
            "channels": ["channel_username", "-100123456789", ...],
            "last_update_id": 0   # auto-managed offset bookmark
        }
    """

    name = "telegram"
    source_type = "telegram"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._bot_token: str = settings.telegram_bot_token
        self._session: aiohttp.ClientSession | None = None

    @property
    def _base_url(self) -> str:
        return f"{TELEGRAM_API}/bot{self._bot_token}"

    @property
    def _channels(self) -> list[str]:
        return self.config.get("channels", [])

    @property
    def _last_update_id(self) -> int:
        return self.config.get("last_update_id", 0)

    @_last_update_id.setter
    def _last_update_id(self, value: int) -> None:
        self.config["last_update_id"] = value

    async def setup(self) -> None:
        if not self._bot_token:
            raise RuntimeError(
                "DARKDISCO_TELEGRAM_BOT_TOKEN not configured — "
                "cannot initialize Telegram connector"
            )
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        )

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        """Fetch new messages from monitored Telegram channels/groups.

        Uses getUpdates with offset to track position. Only returns messages
        from channels listed in self.config["channels"].
        """
        if not self._bot_token:
            logger.warning("Telegram bot token not configured, skipping poll")
            return []

        if not self._channels:
            logger.warning("No channels configured for Telegram connector")
            return []

        if self._session is None:
            await self.setup()

        # Build a set of allowed channel identifiers for filtering
        allowed = _normalize_channel_set(self._channels)

        mentions: list[RawMention] = []
        offset = self._last_update_id + 1 if self._last_update_id else 0

        while True:
            updates = await self._get_updates(offset=offset)
            if not updates:
                break

            for update in updates:
                update_id = update["update_id"]
                # Always advance the offset
                if update_id >= offset:
                    offset = update_id + 1

                msg = _extract_message(update)
                if msg is None:
                    continue

                # Filter to only monitored channels
                chat = msg.get("chat", {})
                if not _chat_matches(chat, allowed):
                    continue

                mention = _message_to_mention(msg, chat)
                if mention is not None:
                    # Apply time filter if requested
                    if since and mention.discovered_at < since:
                        continue
                    mentions.append(mention)

            # Persist the high-water mark
            self._last_update_id = offset - 1

            # Respect rate limits between pages
            await asyncio.sleep(RATE_LIMIT_DELAY)

            # If we got fewer than the max, we've consumed all pending updates
            if len(updates) < MAX_UPDATES_PER_POLL:
                break

        logger.info(
            "Telegram poll complete: %d mentions from %d channels",
            len(mentions),
            len(self._channels),
        )
        return mentions

    async def health_check(self) -> dict:
        """Verify bot token is valid by calling getMe."""
        if not self._bot_token:
            return {"healthy": False, "message": "Bot token not configured"}

        if self._session is None:
            await self.setup()

        try:
            data = await self._api_call("getMe")
            bot_name = data.get("username", "unknown")
            return {
                "healthy": True,
                "message": f"Connected as @{bot_name}",
                "bot_username": bot_name,
                "channels_configured": len(self._channels),
            }
        except Exception as exc:
            return {"healthy": False, "message": str(exc)}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _api_call(self, method: str, **params: Any) -> Any:
        """Call a Telegram Bot API method with retry logic."""
        url = f"{self._base_url}/{method}"
        last_exc: Exception | None = None

        for attempt in range(MAX_RETRIES):
            try:
                async with self._session.get(url, params=params) as resp:  # type: ignore[union-attr]
                    if resp.status == 429:
                        # Rate limited — honor Retry-After header
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        logger.warning(
                            "Telegram rate limited, retrying in %ds", retry_after
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    body = await resp.json()

                    if not body.get("ok"):
                        error_code = body.get("error_code", resp.status)
                        description = body.get("description", "Unknown error")
                        raise TelegramAPIError(error_code, description)

                    return body.get("result")

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                wait = RETRY_BACKOFF ** (attempt + 1)
                logger.warning(
                    "Telegram API %s failed (attempt %d/%d): %s — retrying in %ds",
                    method, attempt + 1, MAX_RETRIES, exc, wait,
                )
                await asyncio.sleep(wait)

        raise TelegramAPIError(
            0, f"Failed after {MAX_RETRIES} retries: {last_exc}"
        )

    async def _get_updates(self, offset: int = 0) -> list[dict]:
        """Fetch pending updates from the Bot API."""
        params: dict[str, Any] = {
            "limit": MAX_UPDATES_PER_POLL,
            "timeout": 0,  # non-blocking (short poll)
            "allowed_updates": '["message","channel_post"]',
        }
        if offset:
            params["offset"] = offset

        return await self._api_call("getUpdates", **params)


class TelegramAPIError(Exception):
    """Raised when the Telegram Bot API returns an error."""

    def __init__(self, code: int, description: str):
        self.code = code
        self.description = description
        super().__init__(f"Telegram API error {code}: {description}")


# ------------------------------------------------------------------
# Pure helpers (no side effects — easy to test)
# ------------------------------------------------------------------


def _normalize_channel_set(channels: list[str]) -> set[str]:
    """Build a set of normalized channel identifiers for matching.

    Accepts usernames (with or without @) and numeric chat IDs.
    """
    normalized: set[str] = set()
    for ch in channels:
        ch = ch.strip()
        if not ch:
            continue
        # Strip leading @ for username matching
        if ch.startswith("@"):
            ch = ch[1:]
        normalized.add(ch.lower())
    return normalized


def _chat_matches(chat: dict, allowed: set[str]) -> bool:
    """Check if a chat dict matches one of the allowed channels."""
    # Match by username
    username = chat.get("username", "")
    if username and username.lower() in allowed:
        return True
    # Match by numeric ID (as string)
    chat_id = str(chat.get("id", ""))
    if chat_id in allowed:
        return True
    # Match by title (for groups without usernames)
    title = chat.get("title", "")
    if title and title.lower() in allowed:
        return True
    return False


def _extract_message(update: dict) -> dict | None:
    """Extract the message payload from an update.

    Telegram sends channel messages as 'channel_post' and group messages
    as 'message'. We handle both.
    """
    return update.get("channel_post") or update.get("message")


def _message_to_mention(msg: dict, chat: dict) -> RawMention | None:
    """Convert a Telegram message dict to a RawMention."""
    # Extract text content — may be in text or caption (for media)
    text = msg.get("text", "") or msg.get("caption", "")
    if not text:
        # Skip messages with no text content (stickers, etc.)
        return None

    # Build author string
    sender = msg.get("from", {})
    author_parts = [
        sender.get("first_name", ""),
        sender.get("last_name", ""),
    ]
    author = " ".join(p for p in author_parts if p).strip() or None
    if not author and sender.get("username"):
        author = f"@{sender['username']}"

    # Channel title as the "title" field
    title = chat.get("title", "")

    # Timestamps
    msg_date = msg.get("date", 0)
    discovered = (
        datetime.fromtimestamp(msg_date, tz=timezone.utc) if msg_date
        else datetime.now(tz=timezone.utc)
    )

    # Build metadata with file/media info
    metadata = _extract_media_metadata(msg)
    metadata["message_id"] = msg.get("message_id")
    metadata["chat_id"] = chat.get("id")
    metadata["chat_type"] = chat.get("type")
    if sender.get("username"):
        metadata["sender_username"] = sender["username"]

    # Source URL: link to specific message if possible
    chat_username = chat.get("username")
    msg_id = msg.get("message_id")
    source_url = None
    if chat_username and msg_id:
        source_url = f"https://t.me/{chat_username}/{msg_id}"

    return RawMention(
        source_name="telegram",
        source_url=source_url,
        title=title,
        content=text,
        author=author,
        discovered_at=discovered,
        metadata=metadata,
    )


def _extract_media_metadata(msg: dict) -> dict:
    """Extract metadata about attached media (documents, photos, etc.)."""
    meta: dict[str, Any] = {}

    if "document" in msg:
        doc = msg["document"]
        meta["has_document"] = True
        meta["document_file_name"] = doc.get("file_name")
        meta["document_mime_type"] = doc.get("mime_type")
        meta["document_file_size"] = doc.get("file_size")

    if "photo" in msg:
        # photo is an array of PhotoSize; take the largest
        photos = msg["photo"]
        if photos:
            largest = max(photos, key=lambda p: p.get("file_size", 0))
            meta["has_photo"] = True
            meta["photo_file_id"] = largest.get("file_id")
            meta["photo_width"] = largest.get("width")
            meta["photo_height"] = largest.get("height")

    if "video" in msg:
        video = msg["video"]
        meta["has_video"] = True
        meta["video_duration"] = video.get("duration")
        meta["video_file_size"] = video.get("file_size")

    if "sticker" in msg:
        meta["has_sticker"] = True

    if "forward_from_chat" in msg:
        fwd = msg["forward_from_chat"]
        meta["forwarded_from"] = fwd.get("title") or fwd.get("username")

    return meta
