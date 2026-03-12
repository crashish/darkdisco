"""Discord connector — monitors guild channels via discord.py bot.

Uses a Discord bot account to passively read messages from configured
channels across multiple guilds. The bot must be invited to target servers
with MESSAGE_CONTENT intent enabled.

Setup:
    1. Create a bot at https://discord.com/developers/applications
    2. Enable MESSAGE_CONTENT privileged intent
    3. Generate invite URL with permissions: Read Messages, Read Message History
    4. Set DARKDISCO_DISCORD_BOT_TOKEN in .env

Config (source.config JSONB):
    guild_channels: dict[str, list[str]]  — guild_id -> list of channel_ids
    last_message_ids: dict[str, str]  — channel_id -> last seen message ID (snowflake)
    history_limit: int  — max messages to fetch per channel per poll (default 100)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

HISTORY_LIMIT_DEFAULT = 100
INTER_CHANNEL_DELAY = 0.5  # seconds between channel reads


class DiscordConnector(BaseConnector):
    """Monitors Discord guild channels via a bot account (discord.py)."""

    name = "discord"
    source_type = "discord"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._bot = None
        self._http = None

    @property
    def _guild_channels(self) -> dict[str, list[str]]:
        return self.config.get("guild_channels", {})

    @property
    def _last_message_ids(self) -> dict[str, str]:
        return self.config.setdefault("last_message_ids", {})

    @property
    def _history_limit(self) -> int:
        return self.config.get("history_limit", HISTORY_LIMIT_DEFAULT)

    async def setup(self) -> None:
        token = settings.discord_bot_token
        if not token:
            raise RuntimeError(
                "DARKDISCO_DISCORD_BOT_TOKEN must be configured"
            )

        try:
            import discord
        except ImportError:
            raise RuntimeError(
                "discord.py is required for the Discord connector. "
                "Install it with: pip install discord.py"
            )

        intents = discord.Intents.default()
        intents.message_content = True

        self._bot = discord.Client(intents=intents)

        # Connect without blocking on the event loop
        # We use the HTTP client directly for polling
        await self._bot.login(token)
        self._http = self._bot.http

    async def teardown(self) -> None:
        if self._bot:
            await self._bot.close()
            self._bot = None
            self._http = None

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        if not self._guild_channels:
            logger.warning("No guild channels configured for Discord connector")
            return []

        if self._http is None:
            await self.setup()

        all_mentions: list[RawMention] = []

        for guild_id, channel_ids in self._guild_channels.items():
            for channel_id in channel_ids:
                try:
                    mentions = await self._poll_channel(
                        guild_id, channel_id, since
                    )
                    all_mentions.extend(mentions)
                    logger.info(
                        "Discord channel %s/%s: %d mentions",
                        guild_id, channel_id, len(mentions),
                    )
                except Exception:
                    logger.exception(
                        "Failed to poll Discord channel %s/%s",
                        guild_id, channel_id,
                    )

                await asyncio.sleep(INTER_CHANNEL_DELAY)

        logger.info(
            "Discord poll complete: %d total mentions from %d guilds",
            len(all_mentions), len(self._guild_channels),
        )
        return all_mentions

    async def health_check(self) -> dict:
        if self._http is None:
            try:
                await self.setup()
            except Exception as exc:
                return {"healthy": False, "message": str(exc)[:200]}

        assert self._http is not None  # guaranteed by setup()
        try:
            user = await self._http.get_user("@me")
            username = f"{user['username']}#{user.get('discriminator', '0')}"
            guild_count = len(self._guild_channels)
            channel_count = sum(
                len(chs) for chs in self._guild_channels.values()
            )
            return {
                "healthy": True,
                "message": f"Connected as {username}",
                "guilds_configured": guild_count,
                "channels_configured": channel_count,
            }
        except Exception as exc:
            return {"healthy": False, "message": str(exc)[:200]}

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _poll_channel(
        self,
        guild_id: str,
        channel_id: str,
        since: datetime | None,
    ) -> list[RawMention]:
        import discord

        # Get high-water mark for incremental reads
        after_id = self._last_message_ids.get(channel_id)
        new_high_water = after_id

        # Fetch messages from the Discord API
        params: dict[str, Any] = {"limit": self._history_limit}
        if after_id:
            params["after"] = int(after_id)

        assert self._http is not None  # guaranteed by poll() -> setup()
        try:
            messages = await self._http.logs_from(
                int(channel_id), **params
            )
        except discord.Forbidden:
            logger.warning(
                "No permission to read channel %s in guild %s",
                channel_id, guild_id,
            )
            return []
        except discord.NotFound:
            logger.warning(
                "Channel %s not found in guild %s",
                channel_id, guild_id,
            )
            return []

        mentions: list[RawMention] = []

        for msg_data in messages:
            msg_id = msg_data["id"]

            # Track highest message ID seen
            if new_high_water is None or int(msg_id) > int(new_high_water):
                new_high_water = msg_id

            # Time filter
            if since:
                msg_time = datetime.fromisoformat(
                    msg_data["timestamp"].replace("+00:00", "+00:00")
                )
                if msg_time.tzinfo is None:
                    msg_time = msg_time.replace(tzinfo=timezone.utc)
                if msg_time < since:
                    continue

            mention = _message_to_mention(msg_data, guild_id, channel_id)
            if mention is not None:
                mentions.append(mention)

        # Update bookmark
        if new_high_water and (
            after_id is None or int(new_high_water) > int(after_id)
        ):
            self._last_message_ids[channel_id] = str(new_high_water)

        return mentions


# ------------------------------------------------------------------
# Pure helpers
# ------------------------------------------------------------------


def _message_to_mention(
    msg: dict, guild_id: str, channel_id: str,
) -> RawMention | None:
    """Convert a Discord message dict to a RawMention."""
    content = msg.get("content", "")

    # Also include embed descriptions
    embeds = msg.get("embeds", [])
    for embed in embeds:
        if embed.get("description"):
            content += "\n" + embed["description"]
        if embed.get("title"):
            content += "\n" + embed["title"]

    if not content.strip():
        return None

    # Author
    author_data = msg.get("author", {})
    author = author_data.get("username")
    if author and author_data.get("discriminator", "0") != "0":
        author = f"{author}#{author_data['discriminator']}"

    # Timestamp
    ts_str = msg.get("timestamp", "")
    try:
        discovered = datetime.fromisoformat(ts_str)
        if discovered.tzinfo is None:
            discovered = discovered.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        discovered = datetime.now(tz=timezone.utc)

    # Source URL
    source_url = (
        f"https://discord.com/channels/{guild_id}/{channel_id}/{msg['id']}"
    )

    # Metadata
    metadata: dict[str, Any] = {
        "message_id": msg["id"],
        "guild_id": guild_id,
        "channel_id": channel_id,
    }

    if msg.get("attachments"):
        metadata["has_attachments"] = True
        metadata["attachment_count"] = len(msg["attachments"])
        for att in msg["attachments"]:
            if att.get("filename"):
                metadata.setdefault("file_names", []).append(att["filename"])

    if msg.get("referenced_message"):
        ref = msg["referenced_message"]
        ref_author = ref.get("author", {}).get("username", "unknown")
        metadata["reply_to"] = ref_author

    return RawMention(
        source_name=f"discord:{guild_id}/{channel_id}",
        source_url=source_url,
        title=f"Discord #{channel_id}",
        content=content,
        author=author,
        discovered_at=discovered,
        metadata=metadata,
    )
