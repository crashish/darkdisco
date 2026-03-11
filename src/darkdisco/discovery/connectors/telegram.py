"""Telegram connector — monitors channels and groups for mentions."""

from __future__ import annotations

import logging
from datetime import datetime

from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)


class TelegramConnector(BaseConnector):
    """Monitors Telegram channels/groups for keyword matches.

    Uses Telegram Bot API to read messages from channels the bot has been
    added to. Channels are configured per source.
    """

    name = "telegram"
    source_type = "telegram"

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        # TODO: Implement Telegram polling
        # - Use bot token from config
        # - Track last_update_id per channel
        # - Extract text, images, files from messages
        logger.info("TelegramConnector.poll() — not yet implemented")
        return []

    async def health_check(self) -> dict:
        return {"healthy": True, "message": "stub"}
