"""Tor forum connector — monitors dark web forums for mentions."""

from __future__ import annotations

import logging
from datetime import datetime

from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)


class ForumConnector(BaseConnector):
    """Monitors dark web forums for keyword matches.

    Accesses forums via Tor SOCKS proxy. Each forum has its own
    scraping logic due to varying page structures.
    """

    name = "forum"
    source_type = "forum"

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        # TODO: Implement forum scraping
        # - Configure target forums in source.config
        # - Use aiohttp with Tor SOCKS proxy
        # - Parse forum posts/threads for new content
        # - Track last-seen post IDs per forum
        logger.info("ForumConnector.poll() — not yet implemented")
        return []

    async def health_check(self) -> dict:
        return {"healthy": True, "message": "stub"}
