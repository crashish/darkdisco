"""Paste site connector — monitors paste services for mentions."""

from __future__ import annotations

import logging
from datetime import datetime

from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)


class PasteSiteConnector(BaseConnector):
    """Monitors paste sites (Pastebin, PrivateBin, etc.) for keyword matches.

    Supports both clearnet and Tor-hosted paste services.
    """

    name = "paste_site"
    source_type = "paste_site"

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        # TODO: Implement paste site scraping
        # Options:
        # - Pastebin scraping API (paid)
        # - PasteBin Google dorking fallback
        # - PrivateBin Tor instances
        # - Custom paste site list from config
        logger.info("PasteSiteConnector.poll() — not yet implemented")
        return []

    async def health_check(self) -> dict:
        return {"healthy": True, "message": "stub"}
