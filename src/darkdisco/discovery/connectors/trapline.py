"""Trapline SaaS connector — sync institution watchlists and ingest phishing findings.

Darkdisco acts as a trapline client:
- Outbound: pushes institution domains, names, and BINs to trapline's watchlist API
- Inbound: receives phishing findings via webhook (handled in routes, not here)

The poll() method syncs the watchlist; it does not return RawMentions since
trapline delivers findings via webhook push rather than polling.
"""

from __future__ import annotations

import logging
from datetime import datetime

import httpx

from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)


class TraplineConnector(BaseConnector):
    name = "trapline"
    source_type = "trapline"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.api_url = self.config.get("api_url", "").rstrip("/")
        self.api_key = self.config.get("api_key", "")
        self._client: httpx.AsyncClient | None = None

    async def setup(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self.api_url,
            headers={
                "X-API-Key": self.api_key,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    async def teardown(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        """Sync institution watchlist data to trapline. Returns empty list.

        Trapline delivers findings via webhook, so polling only pushes
        our watchlist updates outbound.
        """
        await self._sync_watchlist()
        return []

    async def health_check(self) -> dict:
        if not self.api_url or not self.api_key:
            return {"healthy": False, "message": "Trapline API URL or key not configured"}
        try:
            assert self._client is not None
            resp = await self._client.get("/api/dashboard/health")
            resp.raise_for_status()
            return {"healthy": True, "message": "Trapline API reachable"}
        except Exception as exc:
            return {"healthy": False, "message": str(exc)}

    async def _sync_watchlist(self) -> None:
        """Push all active institution domains, names, and BINs to trapline."""
        from sqlalchemy import select

        from darkdisco.common.database import async_session
        from darkdisco.common.models import Institution

        async with async_session() as session:
            result = await session.execute(
                select(Institution).where(Institution.active.is_(True))
            )
            institutions = result.scalars().all()

        if not institutions:
            logger.info("No active institutions to sync to trapline")
            return

        # Build watchlist payloads in trapline's expected format:
        #   domains: {"entries": [{"type": "domain", "value": "..."}, {"type": "brand", "value": "..."}]}
        #   bins:    {"bins": [{"bin_prefix": "...", "issuer": "..."}]}
        domain_entries = []
        bin_entries = []

        for inst in institutions:
            # Add institution name as a brand entry
            if inst.name:
                domain_entries.append({"type": "brand", "value": inst.name})

            # Add all domains
            if inst.primary_domain:
                domain_entries.append({"type": "domain", "value": inst.primary_domain})
            if inst.additional_domains:
                for d in inst.additional_domains:
                    domain_entries.append({"type": "domain", "value": d})

            # Collect BIN ranges
            if inst.bin_ranges:
                for bin_prefix in inst.bin_ranges:
                    bin_entries.append({
                        "bin_prefix": str(bin_prefix),
                        "issuer": inst.name,
                    })

        assert self._client is not None

        # Sync domains + brands
        if domain_entries:
            resp = await self._client.post(
                "/api/v1/watchlist/domains",
                json={"entries": domain_entries},
            )
            resp.raise_for_status()
            logger.info(
                "Synced %d watchlist entries (%d domains, %d brands) to trapline",
                len(domain_entries),
                sum(1 for e in domain_entries if e["type"] == "domain"),
                sum(1 for e in domain_entries if e["type"] == "brand"),
            )

        # Sync BINs
        if bin_entries:
            resp = await self._client.post(
                "/api/v1/watchlist/bins",
                json={"bins": bin_entries},
            )
            resp.raise_for_status()
            logger.info(
                "Synced %d BIN entries to trapline",
                len(bin_entries),
            )
