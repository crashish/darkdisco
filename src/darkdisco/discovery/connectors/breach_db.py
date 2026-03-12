"""Breach database connector — checks external APIs for leaked credentials.

Integrates with:
- DeHashed API (domain-based credential search)
- Have I Been Pwned (domain breach check)
- Intelligence X (leak/paste/darknet search)

Source config schema (stored in Source.config JSONB):
{
    "domains": ["example.com", "example.org"],
    "seen_hashes": ["<content_hash>", ...],
    "dehashed_enabled": true,
    "hibp_enabled": true,
    "intelx_enabled": true,
    "dehashed_max_results": 500,
    "intelx_max_results": 10,
    "request_delay_seconds": 2
}
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone

import aiohttp

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

_DEHASHED_BASE = "https://api.dehashed.com"
_HIBP_BASE = "https://haveibeenpwned.com/api/v3"
_INTELX_BASE = "https://2.intelx.io"

# HIBP rate-limits to ~1 req per 1.5s for the paid tier
_HIBP_DELAY = 1.6
# IntelX: wait for search results to populate before fetching
_INTELX_SEARCH_WAIT = 2.0

_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=15)
_DEFAULT_UA = "DarkDisco-BreachMonitor/1.0"


class BreachDBConnector(BaseConnector):
    """Checks breach/leak databases for institution-related credentials.

    Queries DeHashed, HIBP, and Intelligence X for domains configured in
    source.config. Returns one RawMention per breach or credential batch found.
    """

    name = "breach_db"
    source_type = "breach_db"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=_REQUEST_TIMEOUT,
            headers={"User-Agent": _DEFAULT_UA},
        )

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        if not self._session:
            await self.setup()

        domains = self.config.get("domains", [])
        if not domains:
            logger.warning("BreachDBConnector: no domains configured")
            return []

        seen_hashes: set[str] = set(self.config.get("seen_hashes", []))
        mentions: list[RawMention] = []
        delay = self.config.get("request_delay_seconds", 2)

        for domain in domains:
            if self.config.get("dehashed_enabled", True) and settings.dehashed_api_key:
                new = await self._poll_dehashed(domain, since, seen_hashes)
                mentions.extend(new)
                if delay:
                    await asyncio.sleep(delay)

            if self.config.get("hibp_enabled", True) and settings.hibp_api_key:
                new = await self._poll_hibp(domain, since, seen_hashes)
                mentions.extend(new)
                if delay:
                    await asyncio.sleep(max(delay, _HIBP_DELAY))

            if self.config.get("intelx_enabled", True) and settings.intelx_api_key:
                new = await self._poll_intelx(domain, since, seen_hashes)
                mentions.extend(new)
                if delay:
                    await asyncio.sleep(delay)

        # Persist seen hashes (cap at 10k)
        self.config["seen_hashes"] = list(seen_hashes)[-10000:]

        logger.info(
            "BreachDBConnector polled %d domains, found %d new mentions",
            len(domains), len(mentions),
        )
        return mentions

    # ------------------------------------------------------------------
    # DeHashed
    # ------------------------------------------------------------------

    async def _poll_dehashed(
        self,
        domain: str,
        since: datetime | None,
        seen_hashes: set[str],
    ) -> list[RawMention]:
        """Query DeHashed for credentials associated with a domain."""
        assert self._session is not None

        max_results = self.config.get("dehashed_max_results", 500)
        mentions: list[RawMention] = []

        auth = aiohttp.BasicAuth(settings.dehashed_email, settings.dehashed_api_key)
        params = {
            "query": f"domain:{domain}",
            "size": str(min(max_results, 10000)),
        }

        try:
            async with self._session.get(
                f"{_DEHASHED_BASE}/search",
                params=params,
                auth=auth,
                headers={"Accept": "application/json"},
            ) as resp:
                if resp.status == 401:
                    logger.error("DeHashed auth failed — check API key")
                    return []
                if resp.status == 429:
                    logger.warning("DeHashed rate-limited for domain %s", domain)
                    return []
                if resp.status != 200:
                    logger.warning("DeHashed HTTP %d for domain %s", resp.status, domain)
                    return []

                data = await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("DeHashed request failed for %s: %s", domain, exc)
            return []

        entries = data.get("entries") or []
        total = data.get("total", len(entries))

        if not entries:
            logger.debug("DeHashed: no results for %s", domain)
            return []

        # Group entries into a single mention per poll-domain batch
        # with individual credential details in metadata
        content_key = f"dehashed:{domain}:{total}:{len(entries)}"
        content_hash = hashlib.sha256(content_key.encode()).hexdigest()[:16]

        if content_hash in seen_hashes:
            return []

        # Build a summary of unique credential types found
        usernames = set()
        emails = set()
        passwords_found = 0
        hashed_passwords_found = 0

        for entry in entries:
            if entry.get("username"):
                usernames.add(entry["username"])
            if entry.get("email"):
                emails.add(entry["email"])
            if entry.get("password"):
                passwords_found += 1
            if entry.get("hashed_password"):
                hashed_passwords_found += 1

        parts = [
            f"DeHashed found {total} credential entries for domain {domain}.",
            f"Unique emails: {len(emails)}",
            f"Unique usernames: {len(usernames)}",
            f"Plaintext passwords: {passwords_found}",
            f"Hashed passwords: {hashed_passwords_found}",
        ]
        # Include sample databases breached
        databases = {e.get("database_name") for e in entries if e.get("database_name")}
        if databases:
            parts.append(f"Source databases: {', '.join(sorted(databases)[:10])}")

        seen_hashes.add(content_hash)
        mentions.append(RawMention(
            source_name=f"breach_db:dehashed",
            source_url=f"https://dehashed.com/search?query=domain:{domain}",
            title=f"[DeHashed] {total} credentials for {domain}",
            content="\n".join(parts),
            discovered_at=datetime.now(timezone.utc),
            metadata={
                "domain": domain,
                "provider": "dehashed",
                "total_entries": total,
                "unique_emails": len(emails),
                "unique_usernames": len(usernames),
                "plaintext_passwords": passwords_found,
                "hashed_passwords": hashed_passwords_found,
                "source_databases": sorted(databases)[:20],
                "content_hash": content_hash,
            },
        ))

        return mentions

    # ------------------------------------------------------------------
    # Have I Been Pwned
    # ------------------------------------------------------------------

    async def _poll_hibp(
        self,
        domain: str,
        since: datetime | None,
        seen_hashes: set[str],
    ) -> list[RawMention]:
        """Query HIBP for breaches affecting a domain."""
        assert self._session is not None

        mentions: list[RawMention] = []
        headers = {
            "hibp-api-key": settings.hibp_api_key,
            "Accept": "application/json",
        }

        try:
            async with self._session.get(
                f"{_HIBP_BASE}/breaches",
                params={"domain": domain},
                headers=headers,
            ) as resp:
                if resp.status == 401:
                    logger.error("HIBP auth failed — check API key")
                    return []
                if resp.status == 404:
                    logger.debug("HIBP: no breaches for domain %s", domain)
                    return []
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After", "")
                    logger.warning(
                        "HIBP rate-limited for domain %s (retry-after: %s)",
                        domain, retry_after,
                    )
                    return []
                if resp.status != 200:
                    logger.warning("HIBP HTTP %d for domain %s", resp.status, domain)
                    return []

                breaches = await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("HIBP request failed for %s: %s", domain, exc)
            return []

        if not breaches:
            return []

        for breach in breaches:
            breach_name = breach.get("Name", "unknown")
            content_hash = hashlib.sha256(
                f"hibp:{domain}:{breach_name}".encode()
            ).hexdigest()[:16]

            if content_hash in seen_hashes:
                continue

            # Filter by since date if provided
            breach_date_str = breach.get("BreachDate")
            breach_date = None
            if breach_date_str:
                try:
                    breach_date = datetime.strptime(
                        breach_date_str, "%Y-%m-%d"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

            if since and breach_date and breach_date < since:
                continue

            pwn_count = breach.get("PwnCount", 0)
            data_classes = breach.get("DataClasses", [])
            description = breach.get("Description", "")

            parts = [
                f"HIBP breach '{breach_name}' affects domain {domain}.",
                f"Breach date: {breach_date_str or 'unknown'}",
                f"Records exposed: {pwn_count:,}",
                f"Data types: {', '.join(data_classes[:15])}",
            ]
            if description:
                clean_desc = re.sub(r"<[^>]+>", "", description)
                parts.append(f"Description: {clean_desc[:500]}")

            seen_hashes.add(content_hash)
            mentions.append(RawMention(
                source_name="breach_db:hibp",
                source_url=f"https://haveibeenpwned.com/api/v3/breach/{breach_name}",
                title=f"[HIBP] {breach_name} — {pwn_count:,} records ({domain})",
                content="\n".join(parts),
                discovered_at=breach_date or datetime.now(timezone.utc),
                metadata={
                    "domain": domain,
                    "provider": "hibp",
                    "breach_name": breach_name,
                    "breach_date": breach_date_str,
                    "pwn_count": pwn_count,
                    "data_classes": data_classes,
                    "is_verified": breach.get("IsVerified", False),
                    "is_sensitive": breach.get("IsSensitive", False),
                    "content_hash": content_hash,
                },
            ))

        return mentions

    # ------------------------------------------------------------------
    # Intelligence X
    # ------------------------------------------------------------------

    async def _poll_intelx(
        self,
        domain: str,
        since: datetime | None,
        seen_hashes: set[str],
    ) -> list[RawMention]:
        """Query Intelligence X for leaks/pastes/darknet data for a domain."""
        assert self._session is not None

        max_results = self.config.get("intelx_max_results", 10)
        mentions: list[RawMention] = []
        headers = {
            "x-key": settings.intelx_api_key,
            "Content-Type": "application/json",
        }

        # Build search payload — media=0 means all types
        payload: dict = {
            "term": domain,
            "maxresults": max_results,
            "media": 0,
            "timeout": 10,
        }
        if since:
            payload["datefrom"] = since.strftime("%Y-%m-%d %H:%M:%S")

        try:
            # Start the search
            async with self._session.post(
                f"{_INTELX_BASE}/intelligent/search",
                json=payload,
                headers=headers,
            ) as resp:
                if resp.status == 401:
                    logger.error("IntelX auth failed — check API key")
                    return []
                if resp.status == 429:
                    logger.warning("IntelX rate-limited for domain %s", domain)
                    return []
                if resp.status != 200:
                    logger.warning("IntelX search HTTP %d for domain %s", resp.status, domain)
                    return []
                search_data = await resp.json()
                search_id = search_data.get("id")
                if not search_id:
                    logger.warning("IntelX: no search ID returned for %s", domain)
                    return []

            # Wait for results to populate
            await asyncio.sleep(_INTELX_SEARCH_WAIT)

            # Fetch results
            async with self._session.get(
                f"{_INTELX_BASE}/intelligent/search/result",
                params={"id": search_id},
                headers=headers,
            ) as resp:
                if resp.status != 200:
                    logger.warning("IntelX result HTTP %d for domain %s", resp.status, domain)
                    return []
                results = await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("IntelX request failed for %s: %s", domain, exc)
            return []

        records = results.get("records") or []
        if not records:
            logger.debug("IntelX: no results for %s", domain)
            return []

        for record in records:
            system_id = record.get("systemid", "")
            name = record.get("name", "unknown")
            bucket = record.get("bucket", "unknown")
            media_type = record.get("media", 0)
            added = record.get("added", "")
            size = record.get("size", 0)

            content_key = f"intelx:{domain}:{system_id}"
            content_hash = hashlib.sha256(content_key.encode()).hexdigest()[:16]

            if content_hash in seen_hashes:
                continue

            # Parse the added timestamp if present
            discovered_at = datetime.now(timezone.utc)
            if added:
                try:
                    discovered_at = datetime.fromisoformat(
                        added.replace("Z", "+00:00")
                    )
                except ValueError:
                    pass

            # Map IntelX media type codes to human-readable categories
            media_labels = {
                0: "all", 1: "paste", 2: "paste_doc", 3: "forum",
                4: "social_media", 5: "web", 6: "leak", 7: "wiki",
                8: "WHOIS", 9: "darknet", 10: "news", 11: "code",
                12: "government", 13: "torrent", 14: "social",
                24: "document",
            }
            media_label = media_labels.get(media_type, f"type_{media_type}")

            parts = [
                f"IntelX found '{name}' for domain {domain}.",
                f"Category: {media_label} (bucket: {bucket})",
                f"Size: {size:,} bytes" if size else "Size: unknown",
                f"Date added: {added or 'unknown'}",
            ]

            seen_hashes.add(content_hash)
            mentions.append(RawMention(
                source_name="breach_db:intelx",
                source_url=f"https://intelx.io/?s={domain}",
                title=f"[IntelX] {media_label}: {name} ({domain})",
                content="\n".join(parts),
                discovered_at=discovered_at,
                metadata={
                    "domain": domain,
                    "provider": "intelx",
                    "system_id": system_id,
                    "name": name,
                    "bucket": bucket,
                    "media_type": media_type,
                    "media_label": media_label,
                    "size": size,
                    "date_added": added,
                    "content_hash": content_hash,
                },
            ))

        return mentions

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def health_check(self) -> dict:
        if not self._session:
            await self.setup()
        assert self._session is not None

        checks: list[str] = []
        healthy = True

        # Check DeHashed
        if settings.dehashed_api_key:
            try:
                auth = aiohttp.BasicAuth(settings.dehashed_email, settings.dehashed_api_key)
                async with self._session.get(
                    f"{_DEHASHED_BASE}/search",
                    params={"query": "domain:example.com", "size": "1"},
                    auth=auth,
                    headers={"Accept": "application/json"},
                ) as resp:
                    if resp.status in (200, 400):
                        checks.append("dehashed:ok")
                    else:
                        checks.append(f"dehashed:http_{resp.status}")
                        healthy = False
            except aiohttp.ClientError as exc:
                checks.append(f"dehashed:error({exc})")
                healthy = False
        else:
            checks.append("dehashed:no_api_key")

        # Check HIBP
        if settings.hibp_api_key:
            try:
                async with self._session.get(
                    f"{_HIBP_BASE}/breaches",
                    params={"domain": "example.com"},
                    headers={
                        "hibp-api-key": settings.hibp_api_key,
                        "Accept": "application/json",
                    },
                ) as resp:
                    if resp.status in (200, 404):
                        checks.append("hibp:ok")
                    else:
                        checks.append(f"hibp:http_{resp.status}")
                        healthy = False
            except aiohttp.ClientError as exc:
                checks.append(f"hibp:error({exc})")
                healthy = False
        else:
            checks.append("hibp:no_api_key")

        # Check IntelX
        if settings.intelx_api_key:
            try:
                async with self._session.get(
                    f"{_INTELX_BASE}/authenticate/info",
                    headers={"x-key": settings.intelx_api_key},
                ) as resp:
                    if resp.status == 200:
                        checks.append("intelx:ok")
                    else:
                        checks.append(f"intelx:http_{resp.status}")
                        healthy = False
            except aiohttp.ClientError as exc:
                checks.append(f"intelx:error({exc})")
                healthy = False
        else:
            checks.append("intelx:no_api_key")

        return {
            "healthy": healthy,
            "message": "; ".join(checks),
        }
