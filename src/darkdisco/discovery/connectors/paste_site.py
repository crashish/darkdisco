"""Paste site connector — monitors paste services for credential dumps and data leaks.

Supports clearnet paste sites (Pastebin, Rentry, dpaste) and Tor-hosted
PrivateBin instances.  Tracks seen paste IDs to avoid duplicate processing.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from xml.etree import ElementTree

import aiohttp
from aiohttp_socks import ProxyConnector

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

# Default paste sites when none configured via source.config["sites"]
DEFAULT_SITES: list[dict] = [
    {
        "name": "pastebin",
        "type": "pastebin_api",
        "base_url": "https://pastebin.com",
        "scrape_url": "https://scrape.pastebin.com/api_scraping.php",
        "tor": False,
    },
    {
        "name": "rentry",
        "type": "rentry",
        "base_url": "https://rentry.co",
        "tor": False,
    },
    {
        "name": "dpaste",
        "type": "dpaste",
        "base_url": "https://dpaste.org",
        "tor": False,
    },
]

# Rate-limit defaults (seconds between requests per site)
RATE_LIMIT_PASTEBIN = 2.0  # Pastebin scraping API enforces ~1 req/sec
RATE_LIMIT_DEFAULT = 1.0


class PasteSiteConnector(BaseConnector):
    """Monitors paste sites for keyword matches.

    Config keys (via ``source.config``):
        sites: list of site dicts (name, type, base_url, tor, optional scrape_url)
        keywords: list of search terms (used for Pastebin scrape filtering)
        max_pastes_per_poll: cap on pastes fetched per poll cycle (default 250)
    """

    name = "paste_site"
    source_type = "paste_site"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._sites: list[dict] = self.config.get("sites") or DEFAULT_SITES
        self._max_per_poll: int = self.config.get("max_pastes_per_poll", 250)
        self._seen_ids: set[str] = set(self.config.get("seen_ids", []))
        self._last_request_at: dict[str, float] = {}
        self._session: aiohttp.ClientSession | None = None
        self._tor_session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "DarkDisco/1.0"},
        )
        if any(s.get("tor") for s in self._sites):
            connector = ProxyConnector.from_url(settings.tor_socks_proxy)
            self._tor_session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=60),
                headers={"User-Agent": "DarkDisco/1.0"},
            )

    async def teardown(self) -> None:
        if self._session:
            await self._session.close()
        if self._tor_session:
            await self._tor_session.close()

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        if not self._session:
            await self.setup()

        mentions: list[RawMention] = []

        for site in self._sites:
            try:
                site_mentions = await self._poll_site(site, since)
                mentions.extend(site_mentions)
            except Exception:
                logger.exception("Error polling paste site %s", site.get("name"))

        logger.info(
            "PasteSiteConnector.poll() — fetched %d mentions from %d sites",
            len(mentions),
            len(self._sites),
        )
        return mentions

    async def _poll_site(
        self, site: dict, since: datetime | None
    ) -> list[RawMention]:
        site_type = site.get("type", "generic")
        dispatch = {
            "pastebin_api": self._poll_pastebin,
            "rentry": self._poll_rentry,
            "dpaste": self._poll_dpaste,
            "privatebin": self._poll_privatebin,
        }
        handler = dispatch.get(site_type, self._poll_generic)
        return await handler(site, since)

    # ------------------------------------------------------------------
    # Pastebin (scraping API)
    # ------------------------------------------------------------------

    async def _poll_pastebin(
        self, site: dict, since: datetime | None
    ) -> list[RawMention]:
        """Use Pastebin's scraping API to fetch recent public pastes."""
        api_key = settings.pastebin_api_key
        if not api_key:
            logger.warning("Pastebin API key not configured — skipping")
            return []

        scrape_url = site.get("scrape_url", "https://scrape.pastebin.com/api_scraping.php")
        mentions: list[RawMention] = []

        await self._rate_limit("pastebin", RATE_LIMIT_PASTEBIN)
        assert self._session is not None

        # Fetch recent paste metadata
        params = {"limit": str(min(self._max_per_poll, 250))}
        async with self._session.get(scrape_url, params=params) as resp:
            if resp.status == 429:
                logger.warning("Pastebin rate limited — backing off")
                return []
            if resp.status != 200:
                logger.warning("Pastebin scrape API returned %d", resp.status)
                return []
            paste_list = await resp.json(content_type=None)

        if not isinstance(paste_list, list):
            logger.warning("Unexpected Pastebin response format")
            return []

        for paste_meta in paste_list:
            paste_key = paste_meta.get("key", "")
            paste_id = f"pastebin:{paste_key}"

            if paste_id in self._seen_ids:
                continue

            # Filter by timestamp if since is provided
            paste_date = paste_meta.get("date")
            if since and paste_date:
                try:
                    paste_dt = datetime.fromtimestamp(int(paste_date), tz=timezone.utc)
                    if paste_dt < since:
                        continue
                except (ValueError, TypeError):
                    pass

            # Fetch paste content
            await self._rate_limit("pastebin", RATE_LIMIT_PASTEBIN)
            raw_url = f"https://scrape.pastebin.com/api_scrape_item.php?i={paste_key}"
            content = await self._fetch_text(self._session, raw_url)
            if content is None:
                continue

            self._seen_ids.add(paste_id)
            mentions.append(
                RawMention(
                    source_name="pastebin",
                    source_url=f"https://pastebin.com/{paste_key}",
                    title=paste_meta.get("title", "Untitled"),
                    content=content[: settings.max_content_size],
                    author=paste_meta.get("user", None),
                    discovered_at=datetime.now(tz=timezone.utc),
                    metadata={
                        "paste_id": paste_id,
                        "syntax": paste_meta.get("syntax", ""),
                        "size": paste_meta.get("size", 0),
                        "content_hash": hashlib.sha256(
                            content.encode(errors="replace")
                        ).hexdigest(),
                    },
                )
            )

            if len(mentions) >= self._max_per_poll:
                break

        return mentions

    # ------------------------------------------------------------------
    # Rentry
    # ------------------------------------------------------------------

    async def _poll_rentry(
        self, site: dict, since: datetime | None
    ) -> list[RawMention]:
        """Poll Rentry recent pastes via its RSS/Atom feed."""
        base_url = site.get("base_url", "https://rentry.co")
        feed_url = f"{base_url}/feed"
        session = self._tor_session if site.get("tor") else self._session
        assert session is not None

        await self._rate_limit("rentry", RATE_LIMIT_DEFAULT)
        content = await self._fetch_text(session, feed_url)
        if content is None:
            return []

        return self._parse_atom_feed(content, "rentry", base_url, since)

    # ------------------------------------------------------------------
    # dpaste
    # ------------------------------------------------------------------

    async def _poll_dpaste(
        self, site: dict, since: datetime | None
    ) -> list[RawMention]:
        """Poll dpaste recent pastes by scraping the recent page."""
        base_url = site.get("base_url", "https://dpaste.org")
        session = self._tor_session if site.get("tor") else self._session
        assert session is not None

        return await self._scrape_recent_page(
            session, base_url, "dpaste", since
        )

    async def _scrape_recent_page(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        site_name: str,
        since: datetime | None,
    ) -> list[RawMention]:
        """Scrape a paste site's 'recent pastes' page for links and content."""
        await self._rate_limit(site_name, RATE_LIMIT_DEFAULT)
        html = await self._fetch_text(session, f"{base_url}/")
        if html is None:
            return []

        mentions: list[RawMention] = []
        # Extract paste links — look for href patterns like /XXXX or /paste/XXXX
        paste_paths = re.findall(r'href="(/[A-Za-z0-9_-]{4,})"', html)
        seen_paths: set[str] = set()

        for path in paste_paths:
            if path in seen_paths:
                continue
            seen_paths.add(path)

            paste_id = f"{site_name}:{path}"
            if paste_id in self._seen_ids:
                continue

            await self._rate_limit(site_name, RATE_LIMIT_DEFAULT)
            paste_url = f"{base_url}{path}"
            paste_content = await self._fetch_text(session, paste_url)
            if paste_content is None:
                continue

            self._seen_ids.add(paste_id)
            mentions.append(
                RawMention(
                    source_name=site_name,
                    source_url=paste_url,
                    title=path.strip("/"),
                    content=paste_content[: settings.max_content_size],
                    discovered_at=datetime.now(tz=timezone.utc),
                    metadata={
                        "paste_id": paste_id,
                        "content_hash": hashlib.sha256(
                            paste_content.encode(errors="replace")
                        ).hexdigest(),
                    },
                )
            )

            if len(mentions) >= self._max_per_poll:
                break

        return mentions

    # ------------------------------------------------------------------
    # PrivateBin (Tor .onion instances)
    # ------------------------------------------------------------------

    async def _poll_privatebin(
        self, site: dict, since: datetime | None
    ) -> list[RawMention]:
        """Poll a PrivateBin instance.

        PrivateBin pastes are encrypted client-side, so we can only monitor
        discussion/comment endpoints or known paste URLs shared elsewhere.
        For .onion instances we route via the Tor proxy.
        """
        base_url = site.get("base_url", "")
        if not base_url:
            logger.warning("PrivateBin site missing base_url — skipping")
            return []

        session = self._tor_session if site.get("tor") else self._session
        if session is None:
            logger.warning(
                "Tor session not available for PrivateBin .onion site %s",
                base_url,
            )
            return []

        # PrivateBin doesn't expose a listing API.  Poll known paste URLs
        # from config if provided.
        known_urls: list[str] = site.get("known_paste_urls", [])
        mentions: list[RawMention] = []

        for url in known_urls:
            paste_id = f"privatebin:{hashlib.sha256(url.encode()).hexdigest()[:16]}"
            if paste_id in self._seen_ids:
                continue

            await self._rate_limit("privatebin", RATE_LIMIT_DEFAULT)
            try:
                async with session.get(f"{url}&json") as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
            except Exception:
                logger.debug("Failed to fetch PrivateBin paste %s", url)
                continue

            # PrivateBin JSON response contains ct (ciphertext) — we store
            # the metadata even though content is encrypted, as the paste's
            # existence at a known URL is itself an indicator.
            self._seen_ids.add(paste_id)
            mentions.append(
                RawMention(
                    source_name="privatebin",
                    source_url=url,
                    title="PrivateBin paste",
                    content=data.get("ct", "")[: settings.max_content_size],
                    discovered_at=datetime.now(tz=timezone.utc),
                    metadata={
                        "paste_id": paste_id,
                        "format": data.get("adata", [{}])[0] if data.get("adata") else {},
                        "content_hash": hashlib.sha256(
                            str(data.get("ct", "")).encode()
                        ).hexdigest(),
                    },
                )
            )

        return mentions

    # ------------------------------------------------------------------
    # Generic fallback
    # ------------------------------------------------------------------

    async def _poll_generic(
        self, site: dict, since: datetime | None
    ) -> list[RawMention]:
        """Fallback scraper for uncategorized paste sites."""
        base_url = site.get("base_url", "")
        if not base_url:
            return []

        session = self._tor_session if site.get("tor") else self._session
        if session is None:
            return []

        return await self._scrape_recent_page(
            session, base_url, site.get("name", "unknown"), since
        )

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def health_check(self) -> dict:
        results: dict[str, str] = {}
        if not self._session:
            await self.setup()

        for site in self._sites:
            name = site.get("name", "unknown")
            url = site.get("base_url", "")
            session = self._tor_session if site.get("tor") else self._session
            if not session or not url:
                results[name] = "skipped"
                continue
            try:
                async with session.head(url, allow_redirects=True) as resp:
                    results[name] = "ok" if resp.status < 400 else f"http_{resp.status}"
            except Exception as exc:
                results[name] = f"error: {exc}"

        all_ok = all(v == "ok" for v in results.values() if v != "skipped")
        return {
            "healthy": all_ok,
            "message": "all reachable" if all_ok else "some sites unreachable",
            "sites": results,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _rate_limit(self, site_key: str, interval: float) -> None:
        """Enforce minimum interval between requests to a site."""
        last = self._last_request_at.get(site_key, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < interval:
            await asyncio.sleep(interval - elapsed)
        self._last_request_at[site_key] = time.monotonic()

    async def _fetch_text(
        self, session: aiohttp.ClientSession, url: str
    ) -> str | None:
        """Fetch a URL and return its text body, or None on failure."""
        try:
            async with session.get(url) as resp:
                if resp.status == 429:
                    logger.warning("Rate limited on %s", url)
                    return None
                if resp.status != 200:
                    return None
                return await resp.text(errors="replace")
        except Exception:
            logger.debug("Failed to fetch %s", url, exc_info=True)
            return None

    def _parse_atom_feed(
        self,
        xml_text: str,
        site_name: str,
        base_url: str,
        since: datetime | None,
    ) -> list[RawMention]:
        """Parse an Atom/RSS feed into RawMention objects."""
        mentions: list[RawMention] = []
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError:
            logger.warning("Failed to parse feed from %s", site_name)
            return []

        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # Try Atom entries first, fall back to RSS items
        entries = root.findall(".//atom:entry", ns) or root.findall(".//item")

        for entry in entries:
            title_el = entry.find("atom:title", ns) or entry.find("title")
            link_el = entry.find("atom:link", ns) or entry.find("link")
            content_el = (
                entry.find("atom:content", ns)
                or entry.find("atom:summary", ns)
                or entry.find("description")
            )
            updated_el = (
                entry.find("atom:updated", ns)
                or entry.find("atom:published", ns)
                or entry.find("pubDate")
            )

            title = title_el.text if title_el is not None and title_el.text else "Untitled"
            link = ""
            if link_el is not None:
                link = link_el.get("href", "") or (link_el.text or "")
            content = content_el.text if content_el is not None and content_el.text else ""

            paste_id = f"{site_name}:{hashlib.sha256(link.encode()).hexdigest()[:16]}"
            if paste_id in self._seen_ids:
                continue

            # Filter by date if possible
            if since and updated_el is not None and updated_el.text:
                try:
                    entry_dt = datetime.fromisoformat(
                        updated_el.text.replace("Z", "+00:00")
                    )
                    if entry_dt < since:
                        continue
                except ValueError:
                    pass

            self._seen_ids.add(paste_id)
            mentions.append(
                RawMention(
                    source_name=site_name,
                    source_url=link or f"{base_url}/",
                    title=title,
                    content=content[: settings.max_content_size],
                    discovered_at=datetime.now(tz=timezone.utc),
                    metadata={
                        "paste_id": paste_id,
                        "content_hash": hashlib.sha256(
                            content.encode(errors="replace")
                        ).hexdigest(),
                    },
                )
            )

            if len(mentions) >= self._max_per_poll:
                break

        return mentions
