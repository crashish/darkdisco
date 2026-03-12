"""Ransomware blog connector — monitors ransomware group leak sites."""

from __future__ import annotations

import hashlib
import logging
import re
import typing
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp
from aiohttp_socks import ProxyConnector

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

# Default request timeout for Tor connections (seconds)
_TOR_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=60)

# Common user-agent to blend in with Tor Browser traffic
_TOR_UA = "Mozilla/5.0 (Windows NT 10.0; rv:128.0) Gecko/20100101 Firefox/128.0"


@dataclass
class VictimListing:
    """A single victim entry parsed from a ransomware blog."""

    group_name: str
    victim_name: str
    url: str | None = None
    post_date: datetime | None = None
    data_volume: str | None = None
    countdown_deadline: datetime | None = None
    description: str = ""
    raw_html: str = ""
    metadata: dict = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        """Deterministic hash for deduplication."""
        key = f"{self.group_name}:{self.victim_name}:{self.post_date or ''}".lower()
        return hashlib.sha256(key.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Group-specific parsers
# ---------------------------------------------------------------------------
# Each parser receives raw HTML and returns a list of VictimListing.
# Ransomware blogs have wildly different layouts; each group needs its own
# parser. When a group rebrands or changes their page structure, only the
# corresponding parser needs updating.
# ---------------------------------------------------------------------------


def _parse_generic(html: str, group_name: str, base_url: str) -> list[VictimListing]:
    """Fallback parser: extract plausible victim entries via heuristics.

    Many ransomware blogs list victims in repeated HTML blocks (divs, cards,
    table rows) with a company name, date, and optional data-size claim.
    This parser looks for those patterns without assuming exact markup.
    """
    listings: list[VictimListing] = []

    # Strategy 1: look for repeated <div> or <article> blocks containing
    # company-like names and dates.
    # Common date patterns across many blogs
    date_re = re.compile(
        r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})"  # 2024-01-15 / 2024/01/15
        r"|(\d{1,2}[-/]\d{1,2}[-/]\d{4})"  # 01-15-2024
        r"|(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})",  # 15 January 2024
        re.IGNORECASE,
    )

    # Data volume patterns: "1.5 TB", "500 GB", etc.
    volume_re = re.compile(r"\b(\d+(?:\.\d+)?\s*(?:TB|GB|MB|KB|tb|gb|mb|kb))\b")

    # Countdown / deadline patterns
    countdown_re = re.compile(
        r"(?:deadline|timer|countdown|publish(?:ed)?|release)[:\s]*"
        r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:\s+\d{1,2}:\d{2})?)",
        re.IGNORECASE,
    )

    # Split HTML into blocks — look for divs with class names hinting at entries
    # Also try <tr> rows and <article> tags.
    block_re = re.compile(
        r"<(?:div|article|tr|li)[^>]*class=[\"'][^\"']*"
        r"(?:post|victim|entry|card|item|blog|listing|row)[^\"']*[\"'][^>]*>"
        r"(.*?)</(?:div|article|tr|li)>",
        re.IGNORECASE | re.DOTALL,
    )

    blocks = block_re.findall(html)
    if not blocks:
        # Fallback: split on horizontal rules or large divs
        blocks = re.split(r"<hr\s*/?>|<div\s+class=", html)

    for block in blocks:
        # Strip HTML tags for text analysis
        text = re.sub(r"<[^>]+>", " ", block)
        text = re.sub(r"\s+", " ", text).strip()

        if len(text) < 10 or len(text) > 5000:
            continue

        # Try to extract a company/victim name — usually the first bold or
        # heading text, or the first line of significant text
        name_match = re.search(
            r"<(?:h[1-6]|b|strong)[^>]*>\s*([^<]{3,100})\s*</(?:h[1-6]|b|strong)>",
            block,
            re.IGNORECASE,
        )
        victim_name = name_match.group(1).strip() if name_match else ""

        if not victim_name:
            # Take first non-trivial line as the name
            lines = [ln.strip() for ln in text.split("\n") if len(ln.strip()) > 3]
            if lines:
                victim_name = lines[0][:100]

        if not victim_name or len(victim_name) < 3:
            continue

        # Extract date
        post_date = None
        dm = date_re.search(text)
        if dm:
            post_date = _parse_date(dm.group(0))

        # Extract data volume
        data_volume = None
        vm = volume_re.search(text)
        if vm:
            data_volume = vm.group(1)

        # Extract countdown/deadline
        countdown = None
        cm = countdown_re.search(text)
        if cm:
            countdown = _parse_date(cm.group(1))

        listings.append(VictimListing(
            group_name=group_name,
            victim_name=victim_name,
            url=base_url,
            post_date=post_date,
            data_volume=data_volume,
            countdown_deadline=countdown,
            description=text[:2000],
            raw_html=block[:5000],
        ))

    return listings


def _parse_date(s: str) -> datetime | None:
    """Best-effort date parsing from various formats."""
    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m-%d-%Y",
        "%m/%d/%Y",
        "%d %B %Y",
        "%d %b %Y",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Known ransomware groups and their default blog configurations
# ---------------------------------------------------------------------------
# These are seeded into source.config when a source is created. The URLs
# rotate frequently; operators update last_known_url via the admin UI or API.
# ---------------------------------------------------------------------------

KNOWN_GROUPS: dict[str, dict] = {
    "lockbit": {
        "display_name": "LockBit",
        "parser": "generic",
        "notes": "LockBit 3.0 / LockBit Black — most prolific group",
    },
    "alphv": {
        "display_name": "ALPHV/BlackCat",
        "parser": "generic",
        "notes": "Rust-based RaaS, rebranded multiple times",
    },
    "clop": {
        "display_name": "Cl0p",
        "parser": "generic",
        "notes": "Known for MOVEit, GoAnywhere mass exploitation",
    },
    "play": {
        "display_name": "Play",
        "parser": "generic",
        "notes": "Play ransomware — intermittent blog presence",
    },
    "royal": {
        "display_name": "Royal/BlackSuit",
        "parser": "generic",
        "notes": "Rebranded from Royal to BlackSuit",
    },
    "akira": {
        "display_name": "Akira",
        "parser": "generic",
        "notes": "Retro-styled leak site",
    },
    "bianlian": {
        "display_name": "BianLian",
        "parser": "generic",
        "notes": "Shifted to exfiltration-only model",
    },
    "medusa": {
        "display_name": "Medusa",
        "parser": "generic",
        "notes": "Medusa Blog — double extortion",
    },
    "rhysida": {
        "display_name": "Rhysida",
        "parser": "generic",
        "notes": "Targets healthcare and education",
    },
    "hunters": {
        "display_name": "Hunters International",
        "parser": "generic",
        "notes": "Successor to Hive operation",
    },
}

# Map parser name → function
_PARSERS: dict[str, typing.Callable] = {
    "generic": _parse_generic,
}


class RansomwareBlogConnector(BaseConnector):
    """Monitors ransomware group blogs/leak sites for victim listings.

    Accesses .onion sites via Tor SOCKS proxy. Each ransomware group has its
    own blog URL (which rotates) and page structure (which varies).

    Source config schema (stored in Source.config JSONB):
    {
        "groups": {
            "<group_key>": {
                "last_known_url": "http://<onion>.onion/",
                "mirror_urls": ["http://<mirror>.onion/"],
                "parser": "generic",
                "enabled": true
            },
            ...
        },
        "seen_hashes": ["<content_hash>", ...],
        "request_delay_seconds": 5,
        "max_pages": 3
    }
    """

    name = "ransomware_blog"
    source_type = "ransomware_blog"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Create aiohttp session with Tor SOCKS proxy."""
        proxy_url = self.config.get("tor_proxy", settings.tor_socks_proxy)
        # aiohttp_socks doesn't support socks5h scheme; use socks5 with rdns=True
        if proxy_url and proxy_url.startswith("socks5h://"):
            proxy_url = "socks5://" + proxy_url[len("socks5h://"):]
        connector = ProxyConnector.from_url(proxy_url, rdns=True)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=_TOR_TIMEOUT,
            headers={"User-Agent": _TOR_UA},
        )

    async def teardown(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        """Scrape all enabled ransomware group blogs for new victim listings.

        Returns RawMention objects for each new victim listing not previously
        seen (tracked via content_hash in seen_hashes).
        """
        if not self._session:
            await self.setup()

        groups_cfg = self.config.get("groups", {})
        seen_hashes: set[str] = set(self.config.get("seen_hashes", []))
        mentions: list[RawMention] = []

        for group_key, group_conf in groups_cfg.items():
            if not group_conf.get("enabled", True):
                continue

            group_info = KNOWN_GROUPS.get(group_key, {})
            display_name = group_info.get("display_name", group_key)

            listings = await self._scrape_group(
                group_key=group_key,
                group_conf=group_conf,
                display_name=display_name,
            )

            for listing in listings:
                if listing.content_hash in seen_hashes:
                    continue

                # Filter by since if provided
                if since and listing.post_date and listing.post_date < since:
                    continue

                seen_hashes.add(listing.content_hash)
                mentions.append(self._listing_to_mention(listing))

        # Persist seen hashes back into config for next poll
        # Cap at 10k to prevent unbounded growth
        self.config["seen_hashes"] = list(seen_hashes)[-10000:]

        logger.info(
            "RansomwareBlogConnector polled %d groups, found %d new listings",
            len(groups_cfg),
            len(mentions),
        )
        return mentions

    async def _scrape_group(
        self,
        group_key: str,
        group_conf: dict,
        display_name: str,
    ) -> list[VictimListing]:
        """Scrape a single ransomware group's blog."""
        urls = self._get_urls(group_conf)
        if not urls:
            logger.warning("No URLs configured for group %s", group_key)
            return []

        parser_name = group_conf.get(
            "parser",
            KNOWN_GROUPS.get(group_key, {}).get("parser", "generic"),
        )
        parser_fn = _PARSERS.get(parser_name, _parse_generic)

        for url in urls:
            try:
                html = await self._fetch_page(url)
                if not html:
                    continue

                listings = parser_fn(html, display_name, url)

                # If we successfully parsed, update last_known_url
                group_conf["last_known_url"] = url
                return listings

            except aiohttp.ClientError as exc:
                logger.warning(
                    "Failed to fetch %s for group %s: %s",
                    url, group_key, exc,
                )
                continue
            except Exception:
                logger.exception(
                    "Unexpected error scraping group %s at %s",
                    group_key, url,
                )
                continue

        logger.error("All URLs failed for group %s", group_key)
        return []

    async def _fetch_page(self, url: str) -> str | None:
        """Fetch a single page via Tor."""
        assert self._session is not None
        async with self._session.get(url) as resp:
            if resp.status != 200:
                logger.warning("HTTP %d from %s", resp.status, url)
                return None
            # Respect max content size
            max_size = self.config.get("max_content_size", settings.max_content_size)
            body = await resp.read()
            if len(body) > max_size:
                logger.warning(
                    "Response from %s exceeds max size (%d > %d)",
                    url, len(body), max_size,
                )
                return None
            return body.decode("utf-8", errors="replace")

    @staticmethod
    def _get_urls(group_conf: dict) -> list[str]:
        """Collect all candidate URLs for a group, primary first."""
        urls = []
        primary = group_conf.get("last_known_url", "")
        if primary:
            urls.append(primary)
        for mirror in group_conf.get("mirror_urls", []):
            if mirror and mirror not in urls:
                urls.append(mirror)
        return urls

    @staticmethod
    def _listing_to_mention(listing: VictimListing) -> RawMention:
        """Convert a VictimListing into a RawMention for the matching pipeline."""
        meta: dict = {
            "group_name": listing.group_name,
            "content_hash": listing.content_hash,
        }
        if listing.data_volume:
            meta["data_volume"] = listing.data_volume
        if listing.countdown_deadline:
            meta["countdown_deadline"] = listing.countdown_deadline.isoformat()
        if listing.metadata:
            meta.update(listing.metadata)

        title = f"[{listing.group_name}] {listing.victim_name}"
        parts = [listing.description]
        if listing.data_volume:
            parts.append(f"Claimed data volume: {listing.data_volume}")
        if listing.countdown_deadline:
            parts.append(f"Deadline: {listing.countdown_deadline.isoformat()}")
        content = "\n".join(parts)

        return RawMention(
            source_name=f"ransomware_blog:{listing.group_name.lower().replace(' ', '_')}",
            source_url=listing.url,
            title=title,
            content=content,
            author=listing.group_name,
            discovered_at=listing.post_date or datetime.now(timezone.utc),
            metadata=meta,
        )

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def health_check(self) -> dict:
        """Verify Tor connectivity by checking at least one configured URL."""
        if not self._session:
            await self.setup()
        session = self._session
        assert session is not None

        groups_cfg = self.config.get("groups", {})
        if not groups_cfg:
            return {"healthy": False, "message": "No groups configured"}

        # Try the first enabled group
        for group_key, group_conf in groups_cfg.items():
            if not group_conf.get("enabled", True):
                continue
            urls = self._get_urls(group_conf)
            for url in urls:
                try:
                    async with session.head(url, allow_redirects=True) as resp:
                        if resp.status < 500:
                            return {
                                "healthy": True,
                                "message": f"Tor connected — {group_key} reachable (HTTP {resp.status})",
                            }
                except aiohttp.ClientError as exc:
                    return {
                        "healthy": False,
                        "message": f"Tor connection failed for {group_key}: {exc}",
                    }

        return {"healthy": False, "message": "No reachable groups"}
