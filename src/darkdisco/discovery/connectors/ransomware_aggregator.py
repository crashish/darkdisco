"""Ransomware aggregator connector — queries structured APIs instead of scraping .onion sites.

Uses clearnet aggregator APIs (ransomware.live, ransomwatch.telemetry.ltd) that collect
and normalize victim claims from all major ransomware groups. Much more reliable than
direct .onion access since URLs don't rotate and responses are structured JSON.

Source config schema (stored in Source.config JSONB):
{
    "api_base": "https://api.ransomware.live",    # primary API
    "fallback_api_base": "https://ransomwatch.telemetry.ltd",  # fallback
    "seen_hashes": ["<content_hash>", ...],
    "groups_filter": ["lockbit", "alphv"],  # optional: only these groups (empty = all)
    "request_timeout": 60,
    "max_results_per_poll": 500
}
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp

from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

# Default API endpoints
_DEFAULT_API_BASE = "https://api.ransomware.live"
_FALLBACK_API_BASE = "https://ransomwatch.telemetry.ltd"

_USER_AGENT = "DarkDisco/1.0 (threat-intel-aggregator)"


@dataclass
class AggregatorVictim:
    """A single victim claim from the aggregator API."""

    group_name: str
    victim_name: str
    website: str | None = None
    published: datetime | None = None
    description: str = ""
    country: str | None = None
    activity: str | None = None
    claim_url: str | None = None
    metadata: dict = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        """Deterministic hash for deduplication."""
        key = f"{self.group_name}:{self.victim_name}:{self.published or ''}".lower()
        return hashlib.sha256(key.encode()).hexdigest()[:16]


class RansomwareAggregatorConnector(BaseConnector):
    """Queries ransomware aggregator APIs for victim claims.

    These APIs aggregate claims from all major ransomware groups into a
    structured JSON format, eliminating the need for direct .onion access
    and HTML scraping. Supports ransomware.live and ransomwatch as backends.
    """

    name = "ransomware_aggregator"
    source_type = "ransomware_aggregator"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Create aiohttp session for clearnet API access."""
        timeout_secs = self.config.get("request_timeout", 60)
        timeout = aiohttp.ClientTimeout(total=timeout_secs, connect=30)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": _USER_AGENT},
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
        """Fetch recent victim claims from the aggregator API.

        Tries the primary API first, falls back to the secondary if it fails.
        Returns RawMention objects for each new claim not previously seen.
        """
        if not self._session:
            await self.setup()

        seen_hashes: set[str] = set(self.config.get("seen_hashes", []))
        groups_filter: set[str] = set(
            g.lower() for g in self.config.get("groups_filter", [])
        )
        max_results = self.config.get("max_results_per_poll", 500)

        # Try primary, then fallback
        api_base = self.config.get("api_base", _DEFAULT_API_BASE)
        fallback = self.config.get("fallback_api_base", _FALLBACK_API_BASE)

        victims: list[AggregatorVictim] = []
        last_error = None

        for base_url in (api_base, fallback):
            if not base_url:
                continue
            try:
                victims = await self._fetch_victims(base_url)
                break
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Failed to fetch from %s: %s", base_url, exc,
                )
                continue

        if not victims and last_error:
            raise last_error

        # Filter and deduplicate
        mentions: list[RawMention] = []
        for victim in victims:
            if len(mentions) >= max_results:
                break

            # Apply group filter if configured
            if groups_filter and victim.group_name.lower() not in groups_filter:
                continue

            if victim.content_hash in seen_hashes:
                continue

            # Filter by since if provided
            if since and victim.published and victim.published < since:
                continue

            seen_hashes.add(victim.content_hash)
            mentions.append(self._victim_to_mention(victim))

        # Persist seen hashes (cap at 10k)
        self.config["seen_hashes"] = list(seen_hashes)[-10000:]

        logger.info(
            "RansomwareAggregatorConnector polled, found %d new claims",
            len(mentions),
        )
        return mentions

    async def _fetch_victims(
        self, api_base: str,
    ) -> list[AggregatorVictim]:
        """Fetch victim data from an aggregator API endpoint.

        Supports both ransomware.live and ransomwatch API formats:
        - ransomware.live: GET /recentvictims or /victims
        - ransomwatch: GET /api/victims
        """
        assert self._session is not None
        api_base = api_base.rstrip("/")

        # Try the recent victims endpoint first (most efficient)
        victims: list[AggregatorVictim] = []

        for endpoint in ("/recentvictims", "/victims", "/api/victims"):
            url = f"{api_base}{endpoint}"
            try:
                async with self._session.get(url) as resp:
                    if resp.status == 404:
                        continue
                    if resp.status != 200:
                        logger.warning("HTTP %d from %s", resp.status, url)
                        continue

                    data = await resp.json(content_type=None)
                    if not isinstance(data, list):
                        continue

                    victims = self._parse_victims(data)
                    if victims:
                        logger.debug(
                            "Fetched %d victims from %s", len(victims), url,
                        )
                        break
            except aiohttp.ContentTypeError:
                logger.warning("Non-JSON response from %s", url)
                continue

        if not victims:
            raise RuntimeError(f"No victim data from any endpoint at {api_base}")

        return victims

    @staticmethod
    def _parse_victims(data: list[dict]) -> list[AggregatorVictim]:
        """Parse victim entries from aggregator JSON response.

        Handles field name variations across different aggregator APIs.
        """
        victims: list[AggregatorVictim] = []

        for entry in data:
            if not isinstance(entry, dict):
                continue

            # Group name: "group_name", "group", "gang"
            group = (
                entry.get("group_name")
                or entry.get("group")
                or entry.get("gang")
                or ""
            )

            # Victim name: "victim", "post_title", "name", "company"
            victim_name = (
                entry.get("victim")
                or entry.get("post_title")
                or entry.get("name")
                or entry.get("company")
                or ""
            )

            if not group or not victim_name:
                continue

            # Published date: "published", "discovered", "date", "post_date"
            published = None
            date_str = (
                entry.get("published")
                or entry.get("discovered")
                or entry.get("date")
                or entry.get("post_date")
            )
            if date_str:
                published = _parse_datetime(date_str)

            # Website/URL: "website", "post_url", "url", "link"
            website = (
                entry.get("website")
                or entry.get("post_url")
                or entry.get("url")
                or entry.get("link")
            )

            # Description: "description", "body", "content", "details"
            description = (
                entry.get("description")
                or entry.get("body")
                or entry.get("content")
                or entry.get("details")
                or ""
            )

            # Country: "country", "country_code"
            country = entry.get("country") or entry.get("country_code")

            # Activity status: "activity", "status"
            activity = entry.get("activity") or entry.get("status")

            # Claim URL (link to the original .onion post)
            claim_url = entry.get("claim_url") or entry.get("onion_url")

            # Collect remaining fields as metadata
            known_keys = {
                "group_name", "group", "gang", "victim", "post_title",
                "name", "company", "published", "discovered", "date",
                "post_date", "website", "post_url", "url", "link",
                "description", "body", "content", "details", "country",
                "country_code", "activity", "status", "claim_url",
                "onion_url",
            }
            extra = {k: v for k, v in entry.items() if k not in known_keys and v}

            victims.append(AggregatorVictim(
                group_name=group,
                victim_name=victim_name,
                website=website,
                published=published,
                description=str(description)[:5000],
                country=country,
                activity=activity,
                claim_url=claim_url,
                metadata=extra,
            ))

        return victims

    @staticmethod
    def _victim_to_mention(victim: AggregatorVictim) -> RawMention:
        """Convert an AggregatorVictim into a RawMention for the matching pipeline."""
        meta: dict = {
            "group_name": victim.group_name,
            "content_hash": victim.content_hash,
            "data_source": "aggregator_api",
        }
        if victim.country:
            meta["country"] = victim.country
        if victim.activity:
            meta["activity"] = victim.activity
        if victim.website:
            meta["victim_website"] = victim.website
        if victim.claim_url:
            meta["claim_url"] = victim.claim_url
        if victim.metadata:
            meta.update(victim.metadata)

        title = f"[{victim.group_name}] {victim.victim_name}"
        parts = [victim.description] if victim.description else []
        if victim.website:
            parts.append(f"Victim website: {victim.website}")
        if victim.country:
            parts.append(f"Country: {victim.country}")
        content = "\n".join(parts)

        return RawMention(
            source_name=f"ransomware_aggregator:{victim.group_name.lower().replace(' ', '_')}",
            source_url=victim.claim_url or victim.website,
            title=title,
            content=content,
            author=victim.group_name,
            discovered_at=victim.published or datetime.now(timezone.utc),
            metadata=meta,
        )

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def health_check(self) -> dict:
        """Verify connectivity to the aggregator API."""
        if not self._session:
            await self.setup()
        assert self._session is not None

        api_base = self.config.get("api_base", _DEFAULT_API_BASE).rstrip("/")
        fallback = self.config.get("fallback_api_base", _FALLBACK_API_BASE)

        last_err = "no endpoints configured"
        for base_url in (api_base, fallback):
            if not base_url:
                continue
            base_url = base_url.rstrip("/")
            # Try a lightweight endpoint
            for endpoint in ("/recentvictims", "/api/victims", "/groups"):
                url = f"{base_url}{endpoint}"
                try:
                    async with self._session.get(url) as resp:
                        if resp.status < 500:
                            return {
                                "healthy": True,
                                "message": f"API reachable at {base_url} (HTTP {resp.status})",
                            }
                except aiohttp.ClientError as exc:
                    last_err = str(exc)
                    continue

        return {
            "healthy": False,
            "message": f"All aggregator API endpoints unreachable: {last_err}",
        }


def _parse_datetime(value: str | datetime) -> datetime | None:
    """Best-effort datetime parsing for aggregator API responses."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if not isinstance(value, str):
        return None

    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None
