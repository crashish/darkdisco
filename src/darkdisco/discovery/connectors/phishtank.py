"""PhishTank connector — community-submitted confirmed phishing URLs.

Polls the PhishTank API/data feed for confirmed phishing URLs, matching against
monitored institution domains. PhishTank provides a free API with community-verified
phishing reports.

PhishTank API: check individual URLs or download the full verified phishing database.
Free tier requires registration for an API key.

Source config schema (stored in Source.config JSONB):
{
    "watch_domains": ["firstnational.com", "example-bank.com"],
    "watch_brands": ["First National Bank", "Example Credit Union"],
    "use_online_check": false,
    "max_results": 500,
    "request_delay_seconds": 2,
    "seen_ids": ["12345", "67890"]
}
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

import aiohttp

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

_PHISHTANK_BASE = "https://checkurl.phishtank.com/checkurl"
_PHISHTANK_DATA_URL = "http://data.phishtank.com/data/{api_key}/online-valid.json"
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=15)
_DEFAULT_UA = "phishtank/DarkDisco-PhishMonitor"


class PhishTankConnector(BaseConnector):
    """Checks PhishTank for confirmed phishing URLs targeting monitored institutions.

    Two modes:
    1. Data feed mode (default): Downloads the verified phishing database and filters
       by watched domains/brands. More comprehensive but larger download.
    2. Online check mode: Checks specific URLs against PhishTank. Useful for
       verifying individual suspicious URLs.
    """

    name = "phishtank"
    source_type = "phishtank"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None

    async def setup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=_REQUEST_TIMEOUT,
            headers={"User-Agent": _DEFAULT_UA},
        )

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        if not self._session:
            await self.setup()

        watch_domains = self.config.get("watch_domains", [])
        watch_brands = self.config.get("watch_brands", [])
        if not watch_domains and not watch_brands:
            logger.warning("PhishTankConnector: no watch_domains or watch_brands configured")
            return []

        seen_ids: set[str] = set(self.config.get("seen_ids", []))
        mentions: list[RawMention] = []

        if self.config.get("use_online_check", False):
            # Online check mode — check specific URLs (not typically used for polling)
            logger.debug("PhishTank: online check mode not used for polling")
        else:
            # Data feed mode — download and filter
            mentions = await self._poll_data_feed(
                watch_domains, watch_brands, since, seen_ids,
            )

        # Persist seen IDs (cap at 10k)
        self.config["seen_ids"] = list(seen_ids)[-10000:]

        logger.info(
            "PhishTankConnector found %d new mentions",
            len(mentions),
        )
        return mentions

    async def _poll_data_feed(
        self,
        watch_domains: list[str],
        watch_brands: list[str],
        since: datetime | None,
        seen_ids: set[str],
    ) -> list[RawMention]:
        """Download the PhishTank verified phishing database and filter for matches."""
        assert self._session is not None

        mentions: list[RawMention] = []
        max_results = self.config.get("max_results", 500)

        api_key = settings.phishtank_api_key
        if not api_key:
            logger.warning("PhishTank: no API key configured, using unauthenticated feed")
            url = "http://data.phishtank.com/data/online-valid.json"
        else:
            url = _PHISHTANK_DATA_URL.format(api_key=api_key)

        try:
            async with self._session.get(url) as resp:
                if resp.status == 403:
                    logger.error("PhishTank access denied — check API key or rate limits")
                    return []
                if resp.status == 429:
                    logger.warning("PhishTank rate-limited")
                    return []
                if resp.status != 200:
                    logger.warning("PhishTank HTTP %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
        except aiohttp.ClientError as exc:
            logger.error("PhishTank data feed request failed: %s", exc)
            return []

        if not isinstance(data, list):
            logger.warning("PhishTank: unexpected response format")
            return []

        # Normalize domains for matching (lowercase, strip www.)
        domain_set = {d.lower().removeprefix("www.") for d in watch_domains}
        brand_lower = [b.lower() for b in watch_brands]

        matched = 0
        for entry in data:
            if matched >= max_results:
                break

            phish_id = str(entry.get("phish_id", ""))
            if not phish_id or phish_id in seen_ids:
                continue

            phish_url = entry.get("url", "")
            target = entry.get("target", "")
            submission_time = entry.get("submission_time", "")
            verified = entry.get("verified", "")
            verification_time = entry.get("verification_time", "")
            online = entry.get("online", "")
            details_url = entry.get("phish_detail_url", "")

            # Filter by submission time if since is provided
            if since and submission_time:
                try:
                    sub_dt = datetime.strptime(
                        submission_time, "%Y-%m-%dT%H:%M:%S+00:00"
                    ).replace(tzinfo=timezone.utc)
                    if sub_dt < since:
                        continue
                except ValueError:
                    pass

            # Check if this phish targets any of our watched domains/brands
            phish_url_lower = phish_url.lower()
            target_lower = target.lower() if target else ""

            matched_domain = None
            matched_brand = None

            # Check domain presence in phishing URL
            for domain in domain_set:
                if domain in phish_url_lower:
                    matched_domain = domain
                    break

            # Check brand name in target field or URL
            if not matched_domain:
                for brand in brand_lower:
                    if brand in target_lower or brand in phish_url_lower:
                        matched_brand = brand
                        break

            if not matched_domain and not matched_brand:
                continue

            matched += 1

            # Parse timestamps
            discovered_at = datetime.now(timezone.utc)
            if verification_time:
                try:
                    discovered_at = datetime.strptime(
                        verification_time, "%Y-%m-%dT%H:%M:%S+00:00"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

            match_reason = matched_domain or matched_brand
            parts = [
                f"PhishTank confirmed phishing URL: {phish_url}",
                f"Target: {target}" if target else "",
                f"Match: {match_reason}",
                f"Verified: {verified} at {verification_time}" if verified == "yes" else "",
                f"Status: {'online' if online == 'yes' else 'offline'}",
                f"Submitted: {submission_time}",
            ]
            parts = [p for p in parts if p]

            content_hash = hashlib.sha256(
                f"phishtank:{phish_id}".encode()
            ).hexdigest()[:16]

            seen_ids.add(phish_id)
            mentions.append(RawMention(
                source_name="phishtank",
                source_url=details_url or f"https://phishtank.org/phish_detail.php?phish_id={phish_id}",
                title=f"[PhishTank] Confirmed phishing: {target or phish_url}",
                content="\n".join(parts),
                discovered_at=discovered_at,
                metadata={
                    "phish_id": phish_id,
                    "phish_url": phish_url,
                    "target": target,
                    "verified": verified == "yes",
                    "online": online == "yes",
                    "submission_time": submission_time,
                    "verification_time": verification_time,
                    "matched_domain": matched_domain,
                    "matched_brand": matched_brand,
                    "content_hash": content_hash,
                },
            ))

        return mentions

    async def check_url(self, url: str) -> dict | None:
        """Check a single URL against PhishTank (online check mode).

        Returns PhishTank result dict if the URL is in their database, None otherwise.
        Useful for ad-hoc verification of suspicious URLs found by other connectors.
        """
        if not self._session:
            await self.setup()
        assert self._session is not None

        if not settings.phishtank_api_key:
            logger.warning("PhishTank: API key required for online URL checks")
            return None

        form_data = aiohttp.FormData()
        form_data.add_field("url", url)
        form_data.add_field("format", "json")
        form_data.add_field("app_key", settings.phishtank_api_key)

        try:
            async with self._session.post(_PHISHTANK_BASE, data=form_data) as resp:
                if resp.status != 200:
                    logger.warning("PhishTank check_url HTTP %d for %s", resp.status, url)
                    return None
                data = await resp.json(content_type=None)
                results = data.get("results", {})
                if results.get("in_database"):
                    return results
                return None
        except aiohttp.ClientError as exc:
            logger.error("PhishTank check_url failed for %s: %s", url, exc)
            return None

    async def health_check(self) -> dict:
        if not self._session:
            await self.setup()
        assert self._session is not None

        # Try a lightweight check — just verify the data feed is accessible
        api_key = settings.phishtank_api_key
        if api_key:
            url = _PHISHTANK_DATA_URL.format(api_key=api_key)
        else:
            url = "http://data.phishtank.com/data/online-valid.json"

        try:
            async with self._session.head(url) as resp:
                if resp.status == 200:
                    return {"healthy": True, "message": "phishtank:ok"}
                return {
                    "healthy": False,
                    "message": f"phishtank:http_{resp.status}",
                }
        except aiohttp.ClientError as exc:
            return {"healthy": False, "message": f"phishtank:error({exc})"}
