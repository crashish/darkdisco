"""URLscan.io connector — detects live phishing pages targeting monitored institutions.

Polls the URLscan.io search API for recent scans matching institution brand keywords,
returning mentions when phishing pages are detected. Supports both free and paid tiers.

URLscan.io API: search endpoint returns recent scan results matching a query.
Free tier: 100 searches/day, 1000 results/day.

Source config schema (stored in Source.config JSONB):
{
    "search_queries": ["First National Bank", "example.com login"],
    "verdicts_only": true,
    "min_score": 50,
    "max_results_per_query": 100,
    "request_delay_seconds": 2,
    "seen_ids": ["scan-uuid-1", "scan-uuid-2"]
}
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone

import aiohttp

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

_URLSCAN_BASE = "https://urlscan.io/api/v1"
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=15)
_DEFAULT_UA = "DarkDisco-PhishMonitor/1.0"


class URLScanConnector(BaseConnector):
    """Searches URLscan.io for phishing pages targeting monitored institutions.

    Queries the search API with institution brand keywords and domains, looking for
    scans flagged as malicious/phishing. Returns one RawMention per detected
    phishing page.
    """

    name = "urlscan"
    source_type = "urlscan"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None

    async def setup(self) -> None:
        headers = {"User-Agent": _DEFAULT_UA}
        if settings.urlscan_api_key:
            headers["API-Key"] = settings.urlscan_api_key
        self._session = aiohttp.ClientSession(
            timeout=_REQUEST_TIMEOUT,
            headers=headers,
        )

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        if not self._session:
            await self.setup()

        queries = self.config.get("search_queries", [])
        if not queries:
            logger.warning("URLScanConnector: no search_queries configured")
            return []

        seen_ids: set[str] = set(self.config.get("seen_ids", []))
        mentions: list[RawMention] = []
        delay = self.config.get("request_delay_seconds", 2)
        max_results = self.config.get("max_results_per_query", 100)
        min_score = self.config.get("min_score", 0)
        verdicts_only = self.config.get("verdicts_only", True)

        for query in queries:
            new = await self._search_query(
                query, since, seen_ids, max_results, min_score, verdicts_only,
            )
            mentions.extend(new)
            if delay and len(queries) > 1:
                await asyncio.sleep(delay)

        # Persist seen IDs (cap at 10k)
        self.config["seen_ids"] = list(seen_ids)[-10000:]

        logger.info(
            "URLScanConnector polled %d queries, found %d new mentions",
            len(queries), len(mentions),
        )
        return mentions

    async def _search_query(
        self,
        query: str,
        since: datetime | None,
        seen_ids: set[str],
        max_results: int,
        min_score: int,
        verdicts_only: bool,
    ) -> list[RawMention]:
        assert self._session is not None

        mentions: list[RawMention] = []

        # Build search query — URLscan uses Elasticsearch query syntax
        search_q = query
        if since:
            date_str = since.strftime("%Y-%m-%dT%H:%M:%S")
            search_q = f"({query}) AND date:>{date_str}"
        if verdicts_only:
            search_q += " AND verdicts.overall.malicious:true"

        params = {"q": search_q, "size": str(min(max_results, 10000))}

        try:
            async with self._session.get(
                f"{_URLSCAN_BASE}/search/",
                params=params,
            ) as resp:
                if resp.status == 401:
                    logger.error("URLscan auth failed — check API key")
                    return []
                if resp.status == 429:
                    logger.warning("URLscan rate-limited for query: %s", query)
                    return []
                if resp.status != 200:
                    logger.warning("URLscan HTTP %d for query: %s", resp.status, query)
                    return []
                data = await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("URLscan request failed for query %s: %s", query, exc)
            return []

        results = data.get("results", [])
        if not results:
            logger.debug("URLscan: no results for query: %s", query)
            return []

        for result in results:
            task = result.get("task", {})
            page = result.get("page", {})
            stats = result.get("stats", {})
            verdicts = result.get("verdicts", {})
            result_id = result.get("_id", "")

            if not result_id or result_id in seen_ids:
                continue

            # Check malicious score threshold
            overall_verdict = verdicts.get("overall", {})
            score = overall_verdict.get("score", 0)
            if min_score and score < min_score:
                continue

            scan_url = task.get("url", "")
            scan_time = task.get("time", "")
            page_domain = page.get("domain", "")
            page_ip = page.get("ip", "")
            page_country = page.get("country", "")
            page_title = page.get("title", "")
            screenshot_url = result.get("screenshot", "")

            # Parse scan timestamp
            discovered_at = datetime.now(timezone.utc)
            if scan_time:
                try:
                    discovered_at = datetime.fromisoformat(
                        scan_time.replace("Z", "+00:00")
                    )
                except ValueError:
                    pass

            # Build readable content
            malicious_tags = overall_verdict.get("categories", [])
            brands = overall_verdict.get("brands", [])

            parts = [
                f"URLscan.io detected a malicious page: {scan_url}",
                f"Domain: {page_domain}",
                f"IP: {page_ip} ({page_country})" if page_ip else "",
                f"Page title: {page_title}" if page_title else "",
                f"Malicious score: {score}/100",
                f"Categories: {', '.join(malicious_tags)}" if malicious_tags else "",
                f"Brands targeted: {', '.join(brands)}" if brands else "",
                f"Scan time: {scan_time}",
            ]
            parts = [p for p in parts if p]

            content_hash = hashlib.sha256(
                f"urlscan:{result_id}".encode()
            ).hexdigest()[:16]

            seen_ids.add(result_id)
            mentions.append(RawMention(
                source_name="urlscan",
                source_url=f"https://urlscan.io/result/{result_id}/",
                title=f"[URLscan] Phishing page: {page_domain} — {page_title or scan_url}",
                content="\n".join(parts),
                discovered_at=discovered_at,
                metadata={
                    "scan_id": result_id,
                    "scan_url": scan_url,
                    "domain": page_domain,
                    "ip": page_ip,
                    "country": page_country,
                    "page_title": page_title,
                    "score": score,
                    "categories": malicious_tags,
                    "brands": brands,
                    "screenshot_url": screenshot_url,
                    "stats": {
                        "requests": stats.get("requests", 0),
                        "ips": stats.get("ips", 0),
                        "domains": stats.get("domains", 0),
                    },
                    "content_hash": content_hash,
                    "search_query": query,
                },
            ))

        return mentions

    async def health_check(self) -> dict:
        if not self._session:
            await self.setup()
        assert self._session is not None

        try:
            async with self._session.get(
                f"{_URLSCAN_BASE}/search/",
                params={"q": "domain:example.com", "size": "1"},
            ) as resp:
                if resp.status == 200:
                    return {"healthy": True, "message": "urlscan:ok"}
                return {
                    "healthy": False,
                    "message": f"urlscan:http_{resp.status}",
                }
        except aiohttp.ClientError as exc:
            return {"healthy": False, "message": f"urlscan:error({exc})"}
