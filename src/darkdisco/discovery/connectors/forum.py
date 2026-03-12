"""Forum connector — scrapes dark web forums via Tor for threat intel.

Monitors forums like BreachForums, XSS.is, Exploit.in for posts mentioning
monitored institutions. Each forum has its own scraping profile stored in
source.config.

Config (source.config JSONB):
    forums: list[dict]  — forum definitions, each with:
        name: str           — human label
        base_url: str       — .onion or clearnet URL
        recent_path: str    — path to recent posts/threads
        selector_profile: str — "mybb", "xenforo", or "generic"
        selectors: dict     — override CSS selectors (optional)
        last_seen_id: str   — bookmark for incremental scraping
    max_pages: int          — max pages to scrape per poll (default 3)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

import aiohttp
from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

DEFAULT_SELECTORS = {
    "mybb": {
        "thread_list": ".tborder .inline_row, .tborder .alt_row, .structItem",
        "thread_title": ".subject_new a, .subject_old a, .structItem-title a",
        "thread_link": ".subject_new a, .subject_old a, .structItem-title a",
        "thread_author": ".author a, .structItem-minor a.username",
        "thread_date": ".lastpost, .structItem-cell--latest",
    },
    "xenforo": {
        "thread_list": ".structItem",
        "thread_title": ".structItem-title a",
        "thread_link": ".structItem-title a",
        "thread_author": ".structItem-minor .username",
        "thread_date": ".structItem-cell--latest time",
    },
    "generic": {
        "thread_list": "tr, .thread, .topic, article",
        "thread_title": "a[href*='thread'], a[href*='topic'], a[href*='post'], h3 a, h4 a",
        "thread_link": "a[href*='thread'], a[href*='topic'], a[href*='post'], h3 a, h4 a",
        "thread_author": ".author, .username, .poster",
        "thread_date": "time, .date, .timestamp",
    },
}

REQUEST_TIMEOUT = 60
MAX_RETRIES = 2
RETRY_DELAY = 5
INTER_PAGE_DELAY = 3


class ForumConnector(BaseConnector):
    """Scrapes dark web forums for threat mentions via Tor SOCKS proxy."""

    name = "forum"
    source_type = "forum"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None

    @property
    def _forums(self) -> list[dict]:
        return self.config.get("forums", [])

    @property
    def _max_pages(self) -> int:
        return self.config.get("max_pages", 3)

    async def setup(self) -> None:
        # Normalize socks5h:// → socks5:// with rdns=True for .onion resolution
        proxy_url = settings.tor_socks_proxy.replace("socks5h://", "socks5://")
        connector = ProxyConnector.from_url(proxy_url, rdns=True)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:128.0) Gecko/20100101 Firefox/128.0",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        if not self._forums:
            logger.warning("No forums configured")
            return []
        if self._session is None:
            await self.setup()

        all_mentions: list[RawMention] = []
        for forum_cfg in self._forums:
            try:
                mentions = await self._poll_forum(forum_cfg)
                all_mentions.extend(mentions)
                logger.info("Forum %s: %d mentions", forum_cfg.get("name", "?"), len(mentions))
            except Exception:
                logger.exception("Failed to poll forum %s", forum_cfg.get("name", "?"))
            await asyncio.sleep(INTER_PAGE_DELAY)

        logger.info("Forum poll complete: %d total mentions", len(all_mentions))
        return all_mentions

    async def health_check(self) -> dict:
        if not self._forums:
            return {"healthy": False, "message": "No forums configured"}
        if self._session is None:
            await self.setup()

        results = {}
        for forum_cfg in self._forums:
            name = forum_cfg.get("name", "unknown")
            try:
                async with self._session.get(forum_cfg["base_url"]) as resp:  # type: ignore[union-attr]
                    results[name] = {"healthy": resp.status < 400, "status": resp.status}
            except Exception as exc:
                results[name] = {"healthy": False, "error": str(exc)[:200]}

        healthy = sum(1 for r in results.values() if r.get("healthy"))
        return {"healthy": healthy > 0, "message": f"{healthy}/{len(results)} reachable", "forums": results}

    async def _poll_forum(self, forum_cfg: dict) -> list[RawMention]:
        base_url = forum_cfg.get("base_url", "")
        recent_path = forum_cfg.get("recent_path", "/")
        forum_name = forum_cfg.get("name", base_url)
        profile = forum_cfg.get("selector_profile", "generic")
        selectors = forum_cfg.get("selectors") or DEFAULT_SELECTORS.get(profile, DEFAULT_SELECTORS["generic"])
        last_seen = forum_cfg.get("last_seen_id", "")

        mentions: list[RawMention] = []
        new_last_seen = last_seen

        for page in range(self._max_pages):
            url = _build_page_url(base_url, recent_path, page)
            html = await self._fetch(url)
            if not html:
                break

            threads = _parse_thread_list(html, selectors, base_url)
            if not threads:
                break

            for thread in threads:
                tid = thread.get("id", "")
                if last_seen and tid and tid <= last_seen:
                    continue
                if tid and (not new_last_seen or tid > new_last_seen):
                    new_last_seen = tid

                mentions.append(RawMention(
                    source_name=f"forum:{forum_name}",
                    source_url=thread.get("url"),
                    title=thread.get("title", ""),
                    content=thread.get("title", ""),
                    author=thread.get("author"),
                    discovered_at=thread.get("date", datetime.now(tz=timezone.utc)),
                    metadata={"forum": forum_name, "thread_id": tid, "base_url": base_url},
                ))

            if len(threads) < 10:
                break
            await asyncio.sleep(INTER_PAGE_DELAY)

        if new_last_seen != last_seen:
            forum_cfg["last_seen_id"] = new_last_seen
        return mentions

    async def _fetch(self, url: str) -> str | None:
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with self._session.get(url) as resp:  # type: ignore[union-attr]
                    if resp.status == 200:
                        return await resp.text()
                    logger.warning("Forum fetch %s returned %d", url, resp.status)
                    return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    logger.warning("Failed to fetch %s: %s", url, exc)
                    return None
        return None


def _build_page_url(base_url: str, recent_path: str, page: int) -> str:
    url = f"{base_url.rstrip('/')}/{recent_path.lstrip('/')}"
    if page > 0:
        url += f"{'&' if '?' in url else '?'}page={page + 1}"
    return url


def _parse_thread_list(html: str, selectors: dict, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    threads: list[dict] = []
    for elem in soup.select(selectors.get("thread_list", "tr")):
        thread = _parse_thread(elem, selectors, base_url)
        if thread and thread.get("title"):
            threads.append(thread)
    return threads


def _parse_thread(elem, selectors: dict, base_url: str) -> dict | None:
    result: dict[str, Any] = {}
    title_el = elem.select_one(selectors.get("thread_title", "a"))
    if not title_el:
        return None
    result["title"] = title_el.get_text(strip=True)

    link_el = elem.select_one(selectors.get("thread_link", "a"))
    if link_el and link_el.get("href"):
        href = link_el["href"]
        if not href.startswith("http"):
            href = f"{base_url.rstrip('/')}/{href.lstrip('/')}"
        result["url"] = href
        result["id"] = _extract_thread_id(href)

    author_el = elem.select_one(selectors.get("thread_author", ".author"))
    if author_el:
        result["author"] = author_el.get_text(strip=True)

    date_el = elem.select_one(selectors.get("thread_date", "time"))
    result["date"] = _parse_date(date_el) if date_el else datetime.now(tz=timezone.utc)
    return result


def _extract_thread_id(url: str) -> str:
    for pattern in [r"/threads?/(\d+)", r"[?&]tid=(\d+)", r"/post/(\d+)", r"/topic/(\d+)", r"\.(\d+)/?$"]:
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _parse_date(el) -> datetime:
    for attr in ("datetime", "data-time", "data-timestamp"):
        val = el.get(attr)
        if not val:
            continue
        try:
            if attr == "datetime":
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            return datetime.fromtimestamp(int(val), tz=timezone.utc)
        except (ValueError, OSError):
            pass
    return datetime.now(tz=timezone.utc)
