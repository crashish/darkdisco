"""Forum connector — scrapes dark web forums via Tor for threat intel.

Monitors forums like BreachForums, XSS.is, Exploit.in for posts mentioning
monitored institutions. Each forum has its own scraping profile stored in
source.config.

Config (source.config JSONB):
    forums: list[dict]  — forum definitions, each with:
        name: str           — human label
        base_url: str       — .onion or clearnet URL
        fallback_urls: list[str] — alternate URLs if base_url is unreachable
        recent_path: str    — path to recent posts/threads
        selector_profile: str — "mybb", "xenforo", or "generic"
        selectors: dict     — override CSS selectors (optional)
        last_seen_id: str   — bookmark for incremental scraping
        auth: dict          — authentication config (optional):
            method: str     — "cookie" or "form"
            cookies: dict   — cookie name→value pairs (for method="cookie")
            login_url: str  — form login endpoint (for method="form")
            login_data: dict — POST form fields (for method="form")
            login_check: str — CSS selector that only appears when logged in
        scrape_content: bool — fetch thread body, not just title (default true)
        content_selectors: dict — CSS selectors for thread content (optional):
            post_body: str  — selector for post content
            first_post: str — selector for the first/OP post
    max_pages: int          — max pages to scrape per poll (default 3)
    max_content_threads: int — max threads to fetch content for per poll (default 20)
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

DEFAULT_CONTENT_SELECTORS = {
    "mybb": {
        "post_body": ".post_body, .message-body",
        "first_post": ".post:first-child .post_body, .message:first-child .message-body",
    },
    "xenforo": {
        "post_body": ".message-body .bbWrapper, .message-content .bbWrapper",
        "first_post": ".message:first-child .bbWrapper, .block-body .message:first-child .bbWrapper",
    },
    "generic": {
        "post_body": ".post-content, .post_body, .message-body, .entry-content, .postcontent, article .content",
        "first_post": ".post:first-child, .message:first-child, article:first-child",
    },
}

REQUEST_TIMEOUT = 60
MAX_RETRIES = 2
RETRY_DELAY = 5
INTER_PAGE_DELAY = 3
MAX_CONTENT_LENGTH = 50_000  # chars — truncate thread content beyond this


class ForumConnector(BaseConnector):
    """Scrapes dark web forums for threat mentions via Tor SOCKS proxy."""

    name = "forum"
    source_type = "forum"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None
        self._active_urls: dict[str, str] = {}  # forum_name → resolved base_url

    @property
    def _forums(self) -> list[dict]:
        return self.config.get("forums", [])

    @property
    def _max_pages(self) -> int:
        return self.config.get("max_pages", 3)

    @property
    def _max_content_threads(self) -> int:
        return self.config.get("max_content_threads", 20)

    async def setup(self) -> None:
        proxy_url = settings.tor_socks_proxy.replace("socks5h://", "socks5://")
        connector = ProxyConnector.from_url(proxy_url, rdns=True)
        jar = aiohttp.CookieJar(unsafe=True)  # unsafe=True allows cookies for IP/.onion hosts
        self._session = aiohttp.ClientSession(
            connector=connector,
            cookie_jar=jar,
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
                base_url = await self._resolve_base_url(forum_cfg)
                if not base_url:
                    results[name] = {"healthy": False, "error": "all URLs unreachable"}
                    continue
                async with self._session.get(base_url) as resp:  # type: ignore[union-attr]
                    results[name] = {"healthy": resp.status < 400, "status": resp.status, "url": base_url}
            except Exception as exc:
                results[name] = {"healthy": False, "error": str(exc)[:200]}

        healthy = sum(1 for r in results.values() if r.get("healthy"))
        return {"healthy": healthy > 0, "message": f"{healthy}/{len(results)} reachable", "forums": results}

    # --- Domain resolution ---

    async def _resolve_base_url(self, forum_cfg: dict) -> str | None:
        """Try base_url first, then fallback_urls. Returns first reachable URL or None."""
        name = forum_cfg.get("name", "unknown")

        # Return cached active URL if we have one and it's still good
        if name in self._active_urls:
            cached = self._active_urls[name]
            if await self._probe_url(cached):
                return cached
            del self._active_urls[name]

        # Try primary URL
        base_url = forum_cfg.get("base_url", "")
        if base_url and await self._probe_url(base_url):
            self._active_urls[name] = base_url
            return base_url

        # Try fallback URLs
        for url in forum_cfg.get("fallback_urls", []):
            if await self._probe_url(url):
                logger.info("Forum %s: primary URL unreachable, using fallback %s", name, url)
                self._active_urls[name] = url
                return url

        logger.warning("Forum %s: all URLs unreachable (primary: %s, %d fallbacks)",
                        name, base_url, len(forum_cfg.get("fallback_urls", [])))
        return None

    async def _probe_url(self, url: str) -> bool:
        """Quick HEAD/GET probe to check if a URL is reachable."""
        try:
            async with self._session.get(url, allow_redirects=True) as resp:  # type: ignore[union-attr]
                return resp.status < 500
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return False

    # --- Authentication ---

    async def _authenticate(self, forum_cfg: dict, base_url: str) -> bool:
        """Authenticate to a forum if auth config is present. Returns True if authenticated."""
        auth_cfg = forum_cfg.get("auth")
        if not auth_cfg:
            return True  # No auth needed

        method = auth_cfg.get("method", "cookie")

        if method == "cookie":
            return self._apply_cookies(auth_cfg, base_url)
        elif method == "form":
            return await self._form_login(auth_cfg, base_url, forum_cfg.get("name", "unknown"))

        logger.warning("Unknown auth method: %s", method)
        return False

    def _apply_cookies(self, auth_cfg: dict, base_url: str) -> bool:
        """Set cookies from config into the session cookie jar."""
        cookies = auth_cfg.get("cookies", {})
        if not cookies:
            logger.warning("Cookie auth configured but no cookies provided")
            return False

        from yarl import URL
        jar = self._session.cookie_jar  # type: ignore[union-attr]
        for name, value in cookies.items():
            jar.update_cookies({name: value}, URL(base_url))

        logger.info("Applied %d auth cookies", len(cookies))
        return True

    async def _form_login(self, auth_cfg: dict, base_url: str, forum_name: str) -> bool:
        """Perform form-based login and let the cookie jar capture session cookies."""
        login_url = auth_cfg.get("login_url", "")
        if not login_url:
            logger.warning("Form auth configured but no login_url")
            return False

        if not login_url.startswith("http"):
            login_url = f"{base_url.rstrip('/')}/{login_url.lstrip('/')}"

        login_data = auth_cfg.get("login_data", {})
        if not login_data:
            logger.warning("Form auth configured but no login_data")
            return False

        try:
            async with self._session.post(login_url, data=login_data, allow_redirects=True) as resp:  # type: ignore[union-attr]
                if resp.status >= 400:
                    logger.warning("Forum %s login returned %d", forum_name, resp.status)
                    return False

                # Verify login succeeded if a check selector is configured
                check_selector = auth_cfg.get("login_check")
                if check_selector:
                    body = await resp.text()
                    soup = BeautifulSoup(body, "html.parser")
                    if not soup.select_one(check_selector):
                        logger.warning("Forum %s login check failed (selector %s not found)", forum_name, check_selector)
                        return False

                logger.info("Forum %s: authenticated via form login", forum_name)
                return True
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Forum %s login failed: %s", forum_name, exc)
            return False

    # --- Forum polling ---

    async def _poll_forum(self, forum_cfg: dict) -> list[RawMention]:
        base_url = await self._resolve_base_url(forum_cfg)
        if not base_url:
            return []

        # Authenticate before scraping
        if not await self._authenticate(forum_cfg, base_url):
            logger.warning("Forum %s: authentication failed, scraping unauthenticated",
                            forum_cfg.get("name", "?"))

        recent_path = forum_cfg.get("recent_path", "/")
        forum_name = forum_cfg.get("name", base_url)
        profile = forum_cfg.get("selector_profile", "generic")
        selectors = forum_cfg.get("selectors") or DEFAULT_SELECTORS.get(profile, DEFAULT_SELECTORS["generic"])
        last_seen = forum_cfg.get("last_seen_id", "")
        scrape_content = forum_cfg.get("scrape_content", True)

        mentions: list[RawMention] = []
        new_last_seen = last_seen
        content_fetched = 0

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

                # Fetch thread content if enabled and we haven't hit the limit
                content = thread.get("title", "")
                thread_url = thread.get("url")
                if scrape_content and thread_url and content_fetched < self._max_content_threads:
                    thread_content = await self._fetch_thread_content(
                        thread_url, forum_cfg, profile,
                    )
                    if thread_content:
                        content = thread_content
                        content_fetched += 1

                mentions.append(RawMention(
                    source_name=f"forum:{forum_name}",
                    source_url=thread_url,
                    title=thread.get("title", ""),
                    content=content,
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

    # --- Content scraping ---

    async def _fetch_thread_content(
        self, thread_url: str, forum_cfg: dict, profile: str,
    ) -> str | None:
        """Fetch the first post content from a thread page."""
        html = await self._fetch(thread_url)
        if not html:
            return None

        content_selectors = (
            forum_cfg.get("content_selectors")
            or DEFAULT_CONTENT_SELECTORS.get(profile, DEFAULT_CONTENT_SELECTORS["generic"])
        )

        soup = BeautifulSoup(html, "html.parser")

        # Try first_post selector first (more precise — just the OP)
        first_post_sel = content_selectors.get("first_post", "")
        if first_post_sel:
            el = soup.select_one(first_post_sel)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if text:
                    return text[:MAX_CONTENT_LENGTH]

        # Fall back to post_body selector (get all post bodies, take first)
        body_sel = content_selectors.get("post_body", "")
        if body_sel:
            posts = soup.select(body_sel)
            if posts:
                text = posts[0].get_text(separator="\n", strip=True)
                if text:
                    return text[:MAX_CONTENT_LENGTH]

        return None

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
