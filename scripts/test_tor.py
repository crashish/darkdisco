#!/usr/bin/env python3
"""Test Tor connectivity and forum reachability.

Usage:
    PYTHONPATH=src .venv/bin/python scripts/test_tor.py [--local]

By default tests via docker Tor (socks5h://tor:9050).
Use --local for local Tor (socks5h://127.0.0.1:9050).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import aiohttp
from aiohttp_socks import ProxyConnector


PROXY_DOCKER = "socks5://tor:9050"
PROXY_LOCAL = "socks5://127.0.0.1:9050"

TARGETS = [
    # Clearnet through Tor
    ("Tor check", "https://check.torproject.org/api/ip"),
    # Forum clearnet mirrors
    ("BreachForums (.bf)", "https://breachforums.bf/"),
    ("Exploit.in", "https://exploit.in/"),
]


async def test_connectivity(proxy_url: str):
    print(f"Testing with proxy: {proxy_url}\n")

    try:
        connector = ProxyConnector.from_url(proxy_url)
    except Exception as e:
        print(f"ERROR: Cannot create proxy connector: {e}")
        print("Is Tor running?")
        return

    timeout = aiohttp.ClientTimeout(total=30)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "text/html,application/xhtml+xml",
    }

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers=headers,
    ) as session:
        for name, url in TARGETS:
            try:
                async with session.get(url) as resp:
                    status = resp.status
                    content_len = resp.content_length or 0
                    # Read a bit of body for Tor check
                    body = ""
                    if "torproject" in url:
                        body = await resp.text()
                    elif status == 200:
                        text = await resp.text()
                        content_len = len(text)
                        # Extract title
                        import re
                        m = re.search(r"<title>(.*?)</title>", text[:2000], re.I)
                        body = m.group(1) if m else f"{content_len} bytes"

                    if status < 400:
                        print(f"  [OK]    {name:25s} → {status} | {body[:80]}")
                    else:
                        print(f"  [WARN]  {name:25s} → {status}")

            except Exception as e:
                print(f"  [FAIL]  {name:25s} → {e}")

            await asyncio.sleep(2)


if __name__ == "__main__":
    proxy = PROXY_LOCAL if "--local" in sys.argv else PROXY_DOCKER
    asyncio.run(test_connectivity(proxy))
