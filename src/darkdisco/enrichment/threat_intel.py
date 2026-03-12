"""Threat intelligence enrichment — look up findings against external intel sources.

Supports DeHashed, IntelX, and HIBP. Each provider returns structured intel
that gets merged into the finding's metadata.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime

import aiohttp

from darkdisco.config import settings

logger = logging.getLogger(__name__)


@dataclass
class IntelHit:
    """A single hit from a threat intel provider."""

    provider: str
    category: str  # "breach", "paste", "stealer_log", "leak", etc.
    summary: str
    severity_boost: int = 0  # 0 = no change, positive = increase severity
    details: dict = field(default_factory=dict)
    timestamp: datetime | None = None


class ThreatIntelProvider:
    """Base class for threat intel providers."""

    name: str = "base"

    async def lookup(  # noqa: ARG002
        self, indicators: dict, session: aiohttp.ClientSession
    ) -> list[IntelHit]:
        raise NotImplementedError


class DeHashedProvider(ThreatIntelProvider):
    """Look up email/domain indicators in DeHashed breach database."""

    name = "dehashed"

    async def lookup(self, indicators: dict, session: aiohttp.ClientSession) -> list[IntelHit]:
        if not settings.dehashed_api_key or not settings.dehashed_email:
            return []

        hits = []
        queries = []

        for domain in indicators.get("domains", []):
            queries.append(f"domain:{domain}")
        for email in indicators.get("emails", []):
            queries.append(f"email:{email}")

        auth = aiohttp.BasicAuth(settings.dehashed_email, settings.dehashed_api_key)

        for query in queries[:5]:  # Rate limit: max 5 queries per enrichment
            try:
                async with session.get(
                    "https://api.dehashed.com/search",
                    params={"query": query, "size": 10},
                    auth=auth,
                    headers={"Accept": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.warning("DeHashed returned %d for query %s", resp.status, query)
                        continue
                    data = await resp.json()
                    total = data.get("total", 0)
                    if total > 0:
                        hits.append(IntelHit(
                            provider=self.name,
                            category="breach",
                            summary=f"{total} breach records found for {query}",
                            severity_boost=1 if total > 100 else 0,
                            details={
                                "query": query,
                                "total_records": total,
                                "sample_databases": list({
                                    e.get("database_name", "unknown")
                                    for e in data.get("entries", [])[:10]
                                }),
                            },
                        ))
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("DeHashed lookup failed for %s: %s", query, exc)

        return hits


class HIBPProvider(ThreatIntelProvider):
    """Look up emails/domains against Have I Been Pwned."""

    name = "hibp"

    async def lookup(self, indicators: dict, session: aiohttp.ClientSession) -> list[IntelHit]:
        if not settings.hibp_api_key:
            return []

        hits = []
        headers = {
            "hibp-api-key": settings.hibp_api_key,
            "user-agent": "DarkDisco-ThreatIntel",
        }

        for email in indicators.get("emails", [])[:5]:
            try:
                async with session.get(
                    f"https://haveibeenpwned.com/api/v3/breachedaccount/{email}",
                    headers=headers,
                    params={"truncateResponse": "false"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 404:
                        continue  # Not breached
                    if resp.status == 429:
                        logger.warning("HIBP rate limited, backing off")
                        break
                    if resp.status != 200:
                        continue
                    breaches = await resp.json()
                    if breaches:
                        hits.append(IntelHit(
                            provider=self.name,
                            category="breach",
                            summary=f"{email} found in {len(breaches)} breaches",
                            severity_boost=1 if len(breaches) >= 5 else 0,
                            details={
                                "email": email,
                                "breach_count": len(breaches),
                                "breaches": [
                                    {
                                        "name": b.get("Name"),
                                        "date": b.get("BreachDate"),
                                        "data_classes": b.get("DataClasses", []),
                                    }
                                    for b in breaches[:10]
                                ],
                            },
                        ))
                # HIBP requires 1.5s between requests
                await asyncio.sleep(1.6)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("HIBP lookup failed for %s: %s", email, exc)

        return hits


class IntelXProvider(ThreatIntelProvider):
    """Look up indicators in Intelligence X."""

    name = "intelx"

    async def lookup(self, indicators: dict, session: aiohttp.ClientSession) -> list[IntelHit]:
        if not settings.intelx_api_key:
            return []

        hits = []
        headers = {
            "x-key": settings.intelx_api_key,
            "Content-Type": "application/json",
        }

        search_terms = (
            indicators.get("domains", [])
            + indicators.get("emails", [])
            + indicators.get("keywords", [])
        )

        for term in search_terms[:3]:  # Rate limit
            try:
                # Start search
                async with session.post(
                    "https://2.intelx.io/intelligent/search",
                    json={"term": term, "maxresults": 10, "media": 0, "timeout": 10},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        continue
                    search_data = await resp.json()
                    search_id = search_data.get("id")
                    if not search_id:
                        continue

                # Fetch results
                await asyncio.sleep(2)
                async with session.get(
                    f"https://2.intelx.io/intelligent/search/result",
                    params={"id": search_id},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        continue
                    results = await resp.json()
                    records = results.get("records", [])
                    if records:
                        hits.append(IntelHit(
                            provider=self.name,
                            category="leak",
                            summary=f"{len(records)} IntelX results for {term}",
                            severity_boost=1 if len(records) > 5 else 0,
                            details={
                                "term": term,
                                "result_count": len(records),
                                "buckets": list({r.get("bucket", "unknown") for r in records[:10]}),
                            },
                        ))
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("IntelX lookup failed for %s: %s", term, exc)

        return hits


# Registry of all available providers
_PROVIDERS: list[ThreatIntelProvider] = [
    DeHashedProvider(),
    HIBPProvider(),
    IntelXProvider(),
]


def extract_indicators(finding_data: dict) -> dict:
    """Extract searchable indicators from a finding's content and metadata.

    Returns dict with keys: domains, emails, keywords, ips, bins.
    """
    import re

    content = f"{finding_data.get('title', '')} {finding_data.get('raw_content', '')} {finding_data.get('summary', '')}"

    emails = list(set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", content)))
    domains = list(set(re.findall(r"\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}\b", content)))
    # Filter out common non-domain patterns
    domains = [d for d in domains if "." in d and not d.startswith(".") and d not in emails]

    # Extract from matched terms if available
    matched_terms = finding_data.get("matched_terms", []) or []
    keywords = []
    for term in matched_terms:
        if isinstance(term, dict):
            keywords.append(term.get("value", ""))

    return {
        "emails": emails[:10],
        "domains": domains[:10],
        "keywords": keywords[:5],
    }


async def enrich_finding(finding_data: dict) -> dict:
    """Run all threat intel providers against a finding's indicators.

    Args:
        finding_data: Dict with keys matching Finding model fields.

    Returns:
        Dict with enrichment results to merge into finding metadata.
    """
    indicators = extract_indicators(finding_data)

    # Skip if no useful indicators
    if not any(indicators.values()):
        return {"threat_intel": [], "severity_boost": 0}

    all_hits: list[IntelHit] = []

    async with aiohttp.ClientSession() as session:
        tasks = [provider.lookup(indicators, session) for provider in _PROVIDERS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for provider_result in results:
            if isinstance(provider_result, BaseException):
                logger.warning("Threat intel provider error: %s", provider_result)
                continue
            all_hits.extend(provider_result)

    max_boost = max((h.severity_boost for h in all_hits), default=0)

    return {
        "threat_intel": [
            {
                "provider": h.provider,
                "category": h.category,
                "summary": h.summary,
                "details": h.details,
                "timestamp": h.timestamp.isoformat() if h.timestamp else None,
            }
            for h in all_hits
        ],
        "severity_boost": max_boost,
        "indicators_searched": {k: len(v) for k, v in indicators.items()},
    }
