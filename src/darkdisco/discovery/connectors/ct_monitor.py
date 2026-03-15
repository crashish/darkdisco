"""Certificate Transparency monitoring connector.

Monitors CT logs via crt.sh for domains resembling monitored institutions.
Detects look-alike/typosquatting domains registered in CT logs that may be
used for phishing campaigns targeting specific financial institutions.

Unlike Trapline's broad certstream monitoring, this connector focuses on
institution-specific domain matching by cross-referencing against the
darkdisco institution list (primary_domain, additional_domains, name).
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher

import aiohttp

from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

# crt.sh API timeout — it can be slow under load
_CRT_SH_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=15)

# Minimum similarity ratio (0-1) for fuzzy domain matching
_DEFAULT_SIMILARITY_THRESHOLD = 0.7

# Common phishing keywords prepended/appended to legitimate domains
_PHISHING_AFFIXES = [
    "secure", "login", "verify", "update", "account", "online", "banking",
    "auth", "confirm", "alert", "support", "service", "signin", "client",
    "mobile", "app", "portal", "access", "customer", "member", "webmail",
]

# TLDs commonly abused for phishing
_SUSPICIOUS_TLDS = {
    ".xyz", ".top", ".club", ".online", ".site", ".info", ".icu",
    ".buzz", ".shop", ".tk", ".ml", ".ga", ".cf", ".gq",
    ".work", ".live", ".store", ".tech", ".space", ".fun",
}


def _extract_base_domain(domain: str) -> str:
    """Extract the registrable base from a domain (strip subdomains and TLD)."""
    parts = domain.lower().strip(".").split(".")
    if len(parts) >= 2:
        return parts[-2]
    return parts[0]


def _normalize_name(name: str) -> str:
    """Normalize an institution name for matching (lowercase, alphanum only)."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _is_look_alike(candidate: str, targets: list[str], threshold: float) -> tuple[bool, str, float]:
    """Check if a candidate domain looks like any target domain/name.

    Returns (is_match, best_target, similarity_score).
    """
    candidate_base = _extract_base_domain(candidate)
    candidate_norm = re.sub(r"[^a-z0-9]", "", candidate_base)

    best_score = 0.0
    best_target = ""

    for target in targets:
        target_norm = _normalize_name(target)
        if not target_norm:
            continue

        # Exact containment — target name appears in the candidate
        if target_norm in candidate_norm or candidate_norm in target_norm:
            return True, target, 1.0

        # Check if candidate is target + phishing affix
        for affix in _PHISHING_AFFIXES:
            if candidate_norm == target_norm + affix or candidate_norm == affix + target_norm:
                return True, target, 0.95

        # Check with hyphens (e.g. chase-secure-login)
        candidate_dehyphen = re.sub(r"[^a-z0-9]", "", candidate.lower().split(".")[0])
        if target_norm in candidate_dehyphen:
            return True, target, 0.9

        # Fuzzy similarity
        score = SequenceMatcher(None, candidate_norm, target_norm).ratio()
        if score > best_score:
            best_score = score
            best_target = target

    if best_score >= threshold:
        return True, best_target, best_score

    return False, best_target, best_score


def _domain_content_hash(domain: str, not_before: str) -> str:
    """Deterministic hash for deduplication of CT entries."""
    key = f"ct:{domain}:{not_before}".lower()
    return hashlib.sha256(key.encode()).hexdigest()[:16]


class CTMonitorConnector(BaseConnector):
    """Monitors Certificate Transparency logs for look-alike domains.

    Uses crt.sh JSON API to search for certificates matching institution
    domain patterns. Identifies potential phishing infrastructure by
    detecting newly-registered domains that resemble monitored institutions.

    Source config schema (stored in Source.config JSONB):
    {
        "institutions": [
            {
                "institution_id": "<uuid>",
                "name": "Chase Bank",
                "primary_domain": "chase.com",
                "additional_domains": ["jpmorganchase.com"],
                "short_name": "Chase",
                "search_terms": ["chase", "jpmchase"]
            },
            ...
        ],
        "similarity_threshold": 0.7,
        "max_results_per_query": 200,
        "seen_hashes": ["<hash>", ...],
        "exclude_domains": ["chase.com", "jpmorganchase.com"]
    }

    The institutions list is typically synced from the DB by the pipeline
    scheduler before each poll cycle.
    """

    name = "ct_monitor"
    source_type = "ct_monitor"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None

    async def setup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=_CRT_SH_TIMEOUT,
            headers={"User-Agent": "darkdisco-ct-monitor/1.0"},
        )

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        """Query crt.sh for certificates matching institution patterns.

        For each configured institution, searches CT logs for domains that
        contain or resemble the institution's name/domain. New look-alike
        domains are returned as RawMentions for the matching pipeline.
        """
        if not self._session:
            await self.setup()

        institutions = self.config.get("institutions", [])
        if not institutions:
            logger.warning("CTMonitorConnector: no institutions configured")
            return []

        threshold = self.config.get("similarity_threshold", _DEFAULT_SIMILARITY_THRESHOLD)
        max_results = self.config.get("max_results_per_query", 200)
        seen_hashes: set[str] = set(self.config.get("seen_hashes", []))
        exclude_domains: set[str] = {d.lower() for d in self.config.get("exclude_domains", [])}
        mentions: list[RawMention] = []

        for inst in institutions:
            inst_mentions = await self._poll_institution(
                inst, threshold, max_results, seen_hashes, exclude_domains, since,
            )
            mentions.extend(inst_mentions)

        # Persist seen hashes — cap at 50k to prevent unbounded growth
        self.config["seen_hashes"] = list(seen_hashes)[-50000:]

        logger.info(
            "CTMonitorConnector polled %d institutions, found %d new look-alike domains",
            len(institutions),
            len(mentions),
        )
        return mentions

    async def _poll_institution(
        self,
        inst: dict,
        threshold: float,
        max_results: int,
        seen_hashes: set[str],
        exclude_domains: set[str],
        since: datetime | None,
    ) -> list[RawMention]:
        """Poll CT logs for a single institution's look-alike domains."""
        name = inst.get("name", "")
        primary_domain = inst.get("primary_domain", "")
        additional_domains = inst.get("additional_domains", []) or []
        short_name = inst.get("short_name", "")

        # Build search queries — use the base domain name and institution name
        search_queries: list[str] = []
        if primary_domain:
            base = _extract_base_domain(primary_domain)
            if len(base) >= 3:
                search_queries.append(f"%{base}%")
        if short_name and len(short_name) >= 3:
            normalized = _normalize_name(short_name)
            if normalized and f"%{normalized}%" not in search_queries:
                search_queries.append(f"%{normalized}%")
        for term in inst.get("search_terms", []):
            if len(term) >= 3:
                q = f"%{term.lower()}%"
                if q not in search_queries:
                    search_queries.append(q)

        if not search_queries:
            return []

        # Build target list for similarity comparison
        targets: list[str] = []
        if primary_domain:
            targets.append(_extract_base_domain(primary_domain))
        for d in additional_domains:
            targets.append(_extract_base_domain(d))
        if name:
            targets.append(name)
        if short_name:
            targets.append(short_name)

        # Collect all legitimate domains to exclude from results
        legit_domains = set(exclude_domains)
        if primary_domain:
            legit_domains.add(primary_domain.lower())
        for d in additional_domains:
            legit_domains.add(d.lower())

        mentions: list[RawMention] = []

        for query in search_queries:
            certs = await self._query_crt_sh(query, max_results)
            if certs is None:
                continue

            for cert in certs:
                common_name = (cert.get("common_name") or "").lower().strip()
                name_value = (cert.get("name_value") or "").lower().strip()
                not_before = cert.get("not_before", "")
                issuer = cert.get("issuer_name", "")

                # Process all domains in the certificate (CN + SANs)
                domains: set[str] = set()
                if common_name and "*" not in common_name:
                    domains.add(common_name)
                for san in name_value.split("\n"):
                    san = san.strip()
                    if san and "*" not in san:
                        domains.add(san)

                for domain in domains:
                    # Skip legitimate domains
                    if domain in legit_domains:
                        continue
                    # Skip if any legit domain is a parent of this domain
                    if any(domain.endswith("." + ld) for ld in legit_domains):
                        continue

                    content_hash = _domain_content_hash(domain, not_before)
                    if content_hash in seen_hashes:
                        continue

                    # Check if this domain looks like the institution
                    is_match, matched_target, score = _is_look_alike(
                        domain, targets, threshold,
                    )
                    if not is_match:
                        continue

                    # Filter by time if requested
                    cert_time = _parse_ct_timestamp(not_before)
                    if since and cert_time and cert_time < since:
                        continue

                    seen_hashes.add(content_hash)

                    # Determine suspicion indicators
                    indicators = _suspicion_indicators(domain, matched_target, score)

                    title = f"[CT] Look-alike domain: {domain}"
                    content_parts = [
                        f"Suspicious domain detected in Certificate Transparency logs:",
                        f"  Domain: {domain}",
                        f"  Resembles: {matched_target} (similarity: {score:.0%})",
                        f"  Certificate issued: {not_before}",
                        f"  Issuer: {issuer}",
                        f"  Institution: {name}",
                    ]
                    if indicators:
                        content_parts.append(f"  Indicators: {', '.join(indicators)}")

                    # Include institution domains in content so the matcher
                    # can match against domain watch terms
                    if primary_domain:
                        content_parts.append(f"  Monitored domain: {primary_domain}")

                    metadata = {
                        "ct_domain": domain,
                        "matched_target": matched_target,
                        "similarity_score": round(score, 3),
                        "institution_id": inst.get("institution_id", ""),
                        "institution_name": name,
                        "not_before": not_before,
                        "issuer": issuer,
                        "content_hash": content_hash,
                        "indicators": indicators,
                    }

                    mentions.append(RawMention(
                        source_name=f"ct_monitor:{_normalize_name(name) or 'unknown'}",
                        source_url=f"https://crt.sh/?q={domain}",
                        title=title,
                        content="\n".join(content_parts),
                        discovered_at=cert_time or datetime.now(timezone.utc),
                        metadata=metadata,
                    ))

        return mentions

    async def _query_crt_sh(self, query: str, max_results: int) -> list[dict] | None:
        """Query the crt.sh JSON API for certificates matching a pattern.

        Args:
            query: SQL LIKE pattern (e.g. '%chase%')
            max_results: Maximum number of results to return

        Returns:
            List of certificate dicts, or None on error.
        """
        assert self._session is not None
        url = "https://crt.sh/"
        params = {
            "q": query,
            "output": "json",
            "deduplicate": "Y",
        }

        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 404:
                    return []
                if resp.status != 200:
                    logger.warning("crt.sh returned HTTP %d for query %s", resp.status, query)
                    return None

                data = await resp.json(content_type=None)
                if not isinstance(data, list):
                    return []
                # Sort by most recent first and limit
                data.sort(key=lambda c: c.get("not_before", ""), reverse=True)
                return data[:max_results]

        except aiohttp.ClientError as exc:
            logger.warning("crt.sh query failed for %s: %s", query, exc)
            return None
        except Exception:
            logger.exception("Unexpected error querying crt.sh for %s", query)
            return None

    async def health_check(self) -> dict:
        """Verify crt.sh API is reachable."""
        if not self._session:
            await self.setup()
        assert self._session is not None

        try:
            async with self._session.get(
                "https://crt.sh/",
                params={"q": "example.com", "output": "json"},
            ) as resp:
                if resp.status == 200:
                    return {"healthy": True, "message": "crt.sh API reachable"}
                return {
                    "healthy": False,
                    "message": f"crt.sh returned HTTP {resp.status}",
                }
        except aiohttp.ClientError as exc:
            return {"healthy": False, "message": f"crt.sh unreachable: {exc}"}


def _parse_ct_timestamp(ts: str) -> datetime | None:
    """Parse a crt.sh timestamp like '2024-01-15T12:00:00'."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(ts.strip(), fmt).replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            continue
    return None


def _suspicion_indicators(domain: str, target: str, score: float) -> list[str]:
    """Identify specific indicators that make a domain suspicious."""
    indicators = []
    domain_lower = domain.lower()
    target_norm = _normalize_name(target)

    # Check for phishing keywords
    for affix in _PHISHING_AFFIXES:
        if affix in domain_lower and target_norm in re.sub(r"[^a-z0-9]", "", domain_lower):
            indicators.append(f"phishing-keyword:{affix}")
            break

    # Suspicious TLD
    for tld in _SUSPICIOUS_TLDS:
        if domain_lower.endswith(tld):
            indicators.append(f"suspicious-tld:{tld}")
            break

    # Hyphenated look-alike (e.g. chase-secure-login.com)
    if "-" in domain_lower and target_norm in re.sub(r"[^a-z0-9]", "", domain_lower):
        indicators.append("hyphenated-lookalike")

    # Homograph-style (character substitution)
    if score >= 0.8 and score < 1.0:
        indicators.append("possible-typosquat")

    return indicators
