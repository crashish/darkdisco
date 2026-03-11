"""Watch term matcher — matches raw mentions against institution watch terms."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from darkdisco.common.models import WatchTerm, WatchTermType
from darkdisco.discovery.connectors.base import RawMention

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of matching a mention against watch terms."""

    institution_id: str
    matched_terms: list[dict]  # [{term_id, term_type, value, context}]
    severity_hint: str  # suggested severity based on match type


def match_mention(mention: RawMention, watch_terms: list[WatchTerm]) -> list[MatchResult]:
    """Match a raw mention against all active watch terms.

    Returns one MatchResult per institution that matched.
    """
    # Group terms by institution
    by_institution: dict[str, list[WatchTerm]] = {}
    for wt in watch_terms:
        if not wt.enabled:
            continue
        by_institution.setdefault(wt.institution_id, []).append(wt)

    results = []
    searchable = f"{mention.title}\n{mention.content}".lower()

    for inst_id, terms in by_institution.items():
        matched = []
        for term in terms:
            value = term.value
            if not term.case_sensitive:
                value = value.lower()

            if term.term_type == WatchTermType.regex:
                try:
                    flags = 0 if term.case_sensitive else re.IGNORECASE
                    if re.search(value, f"{mention.title}\n{mention.content}", flags):
                        matched.append(_term_dict(term))
                except re.error:
                    logger.warning("Invalid regex in watch term %s: %s", term.id, value)
            elif term.term_type == WatchTermType.bin_range:
                # BIN ranges are 6-8 digit prefixes
                if re.search(rf"\b{re.escape(value)}\d{{2,10}}\b", searchable):
                    matched.append(_term_dict(term))
            else:
                # All other types: substring match
                if value in searchable:
                    matched.append(_term_dict(term))

        if matched:
            results.append(MatchResult(
                institution_id=inst_id,
                matched_terms=matched,
                severity_hint=_severity_hint(matched),
            ))

    return results


def _term_dict(term: WatchTerm) -> dict:
    return {
        "term_id": term.id,
        "term_type": term.term_type.value,
        "value": term.value,
    }


def _severity_hint(matched: list[dict]) -> str:
    """Suggest severity based on what types of terms matched."""
    types = {m["term_type"] for m in matched}
    if "bin_range" in types or "routing_number" in types:
        return "critical"
    if "domain" in types and "executive_name" in types:
        return "high"
    if "institution_name" in types:
        return "medium"
    return "low"
