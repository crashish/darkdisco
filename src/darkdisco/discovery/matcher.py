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
    raw_text = f"{mention.title}\n{mention.content}"

    for inst_id, terms in by_institution.items():
        matched = []
        for term in terms:
            value = term.value
            if not term.case_sensitive:
                value = value.lower()

            highlights: list[dict] = []

            if term.term_type == WatchTermType.regex:
                try:
                    flags = 0 if term.case_sensitive else re.IGNORECASE
                    highlights = _find_all_spans(value, raw_text, flags=flags)
                    if highlights:
                        matched.append(_term_dict(term, highlights))
                except re.error:
                    logger.warning("Invalid regex in watch term %s: %s", term.id, value)
            elif term.term_type == WatchTermType.bin_range:
                pattern = rf"\b{re.escape(value)}\d{{2,10}}\b"
                highlights = _find_all_spans(pattern, raw_text, flags=re.IGNORECASE)
                if highlights:
                    matched.append(_term_dict(term, highlights))
            elif term.term_type == WatchTermType.domain:
                escaped = re.escape(value)
                pattern = rf"(?:^|[\s//@.:])({escaped})(?:$|[\s/,;:)\]>]|/)"
                highlights = _find_all_spans(pattern, raw_text, flags=re.IGNORECASE, group=1)
                if highlights:
                    matched.append(_term_dict(term, highlights))
            elif term.term_type == WatchTermType.institution_name:
                escaped = re.escape(value)
                pattern = rf"\b{escaped}\b"
                highlights = _find_all_spans(pattern, raw_text, flags=re.IGNORECASE)
                if highlights:
                    matched.append(_term_dict(term, highlights))
            else:
                # keyword, executive_name, routing_number: find all occurrences
                highlights = _find_all_substring(value, searchable)
                if highlights:
                    matched.append(_term_dict(term, highlights))

        if matched:
            results.append(MatchResult(
                institution_id=inst_id,
                matched_terms=matched,
                severity_hint=_severity_hint(matched),
            ))

    return results


def _find_all_spans(
    pattern: str, text: str, *, flags: int = 0, group: int = 0,
) -> list[dict]:
    """Find all regex matches and return their character spans."""
    spans = []
    for m in re.finditer(pattern, text, flags):
        start, end = m.span(group)
        spans.append({"start": start, "end": end})
    return spans


def _find_all_substring(value_lower: str, searchable: str) -> list[dict]:
    """Find all substring occurrences in the lowercased text."""
    spans = []
    start = 0
    while True:
        idx = searchable.find(value_lower, start)
        if idx == -1:
            break
        spans.append({"start": idx, "end": idx + len(value_lower)})
        start = idx + 1
    return spans


def _term_dict(term: WatchTerm, highlights: list[dict] | None = None) -> dict:
    d: dict = {
        "term_id": term.id,
        "term_type": term.term_type.value,
        "value": term.value,
    }
    if highlights:
        d["highlights"] = highlights
    return d


def recompute_highlights(matched_terms: list[dict], raw_content: str) -> list[dict]:
    """Recompute highlight offsets against the actual stored raw_content.

    The matcher computes offsets against the full mention text, but the finding
    may store different content (e.g. attributed to specific extracted files).
    This recalculates offsets so they match what's displayed.
    """
    result = []
    for term in matched_terms:
        value = term["value"]
        # Case-insensitive search for the term value in raw_content
        highlights = _find_all_substring(value.lower(), raw_content.lower())
        updated = dict(term)
        if highlights:
            updated["highlights"] = highlights
        else:
            updated.pop("highlights", None)
        result.append(updated)
    return result


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
