"""Watch term matcher — matches raw mentions against institution watch terms.

Includes noise-reduction filters (fraud indicator co-occurrence and negative
pattern suppression) loaded from data/matching_filters.yaml.  Edit that file
and restart the worker to tune.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from darkdisco.common.models import WatchTerm, WatchTermType
from darkdisco.discovery.connectors.base import RawMention

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Noise-reduction filter config
# ---------------------------------------------------------------------------

# Strong signal term types bypass noise filters entirely — a BIN range or
# routing number in a dark-web post is inherently actionable.
_STRONG_SIGNAL_TYPES = frozenset({
    WatchTermType.bin_range,
    WatchTermType.routing_number,
    WatchTermType.domain,
    WatchTermType.regex,
})

# Weak signal types that require fraud indicator co-occurrence.
_WEAK_SIGNAL_TYPES = frozenset({
    WatchTermType.institution_name,
    WatchTermType.keyword,
})


@dataclass
class MatchingFilters:
    """Loaded noise-reduction filters."""

    fraud_indicators: list[str] = field(default_factory=list)
    negative_patterns: list[re.Pattern] = field(default_factory=list)


def _resolve_filter_path() -> Path:
    """Locate matching_filters.yaml relative to the project data/ dir."""
    # Try env override first
    env_path = os.environ.get("DARKDISCO_MATCHING_FILTERS")
    if env_path:
        return Path(env_path)
    # Walk up from this file to find the data/ dir
    here = Path(__file__).resolve().parent
    for ancestor in [here, here.parent, here.parent.parent, here.parent.parent.parent]:
        candidate = ancestor / "data" / "matching_filters.yaml"
        if candidate.exists():
            return candidate
    # Default path (might not exist yet — that's OK)
    return here.parent.parent.parent / "data" / "matching_filters.yaml"


def load_matching_filters() -> MatchingFilters:
    """Load fraud indicators and negative patterns from YAML config.

    Returns empty filters (no suppression) if the file is missing, so the
    matcher degrades gracefully to its original behaviour.
    """
    path = _resolve_filter_path()
    if not path.exists():
        logger.warning("Matching filters file not found at %s — noise filtering disabled", path)
        return MatchingFilters()

    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except Exception:
        logger.exception("Failed to parse %s — noise filtering disabled", path)
        return MatchingFilters()

    fraud_indicators = [s.lower() for s in (raw.get("fraud_indicators") or []) if isinstance(s, str)]

    negative_patterns: list[re.Pattern] = []
    for pat in raw.get("negative_patterns") or []:
        if not isinstance(pat, str):
            continue
        try:
            negative_patterns.append(re.compile(pat, re.IGNORECASE))
        except re.error as exc:
            logger.warning("Skipping invalid negative pattern %r: %s", pat, exc)

    logger.info(
        "Loaded matching filters: %d fraud indicators, %d negative patterns from %s",
        len(fraud_indicators), len(negative_patterns), path,
    )
    return MatchingFilters(fraud_indicators=fraud_indicators, negative_patterns=negative_patterns)


# Module-level singleton — reloaded on worker restart.
_filters: MatchingFilters | None = None


def _get_filters() -> MatchingFilters:
    global _filters
    if _filters is None:
        _filters = load_matching_filters()
    return _filters


def reload_filters() -> MatchingFilters:
    """Force-reload filters from disk (useful for tests or hot-reload)."""
    global _filters
    _filters = load_matching_filters()
    return _filters


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class MatchResult:
    """Result of matching a mention against watch terms."""

    institution_id: str
    matched_terms: list[dict]  # [{term_id, term_type, value, context}]
    severity_hint: str  # suggested severity based on match type
    noise_filtered: bool = False  # True if suppressed by noise filters


# ---------------------------------------------------------------------------
# Core matching
# ---------------------------------------------------------------------------

def match_mention(mention: RawMention, watch_terms: list[WatchTerm]) -> list[MatchResult]:
    """Match a raw mention against all active watch terms.

    Returns one MatchResult per institution that matched AND passed noise
    filters.  Mentions that match only on weak signal types (institution_name,
    keyword) are subject to:
      1. Fraud indicator co-occurrence — the text must also contain at least
         one fraud-related keyword.
      2. Negative pattern suppression — if the text matches a benign
         conversation pattern, the match is dropped.

    Strong signal types (bin_range, routing_number, domain, regex) bypass
    noise filters entirely.
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
            # --- Noise filter gate ---
            if _should_filter_as_noise(matched, searchable):
                logger.debug(
                    "Noise-filtered: institution=%s matched_terms=%s",
                    inst_id,
                    ", ".join(f"{t['term_type']}:{t['value']}" for t in matched),
                )
                continue

            results.append(MatchResult(
                institution_id=inst_id,
                matched_terms=matched,
                severity_hint=_severity_hint(matched),
            ))

    return results


# ---------------------------------------------------------------------------
# Noise filter logic
# ---------------------------------------------------------------------------

def _should_filter_as_noise(matched_terms: list[dict], searchable_lower: str) -> bool:
    """Decide whether a set of matched terms should be suppressed as noise.

    Returns True (filter it out) when:
      - ALL matched term types are weak signals, AND
      - EITHER no fraud indicator co-occurs, OR a negative pattern matches.

    Returns False (keep it) when any strong signal type is present.
    """
    term_types = {t["term_type"] for t in matched_terms}

    # If any strong signal type matched, always keep
    strong_type_values = {t.value for t in _STRONG_SIGNAL_TYPES}
    if term_types & strong_type_values:
        return False

    # Only weak signals matched — apply noise filters
    filters = _get_filters()

    # If no filters loaded, degrade to original behaviour (no filtering)
    if not filters.fraud_indicators and not filters.negative_patterns:
        return False

    # Check negative patterns first (fast rejection)
    for pattern in filters.negative_patterns:
        if pattern.search(searchable_lower):
            logger.debug("Negative pattern matched: %s", pattern.pattern)
            return True

    # Require at least one fraud indicator to co-occur
    if filters.fraud_indicators:
        found_indicator = False
        for indicator in filters.fraud_indicators:
            if indicator in searchable_lower:
                found_indicator = True
                break
        if not found_indicator:
            logger.debug("No fraud indicator co-occurrence found")
            return True

    return False


# ---------------------------------------------------------------------------
# Helpers (unchanged)
# ---------------------------------------------------------------------------

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
