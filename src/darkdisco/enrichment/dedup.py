"""Dedup scoring — fuzzy similarity detection beyond exact content hash matching.

The pipeline already does exact content_hash dedup. This module catches
near-duplicates: slightly reworded reposts, cross-source duplicates,
and findings about the same underlying event.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from darkdisco.common.models import Finding

logger = logging.getLogger(__name__)

# Similarity threshold (0-1). Above this, findings are considered near-duplicates.
SIMILARITY_THRESHOLD = 0.75


@dataclass
class DedupResult:
    """Result of dedup analysis for a finding."""

    is_duplicate: bool
    duplicate_of: str | None = None  # finding ID of the original
    similarity_score: float = 0.0
    dedup_reason: str | None = None


def _normalize_text(text: str) -> str:
    """Normalize text for comparison: lowercase, strip punctuation, collapse whitespace."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _ngrams(text: str, n: int = 3) -> set[str]:
    """Generate character n-grams from text."""
    if len(text) < n:
        return {text}
    return {text[i : i + n] for i in range(len(text) - n + 1)}


def _jaccard_similarity(set_a: set, set_b: set) -> float:
    """Jaccard similarity between two sets."""
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def _simhash(text: str) -> int:
    """Compute a 64-bit simhash for near-duplicate detection."""
    tokens = _normalize_text(text).split()
    v = [0] * 64
    for token in tokens:
        h = int(hashlib.md5(token.encode()).hexdigest(), 16)
        for i in range(64):
            if h & (1 << i):
                v[i] += 1
            else:
                v[i] -= 1
    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= 1 << i
    return fingerprint


def _hamming_distance(a: int, b: int) -> int:
    """Count differing bits between two integers."""
    return bin(a ^ b).count("1")


def compute_similarity(content_a: str, content_b: str) -> float:
    """Compute similarity between two pieces of content using multiple signals.

    Combines n-gram Jaccard similarity with simhash hamming distance
    for a balanced score.
    """
    norm_a = _normalize_text(content_a)
    norm_b = _normalize_text(content_b)

    # Fast path: identical normalized content
    if norm_a == norm_b:
        return 1.0

    # N-gram Jaccard similarity (good for reworded content)
    ngrams_a = _ngrams(norm_a, 3)
    ngrams_b = _ngrams(norm_b, 3)
    jaccard = _jaccard_similarity(ngrams_a, ngrams_b)

    # Simhash hamming distance (good for large texts with minor edits)
    hash_a = _simhash(content_a)
    hash_b = _simhash(content_b)
    hamming = _hamming_distance(hash_a, hash_b)
    # Normalize hamming distance to 0-1 similarity (64 bits max)
    simhash_sim = 1.0 - (hamming / 64.0)

    # Weighted combination: Jaccard is more reliable for short texts,
    # simhash for long texts
    if len(norm_a) < 200 or len(norm_b) < 200:
        return 0.7 * jaccard + 0.3 * simhash_sim
    return 0.5 * jaccard + 0.5 * simhash_sim


def check_dedup(
    finding_data: dict,
    session: Session,
    lookback_hours: int = 72,
) -> DedupResult:
    """Check if a finding is a near-duplicate of an existing finding.

    Compares against recent findings for the same institution using
    fuzzy text similarity.

    Args:
        finding_data: Dict with Finding-like fields (institution_id, raw_content, title, etc.)
        session: SQLAlchemy session for querying existing findings.
        lookback_hours: How far back to search for duplicates.

    Returns:
        DedupResult indicating whether this is a duplicate.
    """
    from datetime import datetime, timedelta, timezone

    institution_id = finding_data.get("institution_id")
    content = finding_data.get("raw_content") or finding_data.get("summary") or ""
    title = finding_data.get("title", "")
    new_text = f"{title}\n{content}"

    if not new_text.strip():
        return DedupResult(is_duplicate=False)

    # Query recent findings for the same institution
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    recent_findings = session.execute(
        select(Finding)
        .where(
            Finding.institution_id == institution_id,
            Finding.discovered_at >= cutoff,
        )
        .order_by(Finding.discovered_at.desc())
        .limit(100)  # Cap to avoid scanning too many
    ).scalars().all()

    best_score = 0.0
    best_match_id = None

    for existing in recent_findings:
        existing_text = f"{existing.title}\n{existing.raw_content or existing.summary or ''}"
        score = compute_similarity(new_text, existing_text)

        if score > best_score:
            best_score = score
            best_match_id = existing.id

    if best_score >= SIMILARITY_THRESHOLD:
        return DedupResult(
            is_duplicate=True,
            duplicate_of=best_match_id,
            similarity_score=best_score,
            dedup_reason=f"Near-duplicate of finding {best_match_id} (similarity: {best_score:.2f})",
        )

    return DedupResult(
        is_duplicate=False,
        similarity_score=best_score,
    )
