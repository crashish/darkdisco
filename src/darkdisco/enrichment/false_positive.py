"""False positive filtering — detect and flag likely false positive findings.

Uses heuristic rules to score findings on a false-positive likelihood scale.
High-scoring findings get auto-flagged or downgraded rather than alerting analysts.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Score threshold: findings above this are considered likely false positives
# Raised from 0.70 to 0.80 — the original threshold was too aggressive and
# suppressed legitimate findings before the system produced any real results.
FP_THRESHOLD = 0.80


@dataclass
class FPSignal:
    """A single false-positive signal with its weight."""

    rule: str
    description: str
    weight: float  # 0.0-1.0 contribution to FP score


@dataclass
class FPResult:
    """Result of false positive analysis."""

    fp_score: float  # 0.0-1.0 (higher = more likely false positive)
    is_likely_fp: bool
    signals: list[FPSignal] = field(default_factory=list)
    recommendation: str = "keep"  # "keep", "downgrade", "auto_dismiss"


def _check_generic_mention(_content: str, matched_terms: list[dict]) -> FPSignal | None:
    """Flag mentions that match only on very generic terms.

    Only flags keyword-type matches, NOT institution_name — institution names
    are the primary matching mechanism and shouldn't be penalized.
    """
    if not matched_terms:
        return None

    # Only flag pure keyword-only matches as generic; institution_name,
    # domain, bin_range, etc. are strong signals we should keep.
    generic_types = {"keyword"}
    all_generic = all(t.get("term_type") in generic_types for t in matched_terms)

    if all_generic and len(matched_terms) == 1:
        term = matched_terms[0]
        value = term.get("value", "")
        # Very short generic terms are more likely to be FPs
        if len(value) < 5:
            return FPSignal(
                rule="generic_short_term",
                description=f"Single short generic match: '{value}'",
                weight=0.4,
            )
        # Common words that happen to be bank names
        common_words = {
            "first", "national", "american", "united", "community",
            "central", "western", "eastern", "northern", "southern",
        }
        if value.lower() in common_words:
            return FPSignal(
                rule="common_word_match",
                description=f"Matched common word: '{value}'",
                weight=0.35,
            )
    return None


def _check_boilerplate_content(content: str) -> FPSignal | None:
    """Detect boilerplate/template content that often triggers false matches."""
    boilerplate_patterns = [
        r"(?i)terms\s+(?:and|&)\s+conditions",
        r"(?i)privacy\s+policy",
        r"(?i)copyright\s+\d{4}",
        r"(?i)all\s+rights\s+reserved",
        r"(?i)cookie\s+(?:policy|consent|notice)",
        r"(?i)unsubscribe\s+from\s+(?:this|these)",
        r"(?i)this\s+(?:email|message)\s+was\s+sent\s+to",
    ]
    matches = sum(1 for p in boilerplate_patterns if re.search(p, content))
    if matches >= 2:
        return FPSignal(
            rule="boilerplate_content",
            description=f"Content matches {matches} boilerplate patterns",
            weight=min(0.4 + matches * 0.1, 0.8),
        )
    return None


def _check_legitimate_context(content: str) -> FPSignal | None:
    """Detect content that's clearly from legitimate/non-threat contexts."""
    legitimate_patterns = [
        r"(?i)job\s+(?:posting|listing|opening|opportunity)",
        r"(?i)(?:hiring|we.re\s+looking\s+for)",
        r"(?i)press\s+release",
        r"(?i)annual\s+report",
        r"(?i)(?:quarterly|annual)\s+earnings",
        r"(?i)customer\s+(?:review|testimonial|feedback)",
        r"(?i)(?:news|blog)\s+(?:article|post)",
    ]
    matches = sum(1 for p in legitimate_patterns if re.search(p, content))
    if matches >= 2:
        return FPSignal(
            rule="legitimate_context",
            description=f"Content appears to be from legitimate context ({matches} indicators)",
            weight=0.3 * min(matches, 3),
        )
    return None


def _check_low_content(content: str) -> FPSignal | None:
    """Flag findings with very little actionable content."""
    stripped = content.strip()
    if len(stripped) < 50:
        return FPSignal(
            rule="low_content",
            description=f"Very short content ({len(stripped)} chars)",
            weight=0.3,
        )
    # Check if content is mostly URLs or hashes (not actionable text)
    words = stripped.split()
    non_url_words = [w for w in words if not re.match(r"https?://", w) and len(w) < 64]
    if len(non_url_words) < 5:
        return FPSignal(
            rule="low_actionable_content",
            description="Content is mostly URLs or hashes, little actionable text",
            weight=0.25,
        )
    return None


def _check_repeated_source(content: str, metadata: dict) -> FPSignal | None:
    """Flag content from sources known to produce high FP rates."""
    source_type = metadata.get("source_type", "")
    # Auto-generated content from paste sites with no context is often FP
    if source_type == "paste_site":
        # Check if content looks auto-generated (lots of random chars)
        if content and len(content) > 100:
            alphanum_ratio = sum(c.isalnum() for c in content) / len(content)
            if alphanum_ratio > 0.9:
                return FPSignal(
                    rule="auto_generated_paste",
                    description="Paste site content appears auto-generated",
                    weight=0.35,
                )
    return None


# All FP check functions
_FP_CHECKS = [
    _check_generic_mention,
    _check_boilerplate_content,
    _check_legitimate_context,
    _check_low_content,
]


def check_false_positive(finding_data: dict) -> FPResult:
    """Analyze a finding for false positive indicators.

    Args:
        finding_data: Dict with Finding-like fields.

    Returns:
        FPResult with score, signals, and recommendation.
    """
    content = f"{finding_data.get('title', '')} {finding_data.get('raw_content', '') or finding_data.get('summary', '')}"
    matched_terms = finding_data.get("matched_terms", []) or []
    metadata = finding_data.get("metadata", {}) or {}

    signals: list[FPSignal] = []

    # Run content-based checks
    for check_fn in _FP_CHECKS:
        # Each check takes different args, dispatch accordingly
        if check_fn == _check_generic_mention:
            signal = check_fn(content, matched_terms)
        else:
            signal = check_fn(content)
        if signal:
            signals.append(signal)

    # Run metadata-based checks
    source_signal = _check_repeated_source(content, metadata)
    if source_signal:
        signals.append(source_signal)

    # Compute aggregate score (capped at 1.0)
    if not signals:
        return FPResult(fp_score=0.0, is_likely_fp=False, recommendation="keep")

    # Combine weights: use a soft-max approach so multiple weak signals
    # can push the score high, but a single signal can't exceed its weight
    combined = 1.0
    for s in signals:
        combined *= (1.0 - s.weight)
    fp_score = min(1.0 - combined, 1.0)

    is_fp = fp_score >= FP_THRESHOLD

    if is_fp:
        recommendation = "auto_dismiss" if fp_score >= 0.95 else "downgrade"
    else:
        recommendation = "keep"

    return FPResult(
        fp_score=fp_score,
        is_likely_fp=is_fp,
        signals=signals,
        recommendation=recommendation,
    )
