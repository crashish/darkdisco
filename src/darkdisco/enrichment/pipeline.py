"""Enrichment pipeline — orchestrates threat intel, dedup, and FP filtering.

Called by the Celery pipeline worker after matching produces candidate findings.
Enrichment runs synchronously within the Celery task but dispatches async IO
for external threat intel lookups.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from darkdisco.enrichment.dedup import DedupResult, check_dedup
from darkdisco.enrichment.false_positive import FPResult, check_false_positive
from darkdisco.enrichment.threat_intel import enrich_finding

logger = logging.getLogger(__name__)

# Severity levels ordered by rank (lower index = more severe)
_SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"]


@dataclass
class EnrichmentResult:
    """Combined result of all enrichment steps for a finding."""

    # Should this finding be created?
    should_create: bool = True
    # Final severity after enrichment adjustments
    adjusted_severity: str | None = None
    # Merged metadata from all enrichment sources
    enrichment_metadata: dict = field(default_factory=dict)
    # Individual results
    dedup: DedupResult | None = None
    fp: FPResult | None = None
    threat_intel: dict = field(default_factory=dict)
    # Reason if finding was suppressed
    suppression_reason: str | None = None


def _boost_severity(current: str, boost: int) -> str:
    """Increase severity by boost levels (e.g., medium + 1 = high)."""
    try:
        idx = _SEVERITY_ORDER.index(current)
    except ValueError:
        return current
    new_idx = max(0, idx - boost)
    return _SEVERITY_ORDER[new_idx]


def _downgrade_severity(current: str) -> str:
    """Decrease severity by one level."""
    try:
        idx = _SEVERITY_ORDER.index(current)
    except ValueError:
        return current
    new_idx = min(len(_SEVERITY_ORDER) - 1, idx + 1)
    return _SEVERITY_ORDER[new_idx]


def enrich_and_filter(
    finding_data: dict,
    session: Session,
    run_threat_intel: bool = True,
) -> EnrichmentResult:
    """Run the full enrichment pipeline on a candidate finding.

    Steps:
    1. Dedup scoring — check for near-duplicate existing findings
    2. False positive filtering — heuristic FP detection
    3. Threat intel enrichment — external lookups (async, bridged to sync)

    Args:
        finding_data: Dict with Finding-like fields.
        session: SQLAlchemy session for DB queries.
        run_threat_intel: Whether to run external threat intel lookups.

    Returns:
        EnrichmentResult with the verdict and enrichment data.
    """
    result = EnrichmentResult()
    current_severity = finding_data.get("severity", "medium")

    # --- Step 1: Dedup ---
    try:
        dedup_result = check_dedup(finding_data, session)
        result.dedup = dedup_result

        if dedup_result.is_duplicate:
            result.should_create = False
            result.suppression_reason = dedup_result.dedup_reason
            result.enrichment_metadata["dedup"] = {
                "is_duplicate": True,
                "duplicate_of": dedup_result.duplicate_of,
                "similarity_score": dedup_result.similarity_score,
            }
            logger.info(
                "Finding suppressed as near-duplicate (score=%.2f, original=%s)",
                dedup_result.similarity_score,
                dedup_result.duplicate_of,
            )
            return result

        result.enrichment_metadata["dedup"] = {
            "is_duplicate": False,
            "best_similarity": dedup_result.similarity_score,
        }
    except Exception:
        logger.exception("Dedup check failed, continuing without dedup")

    # --- Step 2: False positive filtering ---
    try:
        fp_result = check_false_positive(finding_data)
        result.fp = fp_result

        result.enrichment_metadata["false_positive"] = {
            "fp_score": fp_result.fp_score,
            "is_likely_fp": fp_result.is_likely_fp,
            "recommendation": fp_result.recommendation,
            "signals": [
                {"rule": s.rule, "description": s.description, "weight": s.weight}
                for s in fp_result.signals
            ],
        }

        if fp_result.recommendation == "auto_dismiss":
            result.should_create = False
            result.suppression_reason = (
                f"Auto-dismissed as likely false positive (score={fp_result.fp_score:.2f})"
            )
            logger.info("Finding auto-dismissed as false positive (score=%.2f)", fp_result.fp_score)
            return result

        if fp_result.recommendation == "downgrade":
            current_severity = _downgrade_severity(current_severity)
            logger.info(
                "Finding severity downgraded due to FP signals (score=%.2f)",
                fp_result.fp_score,
            )
    except Exception:
        logger.exception("FP check failed, continuing without FP filtering")

    # --- Step 3: Threat intel enrichment ---
    if run_threat_intel:
        try:
            intel_data = asyncio.run(enrich_finding(finding_data))
            result.threat_intel = intel_data
            result.enrichment_metadata["threat_intel"] = intel_data

            # Apply severity boost from threat intel
            severity_boost = intel_data.get("severity_boost", 0)
            if severity_boost > 0:
                current_severity = _boost_severity(current_severity, severity_boost)
                logger.info(
                    "Finding severity boosted by %d from threat intel",
                    severity_boost,
                )
        except Exception:
            logger.exception("Threat intel enrichment failed, continuing without it")

    result.adjusted_severity = current_severity
    return result
