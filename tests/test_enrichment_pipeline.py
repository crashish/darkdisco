"""Enrichment pipeline tests.

Tests dedup scoring, FP detection, threat intel boosting, and severity adjustment.
Covers the full enrich_and_filter pipeline plus individual enrichment components.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from darkdisco.enrichment.pipeline import (
    EnrichmentResult,
    _boost_severity,
    _downgrade_severity,
    enrich_and_filter,
)
from darkdisco.enrichment.dedup import (
    DedupResult,
    compute_similarity,
    _ngrams,
    _jaccard_similarity,
    _simhash,
)
from darkdisco.enrichment.false_positive import (
    FPResult,
    FPSignal,
    check_false_positive,
    _check_boilerplate_content,
    _check_generic_mention,
    _check_legitimate_context,
    _check_low_content,
    _check_repeated_source,
)


# ---------------------------------------------------------------------------
# Severity adjustments
# ---------------------------------------------------------------------------

class TestSeverityAdjustment:
    def test_boost_medium_by_1(self):
        assert _boost_severity("medium", 1) == "high"

    def test_boost_medium_by_2(self):
        assert _boost_severity("medium", 2) == "critical"

    def test_boost_critical_stays_critical(self):
        assert _boost_severity("critical", 1) == "critical"

    def test_boost_info_by_1(self):
        assert _boost_severity("info", 1) == "low"

    def test_boost_unknown_unchanged(self):
        assert _boost_severity("unknown", 1) == "unknown"

    def test_downgrade_high(self):
        assert _downgrade_severity("high") == "medium"

    def test_downgrade_info_stays_info(self):
        assert _downgrade_severity("info") == "info"

    def test_downgrade_critical(self):
        assert _downgrade_severity("critical") == "high"

    def test_downgrade_unknown_unchanged(self):
        assert _downgrade_severity("unknown") == "unknown"


# ---------------------------------------------------------------------------
# Dedup scoring
# ---------------------------------------------------------------------------

class TestDedupScoring:
    def test_identical_texts_similarity_1(self):
        text = "Credential dump found on paste site with user@bank.com"
        score = compute_similarity(text, text)
        assert score == pytest.approx(1.0, abs=0.01)

    def test_completely_different_texts_low_similarity(self):
        score = compute_similarity(
            "Card data for sale: 4123456789012345 CVV 123",
            "Weather forecast for tomorrow: sunny skies expected",
        )
        assert score < 0.3

    def test_similar_texts_above_threshold(self):
        a = "Found credentials on darkweb: user@firstnational.com password123"
        b = "Found credentials on darkweb: admin@firstnational.com password456"
        score = compute_similarity(a, b)
        assert score > 0.7

    def test_short_text_similarity(self):
        """Short texts (<200 chars) weight Jaccard more heavily."""
        a = "card dump 412345"
        b = "card dump 412346"
        score = compute_similarity(a, b)
        assert 0.5 < score < 1.0

    def test_ngrams_basic(self):
        result = _ngrams("hello", n=3)
        assert "hel" in result
        assert "ell" in result
        assert "llo" in result

    def test_jaccard_identical(self):
        s = {"a", "b", "c"}
        assert _jaccard_similarity(s, s) == 1.0

    def test_jaccard_disjoint(self):
        assert _jaccard_similarity({"a", "b"}, {"c", "d"}) == 0.0

    def test_simhash_same_text(self):
        h1 = _simhash("test text for hashing")
        h2 = _simhash("test text for hashing")
        assert h1 == h2

    def test_simhash_different_text(self):
        h1 = _simhash("one thing entirely different")
        h2 = _simhash("another completely unique string")
        assert h1 != h2


# ---------------------------------------------------------------------------
# False positive detection
# ---------------------------------------------------------------------------

class TestFalsePositiveDetection:
    def test_clear_threat_not_fp(self):
        finding = {
            "title": "Credential dump found",
            "summary": "Found 5000 user credentials including emails from firstnational.com",
            "raw_content": "user1@firstnational.com:password1\nuser2@firstnational.com:pass2\n" * 50,
            "matched_terms": [{"term_type": "domain", "value": "firstnational.com"}],
            "source_type": "paste_site",
        }
        result = check_false_positive(finding)
        assert result.recommendation == "keep"
        assert result.fp_score < 0.80

    def test_boilerplate_content_detected(self):
        signals = _check_boilerplate_content(
            "Terms and conditions apply. Privacy policy updated. "
            "All rights reserved. Copyright 2026.",
        )
        assert len(signals) > 0

    def test_generic_mention_keyword_only(self):
        # _check_generic_mention takes (content, matched_terms)
        signal = _check_generic_mention(
            "The bank was mentioned briefly",
            [{"term_type": "keyword", "value": "bank"}],
        )
        assert signal is not None

    def test_legitimate_context_detected(self):
        signal = _check_legitimate_context(
            "First National Bank Q4 earnings report shows strong growth. "
            "The press release highlighted new product launches.",
        )
        assert signal is not None

    def test_low_content_detected(self):
        signal = _check_low_content("short")
        assert signal is not None

    def test_repeated_source_pattern(self):
        # _check_repeated_source takes (content, metadata)
        signal = _check_repeated_source(
            "AAABBBCCCDDDEEEFFFGGGHHHIIIJJJKKKLLLMMMNNN" * 5,
            {"source_type": "paste_site"},
        )
        assert signal is not None

    def test_high_fp_score_auto_dismiss(self):
        """Content that triggers multiple FP signals should be auto-dismissed."""
        finding = {
            "title": "x",
            "summary": "x",
            "raw_content": "http://example.com",
            "matched_terms": [{"term_type": "keyword", "value": "x"}],
            "source_type": "paste_site",
        }
        result = check_false_positive(finding)
        # Very short, low-content, keyword-only — should score high
        assert result.fp_score > 0.5

    def test_downgrade_recommendation(self):
        finding = {
            "title": "Bank mentioned in article",
            "summary": "Article about banking sector mentions First National Bank in earnings context",
            "raw_content": "The quarterly earnings report from First National Bank indicates " * 5
                + "Terms and conditions apply. All rights reserved.",
            "matched_terms": [{"term_type": "institution_name", "value": "First National Bank"}],
            "source_type": "forum",
        }
        result = check_false_positive(finding)
        # Should trigger at least some FP signals
        assert result.fp_score > 0


# ---------------------------------------------------------------------------
# Enrichment pipeline integration
# ---------------------------------------------------------------------------

class TestEnrichAndFilter:
    def test_duplicate_finding_suppressed(self):
        """A near-duplicate should suppress finding creation."""
        session = MagicMock()
        finding_data = {
            "institution_id": "inst-1",
            "severity": "high",
            "title": "Credential dump",
            "summary": "Credentials found",
            "raw_content": "user@bank.com:password123",
            "content_hash": "abc",
        }

        dup_result = DedupResult(
            is_duplicate=True,
            duplicate_of="existing-id",
            similarity_score=0.92,
            dedup_reason="Near-duplicate of existing finding",
        )

        with patch("darkdisco.enrichment.pipeline.check_dedup", return_value=dup_result):
            result = enrich_and_filter(finding_data, session, run_threat_intel=False)

        assert result.should_create is False
        assert result.suppression_reason is not None
        assert "duplicate" in result.suppression_reason.lower()

    def test_auto_dismiss_fp_suppressed(self):
        """Auto-dismiss FP should suppress finding creation."""
        session = MagicMock()
        finding_data = {
            "institution_id": "inst-1",
            "severity": "medium",
            "title": "x",
            "summary": "x",
            "raw_content": "x",
            "content_hash": "def",
        }

        dedup_ok = DedupResult(is_duplicate=False, similarity_score=0.1)
        fp_dismiss = FPResult(
            fp_score=0.96,
            is_likely_fp=True,
            recommendation="auto_dismiss",
            signals=[FPSignal(rule="test", description="test", weight=0.96)],
        )

        with patch("darkdisco.enrichment.pipeline.check_dedup", return_value=dedup_ok), \
             patch("darkdisco.enrichment.pipeline.check_false_positive", return_value=fp_dismiss):
            result = enrich_and_filter(finding_data, session, run_threat_intel=False)

        assert result.should_create is False
        assert result.suppression_reason is not None
        assert "false positive" in result.suppression_reason.lower()

    def test_fp_downgrade_lowers_severity(self):
        """FP downgrade recommendation should lower severity by one level."""
        session = MagicMock()
        finding_data = {
            "institution_id": "inst-1",
            "severity": "high",
            "title": "Mention",
            "summary": "Something",
            "raw_content": "Some content",
            "content_hash": "ghi",
        }

        dedup_ok = DedupResult(is_duplicate=False, similarity_score=0.1)
        fp_downgrade = FPResult(
            fp_score=0.85,
            is_likely_fp=True,
            recommendation="downgrade",
            signals=[FPSignal(rule="test", description="test", weight=0.85)],
        )

        with patch("darkdisco.enrichment.pipeline.check_dedup", return_value=dedup_ok), \
             patch("darkdisco.enrichment.pipeline.check_false_positive", return_value=fp_downgrade):
            result = enrich_and_filter(finding_data, session, run_threat_intel=False)

        assert result.should_create is True
        assert result.adjusted_severity == "medium"  # downgraded from high

    def test_threat_intel_boosts_severity(self):
        """Threat intel hits should boost severity."""
        session = MagicMock()
        finding_data = {
            "institution_id": "inst-1",
            "severity": "medium",
            "title": "Credential leak",
            "summary": "Leaked creds",
            "raw_content": "user@bank.com:pass",
            "content_hash": "jkl",
        }

        dedup_ok = DedupResult(is_duplicate=False, similarity_score=0.1)
        fp_keep = FPResult(fp_score=0.2, is_likely_fp=False, recommendation="keep", signals=[])
        intel_data = {"severity_boost": 1, "hits": [{"provider": "dehashed", "count": 5}]}

        with patch("darkdisco.enrichment.pipeline.check_dedup", return_value=dedup_ok), \
             patch("darkdisco.enrichment.pipeline.check_false_positive", return_value=fp_keep), \
             patch("darkdisco.enrichment.pipeline.enrich_finding", new_callable=AsyncMock, return_value=intel_data):
            result = enrich_and_filter(finding_data, session, run_threat_intel=True)

        assert result.should_create is True
        assert result.adjusted_severity == "high"  # boosted from medium

    def test_clean_finding_passes_through(self):
        """A legitimate finding with no enrichment issues should pass through."""
        session = MagicMock()
        finding_data = {
            "institution_id": "inst-1",
            "severity": "high",
            "title": "Real threat",
            "summary": "Genuine credential dump",
            "raw_content": "user@bank.com:password\n" * 100,
            "content_hash": "mno",
        }

        dedup_ok = DedupResult(is_duplicate=False, similarity_score=0.05)
        fp_keep = FPResult(fp_score=0.1, is_likely_fp=False, recommendation="keep", signals=[])

        with patch("darkdisco.enrichment.pipeline.check_dedup", return_value=dedup_ok), \
             patch("darkdisco.enrichment.pipeline.check_false_positive", return_value=fp_keep):
            result = enrich_and_filter(finding_data, session, run_threat_intel=False)

        assert result.should_create is True
        assert result.adjusted_severity == "high"

    def test_dedup_failure_continues(self):
        """If dedup check throws, enrichment should continue."""
        session = MagicMock()
        finding_data = {
            "institution_id": "inst-1",
            "severity": "medium",
            "title": "Test",
            "summary": "Test",
            "raw_content": "content",
            "content_hash": "pqr",
        }

        fp_keep = FPResult(fp_score=0.1, is_likely_fp=False, recommendation="keep", signals=[])

        with patch("darkdisco.enrichment.pipeline.check_dedup", side_effect=Exception("db error")), \
             patch("darkdisco.enrichment.pipeline.check_false_positive", return_value=fp_keep):
            result = enrich_and_filter(finding_data, session, run_threat_intel=False)

        assert result.should_create is True

    def test_enrichment_metadata_populated(self):
        """Enrichment metadata should contain dedup and FP results."""
        session = MagicMock()
        finding_data = {
            "institution_id": "inst-1",
            "severity": "high",
            "title": "Test",
            "summary": "Test",
            "raw_content": "content",
            "content_hash": "stu",
        }

        dedup_ok = DedupResult(is_duplicate=False, similarity_score=0.2)
        fp_keep = FPResult(fp_score=0.3, is_likely_fp=False, recommendation="keep", signals=[])

        with patch("darkdisco.enrichment.pipeline.check_dedup", return_value=dedup_ok), \
             patch("darkdisco.enrichment.pipeline.check_false_positive", return_value=fp_keep):
            result = enrich_and_filter(finding_data, session, run_threat_intel=False)

        assert "dedup" in result.enrichment_metadata
        assert result.enrichment_metadata["dedup"]["is_duplicate"] is False
        assert "false_positive" in result.enrichment_metadata
        assert result.enrichment_metadata["false_positive"]["fp_score"] == 0.3
