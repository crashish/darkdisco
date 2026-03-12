"""Integration tests for enrichment modules — threat intel, dedup, FP filtering."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from darkdisco.enrichment.dedup import (
    SIMILARITY_THRESHOLD,
    DedupResult,
    _jaccard_similarity,
    _ngrams,
    _normalize_text,
    _simhash,
    compute_similarity,
)
from darkdisco.enrichment.false_positive import (
    FP_THRESHOLD,
    FPResult,
    _check_boilerplate_content,
    _check_generic_mention,
    _check_legitimate_context,
    _check_low_content,
    _check_repeated_source,
    check_false_positive,
)
from darkdisco.enrichment.pipeline import (
    EnrichmentResult,
    _boost_severity,
    _downgrade_severity,
)
from darkdisco.enrichment.threat_intel import extract_indicators


# ---------------------------------------------------------------------------
# Text normalization & similarity primitives
# ---------------------------------------------------------------------------


class TestTextNormalization:
    def test_normalize_lowercase(self):
        assert _normalize_text("Hello WORLD") == "hello world"

    def test_normalize_strip_punctuation(self):
        assert "hello" in _normalize_text("hello!!!")

    def test_normalize_collapse_whitespace(self):
        assert _normalize_text("hello   world") == "hello world"

    def test_ngrams_basic(self):
        grams = _ngrams("abcdef", 3)
        assert "abc" in grams
        assert "def" in grams
        assert len(grams) == 4

    def test_ngrams_short_text(self):
        grams = _ngrams("ab", 3)
        assert grams == {"ab"}

    def test_jaccard_identical(self):
        s = {"a", "b", "c"}
        assert _jaccard_similarity(s, s) == 1.0

    def test_jaccard_disjoint(self):
        assert _jaccard_similarity({"a", "b"}, {"c", "d"}) == 0.0

    def test_jaccard_partial(self):
        score = _jaccard_similarity({"a", "b", "c"}, {"b", "c", "d"})
        assert 0.0 < score < 1.0

    def test_jaccard_empty_sets(self):
        assert _jaccard_similarity(set(), set()) == 1.0

    def test_simhash_deterministic(self):
        h1 = _simhash("test content for hashing")
        h2 = _simhash("test content for hashing")
        assert h1 == h2

    def test_simhash_different_inputs(self):
        h1 = _simhash("first document about banking")
        h2 = _simhash("completely different content about cars")
        assert h1 != h2


class TestComputeSimilarity:
    def test_identical_texts(self):
        assert compute_similarity("hello world", "hello world") == 1.0

    def test_very_similar_texts(self):
        a = "credential dump for bank users found on paste site"
        b = "credential dump for bank users discovered on paste site"
        score = compute_similarity(a, b)
        assert score > 0.7

    def test_completely_different(self):
        a = "banking credential leak with sensitive data"
        b = "weather forecast for tomorrow sunny skies expected"
        score = compute_similarity(a, b)
        assert score < 0.3

    def test_long_texts_use_balanced_weights(self):
        # Long texts (>200 chars) use 0.5/0.5 weighting
        a = "x " * 150
        b = "x " * 150
        score = compute_similarity(a, b)
        assert score == 1.0


# ---------------------------------------------------------------------------
# False positive signal detectors
# ---------------------------------------------------------------------------


class TestFPSignalDetectors:
    def test_generic_mention_short_term(self):
        signal = _check_generic_mention("some content", [{"term_type": "keyword", "value": "abc"}])
        assert signal is not None
        assert signal.rule == "generic_short_term"

    def test_generic_mention_common_word(self):
        signal = _check_generic_mention("content", [{"term_type": "institution_name", "value": "first"}])
        assert signal is not None
        assert signal.rule == "common_word_match"

    def test_generic_mention_no_signal_for_specific(self):
        signal = _check_generic_mention("content", [{"term_type": "domain", "value": "bank.com"}])
        assert signal is None

    def test_boilerplate_detected(self):
        content = "Terms and Conditions apply. Privacy Policy. All rights reserved."
        signal = _check_boilerplate_content(content)
        assert signal is not None
        assert signal.rule == "boilerplate_content"

    def test_boilerplate_not_detected(self):
        content = "Found credential dump with 50k entries from banking systems"
        signal = _check_boilerplate_content(content)
        assert signal is None

    def test_legitimate_context_job_posting(self):
        content = "Job posting for senior developer at First National Bank"
        signal = _check_legitimate_context(content)
        assert signal is not None

    def test_legitimate_context_threat_content(self):
        content = "Leaked database with 100k credentials from banking portal"
        signal = _check_legitimate_context(content)
        assert signal is None

    def test_low_content_very_short(self):
        signal = _check_low_content("abc")
        assert signal is not None
        assert signal.rule == "low_content"

    def test_low_content_normal_length(self):
        signal = _check_low_content("This is a reasonably long piece of content that should pass the minimum length check easily")
        assert signal is None

    def test_repeated_source_auto_generated_paste(self):
        content = "a" * 200  # High alphanumeric ratio
        signal = _check_repeated_source(content, {"source_type": "paste_site"})
        assert signal is not None
        assert signal.rule == "auto_generated_paste"


# ---------------------------------------------------------------------------
# Indicator extraction for threat intel
# ---------------------------------------------------------------------------


class TestIndicatorExtraction:
    def test_extract_emails(self):
        finding = {
            "title": "Credential leak",
            "raw_content": "user@example.com and admin@bank.org found",
            "summary": "",
            "matched_terms": [],
        }
        indicators = extract_indicators(finding)
        assert "user@example.com" in indicators["emails"]
        assert "admin@bank.org" in indicators["emails"]

    def test_extract_domains(self):
        finding = {
            "title": "Found on darkweb.com",
            "raw_content": "Credentials from bank.example.com leaked",
            "summary": "",
            "matched_terms": [],
        }
        indicators = extract_indicators(finding)
        assert any("example.com" in d for d in indicators["domains"])

    def test_extract_keywords_from_matched_terms(self):
        finding = {
            "title": "",
            "raw_content": "",
            "summary": "",
            "matched_terms": [
                {"term_type": "keyword", "value": "firstnational"},
                {"term_type": "domain", "value": "fnb.com"},
            ],
        }
        indicators = extract_indicators(finding)
        assert "firstnational" in indicators["keywords"]
        assert "fnb.com" in indicators["keywords"]

    def test_extract_no_indicators(self):
        finding = {"title": "Simple text", "raw_content": "No emails or domains here", "summary": ""}
        indicators = extract_indicators(finding)
        assert len(indicators["emails"]) == 0


# ---------------------------------------------------------------------------
# Enrichment pipeline severity adjustments
# ---------------------------------------------------------------------------


class TestSeverityAdjustments:
    def test_boost_chain(self):
        # info -> low -> medium -> high -> critical
        assert _boost_severity("info", 4) == "critical"

    def test_downgrade_chain(self):
        s = "critical"
        for expected in ["high", "medium", "low", "info", "info"]:
            s = _downgrade_severity(s)
            assert s == expected

    def test_boost_zero_no_change(self):
        assert _boost_severity("medium", 0) == "medium"


# ---------------------------------------------------------------------------
# Full FP scoring
# ---------------------------------------------------------------------------


class TestFPScoring:
    def test_clean_content_low_score(self):
        result = check_false_positive({
            "title": "Leaked database dump",
            "raw_content": "50k credentials from banking portal with SSNs and account numbers",
            "matched_terms": [
                {"term_type": "domain", "value": "firstnational.com"},
                {"term_type": "bin_range", "value": "412345"},
            ],
        })
        assert result.fp_score < FP_THRESHOLD
        assert result.recommendation == "keep"

    def test_high_fp_score_auto_dismiss(self):
        result = check_false_positive({
            "title": "a",
            "raw_content": (
                "Terms and Conditions. Privacy Policy. Copyright 2024. "
                "All rights reserved. Cookie consent. "
                "This email was sent to unsubscribe from these emails. "
                "Press release. Annual report. Job posting. We're hiring."
            ),
            "matched_terms": [{"term_type": "keyword", "value": "x"}],
        })
        assert result.fp_score > 0.5

    def test_moderate_fp_signals_downgrade(self):
        result = check_false_positive({
            "title": "News about First National Bank",
            "raw_content": "Press release from the bank about quarterly earnings and new hiring.",
            "matched_terms": [{"term_type": "institution_name", "value": "First National Bank"}],
        })
        # Legitimate context signals should trigger
        assert any(s.rule == "legitimate_context" for s in result.signals)
