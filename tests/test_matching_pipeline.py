"""End-to-end matching pipeline tests.

Tests the full path: raw mention → watch term matching → fraud indicator check →
negative pattern suppression → finding creation. Verifies negative patterns suppress
spam, fraud indicators gate weak signals, and strong signals bypass filters.
"""

from __future__ import annotations

import re
from unittest.mock import patch
from uuid import uuid4

from darkdisco.common.models import WatchTerm, WatchTermType
from darkdisco.discovery.connectors.base import RawMention
from darkdisco.discovery.matcher import (
    MatchingFilters,
    WatchTermIndex,
    _find_all_spans,
    _find_all_substring,
    _severity_hint,
    _should_filter_as_noise,
    load_matching_filters,
    match_mention,
    recompute_highlights,
    reload_filters,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_watch_term(
    institution_id: str = "inst-1",
    term_type: WatchTermType = WatchTermType.domain,
    value: str = "firstnational.com",
    enabled: bool = True,
    case_sensitive: bool = False,
):
    """Create a mock WatchTerm-like object for matching tests."""
    from unittest.mock import MagicMock
    wt = MagicMock(spec=WatchTerm)
    wt.id = str(uuid4())
    wt.institution_id = institution_id
    wt.term_type = term_type
    wt.value = value
    wt.enabled = enabled
    wt.case_sensitive = case_sensitive
    return wt


def _make_mention(content: str, title: str = "") -> RawMention:
    return RawMention(
        source_name="test-paste",
        source_url="http://test.onion/paste/1",
        title=title,
        content=content,
        author=None,
        metadata={},
    )


FILTERS_WITH_INDICATORS = MatchingFilters(
    fraud_indicators=["cvv", "fullz", "dumps", "credential", "breach", "leak", "stealer"],
    negative_patterns=[
        re.compile(r"customer\s+service|how\s+do\s+i\s+contact", re.IGNORECASE),
        re.compile(r"job\s+(opening|posting|hiring)", re.IGNORECASE),
        re.compile(r"press\s+release|earnings\s+report", re.IGNORECASE),
    ],
)


# ---------------------------------------------------------------------------
# Strong signal types bypass noise filters
# ---------------------------------------------------------------------------

class TestStrongSignalsBypass:
    """BIN, routing number, domain, and regex matches bypass noise filters."""

    def test_bin_range_match_bypasses_filters(self):
        """A BIN range match should always produce a finding, no fraud indicator needed."""
        terms = [_make_watch_term(term_type=WatchTermType.bin_range, value="412345")]
        mention = _make_mention("Found card: 4123451234567890 on the forum")

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 1
        assert results[0].severity_hint == "critical"
        assert results[0].matched_terms[0]["term_type"] == "bin_range"

    def test_routing_number_bypasses_filters(self):
        terms = [_make_watch_term(term_type=WatchTermType.routing_number, value="021000021")]
        mention = _make_mention("Wire transfer instructions: routing 021000021 account 12345")

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 1
        assert results[0].severity_hint == "critical"

    def test_domain_match_bypasses_filters(self):
        terms = [_make_watch_term(term_type=WatchTermType.domain, value="firstnational.com")]
        mention = _make_mention("Login at firstnational.com to verify your account")

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 1
        assert results[0].matched_terms[0]["term_type"] == "domain"

    def test_regex_match_bypasses_filters(self):
        terms = [_make_watch_term(term_type=WatchTermType.regex, value=r"fnb[\-_]?leak")]
        mention = _make_mention("New file posted: fnb_leak_2026.zip")

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 1


# ---------------------------------------------------------------------------
# Weak signals gated by fraud indicators
# ---------------------------------------------------------------------------

class TestFraudIndicatorGating:
    """Weak signal types (institution_name, keyword) require fraud indicator co-occurrence."""

    def test_institution_name_with_fraud_indicator_passes(self):
        terms = [_make_watch_term(term_type=WatchTermType.institution_name, value="First National Bank")]
        mention = _make_mention(
            "First National Bank credential dump posted on dark web forum"
        )

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 1
        assert results[0].severity_hint == "medium"

    def test_institution_name_without_fraud_indicator_suppressed(self):
        terms = [_make_watch_term(term_type=WatchTermType.institution_name, value="First National Bank")]
        mention = _make_mention(
            "First National Bank is a great place to open a checking account"
        )

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 0

    def test_keyword_with_fraud_indicator_passes(self):
        terms = [_make_watch_term(term_type=WatchTermType.keyword, value="fnb")]
        mention = _make_mention("fnb fullz available, DM for price")

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 1

    def test_keyword_without_fraud_indicator_suppressed(self):
        terms = [_make_watch_term(term_type=WatchTermType.keyword, value="fnb")]
        mention = _make_mention("fnb has good interest rates this month")

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 0


# ---------------------------------------------------------------------------
# Negative pattern suppression
# ---------------------------------------------------------------------------

class TestNegativePatternSuppression:
    """Negative patterns suppress weak matches even with fraud indicators."""

    def test_customer_service_query_suppressed(self):
        terms = [_make_watch_term(term_type=WatchTermType.institution_name, value="First National Bank")]
        mention = _make_mention(
            "How do I contact First National Bank customer service about a breach notice?"
        )

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 0

    def test_job_posting_suppressed(self):
        terms = [_make_watch_term(term_type=WatchTermType.institution_name, value="First National Bank")]
        mention = _make_mention(
            "Job opening at First National Bank - cybersecurity analyst. "
            "Investigate credential theft and data breach incidents."
        )

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 0

    def test_press_release_suppressed(self):
        terms = [_make_watch_term(term_type=WatchTermType.institution_name, value="First National Bank")]
        mention = _make_mention(
            "Press release: First National Bank reports breach of 1000 records"
        )

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        assert len(results) == 0

    def test_negative_pattern_does_not_suppress_strong_signal(self):
        """Strong signal (domain) should NOT be suppressed by negative patterns."""
        terms = [_make_watch_term(term_type=WatchTermType.domain, value="firstnational.com")]
        mention = _make_mention(
            "Press release: phishing site at firstnational.com/login targeting customers"
        )

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)

        # Domain is a strong signal, bypasses negative patterns
        assert len(results) == 1


# ---------------------------------------------------------------------------
# Multi-institution and disabled terms
# ---------------------------------------------------------------------------

class TestMultiInstitutionMatching:
    def test_multiple_institutions_matched(self):
        terms = [
            _make_watch_term(institution_id="inst-1", term_type=WatchTermType.domain, value="bankone.com"),
            _make_watch_term(institution_id="inst-2", term_type=WatchTermType.domain, value="banktwo.com"),
        ]
        mention = _make_mention("Phishing kits for bankone.com and banktwo.com available")

        results = match_mention(mention, terms)
        institutions = {r.institution_id for r in results}
        assert institutions == {"inst-1", "inst-2"}

    def test_disabled_terms_ignored(self):
        terms = [
            _make_watch_term(term_type=WatchTermType.domain, value="firstnational.com", enabled=False),
        ]
        mention = _make_mention("Credentials for firstnational.com leaked")
        results = match_mention(mention, terms)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Severity hints
# ---------------------------------------------------------------------------

class TestSeverityHints:
    def test_bin_range_critical(self):
        assert _severity_hint([{"term_type": "bin_range", "value": "412345"}]) == "critical"

    def test_routing_number_critical(self):
        assert _severity_hint([{"term_type": "routing_number", "value": "021000021"}]) == "critical"

    def test_domain_and_executive_high(self):
        terms = [
            {"term_type": "domain", "value": "bank.com"},
            {"term_type": "executive_name", "value": "John Smith"},
        ]
        assert _severity_hint(terms) == "high"

    def test_institution_name_medium(self):
        assert _severity_hint([{"term_type": "institution_name", "value": "Bank"}]) == "medium"

    def test_keyword_only_low(self):
        assert _severity_hint([{"term_type": "keyword", "value": "test"}]) == "low"


# ---------------------------------------------------------------------------
# Highlight offset computation
# ---------------------------------------------------------------------------

class TestHighlights:
    def test_find_all_spans_regex(self):
        spans = _find_all_spans(r"test\d+", "test1 foo test22", flags=re.IGNORECASE)
        assert len(spans) == 2
        assert spans[0]["start"] == 0
        assert spans[0]["end"] == 5
        assert spans[1]["start"] == 10
        assert spans[1]["end"] == 16

    def test_find_all_substring(self):
        spans = _find_all_substring("hello", "hello world hello")
        assert len(spans) == 2

    def test_recompute_highlights_basic(self):
        terms = [{"term_type": "domain", "value": "bank.com"}]
        content = "Login at bank.com with your credentials"
        result = recompute_highlights(terms, content)
        assert len(result) == 1
        assert "highlights" in result[0]
        assert result[0]["highlights"][0]["start"] == 9
        assert result[0]["highlights"][0]["end"] == 17

    def test_recompute_highlights_no_match(self):
        terms = [{"term_type": "domain", "value": "other.com"}]
        content = "No matching content here"
        result = recompute_highlights(terms, content)
        assert "highlights" not in result[0]


# ---------------------------------------------------------------------------
# Noise filter logic (unit level)
# ---------------------------------------------------------------------------

class TestNoiseFilterLogic:
    def test_strong_signal_not_filtered(self):
        matched = [{"term_type": "domain", "value": "bank.com"}]
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            assert _should_filter_as_noise(matched, "bank.com is down") is False

    def test_weak_signal_no_indicator_filtered(self):
        matched = [{"term_type": "institution_name", "value": "Bank Corp"}]
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            assert _should_filter_as_noise(matched, "bank corp has great rates") is True

    def test_weak_signal_with_indicator_not_filtered(self):
        matched = [{"term_type": "institution_name", "value": "Bank Corp"}]
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            assert _should_filter_as_noise(matched, "bank corp cvv dump for sale") is False

    def test_weak_signal_negative_pattern_filtered(self):
        matched = [{"term_type": "institution_name", "value": "Bank Corp"}]
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            assert _should_filter_as_noise(matched, "job opening at bank corp for breach analyst") is True

    def test_no_filters_loaded_passes_through(self):
        matched = [{"term_type": "institution_name", "value": "Bank"}]
        empty_filters = MatchingFilters()
        with patch("darkdisco.discovery.matcher._get_filters", return_value=empty_filters):
            assert _should_filter_as_noise(matched, "bank mentioned casually") is False


# ---------------------------------------------------------------------------
# Filter loading
# ---------------------------------------------------------------------------

class TestFilterLoading:
    def test_load_from_yaml_file(self):
        filters = load_matching_filters()
        # The project has data/matching_filters.yaml with real data
        assert len(filters.fraud_indicators) > 0
        assert len(filters.negative_patterns) > 0

    def test_load_missing_file_returns_empty(self, tmp_path):
        with patch("darkdisco.discovery.matcher._resolve_filter_path", return_value=tmp_path / "nonexistent.yaml"):
            filters = load_matching_filters()
        assert filters.fraud_indicators == []
        assert filters.negative_patterns == []

    def test_load_malformed_yaml_returns_empty(self, tmp_path):
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("{{{{invalid yaml")
        with patch("darkdisco.discovery.matcher._resolve_filter_path", return_value=bad_file):
            filters = load_matching_filters()
        assert filters.fraud_indicators == []
        assert filters.negative_patterns == []

    def test_reload_filters(self):
        filters = reload_filters()
        assert isinstance(filters, MatchingFilters)


# ---------------------------------------------------------------------------
# Match type coverage: all WatchTermType values
# ---------------------------------------------------------------------------

class TestAllTermTypes:
    def test_domain_match(self):
        terms = [_make_watch_term(term_type=WatchTermType.domain, value="example.com")]
        mention = _make_mention("visit example.com/login")
        results = match_mention(mention, terms)
        assert len(results) == 1

    def test_bin_range_match(self):
        terms = [_make_watch_term(term_type=WatchTermType.bin_range, value="412345")]
        mention = _make_mention("Card: 41234567890123")
        results = match_mention(mention, terms)
        assert len(results) == 1

    def test_regex_match(self):
        terms = [_make_watch_term(term_type=WatchTermType.regex, value=r"fnb[\-_]leak")]
        mention = _make_mention("Download fnb-leak archive")
        results = match_mention(mention, terms)
        assert len(results) == 1

    def test_institution_name_match_with_indicator(self):
        terms = [_make_watch_term(term_type=WatchTermType.institution_name, value="Acme Bank")]
        mention = _make_mention("Acme Bank credential dump available")
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)
        assert len(results) == 1

    def test_keyword_match_with_indicator(self):
        terms = [_make_watch_term(term_type=WatchTermType.keyword, value="acmebank")]
        mention = _make_mention("acmebank fullz for sale")
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)
        assert len(results) == 1

    def test_executive_name_match_with_indicator(self):
        terms = [_make_watch_term(term_type=WatchTermType.executive_name, value="john smith")]
        mention = _make_mention("john smith credentials leaked from breach")
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = match_mention(mention, terms)
        assert len(results) == 1


# ---------------------------------------------------------------------------
# WatchTermIndex (pre-compiled batch matching)
# ---------------------------------------------------------------------------

class TestWatchTermIndex:
    """WatchTermIndex should produce identical results to match_mention."""

    def test_index_matches_same_as_match_mention(self):
        """Index-based matching must agree with the original function."""
        terms = [
            _make_watch_term(institution_id="inst-1", term_type=WatchTermType.domain, value="bankone.com"),
            _make_watch_term(institution_id="inst-2", term_type=WatchTermType.bin_range, value="412345"),
            _make_watch_term(institution_id="inst-3", term_type=WatchTermType.keyword, value="fnb"),
        ]
        mention = _make_mention("bankone.com phishing kit and card 41234567890123 plus fnb fullz")

        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            original = match_mention(mention, terms)
            index = WatchTermIndex(terms)
            indexed = index.match(mention)

        original_insts = {r.institution_id for r in original}
        indexed_insts = {r.institution_id for r in indexed}
        assert original_insts == indexed_insts

    def test_index_empty_terms(self):
        index = WatchTermIndex([])
        mention = _make_mention("anything here")
        assert index.match(mention) == []
        assert index.term_count == 0

    def test_index_disabled_terms_excluded(self):
        terms = [
            _make_watch_term(term_type=WatchTermType.domain, value="bank.com", enabled=False),
        ]
        index = WatchTermIndex(terms)
        assert index.term_count == 0

    def test_index_all_term_types(self):
        """Verify the index handles every WatchTermType."""
        terms = [
            _make_watch_term(institution_id="i1", term_type=WatchTermType.domain, value="example.com"),
            _make_watch_term(institution_id="i2", term_type=WatchTermType.bin_range, value="412345"),
            _make_watch_term(institution_id="i3", term_type=WatchTermType.regex, value=r"fnb[\-_]leak"),
            _make_watch_term(institution_id="i4", term_type=WatchTermType.institution_name, value="Acme Bank"),
            _make_watch_term(institution_id="i5", term_type=WatchTermType.keyword, value="acmebank"),
            _make_watch_term(institution_id="i6", term_type=WatchTermType.executive_name, value="john smith"),
            _make_watch_term(institution_id="i7", term_type=WatchTermType.routing_number, value="021000021"),
        ]
        index = WatchTermIndex(terms)
        assert index.term_count == 7
        assert index.institution_count == 7

        mention = _make_mention(
            "visit example.com, card 41234567890123, fnb_leak archive, "
            "Acme Bank acmebank john smith 021000021 credential dump"
        )
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            results = index.match(mention)

        matched_insts = {r.institution_id for r in results}
        # All institutions should match
        assert matched_insts == {"i1", "i2", "i3", "i4", "i5", "i6", "i7"}

    def test_index_noise_filtering_applied(self):
        """Weak signals without fraud indicators should be filtered by the index too."""
        terms = [_make_watch_term(term_type=WatchTermType.institution_name, value="First National Bank")]
        mention = _make_mention("First National Bank has great rates")
        with patch("darkdisco.discovery.matcher._get_filters", return_value=FILTERS_WITH_INDICATORS):
            index = WatchTermIndex(terms)
            results = index.match(mention)
        assert len(results) == 0

    def test_index_invalid_regex_skipped(self):
        """Invalid regex terms should be skipped without crashing."""
        terms = [
            _make_watch_term(term_type=WatchTermType.regex, value="[invalid"),
            _make_watch_term(term_type=WatchTermType.domain, value="good.com"),
        ]
        index = WatchTermIndex(terms)
        assert index.term_count == 1  # only the valid domain term
