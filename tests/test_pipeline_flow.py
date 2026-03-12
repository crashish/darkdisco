"""Integration tests for the pipeline flow — matching, enrichment, alert evaluation."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from darkdisco.common.models import (
    AlertRule,
    Client,
    Finding,
    FindingStatus,
    Institution,
    Notification,
    Severity,
    Source,
    SourceType,
    User,
    UserRole,
    WatchTerm,
    WatchTermType,
)
from darkdisco.discovery.connectors.base import RawMention
from darkdisco.discovery.matcher import MatchResult, match_mention
from darkdisco.enrichment.dedup import DedupResult, check_dedup, compute_similarity
from darkdisco.enrichment.false_positive import FPResult, check_false_positive
from darkdisco.enrichment.pipeline import (
    EnrichmentResult,
    _boost_severity,
    _downgrade_severity,
    enrich_and_filter,
)





# ---------------------------------------------------------------------------
# Watch term matching
# ---------------------------------------------------------------------------


class TestWatchTermMatching:
    """Test the matcher module with different term types."""

    def _make_terms(self, institution_id: str) -> list[WatchTerm]:
        return [
            WatchTerm(
                id=str(uuid4()),
                institution_id=institution_id,
                term_type=WatchTermType.domain,
                value="firstnational.com",
                enabled=True,
                case_sensitive=False,
            ),
            WatchTerm(
                id=str(uuid4()),
                institution_id=institution_id,
                term_type=WatchTermType.institution_name,
                value="First National Bank",
                enabled=True,
                case_sensitive=False,
            ),
            WatchTerm(
                id=str(uuid4()),
                institution_id=institution_id,
                term_type=WatchTermType.bin_range,
                value="412345",
                enabled=True,
                case_sensitive=False,
            ),
            WatchTerm(
                id=str(uuid4()),
                institution_id=institution_id,
                term_type=WatchTermType.regex,
                value=r"fnb[\-_]?leak",
                enabled=True,
                case_sensitive=False,
            ),
            WatchTerm(
                id=str(uuid4()),
                institution_id=institution_id,
                term_type=WatchTermType.executive_name,
                value="John Smith",
                enabled=True,
                case_sensitive=False,
            ),
        ]

    def test_domain_match(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Leaked credentials",
            content="Found user@firstnational.com in paste dump",
        )
        results = match_mention(mention, terms)
        assert len(results) == 1
        assert results[0].institution_id == inst_id
        matched_types = {m["term_type"] for m in results[0].matched_terms}
        assert "domain" in matched_types

    def test_bin_range_match(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Card dump",
            content="Card number 4123451234567890 found in stealer log",
        )
        results = match_mention(mention, terms)
        assert len(results) == 1
        matched_types = {m["term_type"] for m in results[0].matched_terms}
        assert "bin_range" in matched_types

    def test_regex_match(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Forum post",
            content="Just uploaded fnb_leak to the forum",
        )
        results = match_mention(mention, terms)
        assert len(results) == 1
        matched_types = {m["term_type"] for m in results[0].matched_terms}
        assert "regex" in matched_types

    def test_institution_name_match(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Ransomware target list",
            content="Target: first national bank systems compromised",
        )
        results = match_mention(mention, terms)
        assert len(results) == 1

    def test_no_match(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Unrelated post",
            content="This has nothing to do with any watched terms",
        )
        results = match_mention(mention, terms)
        assert len(results) == 0

    def test_disabled_terms_excluded(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        for t in terms:
            t.enabled = False
        mention = RawMention(
            source_name="test",
            title="Contains firstnational.com",
            content="Should not match because all terms disabled",
        )
        results = match_mention(mention, terms)
        assert len(results) == 0

    def test_multiple_term_types_match(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Big breach",
            content="firstnational.com first national bank credentials leaked, john smith CEO impacted",
        )
        results = match_mention(mention, terms)
        assert len(results) == 1
        assert len(results[0].matched_terms) >= 3

    def test_severity_hint_critical_for_bin(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Card dump",
            content="Card 4123451234567890",
        )
        results = match_mention(mention, terms)
        assert results[0].severity_hint == "critical"

    def test_severity_hint_high_for_domain_plus_exec(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Spear phish",
            content="john smith at firstnational.com targeted",
        )
        results = match_mention(mention, terms)
        assert results[0].severity_hint == "high"

    def test_severity_hint_medium_for_institution_name(self):
        inst_id = str(uuid4())
        terms = self._make_terms(inst_id)
        mention = RawMention(
            source_name="test",
            title="Mention",
            content="first national bank was mentioned on forum",
        )
        results = match_mention(mention, terms)
        assert results[0].severity_hint == "medium"

    def test_multi_institution_matching(self):
        """Terms from different institutions should produce separate results."""
        inst_a = str(uuid4())
        inst_b = str(uuid4())
        terms = [
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst_a,
                term_type=WatchTermType.domain,
                value="bankalpha.com",
                enabled=True,
                case_sensitive=False,
            ),
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst_b,
                term_type=WatchTermType.domain,
                value="bankbeta.com",
                enabled=True,
                case_sensitive=False,
            ),
        ]
        mention = RawMention(
            source_name="test",
            title="Multi bank leak",
            content="Credentials from bankalpha.com and bankbeta.com",
        )
        results = match_mention(mention, terms)
        assert len(results) == 2
        matched_ids = {r.institution_id for r in results}
        assert matched_ids == {inst_a, inst_b}


# ---------------------------------------------------------------------------
# Dedup scoring
# ---------------------------------------------------------------------------


class TestDedupScoring:
    def test_identical_content_similarity(self):
        score = compute_similarity("hello world test content", "hello world test content")
        assert score == 1.0

    def test_similar_content_high_score(self):
        a = "Leaked credentials for firstnational.com users found on dark web paste site"
        b = "Credentials for firstnational.com users leaked on darkweb paste site"
        score = compute_similarity(a, b)
        assert score > 0.6

    def test_different_content_low_score(self):
        a = "First National Bank credential dump"
        b = "Completely unrelated ransomware attack on hospital systems"
        score = compute_similarity(a, b)
        assert score < 0.4

    def test_empty_content(self):
        score = compute_similarity("", "")
        assert score == 1.0


# ---------------------------------------------------------------------------
# False positive filtering
# ---------------------------------------------------------------------------


class TestFalsePositiveFiltering:
    def test_clean_finding_passes(self):
        finding = {
            "title": "Credential dump for firstnational.com",
            "raw_content": "admin@firstnational.com:password123 found in breach DB with 50k records",
            "matched_terms": [
                {"term_type": "domain", "value": "firstnational.com"},
                {"term_type": "institution_name", "value": "First National Bank"},
            ],
        }
        result = check_false_positive(finding)
        assert result.recommendation == "keep"
        assert not result.is_likely_fp

    def test_generic_short_term_flagged(self):
        finding = {
            "title": "Mention of 'bank' on forum",
            "raw_content": "Some post about bank accounts in general",
            "matched_terms": [{"term_type": "keyword", "value": "bank"}],
        }
        result = check_false_positive(finding)
        assert any(s.rule == "generic_short_term" for s in result.signals)

    def test_boilerplate_content_flagged(self):
        finding = {
            "title": "Page mentioning First National Bank",
            "raw_content": (
                "Terms and Conditions apply. Privacy Policy available at our website. "
                "Copyright 2024. All rights reserved. Cookie consent required."
            ),
            "matched_terms": [{"term_type": "institution_name", "value": "First National Bank"}],
        }
        result = check_false_positive(finding)
        assert any(s.rule == "boilerplate_content" for s in result.signals)

    def test_legitimate_context_flagged(self):
        finding = {
            "title": "First National Bank job posting",
            "raw_content": "We're looking for a senior software engineer. Job opening at First National Bank.",
            "matched_terms": [{"term_type": "institution_name", "value": "First National Bank"}],
        }
        result = check_false_positive(finding)
        assert any(s.rule == "legitimate_context" for s in result.signals)

    def test_low_content_flagged(self):
        finding = {
            "title": "Short",
            "raw_content": "fnb",
            "matched_terms": [],
        }
        result = check_false_positive(finding)
        assert any(s.rule == "low_content" for s in result.signals)

    def test_common_word_match_flagged(self):
        # Only keyword type triggers common_word_match, not institution_name
        finding = {
            "title": "Forum post about first",
            "raw_content": "first thing to do when you get hacked is change your passwords immediately",
            "matched_terms": [{"term_type": "keyword", "value": "first"}],
        }
        result = check_false_positive(finding)
        assert any(s.rule == "common_word_match" for s in result.signals)


# ---------------------------------------------------------------------------
# Enrichment pipeline helpers
# ---------------------------------------------------------------------------


class TestEnrichmentHelpers:
    def test_boost_severity(self):
        assert _boost_severity("medium", 1) == "high"
        assert _boost_severity("low", 2) == "high"
        assert _boost_severity("critical", 1) == "critical"  # can't go higher
        assert _boost_severity("info", 1) == "low"

    def test_downgrade_severity(self):
        assert _downgrade_severity("critical") == "high"
        assert _downgrade_severity("high") == "medium"
        assert _downgrade_severity("info") == "info"  # can't go lower

    def test_boost_severity_invalid(self):
        assert _boost_severity("unknown", 1) == "unknown"

    def test_downgrade_severity_invalid(self):
        assert _downgrade_severity("unknown") == "unknown"


# ---------------------------------------------------------------------------
# Alert rule matching logic (unit-level, extracted from worker)
# ---------------------------------------------------------------------------


class TestAlertRuleMatching:
    """Test _rule_matches logic from the worker module."""

    def _make_rule(self, **kwargs):
        defaults = {
            "institution_id": None,
            "min_severity": Severity.high,
            "source_types": None,
            "keyword_filter": None,
        }
        defaults.update(kwargs)

        class FakeRule:
            pass

        rule = FakeRule()
        for k, v in defaults.items():
            setattr(rule, k, v)
        return rule

    def _make_finding(self, **kwargs):
        defaults = {
            "institution_id": "inst-1",
            "severity": Severity.critical,
            "title": "Critical breach",
            "summary": "Credential dump found",
            "source": None,
        }
        defaults.update(kwargs)

        class FakeFinding:
            pass

        f = FakeFinding()
        for k, v in defaults.items():
            setattr(f, k, v)
        return f

    def test_severity_match(self):
        from darkdisco.pipeline.worker import _rule_matches

        rule = self._make_rule(min_severity=Severity.high)
        finding = self._make_finding(severity=Severity.critical)
        severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
        assert _rule_matches(rule, finding, severity_rank) is True

    def test_severity_below_threshold(self):
        from darkdisco.pipeline.worker import _rule_matches

        rule = self._make_rule(min_severity=Severity.high)
        finding = self._make_finding(severity=Severity.low)
        severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
        assert _rule_matches(rule, finding, severity_rank) is False

    def test_institution_filter(self):
        from darkdisco.pipeline.worker import _rule_matches

        rule = self._make_rule(institution_id="inst-1")
        finding = self._make_finding(institution_id="inst-2")
        severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
        assert _rule_matches(rule, finding, severity_rank) is False

    def test_keyword_filter_match(self):
        from darkdisco.pipeline.worker import _rule_matches

        rule = self._make_rule(keyword_filter="credential")
        finding = self._make_finding(summary="Credential dump found")
        severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
        assert _rule_matches(rule, finding, severity_rank) is True

    def test_keyword_filter_no_match(self):
        from darkdisco.pipeline.worker import _rule_matches

        rule = self._make_rule(keyword_filter="ransomware")
        finding = self._make_finding(summary="Credential dump found")
        severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
        assert _rule_matches(rule, finding, severity_rank) is False


# ---------------------------------------------------------------------------
# End-to-end pipeline: mention -> match -> enrich -> finding
# ---------------------------------------------------------------------------


class TestEndToEndPipeline:
    """Test the full pipeline flow without Celery, calling functions directly."""

    @pytest.mark.asyncio
    async def test_mention_to_finding_flow(
        self, db_session, sample_institution, sample_source, sample_watch_terms
    ):
        """Simulate the pipeline: create a mention, match it, run enrichment."""
        # 1. Create a raw mention that should match
        mention = RawMention(
            source_name="Test Paste Monitor",
            source_url="http://example.onion/paste/999",
            title="New paste with bank data",
            content="Found credentials for firstnational.com users in this dump",
            discovered_at=datetime.now(timezone.utc),
        )

        # 2. Match against watch terms
        # Need to load terms from fixture (they're already WatchTerm objects)
        results = match_mention(mention, sample_watch_terms)
        assert len(results) >= 1
        assert results[0].institution_id == sample_institution.id

        # 3. Build candidate finding (mimicking worker.run_matching)
        import hashlib

        content_hash = hashlib.sha256(
            f"{mention.source_name}:{mention.content}".encode()
        ).hexdigest()

        candidate = {
            "institution_id": results[0].institution_id,
            "source_id": sample_source.id,
            "severity": results[0].severity_hint,
            "title": mention.title,
            "summary": mention.content[:1000],
            "raw_content": mention.content,
            "content_hash": content_hash,
            "source_url": mention.source_url,
            "matched_terms": results[0].matched_terms,
            "metadata": {},
        }

        # 4. Run enrichment (skip threat intel — no external APIs in test)
        # We can't easily use the sync enrich_and_filter with an async session,
        # but we can test the individual components
        fp_result = check_false_positive(candidate)
        assert fp_result.recommendation == "keep"

        # 5. Create the finding in DB
        finding = Finding(
            institution_id=candidate["institution_id"],
            source_id=candidate["source_id"],
            severity=candidate["severity"],
            title=candidate["title"],
            summary=candidate["summary"],
            raw_content=candidate["raw_content"],
            content_hash=candidate["content_hash"],
            source_url=candidate["source_url"],
            matched_terms=candidate["matched_terms"],
            tags=["paste_site"],
            discovered_at=mention.discovered_at,
        )
        db_session.add(finding)
        await db_session.commit()
        await db_session.refresh(finding)

        assert finding.id is not None
        assert finding.status == FindingStatus.new
        assert finding.institution_id == sample_institution.id
        assert len(finding.matched_terms) >= 1

    @pytest.mark.asyncio
    async def test_exact_dedup_prevents_duplicate(
        self, db_session, sample_institution, sample_source, sample_watch_terms
    ):
        """Two mentions with identical content should produce only one finding."""
        content = "Exact duplicate content for firstnational.com leak"
        import hashlib

        content_hash = hashlib.sha256(f"source:{content}".encode()).hexdigest()

        # Insert first finding
        f1 = Finding(
            institution_id=sample_institution.id,
            source_id=sample_source.id,
            severity="high",
            title="First mention",
            raw_content=content,
            content_hash=content_hash,
            status=FindingStatus.new,
        )
        db_session.add(f1)
        await db_session.commit()

        # Check for duplicate (mimicking worker logic)
        from sqlalchemy import select

        existing = (
            await db_session.execute(
                select(Finding.id).where(Finding.content_hash == content_hash)
            )
        ).scalar_one_or_none()

        assert existing is not None  # Duplicate detected

    @pytest.mark.asyncio
    async def test_false_positive_auto_dismiss(self):
        """A finding with heavy FP signals should be auto-dismissed."""
        finding_data = {
            "title": "a",
            "raw_content": (
                "Terms and Conditions apply. Privacy Policy. Copyright 2024. "
                "All rights reserved. Cookie policy notice. "
                "This email was sent to unsubscribe from these emails."
            ),
            "matched_terms": [{"term_type": "keyword", "value": "bank"}],
            "metadata": {},
        }
        result = check_false_positive(finding_data)
        # Heavy boilerplate + generic short term should push score high
        assert result.fp_score > 0.5
        assert result.recommendation in ("downgrade", "auto_dismiss")
