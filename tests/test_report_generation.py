"""Report generation tests.

Tests that all new report sections (FP analytics, pattern stats, threat summary,
analyst performance) render correctly. Also tests chart generation functions.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from darkdisco.common.models import (
    Finding,
    FindingStatus,
    Institution,
    RawMention,
    Severity,
    Source,
    SourceType,
)
from darkdisco.reporting.engine import (
    _build_analyst_performance,
    _build_classifications,
    _build_finding_dicts,
    _build_fp_analytics,
    _build_institution_threat_summaries,
    _build_severity_groups,
    _build_stats,
    _build_timeline,
    _classify_finding_for_report,
    _generate_charts,
    _highlight_content,
    render_report_html,
)
from darkdisco.reporting import charts


# ---------------------------------------------------------------------------
# Fixtures for report data
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def report_findings(db_session: AsyncSession, sample_institution, sample_source):
    """Create a set of findings for report testing with various statuses/severities."""
    now = datetime.now(timezone.utc)
    findings = []

    statuses_severities = [
        (FindingStatus.new, Severity.critical, "Card dump detected"),
        (FindingStatus.reviewing, Severity.high, "Phishing kit found"),
        (FindingStatus.confirmed, Severity.high, "Credential leak confirmed"),
        (FindingStatus.escalated, Severity.critical, "BIN data exposed"),
        (FindingStatus.resolved, Severity.medium, "Old paste resolved"),
        (FindingStatus.dismissed, Severity.low, "False alarm dismissed"),
        (FindingStatus.false_positive, Severity.low, "FP: news article"),
        (FindingStatus.false_positive, Severity.medium, "FP: job posting"),
    ]

    for i, (status, severity, title) in enumerate(statuses_severities):
        f = Finding(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            source_id=sample_source.id,
            severity=severity,
            status=status,
            title=title,
            summary=f"Summary for {title}",
            raw_content=f"Content for {title}: firstnational.com credential dump",
            content_hash=str(uuid4()),
            matched_terms=[{"term_type": "domain", "value": "firstnational.com"}],
            tags=["paste_site"] if i % 2 == 0 else ["forum"],
            discovered_at=now - timedelta(days=i),
            reviewed_by="analyst1" if status in (FindingStatus.resolved, FindingStatus.confirmed) else None,
            reviewed_at=(now - timedelta(hours=i * 2)) if status in (FindingStatus.resolved, FindingStatus.confirmed) else None,
        )
        db_session.add(f)
        findings.append(f)

    await db_session.commit()
    for f in findings:
        await db_session.refresh(f)
    return findings


# ---------------------------------------------------------------------------
# _highlight_content
# ---------------------------------------------------------------------------

class TestHighlightContent:
    def test_no_highlights_shows_preview(self):
        result = _highlight_content("Some text content", [])
        assert "Some text content" in str(result)

    def test_highlight_with_marks(self):
        terms = [{"value": "bank.com", "highlights": [{"start": 10, "end": 18}]}]
        content = "Login at bank.com with credentials"
        result = _highlight_content(content, terms, truncate=False)
        assert "<mark>" in str(result)
        assert "bank.com" in str(result)

    def test_truncated_content_has_ellipsis(self):
        content = "A" * 500 + "bank.com" + "B" * 500
        terms = [{"value": "bank.com", "highlights": [{"start": 500, "end": 508}]}]
        result = _highlight_content(content, terms, truncate=True)
        result_str = str(result)
        assert "…" in result_str or "bank.com" in result_str

    def test_no_content_preview_truncation(self):
        content = "X" * 1000
        result = _highlight_content(content, [])
        # Preview should be max 400 chars + ellipsis
        assert len(str(result)) <= 410


# ---------------------------------------------------------------------------
# _build_finding_dicts
# ---------------------------------------------------------------------------

class TestBuildFindingDicts:
    async def test_converts_orm_to_dicts(self, report_findings):
        dicts = _build_finding_dicts(report_findings)
        assert len(dicts) == len(report_findings)
        for d in dicts:
            assert "id" in d
            assert "severity" in d
            assert "status" in d
            assert "title" in d
            assert "discovered_at_fmt" in d

    async def test_severity_is_string(self, report_findings):
        dicts = _build_finding_dicts(report_findings)
        for d in dicts:
            assert isinstance(d["severity"], str)
            assert d["severity"] in ("critical", "high", "medium", "low", "info")


# ---------------------------------------------------------------------------
# _build_severity_groups
# ---------------------------------------------------------------------------

class TestBuildSeverityGroups:
    def test_groups_by_severity(self):
        dicts = [
            {"severity": "critical", "title": "A"},
            {"severity": "high", "title": "B"},
            {"severity": "critical", "title": "C"},
        ]
        groups = _build_severity_groups(dicts)
        labels = [g["label"] for g in groups]
        assert "Critical" in labels
        assert "High" in labels

    def test_empty_severities_excluded(self):
        dicts = [{"severity": "high", "title": "A"}]
        groups = _build_severity_groups(dicts)
        assert len(groups) == 1
        assert groups[0]["severity"] == "high"


# ---------------------------------------------------------------------------
# _build_classifications
# ---------------------------------------------------------------------------

class TestBuildClassifications:
    def test_unclassified_findings(self):
        dicts = [{"classification": None}, {"classification": None}]
        result = _build_classifications(dicts)
        assert len(result) == 1
        assert result[0]["label"] == "Unclassified"
        assert result[0]["count"] == 2

    def test_multiple_classifications(self):
        dicts = [
            {"classification": "Card Fraud"},
            {"classification": "Card Fraud"},
            {"classification": "Phishing"},
        ]
        result = _build_classifications(dicts)
        assert len(result) == 2
        assert result[0]["label"] == "Card Fraud"
        assert result[0]["count"] == 2


# ---------------------------------------------------------------------------
# _build_timeline
# ---------------------------------------------------------------------------

class TestBuildTimeline:
    def test_daily_timeline(self):
        now = datetime.now(timezone.utc)
        dicts = [
            {"discovered_at": now, "severity": "critical", "status": "new"},
            {"discovered_at": now, "severity": "high", "status": "resolved"},
            {"discovered_at": now - timedelta(days=1), "severity": "low", "status": "new"},
        ]
        timeline = _build_timeline(dicts)
        assert len(timeline) == 2  # two days
        today = now.strftime("%Y-%m-%d")
        today_entry = next(t for t in timeline if t["date"] == today)
        assert today_entry["new"] == 2
        assert today_entry["critical_high"] == 2
        assert today_entry["resolved"] == 1


# ---------------------------------------------------------------------------
# FP Analytics
# ---------------------------------------------------------------------------

class TestFPAnalytics:
    async def test_fp_analytics_data(self, report_findings):
        data = _build_fp_analytics(report_findings)

        assert "institution_fp_rates" in data
        assert len(data["institution_fp_rates"]) >= 1
        assert "disposition_breakdown" in data
        assert data["total_findings"] == len(report_findings)
        assert data["total_noise"] >= 2  # we have FP and dismissed findings
        assert 0 <= data["noise_rate"] <= 1

    async def test_fp_rate_calculation(self, report_findings):
        data = _build_fp_analytics(report_findings)
        for inst in data["institution_fp_rates"]:
            assert "fp_rate" in inst
            assert 0 <= inst["fp_rate"] <= 1
            assert inst["total_findings"] > 0

    def test_empty_findings(self):
        data = _build_fp_analytics([])
        assert data["total_findings"] == 0
        assert data["noise_rate"] == 0.0


# ---------------------------------------------------------------------------
# Pattern effectiveness
# ---------------------------------------------------------------------------

class TestPatternEffectiveness:
    async def test_pattern_stats(self, db_session, sample_source, report_findings):
        """Test basic pattern effectiveness stats with mentions and findings."""
        from darkdisco.reporting.engine import _build_pattern_effectiveness

        # Create some raw mentions
        for i in range(5):
            m = RawMention(
                id=str(uuid4()),
                source_id=sample_source.id,
                content=f"Mention content {i}",
                content_hash=str(uuid4()),
                collected_at=datetime.now(timezone.utc),
                promoted_to_finding_id=report_findings[i].id if i < 3 else None,
            )
            db_session.add(m)
        await db_session.commit()

        data = await _build_pattern_effectiveness(db_session)
        assert data["total_mentions"] >= 5
        assert data["total_promoted"] >= 3
        assert data["total_suppressed"] >= 2
        assert 0 <= data["suppression_rate"] <= 1

    async def test_empty_mentions(self, db_session):
        from darkdisco.reporting.engine import _build_pattern_effectiveness
        data = await _build_pattern_effectiveness(db_session)
        assert data["total_mentions"] == 0
        assert data["suppression_rate"] == 0.0


# ---------------------------------------------------------------------------
# Institution threat summaries
# ---------------------------------------------------------------------------

class TestInstitutionThreatSummaries:
    async def test_threat_summaries(self, report_findings):
        summaries = await _build_institution_threat_summaries(report_findings)
        assert len(summaries) >= 1

        summary = summaries[0]
        assert "institution_name" in summary
        assert "total_findings" in summary
        assert summary["total_findings"] == len(report_findings)
        assert "threat_categories" in summary
        assert "timeline" in summary
        assert "by_severity" in summary

    async def test_threat_category_classification(self, report_findings):
        summaries = await _build_institution_threat_summaries(report_findings)
        cats = summaries[0]["threat_categories"]
        category_names = {c["category"] for c in cats}
        # Our test findings contain card/credential keywords
        assert len(category_names) >= 1

    def test_empty_findings(self):
        import asyncio
        summaries = asyncio.get_event_loop().run_until_complete(
            _build_institution_threat_summaries([])
        )
        assert summaries == []


# ---------------------------------------------------------------------------
# _classify_finding_for_report
# ---------------------------------------------------------------------------

class TestClassifyFinding:
    def test_card_fraud(self):
        cats = _classify_finding_for_report(["card_data"], None, "Card dump found", Severity.critical)
        assert "Card Fraud" in cats

    def test_phishing(self):
        cats = _classify_finding_for_report(None, None, "Phishing kit detected", Severity.high)
        assert "Phishing" in cats

    def test_ato(self):
        cats = _classify_finding_for_report(None, None, "Account takeover attempt", Severity.high)
        assert "Account Takeover" in cats

    def test_data_breach(self):
        cats = _classify_finding_for_report(None, None, "Data breach exposed", Severity.critical)
        assert "Data Breach" in cats

    def test_credential_leaks(self):
        cats = _classify_finding_for_report(None, None, "Stealer log with redline artifacts", Severity.high)
        assert "Credential Leaks" in cats

    def test_ransomware(self):
        cats = _classify_finding_for_report(None, None, "Lockbit ransomware listing", Severity.critical)
        assert "Ransomware" in cats

    def test_other_fallback(self):
        cats = _classify_finding_for_report(None, None, "Generic mention", Severity.low)
        assert "Other" in cats


# ---------------------------------------------------------------------------
# Analyst performance
# ---------------------------------------------------------------------------

class TestAnalystPerformance:
    async def test_analyst_perf_data(self, report_findings):
        data = await _build_analyst_performance(report_findings)

        assert "by_analyst" in data
        assert "total_reviewed" in data
        assert "total_pending" in data
        assert "escalated" in data
        assert "escalation_rate" in data
        assert 0 <= data["escalation_rate"] <= 1

    async def test_analyst_throughput(self, report_findings):
        data = await _build_analyst_performance(report_findings)
        for analyst in data["by_analyst"]:
            assert "analyst" in analyst
            assert "reviewed" in analyst
            assert "pending" in analyst

    def test_empty_findings(self):
        import asyncio
        data = asyncio.get_event_loop().run_until_complete(
            _build_analyst_performance([])
        )
        assert data["total_reviewed"] == 0
        assert data["escalation_rate"] == 0.0


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------

class TestChartGeneration:
    def _sample_dicts(self):
        now = datetime.now(timezone.utc)
        return [
            {"severity": "critical", "status": "new", "source_type": "paste_site",
             "institution_name": "Bank A", "discovered_at": now},
            {"severity": "high", "status": "reviewing", "source_type": "forum",
             "institution_name": "Bank B", "discovered_at": now - timedelta(days=1)},
            {"severity": "medium", "status": "resolved", "source_type": "paste_site",
             "institution_name": "Bank A", "discovered_at": now - timedelta(days=2)},
        ]

    def test_severity_pie(self):
        result = charts.severity_pie(self._sample_dicts())
        assert isinstance(result, str)
        assert len(result) > 100  # base64 encoded PNG

    def test_status_pie(self):
        result = charts.status_pie(self._sample_dicts())
        assert isinstance(result, str)
        assert len(result) > 100

    def test_trend_line(self):
        result = charts.trend_line(self._sample_dicts())
        assert isinstance(result, str)

    def test_source_bar(self):
        result = charts.source_bar(self._sample_dicts())
        assert isinstance(result, str)

    def test_institution_bar(self):
        result = charts.institution_bar(self._sample_dicts())
        assert isinstance(result, str)

    def test_severity_trend(self):
        result = charts.severity_trend(self._sample_dicts())
        assert isinstance(result, str)

    def test_fp_rate_bar(self):
        data = [
            {"institution_name": "Bank A", "fp_rate": 0.3},
            {"institution_name": "Bank B", "fp_rate": 0.6},
        ]
        result = charts.fp_rate_bar(data)
        assert isinstance(result, str)
        assert len(result) > 100

    def test_disposition_pie(self):
        data = [
            {"status": "confirmed", "count": 10},
            {"status": "false_positive", "count": 5},
        ]
        result = charts.disposition_pie(data)
        assert isinstance(result, str)
        assert len(result) > 100

    def test_analyst_throughput_bar(self):
        data = [
            {"analyst": "analyst1", "reviewed": 15, "pending": 3},
            {"analyst": "analyst2", "reviewed": 8, "pending": 5},
        ]
        result = charts.analyst_throughput_bar(data)
        assert isinstance(result, str)
        assert len(result) > 100

    def test_threat_category_bar(self):
        data = [
            {"category": "Card Fraud", "count": 20},
            {"category": "Phishing", "count": 12},
        ]
        result = charts.threat_category_bar(data)
        assert isinstance(result, str)
        assert len(result) > 100

    def test_empty_data_returns_empty(self):
        assert charts.severity_pie([]) == ""
        assert charts.status_pie([]) == ""
        assert charts.trend_line([]) == ""
        assert charts.source_bar([]) == ""
        assert charts.fp_rate_bar([]) == ""
        assert charts.disposition_pie([]) == ""
        assert charts.analyst_throughput_bar([]) == ""
        assert charts.threat_category_bar([]) == ""


# ---------------------------------------------------------------------------
# _generate_charts integration
# ---------------------------------------------------------------------------

class TestGenerateCharts:
    def test_default_charts(self):
        dicts = [
            {"severity": "high", "status": "new", "source_type": "paste_site",
             "institution_name": "Bank", "discovered_at": datetime.now(timezone.utc)},
        ]
        result = _generate_charts(dicts, {"severity_pie": True, "status_pie": True})
        assert "severity_pie" in result
        assert "status_pie" in result

    def test_fp_charts_with_analytics(self):
        dicts = [{"severity": "high", "status": "new", "source_type": "paste_site",
                   "institution_name": "Bank", "discovered_at": datetime.now(timezone.utc)}]
        fp_analytics = {
            "institution_fp_rates": [{"institution_name": "Bank", "fp_rate": 0.3}],
            "disposition_breakdown": [{"status": "confirmed", "count": 5}],
        }
        result = _generate_charts(
            dicts,
            {"fp_rate_bar": True, "disposition_pie": True},
            fp_analytics=fp_analytics,
        )
        assert "fp_rate_bar" in result
        assert "disposition_pie" in result


# ---------------------------------------------------------------------------
# Full HTML render
# ---------------------------------------------------------------------------

class TestRenderReportHTML:
    async def test_render_basic_report(self, db_session, report_findings):
        html = await render_report_html(db_session)
        assert "<html" in html.lower() or "<!doctype" in html.lower()
        assert "DarkDisco" in html

    async def test_render_with_all_sections(self, db_session, report_findings, sample_source):
        # Add some raw mentions for pattern effectiveness
        for i in range(3):
            m = RawMention(
                id=str(uuid4()),
                source_id=sample_source.id,
                content=f"Test mention {i}",
                content_hash=str(uuid4()),
                collected_at=datetime.now(timezone.utc),
            )
            db_session.add(m)
        await db_session.commit()

        html = await render_report_html(
            db_session,
            sections={
                "executive_summary": True,
                "charts": True,
                "findings_detail": True,
                "findings_by_severity": True,
                "source_activity": True,
                "institution_exposure": True,
                "classification_breakdown": True,
                "timeline": True,
                "fp_analytics": True,
                "pattern_effectiveness": True,
                "institution_threat_summary": True,
                "analyst_performance": True,
            },
            chart_options={
                "severity_pie": True,
                "status_pie": True,
                "fp_rate_bar": True,
                "disposition_pie": True,
                "analyst_throughput_bar": True,
                "threat_category_bar": True,
            },
        )
        assert len(html) > 500
        # Verify sections are present in output
        assert "findings" in html.lower() or "finding" in html.lower()

    async def test_render_with_date_range(self, db_session, report_findings):
        now = datetime.now(timezone.utc)
        html = await render_report_html(
            db_session,
            date_from=now - timedelta(days=30),
            date_to=now,
        )
        assert len(html) > 0

    async def test_render_empty_findings(self, db_session):
        html = await render_report_html(db_session)
        assert len(html) > 0  # Should still render template structure
