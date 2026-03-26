"""Alert rules tests.

Tests trigger evaluation, notification dispatch, and the alert rules API.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from darkdisco.common.models import (
    AlertRule,
    Finding,
    FindingStatus,
    Notification,
    Severity,
    Source,
    SourceType,
)
from darkdisco.pipeline.worker import _rule_matches
from darkdisco.pipeline.notify import deliver_notification


# ---------------------------------------------------------------------------
# _rule_matches tests
# ---------------------------------------------------------------------------

SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}


def _make_finding(
    severity: str = "high",
    institution_id: str = "inst-1",
    title: str = "Test finding",
    summary: str = "Test summary",
    source_type: str | None = None,
) -> MagicMock:
    f = MagicMock()
    f.institution_id = institution_id
    f.severity = MagicMock()
    f.severity.value = severity
    f.title = title
    f.summary = summary
    if source_type:
        f.source = MagicMock()
        f.source.source_type = MagicMock()
        f.source.source_type.value = source_type
    else:
        f.source = None
    return f


def _make_rule(
    min_severity: str = "high",
    institution_id: str | None = None,
    source_types: list | None = None,
    keyword_filter: str | None = None,
    enabled: bool = True,
) -> MagicMock:
    r = MagicMock()
    r.institution_id = institution_id
    r.min_severity = MagicMock()
    r.min_severity.value = min_severity
    r.source_types = source_types
    r.keyword_filter = keyword_filter
    r.enabled = enabled
    return r


class TestRuleMatches:
    def test_matching_severity(self):
        rule = _make_rule(min_severity="high")
        finding = _make_finding(severity="critical")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is True

    def test_exact_severity_matches(self):
        rule = _make_rule(min_severity="high")
        finding = _make_finding(severity="high")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is True

    def test_lower_severity_rejected(self):
        rule = _make_rule(min_severity="high")
        finding = _make_finding(severity="medium")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is False

    def test_institution_filter_match(self):
        rule = _make_rule(institution_id="inst-1")
        finding = _make_finding(institution_id="inst-1")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is True

    def test_institution_filter_no_match(self):
        rule = _make_rule(institution_id="inst-2")
        finding = _make_finding(institution_id="inst-1")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is False

    def test_no_institution_filter_matches_all(self):
        rule = _make_rule(institution_id=None)
        finding = _make_finding(institution_id="any-inst")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is True

    def test_source_type_filter_match(self):
        rule = _make_rule(source_types=["paste_site", "forum"])
        finding = _make_finding(source_type="paste_site")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is True

    def test_source_type_filter_no_match(self):
        rule = _make_rule(source_types=["telegram"])
        finding = _make_finding(source_type="paste_site")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is False

    def test_keyword_filter_match(self):
        rule = _make_rule(keyword_filter="credential")
        finding = _make_finding(title="Credential dump found", summary="Contains leaked credentials")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is True

    def test_keyword_filter_no_match(self):
        rule = _make_rule(keyword_filter="ransomware")
        finding = _make_finding(title="Credential dump", summary="Passwords leaked")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is False

    def test_keyword_filter_case_insensitive(self):
        rule = _make_rule(keyword_filter="CREDENTIAL")
        finding = _make_finding(title="credential dump")
        assert _rule_matches(rule, finding, SEVERITY_RANK) is True


# ---------------------------------------------------------------------------
# Notification delivery
# ---------------------------------------------------------------------------

class TestNotificationDelivery:
    def test_no_channels_enabled(self):
        rule = MagicMock()
        rule.notify_email = False
        rule.notify_slack = False
        rule.notify_webhook_url = None
        result = deliver_notification(
            rule=rule, title="Test", message="Test msg", finding_id="f-1",
        )
        assert result == {}

    @patch("darkdisco.pipeline.notify._send_email", return_value=True)
    @patch("darkdisco.pipeline.notify._resolve_email", return_value="test@example.com")
    def test_email_channel(self, mock_resolve, mock_send):
        rule = MagicMock()
        rule.notify_email = True
        rule.notify_slack = False
        rule.notify_webhook_url = None
        rule.owner_id = "user-1"
        result = deliver_notification(
            rule=rule, title="Alert", message="Body", finding_id="f-1",
        )
        assert result["email"] is True
        mock_send.assert_called_once()

    @patch("darkdisco.pipeline.notify._send_slack", return_value=True)
    def test_slack_channel(self, mock_send):
        rule = MagicMock()
        rule.notify_email = False
        rule.notify_slack = True
        rule.notify_webhook_url = None
        result = deliver_notification(
            rule=rule, title="Alert", message="Body", finding_id="f-1",
        )
        assert result["slack"] is True

    @patch("darkdisco.pipeline.notify._send_webhook", return_value=True)
    def test_webhook_channel(self, mock_send):
        rule = MagicMock()
        rule.notify_email = False
        rule.notify_slack = False
        rule.notify_webhook_url = "https://hooks.example.com/alert"
        rule.id = "rule-1"
        result = deliver_notification(
            rule=rule, title="Alert", message="Body", finding_id="f-1",
        )
        assert result["webhook"] is True
        mock_send.assert_called_once()

    @patch("darkdisco.pipeline.notify._send_email", return_value=True)
    @patch("darkdisco.pipeline.notify._send_slack", return_value=True)
    @patch("darkdisco.pipeline.notify._send_webhook", return_value=True)
    @patch("darkdisco.pipeline.notify._resolve_email", return_value="test@example.com")
    def test_all_channels(self, mock_resolve, mock_webhook, mock_slack, mock_email):
        rule = MagicMock()
        rule.notify_email = True
        rule.notify_slack = True
        rule.notify_webhook_url = "https://hooks.example.com/alert"
        rule.owner_id = "user-1"
        rule.id = "rule-1"
        result = deliver_notification(
            rule=rule, title="Alert", message="Body", finding_id="f-1",
        )
        assert result["email"] is True
        assert result["slack"] is True
        assert result["webhook"] is True

    @patch("darkdisco.pipeline.notify._send_email", return_value=False)
    @patch("darkdisco.pipeline.notify._resolve_email", return_value="test@example.com")
    def test_email_failure_reported(self, mock_resolve, mock_send):
        rule = MagicMock()
        rule.notify_email = True
        rule.notify_slack = False
        rule.notify_webhook_url = None
        rule.owner_id = "user-1"
        result = deliver_notification(
            rule=rule, title="Alert", message="Body",
        )
        assert result["email"] is False


# ---------------------------------------------------------------------------
# Alert rules API tests
# ---------------------------------------------------------------------------

class TestAlertRulesAPI:
    async def test_create_alert_rule(self, client, auth_headers, test_user, sample_institution):
        user, _ = test_user
        resp = await client.post(
            "/api/alert-rules",
            json={
                "name": "Critical alerts for FNB",
                "institution_id": sample_institution.id,
                "min_severity": "critical",
                "enabled": True,
                "notify_email": True,
                "notify_slack": False,
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "Critical alerts for FNB"
        assert data["min_severity"] == "critical"

    async def test_list_alert_rules(self, client, auth_headers, sample_alert_rule):
        resp = await client.get("/api/alert-rules", headers=auth_headers)
        assert resp.status_code == 200
        rules = resp.json()
        assert len(rules) >= 1

    async def test_update_alert_rule(self, client, auth_headers, sample_alert_rule):
        resp = await client.put(
            f"/api/alert-rules/{sample_alert_rule.id}",
            json={"min_severity": "critical", "enabled": False},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["min_severity"] == "critical"
        assert data["enabled"] is False

    async def test_delete_alert_rule(self, client, auth_headers, sample_alert_rule):
        resp = await client.delete(
            f"/api/alert-rules/{sample_alert_rule.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 204

    async def test_notifications_listed(self, client, auth_headers, db_session, test_user, sample_alert_rule, sample_finding):
        user, _ = test_user
        # Create a notification
        notif = Notification(
            id=str(uuid4()),
            user_id=user.id,
            alert_rule_id=sample_alert_rule.id,
            finding_id=sample_finding.id,
            title="Test notification",
            message="Test message",
        )
        db_session.add(notif)
        await db_session.commit()

        resp = await client.get("/api/notifications", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1

    async def test_mark_notification_read(self, client, auth_headers, db_session, test_user, sample_alert_rule, sample_finding):
        user, _ = test_user
        notif = Notification(
            id=str(uuid4()),
            user_id=user.id,
            alert_rule_id=sample_alert_rule.id,
            finding_id=sample_finding.id,
            title="Unread notification",
            message="Mark me read",
        )
        db_session.add(notif)
        await db_session.commit()
        await db_session.refresh(notif)

        resp = await client.put(
            f"/api/notifications/{notif.id}/read",
            headers=auth_headers,
        )
        assert resp.status_code == 200

    async def test_mark_all_read(self, client, auth_headers, db_session, test_user, sample_alert_rule, sample_finding):
        user, _ = test_user
        for i in range(3):
            notif = Notification(
                id=str(uuid4()),
                user_id=user.id,
                alert_rule_id=sample_alert_rule.id,
                finding_id=sample_finding.id,
                title=f"Notification {i}",
                message=f"Message {i}",
            )
            db_session.add(notif)
        await db_session.commit()

        resp = await client.post(
            "/api/notifications/mark-all-read",
            headers=auth_headers,
        )
        assert resp.status_code == 200
