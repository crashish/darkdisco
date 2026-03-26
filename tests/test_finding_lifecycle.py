"""Finding lifecycle tests.

Tests status transitions, analyst notes, and audit log via the API.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from darkdisco.common.models import (
    Finding,
    FindingAuditLog,
    FindingStatus,
    Severity,
)


# ---------------------------------------------------------------------------
# Valid status transitions (parametrized)
# ---------------------------------------------------------------------------

_VALID_TRANSITIONS = {
    "new": ["reviewing", "confirmed", "dismissed", "false_positive"],
    "reviewing": ["escalated", "confirmed", "dismissed", "resolved", "false_positive"],
    "escalated": ["resolved", "confirmed", "dismissed", "false_positive", "reviewing"],
    "confirmed": ["resolved", "escalated", "dismissed", "false_positive", "reviewing"],
    "dismissed": ["reviewing", "confirmed", "false_positive", "resolved"],
    "resolved": ["reviewing", "confirmed", "false_positive", "dismissed"],
    "false_positive": ["reviewing", "confirmed", "dismissed"],
}

_ALL_STATUSES = list(FindingStatus)


def _valid_transition_params():
    params = []
    for from_status, to_list in _VALID_TRANSITIONS.items():
        for to_status in to_list:
            params.append((from_status, to_status))
    return params


def _invalid_transition_params():
    params = []
    all_status_values = {s.value for s in _ALL_STATUSES}
    for from_status, valid_to in _VALID_TRANSITIONS.items():
        invalid_to = all_status_values - set(valid_to) - {from_status}
        for to_status in sorted(invalid_to):
            params.append((from_status, to_status))
    return params


class TestStatusTransitions:
    @pytest.mark.parametrize("from_status,to_status", _valid_transition_params())
    async def test_valid_transitions(
        self, client, auth_headers, db_session, sample_institution, sample_source,
        from_status, to_status,
    ):
        """Every valid status transition should succeed via the API."""
        finding = Finding(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            source_id=sample_source.id,
            severity=Severity.high,
            status=FindingStatus(from_status),
            title="Test finding",
            summary="Test",
            raw_content="test content",
            content_hash=str(uuid4()),
        )
        db_session.add(finding)
        await db_session.commit()

        resp = await client.post(
            f"/api/findings/{finding.id}/transition",
            json={"new_status": to_status, "reason": "test transition"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == to_status

    @pytest.mark.parametrize("from_status,to_status", _invalid_transition_params()[:10])
    async def test_invalid_transitions_rejected(
        self, client, auth_headers, db_session, sample_institution, sample_source,
        from_status, to_status,
    ):
        """Invalid status transitions should return 400."""
        finding = Finding(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            source_id=sample_source.id,
            severity=Severity.high,
            status=FindingStatus(from_status),
            title="Test finding",
            summary="Test",
            raw_content="test content",
            content_hash=str(uuid4()),
        )
        db_session.add(finding)
        await db_session.commit()

        resp = await client.post(
            f"/api/findings/{finding.id}/transition",
            json={"new_status": to_status, "reason": "test"},
            headers=auth_headers,
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Analyst notes
# ---------------------------------------------------------------------------

class TestAnalystNotes:
    async def test_add_note_to_finding(
        self, client, auth_headers, sample_finding,
    ):
        resp = await client.post(
            f"/api/findings/{sample_finding.id}/note",
            json={"note": "Reviewed and confirmed as legitimate threat."},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "legitimate threat" in data.get("analyst_notes", "")

    async def test_add_multiple_notes(
        self, client, auth_headers, sample_finding,
    ):
        await client.post(
            f"/api/findings/{sample_finding.id}/note",
            json={"note": "First review note."},
            headers=auth_headers,
        )
        resp = await client.post(
            f"/api/findings/{sample_finding.id}/note",
            json={"note": "Second review note."},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        notes = resp.json().get("analyst_notes", "")
        assert "Second review note" in notes


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

class TestAuditLog:
    async def test_transition_creates_audit_entry(
        self, client, auth_headers, sample_finding,
    ):
        # Perform a status transition
        await client.post(
            f"/api/findings/{sample_finding.id}/transition",
            json={"new_status": "reviewing", "reason": "Starting review"},
            headers=auth_headers,
        )

        # Fetch audit log
        resp = await client.get(
            f"/api/findings/{sample_finding.id}/audit-log",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        entries = resp.json()
        assert len(entries) >= 1

        transition_entry = next(
            (e for e in entries if e.get("action") == "status_transition"),
            None,
        )
        assert transition_entry is not None
        assert transition_entry.get("old_value") == "new"
        assert transition_entry.get("new_value") == "reviewing"

    async def test_note_creates_audit_entry(
        self, client, auth_headers, sample_finding,
    ):
        await client.post(
            f"/api/findings/{sample_finding.id}/note",
            json={"note": "Audit test note"},
            headers=auth_headers,
        )

        resp = await client.get(
            f"/api/findings/{sample_finding.id}/audit-log",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        entries = resp.json()
        note_entry = next(
            (e for e in entries if "note" in e.get("action", "").lower()),
            None,
        )
        assert note_entry is not None

    async def test_audit_log_records_username(
        self, client, auth_headers, sample_finding,
    ):
        await client.post(
            f"/api/findings/{sample_finding.id}/transition",
            json={"new_status": "reviewing", "reason": "test"},
            headers=auth_headers,
        )

        resp = await client.get(
            f"/api/findings/{sample_finding.id}/audit-log",
            headers=auth_headers,
        )
        entries = resp.json()
        assert any(e.get("username") == "testanalyst" for e in entries)


# ---------------------------------------------------------------------------
# Finding CRUD
# ---------------------------------------------------------------------------

class TestFindingCRUD:
    async def test_create_finding(
        self, client, auth_headers, sample_institution, sample_source,
    ):
        resp = await client.post(
            "/api/findings",
            json={
                "institution_id": sample_institution.id,
                "source_id": sample_source.id,
                "severity": "high",
                "title": "New threat detected",
                "summary": "Found suspicious activity",
                "raw_content": "suspicious content here",
                "content_hash": str(uuid4()),
                "matched_terms": [{"term_type": "domain", "value": "test.com"}],
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["title"] == "New threat detected"
        assert data["status"] == "new"

    async def test_get_finding(self, client, auth_headers, sample_finding):
        resp = await client.get(
            f"/api/findings/{sample_finding.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["id"] == sample_finding.id

    async def test_update_finding(self, client, auth_headers, sample_finding):
        resp = await client.put(
            f"/api/findings/{sample_finding.id}",
            json={"severity": "critical", "classification": "Card Fraud"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["severity"] == "critical"

    async def test_list_findings_with_filters(
        self, client, auth_headers, sample_finding,
    ):
        resp = await client.get(
            "/api/findings",
            params={"severity": "high", "status": "new"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1

    async def test_search_findings(self, client, auth_headers, sample_finding):
        resp = await client.get(
            "/api/findings/search",
            params={"q": "credential"},
            headers=auth_headers,
        )
        assert resp.status_code == 200

    async def test_delete_finding(
        self, client, auth_headers, db_session, sample_institution, sample_source,
    ):
        finding = Finding(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            source_id=sample_source.id,
            severity=Severity.low,
            status=FindingStatus.new,
            title="To delete",
            summary="Delete me",
            content_hash=str(uuid4()),
        )
        db_session.add(finding)
        await db_session.commit()

        resp = await client.delete(
            f"/api/findings/{finding.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 204
