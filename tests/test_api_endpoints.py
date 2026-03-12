"""Integration tests for all API endpoints — CRUD, auth, filtering, status transitions."""

from __future__ import annotations

import pytest

from darkdisco.common.models import Notification

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Health & Auth
# ---------------------------------------------------------------------------


async def test_health_endpoint(client):
    resp = await client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_login_success(client, test_user):
    resp = await client.post("/api/auth/login", json={"username": "testanalyst", "password": "testpass123"})
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


async def test_login_bad_password(client, test_user):
    resp = await client.post("/api/auth/login", json={"username": "testanalyst", "password": "wrong"})
    assert resp.status_code == 401


async def test_login_nonexistent_user(client):
    resp = await client.post("/api/auth/login", json={"username": "nobody", "password": "x"})
    assert resp.status_code == 401


async def test_protected_route_no_token(client):
    resp = await client.get("/api/clients")
    assert resp.status_code == 401


async def test_protected_route_bad_token(client):
    resp = await client.get("/api/clients", headers={"Authorization": "Bearer invalid.token.here"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Clients CRUD
# ---------------------------------------------------------------------------


async def test_create_client(client, auth_headers):
    resp = await client.post(
        "/api/clients",
        json={"name": "Acme Bank", "contract_ref": "ACME-001"},
        headers=auth_headers,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Acme Bank"
    assert data["contract_ref"] == "ACME-001"
    assert data["active"] is True
    assert "id" in data


async def test_list_clients(client, auth_headers, sample_client):
    resp = await client.get("/api/clients", headers=auth_headers)
    assert resp.status_code == 200
    clients = resp.json()
    assert len(clients) >= 1
    assert any(c["id"] == sample_client.id for c in clients)


async def test_list_clients_filter_active(client, auth_headers, sample_client):
    resp = await client.get("/api/clients?active=true", headers=auth_headers)
    assert resp.status_code == 200
    assert all(c["active"] for c in resp.json())


async def test_get_client(client, auth_headers, sample_client):
    resp = await client.get(f"/api/clients/{sample_client.id}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["name"] == sample_client.name


async def test_get_client_not_found(client, auth_headers):
    resp = await client.get("/api/clients/nonexistent-id", headers=auth_headers)
    assert resp.status_code == 404


async def test_update_client(client, auth_headers, sample_client):
    resp = await client.put(
        f"/api/clients/{sample_client.id}",
        json={"name": "Updated Bank Corp"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "Updated Bank Corp"


async def test_delete_client(client, auth_headers, sample_client):
    resp = await client.delete(f"/api/clients/{sample_client.id}", headers=auth_headers)
    assert resp.status_code == 204

    resp = await client.get(f"/api/clients/{sample_client.id}", headers=auth_headers)
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Institutions CRUD
# ---------------------------------------------------------------------------


async def test_create_institution(client, auth_headers, sample_client):
    resp = await client.post(
        "/api/institutions",
        json={
            "client_id": sample_client.id,
            "name": "Second National Bank",
            "primary_domain": "secondnational.com",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Second National Bank"
    assert data["client_id"] == sample_client.id


async def test_create_institution_bad_client(client, auth_headers):
    resp = await client.post(
        "/api/institutions",
        json={"client_id": "nonexistent", "name": "Ghost Bank"},
        headers=auth_headers,
    )
    assert resp.status_code == 400


async def test_list_institutions(client, auth_headers, sample_institution):
    resp = await client.get("/api/institutions", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


async def test_list_institutions_filter_by_client(client, auth_headers, sample_institution, sample_client):
    resp = await client.get(f"/api/institutions?client_id={sample_client.id}", headers=auth_headers)
    assert resp.status_code == 200
    insts = resp.json()
    assert all(i["client_id"] == sample_client.id for i in insts)


async def test_get_institution(client, auth_headers, sample_institution):
    resp = await client.get(f"/api/institutions/{sample_institution.id}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["name"] == "First National Bank"


async def test_update_institution(client, auth_headers, sample_institution):
    resp = await client.put(
        f"/api/institutions/{sample_institution.id}",
        json={"short_name": "FNB-Updated"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["short_name"] == "FNB-Updated"


async def test_delete_institution(client, auth_headers, sample_institution):
    resp = await client.delete(f"/api/institutions/{sample_institution.id}", headers=auth_headers)
    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Watch Terms CRUD
# ---------------------------------------------------------------------------


async def test_create_watch_term(client, auth_headers, sample_institution):
    resp = await client.post(
        "/api/watch-terms",
        json={
            "institution_id": sample_institution.id,
            "term_type": "domain",
            "value": "fnb.com",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201
    assert resp.json()["value"] == "fnb.com"
    assert resp.json()["term_type"] == "domain"


async def test_create_watch_term_bad_institution(client, auth_headers):
    resp = await client.post(
        "/api/watch-terms",
        json={"institution_id": "nonexistent", "term_type": "domain", "value": "x.com"},
        headers=auth_headers,
    )
    assert resp.status_code == 400


async def test_list_watch_terms(client, auth_headers, sample_watch_terms):
    resp = await client.get("/api/watch-terms", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) >= len(sample_watch_terms)


async def test_list_watch_terms_filter_by_institution(
    client, auth_headers, sample_watch_terms, sample_institution
):
    resp = await client.get(
        f"/api/watch-terms?institution_id={sample_institution.id}",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert all(t["institution_id"] == sample_institution.id for t in resp.json())


async def test_update_watch_term(client, auth_headers, sample_watch_terms):
    term = sample_watch_terms[0]
    resp = await client.put(
        f"/api/watch-terms/{term.id}",
        json={"enabled": False},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["enabled"] is False


async def test_delete_watch_term(client, auth_headers, sample_watch_terms):
    term = sample_watch_terms[0]
    resp = await client.delete(f"/api/watch-terms/{term.id}", headers=auth_headers)
    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Sources CRUD
# ---------------------------------------------------------------------------


async def test_create_source(client, auth_headers):
    resp = await client.post(
        "/api/sources",
        json={
            "name": "BreachForums Monitor",
            "source_type": "forum",
            "enabled": True,
            "poll_interval_seconds": 600,
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "BreachForums Monitor"
    assert data["source_type"] == "forum"


async def test_list_sources(client, auth_headers, sample_source):
    resp = await client.get("/api/sources", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


async def test_list_sources_filter_by_type(client, auth_headers, sample_source):
    resp = await client.get("/api/sources?source_type=paste_site", headers=auth_headers)
    assert resp.status_code == 200
    assert all(s["source_type"] == "paste_site" for s in resp.json())


async def test_update_source(client, auth_headers, sample_source):
    resp = await client.put(
        f"/api/sources/{sample_source.id}",
        json={"poll_interval_seconds": 900},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["poll_interval_seconds"] == 900


async def test_delete_source(client, auth_headers, sample_source):
    resp = await client.delete(f"/api/sources/{sample_source.id}", headers=auth_headers)
    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Findings CRUD & Status Transitions
# ---------------------------------------------------------------------------


async def test_create_finding(client, auth_headers, sample_institution):
    resp = await client.post(
        "/api/findings",
        json={
            "institution_id": sample_institution.id,
            "title": "Test finding",
            "severity": "medium",
            "summary": "A test finding for integration testing",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["title"] == "Test finding"
    assert data["status"] == "new"
    assert data["severity"] == "medium"


async def test_create_finding_bad_institution(client, auth_headers):
    resp = await client.post(
        "/api/findings",
        json={"institution_id": "nonexistent", "title": "Bad finding"},
        headers=auth_headers,
    )
    assert resp.status_code == 400


async def test_list_findings(client, auth_headers, sample_finding):
    resp = await client.get("/api/findings", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


async def test_list_findings_filter_severity(client, auth_headers, sample_finding):
    resp = await client.get("/api/findings?severity=high", headers=auth_headers)
    assert resp.status_code == 200
    assert all(f["severity"] == "high" for f in resp.json())


async def test_list_findings_filter_status(client, auth_headers, sample_finding):
    resp = await client.get("/api/findings?status=new", headers=auth_headers)
    assert resp.status_code == 200
    assert all(f["status"] == "new" for f in resp.json())


async def test_list_findings_pagination(client, auth_headers, sample_finding):
    resp = await client.get("/api/findings?page=1&page_size=1", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) <= 1


async def test_search_findings(client, auth_headers, sample_finding):
    resp = await client.get("/api/findings/search?q=firstnational", headers=auth_headers)
    assert resp.status_code == 200
    results = resp.json()
    assert len(results) >= 1


async def test_search_findings_no_match(client, auth_headers, sample_finding):
    resp = await client.get("/api/findings/search?q=zzzznonexistent", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) == 0


async def test_get_finding(client, auth_headers, sample_finding):
    resp = await client.get(f"/api/findings/{sample_finding.id}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["title"] == sample_finding.title


async def test_update_finding(client, auth_headers, sample_finding):
    resp = await client.put(
        f"/api/findings/{sample_finding.id}",
        json={"analyst_notes": "Investigated — confirmed real"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["analyst_notes"] == "Investigated — confirmed real"


async def test_finding_status_transition_valid(client, auth_headers, sample_finding):
    """new -> reviewing is valid."""
    resp = await client.post(
        f"/api/findings/{sample_finding.id}/transition",
        json={"status": "reviewing", "notes": "Starting review"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "reviewing"
    assert "Starting review" in resp.json()["analyst_notes"]


async def test_finding_status_transition_invalid(client, auth_headers, sample_finding):
    """new -> resolved is not a valid transition."""
    resp = await client.post(
        f"/api/findings/{sample_finding.id}/transition",
        json={"status": "resolved"},
        headers=auth_headers,
    )
    assert resp.status_code == 409


async def test_finding_status_transition_chain(client, auth_headers, sample_finding):
    """Test full lifecycle: new -> reviewing -> escalated -> resolved."""
    finding_id = sample_finding.id

    # new -> reviewing
    resp = await client.post(
        f"/api/findings/{finding_id}/transition",
        json={"status": "reviewing"},
        headers=auth_headers,
    )
    assert resp.status_code == 200

    # reviewing -> escalated
    resp = await client.post(
        f"/api/findings/{finding_id}/transition",
        json={"status": "escalated", "notes": "Critical — escalating"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "escalated"

    # escalated -> resolved
    resp = await client.post(
        f"/api/findings/{finding_id}/transition",
        json={"status": "resolved", "notes": "Remediated"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "resolved"


async def test_finding_reopen(client, auth_headers, sample_finding):
    """Test reopen: new -> reviewing -> resolved -> reviewing."""
    fid = sample_finding.id
    await client.post(f"/api/findings/{fid}/transition", json={"status": "reviewing"}, headers=auth_headers)
    await client.post(f"/api/findings/{fid}/transition", json={"status": "resolved"}, headers=auth_headers)

    resp = await client.post(
        f"/api/findings/{fid}/transition",
        json={"status": "reviewing", "notes": "Reopening"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "reviewing"


async def test_delete_finding(client, auth_headers, sample_finding):
    resp = await client.delete(f"/api/findings/{sample_finding.id}", headers=auth_headers)
    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Dashboard Stats
# ---------------------------------------------------------------------------


async def test_dashboard_stats(client, auth_headers, sample_finding):
    resp = await client.get("/api/dashboard/stats", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_findings"] >= 1
    assert "by_severity" in data
    assert "by_status" in data
    assert "recent_findings" in data


async def test_dashboard_stats_filter_institution(client, auth_headers, sample_finding, sample_institution):
    resp = await client.get(
        f"/api/dashboard/stats?institution_id={sample_institution.id}",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["total_findings"] >= 1


# ---------------------------------------------------------------------------
# Alert Rules CRUD
# ---------------------------------------------------------------------------


async def test_create_alert_rule(client, auth_headers, test_user, sample_institution):
    user, _ = test_user
    resp = await client.post(
        "/api/alert-rules",
        json={
            "name": "Critical alerts for FNB",
            "owner_id": user.id,
            "institution_id": sample_institution.id,
            "min_severity": "critical",
            "notify_email": True,
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Critical alerts for FNB"
    assert data["min_severity"] == "critical"
    assert data["notify_email"] is True


async def test_list_alert_rules(client, auth_headers, sample_alert_rule):
    resp = await client.get("/api/alert-rules", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


async def test_update_alert_rule(client, auth_headers, sample_alert_rule):
    resp = await client.put(
        f"/api/alert-rules/{sample_alert_rule.id}",
        json={"min_severity": "medium"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["min_severity"] == "medium"


async def test_delete_alert_rule(client, auth_headers, sample_alert_rule):
    resp = await client.delete(f"/api/alert-rules/{sample_alert_rule.id}", headers=auth_headers)
    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Notifications CRUD
# ---------------------------------------------------------------------------


async def test_list_notifications_empty(client, auth_headers):
    resp = await client.get("/api/notifications", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json() == []


async def test_notification_lifecycle(client, auth_headers, db_session, test_user, sample_finding):
    """Create notification in DB, then read/mark via API."""
    user, _ = test_user
    notif = Notification(
        user_id=user.id,
        finding_id=sample_finding.id,
        title="Test notification",
        message="You have a new finding",
    )
    db_session.add(notif)
    await db_session.commit()
    await db_session.refresh(notif)

    # List
    resp = await client.get(f"/api/notifications?user_id={user.id}", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) >= 1

    # Get single
    resp = await client.get(f"/api/notifications/{notif.id}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["read"] is False

    # Mark read
    resp = await client.put(
        f"/api/notifications/{notif.id}/read",
        json={"read": True},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["read"] is True

    # Filter unread only (should be empty now)
    resp = await client.get(
        f"/api/notifications?user_id={user.id}&unread_only=true",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert len(resp.json()) == 0


async def test_mark_all_notifications_read(client, auth_headers, db_session, test_user):
    user, _ = test_user
    for i in range(3):
        db_session.add(Notification(user_id=user.id, title=f"Notif {i}", message="test"))
    await db_session.commit()

    resp = await client.post(
        f"/api/notifications/mark-all-read?user_id={user.id}",
        headers=auth_headers,
    )
    assert resp.status_code == 204

    resp = await client.get(
        f"/api/notifications?user_id={user.id}&unread_only=true",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert len(resp.json()) == 0
