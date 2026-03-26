"""API CRUD endpoint tests.

Tests all CRUD endpoints: institution management, watch terms, sources,
matching filters, analytics/disposition, BIN lookup, and pipeline dry-run.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from darkdisco.common.models import (
    BINRecord,
    CardBrand,
    CardType,
    Client,
    Finding,
    FindingStatus,
    Institution,
    Severity,
    Source,
    SourceType,
    WatchTerm,
    WatchTermType,
)


# ---------------------------------------------------------------------------
# Institution management
# ---------------------------------------------------------------------------

class TestInstitutionAPI:
    async def test_create_institution(self, client, auth_headers, sample_client):
        resp = await client.post(
            "/api/institutions",
            json={
                "client_id": sample_client.id,
                "name": "Second National Bank",
                "short_name": "SNB",
                "primary_domain": "secondnational.com",
                "bin_ranges": ["512345"],
                "routing_numbers": ["021000022"],
                "active": True,
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "Second National Bank"
        assert data["primary_domain"] == "secondnational.com"

    async def test_list_institutions(self, client, auth_headers, sample_institution):
        resp = await client.get("/api/institutions", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1

    async def test_get_institution(self, client, auth_headers, sample_institution):
        resp = await client.get(
            f"/api/institutions/{sample_institution.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["id"] == sample_institution.id

    async def test_update_institution(self, client, auth_headers, sample_institution):
        resp = await client.put(
            f"/api/institutions/{sample_institution.id}",
            json={"short_name": "FNB-Updated"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["short_name"] == "FNB-Updated"

    async def test_delete_institution(self, client, auth_headers, db_session, sample_client):
        inst = Institution(
            id=str(uuid4()),
            client_id=sample_client.id,
            name="Temp Bank",
            primary_domain="temp.com",
            active=True,
        )
        db_session.add(inst)
        await db_session.commit()

        resp = await client.delete(
            f"/api/institutions/{inst.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 204

    async def test_get_nonexistent_institution(self, client, auth_headers):
        resp = await client.get(
            f"/api/institutions/{uuid4()}",
            headers=auth_headers,
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Watch terms
# ---------------------------------------------------------------------------

class TestWatchTermsAPI:
    async def test_create_watch_term(self, client, auth_headers, sample_institution):
        resp = await client.post(
            "/api/watch-terms",
            json={
                "institution_id": sample_institution.id,
                "term_type": "domain",
                "value": "testbank.com",
                "enabled": True,
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["value"] == "testbank.com"
        assert data["term_type"] == "domain"

    async def test_list_watch_terms(self, client, auth_headers, sample_watch_terms):
        resp = await client.get("/api/watch-terms", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= len(sample_watch_terms)

    async def test_update_watch_term(self, client, auth_headers, sample_watch_terms):
        term = sample_watch_terms[0]
        resp = await client.put(
            f"/api/watch-terms/{term.id}",
            json={"enabled": False},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["enabled"] is False

    async def test_delete_watch_term(self, client, auth_headers, sample_watch_terms):
        term = sample_watch_terms[0]
        resp = await client.delete(
            f"/api/watch-terms/{term.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 204

    async def test_create_all_term_types(self, client, auth_headers, sample_institution):
        for term_type in ["domain", "institution_name", "bin_range", "keyword", "regex", "executive_name", "routing_number"]:
            resp = await client.post(
                "/api/watch-terms",
                json={
                    "institution_id": sample_institution.id,
                    "term_type": term_type,
                    "value": f"test_{term_type}",
                    "enabled": True,
                },
                headers=auth_headers,
            )
            assert resp.status_code == 201, f"Failed to create {term_type} watch term"


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

class TestSourcesAPI:
    async def test_create_source(self, client, auth_headers):
        resp = await client.post(
            "/api/sources",
            json={
                "name": "New Paste Monitor",
                "source_type": "paste_site",
                "enabled": True,
                "poll_interval_seconds": 600,
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "New Paste Monitor"

    async def test_list_sources(self, client, auth_headers, sample_source):
        resp = await client.get("/api/sources", headers=auth_headers)
        assert resp.status_code == 200
        assert len(resp.json()) >= 1

    async def test_update_source(self, client, auth_headers, sample_source):
        resp = await client.put(
            f"/api/sources/{sample_source.id}",
            json={"enabled": False},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["enabled"] is False

    async def test_delete_source(self, client, auth_headers, db_session):
        src = Source(
            id=str(uuid4()),
            name="Temp Source",
            source_type=SourceType.forum,
            enabled=True,
            poll_interval_seconds=300,
        )
        db_session.add(src)
        await db_session.commit()

        resp = await client.delete(
            f"/api/sources/{src.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

class TestClientsAPI:
    async def test_create_client(self, client, auth_headers):
        resp = await client.post(
            "/api/clients",
            json={
                "name": "New Client Corp",
                "contract_ref": "NC-2026-001",
                "active": True,
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201

    async def test_list_clients(self, client, auth_headers, sample_client):
        resp = await client.get("/api/clients", headers=auth_headers)
        assert resp.status_code == 200
        assert len(resp.json()) >= 1

    async def test_get_client(self, client, auth_headers, sample_client):
        resp = await client.get(
            f"/api/clients/{sample_client.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 200

    async def test_update_client(self, client, auth_headers, sample_client):
        resp = await client.put(
            f"/api/clients/{sample_client.id}",
            json={"name": "Updated Bank Corp"},
            headers=auth_headers,
        )
        assert resp.status_code == 200

    async def test_delete_client(self, client, auth_headers, db_session):
        c = Client(id=str(uuid4()), name="Temp Client", active=True)
        db_session.add(c)
        await db_session.commit()

        resp = await client.delete(
            f"/api/clients/{c.id}",
            headers=auth_headers,
        )
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class TestDashboardAPI:
    async def test_dashboard_stats(self, client, auth_headers, sample_finding):
        resp = await client.get("/api/dashboard/stats", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "total_findings" in data


# ---------------------------------------------------------------------------
# Mentions
# ---------------------------------------------------------------------------

class TestMentionsAPI:
    async def test_list_mentions(self, client, auth_headers):
        resp = await client.get("/api/mentions", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data or isinstance(data, list) or "total" in data


# ---------------------------------------------------------------------------
# Analytics / Disposition
# ---------------------------------------------------------------------------

class TestAnalyticsAPI:
    async def test_disposition_analytics(self, client, auth_headers, sample_finding):
        resp = await client.get("/api/analytics/disposition", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        # Should return analytics structure
        assert isinstance(data, dict)

    async def test_institution_threat_summary(self, client, auth_headers, sample_institution, sample_finding):
        resp = await client.get(
            f"/api/institutions/{sample_institution.id}/threat-summary",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# BIN lookup
# ---------------------------------------------------------------------------

class TestBINLookupAPI:
    async def test_bin_lookup(self, client, auth_headers, db_session):
        # Create a BIN record
        record = BINRecord(
            id=str(uuid4()),
            bin_prefix="412345",
            issuer_name="First National Bank",
            card_brand=CardBrand.visa,
            card_type=CardType.credit,
            country_code="US",
            country_name="United States",
        )
        db_session.add(record)
        await db_session.commit()

        resp = await client.get(
            "/api/bins/lookup",
            params={"bin": "412345"},
            headers=auth_headers,
        )
        assert resp.status_code == 200

    async def test_bin_stats(self, client, auth_headers):
        resp = await client.get("/api/bins/stats", headers=auth_headers)
        assert resp.status_code == 200

    async def test_bin_list(self, client, auth_headers):
        resp = await client.get("/api/bins", headers=auth_headers)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

class TestHealthAPI:
    async def test_health_no_auth(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200

    async def test_auth_login(self, client, test_user):
        user, _ = test_user
        resp = await client.post(
            "/api/auth/login",
            json={"username": "testanalyst", "password": "testpass123"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data

    async def test_auth_invalid_password(self, client, test_user):
        resp = await client.post(
            "/api/auth/login",
            json={"username": "testanalyst", "password": "wrongpassword"},
        )
        assert resp.status_code in (401, 403)


# ---------------------------------------------------------------------------
# Integration: domain match
# ---------------------------------------------------------------------------

class TestIntegrationEndpoints:
    async def test_domain_match(self, client, sample_institution):
        resp = await client.get(
            "/api/integration/match-domain",
            params={"domain": "firstnational.com"},
        )
        assert resp.status_code == 200

    async def test_institution_domains_export(self, client, sample_institution):
        resp = await client.get("/api/integration/institutions/domains")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Matching filters API
# ---------------------------------------------------------------------------

class TestMatchingFiltersAPI:
    async def test_get_matching_filters(self, client, auth_headers):
        resp = await client.get("/api/matching-filters", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "fraud_indicators" in data
        assert "negative_patterns" in data

    async def test_test_matching_filters(self, client, auth_headers):
        resp = await client.post(
            "/api/matching-filters/test",
            json={
                "content": "First National Bank credential dump found on dark web",
                "watch_terms": [{"term_type": "institution_name", "value": "First National Bank"}],
            },
            headers=auth_headers,
        )
        assert resp.status_code == 200
