"""Integration tests for connectors — base connector interface and mock implementations."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from darkdisco.discovery.connectors.base import BaseConnector, RawMention



class MockConnector(BaseConnector):
    """Test connector that returns canned results."""

    name = "mock"
    source_type = "other"

    def __init__(self, config=None, mentions=None):
        super().__init__(config)
        self._mentions = mentions or []
        self.setup_called = False
        self.teardown_called = False

    async def poll(self, since=None):
        return self._mentions

    async def health_check(self):
        return {"healthy": True, "message": "Mock connector OK"}

    async def setup(self):
        self.setup_called = True

    async def teardown(self):
        self.teardown_called = True


class TestBaseConnectorInterface:
    @pytest.mark.asyncio
    async def test_connector_lifecycle(self):
        mentions = [
            RawMention(
                source_name="mock",
                title="Test mention",
                content="Found credentials for example.com",
                discovered_at=datetime.now(timezone.utc),
            ),
        ]
        connector = MockConnector(mentions=mentions)

        await connector.setup()
        assert connector.setup_called

        results = await connector.poll(since=None)
        assert len(results) == 1
        assert results[0].source_name == "mock"

        health = await connector.health_check()
        assert health["healthy"] is True

        await connector.teardown()
        assert connector.teardown_called

    @pytest.mark.asyncio
    async def test_connector_with_config(self):
        config = {"urls": ["http://example.onion"], "rate_limit": 3}
        connector = MockConnector(config=config)
        assert connector.config == config

    @pytest.mark.asyncio
    async def test_connector_poll_with_since(self):
        old_mention = RawMention(
            source_name="mock",
            title="Old mention",
            content="Old content",
            discovered_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        new_mention = RawMention(
            source_name="mock",
            title="New mention",
            content="New content",
            discovered_at=datetime.now(timezone.utc),
        )
        connector = MockConnector(mentions=[old_mention, new_mention])
        results = await connector.poll(since=datetime(2025, 1, 1, tzinfo=timezone.utc))
        assert len(results) == 2  # Mock doesn't filter by since


class TestRawMention:
    def test_raw_mention_defaults(self):
        m = RawMention(source_name="test")
        assert m.title == ""
        assert m.content == ""
        assert m.author is None
        assert m.metadata == {}
        assert m.source_url is None

    def test_raw_mention_full(self):
        m = RawMention(
            source_name="pastebin",
            source_url="http://example.onion/paste/1",
            title="Data dump",
            content="user:pass\nuser2:pass2",
            author="anon",
            discovered_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            metadata={"paste_id": "abc123"},
        )
        assert m.source_name == "pastebin"
        assert m.metadata["paste_id"] == "abc123"


class TestConnectorLoading:
    """Test the dynamic connector loading mechanism."""

    def test_connector_map_contains_all_types(self):
        from darkdisco.pipeline.worker import _CONNECTOR_MAP

        expected = {
            "paste_site", "forum", "telegram", "telegram_intel",
            "discord", "breach_db", "ransomware_blog",
            "ransomware_aggregator", "stealer_log", "ct_monitor",
            "urlscan", "phishtank",
        }
        assert set(_CONNECTOR_MAP.keys()) == expected

    def test_load_connector_by_type(self):
        from darkdisco.pipeline.worker import _load_connector

        class FakeSource:
            source_type = type("ST", (), {"value": "paste_site"})()
            connector_class = None
            config = {}

        connector = _load_connector(FakeSource())
        assert connector is not None

    def test_load_connector_by_explicit_class(self):
        from darkdisco.pipeline.worker import _load_connector

        class FakeSource:
            source_type = type("ST", (), {"value": "other"})()
            connector_class = "darkdisco.discovery.connectors.paste_site:PasteSiteConnector"
            config = {}

        connector = _load_connector(FakeSource())
        assert connector is not None

    def test_load_connector_unknown_type_raises(self):
        from darkdisco.pipeline.worker import _load_connector

        class FakeSource:
            source_type = type("ST", (), {"value": "nonexistent"})()
            connector_class = None
            config = {}

        with pytest.raises(ValueError, match="No connector"):
            _load_connector(FakeSource())


# ---------------------------------------------------------------------------
# Stealer log connector tests
# ---------------------------------------------------------------------------


def _make_zip_archive(files: dict[str, str]) -> bytes:
    """Helper: create an in-memory zip archive from {filename: text_content}."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


class TestStealerLogParsers:
    """Test the credential parsing functions directly."""

    def test_parse_password_triples_redline(self):
        from darkdisco.discovery.connectors.stealer_log import _parse_password_triples

        text = (
            "URL: https://bank.example.com/login\n"
            "Login: user@example.com\n"
            "Password: s3cret123\n"
            "Application: Chrome\n"
            "\n"
            "URL: https://mail.example.org\n"
            "Login: john\n"
            "Password: pa$$word\n"
        )
        creds = _parse_password_triples(text)
        assert len(creds) == 2
        assert creds[0].url == "https://bank.example.com/login"
        assert creds[0].username == "user@example.com"
        assert creds[0].password == "s3cret123"
        assert creds[0].application == "Chrome"
        assert creds[1].url == "https://mail.example.org"

    def test_parse_password_triples_empty(self):
        from darkdisco.discovery.connectors.stealer_log import _parse_password_triples

        assert _parse_password_triples("") == []
        assert _parse_password_triples("just some text\nno credentials") == []

    def test_parse_csv_credentials(self):
        from darkdisco.discovery.connectors.stealer_log import _parse_csv_credentials

        text = (
            "url,username,password\n"
            "https://bank.com/login,user1,pass1\n"
            "https://shop.com,user2,pass2\n"
        )
        creds = _parse_csv_credentials(text)
        assert len(creds) == 2
        assert creds[0].url == "https://bank.com/login"
        assert creds[0].username == "user1"
        assert creds[1].password == "pass2"

    def test_parse_csv_credentials_empty(self):
        from darkdisco.discovery.connectors.stealer_log import _parse_csv_credentials

        assert _parse_csv_credentials("") == []

    def test_detect_family_redline(self):
        from darkdisco.discovery.connectors.stealer_log import _detect_family

        files = ["Passwords.txt", "Cookies/Chrome.txt", "SystemInfo.txt"]
        assert _detect_family(files) == "redline"

    def test_detect_family_raccoon(self):
        from darkdisco.discovery.connectors.stealer_log import _detect_family

        files = ["passwords.txt", "cookies.txt", "autofill.txt"]
        assert _detect_family(files) == "raccoon"

    def test_detect_family_generic(self):
        from darkdisco.discovery.connectors.stealer_log import _detect_family

        files = ["some_credentials.txt", "data.log"]
        assert _detect_family(files) == "generic"

    def test_parse_system_info(self):
        from darkdisco.discovery.connectors.stealer_log import _parse_system_info

        text = (
            "OS: Windows 10 Pro\n"
            "IP Address: 192.168.1.100\n"
            "Country: United States\n"
            "HWID: ABC123\n"
            "Username: jdoe\n"
        )
        info = _parse_system_info(text)
        assert info["os"] == "Windows 10 Pro"
        assert info["ip"] == "192.168.1.100"
        assert info["country"] == "United States"
        assert info["hwid"] == "ABC123"
        assert info["local_user"] == "jdoe"


class TestStealerLogArchiveExtraction:
    """Test archive extraction and family-specific parsing."""

    def test_extract_zip_archive(self):
        from darkdisco.discovery.connectors.stealer_log import StealerLogConnector

        archive_data = _make_zip_archive({
            "Passwords.txt": "URL: https://example.com\nLogin: user\nPassword: pass\n",
            "SystemInfo.txt": "OS: Windows 10\n",
        })
        files = StealerLogConnector._extract_archive(archive_data, "test.zip")
        assert "Passwords.txt" in files
        assert "SystemInfo.txt" in files

    def test_extract_rejects_path_traversal(self):
        from darkdisco.discovery.connectors.stealer_log import StealerLogConnector

        # Create zip with path traversal attempt
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("../../../etc/passwd", "root:x:0:0:root")
            zf.writestr("safe.txt", "safe content")
        data = buf.getvalue()

        files = StealerLogConnector._extract_archive(data, "evil.zip")
        assert "../../../etc/passwd" not in files
        assert "safe.txt" in files

    def test_parse_redline_archive(self):
        from darkdisco.discovery.connectors.stealer_log import _parse_redline

        files = {
            "Passwords.txt": (
                "URL: https://bank.com/login\n"
                "Login: user@bank.com\n"
                "Password: bankpass\n"
                "\n"
                "URL: https://shop.com\n"
                "Login: shopper\n"
                "Password: shop123\n"
            ).encode(),
            "SystemInfo.txt": "OS: Windows 11\nIP Address: 10.0.0.1\n".encode(),
            "Cookies/chrome.txt": "cookie1\ncookie2\ncookie3\n".encode(),
        }
        archive = _parse_redline(files, "test.zip", "abc123" * 10)
        assert archive.stealer_family == "redline"
        assert len(archive.credentials) == 2
        assert archive.credentials[0].url == "https://bank.com/login"
        assert archive.system_info["os"] == "Windows 11"
        assert archive.cookies_count == 3

    def test_parse_raccoon_archive(self):
        from darkdisco.discovery.connectors.stealer_log import _parse_raccoon

        files = {
            "passwords.txt": (
                "url,username,password\n"
                "https://email.com,user1,pass1\n"
            ).encode(),
            "cookies.txt": "line1\nline2\n".encode(),
        }
        archive = _parse_raccoon(files, "test.zip", "def456" * 10)
        assert archive.stealer_family == "raccoon"
        assert len(archive.credentials) == 1
        assert archive.cookies_count == 2


class TestStealerLogConnector:
    """Test the full connector lifecycle with mocked S3."""

    def test_connector_attributes(self):
        from darkdisco.discovery.connectors.stealer_log import StealerLogConnector

        conn = StealerLogConnector()
        assert conn.name == "stealer_log"
        assert conn.source_type == "stealer_log"

    @pytest.mark.asyncio
    async def test_connector_health_check_success(self):
        from darkdisco.discovery.connectors.stealer_log import StealerLogConnector

        conn = StealerLogConnector(config={"s3_bucket": "test-bucket"})
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        conn._s3_client = mock_s3

        result = await conn.health_check()
        assert result["healthy"] is True

    @pytest.mark.asyncio
    async def test_connector_poll_with_seen_hashes_dedup(self):
        from darkdisco.discovery.connectors.stealer_log import StealerLogConnector

        archive_data = _make_zip_archive({
            "Passwords.txt": "URL: https://example.com\nLogin: user\nPassword: pass\n",
            "SystemInfo.txt": "OS: Windows 10\n",
        })
        import hashlib
        sha = hashlib.sha256(archive_data).hexdigest()[:16]

        # Pre-seed seen_hashes with this archive's hash
        conn = StealerLogConnector(config={
            "s3_prefix": "logs/",
            "s3_bucket": "test-bucket",
            "seen_hashes": [sha],
        })

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "logs/dump.zip", "LastModified": datetime(2026, 3, 1, tzinfo=timezone.utc)},
                ]
            }
        ]
        mock_s3.head_object.return_value = {"ContentLength": len(archive_data)}
        mock_s3.get_object.return_value = {"Body": io.BytesIO(archive_data)}
        conn._s3_client = mock_s3

        mentions = await conn.poll()
        assert len(mentions) == 0  # deduped

    @pytest.mark.asyncio
    async def test_connector_poll_new_archive(self):
        from darkdisco.discovery.connectors.stealer_log import StealerLogConnector

        archive_data = _make_zip_archive({
            "Passwords.txt": (
                "URL: https://bank.example.com/login\n"
                "Login: user@bank.com\n"
                "Password: s3cret\n"
            ),
            "SystemInfo.txt": "OS: Windows 10\nIP Address: 1.2.3.4\n",
        })

        conn = StealerLogConnector(config={
            "s3_prefix": "logs/",
            "s3_bucket": "test-bucket",
            "seen_hashes": [],
        })

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "logs/new_dump.zip", "LastModified": datetime(2026, 3, 10, tzinfo=timezone.utc)},
                ]
            }
        ]
        mock_s3.head_object.return_value = {"ContentLength": len(archive_data)}
        mock_s3.get_object.return_value = {"Body": io.BytesIO(archive_data)}
        conn._s3_client = mock_s3

        mentions = await conn.poll()
        assert len(mentions) == 1
        mention = mentions[0]
        assert "stealer_log:" in mention.source_name
        assert "bank.example.com" in mention.content
        assert mention.metadata["total_credentials"] == 1
        # Verify seen_hashes was updated
        assert len(conn.config["seen_hashes"]) == 1

    def test_connector_in_worker_map(self):
        from darkdisco.pipeline.worker import _CONNECTOR_MAP

        assert "stealer_log" in _CONNECTOR_MAP
        assert "StealerLogConnector" in _CONNECTOR_MAP["stealer_log"]


# ---------------------------------------------------------------------------
# URLScan connector tests
# ---------------------------------------------------------------------------


class TestURLScanConnector:
    def test_connector_attributes(self):
        from darkdisco.discovery.connectors.urlscan import URLScanConnector

        conn = URLScanConnector()
        assert conn.name == "urlscan"
        assert conn.source_type == "urlscan"

    def test_connector_in_worker_map(self):
        from darkdisco.pipeline.worker import _CONNECTOR_MAP

        assert "urlscan" in _CONNECTOR_MAP
        assert "URLScanConnector" in _CONNECTOR_MAP["urlscan"]

    @pytest.mark.asyncio
    async def test_poll_no_queries_warns(self):
        from darkdisco.discovery.connectors.urlscan import URLScanConnector

        conn = URLScanConnector(config={})
        mentions = await conn.poll()
        assert mentions == []

    @pytest.mark.asyncio
    async def test_poll_with_mock_results(self):
        from darkdisco.discovery.connectors.urlscan import URLScanConnector

        conn = URLScanConnector(config={
            "search_queries": ["Example Bank"],
            "seen_ids": [],
            "verdicts_only": False,
            "min_score": 0,
        })

        mock_response = {
            "results": [
                {
                    "_id": "scan-uuid-001",
                    "task": {
                        "url": "https://evil-example-bank.com/login",
                        "time": "2026-03-15T12:00:00Z",
                    },
                    "page": {
                        "domain": "evil-example-bank.com",
                        "ip": "1.2.3.4",
                        "country": "RU",
                        "title": "Example Bank - Login",
                    },
                    "stats": {"requests": 10, "ips": 2, "domains": 3},
                    "verdicts": {
                        "overall": {
                            "score": 85,
                            "malicious": True,
                            "categories": ["phishing"],
                            "brands": ["Example Bank"],
                        }
                    },
                    "screenshot": "https://urlscan.io/screenshots/scan-uuid-001.png",
                },
            ]
        }

        mock_session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_response)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)
        conn._session = mock_session

        mentions = await conn.poll()
        assert len(mentions) == 1
        m = mentions[0]
        assert "evil-example-bank.com" in m.title
        assert m.metadata["scan_id"] == "scan-uuid-001"
        assert m.metadata["score"] == 85
        assert "scan-uuid-001" in conn.config["seen_ids"]

    @pytest.mark.asyncio
    async def test_poll_deduplicates_seen_ids(self):
        from darkdisco.discovery.connectors.urlscan import URLScanConnector

        conn = URLScanConnector(config={
            "search_queries": ["Test"],
            "seen_ids": ["already-seen-001"],
            "verdicts_only": False,
            "min_score": 0,
        })

        mock_response = {
            "results": [
                {
                    "_id": "already-seen-001",
                    "task": {"url": "https://x.com", "time": ""},
                    "page": {"domain": "x.com", "ip": "", "country": "", "title": ""},
                    "stats": {},
                    "verdicts": {"overall": {"score": 90, "categories": [], "brands": []}},
                },
            ]
        }

        mock_session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_response)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)
        conn._session = mock_session

        mentions = await conn.poll()
        assert len(mentions) == 0

    @pytest.mark.asyncio
    async def test_health_check(self):
        from darkdisco.discovery.connectors.urlscan import URLScanConnector

        conn = URLScanConnector(config={})
        mock_session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)
        conn._session = mock_session

        result = await conn.health_check()
        assert result["healthy"] is True
        assert result["message"] == "urlscan:ok"


# ---------------------------------------------------------------------------
# PhishTank connector tests
# ---------------------------------------------------------------------------


class TestPhishTankConnector:
    def test_connector_attributes(self):
        from darkdisco.discovery.connectors.phishtank import PhishTankConnector

        conn = PhishTankConnector()
        assert conn.name == "phishtank"
        assert conn.source_type == "phishtank"

    def test_connector_in_worker_map(self):
        from darkdisco.pipeline.worker import _CONNECTOR_MAP

        assert "phishtank" in _CONNECTOR_MAP
        assert "PhishTankConnector" in _CONNECTOR_MAP["phishtank"]

    @pytest.mark.asyncio
    async def test_poll_no_config_warns(self):
        from darkdisco.discovery.connectors.phishtank import PhishTankConnector

        conn = PhishTankConnector(config={})
        mentions = await conn.poll()
        assert mentions == []

    @pytest.mark.asyncio
    async def test_poll_with_mock_feed(self):
        from darkdisco.discovery.connectors.phishtank import PhishTankConnector

        conn = PhishTankConnector(config={
            "watch_domains": ["firstnational.com"],
            "watch_brands": ["First National Bank"],
            "seen_ids": [],
        })

        mock_feed = [
            {
                "phish_id": "99001",
                "url": "https://firstnational-login.evil.com/secure",
                "target": "First National Bank",
                "submission_time": "2026-03-15T10:00:00+00:00",
                "verified": "yes",
                "verification_time": "2026-03-15T10:30:00+00:00",
                "online": "yes",
                "phish_detail_url": "https://phishtank.org/phish_detail.php?phish_id=99001",
            },
            {
                "phish_id": "99002",
                "url": "https://some-other-bank.evil.com",
                "target": "Other Bank",
                "submission_time": "2026-03-15T11:00:00+00:00",
                "verified": "yes",
                "verification_time": "2026-03-15T11:30:00+00:00",
                "online": "yes",
                "phish_detail_url": "",
            },
        ]

        mock_session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_feed)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)
        conn._session = mock_session

        mentions = await conn.poll()
        assert len(mentions) == 1  # Only the first matches
        m = mentions[0]
        assert "First National Bank" in m.title
        assert m.metadata["phish_id"] == "99001"
        assert m.metadata["matched_brand"] == "first national bank"
        assert "99001" in conn.config["seen_ids"]

    @pytest.mark.asyncio
    async def test_poll_domain_matching(self):
        from darkdisco.discovery.connectors.phishtank import PhishTankConnector

        conn = PhishTankConnector(config={
            "watch_domains": ["example-bank.com"],
            "watch_brands": [],
            "seen_ids": [],
        })

        mock_feed = [
            {
                "phish_id": "88001",
                "url": "https://www.example-bank.com.evil.ru/login",
                "target": "Unknown",
                "submission_time": "2026-03-15T08:00:00+00:00",
                "verified": "yes",
                "verification_time": "2026-03-15T08:30:00+00:00",
                "online": "yes",
                "phish_detail_url": "",
            },
        ]

        mock_session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_feed)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)
        conn._session = mock_session

        mentions = await conn.poll()
        assert len(mentions) == 1
        assert mentions[0].metadata["matched_domain"] == "example-bank.com"

    @pytest.mark.asyncio
    async def test_poll_deduplicates_seen_ids(self):
        from darkdisco.discovery.connectors.phishtank import PhishTankConnector

        conn = PhishTankConnector(config={
            "watch_domains": ["test.com"],
            "watch_brands": [],
            "seen_ids": ["77001"],
        })

        mock_feed = [
            {
                "phish_id": "77001",
                "url": "https://test.com.evil.com",
                "target": "",
                "submission_time": "2026-03-15T08:00:00+00:00",
                "verified": "yes",
                "verification_time": "",
                "online": "yes",
                "phish_detail_url": "",
            },
        ]

        mock_session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_feed)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)
        conn._session = mock_session

        mentions = await conn.poll()
        assert len(mentions) == 0

    @pytest.mark.asyncio
    async def test_health_check(self):
        from darkdisco.discovery.connectors.phishtank import PhishTankConnector

        conn = PhishTankConnector(config={})
        mock_session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.head = MagicMock(return_value=mock_resp)
        conn._session = mock_session

        result = await conn.health_check()
        assert result["healthy"] is True
        assert result["message"] == "phishtank:ok"
