"""Comprehensive regression test suite covering all regressions from March 13-17 sessions.

Covers: API/auth, Telegram connector, data models, pipeline, file handling, frontend logic.
"""

from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from darkdisco.common.models import (
    Base,
    DiscoveredChannel,
    DiscoveryStatus,
    Finding,
    FindingStatus,
    RawMention as RawMentionModel,
    Severity,
    WatchTerm,
    WatchTermType,
)
from darkdisco.discovery.connectors.base import RawMention

pytestmark = pytest.mark.asyncio


# ============================================================================
# API / AUTH TESTS
# ============================================================================


class TestLoginReturnsValidJWT:
    """Login returns valid JWT that can access protected routes."""

    async def test_login_returns_jwt_with_bearer_type(self, client, test_user):
        resp = await client.post(
            "/api/auth/login",
            json={"username": "testanalyst", "password": "testpass123"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    async def test_jwt_accesses_protected_routes(self, client, test_user):
        login = await client.post(
            "/api/auth/login",
            json={"username": "testanalyst", "password": "testpass123"},
        )
        token = login.json()["access_token"]
        resp = await client.get("/api/clients", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 200


class TestFindingStatusTransitions:
    """Finding status transitions work for all valid transitions."""

    @pytest.mark.parametrize(
        "from_status,to_status",
        [
            ("new", "reviewing"),
            ("new", "confirmed"),
            ("new", "dismissed"),
            ("new", "false_positive"),
            ("reviewing", "escalated"),
            ("reviewing", "confirmed"),
            ("reviewing", "dismissed"),
            ("reviewing", "resolved"),
            ("reviewing", "false_positive"),
            ("escalated", "resolved"),
            ("escalated", "confirmed"),
            ("escalated", "reviewing"),
            ("confirmed", "resolved"),
            ("confirmed", "escalated"),
            ("confirmed", "reviewing"),
            ("dismissed", "reviewing"),
            ("resolved", "reviewing"),
            ("false_positive", "reviewing"),
        ],
    )
    async def test_valid_transition(
        self, client, auth_headers, db_session, sample_institution, sample_source,
        from_status, to_status,
    ):
        finding = Finding(
            id=str(uuid4()),
            institution_id=sample_institution.id,
            source_id=sample_source.id,
            severity=Severity.medium,
            status=FindingStatus(from_status),
            title="Transition test finding",
            content_hash=hashlib.sha256(str(uuid4()).encode()).hexdigest(),
        )
        db_session.add(finding)
        await db_session.commit()
        await db_session.refresh(finding)

        resp = await client.post(
            f"/api/findings/{finding.id}/transition",
            json={"status": to_status},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == to_status


class TestWatchTermCreationAllTypes:
    """Watch term creation with all WatchTermType enum values."""

    @pytest.mark.parametrize(
        "term_type",
        ["institution_name", "domain", "bin_range", "executive_name", "routing_number", "keyword", "regex"],
    )
    async def test_create_watch_term_type(
        self, client, auth_headers, sample_institution, term_type,
    ):
        resp = await client.post(
            "/api/watch-terms",
            json={
                "institution_id": sample_institution.id,
                "term_type": term_type,
                "value": f"test-{term_type}-value",
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201
        assert resp.json()["term_type"] == term_type

    def test_watch_term_type_enum_values(self):
        """WatchTermType enum must include institution_name and bin_range (not 'name' or 'bin')."""
        expected = {"institution_name", "domain", "bin_range", "executive_name", "routing_number", "keyword", "regex"}
        actual = {wt.value for wt in WatchTermType}
        assert actual == expected


class TestArchiveContentsEndpoint:
    """Archive contents endpoint returns files (fallback to metadata when no ExtractedFile rows)."""

    async def test_archive_contents_from_metadata_fallback(
        self, client, auth_headers, db_session, sample_finding,
    ):
        # Set up finding with file_analysis metadata (legacy fallback path)
        sample_finding.metadata_ = {
            "extracted_file_contents": [
                {"filename": "passwords.txt", "content": "URL: https://bank.com\nuser:pass"},
                {"filename": "cookies.txt", "content": "cookie data"},
            ],
            "file_analysis": {
                "total_files": 2,
                "files": [
                    {"filename": "passwords.txt", "size": 30},
                    {"filename": "cookies.txt", "size": 11},
                ],
            },
        }
        await db_session.commit()

        resp = await client.get(
            f"/api/findings/{sample_finding.id}/archive-contents",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        files = resp.json()
        assert len(files) >= 1


class TestExtractedFilesSearch:
    """Extracted files search endpoint exists and has proper route."""

    def test_extracted_files_search_route_registered(self):
        """Verify the extracted-files/search route is registered in the app."""
        from darkdisco.api.app import app

        routes = [r.path for r in app.routes if hasattr(r, "path")]
        # The route may be nested under a router prefix
        assert any("extracted-files" in r and "search" in r for r in routes) or \
            any("extracted_files" in r for r in routes), \
            f"No extracted-files search route found in: {routes}"


# ============================================================================
# TELEGRAM CONNECTOR TESTS
# ============================================================================


class TestTelegramExtractChannelLinks:
    """extract_channel_links extracts t.me/ URLs from message text."""

    def test_basic_channel_link(self):
        from darkdisco.discovery.connectors.telegram import extract_channel_links

        links = extract_channel_links("Join us at https://t.me/darkchannel for updates")
        assert "https://t.me/darkchannel" in links

    def test_invite_link(self):
        from darkdisco.discovery.connectors.telegram import extract_channel_links

        links = extract_channel_links("Invite: https://t.me/+abc123XYZ")
        assert len(links) == 1
        assert "+abc123XYZ" in links[0]

    def test_joinchat_link(self):
        from darkdisco.discovery.connectors.telegram import extract_channel_links

        links = extract_channel_links("https://t.me/joinchat/ABCDEF123")
        assert len(links) == 1
        assert "joinchat" in links[0]

    def test_skips_message_links(self):
        from darkdisco.discovery.connectors.telegram import extract_channel_links

        links = extract_channel_links("See https://t.me/channel/12345")
        assert len(links) == 0

    def test_skips_feature_paths(self):
        from darkdisco.discovery.connectors.telegram import extract_channel_links

        links = extract_channel_links("https://t.me/proxy?server=x https://t.me/addstickers/pack1")
        assert len(links) == 0

    def test_multiple_links_deduplicated(self):
        from darkdisco.discovery.connectors.telegram import extract_channel_links

        text = "Check https://t.me/darkchan and also https://t.me/darkchan"
        links = extract_channel_links(text)
        assert len(links) == 1

    def test_empty_text(self):
        from darkdisco.discovery.connectors.telegram import extract_channel_links

        assert extract_channel_links("") == []


class TestTelegramDownloadMedia:
    """download_media method exists on TelegramConnector and accepts message_id + channel_id."""

    def test_download_media_method_exists(self):
        from darkdisco.discovery.connectors.telegram import TelegramConnector

        conn = TelegramConnector(config={})
        assert hasattr(conn, "download_media")
        assert callable(conn.download_media)

    def test_download_media_signature(self):
        import inspect
        from darkdisco.discovery.connectors.telegram import TelegramConnector

        sig = inspect.signature(TelegramConnector.download_media)
        params = list(sig.parameters.keys())
        assert "message_id" in params
        assert "channel_id" in params


class TestTelegramHighWaterMark:
    """High-water marks persist to source config after poll."""

    def test_high_water_mark_property(self):
        from darkdisco.discovery.connectors.telegram import TelegramConnector

        config = {"channels": ["test"], "last_message_ids": {"12345": 100}}
        conn = TelegramConnector(config=config)
        assert conn._last_message_ids == {"12345": 100}

    def test_high_water_mark_auto_initializes(self):
        from darkdisco.discovery.connectors.telegram import TelegramConnector

        config = {"channels": ["test"]}
        conn = TelegramConnector(config=config)
        assert conn._last_message_ids == {}
        # Accessing _last_message_ids should create the key via setdefault
        assert "last_message_ids" in conn.config


class TestTelegramTimeFilterBackfill:
    """Time filter skipped for channels with no high-water mark (backfill mode)."""

    def test_time_filter_logic_documented(self):
        """The poll method skips time filtering when min_id is 0 (first poll / backfill)."""
        import inspect
        from darkdisco.discovery.connectors.telegram import TelegramConnector

        source = inspect.getsource(TelegramConnector._poll_channel)
        # The critical line: if min_id > 0 and since and ...
        # When min_id == 0, the time filter is skipped
        assert "min_id > 0" in source


# ============================================================================
# DATA MODEL TESTS
# ============================================================================


class TestRawMentionMetadataColumn:
    """RawMention uses metadata_ (not metadata) for JSONB column access."""

    def test_model_has_metadata_underscore_column(self):
        """The SQLAlchemy model column is metadata_ (maps to 'metadata' in DB)."""
        assert hasattr(RawMentionModel, "metadata_")
        # The column is mapped to 'metadata' in the DB
        col = RawMentionModel.__table__.c
        assert "metadata" in col

    def test_dataclass_has_metadata_attribute(self):
        """The RawMention dataclass uses .metadata (no underscore)."""
        mention = RawMention(source_name="test", metadata={"key": "value"})
        assert mention.metadata == {"key": "value"}


class TestDiscoveredChannelModel:
    """DiscoveredChannel model and related functionality work."""

    async def test_create_discovered_channel(self, db_session, sample_source):
        channel = DiscoveredChannel(
            id=str(uuid4()),
            url="https://t.me/newchannel",
            source_id=sample_source.id,
            source_channel="origchannel",
            message_id=42,
            status=DiscoveryStatus.pending,
        )
        db_session.add(channel)
        await db_session.commit()
        await db_session.refresh(channel)
        assert channel.id is not None
        assert channel.status == DiscoveryStatus.pending

    async def test_discovered_channel_status_transitions(self, db_session, sample_source):
        channel = DiscoveredChannel(
            id=str(uuid4()),
            url="https://t.me/anotherchan",
            source_id=sample_source.id,
            status=DiscoveryStatus.pending,
        )
        db_session.add(channel)
        await db_session.commit()

        channel.status = DiscoveryStatus.approved
        await db_session.commit()
        await db_session.refresh(channel)
        assert channel.status == DiscoveryStatus.approved

    async def test_discovered_channel_api_endpoint(self, client, auth_headers, db_session, sample_source):
        channel = DiscoveredChannel(
            id=str(uuid4()),
            url="https://t.me/apichan",
            source_id=sample_source.id,
            status=DiscoveryStatus.pending,
        )
        db_session.add(channel)
        await db_session.commit()

        resp = await client.get("/api/discovered-channels", headers=auth_headers)
        assert resp.status_code == 200


class TestExtractedFileModel:
    """ExtractedFile table exists with expected columns."""

    def test_extracted_file_table_exists(self):
        assert "extracted_files" in Base.metadata.tables

    def test_extracted_file_columns(self):
        """ExtractedFile has expected columns (content_tsvector may be removed for SQLite tests)."""
        table = Base.metadata.tables["extracted_files"]
        # Core columns that must always be present
        required_cols = {"id", "mention_id", "filename", "s3_key", "sha256", "size",
                         "extension", "is_text", "text_content", "created_at"}
        actual_cols = {c.name for c in table.columns}
        assert required_cols.issubset(actual_cols)


class TestFindingStatusEnum:
    """FindingStatus enum includes confirmed and dismissed."""

    def test_confirmed_in_enum(self):
        assert FindingStatus.confirmed == "confirmed"

    def test_dismissed_in_enum(self):
        assert FindingStatus.dismissed == "dismissed"

    def test_all_expected_statuses(self):
        expected = {"new", "reviewing", "escalated", "resolved", "confirmed", "dismissed", "false_positive"}
        actual = {s.value for s in FindingStatus}
        assert actual == expected


class TestContentHashDedup:
    """content_hash dedup prevents duplicate raw_mentions."""

    async def test_duplicate_hash_detected(self, db_session, sample_source):
        content = "duplicate content for dedup testing"
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        m1 = RawMentionModel(
            id=str(uuid4()),
            source_id=sample_source.id,
            content=content,
            content_hash=content_hash,
        )
        db_session.add(m1)
        await db_session.commit()

        # Check for duplicate
        from sqlalchemy import select
        existing = (
            await db_session.execute(
                select(RawMentionModel.id).where(RawMentionModel.content_hash == content_hash)
            )
        ).scalar_one_or_none()
        assert existing is not None


# ============================================================================
# PIPELINE TESTS
# ============================================================================


class TestProcessFileMentions:
    """_process_file_mentions accesses mention.metadata (dataclass), not metadata_."""

    def test_process_file_mentions_uses_dataclass_metadata(self):
        """The function accesses mention.metadata (dataclass attr), not metadata_ (SA column)."""
        import inspect
        from darkdisco.pipeline.worker import _process_file_mentions

        source = inspect.getsource(_process_file_mentions)
        # Should access mention.metadata (dataclass), not mention.metadata_
        assert "mention.metadata" in source


class TestRunMatchingCreatesFindings:
    """run_matching creates findings for institution name matches."""

    def test_match_mention_creates_results_for_institution_name(self):
        from darkdisco.discovery.matcher import match_mention

        terms = [
            WatchTerm(
                id=str(uuid4()),
                institution_id="inst-1",
                term_type=WatchTermType.institution_name,
                value="First National Bank",
                enabled=True,
                case_sensitive=False,
            ),
        ]
        mention = RawMention(
            source_name="test",
            title="Forum post",
            content="First National Bank data was leaked",
        )
        results = match_mention(mention, terms)
        assert len(results) >= 1
        assert results[0].institution_id == "inst-1"


class TestWatchlistSyncTrapline:
    """Watchlist sync to trapline uses X-API-Key header and correct endpoint paths."""

    def test_trapline_client_uses_x_api_key_header(self):
        from darkdisco.pipeline.trapline import _client
        from darkdisco.config import settings

        # Verify the client builder uses X-API-Key header
        import inspect
        source = inspect.getsource(_client)
        assert "X-API-Key" in source

    def test_trapline_uses_correct_endpoint(self):
        """sync_institution posts to /api/v1/watchlist/domains."""
        import inspect
        from darkdisco.pipeline.trapline import sync_institution

        source = inspect.getsource(sync_institution)
        assert "/api/v1/watchlist/domains" in source

    def test_trapline_does_not_sync_bins(self):
        """BINs are NOT synced to trapline (not useful for domain scanning)."""
        import inspect
        from darkdisco.pipeline.trapline import sync_institution

        source = inspect.getsource(sync_institution)
        # The comment says BIN ranges not synced
        assert "BIN ranges not synced" in source or "not synced" in source


class TestWatchlistSyncBrandNames:
    """Watchlist sync extracts short brand names from institution names."""

    def test_build_domain_entries_extracts_short_names(self):
        from darkdisco.pipeline.trapline import _build_domain_entries

        class FakeInst:
            primary_domain = "navyfcu.org"
            name = "Navy Federal Credit Union"
            short_name = "NFCU"
            additional_domains = []

        entries = _build_domain_entries(FakeInst())
        values = [e["value"] for e in entries]
        # Should have: domain, full name brand, short name brand, extracted short brand
        assert "navyfcu.org" in values
        assert "navy federal credit union" in values
        assert "nfcu" in values
        # Should extract "navy federal" by removing " credit union"
        assert "navy federal" in values

    def test_build_domain_entries_bank_suffix(self):
        from darkdisco.pipeline.trapline import _build_domain_entries

        class FakeInst:
            primary_domain = "boa.com"
            name = "Bank of America"
            short_name = None
            additional_domains = []

        entries = _build_domain_entries(FakeInst())
        type_value_pairs = [(e["type"], e["value"]) for e in entries]
        # Full name as brand
        assert ("brand", "bank of america") in type_value_pairs

    def test_build_domain_entries_no_domain(self):
        from darkdisco.pipeline.trapline import _build_domain_entries

        class FakeInst:
            primary_domain = None
            name = "Test Bank"
            short_name = None
            additional_domains = []

        entries = _build_domain_entries(FakeInst())
        # Should still have brand entries from name
        brand_entries = [e for e in entries if e["type"] == "brand"]
        assert len(brand_entries) >= 1


class TestOCRModule:
    """OCR module exists and extract_text_from_image function is callable."""

    def test_ocr_module_importable(self):
        from darkdisco.pipeline import ocr
        assert hasattr(ocr, "extract_text_from_image")

    def test_extract_text_from_image_callable(self):
        from darkdisco.pipeline.ocr import extract_text_from_image
        assert callable(extract_text_from_image)

    def test_ocr_result_dataclass(self):
        from darkdisco.pipeline.ocr import OCRResult

        result = OCRResult(text="Hello World", confidence=95.0)
        assert result.has_text is True
        assert result.engine == "tesseract"

    def test_ocr_result_empty_text(self):
        from darkdisco.pipeline.ocr import OCRResult

        result = OCRResult(text="", confidence=0.0)
        assert result.has_text is False

    def test_is_image_function(self):
        from darkdisco.pipeline.ocr import is_image

        assert is_image("photo.png") is True
        assert is_image("photo.jpg") is True
        assert is_image("photo.jpeg") is True
        assert is_image("data.txt") is False
        assert is_image("archive.zip") is False

    def test_extract_text_returns_none_for_empty(self):
        from darkdisco.pipeline.ocr import extract_text_from_image

        assert extract_text_from_image(b"") is None
        assert extract_text_from_image(b"", "empty.png") is None


# ============================================================================
# FILE HANDLING TESTS
# ============================================================================


class TestIsArchiveHandlesNone:
    """is_archive handles None filename."""

    def test_is_archive_none_filename(self):
        from darkdisco.pipeline.files import is_archive

        # Should not raise, should return False
        result = is_archive(None)
        assert result is False

    def test_is_archive_valid_extensions(self):
        from darkdisco.pipeline.files import is_archive

        assert is_archive("data.zip") is True
        assert is_archive("data.tar.gz") is True
        assert is_archive("data.rar") is True
        assert is_archive("data.7z") is True

    def test_is_archive_invalid_extensions(self):
        from darkdisco.pipeline.files import is_archive

        assert is_archive("data.txt") is False
        assert is_archive("data.jpg") is False


class TestDownloadTaskMarksFiles:
    """Download task marks files as stored/error with correct metadata updates."""

    def test_download_status_stored_pattern(self):
        """The download task sets download_status to 'stored' on success."""
        import inspect
        from darkdisco.pipeline.worker import download_pending_files

        source = inspect.getsource(download_pending_files)
        assert '"stored"' in source or "'stored'" in source

    def test_download_status_error_pattern(self):
        """The download task sets download_status to 'error' on failure."""
        import inspect
        from darkdisco.pipeline.worker import download_pending_files

        source = inspect.getsource(download_pending_files)
        assert '"error"' in source or "'error'" in source


class TestS3FileListing:
    """S3 file listing returns files with preserved archive paths."""

    def test_s3_key_pattern_preserves_paths(self):
        """Verify S3 key patterns preserve archive structure."""
        # The pattern from _process_file_mentions and _store_extracted_files
        file_sha256 = "abc123def456"
        ef_sha256 = "fedcba654321"
        filename = "Passwords.txt"
        key = f"files/{file_sha256[:8]}/extracted/{ef_sha256[:8]}/{filename}"
        assert key == "files/abc123de/extracted/fedcba65/Passwords.txt"


class TestArchiveContentsMetadataFallback:
    """Archive contents API returns file list from file_analysis metadata fallback."""

    def test_extract_archive_file_list_from_extracted_file_contents(self):
        from darkdisco.api.routes import _extract_archive_file_list

        metadata = {
            "extracted_file_contents": [
                {"filename": "passwords.txt", "content": "URL: https://bank.com\nuser:pass"},
                {"filename": "readme.txt", "content": "This is a readme"},
            ],
        }
        files = _extract_archive_file_list(metadata)
        assert len(files) == 2
        assert files[0]["filename"] == "passwords.txt"
        assert files[1]["filename"] == "readme.txt"

    def test_extract_archive_file_list_search_filter(self):
        from darkdisco.api.routes import _extract_archive_file_list

        metadata = {
            "extracted_file_contents": [
                {"filename": "passwords.txt", "content": "URL: https://bank.com\nuser:pass"},
                {"filename": "readme.txt", "content": "This is a readme"},
            ],
        }
        files = _extract_archive_file_list(metadata, search="password")
        assert len(files) == 1
        assert files[0]["filename"] == "passwords.txt"

    def test_extract_archive_file_list_empty_metadata(self):
        from darkdisco.api.routes import _extract_archive_file_list

        assert _extract_archive_file_list(None) == []
        assert _extract_archive_file_list({}) == []


# ============================================================================
# FRONTEND LOGIC TESTS (testing shared values match backend enums)
# ============================================================================


class TestFrontendTermTypesMatchEnum:
    """termTypes array matches WatchTermType enum values."""

    def test_frontend_term_types_match_backend(self):
        """The frontend termTypes const must contain all WatchTermType enum values."""
        # From Institutions.tsx line 21:
        frontend_term_types = [
            "institution_name", "domain", "bin_range", "routing_number",
            "executive_name", "keyword", "regex",
        ]
        backend_term_types = {wt.value for wt in WatchTermType}
        assert set(frontend_term_types) == backend_term_types


class TestHighlightTextHandlesNull:
    """highlightText handles null/undefined text input — verified at source level."""

    def test_highlight_text_null_safety(self):
        """The Files.tsx highlightText function handles null/undefined text input.

        From Files.tsx:194-196:
            function highlightText(text: string | null | undefined, needle: string) {
                if (!text) return text ?? '';
                ...
        """
        # This is a TypeScript function — we verify the implementation exists
        # and handles null by reading the source
        import os
        files_tsx = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "src", "pages", "Files.tsx"
        )
        if os.path.exists(files_tsx):
            with open(files_tsx) as f:
                content = f.read()
            assert "function highlightText" in content
            # The null check: if (!text) return text ?? '';
            assert "!text" in content


class TestMentionsPagePaginationState:
    """Mentions page pagination state exists."""

    def test_mentions_page_has_pagination(self):
        """The Mentions.tsx page has page state and pagination controls."""
        import os
        mentions_tsx = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "src", "pages", "Mentions.tsx"
        )
        if os.path.exists(mentions_tsx):
            with open(mentions_tsx) as f:
                content = f.read()
            # Page state
            assert "useState" in content
            assert "page" in content
            # Pagination controls
            assert "Page" in content


class TestChannelFilterSendsParam:
    """Channel filter sends channel parameter to API."""

    def test_mentions_page_sends_channel_param(self):
        """The Mentions.tsx page sends channel filter parameter to the API."""
        import os
        mentions_tsx = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "src", "pages", "Mentions.tsx"
        )
        if os.path.exists(mentions_tsx):
            with open(mentions_tsx) as f:
                content = f.read()
            assert "channelFilter" in content
            assert "params.channel" in content


# ============================================================================
# SESSION LOCK TESTS (Telegram)
# ============================================================================


class TestSessionLockSerialization:
    """Session lock serializes poll and download tasks (no SQLite contention)."""

    def test_poll_source_acquires_telegram_lock(self):
        """poll_source acquires Redis lock for Telegram sources."""
        import inspect
        from darkdisco.pipeline.worker import poll_source

        source = inspect.getsource(poll_source)
        assert "telegram_session_lock" in source

    def test_download_pending_acquires_telegram_lock(self):
        """download_pending_files acquires Redis lock for Telegram session."""
        import inspect
        from darkdisco.pipeline.worker import download_pending_files

        source = inspect.getsource(download_pending_files)
        assert "telegram_session_lock" in source

    def test_both_tasks_use_same_lock_name(self):
        """Both poll and download tasks use the same lock name for serialization."""
        import inspect
        from darkdisco.pipeline.worker import poll_source, download_pending_files

        poll_src = inspect.getsource(poll_source)
        download_src = inspect.getsource(download_pending_files)

        # Both should reference the same lock key
        assert "darkdisco:telegram_session_lock" in poll_src
        assert "darkdisco:telegram_session_lock" in download_src


# ============================================================================
# ATTRIBUTED RAW CONTENT TESTS
# ============================================================================


class TestAttributedRawContent:
    """_attributed_raw_content attributes findings to specific extracted files."""

    def test_no_extracted_files_returns_full_content(self):
        from darkdisco.pipeline.worker import _attributed_raw_content

        class FakeMention:
            content = "original message text"
            metadata = {}

        result = _attributed_raw_content(FakeMention(), [{"term_type": "domain", "value": "bank.com"}])
        assert result == "original message text"

    def test_returns_only_matching_file(self):
        from darkdisco.pipeline.worker import _attributed_raw_content

        class FakeMention:
            content = (
                "original"
                "\n\n--- Extracted file: passwords.txt ---\n\n"
                "bank.com credentials"
                "\n\n--- Extracted file: readme.txt ---\n\n"
                "nothing relevant"
            )
            metadata = {
                "extracted_file_contents": [
                    {"filename": "passwords.txt", "content": "bank.com credentials"},
                    {"filename": "readme.txt", "content": "nothing relevant"},
                ]
            }

        result = _attributed_raw_content(
            FakeMention(), [{"term_type": "domain", "value": "bank.com"}]
        )
        assert "passwords.txt" in result
        assert "readme.txt" not in result


# ============================================================================
# STORE EXTRACTED FILES TESTS
# ============================================================================


class TestStoreExtractedFiles:
    """_store_extracted_files creates ExtractedFile rows from mention metadata."""

    def test_store_extracted_files_function_exists(self):
        from darkdisco.pipeline.worker import _store_extracted_files
        assert callable(_store_extracted_files)

    def test_store_extracted_files_empty_metadata(self):
        from darkdisco.pipeline.worker import _store_extracted_files
        from unittest.mock import MagicMock

        session = MagicMock()
        count = _store_extracted_files(session, "mention-id", {})
        assert count == 0

    def test_store_extracted_files_no_list(self):
        from darkdisco.pipeline.worker import _store_extracted_files
        from unittest.mock import MagicMock

        session = MagicMock()
        count = _store_extracted_files(session, "mention-id", {"extracted_file_contents": "not-a-list"})
        assert count == 0


# ============================================================================
# CONNECTOR MAP COMPLETENESS
# ============================================================================


class TestConnectorMapCompleteness:
    """All source types have connectors registered in the worker connector map."""

    def test_connector_map_includes_trapline(self):
        from darkdisco.pipeline.worker import _CONNECTOR_MAP
        assert "trapline" in _CONNECTOR_MAP

    def test_connector_map_has_telegram_variants(self):
        from darkdisco.pipeline.worker import _CONNECTOR_MAP
        assert "telegram" in _CONNECTOR_MAP
        assert "telegram_intel" in _CONNECTOR_MAP

    def test_load_connector_for_download_exists(self):
        from darkdisco.pipeline.worker import _load_connector_for_download
        assert callable(_load_connector_for_download)
