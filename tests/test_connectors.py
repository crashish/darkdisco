"""Integration tests for connectors — base connector interface and mock implementations."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

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

        expected = {"paste_site", "forum", "telegram", "breach_db", "ransomware_blog"}
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
