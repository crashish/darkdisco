"""Tests for the Telegram session pool and flood-wait tracking."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from darkdisco.pipeline.telegram_sessions import (
    CIRCUIT_BREAKER_COOLDOWN,
    FLOOD_WAIT_CIRCUIT_THRESHOLD,
    SessionRole,
    _backoff_buffer,
    get_download_session_name,
    get_flood_wait_stats,
    get_poll_session_name,
    is_circuit_breaker_active,
    record_flood_wait,
    session_backoff_remaining,
    session_path_for_role,
)


@pytest.fixture
def fake_session_dir(tmp_path):
    """Create a fake primary session file for testing."""
    session_file = tmp_path / "darkdisco_monitor.session"
    session_file.write_text("fake-session-data")
    return tmp_path / "darkdisco_monitor"


@pytest.fixture
def mock_settings(fake_session_dir):
    with patch("darkdisco.pipeline.telegram_sessions.settings") as mock:
        mock.telegram_session_name = str(fake_session_dir)
        mock.celery_broker_url = "redis://localhost:6379/1"
        yield mock


class TestSessionFileManagement:
    def test_session_path_creates_copy(self, mock_settings, fake_session_dir):
        """Role-specific session file is created from primary."""
        path = session_path_for_role(SessionRole.POLL)
        assert path.endswith("_poll")
        assert Path(path + ".session").exists()

    def test_session_path_all_roles(self, mock_settings, fake_session_dir):
        """Each role gets a distinct session file."""
        paths = set()
        for role in SessionRole:
            p = session_path_for_role(role)
            paths.add(p)
            assert Path(p + ".session").exists()
        assert len(paths) == len(SessionRole)

    def test_primary_missing_raises(self, tmp_path):
        """FileNotFoundError if primary session doesn't exist."""
        with patch("darkdisco.pipeline.telegram_sessions.settings") as mock:
            mock.telegram_session_name = str(tmp_path / "nonexistent")
            with pytest.raises(FileNotFoundError):
                session_path_for_role(SessionRole.POLL)


class TestBackoffBuffer:
    def test_minimum_buffer(self):
        assert _backoff_buffer(1) == 5.0

    def test_proportional_buffer(self):
        assert _backoff_buffer(100) == 10.0  # 100 * 0.1 = 10


class TestFloodWaitTracking:
    @pytest.fixture
    def mock_redis(self):
        """Mock Redis client for flood-wait tests."""
        mock_r = MagicMock()
        mock_r.hgetall.return_value = {}
        mock_r.exists.return_value = 0
        mock_r.zcount.return_value = 0
        with patch("darkdisco.pipeline.telegram_sessions._redis_client", return_value=mock_r):
            yield mock_r

    def test_record_flood_wait_stores_data(self, mock_redis):
        record_flood_wait(SessionRole.POLL, 30)
        mock_redis.hset.assert_called_once()
        call_kwargs = mock_redis.hset.call_args
        assert "darkdisco:tg_flood:poll" in call_kwargs[0] or call_kwargs[1].get("name", "") == "darkdisco:tg_flood:poll"

    def test_severe_flood_trips_breaker(self, mock_redis):
        record_flood_wait(SessionRole.POLL, FLOOD_WAIT_CIRCUIT_THRESHOLD)
        # Should call set() on circuit breaker key
        mock_redis.set.assert_called_once()
        args = mock_redis.set.call_args
        assert "darkdisco:tg_circuit_breaker" in str(args)

    def test_mild_flood_no_breaker(self, mock_redis):
        record_flood_wait(SessionRole.POLL, 10)
        mock_redis.set.assert_not_called()

    def test_circuit_breaker_check(self, mock_redis):
        mock_redis.exists.return_value = 0
        assert not is_circuit_breaker_active()
        mock_redis.exists.return_value = 1
        assert is_circuit_breaker_active()

    def test_backoff_remaining_no_data(self, mock_redis):
        assert session_backoff_remaining(SessionRole.POLL) == 0.0

    def test_backoff_remaining_with_data(self, mock_redis):
        future = time.time() + 30
        mock_redis.hgetall.return_value = {
            b"backoff_until": str(future).encode(),
        }
        remaining = session_backoff_remaining(SessionRole.POLL)
        assert 29.0 < remaining <= 30.0


class TestSessionAllocation:
    @pytest.fixture
    def mock_deps(self, mock_settings, fake_session_dir):
        mock_r = MagicMock()
        mock_r.exists.return_value = 0
        mock_r.hgetall.return_value = {}
        with patch("darkdisco.pipeline.telegram_sessions._redis_client", return_value=mock_r):
            yield mock_r

    def test_poll_session_normal(self, mock_deps, fake_session_dir):
        name = get_poll_session_name()
        assert "_poll" in name

    def test_poll_session_circuit_breaker(self, mock_deps, mock_settings, fake_session_dir):
        mock_deps.exists.return_value = 1  # circuit breaker active
        name = get_poll_session_name()
        # Falls back to primary — should NOT end with _poll suffix
        assert not name.endswith("_poll")

    def test_download_session_indices(self, mock_deps, fake_session_dir):
        name0 = get_download_session_name(0)
        name1 = get_download_session_name(1)
        assert "_download_1" in name0
        assert "_download_2" in name1
