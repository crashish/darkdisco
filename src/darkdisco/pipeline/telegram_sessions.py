"""Telegram session pool — manages multiple Telethon sessions for concurrent operations.

Each Telethon session maps to an independent MTProto connection backed by its
own SQLite file.  By giving the poll task and download task(s) separate sessions
we eliminate the Redis lock that previously serialized all Telegram I/O.

Rate-limit safety
-----------------
*   Per-session FloodWaitError tracking is stored in Redis.
*   A circuit breaker automatically falls back to single-session mode when
    flood-wait durations exceed a configurable threshold.
*   Exponential backoff is applied per session after each flood wait.
"""

from __future__ import annotations

import logging
import shutil
import time
from enum import Enum
from pathlib import Path

import redis as _redis

from darkdisco.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Session roles
# ---------------------------------------------------------------------------


class SessionRole(str, Enum):
    """Named session roles.  Each role gets its own .session file."""

    POLL = "poll"
    DOWNLOAD_1 = "download_1"
    DOWNLOAD_2 = "download_2"
    DISCOVERY = "discovery"


# Mapping from role to filename suffix appended to the primary session base.
_SESSION_SUFFIXES: dict[SessionRole, str] = {
    SessionRole.POLL: "_poll",
    SessionRole.DOWNLOAD_1: "_download_1",
    SessionRole.DOWNLOAD_2: "_download_2",
    SessionRole.DISCOVERY: "_discovery",
}

# ---------------------------------------------------------------------------
# Redis keys
# ---------------------------------------------------------------------------

_FLOOD_WAIT_KEY_PREFIX = "darkdisco:tg_flood:"  # per-session flood-wait state
_CIRCUIT_BREAKER_KEY = "darkdisco:tg_circuit_breaker"
_FLOOD_WAIT_LOG_KEY = "darkdisco:tg_flood_log"  # sorted set of recent waits

# Thresholds
FLOOD_WAIT_CIRCUIT_THRESHOLD = 60  # seconds — trip breaker if any wait ≥ this
CIRCUIT_BREAKER_COOLDOWN = 300  # seconds to stay in single-session mode
MAX_CONCURRENT_SESSIONS = 2  # conservative start per bead notes


def _redis_client() -> _redis.Redis:
    return _redis.from_url(settings.celery_broker_url)


# ---------------------------------------------------------------------------
# Session file management
# ---------------------------------------------------------------------------


def _primary_session_path() -> Path:
    """Resolved path to the primary (authenticated) .session file."""
    base = Path(settings.telegram_session_name).expanduser()
    session_file = Path(str(base) + ".session")
    return session_file


def session_path_for_role(role: SessionRole) -> str:
    """Return the Telethon session *name* (without .session suffix) for a role.

    If the role-specific session file does not yet exist it is copied from the
    primary session.
    """
    primary = _primary_session_path()
    if not primary.exists():
        raise FileNotFoundError(
            f"Primary Telegram session not found at {primary}.  "
            "Run interactive login first."
        )

    suffix = _SESSION_SUFFIXES[role]
    role_base = str(primary).replace(".session", suffix)
    role_file = Path(role_base + ".session")

    if not role_file.exists():
        shutil.copy2(str(primary), str(role_file))
        logger.info("Initialized %s session from primary: %s", role.value, role_file)
    elif primary.stat().st_mtime > role_file.stat().st_mtime + 3600:
        # Primary is >1 hour newer — refresh entity cache
        shutil.copy2(str(primary), str(role_file))
        logger.info("Refreshed %s session from primary (entity cache update)", role.value)

    # Telethon wants the path *without* the .session extension
    return role_base


def initialize_all_sessions() -> dict[SessionRole, str]:
    """Ensure all role-specific session files exist.  Returns {role: path}."""
    result = {}
    for role in SessionRole:
        try:
            result[role] = session_path_for_role(role)
        except FileNotFoundError:
            logger.warning("Cannot initialize session for %s — primary missing", role.value)
    return result


# ---------------------------------------------------------------------------
# Flood-wait tracking
# ---------------------------------------------------------------------------


def record_flood_wait(role: SessionRole, wait_seconds: int) -> None:
    """Record a FloodWaitError for a session.  May trip the circuit breaker."""
    r = _redis_client()
    now = time.time()

    # Store per-session last flood wait
    key = f"{_FLOOD_WAIT_KEY_PREFIX}{role.value}"
    r.hset(key, mapping={
        "last_wait": str(wait_seconds),
        "last_at": str(now),
        "backoff_until": str(now + wait_seconds + _backoff_buffer(wait_seconds)),
    })
    r.expire(key, 3600)

    # Append to global flood log (sorted set scored by timestamp)
    r.zadd(_FLOOD_WAIT_LOG_KEY, {f"{role.value}:{wait_seconds}:{now}": now})
    # Trim entries older than 10 minutes
    r.zremrangebyscore(_FLOOD_WAIT_LOG_KEY, "-inf", now - 600)

    logger.warning(
        "FloodWaitError on session %s: %ds wait recorded", role.value, wait_seconds
    )

    # Trip circuit breaker if wait is severe
    if wait_seconds >= FLOOD_WAIT_CIRCUIT_THRESHOLD:
        _trip_circuit_breaker(r, wait_seconds, role)


def _backoff_buffer(wait_seconds: int) -> float:
    """Extra buffer on top of the Telegram-mandated wait.

    Adds 10% + 5s to avoid hitting the limit boundary.
    """
    return max(5.0, wait_seconds * 0.1)


def _trip_circuit_breaker(r: _redis.Redis, wait_seconds: int, role: SessionRole) -> None:
    """Activate single-session fallback mode."""
    cooldown = max(CIRCUIT_BREAKER_COOLDOWN, wait_seconds * 2)
    r.set(_CIRCUIT_BREAKER_KEY, f"{role.value}:{wait_seconds}", ex=int(cooldown))
    logger.critical(
        "Circuit breaker TRIPPED by session %s (wait=%ds).  "
        "Falling back to single-session mode for %ds.",
        role.value, wait_seconds, cooldown,
    )


def is_circuit_breaker_active() -> bool:
    """True when the system should operate in single-session (safe) mode."""
    r = _redis_client()
    return r.exists(_CIRCUIT_BREAKER_KEY) > 0


def session_backoff_remaining(role: SessionRole) -> float:
    """Seconds remaining before this session should be used again.  0 = ready."""
    r = _redis_client()
    key = f"{_FLOOD_WAIT_KEY_PREFIX}{role.value}"
    data = r.hgetall(key)
    if not data:
        return 0.0
    backoff_until = float(data.get(b"backoff_until", b"0"))
    remaining = backoff_until - time.time()
    return max(0.0, remaining)


def get_flood_wait_stats() -> dict:
    """Return current flood-wait state for monitoring."""
    r = _redis_client()
    stats: dict = {
        "circuit_breaker_active": is_circuit_breaker_active(),
        "sessions": {},
    }
    for role in SessionRole:
        key = f"{_FLOOD_WAIT_KEY_PREFIX}{role.value}"
        data = r.hgetall(key)
        if data:
            stats["sessions"][role.value] = {
                "last_wait": int(float(data.get(b"last_wait", b"0"))),
                "backoff_remaining": round(session_backoff_remaining(role), 1),
            }

    # Recent flood log count
    now = time.time()
    stats["floods_last_10min"] = r.zcount(_FLOOD_WAIT_LOG_KEY, now - 600, "+inf")
    return stats


# ---------------------------------------------------------------------------
# Session allocation for tasks
# ---------------------------------------------------------------------------


def get_poll_session_name() -> str:
    """Return session name for the poll task.

    Falls back to primary session if circuit breaker is active.
    """
    if is_circuit_breaker_active():
        logger.info("Circuit breaker active — poll using primary session")
        return str(Path(settings.telegram_session_name).expanduser())

    backoff = session_backoff_remaining(SessionRole.POLL)
    if backoff > 0:
        logger.info("Poll session in backoff (%.0fs remaining)", backoff)
        # Still use the poll session but caller should delay
        # Returning it anyway — caller is responsible for sleeping

    return session_path_for_role(SessionRole.POLL)


def get_download_session_name(worker_index: int = 0) -> str:
    """Return session name for a download worker.

    worker_index 0 → DOWNLOAD_1, 1 → DOWNLOAD_2.
    Falls back to single download session if circuit breaker is active.
    """
    if is_circuit_breaker_active():
        logger.info("Circuit breaker active — download using single session")
        return session_path_for_role(SessionRole.DOWNLOAD_1)

    role = SessionRole.DOWNLOAD_1 if worker_index == 0 else SessionRole.DOWNLOAD_2
    return session_path_for_role(role)


def get_discovery_session_name() -> str:
    """Return session name for channel discovery."""
    if is_circuit_breaker_active():
        return str(Path(settings.telegram_session_name).expanduser())
    return session_path_for_role(SessionRole.DISCOVERY)
