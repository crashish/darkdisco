"""Application settings — loaded from environment variables."""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

_JWT_SECRET_FILE = Path(os.environ.get("DARKDISCO_DATA_DIR", "data")) / ".jwt_secret"


def _stable_jwt_secret() -> str:
    """Return a stable JWT secret that persists across restarts.

    Priority:
    1. DARKDISCO_JWT_SECRET env var (if set and not the placeholder)
    2. Previously generated secret from data/.jwt_secret file
    3. Generate a new secret, persist it, and return it

    This prevents token invalidation on container/process restart.
    """
    # Check env var first — if explicitly set, use it
    env_secret = os.environ.get("DARKDISCO_JWT_SECRET", "")
    if env_secret and env_secret != "change-me-in-production":
        return env_secret

    # Check for persisted secret file
    if _JWT_SECRET_FILE.exists():
        stored = _JWT_SECRET_FILE.read_text().strip()
        if stored:
            return stored

    # Generate a new stable secret and persist it
    new_secret = hashlib.sha256(os.urandom(64)).hexdigest()
    try:
        _JWT_SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
        _JWT_SECRET_FILE.write_text(new_secret)
        _JWT_SECRET_FILE.chmod(0o600)
        logger.info("Generated and persisted new JWT secret to %s", _JWT_SECRET_FILE)
    except OSError:
        logger.warning(
            "Could not persist JWT secret to %s — tokens will not survive restart",
            _JWT_SECRET_FILE,
        )
    return new_secret


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://darkdisco:darkdisco@localhost:5432/darkdisco"

    # Redis (for Celery broker and caching)
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # JWT auth
    jwt_secret: str = ""
    jwt_expire_minutes: int = 480
    jwt_algorithm: str = "HS256"

    # Tor proxy for dark web access
    tor_socks_proxy: str = "socks5h://127.0.0.1:9050"
    tor_control_port: int = 9051
    tor_control_password: str = ""

    # Source connector defaults
    default_poll_interval: int = 3600  # seconds
    max_content_size: int = 10 * 1024 * 1024  # 10MB

    # External API keys (all optional)
    dehashed_api_key: str = ""
    dehashed_email: str = ""
    intelx_api_key: str = ""
    hibp_api_key: str = ""
    telegram_bot_token: str = ""  # Legacy — kept for compat
    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    telegram_session_name: str = "data/darkdisco_monitor"
    discord_bot_token: str = ""
    pastebin_api_key: str = ""
    urlscan_api_key: str = ""
    phishtank_api_key: str = ""

    # Email (SMTP) notifications
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from_address: str = "alerts@darkdisco.local"
    smtp_default_recipient: str = ""
    smtp_use_tls: bool = False
    smtp_use_starttls: bool = True

    # Slack notifications
    slack_webhook_url: str = ""

    # S3/MinIO for attachment storage
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "darkdisco"
    s3_secret_key: str = "darkdisco-secret"
    s3_bucket: str = "darkdisco-attachments"

    # Trapline integration (client watchlist sync + webhook receiver)
    trapline_api_url: str = ""  # e.g. https://trapline.example.com
    trapline_api_key: str = ""
    trapline_webhook_secret: str = ""  # HMAC-SHA256 secret for verifying inbound webhooks
    trapline_sync_interval: int = 3600  # seconds

    # OCR (image text extraction)
    ocr_enabled: bool = True
    ocr_min_confidence: float = 25.0  # discard results below this threshold

    # Celery tuning
    celery_task_soft_time_limit: int = 300
    celery_task_time_limit: int = 600

    model_config = {"env_prefix": "DARKDISCO_", "env_file": ".env", "extra": "ignore"}

    def model_post_init(self, __context: object) -> None:
        if not self.jwt_secret or self.jwt_secret == "change-me-in-production":
            object.__setattr__(self, "jwt_secret", _stable_jwt_secret())


settings = Settings()
