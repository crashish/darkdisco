"""Application settings — loaded from environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://darkdisco:darkdisco@localhost:5432/darkdisco"

    # Redis (for Celery broker and caching)
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # JWT auth
    jwt_secret: str = "change-me-in-production"
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

    # Trapline integration (client watchlist sync)
    trapline_api_url: str = ""  # e.g. https://trapline.example.com
    trapline_api_key: str = ""

    # Celery tuning
    celery_task_soft_time_limit: int = 300
    celery_task_time_limit: int = 600

    model_config = {"env_prefix": "DARKDISCO_", "env_file": ".env", "extra": "ignore"}


settings = Settings()
