"""DarkDisco data models.

Multi-tenant from the start: Client → Institution → WatchTerm → Finding.
"""

from __future__ import annotations

import enum
from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    Computed,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TSVECTOR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# --- Enums ---


class Severity(str, enum.Enum):
    critical = "critical"
    high = "high"
    medium = "medium"
    low = "low"
    info = "info"


class FindingStatus(str, enum.Enum):
    new = "new"
    reviewing = "reviewing"
    escalated = "escalated"
    resolved = "resolved"
    confirmed = "confirmed"
    dismissed = "dismissed"
    false_positive = "false_positive"


class SourceType(str, enum.Enum):
    paste_site = "paste_site"
    forum = "forum"
    marketplace = "marketplace"
    telegram = "telegram"
    telegram_intel = "telegram_intel"
    discord = "discord"
    breach_db = "breach_db"
    ransomware_blog = "ransomware_blog"
    ransomware_aggregator = "ransomware_aggregator"
    stealer_log = "stealer_log"
    ct_monitor = "ct_monitor"
    urlscan = "urlscan"
    phishtank = "phishtank"
    trapline = "trapline"
    other = "other"


class WatchTermType(str, enum.Enum):
    institution_name = "institution_name"
    domain = "domain"
    bin_range = "bin_range"
    executive_name = "executive_name"
    routing_number = "routing_number"
    keyword = "keyword"
    regex = "regex"


class UserRole(str, enum.Enum):
    analyst = "analyst"
    admin = "admin"


# --- Models ---


class Client(Base):
    """A consulting client (one contract = one client). Top-level tenant."""

    __tablename__ = "clients"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    contract_ref: Mapped[str | None] = mapped_column(String(100))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    institutions: Mapped[list[Institution]] = relationship(back_populates="client", cascade="all, delete-orphan")


class Institution(Base):
    """A bank or credit union being monitored under a client contract."""

    __tablename__ = "institutions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    client_id: Mapped[str] = mapped_column(ForeignKey("clients.id"), index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    short_name: Mapped[str | None] = mapped_column(String(50))
    charter_type: Mapped[str | None] = mapped_column(String(50))  # bank, credit_union, thrift
    state: Mapped[str | None] = mapped_column(String(2))
    primary_domain: Mapped[str | None] = mapped_column(String(255))
    additional_domains: Mapped[list | None] = mapped_column(JSONB)
    bin_ranges: Mapped[list | None] = mapped_column(JSONB)  # card BIN prefixes
    routing_numbers: Mapped[list | None] = mapped_column(JSONB)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    client: Mapped[Client] = relationship(back_populates="institutions")
    watch_terms: Mapped[list[WatchTerm]] = relationship(back_populates="institution", cascade="all, delete-orphan")
    findings: Mapped[list[Finding]] = relationship(back_populates="institution")

    __table_args__ = (
        Index("ix_institutions_client_name", "client_id", "name"),
    )


class WatchTerm(Base):
    """A search term to monitor for an institution.

    Decouples "what to look for" from "where to look".
    """

    __tablename__ = "watch_terms"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    institution_id: Mapped[str] = mapped_column(ForeignKey("institutions.id"), index=True, nullable=False)
    term_type: Mapped[WatchTermType] = mapped_column(Enum(WatchTermType), nullable=False)
    value: Mapped[str] = mapped_column(String(500), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    case_sensitive: Mapped[bool] = mapped_column(Boolean, default=False)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    institution: Mapped[Institution] = relationship(back_populates="watch_terms")

    __table_args__ = (
        Index("ix_watch_terms_inst_type", "institution_id", "term_type"),
    )


class Source(Base):
    """A dark web source / collection point."""

    __tablename__ = "sources"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    source_type: Mapped[SourceType] = mapped_column(Enum(SourceType), nullable=False)
    url: Mapped[str | None] = mapped_column(Text)  # onion or clearnet URL
    connector_class: Mapped[str | None] = mapped_column(String(255))  # Python class path
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    poll_interval_seconds: Mapped[int] = mapped_column(Integer, default=3600)
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    config: Mapped[dict | None] = mapped_column(JSONB)  # source-specific config
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    findings: Mapped[list[Finding]] = relationship(back_populates="source")


class Finding(Base):
    """A single dark web mention / hit.

    Central entity in the analyst workflow.
    """

    __tablename__ = "findings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    institution_id: Mapped[str] = mapped_column(ForeignKey("institutions.id"), index=True, nullable=False)
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.id"), index=True)
    severity: Mapped[Severity] = mapped_column(Enum(Severity), default=Severity.medium, index=True)
    status: Mapped[FindingStatus] = mapped_column(Enum(FindingStatus), default=FindingStatus.new, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    raw_content: Mapped[str | None] = mapped_column(Text)  # original scraped content
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True)  # SHA-256 for dedup
    source_url: Mapped[str | None] = mapped_column(Text)  # original URL (onion or clearnet)
    matched_terms: Mapped[list | None] = mapped_column(JSONB)  # which watch terms triggered
    tags: Mapped[list | None] = mapped_column(JSONB)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB)
    analyst_notes: Mapped[str | None] = mapped_column(Text)
    assigned_to: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    reviewed_by: Mapped[str | None] = mapped_column(String(36))
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    institution: Mapped[Institution] = relationship(back_populates="findings")
    source: Mapped[Source | None] = relationship(back_populates="findings")

    @property
    def institution_name(self) -> str | None:
        return self.institution.name if self.institution else None

    @property
    def source_type(self) -> str | None:
        return self.source.source_type.value if self.source else None

    @property
    def source_name(self) -> str | None:
        return self.source.name if self.source else None

    __table_args__ = (
        Index("ix_findings_status_severity", "status", "severity"),
        Index("ix_findings_institution_status", "institution_id", "status"),
        Index("ix_findings_discovered", "discovered_at"),
    )


class RawMention(Base):
    """A raw collected mention that hasn't been matched to any watchterm yet.

    Stores ingested data from source connectors before/without watchterm matching,
    allowing analysts to manually review and optionally promote to findings.
    """

    __tablename__ = "raw_mentions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.id"), index=True, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True)  # SHA-256 dedup
    source_url: Mapped[str | None] = mapped_column(Text)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB)  # source-specific context
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    promoted_to_finding_id: Mapped[str | None] = mapped_column(ForeignKey("findings.id"))

    source: Mapped[Source] = relationship()

    @property
    def source_name(self) -> str | None:
        return self.source.name if self.source else None

    @property
    def source_type(self) -> str | None:
        return self.source.source_type.value if self.source else None

    __table_args__ = (
        Index("ix_raw_mentions_source_collected", "source_id", "collected_at"),
        Index("ix_raw_mentions_promoted", "promoted_to_finding_id"),
    )


class ExtractedFile(Base):
    """A file extracted from an archive attachment on a raw mention.

    Normalizes the per-file data previously stored in JSONB metadata
    (extracted_file_contents) into a proper relational table with
    full-text search on text_content.
    """

    __tablename__ = "extracted_files"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    mention_id: Mapped[str] = mapped_column(ForeignKey("raw_mentions.id"), index=True, nullable=False)
    filename: Mapped[str] = mapped_column(String(1024), nullable=False)
    s3_key: Mapped[str | None] = mapped_column(String(512))
    sha256: Mapped[str | None] = mapped_column(String(64), index=True)
    size: Mapped[int | None] = mapped_column(BigInteger)
    extension: Mapped[str | None] = mapped_column(String(32))
    is_text: Mapped[bool] = mapped_column(Boolean, default=False)
    text_content: Mapped[str | None] = mapped_column(Text)
    content_tsvector = mapped_column(
        TSVECTOR,
        Computed("to_tsvector('english', COALESCE(text_content, ''))", persisted=True),
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    mention: Mapped[RawMention] = relationship()

    __table_args__ = (
        Index("ix_extracted_files_content_fts", "content_tsvector", postgresql_using="gin"),
        Index("ix_extracted_files_mention_filename", "mention_id", "filename"),
    )


class DiscoveryStatus(str, enum.Enum):
    pending = "pending"
    approved = "approved"
    joined = "joined"
    failed = "failed"
    ignored = "ignored"


class DiscoveredChannel(Base):
    """A Telegram channel link discovered in monitored message content.

    Tracks t.me/ links found during polling so admins can review and
    auto-join promising channels.
    """

    __tablename__ = "discovered_channels"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    url: Mapped[str] = mapped_column(String(512), nullable=False)
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.id"), index=True, nullable=False)
    source_channel: Mapped[str | None] = mapped_column(String(255))  # which channel it was found in
    message_id: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[DiscoveryStatus] = mapped_column(
        Enum(DiscoveryStatus), default=DiscoveryStatus.pending,
    )
    added_to_source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.id"))
    notes: Mapped[str | None] = mapped_column(Text)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    joined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    source: Mapped[Source] = relationship(foreign_keys=[source_id])

    __table_args__ = (
        Index("ix_discovered_channels_url", "url"),
        Index("ix_discovered_channels_status", "status"),
        Index("ix_discovered_channels_source_status", "source_id", "status"),
    )


class ImageOCRCache(Base):
    """Cache of OCR results keyed by image SHA-256 hash.

    Prevents re-processing the same image when actors repost screenshots
    across channels or in repeated messages.
    """

    __tablename__ = "image_ocr_cache"

    sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    ocr_text: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    engine: Mapped[str] = mapped_column(String(32), default="easyocr")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FindingAttachment(Base):
    """File or screenshot attached to a finding."""

    __tablename__ = "finding_attachments"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    finding_id: Mapped[str] = mapped_column(ForeignKey("findings.id"), index=True, nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    content_type: Mapped[str | None] = mapped_column(String(100))
    size_bytes: Mapped[int | None] = mapped_column(Integer)
    sha256_hash: Mapped[str | None] = mapped_column(String(64))
    s3_key: Mapped[str | None] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class User(Base):
    """Application user (analyst or admin)."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    username: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), default=UserRole.analyst)
    disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_login: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AlertRule(Base):
    """Automated alerting rule — triggers notifications on new findings matching criteria."""

    __tablename__ = "alert_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    institution_id: Mapped[str | None] = mapped_column(ForeignKey("institutions.id"))
    min_severity: Mapped[Severity] = mapped_column(Enum(Severity), default=Severity.high)
    source_types: Mapped[list | None] = mapped_column(JSONB)  # filter by source type
    keyword_filter: Mapped[str | None] = mapped_column(String(500))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    notify_email: Mapped[bool] = mapped_column(Boolean, default=False)
    notify_slack: Mapped[bool] = mapped_column(Boolean, default=False)
    notify_webhook_url: Mapped[str | None] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Notification(Base):
    """In-app notification for an analyst."""

    __tablename__ = "notifications"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True, nullable=False)
    alert_rule_id: Mapped[str | None] = mapped_column(ForeignKey("alert_rules.id"))
    finding_id: Mapped[str | None] = mapped_column(ForeignKey("findings.id"))
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    message: Mapped[str | None] = mapped_column(Text)
    read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
