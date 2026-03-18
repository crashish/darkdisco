"""Pydantic request/response schemas for the DarkDisco API."""

from __future__ import annotations

from datetime import datetime

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from darkdisco.common.models import (
    DiscoveryStatus,
    FindingStatus,
    Severity,
    SourceType,
    WatchTermType,
)


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

class ClientCreate(BaseModel):
    name: str
    contract_ref: str | None = None
    active: bool = True
    notes: str | None = None


class ClientUpdate(BaseModel):
    name: str | None = None
    contract_ref: str | None = None
    active: bool | None = None
    notes: str | None = None


class ClientOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    contract_ref: str | None = None
    active: bool
    notes: str | None = None
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Institutions
# ---------------------------------------------------------------------------

class InstitutionCreate(BaseModel):
    client_id: str
    name: str
    short_name: str | None = None
    charter_type: str | None = None
    state: str | None = None
    primary_domain: str | None = None
    additional_domains: list | None = None
    bin_ranges: list | None = None
    routing_numbers: list | None = None
    metadata: dict | None = Field(None, validation_alias=AliasChoices("metadata_", "metadata"))
    active: bool = True


class InstitutionUpdate(BaseModel):
    name: str | None = None
    short_name: str | None = None
    charter_type: str | None = None
    state: str | None = None
    primary_domain: str | None = None
    additional_domains: list | None = None
    bin_ranges: list | None = None
    routing_numbers: list | None = None
    metadata: dict | None = Field(None, validation_alias=AliasChoices("metadata_", "metadata"))
    active: bool | None = None


class InstitutionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    client_id: str
    name: str
    short_name: str | None = None
    charter_type: str | None = None
    state: str | None = None
    primary_domain: str | None = None
    additional_domains: list | None = None
    bin_ranges: list | None = None
    routing_numbers: list | None = None
    active: bool
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Watch Terms
# ---------------------------------------------------------------------------

class WatchTermCreate(BaseModel):
    institution_id: str
    term_type: WatchTermType
    value: str
    enabled: bool = True
    case_sensitive: bool = False
    notes: str | None = None


class WatchTermUpdate(BaseModel):
    term_type: WatchTermType | None = None
    value: str | None = None
    enabled: bool | None = None
    case_sensitive: bool | None = None
    notes: str | None = None


class WatchTermOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    institution_id: str
    term_type: WatchTermType
    value: str
    enabled: bool
    case_sensitive: bool
    notes: str | None = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

class SourceCreate(BaseModel):
    name: str
    source_type: SourceType
    url: str | None = None
    connector_class: str | None = None
    enabled: bool = True
    poll_interval_seconds: int = 3600
    config: dict | None = None


class SourceUpdate(BaseModel):
    name: str | None = None
    source_type: SourceType | None = None
    url: str | None = None
    connector_class: str | None = None
    enabled: bool | None = None
    poll_interval_seconds: int | None = None
    config: dict | None = None


class SourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    source_type: SourceType
    url: str | None = None
    connector_class: str | None = None
    enabled: bool
    poll_interval_seconds: int
    last_polled_at: datetime | None = None
    last_error: str | None = None
    config: dict | None = None
    created_at: datetime
    # Computed fields for frontend
    health: str = "offline"
    finding_count: int = 0
    avg_poll_seconds: int = 0
    last_poll: datetime | None = None


# ---------------------------------------------------------------------------
# Findings
# ---------------------------------------------------------------------------

class FindingCreate(BaseModel):
    institution_id: str
    source_id: str | None = None
    severity: Severity = Severity.medium
    status: FindingStatus = FindingStatus.new
    title: str
    summary: str | None = None
    raw_content: str | None = None
    content_hash: str | None = None
    source_url: str | None = None
    matched_terms: list | None = None
    tags: list | None = None
    metadata: dict | None = Field(None, validation_alias=AliasChoices("metadata_", "metadata"))
    analyst_notes: str | None = None
    assigned_to: str | None = None


class FindingUpdate(BaseModel):
    severity: Severity | None = None
    status: FindingStatus | None = None
    title: str | None = None
    summary: str | None = None
    analyst_notes: str | None = None
    assigned_to: str | None = None
    tags: list | None = None
    metadata: dict | None = Field(None, validation_alias=AliasChoices("metadata_", "metadata"))


class FindingOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    institution_id: str
    institution_name: str | None = None
    source_id: str | None = None
    source_type: str | None = None
    source_name: str | None = None
    severity: Severity
    status: FindingStatus
    title: str
    summary: str | None = None
    raw_content: str | None = None
    content_hash: str | None = None
    source_url: str | None = None
    matched_terms: list | None = None
    tags: list | None = None
    analyst_notes: str | None = None
    assigned_to: str | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    metadata: dict | None = Field(None, validation_alias=AliasChoices("metadata_", "metadata"))
    discovered_at: datetime
    created_at: datetime
    updated_at: datetime
    # Computed from joins
    institution_name: str | None = None
    source_type: str | None = None
    source_name: str | None = None


class PaginatedFindingsOut(BaseModel):
    items: list[FindingOut]
    total: int
    page: int
    page_size: int


class FindingStatusTransition(BaseModel):
    status: FindingStatus
    notes: str | None = None


# ---------------------------------------------------------------------------
# Raw Mentions
# ---------------------------------------------------------------------------

class RawMentionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    source_id: str
    source_name: str | None = None
    source_type: str | None = None
    content: str
    content_hash: str | None = None
    source_url: str | None = None
    metadata: dict | None = Field(None, validation_alias=AliasChoices("metadata_", "metadata"))
    collected_at: datetime
    promoted_to_finding_id: str | None = None


class PaginatedMentionsOut(BaseModel):
    items: list[RawMentionOut]
    total: int
    page: int
    page_size: int


class RawMentionPromote(BaseModel):
    institution_id: str
    title: str
    severity: Severity = Severity.medium
    summary: str | None = None
    tags: list | None = None


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class SeverityCount(BaseModel):
    severity: Severity
    count: int


class StatusCount(BaseModel):
    status: FindingStatus
    count: int


class DashboardStats(BaseModel):
    total_findings: int
    findings_by_severity: dict[str, int]
    new_today: int
    monitored_institutions: int
    active_sources: int
    findings_trend: list[dict]
    by_severity: list[SeverityCount]
    by_status: list[StatusCount]
    recent_findings: list[FindingOut]


# ---------------------------------------------------------------------------
# Generic pagination wrapper
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Alert Rules
# ---------------------------------------------------------------------------

class AlertRuleCreate(BaseModel):
    name: str
    owner_id: str
    institution_id: str | None = None
    min_severity: Severity = Severity.high
    source_types: list[str] | None = None
    keyword_filter: str | None = None
    enabled: bool = True
    notify_email: bool = False
    notify_slack: bool = False
    notify_webhook_url: str | None = None


class AlertRuleUpdate(BaseModel):
    name: str | None = None
    institution_id: str | None = None
    min_severity: Severity | None = None
    source_types: list[str] | None = None
    keyword_filter: str | None = None
    enabled: bool | None = None
    notify_email: bool | None = None
    notify_slack: bool | None = None
    notify_webhook_url: str | None = None


class AlertRuleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    owner_id: str
    institution_id: str | None = None
    min_severity: Severity
    source_types: list[str] | None = None
    keyword_filter: str | None = None
    enabled: bool
    notify_email: bool
    notify_slack: bool
    notify_webhook_url: str | None = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

class NotificationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    alert_rule_id: str | None = None
    finding_id: str | None = None
    title: str
    message: str | None = None
    read: bool
    created_at: datetime


class NotificationMarkRead(BaseModel):
    read: bool = True


# ---------------------------------------------------------------------------
# Generic pagination wrapper
# ---------------------------------------------------------------------------

class PaginatedResponse(BaseModel):
    items: list
    total: int
    page: int
    page_size: int


# ---------------------------------------------------------------------------
# Telegram Channel Management
# ---------------------------------------------------------------------------

class ChannelOut(BaseModel):
    channel: str
    last_message_id: int | None = None


class ChannelAdd(BaseModel):
    channel: str
    join: bool = True


class ChannelRemoveOut(BaseModel):
    removed: str


# ---------------------------------------------------------------------------
# Discord Guild/Channel Management
# ---------------------------------------------------------------------------

class DiscordGuildChannelOut(BaseModel):
    guild_id: str
    channel_ids: list[str]


class DiscordChannelAdd(BaseModel):
    guild_id: str
    channel_id: str


class DiscordChannelRemoveOut(BaseModel):
    guild_id: str
    removed_channel: str


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TokenRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ---------------------------------------------------------------------------
# Integration (trapline connector)
# ---------------------------------------------------------------------------

class DomainMatchResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    institution_id: str
    name: str
    primary_domain: str | None = None
    additional_domains: list | None = None
    bin_ranges: list | None = None
    match_type: str  # "exact_primary", "exact_additional", "fuzzy_name"
    score: float  # 1.0 for exact, <1.0 for fuzzy


class InstitutionDomainExport(BaseModel):
    institution_id: str
    name: str
    primary_domain: str | None = None
    additional_domains: list[str]
    bin_ranges: list | None = None


class TraplineWebhookPayload(BaseModel):
    """Inbound webhook payload from trapline when a finding completes the pipeline."""
    event: str = "finding.completed"
    domain: str
    score: float = 0.0
    brands: list[str] = []
    artifacts: dict = {}
    screenshot_url: str | None = None
    finding_id: str | None = None  # trapline's own finding ID
    completed_at: str | None = None
    # Enrichment fields
    dns_records: dict | None = None        # {"A": [...], "CNAME": [...], "MX": [...], "NS": [...], "resolved_ips": [{"ip": ..., "asn": ..., "org": ...}]}
    whois: dict | None = None              # {"registrar": ..., "creation_date": ..., "expiry_date": ..., "name_servers": [...], "registrant_org": ..., "registrant_country": ...}
    tls_certificate: dict | None = None    # {"issuer": ..., "subject": ..., "not_before": ..., "not_after": ..., "sans": [...], "serial_number": ...}
    network_log: list[dict] | None = None  # [{"domain": ..., "resource_type": ..., "status": ..., "url": ...}, ...]
    score_breakdown: list[dict] | None = None  # [{"signal": ..., "weight": ..., "detail": ...}, ...]


class TraplineWebhookResponse(BaseModel):
    status: str
    finding_id: str | None = None
    institution_id: str | None = None
    message: str | None = None


# ---------------------------------------------------------------------------
# Pipeline diagnostics
# ---------------------------------------------------------------------------

class PipelineStatus(BaseModel):
    enabled_sources: int
    total_sources: int
    active_watch_terms: int
    total_findings: int
    recent_findings_24h: int
    sources: list[dict]
    watch_term_coverage: dict[str, int]


class DryRunRequest(BaseModel):
    content: str
    title: str = ""
    source_name: str = "dry-run"


class DryRunMatch(BaseModel):
    institution_id: str
    institution_name: str | None = None
    matched_terms: list[dict]
    severity_hint: str


class DryRunResult(BaseModel):
    matches: list[DryRunMatch]
    fp_analysis: dict | None = None
    would_create_finding: bool



# ---------------------------------------------------------------------------
# Discovered Channels
# ---------------------------------------------------------------------------

class DiscoveredChannelOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    url: str
    source_id: str
    source_channel: str | None = None
    message_id: int | None = None
    status: DiscoveryStatus
    added_to_source_id: str | None = None
    notes: str | None = None
    discovered_at: datetime
    joined_at: datetime | None = None


class DiscoveredChannelUpdate(BaseModel):
    status: DiscoveryStatus
    target_source_id: str | None = None
    notes: str | None = None


# ---------------------------------------------------------------------------
# Bulk BIN / Routing Number Population
# ---------------------------------------------------------------------------

class BinRoutingEntry(BaseModel):
    """A single institution's BIN/routing data for bulk population."""
    name: str
    bin_ranges: list[str] = []
    routing_numbers: list[str] = []


class BinRoutingResult(BaseModel):
    """Per-institution result from bulk population."""
    name: str
    status: str  # "updated", "up_to_date", "not_found"
    bins_added: int = 0
    routing_added: int = 0
    watch_terms_created: int = 0


class BinRoutingSummary(BaseModel):
    """Summary of a bulk BIN/routing population run."""
    matched: int
    not_found: int
    institutions_updated: int
    bins_added: int
    routing_added: int
    watch_terms_created: int
    results: list[BinRoutingResult]
