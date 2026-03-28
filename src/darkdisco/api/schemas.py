"""Pydantic request/response schemas for the DarkDisco API."""

from __future__ import annotations

from datetime import datetime

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from darkdisco.common.models import (
    DateRangeMode,
    DeliveryMethod,
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
    last_polled_at: datetime | None = None
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
    classification: str | None = None
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
    classification: str | None = None
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


class FindingNoteAdd(BaseModel):
    content: str


class FindingAuditLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    finding_id: str
    action: str
    username: str | None = None
    field: str | None = None
    old_value: str | None = None
    new_value: str | None = None
    created_at: datetime


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


class UserOut(BaseModel):
    id: str
    username: str
    role: str
    disabled: bool
    created_at: str | None = None
    last_login: str | None = None


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


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

class InstitutionExportRow(BaseModel):
    """A single institution with its related data for export."""
    name: str
    short_name: str | None = None
    charter_type: str | None = None
    state: str | None = None
    primary_domain: str | None = None
    additional_domains: list | None = None
    bin_ranges: list | None = None
    routing_numbers: list | None = None
    active: bool = True
    watch_terms: list[dict] | None = None


class InstitutionImportResult(BaseModel):
    """Summary of an institution import operation."""
    imported: int
    skipped: int
    errors: list[str]


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


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

class ReportSections(BaseModel):
    """Toggle individual report sections."""
    executive_summary: bool = True
    charts: bool = True
    findings_detail: bool = True
    findings_by_severity: bool = True
    source_activity: bool = True
    institution_exposure: bool = True
    classification_breakdown: bool = True
    timeline: bool = True
    appendix_full_content: bool = False
    fp_analytics: bool = False
    pattern_effectiveness: bool = False
    institution_threat_summary: bool = False
    analyst_performance: bool = False


class ReportChartOptions(BaseModel):
    """Toggle individual charts."""
    severity_pie: bool = True
    status_pie: bool = True
    trend_line: bool = True
    source_bar: bool = True
    institution_bar: bool = True
    severity_trend: bool = True
    fp_rate_bar: bool = False
    disposition_pie: bool = False
    analyst_throughput_bar: bool = False
    threat_category_bar: bool = False


class ReportRequest(BaseModel):
    """Request body for report generation."""
    title: str = "DarkDisco Threat Intelligence Report"
    subtitle: str = "Dark Web Threat Intelligence Report"
    date_from: datetime | None = None
    date_to: datetime | None = None
    client_id: str | None = None
    institution_id: str | None = None
    severities: list[str] | None = None
    statuses: list[str] | None = None
    sections: ReportSections = Field(default_factory=ReportSections)
    charts: ReportChartOptions = Field(default_factory=ReportChartOptions)
    truncate_content: bool = True


class ReportTemplateConfig(BaseModel):
    """Saved report configuration (everything except date range)."""
    title: str = "DarkDisco Threat Intelligence Report"
    client_id: str | None = None
    institution_id: str | None = None
    severities: list[str] | None = None
    statuses: list[str] | None = None
    sections: ReportSections = Field(default_factory=ReportSections)
    charts: ReportChartOptions = Field(default_factory=ReportChartOptions)


class ReportTemplateCreate(BaseModel):
    """Create a report template."""
    name: str
    description: str | None = None
    config: ReportTemplateConfig


class ReportTemplateUpdate(BaseModel):
    """Update a report template."""
    name: str | None = None
    description: str | None = None
    config: ReportTemplateConfig | None = None


class ReportTemplateOut(BaseModel):
    """Report template response."""
    model_config = ConfigDict(from_attributes=True)
    id: str
    name: str
    description: str | None
    owner_id: str
    config: dict
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Report Schedules
# ---------------------------------------------------------------------------

class ReportScheduleCreate(BaseModel):
    """Create a scheduled report."""
    template_id: str
    name: str
    cron_expression: str | None = None
    interval_seconds: int | None = None
    date_range_mode: DateRangeMode = DateRangeMode.last_7d
    enabled: bool = True
    delivery_method: DeliveryMethod = DeliveryMethod.s3_store
    recipients: list[str] | None = None


class ReportScheduleUpdate(BaseModel):
    """Update a scheduled report."""
    name: str | None = None
    template_id: str | None = None
    cron_expression: str | None = None
    interval_seconds: int | None = None
    date_range_mode: DateRangeMode | None = None
    enabled: bool | None = None
    delivery_method: DeliveryMethod | None = None
    recipients: list[str] | None = None


class ReportScheduleOut(BaseModel):
    """Report schedule response."""
    model_config = ConfigDict(from_attributes=True)
    id: str
    template_id: str
    owner_id: str
    name: str
    cron_expression: str | None
    interval_seconds: int | None
    date_range_mode: DateRangeMode
    enabled: bool
    delivery_method: DeliveryMethod
    recipients: list[str] | None
    last_run_at: datetime | None
    next_run_at: datetime | None
    created_at: datetime
    updated_at: datetime


class GeneratedReportOut(BaseModel):
    """Generated report response."""
    model_config = ConfigDict(from_attributes=True)
    id: str
    schedule_id: str | None
    template_id: str | None
    owner_id: str
    title: str
    file_size: int | None
    date_range_mode: str | None
    date_from: datetime | None
    date_to: datetime | None
    status: str
    error_message: str | None
    created_at: datetime


# ---------------------------------------------------------------------------
# BIN Database
# ---------------------------------------------------------------------------

class BINRecordOut(BaseModel):
    """BIN record response."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    bin_prefix: str
    bin_range_start: str | None = None
    bin_range_end: str | None = None
    issuer_name: str | None = None
    card_brand: str | None = None
    card_type: str | None = None
    card_level: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    bank_url: str | None = None
    bank_phone: str | None = None
    source: str | None = None
    updated_at: datetime | None = None


class BINLookupResponse(BaseModel):
    """Response from BIN lookup endpoint."""
    bin_prefix: str
    found: bool
    issuer_name: str | None = None
    card_brand: str | None = None
    card_type: str | None = None
    card_level: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    bank_url: str | None = None
    bank_phone: str | None = None


class BINImportResponse(BaseModel):
    """Response from BIN import endpoint."""
    imported: int
    updated: int
    skipped: int
    errors: list[str]
    source: str


class BINStatsResponse(BaseModel):
    """BIN database statistics."""
    total_records: int
    by_brand: dict[str, int]
    by_source: dict[str, int]
    by_country: list[dict]
    top_issuers: list[dict]


# ---------------------------------------------------------------------------
# Matching Filters
# ---------------------------------------------------------------------------

class MatchingFiltersOut(BaseModel):
    """Current matching filters configuration."""
    fraud_indicators: list[str]
    negative_patterns: list[str]


class MatchingFiltersUpdate(BaseModel):
    """Update matching filters configuration."""
    fraud_indicators: list[str]
    negative_patterns: list[str]


class MatchingFiltersTestRequest(BaseModel):
    """Request to test text against matching filters."""
    text: str


class MatchingFiltersTestResult(BaseModel):
    """Result of testing text against matching filters."""
    matched_negative_patterns: list[str]
    matched_fraud_indicators: list[str]
    would_suppress: bool
    would_require_fraud_indicator: bool


# ---------------------------------------------------------------------------
# Institution Threat Summary
# ---------------------------------------------------------------------------

class ThreatCategoryBreakdown(BaseModel):
    category: str
    count: int

class SourceChannelBreakdown(BaseModel):
    source_type: str
    count: int

class ThreatSummary(BaseModel):
    """Per-institution threat summary with timeline, categories, and brief."""
    institution_id: str
    institution_name: str
    findings_timeline: list[dict]
    threat_categories: list[ThreatCategoryBreakdown]
    total_findings: int
    confirmed_threats: int
    active_threat_actors: int
    top_source_channels: list[SourceChannelBreakdown]
    by_severity: dict[str, int]
    by_status: dict[str, int]
    executive_brief: str


# ---------------------------------------------------------------------------
# Analytics / Disposition Dashboard
# ---------------------------------------------------------------------------


class InstitutionFPRate(BaseModel):
    institution_id: str
    institution_name: str
    total_findings: int
    false_positives: int
    dismissed: int
    confirmed: int
    fp_rate: float  # 0.0–1.0


class PatternEffectiveness(BaseModel):
    total_mentions: int
    total_promoted: int
    total_suppressed: int
    suppression_rate: float
    fp_score_distribution: list[dict]  # [{bucket: "0.0-0.2", count: N}, ...]


class AnalystWorkload(BaseModel):
    pending_review: int  # new + reviewing
    avg_disposition_hours: float | None
    disposition_breakdown: list[dict]  # [{status: "confirmed", count: N}, ...]
    by_analyst: list[dict]  # [{analyst: "name", reviewed: N, pending: N}, ...]


class DispositionTrend(BaseModel):
    date: str
    confirmed: int
    dismissed: int
    false_positive: int
    escalated: int
    new: int


class DispositionAnalytics(BaseModel):
    institution_fp_rates: list[InstitutionFPRate]
    pattern_effectiveness: PatternEffectiveness
    analyst_workload: AnalystWorkload
    disposition_trends: list[DispositionTrend]


# ---------------------------------------------------------------------------
# System Settings
# ---------------------------------------------------------------------------

class SystemSettingOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    key: str
    value: str
    description: str | None = None
    updated_at: datetime | None = None


class SystemSettingUpdate(BaseModel):
    value: str
