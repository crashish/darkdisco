"""Pydantic request/response schemas for the DarkDisco API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from darkdisco.common.models import (
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
    metadata_: dict | None = Field(None, alias="metadata")
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
    metadata_: dict | None = Field(None, alias="metadata")
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


class PollTriggerResponse(BaseModel):
    source_id: str
    status: str
    message: str
    polled_at: datetime


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
    metadata_: dict | None = Field(None, alias="metadata")
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
    metadata_: dict | None = Field(None, alias="metadata")


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
    discovered_at: datetime
    created_at: datetime
    updated_at: datetime


class FindingStatusTransition(BaseModel):
    status: FindingStatus
    notes: str | None = None


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
# Auth
# ---------------------------------------------------------------------------

class TokenRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
